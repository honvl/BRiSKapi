"""Decoded recordings on disk and the friendly quote/master views built from them."""
from __future__ import annotations

import datetime as dt
import gzip
import io
import json
from pathlib import Path
import re
from typing import Iterator

from briskapi.schema import canonical_lines, validate_stream

JST = dt.timezone(dt.timedelta(hours=9), 'JST')
PRICES = ('last_price10', 'open_price10', 'bid_price10', 'ask_price10', 'indicative_price10',
          'closing_indicative_price10', 'indicative_open_price10', 'auction_reference_price10')
MASTER_PRICES = ('base_price10', 'limit_up10', 'limit_down10')


class BriskError(Exception):
    """Base class for API errors."""


class NotFoundError(BriskError, KeyError):
    """A security code or recording is not available."""

    def __str__(self):
        return str(self.args[0]) if self.args else ''


class Table(list):
    """Rows as dicts; `to_pandas()` returns a DataFrame when pandas is installed."""

    def to_pandas(self):
        try:
            import pandas as pd
        except ImportError as e:  # pragma: no cover - depends on the environment
            raise ImportError("Install pandas (pip install 'briskapi[pandas]') to use to_pandas()") from e
        return pd.DataFrame(list(self))


class _Lines:
    def __init__(self, lines):
        self.lines = iter(lines)

    def readline(self, size=-1):
        return next(self.lines, b'')


def timestamp(date: str, micros: int) -> dt.datetime:
    """JST datetime for microseconds since midnight on a YYYYMMDD trading date."""
    day = dt.datetime.strptime(date, '%Y%m%d').replace(tzinfo=JST)
    return day + dt.timedelta(microseconds=micros)


def micros(date: str, at) -> int | None:
    """Accept None, source microseconds, 'HH:MM[:SS[.ffffff]]', time or aware/naive JST datetime."""
    if at is None or (type(at) is int and at >= 0):
        return at
    if isinstance(at, str):
        match = re.fullmatch(r'(\d{1,2}):(\d{2})(?::(\d{2})(?:\.(\d{1,6}))?)?', at)
        if not match:
            raise ValueError(f'Use HH:MM[:SS[.ffffff]] for times, not {at!r}')
        h, m, s, frac = match.groups()
        at = dt.time(int(h), int(m), int(s or 0), int((frac or '0').ljust(6, '0')))
    if isinstance(at, dt.datetime):
        at = at.astimezone(JST) if at.tzinfo else at.replace(tzinfo=JST)
        if at.strftime('%Y%m%d') != date:
            raise ValueError(f'{at.isoformat()} is not on trading date {date}')
        at = at.timetz()
    if isinstance(at, dt.time):
        return ((at.hour * 60 + at.minute) * 60 + at.second) * 1_000_000 + at.microsecond
    raise TypeError(f'Unsupported time: {at!r}')


def quote_view(quote: dict, date: str) -> dict:
    """Yen prices (None for the vendor's zero sentinel), JST times and the raw quantities/enums."""
    view = {'code': quote['code'], 'issue_id': quote['issue_id'], 'time': timestamp(date, quote['source_time_us'])}
    for key, value in quote.items():
        if key in PRICES:
            view[key[:-2]] = value / 10 if value else None
        elif key == 'special_quote_time_us':
            view['special_quote_time'] = timestamp(date, value) if value else None
        elif key not in view and key != 'source_time_us':
            view[key] = value
    return view


def master_view(entry: dict) -> dict:
    """Master entry with base price and daily limits in yen."""
    return {key[:-2] if key in MASTER_PRICES else key: value / 10 if key in MASTER_PRICES else value
            for key, value in entry.items()}


class Recording:
    """A decoded BRiSK stream: events.jsonl, events.jsonl.gz or a directory holding one.

    Reading is streaming; whole-market queries parse the file once (about ten
    seconds for the complete 420 MB demo replay) and the final state is cached.
    """

    contribution = None  # Publication status when record() contributed this recording.

    def __init__(self, path):
        path = Path(path).expanduser()
        self.directory = path if path.is_dir() else path.parent
        if path.is_dir():
            path = next((path / n for n in ('events.jsonl', 'events.jsonl.gz') if (path / n).exists()), None)
            if path is None:
                raise NotFoundError(f'No events.jsonl(.gz) in {self.directory}')
        if not path.exists():
            raise NotFoundError(f'No recording at {path}')
        self.path = path
        manifest = self.directory / 'manifest.json'
        self.manifest = json.loads(manifest.read_text()) if manifest.exists() else None
        self._bootstrap = None
        self._final = None
        self._last_source_time_us = None

    def __repr__(self):
        return f'Recording({str(self.path)!r})'

    def open(self):
        return gzip.open(self.path, 'rb') if self.path.suffix == '.gz' else self.path.open('rb')

    def lines(self) -> Iterator[bytes]:
        with self.open() as f:
            yield from f

    def batches(self) -> Iterator[dict]:
        """Every decoded batch in order, exactly as recorded."""
        for line in self.lines():
            yield json.loads(line)

    @property
    def bootstrap(self) -> dict:
        if self._bootstrap is None:
            with self.open() as f:
                self._bootstrap = json.loads(f.readline())
        return self._bootstrap

    @property
    def trading_date(self) -> str:
        return self.bootstrap['trading_date']

    @property
    def source(self) -> str:
        return self.bootstrap['source']

    @property
    def clock_range(self) -> tuple[int, int]:
        """First and last batch clocks, including empty batches and a clean end."""
        if self.manifest:
            last = self.manifest['summary']['last_source_time_us']
        else:
            if self._last_source_time_us is None:
                for batch in self.batches():
                    self._last_source_time_us = batch['source_time_us']
            last = self._last_source_time_us
        return self.bootstrap['source_time_us'], last

    @property
    def codes(self) -> list[str]:
        return [m['code'] for m in self.bootstrap['master']]

    def issue(self, code: str) -> dict:
        for entry in self.bootstrap['master']:
            if entry['code'] == str(code):
                return entry
        raise NotFoundError(f'{code} is not in this recording')

    def validate(self) -> dict:
        """Run the archive's full checks (canonical form, timing bounds, reference replay)."""
        with self.open() as f:
            return validate_stream(_Lines(canonical_lines(f)))

    def state(self, at=None) -> dict[int, dict]:
        """Raw latest quote per issue ID at a source time (default: end of recording)."""
        limit = micros(self.trading_date, at)
        if limit is None and self._final is not None:
            return dict(self._final)
        quotes = {}
        for batch in self.batches():
            # A batch clock bounds its quotes from above, so a later batch can
            # still contain an update from before the requested quote time.
            quotes.update((q['issue_id'], q) for q in batch.get('quotes', ())
                          if limit is None or q['source_time_us'] <= limit)
            self._last_source_time_us = batch['source_time_us']
        if limit is None:
            self._final = dict(quotes)
        return quotes

    def updates(self, code: str, start=None, end=None) -> Iterator[dict]:
        """Raw quote updates for one security, skipping unrelated batches without parsing them."""
        issue = self.issue(code)['issue_id']
        needle = json.dumps(str(code)).encode()  # Matches any JSON spacing; a false hit only costs a parse.
        low, high = micros(self.trading_date, start), micros(self.trading_date, end)
        for line in self.lines():
            if needle not in line:
                continue
            for quote in json.loads(line).get('quotes', ()):
                if quote['issue_id'] != issue:
                    continue
                if high is not None and quote['source_time_us'] > high:
                    return
                if low is None or quote['source_time_us'] >= low:
                    yield quote
