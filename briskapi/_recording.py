"""Decoded recordings on disk and the friendly quote/master views built from them."""
from __future__ import annotations

import datetime as dt
import gzip
import io
import json
from pathlib import Path
import re
from typing import Iterator

from briskapi import _parquet
from briskapi.schema import BOOK_CHANGES, canonical_lines, encode, validate_stream

JST = dt.timezone(dt.timedelta(hours=9), 'JST')
PRICES = ('last_price10', 'open_price10', 'bid_price10', 'ask_price10', 'indicative_price10',
          'closing_indicative_price10', 'indicative_open_price10', 'reference_price10')
MASTER_PRICES = ('base_price10', 'limit_up10', 'limit_down10')
BOOK_VIEWS = {'bid_levels': 'bids', 'ask_levels': 'asks', 'bid_closing_levels': 'closing_bids', 'ask_closing_levels': 'closing_asks'}
SIDES = {0: 'buy', 1: 'sell'}
# The decoder's trade types: 1 buyer-initiated, 2 seller-initiated; 0 (most opening auction prints) has no side.
TRADE_SIDES = {1: 'buy', 2: 'sell'}


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
    """Yen prices (None for the vendor's zero sentinel), JST times and the raw quantities/enums.

    The book sides become `bids` and `asks` (and the closing-condition orders' `closing_bids` and
    `closing_asks`): the best prices holding orders, best first, as {'price', 'quantity', 'orders'}.
    `turnover10` (tenths of a yen) becomes `turnover` in yen. `special_quote` is None, or the special
    quote (quote_flag 2) as {'side': 'buy' or 'sell', 'price', 'until'}, where `until` is when its 180 s
    timer ends; the raw `quote_flag` and `quote_side` stay.
    """
    view = {'code': quote['code'], 'issue_id': quote['issue_id'], 'time': timestamp(date, quote['source_time_us'])}
    for key, value in quote.items():
        if key in PRICES:
            view[key[:-2]] = value / 10 if value else None
        elif key in BOOK_VIEWS:
            view[BOOK_VIEWS[key]] = [{'price': p / 10, 'quantity': q, 'orders': n} for p, q, n in value]
        elif key == 'turnover10':
            view['turnover'] = value / 10
        elif key == 'special_quote_time_us':
            view['special_quote_time'] = timestamp(date, value) if value else None
        elif key not in view and key not in ('source_time_us', BOOK_CHANGES):   # the whole books: Ticker.book()
            view[key] = value
    special = quote.get('quote_flag') == 2
    view['special_quote'] = {'side': SIDES.get(quote['quote_side']), 'price': view.get('reference_price'),
                             'until': view.get('special_quote_time')} if special else None
    return view


def trade_view(trade: dict, date: str) -> dict:
    """A trade with its JST time, yen price, shares and side ('buy', 'sell' or None)."""
    return {'code': trade['code'], 'issue_id': trade['issue_id'], 'time': timestamp(date, trade['time_us']),
            'price': trade['price10'] / 10, 'quantity': trade['quantity'], 'side': TRADE_SIDES.get(trade['side']),
            'frame': trade['frame']}


def master_view(entry: dict) -> dict:
    """Master entry with base price and daily limits in yen."""
    return {key[:-2] if key in MASTER_PRICES else key: value / 10 if key in MASTER_PRICES else value
            for key, value in entry.items()}


class Recording:
    """A decoded BRiSK stream: a recording directory of Parquet tables (see briskapi._parquet), or an
    older events.jsonl, events.jsonl.gz or directory holding one.

    Parquet recordings answer quotes, snapshots and per-security history from their columns, read
    once and kept in memory (one security's history is read alone). JSONL is read as a stream.
    """

    uploaded = None  # The archive's status when record() uploaded this recording.

    def __init__(self, path):
        path = Path(path).expanduser()
        self.directory = path if path.is_dir() else path.parent
        self.format = 'jsonl'
        if path.is_dir() and (path / _parquet.DEPTHS).exists():
            self.format = 'parquet'
        elif path.is_dir():
            path = next((path / n for n in ('events.jsonl', 'events.jsonl.gz') if (path / n).exists()), None)
            if path is None:
                raise NotFoundError(f'No recording in {self.directory}: expected data/*.parquet or events.jsonl[.gz]')
        if not path.exists():
            raise NotFoundError(f'No recording at {path}')
        self.path = path
        manifest = self.directory / 'manifest.json'
        self.manifest = json.loads(manifest.read_text()) if manifest.exists() else None
        self._bootstrap = None
        self._columns = None
        self._final = None
        self._last_source_time_us = None

    def __repr__(self):
        return f'Recording({str(self.path)!r})'

    def _load(self) -> _parquet.Columns:
        if self._columns is None:
            self._columns = _parquet.Columns.load(self.directory)
        return self._columns

    def open(self):
        """The JSONL file (JSONL recordings only)."""
        if self.format == 'parquet':
            raise BriskError(f'{self.directory} is a Parquet recording; use batches()')
        return gzip.open(self.path, 'rb') if self.path.suffix == '.gz' else self.path.open('rb')

    def lines(self) -> Iterator[bytes]:
        """Batches as JSON lines (canonical ones for a Parquet recording)."""
        if self.format == 'parquet':
            yield from map(encode, self.batches())
            return
        with self.open() as f:
            yield from f

    def batches(self) -> Iterator[dict]:
        """Every decoded batch in order, exactly as recorded."""
        if self.format == 'parquet':
            yield from self._load().iter_batches()
            return
        for line in self.lines():
            yield json.loads(line)

    @property
    def bootstrap(self) -> dict:
        """The first batch: source, clocks and master (its quotes too, for JSONL recordings)."""
        if self._bootstrap is None:
            if self.format == 'parquet':
                self._bootstrap, self._last_source_time_us = _parquet.header(self.directory)
            else:
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
        elif self.format == 'parquet':
            last = _parquet.header(self.directory)[1]
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
        """Run the archive's checks (exact tables, timing bounds, a replay's reference, a broker's structure)."""
        if self.format == 'parquet':
            return _parquet.summarize(self.directory)
        with self.open() as f:
            return validate_stream(_Lines(canonical_lines(f)))

    def state(self, at=None) -> dict[int, dict]:
        """Raw latest quote per issue ID at a source time (default: end of recording)."""
        limit = micros(self.trading_date, at)
        if limit is None and self._final is not None:
            return dict(self._final)
        if self.format == 'parquet':
            # Read a row group at a time unless already in memory: a broker's session is tens of millions of updates.
            quotes = self._columns.state(limit) if self._columns is not None else _parquet.state(self.directory, limit)
            if limit is None:
                self._final = dict(quotes)
            return quotes
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

    def trades(self, code=None, start=None, end=None) -> list[dict]:
        """Raw trades in source-time order, of one security or all, optionally bounded by time."""
        issue = None if code is None else self.issue(code)['issue_id']
        low, high = micros(self.trading_date, start), micros(self.trading_date, end)
        if self.format == 'parquet':
            columns = self._columns or (_parquet.Columns.load(self.directory, issue_id=issue) if issue is not None
                                        else _parquet.Columns.trades_of(self.directory))
            trades = columns.trades(issue, low, high)
        else:
            trades = [t for batch in self.batches() for t in batch.get('trades', ())
                      if (issue is None or t['issue_id'] == issue) and (low is None or t['time_us'] >= low)
                      and (high is None or t['time_us'] <= high)]
        return sorted(trades, key=lambda t: t['time_us'])  # stable: each issue keeps the decoder's order

    def frames(self) -> list[dict]:
        """Every batch without its master, quotes and trades: clocks, local timing, skipped issues,
        and how many quotes and trades it held."""
        if self.format == 'parquet':
            return _parquet.frames(self.directory)
        return [{**{k: v for k, v in b.items() if k not in ('master', 'quotes', 'trades')},
                 'quote_count': len(b.get('quotes', ())), 'trade_count': len(b.get('trades', ()))} for b in self.batches()]

    def book_updates(self, code: str, at=None) -> Iterator[dict]:
        """Raw quote updates for one security from its first of the session that holds the time `at` to `at`, whose
        book changes replay to its whole books then (Ticker.book): a recording is one session."""
        return self.updates(code, None, at)

    def updates(self, code: str, start=None, end=None) -> Iterator[dict]:
        """Raw quote updates for one security, skipping unrelated batches without parsing them."""
        issue = self.issue(code)['issue_id']
        low, high = micros(self.trading_date, start), micros(self.trading_date, end)
        if self.format == 'parquet':
            columns = self._columns or _parquet.Columns.load(self.directory, issue_id=issue)
            yield from columns.updates(issue, low, high)
            return
        needle = json.dumps(str(code)).encode()  # Matches any JSON spacing; a false hit only costs a parse.
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
