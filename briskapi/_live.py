"""Live consumption of the pinned public demo, plus recording and automatic contribution."""
from __future__ import annotations

import json
import os
from pathlib import Path
import queue
import shutil
import subprocess
import sys
import tempfile
import threading
from typing import Callable, Iterator
import warnings

from briskapi import cli
from briskapi._recording import BriskError, NotFoundError, Recording, Table, micros, quote_view
from briskapi.timing import TimingStats

DECODER = cli.DECODER
_END = object()


def _codes(codes):
    return codes if isinstance(codes, str) else ','.join(map(str, codes))


def _decoder(web, cache, codes, speed, limit_frames, node):
    if bool(web) == bool(cache):
        raise ValueError('Select exactly one of web=True or cache=DIR')
    command = [node, str(DECODER), *(['--web'] if web else ['--cache', str(cache)]), '--speed', str(speed)]
    if codes:
        command += ['--codes', _codes(codes)]
    if limit_frames:
        command += ['--limit-frames', str(limit_frames)]
    return subprocess.Popen(command, stdout=subprocess.PIPE)


def _stop(process):
    if process.poll() is None:
        process.kill()
    process.stdout.close()
    process.wait()


def stream(web=False, cache=None, codes=None, speed=1, limit_frames=None, node='node') -> Iterator[dict]:
    """Yield raw decoded batches as each frame replays (bootstrap, quote deltas, end)."""
    cli.check_node(node)
    process = _decoder(web, cache, codes, speed, limit_frames, node)
    try:
        for line in process.stdout:
            yield json.loads(line)
    finally:
        _stop(process)
    if process.returncode:
        raise BriskError(f'Decoder exited with {process.returncode}')


def _consent(contribute, limit_frames):
    """(saved alias/license or None, whether to upload) for a session under saved consent."""
    choice = cli.load_consent()
    declared = bool(choice and choice['enabled']) and limit_frames is None
    if contribute and not declared:
        raise BriskError('contribute=True needs briskapi.consent(accept=True) and a complete replay')
    if contribute is None and choice is None:
        warnings.warn('Contribution is undecided, so this session stays local. Decide once with '
                      '`brisk consent --accept|--revoke` or briskapi.consent(accept=True|revoke=True); see https://github.com/honvl/BRiSKapi/blob/main/PRIVACY.md',
                      stacklevel=3)
    upload = declared and contribute is not False and os.environ.get('BRISK_CONTRIBUTE') != '0'
    return (choice if declared else None), upload


class Feed:
    """Live auction state, updated in a background thread as the decoder emits frames.

        with briskapi.connect(web=True, codes=["7203", "6758"]) as feed:
            feed.on_quote(lambda q: print(q["code"], q["indicative_price"]))
            for q in feed.quotes("7203"):        # blocks for each update
                ...
            briskapi.Ticker("7203").quote()         # current state, any time

    Consumers apply backpressure: a slow `quotes()` iterator or callback slows the
    feed instead of dropping updates. A complete session is contributed to the
    archive when saved consent allows (see PRIVACY.md); pass contribute=False to
    keep it local.
    """

    manifest = None

    def __init__(self, web=False, cache=None, codes=None, speed=1, limit_frames=None, contribute=None,
                 history=False, node='node', *, command=None, env=None, timing=None):
        # `command` runs another decoder host (SBI live). Its market data is never
        # contributed; with `timing` set, a timing-only summary may be.
        cli.check_node(command[0] if command else node)
        if command is None:
            choice, upload = _consent(contribute, limit_frames)
            self._choice, self._timing = (choice if upload else None), None
        else:
            choice, upload = _consent(contribute, None) if timing else (None, False)
            self._choice = None
            self._timing = (TimingStats(timing), choice) if upload else None
        self.timing_contribution = None
        self._history = {} if history else None
        self._quotes: dict[int, dict] = {}
        self._bootstrap = None
        self._listeners: list = []
        self._changed = threading.Condition()
        self._closing = False
        self.status, self.error, self.seq, self.contribution = 'starting', None, -1, None
        self._tmp = tempfile.TemporaryDirectory() if self._choice else None
        self._process = (subprocess.Popen(command, stdout=subprocess.PIPE, env=env) if command
                         else _decoder(web, cache, codes, speed, limit_frames, node))
        self._thread = threading.Thread(target=self._run, name='brisk-feed', daemon=True)
        self._thread.start()

    def __repr__(self):
        return f'Feed(status={self.status!r}, seq={self.seq})'

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # Recording-compatible reads used by Ticker and Market.
    @property
    def bootstrap(self) -> dict:
        self.ready()
        return self._bootstrap

    @property
    def trading_date(self) -> str:
        return self.bootstrap['trading_date']

    @property
    def source(self) -> str:
        return self.bootstrap['source']

    @property
    def codes(self) -> list[str]:
        return [m['code'] for m in self.bootstrap['master']]

    def issue(self, code) -> dict:
        for entry in self.bootstrap['master']:
            if entry['code'] == str(code):
                return entry
        raise NotFoundError(f'{code} is not in this feed')

    def state(self, at=None) -> dict[int, dict]:
        """Current raw quote per issue. Live feeds hold only the present; record() for time travel."""
        if at is not None:
            raise ValueError('A live feed holds current state only; use a Recording for past times')
        self.ready()
        with self._changed:
            return dict(self._quotes)

    def updates(self, code, start=None, end=None) -> Iterator[dict]:
        """Raw updates received so far for one security (needs history=True)."""
        if self._history is None:
            raise BriskError('Pass history=True to briskapi.connect() to retain updates')
        issue = self.issue(code)['issue_id']
        low, high = micros(self.trading_date, start), micros(self.trading_date, end)
        with self._changed:
            retained = list(self._history.get(issue, ()))
        return (q for q in retained if (low is None or q['source_time_us'] >= low)
                and (high is None or q['source_time_us'] <= high))

    # Live consumption.
    def ready(self, timeout=60) -> Feed:
        """Wait until the bootstrap (master and initial quotes) has been applied."""
        with self._changed:
            if not self._changed.wait_for(lambda: self._bootstrap is not None or self.status in {'failed', 'closed', 'completed'}, timeout):
                raise TimeoutError('Feed did not start in time')
        if self._bootstrap is None:
            raise BriskError(f'Feed {self.status} before bootstrap: {self.error}')
        return self

    def on_quote(self, callback: Callable[[dict], object], codes=None, raw=False) -> Callable[[], None]:
        """Call `callback(quote)` on the feed thread for each update; returns an unsubscribe function.

        The callback first receives each selected security's current quote.
        An exception in a callback stops the feed and is re-raised by wait().
        """
        return self._listen(callback, codes, raw)[1]

    def quotes(self, codes=None, raw=False, timeout=None) -> Iterator[dict]:
        """Iterate updates as they arrive, starting with each selected security's current quote."""
        inbox: queue.Queue = queue.Queue(maxsize=10_000)
        active = [True]

        def deliver(item):
            # Backpressure, but never wedge the feed on an abandoned iterator.
            while active[0]:
                try:
                    return inbox.put(item, timeout=0.2)
                except queue.Full:
                    pass

        current, remove = self._listen(deliver, codes, raw, end=lambda: deliver(_END), replay=False)
        try:
            yield from current
            while (item := inbox.get(timeout=timeout)) is not _END:
                yield item
        except queue.Empty:
            raise TimeoutError('No update within timeout') from None
        finally:
            active[0] = False
            remove()
        self._raise()

    def snapshot(self, raw=False) -> Table:
        state, date = self.state(), self.trading_date
        return Table(state[i] if raw else quote_view(state[i], date) for i in sorted(state))

    def wait(self, timeout=None) -> Feed:
        """Block until the replay ends (and any contribution finishes); raise if it failed."""
        self._thread.join(timeout)
        if self._thread.is_alive():
            raise TimeoutError('Feed still running')
        self._raise()
        return self

    def close(self):
        """Stop the decoder. A session closed before its end is never contributed."""
        self._closing = True
        if self._process.poll() is None:
            self._process.kill()
        self._thread.join()

    def _raise(self):
        if self.status == 'failed':
            raise BriskError(f'Feed failed: {self.error}') from self.error

    def _listen(self, deliver, codes, raw, end=None, replay=True):
        self.ready()
        wanted = None if codes is None else {self.issue(c)['issue_id'] for c in ([codes] if isinstance(codes, str) else codes)}
        date = self._bootstrap['trading_date']
        listener = (wanted, deliver, (lambda q: q) if raw else (lambda q: quote_view(q, date)), end)
        with self._changed:
            # Snapshot and registration are atomic, so no update is missed or reordered.
            current = [listener[2](self._quotes[i]) for i in sorted(self._quotes) if wanted is None or i in wanted]
            if self.status == 'running':
                self._listeners.append(listener)
            elif end:
                end()
            if replay:
                for quote in current:
                    deliver(quote)

        def remove():
            with self._changed:
                if listener in self._listeners:
                    self._listeners.remove(listener)
        return current, remove

    def _run(self):
        recorder = None
        try:
            if self._tmp:
                recorder = open(Path(self._tmp.name) / 'events.jsonl', 'wb')
            ended = False
            for line in self._process.stdout:
                batch = json.loads(line)
                if batch.get('seq') != self.seq + 1 or (self.seq < 0) != (batch.get('type') == 'bootstrap'):
                    raise BriskError(f'Stream sequence error at {batch.get("seq")}')
                if recorder:
                    recorder.write(line)
                ended = batch['type'] == 'end'
                if self._timing:
                    self._timing[0].add(batch)
                self._apply(batch)
            if self._closing:
                raise BriskError('closed')
            if self._process.wait():
                raise BriskError(f'Decoder exited with {self._process.returncode}')
            if not ended:
                raise BriskError('Decoder closed before replay completion')
            with self._changed:
                self.status = 'completed'
                self._changed.notify_all()
            self._finish()
            if recorder:
                recorder.close()
                self._contribute()
        except Exception as error:  # noqa: BLE001 - every failure ends the feed visibly
            with self._changed:
                self.status, self.error = ('closed', None) if self._closing else ('failed', error)
                self._changed.notify_all()
            self._finish()
        finally:
            _stop(self._process)
            if self._timing and self.status in {'completed', 'closed'}:
                self._contribute_timing()
            if recorder:
                recorder.close()
            if self._tmp:
                self._tmp.cleanup()

    def _apply(self, batch):
        quotes = batch.get('quotes', ())
        with self._changed:
            if batch['type'] == 'bootstrap':
                self._bootstrap = {k: v for k, v in batch.items() if k != 'quotes'}
                self.status = 'running'
            self._quotes.update((q['issue_id'], q) for q in quotes)
            if self._history is not None:
                for q in quotes:
                    self._history.setdefault(q['issue_id'], []).append(q)
            self.seq = batch['seq']
            listeners = list(self._listeners)
            self._changed.notify_all()
        for wanted, deliver, view, _ in listeners:
            for q in quotes:
                if wanted is None or q['issue_id'] in wanted:
                    deliver(view(q))

    def _finish(self):
        with self._changed:
            listeners, self._listeners = self._listeners, []
        for *_, end in listeners:
            if end:
                end()

    def _contribute_timing(self):
        stats, choice = self._timing
        try:
            report = stats.report(choice['contributor'], choice['license'])
            if report:
                self.timing_contribution = cli.contribute_timing(report, cli.settings()['api_url'])
        except Exception as error:  # noqa: BLE001 - the session itself succeeded; report, do not fail
            self.timing_contribution = {'status': 'failed', 'error': str(error)}
            warnings.warn(f'Timing contribution failed: {error}')

    def _contribute(self):
        package = Path(self._tmp.name) / 'package'
        try:
            cli.package(Path(self._tmp.name) / 'events.jsonl', package, self._choice['contributor'], self._choice['license'])
            self.contribution = cli.contribute(package, cli.settings()['api_url'], verify=False, out=sys.stderr)
        except Exception as error:  # noqa: BLE001 - market data was delivered; report, do not fail
            self.contribution = {'status': 'failed', 'error': str(error)}
            warnings.warn(f'Automatic contribution failed: {error}')


def connect(web=False, cache=None, codes=None, speed=1, limit_frames=None, contribute=None, history=False,
            node='node', timeout=60) -> Feed:
    """Start a live feed and return once its bootstrap state is available."""
    feed = Feed(web, cache, codes, speed, limit_frames, contribute, history, node)
    try:
        return feed.ready(timeout)
    except BaseException:
        feed.close()
        raise


def record(output, web=False, cache=None, codes=None, speed=1, limit_frames=None, contribute=None,
           binary=None) -> Recording:
    """Record a demo replay into `output` and contribute it according to saved consent.

    contribute=None follows `briskapi.consent()` (and BRISK_CONTRIBUTE=0 opts out),
    False keeps the recording local, True requires saved consent. Partial replays
    (limit_frames) always stay local.
    """
    output = Path(output)
    choice, upload = _consent(contribute, limit_frames)
    with tempfile.TemporaryDirectory() as tmp:
        events = Path(tmp) / 'events.jsonl'
        cli.record_events(events, web, cache, codes and _codes(codes), limit_frames, speed, binary)
        if choice is None:
            output.mkdir(parents=True, exist_ok=True)
            if (output / 'events.jsonl').exists():
                raise BriskError(f'{output / "events.jsonl"} already exists')
            shutil.move(str(events), str(output / 'events.jsonl'))
            return Recording(output)
        cli.package(events, output, choice['contributor'], choice['license'])
    recording = Recording(output)
    if upload:
        recording.contribution = cli.contribute(output, cli.settings()['api_url'], verify=False)
    return recording
