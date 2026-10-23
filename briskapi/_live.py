"""Live consumption of the pinned public demo, plus recording and automatic upload to your private archive."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import queue
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from typing import Callable, Iterator
import warnings

from briskapi import _gateway, _parquet, _spool, cli, schema
from briskapi._recording import BriskError, NotFoundError, Recording, Table, micros, quote_view
from briskapi.timing import TimingStats

DECODER = cli.DECODER
_END = object()
# On Windows the decoder host runs at above-normal priority. Its quote workers are one thread each and a frame waits for the slowest, so a
# worker that another program's thread has taken a core from holds all of them up: on a desktop with 30-45% other CPU load (i5-12600K) the
# whole feed went from 24k to 47k quote updates a second (4 workers) and from 25k to 45k (8, the default there). The child's priority only.
HOST_PRIORITY = {'creationflags': subprocess.ABOVE_NORMAL_PRIORITY_CLASS} if sys.platform == 'win32' else {}


def _codes(codes):
    return codes if isinstance(codes, str) else ','.join(map(str, codes))


def _decoder(cache, codes, speed, limit_frames, runtime):
    """The demo's decoder: its files downloaded from BRiSK's demo site, or a copy in `cache`."""
    command = [cli.pick_runtime(runtime), str(DECODER), *(['--web'] if cache is None else ['--cache', str(cache)]), '--speed', str(speed)]
    if codes:
        command += ['--codes', _codes(codes)]
    if limit_frames:
        command += ['--limit-frames', str(limit_frames)]
    return subprocess.Popen(command, stdout=subprocess.PIPE, **HOST_PRIORITY)


def _stored(result):
    return bool(result['uploaded']) and result['uploaded'].get('status') == 'stored'


def _hms(us):
    """A source clock (microseconds after JST midnight) as HH:MM:SS."""
    return f'{us // 3_600_000_000:02d}:{us // 60_000_000 % 60:02d}:{us // 1_000_000 % 60:02d}'


def _saving_label(spool, start=None):
    """Which recording a spool is saving, for `brisk live`'s lines: its site (or source), its trading date and where its piece
    (the one last tried, or the one beginning at `start`) begins (a day is saved in pieces)."""
    header = spool.header or _parquet.bootstrap_row(spool.parts()[0])
    site = schema.site_of(header.get('input_transport')) or header['source']
    date = header['trading_date']
    start = spool.piece_start if start is None else start
    return f'{site} {date[:4]}-{date[4:6]}-{date[6:]}' + ('' if start is None else f' from {_hms(start)}')


def _made(result):
    """Whether a result is of a recording that was made (or that failed to be): not of a segment the stream sent no update to."""
    return any(result[key] is not None for key in ('name', 'path', 'error'))


def _stop(process):
    if process.poll() is None:
        process.kill()
    process.stdout.close()
    if process.stdin:
        try:
            process.stdin.close()
        except OSError:  # closing flushes buffered control messages; the host may have already exited
            pass
    process.wait()


def stream(cache=None, codes=None, speed=1, limit_frames=None, runtime=None) -> Iterator[dict]:
    """Yield the demo's raw decoded batches as each frame replays (bootstrap, quote deltas, end): its files
    downloaded from BRiSK's demo site, or a copy downloaded before (`cache`)."""
    cli.check_runtime(runtime)
    process = _decoder(cache, codes, speed, limit_frames, runtime)
    try:
        for line in process.stdout:
            yield json.loads(line)
    finally:
        _stop(process)
    if process.returncode:
        raise BriskError(f'Decoder exited with {process.returncode}')


def _notice(value, created, old=None):
    from briskapi import identity
    warnings.warn(cli.policy_changed(old) + 'Complete sessions upload to your private BRiSK archive by default: a free off-computer copy only '
                  f'your identity can reach (owner {cli.owner(value)}, kept in {" and ".join(identity.where())}). '
                  + ('Keep its recovery code: `brisk identity --recovery-code`. ' if created else '')
                  + 'Opt out with upload=False, briskapi.consent(revoke=True) or BRISK_UPLOAD=0; see '
                  'https://github.com/honvl/BRiSKapi/blob/main/PRIVACY.md', stacklevel=6)


def _consent(upload, limit_frames):
    """(whether to package for upload, whether to upload) for a session: complete sessions upload by default
    (said once per process) or as you chose with briskapi.consent, unless this run opts out."""
    choice = cli.consented(upload, ask=False, notify=_notice)
    allowed = bool(choice and choice['enabled']) and limit_frames is None
    if upload and not allowed:
        raise BriskError('upload=True needs uploads on (briskapi.consent(accept=True)) and a complete replay')
    return allowed, allowed and not cli.opted_out(upload)


# Threads parsing the host's lines on a free-threaded build (see Feed._lines).
PARSERS = min(4, os.cpu_count() or 1)
# The host ends every line; one without its end means the host exited before the pipe took all it wrote.
CUT_SHORT = 'The decoder\'s output stopped in the middle of a line: it exited before writing all of it'


class Feed:
    """Live auction state, updated in a background thread as the decoder emits frames.

        with briskapi.connect(site="demo", codes=["7203", "6758"]) as feed:
            feed.on_quote(lambda q: print(q["code"], q["indicative_price"]))
            for q in feed.quotes("7203"):        # blocks for each update
                ...
            briskapi.Ticker("7203").quote()         # current state, any time

    Consumers apply backpressure: a slow `quotes()` iterator or callback slows the
    feed instead of dropping updates. A complete session is uploaded to your private
    archive automatically, by default or as you chose with briskapi.consent (see
    PRIVACY.md); pass upload=False to keep it local. gateway=True also serves the feed to
    briskapi.Live on this computer (see briskapi._gateway); `feed.gateway` is then that
    server, or None (with a warning) if another feed already serves the same dataset.
    """

    manifest = None
    gateway = None
    _lane_split = True   # the fast lane's lines go to the gateway on a thread of their own (_lane_loop); the benchmark turns it off to compare

    def __init__(self, cache=None, codes=None, speed=1, limit_frames=None, upload=None,
                 history=False, runtime=None, *, command=None, env=None, timing=None, upload_market_data=None, output=None,
                 on_day=None, gateway=False, relogin=None):
        # A broker session uploads its timing summary and its market recording; upload_market_data=False keeps
        # the market recording local.
        if upload_market_data is not None and (command is None or timing not in schema.BROKER_TRANSPORTS):
            raise BriskError('upload_market_data applies to a broker live session (a site other than the demo)')
        if upload is False and upload_market_data:
            raise BriskError('upload=False keeps the whole session local; drop upload_market_data=True')
        cli.check_runtime(command[0] if command else runtime)
        if command is None:
            self._package, self._timing = _consent(upload, limit_frames)[1], None
        else:
            wanted = upload if upload is not None else (True if upload_market_data else None)
            uploads = _consent(wanted, None)[1] if timing else False
            self._package = uploads and upload_market_data is not False
            self._timing = timing if uploads else None  # the source whose timing summaries are uploaded
        # Each day (bootstrap ... end) is packaged, kept in `output` as SITE_YYYYMMDD_FROM-TO and uploaded, on a
        # worker thread while the feed goes on; a broker's in pieces as it runs (_spool.Spool.ship).
        self._output = Path(output) if output else None
        if self._output and limit_frames is not None:
            raise BriskError('A partial replay (limit_frames) is not a recording to keep; drop output')
        self._stats, self._on_day = None, on_day
        self.recordings: list[dict] = []  # one per recording: trading_date, name, path, uploaded, timing_uploaded, repaired
        self.timing_uploaded = None
        self._history = {} if history else None
        self._trade_history = []
        self._quotes: dict[int, dict] = {}
        self._bootstrap = None
        self._last_source_time_us = None
        self._listeners: list = []
        self._changed = threading.Condition()
        self._closing = False
        # relogin(closed): signs in again when the host says its session expired while it waited (see _expired); `closed`
        # is set when the feed is closed.
        self._relogin, self._closed = relogin, threading.Event()
        # With the GIL off, the fast lane's lines go to the gateway on a thread of their own (_lane_loop) instead of waiting in the
        # feed thread behind each batch of the recording's before them. Each thread counts the host's lines, and each says how far it
        # has read, so the few lines that must keep their order between the two (a session's lanes line, its bootstrap and its end)
        # do: see _lane_loop and _run.
        self._lane_thread, self._lane_alive, self._lane_failure = False, False, None
        self._lane_pos = self._run_pos = 0
        self.status, self.error, self.seq, self.uploaded = 'starting', None, -1, None
        # A broker host with no session to join says so (date, session_status) and waits for the next trading day's.
        self.waiting: dict | None = None
        self._day_over = False   # the running day has ended: nothing records until the next one's bootstrap
        self._day_updates, self._day_began = 0, None   # this day's quote updates since its bootstrap, and when it began
        # Each segment's batches go to disk as they arrive (briskapi._spool): to package it, to keep what a stop
        # or a crash cuts short, and for a gateway's replays.
        self._spooling, self._broker = bool(self._package or self._output or gateway), command is not None
        self._worker = ThreadPoolExecutor(1, thread_name_prefix='brisk-day')
        threading.Thread(target=_parquet.warm, name='brisk-warm', daemon=True).start()   # before the day's bootstrap needs it
        if self._spooling:
            self._worker.submit(self._recover)
        # A broker host serving a gateway has a fast lane (see brisk.cjs): it is told on stdin which symbols for.
        lanes = bool(command and gateway)
        self._lanes_lock = threading.Lock()
        self._process = (subprocess.Popen(command, stdout=subprocess.PIPE, stdin=subprocess.PIPE,
                                          env={**(env or os.environ), 'BRISK_CONTROL': '1',
                                               **({'BRISK_LANES': '1'} if lanes else {})}, **HOST_PRIORITY) if command
                         else _decoder(cache, codes, speed, limit_frames, runtime))
        if gateway:
            try:
                # A broker's stream is XTKS.BRISK, as its recordings are in the cloud API; the demo's XTKS.BRISKDEMO.
                self.gateway = _gateway.Gateway('XTKS.BRISK' if command else 'XTKS.BRISKDEMO',
                                                on_fast=self._fast_codes if lanes else None)
            except BriskError as error:
                warnings.warn(str(error))
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
    def clock_range(self) -> tuple[int, int]:
        """First and most recently received batch clocks."""
        self.ready()
        with self._changed:
            return self._bootstrap['source_time_us'], self._last_source_time_us

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

    def trades(self, code=None, start=None, end=None) -> list[dict]:
        """Raw trades received so far, of one security or all, in source-time order (needs history=True)."""
        if self._history is None:
            raise BriskError('Pass history=True to briskapi.connect() to retain trades')
        issue = None if code is None else self.issue(code)['issue_id']
        low, high = micros(self.trading_date, start), micros(self.trading_date, end)
        with self._changed:
            retained = list(self._trade_history)
        return sorted((t for t in retained if (issue is None or t['issue_id'] == issue)
                       and (low is None or t['time_us'] >= low) and (high is None or t['time_us'] <= high)),
                      key=lambda t: t['time_us'])

    def frames(self):
        raise BriskError('A live feed keeps no frames; record the session or load a recording')

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
        """Wait until the bootstrap (master and initial quotes) has been applied. A broker feed started with no
        session to join (after the close, on a holiday; status 'waiting') waits for the next trading day's,
        however long `timeout` is."""
        with self._changed:
            while not self._changed.wait_for(lambda: self._bootstrap is not None or self.status in {'failed', 'closed', 'completed'}, timeout):
                if self.status != 'waiting':
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
        """Block until the replay ends (and any upload finishes); raise if it failed."""
        self._thread.join(timeout)
        if self._thread.is_alive():
            raise TimeoutError('Feed still running')
        self._raise()
        return self

    def close(self, timeout=30):
        """Stop the feed. A broker session first ends its day where it is, as the day's end would (its host writes
        the day's end), so the day is kept and uploaded like any other; a host that has not ended within `timeout`
        seconds is killed and its recording repaired and finalized as the session requested. A demo replay
        stops at once (a partial replay is never uploaded). Returns once the days are kept and uploaded."""
        self._closing = True
        self._closed.set()
        if self._process.poll() is None and self._broker:
            # A control message works on Windows too, where terminate() cannot run the host's SIGTERM handler.
            with self._lanes_lock:
                try:
                    self._process.stdin.write(b'{"stop":true}\n')
                    self._process.stdin.flush()
                except (OSError, ValueError):  # the host has already ended
                    pass
            try:
                self._process.wait(timeout)
            except subprocess.TimeoutExpired:
                pass
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

    def _lines(self):
        """The host's batches with their lines, in order. Parsing them is most of the feed's work, so with the GIL
        off (free-threaded Python) one thread reads the pipe a chunk at a time while PARSERS threads parse."""
        stdout = self._process.stdout
        if sys._is_gil_enabled():
            for line in stdout:
                if not line.endswith(b'\n'):
                    raise BriskError(CUT_SHORT)
                yield json.loads(line), line
            return
        chunks: queue.Queue = queue.Queue(maxsize=64)   # at most 64 MB read ahead
        stop = threading.Event()
        tap = queue.Queue() if self.gateway and self._lane_split else None   # the same chunks, for the lane thread

        def put(item):   # never stuck on a full queue once the feed has stopped reading
            if tap is not None:
                tap.put(item)
            while not stop.is_set():
                try:
                    return chunks.put(item, timeout=0.1)
                except queue.Full:
                    pass

        def parse(lines):
            return [(json.loads(line), line) for line in lines]

        def read(pool):
            try:
                tail = b''
                # read1 returns what the pipe holds: a line when the stream is slow, a megabyte in a burst.
                while not stop.is_set() and (data := stdout.read1(2**20)):
                    data = tail + data
                    cut = data.rfind(b'\n') + 1
                    tail = data[cut:]
                    if cut:
                        put(pool.submit(parse, data[:cut].splitlines(keepends=True)))
                if tail:
                    raise BriskError(CUT_SHORT)
            except BaseException as error:  # noqa: BLE001 - the feed thread raises it
                put(error)
            finally:
                put(None)

        with ThreadPoolExecutor(PARSERS, thread_name_prefix='brisk-parse') as pool:
            if tap is not None:
                self._lane_thread = self._lane_alive = True
                threading.Thread(target=self._lane_loop, args=(tap, stop), name='brisk-lane', daemon=True).start()
            threading.Thread(target=read, args=(pool,), name='brisk-read', daemon=True).start()
            try:
                while (chunk := chunks.get()) is not None:
                    if isinstance(chunk, BaseException):
                        raise chunk
                    yield from chunk.result()
            finally:
                stop.set()

    def _lane_loop(self, tap, stop):
        """Pass the fast lane's lines (and a session's lanes line) to the gateway as they are read, whatever the feed thread is
        busy with: the recording's batches of a burst used to be passed first, and a quote on the lane was as old as that backlog.
        Three kinds of line keep their place against the feed thread's: a lanes line comes after every line before it has been
        passed on (sessions are moved between lanes as the earlier ones ended), a bootstrap after the feed thread published it
        (the lane's quotes build on its state), and the feed thread waits at a bootstrap and an end for the quotes before them."""
        index = 0

        def until(done):
            while not (done() or stop.is_set()):
                time.sleep(0.0005)
        try:
            while not stop.is_set():
                try:
                    chunk = tap.get(timeout=0.1)
                except queue.Empty:
                    continue
                if chunk is None or isinstance(chunk, BaseException):
                    return
                for batch, line in chunk.result():
                    kind = batch.get('type')
                    if kind == 'lanes':
                        self._lane_pos = index
                        until(lambda at=index: self._run_pos >= at)
                        self.gateway.lane(batch, line)
                    elif batch.get('lane') == 'fast':
                        self.gateway.lane(batch, line)
                    elif kind == 'bootstrap':
                        self._lane_pos = index
                        until(lambda at=index: self._run_pos > at)
                    index += 1
                    self._lane_pos = index
        except BaseException as error:  # noqa: BLE001 - the feed thread raises it
            self._lane_failure = error
        finally:
            self._lane_alive = False

    def _wait_lane(self, at):
        """Until the lane thread has passed on every line before line `at`."""
        while self._lane_pos < at and self._lane_alive:
            time.sleep(0.0005)
        if self._lane_failure:
            raise self._lane_failure

    def _run(self):
        spool, days, ended, index = None, 0, False, 0
        try:
            for batch, line in self._lines():
                at, index = index, index + 1
                if self._lane_thread:   # the lane's lines are the lane thread's (_lane_loop)
                    if self._lane_failure:
                        raise self._lane_failure
                    if batch.get('lane') == 'fast' or batch.get('type') == 'lanes':
                        if batch.get('type') == 'lanes':
                            self._run_pos = at + 1   # every line before it has been taken
                        continue
                    if batch.get('type') in ('bootstrap', 'end'):
                        self._wait_lane(at)   # the lane's quotes before it first
                elif batch.get('lane') == 'fast' or batch.get('type') == 'lanes':  # out of band: the gateway's
                    if self.gateway:
                        self.gateway.lane(batch, line)
                    continue
                if batch.get('type') == 'expired':   # out of band: the session ended while the host waited for the next day
                    self._expired()
                    continue
                if batch.get('type') == 'waiting':  # out of band, not a day's record: kept nowhere
                    with self._changed:
                        self.waiting = {key: batch.get(key) for key in ('date', 'session_status')}
                        if self._bootstrap is None:
                            self.status = 'waiting'
                        self._changed.notify_all()
                    continue
                # Each day runs bootstrap ... end, numbered from its own bootstrap; a new day follows an end.
                first = batch.get('type') == 'bootstrap'
                if batch.get('seq') != (0 if first else self.seq + 1) or first != (self.seq < 0 or ended):
                    raise BriskError(f'Stream sequence error at {batch.get("seq")}')
                if first:
                    days += 1
                    # A broker's day that is kept or uploaded ships a piece at a time as it goes (Spool.ship).
                    ships = self._broker and bool(self._package or self._output)
                    spool = _spool.Spool(intent=self._intent(), on_part=self._part_written if ships else None) \
                        if self._spooling else None
                    self._stats = TimingStats(self._timing) if self._timing else None
                if spool:
                    spool.add(batch, line)
                ended = batch['type'] == 'end'
                if self._stats:
                    self._stats.add(batch)
                self._apply(batch, new_day=first and days > 1)
                if self.gateway:
                    self.gateway.publish(batch, line, spool)
                if first:
                    self._run_pos = at + 1   # published: the lane's quotes of this session may follow
                if ended:
                    self._worker.submit(self._finish_day, spool, self._stats)
                    spool, self._stats = None, None
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
        except Exception as error:  # noqa: BLE001 - every failure ends the feed visibly
            with self._changed:
                self.status, self.error = ('closed', None) if self._closing else ('failed', error)
                self._changed.notify_all()
            self._finish()
        finally:
            _stop(self._process)
            if spool:
                self._worker.submit(self._keep, spool)   # never uploaded
            if self._stats and self.status in {'completed', 'closed'}:
                self.timing_uploaded = self._upload_timing(self._stats)
            self._worker.shutdown(wait=True)  # finished recordings are still kept and uploaded

    def _expired(self):
        """The host's session expired while it waited for the next trading day (the brokers end their sessions at 05:00):
        sign in again where that can be done without a person (`relogin`), on a thread of its own: the host reads the
        cookies it saves, and goes on."""
        if self._relogin is None or self._closing:
            return

        def run():
            try:
                self._relogin(self._closed)
            except Exception as error:  # noqa: BLE001 - the host goes on waiting for cookies whatever happens here
                print(f'[brisk] signing in again failed: {error}', file=sys.stderr, flush=True)
        threading.Thread(target=run, name='brisk-login', daemon=True).start()

    def _apply(self, batch, new_day=False):
        quotes = batch.get('quotes', ())
        with self._changed:
            if new_day:  # the next trading day's state replaces the last one's
                self._quotes.clear()
                self._trade_history.clear()
                if self._history is not None:
                    self._history.clear()
            if batch['type'] == 'bootstrap':
                self._bootstrap = {k: v for k, v in batch.items() if k != 'quotes'}
                self.status, self.waiting = 'running', None
                self._day_updates, self._day_began, self._day_over = 0, time.monotonic(), False
            else:
                self._day_updates += len(quotes)
                self._day_over = batch['type'] == 'end'
            self._quotes.update((q['issue_id'], q) for q in quotes)
            if self._history is not None:
                for q in quotes:
                    self._history.setdefault(q['issue_id'], []).append(q)
                self._trade_history.extend(batch.get('trades', ()))
            self.seq = batch['seq']
            self._last_source_time_us = batch['source_time_us']
            listeners = list(self._listeners)
            self._changed.notify_all()
        for wanted, deliver, view, _ in listeners:
            for q in quotes:
                if wanted is None or q['issue_id'] in wanted:
                    deliver(view(q))

    def _fast_codes(self, codes):
        """Tell the host which symbols the fast lane reads (the gateway's, as Live sessions subscribe)."""
        with self._lanes_lock:
            try:
                self._process.stdin.write(json.dumps({'fast': sorted(codes)}).encode() + b'\n')
                self._process.stdin.flush()
            except (OSError, ValueError):   # the host has ended
                pass

    def _progress(self):
        """The running day: its trading date, seconds since its bootstrap, its quote updates and its latest source
        clock (None before a day has begun, and between days)."""
        with self._changed:
            if self._bootstrap is None or self._day_over:
                return None
            return {'trading_date': self._bootstrap['trading_date'], 'seconds': time.monotonic() - self._day_began,
                    'updates': self._day_updates, 'source_time_us': self._last_source_time_us}

    def _finish(self):
        with self._changed:
            listeners, self._listeners = self._listeners, []
        for *_, end in listeners:
            if end:
                end()
        if self.gateway:
            self.gateway.close(self.status, self.error)

    def _intent(self):
        """This session's choices for its days, kept with each spool for whoever finishes it."""
        return {'upload': bool(self._package), 'output': str(self._output) if self._output else None}

    def _recover(self):
        """Finish the segments that processes which stopped unexpectedly left in the spool, as their sessions would
        have: each repaired with the end it never got, kept in its session's output and uploaded if that session
        uploaded and uploads are still on here."""
        for spool in _spool.leftovers():
            intent = spool.intent
            if intent is None:   # a spool from before sessions recorded their choices: keep it, upload nothing
                intent = {'upload': False, 'output': str(_spool.kept_dir())}
            upload = bool(intent.get('upload')) and _consent(None, None)[1]
            output = Path(intent['output']) if intent.get('output') else None
            if not (upload or output):   # its session kept nothing (a gateway's spool only)
                spool.remove()
                continue
            for result in self._finish_segment(spool, None, upload=upload, output=output, repair=True):
                if not _made(result):   # no update had come: there was nothing to repair
                    continue
                where = ('uploaded' if _stored(result) else 'not uploaded') + (f', kept in {result["path"]}' if result['path'] else '')
                warnings.warn(f'A feed that stopped unexpectedly left {result["name"] or spool.path} behind: '
                              + ('repaired with its end and ' if result['repaired'] else '') + where)

    def _keep(self, spool):
        """Finish a broker segment cut short (its host did not end it: a crash, a kill, a host that did not stop in
        time) like a day, with the end its host never wrote. A demo's is dropped: a partial replay is never uploaded."""
        if not (self._broker and (self._package or self._output)):
            spool.remove()
            return
        results = self._finish_segment(spool, None, upload=self._package, output=self._output, repair=True)
        for result in results:
            self._report(result)
        last = results[-1]
        if not _made(last):   # no update had come: there was nothing to repair
            return
        warnings.warn(f'The day ended early without its end: {last["name"] or spool.path} was repaired with one'
                      + (' and uploaded' if _stored(last) else ''))

    def _finish_day(self, spool, stats):
        """Package what is left of a finished day, keep it in `output` and upload it and its timing summary, as
        chosen."""
        for result in self._finish_segment(spool, stats, upload=self._package, output=self._output):
            self._report(result)

    def _part_written(self, spool):
        """A part of a broker's day is on disk (on the spool's thread): see whether a piece is ready to ship."""
        try:
            self._worker.submit(self._ship_ready, spool)
        except RuntimeError:   # the feed has ended: its days are finished
            pass

    def _ship_ready(self, spool):
        """Ship the pieces of a running day that are ready (Spool.ship), reporting each like a day."""
        while not spool.removed:
            shipped = spool.shipped
            result = self._ship(spool, final=False, upload=self._package, output=self._output)
            if result is None:
                return
            self._report(result)
            if spool.shipped == shipped:
                return

    def _finish_segment(self, spool, stats, *, upload, output, repair=False):
        """Ship what is left of a segment, in pieces (Spool.ship; the last repaired with its end if asked), keep
        them in `output`, upload them and `stats`' timing summary; the spool is removed after, unless a piece of it
        could be neither saved nor kept: then it stays where it is, for the next feed to start to finish
        (_recover), and a warning says so. Returns each piece's result (trading_date, name, path, uploaded,
        timing_uploaded, repaired), the timing summary's in the last."""
        results = []
        try:
            while spool and (upload or output):
                shipped = spool.shipped
                result = self._ship(spool, final=True, upload=upload, output=output, repair=repair)
                if result is None:
                    break
                results.append(result)
                if spool.shipped == shipped:
                    break
            results = results or [self._result()]
            if stats:
                results[-1]['timing_uploaded'] = self._upload_timing(stats)
        finally:
            if spool:
                lost = [result for result in results if result['error'] and not result['path']]
                if lost:
                    warnings.warn(f'Nothing of {lost[0]["name"] or "the recording"} could be saved; its spool is left in '
                                  f'{spool.path}, which the next feed to start finishes (delete it to give it up)')
                else:
                    spool.remove()
        return results

    def _result(self):
        return {'trading_date': self._bootstrap and self._bootstrap['trading_date'], 'name': None, 'path': None,
                'uploaded': None, 'timing_uploaded': None, 'repaired': False, 'error': None}

    def _ship(self, spool, *, final, upload, output, repair=False):
        """Package the spool's next piece (Spool.ship), keep it in `output` and upload it, as chosen. Returns its
        result, or None when no piece is ready."""
        result, package, step = self._result(), spool.path / 'package', 'Packaging'
        save = cli.saving(lambda: _saving_label(spool))   # says nothing until a piece is being packaged
        shutil.rmtree(package, ignore_errors=True)
        try:
            left = len(spool.skipped)
            shipped = spool.ship(package, final=final, repair=repair, progress=save.packaging)
            for first, last in spool.skipped[left:]:   # a piece the stream sent no update to: there is nothing to save
                cli.saving(lambda first=first: _saving_label(spool, first)).empty(_hms(last))
            if shipped is None:
                return None
            m, result['repaired'] = shipped
            if m:
                transport = _parquet.bootstrap_row(package).get('input_transport')
                result['name'] = schema.local_name(m['summary'], transport)
                result['trading_date'] = m['summary']['trading_date']
            if output:
                target = output / result['name']
                if target.exists():
                    warnings.warn(f'{target} already exists; this recording was not kept')
                else:
                    output.mkdir(parents=True, exist_ok=True)
                    shutil.copytree(package, target)
                    result['path'] = str(target)
                    save.saved(target)
            if upload:
                step = 'Automatic upload'
                result['uploaded'] = cli.upload(package, cli.settings()['api_url'], verify=False, out=sys.stderr,
                                                progress=save.uploading)
                save.uploaded()
        except Exception as error:  # noqa: BLE001 - market data was delivered; report, do not fail
            save.failed(step, error)
            # What failed, the packaging or the upload: a recording not uploaded (uploads off) never says it was.
            result['error'] = f'{step} failed: {error}'
            if upload:
                result['uploaded'] = {'status': 'failed', 'error': str(error)}
            kept = None if result['path'] else self._rescue(spool, package, result['name'], save, retry=bool(upload))
            result['path'] = result['path'] or (kept and str(kept))
            warnings.warn(f'{step} failed: {error}' +
                          (f'; the recording is kept in {kept}' + (f' (`brisk upload {kept}` retries)' if
                                                                   (kept / 'manifest.json').exists() else '')
                           if kept else ''))
        finally:
            shutil.rmtree(package, ignore_errors=True)
        return result

    def _report(self, result):
        self.uploaded, self.timing_uploaded = result['uploaded'], result['timing_uploaded']
        self.recordings.append(result)
        if self._on_day:
            try:
                self._on_day(result)
            except Exception as error:  # noqa: BLE001 - a reporting callback must not stop the recording
                warnings.warn(f'on_day callback failed: {error}')

    def _rescue(self, spool, package, name, save, retry=False):
        """Keep a piece whose packaging or upload failed in briskapi._spool.kept_dir(): its package if it has one
        (an upload can be retried), else its tables (Spool.keep); None if even that fails. `save` tells how it goes."""
        try:
            save.keeping(_spool.kept_dir())
            if name and (package / 'manifest.json').exists():
                target = _spool.kept_dir() / name
                if not target.exists():
                    shutil.copytree(package, target)
                    save.saved(target, f'`brisk upload {target}` retries the upload' if retry else None)
                    return target
            kept = spool.keep(_spool.kept_dir(), progress=save.packaging)
            if kept is None:
                save.failed('Keeping the recording', 'there is nothing of it to keep')
            else:
                save.saved(kept)
            return kept
        except Exception as error:  # noqa: BLE001 - reported with the upload's failure
            save.failed('Keeping the recording', error)
            warnings.warn(f'Could not keep the day in {_spool.kept_dir()}: {error}')
            return None

    def _upload_timing(self, stats):
        try:
            report = stats.report()
            return cli.upload_timing(report, cli.settings()['api_url']) if report else None
        except Exception as error:  # noqa: BLE001 - the session itself succeeded; report, do not fail
            warnings.warn(f'Timing upload failed: {error}')
            return {'status': 'failed', 'error': str(error)}


def connect(cache=None, codes=None, speed=1, limit_frames=None, upload=None, history=False,
            runtime=None, timeout=60, output=None, on_day=None, gateway=False) -> Feed:
    """briskapi.connect(site="demo"): start the demo's feed and return once its bootstrap state is available. `output` keeps the finished
    recording there (as SOURCE_YYYYMMDD_FROM-TO); `on_day(result)` is called once it is kept and uploaded.
    gateway=True also serves it to briskapi.Live on this computer."""
    feed = Feed(cache, codes, speed, limit_frames, upload, history, runtime, output=output, on_day=on_day,
                gateway=gateway)
    try:
        return feed.ready(timeout)
    except BaseException:
        feed.close()
        raise


def record(output, cache=None, codes=None, speed=1, limit_frames=None, upload=None,
           binary=None) -> Recording:
    """Record a demo replay into the directory `output` (Parquet tables, see briskapi._parquet) and
    upload it to your private archive according to saved consent. The demo's files are downloaded from
    BRiSK's demo site, or replayed from a copy downloaded before (`cache`).

    upload=None uploads by default or as you chose with `briskapi.consent()` (BRISK_UPLOAD=0 opts
    out), False keeps the recording local, True requires uploads on. Partial replays (limit_frames)
    always stay local.
    """
    output = Path(output)
    packaged, upload = _consent(upload, limit_frames)
    with tempfile.TemporaryDirectory() as tmp:
        events = Path(tmp) / 'events.jsonl'
        cli.record_events(events, cache, codes and _codes(codes), limit_frames, speed, binary)
        if not packaged:
            if (output / _parquet.DEPTHS).exists():
                raise BriskError(f'A recording already exists in {output}')
            cli.save_recording(events, output)
            return Recording(output)
        cli.package(events, output)
    recording = Recording(output)
    if upload:
        recording.uploaded = cli.upload(output, cli.settings()['api_url'], verify=False)
    return recording
