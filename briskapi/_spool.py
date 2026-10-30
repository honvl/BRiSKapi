"""A recording kept on disk while it is made, so that an interruption doesn't lose it.

A feed writes each segment (a bootstrap to its end: a trading day, or the part of one before a lost
connection) to a spool folder as its batches arrive, in the recording's own Parquet tables, a part at a time:

    spool/segment-ID/owner.json                  the process writing it
    spool/segment-ID/data/equity.parquet         the segment's master, written with its bootstrap
    spool/segment-ID/parts/000001/data/...       the depths, trades and batch tables of each closed part
    spool/segment-ID/shipped.json                how many parts are already shipped as pieces (see ship())

A part closes every PART_SECONDS, or sooner after PART_BYTES of decoder output, and when the segment ends or
the feed stops, so a crash loses at most the part still open. A segment takes about the disk of its finished
recording (its decoded JSON would take some twenty times more). package() turns the parts into the canonical
recording: the same tables as from the segment's batches at once (brisk-parquet writes them). A whole-market day is far more than one
recording is comfortable with, so a broker feed ships it as it goes, the morning's session as soon as it is over
and the afternoon's at the day's end (ship()), each a recording of its own; the afternoon's starts with a
bootstrap of the book as the morning's left it. A feed that crashed leaves its folder behind, with its
session's choices (upload, output) in owner.json; the next feed ships what is left as that session would have,
repairing the end it never got (leftovers(), brisk-parquet).
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import io
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
import threading
import time
from typing import NamedTuple
import warnings

from briskapi import _native, _parquet
from briskapi.schema import SKIPPED, canonical_batches, local_name

PART_SECONDS = 30
PART_BYTES = 64 * 2**20
# A broker's day ships in two pieces: the morning's, at the first part to close after noon (JST: the lunch break),
# and the afternoon's at the day's end. A completed part reaching MAX_PIECE_ROWS starts an extra piece:
# 80 million leaves room for a tester's 63-million-update morning and 41-million-update afternoon, below
# the archive's 100-million-depth-row limit (including a cut's opening book and the last complete part).
LUNCH_US = 12 * 3600 * 10**6
MAX_PIECE_ROWS = 80_000_000
# A piece that could not be packaged (the program did not start, a full disk) is tried again from the same parts, but
# not at every part written: after RETRY_SECONDS, MAX_ATTEMPTS times in the day, and once more when the day ends.
RETRY_SECONDS = 300
MAX_ATTEMPTS = 3
DEMO_SOURCES = ('historical_mock', 'synthetic_test')   # the same for everyone: nothing to keep


class _Cut(NamedTuple):
    """Where the shipped pieces left the day: a file with each issue's last quote (a depths table) and the last batch's clock."""
    state: object
    clock: int


def data_dir() -> Path:
    """Where briskapi keeps data on this computer (XDG_DATA_HOME, else ~/.local/share)."""
    return Path(os.environ.get('XDG_DATA_HOME') or Path.home() / '.local' / 'share') / 'brisk'


def spool_root() -> Path:
    return data_dir() / 'spool'


def kept_dir() -> Path:
    """Where a stopped or recovered broker segment is kept when its feed has no `output`."""
    return data_dir() / 'recordings'


def _fsync(path):
    # Windows' _commit (os.fsync) needs a handle opened for writing.
    fd = os.open(path, os.O_RDWR)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class Spool:
    """One segment's spool. The feed calls add() with each batch and its line, in order, from one thread."""

    def __init__(self, root=None, intent=None, on_part=None):
        root = Path(root or spool_root())
        root.mkdir(parents=True, exist_ok=True)
        self.path = Path(tempfile.mkdtemp(prefix='segment-', dir=root))
        # What the session wanted for its days, for whoever finishes this spool if the session cannot.
        self.intent = intent
        (self.path / 'owner.json').write_text(json.dumps({'pid': os.getpid(), 'started': time.time(), 'intent': intent}))
        self.header = None        # the bootstrap without quotes and trades
        self._midnight = None     # its day's start, UTC nanoseconds
        self.master = None
        self.ended = False
        self.count = 0            # lines so far
        self._lock = threading.Lock()
        self._lines, self._size, self._opened = [], 0, time.monotonic()
        self._pending = []        # [(first line, lines)] handed to the writer, not yet closed
        self._parts = []          # [(folder, first line, end line)] closed
        self._writer = ThreadPoolExecutor(1, thread_name_prefix='brisk-spool')
        self._error = None
        self._readers, self._removed = 0, False
        self._on_part = on_part     # called (on the writer's thread) with the spool after each part is written
        self._shipped, self._cut, self._cut_at, self._index = 0, None, 0, {}
        self.last_parts, self._last_piece = [], None   # the parts of the piece last tried, and how (None: from the bootstrap)
        self._failed_at, self.attempts = None, 0       # when packaging last failed, and how often since it last succeeded
        self.skipped = []         # (first, last) clock of each piece not made, for the stream gave it no update

    @classmethod
    def reopen(cls, folder, intent=None):
        """A spool a stopped process left: its closed parts, to package or keep."""
        spool = cls.__new__(cls)
        spool.path, spool.intent, spool.header, spool.master, spool.ended, spool.count = Path(folder), intent, None, None, False, 0
        spool._lock, spool._lines, spool._size, spool._pending = threading.Lock(), [], 0, []
        parts = sorted(p for p in (spool.path / 'parts').glob('[0-9]*') if p.is_dir()) if (spool.path / 'parts').is_dir() else []
        spool._parts = [(part, 0, 0) for part in parts]
        spool._writer, spool._error, spool._readers, spool._removed = ThreadPoolExecutor(1), None, 0, False
        spool._opened, spool._midnight = time.monotonic(), None
        spool._on_part, spool._cut, spool._cut_at, spool._index, spool.last_parts, spool._last_piece = None, None, 0, {}, [], None
        spool._failed_at, spool.attempts, spool.skipped = None, 0, []
        try:
            spool._shipped = min(int(json.loads((spool.path / 'shipped.json').read_text())['parts']), len(parts))
        except (OSError, ValueError, KeyError, TypeError):
            spool._shipped = 0
        return spool

    def __repr__(self):
        return f'Spool({str(self.path)!r}, lines={self.count})'

    def add(self, batch, line: bytes):
        if self._error:
            raise self._error
        if batch['type'] == 'bootstrap':
            # As a recording keeps it: fixed transport provenance only, never connection diagnostics.
            batch = next(canonical_batches(io.BytesIO(line)))
            self.header = {k: v for k, v in batch.items() if k != SKIPPED} | {'master': [], 'quotes': [], 'trades': []}
            self._midnight = _parquet.midnight_ns(batch['trading_date'])
            self.master = batch['master']
            equity = self.path / _parquet.EQUITY
            equity.parent.mkdir(parents=True, exist_ok=True)
            _parquet.pq.write_table(_parquet.equity_table(batch), equity, **_parquet.WRITE)
            _fsync(equity)
        with self._lock:
            self._lines.append(line)
            self._size += len(line)
            self.count += 1
            self.ended = batch['type'] == 'end'
        if self.ended or self._size >= PART_BYTES or time.monotonic() - self._opened >= PART_SECONDS:
            self.close_part()

    def close_part(self, wait=False):
        """Hand the open part to the writer (and with `wait`, until every part is on disk)."""
        with self._lock:
            lines, first = self._lines, self.count - len(self._lines)
            self._lines, self._size, self._opened = [], 0, time.monotonic()
            if lines:
                self._pending.append((first, lines))
                number = len(self._parts) + len(self._pending)
        if lines:
            self._writer.submit(self._write, number, first, lines)
        if wait:
            self._writer.submit(lambda: None).result()
            if self._error:
                raise self._error

    def _write(self, number, first, lines):
        if self._error:
            return
        try:
            folder = self.path / 'parts' / f'{number:06d}'
            staging = folder.with_name(f'.{folder.name}')
            shutil.rmtree(staging, ignore_errors=True)
            # Canonical, as package() makes them: nothing of the connection's diagnostics reaches the disk.
            _native.part(lines, staging, self._midnight, threads=_native.threads())
            for name in (_parquet.BATCHES, _parquet.DEPTHS, _parquet.TRADES):
                _fsync(staging / name)
            os.replace(staging, folder)
            with self._lock:
                self._pending.pop(0)
                self._parts.append((folder, first, first + len(lines)))
        except Exception as error:  # noqa: BLE001 - the feed raises it with its next batch
            self._error = error
            return
        if self._on_part:
            self._on_part(self)

    def replay(self, count, codes=None):
        """The segment's first `count` lines, as a Live replay sends them: closed parts re-encoded, then the lines
        not yet written as they arrived. With `codes`, a part's lines hold only their symbols' quotes and trades (and
        none for a batch that has none): a session of a few symbols is not sent the whole market. The spool stays
        until the replay is read or closed."""
        issues = None if codes is None else {m['issue_id'] for m in self.master if m['code'] in codes}
        with self._lock:
            parts, pending = list(self._parts), [(first, list(lines)) for first, lines in self._pending]
            pending.append((self.count - len(self._lines), list(self._lines)))
            self._readers += 1
        return _Replay(self, parts, pending, count, issues)

    def _release(self):
        with self._lock:
            self._readers -= 1
            gone = self._removed and not self._readers
        if gone:
            shutil.rmtree(self.path, ignore_errors=True)

    def parts(self):
        return [folder for folder, _, _ in self._parts]

    def package(self, output, repair=False, progress=None):
        """What is left of the segment, in one piece, as a package in `output` (see briskapi.cli.package_parts);
        with `repair`, one cut short gets the end its host never wrote."""
        self.close_part(wait=True)
        shipped = self._package(self.parts()[self._shipped:], Path(output), last=True, repair=repair, progress=progress)
        return shipped and shipped[0]

    def ship(self, output, final=False, repair=False, progress=None):
        """Package the next piece of the segment not shipped yet into `output`, as a recording of its own: once a
        part that closed after noon ends the morning (LUNCH_US), or its parts hold MAX_PIECE_ROWS quote updates;
        with `final` (the segment over, its parts all closed) also what is left, ending as the segment did (or,
        with `repair`, with the end its host never wrote). Returns (manifest, repaired), or None when no piece is
        ready. The parts count as shipped once packaged, or refused (brisk-parquet or the archive's rules found the
        piece wanting: trying again would not help; last_parts names them for keep()), so a piece is never shipped
        twice. A piece that could not be packaged here and now (the program did not start, a full disk) raises and
        stays: it is tried again from the same parts, after RETRY_SECONDS, MAX_ATTEMPTS times in the day, and with
        `final` once more; until then this returns None. A piece the stream sent no update to (a reconnection in the
        lunch break: its book and the day's history, as the segment before had them) is not made: it is in `skipped`.
        `progress(label)` is told how its packaging goes (see briskapi._native.package)."""
        if final:
            self.close_part(wait=True)
        elif self._failed_at is not None and (self.attempts >= MAX_ATTEMPTS or time.monotonic() - self._failed_at < RETRY_SECONDS):
            return None
        while True:
            parts, last = self._next(self.parts()[self._shipped:], final)
            if parts is None:
                return None
            if any(self._info(part)['updates'] for part in parts):
                return self._package(parts, Path(output), last=last, repair=repair and last, progress=progress)
            self.skipped.append((self._info(parts[0])['first'], self._info(parts[-1])['last']))
            self._mark(len(parts))

    def _next(self, parts, final):
        """The parts of the next piece, and whether it is the segment's last; (None, False) when none is ready."""
        if parts:
            began, rows = self._info(parts[0])['first'], 0
            for count, part in enumerate(parts, 1):
                info = self._info(part)
                if info['end']:
                    break   # the segment's end goes with the rest
                rows += info['rows']
                if (began < LUNCH_US <= info['last'] or rows >= MAX_PIECE_ROWS) and (count < len(parts) or not final):
                    return parts[:count], False
        return (parts, True) if final and parts else (None, False)

    def _package(self, parts, output, last, repair=False, progress=None):
        from briskapi import cli
        # Until the piece is known whole, what keep() can save of it is the day from its bootstrap through these parts.
        self.last_parts, self._last_piece = parts, None
        if not parts:
            return None
        try:
            bootstrap = _parquet.bootstrap_row(self.parts()[0])
            cut = self._cut_state() if self._shipped else None
        except Exception:
            self._failed()
            raise
        piece = {'midnight': _parquet.midnight_ns(bootstrap['trading_date']), 'day_start': self.parts()[0],
                 'cut': cut and (cut.state, cut.clock), 'bootstrap': bootstrap,
                 'state_out': self.path / 'cut' / f'{self._shipped + len(parts):06d}.parquet',
                 'end': 'cut' if not last else 'repair' if repair else None}
        self._last_piece = piece
        try:
            manifest = cli.package_parts(self.path / _parquet.EQUITY, parts, output, piece=piece, progress=progress)
        except ValueError:   # refused: the piece's parts are tried; keep() may still save their tables
            self._mark(len(parts))
            self._failed()
            raise
        except Exception:    # not packaged here and now: the same parts are tried again later (see ship)
            self._failed()
            raise
        if 'clock' in piece:
            self._cut, self._cut_at = _Cut(piece['state_out'], piece['clock']), self._shipped + len(parts)
        self._mark(len(parts))
        self._failed_at, self.attempts = None, 0
        return manifest, piece.get('repaired', False)

    def _failed(self):
        self._failed_at, self.attempts = time.monotonic(), self.attempts + 1

    def _whole(self, parts):
        """A piece of the day from its bootstrap through `parts`' last, with an end if it has none."""
        bootstrap = _parquet.bootstrap_row(parts[0])
        return {'midnight': _parquet.midnight_ns(bootstrap['trading_date']), 'day_start': parts[0], 'cut': None,
                'bootstrap': bootstrap, 'end': 'repair'}

    @property
    def piece_start(self):
        """Where the piece last tried begins, in microseconds after JST midnight: where the piece before it ended, or the
        bootstrap's clock; None before a piece was tried."""
        piece = self._last_piece
        return piece and (piece['cut'][1] if piece['cut'] else piece['bootstrap']['source_time_us'])

    def _info(self, part):
        if part not in self._index:
            bootstrap = _parquet.bootstrap_row(self.parts()[0])
            self._index[part] = _parquet.part_index(part, _parquet.midnight_ns(bootstrap['trading_date']))
        return self._index[part]

    def _cut_state(self):
        """Where the shipped pieces left the day; for a reopened spool, or after a piece that failed, found again from
        their parts (brisk-parquet reads them a range of issues at a time)."""
        if self._cut is None or self._cut_at != self._shipped:
            parts = self.parts()[:self._shipped]
            state = self.path / 'cut' / f'{self._shipped:06d}.parquet'
            midnight = _parquet.midnight_ns(_parquet.bootstrap_row(self.parts()[0])['trading_date'])
            clock = _native.state(self.path, parts, midnight, state, day_start=self.parts()[0], threads=_native.threads())
            self._cut, self._cut_at = _Cut(state, clock), self._shipped
        return self._cut

    def _mark(self, count):
        self._shipped += count
        staging = self.path / '.shipped.json'
        staging.write_text(json.dumps({'parts': self._shipped}))
        os.replace(staging, self.path / 'shipped.json')

    @property
    def removed(self):
        return self._removed

    @property
    def shipped(self):
        """How many of the parts are shipped (or were tried)."""
        return self._shipped

    def keep(self, root, progress=None):
        """The piece last tried (or, when where the pieces before it left the book could not be found, the day from its
        bootstrap through the piece's parts), or else (nothing tried yet) the segment so far, its open part closed, as a
        recording (tables, no manifest) in a folder of `root` named after it; None when there is nothing to keep. The parts
        kept count as shipped: they are saved. `progress(label)` is told how packaging it goes (see
        briskapi._native.package)."""
        if self.last_parts:
            piece, parts = self._last_piece, self.last_parts
            if piece is None:
                parts = self.parts()[:self.parts().index(parts[-1]) + 1]
                piece = self._whole(parts)
        elif not self._shipped:
            self.close_part(wait=True)
            parts = self.parts()
            if not parts:
                return None
            piece = self._whole(parts)
        else:
            return None
        staging = Path(tempfile.mkdtemp(dir=self.path))
        try:
            summary = _native.package(self.path, parts, staging, piece['midnight'], day_start=piece['day_start'], cut=piece['cut'],
                                      end=piece['end'] or 'repair', threads=_native.threads(), progress=progress)['summary']
        except BaseException:
            shutil.rmtree(staging, ignore_errors=True)
            raise
        name = local_name(summary, piece['bootstrap'].get('input_transport'))
        target = Path(root) / name
        for n in range(2, 100):
            if not target.exists():
                break
            target = Path(root) / f'{name}-{n}'
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(staging, target)
        self._mark(max(0, self.parts().index(parts[-1]) + 1 - self._shipped))
        return target

    def remove(self):
        """Delete the spool (once any replay still reading it ends)."""
        self._writer.shutdown(wait=True)
        with self._lock:
            self._removed = True
            readers = self._readers
        if not readers:
            shutil.rmtree(self.path, ignore_errors=True)


class _Replay:
    def __init__(self, spool, parts, pending, count, issues=None):
        self._spool, self._parts, self._pending, self._count, self._open = spool, parts, pending, count, True
        self._issues = issues

    def __iter__(self):
        try:
            for folder, first, _ in self._parts:
                if first >= self._count:
                    return
                for batch in _parquet.part_batches(folder, self._spool.header, self._spool.master, self._issues):
                    if batch['seq'] >= self._count:   # a batch's number is its line's: the replay ends where the live stream begins
                        return
                    yield json.dumps(batch, separators=(',', ':'), ensure_ascii=False).encode() + b'\n'
            at = self._parts[-1][2] if self._parts else 0
            for first, lines in self._pending:
                for line in lines[max(at - first, 0):]:
                    if at >= self._count:
                        return
                    yield line
                    at += 1
        finally:
            self.close()

    def close(self):
        if self._open:
            self._open = False
            self._spool._release()


def _alive(pid) -> bool:
    if sys.platform == 'win32':
        # os.kill(pid, 0) terminates a Windows process. Query its handle instead.
        import _winapi
        try:
            handle = _winapi.OpenProcess(_winapi.SYNCHRONIZE, False, pid)
        except OSError as error:
            if error.winerror == 87:  # ERROR_INVALID_PARAMETER: no such process
                return False
            if error.winerror == 5:   # ERROR_ACCESS_DENIED: a protected process
                return True
            raise
        try:
            return _winapi.WaitForSingleObject(handle, 0) == _winapi.WAIT_TIMEOUT
        finally:
            _winapi.CloseHandle(handle)
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def leftovers(root=None):
    """The broker segments that processes which stopped unexpectedly left in the spool, reopened (Spool.reopen)
    with their session's choices as `intent` (None from a spool that never recorded them). A demo's is the same for
    everyone and is deleted; a damaged spool is reported and left for a look."""
    root, found = Path(root or spool_root()), []
    for folder in sorted(root.glob('segment-*')) if root.is_dir() else ():
        try:
            owner = json.loads((folder / 'owner.json').read_text())
        except (OSError, ValueError):
            owner = {}
        if isinstance(owner.get('pid'), int) and owner['pid'] != os.getpid() and _alive(owner['pid']):
            continue
        spool = Spool.reopen(folder, owner.get('intent'))
        try:
            parts = spool.parts()
            source = parts and _parquet.pq.read_table(parts[0] / _parquet.BATCHES, columns=['source']).column(0)[0].as_py()
            if parts and not (folder / _parquet.EQUITY).exists():
                raise ValueError('its equity table is missing')
        except Exception as error:  # noqa: BLE001 - a damaged spool must not stop the next feed
            warnings.warn(f'Could not recover the recording in {folder}: {error}')
            continue
        if source and source not in DEMO_SOURCES:
            found.append(spool)
        else:
            spool.remove()
    return found
