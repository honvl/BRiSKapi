"""Python's way of writing a day's spooled parts as pieces, kept as the reference brisk-parquet's `package` is checked against.

These were the recorder's: a range of issues at a time, decoded to numpy, re-encoded with pyarrow. They take minutes for a day,
so only tests run them (tests/test_native.py)."""
from concurrent.futures import ThreadPoolExecutor
import gc
import io
import os
from pathlib import Path
import sys
import threading
from typing import NamedTuple

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from briskapi._parquet import (BATCH_SCHEMA, BATCHES, BOOKS, DEPTH_CHANGES_SCHEMA, DEPTH_SCHEMA, DEPTHS, EQUITY, FILES, INTS, KINDS, PART_ISSUES,
                               ROW_GROUP, SCHEMAS, TRADE_SCHEMA, TRADES, Columns, _decode_batches, _decode_equity, _decode_trades,
                               _depth_rows, _no_trades, _numbers, _part_columns, _rows, batch_lengths,
                               midnight_ns, options, quote_lengths, trade_lengths)
import briskapi.schema as schema
from briskapi.schema import SKIPPED, apply_changes, canonical_batches, require

class Cut(NamedTuple):
    """Where shipped pieces left the day: each issue's last quote (Columns) and the last batch's clock."""
    latest: object
    clock: int


def _per_batch(seqs, n):
    return np.bincount(seqs, minlength=n)[:n] if len(seqs) else np.zeros(n, np.int64)


def _updates(batches, quotes):
    """The quote updates of the 'quotes' batches (`quotes`: each batch's quote count), as an end counts them."""
    return int(sum(n for b, n in zip(batches, quotes.tolist()) if b['type'] == 'quotes'))


def part_columns(lines, header):
    """Start building the Columns of consecutive decoder lines, canonical (canonical_batches): the same as
    Columns.from_batches of them. Returns a function that gives them. Without the GIL they are parsed and built
    from now on, in slices of at least SLICE_LINES lines on threads; with it, by the function, here.
    `header` (the segment's bootstrap without quotes and trades) stands in for the bootstrap of a slice that
    starts later."""
    def build(piece):
        batches = list(canonical_batches(io.BytesIO(b''.join(piece))))
        first = batches[0]['type'] == 'bootstrap'
        columns = Columns.from_batches(batches if first else [header, *batches])
        if not first:
            columns.batches = columns.batches[1:]
        return columns
    pool = _threads()
    if pool is None:
        return lambda: build(lines)
    count = max(1, min(_THREADS, len(lines) // SLICE_LINES))
    step = -(-len(lines) // count)
    pieces = [pool.submit(build, lines[i:i + step]) for i in range(0, len(lines), step)]

    def columns():
        built = [piece.result() for piece in pieces]
        return built[0] if len(built) == 1 else Columns.concat(built, built[0].master)
    return columns


def write_part(columns, header, directory, carry=None):
    """A spool part (see briskapi._spool): the depths, trades and batch tables of consecutive decoder lines'
    Columns (part_columns).

    `header` is the segment's bootstrap without quotes and trades; it stands in for the master when the part
    starts later. `carry` continues trade IDs from the parts before. Equity is written once, by the spool.
    """
    midnight = midnight_ns(header['trading_date'])
    columns.trade_ids(carry)
    directory = Path(directory)
    (directory / BATCHES).parent.mkdir(parents=True, exist_ok=True)
    try:
        pq.write_table(columns._batches(midnight), directory / BATCHES, row_group_size=ROW_GROUP, **options(BATCHES))
        rows = columns.sorted()
        issues = rows.q['issue_id']
        with pq.ParquetWriter(directory / DEPTHS, DEPTH_CHANGES_SCHEMA if rows.changes else DEPTH_SCHEMA,
                              **options(DEPTHS)) as writer:
            # A row group per PART_ISSUES issue IDs, so that a piece of the day is read a range of issues at a time.
            starts = np.flatnonzero(np.diff(issues // PART_ISSUES, prepend=-1)) if len(issues) else np.zeros(1, np.int64)
            for start, stop in zip(starts.tolist(), [*starts[1:].tolist(), len(issues)]):
                writer.write_table(rows.take(np.arange(start, stop))._depths(midnight), row_group_size=ROW_GROUP)
        trades = columns._trades(midnight, slice(None))
        owners = np.asarray(columns.t['issue_id'])
        with pq.ParquetWriter(directory / TRADES, TRADE_SCHEMA, **options(TRADES)) as writer:
            starts = np.flatnonzero(np.diff(owners // PART_ISSUES, prepend=-1)) if len(owners) else np.zeros(1, np.int64)
            for start, stop in zip(starts.tolist(), [*starts[1:].tolist(), len(owners)]):
                writer.write_table(trades.slice(start, stop - start), row_group_size=ROW_GROUP)
    except (TypeError, OverflowError) as error:
        raise ValueError(f'Invalid recording value: {error}') from None


# Building columns is Python for every row; without the GIL (free-threaded Python) it was spread over _THREADS threads, a
# spool part in slices of at least SLICE_LINES lines.
_THREADS = min(4, max(1, (os.cpu_count() or 1) // 3))
SLICE_LINES = 16
_pool, _pool_lock = None, threading.Lock()


def _threads():
    """The shared pool, or None with the GIL on."""
    global _pool
    if getattr(sys, '_is_gil_enabled', lambda: True)():
        return None
    with _pool_lock:
        _pool = _pool or ThreadPoolExecutor(_THREADS, thread_name_prefix='brisk-parquet')
    return _pool


PIECE_ROWS = 500_000
DECODE_ROWS = 50_000   # rows of Arrow decoded at a time into numpy, which takes a third of the memory

def latest(columns, before=None):
    """Each issue's last quote row in `columns`, or in `before` (the same, for what came earlier) for an issue that
    has none there: the book as it stands after them."""
    rows = [c for c in (before, columns) if c is not None and len(c)]
    if not rows:
        return before
    t, t_code = _no_trades()
    both = Columns.concat([Columns(c.q, c.code, c.books, c.master, [], t, t_code, changes=c.changes) for c in rows], columns.master)
    order = both.order
    issues = both.q['issue_id'][order]
    last = np.ones(len(order), bool)
    last[:-1] = issues[1:] != issues[:-1]
    out = both.take(order[last])
    out.batches, out.order = [], None
    return out


def after(columns, cut, bootstrap):
    """Consecutive batches of a day (`columns`, from a spool's parts) as a recording of its own: a bootstrap like
    the day's (`bootstrap`, its batch row) at the clock of the batch before them, holding each issue's quote as
    of then (`cut.latest`; the issues with none skipped), then the batches, numbered on from it."""
    shift = columns.batches[0]['seq'] - 1
    head = {k: v for k, v in bootstrap.items() if k != SKIPPED} | {'seq': 0, 'source_time_us': cut.clock}
    quoted = set(cut.latest.q['issue_id'].tolist()) if cut.latest is not None else set()
    skipped = sorted(m['issue_id'] for m in columns.master if m['issue_id'] not in quoted)
    if skipped:
        head[SKIPPED] = skipped
    t, t_code = _no_trades()
    pieces = [Columns({**columns.q, 'seq': columns.q['seq'] - shift}, columns.code, columns.books, columns.master,
                      [dict(b, seq=b['seq'] - shift) for b in columns.batches], {**columns.t, 'seq': columns.t['seq'] - shift},
                      columns.t_code, changes=columns.changes)]
    if cut.latest is not None:
        state = cut.latest
        pieces.insert(0, Columns({**state.q, 'seq': np.zeros(len(state), np.int64)}, state.code, state.books,
                                 columns.master, [head], t, t_code, changes=state.changes))
    else:
        pieces.insert(0, Columns({k: np.zeros(0, np.int64) for k in INTS}, np.zeros(0, dtype=object),
                                 {side: (np.zeros(1, np.int64), np.zeros((0, 3), np.int64)) for side in BOOKS},
                                 columns.master, [head], t, t_code))
    return Columns.concat(pieces, columns.master)


def recount_end(columns):
    """A segment's end counts its own quote updates (the host's counts the whole day's)."""
    columns.batches[-1] = dict(columns.batches[-1], quote_updates=_updates(columns.batches,
                                                                         _per_batch(columns.q['seq'], len(columns.batches))))







def whole_books(columns, order):
    """The last row of each issue in `order` (rows in `order`, an issue's in batch order) as Columns of quotes alone, with
    its changes the issue's whole books after it, each best first: the rows a piece after a cut starts from."""
    offsets, levels = columns.changes
    issues = columns.q['issue_id'][order]
    last = np.ones(len(order), bool)
    last[:-1] = issues[1:] != issues[:-1]
    books, previous, counts, flat = None, None, [], []
    for at, row in enumerate(order.tolist()):
        if issues[at] != previous:
            books, previous = [{} for _ in schema.BOOKS], issues[at]
        apply_changes(books, levels[offsets[row]:offsets[row + 1]].tolist())
        if last[at]:
            whole = [[k, price, *books[k][price]] for k in range(len(schema.BOOKS)) for price in sorted(books[k], reverse=k % 2 == 0)]
            counts.append(len(whole))
            flat.extend(whole)
    out = _rows(columns, order[last])
    out.changes = np.concatenate([[0], np.cumsum(counts)]), np.array(flat, np.int64).reshape(-1, 4)
    return out


class _TableWriter:
    """Writes tables given a range at a time in the row groups writing their rows at once would make: ROW_GROUP rows
    each, the last shorter."""

    def __init__(self, path, schema, name):
        self._writer, self._pending = pq.ParquetWriter(path, schema, **options(name)), None

    def add(self, table):
        self._pending = table if self._pending is None else pa.concat_tables([self._pending, table])
        while self._pending.num_rows >= ROW_GROUP:
            self._writer.write_table(self._pending.slice(0, ROW_GROUP), row_group_size=ROW_GROUP)
            self._pending = self._pending.slice(ROW_GROUP)

    def close(self):
        if self._pending is not None and self._pending.num_rows:
            self._writer.write_table(self._pending, row_group_size=ROW_GROUP)
        self._writer.close()


def _leaves(type_):
    """How many columns of a Parquet file a field of this Arrow type takes (a list or struct takes its items')."""
    if pa.types.is_list(type_):
        return _leaves(type_.value_type)
    if pa.types.is_struct(type_):
        return sum(_leaves(type_.field(i).type) for i in range(type_.num_fields))
    return 1


def leaf_index(schema, name):
    """The index of a top-level column among a file's columns (nested ones, such as the books, take several). Not
    asked of the file: its schema's column(c).path keeps some 0.8 MB of memory for every call, and a piece reads
    the footers of a thousand parts."""
    index = 0
    for field in schema:
        if field.name == name:
            return index
        index += _leaves(field.type)
    raise KeyError(name)


def _layout(parts, name):
    """Where each issue lies in a table of the parts: per part, the lowest and highest issue ID of each of its row
    groups (write_part gives each PART_ISSUES issue IDs their own), and the rows each block of PART_ISSUES holds
    across the parts."""
    spans, blocks, column = [], {}, leaf_index(SCHEMAS[name], 'issue_id')
    for part in parts:
        meta = pq.read_metadata(Path(part) / name)
        low, high = np.zeros(meta.num_row_groups, np.int64), np.full(meta.num_row_groups, 2**62, np.int64)
        rows = np.zeros(meta.num_row_groups, np.int64)
        for g in range(meta.num_row_groups):
            stats = meta.row_group(g).column(column).statistics
            if stats is not None and stats.has_min_max:
                low[g], high[g] = stats.min, stats.max
            rows[g] = meta.row_group(g).num_rows
            blocks[low[g] // PART_ISSUES] = blocks.get(low[g] // PART_ISSUES, 0) + rows[g]
        spans.append((low, high, rows))
    return spans, blocks


def _ranges(blocks):
    """Issue ID ranges [low, high) of about PIECE_ROWS rows each, from the rows of each block (see _layout)."""
    ranges, low, rows = [], 0, 0
    for block in sorted(blocks):
        rows += blocks[block]
        if rows >= PIECE_ROWS:
            ranges.append((low, (block + 1) * PART_ISSUES))
            low, rows = (block + 1) * PART_ISSUES, 0
    return [*ranges, (low, 2**63 - 1)]


def _read_range(parts, spans, name, low, high, chunk=None):
    """The tables of the parts' rows of `name` of issues in [low, high), the parts' one after another: only the
    row groups whose issue IDs can match are read, each part's file opened once, and as many parts' at a time as hold
    `chunk` rows (all of them by default)."""
    tables, rows = [], 0
    for part, (first, last, count) in zip(parts, spans):
        groups = np.flatnonzero((last >= low) & (first < high))
        if len(groups):
            file = pq.ParquetFile(Path(part) / name)
            try:
                tables.append(file.read_row_groups(groups.tolist()))
            finally:
                file.close()
                del file
            rows += int(count[groups].sum())
            if chunk is not None and rows >= chunk:
                yield pa.concat_tables(tables)
                tables, rows = [], 0
    if tables:
        yield pa.concat_tables(tables)


def write_piece(equity, parts, directory, midnight, cut=None, bootstrap=None, end=None):
    """Write consecutive spooled parts as one recording in `directory`, a range of issues from every part at a
    time, and its batches a row group at a time, so that no day is ever whole in memory (a piece of the day is some
    60 million quote updates and a million batches): the same tables as their Columns written at once (load_parts,
    then `after` a cut, then their end, then Columns.write), which tests/test_spool.py checks.

    `cut` and `bootstrap` (the day's bootstrap row): a piece that starts after a cut (see after()). `end`: None for
    the parts' own end, 'repair' for one if they have none (repair_end), 'cut' for one that ends a piece.
    Returns (summary, each issue's last quote as Columns, the last batch's clock, whether an end was repaired)."""
    directory = Path(directory)
    for name in FILES:
        (directory / name).parent.mkdir(parents=True, exist_ok=True)
    master = _decode_equity(pq.read_table(equity))
    # The batches' small tables, once, for what the whole needs: where it starts and ends, each batch's kind and
    # the first and last local receipt time (an end repaired, or recounted, needs them).
    first = last = None
    kinds, received = [], [None, None]
    for part in parts:
        batches = _decode_batches(pq.read_table(Path(part) / BATCHES), midnight)
        first, last = first or batches[0], batches[-1]
        kinds.append(np.array([KINDS[b['type']] for b in batches], np.int8))
        for b in batches:
            if b['type'] == 'quotes' and b.get('received_unix_ms') is not None:
                received[0] = b['received_unix_ms'] if received[0] is None else received[0]
                received[1] = b['received_unix_ms']
    require(first is not None, 'Recording has no bootstrap')
    kinds, first_seq = np.concatenate(kinds), first['seq']
    counts = [np.zeros(len(kinds), np.int64), np.zeros(len(kinds), np.int64)]
    for part in parts:
        for i, name in enumerate((DEPTHS, TRADES)):
            seqs = _numbers(pq.read_table(Path(part) / name, columns=['seq']).column('seq')) - first_seq
            counts[i] += _per_batch(seqs, len(kinds))
    shift, head, quoted = 0, None, set()
    if cut is not None:
        shift = first_seq - 1
        head = {k: v for k, v in bootstrap.items() if k != SKIPPED} | {'seq': 0, 'source_time_us': cut.clock}
        quoted = set(cut.latest.q['issue_id'].tolist()) if cut.latest is not None else set()
        skipped = sorted(m['issue_id'] for m in master if m['issue_id'] not in quoted)
        if skipped:
            head[SKIPPED] = skipped
        kinds = np.concatenate([[KINDS['bootstrap']], kinds]).astype(np.int8)
        counts = [np.concatenate([[len(quoted)], counts[0]]), np.concatenate([[0], counts[1]])]
    else:
        require(first['type'] == 'bootstrap', 'Recording has no bootstrap')
    updates = int(counts[0][kinds == KINDS['quotes']].sum())
    last = dict(last, seq=last['seq'] - shift)
    tail, recount, repaired = None, False, False
    if end == 'cut' or (end == 'repair' and last['type'] != 'end'):
        tail = {'type': 'end', 'seq': last['seq'] + 1, 'source_time_us': last['source_time_us'], 'frames': last['seq'],
                'quote_updates': updates,
                'replay_wall_ms': float(received[1] - received[0]) if received[0] is not None else 0.0}
        counts, repaired = [np.append(c, 0) for c in counts], end == 'repair'
        kinds = np.append(kinds, KINDS['end']).astype(np.int8)
    elif cut is not None and last['type'] == 'end':
        recount, last = True, dict(last, quote_updates=updates)
    final = tail or last
    require(final['type'] == 'end', 'Recording has no clean end')
    total = len(kinds)
    from_parts = total - (head is not None) - (tail is not None)   # batches that come from the parts

    def batch_rows():
        """The piece's batches in order: its head, the parts' (numbered on from it), its end."""
        at = 0
        if head is not None:
            yield head
        for part in parts:
            for b in _decode_batches(pq.read_table(Path(part) / BATCHES), midnight):
                at += 1
                b = dict(b, seq=b['seq'] - shift) if shift else b
                yield dict(b, quote_updates=updates) if recount and at == from_parts else b
        if tail is not None:
            yield tail

    shell = Columns(None, None, None, master, [], *_no_trades())
    expanded, quote_rows, latest, quote_bytes, trade_bytes = 0, 0, [], 0, 0
    try:
        pq.write_table(shell._equity(midnight), directory / EQUITY, row_group_size=ROW_GROUP, **options(EQUITY))
        batch_writer = _TableWriter(directory / BATCHES, BATCH_SCHEMA, BATCHES)
        chunk, at, first_row = [], 0, None
        for row in batch_rows():
            first_row = first_row or row
            chunk.append(row)
            if len(chunk) == ROW_GROUP:
                batch_writer.add(Columns(None, None, None, master, chunk, *_no_trades())._batches(midnight))
                expanded += batch_lengths(chunk, master, counts[0][at:at + len(chunk)], counts[1][at:at + len(chunk)])
                at, chunk = at + len(chunk), []
        if chunk:
            batch_writer.add(Columns(None, None, None, master, chunk, *_no_trades())._batches(midnight))
            expanded += batch_lengths(chunk, master, counts[0][at:], counts[1][at:])
        batch_writer.close()
        depth_spans, blocks = _layout(parts, DEPTHS)
        trade_spans, _ = _layout(parts, TRADES)
        changed = (cut is not None and cut.latest is not None and cut.latest.changes is not None) or any(
            'changes' in pq.read_schema(Path(part) / DEPTHS).names for part in parts)   # a part with no quotes says nothing
        depths = _TableWriter(directory / DEPTHS, DEPTH_CHANGES_SCHEMA if changed else DEPTH_SCHEMA, DEPTHS)
        trades = _TableWriter(directory / TRADES, TRADE_SCHEMA, TRADES)
        for low, high in _ranges(blocks):
            pieces = []
            for raw in _read_range(parts, depth_spans, DEPTHS, low, high, DECODE_ROWS):
                rows = _depth_rows(raw, midnight, master)   # decoded to numpy, a few parts at a time
                del raw
                issue = rows.q['issue_id']
                if not ((issue >= low) & (issue < high)).all():
                    rows = _rows(rows, np.flatnonzero((issue >= low) & (issue < high)))
                rows.q['seq'] = rows.q['seq'] - shift
                pieces.append(rows)
            if cut is not None and cut.latest is not None:
                issue = cut.latest.q['issue_id']
                state = _rows(cut.latest, np.flatnonzero((issue >= low) & (issue < high)))
                state.q['seq'] = np.zeros(len(state), np.int64)
                pieces.insert(0, state)
            if pieces:
                joined = pieces[0] if len(pieces) == 1 else Columns.concat(
                    [Columns(r.q, r.code, r.books, master, [], *_no_trades(), changes=r.changes) for r in pieces], master)
                del pieces, rows
                order = joined.order if joined.order is not None else np.lexsort((joined.q['seq'], joined.q['issue_id']))
                issues = joined.q['issue_id'][order]
                newest = np.ones(len(issues), bool)
                newest[:-1] = issues[1:] != issues[:-1]
                latest.append(whole_books(joined, order) if joined.changes else _rows(joined, order[newest]))
                points = joined.checkpoints(order) if joined.changes else None
                for start in range(0, len(order), ROW_GROUP):   # written a row group at a time: never whole as Arrow
                    ordered = _rows(joined, order[start:start + ROW_GROUP])
                    depths.add(ordered._depths(midnight, points and points[start:start + ROW_GROUP]))
                    quote_bytes += int(quote_lengths(ordered).sum())
                    quote_rows += len(ordered)
                del joined, order, issues
            for raw in _read_range(parts, trade_spans, TRADES, low, high):
                t, t_code = _decode_trades(raw, midnight)
                del raw
                keep = np.flatnonzero((t['issue_id'] >= low) & (t['issue_id'] < high))
                t, t_code = {k: v[keep] for k, v in t.items()}, t_code[keep]
                t['seq'] = t['seq'] - shift
                order = np.lexsort((np.arange(len(t_code)), t['seq'], t['issue_id']))
                rows = Columns(None, None, None, master, [], {k: v[order] for k, v in t.items()}, t_code[order])
                if len(t_code):
                    trades.add(rows._trades(midnight, slice(None)))
                    trade_bytes += int(trade_lengths(rows.t, rows.t_code).sum())
                del rows, t, t_code
            # A footer or a table is a little Python object around a lot of C++ memory, so the collector, which
            # counts objects, seldom runs: without this a piece took gigabytes more than it holds.
            gc.collect()
            pa.default_memory_pool().release_unused()   # and Arrow's allocator keeps what it freed, too
        depths.close()
        trades.close()
    except (TypeError, OverflowError) as error:
        raise ValueError(f'Invalid recording value: {error}') from None
    first_row = first_row or head
    summary = dict(source=first_row['source'], trading_date=first_row['trading_date'], first_source_time_us=first_row['source_time_us'],
                   last_source_time_us=final['source_time_us'], codes=sorted(m['code'] for m in master),
                   batches=total, quote_updates=quote_rows, expanded_bytes=expanded + quote_bytes + trade_bytes)
    state = None
    if latest:
        state = Columns.concat([Columns(r.q, r.code, r.books, master, [], *_no_trades(), changes=r.changes) for r in latest], master)
        state = _rows(state, state.order)
    return summary, state, final['source_time_us'], repaired



def load_parts(equity, parts, midnight=None):
    """The canonical Columns of a spooled segment: its equity table and its parts, in order. Written, they are
    the same bytes as the segment's batches written at once (Columns.from_batches(...).write). Parts that do not
    start with the bootstrap need its day's `midnight`."""
    master = _decode_equity(pq.read_table(equity))
    pieces = []
    for part in parts:
        if midnight is None:
            first = _decode_batches(pq.read_table(Path(part) / BATCHES))[0]
            midnight = midnight_ns(first['trading_date'])
        pieces.append(_part_columns(Path(part), midnight, master))
    require(pieces, 'Recording has no bootstrap')
    return Columns.concat(pieces, master)
