"""Python's rules for a broker's recording, kept as the reference the Rust verifier (rust/brisk_parquet) is checked against.

These were the archive's checks before brisk-parquet: each row group decoded to numpy, re-encoded and compared with the
table read. They take minutes for a day, so only tests run them (tests/test_native.py)."""
import datetime as dt
import decimal
import re
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

import briskapi._parquet as _parquet
from briskapi._parquet import (BATCHES, DEPTHS, EQUITY, KINDS, TRADES, BROKER_MAX_ROWS, Columns, _decode_batches,
                               _decode_equity, _decode_trades, _depth_rows, _no_trades, as_format, batch_lengths,
                               check, midnight_ns, quote_lengths, trade_lengths)
from briskapi.schema import (BOOK_CHANGES, BOOKS, LEVELS, MASTER_KEYS, MAX_BOOK_LEVELS, MAX_DECODE_NS, MAX_HISTORY, MAX_SESSION_MS,
                             SKIPPED, UNIX_MS, _changes, apply_changes, best, code, integer, measurement, require, transports,
                             validate_manifest)
from briskapi.schema import broker as is_broker
from reference_package import _per_batch


def _check_master(master):
    """The master as schema's checks take it (see schema._scan): unique, sorted identities and valid entries."""
    require(isinstance(master, list) and 0 < len(master) <= 20000, 'Invalid master size')
    issue = -1
    for m in master:
        require(isinstance(m, dict) and set(m) == MASTER_KEYS, 'Unknown or missing master fields')
        previous, issue = issue, integer(m['issue_id'], 2**32 - 1)
        require(issue > previous, 'Master identities must be unique and sorted')
        code(m['code'])
        for key, value in m.items():
            if key == 'name':
                require(isinstance(value, str) and len(value) <= 256, 'Invalid security name')
            elif key != 'code':
                integer(value)




def _batch_groups(directory):
    """(day's midnight, a row group's table, its batches) for each row group of a recording's batches table: a
    broker's session has a million batches or more, which as dicts take some 1.6 GB, so they are never all at once."""
    source, midnight = pq.ParquetFile(Path(directory) / BATCHES), None
    for g in range(source.num_row_groups):
        table = source.read_row_group(g)
        batches = _decode_batches(table, midnight)
        if midnight is None:
            midnight = midnight_ns(batches[0]['trading_date'])
        yield midnight, table, batches


class _BatchRules:
    """The batch rules of schema._scan, for a broker's recording, applied to its batches (without their quotes and
    trades) a row group at a time, `n` in all: add() each group's batches, then finish(), which gives each batch's
    kind (KINDS) and clock, the skipped issues as issue << 32 | batch, and the first and last batch."""

    def __init__(self, master, n):
        require(0 < n <= BROKER_MAX_ROWS[BATCHES], 'Recording has no bootstrap')
        self.identities, self.n, self.seq, self.last = {m['issue_id'] for m in master}, n, 0, 0
        self.market_size = len(master)
        self.kinds, self.clocks, self.skipped = np.zeros(n, np.int8), np.zeros(n, np.int64), []
        self.received = self.previous_kind = self.first = self.final = None
        self.deltas = 0

    def add(self, batches):
        for b in batches:
            seq, kind = self.seq, b['type']
            if seq == 0:
                self._first(b)
            require(seq < self.n, 'Sequence gap')
            require(integer(b['seq']) == seq, 'Sequence gap')
            now = integer(b['source_time_us'], 86_400_000_000 - 1)
            require(now >= self.last, 'Market clock regression')
            require(seq == 0 or kind != 'bootstrap', 'Expected delta/end')
            if kind == 'history':
                require(self.previous_kind in {'bootstrap', 'history'} and now == self.last, 'History out of place')
            elif seq:
                require(kind in {'quotes', 'end'}, 'Expected delta/end')
            require(kind != 'end' or seq == self.n - 1, 'Data after end batch')
            if kind == 'quotes':
                integer(b['decode_ns'], MAX_DECODE_NS)
                stamp = integer(b['received_unix_ms'])
                require(UNIX_MS[0] <= stamp < UNIX_MS[1], 'Implausible local receipt clock')
                if self.received is None:
                    self.received = stamp
                require(abs(stamp - self.received) <= MAX_SESSION_MS, 'Local receipt clock span')
                self.deltas += 1
                require(b['replay_lateness_ms'] is None, 'Broker streams are not demo replays')
            if SKIPPED in b:
                skipped = b[SKIPPED]
                require(kind in ('bootstrap', 'quotes') and isinstance(skipped, list) and skipped and
                        all(issue in self.identities for issue in skipped) and skipped == sorted(set(skipped)),
                        'Invalid skipped issues')
                self.skipped += (issue << 32 | seq for issue in skipped)
            if kind == 'end':
                require(integer(b['frames']) >= self.deltas, 'End summary mismatch')
                integer(b['quote_updates'])
                measurement(b['replay_wall_ms'], MAX_SESSION_MS)
            self.kinds[seq], self.clocks[seq] = KINDS[kind], now
            self.last, self.previous_kind, self.seq, self.final = now, kind, seq + 1, b

    def _first(self, first):
        require(first['type'] == 'bootstrap' and is_broker(first['source']), 'Unsupported bootstrap/source')
        require(isinstance(first['trading_date'], str) and re.fullmatch(r'\d{8}', first['trading_date']), 'Invalid date')
        dt.datetime.strptime(first['trading_date'], '%Y%m%d')
        require(integer(first['market_issue_count']) >= self.market_size, 'Invalid market count')
        require(first['input_transport'] in transports(first['source']), 'Invalid broker transport fields')
        require(first['source_timestamp_origin'] == 'brisk_decoder_unverified', 'Invalid clock provenance')
        require(first['exchange_delay_ms'] is None, 'Unverified exchange delay')
        self.first = first

    def finish(self):
        require(self.seq == self.n and self.final['type'] == 'end', 'Recording has no clean end')
        return self.kinds, self.clocks, np.array(sorted(self.skipped), np.int64), self.first, self.final


def _monotonic(issues, values, carry):
    """Each issue's values do not go back (rows sorted by issue), from `carry` ({issue: last value}, updated)."""
    if not len(issues):
        return True
    same = issues[1:] == issues[:-1]
    ok = not (same & (values[1:] < values[:-1])).any()
    first = int(issues[0])
    ok = ok and (first not in carry or values[0] >= carry[first])
    ends = np.flatnonzero(np.append(~same, True))
    carry.update(zip(issues[ends].tolist(), values[ends].tolist()))
    return ok


class _Replay:
    """The book changes of a broker's quotes, replayed a row group at a time (schema._scan's rule, and the checkpoints
    Columns.checkpoints puts where it does): each issue's books and the changes counted since its last checkpoint carry on
    from one row group to the next."""

    def __init__(self):
        self.issue, self.books, self.count = None, None, 0

    def group(self, table, rows):
        """Refuse the row group's quotes (`table` and its Columns `rows`, which have changes) unless their changes obey the
        rules and the table's checkpoints are where they should be, with what they should hold."""
        cells = table.column('checkpoint').to_pylist()
        for at, q in enumerate(rows.quotes(np.arange(len(rows)))):
            changes = q[BOOK_CHANGES]
            if q['issue_id'] != self.issue:
                self.issue, self.books, self.count = q['issue_id'], [{} for _ in BOOKS], -len(changes)
            require(len(changes) <= len(BOOKS) * MAX_BOOK_LEVELS, 'Invalid book changes')
            _changes(changes)
            apply_changes(self.books, changes)
            require(all(len(book) <= MAX_BOOK_LEVELS for book in self.books), 'Invalid book changes')
            require(all(best(self.books, k) == q[name] for k, name in enumerate(BOOKS)), 'Book changes do not give the quote\'s book')
            self.count += len(changes)
            expected = None
            if self.count >= _parquet.CHECKPOINT_CHANGES:
                self.count = 0
                expected = [{'book': k, 'price': decimal.Decimal(price) / 10, 'size': decimal.Decimal(quantity), 'count': orders}
                            for k, book in enumerate(self.books)
                            for price, (quantity, orders) in sorted(book.items(), reverse=k % 2 == 0)]
            require(cells[at] == expected, f'Non-canonical {DEPTHS}')


def verify_broker(directory, manifest=None, progress=None):
    """Verify a broker's recording (all four tables, in `directory`) a row group at a time, never whole in memory:
    exactly the expected tables, each row group the canonical encoding of its content (so nothing but the
    recording's fields can be in it), the batches' rules (schema._scan's: sequence, kinds, clocks, transport,
    local timing), and of every quote and trade its identity, order, batch, clock and value ranges, and its
    books' order. Not checked, the market's own consistency (a broker's content is uploader-declared: trades
    against volumes, each issue's frames over time). Returns the summary, which must be the manifest's.
    `progress(label)` is told as each table is done, for the archive's timing lines."""
    progress = progress or (lambda label: None)
    directory = Path(directory)
    if manifest is not None:
        validate_manifest(manifest)
    fmt = check(directory, broker=True, fmt=manifest and manifest['schema'])
    master = _decode_equity(pq.read_table(directory / EQUITY))
    _check_master(master)
    rules, midnight = _BatchRules(master, pq.ParquetFile(directory / BATCHES).metadata.num_rows), None
    for midnight, table, batches in _batch_groups(directory):   # a row group of batches at a time
        rules.add(batches)
        expected = Columns(None, None, None, master, batches, *_no_trades())._batches(midnight)
        require(table.equals(as_format(expected, BATCHES, fmt)), f'Non-canonical {BATCHES}')
    kinds, clocks, skipped, first, final = rules.finish()
    progress('verify: batches')
    shell = Columns(None, None, None, master, [], *_no_trades())
    require(pq.read_table(directory / EQUITY).equals(as_format(shell._equity(midnight), EQUITY, fmt)), f'Non-canonical {EQUITY}')
    ids = np.array([m['issue_id'] for m in master], np.int64)
    codes = np.array([m['code'] for m in master], dtype=object)
    n = len(kinds)
    quotes, trades = np.zeros(n, np.int64), np.zeros(n, np.int64)
    quote_bytes = trade_bytes = 0
    last_key, frames, times = (-1, -1), {}, {}

    def identity(issues, row_codes):
        at = np.searchsorted(ids, issues)
        require(not len(issues) or ((at < len(ids)) & (ids[np.minimum(at, len(ids) - 1)] == issues)).all(), 'Unknown issue')
        require(not len(issues) or (codes[at] == row_codes).all(), 'Quote identity mismatch/duplicate')

    def batch_of(seqs, allowed, times_us, what):
        require(not len(seqs) or (seqs.max() < n and np.isin(kinds[seqs], [KINDS[k] for k in allowed]).all()),
                f'{what} outside their batches')
        require(not len(seqs) or (times_us <= clocks[seqs]).all(), f'{what} after their batch clock')

    source, replay = pq.ParquetFile(directory / DEPTHS), _Replay()
    for g in range(source.num_row_groups):
        table = source.read_row_group(g)
        rows = _depth_rows(table, midnight, master)
        # (A table's checkpoints are told apart: they follow the changes' replay below.)
        without = lambda t: t.drop_columns(['checkpoint']) if 'checkpoint' in t.column_names else t  # noqa: E731
        require(without(table).equals(without(as_format(rows._depths(midnight), DEPTHS, fmt))), f'Non-canonical {DEPTHS}')
        issue, seq = rows.q['issue_id'], rows.q['seq']
        identity(issue, rows.code)
        keys = issue << 32 | seq
        require(not len(keys) or ((np.diff(keys) > 0).all() and keys[0] > (last_key[0] << 32 | last_key[1])),
                'Quotes not sorted by issue, then batch')
        if len(keys):
            last_key = (int(issue[-1]), int(seq[-1]))
        batch_of(seq, ['bootstrap', 'quotes'], rows.q['source_time_us'], 'Quotes')
        require(not len(skipped) or not np.isin(keys, skipped).any(), 'Invalid skipped issues')
        require(all(((v >= 0) & (v < 2**53)).all() for v in rows.q.values()) and (rows.q['frame'] < 2**32).all()
                and (issue < 2**32).all(), 'Invalid unsigned integer')
        require(_monotonic(issue, rows.q['frame'], frames) and _monotonic(issue, rows.q['source_time_us'], times),
                'Quote frame/time regression')
        for side, (offsets, levels) in rows.books.items():
            require(not len(offsets) or np.diff(offsets).max(initial=0) <= LEVELS, 'Invalid book side')
            require(((levels >= 0) & (levels < 2**53)).all(), 'Invalid unsigned integer')
            if len(levels) > 1:
                within = np.ones(len(levels) - 1, bool)
                within[(offsets[1:-1] - 1)[(offsets[1:-1] > 0) & (offsets[1:-1] < len(levels))]] = False
                step = np.diff(levels[:, 0])
                order = step < 0 if side.startswith('bid') else step > 0
                require(order[within].all(), 'Book prices out of order')
        if rows.changes is not None:
            replay.group(table, rows)
        quotes += _per_batch(seq, n)
        quote_bytes += int(quote_lengths(rows).sum())
    progress('verify: depths')
    plain = lambda table: table.set_column(2, 'aggressor_side', table.column(2).cast(pa.string()))  # noqa: E731
    source, carry, last_key = pq.ParquetFile(directory / TRADES), {}, (-1, -1)
    for g in range(source.num_row_groups):
        table = source.read_row_group(g)
        t, t_code = _decode_trades(table, midnight)
        rows = Columns(None, None, None, master, [], t, t_code)
        rows.trade_ids(carry)
        require(plain(table).equals(plain(as_format(rows._trades(midnight, slice(None)), TRADES, fmt))), f'Non-canonical {TRADES}')
        identity(t['issue_id'], t_code)
        keys = t['issue_id'] << 32 | t['seq']
        require(not len(keys) or ((np.diff(keys) >= 0).all() and keys[0] >= (last_key[0] << 32 | last_key[1])),
                'Trades not sorted by issue, then batch')
        if len(keys):
            last_key = (int(t['issue_id'][-1]), int(t['seq'][-1]))
        batch_of(t['seq'], ['bootstrap', 'quotes', 'history'], t['time_us'], 'Trades')
        require(all(((v >= 0) & (v < 2**53)).all() for v in t.values()), 'Invalid unsigned integer')
        require((t['price10'] > 0).all() and (t['quantity'] > 0).all() and (t['frame'] < 2**32).all(), 'Invalid trade')
        trades += _per_batch(t['seq'], n)
        trade_bytes += int(trade_lengths(t, t_code).sum())
    progress('verify: trades')
    require(quotes[0] + len(first.get(SKIPPED, ())) == len(master), 'Incomplete bootstrap')
    history = kinds == KINDS['history']
    require(((trades[history] > 0) & (trades[history] <= MAX_HISTORY)).all(), 'Invalid trades')
    require(final['quote_updates'] == int(quotes[kinds == KINDS['quotes']].sum()), 'End summary mismatch')
    expanded, at = quote_bytes + trade_bytes, 0
    for _, _, batches in _batch_groups(directory):   # their canonical sizes, a row group at a time again
        expanded += batch_lengths(batches, master, quotes[at:at + len(batches)], trades[at:at + len(batches)])
        at += len(batches)
    summary = dict(source=first['source'], trading_date=first['trading_date'], first_source_time_us=first['source_time_us'],
                   last_source_time_us=final['source_time_us'], codes=sorted(m['code'] for m in master), batches=n,
                   quote_updates=int(quotes.sum()), expanded_bytes=expanded)
    require(manifest is None or summary == manifest['summary'], 'Summary mismatch')
    return summary
