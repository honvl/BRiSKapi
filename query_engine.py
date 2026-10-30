"""The cloud API's engine: Databento-style records from stored recordings, and their OHLCV bars.

Server side only (the query and archive functions); not part of the briskapi package, so bars come
from the cloud. The records themselves come from briskapi.records, shared with briskapi.Live. Everything
reads the archive bucket through `Bucket`, a boto3-style client.

Datasets follow Databento's VENUE.FEED naming: XTKS.BRISK is an owner's live recordings (any broker's
BRiSK: the same app and stream), XTKS.BRISKDEMO the public demo's market tables, kept once in the
archive. OHLCV bars for XTKS.BRISK are an owner's own: built ahead from its current live recordings of
each trading day (deduplicated by trade) whenever one of them is stored, deleted or restored, holding only
the bars whose time span within the trading sessions those recordings cover. No one else's uploads enter
them, and an owner's own are complete there: a stored recording accounts for every trade between its first
and last quote of each security, or its cumulative volumes would refuse it.
"""
from __future__ import annotations

import datetime as dt
import io
import json
from typing import NamedTuple

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from briskapi import records
from briskapi._parquet import BATCHES, DEPTHS, EQUITY, FILES, MARKET, TRADES, midnight_ns
from briskapi.records import (BARS, DATASETS, DAY_NS, EARLIEST, FIELDS, JST_NS, LATEST, SCHEMAS, TIME,  # noqa: F401 - the
                              fields, instrument_id, symbols_of, timestamp)                         # service's names
from briskapi.schema import BROKER_SOURCE, RECORDING_PREFIX, REFERENCES, SCHEMA, require

LIVE = (BROKER_SOURCE,)   # every broker's BRiSK, one source (schema.BROKER_TRANSPORTS: the sites)
DEMO_SOURCE = 'historical_mock'
ENCODINGS = {'parquet': ('application/vnd.apache.parquet', 'parquet'), 'csv': ('text/csv', 'csv'),
             'json': ('application/x-ndjson', 'json')}
MAX_DAYS = 31
BARS_ROOT = 'bars'    # bars/OWNER/YYYYMMDD/ohlcv-*.parquet


class NotFound(Exception):
    pass


# The bucket

class Bucket:
    """The archive bucket through a boto3-style client: listing, reads (ranged for Parquet), writes."""

    BLOCK = 4 * 1024 * 1024

    def __init__(self, s3, name):
        self.s3, self.name = s3, name

    def keys(self, prefix):
        return [item['Key'] for page in self.s3.get_paginator('list_objects_v2').paginate(Bucket=self.name, Prefix=prefix)
                for item in page.get('Contents', [])]

    def read(self, key, start=None, end=None):
        args = {'Range': f'bytes={start}-{end - 1}'} if start is not None else {}
        try:
            obj = self.s3.get_object(Bucket=self.name, Key=key, **args)
        except Exception as error:  # noqa: BLE001 - only a missing key is NotFound
            if getattr(error, 'response', {}).get('Error', {}).get('Code') in {'NoSuchKey', '404'}:
                raise NotFound(key) from None
            raise
        with obj['Body'] as body:
            return body.read()

    def json(self, key):
        return json.loads(self.read(key))

    def exists(self, key):
        try:
            self.s3.head_object(Bucket=self.name, Key=key)
            return True
        except Exception as error:  # noqa: BLE001
            if getattr(error, 'response', {}).get('Error', {}).get('Code') in {'NoSuchKey', '404', 'NotFound'}:
                return False
            raise

    def parquet(self, key):
        """A Parquet file read in 4 MiB blocks on demand: a query reads only the row groups it needs."""
        try:
            size = self.s3.head_object(Bucket=self.name, Key=key)['ContentLength']
        except Exception as error:  # noqa: BLE001 - only a missing key is NotFound
            if getattr(error, 'response', {}).get('Error', {}).get('Code') in {'NoSuchKey', '404', 'NotFound'}:
                raise NotFound(key) from None
            raise
        return pq.ParquetFile(_Ranged(self, key, size))

    def write(self, key, data, content_type='application/octet-stream'):
        self.s3.put_object(Bucket=self.name, Key=key, Body=data, ContentType=content_type)

    def delete(self, key):
        self.s3.delete_object(Bucket=self.name, Key=key)


class _Ranged(io.RawIOBase):
    def __init__(self, bucket, key, size):
        self.bucket, self.key, self.size, self.at, self.blocks = bucket, key, size, 0, {}

    def readable(self):
        return True

    def seekable(self):
        return True

    def tell(self):
        return self.at

    def seek(self, offset, whence=0):
        self.at = {0: offset, 1: self.at + offset, 2: self.size + offset}[whence]
        return self.at

    def readinto(self, buffer):
        n = min(len(buffer), self.size - self.at)
        out, done = memoryview(buffer), 0
        while done < n:
            block, offset = divmod(self.at + done, Bucket.BLOCK)
            if block not in self.blocks:
                start = block * Bucket.BLOCK
                self.blocks[block] = self.bucket.read(self.key, start, min(start + Bucket.BLOCK, self.size))
            data = self.blocks[block][offset:offset + n - done]
            out[done:done + len(data)] = data
            done += len(data)
        self.at += n
        return n


# Recordings

class Recording(NamedTuple):
    prefix: str
    date: str          # YYYYMMDD, the trading date
    first: int         # UTC ns of the first and last source clock
    last: int
    codes: frozenset
    files: dict        # table name -> key
    receipt: bool      # whether the batches table (local receipt times) exists


def recordings(bucket, owner, dates=None):
    """An owner's live recordings (XTKS.BRISK), for the given trading dates (all when None)."""
    keys = bucket.keys(f'archive/{owner}/') if dates is None else \
        [key for date in dates for key in bucket.keys(f'archive/{owner}/{date}/')]
    found = []
    for key in sorted(keys):
        prefix = key.rsplit('/', 1)[0]
        if not key.endswith('/manifest.json') or not RECORDING_PREFIX.fullmatch(prefix):
            continue
        summary = bucket.json(key)['summary']
        if summary['source'] in LIVE:
            found.append(_recording(prefix, summary, bucket.json(f'{prefix}/files.json'), receipt=True))
    return found


def _recording(prefix, summary, files, receipt):
    midnight = midnight_ns(summary['trading_date'])
    return Recording(prefix, summary['trading_date'], midnight + summary['first_source_time_us'] * 1000,
                     midnight + summary['last_source_time_us'] * 1000, frozenset(summary['codes']), files, receipt)


def demo(bucket):
    """The public demo's market tables, as the archive keeps them once."""
    reference = REFERENCES[DEMO_SOURCE]
    # The archive keeps a copy in each table format (archive_service.reference_prefix): the newest it holds.
    base = f"reference/{DEMO_SOURCE}/{reference['timeline']}"
    prefix = next((p for p in (f'{base}/{SCHEMA}', base) if bucket.exists(f'{p}/complete.json')), None)
    if prefix is None:
        raise NotFound('The archive holds no copy of the demo yet')
    equity = pq.read_table(io.BytesIO(bucket.read(f'{prefix}/{EQUITY}')))
    meta = bucket.parquet(f'{prefix}/{DEPTHS}').metadata
    column = _leaf(meta, 'ts_event')
    stats = [meta.row_group(g).column(column).statistics for g in range(meta.num_row_groups)]
    midnight = midnight_ns(reference['trading_date'])
    first, last = min(_ns(s.min) for s in stats), max(_ns(s.max) for s in stats)
    summary = {'trading_date': reference['trading_date'], 'first_source_time_us': (first - midnight) // 1000,
               'last_source_time_us': (last - midnight) // 1000, 'codes': equity.column('raw_symbol').to_pylist()}
    return _recording(prefix, summary, {name: f'{prefix}/{name}' for name in MARKET}, receipt=False)


# Requests

def trading_dates(start, end):
    """The JST trading dates [start, end) touches."""
    first, last = (start + JST_NS) // DAY_NS, (end - 1 + JST_NS) // DAY_NS
    require(last - first < MAX_DAYS, f'At most {MAX_DAYS} days per request')
    return [(dt.date(1970, 1, 1) + dt.timedelta(days=day)).strftime('%Y%m%d') for day in range(first, last + 1)]


class Request(NamedTuple):
    dataset: str
    schema: str
    symbols: set | None
    start: int
    end: int
    limit: int | None
    snapshot: bool = False   # book-deltas: each security's books as of the start, first


def request(dataset, schema='trades', symbols=None, start=None, end=None, limit=None, stype_in='raw_symbol', snapshot=False,
            **_) -> Request:
    require(dataset in DATASETS, f'Unknown dataset {dataset!r}; one of {", ".join(DATASETS)}')
    require(schema in SCHEMAS, f'Unknown schema {schema!r}; one of {", ".join(SCHEMAS)}')
    require(stype_in in (None, '', 'raw_symbol'), 'Only stype_in=raw_symbol (JPX codes) is supported')
    require(type(snapshot) is bool and (not snapshot or schema == 'book-deltas'), 'snapshot is true or false, for book-deltas')
    begin = timestamp(start)
    finish = timestamp(end, default=(begin + JST_NS) // DAY_NS * DAY_NS - JST_NS + DAY_NS)
    require(EARLIEST <= begin < finish <= LATEST, 'end must be after start, both between 2000 and 2100')
    trading_dates(begin, finish)
    count = None if limit in (None, '', 0, '0') else int(limit)
    require(count is None or count > 0, 'limit must be positive')
    return Request(dataset, schema, symbols_of(symbols), begin, finish, count, snapshot)


# Reading a recording's tables

def _ids(bucket, rec, symbols):
    """{issue_id: code} for the requested codes in this recording's master."""
    equity = pq.read_table(io.BytesIO(bucket.read(rec.files[EQUITY])), columns=['raw_symbol', 'issue_id'])
    return {i: c for c, i in zip(equity.column('raw_symbol').to_pylist(), equity.column('issue_id').to_pylist())
            if symbols is None or c in symbols}


def _rows(bucket, rec, name, ids, start, end, columns=None):
    """A table's rows for these issues within [start, end): only row groups whose statistics can match."""
    source = bucket.parquet(rec.files[name])
    meta = source.metadata
    leaves = [_leaf(meta, 'issue_id'), _leaf(meta, 'ts_event')]
    low, high = (min(ids), max(ids)) if ids else (0, -1)
    groups = []
    for g in range(meta.num_row_groups):
        stats = {meta.row_group(g).column(c).path_in_schema: meta.row_group(g).column(c).statistics
                 for c in leaves if c is not None}
        issue = stats.get('issue_id')
        if issue is not None and issue.has_min_max and (issue.max < low or issue.min > high):
            continue
        ts = stats.get('ts_event')
        if ts is not None and ts.has_min_max and (_ns(ts.max) < start or _ns(ts.min) >= end):
            continue
        groups.append(g)
    if not groups or not ids:
        return source.schema_arrow.empty_table() if columns is None else \
            pa.schema([source.schema_arrow.field(c) for c in columns]).empty_table()
    table = source.read_row_groups(groups, columns=columns)
    ts = pc.cast(table.column('ts_event'), pa.int64())
    keep = pc.and_(pc.is_in(table.column('issue_id'), value_set=pa.array(sorted(ids), pa.uint64())),
                   pc.and_(pc.greater_equal(ts, start), pc.less(ts, end)))
    return table.filter(keep)


def _leaf(meta, name):
    """A top-level column's index among the file's leaf columns (nested book lists add leaves before it)."""
    if not meta.num_row_groups:
        return None
    group = meta.row_group(0)
    return next((c for c in range(group.num_columns) if group.column(c).path_in_schema == name), None)


def _ns(value):
    if isinstance(value, dt.datetime):
        return int(value.timestamp()) * 10**9 + value.microsecond * 1000
    return int(value)


def _receipts(bucket, rec, seq, ts_event):
    """Local receipt times (UTC ns) for rows of these batches; the source clock for a replay copy."""
    if not rec.receipt or rec.files.get(BATCHES) is None:
        return ts_event
    batches = pq.read_table(io.BytesIO(bucket.read(rec.files[BATCHES])), columns=['seq', 'received_unix_ms'])
    received = np.zeros(int(pc.max(batches.column('seq')).as_py() or 0) + 1, np.int64)
    received[batches.column('seq').to_numpy()] = batches.column('received_unix_ms').fill_null(0).to_numpy().astype(np.int64) * 10**6
    values = received[seq.to_numpy()]
    return pa.array(np.where(values > 0, values, pc.cast(ts_event, pa.int64()).to_numpy()), TIME)


_DELTA_COLUMNS = ['issue_id', 'seq', 'sequence', 'ts_event', 'identifier', 'changes', 'checkpoint']


def _deltas(bucket, rec, ids, req, publisher):
    """One recording's book-deltas: each issue's rows replayed from its last checkpoint before the start (or from its
    first quote), never the day before it; those within [start, end) are returned, and with `snapshot` its books as
    of the start first. A recording without book changes (the demo's, a broker's from before 1.9.0) has none."""
    source = bucket.parquet(rec.files[DEPTHS])
    meta = source.metadata
    if 'changes' not in source.schema_arrow.names or not ids or not meta.num_row_groups:
        return records.empty('book-deltas')
    leaves = {name: _leaf(meta, name) for name in ('issue_id', 'ts_event')}
    low, high = min(ids), max(ids)
    wanted = pa.array(sorted(ids), pa.uint64())
    groups = []   # the row groups holding the issues before the end, in order (rows run by issue, then batch)
    for g in range(meta.num_row_groups):
        issue, ts = (meta.row_group(g).column(leaves[k]).statistics for k in ('issue_id', 'ts_event'))
        if issue.has_min_max and (issue.max < low or issue.min > high):
            continue
        if ts.has_min_max and _ns(ts.min) >= req.end:
            continue
        groups.append((g, ts.has_min_max and _ns(ts.max) < req.start))
    # Each issue's anchor: its last checkpoint before the start, else its first row (both as (group, row)).
    anchors, first = {}, {}
    for g, _ in groups:
        light = source.read_row_group(g, columns=['issue_id', 'ts_event', 'checkpoint'])
        issues = light.column('issue_id').to_numpy()
        ts = pc.cast(light.column('ts_event'), pa.int64()).to_numpy()
        points = light.column('checkpoint').is_valid().to_numpy(zero_copy_only=False)
        for row in np.flatnonzero(pc.is_in(light.column('issue_id'), value_set=wanted).to_numpy(zero_copy_only=False)):
            issue = int(issues[row])
            first.setdefault(issue, (g, int(row)))
            if points[row] and ts[row] < req.start:
                anchors[issue] = (g, int(row))
    books, tables, flags, seeds = records.Books(), [], [], []
    for g, before in groups:
        if before and not any(anchors.get(i, first.get(i, (g + 1,)))[0] <= g for i in ids):
            continue   # wholly before every anchor
        table = source.read_row_group(g, columns=_DELTA_COLUMNS)
        issues, ts = table.column('issue_id').to_pylist(), pc.cast(table.column('ts_event'), pa.int64()).to_pylist()
        keep, is_first = [], []
        for row, issue in enumerate(issues):
            if issue not in ids or ts[row] >= req.end:
                continue
            at = anchors.get(issue)
            if (g, row) == at:   # the checkpoint: the books after it
                point = table.column('checkpoint')[row].as_py()
                books.seed(issue, table.column('identifier')[row].as_py().removesuffix('.XTKS'),
                           [[c['book'], round(c['price'] * 10), int(c['size']), c['count']] for c in point],
                           table.column('sequence')[row].as_py(), ts[row], ts[row])
                seeds.append((issue, table.column('seq')[row].as_py(), ts[row]))
            elif (g, row) > (at or first[issue]) or (at is None and (g, row) == first[issue]):
                keep.append(row)
                is_first.append(at is None and (g, row) == first[issue])
        if keep:
            tables.append(table.take(pa.array(keep, pa.int64())))
            flags += is_first
    if seeds:   # a checkpoint's update was received when its batch was
        received = _receipts(bucket, rec, pa.array([s for _, s, _ in seeds], pa.uint64()),
                             pa.array([e for *_, e in seeds], pa.int64()).cast(TIME))
        for (issue, _, _), recv in zip(seeds, pc.cast(received, pa.int64()).to_pylist()):
            frame, event, _ = books.last[issue]
            books.last[issue] = (frame, event, recv)
    rows = pa.concat_tables(tables) if tables else source.schema_arrow.empty_table().select(_DELTA_COLUMNS)
    ts_recv = _receipts(bucket, rec, rows.column('seq').combine_chunks(), rows.column('ts_event').combine_chunks()) \
        if rows.num_rows else None
    if not req.snapshot:
        return records.deltas(rows, publisher, books, ts_recv, start=req.start, first=flags)
    # The books as of the start, then what changed since.
    before = pc.less(pc.cast(rows.column('ts_event'), pa.int64()), req.start).to_numpy(zero_copy_only=False) \
        if rows.num_rows else np.zeros(0, bool)
    take = lambda mask: pa.array(np.flatnonzero(mask), pa.int64())   # noqa: E731
    flags = np.array(flags, bool)
    records.deltas(rows.take(take(before)), publisher, books, ts_recv and ts_recv.take(take(before)), first=list(flags[before]))
    return pa.concat_tables([records.book_snapshot(books, publisher, ids),
                             records.deltas(rows.take(take(~before)), publisher, books, ts_recv and ts_recv.take(take(~before)),
                                            first=list(flags[~before]))])


def _project(bucket, rec, req, publisher):
    """One recording's records of the requested (non-bar) schema."""
    ids = _ids(bucket, rec, req.symbols)
    if req.schema == 'book-deltas':
        return _deltas(bucket, rec, ids, req, publisher)
    if req.schema == 'definition':
        equity = pq.read_table(io.BytesIO(bucket.read(rec.files[EQUITY])))
        equity = equity.filter(pc.is_in(equity.column('issue_id'), value_set=pa.array(sorted(ids), pa.uint64())))
        midnight = midnight_ns(rec.date)
        if not (req.start <= midnight + DAY_NS - 1 and midnight < req.end):
            equity = equity.slice(0, 0)
        return records.definition(equity, midnight, publisher)
    table = _rows(bucket, rec, TRADES if req.schema == 'trades' else DEPTHS, ids, req.start, req.end)
    ts_recv = None
    if table.num_rows:
        ts_recv = _receipts(bucket, rec, table.column('seq').combine_chunks(), table.column('ts_event').combine_chunks())
    equity = pq.read_table(io.BytesIO(bucket.read(rec.files[EQUITY])), columns=['issue_id', 'max_price', 'min_price']) \
        if req.schema == 'imbalance' else None
    return records.project(req.schema, table, publisher, ts_recv, equity)


# Bars

def build_bars(trades: list[pa.Table], dedupe=None) -> dict[str, pa.Table]:
    """OHLCV bars per security from trades (code, trade_id, frame, ts_event, price, size), deduplicated by
    (code, trade_id) across captures (`dedupe`: whether there are several, by default as many tables as captures);
    intervals aligned to JST."""
    if not trades:
        return {schema: _empty_bars() for schema in BARS}
    table = pa.concat_tables([t.select(['symbol', 'trade_id', 'sequence', 'ts_event', 'price', 'size']) for t in trades])
    if len(trades) > 1 if dedupe is None else dedupe:
        table = _dedupe(table, ['symbol', 'trade_id'])
    place = pc.cast(pc.list_element(pc.split_pattern(table.column('trade_id'), '-'), 1), pa.int64())
    table = table.append_column('place', place).sort_by([('symbol', 'ascending'), ('ts_event', 'ascending'),
                                                          ('sequence', 'ascending'), ('place', 'ascending')])
    ts = pc.cast(table.column('ts_event'), pa.int64()).to_numpy()
    out = {}
    for schema, length in BARS.items():
        start = (ts + JST_NS) // length * length - JST_NS
        grouped = table.append_column('bar', pa.array(start, pa.int64())).group_by(['symbol', 'bar'], use_threads=False) \
            .aggregate([('price', 'first'), ('price', 'max'), ('price', 'min'), ('price', 'last'), ('size', 'sum')])
        bars = pa.table({'ts_event': pc.cast(grouped.column('bar'), TIME), 'symbol': grouped.column('symbol'),
                         'open': grouped.column('price_first'), 'high': grouped.column('price_max'),
                         'low': grouped.column('price_min'), 'close': grouped.column('price_last'),
                         'volume': pc.cast(grouped.column('size_sum'), pa.uint64())})
        out[schema] = bars.sort_by([('symbol', 'ascending'), ('ts_event', 'ascending')])
    return out


def _empty_bars():
    return pa.schema([('ts_event', TIME), ('symbol', pa.string()), ('open', pa.float64()), ('high', pa.float64()),
                      ('low', pa.float64()), ('close', pa.float64()), ('volume', pa.uint64())]).empty_table()


def _bar_trades(table):
    """Trades as build_bars takes them."""
    return pa.table({'symbol': records.codes(table), 'trade_id': table.column('trade_id'), 'sequence': table.column('frame'),
                     'ts_event': table.column('ts_event'), 'price': records.price(table.column('price')),
                     'size': pc.cast(table.column('size'), pa.uint64())})


# Trades gathered at once for bars: at some 150 bytes a trade (before it is sorted), a trading day's fifteen million would
# not fit in the function's memory.
BAR_ROWS = 1_500_000
_BAR_COLUMNS = ['identifier', 'trade_id', 'frame', 'ts_event', 'price', 'size', 'issue_id']


def day_bars(bucket, recs):
    """build_bars of these recordings' trades, a range of issues at a time: each recording's trades are sorted by issue, and a
    row group's footer says which it holds, so a range gathers about BAR_ROWS trades from all of them, and a security's
    trades from every recording are together for deduplication."""
    files = [pq.ParquetFile(io.BytesIO(bucket.read(r.files[TRADES]))) for r in recs]
    spans = []   # (lowest issue, highest, rows, file, row group)
    for i, source in enumerate(files):
        meta, at = source.metadata, _leaf(source.metadata, 'issue_id')
        for g in range(meta.num_row_groups):
            stats = meta.row_group(g).column(at).statistics if at is not None else None
            low, high = (stats.min, stats.max) if stats is not None and stats.has_min_max else (0, 2**64 - 1)
            spans.append((low, high, meta.row_group(g).num_rows, i, g))
    bounds, rows = [0], 0
    for low, count in sorted((s[0], s[2]) for s in spans):
        if rows >= BAR_ROWS and low > bounds[-1]:
            bounds.append(low)
            rows = 0
        rows += count
    out = {schema: [] for schema in BARS}
    for low, high in zip(bounds, [*bounds[1:], None]):
        tables = []
        for i, source in enumerate(files):
            groups = [g for first, last, _, f, g in spans if f == i and last >= low and (high is None or first < high)]
            if not groups:
                continue
            table = source.read_row_groups(groups, columns=_BAR_COLUMNS)
            issue = table.column('issue_id')
            keep = pc.greater_equal(issue, pa.scalar(low, pa.uint64()))
            if high is not None:
                keep = pc.and_(keep, pc.less(issue, pa.scalar(high, pa.uint64())))
            table = table.filter(keep)
            if table.num_rows:
                tables.append(_bar_trades(table))
        for schema, bars in build_bars(tables, dedupe=len(recs) > 1).items():
            out[schema].append(bars)
    return {schema: pa.concat_tables(parts).sort_by([('symbol', 'ascending'), ('ts_event', 'ascending')]) if parts
            else _empty_bars() for schema, parts in out.items()}


def bars_root(owner, date):
    return f'{BARS_ROOT}/{owner}/{date}'


def rebuild_bars(bucket, owner, date, attempts=3):
    """Build an owner's bars for a trading day from its current live recordings of that day, covered as `covered`
    says; with none left, remove them. Rebuilds of one owner's day are not serialized, so one that finds its
    recordings changed meanwhile (another upload, delete or restore) runs again."""
    for _ in range(attempts):
        before = bucket.keys(f'archive/{owner}/{date}/')
        built = _rebuild(bucket, owner, date, recordings(bucket, owner, [date]))
        if bucket.keys(f'archive/{owner}/{date}/') == before:
            return built
    return built


def _rebuild(bucket, owner, date, recs):
    root = bars_root(owner, date)
    if not recs:
        for key in bucket.keys(f'{root}/'):
            bucket.delete(key)
        return 0
    for schema, bars in day_bars(bucket, recs).items():
        out = io.BytesIO()
        # Sorted by symbol in 65,536-row groups: a query for a few securities reads only theirs.
        pq.write_table(covered(bars, BARS[schema], recs), out, compression='zstd', row_group_size=65_536)
        bucket.write(f'{root}/{schema}.parquet', out.getvalue(), 'application/vnd.apache.parquet')
    return len(recs)


def covered(bars, length, recs):
    """The bars an owner's recordings cover: those whose span, clipped to the trading sessions, lies within the
    owner's coverage of that security (the union of its recordings' clock ranges). A bar its recordings cover only
    in part would be missing trades."""
    if not bars.num_rows:
        return bars
    starts = pc.cast(bars.column('ts_event'), pa.int64()).to_numpy()
    pieces = records.session_pieces(starts, starts + length)
    symbols = bars.column('symbol')
    # Securities in the same recordings share their windows (usually every security is in every recording), so
    # each such group is one vectorized pass, not one per security.
    groups = {}
    for code in pc.unique(symbols).to_pylist():
        groups.setdefault(tuple(i for i, r in enumerate(recs) if code in r.codes), []).append(code)
    keep = np.zeros(bars.num_rows, bool)
    for members, codes in groups.items():
        windows = _merge([(recs[i].first, recs[i].last + 1) for i in members])
        if not windows:
            continue
        lows, highs = np.array([w[0] for w in windows]), np.array([w[1] for w in windows])
        rows = np.nonzero(pc.is_in(symbols, value_set=pa.array(codes)).to_numpy())[0]
        ok = np.ones(len(rows), bool)
        for low, high in pieces:
            low, high = low[rows], high[rows]
            where = np.searchsorted(lows, low, 'right') - 1
            ok &= (high <= low) | ((where >= 0) & (highs[np.maximum(where, 0)] >= high))
        keep[rows] = ok
    return bars.filter(pa.array(keep))


def _merge(windows):
    merged = []
    for low, high in sorted(windows):
        if merged and low <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], high)
        else:
            merged.append([low, high])
    return merged


def _dedupe(table, keys):
    joined = pc.binary_join_element_wise(*[pc.cast(table.column(k), pa.string()) for k in keys], '\x1f')
    first = pa.table({'k': joined, 'i': pa.array(np.arange(table.num_rows))}).group_by('k', use_threads=False) \
        .aggregate([('i', 'min')]).column('i_min')
    return table.take(pa.array(np.sort(first.to_numpy())))


# Queries

def _one_capture(recs):
    """The recordings book-deltas reads: a day's consecutive pieces, but of captures that overlap in time only the
    longest (its changes are each from its own previous update, so two captures' do not interleave)."""
    kept = []
    for rec in sorted(recs, key=lambda r: r.first - r.last):
        if not any(rec.date == k.date and rec.first < k.last and k.first < rec.last for k in kept):
            kept.append(rec)
    return sorted(kept, key=lambda r: r.first)


def get_range(bucket, owner, req: Request) -> pa.Table:
    """The records of a request, merged across recordings, deduplicated, time-ordered, cut to `limit`."""
    publisher = DATASETS[req.dataset][0]
    dates = trading_dates(req.start, req.end)
    if req.dataset == 'XTKS.BRISKDEMO':
        rec = demo(bucket)
        recs = [rec] if rec.date in dates else []
    else:
        recs = recordings(bucket, owner, dates)
    if req.schema in BARS:
        table = _bars(bucket, owner, req, recs, publisher)
    else:
        if req.schema == 'book-deltas':
            recs = _one_capture(recs)
        parts = [_project(bucket, rec, req, publisher) for rec in recs]
        table = pa.concat_tables(parts) if parts else records.empty(req.schema)
        if len(parts) > 1 and req.schema != 'book-deltas':
            key = {'trades': ['instrument_id', 'trade_id'], 'definition': ['instrument_id', 'ts_event'],
                   'imbalance': ['instrument_id', 'sequence', 'auction_type']}.get(req.schema, ['instrument_id', 'sequence'])
            # A record several captures hold keeps its EARLIEST receipt: the first moment a program on any of them could
            # have known it (a stable sort, so equal receipts keep the captures' order).
            table = _dedupe(table.sort_by('ts_recv') if 'ts_recv' in table.column_names else table, key)
        order = [('ts_event', 'ascending'), ('instrument_id', 'ascending')] + \
            ([('sequence', 'ascending')] if 'sequence' in table.column_names else [])
        table = table.sort_by(order)
    return table.slice(0, req.limit) if req.limit else table


def _bars(bucket, owner, req, recs, publisher):
    length = BARS[req.schema]
    parts = []
    for date in sorted({r.date for r in recs}):
        day = [r for r in recs if r.date == date]
        if req.dataset == 'XTKS.BRISKDEMO':
            bars = day_bars(bucket, day)[req.schema]
        else:
            try:
                bars = _built(bucket, f'{bars_root(owner, date)}/{req.schema}.parquet', req.symbols)
            except NotFound:  # not built (a rebuild failed): from the recordings now
                bars = covered(day_bars(bucket, day)[req.schema], length, day)
        ts = pc.cast(bars.column('ts_event'), pa.int64())
        keep = pc.and_(pc.greater_equal(ts, req.start), pc.less(ts, req.end))
        if req.symbols is not None:
            keep = pc.and_(keep, pc.is_in(bars.column('symbol'), value_set=pa.array(sorted(req.symbols))))
        parts.append(bars.filter(keep))
    bars = pa.concat_tables(parts) if parts else _empty_bars()
    n = bars.num_rows
    table = pa.table({'ts_event': bars.column('ts_event'), 'publisher_id': pa.array(np.full(n, publisher, np.uint16)),
                      'instrument_id': pa.array([instrument_id(c) for c in bars.column('symbol').to_pylist()], pa.uint32()),
                      'open': bars.column('open'), 'high': bars.column('high'), 'low': bars.column('low'),
                      'close': bars.column('close'), 'volume': bars.column('volume'), 'symbol': bars.column('symbol')})
    return table.sort_by([('ts_event', 'ascending'), ('instrument_id', 'ascending')])


def _built(bucket, key, symbols):
    """A day's built bars, only the row groups whose symbol range can hold a requested one."""
    source = bucket.parquet(key)
    if symbols is None:
        return source.read()
    meta, at = source.metadata, _leaf(source.metadata, 'symbol')
    groups = []
    for g in range(meta.num_row_groups):
        stats = meta.row_group(g).column(at).statistics
        if stats is None or not stats.has_min_max or any(stats.min <= s <= stats.max for s in symbols):
            groups.append(g)
    return source.read_row_groups(groups) if groups else source.schema_arrow.empty_table()


def encode(table, encoding='parquet', compression=None, pretty_ts=True, pretty_px=True, map_symbols=True):
    """(body, content type, file extension) of a result in one of our encodings."""
    require(encoding in ENCODINGS, f'Unknown encoding {encoding!r}; one of {", ".join(ENCODINGS)}')
    require(compression in (None, '', 'none', 'zstd'), 'compression is none or zstd')
    if not map_symbols and 'symbol' in table.column_names:
        table = table.drop_columns(['symbol'])
    content, extension = ENCODINGS[encoding]
    out = io.BytesIO()
    if encoding == 'parquet':
        pq.write_table(table, out, compression='zstd')
        return out.getvalue(), content, extension
    columns = {}
    for name in table.column_names:
        column, kind = table.column(name), table.schema.field(name).type
        if pa.types.is_timestamp(kind):
            column = pc.strftime(column, format='%Y-%m-%dT%H:%M:%SZ') if pretty_ts else pc.cast(column, pa.int64())
        elif pa.types.is_floating(kind) and not pretty_px and name not in {'replay_lateness_ms'}:
            column = pc.cast(pc.round(pc.multiply(column, 10**9)), pa.int64())
        elif pa.types.is_nested(kind):
            column = pa.array([None if v is None else json.dumps(v) for v in column.to_pylist()], pa.string()) \
                if encoding == 'csv' else column
        columns[name] = column
    table = pa.table(columns)
    if encoding == 'csv':
        pacsv.write_csv(table, out)
    else:
        for row in table.to_pylist():
            out.write((json.dumps(row, default=str) + '\n').encode())
    if compression == 'zstd':
        return pa.Codec('zstd').compress(out.getvalue(), asbytes=True), 'application/zstd', extension + '.zst'
    return out.getvalue(), content, extension


# Metadata

def dataset_dates(bucket, owner, dataset):
    """The trading dates with data, and each one's covered clock windows (JST HH:MM:SS)."""
    recs = [demo(bucket)] if dataset == 'XTKS.BRISKDEMO' else recordings(bucket, owner)
    days = {}
    for rec in recs:
        days.setdefault(rec.date, []).append((rec.first, rec.last))
    clock = lambda ns, date: (dt.datetime.fromtimestamp((ns - midnight_ns(date)) / 10**9, dt.timezone.utc)  # noqa: E731
                              .strftime('%H:%M:%S'))
    return {date: [[clock(low, date), clock(high, date)] for low, high in _merge(windows)] for date, windows in sorted(days.items())}
