"""Databento-style records from BRiSK's tables: the schemas the cloud API returns and briskapi.Live streams.

Shared by the cloud API's engine (query_engine.py, over stored recordings) and briskapi.Live (over the
feed running on this computer), so a record is the same whether it comes from history or live. Each
projection takes a recording's tables as briskapi._parquet writes them (order_book_depths, trades and
equity) and returns exactly the columns FIELDS lists. OHLCV bars are built from trades where they are
served: in the cloud from an owner's stored recordings, and by briskapi.Live from the stream; never by
Historical from files on your computer.
"""
from __future__ import annotations

import datetime as dt
import re

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from briskapi._parquet import DEPTH_SCHEMA, midnight_ns
from briskapi.schema import require

JST_NS = 9 * 3600 * 10**9
DAY_NS = 86400 * 10**9
# Datasets follow Databento's VENUE.FEED naming; the number is each one's publisher_id.
DATASETS = {'XTKS.BRISK': (1, 'Tokyo Stock Exchange via BRiSK'),
            'XTKS.BRISKDEMO': (2, 'Tokyo Stock Exchange, BRiSK Next demo replay')}
BARS = {'ohlcv-1s': 10**9, 'ohlcv-1m': 60 * 10**9, 'ohlcv-1h': 3600 * 10**9, 'ohlcv-1d': DAY_NS}
SCHEMAS = ('trades', 'mbp-10', 'book-deltas', 'definition', 'imbalance', 'brisk-quotes', *BARS)
# book-deltas' flags, Databento's: the last record of an update, and a snapshot's.
F_LAST, F_SNAPSHOT = 128, 32
# The Tokyo Stock Exchange's trading sessions since 2024-11-05, as [start, end) seconds after JST midnight;
# the end second holds the closing auction's prints.
SESSIONS = ((9 * 3600, 11 * 3600 + 30 * 60 + 1), (12 * 3600 + 30 * 60, 15 * 3600 + 30 * 60 + 1))
MAX_SYMBOLS = 2000
# The times a request may name, as UTC nanoseconds: 2000 to 2100.
EARLIEST, LATEST = 946_684_800 * 10**9, 4_102_444_800 * 10**9
LEVELS = 10
TIME = pa.timestamp('ns', tz='UTC')
BOOK = pa.list_(pa.field('item', pa.struct([('price', pa.float64()), ('size', pa.uint64()), ('count', pa.uint32())]), False))


# Requests

def timestamp(value, default=None) -> int:
    """UTC nanoseconds from an integer (ns), a date (the JST trading day's midnight) or an ISO 8601 time
    (JST unless it says otherwise)."""
    if value is None or value == '':
        require(default is not None, 'start is required')
        return default
    if isinstance(value, int) or re.fullmatch(r'\d{10,}', str(value)):
        return int(value)
    text = str(value).strip().replace('Z', '+00:00')
    if re.fullmatch(r'\d{4}-\d{2}-\d{2}', text):
        return midnight_ns(text.replace('-', ''))
    try:
        moment = dt.datetime.fromisoformat(text)
    except ValueError:
        raise ValueError(f'Invalid time {value!r}: use ISO 8601, a date or UTC nanoseconds') from None
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=dt.timezone(dt.timedelta(hours=9)))
    try:
        return int(moment.timestamp()) * 10**9 + moment.microsecond * 1000
    except OverflowError:
        raise ValueError(f'Invalid time {value!r}: out of range') from None


def symbols_of(symbols):
    """The requested JPX codes as a set, or None for ALL_SYMBOLS."""
    if symbols in (None, '', 'ALL_SYMBOLS') or symbols == ['ALL_SYMBOLS']:
        return None
    codes = [str(s).strip() for s in (symbols.split(',') if isinstance(symbols, str) else symbols) if str(s).strip()]
    require(codes and len(codes) <= MAX_SYMBOLS, f'Give 1 to {MAX_SYMBOLS} symbols, or ALL_SYMBOLS')
    for code in codes:
        require(re.fullmatch(r'[0-9A-Z]{4,5}', code), f'Invalid symbol {code!r}')
    return set(codes)


def instrument_id(code: str) -> int:
    """A stable uint32 per JPX code: digits-only codes are their number, others 100,000 plus their base-36 value."""
    return int(code) if code.isdigit() else 100_000 + int(code, 36)


def session_pieces(begin, end):
    """[begin, end) (numpy arrays of UTC ns) clipped to each trading session of its JST day; a span outside the
    sessions keeps its whole self. A bar is covered when every non-empty piece is."""
    day = (begin + JST_NS) // DAY_NS * DAY_NS - JST_NS
    pieces = [(np.maximum(begin, day + low * 10**9), np.minimum(end, day + high * 10**9)) for low, high in SESSIONS]
    # A bar longer than a day (none) or outside every session is judged on its whole span.
    outside = np.all([high <= low for low, high in pieces], axis=0)
    return [(np.where(outside, begin, low), np.where(outside, end, high)) for low, high in pieces]


# Projections

def codes(table):
    """The JPX codes of rows carrying v2's identifier (CODE.XTKS)."""
    return pc.replace_substring_regex(table.column('identifier'), r'\.XTKS$', '')


def common(table, publisher, ts_recv=None):
    """The columns every record starts with, and the rows' codes. `ts_recv` defaults to BRiSK's stamp."""
    symbols = codes(table)
    ids = pa.array([instrument_id(c) for c in symbols.to_pylist()], pa.uint32())
    ts_event = table.column('ts_event').combine_chunks() if table.num_rows else pa.array([], TIME)
    ts_recv = ts_event if ts_recv is None or not table.num_rows else ts_recv
    return {'ts_recv': pc.cast(ts_recv, TIME), 'ts_event': pc.cast(ts_event, TIME),
            'publisher_id': pa.array(np.full(table.num_rows, publisher, np.uint16)), 'instrument_id': ids}, symbols


def price(column):
    return pc.cast(column, pa.float64())


def flat_levels(books, name):
    """bid/ask levels 00-09 (price, size, count) from a list<struct> column, null past a side's last level."""
    books = books.combine_chunks() if isinstance(books, pa.ChunkedArray) else books
    lengths = pc.list_value_length(books).fill_null(0).to_numpy()
    offsets = books.offsets.to_numpy()[:-1]
    values = books.values
    out = {}
    for level in range(LEVELS):
        valid = lengths > level
        index = pa.array(np.where(valid, offsets + level, 0), pa.int64())
        mask = pa.array(~valid)
        for field, key, kind in (('price', 'px', pa.float64()), ('size', 'sz', pa.uint64()), ('count', 'ct', pa.uint32())):
            column = values.field(field) if len(values) else pa.array([], values.type.field(field).type)
            taken = column.take(index) if len(values) else pa.nulls(len(index), column.type)
            taken = pc.cast(taken, kind)
            out[f'{name}_{key}_{level:02d}'] = pc.if_else(mask, pa.scalar(None, kind), taken)
    return out


def book(column):
    """Book levels as {price (yen), size (shares), count (orders)}, best first."""
    books = column.combine_chunks() if isinstance(column, pa.ChunkedArray) else column
    if not len(books):
        return pa.array([], BOOK)
    values = books.values
    levels = pa.StructArray.from_arrays([price(values.field('price')), pc.cast(values.field('size'), pa.uint64()),
                                         values.field('count')], names=['price', 'size', 'count'])
    return pa.ListArray.from_arrays(books.offsets, levels, type=BOOK)


def project(schema, table, publisher, ts_recv=None, equity=None):
    """Records of a non-bar schema but definition from rows of the trades table (`trades`) or the depths table
    (the others); imbalance also needs the equity table, for each security's price limits."""
    head, symbols = common(table, publisher, ts_recv)
    n = table.num_rows
    if schema == 'trades':
        side = pc.cast(table.column('aggressor_side'), pa.string()) if n else pa.array([], pa.string())
        return pa.table({**head, 'action': pa.array(['T'] * n, pa.string()),
                         'side': pc.if_else(pc.equal(side, 'BUY'), 'B', pc.if_else(pc.equal(side, 'SELL'), 'A', 'N'))
                         if n else pa.array([], pa.string()),
                         'depth': pa.array(np.zeros(n, np.uint8)), 'price': price(table.column('price')),
                         'size': pc.cast(table.column('size'), pa.uint64()), 'flags': pa.array(np.zeros(n, np.uint8)),
                         'sequence': table.column('frame'), 'trade_id': table.column('trade_id'), 'symbol': symbols})
    if schema == 'mbp-10':
        # BRiSK gives each update's book, not the order event that changed it: action, side, depth, price
        # and size are left empty ('N'), and the session's first rows carry Databento's snapshot flag (32).
        flags = pc.if_else(pc.equal(table.column('seq'), 0), pa.scalar(32, pa.uint8()), pa.scalar(0, pa.uint8())) \
            if n else pa.array([], pa.uint8())
        return pa.table({**head, 'action': pa.array(['N'] * n, pa.string()), 'side': pa.array(['N'] * n, pa.string()),
                         'depth': pa.array(np.zeros(n, np.uint8)), 'price': pa.nulls(n, pa.float64()),
                         'size': pa.array(np.zeros(n, np.uint64)), 'flags': flags, 'sequence': table.column('sequence'),
                         **flat_levels(table.column('bids'), 'bid'), **flat_levels(table.column('asks'), 'ask'),
                         'symbol': symbols})
    if schema == 'imbalance':
        return _imbalance(table, head, symbols, equity)
    require(schema == 'brisk-quotes', f'No projection for {schema!r}')
    # Everything BRiSK records for each update (its books ten levels deep: book-deltas has them whole).
    drop = {'identifier', 'ts_event', 'ts_init', 'issue_id', 'seq', 'changes', 'checkpoint'}
    rest = {}
    for name in table.column_names:
        if name in drop:
            continue
        kind = table.schema.field(name).type
        rest[name] = book(table.column(name)) if pa.types.is_list(kind) else \
            price(table.column(name)) if pa.types.is_decimal(kind) else table.column(name)
    return pa.table({**head, **rest, 'symbol': symbols})


class Books:
    """Each security's whole books (bid, ask, and the closing-condition orders' two) as book-deltas replays them:
    {issue_id: four dicts of price10 to (quantity, orders)}, its code, and the frame, clock and receipt of the update
    that left them so. Live keeps one from the session's start; the cloud API seeds one from each security's last
    checkpoint (or its first quote) before the request's start."""

    def __init__(self):
        self.books, self.codes, self.last = {}, {}, {}

    def seed(self, issue, code, checkpoint, frame, ts_event, ts_recv):
        """A security's books are a checkpoint's ([book, price10, quantity, orders] rows), as of that update."""
        books = [{} for _ in range(4)]
        for book, price, quantity, orders in checkpoint:
            books[book][price] = (quantity, orders)
        self.books[issue], self.codes[issue], self.last[issue] = books, code, (frame, ts_event, ts_recv)

    def levels(self, issue):
        """A security's books as [book, price10, quantity, orders] rows, each book's best first."""
        books = self.books[issue]
        return [[k, price, *books[k][price]] for k in range(4) for price in sorted(books[k], reverse=k % 2 == 0)]


def _levels(changes):
    """Each row's book changes as [book, price10, quantity, orders] lists, from a depths table's `changes`."""
    changes = changes.combine_chunks() if isinstance(changes, pa.ChunkedArray) else changes
    values = changes.values
    flat = np.stack([values.field('book').to_numpy(zero_copy_only=False).astype(np.int64),
                     np.round(pc.cast(values.field('price'), pa.float64()).to_numpy(zero_copy_only=False) * 10).astype(np.int64),
                     pc.cast(values.field('size'), pa.int64()).to_numpy(zero_copy_only=False),
                     values.field('count').to_numpy(zero_copy_only=False).astype(np.int64)], axis=1).tolist() if len(values) else []
    offsets = changes.offsets.to_numpy().tolist()
    return [flat[offsets[i]:offsets[i + 1]] for i in range(len(changes))]


class _Out:
    """book-deltas records being made, column by column."""

    def __init__(self):
        self.columns = {name: [] for name, _, _ in FIELDS['book-deltas'] if name != 'publisher_id'}

    def add(self, code, frame, ts_event, ts_recv, action, book, price, size, count, flags):
        c = self.columns
        c['ts_recv'].append(ts_recv); c['ts_event'].append(ts_event); c['instrument_id'].append(instrument_id(code))
        c['action'].append(action); c['side'].append('N' if book is None else 'B' if book % 2 == 0 else 'A')
        c['closing'].append(book is not None and book >= 2); c['price'].append(None if price is None else price / 10)
        c['size'].append(size); c['count'].append(count); c['flags'].append(flags); c['sequence'].append(frame)
        c['symbol'].append(code)

    def whole(self, code, frame, ts_event, ts_recv, levels):
        """A clear and the books' levels, flagged as a snapshot's."""
        self.add(code, frame, ts_event, ts_recv, 'R', None, None, 0, 0, F_SNAPSHOT | (0 if levels else F_LAST))
        for i, (book, price, quantity, orders) in enumerate(levels):
            self.add(code, frame, ts_event, ts_recv, 'A', book, price, quantity, orders, F_SNAPSHOT | (F_LAST if i == len(levels) - 1 else 0))

    def table(self, publisher):
        c, n = self.columns, len(self.columns['action'])
        kinds = {name: kind for name, kind, _ in FIELDS['book-deltas']}
        return pa.table({name: (pa.array(np.full(n, publisher, np.uint16)) if name == 'publisher_id' else
                                pa.array(c[name], pa.int64()).cast(TIME) if kinds[name] == TIME else pa.array(c[name], kinds[name]))
                         for name in kinds})


def deltas(table, publisher, books, ts_recv=None, start=None, first=None):
    """book-deltas records from rows of a depths table that has book changes, by issue then batch, replayed onto
    `books` (a Books, updated): each change is an add (a level the book lacked), a modify or a cancel (the level
    emptied), an update's last flagged F_LAST. A security's first quote (`first`: whether each row is one; None: those
    of issues `books` has not seen) clears its books and adds them whole, flagged F_SNAPSHOT. Rows before `start`
    (UTC ns) are replayed but give no record."""
    n, out = table.num_rows, _Out()
    head, symbols = common(table, publisher, ts_recv)
    issues, frames, codes_ = table.column('issue_id').to_pylist(), table.column('sequence').to_pylist(), symbols.to_pylist()
    events, receipts = pc.cast(head['ts_event'], pa.int64()).to_pylist(), pc.cast(head['ts_recv'], pa.int64()).to_pylist()
    changes = _levels(table.column('changes')) if n else []
    for row in range(n):
        issue, code, when = issues[row], codes_[row], (frames[row], events[row], receipts[row])
        live = start is None or events[row] >= start
        is_first = (issue not in books.books) if first is None else first[row]
        if is_first:
            books.books[issue] = [{} for _ in range(4)]
            if live:
                out.whole(code, *when, changes[row])
        state, done = books.books[issue], len(changes[row]) - 1
        for i, (book, price, quantity, orders) in enumerate(changes[row]):
            had = price in state[book]
            if quantity:
                state[book][price] = (quantity, orders)
            else:
                state[book].pop(price, None)
            if live and not is_first:
                out.add(code, *when, 'C' if not quantity else 'M' if had else 'A', book, price, quantity, orders,
                        F_LAST if i == done else 0)
        books.codes[issue], books.last[issue] = code, when
    return out.table(publisher)


def book_snapshot(books, publisher, issues=None):
    """book-deltas records of the securities' books as `books` holds them (those of `issues`, if given): each a clear and
    its levels flagged F_SNAPSHOT, at the update that left them so, in Historical's order."""
    out = _Out()
    chosen = sorted(books.books if issues is None else set(issues) & books.books.keys(),
                    key=lambda i: (books.last[i][1], instrument_id(books.codes[i]), books.last[i][0]))
    for issue in chosen:
        out.whole(books.codes[issue], *books.last[issue], books.levels(issue))
    return out.table(publisher)


def _imbalance(table, head, symbols, equity):
    """Each update's opening (O) and closing (C) auction state, where BRiSK shows an indicative price. Without the equity
    table (before a trading day has begun, a gateway answers latest() with no rows) the collars are null."""
    ids = equity.column('issue_id').to_pylist() if equity is not None else []
    upper = dict(zip(ids, pc.cast(equity.column('max_price'), pa.float64()).to_pylist())) if ids else {}
    lower = dict(zip(ids, pc.cast(equity.column('min_price'), pa.float64()).to_pylist())) if ids else {}
    issue = table.column('issue_id').to_pylist()
    parts = []
    for auction, prefix, side in (('O', '', ''), ('C', 'closing_', '_closing')):
        indicative = table.column(f'{prefix}indicative_price')
        keep = pc.is_valid(indicative)
        buy = pc.cast(table.column(f'{prefix}market_buy_quantity'), pa.int64())
        sell = pc.cast(table.column(f'{prefix}market_sell_quantity'), pa.int64())
        imbalance = pc.subtract(buy, sell)
        n = table.num_rows
        part = pa.table({**head, 'auction_type': pa.array([auction] * n, pa.string()),
                         'ref_price': price(table.column('reference_price')), 'ind_match_price': price(indicative),
                         'paired_qty': table.column(f'{prefix}indicative_volume'),
                         'side': pc.if_else(pc.equal(table.column(f'{prefix}indicative_imbalance_side'), 1), 'A', 'B')
                         if n else pa.array([], pa.string()),
                         'market_buy_qty': table.column(f'{prefix}market_buy_quantity'),
                         'market_sell_qty': table.column(f'{prefix}market_sell_quantity'),
                         'market_imbalance_qty': pc.cast(pc.abs(imbalance), pa.uint64()),
                         'market_imbalance_side': pc.if_else(pc.greater(imbalance, 0), 'B',
                                                             pc.if_else(pc.less(imbalance, 0), 'A', 'N')) if n else pa.array([], pa.string()),
                         'bid_total_qty': table.column(f'bid{side}_total_quantity'),
                         'ask_total_qty': table.column(f'ask{side}_total_quantity'),
                         'upper_collar': pa.array([upper.get(i) for i in issue], pa.float64()),
                         'lower_collar': pa.array([lower.get(i) for i in issue], pa.float64()),
                         'auction_time': table.column('special_quote_time'), 'sequence': table.column('sequence'),
                         'symbol': symbols})
        parts.append(part.filter(keep))
    return pa.concat_tables(parts)


def definition(equity, midnight, publisher):
    """One definition per row of the equity table, stamped with the trading date's JST midnight (UTC ns)."""
    n = equity.num_rows
    symbols = equity.column('raw_symbol')
    number = lambda name: pc.cast(equity.column(name), pa.float64())  # noqa: E731
    return pa.table({'ts_recv': pa.array(np.full(n, midnight), TIME), 'ts_event': pa.array(np.full(n, midnight), TIME),
                     'publisher_id': pa.array(np.full(n, publisher, np.uint16)),
                     'instrument_id': pa.array([instrument_id(c) for c in symbols.to_pylist()], pa.uint32()),
                     'raw_symbol': symbols, 'exchange': pa.array(['XTKS'] * n, pa.string()),
                     'currency': pa.array(['JPY'] * n, pa.string()),
                     'min_lot_size_round_lot': pc.cast(pc.cast(equity.column('lot_size'), pa.float64()), pa.int64()),
                     'min_price_increment': number('price_increment'), 'high_limit_price': number('max_price'),
                     'low_limit_price': number('min_price'), 'base_price': price(equity.column('base_price')),
                     'name': equity.column('name'), 'issue_type': equity.column('issue_type'),
                     'tick_type': equity.column('tick_type'), 'symbol': symbols})


def empty(schema):
    return pa.schema([(name, kind) for name, kind, _ in FIELDS[schema]]).empty_table()


def fields(schema):
    """[{name, type, description}] of a schema's records, as returned (docs/data-format.md explains them)."""
    require(schema in SCHEMAS, f'Unknown schema {schema!r}')
    return [{'name': name, 'type': str(kind), 'description': text} for name, kind, text in FIELDS[schema]]


# Every schema's columns, in order: (name, Arrow type, meaning). Queries and live sessions return exactly these.
_HEAD = [('ts_recv', TIME, "When the uploader's computer received the frame (millisecond precision; the demo: ts_event)"),
         ('ts_event', TIME, "BRiSK's server stamp for the frame (microseconds); not established to be the exchange's time"),
         ('publisher_id', pa.uint16(), '1 for XTKS.BRISK, 2 for XTKS.BRISKDEMO (metadata.list_publishers)'),
         ('instrument_id', pa.uint32(), 'Fixed per code: digits-only codes are their number, others 100,000 plus base 36')]
_SYMBOL = [('symbol', pa.string(), 'The JPX code (left out with map_symbols=false)')]
_QUOTE_TEXT = {
    'bids': 'The ten best bid levels, best first: {price (yen), size (shares), count (orders)}',
    'asks': 'The ten best ask levels, best first: {price, size, count}',
    'flags': '16, plus 32 on a session\'s first update of each security (the snapshot)',
    'sequence': "BRiSK's frame number",
    'last_price': 'Last traded price (yen); null when BRiSK shows none',
    'open_price': "The session's opening price (yen)",
    'bid_price': 'Best bid (yen)', 'ask_price': 'Best ask (yen)',
    'indicative_price': 'Indicative matching price of the auction (yen), excluding closing-condition orders',
    'closing_indicative_price': "The closing-condition orders' indicative price (yen)",
    'indicative_open_price': 'Indicative opening price (yen)',
    'reference_price': 'Where indicative matching starts: the special quote price, else the last, else the base price (yen)',
    'max_frame': "The latest frame BRiSK had seen for this update",
    'indicative_volume': 'Shares that would match at the indicative price',
    'indicative_imbalance_side': '1 when sell exceeds buy at the indicative price (or nothing matches), else 0',
    'closing_indicative_volume': 'The same for the closing-condition orders',
    'closing_indicative_imbalance_side': 'The same for the closing-condition orders',
    'quote_flag': 'Special-quote flags (raw BRiSK value)', 'quote_side': 'Special-quote side (raw: 0 buy, 1 sell)',
    'volume': "The day's cumulative volume (shares)",
    'issue_status': "The broker's issue status (raw BRiSK value)",
    'market_buy_quantity': 'Market buy orders (shares)', 'market_sell_quantity': 'Market sell orders (shares)',
    'closing_market_buy_quantity': 'Closing-condition market buy orders (shares)',
    'closing_market_sell_quantity': 'Closing-condition market sell orders (shares)',
    'bid_quantity': 'Shares at the best bid', 'bid_orders': 'Orders at the best bid',
    'ask_quantity': 'Shares at the best ask', 'ask_orders': 'Orders at the best ask',
    'bid_total_quantity': 'Shares bid on the whole board', 'bid_total_orders': 'Bid orders on the whole board',
    'bid_closing_total_quantity': 'Closing-condition shares bid', 'bid_closing_total_orders': 'Closing-condition bid orders',
    'ask_total_quantity': 'Shares offered on the whole board', 'ask_total_orders': 'Ask orders on the whole board',
    'ask_closing_total_quantity': 'Closing-condition shares offered', 'ask_closing_total_orders': 'Closing-condition ask orders',
    'special_quote_time': "When the special quote's timer ends; null without one",
    'turnover': "The day's cumulative turnover (yen)",
    'closing_bids': "The closing-condition orders' bid levels, like bids", 'closing_asks': "The closing-condition orders' ask levels",
}


def _quote_fields():
    out = []
    for field in DEPTH_SCHEMA:
        if field.name in {'identifier', 'ts_event', 'ts_init', 'issue_id', 'seq'}:
            continue
        kind = BOOK if pa.types.is_list(field.type) else pa.float64() if pa.types.is_decimal(field.type) else field.type
        out.append((field.name, kind, _QUOTE_TEXT[field.name]))
    return out


_BAR = [('ts_event', TIME, "The bar's start (JST-aligned; a day bar starts at the trading date's JST midnight)"),
        *_HEAD[2:], ('open', pa.float64(), 'First trade price (yen)'), ('high', pa.float64(), 'Highest (yen)'),
        ('low', pa.float64(), 'Lowest (yen)'), ('close', pa.float64(), 'Last trade price (yen)'),
        ('volume', pa.uint64(), 'Shares traded'), *_SYMBOL]
FIELDS = {
    'trades': [*_HEAD, ('action', pa.string(), "'T'"),
               ('side', pa.string(), "The aggressor: 'B' buyer-initiated, 'A' seller-initiated, 'N' none (most opening prints)"),
               ('depth', pa.uint8(), '0'), ('price', pa.float64(), 'Trade price (yen)'), ('size', pa.uint64(), 'Shares'),
               ('flags', pa.uint8(), '0'), ('sequence', pa.uint64(), "BRiSK's frame number"),
               ('trade_id', pa.string(), "FRAME-PLACE: the frame and the trade's place in it; unique per instrument"),
               *_SYMBOL],
    'mbp-10': [*_HEAD, ('action', pa.string(), "'N': BRiSK sends books, not the order events that changed them"),
               ('side', pa.string(), "'N'"), ('depth', pa.uint8(), '0'), ('price', pa.float64(), 'null'),
               ('size', pa.uint64(), '0'), ('flags', pa.uint8(), "32 (snapshot) on a session's first update of each security, else 0"),
               ('sequence', pa.uint64(), "BRiSK's frame number"),
               *[(f'{side}_{key}_{level:02d}', kind, text.format(side=side, level=level))
                 for side in ('bid', 'ask') for level in range(LEVELS)
                 for key, kind, text in (('px', pa.float64(), '{side} level {level} price (yen; 00 is the best); null past the last level'),
                                         ('sz', pa.uint64(), '{side} level {level} shares'),
                                         ('ct', pa.uint32(), '{side} level {level} orders'))],
               *_SYMBOL],
    'definition': [('ts_recv', TIME, "The trading date's JST midnight"), ('ts_event', TIME, "The trading date's JST midnight"),
                   *_HEAD[2:], ('raw_symbol', pa.string(), 'The JPX code'), ('exchange', pa.string(), "'XTKS'"),
                   ('currency', pa.string(), "'JPY'"), ('min_lot_size_round_lot', pa.int64(), 'Shares per trading unit'),
                   ('min_price_increment', pa.float64(), "BRiSK's price resolution (0.1 yen), not the tick size, which depends on price and tick_type"),
                   ('high_limit_price', pa.float64(), "The day's upper price limit (yen)"),
                   ('low_limit_price', pa.float64(), "The day's lower price limit (yen)"),
                   ('base_price', pa.float64(), "The day's base price (yen)"), ('name', pa.string(), "The security's name"),
                   ('issue_type', pa.uint64(), 'Raw BRiSK issue type'), ('tick_type', pa.uint64(), 'Raw BRiSK tick table'),
                   *_SYMBOL],
    'imbalance': [*_HEAD, ('auction_type', pa.string(), "'O' the session's auction, 'C' the closing-condition orders'"),
                  ('ref_price', pa.float64(), 'Where indicative matching starts (yen; see reference_price)'),
                  ('ind_match_price', pa.float64(), 'Indicative matching price (yen)'),
                  ('paired_qty', pa.uint64(), 'Shares that would match there'),
                  ('side', pa.string(), "'A' when sell exceeds buy at that price (or nothing matches), else 'B'"),
                  ('market_buy_qty', pa.uint64(), 'Market buy orders (shares)'),
                  ('market_sell_qty', pa.uint64(), 'Market sell orders (shares)'),
                  ('market_imbalance_qty', pa.uint64(), 'The difference between market buy and market sell orders, without sign (shares)'),
                  ('market_imbalance_side', pa.string(), "'B' more market buying, 'A' more selling, 'N' equal"),
                  ('bid_total_qty', pa.uint64(), 'Shares bid on the whole board'),
                  ('ask_total_qty', pa.uint64(), 'Shares offered on the whole board'),
                  ('upper_collar', pa.float64(), "The day's upper price limit (yen)"),
                  ('lower_collar', pa.float64(), "The day's lower price limit (yen)"),
                  ('auction_time', TIME, "When the special quote's timer ends; null without one"),
                  ('sequence', pa.uint64(), "BRiSK's frame number"), *_SYMBOL],
    'book-deltas': [*_HEAD, ('action', pa.string(), "'A' a price level appears, 'M' its shares or orders change, 'C' it empties, "
                                                   "'R' the security's books are cleared (a snapshot's levels follow)"),
                    ('side', pa.string(), "'B' a bid level, 'A' an ask level, 'N' for 'R'"),
                    ('closing', pa.bool_(), "The closing-condition orders' book, not the regular one"),
                    ('price', pa.float64(), "The level's price (yen); null for 'R'"),
                    ('size', pa.uint64(), 'Shares at the level after this event (0 for C)'),
                    ('count', pa.uint32(), 'Orders at the level after this event (0 for C)'),
                    ('flags', pa.uint8(), "128 on an update's last record; 32 on a snapshot's (a security's first update, "
                                         "or the books as of the start with snapshot=true)"),
                    ('sequence', pa.uint64(), "BRiSK's frame number"), *_SYMBOL],
    'brisk-quotes': [*_HEAD, *_quote_fields(), *_SYMBOL],
    **{schema: _BAR for schema in BARS},
}
