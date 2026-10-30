"""Bounded validation of decoded batches, shared by local packaging and automatic ingest.

The archive accepts recordings with exact market/timing fields, checked batch by batch
in their canonical JSON form whatever the file format (see briskapi/_parquet.py).
Demo and synthetic market content must match a pinned reference replay. Opted-in
broker recordings are checked for their structure, a row group at a time (briskapi._parquet.verify_broker),
but their market content cannot be independently verified. The service writes accepted content itself.
"""
import datetime as dt
import hashlib
import io
import json
import math
from pathlib import Path
import re

MAX_COMPRESSED = 64 * 1024 * 1024
MAX_BROKER_COMPRESSED = 4 * 1024**3
# Bounds on the canonical JSON form of a recording and of each batch.
MAX_EXPANDED = 2 * 1024 * 1024 * 1024
MAX_LINE = 16 * 1024 * 1024
SCHEMA = 'brisk-nautilus-parquet-v3'
# The format before v3 (the tables of updates also held ts_init, a copy of ts_event): still read and accepted.
LEGACY_SCHEMA = 'brisk-nautilus-parquet-v2'
SOURCES = {'historical_mock', 'synthetic_test', 'brisk_live'}
QUOTE_KEYS = set('issue_id code frame source_time_us max_frame last_price10 open_price10 bid_price10 ask_price10 indicative_price10 indicative_volume indicative_imbalance_side closing_indicative_price10 closing_indicative_volume closing_indicative_imbalance_side quote_flag quote_side special_quote_time_us indicative_open_price10 reference_price10 volume turnover10 issue_status market_buy_quantity market_sell_quantity closing_market_buy_quantity closing_market_sell_quantity bid_quantity bid_orders ask_quantity ask_orders bid_levels ask_levels bid_closing_levels ask_closing_levels bid_total_quantity bid_total_orders bid_closing_total_quantity bid_closing_total_orders ask_total_quantity ask_total_orders ask_closing_total_quantity ask_closing_total_orders'.split())
# A side of the book (or of its closing-condition orders): up to LEVELS [price10, quantity, orders], best price first.
BOOK_KEYS = {'bid_levels', 'ask_levels', 'bid_closing_levels', 'ask_closing_levels'}
# A broker quote's whole books, as what changed since its security's previous quote in the recording (the first holds
# them whole): [book, price10, quantity, orders], book the index in BOOKS, each book's best price first, and 0 shares
# and 0 orders for a level that emptied. Replayed, the books' best LEVELS are the quote's own lists. A recording's
# quotes all have it or (a broker's from before 1.9.0, a demo's) none.
BOOK_CHANGES = 'book_changes'
BOOKS = ('bid_levels', 'ask_levels', 'bid_closing_levels', 'ask_closing_levels')
MAX_BOOK_LEVELS = 65536   # a book's nonempty levels, at most (a side spans fewer ticks between its daily limits)
# A recording's tables also hold each issue's whole books (its depths table's `checkpoint`) after every CHECKPOINT_CHANGES
# book changes since the last time (an issue's first quote holds them whole): reading its books at any moment
# replays at most that many changes, never the day so far.
CHECKPOINT_CHANGES = 1000
# One execution, as the decoder records it: `side` is its raw type (1 buyer-initiated, 2 seller-initiated,
# 0 most opening auction prints), `quantity` shares and `frame` the issue's frame number.
TRADE_KEYS = set('issue_id code frame time_us price10 quantity side'.split())
LEVELS = 10
MASTER_KEYS = set('issue_id code tick_type base_price10 limit_up10 limit_down10 lot_size issue_type name'.split())
BATCH_KEYS = {
    'bootstrap': set('type seq source trading_date input_transport source_timestamp_origin exchange_delay_ms source_time_us market_issue_count master quotes trades'.split()),
    'quotes': set('type seq source_time_us received_unix_ms decode_ns replay_lateness_ms quotes trades'.split()),
    'end': set('type seq source_time_us frames quote_updates replay_wall_ms'.split()),
    # Trades the snapshot restored (from before the stream), right after the bootstrap and at its clock.
    'history': set('type seq source_time_us trades'.split()),
}
# Issues whose quote could not be read (the decoder's state for them was inconsistent) are listed
# instead, on the batches that would have carried them.
SKIPPED = 'skipped_issues'
# Trades per history batch, at most.
MAX_HISTORY = 50_000
SUMMARY_KEYS = {'source', 'trading_date', 'first_source_time_us', 'last_source_time_us', 'codes', 'batches', 'quote_updates', 'expanded_bytes'}
DEMO_ORIGIN = 'https://next-demo.brisk.jp/'
# Broker-hosted BRiSK (SBI, Tachibana e-shiten, Matsui, Monex, SMBC Nikko) is one app with the same market data,
# so its recordings are one source, BROKER_SOURCE; each keeps only its site's fixed transport provenance, which
# says which broker's it came through (BROKER_TRANSPORTS, by briskapi.brisk site).
BROKER_SOURCE = 'brisk_live'
BROKER_TRANSPORTS = {'sbi': {'kind': 'sbi_websocket', 'origin': 'https://sbi.brisk.jp/'},
                     'e-shiten': {'kind': 'eshiten_websocket', 'origin': 'https://next.e-shiten.brisk.jp/'},
                     'matsui': {'kind': 'matsui_websocket', 'origin': 'https://matsui.brisk.jp/'},
                     'monex': {'kind': 'monex_websocket', 'origin': 'https://monex.brisk.jp/'},
                     'smbcnikko': {'kind': 'smbcnikko_websocket', 'origin': 'https://smbcnikko.brisk.jp/'}}
# Before 1.4.0 each broker's recordings were a source of their own. Those kept on a computer still read as a broker's
# (each with its own site's transport); the archive takes none of them.
LEGACY_BROKER_SOURCES = {'sbi_live': 'sbi', 'eshiten_live': 'e-shiten', 'matsui_live': 'matsui', 'monex_live': 'monex',
                         'smbcnikko_live': 'smbcnikko'}
UPGRADE = 'This briskapi is too old for the archive: upgrade it (python -m pip install -U briskapi)'

def broker(source):
    """Whether a recording's source is a broker's live BRiSK (or, kept on a computer, one from before 1.4.0)."""
    return source == BROKER_SOURCE or source in LEGACY_BROKER_SOURCES

def transports(source):
    """The transport provenance a broker's recording of this source may have: any broker's, or a legacy source's own."""
    if source in LEGACY_BROKER_SOURCES:
        return [BROKER_TRANSPORTS[LEGACY_BROKER_SOURCES[source]]]
    return list(BROKER_TRANSPORTS.values()) if source == BROKER_SOURCE else []

def site_of(transport):
    """The broker site whose transport provenance this is, or None."""
    return next((site for site, fixed in BROKER_TRANSPORTS.items() if transport == fixed), None)
REFERENCE_DIR = Path(__file__).resolve().parent / 'references'
# Local measurements are client reported, and checked for plausibility only: a range, a span, a precision where one is stated.
# They are timing data, not a promise of any order (a computer's clock adjusting itself sets the receipt clock back), and these
# checks do not stop other data being carried in them: decode_ns takes any value up to MAX_DECODE_NS, in any order.
MAX_DECODE_NS = 10 * 10**9
UNIX_MS = (1_600_000_000_000, 4_102_444_800_000)
MAX_SESSION_MS = 86_400_000
MAX_FETCH_MS = 600_000

def require(condition, message):
    if not condition:
        raise ValueError(message)

def integer(value, maximum=2**53-1):
    require(type(value) is int and 0 <= value <= maximum, 'Invalid unsigned integer')
    return value

def measurement(value, maximum):
    """Non-negative milliseconds with at most microsecond precision."""
    require(type(value) in (int, float) and 0 <= value <= maximum and math.copysign(1, value) > 0
            and round(value, 3) == value, 'Invalid local timing')
    return value

def code(value):
    require(isinstance(value, str) and re.fullmatch(r'[A-Z0-9]{4,8}', value), 'Invalid security code')
    return value

def book(levels, descending):
    """One side of a quote's book: up to LEVELS [price10, quantity, orders], best price first."""
    require(isinstance(levels, list) and len(levels) <= LEVELS, 'Invalid book side')
    for level in levels:
        require(isinstance(level, list) and len(level) == 3, 'Invalid book level')
        for value in level:
            integer(value)
    prices = [level[0] for level in levels]
    require(prices == sorted(set(prices), reverse=descending), 'Book prices out of order')

def book_changes(before, after):
    """What changed from the four books `before` (None: none yet) to `after`, as BOOK_CHANGES lists it. Each is
    [price10, quantity, orders] levels, best first, in BOOKS order."""
    out = []
    for book, now in enumerate(after):
        old = {level[0]: level for level in before[book]} if before else {}
        new = {level[0]: level for level in now}
        for price in sorted(old.keys() | new.keys(), reverse=book % 2 == 0):
            if price not in new:
                out.append([book, price, 0, 0])
            elif old.get(price) != new[price]:
                out.append([book, *new[price]])
    return out

def apply_changes(books, changes):
    """Apply a quote's BOOK_CHANGES to `books` (four dicts, price10 to [quantity, orders]), in place."""
    for book, price, quantity, orders in changes:
        if quantity:
            books[book][price] = [quantity, orders]
        else:
            require(books[book].pop(price, None) is not None, 'Book change empties a level the book does not have')

def best(books, book, count=LEVELS):
    """A book's best `count` levels (all with None), as a quote lists them."""
    levels = sorted(books[book].items(), reverse=book % 2 == 0)[:count]
    return [[price, quantity, orders] for price, (quantity, orders) in levels]

def _changes(value):
    """Check a quote's BOOK_CHANGES: each book's levels once, best first, an emptied level with 0 shares and orders."""
    require(isinstance(value, list) and len(value) <= len(BOOKS) * MAX_BOOK_LEVELS, 'Invalid book changes')
    last = None
    for change in value:
        require(isinstance(change, list) and len(change) == 4, 'Invalid book change')
        book, price, quantity, orders = (integer(v) for v in change)
        require(book < len(BOOKS) and price > 0 and (quantity == 0) == (orders == 0), 'Invalid book change')
        require(last is None or book > last[0] or (book == last[0] and (price < last[1] if book % 2 == 0 else price > last[1])),
                'Book changes out of order')
        last = (book, price)

def _nonfinite(_):
    raise ValueError('Nonfinite JSON')

def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False)

def encode(batch):
    return (canonical(batch) + '\n').encode()

def _issue(item):
    require(isinstance(item, dict) and type(item.get('issue_id')) is int, 'Invalid issue entry')
    return item['issue_id']

def canonical_batches(source):
    """Decoder JSONL as canonical batches: issues sorted, timings in microseconds, fixed transport provenance only."""
    while line := source.readline(MAX_LINE + 1):
        require(len(line) <= MAX_LINE, 'Recording exceeds decode limit')
        b = json.loads(line, parse_constant=_nonfinite)
        require(isinstance(b, dict), 'Invalid batch')
        transport = b.get('input_transport') if b.get('type') == 'bootstrap' else None
        for fixed in transports(b.get('source')) if isinstance(transport, dict) else ():
            if all(transport.get(k) == v for k, v in fixed.items()):
                # Keep only the site's fixed transport provenance, never session/profile or
                # decoder paths and connection diagnostics from the local host.
                b['input_transport'] = dict(fixed)
                break
        for key in ('master', 'quotes', 'trades'):
            if isinstance(b.get(key), list):
                b[key].sort(key=_issue)  # stable: trades keep the decoder's order within an issue
        if isinstance(b.get(SKIPPED), list) and all(type(issue) is int for issue in b[SKIPPED]):
            b[SKIPPED].sort()
        for holder, key in ((b, 'replay_lateness_ms'), (b, 'replay_wall_ms'), (b.get('input_transport'), 'asset_fetch_ms')):
            if isinstance(holder, dict) and type(holder.get(key)) is float:
                holder[key] = round(holder[key], 3)
        yield b

def canonical_lines(source):
    """Decoder JSONL rewritten as canonical lines (see canonical_batches)."""
    return map(encode, canonical_batches(source))

class Fingerprint:
    """Digest of market content: the batch clock timeline plus one hash chain per issue."""
    def __init__(self):
        self.timeline = hashlib.sha256()
        self.issues = {}

    def batch(self, seq, now):
        self.timeline.update(b'%d:%d\n' % (seq, now))

    def master(self, entry):
        self.issues[entry['issue_id']] = hashlib.sha256(canonical(entry).encode())

    def quote(self, seq, quote):
        self.issues[quote['issue_id']].update(b'%d:' % seq + canonical(quote).encode() + b'\n')

    def trade(self, seq, trade):
        self.issues[trade['issue_id']].update(b'T%d:' % seq + canonical(trade).encode() + b'\n')

    def digest(self, issue):
        return self.issues[issue].hexdigest()[:32]

def _transport(value, source):
    if broker(source):
        require(value in transports(source), 'Invalid broker transport fields')
        return
    require(value == {'kind': 'local_cache'} or (isinstance(value, dict) and set(value) == {'kind', 'origin', 'asset_fetch_ms'}
            and value['kind'] == 'https_recorded_assets' and value['origin'] == DEMO_ORIGIN), 'Invalid transport fields')
    if 'asset_fetch_ms' in value:
        measurement(value['asset_fetch_ms'], MAX_FETCH_MS)

def _lines(stream):
    """(batch, line) pairs from JSONL, reading no line beyond the decode limit."""
    while line := stream.readline(MAX_LINE + 1):
        require(len(line) <= MAX_LINE, 'Recording exceeds decode limit')
        yield json.loads(line, parse_constant=_nonfinite), line

def _scan(entries):
    """Check structure, canonical form and local timing; fingerprint market content.

    `entries` yields (batch, line): a JSONL line must be the batch's canonical encoding, and
    batches decoded from another format pass None.
    """
    identities, clocks, executed, offsets, executions, restored, books = {}, {}, {}, {}, {}, {}, {}
    changed = None   # whether the recording's quotes have BOOK_CHANGES (a broker's from 1.9.0 on)
    fingerprint = Fingerprint()
    seq = last = count = expanded = updates = deltas = market = 0
    first = source = date = paced = received = previous_kind = None
    ended = False
    for b, line in entries:
        require(isinstance(b, dict), 'Invalid batch')
        encoded = encode(b)
        # Exactly one byte encoding per value: no whitespace, duplicate keys,
        # escapes, key order or number spellings that could carry other data.
        require(line is None or encoded == line, 'Non-canonical encoding')
        expanded += len(encoded)
        require(len(encoded) <= MAX_LINE and expanded <= MAX_EXPANDED, 'Recording exceeds decode limit')
        require(not ended, 'Data after end batch')
        kind = b.get('type')
        require(kind in BATCH_KEYS and set(b) - {SKIPPED} == BATCH_KEYS[kind]
                and (SKIPPED not in b or kind in ('bootstrap', 'quotes')), 'Unknown or missing batch fields')
        require(integer(b['seq']) == seq, 'Sequence gap')
        now = integer(b['source_time_us'], 86_400_000_000 - 1)
        require(now >= last, 'Market clock regression')
        fingerprint.batch(seq, now)
        if seq == 0:
            require(kind == 'bootstrap' and isinstance(b['source'], str) and b['source'] in SOURCES,
                    'Unsupported bootstrap/source')
            source, date, first = b['source'], b['trading_date'], now
            require(isinstance(date, str) and re.fullmatch(r'\d{8}', date), 'Invalid date')
            dt.datetime.strptime(date, '%Y%m%d')
            master = b['master']
            require(isinstance(master, list) and 0 < len(master) <= 20000, 'Invalid master size')
            market = integer(b['market_issue_count'])
            require(market >= len(master), 'Invalid market count')
            issue = -1
            for m in master:
                require(isinstance(m, dict) and set(m) == MASTER_KEYS, 'Unknown or missing master fields')
                previous_issue, issue = issue, integer(m['issue_id'], 2**32-1)
                require(issue > previous_issue, 'Master identities must be unique and sorted')
                identities[issue] = code(m['code'])
                for key, value in m.items():
                    if key == 'name':
                        require(isinstance(value, str) and len(value) <= 256, 'Invalid security name')
                    elif key != 'code':
                        integer(value)
                fingerprint.master(m)
            # No account/session information is retained in the archive.
            _transport(b['input_transport'], source)
            require(b['source_timestamp_origin'] == 'brisk_decoder_unverified', 'Invalid clock provenance')
            require(b['exchange_delay_ms'] is None, 'Unverified exchange delay')
        elif kind == 'history':
            require(previous_kind in {'bootstrap', 'history'} and now == last, 'History out of place')
        else:
            require(kind in {'quotes', 'end'}, 'Expected delta/end')
        if kind == 'quotes':
            integer(b['decode_ns'], MAX_DECODE_NS)
            stamp = integer(b['received_unix_ms'])
            require(UNIX_MS[0] <= stamp < UNIX_MS[1], 'Implausible local receipt clock')
            if received is None:
                received = stamp
            require(abs(stamp - received) <= MAX_SESSION_MS, 'Local receipt clock span')
            lateness = b['replay_lateness_ms']
            if broker(source):
                require(lateness is None, 'Broker streams are not demo replays')
            if lateness is not None:
                measurement(lateness, MAX_SESSION_MS)
            # A replay is paced (measured) or unpaced (null) throughout.
            paced = (lateness is not None) if paced is None else paced
            require(paced == (lateness is not None), 'Inconsistent replay pacing')
        # Trades: the batch's executions, by issue, each issue's in the decoder's order. Restored ones
        # (history) are already in the bootstrap's volumes, and in an order of their own.
        trades = b.get('trades', [])
        require(isinstance(trades, list) and (kind != 'history' or 0 < len(trades) <= MAX_HISTORY), 'Invalid trades')
        order = restored if kind == 'history' else executions
        previous_issue = -1
        for t in trades:
            require(isinstance(t, dict) and set(t) == TRADE_KEYS, 'Unknown or missing trade fields')
            issue = integer(t['issue_id'], 2**32-1)
            require(issue >= previous_issue and identities.get(issue) == code(t['code']), 'Trade identity mismatch/order')
            previous_issue = issue
            frame, at = integer(t['frame'], 2**32-1), integer(t['time_us'], now)
            previous = order.get(issue, (0, 0))
            require(frame >= previous[0] and at >= previous[1], 'Trade frame/time regression')
            order[issue] = (frame, at)
            price, quantity = integer(t['price10']), integer(t['quantity'])
            integer(t['side'], 255)
            require(price > 0 and quantity > 0, 'Invalid trade')
            if kind != 'history':
                volume, turnover = executed.get(issue, (0, 0))
                executed[issue] = (volume + quantity, turnover + price * quantity)
        quotes = b.get('quotes', [])
        require(isinstance(quotes, list) and len(quotes) <= len(identities), 'Invalid quote count')
        previous_issue = -1
        for q in quotes:
            if changed is None:
                changed = isinstance(q, dict) and BOOK_CHANGES in q and broker(source)
            require(isinstance(q, dict) and set(q) == (QUOTE_KEYS | {BOOK_CHANGES} if changed else QUOTE_KEYS),
                    'Unknown or missing quote fields')
            issue = integer(q['issue_id'], 2**32-1)
            require(issue > previous_issue and identities.get(issue) == code(q['code']), 'Quote identity mismatch/duplicate')
            previous_issue = issue
            frame, clock = integer(q['frame'], 2**32-1), integer(q['source_time_us'], now)
            previous = clocks.get(issue, (0, 0))
            require(frame >= previous[0] and clock >= previous[1], 'Quote frame/time regression')
            clocks[issue] = (frame, clock)
            for key, value in q.items():
                if key in BOOK_KEYS:
                    book(value, descending=key.startswith('bid'))
                elif key == BOOK_CHANGES:
                    _changes(value)
                elif key != 'code':
                    integer(value)
            if changed:
                # Replayed, the books' best levels are the quote's own: the changes hold what the lists show and more.
                state = books.setdefault(issue, [{} for _ in BOOKS])
                apply_changes(state, q[BOOK_CHANGES])
                require(all(len(side) <= MAX_BOOK_LEVELS for side in state), 'Invalid book changes')
                require(all(best(state, k) == q[name] for k, name in enumerate(BOOKS)), 'Book changes do not give the quote\'s book')
            # The trades so far account for every change in an issue's volume and turnover.
            volume, turnover = executed.get(issue, (0, 0))
            offset = (q['volume'] - volume, q['turnover10'] - turnover)
            require(offsets.setdefault(issue, offset) == offset, 'Trades do not add up to the volume/turnover')
            fingerprint.quote(seq, q)
        for t in trades:
            fingerprint.trade(seq, t)
        skipped = b.get(SKIPPED, [])
        require(isinstance(skipped, list) and skipped and all(issue in identities for issue in skipped)
                and skipped == sorted(set(skipped)) and not set(skipped) & {q['issue_id'] for q in quotes}
                if SKIPPED in b else True, 'Invalid skipped issues')
        if seq == 0:
            require(len(quotes) + len(skipped) == len(identities), 'Incomplete bootstrap')
        if kind == 'end':
            frames = integer(b['frames'])
            # A broker's frames are the ones received, before the bootstrap too, one for each quote batch: history batches
            # (the trades a mid-session start restores) are not frames.
            require((frames >= deltas if broker(source) else frames == seq)
                    and integer(b['quote_updates']) == updates, 'End summary mismatch')
            measurement(b['replay_wall_ms'], MAX_SESSION_MS)
            ended = True
        elif kind == 'quotes':
            updates += len(quotes)
            deltas += 1
        seq += 1
        last, count, previous_kind = now, count + len(quotes), kind
    require(ended, 'Recording has no clean end')
    summary = dict(source=source, trading_date=date, first_source_time_us=first,
                   last_source_time_us=last, codes=sorted(identities.values()), batches=seq,
                   quote_updates=count, expanded_bytes=expanded)
    return summary, fingerprint, market

def build_reference(stream):
    """Fingerprint a complete, trusted replay (canonical JSONL) of every issue in a source's market."""
    summary, fingerprint, market = _scan(_lines(stream))
    require(set(fingerprint.issues) == set(range(market)), 'Reference must cover the whole market')
    return dict(source=summary['source'], trading_date=summary['trading_date'], market_issue_count=market,
                batches=summary['batches'], timeline=fingerprint.timeline.hexdigest(),
                issues=[fingerprint.digest(i) for i in range(market)])

def synthetic_recording():
    """The only accepted synthetic_test recording: a one-issue upload probe."""
    master = {**dict.fromkeys(MASTER_KEYS, 0), 'issue_id': 0, 'code': '0000', 'lot_size': 100, 'name': 'Synthetic fixture'}
    quote = {**dict.fromkeys(QUOTE_KEYS, 0), 'issue_id': 0, 'code': '0000', 'frame': 1, 'max_frame': 1,
             'source_time_us': 100, 'indicative_price10': 1230, 'indicative_volume': 100,
             **{key: [] for key in BOOK_KEYS}}
    trade = dict(issue_id=0, code='0000', frame=2, time_us=105, price10=1240, quantity=100, side=1)
    return [dict(type='bootstrap', seq=0, source='synthetic_test', trading_date='20210927', source_time_us=100,
                 input_transport={'kind': 'local_cache'}, source_timestamp_origin='brisk_decoder_unverified',
                 exchange_delay_ms=None, market_issue_count=1, master=[master], quotes=[quote], trades=[]),
            dict(type='quotes', seq=1, source_time_us=110, received_unix_ms=1632700800000, decode_ns=1000,
                 replay_lateness_ms=0, quotes=[{**quote, 'frame': 2, 'max_frame': 2, 'source_time_us': 110,
                                                'indicative_price10': 1240, 'last_price10': 1240, 'volume': 100,
                                                'turnover10': 124000}], trades=[trade]),
            dict(type='end', seq=2, source_time_us=110, frames=2, quote_updates=1, replay_wall_ms=0.5)]

def load_references():
    references = {'synthetic_test': build_reference(io.BytesIO(b''.join(map(encode, synthetic_recording()))))}
    for path in sorted(REFERENCE_DIR.glob('*.json')):
        reference = json.loads(path.read_text())
        references[reference['source']] = reference
    return references

REFERENCES = load_references()

def validate_stream(stream, references=None):
    """Validate canonical JSONL (see validate_batches); every line must be its batch's canonical bytes."""
    return _verify(*_scan(_lines(stream)), references)

def validate_batches(batches, references=None):
    """Validate canonical fields/continuity, plus reference content for demo/synthetic data.

    Broker live content is uploader-declared and has no reference
    replay. Its transport is exactly one of BROKER_TRANSPORTS, and replay_lateness_ms must
    be null. End frames include pre-bootstrap catch-up frames as well as the emitted
    quote batches. A batch lists in skipped_issues the issues whose quote could not be
    read; a bootstrap's quotes and skipped issues together cover its master.
    """
    return _verify(*_scan((b, None) for b in batches), references)

def _verify(summary, fingerprint, market, references):
    if broker(summary['source']):
        return summary
    reference = (REFERENCES if references is None else references).get(summary['source'])
    require(reference is not None, 'No reference replay for this source')
    require(summary['trading_date'] == reference['trading_date'] and market == reference['market_issue_count']
            and summary['batches'] == reference['batches'] and fingerprint.timeline.hexdigest() == reference['timeline'],
            'Recording is not a complete reference replay')
    issues = reference['issues']
    require(all(i < len(issues) and fingerprint.digest(i) == issues[i] for i in fingerprint.issues),
            'Market data differs from the reference replay')
    return summary

# An uploader's identity: a random secret made on its own computer, never registered. The archive
# files uploads under the owner ID, a one-way hash of it, and shows each owner only its own.
IDENTITY = re.compile(r'brisk-id-v1-[a-z2-7]{26}')
OWNER = re.compile(r'[0-9a-f]{32}')
# A stored recording: archive/OWNER/YYYYMMDD/FROM-TO-SHA256, its first and last source clock (JST,
# HHMMSS) in the name so that a day's recordings sort by time.
RECORDING_PREFIX = re.compile(r'archive/([0-9a-f]{32})/(\d{8})/(\d{6})-(\d{6})-([0-9a-f]{64})')
TIMING_KEY = re.compile(r'timing/([0-9a-f]{32})/(\d{8})/([0-9a-f]{64})\.json')

def hhmmss(us: int) -> str:
    """A source clock (microseconds after JST midnight) as HHMMSS."""
    seconds = us // 1_000_000
    return f'{seconds // 3600:02d}{seconds // 60 % 60:02d}{seconds % 60:02d}'

def recording_name(m) -> str:
    """A manifest's archive name below its owner: YYYYMMDD/FROM-TO-SHA256."""
    s = m['summary']
    return f"{s['trading_date']}/{hhmmss(s['first_source_time_us'])}-{hhmmss(s['last_source_time_us'])}-{m['sha256']}"

def local_name(summary, transport=None) -> str:
    """A folder name for a recording kept on this computer: SOURCE_YYYYMMDD_FROM-TO, a broker's SITE_YYYYMMDD_FROM-TO
    (its site, from its bootstrap's `transport`), so two brokers' recordings of a session never meet."""
    head = site_of(transport) if broker(summary['source']) and site_of(transport) else summary['source']
    return f"{head}_{summary['trading_date']}_{hhmmss(summary['first_source_time_us'])}-{hhmmss(summary['last_source_time_us'])}"

def owner(secret):
    """The owner ID of an identity secret."""
    require(isinstance(secret, str) and IDENTITY.fullmatch(secret), 'Invalid identity')
    return hashlib.sha256(b'brisk-owner-v1:' + secret.encode()).hexdigest()[:32]

def max_package(source):
    """The largest package the archive takes of a source: a broker's recording is a trading session of the whole
    market, checked a row group at a time; a demo or synthetic one is checked whole, in memory."""
    return MAX_BROKER_COMPRESSED if broker(source) else MAX_COMPRESSED

def validate_manifest(m):
    require(isinstance(m, dict) and set(m) == {'schema', 'sha256', 'bytes', 'summary'}, 'Invalid manifest fields')
    require(m['schema'] in (SCHEMA, LEGACY_SCHEMA) and isinstance(m['sha256'], str) and re.fullmatch('[0-9a-f]{64}', m['sha256']), 'Invalid schema/hash')
    summary = m['summary']
    require(isinstance(summary, dict) and summary.get('source') not in LEGACY_BROKER_SOURCES, UPGRADE)
    require(set(summary) == SUMMARY_KEYS and isinstance(summary['source'], str) and summary['source'] in SOURCES,
            'Invalid summary')
    require(0 < integer(m['bytes'], max_package(summary['source'])), 'Empty upload')
    require(isinstance(summary['trading_date'], str) and re.fullmatch(r'\d{8}', summary['trading_date']), 'Invalid summary date')
    dt.datetime.strptime(summary['trading_date'], '%Y%m%d')
    for key in SUMMARY_KEYS - {'source', 'trading_date', 'codes'}:
        integer(summary[key])
    codes = summary['codes']
    require(isinstance(codes, list) and 0 < len(codes) <= 20000, 'Invalid summary codes')
    for value in codes:
        code(value)
    require(codes == sorted(set(codes)), 'Invalid summary code order/duplicates')
    first, last = summary['first_source_time_us'], summary['last_source_time_us']
    require(first <= last < 86_400_000_000 and summary['batches'] >= 2
            and summary['quote_updates'] >= len(codes) and summary['expanded_bytes'] > 0, 'Invalid summary bounds')
    reference = REFERENCES.get(summary['source'])
    if reference:
        require(summary['trading_date'] == reference['trading_date'] and summary['batches'] == reference['batches'],
                'Summary is not a complete reference replay')
    return m

# Timing reports: what a broker session uploads besides its market recording.
TIMING_SCHEMA = 'brisk-timing-v3'
TIMING_KEYS = {'schema', 'source', 'site', 'trading_date', 'first_minute', 'last_minute', 'frames', 'stalls',
               'client_version', 'decode_ms', 'source_age_ms', 'interarrival_ms'}
QUANTILES = ('p50', 'p90', 'p99', 'max')
# Bounds per distribution (ms). Source age can be negative when local and feed clocks disagree.
TIMING_BOUNDS = {'decode_ms': (0, 10_000), 'source_age_ms': (-600_000, 600_000), 'interarrival_ms': (0, 3_600_000)}
MAX_TIMING_BYTES = 2048
MIN_TIMING_FRAMES = 100

def validate_timing(report, today=None):
    """A timing report: exact fields, bounded microsecond-precision distributions, no market data.

    `today` (a date) additionally requires a trading date within the last 30 days.
    """
    require(isinstance(report, dict) and report.get('schema') == TIMING_SCHEMA, UPGRADE)
    require(set(report) == TIMING_KEYS, 'Invalid timing report fields')
    require(report['source'] == BROKER_SOURCE and report['site'] in BROKER_TRANSPORTS, 'Invalid timing schema/source')
    date = report['trading_date']
    require(isinstance(date, str) and re.fullmatch(r'\d{8}', date), 'Invalid timing date')
    day = dt.datetime.strptime(date, '%Y%m%d').date()
    if today is not None:
        require(0 <= (today - day).days <= 30, 'Timing report date out of range')
    minutes = [report['first_minute'], report['last_minute']]
    require(all(isinstance(m, str) and re.fullmatch(r'([01]\d|2[0-3]):[0-5]\d', m) for m in minutes)
            and minutes[0] <= minutes[1], 'Invalid session minutes')
    frames = integer(report['frames'], 10_000_000)
    require(frames >= MIN_TIMING_FRAMES and integer(report['stalls']) <= frames, 'Invalid frame counts')
    require(isinstance(report['client_version'], str) and re.fullmatch(r'\d{1,4}\.\d{1,4}\.\d{1,4}', report['client_version']),
            'Invalid client version')
    for name, (low, high) in TIMING_BOUNDS.items():
        values = report[name]
        require(isinstance(values, dict) and set(values) == set(QUANTILES), f'Invalid {name}')
        series = [values[q] for q in QUANTILES]
        require(all(type(v) in (int, float) and low <= v <= high and round(v, 3) == v for v in series)
                and series == sorted(series), f'Invalid {name}')
    require(len(encode(report)) <= MAX_TIMING_BYTES, 'Timing report too large')
    return report
