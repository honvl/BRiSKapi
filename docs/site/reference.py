"""The API reference pages of the documentation site, as data: one entry per method.

Each method page shows, like Databento's documentation, a description, its parameters and return value on
the left, and three cards on the right: the API method's signature, example usage (Python, and HTTP or the
command line where they exist) and the example's response. Responses come from running the examples
(capture.py writes responses.json); an entry with `illustrative=True` shows the shape of a response that
needs a broker login or an upload to produce, with made-up values.

`{QUERY_URL}` in an example is replaced by the deployed cloud API's URL (briskapi/archive.json).
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class P:
    name: str
    type: str
    text: str
    required: bool = False


@dataclass
class Method:
    slug: str
    title: str
    nav: str
    text: str
    signature: str
    params: list = field(default_factory=list)
    returns: str = ''
    examples: dict = field(default_factory=dict)
    responses: dict = field(default_factory=dict)
    illustrative: bool = False
    unrun: tuple = ()     # example languages capture.py leaves out (their output is too long to show)


KEY = 'export BRISK_KEY=$(brisk identity --export)   # your archive identity is the API key\n'
CLIENT = 'import briskapi\n\nclient = briskapi.Historical()\n'
DEMO = 'import briskapi\n\nbriskapi.load("recordings/demo")   # or briskapi.connect() for live\n'
RANGE = ('data = client.timeseries.get_range(\n    dataset="XTKS.BRISKDEMO",\n    symbols=["7203"],\n    schema="trades",\n'
         '    start="2021-09-27T09:00",\n    end="2021-09-27T09:00:05",\n)\n')


def curl(endpoint, *fields, post=False):
    lines = [f"curl {'-X POST' if post else '-G'} '{{QUERY_URL}}v0/{endpoint}' \\", '  -u "$BRISK_KEY:"']
    lines[-1] += ' \\' if fields else ''
    for i, item in enumerate(fields):
        lines.append(f'  -d {item}' + (' \\' if i < len(fields) - 1 else ''))
    return KEY + '\n'.join(lines)


GET_RANGE_PARAMS = [
    P('dataset', 'str', '`XTKS.BRISK` (your own live recordings) or `XTKS.BRISKDEMO` (the public demo, open to every identity).',
      required=True),
    P('start', 'str | int | date', 'The inclusive start. An ISO 8601 time (JST unless it names a zone), a date (that trading day, '
      'from JST midnight) or UTC nanoseconds.', required=True),
    P('end', 'str | int | date | None', 'The exclusive end, in the same forms. Default: the end of the start\'s trading day. At '
      'most 31 days after `start`; both between 2000 and 2100.'),
    P('symbols', 'Iterable[str] | str', 'Up to 2,000 JPX codes (`"7203"`, `"167A"`), as a list or comma-separated. Default '
      '`"ALL_SYMBOLS"`.'),
    P('schema', 'str', '`trades` (default), `mbp-10`, `book-deltas`, `definition`, `imbalance`, `brisk-quotes` or `ohlcv-1s`, '
      '`ohlcv-1m`, `ohlcv-1h`, `ohlcv-1d`. See [Data format](/data-format.md).'),
    P('stype_in', 'str', 'Only `raw_symbol` (JPX codes).'),
    P('stype_out', 'str', 'Only `instrument_id`; every record also has its code in `symbol`.'),
    P('limit', 'int | None', 'The most records to return, after time-ordering.'),
    P('path', 'str | Path | None', 'Also write the records (Parquet) to this file.'),
    P('ts_filter', 'str', 'The clock `start`, `end` and the order use: `ts_event` (default; BRiSK\'s stamp, the same rows whichever '
      'capture holds them) or `ts_recv` (when your computer received the record, for tests that must not look ahead). The cloud '
      'API cuts only on `ts_event`, so a `ts_recv` request asks for `skew` seconds either side and cuts the rest in the client, '
      'with `limit` applied after. Not for bars.'),
    P('skew', 'float', 'Seconds of padding either side of the window for `ts_filter="ts_recv"` (default 10). A warning says when '
      'BRiSK\'s stamps ran ahead of the receipt clock by more than half of it.'),
    P('snapshot', 'bool', 'For `book-deltas` (by `ts_event`): begin with each security\'s whole books as of `start`, a clear (`R`) '
      'and its levels flagged 32, rebuilt by the cloud API from the security\'s last checkpoint. Default False.'),
]

STORE = CLIENT + RANGE

CLOUD = [
    Method('historical/client', 'Historical', 'Historical',
           'The cloud API\'s client. Your archive identity is the API key: nothing to sign up for. Requests go to the '
           'archive\'s query service over HTTPS, and only reach your own recordings and the public demo.',
           'briskapi.Historical(\n    key: str | None = None,\n    gateway: str | None = None,\n)',
           [P('key', 'str | None', 'Your archive identity. Default: this computer\'s (`BRISK_IDENTITY`, the system\'s '
              'credential store or `~/.config/brisk/identity.json`); if there is none, one is made and a warning says how to '
              'keep its recovery code.'),
            P('gateway', 'str | None', 'The cloud API\'s URL. Default: the archive\'s, from the installed '
              '`briskapi/archive.json`. Must be `https://` (or `http://localhost`).')],
           'A `Historical` client, with `timeseries` and `metadata`.',
           {'Python': CLIENT + 'print(client.metadata.list_datasets())', 'HTTP': curl('metadata.list_datasets')}),
    Method('historical/timeseries/get_range', 'Historical.timeseries.get_range', '.get_range',
           'Records of one schema for some securities over a time range, from your recordings in the cloud: merged across '
           'recordings that overlap (each record once) and in time order. OHLCV bars exist only here, built from your own '
           'uploads, and only for the securities and times they cover.',
           'Historical.timeseries.get_range(\n    dataset: str,\n    start: str | int | date,\n    end: str | int | date | None = None,\n'
           '    symbols: Iterable[str] | str = "ALL_SYMBOLS",\n    schema: str = "trades",\n    stype_in: str = "raw_symbol",\n'
           '    stype_out: str = "instrument_id",\n    limit: int | None = None,\n    path: str | Path | None = None,\n'
           '    ts_filter: str = "ts_event",   # or "ts_recv"\n    skew: float = 10.0,\n    snapshot: bool = False,\n) -> DataStore',
           GET_RANGE_PARAMS, 'A [`DataStore`](/reference/historical/store/to_df.md) of the records.',
           {'Python': CLIENT + 'data = client.timeseries.get_range(\n    dataset="XTKS.BRISKDEMO",\n    symbols=["7203"],\n'
            '    schema="trades",\n    start="2021-09-27T09:00",\n    end="2021-09-27T09:01",\n    limit=1,\n)\n'
            'print(data.to_df(tz="Asia/Tokyo").iloc[0])',
            'HTTP': curl('timeseries.get_range', 'dataset=XTKS.BRISKDEMO', 'symbols=7203', 'schema=trades',
                         'start=2021-09-27T09:00', 'end=2021-09-27T09:01', 'encoding=json', 'limit=1', post=True),
            'CLI': 'brisk query XTKS.BRISKDEMO --schema trades --symbols 7203 \\\n  --start 2021-09-27T09:00 --end 2021-09-27T09:01 --limit 1'}),
    Method('historical/timeseries/get_range_bars', 'Historical.timeseries.get_range (bars)', '.get_range: bars',
           'OHLCV bars: one row per security and interval with trades, open to close. For `XTKS.BRISK` they are built ahead '
           'from your own live recordings, and you receive a bar only when your recordings cover that security and the '
           'bar\'s whole span within the trading sessions (09:00–11:30, 12:30–15:30 JST). The demo\'s are open to everyone.',
           'Historical.timeseries.get_range(\n    dataset: str,\n    start: str | int | date,\n    end: str | int | date | None = None,\n'
           '    symbols: Iterable[str] | str = "ALL_SYMBOLS",\n    schema: str = "ohlcv-1m",   # ohlcv-1s, ohlcv-1h, ohlcv-1d\n'
           '    limit: int | None = None,\n) -> DataStore',
           GET_RANGE_PARAMS[:5] + [GET_RANGE_PARAMS[7]], 'A [`DataStore`](/reference/historical/store/to_df.md) of bars: `ts_event` (the bar\'s start), '
           '`open`, `high`, `low`, `close` (yen), `volume` (shares).',
           {'Python': CLIENT + 'bars = client.timeseries.get_range(\n    dataset="XTKS.BRISKDEMO",\n    symbols=["7203", "6758"],\n'
            '    schema="ohlcv-1m",\n    start="2021-09-27",\n)\nprint(bars.to_df(tz="Asia/Tokyo"))',
            'HTTP': curl('timeseries.get_range', 'dataset=XTKS.BRISKDEMO', 'symbols=7203,6758', 'schema=ohlcv-1m',
                         'start=2021-09-27', 'encoding=csv', post=True),
            'CLI': 'brisk query XTKS.BRISKDEMO --schema ohlcv-1m --symbols 7203,6758 --start 2021-09-27'}),
    Method('historical/timeseries/get_snapshot', 'Historical.timeseries.get_snapshot', '.get_snapshot',
           'The latest record of each security at or before a moment: its book as it stood. BRiSK sends a security\'s whole '
           'state on every update, so the last record is the state. By default the moment is on the receipt clock, so it '
           'holds what a program receiving the feed would have held then. A security with no record since `since` is '
           'absent. Implemented in the client over `get_range`; it adds no endpoint. For `book-deltas`, each security\'s whole '
           'books as of the moment by BRiSK\'s stamp: a clear and its levels, rebuilt by the cloud API from its last checkpoint.',
           'Historical.timeseries.get_snapshot(\n    dataset: str,\n    at: str | int | date,\n    symbols: Iterable[str] | str = "ALL_SYMBOLS",\n'
           '    schema: str = "brisk-quotes",\n    since: str | int | date | None = None,\n    ts_filter: str | None = None,\n'
           '    skew: float = 10.0,\n    path: str | Path | None = None,\n) -> DataStore',
           [P('dataset', 'str', '`XTKS.BRISK` (your own live recordings) or `XTKS.BRISKDEMO` (the public demo, open to every identity).',
              required=True),
            P('at', 'str | int | date', 'The moment: an ISO 8601 time (JST unless it names a zone), a date (that trading day, from '
              'JST midnight) or UTC nanoseconds. Records at exactly this moment count.', required=True),
            GET_RANGE_PARAMS[3],
            P('schema', 'str', 'A record schema: `brisk-quotes` (default), `mbp-10`, `book-deltas`, `imbalance`, `trades` or '
              '`definition`; not bars.'),
            P('since', 'str | int | date | None', 'Where the search starts. Default: the JST midnight of `at`\'s trading day. '
              'During the session pass a later one, or the search reads the whole morning.'),
            P('ts_filter', 'str | None', '`ts_recv` (the default: what had been received by `at`) or `ts_event` (BRiSK\'s stamp, '
              'and the only one for `book-deltas`).'),
            GET_RANGE_PARAMS[-2], GET_RANGE_PARAMS[8]],
           'A [`DataStore`](/reference/historical/store/to_df.md) with one row per security, in `instrument_id` order.',
           {'Python': CLIENT + 'snap = client.timeseries.get_snapshot(\n    dataset="XTKS.BRISKDEMO",\n    at="2021-09-27T09:00:00",\n'
            '    symbols=["7203", "6758"],\n    schema="brisk-quotes",\n)\n'
            'print(snap.to_df(tz="Asia/Tokyo")[["symbol", "bid_price", "ask_price", "indicative_price", "indicative_volume"]])'}),
    Method('historical/metadata/list_datasets', 'Historical.metadata.list_datasets', '.list_datasets',
           'The datasets you can query. Named like Databento\'s, venue then feed: `XTKS` is the Tokyo Stock Exchange.',
           'Historical.metadata.list_datasets() -> list[str]', [], 'A list of dataset codes.',
           {'Python': CLIENT + 'print(client.metadata.list_datasets())', 'HTTP': curl('metadata.list_datasets')}),
    Method('historical/metadata/list_publishers', 'Historical.metadata.list_publishers', '.list_publishers',
           'The publisher of each dataset: its `publisher_id` in records, dataset, venue and description. These IDs are '
           'briskapi\'s own; Databento\'s mean other venues.',
           'Historical.metadata.list_publishers() -> list[dict]', [], 'A list of `{publisher_id, dataset, venue, description}`.',
           {'Python': CLIENT + 'print(client.metadata.list_publishers())', 'HTTP': curl('metadata.list_publishers')}),
    Method('historical/metadata/list_schemas', 'Historical.metadata.list_schemas', '.list_schemas',
           'The schemas a dataset offers.', 'Historical.metadata.list_schemas(\n    dataset: str,\n) -> list[str]',
           [P('dataset', 'str', 'The dataset code.', required=True)], 'A list of schema names.',
           {'Python': CLIENT + 'print(client.metadata.list_schemas("XTKS.BRISKDEMO"))',
            'HTTP': curl('metadata.list_schemas', 'dataset=XTKS.BRISKDEMO')}),
    Method('historical/metadata/list_fields', 'Historical.metadata.list_fields', '.list_fields',
           'Every column of a schema\'s records, in order, with its type and meaning. The same as '
           '[Data format](/data-format.md).',
           'Historical.metadata.list_fields(\n    schema: str,\n) -> list[dict]', [P('schema', 'str', 'The schema.', required=True)],
           'A list of `{name, type, description}`.',
           {'Python': CLIENT + 'for field in client.metadata.list_fields("trades")[:5]:\n    print(field)',
            'HTTP': curl('metadata.list_fields', 'schema=trades')}),
    Method('historical/metadata/get_dataset_range', 'Historical.metadata.get_dataset_range', '.get_dataset_range',
           'The first and last trading date with data in a dataset: for `XTKS.BRISK`, of your own recordings.',
           'Historical.metadata.get_dataset_range(\n    dataset: str,\n) -> dict', [P('dataset', 'str', 'The dataset code.', required=True)],
           '`{start, end}`, as ISO 8601 dates (both inclusive). Without data, an error (HTTP 404).',
           {'Python': CLIENT + 'print(client.metadata.get_dataset_range("XTKS.BRISKDEMO"))',
            'HTTP': curl('metadata.get_dataset_range', 'dataset=XTKS.BRISKDEMO')}),
    Method('historical/metadata/get_dataset_condition', 'Historical.metadata.get_dataset_condition', '.get_dataset_condition',
           'Each date\'s condition in a dataset, `available` or `missing`, and, for `XTKS.BRISK`, the clock windows your '
           'recordings cover that day (JST).',
           'Historical.metadata.get_dataset_condition(\n    dataset: str,\n    start_date: str | None = None,\n    end_date: str | None = None,\n) -> list[dict]',
           [P('dataset', 'str', 'The dataset code.', required=True),
            P('start_date', 'str | None', 'The first date (`YYYY-MM-DD`). Default: the dataset\'s first.'),
            P('end_date', 'str | None', 'The last date, inclusive. Default: the dataset\'s last. At most a year.')],
           'A list of `{date, condition, last_modified_date, windows}`.',
           {'Python': CLIENT + 'print(client.metadata.get_dataset_condition("XTKS.BRISKDEMO"))',
            'HTTP': curl('metadata.get_dataset_condition', 'dataset=XTKS.BRISKDEMO')}),
    Method('historical/metadata/get_record_count', 'Historical.metadata.get_record_count', '.get_record_count',
           'How many records a `get_range` request would return, to size it before running it.',
           'Historical.metadata.get_record_count(\n    dataset: str,\n    start: str | int | date,\n    end: str | int | date | None = None,\n'
           '    symbols: Iterable[str] | str = "ALL_SYMBOLS",\n    schema: str = "trades",\n    limit: int | None = None,\n) -> int',
           [p for p in GET_RANGE_PARAMS if p.name in {'dataset', 'start', 'end', 'symbols', 'schema', 'limit'}], 'The number of records.',
           {'Python': CLIENT + 'print(client.metadata.get_record_count(\n    dataset="XTKS.BRISKDEMO",\n    symbols=["7203"],\n'
            '    schema="trades",\n    start="2021-09-27",\n))',
            'HTTP': curl('metadata.get_record_count', 'dataset=XTKS.BRISKDEMO', 'symbols=7203', 'schema=trades',
                         'start=2021-09-27', post=True)}),
    Method('historical/metadata/get_cost', 'Historical.metadata.get_cost', '.get_cost',
           'The cost of a request in US dollars: always 0. The cloud API is free, with limits: 60 requests a minute and 2 GB a '
           'day per identity, and 120 requests a minute and 4 GB a day per IP address.',
           'Historical.metadata.get_cost(\n    dataset: str,\n    start: str | int | date,\n    end: str | int | date | None = None,\n'
           '    symbols: Iterable[str] | str = "ALL_SYMBOLS",\n    schema: str = "trades",\n) -> float',
           [p for p in GET_RANGE_PARAMS if p.name in {'dataset', 'start', 'end', 'symbols', 'schema'}], '`0.0`.',
           {'Python': CLIENT + 'print(client.metadata.get_cost(dataset="XTKS.BRISKDEMO", start="2021-09-27"))',
            'HTTP': curl('metadata.get_cost', 'dataset=XTKS.BRISKDEMO', 'start=2021-09-27', post=True)}),
    Method('historical/store/to_df', 'DataStore.to_df', '.to_df',
           'The records as a pandas DataFrame (needs `pip install \'briskapi[pandas]\'`), indexed by `ts_recv` (or `ts_event` '
           'for bars). As Databento\'s.',
           'DataStore.to_df(\n    price_type: str = "float",\n    pretty_ts: bool = True,\n    map_symbols: bool = True,\n'
           '    tz: str = "UTC",\n    count: int | None = None,\n) -> pandas.DataFrame | Iterator[pandas.DataFrame]',
           [P('price_type', 'str', '`"float"` (yen), `"fixed"` (integers of 1e-9 yen) or `"decimal"` (`decimal.Decimal`).'),
            P('pretty_ts', 'bool', 'Timestamps as `pandas.Timestamp`s; `False` gives UTC nanoseconds.'),
            P('map_symbols', 'bool', 'Keep the `symbol` column (the JPX code).'),
            P('tz', 'str', 'The time zone to show timestamps in, such as `"Asia/Tokyo"`.'),
            P('count', 'int | None', 'Return an iterator of DataFrames of at most this many rows.')],
           'A DataFrame, or an iterator of DataFrames with `count`.',
           {'Python': STORE + 'print(data.to_df(tz="Asia/Tokyo")[["symbol", "price", "size", "side"]])'}),
    Method('historical/store/to_ndarray', 'DataStore.to_ndarray', '.to_ndarray',
           'The records as a numpy structured array.', 'DataStore.to_ndarray(\n    count: int | None = None,\n) -> numpy.ndarray | Iterator[numpy.ndarray]',
           [P('count', 'int | None', 'Return an iterator of arrays of at most this many records.')], 'A structured array.',
           {'Python': STORE + 'records = data.to_ndarray()\nprint(records.dtype.names[:6])\nprint(records["price"][:5])'}),
    Method('historical/store/to_csv', 'DataStore.to_csv', '.to_csv',
           'Write the records as CSV.',
           'DataStore.to_csv(\n    path: str | Path,\n    pretty_px: bool = True,\n    pretty_ts: bool = True,\n    map_symbols: bool = True,\n) -> Path',
           [P('path', 'str | Path', 'Where to write.', required=True),
            P('pretty_px', 'bool', 'Prices in yen; `False` gives integers of 1e-9 yen.'),
            P('pretty_ts', 'bool', 'ISO 8601 timestamps; `False` gives UTC nanoseconds.'),
            P('map_symbols', 'bool', 'Keep the `symbol` column.')],
           'The path written.',
           {'Python': STORE + 'data.to_csv("trades.csv")\nprint(open("trades.csv").read()[:420])'}),
    Method('historical/store/to_json', 'DataStore.to_json', '.to_json',
           'Write the records as JSON lines, one record each.',
           'DataStore.to_json(\n    path: str | Path,\n    pretty_px: bool = True,\n    pretty_ts: bool = True,\n    map_symbols: bool = True,\n) -> Path',
           [P('path', 'str | Path', 'Where to write.', required=True),
            P('pretty_px', 'bool', 'Prices in yen; `False` gives integers of 1e-9 yen.'),
            P('pretty_ts', 'bool', 'ISO 8601 timestamps; `False` gives UTC nanoseconds.'),
            P('map_symbols', 'bool', 'Keep the `symbol` column.')],
           'The path written.',
           {'Python': STORE + 'data.to_json("trades.json")\nprint(open("trades.json").readline())'}),
    Method('historical/store/to_parquet', 'DataStore.to_parquet', '.to_parquet',
           'Write the records as Parquet. With the defaults, the file is exactly what the service returned.',
           'DataStore.to_parquet(\n    path: str | Path,\n    price_type: str = "float",\n    pretty_ts: bool = True,\n    map_symbols: bool = True,\n) -> Path',
           [P('path', 'str | Path', 'Where to write.', required=True),
            P('price_type', 'str', '`"float"`, `"fixed"` or `"decimal"`, as in `to_df`.'),
            P('pretty_ts', 'bool', 'Timestamps as such; `False` gives UTC nanoseconds.'),
            P('map_symbols', 'bool', 'Keep the `symbol` column.')],
           'The path written.',
           {'Python': STORE + 'import pyarrow.parquet as pq\n\ndata.to_parquet("trades.parquet")\nprint(pq.read_schema("trades.parquet"))'}),
    Method('historical/store/to_file', 'DataStore.to_file', '.to_file',
           'Write the records as received from the service (Parquet; Databento\'s would be DBN).',
           'DataStore.to_file(\n    path: str | Path,\n) -> Path', [P('path', 'str | Path', 'Where to write.', required=True)],
           'The path written.', {'Python': STORE + 'print(data.to_file("trades.parquet"))'}),
    Method('historical/store/replay', 'DataStore.replay', '.replay',
           'Call a function with each record, in time order. Iterating the store gives the same records.',
           'DataStore.replay(\n    callback: Callable[[dict], object],\n) -> None',
           [P('callback', 'Callable[[dict], object]', 'Called with each record (a dict).', required=True)], '`None`.',
           {'Python': STORE + 'data.replay(lambda record: print(record["ts_event"], record["price"], record["size"]))'}),
]

PYTHON = [
    Method('python/connect', 'briskapi.connect', 'connect',
           'Start a live feed and make it the default source for `Ticker` and `Market`, from a site, as `brisk live --site` '
           'does. `site="demo"` is the public BRiSK Next demo: not a live connection but a recording of 27 September '
           '2021 that the demo site serves as files, downloaded, checked against pinned hashes and replayed at its recorded '
           'pace; `cache=DIR` replays a copy of those files downloaded before. `site="sbi"`, `"e-shiten"`, `"matsui"`, `"monex"` or '
           '`"smbcnikko"` is your broker\'s live BRiSK ([below](/reference/python/broker_connect.md)). A complete session is uploaded to your private '
           'archive unless you opt out; `output` also keeps it on disk. On the command line: `brisk live` (see '
           '[Recording](/guides/recording.md)).',
           'briskapi.connect(\n    *,\n    site: str,                              # "demo", or a broker: "sbi", ...\n'
           '    codes: list[str] | None = None,\n    history: bool = False,\n    upload: bool | None = None,\n'
           '    output: str | Path | None = None,\n    on_day: Callable | None = None,\n    gateway: bool = False,\n'
           '    timeout: float | None = None,\n    runtime: str | None = None,             # Node or Bun: default Bun on a Mac that has it, else Node\n    cache: str | Path | None = None,        # the demo\n'
           '    speed: float = 1,                       # the demo\n'
           '    limit_frames: int | None = None,        # the demo\n    trace_protocol: bool = False,           # a broker\n'
           '    profile: dict | None = None,            # a broker\n    upload_market_data: bool | None = None,  # a broker\n'
           '    fast_symbols: int | None = None,        # a broker\n) -> Feed',
           [P('site', 'str', '`"demo"`, or `"sbi"`, `"e-shiten"`, `"matsui"`, `"monex"` or `"smbcnikko"`: your '
              'broker\'s live BRiSK, signing in as '
              '[`login`](/reference/python/broker_login.md) does unless it already has.', required=True),
            P('codes', 'list[str] | None', 'Securities to decode (default: the whole market).'),
            P('history', 'bool', 'Keep every update and trade, for `Ticker.history()` and `.trades()`.'),
            P('upload', 'bool | None', '`False` uploads nothing; `None` follows your choice (on by default).'),
            P('output', 'str | Path | None', 'Also keep each recording here (a broker\'s day comes as several), as '
              '`SOURCE_YYYYMMDD_FROM-TO` (a broker\'s `SITE_YYYYMMDD_FROM-TO`).'),
            P('on_day', 'Callable | None', 'Called with each recording\'s result (kept, uploaded).'),
            P('gateway', 'bool', 'Also serve the feed to `briskapi.Live()` on this computer (`brisk live` does by default).'),
            P('timeout', 'float | None', 'Seconds to wait for the initial state (default 60, or 120 for a broker; a broker '
              'feed with no session to join waits for the next trading day\'s).'),
            P('cache', 'str | Path | None', 'The demo from a copy of its files downloaded before (default: download them).'),
            P('speed', 'float', 'The demo\'s replay speed: 1 is the recorded pace, 0 as fast as possible.'),
            P('limit_frames', 'int | None', 'Stop the demo after this many frames (a partial replay is never uploaded).'),
            P('trace_protocol', 'bool', 'A broker\'s connection steps on stderr, tokens redacted.'),
            P('profile', 'dict | None', 'A broker connection\'s unverified Socket.IO parameters.'),
            P('upload_market_data', 'bool | None', '`False` uploads a broker session\'s timing summary but not its market '
              'recording.'),
            P('fast_symbols', 'int | None', 'A broker feed with `gateway`: how many symbols `briskapi.Live` sessions will want '
              'on its fast lane (default 500 to 1,000, by the cores); each 500 takes a worker from the recording.')],
           'A [`Feed`](/reference/python/feed_quotes.md), already holding every selected security\'s state.',
           {'Python': 'import briskapi\n\nfeed = briskapi.connect(site="demo", codes=["7203"], upload=False)\n'
            'print(feed)\nprint(briskapi.Ticker("7203").quote()["indicative_price"])\nfeed.close()',
            'CLI': 'brisk live --site demo --codes 7203 --no-upload --quotes   # one JSON line per quote update'}, unrun=('CLI',)),
    Method('python/record', 'briskapi.record', 'record',
           'Record the demo replay into a folder (Parquet tables, see [Data format](/data-format.md)), upload it unless you '
           'opt out, and make it the default source. On the command line: `brisk record` (see '
           '[Recording](/guides/recording.md)).',
           'briskapi.record(\n    output: str | Path,\n    cache: str | Path | None = None,\n'
           '    codes: list[str] | None = None,\n    speed: float = 1,\n    limit_frames: int | None = None,\n    upload: bool | None = None,\n) -> Recording',
           [P('output', 'str | Path', 'The folder to write.', required=True),
            P('cache', 'str | Path | None', 'Replay a copy of the demo\'s files downloaded before (default: download them).'),
            P('codes', 'list[str] | None', 'Securities to record (default: the whole market).'),
            P('speed', 'float', '1 is the recorded pace, 0 as fast as possible.'),
            P('limit_frames', 'int | None', 'Stop early (never uploaded).'),
            P('upload', 'bool | None', '`False` keeps it local.')],
           'The `Recording`.',
           {'Python': 'import briskapi\n\nrec = briskapi.record("recordings/s1", speed=0, upload=False)\n'
            'print(rec.manifest["summary"]["batches"], rec.manifest["bytes"])',
            'CLI': 'brisk record --speed 0 --no-upload --output recordings/s1'}),
    Method('python/load', 'briskapi.load', 'load',
           'Make a recording (a folder, or a `Recording`) or a live `Feed` the default source for `Ticker` and `Market`.',
           'briskapi.load(\n    source: str | Path | Recording | Feed,\n) -> Recording | Feed',
           [P('source', 'str | Path | Recording | Feed', 'What to read.', required=True)], 'The source.',
           {'Python': DEMO + 'print(briskapi.Market().summary()["trading_date"])'}),
    Method('python/pull', 'briskapi.pull', 'pull',
           'Download one of your recordings from your archive, verify it and make it the default source. Without `output`, '
           'downloads are cached.',
           'briskapi.pull(\n    prefix: str,\n    output: str | Path | None = None,\n) -> Recording',
           [P('prefix', 'str', 'The recording\'s archive prefix, from `briskapi.recordings()`.', required=True),
            P('output', 'str | Path | None', 'Keep it in this folder instead of the cache.')],
           'The `Recording`.',
           {'Python': 'import briskapi\n\nprefix = briskapi.recordings()[0]["prefix"]\nrec = briskapi.pull(prefix)\nprint(rec)',
            'CLI': 'brisk pull PREFIX --output recordings/s1'},
           {'Python': "Recording('/Users/you/.cache/brisk/archive/3f…/20261013/085800-153512-7c14…')"}, illustrative=True),
    Method('python/recordings', 'briskapi.recordings', 'recordings',
           'Your recordings in your private archive.',
           'briskapi.recordings(\n    date: str | None = None,\n    source: str | None = None,\n) -> Table',
           [P('date', 'str | None', 'Only this trading date (`YYYYMMDD`).'),
            P('source', 'str | None', '`brisk_live` (every broker\'s) or `historical_mock` (the demo).')],
           'A `Table` of `{prefix, source, trading_date, securities, batches, quote_updates, bytes, sha256}`.',
           {'Python': 'import briskapi\n\nprint(briskapi.recordings(source="brisk_live").to_pandas())', 'CLI': 'brisk list --source brisk_live'},
           {'Python': '                                      prefix    source trading_date  securities  batches  quote_updates     bytes  …\n'
            '0  archive/3f…/20261013/085800-113000-7c14…  brisk_live   20261013        4445   201834        8913442  51230119  …'},
           illustrative=True),
    Method('python/consent', 'briskapi.consent', 'consent',
           'Show, accept or revoke automatic uploads to your private archive. Uploads are on by default (`default: True` '
           'until you decide). See the [privacy policy](/privacy.md).',
           'briskapi.consent(\n    accept: bool = False,\n    revoke: bool = False,\n) -> dict',
           [P('accept', 'bool', 'Turn uploads on (making your identity if there is none).'),
            P('revoke', 'bool', 'Turn uploads off until you accept again.')],
           'The choice: `{policy_version, enabled, decided_at}`, or the default.',
           {'Python': 'import briskapi\n\nprint(briskapi.consent())', 'CLI': 'brisk consent'}),
]

TICKER = [
    Method('python/ticker_quote', 'Ticker.quote', 'Ticker.quote',
           'A security\'s quote at a time: the latest update at or before it, from the loaded recording or live feed.',
           'briskapi.Ticker(code).quote(\n    at: str | datetime | None = None,\n    raw: bool = False,\n) -> dict',
           [P('at', 'str | datetime | None', 'A JST time (`"09:00:00"`) or `datetime`. Default: the latest.'),
            P('raw', 'bool', 'BRiSK\'s own fields (tenths of a yen, microseconds) instead of yen and `datetime`s.')],
           'A dict of the quote\'s fields (see [Data format](/data-format.md#local-and-cloud-names)).',
           {'Python': DEMO + 'quote = briskapi.Ticker("7203").quote(at="09:00:00")\n'
            'for key in ("time", "indicative_price", "indicative_volume",\n            "last_price", "bid_price", "ask_price", "volume"):\n'
            '    print(key, quote[key])'}),
    Method('python/ticker_history', 'Ticker.history', 'Ticker.history',
           'Every update of a security, oldest first. Reads the loaded recording (exactly its span) or the live feed (with '
           '`history=True`: every update since it connected); never the network. For more than one day, use '
           '[`Historical`](/reference/historical/timeseries/get_range.md).',
           'briskapi.Ticker(code).history(\n    start: str | datetime | None = None,\n    end: str | datetime | None = None,\n    raw: bool = False,\n) -> Table',
           [P('start', 'str | datetime | None', 'From this JST time.'), P('end', 'str | datetime | None', 'Until this JST time.'),
            P('raw', 'bool', 'BRiSK\'s own fields.')],
           'A `Table` (a list of dicts with `.to_pandas()`).',
           {'Python': DEMO + 'updates = briskapi.Ticker("7203").history(start="09:00:00", end="09:00:01").to_pandas()\n'
            'print(updates[["time", "last_price", "volume", "bid_price", "ask_price"]].head())'}),
    Method('python/ticker_trades', 'Ticker.trades', 'Ticker.trades',
           'Every trade of a security, oldest first: price, shares and the initiating side (`buy`, `sell`, or None for most '
           'opening prints). Reads the loaded recording or live feed (with `history=True`, back to the day\'s start as far as '
           'BRiSK\'s snapshot restores them).',
           'briskapi.Ticker(code).trades(\n    start: str | datetime | None = None,\n    end: str | datetime | None = None,\n    raw: bool = False,\n) -> Table',
           [P('start', 'str | datetime | None', 'From this JST time.'), P('end', 'str | datetime | None', 'Until this JST time.'),
            P('raw', 'bool', 'BRiSK\'s own fields.')], 'A `Table` of `{code, issue_id, time, price, quantity, side, frame}`.',
           {'Python': DEMO + 'trades = briskapi.Ticker("7203").trades(end="09:00:01")\nprint(trades.to_pandas()[["time", "price", "quantity", "side"]])'}),
    Method('python/ticker_auction', 'Ticker.auction', 'Ticker.auction',
           'The auction at a time: the indicative price and volume, market orders, the board\'s totals and the market-order '
           'imbalance (buy minus sell). `closing=True` gives the closing auction (closing-condition orders).',
           'briskapi.Ticker(code).auction(\n    at: str | datetime | None = None,\n    closing: bool = False,\n) -> dict',
           [P('at', 'str | datetime | None', 'A JST time. Default: the latest.'),
            P('closing', 'bool', 'The closing auction instead of the session\'s.')], 'A dict of the auction\'s state.',
           {'Python': DEMO + 'auction = briskapi.Ticker("7203").auction(at="09:00:00")\nfor key, value in auction.items():\n    print(f"{key}: {value}")'}),
    Method('python/ticker_info', 'Ticker.info', 'Ticker.info',
           'The security\'s master entry: name, lot size, tick type, base price and daily limits (yen).',
           'briskapi.Ticker(code).info() -> dict', [], 'A dict.', {'Python': DEMO + 'print(briskapi.Ticker("7203").info())'}),
    Method('python/ticker_candles', 'Ticker.candles', 'Ticker.candles',
           'BRiSK\'s own price bars, from your broker\'s BRiSK servers (needs [`briskapi.login`](/reference/python/broker_login.md)). `5m` covers '
           'today only; how far back `1d`, `1w` and `1mo` go is up to BRiSK and not yet known. For bars from your own '
           'recordings, use [`Historical`](/reference/historical/timeseries/get_range_bars.md).',
           'briskapi.Ticker(code).candles(\n    interval: str = "1d",\n) -> Table',
           [P('interval', 'str', '`5m`, `1d`, `1w` or `1mo`.')], 'A `Table` of `{date (or year/week/month), open, high, low, close, turnover}`.',
           {'Python': 'import briskapi\n\nbriskapi.login()   # cookies saved by `brisk login`\n'
            'print(briskapi.Ticker("7203").candles("1d").to_pandas().tail(2))'},
           {'Python': '         date    open    high     low   close      turnover\n'
            '58  2026-10-09  2880.0  2905.0  2861.5  2899.0  4.12e+10\n59  2026-10-13  2902.0  2930.0  2895.0  2921.5  3.87e+10'},
           illustrative=True),
    Method('python/ticker_margin', 'Ticker.margin', 'Ticker.margin',
           'Margin balances and stock-lending fees (JSFC), one row per trading day, from your broker\'s BRiSK.',
           'briskapi.Ticker(code).margin(\n    days: int = 365,\n) -> Table', [P('days', 'int', 'How many trading days back.')],
           'A `Table`, one row per day: `date`, `long_balance`, `short_balance`, `preliminary_long`, `preliminary_short`, '
           '`standardized_long`, `standardized_short` (shares) and the lending fee: `lending_fee`, `lending_fee_pct`, '
           '`lending_fee_days`, `lending_fee_max`.',
           {'Python': 'import briskapi\n\nbriskapi.login()\nprint(briskapi.Ticker("7203").margin(days=5).to_pandas())'},
           {'Python': '         date  long_balance  short_balance  preliminary_long  preliminary_short  …  lending_fee  lending_fee_pct  …\n'
            '0  2026-10-13       1203400         512800           1198700             508300  …         0.05            0.002  …'},
           illustrative=True),
]

MARKET = [
    Method('python/market_snapshot', 'Market.snapshot', 'Market.snapshot',
           'Every security\'s quote at a time.', 'briskapi.Market().snapshot(\n    at: str | datetime | None = None,\n    raw: bool = False,\n) -> Table',
           [P('at', 'str | datetime | None', 'A JST time. Default: the latest.'), P('raw', 'bool', 'BRiSK\'s own fields.')],
           'A `Table`, one quote per security.',
           {'Python': DEMO + 'quotes = briskapi.Market().snapshot(at="09:01:00").to_pandas()\n'
            'top = quotes.sort_values("volume", ascending=False).head()\nprint(top[["code", "last_price", "volume"]])'}),
    Method('python/market_imbalances', 'Market.imbalances', 'Market.imbalances',
           'Securities ranked by the size of their market-order imbalance (buy minus sell); `closing=True` ranks the closing '
           'auction\'s.', 'briskapi.Market().imbalances(\n    at: str | datetime | None = None,\n    top: int | None = None,\n    closing: bool = False,\n) -> Table',
           [P('at', 'str | datetime | None', 'A JST time. Default: the latest.'), P('top', 'int | None', 'Only the first this many.'),
            P('closing', 'bool', 'The closing auction.')], 'A `Table` of auction states, the largest imbalance first.',
           {'Python': DEMO + 'top = briskapi.Market().imbalances(at="09:00:00", top=5).to_pandas()\n'
            'print(top[["code", "indicative_price", "market_order_imbalance"]])'}),
    Method('python/market_special_quotes', 'Market.special_quotes', 'Market.special_quotes',
           'Securities in a special quote (特別気配) at a time: its side, price and when its timer ends.',
           'briskapi.Market().special_quotes(\n    at: str | datetime | None = None,\n) -> Table', [P('at', 'str | datetime | None', 'A JST time.')],
           'A `Table`.', {'Python': DEMO + 'print(briskapi.Market().special_quotes(at="09:01:00").to_pandas().head())'}),
    Method('python/market_trades', 'Market.trades', 'Market.trades',
           'Every security\'s trades in time order, from the loaded recording or live feed.',
           'briskapi.Market().trades(\n    start: str | datetime | None = None,\n    end: str | datetime | None = None,\n    raw: bool = False,\n) -> Table',
           [P('start', 'str | datetime | None', 'From this JST time.'), P('end', 'str | datetime | None', 'Until this JST time.'),
            P('raw', 'bool', 'BRiSK\'s own fields.')], 'A `Table` of trades.',
           {'Python': DEMO + 'trades = briskapi.Market().trades(end="09:00:00.1").to_pandas()\nprint(trades[["code", "time", "price", "quantity", "side"]].head())'}),
    Method('python/market_stocks', 'Market.stocks', 'Market.stocks',
           'The securities master: every security in the recording or feed.', 'briskapi.Market().stocks() -> Table', [],
           'A `Table` of master entries.', {'Python': DEMO + 'print(briskapi.Market().stocks().to_pandas().head(3))'}),
    Method('python/market_summary', 'Market.summary', 'Market.summary',
           'The source, trading date, coverage and clock range of the recording or feed.', 'briskapi.Market().summary() -> dict', [],
           'A dict.', {'Python': DEMO + 'for key, value in briskapi.Market().summary().items():\n    print(f"{key}: {value}")'}),
    Method('python/market_frames', 'Market.frames', 'Market.frames',
           'Every batch (frame) of a recording: clocks, local timing, skipped issues, quote and trade counts.',
           'briskapi.Market().frames() -> Table', [], 'A `Table`, one row per batch.',
           {'Python': DEMO + 'frames = briskapi.Market().frames().to_pandas()\n'
            'print(frames[["seq", "type", "time", "quote_count", "trade_count"]].head())'}),
]

BROKER_DATA = 'import briskapi\n\nbriskapi.login()   # cookies saved by `brisk login`\n'

MARKET_BROKER = [
    Method('python/market_turnover', 'Market.turnover', 'Market.turnover',
           'Turnover and shares outstanding for every listed stock, from your broker\'s BRiSK.',
           'briskapi.Market().turnover() -> Table', [], 'A `Table` of `{code, turnover, shares_outstanding}`.',
           {'Python': BROKER_DATA + 'print(briskapi.Market().turnover().to_pandas().head(3))'},
           {'Python': '   code      turnover  shares_outstanding\n0  1301  1.52e+08             10928283\n'
            '1  1332  1.13e+09            312430277\n2  1333  4.07e+08             52656910'}, illustrative=True),
    Method('python/market_lists', 'Market.lists', 'Market.lists',
           'BRiSK\'s curated stock lists, such as the Nikkei 225 and recent IPOs, by list ID, from your broker\'s BRiSK.',
           'briskapi.Market().lists() -> dict[str, list[str]]', [], 'A dict of list ID to codes.',
           {'Python': BROKER_DATA + 'for name, codes in briskapi.Market().lists().items():\n    print(name, len(codes), codes[:3])'},
           {'Python': 'LIST_ID 225 [\'1332\', \'1333\', \'1605\']\nLIST_ID 38 [\'135A\', \'136A\', \'138A\']'}, illustrative=True),
    Method('python/market_events', 'Market.events', 'Market.events',
           'The day\'s market events, such as basket orders, limit up or down and volume surges, from your broker\'s BRiSK.',
           'briskapi.Market().events(\n    first: int = 0,\n    last: int = 618,\n) -> Table',
           [P('first', 'int', 'The first event index.'), P('last', 'int', 'The last event index.')],
           'A `Table` of `{index, code, kind, type, price, value, change_bps, time}` (prices in yen).',
           {'Python': BROKER_DATA + 'print(briskapi.Market().events().to_pandas().tail(2))'},
           {'Python': '   index  code  kind  type   price  value  change_bps                             time\n'
            '0    611  6758     …     0  3155.0    NaN        52.0 2026-10-13 10:41:07.120000+09:00\n'
            '1    612  7203     …     0  2921.5    NaN       -31.0 2026-10-13 10:41:09.884000+09:00'}, illustrative=True),
    Method('python/market_schedule', 'Market.schedule', 'Market.schedule',
           'The trading date, the session\'s status and the session times (JST), from your broker\'s BRiSK.',
           'briskapi.Market().schedule() -> dict', [],
           'A dict: `date`, `status`, `morning_pre_open`, `morning_open`, `morning_close`, `afternoon_pre_open`, '
           '`afternoon_open`, `afternoon_pre_close`, `afternoon_close`.',
           {'Python': BROKER_DATA + 'for key, value in briskapi.Market().schedule().items():\n    print(f"{key}: {value}")'},
           {'Python': 'date: 20261013\nstatus: …\nmorning_pre_open: 2026-10-13 08:00:00+09:00\nmorning_open: 2026-10-13 09:00:00+09:00\n'
            'morning_close: 2026-10-13 11:30:00+09:00\nafternoon_pre_open: 2026-10-13 12:05:00+09:00\n'
            'afternoon_open: 2026-10-13 12:30:00+09:00\nafternoon_pre_close: 2026-10-13 15:25:00+09:00\n'
            'afternoon_close: 2026-10-13 15:30:00+09:00'}, illustrative=True),
    Method('python/market_watchlist', 'Market.watchlist', 'Market.watchlist',
           'The codes saved in your BRiSK watchlist at your broker.',
           'briskapi.Market().watchlist() -> list[str]', [], 'A list of codes.',
           {'Python': BROKER_DATA + 'print(briskapi.Market().watchlist())'}, {'Python': "['7203', '6758', '9984']"},
           illustrative=True),
]

FEED = [
    Method('python/feed_quotes', 'Feed.quotes', 'Feed.quotes',
           'Iterate a live feed\'s updates as they arrive, starting with each selected security\'s current quote. A slow '
           'consumer slows the feed rather than losing updates.',
           'feed.quotes(\n    codes: list[str] | str | None = None,\n    raw: bool = False,\n    timeout: float | None = None,\n) -> Iterator[dict]',
           [P('codes', 'list[str] | str | None', 'Only these securities.'), P('raw', 'bool', 'BRiSK\'s own fields.'),
            P('timeout', 'float | None', 'Seconds to wait for each update.')], 'An iterator of quotes.',
           {'Python': 'import briskapi\nfrom itertools import islice\n\nfeed = briskapi.connect(site="demo", codes=["7203"], upload=False)\n'
            'for quote in islice(feed.quotes("7203"), 3):\n    print(quote["time"], quote["indicative_price"], quote["volume"])\nfeed.close()'}),
    Method('python/feed_on_quote', 'Feed.on_quote', 'Feed.on_quote',
           'Call a function for each update, on the feed\'s thread. An exception in it stops the feed and is raised by '
           '`wait()`.',
           'feed.on_quote(\n    callback: Callable[[dict], object],\n    codes: list[str] | str | None = None,\n    raw: bool = False,\n) -> Callable[[], None]',
           [P('callback', 'Callable[[dict], object]', 'Called with each quote.', required=True),
            P('codes', 'list[str] | str | None', 'Only these securities.'), P('raw', 'bool', 'BRiSK\'s own fields.')],
           'A function that unsubscribes.',
           {'Python': 'import briskapi\n\nfeed = briskapi.connect(site="demo", codes=["7203"], speed=0, upload=False)\nseen = []\n'
            'stop = feed.on_quote(lambda q: seen.append(q["indicative_price"]), codes="7203")\nfeed.wait()\nprint(len(seen), seen[:3])'}),
    Method('python/feed_snapshot', 'Feed.snapshot', 'Feed.snapshot',
           'Every selected security\'s current quote on a live feed.',
           'feed.snapshot(\n    raw: bool = False,\n) -> Table', [P('raw', 'bool', 'BRiSK\'s own fields.')], 'A `Table`, one quote per security.',
           {'Python': 'import briskapi\n\nfeed = briskapi.connect(site="demo", codes=["7203", "6758"], upload=False)\n'
            'print(feed.snapshot().to_pandas()[["code", "indicative_price", "indicative_volume"]])\nfeed.close()'}),
]

ARCHIVE = [
    Method('python/archive_upload', 'Archive.upload', 'Archive.upload',
           'Upload a packaged recording to your private archive and wait for its validation.',
           'briskapi.Archive().upload(\n    directory: str | Path,\n    timeout: float = 660,\n) -> dict',
           [P('directory', 'str | Path', 'A recording folder with its `manifest.json`.', required=True),
            P('timeout', 'float', 'Seconds to wait for validation.')], 'The stored recording\'s `{status, prefix, sha256}`.',
           {'Python': 'import briskapi\n\nprint(briskapi.Archive().upload("recordings/s1"))', 'CLI': 'brisk upload recordings/s1'},
           {'Python': "{'status': 'stored', 'prefix': 'archive/3f…/20261013/085800-153512-7c14…', 'sha256': '7c14…'}"},
           illustrative=True),
    Method('python/archive_delete', 'Archive.delete', 'Archive.delete',
           'Remove one of your recordings (or timing reports) from your archive. It can be restored for 30 days, then it is '
           'removed.', 'briskapi.Archive().delete(\n    prefix: str,\n) -> dict', [P('prefix', 'str', 'The recording\'s prefix.', required=True)],
           '`{status, removed, note}`.', {'Python': 'import briskapi\n\nprint(briskapi.Archive().delete(prefix))', 'CLI': 'brisk delete PREFIX'},
           {'Python': "{'status': 'deleted', 'removed': 7, 'note': 'Removed from your archive; `brisk restore` brings it back within 30 days.'}"},
           illustrative=True),
    Method('python/archive_deleted', 'Archive.deleted', 'Archive.deleted',
           'What you deleted from your archive and can still restore: for 30 days after deleting it.',
           'briskapi.Archive().deleted(\n    date: str | None = None,\n) -> Table', [P('date', 'str | None', 'Only this trading date (`YYYYMMDD`).')],
           'A `Table` with each one\'s `prefix` and `manifest` (or a timing report\'s `key` and `report`), `deleted_at` and '
           '`restore_until`.',
           {'Python': 'import briskapi\n\nprint(briskapi.Archive().deleted().to_pandas())', 'CLI': 'brisk list --deleted'},
           {'Python': '                                      prefix         manifest            deleted_at         restore_until\n'
            '0  archive/3f…/20261013/085800-153512-7c14…  {\'schema\': …}  2026-10-14T01:02:03Z  2026-11-13T01:02:03Z'}, illustrative=True),
    Method('python/archive_restore', 'Archive.restore', 'Archive.restore',
           'Bring back a recording (or timing report) you deleted in the last 30 days. `Archive().deleted()` lists them.',
           'briskapi.Archive().restore(\n    prefix: str,\n) -> dict', [P('prefix', 'str', 'The recording\'s prefix.', required=True)],
           '`{status, restored}`.', {'Python': 'import briskapi\n\nprint(briskapi.Archive().restore(prefix))', 'CLI': 'brisk restore PREFIX'},
           {'Python': "{'status': 'restored', 'restored': 7}"}, illustrative=True),
    Method('python/archive_prune', 'Archive.prune', 'Archive.prune',
           'Free disk space: remove a local recording your archive holds, after checking its SHA-256. `pull` brings it back.',
           'briskapi.Archive().prune(\n    directory: str | Path,\n) -> dict', [P('directory', 'str | Path', 'The local recording.', required=True)],
           '`{pruned, bytes, prefix}`.', {'Python': 'import briskapi\n\nprint(briskapi.Archive().prune("recordings/s1"))', 'CLI': 'brisk prune recordings/s1'},
           {'Python': "{'pruned': 'recordings/s1', 'bytes': 51230119, 'prefix': 'archive/3f…/20261013/085800-153512-7c14…'}"},
           illustrative=True),
]

BROKER = [
    Method('python/broker_login', 'briskapi.login', 'login',
           'Sign in to your broker\'s BRiSK with its session cookies: given (copied from your logged-in browser), from the '
           'site\'s variable (`BRISK_SBI_COOKIES`, `BRISK_ESHITEN_COOKIES`, `BRISK_MATSUI_COOKIES`, `BRISK_MONEX_COOKIES`, '
           '`BRISK_SMBCNIKKO_COOKIES`; JSON) or saved by `remember=True` or by '
           '`brisk login` (passkey). `Ticker` and `Market`\'s broker data (candles, margin, turnover, lists, events, schedule, '
           'watchlist) then come from it, and `connect(site=...)` uses it. `briskapi.logout()` forgets it and the saved cookies.',
           'briskapi.login(\n    site: str = "sbi",\n    *,\n    cookies: dict | None = None,\n    remember: bool = False,\n) -> Client',
           [P('site', 'str', '`"sbi"`, `"e-shiten"`, `"matsui"`, `"monex"` or `"smbcnikko"`.'), P('cookies', 'dict | None', 'The site\'s session cookies.'),
            P('remember', 'bool', 'Save them (an owner-only file).')], 'The session client.',
           {'Python': 'import briskapi\n\nbriskapi.login("e-shiten")   # BRISK_ESHITEN_COOKIES, or cookies saved by `brisk login`'},
           {'Python': '<briskapi.brisk.Client object>'}, illustrative=True),
    Method('python/broker_connect', 'briskapi.connect (your broker)', 'connect: broker',
           'Your broker\'s live BRiSK feed, decoded by BRiSK\'s own decoder under Node or Bun. It records one trading '
           'day after another until closed, keeping (`output`) and uploading each day as two recordings, its morning '
           'and afternoon sessions. Without `output`, a recording is kept only in your archive, and with uploads off, nowhere. On the command line: '
           '`brisk live --site sbi` or another broker\'s site (see [Recording](/guides/recording.md)).',
           'briskapi.connect(\n    *,\n    site: str,\n    codes: list[str] | None = None,\n    history: bool = False,\n'
           '    upload: bool | None = None,\n    upload_market_data: bool | None = None,\n    output: str | Path | None = None,\n'
           '    on_day: Callable | None = None,\n    gateway: bool = False,\n    fast_symbols: int | None = None,\n'
           '    trace_protocol: bool = False,\n    profile: dict | None = None,\n    timeout: float | None = None,\n) -> Feed',
           [P('site', 'str', '`"sbi"`, `"e-shiten"`, `"matsui"`, `"monex"` or `"smbcnikko"`. Uses the session [`login`](/reference/python/broker_login.md) made, '
              'or signs in as it does.', required=True),
            P('codes', 'list[str] | None', 'Securities to decode.'), P('history', 'bool', 'Keep every update and trade.'),
            P('upload', 'bool | None', '`False` uploads nothing.'),
            P('upload_market_data', 'bool | None', '`False` uploads the timing summary but not the market recording.'),
            P('output', 'str | Path | None', 'Keep each recording here as `SITE_YYYYMMDD_FROM-TO` (its broker\'s site).'),
            P('on_day', 'Callable | None', 'Called with each recording\'s result.'),
            P('gateway', 'bool', 'Also serve the feed to `briskapi.Live()` on this computer (`brisk live` does by default).'),
            P('fast_symbols', 'int | None', 'With `gateway`: how many symbols `briskapi.Live` sessions will want on the feed\'s '
              'fast lane (default 500 to 1,000, by the cores); each 500 takes a worker from the recording.'),
            P('trace_protocol', 'bool', 'Print the connection steps, tokens redacted.'),
            P('profile', 'dict | None', 'Unverified Socket.IO parameters, should the server open with Socket.IO.'),
            P('timeout', 'float | None', 'Seconds to wait for the initial state (default 120). With no session to join '
              '(after the close, on a holiday), the feed waits for the next trading day\'s however long that is.')],
           'A [`Feed`](/reference/python/feed_quotes.md).',
           {'Python': 'import briskapi\n\nfeed = briskapi.connect(site="sbi", codes=["7203"], output="days")\n'
            'for quote in feed.quotes("7203"):\n    print(quote["time"], quote["last_price"])',
            'CLI': 'brisk live --site sbi --codes 7203 --output days'},
           {'Python': '2026-10-13 09:00:00.012000+09:00 2902.0\n2026-10-13 09:00:00.481000+09:00 2903.5'}, illustrative=True),
]

# The navigation of the reference: (group, [methods]).
REFERENCE = [
    ('Cloud API', CLOUD),
    ('Python API', PYTHON + TICKER + MARKET + MARKET_BROKER + FEED + ARCHIVE + BROKER),
]
