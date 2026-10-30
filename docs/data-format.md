# Data format

[English](data-format.md) | [日本語](data-format.ja.md)

What the Parquet (and CSV and JSON) that briskapi gives you contains, column by column. It comes
from two places:

- **The cloud API** (`briskapi.Historical`, `brisk query`): records in Databento's shapes, one
  schema per request. [Cloud API records](#cloud-api-records). `briskapi.Live` gives the same
  records live from the feed on your computer: the same columns, types and values for the same
  session (except that a trade reaching this computer after its bar has ended counts in the next
  live bar, where the cloud places it by its own time).
- **Recordings on your computer** (`brisk record`, `brisk pull`, `brisk live --output`): a
  folder of four Parquet tables in NautilusTrader v2's layout, everything the decoder produced.
  [Recordings](#recordings).

`client.metadata.list_fields(schema)` returns the same columns as the tables below, with their
types and meanings.

## Conventions

- **Times** are UTC nanoseconds (Parquet `timestamp[ns, UTC]`). Tokyo is UTC+9, so
  `to_df(tz="Asia/Tokyo")` or `.dt.tz_convert("Asia/Tokyo")` shows them as the exchange does.
  Every record has `ts_event`, BRiSK's server stamp for the frame it arrived in (microsecond
  precision); whether that is the exchange's own event time is not established. Records also
  have `ts_recv`: when the computer that recorded them received the frame (millisecond
  precision). A demo replay has no meaningful receipt time, so there `ts_recv` equals
  `ts_event`.
- **Prices** are yen. The cloud API gives them as float64 (BRiSK's resolution is 0.1 yen), or, with `to_df(price_type="fixed")`, as integers of
  1e-9 yen as Databento does. Recordings keep them exact as `decimal128(38, 16)`. A null price
  means BRiSK showed none (its zero "unavailable" value).
- **Sizes** are shares; **counts** are orders.
- **Codes and IDs.** `symbol` (or `raw_symbol`) is the JPX code: `7203`, or alphanumeric like
  `167A`. `instrument_id` stays the same every day: digits-only codes are their number (7203),
  others are 100,000 plus their base-36 value (`167A` is 154,694). In recordings, `issue_id` is
  only that day's index into the master; use `identifier` (`7203.XTKS`) or the code instead.
- **Raw BRiSK values** (`issue_status`, `quote_flag`, `issue_type`, `tick_type`) are kept as
  BRiSK sends them; their meanings are not all known.

## Cloud API records

Every schema but the bars starts with the same four columns and ends with `symbol`:

| Column | Type | Meaning |
| --- | --- | --- |
| `ts_recv` | timestamp (ns, UTC) | When the uploader's computer received the frame (the demo: `ts_event`) |
| `ts_event` | timestamp (ns, UTC) | BRiSK's server stamp for the frame |
| `publisher_id` | uint16 | 1 for `XTKS.BRISK`, 2 for `XTKS.BRISKDEMO` (`metadata.list_publishers`) |
| `instrument_id` | uint32 | Fixed per code, as above |
| `symbol` | string | The JPX code (left out with `map_symbols=False`) |

Records are in time order (`ts_event`, then `instrument_id`, then `sequence`). Where several
of your recordings overlap, each record appears once (`book-deltas` reads one of them: see there).
`briskapi.Live` delivers them as the feed receives its batches, each batch's records in that order.

### `trades`

| Column | Type | Meaning |
| --- | --- | --- |
| `action` | string | 'T' |
| `side` | string | The aggressor: 'B' buyer-initiated, 'A' seller-initiated, 'N' none (most opening prints) |
| `depth` | uint8 | 0 |
| `price` | float64 | Trade price (yen) |
| `size` | uint64 | Shares |
| `flags` | uint8 | 0 |
| `sequence` | uint64 | BRiSK's frame number |
| `trade_id` | string | FRAME-PLACE: the frame and the trade's place in it; unique per instrument |

### `mbp-10`

| Column | Type | Meaning |
| --- | --- | --- |
| `action` | string | 'N': BRiSK sends books, not the order events that changed them |
| `side` | string | 'N' |
| `depth` | uint8 | 0 |
| `price` | float64 | null |
| `size` | uint64 | 0 |
| `flags` | uint8 | 32 (snapshot) on a session's first update of each security, else 0 |
| `sequence` | uint64 | BRiSK's frame number |
| `bid_px_NN` | float64 | bid level NN price (yen; NN from 00, the best, to 09); null past the last level |
| `bid_sz_NN` | uint64 | bid level NN shares |
| `bid_ct_NN` | uint32 | bid level NN orders |
| `ask_px_NN` | float64 | ask level NN price (yen; NN from 00, the best, to 09); null past the last level |
| `ask_sz_NN` | uint64 | ask level NN shares |
| `ask_ct_NN` | uint32 | ask level NN orders |

### `book-deltas`

Every price level of a security's books, all day, as what changed: Databento's MBO records, at the
grain BRiSK gives (price levels with their shares and order counts; BRiSK has no order IDs). A
security's first update of a recording clears its books (`R`) and adds every level (`A`), flagged
32; each update after that holds a record for each level that appears (`A`), changes (`M`) or
empties (`C`), its last flagged 128. Replayed in order they give the whole books at every update:
the regular bid and ask books and the closing-condition orders' (`closing`). `snapshot=True`
(Historical's `get_range`, `brisk query --snapshot`, `Live.subscribe` with a `start`) begins with
each security's books as of the start, flagged 32, so the records after it apply to known books;
`Historical.timeseries.get_snapshot(schema="book-deltas")` returns those alone. The cloud API
rebuilds them from a checkpoint the recording holds every 1,000 changes, never from the day's
start. Recordings from before briskapi 1.9, and the demo's, have none. Where two of your
recordings of a day overlap in time, the longer one's are read: each recording's changes are from
its own previous update.

| Column | Type | Meaning |
| --- | --- | --- |
| `action` | string | 'A' a price level appears, 'M' its shares or orders change, 'C' it empties, 'R' the security's books are cleared (a snapshot's levels follow) |
| `side` | string | 'B' a bid level, 'A' an ask level, 'N' for 'R' |
| `closing` | bool | The closing-condition orders' book, not the regular one |
| `price` | float64 | The level's price (yen); null for 'R' |
| `size` | uint64 | Shares at the level after this event (0 for C) |
| `count` | uint32 | Orders at the level after this event (0 for C) |
| `flags` | uint8 | 128 on an update's last record; 32 on a snapshot's (a security's first update, or the books as of the start with snapshot=true) |
| `sequence` | uint64 | BRiSK's frame number |

### `definition`

| Column | Type | Meaning |
| --- | --- | --- |
| `ts_recv` | timestamp (ns, UTC) | The trading date's JST midnight |
| `ts_event` | timestamp (ns, UTC) | The trading date's JST midnight |
| `publisher_id` | uint16 | 1 for XTKS.BRISK, 2 for XTKS.BRISKDEMO (metadata.list_publishers) |
| `instrument_id` | uint32 | Fixed per code: digits-only codes are their number, others 100,000 plus base 36 |
| `raw_symbol` | string | The JPX code |
| `exchange` | string | 'XTKS' |
| `currency` | string | 'JPY' |
| `min_lot_size_round_lot` | int64 | Shares per trading unit |
| `min_price_increment` | float64 | BRiSK's price resolution (0.1 yen), not the tick size, which depends on price and tick_type |
| `high_limit_price` | float64 | The day's upper price limit (yen) |
| `low_limit_price` | float64 | The day's lower price limit (yen) |
| `base_price` | float64 | The day's base price (yen) |
| `name` | string | The security's name |
| `issue_type` | uint64 | Raw BRiSK issue type |
| `tick_type` | uint64 | Raw BRiSK tick table |
| `symbol` | string | The JPX code (left out with map_symbols=false) |

### `imbalance`

| Column | Type | Meaning |
| --- | --- | --- |
| `auction_type` | string | 'O' the session's auction, 'C' the closing-condition orders' |
| `ref_price` | float64 | Where indicative matching starts (yen; see reference_price) |
| `ind_match_price` | float64 | Indicative matching price (yen) |
| `paired_qty` | uint64 | Shares that would match there |
| `side` | string | 'A' when sell exceeds buy at that price (or nothing matches), else 'B' |
| `market_buy_qty` | uint64 | Market buy orders (shares) |
| `market_sell_qty` | uint64 | Market sell orders (shares) |
| `market_imbalance_qty` | uint64 | The difference between market buy and market sell orders, without sign (shares) |
| `market_imbalance_side` | string | 'B' more market buying, 'A' more selling, 'N' equal |
| `bid_total_qty` | uint64 | Shares bid on the whole board |
| `ask_total_qty` | uint64 | Shares offered on the whole board |
| `upper_collar` | float64 | The day's upper price limit (yen) |
| `lower_collar` | float64 | The day's lower price limit (yen) |
| `auction_time` | timestamp (ns, UTC) | When the special quote's timer ends; null without one |
| `sequence` | uint64 | BRiSK's frame number |

### `brisk-quotes`

| Column | Type | Meaning |
| --- | --- | --- |
| `bids` | list of {price, size, count} | The ten best bid levels, best first: {price (yen), size (shares), count (orders)} |
| `asks` | list of {price, size, count} | The ten best ask levels, best first: {price, size, count} |
| `flags` | uint8 | 16, plus 32 on a session's first update of each security (the snapshot) |
| `sequence` | uint64 | BRiSK's frame number |
| `last_price` | float64 | Last traded price (yen); null when BRiSK shows none |
| `open_price` | float64 | The session's opening price (yen) |
| `bid_price` | float64 | Best bid (yen) |
| `ask_price` | float64 | Best ask (yen) |
| `indicative_price` | float64 | Indicative matching price of the auction (yen), excluding closing-condition orders |
| `closing_indicative_price` | float64 | The closing-condition orders' indicative price (yen) |
| `indicative_open_price` | float64 | Indicative opening price (yen) |
| `reference_price` | float64 | Where indicative matching starts: the special quote price, else the last, else the base price (yen) |
| `max_frame` | uint64 | The latest frame BRiSK had seen for this update |
| `indicative_volume` | uint64 | Shares that would match at the indicative price |
| `indicative_imbalance_side` | uint64 | 1 when sell exceeds buy at the indicative price (or nothing matches), else 0 |
| `closing_indicative_volume` | uint64 | The same for the closing-condition orders |
| `closing_indicative_imbalance_side` | uint64 | The same for the closing-condition orders |
| `quote_flag` | uint64 | Special-quote flags (raw BRiSK value) |
| `quote_side` | uint64 | Special-quote side (raw: 0 buy, 1 sell) |
| `volume` | uint64 | The day's cumulative volume (shares) |
| `issue_status` | uint64 | The broker's issue status (raw BRiSK value) |
| `market_buy_quantity` | uint64 | Market buy orders (shares) |
| `market_sell_quantity` | uint64 | Market sell orders (shares) |
| `closing_market_buy_quantity` | uint64 | Closing-condition market buy orders (shares) |
| `closing_market_sell_quantity` | uint64 | Closing-condition market sell orders (shares) |
| `bid_quantity` | uint64 | Shares at the best bid |
| `bid_orders` | uint64 | Orders at the best bid |
| `ask_quantity` | uint64 | Shares at the best ask |
| `ask_orders` | uint64 | Orders at the best ask |
| `bid_total_quantity` | uint64 | Shares bid on the whole board |
| `bid_total_orders` | uint64 | Bid orders on the whole board |
| `bid_closing_total_quantity` | uint64 | Closing-condition shares bid |
| `bid_closing_total_orders` | uint64 | Closing-condition bid orders |
| `ask_total_quantity` | uint64 | Shares offered on the whole board |
| `ask_total_orders` | uint64 | Ask orders on the whole board |
| `ask_closing_total_quantity` | uint64 | Closing-condition shares offered |
| `ask_closing_total_orders` | uint64 | Closing-condition ask orders |
| `special_quote_time` | timestamp (ns, UTC) | When the special quote's timer ends; null without one |
| `turnover` | float64 | The day's cumulative turnover (yen) |
| `closing_bids` | list of {price, size, count} | The closing-condition orders' bid levels, like bids |
| `closing_asks` | list of {price, size, count} | The closing-condition orders' ask levels |

### `ohlcv-1s`, `ohlcv-1m`, `ohlcv-1h`, `ohlcv-1d`

| Column | Type | Meaning |
| --- | --- | --- |
| `ts_event` | timestamp (ns, UTC) | The bar's start (JST-aligned; a day bar starts at the trading date's JST midnight) |
| `open` | float64 | First trade price (yen) |
| `high` | float64 | Highest (yen) |
| `low` | float64 | Lowest (yen) |
| `close` | float64 | Last trade price (yen) |
| `volume` | uint64 | Shares traded |

Bars exist only for intervals with trades, and are built from your own uploads only. You
receive a bar only when they cover that security and the bar's whole span within the trading
sessions (09:00–11:30 and 12:30–15:30 JST); `XTKS.BRISKDEMO` bars are open to everyone.

### CSV and JSON

`encoding="csv"` or `"json"` (one JSON object per line) gives the same columns.
- **Timestamps** are ISO 8601 strings, or UTC nanoseconds with `pretty_ts=false`.
- **Prices** are numbers in yen, or integers of 1e-9 yen with `pretty_px=false`.
- **`brisk-quotes`' book columns** are JSON arrays: in JSON as they are, in CSV as JSON text.
- **`compression=zstd`** compresses the whole response.

## Local and cloud names

`Ticker` and `Market` (on a live feed or a loaded recording) and the cloud API return the same
data under different names: the local calls keep BRiSK's, with yen and Tokyo times; the cloud
API follows Databento's.

| Local (`Ticker`, `Market`) | Cloud API | Note |
| --- | --- | --- |
| `code` | `symbol` (`raw_symbol` in `definition`) | The JPX code |
| `issue_id` | `instrument_id` | Locally, that day's index into the master; in the cloud, fixed per code |
| `time` | `ts_event` | Tokyo `datetime` locally; UTC nanoseconds in the cloud (`to_df(tz="Asia/Tokyo")`) |
| `received_unix_ms` (`Market().frames()`) | `ts_recv` | When the recording computer received the frame |
| `frame` | `sequence` | BRiSK's frame number |
| `trades()`: `price`, `quantity`, `side` (`buy`, `sell`, None) | `trades`: `price`, `size`, `side` (`B`, `A`, `N`) | |
| `bids`, `asks`: `[{price, quantity, orders}]` | `mbp-10`: `bid_px_NN`, `bid_sz_NN`, `bid_ct_NN` (and `ask_…`); `brisk-quotes`: `bids`, `asks` as `[{price, size, count}]` | Best level first |
| `indicative_price`, `closing_indicative_price` | `imbalance`: `ind_match_price`, on `auction_type` `O` and `C` rows | |
| `indicative_volume`, `closing_indicative_volume` | `imbalance`: `paired_qty` | |
| `indicative_imbalance_side` (1 or 0) | `imbalance`: `side` (`A` or `B`) | 1 is `A`: sell exceeds buy |
| `market_buy_quantity`, `market_sell_quantity` (and `closing_…`) | `imbalance`: `market_buy_qty`, `market_sell_qty` | |
| `auction()`: `market_order_imbalance` (buy − sell) | `imbalance`: `market_imbalance_qty` and `market_imbalance_side` | Signed locally; size and side in the cloud |
| `bid_total_quantity`, `ask_total_quantity` (and `bid_closing_…`) | `imbalance`: `bid_total_qty`, `ask_total_qty` | The closing ones on `C` rows |
| `reference_price` | `imbalance`: `ref_price` | |
| `special_quote`: `{side, price, until}` | `imbalance`: `auction_time`; `brisk-quotes`: `quote_side`, `special_quote_time` | |
| `stocks()`: `name`, `lot_size`, `base_price`, `limit_up`, `limit_down`, `tick_type`, `issue_type` | `definition`: `name`, `min_lot_size_round_lot`, `base_price`, `high_limit_price`, `low_limit_price`, `tick_type`, `issue_type` | |
| The other quote fields (`last_price`, `volume`, `turnover`, …) | `brisk-quotes`, same names | |

OHLCV bars (`ohlcv-1s` to `ohlcv-1d`) come from the cloud API, or live from `briskapi.Live`;
there are none locally. `Ticker.candles()` is a different thing: BRiSK's own bars from your broker.

## Recordings

A recording is a folder holding `data/order_book_depths.parquet`, `data/trades.parquet`,
`data/equity.parquet` and `data/custom/BriskBatch.parquet` (plus `manifest.json` once
packaged). The tables follow NautilusTrader v2's schemas, with BRiSK's fields after them. Rows
are sorted by issue, then batch, in row groups of 65,536, so reading one security reads only
its part. `briskapi.Recording(path)`, `Ticker` and `Market` read them for you;
`brisk export DIR --nautilus CATALOG` writes a catalog NautilusTrader v2 loads.

| File | One row per | Columns |
| --- | --- | --- |
| `data/order_book_depths.parquet` | Quote update | `bids`, `asks` (up to ten levels, best first: `price`, `size`, `count`, and v2's `order_id`, always 0), `flags`, `sequence` (the frame), `ts_event`, `identifier` (`CODE.XTKS`), `issue_id`, `seq` (the batch), then the quote fields as in `brisk-quotes` above, with prices as decimals; in a broker's recording from briskapi 1.9 on, then `changes` (what of the four books changed since the security's previous update: `book` 0 bids, 1 asks, 2 and 3 the closing-condition orders', `price`, `size`, `count`; 0 shares for a level that emptied; the first update holds them whole) and `checkpoint` (null, but after every 1,000 changes of a security its whole books, in the same form) |
| `data/trades.parquet` | Execution | `price`, `size`, `aggressor_side` (`BUY`, `SELL`, `NO_AGGRESSOR`), `trade_id`, `ts_event`, `identifier`, `issue_id`, `seq`, `frame`, `side` (BRiSK's raw type: 1 buyer-initiated, 2 seller-initiated) |
| `data/equity.parquet` | Security | v2 `Equity`: `id` (`CODE.XTKS`), `raw_symbol` (the code), `currency`, `price_precision`, `price_increment`, `lot_size`, `max_price`/`min_price` (the day's limits), `ts_event`/`ts_init` (JST midnight); then `issue_id`, `tick_type`, `issue_type`, `name`, `base_price` |
| `data/custom/BriskBatch.parquet` | Decoded batch (frame) | `seq`, `kind` (`bootstrap`, `history`, `quotes`, `end`), `received_unix_ms` (local receipt clock), `decode_ns`, `replay_lateness_ms`, `skipped_issues`, `frames`, `quote_updates`, `replay_wall_ms` (in a day repaired after a crash, the end's `frames` and `replay_wall_ms` are rebuilt from the recording); on the bootstrap row `source`, `trading_date`, `input_transport`, `market_issue_count` |

Receipt times live in `BriskBatch`: join a row's `seq` to the batch's `received_unix_ms`. Each table of updates has
one clock, `ts_event`; recordings made before briskapi 1.5 (format `brisk-nautilus-parquet-v2`) also have
`ts_init`, a copy of it, and the Nautilus export writes it for NautilusTrader.
`history` batches hold trades the session's snapshot restored from earlier that day, all at the
bootstrap's clock. A recording repaired with `brisk repair` also holds batches of their own among the others, for updates another feed's
recording supplied. An update the recording lost while it ran is in a batch placed just before the batch of the next update
of its security, with that batch's `received_unix_ms` and a market clock fitted between its neighbours', so a security's updates
run forward in market time, frame and receipt time alike. What follows everything the recording holds of a security, and
trades, are in batches that carry the other recording's `received_unix_ms`, another computer's clock, so receipt times need
not be steady across those.

### Reading them

```python
import pandas as pd
trades = pd.read_parquet("recordings/s1/data/trades.parquet")
toyota = trades[trades["identifier"] == "7203.XTKS"]

import polars as pl
books = pl.read_parquet("recordings/s1/data/order_book_depths.parquet").filter(pl.col("identifier") == "7203.XTKS")

import duckdb
duckdb.sql("""
    select t.identifier, t.price, t.size, t.ts_event, b.received_unix_ms
    from 'recordings/s1/data/trades.parquet' t
    join 'recordings/s1/data/custom/BriskBatch.parquet' b using (seq)
    order by t.ts_event
""")
```

Decimals read as `decimal.Decimal` in pandas; `pd.read_parquet(...).astype({"price": float})`
or Polars' `cast(pl.Float64)` gives floats.
