# Cloud API

[English](cloud-api.md) | [日本語](cloud-api.ja.md)

A Databento-style historical API over each uploader's private archive: query your own
recordings by dataset, symbols, schema and time range, across days, without downloading whole
recordings. Status: phase 1 is built for 1.0 (see [Phases](#phases)); phases 2 and 3 are
plans. It answers once `infra/deploy.py` has deployed the `brisk-recorder-query` function.
Facts about Databento below were checked against its documentation and the `databento-python`
source on 2026-10-13. The code is `query_engine.py`, `query_service.py` and
`briskapi/_historical.py`, with the schemas and their projections in `briskapi/records.py`
(shared with `briskapi.Live`); [ARCHITECTURE.md](../ARCHITECTURE.md) describes how it works
inside and the [README](../README.md) how to use it.

Decided (2026-10-13):
- our own formats (Parquet, CSV, JSON), not Databento's binary DBN;
- free, with quotas;
- your archive identity is your API key;
- the datasets are `XTKS.BRISK` and `XTKS.BRISKDEMO`;
- OHLCV bars are built once per trading day from every upload (the shared bars), and an
  identity receives only the bars its own uploads cover (moved into phase 1);
- (after 1.0.0) each owner's bars are built from its own uploads only, ahead of queries: the
  shared bars let a made-up upload receive, and change, other owners' bars (see
  [OHLCV bars](#ohlcv-bars));
- requests filter and sort on `ts_event`, and times without a zone are JST.

## Goals

- **Your data, queryable.** One request returns, for example, Toyota's trades or ten-level book
  for a week of your BRiSK sessions, merged and time-ordered, as a DataFrame-ready table.
- **Databento's shape.** The same endpoints, parameters, dataset naming and record schemas, so
  that code and habits carry over.
- **Private by construction.** A request reaches only its owner's recordings and the bars built
  from them, plus the public demo for any identity. Nothing is built from several owners'
  uploads.
- **Cheap to run.** Serverless, reading only the Parquet row groups a request needs; no database.

Non-goals: live streaming from the cloud (live records come from the feed on your own computer,
through `briskapi.Live`, in these same schemas, bars included), other owners' records, SQL, DBN for
now.

## What Databento's historical API is, and what is built

Base URL `https://hist.databento.com/v0/`; the API key is the HTTP basic-auth username with an
empty password. Ours is a function URL with the same `/v0/` paths and key convention (a Bearer
token works too).

| Endpoint | Purpose | Here |
| --- | --- | --- |
| `POST timeseries.get_range` (form body) | The data: `dataset`, `symbols` (comma-joined, up to 2,000, or `ALL_SYMBOLS`), `schema`, `start` (inclusive), `end` (exclusive), `stype_in`, `stype_out`, `limit`, `encoding` (`dbn`, `csv`, `json`), `compression`, and for CSV/JSON `pretty_px`, `pretty_ts`, `map_symbols`. Filters on `ts_recv` where the schema has it, else `ts_event`. | Built, as a POST form or a GET. No `stype_out` (`stype_in` is `raw_symbol` only) and no `dbn`: `encoding` is `parquet` (the default), `csv` or `json`. Filters on `ts_event`. |
| `GET metadata.list_datasets`, `list_schemas`, `list_fields`, `list_publishers` | Catalogue. Publishers pair a dataset with a venue: `{publisher_id, dataset, venue, description}`. | Built. `list_fields` gives each column's name, type and meaning ([data-format.md](data-format.md)). |
| `GET metadata.get_dataset_range`, `get_dataset_condition` | Available dates; per-day quality. | Built. The range is the first and last date with data; the condition is `available` or `missing` per date, with the clock windows (JST) your recordings cover. |
| `GET metadata.get_record_count`, `get_billable_size`, `get_cost` | Size and price of a request before running it. | `get_record_count` runs the query and counts its rows; `get_cost` is 0. `get_billable_size` is phase 3. |
| `GET symbology.resolve` | Map symbols between types (`raw_symbol`, `instrument_id`, ...) over a date range. | Phase 2. |
| `POST batch.submit_job`, `GET batch.list_jobs`, `batch.list_files` (+ download) | Large requests as asynchronous jobs whose files are downloaded later. | Phase 3. |

Datasets are named `VENUE.FEED`: the venue's ISO 10383 market identifier code (MIC), then the
feed. Examples: `XNAS.ITCH` (Nasdaq TotalView-ITCH), `GLBX.MDP3` (CME Globex MDP 3.0),
`XNYS.PILLAR`, `XCIS.TRADESBBO`. Records carry `ts_event` and, where captured, `ts_recv`
(nanoseconds, UTC), `instrument_id` (uint32) and `publisher_id`.

## Datasets, symbols and instruments

Following that convention, the venue is the Tokyo Stock Exchange (MIC `XTKS`, as in our
NautilusTrader instrument IDs such as `7203.XTKS`) and the feed is BRiSK:

| Dataset | Data | Who can query it |
| --- | --- | --- |
| `XTKS.BRISK` | Every live recording you uploaded, whichever broker's BRiSK it came through, and the OHLCV bars built from them | You |
| `XTKS.BRISKDEMO` | The public BRiSK Next demo (TSE, 2021-09-27), from the archive's one verified copy | Every identity |

- **One live dataset.** The broker site a recording came through doesn't matter: every site runs
  the same BRiSK app, which serves the same boot, master, snapshot and decoder (checked on
  2026-10-08). Overlapping captures of a day, from any site, are merged and deduplicated (see
  below).
- **The public demo is open to every identity.** `XTKS.BRISKDEMO` reads
  `reference/historical_mock/TIMELINE/`, the market tables kept once for everyone, so any
  identity can query it, whether or not it uploaded a replay. A replay has no meaningful local
  receipt time, so its records carry `ts_recv = ts_event`. The copy appears with the first
  verified whole-recording demo upload from any identity; `deploy.py --seed-demo PACKAGE`
  makes one at deployment. Until then `XTKS.BRISKDEMO` answers HTTP 404. Its bars are built
  from the demo's trades on the fly and are not gated.
- `list_datasets` returns both datasets, always. `list_publishers`, in Databento's form:

  | `publisher_id` | `dataset` | `venue` | `description` |
  | --- | --- | --- | --- |
  | 1 | `XTKS.BRISK` | `XTKS` | Tokyo Stock Exchange via BRiSK |
  | 2 | `XTKS.BRISKDEMO` | `XTKS` | Tokyo Stock Exchange, BRiSK Next demo replay |

- `raw_symbol` is the JPX code (`7203`, `167A`): four or five characters, 0-9 and A-Z.
  `instrument_id` has to stay the same across days, but a recording's `issue_id` is only an
  index into that day's master. So the ID is derived from the code:
  - digits-only codes are their number (Toyota stays 7203);
  - alphanumeric ones (JPX has issued them since 2024) are 100,000 plus their base-36 value
    (`167A` → 100,000 + 54,694 = 154,694).

  Codes have at most five characters, so this is unique and fits in uint32.
  `symbology.resolve` (phase 2) will report both, per date.

## Schemas from BRiSK recordings

Recordings already hold everything needed; each schema is a projection or a small derivation.
Field names follow Databento's record definitions, so a DataFrame of `trades` or `mbp-10` has
the columns a Databento user expects. Every schema but the bars starts with `ts_recv`,
`ts_event`, `publisher_id` and `instrument_id` and ends with `symbol`.

| Schema | From | Mapping |
| --- | --- | --- |
| `trades` | `data/trades.parquet` | `action` T; price, size; `side` from the aggressor (`B` buyer-initiated, `A` seller-initiated, `N` otherwise, as for most opening prints); `sequence` = frame; `trade_id` |
| `mbp-10` | `data/order_book_depths.parquet` | Each quote update is the ten best levels per side after it, flattened to `bid_px_00`, `bid_sz_00`, `bid_ct_00` … `ask_ct_09` (null past a side's last level). BRiSK gives the book, not the order that changed it, so `action` and `side` are `N`, `depth` is 0, `price` null and `size` 0. The bootstrap batch's rows carry the snapshot flag (`flags` 32). |
| `book-deltas` | `data/order_book_depths.parquet`'s `changes` and `checkpoint` | Every price level of both books (bid and ask, regular and closing-condition), all day, as Databento's MBO records at price-level grain (BRiSK has no order IDs): `action` `A` a level appears, `M` it changes, `C` it empties, `R` a security's books are cleared before a snapshot; `side`, `closing`, `price`, `size` and `count` (orders) after the event; `flags` 128 on an update's last record, 32 on a snapshot's. A security's first update in a recording is a snapshot (`R` then its levels). For each request the engine finds each security's last checkpoint before `start` (the packager writes its whole books every 1,000 changes) from row-group statistics and the checkpoint column, replays at most that many changes, and returns the records in range; `snapshot=true` puts each security's books as of `start` first. Overlapping recordings of a day are not merged (each one's changes are from its own previous update): the longest is read. Broker recordings from 1.9 on; none for the demo. |
| `imbalance` | the auction fields | BRiSK's strength: per update an opening (`O`) row where the opening indicative price shows and a closing (`C`) row where the closing one does, in `auction_type`. `ind_match_price` = indicative price, `paired_qty` = indicative volume, `market_imbalance_qty`/`_side` = market buy minus sell, `bid_total_qty`/`ask_total_qty` = the board totals (the closing ones for `C`), collars = the day's limit prices, `auction_time` = special-quote timer. |
| `definition` | `data/equity.parquet` | One row per security per recording, stamped JST midnight of its trading day: code, name, lot size, tick increment, base price, limits, issue and tick types |
| `brisk-quotes` (ours) | depths, all columns | Everything BRiSK records for each update: closing-condition books, board totals, special-quote time (decimals as float64) |
| `ohlcv-1s/1m/1h/1d` | trades | Aggregated from trades, as Databento does; see [below](#ohlcv-bars) |

Planned for phase 2:

| Schema | From | Mapping |
| --- | --- | --- |
| `mbp-1`, `bbo-1s/1m`, `tbbo` | the same depths | Top level; sampled; or each trade with the book before it |
| `status` | `issue_status`, special quotes | Trading status changes (special quote on/off, halts as BRiSK reports them) |
| `statistics` | open, last, volume, turnover | Databento `stat_type`s where one fits; turnover as a BRiSK extension |

**Encodings.**
- `parquet` (the default for our client): timestamps as UTC nanoseconds
  (`timestamp[ns, UTC]`), prices as float64 yen. Always zstd; `compression` does not apply.
- `csv` and `json` (one record per line), with Databento's `pretty_px` (prices as numbers in
  yen; off, integers in units of 1e-9 yen, Databento's fixed point), `pretty_ts` (ISO 8601 UTC
  timestamps with nanoseconds; off, integer nanoseconds) and `map_symbols` (keep the code)
  switches. Nested columns of `brisk-quotes` are JSON text in CSV.
- `compression=zstd` optional for CSV and JSON.

**Timestamps and times.**
- `ts_event` is BRiSK's server stamp for the frame (a trade's own time for trades). Whether that
  is the exchange's event time is not established (see ARCHITECTURE.md), and the docs must say
  so.
- `ts_recv` is the uploader's local receipt time (`received_unix_ms` of the frame's batch;
  millisecond precision), or `ts_event` where there is none.
- Requests filter and sort on `ts_event`, not on `ts_recv` as Databento does: overlapping
  captures of one day agree on BRiSK's stamp and differ in their receipt times, so the same
  range returns the same rows whichever capture holds them. Ties sort by `instrument_id`, then
  `sequence`.
- A record that several of your captures hold keeps the earliest of their receipt times: the
  first moment a program on any of them could have known it.
- `start` and `end` are ISO 8601, a date or an integer. A date is that JST trading day's
  midnight (so an `end` date excludes the day), a time without a zone is JST, `Z` or an offset
  is honoured, and an integer of ten or more digits is UTC nanoseconds. `end` is exclusive and
  defaults to the end of the start's trading day. A request touches at most 31 JST trading
  dates.

## OHLCV bars

`Historical`'s bars come only from the cloud: the engine (`query_engine.py`) is server side and
not in the `briskapi` wheel. (`briskapi.Live` builds bars from the stream on your computer, by the
same rules.)

- **Building.** From trades, per security and interval. Intervals align to JST (a day bar runs
  JST midnight to midnight); a bar's `ts_event` is its start; an interval without a trade has
  no bar. Open and close are the first and last trade by `ts_event`, then frame, then place in
  the frame; `volume` is the sum of sizes (uint64). Auction prints are trades like any other.
  Columns: `ts_event`, `publisher_id`, `instrument_id`, `open`, `high`, `low`, `close`,
  `volume`, `symbol`.
- **Built ahead, per owner.** For `XTKS.BRISK` an owner's bars for a trading day are built from
  its own current live recordings of that day, with each trade counted once (deduplicated by
  security and trade ID), into `bars/OWNER/YYYYMMDD/ohlcv-1s|1m|1h|1d.parquet`, sorted by
  security so that a query reads only its securities' row groups. The archive service rebuilds
  them when one of those recordings is stored, deleted or restored; with none left the files
  are removed, and a rebuild that fails removes them too (queries then build the bars from the
  recordings, slower but the same).
- **Coverage.** A day's files hold only the bars whose span, clipped to the Tokyo Stock
  Exchange's trading sessions (09:00 to 11:30:01 and 12:30 to 15:30:01 JST, half open, the extra
  second holding the closing auction's prints), lies within the merged clock ranges (first to
  last source clock) of the owner's recordings that hold that security; a bar they cover only in
  part would be missing trades. A bar outside every session is judged on its whole span. A
  recording from 08:55 to 11:31, for example, gets the morning's one-minute and one-hour bars
  but not the day bar. So uploading is how you get bars.
- **Only your own uploads.** Within its coverage an owner's own recordings hold every trade:
  ingest refuses a recording whose trades don't account for every change in each security's
  cumulative volume and turnover between its quotes, so a recording that missed a trade is never
  stored. Others' recordings could add nothing there, only disagree. 1.0.0 built one set of
  shared bars per day from every owner's recordings; a made-up recording could then claim a
  whole day's coverage to receive everyone's bars, or, from an owner ID that sorts first,
  replace real prints in everyone's (`test_no_upload_reveals_or_changes_another_owners_bars`).
- **Speed.** On a day of 1,000,000 trades across 3,774 securities (local, without S3 latency):
  building an owner's four files takes about 0.8 s; a query reads one-minute bars for one
  security in about 1 ms and for every security in about 60 ms (one-second bars: 2 ms and
  140 ms), against about 0.5 s to build them from the recording per request. 1.0.0 applied
  coverage at query time, one pass per security: about 25 s for such a day's four bar sizes.
  [PRIVACY.md](../PRIVACY.md) (policy version 7) describes the bars to uploaders.

## Request flow

```
client ──HTTPS──▶ Lambda function URL "brisk-recorder-query" (Python 3.12 arm64, 3,008 MB, pyarrow layer)
                    │ 1 authenticate: the identity (Basic username or Bearer) → owner ID; count the request
                    │ 2 plan: for each JST trading date in [start, end), list archive/OWNER/DATE/ and read
                    │   each live recording's manifest and files.json
                    │   (XTKS.BRISKDEMO: the reference copy instead)
                    │ 3 resolve symbols per recording from its equity table (code → issue_id)
                    │ 4 read: pyarrow over S3 ranged GETs (4 MiB blocks), only row groups whose issue_id and
                    │   ts_event statistics can match (tables are sorted by issue, then batch);
                    │   bars: the owner's built file for the day, row groups pruned by symbol
                    │ 5 merge recordings, deduplicate, sort by ts_event, project to the schema, encode
                    │ 6 count the result's bytes against the daily quota
                    ▼
              ≤ 4 MiB: inline response (Parquet as base64)
              larger: results/OWNER/<uuid>.<ext> in S3, then 303 to a 15-minute presigned URL
```

- **Several recordings of one day** (a restart, two computers, two sites) can overlap. Rows are
  merged and deduplicated, keeping the first, by (code, trade ID) for trades, (code, time) for
  definitions, (code, frame, auction type) for imbalance and (code, frame) otherwise. BRiSK's
  frame numbers are the server's, so overlapping captures of the stream should agree; to be
  confirmed on live data, across sites included.
- **Row-group pruning.** Writing a Parquet page index (`write_page_index`) would make
  single-symbol reads finer still. It changes the canonical encoding, so it needs a schema version
  bump on client and service together.
- **Why 303 and not streaming.** Function URLs return at most 6 MB buffered, and base64 adds a
  third to Parquet, hence 4 MiB inline. Response streaming needs the Node.js runtime or a custom
  one (to re-check), while this code is Python and pyarrow. A 303 to S3 serves any size, and
  HTTP clients follow it. `requests` drops the Authorization header on a cross-host redirect,
  which S3 presigned URLs require, and `briskapi.Historical` follows it by hand without the key.
  Results expire after a day (lifecycle); each large result is its own object.
- **Lambda limits.** 300 seconds and 3,008 MB (the account's maximum) per request; anything
  bigger is a batch job (phase 3). With no recording in range, a result holds only the common
  columns and `symbol`.

## Authentication and isolation

- **The identity is the API key**, sent as the HTTP basic-auth username with an empty password
  (Databento's convention, so `curl -u $KEY:` works) or as `Authorization: Bearer` (as the
  archive API already does). There are no separate keys to create: `brisk identity --export`
  prints it, and `briskapi.Historical()` finds it as the archive does (`BRISK_IDENTITY`, the
  credential store or the file). The same key can also upload, delete and restore, so it should be
  kept like a password. There is no key registry: any well-formed identity authenticates, and one
  with no recordings sees only the demo. A missing or malformed key is HTTP 400. Guessing keys is
  bounded by the per-address limit, counted before the key is read: among 2^128 possible secrets,
  finding one with recordings is hopeless. The client sends the key only over HTTPS and follows a
  303 to an `https` result without it.
- **Isolation.** The query function's role may read `archive/*`, `reference/*`, `bars/*` and
  `results/*`, and write `results/*`, nothing else. Every path into your recordings and bars is
  built from the authenticated owner ID, never from request input. The other prefix a request reads
  is the demo's reference copy, for `XTKS.BRISKDEMO`.

## Limits and cost

- **Free, with quotas:** 60 requests a minute and 2 GB returned a UTC day per identity; since
  identities cost nothing to make, also 120 requests a minute and 4 GB returned a day per IP
  address (an IPv6 address by its /64; requests count before their key is read), and 20 GB
  returned a day for everyone. They use the same DynamoDB counters as uploads (a request over a
  quota gets HTTP 429). Up to 2,000 symbols and 31 days per request, and `limit`. Results up to 4 MiB are returned inline.
- `get_cost` returns 0. `get_record_count` returns the real number by running the query, so it
  costs the operator a query but no returned bytes. `get_billable_size` (phase 3) will come from
  Parquet metadata.
- **Operator cost per request** (Tokyo list prices):
  - Lambda arm64 is about $0.0000133 per GB-second, so 3 GB for 1 s is about $0.00004.
  - S3 GETs cost $0.00037 per thousand.
  - Data out costs about $0.114 per GB beyond AWS's free 100 GB a month.
  - Egress dominates: a user at the daily cap every day is about $7 a month.

## Python client

```python
import briskapi

client = briskapi.Historical()                 # your identity is the key
data = client.timeseries.get_range(dataset="XTKS.BRISK", symbols=["7203"], schema="trades",
                                   start="2026-10-13", end="2026-10-17")
data.to_df()                                   # indexed by ts_recv (ts_event for bars); Databento's other conversions too
client.timeseries.get_range(dataset="XTKS.BRISK", symbols=["7203"], schema="ohlcv-1m",
                            start="2026-10-13")                 # bars where your uploads cover them
client.metadata.get_dataset_condition("XTKS.BRISK", start_date="2026-10-01")   # recorded windows per day
client.timeseries.get_range(dataset="XTKS.BRISKDEMO", symbols=["7203"], schema="imbalance",
                            start="2021-09-27T08:59+09:00", end="2021-09-27T09:03+09:00")   # any identity
```

Same names and parameters as `databento.Historical`, returning a store with `to_df()`,
`to_ndarray()`, `to_csv()`, `to_json()`, `to_parquet()`, `to_file()`, `replay()`, iteration and `len()`, with `to_df()`'s `price_type`, `pretty_ts`, `map_symbols`, `tz` and `count`. It transports Parquet, so no new dependency. The
gateway comes from `query_url` in `archive.json`.

Two additions to Databento's parameters, for tests that must not look ahead (the client's, no new
endpoint). The server cuts on `ts_event`, but a live feed's records reach the recording computer a
little later (`ts_recv`):

```python
client.timeseries.get_range(dataset="XTKS.BRISK", schema="brisk-quotes", start="2026-10-13T08:55",
                            end="2026-10-13T08:59:58", ts_filter="ts_recv")      # what had been received by 08:59:58
client.timeseries.get_snapshot("XTKS.BRISK", at="2026-10-13T08:59:58", schema="brisk-quotes")   # each security's latest record then
```

- `get_range(..., ts_filter="ts_recv", skew=10.0)` asks the server for the window padded by `skew`
  seconds on both sides, then cuts and sorts on `ts_recv` here, with `limit` applied after. It warns
  when BRiSK's stamps ran ahead of the receipt clock by more than half of `skew` (widen it, or
  records received before `end` but stamped well after it would be missing). Not for OHLCV bars.
- `get_snapshot(dataset, at, symbols, schema="brisk-quotes", since=None, ts_filter="ts_recv", skew=10.0)`
  returns one row per security, in `instrument_id` order: its latest record at or before `at` (of those
  received by `at` on the default clock, the latest by BRiSK's stamp and frame, not the last received:
  two captures of a day can hold a security's frames in another order of receipt than the order they
  happened in). BRiSK sends a security's whole state on every update, so that is its book as it stood; a security with no
  record since `since` (default: the JST midnight of `at`'s trading day) is absent. During the session
  pass a later `since`, or the search reads the morning.
- A demo replay has no receipt clock: its `ts_recv` is its `ts_event`, so both filters agree there.
- `get_range(..., schema="book-deltas", snapshot=True)` (the form's `snapshot=true`) begins with each
  security's whole books as of `start`; `get_snapshot(dataset, at, symbols, schema="book-deltas")`
  returns those alone (a clear and the levels, flagged 32), by BRiSK's stamp, rebuilt server-side
  from each security's last checkpoint: `since` does not apply.

On the command line, `brisk query XTKS.BRISK --schema ohlcv-1m --symbols 7203 --start 2026-10-13`
writes CSV to stdout (`--encoding json|parquet`, `--output FILE`, `--end`, `--limit`, and for
`book-deltas` `--snapshot`). Over
plain HTTP, `-L` follows the 303 for a large result and curl drops the key on the way:

```sh
curl -L -u "$KEY:" -d dataset=XTKS.BRISK -d schema=ohlcv-1m -d symbols=7203 \
     -d start=2026-10-13 -d encoding=csv "$QUERY_URL/v0/timeseries.get_range"
```

Errors are `{"detail": ...}` with HTTP 400 (invalid input, no key), 404 (no data, or no demo
copy yet) or 429 (quota).

## Phases

1. **Core (built for 1.0).**
   - `timeseries.get_range` for `trades`, `mbp-10`, `definition`, `imbalance`, `brisk-quotes`
     and `ohlcv-1s/1m/1h/1d`, in Parquet, CSV or JSON.
   - Bars: the shared bars, kept in step on store, delete and restore, and the per-identity
     coverage rule (moved up from phase 2); after 1.0.0, each owner's own bars, built ahead.
   - The metadata endpoints (`list_*`, `get_dataset_range`, `get_dataset_condition`,
     `get_record_count`, `get_cost`).
   - Identity auth, inline or 303 results, quotas, `briskapi.Historical`, `brisk query` and the
     demo reference seeded at deployment (`deploy.py --seed-demo`).
2. **More schemas.** `mbp-1`, `tbbo`, `bbo-*`, `status` and `statistics`, plus
   `symbology.resolve`.
3. **Large requests.**
   - `batch.submit_job`, `list_jobs` and `list_files`: asynchronous Lambda invocations writing to
     `results/OWNER/JOB/`.
   - `get_billable_size` and the Parquet page index.

Later, if wanted: DBN encoding via `databento-dbn`, which would let the official `databento`
client use the archive directly (`databento.Historical(key, gateway=URL)`). Its client accepts any
non-empty key and any https gateway.

Known gaps in phase 1:
- Rebuilds of an owner's day of bars are not serialized; one that finds the owner's recordings of
  the day changed while it ran runs again, up to three times.
- That overlapping captures agree, across sites, is unconfirmed on live data.

## Infrastructure

- A second function, `brisk-recorder-query`, with its own role (`brisk-recorder-query`),
  log group and function URL, in the same zip and layer as the archive function: 3,008 MB (the
  account's maximum), a 300 second timeout and eight reserved concurrent executions where the
  account allows. Ingest is unchanged except that it keeps owners' bars in step, so the archive
  role gains read, write and delete of current objects under `bars/`.
- Bucket: a `results/` prefix with a one-day lifecycle (`ExpireResults`); `bars/` noncurrent
  versions expire after a day (`ExpireReplacedBars`). The query role reads `archive/`,
  `reference/`, `bars/` and `results/` and writes `results/`.
- DynamoDB: query quotas in the existing table.
- `deploy.py` deploys both functions and writes `query_url` to `archive.json`; `--seed-demo
  PACKAGE` uploads a complete demo recording as the operator so that `XTKS.BRISKDEMO` has data
  (`brisk record --no-upload --output DIR` makes one). `cloud_smoke.py` calls
  `list_datasets` and `list_publishers` as a throwaway identity and checks that a request
  without a key is refused; it does not query data, since its synthetic probe belongs to
  neither dataset. Deployment steps are in [infra/README.md](../infra/README.md).
