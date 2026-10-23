# briskapi

[English](https://github.com/honvl/BRiSKapi/blob/main/README.md) | [日本語](https://github.com/honvl/BRiSKapi/blob/main/README.ja.md) | [Documentation](https://honvl.github.io/BRiSKapi/)

An unofficial Python API and `brisk` command line for BRiSK auction data, with
Databento-style `Live` and `Historical` clients. Consume a live feed, query recordings at any
point in time, keep them in your own private archive, and use BRiSK at your broker (SBI Securities,
Tachibana e-shiten's BRiSK Next, Matsui, Monex or SMBC Nikko) with your own account, signing in with a
passkey if you like. This
is an independent project, not affiliated with or endorsed by BRiSK, Tachibana,
SBI, TSE or JPX.

> **Data sources:**
> - **No account needed:** the public BRiSK Next demo of 27 September 2021 (one
>   pre-open snapshot and the first three minutes after the open), replayed at
>   its recorded pace. This is not live market data.
> - **With your broker's BRiSK (SBI Securities, Tachibana e-shiten's BRiSK Next, Matsui, Monex or SMBC Nikko):**
>   BRiSK market data (candles, margin, alerts, schedule, watchlist) and a
>   live feed.

## Which API should I use?

| You want | Use | It reads |
| --- | --- | --- |
| Quotes, trades and auctions as they happen | `briskapi.connect()`, then `Ticker`/`Market` | Your live feed |
| The same as Databento-style records (`trades`, `mbp-10`, `imbalance`, ...), in any program on this computer | `briskapi.Live()` | The feed `brisk live` runs here (no login of its own) |
| A recording you have, offline or in NautilusTrader | `briskapi.load(dir)` or `briskapi.pull(prefix)`, then `Ticker`/`Market`; `brisk export` | That recording, exactly as recorded |
| History across days, or OHLCV bars, without downloading | `briskapi.Historical()`, `brisk query` | Your uploads in the cloud (the demo for everyone) |
| BRiSK's own bars, margin, turnover, lists, events | `Ticker.candles()`, `.margin()`, `Market().turnover()` and the like | Your broker's BRiSK servers (needs a login) |
| What you have uploaded | `briskapi.Archive()`, `brisk list`, `pull`, `delete`, `restore`, `prune` | Your private archive |

Recording isn't on this list: `brisk live`, `brisk record` and their Python forms record as
they run, and what they record is read through the calls above ([Recording](#recording)).

`Ticker.history()` and `.trades()` never go to the network. They read the loaded recording
or the live feed:
- A recording gives exactly the span it covers.
- A live feed needs `history=True`. It then keeps every update since it connected, and trades
  back to the day's start, as far as BRiSK's snapshot restores them.

`Ticker.candles()` is the call that asks your broker's BRiSK. `5m` covers today only; how far
back `1d`, `1w` and `1mo` go is up to BRiSK and not yet known. For bars from your own
recordings, across days, use `Historical` (`ohlcv-1s` to `ohlcv-1d`).

The local calls and the cloud API name the same data differently (`indicative_price` locally,
`ind_match_price` in the cloud, for example). [docs/data-format.md](https://github.com/honvl/BRiSKapi/blob/main/docs/data-format.md#local-and-cloud-names)
maps one to the other. `briskapi.Live` uses the cloud API's names and records.

## Install

Free-threaded Python 3.14 (`python3.14t`) and Node 22+ (or Bun 1.4+, which BRiSK uses on a Mac that has it). The feed records the whole market,
whose opening auction changes thousands of books within seconds, by reading, parsing and
writing on several cores at once, which Python does only without the GIL (a Python with it
works, but warns that it can fall behind at the open). BRiSK's own decoder is a WebAssembly
module that runs under Node.

```sh
pip install 'briskapi[pandas]'      # `import briskapi` and the `brisk` command
```

BRiSK's decoder and demo data are downloaded at runtime; the package doesn't
include them. Optional prebuilt Rust tools (`brisk_quote_ingest`,
`brisk_recording`) for Linux, macOS and Windows are attached to each
[GitHub release](https://github.com/honvl/BRiSKapi/releases). To work from source,
clone the repository and run `pip install -e '.[pandas]'`.

On Windows, run from PowerShell with Windows' own Python and Node:

```powershell
py install 3.14t
py -3.14t -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e '.[pandas]'
.\.venv\Scripts\brisk.exe live --site demo --codes 7203 --speed 0 --limit-frames 3 --no-upload --quotes
```

## Recording

Recording isn't a third API. The live calls record as they run, and `brisk live` and
`brisk record` are their command-line form, with the same code, options and upload choice.

| Command line | Python | Records | Kept on disk | Uploaded |
| --- | --- | --- | --- | --- |
| `brisk live --site sbi` (or `e-shiten`, `matsui`, `monex`, `smbcnikko`) | `briskapi.connect(site="sbi")` | Your broker's live feed until you stop it, normally two recordings per trading day (its sessions) | Each recording, with `--output DIR` (`output=`) | Each recording as it is made, unless you opt out |
| `brisk live --site demo` | `briskapi.connect(site="demo")` | The demo replay, printed as JSON lines | With `--output DIR` | If the replay completes |
| `brisk record --output DIR` | `briskapi.record(DIR)` | The demo replay | Always, in `DIR` | If the replay completes |

What you record is then read through the other two APIs:
- **On your computer:** `briskapi.load("days/sbi_20261013_085800-113000")`, then `Ticker`
  and `Market`, exactly as recorded; `brisk export` for NautilusTrader or JSONL; or the Parquet
  tables themselves ([data format](https://github.com/honvl/BRiSKapi/blob/main/docs/data-format.md)).
  `brisk pull` (`briskapi.pull()`) brings an uploaded recording back to disk.
- **In the cloud:** `briskapi.Historical()` or `brisk query XTKS.BRISK` reads every broker day
  you uploaded, merged across days, with OHLCV bars. Demo recordings aren't in `XTKS.BRISK`;
  everyone can query the same demo as `XTKS.BRISKDEMO`.

While it runs, `brisk live` also serves its feed to `briskapi.Live()` on this computer
([Live records](#live-records-databento-style)): the records as they arrive, not a recording.

While a day runs, it is written to disk as it arrives: Parquet files in a spool folder
(`~/.local/share/brisk/spool`), about as large as the finished recording, so a crash loses at
most its last half-minute. A broker's day normally becomes two recordings as it runs: the morning
session's, kept and uploaded once the lunch break begins, and the afternoon's at the day's end,
starting from a snapshot of the book where the morning's ended. A busy session is split again when a
completed part brings its recording to 80 million quote rows. The demo's replay is one recording.

- **Without `--output`**, each recording goes to your [archive](#your-private-archive) and
  its local copy is removed. With uploads off as well (`--no-upload`, or
  `--no-upload-market-data` for a broker's market recording), that day isn't kept anywhere,
  so add `--output DIR` to keep it.
- **A lost connection** doesn't lose the day. `brisk live --site sbi` (or another broker's site)
  reconnects by itself, after 5 s, then 10 s, 30 s, a minute and every 2 minutes. What it
  recorded before the drop is a complete recording of its own (`..._085800-101500`), kept and
  uploaded like a day, and the rest of the day is recorded from a fresh snapshot
  (`..._102000-153512`). Only the gap is missing; the cloud API merges the two, and if another
  feed recorded that stretch, `brisk repair` can fill it ([Repairing a recording](#repairing-a-recording-from-another-feed)).
  It stops for good when another connection can't help: an expired session, another BRiSK window
  open on your account, or a site that changed.
- **Stopping** (Ctrl-C, `kill`, shutting the computer down, `feed.close()`) ends the day where
  it is, as the close would: the day so far is a complete recording (`..._085800-121500`), kept
  and uploaded like any day. Start again later and the rest of the day is another recording;
  the cloud API merges them.
- **A crash or a forced kill** can't end the day, so it is repaired: the day so far (all but
  its last half-minute, still in the spool) gets the end marker its host never wrote, and is then
  kept and uploaded as that session's settings said. After a crash this happens when you next
  start a feed; for a host that doesn't end within 30 seconds of being asked to stop, right away.
  The repaired end counts the day's quote updates exactly; its frame count and duration, which
  died with the host, are rebuilt from the recording.
- **A save that fails** keeps what it can. A recording whose upload failed is kept in
  `~/.local/share/brisk/recordings` with its manifest, and `brisk upload DIR` retries. One that could not
  be packaged is tried again five minutes later from the same data, three times during the day and once
  more at its end; its tables are kept there if even that fails. If nothing of a day could be saved,
  its spool folder is left in place, with a warning naming it, and the next feed you start finishes it
  (delete the folder to give the day up).

## Live feed

```python
import briskapi

feed = briskapi.connect(site="demo", codes=["7203", "6758"])   # returns once initial state is in
toyota = briskapi.Ticker("7203")
toyota.quote()        # current quote, updated as each frame arrives
toyota.auction()      # indicative price/volume and market-order imbalance

feed.on_quote(lambda q: print(q["code"], q["indicative_price"]), codes="6758")
for q in feed.quotes("7203"):       # one item per update; ends with the session
    if q["last_price"]:
        print("opened at", q["last_price"], q["time"])
        break

briskapi.Market().imbalances(top=10).to_pandas()
feed.wait()           # or feed.close(); `with briskapi.connect(...) as feed:` also works
```

`briskapi.connect()` takes a site, as `brisk live --site` does:
- `site="demo"`: the public BRiSK Next demo. It isn't a live connection: the demo site serves a
  recording of 27 September 2021 as files (BRiSK's decoder, the master, a snapshot and the
  stream), which are downloaded, checked against pinned hashes and replayed at their recorded
  pace (`speed`: `1` real time, `0` as fast as possible; `limit_frames` stops early).
  `cache=DIR` replays a copy of those files downloaded before instead.
- `site="sbi"`, `"e-shiten"`, `"matsui"`, `"monex"` or `"smbcnikko"`: your broker's live BRiSK ([below](#your-brokers-brisk)).

Other options: `codes`, `history=True` (keep updates for `Ticker.history()`), `upload`,
`output`, `on_day` and `gateway`. Callbacks and iterators
first receive each security's current quote, then every update in order. A slow
consumer slows the feed instead of losing updates.

## Live records (Databento-style)

`brisk live` also serves its feed to other programs on this computer, as the records the
cloud API returns. `briskapi.Live` is shaped like Databento's live client and needs no key or
sign-in: only the feed signs in to your broker.

```sh
brisk live --site sbi   # records, uploads and serves XTKS.BRISK until you stop it (--site demo: the demo, XTKS.BRISKDEMO)
```

```python
import briskapi

live = briskapi.Live()          # finds the feed running on this computer
live.subscribe(dataset="XTKS.BRISK", schema="imbalance", symbols=["7203", "6758"])
live.subscribe(dataset="XTKS.BRISK", schema="trades", symbols=["7203"], start=0)   # the day so far, then live
live.subscribe(dataset="XTKS.BRISK", schema="ohlcv-1m", symbols=["7203"])          # each minute's bar as it ends
for record in live:             # or live.add_callback(fn); live.start(); live.block_for_close()
    print(record.schema, record.symbol, record.ts_event)
```

- **Schemas:** `trades`, `mbp-10`, `imbalance`, `definition`, `brisk-quotes` and the OHLCV bars
  `ohlcv-1s` to `ohlcv-1d`, with the same columns and values as `Historical` returns for the same
  session. `Live` builds its bars from the trades it receives, by the cloud's rules.
- **Bars** come as soon as their interval ends, by the feed's clock: when a later batch is
  stamped past the end, or when this computer's clock, less the delay measured from BRiSK to this
  session (the largest of the last 10 seconds), passes it. A trade that arrives after its own
  bar has ended goes into the next bar still open (the cloud places it by its own time), so
  every trade counts and bars come in order. A bar is given only if the
  session saw its whole span within the trading sessions: with `start`, from the feed's start;
  without, the bars that begin after it subscribed.
- **Where a session starts:** with the day's definitions, then each update as it arrives.
  `start` (a time, a date, UTC nanoseconds, or 0 for everything the feed has) first replays
  the feed's trading day from then: the feed has the day from when it started, plus the day's
  trades its snapshot restored. `snapshot=True` begins `mbp-10`, `imbalance` or `brisk-quotes`
  with each security's current quote (`flags` 32).
- **Records** are dicts, also readable as attributes (`record.price`), with `record.schema`.
  They come in the order the feed receives its batches; within a batch, in `Historical`'s
  order. `add_callback(fn, exception_callback)` and `add_stream(path)` (JSON lines; Databento
  writes DBN) take them too. `block_for_close(timeout)` waits for the session's end, and
  `stop()` ends it. Subscribe before or after `start()`: a later subscription gets its own
  replay or snapshot, and its symbols from then on. A session's subscriptions share one
  dataset. `symbology_map` maps `instrument_id` to the code.
- **The fast lane:** a broker feed records the whole market, which at the opening auction can
  fall seconds behind the stream on a slower computer. A session that subscribes to its own
  symbols, without `start`, gets them on the fast lane: read as each frame arrives, ahead of the
  recording, with the same records. The lane takes about 500 symbols per worker it has (a
  quarter of the feed's, so 500 to 1,000 by default). `ALL_SYMBOLS`, a `start` replay, or symbols beyond what it takes come at
  the recording's pace instead (a warning says so and how many it would take); a session whose later subscription no longer
  fits moves there without missing or repeating a record. For a system of a thousand symbols or more, start the feed with
  `brisk live --site sbi --fast-symbols 1500` (`briskapi.connect(fast_symbols=1500)`), which gives the lane workers for them: each
  500 takes one from the recording, so on a computer with fewer cores also set `BRISK_WORKERS`.
- **The latest quotes of many symbols, at once:** a program that decides at a moment (a cut a few seconds before an auction) wants
  each symbol's freshest quote then, not every update on the way. `live.subscribe("XTKS.BRISK", "mbp-10", symbols=codes,
  stream=False)` has the feed keep those symbols fresh on its fast lane and send nothing; `live.latest()` then returns, in
  one request, a pyarrow table of their last quotes (`mbp-10`, `imbalance` or `brisk-quotes` records, `ts_recv` the moment the feed
  received each, so its age is `now - ts_recv`): some 18 ms for 1,500 symbols, however busy the stream. A session either streams
  or keeps quotes for `latest()`. `tools/live_age.py` prints how old a stream's records are, split into BRiSK's delay
  and briskapi's own.
- **Only this computer:** the feed listens on 127.0.0.1 and admits a session only with the key
  it keeps, with its port, in an owner-only file (`~/.config/brisk/gateway/XTKS.BRISK.json`),
  which `Live()` reads. Nothing it serves leaves your computer. A session that falls more than
  1 GB behind is dropped rather than slowing the feed or its recording.
- **Serving:** `brisk live` serves unless you pass `--no-gateway`; one feed per dataset at a
  time. In Python, pass `gateway=True` to `briskapi.connect()`.

## Recordings and your archive

```python
recordings = briskapi.recordings(source="historical_mock")   # yours only; no account needed
if recordings:
    briskapi.pull(recordings[0]["prefix"])      # verify, decode and cache; becomes the default
else:
    briskapi.record("recordings/my-session")   # record the demo (uploaded unless you opt out)
# Or briskapi.load("recordings/my-session") for a local recording folder (or an older events.jsonl[.gz]).

briskapi.Ticker("7203").quote(at="08:59:59.9999")             # available pre-open quote, in JST
briskapi.Ticker("7203").history(start="09:00", end="09:01")   # every update in a window
briskapi.Market().snapshot(at="09:00:00").to_pandas()
```

A recording is a folder of Parquet tables laid out like a NautilusTrader v2 catalog:
`data/order_book_depths.parquet` (every quote update: v2's `OrderBookDepth`, then BRiSK's
other quote fields), `data/trades.parquet` (every execution, as v2 `TradeTick`),
`data/equity.parquet` (the securities master, as v2 `Equity`) and
`data/custom/BriskBatch.parquet` (per-frame timing). The complete demo replay takes about
29 MB. pandas, Polars and DuckDB read the tables directly; `brisk export` writes a
NautilusTrader catalog or JSONL. [docs/data-format.md](https://github.com/honvl/BRiSKapi/blob/main/docs/data-format.md)
describes every column, with examples for pandas, Polars and DuckDB.

Time queries use each quote's timestamp. Toyota's first demo quote is at
08:59:59.993551 JST; an earlier query raises `NotFoundError`. `Market().summary()`
returns both `start` and `end`, including for local recordings and live feeds.
Your archive holds only what you uploaded, under your identity: a random key the tool
makes on your computer and never registers (see [Your private archive](#your-private-archive)).
Listings skip invalid or unsupported recordings with a warning. One sample stays a public
download for everyone: a [raw wire capture](https://brisk-recordings-honvl-tokyo.s3.ap-northeast-1.amazonaws.com/samples/20260311/cca870a51e7f96f16009c3709c3f597d314b22ed8922bb3ea7fa1acc67a0f85f/events.jsonl.gz) (2026-03-11, from 12:12:40 JST), in
the legacy `brisk-sbi-wire-jsonl-v1` format the tool no longer reads.

## Your broker's BRiSK

The same client serves SBI Securities' BRiSK ([sbi.brisk.jp](https://sbi.brisk.jp), the
default site) and those of Tachibana e-shiten, Matsui, Monex and SMBC Nikko (subsections follow); you need a
BRiSK subscription with the broker. For SBI, log in on sbi.brisk.jp in your browser, then
pass its session cookies: copy them from DevTools, or use `pycookiecheat`'s
`chrome_cookies("https://sbi.brisk.jp")`. Or let `briskapi` sign in with your passkey
([below](#signing-in-with-a-passkey-experimental)).

```python
briskapi.login(cookies={"session_bfaf77a2": "v2.local..."})   # remember=True saves them (owner-only file)
toyota = briskapi.Ticker("7203")
toyota.candles("5m").to_pandas()   # price bars: 5m (today), 1d, 1w or 1mo
toyota.margin(days=30)             # margin balances and stock-lending fees
market = briskapi.Market()
market.turnover()     # turnover and shares outstanding, all stocks
market.lists()        # NK225, recent IPOs, …
market.events()       # basket orders, limit up/down, volume surges
market.schedule()     # trading date, status and session times
market.watchlist()    # your saved codes

feed = briskapi.connect(site="sbi", codes=["7203"])   # live; each session uploaded unless you opt out
toyota.quote()                              # same calls as any feed
# upload_market_data=False uploads no market recordings, upload=False nothing; output="days" keeps each day
```

A broker feed records continuously and keeps running across days. It normally packages and uploads each
day in two recordings, the morning session's once the lunch break begins and the afternoon's (starting
from a snapshot of the book where the morning's ended) at the day's end; a busy session is split again at
80 million quote rows, at the next completed spool part. In the cloud API they read as one day. When the site's
session is done or it starts serving the next trading date (checked every 10 minutes), or the server
closes the stream after the market, the day ends: its last recording is packaged and uploaded, with
the day's timing summary (unless you opt out), while the feed waits for the next session and records
it from a fresh snapshot. Started when no session is running
(after the close, on a holiday), the feed waits for the next trading day's: `connect()` returns
once that day has begun (`feed.status` is `"waiting"` meanwhile). Brokers end their sessions at 05:00 each morning, so a
feed that waits overnight keeps waiting once its session has expired (it says `the session has expired; waiting for
new cookies`) and takes up the cookies of a `brisk login` you make after, with no restart. If a passkey is saved
for that broker (`brisk enroll`), the feed signs in again itself, as `brisk login` does (Chrome opens): up to four tries
over the next hour and forty minutes, then it leaves it to you. Each broker has a passkey of its own, so with two
brokers' feeds each signs in with its own; a feed whose broker has none says how to sign in. `output=DIR` (`briskapi.connect(output=...)`, `brisk live --output DIR`)
also keeps each recording in DIR, in a folder named
`SITE_YYYYMMDD_FROM-TO` (its broker's site and its first and last source clock, JST HHMMSS), such as
`sbi_20261013_085800-113000`; every broker's recordings are the same source, `brisk_live`, as all carry BRiSK's
same market data, and the folder names say which broker each came through, so two recording the same session
(to compare them) never meet. `feed.recordings` lists them (`trading_date`,
`name`, `path`, `uploaded`, `timing_uploaded`, the timing summary's on a day's last, `repaired`, and `error`: what failed, packaging or upload), and
`on_day=callback` is called for each. Without `output`, a recording is kept only in your archive, and
with uploads off, nowhere. Stopping the feed (`feed.close()`, Ctrl-C) ends the day there, so what is
left of it is kept and uploaded like any recording; a lost connection reconnects by itself ([Recording](#recording)). SBI and e-shiten allow one BRiSK
session per user (expect the same of the others): close BRiSK in your browser before you start the feed,
or the two cut each other off within seconds.

Results use the conventions below. Errors are `briskapi.brisk.SessionExpiredError` (log in
again), `briskapi.NotFoundError`, `briskapi.brisk.RateLimitError` and `briskapi.brisk.APIError`.
Requests are limited to one per second.

The live feed runs BRiSK's own decoder under Node, downloaded with your session;
no browser is involved. It follows the vendor client's connect sequence: it feeds
the decoder from the first frame, catches the snapshot up to the stream, answers
the server's pings through the decoder and watches the server heartbeat. It hasn't
yet been validated against a live SBI session. The stream is expected to be a plain
binary WebSocket, as in the Next client. Should the server open with Socket.IO
instead, the host joins its `/v2/user` namespace with unverified parameters that
`briskapi.connect(site="sbi", profile={...})` can set. See what the server answers with
`trace_protocol=True` (every token redacted); anything unexpected stops with an
explicit error rather than a guess. Please report what you see. Live sessions now
get through sign-in, the data endpoints, the decoder download, the stock master and
the snapshot (loaded with the session schedule). The WebSocket upgrade to
`api.brisk.jp` was then answered with HTTP 502; those sessions didn't ask for the
`flex` subprotocol the vendor client asks for, which this version does, and whether
that is enough is not known yet. Your SBI cookies
go only to sbi.brisk.jp.

### Tachibana e-shiten BRiSK Next

e-shiten's BRiSK Next ([next.e-shiten.brisk.jp](https://next.e-shiten.brisk.jp)) is the
same app, so the same client serves it. Log in there in your browser and pass its
cookies with `site="e-shiten"`, or let `briskapi` sign in with your e-shiten passkey
([below](#signing-in-with-a-passkey-experimental)): `brisk enroll --site e-shiten` once,
then `brisk login` before `brisk live --site e-shiten`.

```python
briskapi.login("e-shiten", cookies={...})   # or BRISK_ESHITEN_COOKIES; remember=True keeps them apart from SBI's
feed = briskapi.connect(site="e-shiten", codes=["7203"])   # live
```

Everything above applies, recordings and uploads included (the market recordings as they are made
and each day's timing summary); its cookies go only to
next.e-shiten.brisk.jp. e-shiten allows one
BRiSK session at a time: opening it in a second tab or browser has stopped the first
(message 10011), so the feed and BRiSK in your browser can end each other. Before you
record or upload, read what Tachibana's BRiSK Next terms say about storing and reusing
its data.

### Matsui, Monex and SMBC Nikko

BRiSK for Matsui Securities ([matsui.brisk.jp](https://matsui.brisk.jp)), Monex Securities
([monex.brisk.jp](https://monex.brisk.jp)) and SMBC Nikko Securities
([smbcnikko.brisk.jp](https://smbcnikko.brisk.jp)) is the same vendor's app on hosts of their own, so the
same client serves them: `site="matsui"`, `"monex"` or `"smbcnikko"`, with cookies from your browser, the
site's variable (`BRISK_MATSUI_COOKIES`, `BRISK_MONEX_COOKIES`, `BRISK_SMBCNIKKO_COOKIES`; JSON) or a
passkey sign-in (`brisk enroll --site matsui` once, then `brisk login`).

```python
briskapi.login("matsui", cookies={...})   # remember=True keeps them apart from the other brokers'
feed = briskapi.connect(site="matsui", codes=["7203"])   # live
```

Everything above applies, recordings and uploads included (source `brisk_live`, as every broker's,
in folders named `matsui_…`, `monex_…`, `smbcnikko_…`), and each site's cookies go only to its own host. These three have
not been tried with an account yet: should a site's app differ from SBI's, the feed stops with an
error that says what it did not recognise (`--trace-protocol` shows the connection) rather than
record anything doubtful. Expect one BRiSK session per user, as at SBI and e-shiten, and close
BRiSK in your browser first. Before you record or upload, read what your broker's BRiSK terms say about storing and
reusing its data.

## Signing in with a passkey (experimental)

Many brokers now have you sign in with a passkey. If yours does, `briskapi` can do the
sign-in for you and keep the BRiSK session cookies, instead of you copying them from a
browser. It works with the BRiSK sites of SBI, Matsui, Monex, SMBC Nikko and Tachibana e-shiten,
or any broker you add. It needs Chrome, Node 22+ (or Bun 1.4+) and a safe place for the passkey: the macOS Keychain or Windows
Credential Manager (on Linux, an owner-only file you opt into). It runs on macOS and Windows (run it
from PowerShell or `cmd` so it uses Windows' own Chrome and Node, not WSL's) and is also tested on Linux.

### How it works

A passkey is a pair of keys. The broker keeps the public half. The private half normally lives
in your phone, laptop or password manager, where a fingerprint or PIN lets it sign a challenge
the broker sends, which proves it is you. `briskapi` borrows that arrangement. It starts its own
Chrome, which has a *virtual authenticator*: software that plays the part of your phone and signs
without asking. The broker's site cannot tell the difference.

1. **Once, `brisk enroll`.** You pick your broker. A separate Chrome window opens on its login
   page (a temporary profile, not your everyday Chrome). Sign in as you normally would, then
   register a new passkey in the broker's security settings, just as you would for a new phone.
   The virtual authenticator receives it. When the site says it is registered, press Enter in
   the terminal: `briskapi` saves the passkey, under that broker's name, in the macOS Keychain or Windows Credential
   Manager, and Chrome closes. (A single passkey saved by an earlier version is moved to its broker's name the first time it is read.)
   At e-shiten, `enroll` first asks for your user ID, because its passkey form wants the ID before
   the passkey; it is saved with the passkey. The window opens at e-shiten's usual sign-in (user ID,
   PIN and phone-number authentication), and the passkey is registered on the screen shown after
   signing in or under お客様情報 › 設定情報 › パスキー認証.
2. **Each session, `brisk login`.** The same Chrome opens with the saved passkey loaded, goes to the
   login page and presses the passkey button, and the virtual authenticator signs. Open BRiSK from
   the broker's site in that window (or pass `--launch-url` to have it opened for you).
   `briskapi` reads the BRiSK session cookies, saves them and closes Chrome.
3. **Then use it.** For each of the five brokers, `brisk live --site sbi` (or its own site) and the
   `briskapi.brisk` API use the saved cookies. For a broker you add yourself `briskapi` has no data
   client, so the cookies are only saved (under `~/.config/brisk/cookies/`) for your own use.

Your existing passkeys on your phone or in a password manager are not touched. The broker simply
lists one more passkey, which you can delete in its security settings.

### What to know

- **The saved passkey is a credential.** Whoever has it can sign in to your broker account with no
  further check. It is kept only in the Keychain or Credential Manager (`BRISK_PASSKEY_STORE=file` keeps it in an
  owner-only file instead, which is weaker; on Windows that file's privacy comes from your user
  profile folder, which only you and administrators can read), never leaves your machine and is never shown in a
  log. For e-shiten your user ID is kept with it, in the same place. `brisk forget` deletes it here (with
  several saved, `--site` says which); remove it at the broker too.
- **It is experimental.** Each broker's login page, passkey button text and BRiSK launch come from
  its public pages, and none has been tried against a live account. Override them with
  `--login-url`, `--passkey-button` and `--launch-url`, or in `sites.json` below, and please report
  what fails.
- **A broker may refuse it.** Chrome tells the site it is being automated. Check that your
  broker's terms allow this before relying on it.

### Commands

```sh
brisk sites                    # the brokers you can pick, and where their BRiSK lives
brisk enroll [--site matsui]   # once for each broker; you are asked to choose one if you don't say
brisk login [--site matsui]    # each session; --site when passkeys are saved for more than one broker
brisk forget [--site matsui]   # delete a saved passkey
```

Each broker keeps its own passkey, so enrolling a second broker leaves the first alone (`--replace` is for
enrolling the same broker again). With only one enrolled, `brisk login` and `brisk forget` need no `--site`.

From Python: `passkey.enroll(site="matsui")` (for e-shiten, `passkey.enroll(site="e-shiten", user_id="...")`)
and `signin = passkey.login()` (`passkey.login(site="matsui")` when several are saved), where `signin.cookies` are the
BRiSK cookies and `signin.client` is the data client for sites that have one. Two sign-ins at once (two feeds meeting their
expired sessions together) go one after the other, each with its own Chrome.

To add a broker, or correct a built-in one, edit `~/.config/brisk/sites.json`:

```json
{"sites": [{"id": "mybroker", "name": "My Broker", "login_url": "https://broker.example/login",
            "cookie_host": "mybroker.brisk.jp", "passkey_button": "Sign in with a passkey"}]}
```

Other keys: `cookie_prefix`, `launch_url`, `enroll_url` (where `enroll` starts, if the first sign-in
is on another page than the passkey one) and `user_field` (a CSS selector for a user ID box that must
be filled before the passkey control works; `enroll` then asks for the ID).

How it works inside: [ARCHITECTURE.md](https://github.com/honvl/BRiSKapi/blob/main/ARCHITECTURE.md#passkey-sign-in).

## API reference

| Call | Returns |
| --- | --- |
| `briskapi.connect(site="demo" \| "sbi" \| "e-shiten" \| "matsui" \| "monex" \| "smbcnikko")` | Live `Feed` of the demo or your broker's BRiSK; becomes the default source (`gateway=True` also serves it to `Live`) |
| `briskapi.Live()` | Databento-style live records from the feed on this computer: `subscribe(dataset, schema, symbols='ALL_SYMBOLS', start=None, snapshot=False, stream=True)`, `latest(schema=None, symbols=None)` (a table of the freshest quotes), then iteration or `add_callback(fn)` / `add_stream(path)` with `start()` and `block_for_close(timeout)`; `stop()`, `symbology_map` ([above](#live-records-databento-style)) |
| `Ticker(code).info()` | Name, lot size, tick type, base price and daily limits |
| `Ticker(code).quote(at=None)` | Bid/ask with their quantities and orders, the ten best book levels per side (`bids`, `asks`) and of the closing-condition orders (`closing_bids`, `closing_asks`), the board's order totals, indicative price/volume, market-order and closing quantities, last trade, turnover, issue status and `special_quote` (side, price, when its timer ends) |
| `Ticker(code).auction(at=None, closing=False)` | The auction's indicative state, market orders and board totals with `market_order_imbalance` (market buy minus sell); `closing=True` for the closing auction |
| `Ticker(code).history(start, end)` | Every update in order, from the loaded recording or feed (no network) |
| `Ticker(code).trades(start, end)` | Every execution: time, price, shares and the initiating side (`buy`, `sell`; None for most opening prints), from the loaded recording or feed |
| `Market().stocks()` | Master for every security |
| `Market().snapshot(at=None)` | Every security's quote |
| `Market().imbalances(at=None, top=None, closing=False)` | Securities ranked by absolute market-order imbalance (or the closing auction's) |
| `Market().special_quotes(at=None)` | Securities in a special quote (特別気配): side, price, when its timer ends |
| `Market().trades(start, end)` | Every security's executions in time order |
| `Market().frames()` | A recording's batches: clocks, local timing, quote and trade counts |
| `Market().summary()` | Source, date, coverage and clock range |
| `Feed.quotes(codes)` / `Feed.on_quote(fn, codes)` | Live updates as they arrive |
| `Feed.recordings` | A live feed's recordings so far: `trading_date`, `name`, `path`, `uploaded`, `timing_uploaded`, `repaired`, `error` |
| `briskapi.recordings()` / `.pull()` / `.load()` | Your archive's listing, verified download, local file |
| `briskapi.Archive().upload(dir)` / `.delete(prefix)` / `.deleted()` / `.restore(prefix)` / `.timing()` / `.owner` | Upload, delete, what you deleted in the last 30 days (with `deleted_at`, `restore_until`), bring one back, your timing reports, your owner ID |
| `briskapi.record(output, cache=None, ...)` | A recording of the demo, uploaded per your choice |
| `briskapi.consent(...)` | Your upload choice |
| `briskapi.Historical()` | The cloud API's client, Databento-style; your archive identity is the API key ([below](#querying-your-archive-in-the-cloud)) |
| `Historical().timeseries.get_range(dataset, start, end=None, symbols='ALL_SYMBOLS', schema='trades', limit=None, path=None)` | A store of records from `XTKS.BRISK` (your recordings) or `XTKS.BRISKDEMO`: Databento's conversions: `to_df()` (indexed by `ts_recv`, or `ts_event` for bars), `to_ndarray()`, `to_csv(path)`, `to_json(path)`, `to_parquet(path)`, `to_file(path)`, `replay(callback)`, iteration, `len()` |
| `Historical().metadata.list_datasets()` / `.list_schemas(dataset)` / `.list_fields(schema)` / `.list_publishers()` / `.get_dataset_range(dataset)` | The catalogue; a dataset's first and last date with data |
| `Historical().metadata.get_dataset_condition(dataset, start_date, end_date)` | Each date available or missing, and the clock windows (JST) your recordings cover |
| `Historical().metadata.get_record_count(...)` / `.get_cost(...)` | How many records a request returns; its cost, always 0 (free, with quotas) |
| `Ticker(code).candles(interval)` / `.margin(days)` | BRiSK's own price bars from your broker: `5m` today only, how far back `1d`/`1w`/`1mo` go is up to BRiSK; margin balances and lending fees |
| `Market().turnover()` / `.lists()` / `.events()` / `.schedule()` / `.watchlist()` | Broker BRiSK market data |
| `briskapi.login(site=..., cookies=None)` / `briskapi.logout()` | Your broker's BRiSK session (SBI by default), for the broker data above and `connect(site=...)` |
| `briskapi.passkey.enroll()` / `.login()` / `.forget()` | Passkey sign-in to your broker's BRiSK |

Prices are yen floats, with `None` for the vendor's zero "unavailable" value.
Times are JST `datetime`s on the trading date. Quantities are shares; side, flag
and status codes are raw vendor values. `raw=True` returns vendor fields
(`*_price10` in tenths of a yen, `*_us` in microseconds since JST midnight).
Tabular results are lists of dicts with `.to_pandas()`. Errors are
`briskapi.BriskError` and `briskapi.NotFoundError`. The first whole-market query
reads a recording's tables into memory (about two seconds for the complete demo);
one security's history is read on its own.

## Repairing a recording from another feed

Two feeds of one trading day (two computers, or two brokers) record the same market, and a feed that dropped, or
started late, lacks some of what the other has. `brisk repair` compares recordings of the same day and writes copies of
yours with what only the other feed holds added:

```sh
brisk repair recordings/sbi-am --from recordings/eshiten-am     # compare them: prints counts, writes nothing
brisk repair recordings/sbi-am --from recordings/eshiten-am --output recordings/sbi-am-repaired
brisk repair days/sbi --from days/eshiten --output days/repaired    # folders of recordings: each is paired by its time
```

- **Which recordings repair which.** A recording is repaired from the other feed's recordings of its day that cover mostly
  the same time (at least half of the shorter one's). Recordings with different times, such as a morning's and an
  afternoon's, are pieces one after the other, not partners: a morning is never repaired from an afternoon. Give folders
  (a `brisk live --output` folder holds one recording a piece) and each recording finds its own partners, one or several,
  and is repaired from each in turn.
- **How updates are matched.** A quote update is identified by its security and BRiSK's frame number (a frame's trades
  together). The counts say, of each recording's updates, how many are *same* (the other has them, alike), *only*
  (the other was running and lacks them), *conflict* (the other has that frame with other content) and *outside*
  (before the other started or after it ended); and of its blocks (a security over `--window` minutes, 5 by default),
  how many are identical, lack something the other has, are lacking on both sides, or conflict.
- **What it writes.** Your recording as it is, plus what only the other holds, in batches of their own among yours;
  checked as the archive checks a broker's recording and packaged, so `brisk upload` takes it. Where the two disagree
  yours stands. An update that would run backwards in time among your recording's own is counted under `left_out` in the
  report and not added. A recording that gains nothing is not written; one recording's goes into `--output`, several into
  a folder of them by name.
- **How far it reaches.** Only frames within your recording's own time are added, except where another recording of the
  day follows it (a piece you gave, in a folder or on the command line): the gap before that one is closed, up to a
  microsecond before it starts and never into it, so no time is stored twice. A recording nothing is known to follow is not
  extended; for one such (a feed that stopped early) `--until 15:30:00` (Tokyo's) goes that far, but not into a recording
  that follows.
- **Receipt times.** An update your recording lost while it ran (it holds a later update of that security) goes in a batch of its
  own just before the batch of that later update, with that batch's receipt time: a security's updates then run forward in
  market time, in frame and in receipt time alike, and none counts as received before your computer knew the next one. The
  report's `borrowed_receipt` counts them. (A batch holds one update of a security, so the neighbours' batches have no room; and
  the other feed's own batch clock can lie after your next update's, because one feed can run tens of milliseconds behind the
  other.) What follows everything your recording holds of a security (the gap before the next piece, a feed that stopped
  early) and the trades go in batches of the other's own, with its receipt time (its computer's clock); yours keep yours.
- **When it refuses.** If a pair does not number its frames alike (most of the other's updates that yours has a frame
  of must be the same), no frame says what is missing: it counts and writes nothing. Recordings must be of the same day
  and in the current format (briskapi 1.5 or later), and the ones repaired must have ended. No recording is ever changed.

## Command line

```sh
brisk live --site demo --codes 7203,6758 --quotes   # one JSON object per quote update (--raw for vendor fields)
brisk sites | enroll | login | forget       # sign in to your broker's BRiSK with a passkey through Chrome (experimental)
brisk live --site sbi --codes 7203          # BRiSK at SBI Securities; cookies from BRISK_SBI_COOKIES (JSON) or `brisk login`; --trace-protocol shows the handshake, wire details in BRISK_PROFILE
brisk live --site e-shiten --codes 7203     # Tachibana e-shiten BRiSK Next; cookies from BRISK_ESHITEN_COOKIES (JSON) or `brisk login`
brisk live --site matsui --codes 7203       # Matsui (or --site monex, --site smbcnikko); cookies from BRISK_MATSUI_COOKIES (JSON) or `brisk login`; not yet tried live
brisk live --site sbi --output days         # runs day after day; keeps each recording in days/ (e.g. sbi_20261013_085800-113000) and prints a {"day": {...}} line for it on stderr
brisk live --site sbi                       # records, uploads and serves briskapi.Live() (--no-gateway: serves nothing), with a line on stderr every 10 s (a minute in a log): the day, how long it has recorded, its updates and their rate; and, while it saves a recording, how far that has got
brisk record --output recordings/s1         # record the demo (uploaded unless you opt out); stderr says when it is packaged and saved, and how the upload goes
brisk list --date 20210927 --source historical_mock   # your recordings (--timing: your timing reports; --deleted: what you deleted in the last 30 days)
brisk pull PREFIX --output recordings/downloaded   # use a prefix returned by brisk list
brisk delete PREFIX                         # remove one of your uploads from your archive (restorable for 30 days)
brisk restore PREFIX                        # bring back an upload deleted in the last 30 days (also a timing report's key)
brisk prune recordings/s1                   # free disk: remove a local recording your archive holds
brisk export recordings/s1 --nautilus catalog      # a NautilusTrader v2 catalog, an instrument at a time (or --jsonl FILE, which holds the recording in memory: tens of GB for a broker's session)
brisk repair recordings/a --from recordings/b --output recordings/a-repaired   # add what only another feed's recordings of the day hold (without --output, only compare; folders of recordings work too)
brisk query XTKS.BRISK --schema ohlcv-1m --symbols 7203 --start 2026-10-13   # the cloud API: CSV to stdout (--encoding json|parquet, --output FILE, --end, --limit)
brisk consent [--accept | --revoke]         # show or change automatic uploads (--no-upload: this run only)
brisk upload recordings/s1                  # retry uploading a recording (stderr: bytes sent, the archive's checks, then stored)
brisk identity [--recovery-code | --recover CODE]  # your owner ID; its recovery code; use it on another computer
```

Each command has `--help`. `pull` verifies everything before writing and never
overwrites an existing folder.

Each command is the command-line form of a Python call:

| Command | Python |
| --- | --- |
| `brisk live --site demo` (`--cache DIR`) | `briskapi.connect(site="demo", gateway=True)` (`cache=DIR`; `--no-gateway`: without `gateway`) |
| `brisk live --site sbi` (or another broker) | `briskapi.connect(site="sbi", gateway=True)` |
| `brisk record` | `briskapi.record()` |
| `brisk query` | `briskapi.Historical().timeseries.get_range()` |
| `brisk list` (`--deleted`, `--timing`) | `briskapi.recordings()` (`Archive().deleted()`, `.timing()`) |
| `brisk pull`, `upload`, `delete`, `restore`, `prune` | `briskapi.pull()`, `Archive().upload()`, `.delete()`, `.restore()`, `.prune()` |
| `brisk consent` | `briskapi.consent()` |
| `brisk enroll`, `login`, `forget` | `briskapi.passkey.enroll()`, `.login()`, `.forget()` |
| `brisk export`, `brisk repair`, `brisk identity` | Command line only |

`brisk live --site sbi` (or another broker's site) runs until you stop it, normally keeping each trading
day's morning and afternoon sessions as separate recordings, with extra splits at 80 million quote rows.
Started after the close or on a holiday, it says so and waits for the next trading day. Unless you opt out, it uploads its market recordings as they are made, and each day's timing summary,
without asking; `--no-upload-market-data` stops uploading the recordings and `--no-upload` both
(add `--output DIR` to keep them on disk). `brisk list --source brisk_live` (every broker's) lists
your broker recordings. Every `brisk live` also serves its feed to `briskapi.Live()` on this computer
([Live records](#live-records-databento-style)) unless you pass `--no-gateway`.

Saving a recording takes minutes, and shows. When a session ends, `brisk live` says so
(`session ended; waiting for the next trading day`) while the recording is still being saved. Saving
reports on stderr (every 5 s on a terminal, 30 s in a log; stdout stays the quotes of `--quotes`): which recording
(`sbi 2026-10-13 from 09:00:00`), its stage (preparing, writing batches, quotes and trades, finalizing) with the rows
written of the total, then `saved to PATH in 2m35s` only once it is on disk, or `Packaging failed after ...` and why (`no update came
until 12:00:03; nothing to save` for a stretch in which the stream sent nothing, such as a reconnection in the lunch break). The upload
has lines of its own: bytes sent, the archive's checks, and `uploaded; the archive stored it`. A save that fails is kept in
`~/.local/share/brisk/recordings` and says so the same way (`keeping what was recorded in ...`, then `saved to ...`, with
`brisk upload PATH` to retry an upload that failed). `brisk record`, `package` and `upload` tell their packaging and upload on
stderr likewise, and their JSON results stay on stdout.

## Your private archive

Uploading gives you a free, off-computer copy of every complete session, checked on arrival
(a complete demo replay must match the reference). Once a recording is in your archive,
`brisk prune DIR` deletes the local copy (after checking its SHA-256 against your archive)
and `brisk pull` brings it back, on this computer or any other. Uploading also gives you the
[cloud API](#querying-your-archive-in-the-cloud) and its OHLCV bars, which exist only there.
Each upload is filed under
`archive/OWNER/YYYYMMDD/FROM-TO-SHA256` (the prefix `brisk list` returns), FROM and TO being
the recording's first and last source clock (JST, HHMMSS), so a day's recordings sort by time.

Uploads are private: only you can list, download or delete them. There is no account. The
first time it needs one, the tool makes your **identity**, a random secret, and the archive
files your uploads under a one-way hash of it, your owner ID. The secret is kept twice, in
your system's credential store (the macOS Keychain or Windows Credential Manager) and in an
owner-only file (`~/.config/brisk/identity.json`); each restores the other. When it is made,
the tool shows its **recovery code**, 28 letters and digits: write it down.
`brisk identity --recovery-code` shows it again, and `brisk identity --recover CODE` uses it
on another computer (or set `BRISK_IDENTITY`). Keep it private: whoever holds it is you to
the archive, and without it or a saved copy your uploads can't be reached. The archive's
operator can access what it stores, as with any hosted service; see
[PRIVACY.md](https://github.com/honvl/BRiSKapi/blob/main/PRIVACY.md).

Uploads are on by default. The first time you record or start a live session from the
command line, the tool shows what is uploaded and asks once (Enter accepts, `n` declines),
and saves your answer. After that, every clean, complete session (or recording of a live
feed) uploads automatically,
without asking again. Where nothing can ask, in the Python API or a command run without a
terminal, sessions upload by default and the tool says so once per process (a warning in
Python), until you decide with `brisk consent --accept` or `--revoke`
(`briskapi.consent(accept=True)` or `revoke=True`).

- **What a demo session uploads:** local timing measurements (including your computer's
  clock, which shows when you recorded). A demo replay's market data is the same for
  everyone, so the archive keeps it once: a demo upload sends only its own timing table
  (about 0.2 MB instead of about 29 MB), the archive joins it to its verified copy of the
  demo's market tables, and `brisk pull` returns the complete recording, verified as before.
  (The very first demo upload to an archive sends the whole recording.) Your IP address is
  used only for rate limits, as a keyed hash.
- **What a broker session uploads:** a timing summary (percentiles of
  decode time, data age at receipt and frame spacing, a stall count, the frame count, the
  trading date and the first and last minute; `briskapi.Archive().timing()` lists yours)
  and, once a trading day has ended, that day's decoded market recording: securities master,
  prices, quantities, books, trades, codes and local timing. Cookies, tokens and connection
  diagnostics are never uploaded. The day's trades also give you OHLCV bars in the cloud,
  built from your own recordings only (see [below](#querying-your-archive-in-the-cloud)).
- **Opting out:** `--no-upload` for one run (`upload=False` in Python);
  `--no-upload-market-data` (`upload_market_data=False`) uploads a broker session's timing
  summary but not its market recording (`--output DIR` keeps that); `BRISK_UPLOAD=0` for every run in that environment;
  `brisk consent --revoke` for good.
- **Deleting and restoring:** `brisk delete PREFIX` removes an upload from your archive, so
  you can no longer list or download it. For 30 days you can bring it back:
  `brisk list --deleted` shows what you deleted (with `deleted_at` and `restore_until`) and
  `brisk restore PREFIX` (also a timing report's key) restores it. After 30 days it can no
  longer be restored and is removed.
- Partial replays (`--limit-frames`) are never uploaded. A broker feed you stop mid-day ends that
  day there and uploads what is left of it, and the part of a day before a lost connection is uploaded once the
  feed reconnects. A day a crash or forced kill cuts short is repaired with its end and uploaded
  too (the next time a feed starts, after a crash).

## Querying your archive in the cloud

Uploads also give you a cloud API over your own recordings, shaped like Databento's
historical API (the same names and parameters). Your archive identity is the API key: there
is no signup and no key to create, and `briskapi.Historical()` finds your identity the way
uploads do (and makes one if you have none).

```python
import briskapi

client = briskapi.Historical()
trades = client.timeseries.get_range("XTKS.BRISK", start="2026-10-13", end="2026-10-17",
                                     symbols=["7203"], schema="trades")
trades.to_df()      # a pandas DataFrame indexed by ts_recv; also to_ndarray(), to_csv(), to_json(), to_parquet(), replay()
client.timeseries.get_range("XTKS.BRISK", start="2026-10-13", symbols=["7203"],
                            schema="ohlcv-1m").to_df()     # 1-minute bars (see below)
client.timeseries.get_range("XTKS.BRISKDEMO", start="2021-09-27T08:59", end="2021-09-27T09:03",
                            symbols=["7203"], schema="imbalance")    # the demo, for any identity
client.metadata.get_dataset_condition("XTKS.BRISK", start_date="2026-10-01")   # each date and the clock windows you recorded
```

`get_range(dataset, start, end=None, symbols='ALL_SYMBOLS', schema='trades', limit=None,
path=None, ts_filter='ts_event', skew=10.0)` returns a store; `path` also writes its Parquet file.
The server cuts on BRiSK's stamp (`ts_event`); a live feed's records reach your computer a little
later (`ts_recv`). For a test that must not look ahead, `ts_filter='ts_recv'` cuts and sorts on
the receipt time (it asks for `skew` seconds either side and says if the stamps ran further ahead
than that allows), and `get_snapshot(dataset, at, symbols='ALL_SYMBOLS', schema='brisk-quotes',
since=None)` returns each security's latest record as of `at` (of those received by `at`, unless
`ts_filter='ts_event'`; the latest by stamp and frame, not the last received), one row per security. `client.metadata` has
`list_datasets()`, `list_schemas(dataset)`, `list_fields(schema)`, `list_publishers()`,
`get_dataset_range(dataset)`, `get_dataset_condition(dataset, start_date, end_date)` (each date
available or missing, and the clock windows your recordings cover, in JST),
`get_record_count(...)` and `get_cost(...)`. `get_cost` is always 0: the cloud API is free,
with quotas of 60 requests a minute and 2 GB a day per identity, and 120 requests a minute and
4 GB a day per IP address.

- **Datasets** follow Databento's `VENUE.FEED` naming (XTKS is the Tokyo Stock Exchange).
  `XTKS.BRISK` is your own live recordings, whichever broker's BRiSK they came through.
  `XTKS.BRISKDEMO` is the public demo (2021-09-27), open to every identity.
- **Schemas:** `trades`; `mbp-10` (ten levels per side; BRiSK gives books, not order events,
  so `action` and `side` are `N`); `definition`; `imbalance` (opening and closing auction
  state: indicative price and volume, market-order imbalance); `brisk-quotes` (every BRiSK
  quote field); and the OHLCV bars `ohlcv-1s`, `ohlcv-1m`, `ohlcv-1h` and `ohlcv-1d`.
  `list_fields(schema)` lists a schema's columns.
- **Times:** a date means that JST trading day, a time without a zone is JST and an integer is
  UTC nanoseconds. `end` is exclusive and defaults to the end of the start's trading day. A
  request covers at most 31 days and 2,000 symbols.
- **Symbols** are JPX codes. `instrument_id` is fixed per code: digits-only codes are their
  number (7203), others are 100,000 plus the code's base-36 value (`167A` is 154694).

**`Historical`'s OHLCV bars come only from the cloud**, never from files on your computer
(`Live` builds them from the stream). The archive builds your bars for each trading day
from your own current live recordings of that day, counting each trade once, whenever you
upload, delete or restore one, so a query reads them ready-made. They hold the bars your
recordings cover: the security, and the bar's whole span within the trading sessions (09:00 to
11:30 and 12:30 to 15:30 JST). A recording from 08:55 to 11:31, for example, gets the morning's
1-minute and 1-hour bars but not the day bar. So uploading is how you get bars. No one else's
uploads enter yours, and yours enter no one else's. (`XTKS.BRISKDEMO`'s bars are built from
the demo's own trades.) See [PRIVACY.md](https://github.com/honvl/BRiSKapi/blob/main/PRIVACY.md)
(policy version 8).
`Ticker.candles()` is something else: BRiSK's own bars from your broker, which need a broker
login.

On the command line, `brisk query` writes CSV to stdout. `--encoding json` or `parquet` changes
that (Parquet needs `--output FILE`), `--output FILE` writes a file instead, and `--end` and
`--limit` bound the request:

```sh
brisk query XTKS.BRISK --schema ohlcv-1m --symbols 7203 --start 2026-10-13 > toyota-1m.csv
```

The HTTP API behind both is described in
[docs/cloud-api.md](https://github.com/honvl/BRiSKapi/blob/main/docs/cloud-api.md). The same
records stream live, from the feed on your computer, through `briskapi.Live`
([above](#live-records-databento-style)).

## More documentation

The [documentation site](https://honvl.github.io/BRiSKapi/) has these documents and an API reference with
an example of each call and what it returns.

- [docs/data-format.md](https://github.com/honvl/BRiSKapi/blob/main/docs/data-format.md): every column of recordings and cloud API records, and how to read them
- [docs/cloud-api.md](https://github.com/honvl/BRiSKapi/blob/main/docs/cloud-api.md): how the cloud API works
- [ARCHITECTURE.md](https://github.com/honvl/BRiSKapi/blob/main/ARCHITECTURE.md): how it works, data format, archive integrity and limits
- [PRIVACY.md](https://github.com/honvl/BRiSKapi/blob/main/PRIVACY.md): privacy policy
- [CONTRIBUTING.md](https://github.com/honvl/BRiSKapi/blob/main/CONTRIBUTING.md): development, tests and releases
- [tools/brisk_mock/README.md](https://github.com/honvl/BRiSKapi/blob/main/tools/brisk_mock/README.md): field definitions, timing and latency, checking the SBI master layout, and the optional Rust collector
- [infra/README.md](https://github.com/honvl/BRiSKapi/blob/main/infra/README.md): deploying an archive
- [THIRD_PARTY.md](https://github.com/honvl/BRiSKapi/blob/main/THIRD_PARTY.md): decoder, data and pybrisk attribution

Software is MIT licensed.
