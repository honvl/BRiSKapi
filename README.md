# briskapi

[English](https://github.com/honvl/BRiSKapi/blob/main/README.md) | [日本語](https://github.com/honvl/BRiSKapi/blob/main/README.ja.md)

An unofficial, pybrisk-style Python API and `brisk` command line for BRiSK auction
data. Consume a live feed, query recordings at any point in time, pull shared
recordings from a public archive, and use SBI BRiSK with your own account. This
is an independent project, not affiliated with or endorsed by BRiSK, Tachibana,
SBI, TSE or JPX.

> **Data sources:**
> - **No account needed:** the public BRiSK Next demo of 27 September 2021 (one
>   pre-open snapshot and the first three minutes after the open), replayed at
>   its recorded pace. This is not live market data.
> - **With an SBI Securities BRiSK subscription:** SBI BRiSK market data (candles,
>   margin, alerts, schedule, watchlist) and an experimental live feed.

## Install

Python 3.12+. Live feeds and recording also need Node 22+, because BRiSK's own
decoder is a WebAssembly module that runs under Node.

```sh
pip install 'briskapi[pandas]'      # `import briskapi` and the `brisk` command
```

BRiSK's decoder and demo data are downloaded at runtime; the package doesn't
include them. Optional prebuilt Rust tools (`brisk_quote_ingest`,
`brisk_recording`) for Linux, macOS and Windows are attached to each
[GitHub release](https://github.com/honvl/BRiSKapi/releases). To work from source,
clone the repository and run `pip install -e '.[pandas]'`.

## Live feed

```python
import briskapi

feed = briskapi.connect(web=True, codes=["7203", "6758"])   # returns once initial state is in
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

Options: `web=True` (fetch from the demo site) or `cache=DIR` (a local copy of
the demo assets), `codes`, `speed` (`1` real time, `0` as fast as possible) and
`history=True` (keep updates for `Ticker.history()`). Callbacks and iterators
first receive each security's current quote, then every update in order. A slow
consumer slows the feed instead of losing updates.

## Recordings and the archive

```python
briskapi.recordings(source="historical_mock")   # published recordings; no AWS account needed
briskapi.pull("archive/20210927/SHA256")       # download, verify, decode and cache; becomes the default
briskapi.load("recordings/my-session")         # or a local recording (events.jsonl[.gz] or folder)
briskapi.record("recordings/my-session", web=True)   # record the demo yourself

briskapi.Ticker("7203").quote(at="08:59:59.99")               # state at any JST time
briskapi.Ticker("7203").history(start="09:00", end="09:01")   # every update in a window
briskapi.Market().snapshot(at="09:00:00").to_pandas()
```

## SBI BRiSK

For SBI Securities customers with a BRiSK subscription. Log in on
[sbi.brisk.jp](https://sbi.brisk.jp) in your browser, then pass its session
cookies: copy them from DevTools, or use `pycookiecheat`'s
`chrome_cookies("https://sbi.brisk.jp")`.

```python
from briskapi import sbi

sbi.login(cookies={"session_bfaf77a2": "v2.local..."})   # remember=True saves them (owner-only file)
toyota = briskapi.Ticker("7203")
toyota.candles("5m").to_pandas()   # price bars: 5m (today), 1d, 1w or 1mo
toyota.margin(days=30)             # margin balances and stock-lending fees
market = briskapi.Market()
market.turnover()     # turnover and shares outstanding, all stocks
market.lists()        # NK225, recent IPOs, …
market.events()       # basket orders, limit up/down, volume surges
market.schedule()     # trading date, status and session times
market.watchlist()    # your saved codes

feed = sbi.connect(codes=["7203"])          # live (experimental)
toyota.quote()                              # same calls as any feed
```

Results use the conventions below. Errors are `sbi.SessionExpiredError` (log in
again), `briskapi.NotFoundError`, `sbi.RateLimitError` and `sbi.APIError`.
Requests are limited to one per second.

The live feed runs SBI's own decoder under Node, downloaded with your session;
no browser is involved. It hasn't yet been validated against a live SBI session,
so it fails with an explicit error rather than guessing. Please report what you
see. Your cookies go only to sbi.brisk.jp, and SBI market data never leaves
your computer. With sharing on, a session contributes only a timing summary (see
below).

## API reference

| Call | Returns |
| --- | --- |
| `briskapi.connect(...)` | Live `Feed`; becomes the default source |
| `Ticker(code).info()` | Name, lot size, tick type, base price and daily limits |
| `Ticker(code).quote(at=None)` | Bid/ask, indicative price/volume, market-order and closing quantities, last trade |
| `Ticker(code).auction(at=None)` | Indicative auction state with `market_order_imbalance` (market buy minus sell) |
| `Ticker(code).history(start, end)` | Every update in order |
| `Market().stocks()` | Master for every security |
| `Market().snapshot(at=None)` | Every security's quote |
| `Market().imbalances(at=None, top=None)` | Securities ranked by absolute market-order imbalance |
| `Market().summary()` | Source, date, coverage and clock range |
| `Feed.quotes(codes)` / `Feed.on_quote(fn, codes)` | Live updates as they arrive |
| `briskapi.recordings()` / `.pull()` / `.load()` | Archive listing, verified download, local file |
| `briskapi.record(output, web=True, ...)` | A recording of the demo, shared per your choice |
| `briskapi.consent(...)` | Your sharing choice |
| `Ticker(code).candles(interval)` / `.margin(days)` | SBI BRiSK price bars; margin balances and lending fees |
| `Market().turnover()` / `.lists()` / `.events()` / `.schedule()` / `.watchlist()` | SBI BRiSK market data |
| `briskapi.sbi.login()` / `.connect()` | SBI BRiSK session and live feed |

Prices are yen floats, with `None` for the vendor's zero "unavailable" value.
Times are JST `datetime`s on the trading date. Quantities are shares; side, flag
and status codes are raw vendor values. `raw=True` returns vendor fields
(`*_price10` in tenths of a yen, `*_us` in microseconds since JST midnight).
Tabular results are lists of dicts with `.to_pandas()`. Errors are
`briskapi.BriskError` and `briskapi.NotFoundError`. A whole-market query reads a
recording once (about six seconds for the complete 420 MB demo).

## Command line

```sh
brisk live --web --codes 7203,6758          # one JSON object per quote update (--raw for vendor fields)
brisk live --sbi --codes 7203               # SBI BRiSK; cookies from BRISK_SBI_COOKIES (JSON)
brisk record --web --output recordings/s1   # record a replay (shared if you agreed)
brisk list --date 20210927 --source historical_mock
brisk pull archive/20210927/SHA256 --output recordings/downloaded
brisk consent [--accept | --revoke]         # show or change sharing
brisk upload recordings/s1                  # retry sharing a recording
```

Each command has `--help`. `pull` verifies everything before writing and never
overwrites an existing folder.

## Sharing recordings

The first time you record or start a demo live session from the command line,
the tool shows what would be shared and asks once; Enter accepts. After that,
every complete demo session is uploaded and published automatically. The Python
API never asks: until you decide, sessions stay on your computer.

- **SBI sessions share timing only:** percentiles of decode time, data age at
  receipt and frame spacing, a stall count, the frame count, the trading date,
  the first and last minute, and your alias and license. Never prices,
  quantities or codes. `briskapi.Archive().timing()` lists everyone's reports.
- **What a demo session shares:** the market data you recorded, local timing measurements
  (including your computer's clock, which shows when you recorded), and a public
  alias (random `anon-…` by default) and license. Your IP address is used only to
  rate limit uploads.
- **Visibility:** published recordings are public and permanent, and you cannot
  delete them yourself. See [PRIVACY.md](https://github.com/honvl/BRiSKapi/blob/main/PRIVACY.md).
- **Opting out:** `brisk consent --revoke`, `BRISK_CONTRIBUTE=0`, or `--no-upload`
  for one run.
- **License:** accepting declares that you may redistribute the recordings under
  the chosen data license (CC0-1.0 or CC-BY-4.0). This project's open-source
  license gives no rights to vendor or exchange data. If you can't make that
  declaration, turn sharing off.
- Partial replays (`--limit-frames`) and sessions closed early are never shared.

## More documentation

- [ARCHITECTURE.md](https://github.com/honvl/BRiSKapi/blob/main/ARCHITECTURE.md): how it works, data format, archive integrity and limits
- [PRIVACY.md](https://github.com/honvl/BRiSKapi/blob/main/PRIVACY.md): privacy policy
- [CONTRIBUTING.md](https://github.com/honvl/BRiSKapi/blob/main/CONTRIBUTING.md): development, tests and releases
- [tools/brisk_mock/README.md](https://github.com/honvl/BRiSKapi/blob/main/tools/brisk_mock/README.md): Rust collector, field definitions, timing and latency
- [tools/brisk_mock/NAUTILUS_V2.md](https://github.com/honvl/BRiSKapi/blob/main/tools/brisk_mock/NAUTILUS_V2.md): NautilusTrader v2 integration
- [infra/README.md](https://github.com/honvl/BRiSKapi/blob/main/infra/README.md): deploying your own archive
- [THIRD_PARTY.md](https://github.com/honvl/BRiSKapi/blob/main/THIRD_PARTY.md): decoder, data and pybrisk attribution

Software is MIT licensed.
