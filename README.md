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
recordings = briskapi.recordings(source="historical_mock")   # no AWS account needed
if recordings:
    briskapi.pull(recordings[0]["prefix"])      # verify, decode and cache; becomes the default
else:
    briskapi.record("recordings/my-session", web=True)   # record the demo if the archive is empty
# Or briskapi.load("recordings/my-session") for a local events.jsonl[.gz] or folder.

briskapi.Ticker("7203").quote(at="08:59:59.9999")             # available pre-open quote, in JST
briskapi.Ticker("7203").history(start="09:00", end="09:01")   # every update in a window
briskapi.Market().snapshot(at="09:00:00").to_pandas()
```

Time queries use each quote's timestamp. Toyota's first demo quote is at
08:59:59.993551 JST; an earlier query raises `NotFoundError`. `Market().summary()`
returns both `start` and `end`, including for local recordings and live feeds.
Archive listings skip invalid or unsupported recordings with a warning. Demo
recordings may be absent; `synthetic_test` entries are publication probes.
The legacy [sample data](https://brisk-recordings-honvl-tokyo.s3.ap-northeast-1.amazonaws.com/archive/20260311/cca870a51e7f96f16009c3709c3f597d314b22ed8922bb3ea7fa1acc67a0f85f/events.jsonl.gz)
uses the SBI wire format for protocol research and requires a wire-format decoder.

## SBI BRiSK

For SBI Securities customers with a BRiSK subscription. Log in on
[sbi.brisk.jp](https://sbi.brisk.jp) in your browser, then pass its session
cookies: copy them from DevTools, or use `pycookiecheat`'s
`chrome_cookies("https://sbi.brisk.jp")`. Or let `briskapi` sign in with your passkey
([below](#signing-in-with-a-passkey-experimental)).

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

feed = sbi.connect(codes=["7203"])          # live (experimental); timing sharing follows consent
toyota.quote()                              # same calls as any feed
# To share this session's market data too, first accept the current policy:
# briskapi.consent(accept=True, contributor="your-alias", license="CC0-1.0")
# feed = sbi.connect(codes=["7203"], share_market_data=True)
```

Results use the conventions below. Errors are `sbi.SessionExpiredError` (log in
again), `briskapi.NotFoundError`, `sbi.RateLimitError` and `sbi.APIError`.
Requests are limited to one per second.

The live feed runs SBI's own decoder under Node, downloaded with your session;
no browser is involved. It follows the vendor client's connect sequence: it feeds
the decoder from the first frame, catches the snapshot up to the stream, forwards
the decoder's pings, watches the server heartbeat and joins SBI's Socket.IO
namespace when the server uses it. It hasn't yet been validated against a live SBI
session, and three wire details aren't public: the Socket.IO connect parameters,
the `startLive` payload and the catch-up request body. Set them with
`sbi.connect(profile={...})` and see what the server answers with
`trace_protocol=True` (every token redacted); until they are right it stops with an
explicit error rather than guessing. Please report what you see. Your cookies go
only to sbi.brisk.jp. SBI market data is shared only after an explicit choice for
that capture; the Python API requires `share_market_data=True`. With contribution
on, a timing summary is shared even when market-data sharing is declined.

## Signing in with a passkey (experimental)

Many brokers now have you sign in with a passkey. If yours does, `briskapi` can do the
sign-in for you and keep the BRiSK session cookies, instead of you copying them from a
browser. It works with the BRiSK sites of SBI, Matsui, Monex and SMBC Nikko, or any broker you
add. It needs Chrome, Node 22+ and a safe place for the passkey: the macOS Keychain or Windows
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
   the terminal: `briskapi` saves the passkey and your broker choice in the macOS Keychain or Windows Credential
   Manager, and Chrome closes.
2. **Each session, `brisk login`.** The same Chrome opens with the saved passkey loaded, goes to the
   login page and presses the passkey button, and the virtual authenticator signs. Open BRiSK from
   the broker's site in that window (or pass `--launch-url` to have it opened for you).
   `briskapi` reads the BRiSK session cookies, saves them and closes Chrome.
3. **Then use it.** For SBI, `brisk live --sbi` and the `briskapi.sbi` API use the saved cookies.
   For the other brokers `briskapi` has no data client yet, so the cookies are only saved
   (under `~/.config/brisk/cookies/`) for your own use.

Your existing passkeys on your phone or in a password manager are not touched. The broker simply
lists one more passkey, which you can delete in its security settings.

### What to know

- **The saved passkey is a credential.** Whoever has it can sign in to your broker account with no
  further check. It is kept only in the Keychain or Credential Manager (`BRISK_PASSKEY_STORE=file` keeps it in an
  owner-only file instead, which is weaker; on Windows that file's privacy comes from your user
  profile folder, which only you and administrators can read), never leaves your machine and is never shown in a
  log. `brisk forget` deletes it here; remove it at the broker too.
- **It is experimental.** Each broker's login page, passkey button text and BRiSK launch come from
  its public pages, and none has been tried against a live account. Override them with
  `--login-url`, `--passkey-button` and `--launch-url`, or in `sites.json` below, and please report
  what fails.
- **A broker may refuse it.** Chrome tells the site it is being automated. Check that your
  broker's terms allow this before relying on it.

### Commands

```sh
brisk sites                    # the brokers you can pick, and where their BRiSK lives
brisk enroll [--site matsui]   # once; you are asked to choose a broker if you don't say
brisk login                    # each session; the broker is the one you enrolled with
brisk forget                   # delete the saved passkey
```

From Python: `passkey.enroll(site="matsui")` and `signin = passkey.login()`, where
`signin.cookies` are the BRiSK cookies and `signin.client` is the data client for sites that have one.

To add a broker, or correct a built-in one, edit `~/.config/brisk/sites.json`:

```json
{"sites": [{"id": "mybroker", "name": "My Broker", "login_url": "https://broker.example/login",
            "cookie_host": "mybroker.brisk.jp", "passkey_button": "Sign in with a passkey"}]}
```

How it works inside: [ARCHITECTURE.md](https://github.com/honvl/BRiSKapi/blob/main/ARCHITECTURE.md#passkey-sign-in).

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
| `briskapi.passkey.enroll()` / `.login()` / `.forget()` | Passkey sign-in to your broker's BRiSK |

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
brisk sites | enroll | login | forget       # sign in to your broker's BRiSK with a passkey through Chrome (experimental)
brisk live --sbi --codes 7203               # SBI BRiSK; cookies from BRISK_SBI_COOKIES (JSON) or `brisk login`; --trace-protocol shows the handshake, wire details in BRISK_SBI_PROFILE
brisk record --web --output recordings/s1   # record a replay (shared if you agreed)
brisk list --date 20210927 --source historical_mock
brisk pull PREFIX --output recordings/downloaded   # use a prefix returned by brisk list
brisk consent [--accept | --revoke]         # show or change sharing
brisk upload recordings/s1                  # retry sharing a recording
```

Each command has `--help`. `pull` verifies everything before writing and never
overwrites an existing folder.

At the beginning of each interactive `brisk live --sbi` capture with contribution
enabled, you are asked whether to publish that session's market data; Enter opts
in. The choice is not saved. Use `--share-market-data` to opt in explicitly in a
script, or `--no-share-market-data` to decline without prompting. A clean session
end is required to publish a recording. `brisk list --source sbi_live` lists shared
SBI recordings; their market content is contributor-declared.

## Sharing recordings

The first time you record or start a demo live session from the command line,
the tool shows what would be shared and asks once; Enter accepts. After that,
every complete demo session is uploaded and published automatically. The Python
API never asks: until you decide, sessions stay on your computer.

- **SBI timing sharing:** percentiles of decode time, data age at
  receipt and frame spacing, a stall count, the frame count, the trading date,
  the first and last minute, and your alias and license.
  `briskapi.Archive().timing()` lists everyone's reports.
- **Optional SBI market sharing:** an explicit choice at the start of each capture
  adds the decoded securities master, prices, quantities, codes and local timing
  to the public archive. Cookies, tokens and connection diagnostics are excluded.
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
- [infra/README.md](https://github.com/honvl/BRiSKapi/blob/main/infra/README.md): deploying your own archive
- [THIRD_PARTY.md](https://github.com/honvl/BRiSKapi/blob/main/THIRD_PARTY.md): decoder, data and pybrisk attribution

Software is MIT licensed.
