# Architecture

How briskapi works, what the data means and how the shared archive protects
itself. For usage, see the [README](README.md).

## Components

| Component | Where | Role |
| --- | --- | --- |
| Decoder hosts | `briskapi/decoder/` | Run BRiSK's own WASM decoder in Node, without a browser or Chrome DevTools. `decoder.cjs` replays the public demo, whose assets must match SHA-256 pins in `assets.json`. `sbi.cjs` drives SBI BRiSK's decoder from an authenticated session. Both emit one JSON batch per frame. |
| Python API | `briskapi/` | `Recording` (files), `Feed` (live, background thread), `Ticker`/`Market` views and `Archive`. |
| SBI session | `briskapi/sbi.py` | Cookie login, token boot and SBI's REST data behind `Ticker.candles`/`margin` and `Market.turnover`/`lists`/`events`/`schedule`/`watchlist`; `sbi.connect()` starts the live host. |
| Archive client | `briskapi/cli.py`, `briskapi/schema.py` | The `brisk` CLI, sharing consent, canonical packaging, upload and verified download. |
| Auction state | `rust/brisk_quote_ingest` | Optional Rust tools: `brisk_quote_ingest` validates batches, keeps per-security state and publishes latest-state files; `brisk_recording` reconstructs a saved recording. |
| Archive service | `archive_service.py`, `infra/deploy.py` | A Lambda function URL issues upload tickets; S3-triggered ingest validates and publishes; DynamoDB holds quotas. |

```
demo assets (HTTPS, pinned) ─► decoder.cjs ─┐
SBI session (cookies) ───────► sbi.cjs ─────┴─► JSON batches ─┬─► briskapi.Feed: live state, callbacks, iterators
                                                             ├─► brisk live: JSON lines
                                                             └─► briskapi.record ─► events.jsonl (demo only)

events.jsonl ─► canonical package ─► upload ticket ─► S3 staging ─► validation ─► archive/ ─► briskapi.pull
```

## Data source

The account-free input is the public BRiSK Next demo dated 2021-09-27. Its
initial snapshot is at 08:59:59.999955 JST with 4,131 securities. The frame
container has 18,001 frames labelled 09:00:00.000–09:02:59.990, so it provides
one genuine pre-open snapshot and the opening transition, not a pre-open time
series or a full-depth book. Output is labelled `source=historical_mock`.

Source clocks are preserved. Their origin is the decoder's feed clock, not an
independently established exchange clock, so exchange provenance and exchange
delay are unverified (`exchange_delay_ms` is always null). Live data needs an
authenticated Tachibana BRiSK Next session and validation of the current
protocol: bootstrap, master/snapshot, WebSocket handshake, keepalives and
catch-up. The demo's protocol version is 16000. The [recorder
documentation](tools/brisk_mock/README.md) compares this with pybrisk's
browser-based live reader and defines every field and timing measurement.

## Batch stream

Every source produces the same stream:

1. `bootstrap`: the security master and every selected security's initial quote,
   plus transport and clock provenance.
2. `quotes`: one batch per frame with the quotes that changed in it. Empty
   batches are kept, so sequence continuity and feed health stay separate from a
   quiet security's last update.
3. `end`: frame and update counts, written only after a clean finish.

Sequence gaps, clock regressions, malformed input and interruption invalidate a
stream in the Rust state and in archive validation. The Python feed checks
sequence continuity and a clean end.

Recording time queries filter each quote by its own `source_time_us`, including
quotes in batches whose clock is later than the query. Batch clocks are upper
bounds on contained quote times, so stopping at the first later batch would miss
valid snapshots and deltas. `Market.summary()` uses the first and last batch
clocks, including empty batches and the end marker; recordings cache that range
when no manifest is available, while feeds track it under the state lock.

## Live feed

`briskapi.Feed` reads decoder output on a background thread and applies each batch
to its state under a lock. A new listener takes its snapshot and registers in one
locked step, so it can't miss or duplicate an update. Iterators use a bounded
queue (10,000 items) and callbacks run on the feed thread, so a slow consumer
slows the decoder instead of dropping updates. An abandoned iterator never blocks
the feed. An exception in a callback stops the feed and is re-raised by `wait()`.
A feed keeps current state only, unless `history=True` retains every update per
security.

When sharing is enabled, a demo feed also writes the session to a temporary file.
After a clean end it packages and uploads the file, then deletes it. SBI feeds
accumulate timing statistics when sharing is enabled (`briskapi/timing.py`). Market
recording additionally requires a separate opt-in for that session. The interactive
CLI asks before starting the decoder (Enter accepts); scripts use
`--share-market-data` or `sbi.connect(share_market_data=True)`. The per-session
choice is not persisted. Current consent is required, and `BRISK_CONTRIBUTE=0`
or `contribute=False` disables both forms of sharing. Only a clean end publishes
market data; an interrupted capture's temporary file is deleted.

## Timing reports

A timing report is shared independently of an SBI market-recording opt-in. A report
(`brisk-timing-v1`) holds p50/p90/p99/max of decode time, feed-clock age at
receipt and frame spacing, a stall count (gaps over one second), the frame
count, trading date, first/last JST minute, client version, alias and license.

The ticket API accepts `{"timing": report}`. The service checks exact fields,
bounds and microsecond precision, monotonic percentiles, at least 100 frames and
a trading date within the last 30 days; rate limits it (six per IP address per
hour, 600 in total); and writes its own canonical JSON to
`timing/YYYYMMDD/SHA256.json`, so identical reports deduplicate. A report is at
most 2 KB, so it can't carry other data. `briskapi.Archive().timing()` reads
them back.

## SBI BRiSK

The endpoint sequence was learned from pybrisk: session cookies authenticate
`/api/frontend/boot`, which returns a bearer token; `/api/app/boot` then gives
the trading date, series, schedule, WebSocket URL and master/snapshot hashes.
Requests are rate limited and never follow redirects, because a redirect means
an expired session and following it would forward credentials.

The live host (`briskapi/decoder/sbi.cjs`) uses the same session to fetch the
master and snapshot, finds SBI's decoder through the app's own bundles (it is
served only to logged-in sessions and changes with SBI releases, so it can't be
pinned), checks that it exports every function the host calls, and decodes
WebSocket frames with protocol version 18000. Cookies reach it through the
environment, never the command line.

The host follows the connect sequence in the vendor client (the public Next demo
bundle) and in pybrisk's SBI research (`engineio.cjs` and `sbi.cjs`):

1. The WASM is started with its six callbacks (`send`, `updateNumber`,
   `heartbeat`, `basePrice`, `authError`, `marketFinished`) using the vendor's
   signatures. `send` bytes, which are the WASM's own pings, are written to the
   socket as raw binary frames; the vendor client has no ping timer of its own.
2. The link detects the server's dialect from its first message. A plain server
   (Next) sends binary frames. SBI's, per pybrisk's capture, opens with an Engine.IO
   packet; the host then joins the `/v2/user` namespace with a query, sends a
   `startLive` event and answers Engine.IO keepalives (clients ping on version 3,
   servers on version 4).
3. Frames go to the WASM immediately. When the first frame numbers arrive, the
   host compares them with the snapshot's. Issues whose snapshot is behind are
   caught up with `POST /api/stocks_update/{series}` (up to five retries, never for
   an expired session or an `x-error-reason: too-old` reply) and the reply is
   pushed into the WASM before quote tracing starts. Frames that arrive meanwhile
   are not lost.
4. The server heartbeat, decoded by the WASM, is watched: no beat for 7 s after the
   first one is a connection failure, as in the vendor client. A second rule is the
   vendor's too: during trading hours (08:00-15:00 JST, not 11:30-12:10) the
   decoder's data time may lag the server clock, taken from the heartbeat, by at
   most 90 s. The WASM's `authError` is the single-session rule (one WebSocket per
   user), and `marketFinished` makes an abnormal close a normal end.
5. The decoder protocol comes from the boot response's `flex_version` (an explicit
   option wins; 18000 if the server says nothing). The boot response's
   `exceptional_sq` and `base_prices` are pushed into the WASM after the master is
   loaded and before the snapshot, as the vendor client does, and the master's
   limits are re-read after every frame in case the decoder changes them.

The ping loop is the WASM's own. In the real decoder, market and sync frames never
trigger a ping; each heartbeat frame (`0xF0` and an 8-byte little-endian clock)
triggers one reply ping carrying the server clock as the decoder estimates it. The
loop therefore sustains itself once the server starts it, and the host only has to
forward the bytes.

The stock view and the master are read at version-specific positions. Protocol
16000 (the demo) is the vendor client's own layout. For 18000 (SBI) the stock
view has the same fields in the same order with the leading word dropped, so every
index is one lower; this comes from a single independent parser of SBI's current
build and is not confirmed against SBI itself. The master's 18000 layout has no
independent source at all, so a master that does not look like a market (numeric
codes, base prices inside their limits, plausible lot sizes) is refused. Any other
protocol version is refused rather than read at guessed offsets.

What is verified: the decoder SBI served in pybrisk's March 2026 capture exports
the demo's interface apart from `_getPortfolio` (so SBI quotes have no
`issue_status`), and it initializes under Node with protocol 18000. The demo
decoder is tested against the vendor client's own indices on real data, and its
heartbeat, ping reply and callbacks through its function table; the ping loop and
the whole flow run offline against fake servers (plain, Engine.IO 3 and 4) using it.
What is not verified against a live session: any of the above on SBI itself, the
18000 master layout, and three wire details that are not public: the Socket.IO
connect parameters, the `startLive` payload and the catch-up request body. They live in a profile
(`BRISK_SBI_PROFILE`, or `sbi.connect(profile=...)`); the defaults are labelled
unverified, and the catch-up request is used only when the profile names a format.
`--trace-protocol` prints the connection with every token redacted, so a first
attempt shows what the server answers. Anything wrong ends in an explicit error (a
stream that never initializes, a refused namespace, a rejected catch-up, an
implausible stock view or master) rather than guessed data. Not implemented: reconnecting.
The feed fails and needs a restart.

## Reconstructing a recording

To validate and reconstruct a saved or downloaded recording with the Rust state
(`brisk_recording` from a release archive, or built from source):

```sh
rust/brisk_quote_ingest/target/release/brisk_recording \
  --input recordings/downloaded/events.jsonl --latest /tmp/restored-state.json
```

This rebuilds final state for offline analysis. It does not retimestamp data or
simulate original exchange delivery.

## Shared archive

The public configuration is [briskapi/archive.json](briskapi/archive.json): bucket
`brisk-recordings-honvl-tokyo` in Tokyo (`ap-northeast-1`). Anyone can read
published `archive/` and `timing/` objects and list those prefixes over HTTPS. `incoming/`
(staging) is private. Deployment is described in [infra/README.md](infra/README.md).

### Integrity

The client is open source, so the service validates every upload. Demo and
synthetic content must match a reference; SBI live content has no reference and
is contributor-declared:

- **Reference replay.** Market content must equal the pinned demo replay.
  [briskapi/references/historical_mock.json](briskapi/references/historical_mock.json) holds a
  truncated SHA-256 chain per security (master entry plus every update and the
  batch it arrived in) and a hash of the batch clock timeline; it contains no
  market data. Any subset of securities is accepted, but only complete replays.
  `tools/brisk_mock/build_reference.py` regenerates it after a re-audited asset pin.
  This comparison applies only to demo/synthetic sources, not to `sbi_live`.
- **Canonical bytes.** Each line must be the one canonical JSON encoding of its
  value: sorted keys, issues in ID order, compact separators, no escapes. Extra
  whitespace, duplicate keys, alternative number spellings and other encoding
  variants are rejected. Packaging converts recorder output to this form and
  rounds local timings to microseconds.
- **Exact fields.** Batches, master entries and quotes must have exactly the
  fields defined in `briskapi/schema.py`. Account, session or unknown fields are
  rejected. Local timing values must be plausible: decode ≤ 10 s, receipt clock
  monotonic within 24 h, millisecond values with at most microsecond precision,
  and pacing consistent throughout. End-of-stream counts must match.
  SBI quotes omit `issue_status`, which its decoder does not export, and use null
  replay lateness. Its end frame count includes pre-bootstrap catch-up frames,
  so it can exceed the number of emitted quote batches. SBI packaging removes
  connection/decoder diagnostics and publishes only
  `{"kind":"sbi_websocket","origin":"https://sbi.brisk.jp/"}` as transport metadata.
  The service rejects any additional transport fields.
- **Service-made objects.** The service publishes its own deterministic gzip of
  the validated lines, never the uploaded bytes. Gzip header fields, extra
  members, padding and deflate choices therefore cannot carry data.
  `synthetic_test` accepts only the fixed one-security probe in `briskapi/schema.py`.
- **Nothing lingers in staging.** Each upload is deleted right after validation,
  whether accepted or rejected. A ticket admits only its first object version;
  later POSTs with the same upload form are deleted on arrival.
- **Malformed input.** Corrupt deflate data and deeply nested JSON are rejected
  like any other invalid upload.

A client chooses its alias (up to 64 characters), its license and bounded timing
measurements. These cannot be verified. SBI master names and market values also
remain unverified contributor content; exact fields and canonical encoding do
not establish market accuracy or redistribution permission. Upload quotas apply
to every source.

### Publication and limits

- Upload tickets last 15 minutes and bind the object key, exact size and encryption.
- Uploads are bound to their first S3 object version. Retries of that event are
  safe, and overwriting an upload cannot replace a published dataset.
- Recordings are content addressed: `archive/YYYYMMDD/SHA256/`, where SHA-256 is
  that of the published gzip. Publication writes the data first and the immutable
  manifest last, as the catalog commit marker.
- Limits: 64 MiB compressed, 1 GiB expanded and 16 MiB per JSONL line. Tickets
  are limited to four per IP address per hour (stored as a keyed hash, never the
  address), 64 per hour in total and 5 GiB of authorized uploads per UTC day. A
  complete replay is about 34 MB compressed.
- Ticket metadata in staging expires after two days. Published data is retained.
- `pull` checks compressed size, SHA-256, the complete stream, the manifest and
  the reference replay where available, then moves the folder into place atomically.
  Manifest validation and archive identity checks precede payload download.
  Listings warn and skip invalid/unsupported manifests, including old probes
  with incomplete reference metadata, without hiding S3/network failures.

### Sources

The decoded schema accepts `historical_mock`, `synthetic_test` and `sbi_live`.
The small, self-authored `synthetic_test` fixture verifies publication; filter by
source when selecting market data. Opted-in decoded SBI recordings retain
`source=sbi_live` and undergo canonical-field, integrity and continuity checks.
Their market accuracy and redistribution permission are contributor-declared;
they are not silently treated as reference-verified demo data. New raw-wire
submissions remain unsupported. The existing legacy wire capture is labeled
`sample_data`, with schema `brisk-sbi-wire-jsonl-v1`, for protocol research. It is
separate from the validated decoded-recording catalog and retains its own manifest.

## Development and deployment

Tests and conventions are in [CONTRIBUTING.md](CONTRIBUTING.md). Archive
deployment, IAM scope, costs and removal are in [infra/README.md](infra/README.md).
Archive changes reach contributors only after `infra/deploy.py` runs.
