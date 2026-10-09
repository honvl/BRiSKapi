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

## Passkey sign-in

`briskapi.passkey` (CLI: `brisk sites | enroll | login | forget`) gets a broker's BRiSK session cookies by
signing in with a passkey, in place of copying them from a browser. It is not tied to any one broker:
a *site* (`briskapi/sites.py`) says where the login page is, what its passkey button says and which host
the BRiSK cookies belong to. SBI, Matsui, Monex and SMBC Nikko are built in (each runs BRiSK on its own
`<broker>.brisk.jp`, found by checking the hosts and the brokers' own pages; none is verified against a
live account), and `~/.config/brisk/sites.json` adds or edits entries. A person normally has one
brokerage, so the saved login is a single record, `{site, credential}`, and `login` needs no site argument.

### How a passkey sign-in works, and why Chrome can stand in

A passkey is a WebAuthn credential: a key pair made for one site (its *relying party id*, the site's
domain). Registration makes the authenticator create the pair and hand the public key to the site.
Signing in goes like this: the site sends a random challenge; the browser passes it, with the page's
origin, to an authenticator; the authenticator checks that the origin matches the credential's relying
party id, confirms the user (presence, and verification such as a fingerprint) and signs the challenge,
the origin and a counter; the site checks the signature against the stored public key, the flags and
that the counter did not go backwards. Nothing in that exchange says what kind of authenticator signed.
A site can only learn that from an *attestation* statement made at registration, and browsers and
sites mostly use "none" for passkeys. So an authenticator that is software, and that reports presence
and verification as done, produces a sign-in the site cannot tell from a phone's. Chrome's DevTools
`WebAuthn` domain provides exactly that (a *virtual authenticator*), including exporting and importing
a credential, which is how the passkey is kept between runs. Two things limit it: the site may detect
the automated browser by other means, and the passkey is now a secret that sits in software.
Passless-style virtual USB devices were ruled out: they need `/dev/uhid` (Linux only), and on macOS
creating a virtual HID device needs an entitlement that Apple grants on request. The Chrome route
needs neither and runs wherever Chrome and Node do: macOS, Windows and Linux. On Windows it is run from
Windows itself, using Windows' Chrome, Node and Python; WSL is Linux and would need its own Chrome.

### The helper

`decoder/passkey.cjs` runs under Node and drives a Chrome it starts itself. It holds no site of its own:
the login URL, button text and cookie host come in the request.

1. **Private pipe, no port.** Chrome is started with `--remote-debugging-pipe` and talked to
   over file descriptors 3 and 4 (NUL-framed JSON). A loopback debugging port would let any
   local process attach and read the passkey back out with `WebAuthn.getCredentials`. Windows
   passes those two descriptors to Chrome too (Chrome has a separate named-pipe switch for
   Windows, which is not needed): checked with native Node 25.8 and Chrome 154 on Windows 11.
2. **Ephemeral profile.** The profile is a new private directory, deleted when Chrome exits
   (`--profile-dir` opts into a persistent one). It holds the broker's logged-in session, so a helper
   that is force-killed (which cannot clean up after itself) must not leave it around: each profile
   records its owner's process id, and every start removes `brisk-passkey-*` profiles whose owner is
   gone, leaving live and just-created ones alone. The mock keychain and basic password store
   keep Chrome from prompting for Keychain access. Chrome reports `navigator.webdriver` as
   true whenever DevTools is attached (checked in both modes), and headless Chrome's user
   agent says so too, so a site that blocks automated browsers can tell. The host does not
   hide this. The default is a visible window, which lets a person take over where a guess
   about a broker's pages is wrong or the broker refuses the automation.
3. **A virtual authenticator on every page**, including popups and new tabs
   (`Target.setAutoAttach`). It is a CTAP2.1 platform authenticator with resident keys that
   approves presence and verification itself.
4. **Enrolling** attaches an empty authenticator, opens the login page and waits for
   `WebAuthn.credentialAdded` while the person registers a passkey. The captured credential is
   kept in memory and saved, with the chosen site, only after the person confirms, so a failed
   enrollment never replaces a passkey that still works. `enroll` refuses to overwrite without
   `--replace`, and with no `--site` it asks which broker (or fails, without a terminal).
5. **Signing in** imports the saved credential, opens the login page, clicks the control whose
   text contains the site's `passkey_button` (trusted `Input` mouse events; the shortest matching
   label wins, so a "can't sign in?" link does not) and waits for `credentialAsserted`. Then it
   opens `launch_url` when there is one, or waits for the person to open BRiSK, and polls for a
   cookie that starts with the site's `cookie_prefix` on exactly its BRiSK host. Only that host's
   cookies are returned, never those of the broker's main site, whose session is destroyed with
   the profile.
6. **Delivery.** SBI is the one site with a data client, so its cookies go to `sbi.login()`; the
   others are saved to `~/.config/brisk/cookies/<site>.json` (owner-only) for the person's own use.
   A data client sends its cookies to one host, so a site bound to a client whose cookie host is not
   that client's host is refused before Chrome starts (a `sites.json` entry can never claim a client).

The sign counter is the delicate part. A relying party may reject an assertion whose counter
does not exceed the last it saw, as a sign of a cloned authenticator, and every sign-in
advances it. The host therefore streams the credential as JSON lines after every change, the
Python side saves each one at once, and a failed or interrupted run still reports the
latest. If a save fails, the helper is stopped rather than allowed to sign in again with a
counter that would no longer match.

Secrets move only over stdin and stdout, never on a command line or in a log message (logs
name origins, never paths or queries, and the helper refuses to run attached to a terminal).
The saved login is kept in the macOS Keychain through `security -i`, which reads its command from
stdin: `add-generic-password -w SECRET` would show the secret in the process list. `security`
exits 0 even when a write fails, so each write is read back and compared. On Windows it is one
Credential Manager entry for this user on this machine (`CredWriteW` and friends through `ctypes`, no
extra dependency; an entry holds at most 2,560 bytes, which a passkey fits in), also read back after
each write. Elsewhere it is stored only if `BRISK_PASSKEY_STORE=file` asks for an owner-only
file, which is weaker (and on Windows a file's privacy comes from its folder's ACL, since `0600` means
nothing there). The private key is the whole credential: whoever has it can sign in.

Three things differ on Windows, and the code and tests cover each. A process cannot be signalled to stop
there (terminating one kills it outright, which would leave Chrome and its profile behind), so the
helper treats a closed stdin as the request to close Chrome and exit, on every platform; the Python side
closes it first and terminates only if the helper does not exit within `GRACE` (20) seconds, which is
longer than closing Chrome and deleting its profile can take. (Killing the helper outright does make
Chrome exit on its own within about a second on Windows, because its pipe closes.) Text between
Python and Node is always UTF-8, because Python's default there is the system code page (cp1252) and a
Japanese user name or button text would fail to decode; `sites.json` is read as UTF-8 and also accepts
the byte-order mark and the UTF-16 that Windows editors and PowerShell 5.1 write. And deleting the temporary profile retries,
because Windows can keep a file locked for a moment after Chrome exits.

Tests run Chrome against a fake broker (`tools/brisk_mock/fake_sbi_passkey.cjs`) that verifies
what a real site verifies: the ES256 signature, the relying-party hash, user presence and
verification, and a counter that must increase. They cover enroll, sign-in, a stale passkey
being refused as a clone, `launch_url`, a missing passkey control, delivery to a data client and to a
file, and that nothing secret is logged. They skip when Chrome is absent. They were run on macOS, on native Windows 11 (Windows' own Node,
Python and Chrome; the full suite including the real Credential Manager) and, in CI, on Linux.

What is not verified against any real broker: each site's login URL (SBI's redirected to a
maintenance page when checked, SMBC Nikko's and Monex's refuse a bare request), the passkey control's
text, how BRiSK is launched from the main site, the name of the BRiSK session cookie (only SBI's is
known, so the other sites accept any cookie on their BRiSK host once it appears), whether a broker
accepts a virtual authenticator at registration or asks for more identity checks, whether it refuses
an automated Chrome (see above), and how long the cookie lasts. Each is an overridable default or a
manual step, and a wrong guess ends in an explicit error. The automated runs on Windows are headless; the
visible Chrome window of `enroll` and `login` was also tried by hand on Windows 11 (the real console
prompt, Credential Manager) and on macOS. The DevTools `WebAuthn` domain is marked experimental.

The mechanism itself is verified against a real site that is not a broker, webauthn.io (a public passkey
demo; it logs you in on load when a passkey is present, which `login` catches because it listens before
loading the page): registration through the virtual authenticator, `brisk login` with its cookie and the
advancing sign counter, on macOS and by hand on Windows. A `sites.json` entry for it needs only
`login_url` `https://webauthn.io/`, `cookie_host` `webauthn.io`, `passkey_button` `Authenticate` and
`cookie_prefix` `sessionid`.

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
