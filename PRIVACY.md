# Privacy policy

[English](PRIVACY.md) | [日本語](PRIVACY.ja.md)

Policy version 10, effective 30 October 2026. Applies to the `brisk` Python API and
CLI in this repository, the private recording archive they upload to and its cloud API.

## Your private archive

Uploads are private. Each one is filed under your **identity**: a random secret the tool
makes on your computer the first time it needs one. It is kept in your system's credential
store (the macOS Keychain or Windows Credential Manager; elsewhere, or with
`BRISK_IDENTITY_STORE=file`, only the file) and in `~/.config/brisk/identity.json` (or
`$XDG_CONFIG_HOME/brisk/`), readable only by you; each copy restores the other. There is no
registration, account, email address or password. The archive knows you only by your
**owner ID**, a one-way hash of the secret, and lists, downloads and deletes only the
uploads of the owner ID a request proves it holds. Nobody else can list or download them.
The bucket's only public objects are the samples under `samples/`, a copy of the legacy
wire-format sample capture.

- Whoever holds the secret is you to the archive. Keep it private. It also reads as a
  28-character **recovery code**, shown when the identity is made and again with
  `brisk identity --recovery-code`; `brisk identity --recover CODE` uses it on another
  computer, and `BRISK_IDENTITY` supplies the secret to a process instead.
- Losing every copy and the recovery code loses access to what that identity uploaded.
  The uploads remain stored until the operator removes them.
- `brisk delete PREFIX` (or `briskapi.Archive().delete(prefix)`) removes a recording or
  timing report from your archive: you can no longer list or download it. For **30 days**
  you can bring it back: `brisk list --deleted` shows what you deleted and until when, and
  `brisk restore PREFIX` (or `briskapi.Archive().restore(prefix)`) restores it. After that
  it can't be restored, and the storage removes it within a day.
- The archive's operator (below) runs the storage and can access what it holds, as with
  any hosted service. The operator does not publish or share it.
- **Your bars.** For each trading day of your broker recordings (SBI, e-shiten, Matsui, Monex or SMBC Nikko), the
  archive builds OHLCV bars (open, high, low, close and volume per security, from one second
  to one day) from the trades in your own current recordings of that day, counting each trade
  once, and keeps them under your owner ID. They hold only the bars your recordings cover
  completely. No one else's uploads enter them, and yours enter no one else's. They are
  rebuilt whenever you upload, delete or restore a recording of that day, and replaced
  versions are removed within a day.
- **Cloud API.** Queries (`briskapi.Historical`, `brisk query`) use your identity as the API
  key and read only your own recordings, your bars as above and the public demo, which
  any identity may query. Results are kept for up to a day so that large ones can be
  downloaded. It is free, with quotas per identity (60 requests a minute, 2 GB a day) and per
  IP address (120 requests a minute, 4 GB a day); the service does not log what you query.

## Automatic uploads

Uploads are on by default. The first time you record, package or open a live session from
the command line, the tool shows a short notice of everything described below and asks
once. Pressing Enter accepts, and the choice is saved for future runs. After that, every
clean and complete session is uploaded to your private archive without further prompts: demo sessions (`brisk record`, `brisk live`,
`briskapi.connect()`, `briskapi.record()`), and broker sessions (SBI, Tachibana e-shiten, Matsui,
Monex or SMBC Nikko: `brisk live --site sbi` or another broker's site, `briskapi.connect(site=...)`), which upload a timing
summary and their decoded market recording. A broker session records one trading day after
another until you stop it, and normally uploads each day in two recordings, the morning session's once the
lunch break begins and the afternoon's when the day ends (when the site serves
the next trading date, or the server closes the stream). A session reaching 80 million quote rows is split
again at a completed spool part; each piece follows the same upload choice. If the connection to your broker's BRiSK
drops during a day, the session reconnects by itself: what it recorded before the drop is a
complete recording of its own, uploaded like the others, and the rest of the day is recorded
from a fresh snapshot. Stopping a broker session (Ctrl-C, `kill`, shutting the computer down,
or `close()` in Python) ends its day there, as the day's end would, so what is left of the day is uploaded
like any recording. While a live session runs, each day is written to your computer as it
arrives, as Parquet files in briskapi's spool folder (`~/.local/share/brisk/spool`, or under
`$XDG_DATA_HOME`), so that a crash loses at most its last half-minute; they are deleted once
that day is packaged.

Python API calls never prompt. Where nothing can ask, in the Python API or a command run
without a terminal, and you have not decided yet, sessions are uploaded by default: the tool
says so once per process (a Python warning, or on stderr), naming your owner ID and the
opt-outs, and makes your identity if there is none. Deciding with `brisk consent --accept`
or `--revoke` (`briskapi.consent(accept=True)` or `revoke=True`) replaces the default. Partial replays (`--limit-frames`) are never uploaded. A broker session that couldn't end its
day, because a crash or a forced kill stopped it or it didn't end within 30 seconds of being
asked to stop, is finished for it: the day so far (all but at most its last half-minute) gets the
end marker the session never wrote and is kept and uploaded as that session's settings said
(right away, or by the next feed you start after a crash). A session that kept nothing, with
uploads off and no `--output`, keeps nothing then either.

Opting out:

- decline at the first prompt (`n`);
- `brisk consent --revoke` (or `briskapi.consent(revoke=True)`) turns uploads off until you
  turn them back on with `brisk consent --accept`;
- `BRISK_UPLOAD=0` disables uploads for any process with that environment;
- `--no-upload` (`upload=False` in Python) keeps one run entirely local;
- `--no-upload-market-data` (`upload_market_data=False` on `briskapi.connect()`) keeps one broker
  capture's market recording local while its timing summary is uploaded.

## What an upload contains

Each stored recording consists of four Parquet tables holding the decoded recording, a list
of where they are stored and `manifest.json`. A demo recording's market tables are the same
for everyone, so the archive keeps them once and each demo upload sends and stores only its
own timing table; downloading it returns the whole recording.

- **Market data you recorded**: the BRiSK demo's securities master and decoded auction
  quotes, or a broker session's decoded securities
  master and auction quotes (prices, quantities, turnover, issue status, codes, flags and
  source clocks; on each side of the order book the ten best prices holding orders, with
  their quantities and order counts, the same for the closing-condition orders, and the
  whole board's order totals; and in a broker's recording every price holding orders on
  either side of both books, as what changed from one update to the next, with each
  security's whole books every 1,000 changes); and every trade, with its time, price, shares and side,
  including those the session's snapshot restores from earlier that day. Version 5 added
  the order book, turnover and broker issue status; version 6 the trades, the
  closing-condition books and the board totals; version 9 the same from Matsui, Monex and SMBC
  Nikko sessions; version 10 the whole books beyond the ten best prices. Demo content is checked against the pinned
  reference replay. Broker content is uploader-declared: the archive checks that it holds
  exactly these fields, canonically encoded, and that it is well formed (identities, order, clocks,
  value ranges), but not the market's own consistency.
- **Local timing measurements**: per-frame decode time (`decode_ns`), your computer's
  wall-clock receipt time in milliseconds (`received_unix_ms`), replay lateness, total
  replay time and demo asset download time, plus whether the assets came from the website
  or a local cache. The receipt clock shows the date and time you made the recording.
  Timing values can loosely reflect your computer's speed and load.
- **Manifest**: a summary (source, trading date, securities covered, clock range, counts,
  sizes and SHA-256).

Broker packages include per-frame receipt/decode timing and session duration. They retain
only the site's fixed WebSocket origin as transport provenance (`sbi.brisk.jp`,
`next.e-shiten.brisk.jp`, `matsui.brisk.jp`, `monex.brisk.jp` or `smbcnikko.brisk.jp`); connection and decoder diagnostics are removed before packaging.

## What is never collected

The tool sends nothing about brokerage or BRiSK accounts, cookies, session tokens,
passkeys, orders, usernames, hostnames, file paths, environment variables or hardware
identifiers. Your identity secret travels only to the archive's function, over HTTPS, to
prove which owner ID a request is for; the service keeps the owner ID, never the secret. The
archive service accepts only the fields listed above and rejects any recording that
contains other fields.

## Broker BRiSK

The BRiSK site client (`briskapi.brisk`; `brisk live --site sbi` for SBI, and `site=` and
`brisk live --site` `e-shiten` for Tachibana e-shiten BRiSK Next, `matsui` for Matsui, `monex` for
Monex and `smbcnikko` for SMBC Nikko) uses session cookies from your own browser. They are credentials:

- They are sent only to the site that issued them (`https://sbi.brisk.jp`,
  `https://next.e-shiten.brisk.jp`, `https://matsui.brisk.jp`, `https://monex.brisk.jp` or
  `https://smbcnikko.brisk.jp`), together with the API token it issues. The
  client never follows redirects, so they can't be forwarded elsewhere. The live
  stream may be on another BRiSK host (`api.brisk.jp`); like a browser, the client
  sends it the session named in the site's boot response, not your cookies, and
  refuses a stream outside `brisk.jp`.
- They stay in memory unless you call `briskapi.login(..., remember=True)`, which saves
  them to `~/.config/brisk/sbi-cookies.json` (`<site>-cookies.json` for the others, such as
  `e-shiten-cookies.json`; or `$XDG_CONFIG_HOME/brisk/`), readable only by you. `briskapi.logout()`
  deletes them.
- The live host receives them through its environment, not its command line.
- `brisk enroll` / `brisk login` (passkey sign-in to the broker you choose: SBI, Matsui, Monex,
  SMBC Nikko, Tachibana e-shiten or one you add) capture a passkey that you register at that broker and use it to
  obtain BRiSK session cookies. The passkey's private key is a credential that lets whoever
  has it sign in to your brokerage account. It is kept, together with the name of the broker (and, for
  a broker whose passkey form asks for it first, such as e-shiten, your user ID), only
  in the macOS Keychain or Windows Credential Manager (or, if you set `BRISK_PASSKEY_STORE=file`, in
  an owner-only file under `~/.config/brisk/`, which on Windows is protected by your user profile
  folder), is passed to the sign-in helper over stdin and never on a command line,
  and is never logged, uploaded or included in any recording. Each broker has its own saved passkey; `brisk forget` deletes one here;
  remove it at the broker as well. The user ID is typed into the broker's login page and is never
  logged either. The five brokers' cookies are saved as described above; those of a broker you
  add, which has no data client, are kept in an owner-only file under `~/.config/brisk/cookies/`. The Chrome profile used for sign-in is
  temporary and deleted afterwards (one left by a force-killed run is removed at the next start),
  and only the BRiSK host's cookies are taken from it.
- With uploads on, a broker session's decoded market recording is normally uploaded in two pieces a trading
  day: the morning session's once the lunch break begins, and the afternoon's (starting from a snapshot
  of the book, which repeats the latest quote of every security) when the day ends, with extra splits at
  80 million quote rows. The part of a day before a lost connection is uploaded once the session reconnects;
  what is left of the day is uploaded when you stop the session, or when it crashes or is killed (repaired,
  as above). `--no-upload-market-data` or
  `briskapi.connect(site=..., upload_market_data=False)` keeps it local.
- With uploads on, a broker session uploads one **timing summary** per
  trading day (per part of a day, when a lost connection or a stop splits it) to your private
  archive when the day ends, or is stopped (after at least 100 frames): the
  p50/p90/p99/max of per-frame decode time, of the feed clock's age at local receipt and of
  the spacing between frames; the number of gaps over one second; the frame count; the
  trading date; the first and last minute (JST) of the session; and the briskapi version.
  `upload=False` on `briskapi.connect()` or `BRISK_UPLOAD=0` keeps both the summary and any market
  recording local. The environment opt-out suppresses the interactive question as well.

## Network metadata

Uploading, listing, downloading, deleting and querying contact AWS Lambda function URLs and
Amazon S3 in Tokyo (`ap-northeast-1`); an upload enters AWS at its nearest edge location (S3
Transfer Acceleration) and reaches Tokyo over AWS's own network. These services receive your IP
address with each request.

- The services use your IP address only for rate limits: every request counts against it
  (120 a minute), and so do upload tickets, timing reports, deletes and restores, and the bytes
  the cloud API returns. Identities cost nothing to make, so an address is what keeps one
  computer from guessing or multiplying them. The services store a keyed hash (HMAC-SHA-256 with
  a secret per-deployment key) of the address, or of an IPv6 address's /64 block, never the
  address itself, and your owner ID for per-owner limits, in separate entries. Each entry is
  marked to expire after twice its counting window (two minutes, two hours or two days), and
  DynamoDB deletes expired entries, usually within a few days.
- The service does not log IP addresses, identity secrets or recording contents. Its logs
  record infrastructure errors and are deleted after 14 days. S3 server access logging is
  not enabled.

Recording the demo downloads its assets from `https://next-demo.brisk.jp/`. That
request goes to BRiSK's servers and is covered by their policies, not this one.

## Storage, access and retention

- **Stored recordings and timing reports are private to their owner ID** and kept until you
  delete them (or the operator removes them). What you delete stays restorable for 30 days
  and is removed by the storage within a day after that.
  Recordings are content addressed under your owner ID: the same recording uploaded twice is
  stored once. Their names carry the trading date and their first and last clock time.
- Uploads are validated in a private staging area and deleted immediately after validation,
  whether they are accepted or rejected. Ticket metadata (manifest, owner ID and status) in
  staging expires after two days. Rejected uploads are never stored.
- Recordings stored before policy version 6, when the archive was public, are no longer
  public or reachable through the tool; the operator holds them and removes them on request.
  The exception is the legacy wire-format sample capture (2026-03-11), copied to `samples/`
  as a public download.
- On your computer, the tool stores your upload choice in `~/.config/brisk/uploads.json` and
  your identity in `~/.config/brisk/identity.json` (or `$XDG_CONFIG_HOME/brisk/`) and in the
  system's credential store under `briskapi-archive-identity`.
  `briskapi.pull()` caches verified downloads in `~/.cache/brisk/` (or
  `$XDG_CACHE_HOME/brisk/`). Delete these files at any time; deleting every copy of the
  identity without its recovery code ends access to what it uploaded.

## Operator and changes

The archive is operated by the owner of
[honvl/briskapi](https://github.com/honvl/briskapi). AWS provides the
infrastructure. Send removal requests and privacy questions to the operator through
that GitHub account. Do not post personal data in a public issue.

If a future version collects anything beyond what is described here, or uses it otherwise,
the policy version increases. Earlier choices then no longer apply, a refusal as much as an
acceptance: the command line asks again before uploading, and where nothing can ask (the Python API,
a run without a terminal), the tool warns once that the policy changed, naming the choice it replaces,
and uploads are on by default until you decide, as before you first decide.
