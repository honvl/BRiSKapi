'use strict';
// Experimental live host for broker-hosted BRiSK: SBI's (sbi.brisk.jp, BRISK_SITE=sbi, the
// default), Tachibana e-shiten's BRiSK Next (next.e-shiten.brisk.jp, BRISK_SITE=e-shiten), and
// Matsui's, Monex's and SMBC Nikko's (<site>.brisk.jp, BRISK_SITE=matsui|monex|smbcnikko).
// The session's own WASM decoder under Node, fed from the authenticated WebSocket.
// No browser or Chrome DevTools. Emits the same JSON batches as decoder.cjs
// (source=brisk_live, each with its site's transport; SITES below). Cookies come from the site's variable
// (BRISK_SBI_COOKIES, BRISK_ESHITEN_COOKIES, ...; a JSON object), never from the command line.
//
// It follows the vendor client's connect sequence (public Next demo bundle) and
// upstream's SBI research (pybrisk): feed the WASM from the first frame, wait for its
// frame numbers, catch the snapshot up to them, answer the server's pings through the WASM,
// and watch the server heartbeat. The stream is expected to be plain binary (see engineio.cjs
// for the Socket.IO fallback and why it is unverified); the profile (BRISK_PROFILE) can
// set that fallback's parameters and the catch-up format, and `--trace-protocol` shows what
// the server answers.
const crypto = require('node:crypto');
const { once } = require('node:events');
const fs = require('node:fs');
const { monitorEventLoopDelay } = require('node:perf_hooks');
const { Decoder } = require('./decoder.cjs');
const { Link, resolveProfile, heartbeatMonitor, staleDataCheck, redact } = require('./engineio.cjs');
const { QuotePool, workerCount, POOL_MIN_ISSUES } = require('./workers.cjs');

// Brokers whose BRiSK is this app (briskapi/brisk.py keeps the same table). On 2026-10-08 SBI's and e-shiten's
// served the same boot (flex_version 18000), master, snapshot and decoder, so only the address differs, and every
// broker's recordings are one source (SOURCE), each with its site's transport provenance. Matsui's, Monex's and
// SMBC Nikko's are the same vendor's app on hosts of their own, not yet tried live: their bundles are found from the
// signed-in page as SBI's and e-shiten's are, and anything unexpected is an error.
const SOURCE = 'brisk_live';
const SITES = {
  sbi: { origin: 'https://sbi.brisk.jp', name: 'SBI BRiSK', transport: 'sbi_websocket', cookies: 'BRISK_SBI_COOKIES' },
  'e-shiten': { origin: 'https://next.e-shiten.brisk.jp', name: 'e-shiten BRiSK Next', transport: 'eshiten_websocket',
    cookies: 'BRISK_ESHITEN_COOKIES' },
  matsui: { origin: 'https://matsui.brisk.jp', name: 'Matsui BRiSK', transport: 'matsui_websocket', cookies: 'BRISK_MATSUI_COOKIES' },
  monex: { origin: 'https://monex.brisk.jp', name: 'Monex BRiSK', transport: 'monex_websocket', cookies: 'BRISK_MONEX_COOKIES' },
  smbcnikko: { origin: 'https://smbcnikko.brisk.jp', name: 'SMBC Nikko BRiSK', transport: 'smbcnikko_websocket',
    cookies: 'BRISK_SMBCNIKKO_COOKIES' },
};
const PROTOCOL_VERSION = 18000;
const MAX_BYTES = 64 * 1024 * 1024;
// The site's decoder must provide everything the host calls (the demo ABI minus portfolio).
const REQUIRED = ['_initialize', '_push', '_applyBasePriceQueue', '_stockCount', '_getStockMaster', '_unserialize',
  '_getDate', '_getTime', '_pushWs', '_getFrameNumbers', '_apiRecieved', '_addTraceUpdate', '_getTrace',
  '_clearTrace', '_getStockView', '_fitItaViewRowPrice10', '_getItaRows', '_malloc', 'addFunction'];

function site(id = 'sbi') {
  if (!Object.hasOwn(SITES, id)) throw new Error(`Unknown BRiSK site ${JSON.stringify(id)}; use one of: ${Object.keys(SITES).join(', ')}`);
  return { id, ...SITES[id] };
}

const cookieHeader = cookies => Object.entries(cookies).map(([k, v]) => `${k}=${v}`).join('; ');

// The cookies `brisk login` saved in `file` (a JSON object), or null when there are none to read.
function cookiesIn(file) {
  try {
    const cookies = JSON.parse(fs.readFileSync(file, 'utf8'));
    return cookies && typeof cookies === 'object' && Object.keys(cookies).length ? cookies : null;
  } catch {
    return null;
  }
}

class Session {
  constructor(cookies, fetchImpl = fetch, where = site()) {
    if (!cookies || typeof cookies !== 'object' || !Object.keys(cookies).length) {
      throw new Error(`Set ${where.cookies} to your ${where.name} session cookies (JSON object)`);
    }
    this.cookie = cookieHeader(cookies);
    this.fetch = fetchImpl;
    this.origin = where.origin;
    this.name = where.name;
    this.token = null;
  }

  get(path, kind = 'json') {
    return this.request(path, { kind });
  }

  // The cookies of a login made since: the API token came with the old ones, so it is asked for again.
  use(cookies) {
    this.cookie = cookieHeader(cookies);
    this.token = null;
  }

  async request(path, { method = 'GET', body, contentType, kind = 'json' } = {}) {
    const headers = { cookie: this.cookie };
    if (this.token) headers.authorization = `Bearer ${this.token}`;
    if (contentType) headers['content-type'] = contentType;
    // Redirects mean a login page; following them would forward credentials elsewhere.
    const response = await this.fetch(new URL(path, this.origin), { method, headers, body, redirect: 'manual', signal: AbortSignal.timeout(30000) });
    if ([301, 302, 303, 307, 308, 401, 403].includes(response.status) || response.type === 'opaqueredirect') {
      throw new Error(`${this.name} session expired or invalid; log in again`);
    }
    if (!response.ok) {
      const error = new Error(`${this.name} ${path}: HTTP ${response.status}`);
      error.status = response.status;
      error.reason = response.headers?.get?.('x-error-reason') ?? null;
      throw error;
    }
    const data = Buffer.from(await response.arrayBuffer());
    if (data.length > MAX_BYTES) throw new Error(`${this.name} ${path}: response too large`);
    return kind === 'json' ? JSON.parse(data) : kind === 'text' ? data.toString('utf8') : data;
  }
}

// How the catch-up request is written. The vendor client's, as pybrisk's public capture of
// sbi.brisk.jp (March 2026) recorded it: one JSON object of three parallel lists, each issue's
// update numbers from the snapshot's up to the stream's first. Its binary reply is pushed into
// the decoder as it comes. A profile can still name another format (catchUp).
const CATCH_UP_FORMATS = {
  'issue-lists': issues => ({ contentType: 'application/json',
    body: JSON.stringify({ issue_ids: issues.map(i => i.issue_id), update_numbers_from: issues.map(i => i.from),
      update_numbers_to: issues.map(i => i.to) }) }),
};
const DEFAULT_CATCH_UP = 'issue-lists';

// The vendor client opens the market stream as `new WebSocket(url, "flex")` (its stream service's
// start(), in the SBI app bundle of both March and October 2026), so the upgrade asks for the
// WebSocket subprotocol "flex". Live upgrades sent without it were refused with HTTP 502 whatever
// the other headers; whether the subprotocol is all the server wants is not yet confirmed.
const STREAM_PROTOCOL = 'flex';

// The SBI app's connection check fails when no server heartbeat has come for 10 s
// (heartbeatIntervalMilliSeconds: 1e4 in its configuration); heartbeats come every 5 s.
const HEARTBEAT_MS = 10000;

// A recording covers one trading day. The host asks the site for its trading date every ten minutes;
// when the site's session is done or it serves the next day (or the stream's clock goes back, as a new day's
// would), the day's session ends cleanly and the next one starts from a fresh boot, master and snapshot.
const DAY_CHECK_MS = 10 * 60 * 1000;
// Waiting for a session once the next one is due (the boot's next_date), how often the boot is asked again
// (the vendor app's own check of the date is every minute).
const STREAM_CHECK_MS = 60 * 1000;
const NEW_DAY_US = 3600e6;  // a clock an hour behind the last frame's belongs to another day
// With quote workers, how far the output may fall behind the stream (quotes read, not yet written) before the
// session stops rather than fill the memory.
const OUTPUT_BACKLOG_BYTES = 2 ** 30;
// The fast lane's capacity: symbols per worker of its pool (see live and _gateway.py).
const FAST_SYMBOLS_PER_WORKER = 500;

// Fetch the frames between the snapshot and the stream's first frame. Mirrors the vendor
// client: up to five retries, but never for an expired session or a snapshot that is too old.
async function fetchCatchUp(session, { app, issues, format = DEFAULT_CATCH_UP, retries = 5, backoffMs = n => 1000 + 1000 * n, trace }) {
  const build = CATCH_UP_FORMATS[format];
  if (!build) {
    throw new Error(`The snapshot is ${issues.length} issues behind the stream, so it must be caught up with `
      + `POST /api/stocks_update, but "${format}" is not a known format (known: ${Object.keys(CATCH_UP_FORMATS).join(', ')})`);
  }
  const { contentType, body } = build(issues);
  const path = `/api/stocks_update/${encodeURIComponent(app.series)}?date=${encodeURIComponent(app.date)}`;
  for (let attempt = 0; ; attempt++) {
    try {
      trace?.(`catch-up: POST ${path} (${issues.length} issues, ${body.length} bytes, format ${format})`);
      return await session.request(path, { method: 'POST', body, contentType, kind: 'bytes' });
    } catch (error) {
      if (error.reason === 'too-old') {
        throw new Error(`${session.name} says the snapshot is too old to catch up (x-error-reason: too-old); restart to load a fresh one`);
      }
      if (/session expired/.test(error.message) || attempt >= retries) {
        throw new Error(`Catch-up failed: ${error.message}${error.status ? ` (the server rejected format "${format}")` : ''}`);
      }
      trace?.(`catch-up attempt ${attempt + 1} failed: ${error.message}`);
      await new Promise(resolve => setTimeout(resolve, backoffMs(attempt)));
    }
  }
}

// The decoder is served to logged-in sessions only and changes with releases, so it is
// located through the app's own bundles rather than pinned. The page may name its bundles by
// absolute or relative path (e-shiten: `main-XXXX.js`). A bundle names the loader either as a
// literal path or, in both sites' builds of 2026-10-08, as a template with an exchange variant,
// `./assets/wasm${r}/fita.<hash>.js` (r is '-fukuoka' for the Fukuoka exchange); only the Tokyo
// build is used. Without a literal .wasm path, its name follows the loader's (Emscripten's rule).
async function decoderAssets(session) {
  const page = await session.get('/', 'text');
  const scripts = [...page.matchAll(/<script\b[^>]*\ssrc=["']([^"']+\.js)["']/g)]
    .map(m => new URL(m[1], session.origin + '/'))
    .filter(url => url.origin === session.origin)  // never send the session to another host
    .map(url => url.pathname);
  for (const script of scripts) {
    const source = await session.get(script, 'text');
    const js = source.match(/["'`](?:\.?\/)?assets\/wasm(?:\$\{[\w$.]+\})?\/(fita[\w.-]*)\.js["'`]/);
    if (!js) continue;
    const wasm = source.match(/["'`](?:\.?\/)?assets\/wasm\/(fita[\w.-]*)\.wasm["'`]/);
    const paths = [`/assets/wasm/${js[1]}.js`, `/assets/wasm/${wasm ? wasm[1] : js[1]}.wasm`];
    const assets = { 'fita.js': await session.get(paths[0], 'bytes'), 'fita.wasm': await session.get(paths[1], 'bytes') };
    return { assets, paths };
  }
  throw new Error(`${session.name} decoder not found in the app bundles; the site layout may have changed`);
}

// Where the stream is. The boot's ws_url is relative, and the browser resolves it against the
// API endpoint the frontend boot names (https://api.brisk.jp on e-shiten), not against the site
// that served the page. ws_url carries the session, so only a BRiSK host may receive it.
function streamUrl(wsUrl, apiEndpoint, origin) {
  const base = typeof apiEndpoint === 'string' && apiEndpoint.startsWith('https://') ? apiEndpoint : origin;
  const url = new URL(wsUrl, base.replace(/^https:/, 'wss:'));
  if (url.protocol !== 'wss:' || !(url.hostname === 'brisk.jp' || url.hostname.endsWith('.brisk.jp'))) {
    throw new Error(`Refusing to stream from ${url.protocol}//${url.host}: not a BRiSK host`);
  }
  return url;
}

// The master's record layout also depends on the protocol version, and 18000's is not independently
// confirmed. A wrong layout would read tick types as codes, limits as lot sizes and so on, so
// refuse a master that does not look like a market. (The 2021 demo's passes every test at 99.98%.)
// The decoder runs this as soon as it has read the master, before boot data is matched to it.
function checkMaster(master, protocolVersion) {
  const fraction = predicate => master.filter(predicate).length / (master.length || 1);
  const problems = [];
  if (master.length < 1000) problems.push(`only ${master.length} issues`);
  const rules = [
    // Four digits, five for some preferred shares, or since 2024 letters too (167A).
    ['security codes', 0.95, m => /^[0-9][0-9A-Z]{3,4}$/.test(m.code)],
    ['positive base prices', 0.98, m => m.base_price10 > 0],
    ['limits around the base price', 0.98, m => m.limit_up10 > m.base_price10 && m.base_price10 > m.limit_down10],
    ['lot sizes of 1 to 1,000,000', 0.98, m => m.lot_size >= 1 && m.lot_size <= 1e6],
    ['names', 0.95, m => m.name.length > 0],
  ];
  for (const [what, minimum, predicate] of rules) {
    const seen = fraction(predicate);
    if (seen < minimum) problems.push(`${what}: ${(100 * seen).toFixed(1)}% (need ${(100 * minimum).toFixed(0)}%)`);
  }
  const distinct = new Set(master.map(m => m.code)).size / (master.length || 1);
  if (distinct < 0.99) problems.push(`distinct codes: ${(100 * distinct).toFixed(1)}% (need 99%)`);
  if (problems.length) {
    throw new Error(`BRiSK stock master layout not recognised for protocol ${protocolVersion}: ${problems.join('; ')}`);
  }
}

function checkAbi(wasm) {
  const missing = REQUIRED.filter(name => typeof wasm[name] !== 'function');
  if (missing.length) throw new Error(`BRiSK decoder changed; missing ${missing.join(', ')}`);
}

// A session that lost its connection after it started; its segment so far was closed with an end, as a recording
// of its own (see record()).
class Interrupted extends Error {
  constructor(cause) { super(cause.message); this.cause = cause; }
}

// Failures that another connection cannot mend: a session to sign in again, another BRiSK window, a changed site.
const FATAL = /expired or invalid|log in again|another WebSocket session|not a BRiSK host|layout not recognised|decoder changed|decoder not found|Codes not in the|Unknown BRiSK site|session cookies|profile/i;
const fatal = error => FATAL.test(error.message);

async function live({ cookies, codes = [], emit, fetchImpl = fetch, WebSocketImpl = WebSocket, startTimeoutMs = 60000,
  protocolVersion, profile: overrides = {}, trace = null, catchUp = null, heartbeatMs = HEARTBEAT_MS,
  staleSeconds = 90, connectTimeoutMs = 15000, catchUpBackoffMs, site: siteId = 'sbi', after = null,
  dayCheckMs = DAY_CHECK_MS, streamCheckMs = STREAM_CHECK_MS, sleep = ms => new Promise(resolve => setTimeout(resolve, ms)), onBoot = null,
  resumable = false, stop = null, log = null, workers = null, lanes = null, fastSymbols = null, reloadCookies = null, onCookies = null }) {
  const where = site(siteId);
  const profile = resolveProfile(overrides);
  const session = new Session(cookies, fetchImpl, where);
  if (stop) stop.end = null;  // no day to end yet (a failed setup may have left the last one's): a stop just exits (see main)
  // The cookies of a login made since the ones in use were given (reloadCookies reads them; onCookies tells whoever
  // starts the next day), taken up when the site no longer accepts the old ones: false when there are none.
  const newCookies = () => {
    const fresh = reloadCookies?.();
    if (!fresh || cookieHeader(fresh) === session.cookie) return false;
    session.use(fresh);
    onCookies?.(fresh);
    trace?.('boot: signed in again; using the new cookies');
    return true;
  };
  let frontend, app;
  const boot = async () => {
    frontend = await session.get('/api/frontend/boot');
    session.token = frontend.api_token;
    app = await session.get('/api/app/boot');
  };
  try {
    await boot();
  } catch (error) {
    if (!/expired/.test(error.message) || !newCookies()) throw error;
    await boot();
  }
  // Wait for a session to join. After a day's session the site serves that day until the next one. The vendor
  // app opens the stream only while session_status is "running"; after the close it is "done" (and the boot has
  // no ws_url), and the app reloads a minute after next_date, which the boot gives with its own time (unix
  // seconds), so the wait is taken from the server's clock. Python is told once, outside the days' records.
  // The day is said to have "ended", never to be "recorded": Python saves it minutes later, and says "saved" then.
  // The broker ends its sessions at 05:00 each morning, so a feed that waits overnight meets an expired one: it
  // keeps waiting, asking each minute for the cookies of a new login (`brisk login`, which the Python feed runs itself
  // when a passkey is saved: it is told with an `expired` line), rather than stop.
  let waiting = false, expired = false, again = false;
  for (;;) {
    const sameDay = after !== null && String(app.date) <= after;
    if (!sameDay && app.ws_url && (app.session_status ?? 'running') === 'running') break;
    const why = sameDay ? `${app.date} session ended` : `no session is running (${app.date}, status ${app.session_status ?? 'none'})`;
    trace?.(`boot: ${why}; waiting for the next trading day`);
    if (!waiting) {
      waiting = true;
      log?.(`[brisk] ${where.name}: ${why}; waiting for the next trading day`);
      await emit({ type: 'waiting', date: String(app.date), session_status: app.session_status ?? null });
    }
    const due = (Number(app.next_date) - Number(app.time) + 60) * 1000;
    if (!again) await sleep(expired ? streamCheckMs : due > 0 ? Math.min(due, dayCheckMs) : sameDay ? dayCheckMs : streamCheckMs);
    again = false;
    try {
      await boot();
      expired = false;
    } catch (error) {
      if (!/expired/.test(error.message)) {
        trace?.(`boot check failed (${error.message}); trying again later`);
      } else if (newCookies()) {
        again = true;   // asked for again at once with them
      } else if (!expired) {
        expired = true;
        log?.(`[brisk] ${where.name}: the session has expired; waiting for new cookies (sign in again with \`brisk login\`)`);
        await emit({ type: 'expired' });   // for the Python feed, which signs in again if a passkey is saved for this site
      }
    }
  }
  onBoot?.(String(app.date));
  const began = performance.now();
  // The server says which decoder protocol it speaks (the vendor client passes it to the WASM and
  // falls back to 16000); an explicit option wins, then the boot response, then the brokers' 18000.
  protocolVersion ??= app.flex_version ?? PROTOCOL_VERSION;
  const wsUrl = streamUrl(app.ws_url, frontend.api_endpoint, where.origin);
  trace?.(`boot: date ${app.date}, series ${app.series}, flex_version ${protocolVersion}, `
    + `${(app.base_prices || []).length} base prices, ${(app.exceptional_sq || []).length} exceptional special quotes, `
    + `ws_url ${redact(app.ws_url)} on ${wsUrl.host}`);
  const { assets, paths } = await decoderAssets(session);
  assets['master.dat'] = await session.get(`/api/master/${encodeURIComponent(app.master)}`, 'bytes');
  assets['snapshot.dat'] = await session.get(`/api/snapshot/${encodeURIComponent(app.snapshot)}`, 'bytes');

  // The WASM calls back into the host; these are the vendor client's six callbacks.
  let link = null, failure = null, marketFinished = false, settle, dayOver = false;
  const ended = new Promise(resolve => { settle = resolve; });
  const fail = error => {
    failure = failure || error;
    link?.close();
    settle({ code: -1, reason: '' });
  };
  let monitor = null, timer = null, dayTimer = null, pool = null, fastPool = null;  // started with the link and always stopped, so a failure cannot leave the process hanging
  const endDay = why => {
    if (dayOver) return;
    dayOver = true;
    trace?.(`day over: ${why}`);
    link?.close();
    settle({ code: 1000, reason: why });
  };
  // Asked to stop (see main), the session ends its day here, as the day's end would: a recording to keep and upload.
  if (stop) stop.end = endDay;
  const stale = staleSeconds ? staleDataCheck({ date: app.date, thresholdUs: staleSeconds * 1e6, name: where.name }) : null;
  // Its quotes are the recording's (without quote workers, and every session's bootstrap): each says what of its books changed.
  const decoder = await Decoder.create(assets, { protocolVersion, bookChanges: true, check: checkAbi,
    checkMaster: master => checkMaster(master, protocolVersion), schedule: app.schedule_info ?? null,
    boot: { exceptional_sq: app.exceptional_sq, base_prices: app.base_prices }, callbacks: {
    send: bytes => { if (link) link.send(bytes); },
    heartbeat: ns => { monitor?.beat(); stale?.beat(ns); },
    authError: () => fail(new Error(`${where.name} reports another WebSocket session for this user (only one is allowed); close the other one`)),
    marketFinished: () => { marketFinished = true; },
  } });
  trace?.(`master: ${decoder.master.length} issues, ${decoder.layout.size}-byte records, agreeing with the master file`
    + (decoder.boot.skipped_exceptional_sq ? `; special quotes not sent for ${decoder.boot.skipped_exceptional_sq.join(',')} (not in the master)` : '')
    + (decoder.scheduled ? `; schedule applied (${decoder.ohlc} price bars)` : '')
    + `; books read ${decoder.board ? 'from its ladder' : `through _getItaRows (${decoder.boardSkipped})`}`);
  const selected = new Set(codes);
  const master = decoder.master.filter(m => !selected.size || selected.has(m.code));
  const missing = codes.filter(code => !master.some(m => m.code === code));
  if (missing.length) throw new Error(`Codes not in the ${where.name} master: ${missing.join(',')}`);
  const ids = new Set(master.map(m => m.issue_id));
  const input_transport = { kind: where.transport, origin: where.origin + '/', ws_host: wsUrl.host, series: app.series,
    protocol_version: protocolVersion, boot_applied: decoder.boot,
    decoder: paths, decoder_sha256: crypto.createHash('sha256').update(assets['fita.wasm']).digest('hex'),
    setup_ms: performance.now() - began };

  const requestCatchUp = catchUp || (issues => fetchCatchUp(session, { app, issues, format: profile.catchUp ?? DEFAULT_CATCH_UP,
    backoffMs: catchUpBackoffMs, trace }));
  const wsSession = wsUrl.searchParams.get('session');
  // Only the Socket.IO fallback needs these; a plain WebSocket never asks for the market token.
  const context = async () => {
    let marketToken;
    try { marketToken = (await session.get('/api/app/market-token')).token; } catch (error) {
      throw new Error(`app/market-token failed: ${error.message}`);
    }
    return { marketToken, identity: frontend.identity, wsSession, tabId: crypto.randomUUID(), origin: where.origin };
  };

  let seq = 0, frames = 0, updates = 0, started = false, starting = false, work = Promise.resolve();
  const start = performance.now();
  // What a lost connection says of the host's side, when it was not the host that ended it (see `lost` below): how long
  // the link had been up, since when no frame had come, how late the host's event loop ran and how much memory it held.
  let connectedAt = null, lastFrameAt = null, lag = 0, lagTimer = null;
  const loop = monitorEventLoopDelay({ resolution: 20 });
  // The fast lane (with `lanes`, the Python feed's gateway): the symbols Live sessions subscribed to, read by a
  // pool of their own on each frame's arrival and written at once, out of band, ahead of the recording's batch
  // of that frame, which may wait behind the whole market's. `lanes.codes` says which; the feed changes it.
  let fast = new Set(), fastWork = Promise.resolve();
  const fastOf = () => new Set(lanes ? master.filter(m => lanes.codes.has(m.code)).map(m => m.issue_id) : []);
  const lineOf = (head, quotes, tail) =>
    `${JSON.stringify(head).slice(0, -1)},"quotes":[${quotes.join(',')}],${JSON.stringify(tail).slice(1)}`;
  // A symbol added while the day runs: its quote as the fast lane's decoders hold it now, after batch `seq - 1`.
  const join = () => {
    const next = fastOf();
    const added = [...next].filter(id => !fast.has(id));
    fast = next;
    if (!started || !fastPool || !added.length) return;
    const after = seq - 1, read = fastPool.join(added);
    read.catch(() => {});
    fastWork = fastWork.then(async () => {
      if (failure) return;
      const { quotes } = await read;
      const head = { lane: 'fast', type: 'join', seq: after }, tail = { codes: added.map(id => decoder.master[id].code) };
      await emit({ ...head, quotes, ...tail }, lineOf(head, quotes, tail));
    }).catch(fail);
  };

  const finishStart = caughtUp => {
    clearTimeout(timer);
    decoder.begin();
    pool?.begin(master.map(m => m.issue_id));
    fastPool?.begin(master.map(m => m.issue_id));
    const now = decoder.time();
    const { quotes, skipped } = decoder.quotes(master.map(m => m.issue_id));
    // The stock view layout is unverified for the broker build: refuse implausible values.
    if (!quotes.length || quotes.some(q => q.frame > q.max_frame || q.source_time_us > now)) {
      throw new Error(`${where.name} stock view layout not recognised (ohlcLength=${decoder.ohlc})`);
    }
    if (skipped.length) trace?.(`unreadable (skipped until they update): ${skipped.map(id => decoder.master[id].code).join(',')}`);
    started = true;
    const batch = { type: 'bootstrap', seq: seq++, source: SOURCE, trading_date: String(app.date).replaceAll('-', ''),
      input_transport: { ...input_transport, dialect: link.mode, engineio: link.version, caught_up_issues: caughtUp },
      source_timestamp_origin: 'brisk_decoder_unverified', exchange_delay_ms: null,
      source_time_us: now, market_issue_count: decoder.master.length, master, quotes, trades: decoder.trades(ids),
      ...(skipped.length && { skipped_issues: skipped.sort((a, b) => a - b) }) };
    // Trades the snapshot restored (the session's so far) follow the bootstrap, at its clock.
    const history = decoder.history(ids).map(trades => ({ type: 'history', seq: seq++, source_time_us: now, trades }));
    work = work.then(async () => { for (const b of [batch, ...history]) if (!failure) await emit(b); }).catch(fail);
    fast = fastOf();
    fastWork = work;   // the fast lane's lines come after the day's bootstrap and history
  };

  // The snapshot is normally older than the stream's first frame. The vendor client feeds
  // frames to the WASM at once and fetches the missing range meanwhile; so do we.
  const begin = () => {
    const lagging = decoder.laggingIssues();
    if (!lagging.length) return finishStart(0);
    trace?.(`catch-up needed for ${lagging.length} issues`);
    work = work.then(async () => {
      if (failure) return;
      const reply = await requestCatchUp(lagging);
      decoder.catchUp(reply, lagging);
      pool?.catchUp(reply, lagging);
      fastPool?.catchUp(reply, lagging);
      finishStart(lagging.length);
    }).catch(fail);
  };

  let lastTime = 0;
  const handle = async (frame, received_unix_ms) => {
    if (dayOver) return;
    const t0 = process.hrtime.bigint();
    decoder.feed(frame);
    decoder.applyBasePrice();
    const clock = decoder.time();
    if (clock + NEW_DAY_US < lastTime) return endDay(`the stream's clock went back to ${clock}`);
    lastTime = Math.max(lastTime, clock);
    const { quotes, skipped } = decoder.quotes(decoder.changed().filter(id => ids.has(id)));
    const trades = decoder.trades(ids);
    updates += quotes.length;
    await emit({ type: 'quotes', seq: seq++, source_time_us: decoder.time(), received_unix_ms,
      decode_ns: Number(process.hrtime.bigint() - t0), replay_lateness_ms: null, quotes, trades,
      ...(skipped.length && { skipped_issues: skipped.sort((a, b) => a - b) }) });
  };

  // With quote workers (see workers.cjs), each frame is decoded as it arrives while the workers read its quotes,
  // and the batches are written in order as their quotes come back, as JSON the workers made.
  let held = 0;
  const arrive = (frame, received_unix_ms) => {
    if (dayOver) return;
    const t0 = process.hrtime.bigint();
    decoder.feed(frame);
    decoder.applyBasePrice();
    const clock = decoder.time();
    if (clock + NEW_DAY_US < lastTime) return endDay(`the stream's clock went back to ${clock}`);
    lastTime = Math.max(lastTime, clock);
    const changed = decoder.changed().filter(id => ids.has(id));
    const trades = decoder.trades(ids);
    const decoded_ns = Number(process.hrtime.bigint() - t0);
    const batchSeq = seq++;
    if (fastPool && fast.size) {
      const tail = { trades: trades.filter(t => fast.has(t.issue_id)) };
      const read = fastPool.feed(frame, changed.filter(id => fast.has(id)), { only: true });
      read.catch(() => {});
      fastWork = fastWork.then(async () => {
        if (failure) return;
        const { quotes, skipped, decode_ns } = await read;
        if (skipped.length) tail.skipped_issues = skipped.sort((a, b) => a - b);
        const head = { lane: 'fast', type: 'quotes', seq: batchSeq, source_time_us: clock, received_unix_ms,
          decode_ns: decoded_ns + decode_ns, replay_lateness_ms: null };
        await emit({ ...head, quotes, ...tail }, lineOf(head, quotes, tail));
      }).catch(fail);
    } else {
      fastPool?.feed(frame);   // its decoders stay current while no Live session wants the fast lane
    }
    let bytes = 0;
    const read = pool.feed(frame, changed).then(result => {
      bytes = result.quotes.reduce((n, quote) => n + quote.length, 0);
      if ((held += bytes) > OUTPUT_BACKLOG_BYTES) {
        throw new Error(`${where.name}: the output is ${Math.round(held / 2 ** 20)} MB of quotes behind the stream; `
          + 'whatever reads it is not keeping up');
      }
      return result;
    });
    read.catch(() => {});   // a failure is the chain's to report
    work = work.then(async () => {
      if (failure) return;
      const { quotes, skipped, decode_ns } = await read;
      updates += quotes.length;
      const head = { type: 'quotes', seq: batchSeq, source_time_us: clock, received_unix_ms, decode_ns: decoded_ns + decode_ns,
        replay_lateness_ms: null };
      const tail = { trades, ...(skipped.length && { skipped_issues: skipped.sort((a, b) => a - b) }) };
      held -= bytes;
      await emit({ ...head, quotes, ...tail }, lineOf(head, quotes, tail));
    }).catch(fail);
  };

  // How the link looked when it closed, for the log: a connection the other end or the network drops looks one way
  // (frames until a moment before, the host's loop on time) and a host that was too busy to answer another.
  const describeDrop = () => {
    if (connectedAt === null) return '';
    const now = performance.now(), seconds = Math.round((now - connectedAt) / 1000);
    const up = seconds >= 3600 ? `${Math.floor(seconds / 3600)}h${String(Math.floor(seconds % 3600 / 60)).padStart(2, '0')}m`
      : seconds >= 60 ? `${Math.floor(seconds / 60)}m${String(seconds % 60).padStart(2, '0')}s` : `${seconds}s`;
    return ` after ${up} (last frame ${((now - lastFrameAt) / 1000).toFixed(1)} s earlier, event loop lag up to `
      + `${Math.round(Math.max(lag, loop.max / 1e6))} ms, ${process.versions.bun ? 'Bun' : 'Node'} ${(process.memoryUsage().rss / 2 ** 30).toFixed(1)} GB)`;
  };

  const onFrame = frame => {
    const received_unix_ms = Date.now();
    lastFrameAt = performance.now();
    frames++;
    // A frame arriving shows the link is alive. The WASM's heartbeat is seen only once its frame is decoded, and
    // in a burst (the opening auction) frames queue behind the output for longer than the heartbeat allows; the
    // vendor client, which decodes on arrival, would see it now. Falling behind is the stale check's to judge.
    monitor?.received();
    if (started && pool) {
      try { arrive(frame, received_unix_ms); } catch (error) { fail(error); }
      return;
    }
    if (started) {
      work = work.then(() => failure || handle(frame, received_unix_ms)).catch(fail);
      return;
    }
    try {
      decoder.feed(frame);
      decoder.applyBasePrice();
      pool?.feed(frame);
      fastPool?.feed(frame);
      if (!starting && decoder.initialFrames) { starting = true; begin(); }
    } catch (error) { fail(error); }
  };

  try {
    // Workers for a large selection (by default; `workers` sets how many, 0 none), started before the stream so
    // that they see every frame the host's decoder does.
    // With lanes, a quarter of them (at least one) make the fast lane's pool, the rest the recording's.
    const budget = workers ?? (ids.size >= POOL_MIN_ISSUES ? workerCount() : 0);
    // By default a quarter of the workers (at least one); for `fastSymbols` (the symbols Live sessions will want on it), as many
    // as read them (FAST_SYMBOLS_PER_WORKER each), the recording's taking the rest. A thousand symbols are two workers.
    const fastCount = lanes && budget > 0 ? Math.max(1, fastSymbols ? Math.ceil(fastSymbols / FAST_SYMBOLS_PER_WORKER) : Math.floor(budget / 4)) : 0;
    const count = budget > 0 ? Math.max(1, budget - fastCount) : 0;
    const options = { protocolVersion, schedule: app.schedule_info ?? null,
      boot: { exceptional_sq: app.exceptional_sq, base_prices: app.base_prices } };
    // The recording's workers say what of each quote's books changed; the fast lane's quotes go to Live, which needs none.
    const launched = await Promise.allSettled([count ? QuotePool.start({ assets, ids, count, options: { ...options, bookChanges: true } }) : null,
      fastCount ? QuotePool.start({ assets, ids, count: fastCount, options }) : null]);
    [pool, fastPool] = launched.map(r => (r.status === 'fulfilled' ? r.value : null));   // closed below, either way
    const unlaunched = launched.find(r => r.status === 'rejected');
    if (unlaunched) throw unlaunched.reason;
    if (count) trace?.(`quotes read by ${count} worker threads${fastCount ? `, the fast lane's by ${fastCount}` : ''}`);
    // Out of band, before the day's bootstrap: whether this session has a fast lane, for how many symbols, and
    // which it reads; the gateway admits Live sessions to it by that.
    if (lanes) {
      await emit({ type: 'lanes', fast: Boolean(fastPool), capacity: fastCount * FAST_SYMBOLS_PER_WORKER,
        codes: [...lanes.codes].sort() });
      lanes.onChange = join;
    }
    monitor = heartbeatMonitor({ intervalMs: heartbeatMs, checkEveryMs: Math.min(1000, heartbeatMs / 4), onFail: fail,
      staleCheck: stale && (() => stale.check(decoder.time())) });
    timer = setTimeout(() => fail(new Error(`${where.name} stream did not initialize within ${startTimeoutMs / 1000}s; `
      + 'no first frame numbers arrived (run with --trace-protocol to see what the server sent)')), startTimeoutMs);
    dayTimer = setInterval(async () => {
      try {
        const now = await session.get('/api/app/boot');
        if (String(now.date) !== String(app.date)) endDay(`the site serves ${now.date}`);
        else if (now.session_status === 'done') endDay('the session is done');  // as the vendor app stops reconnecting
      } catch (error) { trace?.(`day check failed: ${error.message}`); }  // the stream's own checks catch a dead session
    }, dayCheckMs);
    // Cookies go only to the host that set them (elsewhere the session in ws_url authenticates);
    // Origin is what a browser on the site sends.
    const headers = { origin: where.origin };
    if (wsUrl.origin === where.origin.replace(/^https:/, 'wss:')) headers.cookie = session.cookie;
    loop.enable();
    lagTimer = setInterval(() => { lag = loop.max / 1e6; loop.reset(); }, 30000);   // the last half minute's worst
    connectedAt = lastFrameAt = performance.now();
    link = new Link({ WebSocketImpl, url: wsUrl, headers, protocols: STREAM_PROTOCOL, name: where.name, profile, context, trace,
      connectTimeoutMs, onBinary: onFrame,
      onClose: closed => settle(closed), onError: error => { failure = failure || error; settle({ code: -1, reason: '' }); } });

    const closed = await ended;
    // A catch-up that finishes extends the chain (it queues the bootstrap), so wait until it stops growing.
    for (let pending = null, lane = null; pending !== work || lane !== fastWork;) {
      [pending, lane] = [work, fastWork];
      await Promise.all([pending, lane]);
    }
    const lost = failure || ((closed.code !== 1000 && !marketFinished && !dayOver) || !started
      ? new Error(`${where.name} stream closed (${[closed.code, closed.reason].filter(Boolean).join(' ')})${describeDrop()}`) : null);
    if (lost && (!resumable || !started || fatal(lost))) throw lost;
    // A lost connection ends the segment as cleanly as the day's end would: what came before it is a recording.
    const summary = { type: 'end', seq, source_time_us: decoder.time(), frames, quote_updates: updates,
      replay_wall_ms: performance.now() - start };
    await emit(summary);
    if (lost) throw new Interrupted(lost);
    return summary;
  } finally {
    clearTimeout(timer);
    clearInterval(dayTimer);
    clearInterval(lagTimer);
    loop.disable();
    monitor?.stop();
    link?.close();
    await Promise.all([pool?.close(), fastPool?.close()]);   // gone before another session's workers start
    if (lanes) lanes.onChange = null;
    if (stop) stop.end = null;
  }
}

const RETRY_MS = [5000, 10000, 30000, 60000, 120000];

// A session's decoder and buffers are garbage once live() has returned, but the engine collects them when it chooses; a session
// started seconds later (a reconnect) would then have the old one's memory beside its own. So the host collects between
// sessions. Bun has its own Bun.gc; under Node gc() is reached without a command-line flag; a runtime that refuses
// keeps its engine's own schedule.
let collect = null;
function collectGarbage() {
  try {
    if (globalThis.Bun) { Bun.gc(true); return; }
    collect ??= (require('node:v8').setFlagsFromString('--expose-gc'), require('node:vm').runInNewContext('gc'));
    collect();
  } catch { /* V8's own schedule */ }
}

// One session per trading day, back to back, until `days` have been recorded (the command line: until
// stopped). Each emits its own bootstrap ... end, which the Python feed keeps as that day's recording. A day
// ends when the site's session is done, and a start with no session running (after the close, on a holiday)
// waits for the next trading day's (see live). A connection lost once the recording has begun ends that
// segment cleanly (Interrupted), and a new connection starts the next from a fresh snapshot, so only the gap is missing; failures before anything was recorded,
// and those another connection cannot mend (fatal), stop it. `stop.requested` (see main) ends it after the
// session's day, ended where it was.
async function record({ days = Infinity, retryMs = RETRY_MS, sleep = ms => new Promise(resolve => setTimeout(resolve, ms)),
  log = line => process.stderr.write(`${line}\n`), stop = null, cookies, ...options }) {
  let after = null, recorded = false, failures = 0;
  for (let day = 0; day < days;) {
    let booted = null;
    try {
      // The cookies of a new login, taken up while a day waited, carry to the days after it.
      await live({ ...options, cookies, sleep, log, after, stop, resumable: true, onBoot: date => { booted = date; },
        onCookies: fresh => { cookies = fresh; } });
      after = booted; day++; recorded = true; failures = 0;
      if (stop?.requested) return;
      collectGarbage();
      continue;
    } catch (error) {
      if (stop?.requested) return;   // stopped before its stream had started: nothing to end
      if (error instanceof Interrupted) { recorded = true; failures = 0; }
      else if (!recorded || fatal(error)) throw error;
      const wait = retryMs[Math.min(failures++, retryMs.length - 1)];
      log(`[brisk] ${error.message}; reconnecting in ${wait / 1000}s (the recording so far is kept)`);
      collectGarbage();
      await sleep(wait);
    }
  }
}

// BRISK_CONTROL=1: the Python parent asks to stop with {"stop": true}, or EOF when it exits.
// BRISK_LANES=1 (the Python feed, serving briskapi.Live): its gateway also says on stdin, one JSON line at a time,
// which symbols Live sessions want on the fast lane ({"fast": [codes]}, all of them each time). live() reads
// `codes` and is told of a change by `onChange`.
function readLanes(input, onStop = null) {
  const lanes = { codes: new Set(), onChange: null };
  require('node:readline').createInterface({ input }).on('line', line => {
    let message;
    try { message = JSON.parse(line); } catch { return; }
    if (message?.stop === true) { onStop?.(); return; }
    if (!Array.isArray(message?.fast)) return;
    lanes.codes = new Set(message.fast.map(String));
    lanes.onChange?.();
  });
  input.on('end', () => onStop?.());  // the Python parent closed or died
  input.unref?.();   // the feed's stdin never keeps the host running
  return lanes;
}

const USAGE = 'Usage: [BRISK_SITE=sbi|e-shiten|matsui|monex|smbcnikko] BRISK_<SITE>_COOKIES=... brisk.cjs [--codes 7203,6758] [--trace-protocol]';

function parseArgs(argv) {
  const options = { codes: [], trace: false };
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === '--codes' && argv[i + 1] !== undefined && !argv[i + 1].startsWith('--') && !options.codesSet) {
      options.codes = argv[++i].split(','); options.codesSet = true;
    } else if (argv[i] === '--trace-protocol' && !options.trace) {
      options.trace = true;
    } else throw new Error(USAGE);
  }
  return options;
}

async function main(argv, env = process.env) {
  const { codes, trace } = parseArgs(argv);
  let profile = {};
  if (env.BRISK_PROFILE) {
    try { profile = JSON.parse(env.BRISK_PROFILE); } catch { throw new Error('BRISK_PROFILE is not valid JSON'); }
  }
  const where = site(env.BRISK_SITE || 'sbi');
  // Ctrl-C, `kill` or a shutdown (and the Python feed's close()) end the running day where it is, with its end,
  // so it is kept and uploaded like any day; with no day running there is nothing to end. Ctrl-C reaches this
  // process and the Python feed's, whose close() then signals too: a repeated signal changes nothing (close()
  // kills the host if it has not ended in time).
  const stop = { requested: false, end: null };
  const onSignal = () => {
    if (stop.requested) return;
    stop.requested = true;
    if (stop.end) stop.end('stopped'); else process.exit(0);
  };
  process.on('SIGINT', onSignal);
  process.on('SIGTERM', onSignal);
  const control = env.BRISK_CONTROL === '1' || env.BRISK_LANES === '1' ? readLanes(process.stdin, onSignal) : null;
  const lanes = env.BRISK_LANES === '1' ? control : null;
  const fastSymbols = env.BRISK_FAST_SYMBOLS ? Number(env.BRISK_FAST_SYMBOLS) : null;
  if (fastSymbols !== null && !(Number.isInteger(fastSymbols) && fastSymbols > 0)) throw new Error('BRISK_FAST_SYMBOLS must be a whole number above 0');
  await record({ cookies: JSON.parse(env[where.cookies] || 'null'), codes, profile, site: where.id, stop, lanes, fastSymbols,
    reloadCookies: env.BRISK_COOKIES_FILE ? () => cookiesIn(env.BRISK_COOKIES_FILE) : null,
    trace: trace ? line => process.stderr.write(`[brisk protocol] ${line}\n`) : null,
    // A batch whose quotes workers read comes with its line already made.
    emit: async (record, line = JSON.stringify(record)) => {
      if (!process.stdout.write(line + '\n')) await once(process.stdout, 'drain');
    } });
}

module.exports = { Session, SITES, SOURCE, site, cookiesIn, decoderAssets, streamUrl, checkAbi, checkMaster, live, record, main, readLanes, fetchCatchUp, CATCH_UP_FORMATS, DEFAULT_CATCH_UP,
  Interrupted, fatal, RETRY_MS,
  HEARTBEAT_MS,
  PROTOCOL_VERSION, REQUIRED };
if (require.main === module) main(process.argv.slice(2)).catch(error => {
  console.error(error.message); process.exitCode = 1;
});
