'use strict';
// Experimental live host for SBI BRiSK (sbi.brisk.jp): the session's own WASM
// decoder under Node, fed from the authenticated WebSocket. No browser or Chrome
// DevTools. Emits the same JSON batches as decoder.cjs (source=sbi_live).
// Cookies come from BRISK_SBI_COOKIES (JSON object), never from the command line.
//
// It follows the vendor client's connect sequence (public Next demo bundle) and
// upstream's SBI research (pybrisk): feed the WASM from the first frame, wait for its
// frame numbers, catch the snapshot up to them, forward the WASM's own pings, and watch
// the server heartbeat. Three wire details are not public (the Socket.IO connect
// parameters, the `startLive` payload and the catch-up request body); they live in the
// profile (BRISK_SBI_PROFILE) and `--trace-protocol` shows what the server answers.
const crypto = require('node:crypto');
const { once } = require('node:events');
const { Decoder } = require('./decoder.cjs');
const { Link, resolveProfile, heartbeatMonitor, redact } = require('./engineio.cjs');

const ORIGIN = 'https://sbi.brisk.jp';
const PROTOCOL_VERSION = 18000;
const MAX_BYTES = 64 * 1024 * 1024;
// SBI's decoder must provide everything the host calls (the demo ABI minus portfolio).
const REQUIRED = ['_initialize', '_push', '_applyBasePriceQueue', '_stockCount', '_getStockMaster', '_unserialize',
  '_getDate', '_getTime', '_pushWs', '_getFrameNumbers', '_apiRecieved', '_addTraceUpdate', '_getTrace',
  '_clearTrace', '_getStockView', '_fitItaViewRowPrice10', '_getItaRows', '_malloc', 'addFunction'];

class Session {
  constructor(cookies, fetchImpl = fetch) {
    if (!cookies || typeof cookies !== 'object' || !Object.keys(cookies).length) {
      throw new Error('Set BRISK_SBI_COOKIES to your SBI BRiSK session cookies (JSON object)');
    }
    this.cookie = Object.entries(cookies).map(([k, v]) => `${k}=${v}`).join('; ');
    this.fetch = fetchImpl;
    this.token = null;
  }

  get(path, kind = 'json') {
    return this.request(path, { kind });
  }

  async request(path, { method = 'GET', body, contentType, kind = 'json' } = {}) {
    const headers = { cookie: this.cookie };
    if (this.token) headers.authorization = `Bearer ${this.token}`;
    if (contentType) headers['content-type'] = contentType;
    // Redirects mean a login page; following them would forward credentials elsewhere.
    const response = await this.fetch(new URL(path, ORIGIN), { method, headers, body, redirect: 'manual', signal: AbortSignal.timeout(30000) });
    if ([301, 302, 303, 307, 308, 401, 403].includes(response.status) || response.type === 'opaqueredirect') {
      throw new Error('SBI BRiSK session expired or invalid; log in again');
    }
    if (!response.ok) {
      const error = new Error(`SBI BRiSK ${path}: HTTP ${response.status}`);
      error.status = response.status;
      error.reason = response.headers?.get?.('x-error-reason') ?? null;
      throw error;
    }
    const data = Buffer.from(await response.arrayBuffer());
    if (data.length > MAX_BYTES) throw new Error(`SBI BRiSK ${path}: response too large`);
    return kind === 'json' ? JSON.parse(data) : kind === 'text' ? data.toString('utf8') : data;
  }
}

// How the catch-up request is written. UNVERIFIED: the vendor client holds
// {issueCodeIdx, from, to} for every issue whose snapshot is behind the stream, but the
// body it sends is not public, so no format is used unless the profile names one.
const CATCH_UP_FORMATS = {
  'json-vendor': issues => ({ contentType: 'application/json',
    body: JSON.stringify(issues.map(i => ({ issueCodeIdx: i.issue_id, from: i.from, to: i.to }))) }),
};

// Fetch the frames between the snapshot and the stream's first frame. Mirrors the vendor
// client: up to five retries, but never for an expired session or a snapshot that is too old.
async function fetchCatchUp(session, { app, issues, format, retries = 5, backoffMs = n => 1000 + 1000 * n, trace }) {
  const build = CATCH_UP_FORMATS[format];
  if (!format || !build) {
    throw new Error(`The snapshot is ${issues.length} issues behind the stream, so it must be caught up with `
      + 'POST /api/stocks_update, but the request body is not public'
      + (format ? ` and "${format}" is not a known format (known: ${Object.keys(CATCH_UP_FORMATS).join(', ')})`
        : '. Name a format in the profile (catchUp), for example {"catchUp":"json-vendor"}, and run with --trace-protocol'));
  }
  const { contentType, body } = build(issues);
  const path = `/api/stocks_update/${encodeURIComponent(app.series)}?date=${encodeURIComponent(app.date)}`;
  for (let attempt = 0; ; attempt++) {
    try {
      trace?.(`catch-up: POST ${path} (${issues.length} issues, ${body.length} bytes, format ${format})`);
      return await session.request(path, { method: 'POST', body, contentType, kind: 'bytes' });
    } catch (error) {
      if (error.reason === 'too-old') {
        throw new Error('SBI BRiSK says the snapshot is too old to catch up (x-error-reason: too-old); restart to load a fresh one');
      }
      if (/session expired/.test(error.message) || attempt >= retries) {
        throw new Error(`Catch-up failed: ${error.message}${error.status ? ` (the server rejected format "${format}")` : ''}`);
      }
      trace?.(`catch-up attempt ${attempt + 1} failed: ${error.message}`);
      await new Promise(resolve => setTimeout(resolve, backoffMs(attempt)));
    }
  }
}

// The decoder is served to logged-in sessions only and changes with SBI releases,
// so it is located through the app's own bundles rather than pinned.
async function decoderAssets(session) {
  const page = await session.get('/', 'text');
  const scripts = [...page.matchAll(/<script[^>]+src="(\/[^"]+\.js)"/g)].map(m => m[1]);
  for (const script of scripts) {
    const source = await session.get(script, 'text');
    const js = source.match(/["'`](\/?assets\/wasm\/fita[\w.-]*\.js)["'`]/);
    const wasm = source.match(/["'`](\/?assets\/wasm\/fita[\w.-]*\.wasm)["'`]/);
    if (js && wasm) {
      const assets = { 'fita.js': await session.get('/' + js[1].replace(/^\//, ''), 'bytes'),
        'fita.wasm': await session.get('/' + wasm[1].replace(/^\//, ''), 'bytes') };
      return { assets, paths: [js[1], wasm[1]] };
    }
  }
  throw new Error('SBI BRiSK decoder not found in the app bundles; the site layout may have changed');
}

function checkAbi(wasm) {
  const missing = REQUIRED.filter(name => typeof wasm[name] !== 'function');
  if (missing.length) throw new Error(`SBI BRiSK decoder changed; missing ${missing.join(', ')}`);
}

async function live({ cookies, codes = [], emit, fetchImpl = fetch, WebSocketImpl = WebSocket, startTimeoutMs = 60000,
  protocolVersion = PROTOCOL_VERSION, profile: overrides = {}, trace = null, catchUp = null, heartbeatMs = 7000,
  connectTimeoutMs = 15000, catchUpBackoffMs }) {
  const profile = resolveProfile(overrides);
  const session = new Session(cookies, fetchImpl);
  const began = performance.now();
  const frontend = await session.get('/api/frontend/boot');
  session.token = frontend.api_token;
  const app = await session.get('/api/app/boot');
  trace?.(`boot: date ${app.date}, series ${app.series}, ws_url ${redact(app.ws_url)}`);
  const { assets, paths } = await decoderAssets(session);
  assets['master.dat'] = await session.get(`/api/master/${encodeURIComponent(app.master)}`, 'bytes');
  assets['snapshot.dat'] = await session.get(`/api/snapshot/${encodeURIComponent(app.snapshot)}`, 'bytes');

  // The WASM calls back into the host; these are the vendor client's six callbacks.
  let link = null, failure = null, marketFinished = false, settle;
  const ended = new Promise(resolve => { settle = resolve; });
  const fail = error => {
    failure = failure || error;
    link?.close();
    settle({ code: -1, reason: '' });
  };
  const monitor = heartbeatMonitor({ intervalMs: heartbeatMs, checkEveryMs: Math.min(1000, heartbeatMs / 4), onFail: fail });
  const decoder = await Decoder.create(assets, { protocolVersion, check: checkAbi, callbacks: {
    send: bytes => { if (link) link.send(bytes); },
    heartbeat: () => monitor.beat(),
    authError: () => fail(new Error('SBI BRiSK reports another WebSocket session for this user (only one is allowed); close the other one')),
    marketFinished: () => { marketFinished = true; },
  } });
  const selected = new Set(codes);
  const master = decoder.master.filter(m => !selected.size || selected.has(m.code));
  const missing = codes.filter(code => !master.some(m => m.code === code));
  if (missing.length) throw new Error(`Codes not in the SBI master: ${missing.join(',')}`);
  const ids = new Set(master.map(m => m.issue_id));
  const input_transport = { kind: 'sbi_websocket', origin: ORIGIN + '/', series: app.series,
    decoder: paths, decoder_sha256: crypto.createHash('sha256').update(assets['fita.wasm']).digest('hex'),
    setup_ms: performance.now() - began };

  const requestCatchUp = catchUp || (issues => fetchCatchUp(session, { app, issues, format: profile.catchUp,
    backoffMs: catchUpBackoffMs, trace }));
  const wsSession = new URL(app.ws_url, ORIGIN).searchParams.get('session');
  // Only a Socket.IO server needs these; a plain WebSocket never asks for the market token.
  const context = async () => {
    let marketToken;
    try { marketToken = (await session.get('/api/app/market-token')).token; } catch (error) {
      throw new Error(`app/market-token failed: ${error.message}`);
    }
    return { marketToken, identity: frontend.identity, wsSession, tabId: crypto.randomUUID(), origin: ORIGIN };
  };

  let seq = 0, frames = 0, updates = 0, started = false, starting = false, work = Promise.resolve();
  const start = performance.now();
  const timer = setTimeout(() => fail(new Error(`SBI BRiSK stream did not initialize within ${startTimeoutMs / 1000}s; `
    + 'no first frame numbers arrived (run with --trace-protocol to see what the server sent)')), startTimeoutMs);

  const finishStart = caughtUp => {
    clearTimeout(timer);
    decoder.begin();
    const now = decoder.time();
    const quotes = master.map(m => decoder.quote(m.issue_id));
    // The stock view layout is unverified for SBI's build: refuse implausible values.
    if (quotes.some(q => q.frame > q.max_frame || q.source_time_us > now)) {
      throw new Error(`SBI BRiSK stock view layout not recognised (ohlcLength=${decoder.ohlc})`);
    }
    started = true;
    const batch = { type: 'bootstrap', seq: seq++, source: 'sbi_live', trading_date: String(app.date).replaceAll('-', ''),
      input_transport: { ...input_transport, dialect: link.mode, engineio: link.version, caught_up_issues: caughtUp },
      source_timestamp_origin: 'brisk_decoder_unverified', exchange_delay_ms: null,
      source_time_us: now, market_issue_count: decoder.master.length, master, quotes };
    work = work.then(() => failure || emit(batch)).catch(fail);
  };

  // The snapshot is normally older than the stream's first frame. The vendor client feeds
  // frames to the WASM at once and fetches the missing range meanwhile; so do we.
  const begin = () => {
    const lagging = decoder.laggingIssues();
    if (!lagging.length) return finishStart(0);
    trace?.(`catch-up needed for ${lagging.length} issues`);
    work = work.then(async () => {
      if (failure) return;
      decoder.push(await requestCatchUp(lagging));
      const left = decoder.laggingIssues();
      if (left.length) {
        throw new Error(`Catch-up left ${left.length} issues behind the stream (first: issue ${left[0].issue_id}, `
          + `frame ${left[0].from}, stream starts at ${left[0].to})`);
      }
      finishStart(lagging.length);
    }).catch(fail);
  };

  const handle = async (frame, received_unix_ms) => {
    const t0 = process.hrtime.bigint();
    decoder.feed(frame);
    const quotes = decoder.changed().filter(id => ids.has(id)).map(id => decoder.quote(id));
    updates += quotes.length;
    await emit({ type: 'quotes', seq: seq++, source_time_us: decoder.time(), received_unix_ms,
      decode_ns: Number(process.hrtime.bigint() - t0), replay_lateness_ms: null, quotes });
  };

  const onFrame = frame => {
    const received_unix_ms = Date.now();
    frames++;
    if (started) {
      work = work.then(() => failure || handle(frame, received_unix_ms)).catch(fail);
      return;
    }
    try {
      decoder.feed(frame);
      if (!starting && decoder.initialFrames) { starting = true; begin(); }
    } catch (error) { fail(error); }
  };

  link = new Link({ WebSocketImpl, url: new URL(app.ws_url, ORIGIN.replace('https:', 'wss:')),
    headers: { cookie: session.cookie }, profile, context, trace, connectTimeoutMs, onBinary: onFrame,
    onClose: closed => settle(closed), onError: error => { failure = failure || error; settle({ code: -1, reason: '' }); } });

  const closed = await ended;
  clearTimeout(timer);
  monitor.stop();
  // A catch-up that finishes extends the chain (it queues the bootstrap), so wait until it stops growing.
  for (let pending = null; pending !== work;) { pending = work; await pending; }
  if (failure) throw failure;
  if ((closed.code !== 1000 && !marketFinished) || !started) {
    throw new Error(`SBI BRiSK stream closed (${closed.code} ${closed.reason || ''})`.trim());
  }
  const summary = { type: 'end', seq, source_time_us: decoder.time(), frames, quote_updates: updates,
    replay_wall_ms: performance.now() - start };
  await emit(summary);
  return summary;
}

const USAGE = 'Usage: BRISK_SBI_COOKIES=... sbi.cjs [--codes 7203,6758] [--trace-protocol]';

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
  if (env.BRISK_SBI_PROFILE) {
    try { profile = JSON.parse(env.BRISK_SBI_PROFILE); } catch { throw new Error('BRISK_SBI_PROFILE is not valid JSON'); }
  }
  await live({ cookies: JSON.parse(env.BRISK_SBI_COOKIES || 'null'), codes, profile,
    trace: trace ? line => process.stderr.write(`[sbi protocol] ${line}\n`) : null,
    emit: async record => {
      if (!process.stdout.write(JSON.stringify(record) + '\n')) await once(process.stdout, 'drain');
    } });
}

module.exports = { Session, decoderAssets, checkAbi, live, main, fetchCatchUp, CATCH_UP_FORMATS, PROTOCOL_VERSION, REQUIRED };
if (require.main === module) main(process.argv.slice(2)).catch(error => {
  console.error(error.message); process.exitCode = 1;
});
