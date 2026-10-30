'use strict';
// The BRiSK live host against a fake sbi.brisk.jp (and e-shiten) and WebSocket, driven by the
// demo's real decoder and frames (no broker account or network needed).
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { Session, site, cookiesIn, decoderAssets, streamUrl, checkAbi, checkMaster, live, record, main, fetchCatchUp, Interrupted, fatal,
  RETRY_MS, readLanes } = require('../../briskapi/decoder/brisk.cjs');
const { frames, Decoder } = require('../../briskapi/decoder/decoder.cjs');
const { QuotePool, workerCount, merge } = require('../../briskapi/decoder/workers.cjs');
const cache = process.env.BRISK_MOCK_CACHE;

const IDENTITY = 'a'.repeat(64);
const MARKET_TOKEN = 'v2.local.MARKETTOKENVALUE';
const WS_SESSION = 'b'.repeat(48);

test('the parent control pipe handles lane changes, stop requests and EOF on every OS', async () => {
  const { PassThrough } = require('node:stream');
  const { once } = require('node:events');
  const input = new PassThrough();
  let stops = 0, changes = 0;
  const lanes = readLanes(input, () => stops++);
  lanes.onChange = () => changes++;
  input.write('not JSON\nnull\n{}\n{"fast":[7203,"6758"]}\n');
  assert.deepEqual([...lanes.codes], ['7203', '6758']);
  assert.equal(changes, 1);
  input.write('{"stop":true}\n');
  assert.equal(stops, 1);
  assert.equal(changes, 1, 'stopping does not change the fast lane');
  const ended = once(input, 'end');
  input.end();
  await ended;
  assert.equal(stops, 2, 'losing the parent also asks the host to stop');
});

function server(files, overrides = {}) {
  const requests = [];
  const routes = {
    '/api/frontend/boot': () => JSON.stringify({ api_token: 'token', identity: IDENTITY }),
    '/api/app/market-token': () => JSON.stringify({ token: MARKET_TOKEN }),
    '/api/app/boot': () => JSON.stringify({ date: '2021-09-27', series: 0, master: 'm1', snapshot: 's1',
      ws_url: '/realtime/0?session=abc' }),
    '/': () => '<html><script src="/main.abc.js"></script></html>',
    '/main.abc.js': () => 'load("assets/wasm/fita.test.js");load("assets/wasm/fita.test.wasm")',
    '/assets/wasm/fita.test.js': () => files['fita.js'],
    '/assets/wasm/fita.test.wasm': () => files['fita.wasm'],
    '/api/master/m1': () => files['master.dat'],
    '/api/snapshot/s1': () => files['snapshot.dat'],
    ...overrides,
  };
  const fetchImpl = async (url, init) => {
    requests.push({ path: url.pathname, search: url.search, method: init.method || 'GET', headers: init.headers,
      redirect: init.redirect, body: init.body });
    const route = routes[url.pathname];
    if (!route) return new Response('missing', { status: 404 });
    const body = route(init);
    return body instanceof Response ? body : new Response(body, { status: 200 });
  };
  return { fetchImpl, requests };
}

function socketFrom(chunks, code = 1000) {
  return class extends EventTarget {
    constructor(url, init) {
      super();
      this.url = url; this.init = init;
      setImmediate(() => {
        this.dispatchEvent(new MessageEvent('message', { data: 'hello' }));
        for (const chunk of chunks) {
          const data = chunk.buffer.slice(chunk.byteOffset, chunk.byteOffset + chunk.length);
          this.dispatchEvent(new MessageEvent('message', { data }));
        }
        setImmediate(() => this.close(code));
      });
      socketFrom.last = this;
    }
    close(closeCode = 1000) {
      if (this.closed) return;
      this.closed = true;
      this.dispatchEvent(Object.assign(new Event('close'), { code: closeCode, reason: '' }));
    }
  };
}

function demo() {
  const files = Object.fromEntries(['fita.js', 'fita.wasm', 'master.dat', 'snapshot.dat', 'ws.dat']
    .map(name => [name, fs.readFileSync(path.join(cache, name))]));
  const chunks = [...frames(files['ws.dat'])].slice(0, 6).map(f => f.data);
  return { files, chunks };
}

test('session sends cookies, refuses redirects and reports expiry', async () => {
  assert.throws(() => new Session(null), /BRISK_SBI_COOKIES/);
  const { fetchImpl, requests } = server({}, { '/api/frontend/boot': () => new Response('', { status: 401 }) });
  const session = new Session({ session_x: 'v1', other: 'v2' }, fetchImpl);
  await assert.rejects(session.get('/api/frontend/boot'), /expired/);
  assert.equal(requests[0].headers.cookie, 'session_x=v1; other=v2');
  assert.equal(requests[0].redirect, 'manual');
  await assert.rejects(session.get('/nope'), /HTTP 404/);
  assert.throws(() => checkAbi({ _initialize() {} }), /missing _push/);
});

test('the BRiSK host bootstraps from boot, master and snapshot, then streams frames', { skip: !cache }, async () => {
  const { files, chunks } = demo();
  const { fetchImpl, requests } = server(files);
  const batches = [];
  const summary = await live({ cookies: { session_x: 'v' }, codes: ['7203'], emit: async b => batches.push(b),
    fetchImpl, WebSocketImpl: socketFrom(chunks), protocolVersion: 16000 });
  assert.equal(String(socketFrom.last.url), 'wss://sbi.brisk.jp/realtime/0?session=abc');
  assert.deepEqual(socketFrom.last.init.headers, { origin: 'https://sbi.brisk.jp', cookie: 'session_x=v' });
  assert.deepEqual(socketFrom.last.init.protocols, ['flex'], 'the stream asks for the vendor client\'s subprotocol');
  assert.ok(requests.filter(r => r.path.startsWith('/api/app')).every(r => r.headers.authorization === 'Bearer token'));
  const [bootstrap, ...rest] = batches;
  assert.equal(bootstrap.source, 'brisk_live');
  assert.equal(bootstrap.trading_date, '20210927');
  assert.deepEqual(bootstrap.master.map(m => m.code), ['7203']);
  assert.equal(bootstrap.quotes[0].indicative_price10, 101500);
  assert.equal(bootstrap.input_transport.kind, 'sbi_websocket');
  assert.equal(bootstrap.input_transport.ws_host, 'sbi.brisk.jp');
  assert.deepEqual(bootstrap.input_transport.decoder, ['/assets/wasm/fita.test.js', '/assets/wasm/fita.test.wasm']);
  assert.equal(bootstrap.input_transport.dialect, 'raw');
  assert.equal(bootstrap.input_transport.caught_up_issues, 0);
  assert.ok(!requests.some(r => r.path === '/api/app/market-token'), 'a plain server never asks for the market token');
  assert.match(bootstrap.input_transport.decoder_sha256, /^[0-9a-f]{64}$/);
  assert.deepEqual(rest.map(b => b.seq), [1, 2, 3, 4, 5, 6]);
  assert.equal(rest.at(-1).type, 'end');
  assert.equal(summary.frames, 6);
});

// A socket that delivers the chunks and stays open until the host closes it.
function heldSocket(chunks) {
  return class extends EventTarget {
    constructor(url, init) {
      super();
      setImmediate(() => {
        for (const chunk of chunks) {
          this.dispatchEvent(new MessageEvent('message', { data: chunk.buffer.slice(chunk.byteOffset, chunk.byteOffset + chunk.length) }));
        }
      });
    }
    close() {
      if (this.closed) return;
      this.closed = true;
      this.dispatchEvent(Object.assign(new Event('close'), { code: 1005, reason: '' }));
    }
  };
}

test('the host records one day after another, each its own bootstrap ... end, rolling when the site serves a new date',
  { skip: !cache }, async () => {
  const { files, chunks } = demo();
  let served = '2021-09-27';
  const { fetchImpl } = server(files, { '/api/app/boot': () => JSON.stringify({ date: served, series: 0, master: 'm1',
    snapshot: 's1', ws_url: '/realtime/0?session=abc' }) });
  const batches = [], waits = [];
  // Day one ends when the server closes the stream; day two stays open until the day check sees a new date.
  let sockets = 0;
  const WebSocketImpl = class { constructor(...a) { return new (sockets++ ? heldSocket(chunks) : socketFrom(chunks))(...a); } };
  const emit = async b => {
    batches.push(b);
    if (b.type === 'bootstrap' && b.trading_date === '20210928') served = '2021-09-29';
  };
  await record({ days: 2, cookies: { s: 'v' }, codes: ['7203'], emit, fetchImpl, WebSocketImpl, protocolVersion: 16000,
    dayCheckMs: 20, sleep: async ms => { waits.push(ms); served = '2021-09-28'; } });
  assert.deepEqual(batches.filter(b => b.type === 'bootstrap').map(b => b.trading_date), ['20210927', '20210928']);
  assert.deepEqual(waits, [20], 'day two waited for the site to serve the next date');
  // Each day runs bootstrap ... end, numbered from its own bootstrap; between them, the wait is said once.
  const second = batches.findIndex((b, i) => i && b.type === 'bootstrap');
  assert.equal(batches[second].seq, 0);
  assert.deepEqual(batches[second - 1], { type: 'waiting', date: '2021-09-27', session_status: null });
  assert.equal(batches[second - 2].type, 'end');
  assert.equal(batches.at(-1).type, 'end');
});

test('started with no session running (after the close), the host says so once and waits for the next day\'s',
  { skip: !cache }, async () => {
  const { files, chunks } = demo();
  const boots = [
    { date: '2021-09-24', series: 0, session_status: 'done' },
    { date: '2021-09-24', series: 0, session_status: 'done', time: 1000, next_date: 1120 },
    { date: '2021-09-27', series: 0, master: 'm1', snapshot: 's1', ws_url: '/realtime/0?session=abc', session_status: 'before' },
    { date: '2021-09-27', series: 0, master: 'm1', snapshot: 's1', ws_url: '/realtime/0?session=abc', session_status: 'running' },
  ];
  let boot = 0, sockets = 0;
  const { fetchImpl } = server(files, { '/api/app/boot': () => JSON.stringify(boots[Math.min(boot++, boots.length - 1)]) });
  const batches = [], waits = [], logged = [];
  await record({ days: 1, cookies: { s: 'v' }, codes: ['7203'], emit: async b => batches.push(b), fetchImpl,
    WebSocketImpl: class { constructor(...a) { sockets++; return new (socketFrom(chunks))(...a); } }, protocolVersion: 16000,
    streamCheckMs: 60e3, dayCheckMs: 600e3, sleep: async ms => { waits.push(ms); }, log: line => logged.push(line) });
  assert.deepEqual(batches[0], { type: 'waiting', date: '2021-09-24', session_status: 'done' });
  assert.equal(batches.filter(b => b.type === 'waiting').length, 1);
  assert.equal(batches[1].type, 'bootstrap');
  assert.equal(batches[1].seq, 0);
  assert.equal(batches[1].trading_date, '20210927');
  assert.equal(batches.at(-1).type, 'end');
  assert.equal(sockets, 1, 'no connection is tried without a stream');
  assert.equal(socketFrom.last.url.pathname, '/realtime/0');
  // Each minute with no next session announced; then until a minute after it is due, by the boot's own clock;
  // a stream that is not running is not joined.
  assert.deepEqual(waits, [60e3, 180e3, 60e3]);
  assert.deepEqual(logged, ['[brisk] SBI BRiSK: no session is running (2021-09-24, status done); '
    + 'waiting for the next trading day']);
});

test('a session the site ends while the host waits for the next day is waited out, and a new login\'s cookies take its place',
  async () => {
  let ended = false, saved = null;
  const { fetchImpl, requests } = server({}, {
    '/api/frontend/boot': init => ended && init.headers.cookie === 's=old' ? new Response('', { status: 401 })
      : JSON.stringify({ api_token: 'token', identity: IDENTITY }),
    '/api/app/boot': () => JSON.stringify({ date: '2021-09-24', series: 0, session_status: 'done', time: 1000, next_date: 90000 }),
  });
  const batches = [], logged = [], waits = [], tried = [];
  const steps = [() => { ended = true; }, () => {}, () => { saved = { s: 'new' }; }];
  await assert.rejects(live({ cookies: { s: 'old' }, fetchImpl, emit: async b => batches.push(b), log: line => logged.push(line),
    streamCheckMs: 60e3, dayCheckMs: 600e3, reloadCookies: () => saved, onCookies: fresh => tried.push(fresh),
    sleep: async ms => {
      waits.push(ms);
      const step = steps.shift();
      if (!step) throw new Error('stopped waiting');
      step();
    } }), /stopped waiting/);
  // The 05:00 expiry came at the first check: it is said once, and asked about each minute, not waited on for ten.
  assert.deepEqual(waits, [600e3, 60e3, 60e3, 600e3]);
  assert.deepEqual(logged, ['[brisk] SBI BRiSK: no session is running (2021-09-24, status done); waiting for the next trading day',
    '[brisk] SBI BRiSK: the session has expired; waiting for new cookies (sign in again with `brisk login`)']);
  assert.equal(batches.filter(b => b.type === 'waiting').length, 1, 'Python is told of the wait once');
  assert.deepEqual(batches.filter(b => b.type === 'expired'), [{ type: 'expired' }], 'and of the expiry once, to sign in again');
  // The login made meanwhile is used at once (no further wait), and passed on to the days after.
  assert.deepEqual(tried, [{ s: 'new' }]);
  assert.deepEqual(requests.slice(-2).map(r => r.headers.cookie), ['s=new', 's=new']);
});

test('cookies of a new login are taken up at the start too, and without one an expired session still stops the host', async () => {
  const { fetchImpl } = server({}, {
    '/api/frontend/boot': init => init.headers.cookie === 's=old' ? new Response('', { status: 403 })
      : JSON.stringify({ api_token: 'token', identity: IDENTITY }),
    '/api/app/boot': () => JSON.stringify({ date: '2021-09-24', series: 0, session_status: 'done', time: 1000, next_date: 90000 }),
  });
  const options = { cookies: { s: 'old' }, fetchImpl, emit: async () => {}, log: () => {}, sleep: async () => { throw new Error('stopped waiting'); } };
  await assert.rejects(live(options), /expired or invalid/);
  await assert.rejects(live({ ...options, reloadCookies: () => ({ s: 'old' }) }), /expired or invalid/, 'the same cookies are no new login');
  await assert.rejects(live({ ...options, reloadCookies: () => ({ s: 'new' }) }), /stopped waiting/);
});

test('the cookies `brisk login` saved are read from their file, and nothing else is taken for them', () => {
  const folder = fs.mkdtempSync(path.join(require('node:os').tmpdir(), 'brisk-cookies-'));
  try {
    const file = path.join(folder, 'sbi-cookies.json');
    assert.equal(cookiesIn(file), null, 'no file yet');
    for (const text of ['', 'not JSON', 'null', '[]', '{}', '"s=v"']) {
      fs.writeFileSync(file, text);
      assert.equal(cookiesIn(file), null, JSON.stringify(text));
    }
    fs.writeFileSync(file, JSON.stringify({ session_x: 'v1' }));
    assert.deepEqual(cookiesIn(file), { session_x: 'v1' });
  } finally {
    fs.rmSync(folder, { recursive: true, force: true });
  }
});

test('a day whose session is over is said to have ended, never to be recorded: Python has still to save it', async () => {
  const { fetchImpl } = server({}, { '/api/app/boot': () => JSON.stringify({ date: '2021-09-27', series: 0,
    session_status: 'done', time: 1000, next_date: 1120 }) });
  const batches = [], logged = [];
  await assert.rejects(live({ cookies: { s: 'v' }, fetchImpl, after: '2021-09-27', emit: async b => batches.push(b),
    log: line => logged.push(line), sleep: async () => { throw new Error('stopped waiting'); } }), /stopped waiting/);
  assert.deepEqual(logged, ['[brisk] SBI BRiSK: 2021-09-27 session ended; waiting for the next trading day']);
  assert.deepEqual(batches, [{ type: 'waiting', date: '2021-09-27', session_status: 'done' }]);
});

test('a day ends when the site\'s session is done, and the next starts with the next session',
  { skip: !cache }, async () => {
  const { files, chunks } = demo();
  let date = '2021-09-27', open = true;
  const { fetchImpl } = server(files, { '/api/app/boot': () => JSON.stringify(open
    ? { date, series: 0, master: 'm1', snapshot: 's1', ws_url: '/realtime/0?session=abc', session_status: 'running' }
    : { date, series: 0, session_status: 'done' }) });
  const batches = [], traces = [];
  const emit = async b => { batches.push(b); if (b.type === 'bootstrap') open = false; };
  await record({ days: 2, cookies: { s: 'v' }, codes: ['7203'], emit, fetchImpl, WebSocketImpl: heldSocket(chunks),
    protocolVersion: 16000, dayCheckMs: 20, trace: line => traces.push(line), log: () => {},
    sleep: async () => { date = '2021-09-28'; open = true; } });
  assert.deepEqual(batches.filter(b => b.type !== 'quotes' && b.type !== 'history').map(b => b.type + (b.trading_date ?? b.date ?? '')),
    ['bootstrap20210927', 'end', 'waiting2021-09-27', 'bootstrap20210928', 'end']);
  assert.equal(traces.filter(line => line === 'day over: the session is done').length, 2);
});

test('a connection lost mid-session ends that segment cleanly, and a new connection records on from a fresh snapshot',
  { skip: !cache }, async () => {
  const { files, chunks } = demo();
  const { fetchImpl } = server(files);
  const batches = [], waits = [], logged = [];
  // The first connection drops (1006); the second ends the day as the server closing the stream does.
  let sockets = 0;
  const WebSocketImpl = class { constructor(...a) { return new (socketFrom(chunks, sockets++ ? 1000 : 1006))(...a); } };
  await record({ days: 1, cookies: { s: 'v' }, codes: ['7203'], emit: async b => batches.push(b), fetchImpl, WebSocketImpl,
    protocolVersion: 16000, sleep: async ms => { waits.push(ms); }, log: line => logged.push(line) });
  assert.equal(sockets, 2);
  assert.deepEqual(batches.filter(b => ['bootstrap', 'end'].includes(b.type)).map(b => `${b.type}:${b.seq}`).slice(0, 1),
    ['bootstrap:0']);
  const ends = batches.map((b, i) => b.type === 'end' ? i : -1).filter(i => i >= 0);
  assert.equal(ends.length, 2, 'each segment has its own end');
  assert.equal(batches[ends[0] + 1].type, 'bootstrap', 'the next segment starts from a fresh snapshot');
  assert.equal(batches[ends[0] + 1].seq, 0);
  assert.equal(batches[ends[0] + 1].trading_date, batches[0].trading_date, 'the same trading day');
  assert.equal(batches[ends[0]].quote_updates, batches.slice(0, ends[0]).filter(b => b.type === 'quotes')
    .reduce((n, b) => n + b.quotes.length, 0));
  assert.deepEqual(waits, [RETRY_MS[0]]);
  assert.match(logged[0], /stream closed \(1006\) after 0s \(last frame [\d.]+ s earlier, [^)]*\); reconnecting in 5s/);
});

test('a connection lost says how the link looked when it closed: its age, its last frame, the host\'s loop and memory',
  { skip: !cache }, async () => {
  const { files, chunks } = demo();
  const { fetchImpl } = server(files);
  await assert.rejects(live({ cookies: { s: 'v' }, codes: ['7203'], emit: async () => {}, fetchImpl, resumable: true,
    WebSocketImpl: socketFrom(chunks, 1006), protocolVersion: 16000, sleep: async () => {}, log: () => {} }),
  error => error instanceof Interrupted
    && /^SBI BRiSK stream closed \(1006\) after \d+s \(last frame \d+\.\d s earlier, event loop lag up to \d+ ms, (?:Node|Bun) \d+\.\d GB\)$/
      .test(error.message));
});

test('reconnecting stops at what another connection cannot mend, and never before anything was recorded',
  { skip: !cache }, async () => {
  const { files, chunks } = demo();
  // Nothing recorded yet: the first failure stops the host, as before.
  let { fetchImpl } = server(files);
  await assert.rejects(record({ days: 1, cookies: { s: 'v' }, codes: ['7203'], emit: async () => {}, fetchImpl,
    WebSocketImpl: socketFrom([], 1006), protocolVersion: 16000, sleep: async () => assert.fail('no retry'),
    log: () => {} }), /stream closed \(1006\)/);
  // Recorded, then the session expired: no reconnecting with cookies that no longer work.
  let boots = 0;
  ({ fetchImpl } = server(files, { '/api/frontend/boot': () => boots++ ? new Response('', { status: 401 })
    : JSON.stringify({ api_token: 'token', identity: IDENTITY }) }));
  const batches = [];
  await assert.rejects(record({ days: 1, cookies: { s: 'v' }, codes: ['7203'], emit: async b => batches.push(b), fetchImpl,
    WebSocketImpl: socketFrom(chunks, 1006), protocolVersion: 16000, sleep: async () => {}, log: () => {} }), /expired/);
  assert.equal(batches.at(-1).type, 'end', 'the segment before was closed cleanly');
  // A connection that fails before the stream starts is retried, with a longer wait each time.
  let attempts = 0;
  const waits = [];
  ({ fetchImpl } = server(files, { '/api/app/boot': () => {
    attempts++;
    return attempts === 2 || attempts === 3 ? new Response('busy', { status: 503 })
      : JSON.stringify({ date: '2021-09-27', series: 0, master: 'm1', snapshot: 's1', ws_url: '/realtime/0?session=abc' });
  } }));
  let sockets = 0;
  await record({ days: 1, cookies: { s: 'v' }, codes: ['7203'], emit: async () => {}, fetchImpl, protocolVersion: 16000,
    WebSocketImpl: class { constructor(...a) { return new (socketFrom(chunks, sockets++ ? 1000 : 1006))(...a); } },
    sleep: async ms => { waits.push(ms); }, log: () => {} });
  assert.deepEqual(waits, [RETRY_MS[0], RETRY_MS[1], RETRY_MS[2]]);
});

test('asked to stop, the host ends the running day there with its end, and neither reconnects nor waits for the next day',
  { skip: !cache }, async () => {
  const { files, chunks } = demo();
  const { fetchImpl } = server(files);
  const batches = [], waits = [];
  let sockets = 0;
  const WebSocketImpl = class { constructor(...a) { sockets++; return new (heldSocket(chunks))(...a); } };
  const stop = { requested: false, end: null };
  // As main's signal handler does once the day is under way (the held socket would otherwise stay open).
  let asked = false;
  const emit = async b => {
    batches.push(b);
    if (b.type === 'quotes' && !asked) {
      asked = true;
      setImmediate(() => { stop.requested = true; stop.end('stopped'); });
    }
  };
  await record({ cookies: { s: 'v' }, codes: ['7203'], emit, fetchImpl, WebSocketImpl, protocolVersion: 16000, stop,
    sleep: async ms => { waits.push(ms); }, log: () => {} });
  assert.equal(sockets, 1);
  assert.deepEqual(waits, []);
  assert.equal(batches.at(-1).type, 'end');
  assert.equal(batches.filter(b => b.type === 'end').length, 1);
  assert.equal(batches.at(-1).quote_updates, batches.filter(b => b.type === 'quotes').reduce((n, b) => n + b.quotes.length, 0));
  assert.equal(stop.end, null, 'no session left to end');
});

test('which failures another connection cannot mend', () => {
  for (const message of ['SBI BRiSK session expired or invalid; log in again', 'SBI BRiSK reports another WebSocket session for this user',
    'Refusing to stream from wss://x: not a BRiSK host', 'BRiSK decoder changed; missing x', 'Codes not in the SBI BRiSK master: 9999']) {
    assert.ok(fatal(new Error(message)), message);
  }
  for (const message of ['SBI BRiSK stream closed (1006)', 'BRiSK connection check failure: no heartbeat for 31s',
    'SBI BRiSK data is stale', 'SBI BRiSK /api/app/boot: HTTP 503', 'Catch-up failed: timeout']) {
    assert.ok(!fatal(new Error(message)), message);
  }
  assert.equal(new Interrupted(new Error('gone')).message, 'gone');
});

test('e-shiten: its own address, a relative bundle, a templated decoder and the stream on the API host',
  { skip: !cache }, async () => {
  // As mapped on next.e-shiten.brisk.jp (2026-10-08): the page names `main-XXXX.js` relatively, the
  // bundle names the loader as a template with no .wasm path, and ws_url is relative to api_endpoint.
  const { files, chunks } = demo();
  const { fetchImpl, requests } = server(files, {
    '/api/frontend/boot': () => JSON.stringify({ api_token: 'token', identity: IDENTITY, api_endpoint: 'https://api.brisk.jp' }),
    '/': () => '<html><script type="module" crossorigin src="main-W3ZVNLJR.js"></script></html>',
    '/main-W3ZVNLJR.js': () => 'const r=x?"-fukuoka":"";import(`./assets/wasm${r}/fita.test.js`)',
  });
  const origins = new Set();
  const batches = [];
  await live({ cookies: { session_x: 'v' }, codes: ['7203'], emit: async b => batches.push(b), site: 'e-shiten',
    fetchImpl: async (url, init) => { origins.add(url.origin); return fetchImpl(url, init); },
    WebSocketImpl: socketFrom(chunks), protocolVersion: 16000 });
  assert.deepEqual([...origins], ['https://next.e-shiten.brisk.jp']);
  assert.ok(requests.some(r => r.path === '/assets/wasm/fita.test.wasm'), 'the .wasm name follows the loader\'s');
  assert.equal(String(socketFrom.last.url), 'wss://api.brisk.jp/realtime/0?session=abc');
  assert.deepEqual(socketFrom.last.init.headers, { origin: 'https://next.e-shiten.brisk.jp' }, 'no cookies to another host');
  assert.deepEqual(socketFrom.last.init.protocols, ['flex']);
  const [bootstrap] = batches;
  assert.equal(bootstrap.source, 'brisk_live');
  assert.equal(bootstrap.input_transport.kind, 'eshiten_websocket');
  assert.equal(bootstrap.input_transport.origin, 'https://next.e-shiten.brisk.jp/');
  assert.equal(bootstrap.input_transport.ws_host, 'api.brisk.jp');
  assert.equal(batches.at(-1).type, 'end');
  await assert.rejects(live({ cookies: { s: 'v' }, site: 'rakuten', emit: async () => {} }), /Unknown BRiSK site "rakuten"; use one of: sbi, e-shiten, matsui, monex, smbcnikko/);
});

test('Matsui, Monex and SMBC Nikko: each its own address and label, the same app as SBI\'s', { skip: !cache }, async () => {
  const { files, chunks } = demo();
  for (const [site, host] of [['matsui', 'matsui.brisk.jp'], ['monex', 'monex.brisk.jp'], ['smbcnikko', 'smbcnikko.brisk.jp']]) {
    const { fetchImpl } = server(files);
    const origins = new Set(), batches = [];
    await live({ cookies: { s: 'v' }, codes: ['7203'], emit: async b => batches.push(b), site,
      fetchImpl: async (url, init) => { origins.add(url.origin); return fetchImpl(url, init); },
      WebSocketImpl: socketFrom(chunks), protocolVersion: 16000 });
    assert.deepEqual([...origins], [`https://${host}`], 'nothing goes to another host');
    assert.equal(String(socketFrom.last.url), `wss://${host}/realtime/0?session=abc`);
    assert.deepEqual(socketFrom.last.init.headers, { origin: `https://${host}`, cookie: 's=v' }, 'its cookies, to its own host');
    const [bootstrap] = batches;
    assert.equal(bootstrap.source, 'brisk_live', 'one source, whichever broker\'s');
    assert.deepEqual([bootstrap.input_transport.kind, bootstrap.input_transport.origin], [`${site}_websocket`, `https://${host}/`]);
    assert.equal(batches.at(-1).type, 'end');
  }
});

test('bundles on other hosts are never fetched, and a session is never streamed off BRiSK', async () => {
  const fetched = [];
  const pages = {
    '/': '<script src="https://cdn.example/x.js"></script><script src=\'./lib/app.js\'></script>',
    '/lib/app.js': 'f("/assets/wasm/fita.abc.js")',
  };
  const session = new Session({ s: 'v' }, async url => {
    fetched.push(url.href);
    return new Response(pages[url.pathname] ?? 'bytes', { status: 200 });
  }, site('e-shiten'));
  const { paths } = await decoderAssets(session);
  assert.deepEqual(paths, ['/assets/wasm/fita.abc.js', '/assets/wasm/fita.abc.wasm']);
  assert.ok(fetched.every(href => href.startsWith('https://next.e-shiten.brisk.jp/')), fetched.join(' '));

  const sbi = 'https://sbi.brisk.jp';
  assert.equal(streamUrl('/realtime/0?session=a', undefined, sbi).href, 'wss://sbi.brisk.jp/realtime/0?session=a');
  assert.equal(streamUrl('/realtime/0?session=a', 'https://api.brisk.jp', sbi).href, 'wss://api.brisk.jp/realtime/0?session=a');
  assert.equal(streamUrl('/realtime/0', 'http://api.brisk.jp', sbi).host, 'sbi.brisk.jp', 'only an https endpoint is a base');
  assert.equal(streamUrl('wss://brisk.jp/ws', 'https://api.brisk.jp', sbi).host, 'brisk.jp');
  for (const [url, endpoint] of [['/ws?session=a', 'https://evil.example'], ['wss://brisk.jp.evil.example/ws', undefined],
    ['ws://api.brisk.jp/ws', undefined], ['https://api.brisk.jp/ws', undefined]]) {
    assert.throws(() => streamUrl(url, endpoint, sbi), /not a BRiSK host/, url);
  }
});

test('the BRiSK host fails loudly on changed sites, unknown codes and abnormal closes', { skip: !cache }, async () => {
  const { files, chunks } = demo();
  const run = (overrides, options = {}) => live({ cookies: { s: 'v' }, emit: async () => {}, protocolVersion: 16000,
    fetchImpl: server(files, overrides).fetchImpl, WebSocketImpl: socketFrom(chunks), ...options });
  await assert.rejects(run({ '/main.abc.js': () => 'nothing' }), /decoder not found/);
  await assert.rejects(run({}, { codes: ['0000'] }), /not in the SBI BRiSK master/);
  await assert.rejects(run({}, { WebSocketImpl: socketFrom(chunks, 1006) }), /closed \(1006/);
  await assert.rejects(run({}, { WebSocketImpl: socketFrom([]) }), /closed/);
  await assert.rejects(main(['--bad']), /Usage/);
  await assert.rejects(main([], {}), /BRISK_SBI_COOKIES/);
  await assert.rejects(main([], { BRISK_SITE: 'e-shiten', BRISK_SBI_COOKIES: '{"a":"b"}' }), /Set BRISK_ESHITEN_COOKIES to your e-shiten/);
});

// ---------------------------------------------------------------------------------------
// Live transport: Engine.IO / Socket.IO dialect, catch-up, and the WASM's six callbacks.

function engineSocket(chunks, { hold = false, version = 3 } = {}) {
  const open = version === 3
    ? '0{"sid":"SID0123456789012345678901234567","upgrades":[],"pingInterval":25000,"pingTimeout":5000}'
    : '0{"sid":"SID0123456789012345678901234567","upgrades":[],"pingInterval":25000,"pingTimeout":5000,"maxPayload":1000000}';
  return class extends EventTarget {
    constructor(url, init) {
      super();
      this.url = url; this.init = init; this.sent = []; this.closed = false;
      engineSocket.last = this;
      setImmediate(() => this.dispatchEvent(new MessageEvent('message', { data: open })));
    }
    send(data) {
      const value = typeof data === 'string' ? data : Buffer.from(data);
      this.sent.push(value);
      if (typeof value !== 'string') return;
      if (value.startsWith('40/v2/user?')) {
        setImmediate(() => { this.text('40'); this.text('40/v2/user,'); });
      } else if (value.startsWith('42/v2/user,')) {
        setImmediate(() => {
          for (const chunk of chunks) {
            this.dispatchEvent(new MessageEvent('message', { data: chunk.buffer.slice(chunk.byteOffset, chunk.byteOffset + chunk.length) }));
          }
          if (!hold) setImmediate(() => this.close(1000));
        });
      }
    }
    text(data) { this.dispatchEvent(new MessageEvent('message', { data })); }
    close(code = 1000) {
      if (this.closed) return;
      this.closed = true;
      this.dispatchEvent(Object.assign(new Event('close'), { code, reason: '' }));
    }
  };
}

const withHost = files => server(files, {
  '/api/app/boot': () => JSON.stringify({ date: '2021-09-27', series: 0, master: 'm1', snapshot: 's1',
    ws_url: `/realtime/0?session=${WS_SESSION}` }),
});

test('the BRiSK host speaks Engine.IO/Socket.IO when the server does, with secrets kept out of the trace',
  { skip: !cache }, async () => {
    const { files, chunks } = demo();
    const { fetchImpl, requests } = withHost(files);
    const batches = [], trace = [];
    const summary = await live({ cookies: { session_x: 'v' }, codes: ['7203'], emit: async b => batches.push(b),
      fetchImpl, WebSocketImpl: engineSocket(chunks), protocolVersion: 16000, trace: line => trace.push(line) });
    const socket = engineSocket.last;
    const join = socket.sent.find(m => typeof m === 'string' && m.startsWith('40/v2/user?'));
    const params = new URLSearchParams(join.split('?')[1]);
    assert.equal(params.get('api_key'), MARKET_TOKEN);
    assert.equal(params.get('visitor_id'), IDENTITY);
    assert.equal(params.get('session_id'), WS_SESSION);
    assert.match(params.get('tabId'), /^[0-9a-f-]{36}$/);
    assert.equal(params.get('url'), 'https://sbi.brisk.jp/');
    assert.ok(socket.sent.some(m => typeof m === 'string' && m.startsWith('42/v2/user,["userEvent",{"name":"startLive"')));
    const tokenRequest = requests.find(r => r.path === '/api/app/market-token');
    assert.equal(tokenRequest.headers.authorization, 'Bearer token');
    assert.equal(tokenRequest.headers.cookie, 'session_x=v');
    const [bootstrap, ...rest] = batches;
    assert.equal(bootstrap.input_transport.dialect, 'engineio');
    assert.equal(bootstrap.input_transport.engineio, 3);
    assert.equal(bootstrap.quotes[0].indicative_price10, 101500);
    assert.equal(rest.at(-1).type, 'end');
    assert.equal(summary.frames, chunks.length);
    const printed = trace.join('\n');
    assert.match(printed, /→ 40\/v2\/user\?api_key=<redacted:25>/);
    for (const secret of [MARKET_TOKEN, IDENTITY, WS_SESSION, 'SID0123456789012345678901234567']) {
      assert.ok(!printed.includes(secret), `trace leaked ${secret.slice(0, 10)}`);
    }
  });

test('a refused market token or namespace is a clear error, not a hang', { skip: !cache }, async () => {
  const { files, chunks } = demo();
  const run = (overrides, socket = engineSocket(chunks), options = {}) => live({ cookies: { s: 'v' }, emit: async () => {},
    protocolVersion: 16000, fetchImpl: server(files, { '/api/app/boot': () => JSON.stringify({ date: '2021-09-27',
      series: 0, master: 'm1', snapshot: 's1', ws_url: `/realtime/0?session=${WS_SESSION}` }), ...overrides }).fetchImpl,
    WebSocketImpl: socket, ...options });
  await assert.rejects(run({ '/api/app/market-token': () => new Response('no', { status: 500 }) }),
    /app\/market-token failed: SBI BRiSK \/api\/app\/market-token: HTTP 500/);
  const refusing = class extends engineSocket(chunks) {
    send(data) {
      super.send(data);
      if (typeof data === 'string' && data.startsWith('40/v2/user?')) setImmediate(() => this.text('44/v2/user,{"message":"bad api_key"}'));
    }
  };
  await assert.rejects(run({}, refusing), /Socket\.IO connect error on \/v2\/user: \{"message":"bad api_key"\}/);
  await assert.rejects(run({}, engineSocket(chunks), { profile: { startLiv: {} } }), /Unknown BRiSK profile keys/);
});

test('profile overrides reach the connect query and the startLive payload', { skip: !cache }, async () => {
  const { files, chunks } = demo();
  await live({ cookies: { s: 'v' }, emit: async () => {}, protocolVersion: 16000, fetchImpl: withHost(files).fetchImpl,
    WebSocketImpl: engineSocket(chunks), profile: { connectQuery: { _v: 'build-9' }, startLive: { series: 0 } } });
  const sent = engineSocket.last.sent.filter(m => typeof m === 'string');
  assert.match(sent.find(m => m.startsWith('40/v2/user?')), /_v=build-9/);
  assert.match(sent.find(m => m.startsWith('42')), /"data":\{"series":0\}/);
});

// Make the real snapshot look `by` frames older than the stream, until `fixed()` says caught up.
function snapshotBehind(by, fixed) {
  const real = Decoder.prototype.frameNumbers;
  Decoder.prototype.frameNumbers = function () {
    const numbers = real.call(this);
    return fixed() ? numbers : numbers.map(n => Math.max(0, n - by));
  };
  return () => { Decoder.prototype.frameNumbers = real; };
}

// The batches a session writes as Python reads them (a batch whose quotes workers read comes as its line),
// without the fields that time the run.
function reading() {
  const batches = [];
  const emit = async (record, line) => { batches.push(JSON.parse(line ?? JSON.stringify(record))); };
  const timeless = () => batches.map(({ received_unix_ms, decode_ns, replay_wall_ms, ...b }) => {
    if (b.input_transport) { const { setup_ms, ...transport } = b.input_transport; b.input_transport = transport; }
    return b;
  });
  return { batches, emit, timeless };
}

// A server that sends its first `split` frames, and the rest after a catch-up: `after` ms later (a number), or as soon as
// `after()` is true (polled: the catch-up has run, however long this computer takes to get there; a fixed delay is a
// bet on its speed, and a slow runner loses it); first calling `between`; then it closes.
function laterSocket(chunks, split, after, between = () => {}) {
  return class extends EventTarget {
    constructor() {
      super();
      const send = list => list.forEach(c => this.dispatchEvent(new MessageEvent('message', { data: c.buffer.slice(c.byteOffset, c.byteOffset + c.length) })));
      setImmediate(() => send(chunks.slice(0, split)));
      const rest = () => { between(); send(chunks.slice(split)); setImmediate(() => this.close(1000)); };
      if (typeof after === 'function') {
        this.timer = setInterval(() => { if (after()) { clearInterval(this.timer); rest(); } }, 5);
      } else {
        this.timer = setTimeout(rest, after);
      }
    }
    send() {}
    close(code = 1000) {
      clearTimeout(this.timer);
      clearInterval(this.timer);
      if (this.closed) return;
      this.closed = true;
      this.dispatchEvent(Object.assign(new Event('close'), { code, reason: '' }));
    }
  };
}

test('with quote workers a session writes the same batches, its quotes read in parallel', { skip: !cache }, async () => {
  const { files } = demo();
  const chunks = [...frames(files['ws.dat'])].slice(0, 300).map(f => f.data);
  const run = async (workers, behind) => {
    const { emit, timeless } = reading();
    let caught = false;
    const restore = behind ? snapshotBehind(5, () => caught) : () => {};
    try {
      await live({ cookies: { s: 'v' }, emit, protocolVersion: 16000, fetchImpl: server(files).fetchImpl, workers,
        WebSocketImpl: behind ? laterSocket(chunks, 3, () => caught) : socketFrom(chunks),
        ...(behind && { catchUp: async () => { caught = true; return Buffer.alloc(0); } }) });
    } finally { restore(); }
    return timeless();
  };
  const alone = await run(0);
  assert.ok(alone.filter(b => b.type === 'quotes').reduce((n, b) => n + b.quotes.length, 0) > 10000, 'the whole market');
  assert.deepEqual(await run(2), alone);
  // A catch-up reaches the workers' decoders as the host's, and the frames after it read the same.
  const caughtUp = await run(0, true);
  assert.ok(caughtUp.filter(b => b.type === 'quotes').length > 200);
  assert.deepEqual(await run(2, true), caughtUp);
});

test('the fast lane writes the subscribed symbols of each frame at once, as the recording has them, and joins new ones',
  { skip: !cache }, async () => {
  const { files } = demo();
  const chunks = [...frames(files['ws.dat'])].slice(0, 300).map(f => f.data);
  const lanes = { codes: new Set(['7203', '6758']), onChange: null };
  const lines = [];
  const emit = async (record, line) => { lines.push(JSON.parse(line ?? JSON.stringify(record))); };
  // Between the stream's two halves a Live session subscribes to another symbol, while the day runs.
  const between = () => { lanes.codes = new Set(['7203', '6758', '9984']); lanes.onChange(); };
  await live({ cookies: { s: 'v' }, emit, protocolVersion: 16000, fetchImpl: server(files).fetchImpl, workers: 3, lanes,
    WebSocketImpl: laterSocket(chunks, 120, 300, between) });
  assert.deepEqual(lines[0], { type: 'lanes', fast: true, capacity: 500, codes: ['6758', '7203'] });
  assert.equal(lines[1].type, 'bootstrap');
  const master = new Map(lines[1].master.map(m => [m.code, m.issue_id]));
  const recording = new Map(lines.filter(b => b.type === 'quotes' && !b.lane).map(b => [b.seq, b]));
  const fastLines = lines.filter(b => b.lane === 'fast' && b.type === 'quotes');
  const join = lines.find(b => b.type === 'join');
  assert.ok(fastLines.length > 200 && join, `${fastLines.length} fast lines`);
  const softbank = master.get('9984');
  // The joined symbol's state is the recording's last quote of it up to the join's batch.
  let last = lines[1].quotes.find(q => q.issue_id === softbank);
  for (const [seq, b] of recording) if (seq <= join.seq) last = b.quotes.find(q => q.issue_id === softbank) ?? last;
  // The recording's quotes also say what of their books changed (book_changes); the fast lane's, read for Live, do not.
  const plainQuote = ({ book_changes, ...q }) => (assert.ok(Array.isArray(book_changes)), q);
  assert.deepEqual(join.codes, ['9984']);
  assert.deepEqual(join.quotes, [plainQuote(last)]);
  for (const f of fastLines) {
    const wanted = new Set([master.get('7203'), master.get('6758'), ...(f.seq > join.seq ? [softbank] : [])]);
    const b = recording.get(f.seq);
    assert.deepEqual(f.quotes, b.quotes.filter(q => wanted.has(q.issue_id)).map(plainQuote), `batch ${f.seq}`);
    assert.deepEqual(f.trades, b.trades.filter(t => wanted.has(t.issue_id)), `batch ${f.seq}`);
    assert.equal(f.source_time_us, b.source_time_us);
  }
  assert.ok(fastLines.some(f => f.seq > join.seq && f.quotes.some(q => q.issue_id === softbank)), 'the joined symbol updates');
  assert.equal(lines.at(-1).type, 'end');
  // Without lanes there is no fast lane, and nothing is said of one.
  const plain = reading();
  await live({ cookies: { s: 'v' }, emit: plain.emit, protocolVersion: 16000, fetchImpl: server(files).fetchImpl, workers: 3,
    WebSocketImpl: socketFrom(chunks.slice(0, 20)) });
  assert.ok(plain.batches.every(b => !b.lane && b.type !== 'lanes'));
});

test('the fast lane has the workers that read as many symbols as Live sessions will want of it', { skip: !cache }, async () => {
  const { files } = demo();
  const chunks = [...frames(files['ws.dat'])].slice(0, 20).map(f => f.data);
  const capacity = async (workers, fastSymbols) => {
    const lines = [];
    const emit = async (record, line) => { lines.push(JSON.parse(line ?? JSON.stringify(record))); };
    await live({ cookies: { s: 'v' }, emit, protocolVersion: 16000, fetchImpl: server(files).fetchImpl, workers,
      lanes: { codes: new Set(['7203']), onChange: null }, fastSymbols, WebSocketImpl: socketFrom(chunks) });
    return lines[0];
  };
  assert.equal((await capacity(4, null)).capacity, 500, 'a quarter of the workers, and at least one');
  assert.equal((await capacity(8, null)).capacity, 1000);
  assert.equal((await capacity(5, 1100)).capacity, 1500, 'three workers for eleven hundred symbols');
  assert.equal((await capacity(3, 500)).capacity, 500);
});

test('the workers\' quotes come back in the host\'s order, and a worker that traced other issues stops the session', () => {
  const results = [{ ids: [2, 4], quotes: ['"q2"', null], decode_ns: 5 }, { ids: [1, 3], quotes: ['"q1"', '"q3"'], decode_ns: 9 }];
  assert.deepEqual(merge([1, 2, 3, 4], results, 2), { quotes: ['"q1"', '"q2"', '"q3"'], skipped: [4], decode_ns: 9 });
  assert.throws(() => merge([1, 2, 3, 4], [{ ...results[0], ids: [2] }, results[1]], 2), /Quote worker 0 decoded other issues.*diverged/);
});

test('closing a pool returns once every one of its worker threads has exited', { skip: !cache }, async () => {
  // A session's workers must be gone before the next session's start: their memory, and a V8 crash seen when a pool
  // started while another's threads were still being terminated.
  const { files } = demo();
  const pool = await QuotePool.start({ assets: files, ids: [0, 1, 2], count: 3, options: { protocolVersion: 16000 } });
  const workers = [...pool.workers];
  assert.ok(workers.every(worker => worker.threadId > 0), 'running');
  await pool.close();
  assert.ok(workers.every(worker => worker.threadId === -1), 'every thread is gone when close() resolves');
  await pool.close();   // and closing again is harmless
});

test('how many quote workers, and a worker that cannot start stops the session', { skip: !cache }, async () => {
  assert.equal(workerCount({ BRISK_WORKERS: '0' }), 0);
  assert.equal(workerCount({ BRISK_WORKERS: '3' }), 3);
  assert.throws(() => workerCount({ BRISK_WORKERS: 'two' }), /BRISK_WORKERS must be a whole number/);
  assert.ok(workerCount({}) >= 2 && workerCount({}) <= 8);
  const { files } = demo();
  await assert.rejects(QuotePool.start({ assets: { ...files, 'master.dat': Buffer.alloc(16) }, ids: [0], count: 1,
    options: { protocolVersion: 16000 } }), /Quote worker 0:/);
});

test('a snapshot behind the stream is caught up while frames keep arriving', { skip: !cache }, async () => {
  const { files, chunks } = demo();
  let caught = false, asked = null;
  const restore = snapshotBehind(5, () => caught);
  const realFeed = Decoder.prototype.feed;
  let fed = 0;
  Decoder.prototype.feed = function (frame) { fed++; return realFeed.call(this, frame); };
  try {
    const batches = [];
    const summary = await live({ cookies: { s: 'v' }, emit: async b => batches.push(b), protocolVersion: 16000,
      fetchImpl: server(files).fetchImpl, WebSocketImpl: socketFrom(chunks),
      catchUp: async issues => { asked = issues; await new Promise(r => setTimeout(r, 40)); caught = true; return Buffer.alloc(0); } });
    assert.ok(asked.length > 1000, 'every lagging issue is requested');
    assert.ok(asked.every(i => i.to - i.from === 5 || i.from === 0));
    assert.deepEqual(Object.keys(asked[0]).sort(), ['from', 'issue_id', 'to']);
    assert.equal(batches[0].type, 'bootstrap');
    assert.equal(batches[0].input_transport.caught_up_issues, asked.length);
    // Frames that arrived during the catch-up reached the WASM; none was dropped.
    assert.equal(summary.frames, chunks.length);
    assert.equal(fed, chunks.length, 'every frame was fed to the decoder, including those during the catch-up');
    assert.equal(batches.at(-1).type, 'end');
  } finally { restore(); Decoder.prototype.feed = realFeed; }
});

test('a catch-up that does not close the gap, or names an unknown format, fails loudly', { skip: !cache }, async () => {
  const { files, chunks } = demo();
  const run = options => live({ cookies: { s: 'v' }, emit: async () => {}, protocolVersion: 16000,
    fetchImpl: server(files).fetchImpl, WebSocketImpl: socketFrom(chunks), catchUpBackoffMs: () => 1, ...options });
  let restore = snapshotBehind(5, () => false);
  try {
    await assert.rejects(run({ catchUp: async () => Buffer.alloc(0) }), /Catch-up left \d+ issues behind the stream/);
    await assert.rejects(run({ profile: { catchUp: 'guess' } }), /"guess" is not a known format \(known: issue-lists\)/);
  } finally { restore(); }
});

test('the catch-up request (the vendor client\'s issue lists, by default), its retries and its refusals', { skip: !cache }, async () => {
  const { files, chunks } = demo();
  let caught = false;
  const restore = snapshotBehind(3, () => caught);
  try {
    const attempts = [];
    const respond = plan => init => {
      attempts.push(init);
      const next = plan.shift();
      if (next === 'ok') { caught = true; return new Response(new Uint8Array(0), { status: 200 }); }
      return new Response('x', next);
    };
    const run = plan => {
      caught = false; attempts.length = 0;
      const host = server(files, { '/api/stocks_update/0': respond(plan) });
      return live({ cookies: { session_x: 'v' }, emit: async () => {}, protocolVersion: 16000, fetchImpl: host.fetchImpl,
        WebSocketImpl: socketFrom(chunks), catchUpBackoffMs: () => 1 })
        .then(summary => ({ summary, host }), error => ({ error, host }));
    };
    let result = await run([{ status: 500 }, { status: 502 }, 'ok']);
    assert.ok(result.summary, 'retried through two failures');
    assert.equal(attempts.length, 3);
    const post = result.host.requests.find(r => r.method === 'POST');
    assert.equal(post.path, '/api/stocks_update/0');
    assert.equal(post.search, '?date=2021-09-27');
    assert.equal(post.headers['content-type'], 'application/json');
    assert.equal(post.headers.authorization, 'Bearer token');
    assert.equal(post.headers.cookie, 'session_x=v');
    const body = JSON.parse(post.body);
    assert.deepEqual(Object.keys(body), ['issue_ids', 'update_numbers_from', 'update_numbers_to']);
    // The decoder is held 3 updates behind on every issue that has any: one entry each, in three parallel lists.
    assert.ok(body.issue_ids.length > 1000);
    assert.equal(body.update_numbers_from.length, body.issue_ids.length);
    assert.equal(body.update_numbers_to.length, body.issue_ids.length);
    assert.ok(body.issue_ids.every((id, k) => Number.isInteger(id)
      && body.update_numbers_to[k] - body.update_numbers_from[k] >= 1 && body.update_numbers_to[k] - body.update_numbers_from[k] <= 3));
    result = await run([{ status: 401 }]);
    assert.match(result.error.message, /session expired/);
    assert.equal(attempts.length, 1, 'an expired session is not retried');
    result = await run([{ status: 409, headers: { 'x-error-reason': 'too-old' } }]);
    assert.match(result.error.message, /snapshot is too old/);
    assert.equal(attempts.length, 1, 'a too-old snapshot is not retried');
    result = await run(Array(8).fill({ status: 500 }));
    assert.match(result.error.message, /Catch-up failed: .*HTTP 500 \(the server rejected format "issue-lists"\)/);
    assert.equal(attempts.length, 6, 'one try and five retries');
  } finally { restore(); }
});

// Drive the WASM's own callbacks through its function table, as the WASM would.
async function liveWithWasm(files, chunks, options = {}) {
  const hook = {};
  const real = Decoder.create;
  Decoder.create = async (assets, create) => {
    const decoder = await real.call(Decoder, assets, { ...create, check: w => {
      create.check?.(w);
      hook.wasm = w;
      const initialize = w._initialize;
      w._initialize = (...a) => { hook.pointers = a.slice(0, 6); return initialize(...a); };
    } });
    hook.decoder = decoder;
    return decoder;
  };
  const batches = [];
  let ready;
  hook.started = new Promise(resolve => { ready = resolve; });
  hook.done = live({ cookies: { s: 'v' }, protocolVersion: 16000, fetchImpl: server(files).fetchImpl,
    WebSocketImpl: hook.Socket = engineSocketPlain(chunks), heartbeatMs: 80, ...options,
    emit: async b => { batches.push(b); if (b.type === 'bootstrap') ready(); } });
  hook.batches = batches;
  hook.done.catch(() => {});   // a test that fails early must not turn the session's end into an unhandled rejection
  hook.restore = () => { Decoder.create = real; engineSocketPlain.last?.close(1000); };
  return hook;
}

function engineSocketPlain(chunks) {
  // A plain server that stays open after sending its frames.
  return class extends EventTarget {
    constructor(url, init) {
      super();
      this.sent = []; this.closed = false;
      engineSocketPlain.last = this;
      setImmediate(() => {
        for (const chunk of chunks) {
          this.dispatchEvent(new MessageEvent('message', { data: chunk.buffer.slice(chunk.byteOffset, chunk.byteOffset + chunk.length) }));
        }
      });
    }
    send(data) { this.sent.push(Buffer.from(data)); }
    close(code = 1000) {
      if (this.closed) return;
      this.closed = true;
      this.dispatchEvent(Object.assign(new Event('close'), { code, reason: '' }));
    }
  };
}

test('the WASM\'s outgoing bytes reach the socket, and a silent heartbeat is a failure', { skip: !cache }, async () => {
  const { files, chunks } = demo();
  const hook = await liveWithWasm(files, chunks.slice(0, 3));
  try {
    await hook.started;
    const { wasm, pointers, decoder } = hook;
    const ptr = wasm._malloc(16);
    wasm.HEAPU8.set([0xf0, 1, 2, 3, 4, 5, 6, 7, 8], ptr);
    wasm.dynCall_viii(pointers[0], decoder.id, ptr, 9);                       // send(id, ptr, len)
    assert.deepEqual([...engineSocketPlain.last.sent[0]], [0xf0, 1, 2, 3, 4, 5, 6, 7, 8]);
    wasm.dynCall_viii(pointers[2], decoder.id, 1, 0);                         // one heartbeat, then silence
    await assert.rejects(hook.done, /connection check failure: no heartbeat/);
  } finally { hook.restore(); }
});

test('frames queued behind a slow output keep the link alive: a burst is not a missing heartbeat', { skip: !cache }, async () => {
  const { files } = demo();
  const all = [...frames(files['ws.dat'])].slice(0, 40).map(f => f.data);
  const hook = {};
  const real = Decoder.create;
  Decoder.create = async (assets, create) => (hook.decoder = await real.call(Decoder, assets, { ...create, check: w => {
    create.check?.(w);
    hook.wasm = w;
    const initialize = w._initialize;
    w._initialize = (...a) => { hook.pointers = a.slice(0, 6); return initialize(...a); };
  } }));
  // The server sends a frame every 100 ms after the opening ones; the output stalls for 2 s (5 heartbeat
  // windows) on the first quotes, so nothing is decoded meanwhile, heartbeats included. Keep enough timer
  // headroom for Windows runners loading other WASM modules or Chrome at the same time.
  const Socket = class extends EventTarget {
    constructor() {
      super();
      Socket.last = this;
      const send = chunk => this.dispatchEvent(new MessageEvent('message', { data: chunk.buffer.slice(chunk.byteOffset, chunk.byteOffset + chunk.length) }));
      setImmediate(() => all.slice(0, 3).forEach(send));
      let i = 3;
      this.timer = setInterval(() => (i < all.length ? send(all[i++]) : clearInterval(this.timer)), 100);
    }
    send() {}
    close(code = 1000) {
      clearInterval(this.timer);
      if (this.closed) return;
      this.closed = true;
      this.dispatchEvent(Object.assign(new Event('close'), { code, reason: '' }));
    }
  };
  let stalled = false;
  try {
    const done = live({ cookies: { s: 'v' }, codes: ['7203'], protocolVersion: 16000, fetchImpl: server(files).fetchImpl,
      WebSocketImpl: Socket, heartbeatMs: 400, emit: async b => {
        if (b.type === 'bootstrap') hook.wasm.dynCall_viii(hook.pointers[2], hook.decoder.id, 1, 0);   // one heartbeat
        if (b.type === 'quotes' && !stalled) {
          stalled = true;
          await new Promise(resolve => setTimeout(resolve, 2000));
          setTimeout(() => Socket.last.close(1000), 1500);
        }
      } });
    const summary = await done;
    assert.ok(stalled);
    assert.equal(summary.type, 'end');
    assert.ok(summary.frames > 20, `frames kept arriving (${summary.frames})`);
  } finally { Decoder.create = real; Socket.last?.close(1000); }
});

test('heartbeats keep a feed alive; another session, or the market closing, ends it cleanly', { skip: !cache }, async () => {
  const { files, chunks } = demo();
  let hook = await liveWithWasm(files, chunks.slice(0, 3));
  try {
    await hook.started;
    const beat = setInterval(() => hook.wasm.dynCall_viii(hook.pointers[2], hook.decoder.id, 1, 0), 20);
    await new Promise(resolve => setTimeout(resolve, 300));          // several windows of 80 ms
    clearInterval(beat);
    hook.wasm.dynCall_vii(hook.pointers[4], hook.decoder.id, 1);    // authError: the single-session rule
    await assert.rejects(hook.done, /another WebSocket session for this user \(only one is allowed\)/);
  } finally { hook.restore(); }
  hook = await liveWithWasm(files, chunks.slice(0, 3));
  try {
    await hook.started;
    hook.wasm.dynCall_vi(hook.pointers[5], hook.decoder.id);        // marketFinished
    engineSocketPlain.last.close(1006);                              // the server then drops the link
    const summary = await hook.done;
    assert.equal(summary.type, 'end');
  } finally { hook.restore(); }
});

test('without marketFinished an abnormal close is still an error', { skip: !cache }, async () => {
  const { files, chunks } = demo();
  const hook = await liveWithWasm(files, chunks.slice(0, 3), { heartbeatMs: 60000 });
  try {
    await hook.started;
    engineSocketPlain.last.close(1006);
    await assert.rejects(hook.done, /closed \(1006/);
  } finally { hook.restore(); }
});

test('arguments and profile are validated before anything is fetched', async () => {
  const cookies = '{"session_x":"v"}';
  await assert.rejects(main(['--codes', '7203', '--codes', '6758'], { BRISK_SBI_COOKIES: cookies }), /Usage/);
  await assert.rejects(main(['--trace-protocol', '--trace-protocol'], { BRISK_SBI_COOKIES: cookies }), /Usage/);
  await assert.rejects(main(['--codes'], { BRISK_SBI_COOKIES: cookies }), /Usage/);
  await assert.rejects(main([], { BRISK_SBI_COOKIES: cookies, BRISK_PROFILE: '{nope' }), /BRISK_PROFILE is not valid JSON/);
  await assert.rejects(main([], { BRISK_SBI_COOKIES: cookies, BRISK_PROFILE: '{"nope":1}' }), /Unknown BRiSK profile keys: nope/);
});

test('fetchCatchUp sends the vendor client\'s issue lists unless told otherwise, and names an unknown format', async () => {
  const sent = [];
  const session = { request: async (path, init) => { sent.push({ path, ...init }); return Buffer.from('reply'); } };
  const issues = [{ issue_id: 0, from: 3678, to: 3680 }, { issue_id: 5, from: 8678, to: 8681 }];
  const reply = await fetchCatchUp(session, { app: { series: 0, date: '2026-03-11' }, issues });
  assert.equal(reply.toString(), 'reply');
  assert.equal(sent[0].path, '/api/stocks_update/0?date=2026-03-11');
  assert.equal(sent[0].contentType, 'application/json');
  assert.equal(sent[0].body, '{"issue_ids":[0,5],"update_numbers_from":[3678,8678],"update_numbers_to":[3680,8681]}');
  await assert.rejects(fetchCatchUp(session, { app: { series: 0, date: '2026-03-11' }, issues, format: 'guess' }),
    /2 issues behind the stream.*POST \/api\/stocks_update.*"guess" is not a known format \(known: issue-lists\)/s);
});

test('a live() that fails leaves no timers behind, so the process can exit', { skip: !cache }, async () => {
  const { files } = demo();
  const timers = () => process.getActiveResourcesInfo().filter(name => name === 'Timeout').length;
  const before = timers();
  const broken = class { constructor() { throw new Error('socket constructor failed'); } };
  await assert.rejects(live({ cookies: { s: 'v' }, emit: async () => {}, protocolVersion: 16000,
    fetchImpl: server(files).fetchImpl, WebSocketImpl: broken }), /socket constructor failed/);
  assert.equal(timers(), before, 'the heartbeat monitor and the start timer were stopped');
});

// ---------------------------------------------------------------------------------------
// Protocol version and boot data from the app boot response, the master guard, the stale rule
// and the heartbeat/ping loop, against the real decoder.

const bootWith = extra => ({ '/api/app/boot': () => JSON.stringify({ date: '2021-09-27', series: 0, master: 'm1',
  snapshot: 's1', ws_url: '/realtime/0?session=abc', ...extra }) });
const quietLive = (files, chunks, extra = {}, options = {}) => live({ cookies: { s: 'v' }, emit: async () => {},
  fetchImpl: server(files, bootWith(extra)).fetchImpl, WebSocketImpl: socketFrom(chunks), ...options });

test('the protocol version comes from the boot response unless an option overrides it', { skip: !cache }, async () => {
  const { files, chunks } = demo();
  const batches = [];
  await quietLive(files, chunks, { flex_version: 16000 }, { emit: async b => batches.push(b) });
  assert.equal(batches[0].input_transport.protocol_version, 16000);
  await assert.rejects(quietLive(files, chunks, { flex_version: 18000 }), Error, 'the demo decoder cannot speak 18000');
  await assert.rejects(quietLive(files, chunks, { flex_version: 17000 }), /layout for protocol 17000 is not known/);
  const forced = [];
  await quietLive(files, chunks, { flex_version: 18000 }, { protocolVersion: 16000, emit: async b => forced.push(b) });
  assert.equal(forced[0].input_transport.protocol_version, 16000, 'an explicit option wins');
});

test('boot base prices and exceptional special quotes are applied, and bad ones refused', { skip: !cache }, async () => {
  const { files, chunks } = demo();
  const batches = [];
  // Values consistent with the demo's snapshot (Toyota's own master limits): the decoder builds its
  // order-book rows from the limits the snapshot was cut with, and aborts if boot data contradicts them.
  // That a change is applied is proved at decoder level (decoder.test.cjs); here the plumbing runs end to end.
  const boot = { flex_version: 16000,
    base_prices: [{ issue_code: '7203', base_price10: 101000, limit_down10: 71000, limit_up10: 131000 }],
    exceptional_sq: [{ buy_sell: 'S', jump_range: 10, jump_sec: 180, issue_code: '7203', quote_limit_down: 9900, quote_limit_up: 10400 }] };
  await quietLive(files, chunks, boot, { codes: ['7203'], emit: async b => batches.push(b) });
  assert.deepEqual(batches[0].input_transport.boot_applied, { exceptional_sq: 1, base_prices: 1, applied: 1 });
  assert.equal(batches[0].master[0].limit_up10, 131000);
  assert.equal(batches[0].quotes[0].indicative_price10, 101500, 'the bootstrap still decodes');
  await assert.rejects(quietLive(files, chunks, { flex_version: 16000,
    base_prices: [{ issue_code: '0000', base_price10: 1, limit_down10: 1, limit_up10: 2 }] }),
  /base_prices names issue code 0000, which is not in the master/);
});

test('checkMaster accepts a market and refuses a misread master', () => {
  const issue = i => ({ code: String(1301 + i), base_price10: 30000 + i, limit_up10: 37000 + i, limit_down10: 23000 + i,
    lot_size: 100, name: 'x' });
  const good = Array.from({ length: 4000 }, (_, i) => issue(i));
  assert.doesNotThrow(() => checkMaster(good, 18000));
  // SBI's master of 2026-03-11: 350 of 4,445 codes have a letter (167A), and some have five digits.
  const lettered = good.map((m, i) => (i % 12 ? m : { ...m, code: `${100 + i / 12}A` }));
  lettered[1] = { ...lettered[1], code: '25935' };
  assert.doesNotThrow(() => checkMaster(lettered, 18000));
  // every field read one word late: tick type as code, base as tick, limits as limits, and so on
  const shifted = good.map(m => ({ code: '3', base_price10: m.limit_up10, limit_up10: m.limit_down10,
    limit_down10: m.lot_size, lot_size: 0, name: '' }));
  assert.throws(() => checkMaster(shifted, 18000), /layout not recognised for protocol 18000: .*security codes.*distinct codes/);
  assert.throws(() => checkMaster(good.slice(0, 10), 18000), /only 10 issues/);
  assert.throws(() => checkMaster([], 18000), /only 0 issues/);
});

test('a stale feed is a failure even though heartbeats arrive', { skip: !cache }, async () => {
  const { files, chunks } = demo();
  const hook = await liveWithWasm(files, chunks.slice(0, 3), { heartbeatMs: 60000, staleSeconds: 90 });
  try {
    await hook.started;
    const server = BigInt(Date.UTC(2021, 8, 27, 1, 30)) * 1_000_000n;           // 10:30 JST; the demo data is at 09:00
    hook.wasm.dynCall_viii(hook.pointers[2], hook.decoder.id, Number(server & 0xFFFFFFFFn) | 0, Number(server >> 32n));
    await assert.rejects(hook.done, /data is stale: the decoder's clock is \d+s behind the server \(limit 90s\)/);
  } finally { hook.restore(); }
});

test('a feed that keeps up with the server clock is not stale', { skip: !cache }, async () => {
  const { files, chunks } = demo();
  const hook = await liveWithWasm(files, chunks.slice(0, 3), { heartbeatMs: 60000, staleSeconds: 90 });
  try {
    await hook.started;
    const server = BigInt(Date.UTC(2021, 8, 27, 0, 0, 30)) * 1_000_000n;        // 09:00:30 JST, 30 s after the data
    hook.wasm.dynCall_viii(hook.pointers[2], hook.decoder.id, Number(server & 0xFFFFFFFFn) | 0, Number(server >> 32n));
    await new Promise(resolve => setTimeout(resolve, 1300));                      // more than one 1 s check
    hook.wasm.dynCall_vi(hook.pointers[5], hook.decoder.id);                      // marketFinished
    engineSocketPlain.last.close(1006);
    assert.equal((await hook.done).type, 'end');
  } finally { hook.restore(); }
});

// A server that starts the ping loop and echoes every ping it receives, like the real one.
function pingingSocket(chunks) {
  return class extends EventTarget {
    constructor() {
      super();
      this.sent = []; this.closed = false; this.echo = true;
      pingingSocket.last = this;
      setImmediate(() => {
        for (const chunk of chunks) {
          this.dispatchEvent(new MessageEvent('message', { data: chunk.buffer.slice(chunk.byteOffset, chunk.byteOffset + chunk.length) }));
        }
        setTimeout(() => this.ping(), 20);
      });
    }
    ping() {
      const frame = Buffer.alloc(9);
      frame[0] = 0xf0;
      frame.writeBigUInt64LE(BigInt(Date.now()) * 1_000_000n, 1);
      this.deliver(frame);
    }
    deliver(frame) {
      this.dispatchEvent(new MessageEvent('message', { data: frame.buffer.slice(frame.byteOffset, frame.byteOffset + frame.length) }));
    }
    send(data) {
      const frame = Buffer.from(data);
      this.sent.push(frame);
      if (this.echo && frame[0] === 0xf0) setTimeout(() => !this.closed && this.deliver(frame), 5);
    }
    close(code = 1000) {
      if (this.closed) return;
      this.closed = true;
      this.dispatchEvent(Object.assign(new Event('close'), { code, reason: '' }));
    }
  };
}

test('the ping loop runs through the real decoder: pings are forwarded, echoes beat, silence fails', { skip: !cache }, async () => {
  const { files, chunks } = demo();
  const batches = [];
  const done = live({ cookies: { s: 'v' }, protocolVersion: 16000, heartbeatMs: 400, staleSeconds: 0,
    fetchImpl: server(files).fetchImpl, WebSocketImpl: pingingSocket(chunks.slice(0, 3)),
    emit: async b => batches.push(b) });
  const settled = done.then(() => 'ended', error => error);
  try {
    // Time the loop from the socket, not from live(): loading the decoder and snapshot can take
    // most of a second on a busy CI runner.
    for (const deadline = Date.now() + 30000; !pingingSocket.last && Date.now() < deadline;) {
      await new Promise(resolve => setTimeout(resolve, 10));
    }
    await new Promise(resolve => setTimeout(resolve, 900));            // more than two heartbeat windows
    const socket = pingingSocket.last;
    const pings = socket.sent.filter(f => f[0] === 0xf0);
    assert.ok(pings.length >= 10, `${pings.length} pings were forwarded and echoed`);
    assert.ok(pings.every(p => p.length === 9));
    assert.equal(await Promise.race([settled, new Promise(resolve => setTimeout(() => resolve('running'), 10))]), 'running',
      'the monitor stayed satisfied while echoes arrived');
    socket.echo = false;                                               // the server goes quiet
    const error = await settled;
    assert.match(error.message, /connection check failure: no heartbeat/);
  } finally {
    pingingSocket.last?.close(1000);                                   // a failed assertion must not leave a session running
  }
});

test('a live session allows the SBI app\'s 10 s between server heartbeats', () => {
  assert.equal(require('../../briskapi/decoder/brisk.cjs').HEARTBEAT_MS, 10000);
});
