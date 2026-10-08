'use strict';
// SBI live host against a fake sbi.brisk.jp and WebSocket, driven by the demo's
// real decoder and frames (no SBI account or network needed).
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { Session, checkAbi, checkMaster, live, main, fetchCatchUp } = require('../../briskapi/decoder/sbi.cjs');
const { frames, Decoder } = require('../../briskapi/decoder/decoder.cjs');
const cache = process.env.BRISK_MOCK_CACHE;

const IDENTITY = 'a'.repeat(64);
const MARKET_TOKEN = 'v2.local.MARKETTOKENVALUE';
const WS_SESSION = 'b'.repeat(48);

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

test('SBI host bootstraps from boot, master and snapshot, then streams frames', { skip: !cache }, async () => {
  const { files, chunks } = demo();
  const { fetchImpl, requests } = server(files);
  const batches = [];
  const summary = await live({ cookies: { session_x: 'v' }, codes: ['7203'], emit: async b => batches.push(b),
    fetchImpl, WebSocketImpl: socketFrom(chunks), protocolVersion: 16000 });
  assert.equal(String(socketFrom.last.url), 'wss://sbi.brisk.jp/realtime/0?session=abc');
  assert.equal(socketFrom.last.init.headers.cookie, 'session_x=v');
  assert.ok(requests.filter(r => r.path.startsWith('/api/app')).every(r => r.headers.authorization === 'Bearer token'));
  const [bootstrap, ...rest] = batches;
  assert.equal(bootstrap.source, 'sbi_live');
  assert.equal(bootstrap.trading_date, '20210927');
  assert.deepEqual(bootstrap.master.map(m => m.code), ['7203']);
  assert.equal(bootstrap.quotes[0].indicative_price10, 101500);
  assert.equal(bootstrap.input_transport.kind, 'sbi_websocket');
  assert.equal(bootstrap.input_transport.dialect, 'raw');
  assert.equal(bootstrap.input_transport.caught_up_issues, 0);
  assert.ok(!requests.some(r => r.path === '/api/app/market-token'), 'a plain server never asks for the market token');
  assert.match(bootstrap.input_transport.decoder_sha256, /^[0-9a-f]{64}$/);
  assert.deepEqual(rest.map(b => b.seq), [1, 2, 3, 4, 5, 6]);
  assert.equal(rest.at(-1).type, 'end');
  assert.equal(summary.frames, 6);
});

test('SBI host fails loudly on changed sites, unknown codes and abnormal closes', { skip: !cache }, async () => {
  const { files, chunks } = demo();
  const run = (overrides, options = {}) => live({ cookies: { s: 'v' }, emit: async () => {}, protocolVersion: 16000,
    fetchImpl: server(files, overrides).fetchImpl, WebSocketImpl: socketFrom(chunks), ...options });
  await assert.rejects(run({ '/main.abc.js': () => 'nothing' }), /decoder not found/);
  await assert.rejects(run({}, { codes: ['0000'] }), /not in the SBI master/);
  await assert.rejects(run({}, { WebSocketImpl: socketFrom(chunks, 1006) }), /closed \(1006/);
  await assert.rejects(run({}, { WebSocketImpl: socketFrom([]) }), /closed/);
  await assert.rejects(main(['--bad']), /Usage/);
  await assert.rejects(main([], {}), /BRISK_SBI_COOKIES/);
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

test('SBI host speaks Engine.IO/Socket.IO when the server does, with secrets kept out of the trace',
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
  await assert.rejects(run({}, engineSocket(chunks), { profile: { startLiv: {} } }), /Unknown SBI profile keys/);
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

test('a catch-up that does not close the gap, or has no known format, fails loudly', { skip: !cache }, async () => {
  const { files, chunks } = demo();
  const run = options => live({ cookies: { s: 'v' }, emit: async () => {}, protocolVersion: 16000,
    fetchImpl: server(files).fetchImpl, WebSocketImpl: socketFrom(chunks), catchUpBackoffMs: () => 1, ...options });
  let restore = snapshotBehind(5, () => false);
  try {
    await assert.rejects(run({ catchUp: async () => Buffer.alloc(0) }), /Catch-up left \d+ issues behind the stream/);
    await assert.rejects(run({}), /request body is not public\. Name a format in the profile \(catchUp\)/);
    await assert.rejects(run({ profile: { catchUp: 'guess' } }), /"guess" is not a known format \(known: json-vendor\)/);
  } finally { restore(); }
});

test('the json-vendor catch-up request, its retries and its refusals', { skip: !cache }, async () => {
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
        WebSocketImpl: socketFrom(chunks), profile: { catchUp: 'json-vendor' }, catchUpBackoffMs: () => 1 })
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
    assert.deepEqual(Object.keys(body[0]).sort(), ['from', 'issueCodeIdx', 'to']);
    result = await run([{ status: 401 }]);
    assert.match(result.error.message, /session expired/);
    assert.equal(attempts.length, 1, 'an expired session is not retried');
    result = await run([{ status: 409, headers: { 'x-error-reason': 'too-old' } }]);
    assert.match(result.error.message, /snapshot is too old/);
    assert.equal(attempts.length, 1, 'a too-old snapshot is not retried');
    result = await run(Array(8).fill({ status: 500 }));
    assert.match(result.error.message, /Catch-up failed: .*HTTP 500 \(the server rejected format "json-vendor"\)/);
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
  await assert.rejects(main([], { BRISK_SBI_COOKIES: cookies, BRISK_SBI_PROFILE: '{nope' }), /BRISK_SBI_PROFILE is not valid JSON/);
  await assert.rejects(main([], { BRISK_SBI_COOKIES: cookies, BRISK_SBI_PROFILE: '{"nope":1}' }), /Unknown SBI profile keys: nope/);
});

test('fetchCatchUp names its own problem when no format is configured', async () => {
  await assert.rejects(fetchCatchUp({}, { app: { series: 0, date: '2021-09-27' }, issues: [{ issue_id: 1, from: 1, to: 2 }] }),
    /1 issues behind the stream.*POST \/api\/stocks_update.*not public/s);
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
  // every field read one word late: tick type as code, base as tick, limits as limits, and so on
  const shifted = good.map(m => ({ code: '3', base_price10: m.limit_up10, limit_up10: m.limit_down10,
    limit_down10: m.lot_size, lot_size: 0, name: '' }));
  assert.throws(() => checkMaster(shifted, 18000), /layout not recognised for protocol 18000: .*numeric codes.*distinct codes/);
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
