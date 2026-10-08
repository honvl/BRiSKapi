'use strict';
// The BRiSK WebSocket link against a scripted fake server: plain binary frames (Next) and
// the Engine.IO / Socket.IO dialect upstream documents for SBI. No account or network.
const test = require('node:test');
const assert = require('node:assert/strict');
const { Link, SBI_PROFILE, resolveProfile, heartbeatMonitor, redact } = require('../../briskapi/decoder/engineio.cjs');

class FakeSocket extends EventTarget {
  constructor(url, init) {
    super();
    this.url = url; this.init = init; this.sent = []; this.closed = false;
    FakeSocket.last = this;
  }
  send(data) { this.sent.push(typeof data === 'string' ? data : Buffer.from(data)); }
  close(code = 1000) {
    if (this.closed) return;
    this.closed = true;
    this.dispatchEvent(Object.assign(new Event('close'), { code, reason: '' }));
  }
  server(data) { this.dispatchEvent(new MessageEvent('message', { data })); }
}

const bytes = (...values) => Uint8Array.from(values).buffer;
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
async function until(condition, what, ms = 1000) {
  const end = Date.now() + ms;
  while (!condition()) {
    if (Date.now() > end) throw new Error(`Timed out waiting for ${what}`);
    await sleep(5);
  }
}

const CTX = { marketToken: 'v2.local.MARKETTOKEN', identity: 'a'.repeat(64), wsSession: 'b'.repeat(40),
  tabId: 'tab-1', origin: 'https://sbi.brisk.jp' };

function open(overrides = {}) {
  const seen = { binary: [], errors: [], closes: [], trace: [] };
  const link = new Link({
    WebSocketImpl: FakeSocket, url: new URL('wss://sbi.brisk.jp/realtime/0?session=' + 'b'.repeat(40)),
    headers: { cookie: 'session_x=v' }, profile: resolveProfile(), context: async () => CTX,
    onBinary: data => seen.binary.push(data), onError: error => seen.errors.push(error),
    onClose: close => seen.closes.push(close), trace: line => seen.trace.push(line), ...overrides,
  });
  return { link, seen, socket: FakeSocket.last };
}

const OPEN_V3 = '0{"sid":"SID0123456789012345678901234567","upgrades":[],"pingInterval":40,"pingTimeout":120}';
const OPEN_V4 = '0{"sid":"SID0123456789012345678901234567","upgrades":[],"pingInterval":40,"pingTimeout":120,"maxPayload":1000000}';

test('a plain server: binary frames pass straight through and text is ignored', async () => {
  const { link, seen, socket } = open();
  assert.equal(socket.init.headers.cookie, 'session_x=v');
  socket.server('hello');
  socket.server(bytes(0x19, 0, 1, 2));
  assert.deepEqual(seen.binary.map(b => [...b]), [[0x19, 0, 1, 2]]);
  assert.equal(link.mode, 'raw');
  assert.deepEqual(socket.sent, []);
  socket.server(bytes(5));
  assert.equal(seen.binary.length, 2);
});

test('a server that starts with binary is plain, too', () => {
  const { link, seen, socket } = open();
  socket.server(bytes(9, 9));
  assert.equal(link.mode, 'raw');
  assert.equal(seen.binary.length, 1);
});

test('Engine.IO 3 (SBI per upstream): join the namespace with a query, send startLive, then binary flows', async () => {
  const { link, seen, socket } = open();
  socket.server(OPEN_V3);
  await until(() => socket.sent.some(m => m.startsWith('40/v2/user')), 'the namespace join');
  const join = socket.sent.find(m => m.startsWith('40/v2/user'));
  const params = new URLSearchParams(join.split('?')[1]);
  assert.equal(params.get('api_key'), CTX.marketToken);
  assert.equal(params.get('visitor_id'), CTX.identity);
  assert.equal(params.get('session_id'), CTX.wsSession);
  assert.equal(params.get('tabId'), 'tab-1');
  assert.equal(params.get('url'), 'https://sbi.brisk.jp/');
  assert.equal(link.version, 3);
  socket.server('40');            // root namespace connected: nothing to do
  assert.ok(!socket.sent.some(m => m.includes('startLive')));
  socket.server('40/v2/user,');   // ours
  assert.deepEqual(JSON.parse(socket.sent.find(m => m.startsWith('42/v2/user,')).slice('42/v2/user,'.length)),
    ['userEvent', { name: 'startLive', data: {} }]);
  assert.equal(link.state, 'live');
  socket.server(bytes(0x19, 7));
  assert.deepEqual([...seen.binary[0]], [0x19, 7]);
  assert.deepEqual(seen.errors, []);
});

test('Engine.IO 3: the client pings every interval and fails when no pong comes', async () => {
  const { seen, socket } = open();
  socket.server(OPEN_V3);
  await until(() => socket.sent.filter(m => m === '2').length >= 2 || seen.errors.length, 'two pings or a failure', 600);
  // 40 ms interval, 120 ms timeout, never answered: the second ping cannot be needed
  await until(() => seen.errors.length, 'a ping timeout');
  assert.match(seen.errors[0].message, /Engine\.IO ping timeout/);
  assert.ok(socket.closed);
});

test('Engine.IO 3: a pong keeps the session alive', async () => {
  const { seen, socket } = open();
  socket.server(OPEN_V3);
  const answer = setInterval(() => socket.server('3'), 15);
  await sleep(300);
  clearInterval(answer);
  assert.deepEqual(seen.errors, []);
  assert.ok(socket.sent.filter(m => m === '2').length >= 3);
  socket.close();
});

test('Engine.IO 4: the server pings, the client pongs, and silence is fatal', async () => {
  const { link, seen, socket } = open();
  socket.server(OPEN_V4);
  assert.equal(link.version, 4);
  socket.server('2');
  assert.ok(socket.sent.includes('3'));
  assert.ok(!socket.sent.includes('2'), 'a version 4 client must not ping');
  await until(() => seen.errors.length, 'the watchdog', 800);
  assert.match(seen.errors[0].message, /stopped pinging/);
});

test('Socket.IO connect errors are reported with credentials removed', async () => {
  const { seen, socket } = open();
  socket.server(OPEN_V3);
  await until(() => socket.sent.some(m => m.startsWith('40/v2/user')), 'the join');
  socket.server('44/v2/user,{"message":"invalid api_key v2.local.SECRETPASETO","token":"deadbeef' + '0'.repeat(30) + '"}');
  await until(() => seen.errors.length, 'the error');
  assert.match(seen.errors[0].message, /connect error on \/v2\/user/);
  assert.doesNotMatch(seen.errors[0].message, /SECRETPASETO|deadbeef0/);
  assert.match(seen.errors[0].message, /<redacted:/);
});

test('a namespace that is never acknowledged fails with a clear message', async () => {
  const { seen, socket } = open({ connectTimeoutMs: 50 });
  socket.server(OPEN_V3);
  await until(() => seen.errors.length, 'the timeout');
  assert.match(seen.errors[0].message, /namespace \/v2\/user was not acknowledged/);
});

test('the server closing our namespace, or the session, is an error', async () => {
  let { seen, socket } = open();
  socket.server(OPEN_V3);
  await until(() => socket.sent.some(m => m.startsWith('40/v2/user')), 'the join');
  socket.server('41/v2/user,');
  await until(() => seen.errors.length, 'the disconnect');
  assert.match(seen.errors[0].message, /closed by the server/);
  ({ seen, socket } = open());
  socket.server(OPEN_V3);
  socket.server('1');
  assert.match(seen.errors[0].message, /server closed the session/);
});

test('a failing context (for example the token request) surfaces as the link error', async () => {
  const { seen, socket } = open({ context: async () => { throw new Error('app/market-token failed: HTTP 500'); } });
  socket.server(OPEN_V3);
  await until(() => seen.errors.length, 'the context failure');
  assert.match(seen.errors[0].message, /market-token failed/);
});

test('malformed or unknown Engine.IO packets fail instead of being guessed at', () => {
  let { seen, socket } = open();
  socket.server('0{not json');
  assert.match(seen.errors[0].message, /Malformed Engine\.IO open packet/);
  ({ seen, socket } = open());
  socket.server(OPEN_V3);
  socket.server('9nonsense');
  assert.match(seen.errors[0].message, /Unknown Engine\.IO packet type/);
});

test('the WASM\'s outgoing bytes (pings) are written as raw binary frames', async () => {
  const { link, socket } = open();
  socket.server(bytes(1));
  link.send(Buffer.from([0xf0, 1, 2, 3, 4, 5, 6, 7, 8]));
  assert.deepEqual([...socket.sent[0]], [0xf0, 1, 2, 3, 4, 5, 6, 7, 8]);
});

test('profile overrides merge over the documented defaults, and typos are rejected', async () => {
  const profile = resolveProfile({ connectQuery: { _v: 'build-7', tabId: 'mine' }, startLive: { series: 0 } });
  const query = profile.connectQuery(CTX);
  assert.equal(query._v, 'build-7');
  assert.equal(query.tabId, 'mine');
  assert.equal(query.api_key, CTX.marketToken);
  assert.deepEqual(profile.startLive(CTX), { series: 0 });
  assert.deepEqual(SBI_PROFILE.startLive(CTX), {});
  assert.throws(() => resolveProfile({ startLiv: {} }), /Unknown SBI profile keys: startLiv/);
  assert.throws(() => resolveProfile([]), /JSON object/);
  assert.throws(() => resolveProfile(null), /JSON object/);
  const { socket } = open({ profile });
  socket.server(OPEN_V3);
  await until(() => socket.sent.some(m => m.startsWith('40/v2/user')), 'the join');
  assert.match(socket.sent.find(m => m.startsWith('40/v2/user')), /_v=build-7/);
  socket.server('40/v2/user,');
  assert.match(socket.sent.find(m => m.startsWith('42')), /"data":\{"series":0\}/);
});

test('binaryPrefix strips the Engine.IO 3 type byte and rejects frames without it', async () => {
  const { seen, socket } = open({ profile: resolveProfile({ binaryPrefix: true }) });
  socket.server(OPEN_V3);
  socket.server(bytes(4, 0x19, 1));
  assert.deepEqual([...seen.binary[0]], [0x19, 1]);
  socket.server(bytes(0x19, 1));
  assert.match(seen.errors[0].message, /without the message type byte/);
});

test('the server closing the socket is passed on with its code', () => {
  const { seen, socket } = open();
  socket.server(bytes(1));
  socket.close(1006);
  assert.deepEqual(seen.closes, [{ code: 1006, reason: '' }]);
});

test('redaction removes every credential shape and tracing never prints one', async () => {
  const secrets = ['v2.local.MARKETTOKEN', 'a'.repeat(64), 'b'.repeat(40), 'SID0123456789012345678901234567'];
  const text = `40/v2/user?api_key=${secrets[0]}&visitor_id=${secrets[1]}&session_id=${secrets[2]}&sid=${secrets[3]}`;
  const clean = redact(text);
  for (const secret of secrets) assert.ok(!clean.includes(secret), `leaked ${secret.slice(0, 8)}`);
  assert.match(clean, /api_key=<redacted:20>/);
  assert.doesNotMatch(redact('{"sid":"abcdefghijkl","token":"xyz"}'), /abcdefghijkl|xyz/);
  const { seen, socket } = open();
  socket.server(OPEN_V3);
  await until(() => socket.sent.some(m => m.startsWith('40/v2/user')), 'the join');
  socket.server('40/v2/user,{"sid":"zzzzzzzzzzzzzzzz"}');
  const printed = seen.trace.join('\n');
  assert.match(printed, /→ 40\/v2\/user\?api_key=<redacted:20>/);
  for (const secret of [...secrets, 'zzzzzzzzzzzzzzzz']) assert.ok(!printed.includes(secret), `trace leaked ${secret.slice(0, 8)}`);
  socket.close();
});

test('the heartbeat monitor waits for a first beat, then fails when beats stop', async () => {
  let clock = 0;
  const failures = [];
  const monitor = heartbeatMonitor({ intervalMs: 7000, checkEveryMs: 5, now: () => clock, onFail: e => failures.push(e) });
  clock = 60000; await sleep(30);
  assert.deepEqual(failures, [], 'no enforcement before the first heartbeat');
  monitor.beat(); clock += 6900; await sleep(30);
  assert.deepEqual(failures, [], 'inside the 7 s window');
  monitor.beat(); clock += 7100; await sleep(30);
  assert.match(failures[0].message, /connection check failure: no heartbeat for 7s/);
  monitor.stop();
});
