'use strict';
// Link to a BRiSK market-data WebSocket.
//
// Two server dialects exist. The Next client talks plain binary frames over a native
// WebSocket. Upstream's SBI capture (pybrisk, 2026-03-11) shows SBI's web app wrapping
// the same frames in Engine.IO / Socket.IO: the server opens with an Engine.IO packet
// (`0{"sid":...,"pingInterval":...}`), the client joins the `/v2/user` namespace with a
// query and sends a `startLive` event, and only then do binary frames flow. The dialect
// is detected from the first message, so a plain server keeps working untouched.
//
// What is NOT public, and is therefore kept in the profile below rather than hard-coded:
// where each connect parameter comes from and what `startLive` carries. Run with
// `--trace-protocol` (secrets redacted) to see the server's side of a first attempt.

const MAX_TRACE = 300;

const ENGINE = { open: '0', close: '1', ping: '2', pong: '3', message: '4', noop: '6' };
const SOCKET = { connect: '0', disconnect: '1', event: '2', connectError: '4' };

const TOKEN = /v2\.local\.[A-Za-z0-9_\-.=]+/g;
const SECRET_PARAM = /\b(api_key|session|session_id|visitor_id|token|csrf_token|api_token|identity|sid)=(?!<redacted:)([^&\s",]+)/g;
const SECRET_FIELD = /("(?:sid|api_key|token|session|session_id|visitor_id|identity|api_token|csrf_token)"\s*:\s*")(?!<redacted:)([^"]+)(")/g;
const LONG_HEX = /\b[0-9a-f]{24,}\b/gi;

// Remove credentials from anything that is about to be printed.
function redact(text) {
  const mask = value => `<redacted:${value.length}>`;
  return String(text)
    .replace(TOKEN, mask)
    .replace(SECRET_PARAM, (_, key, value) => `${key}=${mask(value)}`)
    .replace(SECRET_FIELD, (_, open, value, close) => open + mask(value) + close)
    .replace(LONG_HEX, mask);
}

const SBI_PROFILE = Object.freeze({
  namespace: '/v2/user',
  // UNVERIFIED: upstream names these parameters but not their sources. `api_key` is
  // probably the market token; the rest are plausible, not confirmed.
  connectQuery: ctx => ({ api_key: ctx.marketToken, visitor_id: ctx.identity, session_id: ctx.wsSession,
    tabId: ctx.tabId, url: `${ctx.origin}/` }),
  // UNKNOWN: upstream elides the payload as `{...}`.
  startLive: () => ({}),
  // UNVERIFIED: Engine.IO 3 prefixes binary frames with a type byte; upstream decodes
  // frames from byte 0, so the default is no prefix.
  binaryPrefix: false,
});

const PROFILE_KEYS = new Set(['connectQuery', 'startLive', 'binaryPrefix', 'catchUp']);

// Overrides come from BRISK_SBI_PROFILE / --profile as JSON, so they are plain data.
function resolveProfile(overrides = {}) {
  if (overrides === null || typeof overrides !== 'object' || Array.isArray(overrides)) {
    throw new Error('SBI profile must be a JSON object');
  }
  const unknown = Object.keys(overrides).filter(key => !PROFILE_KEYS.has(key));
  if (unknown.length) throw new Error(`Unknown SBI profile keys: ${unknown.join(', ')}`);
  return {
    namespace: SBI_PROFILE.namespace,
    connectQuery: ctx => ({ ...SBI_PROFILE.connectQuery(ctx), ...overrides.connectQuery }),
    startLive: overrides.startLive === undefined ? SBI_PROFILE.startLive : () => overrides.startLive,
    binaryPrefix: overrides.binaryPrefix ?? SBI_PROFILE.binaryPrefix,
    catchUp: overrides.catchUp,
  };
}

class Link {
  // options: WebSocketImpl, url, headers, profile, context() -> Promise<object>, trace(line),
  //   onBinary(Buffer), onClose({code, reason}), onError(Error), connectTimeoutMs
  constructor(options) {
    this.o = { connectTimeoutMs: 15000, ...options };
    this.mode = 'detect';       // detect | raw | engineio
    this.state = 'connecting';  // engineio: connecting | namespace-requested | live
    this.version = null;
    this.finished = false;
    this.timers = new Set();
    const socket = this.socket = new this.o.WebSocketImpl(this.o.url, { headers: this.o.headers });
    socket.binaryType = 'arraybuffer';
    socket.addEventListener('message', event => this.guard(() => this.onMessage(event.data)));
    socket.addEventListener('error', event =>
      this.fail(new Error(`SBI BRiSK WebSocket error: ${event.message || 'connection failed'}`)));
    socket.addEventListener('close', event => {
      if (this.finished) return;
      this.finish();
      this.o.onClose({ code: event.code, reason: event.reason || '' });
    });
  }

  trace(direction, text) {
    if (!this.o.trace) return;
    const line = redact(text);
    this.o.trace(`${direction} ${line.length > MAX_TRACE ? `${line.slice(0, MAX_TRACE)}...(${line.length} chars)` : line}`);
  }

  guard(fn) {
    try { fn(); } catch (error) { this.fail(error); }
  }

  // Timers stay referenced on purpose: a live session in progress must keep the process
  // alive, and finish() clears every one of them.
  timer(fn, ms, repeat = false) {
    const handle = (repeat ? setInterval : setTimeout)(() => this.guard(fn), ms);
    this.timers.add(handle);
    return handle;
  }

  clear(handle) {
    clearTimeout(handle); clearInterval(handle);
    this.timers.delete(handle);
  }

  finish() {
    this.finished = true;
    for (const handle of this.timers) { clearTimeout(handle); clearInterval(handle); }
    this.timers.clear();
  }

  fail(error) {
    if (this.finished) return;
    this.finish();
    try { this.socket.close(); } catch { /* already closing */ }
    this.o.onError(error);
  }

  // Bytes for the server: the WASM's own pings travel through here as raw binary frames.
  send(bytes) {
    this.trace('→', `binary ${bytes.length} bytes ${Buffer.from(bytes.subarray(0, 12)).toString('hex')}`);
    this.socket.send(bytes);
  }

  sendText(text) {
    this.trace('→', text);
    this.socket.send(text);
  }

  close(code = 1000) {
    try { this.socket.close(code); } catch { /* already closed */ }
  }

  onMessage(data) {
    if (this.finished) return;
    if (typeof data === 'string') return this.onText(data);
    if (this.mode === 'detect') this.mode = 'raw';
    let bytes = Buffer.from(data);
    this.trace('←', `binary ${bytes.length} bytes ${bytes.subarray(0, 12).toString('hex')}`);
    if (this.mode === 'engineio' && this.o.profile.binaryPrefix) {
      if (bytes[0] !== 4) throw new Error(`Engine.IO binary frame without the message type byte (got 0x${bytes[0]?.toString(16)})`);
      bytes = bytes.subarray(1);
    }
    this.o.onBinary(bytes);
  }

  onText(text) {
    this.trace('←', text);
    if (this.mode === 'detect') {
      // A plain server's text is control chatter that carries no market data.
      if (!/^0\{/.test(text)) { this.mode = 'raw'; return; }
      this.mode = 'engineio';
    }
    if (this.mode === 'raw') return;
    const body = text.slice(1);
    switch (text[0]) {
      case ENGINE.open: this.open(body); break;
      case ENGINE.close: this.fail(new Error('Engine.IO: the server closed the session')); break;
      case ENGINE.ping:
        this.sendText(ENGINE.pong + body);
        if (this.version === 4) this.watch();
        break;
      case ENGINE.pong:
        if (this.pongTimer) { this.clear(this.pongTimer); this.pongTimer = null; }
        break;
      case ENGINE.message: this.socketIo(body); break;
      case ENGINE.noop: break;
      default: throw new Error(`Unknown Engine.IO packet type ${JSON.stringify(text[0])}`);
    }
  }

  // Engine.IO 3 clients ping; Engine.IO 4 servers ping. The open packet tells them apart
  // (version 4 adds maxPayload).
  open(body) {
    let info;
    try { info = JSON.parse(body); } catch { throw new Error('Malformed Engine.IO open packet'); }
    this.version = info.maxPayload === undefined ? 3 : 4;
    this.pingInterval = Number(info.pingInterval) || 25000;
    this.pingTimeout = Number(info.pingTimeout) || 5000;
    if (this.version === 3) {
      this.timer(() => {
        this.sendText(ENGINE.ping);
        this.pongTimer = this.timer(() => this.fail(new Error('Engine.IO ping timeout')), this.pingTimeout);
      }, this.pingInterval, true);
    } else {
      this.watch();
    }
    this.o.context().then(ctx => this.guard(() => this.joinNamespace(ctx)), error => this.fail(error));
  }

  watch() {
    if (this.watchdog) this.clear(this.watchdog);
    this.watchdog = this.timer(() => this.fail(new Error('Engine.IO: the server stopped pinging')),
      this.pingInterval + this.pingTimeout);
  }

  joinNamespace(ctx) {
    if (this.finished) return;
    this.ctx = ctx;
    const { namespace, connectQuery } = this.o.profile;
    const query = Object.entries(connectQuery(ctx)).filter(([, value]) => value !== undefined && value !== null)
      .map(([key, value]) => `${encodeURIComponent(key)}=${encodeURIComponent(value)}`).join('&');
    this.state = 'namespace-requested';
    this.sendText(`${ENGINE.message}${SOCKET.connect}${namespace}${query ? `?${query}` : ''}`);
    this.connectTimer = this.timer(() => this.fail(new Error(
      `Socket.IO namespace ${namespace} was not acknowledged within ${this.o.connectTimeoutMs / 1000}s`)),
    this.o.connectTimeoutMs);
  }

  socketIo(body) {
    const type = body[0];
    let rest = body.slice(1);
    let nsp = '/';
    if (rest[0] === '/') {
      const comma = rest.indexOf(',');
      nsp = comma < 0 ? rest : rest.slice(0, comma);
      rest = comma < 0 ? '' : rest.slice(comma + 1);
    }
    const name = nsp.split('?')[0];
    const ours = name === this.o.profile.namespace;
    switch (type) {
      case SOCKET.connect:
        if (ours && this.state === 'namespace-requested') this.startLive();
        break;
      case SOCKET.disconnect:
        if (ours) this.fail(new Error(`Socket.IO namespace ${name} was closed by the server`));
        break;
      case SOCKET.connectError:
        this.fail(new Error(`Socket.IO connect error on ${name}: ${redact(rest) || '(no detail)'}`));
        break;
      default: break; // events and acknowledgements carry nothing the host needs
    }
  }

  startLive() {
    this.clear(this.connectTimer);
    this.state = 'live';
    const { namespace, startLive } = this.o.profile;
    this.sendText(`${ENGINE.message}${SOCKET.event}${namespace},${JSON.stringify(
      ['userEvent', { name: 'startLive', data: startLive(this.ctx) }])}`);
  }
}

// The vendor client closes the connection when its last heartbeat is older than 7 s, but
// only once it has seen one. Beats come from the WASM's heartbeat callback.
function heartbeatMonitor({ intervalMs = 7000, checkEveryMs = 1000, onFail, now = () => performance.now() }) {
  let last = null;
  const timer = setInterval(() => {
    if (last !== null && now() - last > intervalMs) {
      onFail(new Error(`SBI BRiSK connection check failure: no heartbeat for ${Math.round((now() - last) / 1000)}s`));
    }
  }, checkEveryMs);
  return { beat() { last = now(); }, stop() { clearInterval(timer); } };
}

module.exports = { Link, SBI_PROFILE, resolveProfile, heartbeatMonitor, redact };
