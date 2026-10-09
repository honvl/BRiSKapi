'use strict';
// Passkey sign-in to a BRiSK site (SBI, Matsui, Monex, SMBC Nikko, ...) through Chrome's virtual
// authenticator.
//
// A Chrome that this host starts and drives over a private DevTools *pipe* (never a TCP debugging
// port, which any local process could attach to) carries a virtual authenticator holding the user's
// passkey. The site's own passkey ceremony is answered without a touch, and the BRiSK session cookies
// are handed back. It knows nothing about any particular site: the login URL, the passkey control's
// text and the BRiSK cookie host all arrive in the request. Run it through `brisk enroll` /
// `brisk login`, not by hand: the request arrives on stdin and the results (including the passkey's
// private key) leave on stdout as JSON lines. Nothing secret is ever put on a command line or in a
// log message.
//
//   request (first stdin line, JSON): {"mode": "enroll"|"login", "credential": {...}, ...options}
//   stdout, one JSON object per line:
//     {"type":"credential","credential":{...}}   the passkey after every change (sign counter!)
//     {"type":"registered"}                      enroll: the site's registration captured a passkey
//     {"type":"result","ok":true,"cookies":{...}} / {"type":"result","ok":false,"error":"..."}
//   enroll then waits for "done" on stdin (or the window to close) before closing Chrome.
const { spawn } = require('node:child_process');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const readline = require('node:readline');

class PasskeyError extends Error {}

// Only neutral defaults: which site to sign in to is always the caller's choice.
const DEFAULTS = {
  loginUrl: null,
  launchUrl: null,
  cookieHost: null,
  cookiePrefix: '',
  passkeyButton: 'パスキー',
  headless: false,
  profileDir: null,
  chrome: null,
  assertTimeoutMs: 120_000,
  cookieTimeoutMs: 180_000,
  enrollTimeoutMs: 900_000,
  clickWaitMs: 20_000,
};

// A platform authenticator that approves presence and verification by itself.
const AUTHENTICATOR = {
  protocol: 'ctap2', ctap2Version: 'ctap2_1', transport: 'internal', hasResidentKey: true,
  hasUserVerification: true, isUserVerified: true, automaticPresenceSimulation: true,
};

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const log = (message) => process.stderr.write(`brisk passkey: ${message}\n`);

// Only the origin of a URL is ever logged: paths and queries can carry tokens.
function originOf(url) {
  try { return new URL(url).origin; } catch { return 'an invalid URL'; }
}

function resolveOptions(request, mode) {
  const options = { ...DEFAULTS };
  for (const key of Object.keys(DEFAULTS)) {
    if (request[key] !== undefined && request[key] !== null) options[key] = request[key];
  }
  if (!options.loginUrl) throw new PasskeyError('loginUrl is required');
  if (mode === 'login' && !options.cookieHost) throw new PasskeyError('cookieHost is required');
  if (options.cookieHost !== null && !/^[a-z0-9.-]+$/i.test(options.cookieHost)) throw new PasskeyError('cookieHost must be a host name');
  for (const key of ['loginUrl', 'launchUrl']) {
    if (options[key] === null) continue;
    let url;
    try { url = new URL(options[key]); } catch { throw new PasskeyError(`${key} is not a valid URL`); }
    if (url.protocol !== 'https:' && url.protocol !== 'http:') throw new PasskeyError(`${key} must be an http(s) URL`);
  }
  for (const key of ['assertTimeoutMs', 'cookieTimeoutMs', 'enrollTimeoutMs', 'clickWaitMs']) {
    if (!Number.isFinite(options[key]) || options[key] <= 0) throw new PasskeyError(`${key} must be a positive number`);
  }
  return options;
}

function validateCredential(credential) {
  const fields = ['credentialId', 'privateKey', 'rpId'];
  if (!credential || typeof credential !== 'object' || fields.some((f) => typeof credential[f] !== 'string' || !credential[f])
      || credential.isResidentCredential !== true || !Number.isInteger(credential.signCount)) {
    throw new PasskeyError('The stored passkey is not valid; run `brisk enroll --replace` again');
  }
}

const MAC_CHROME = ['/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
  '/Applications/Chromium.app/Contents/MacOS/Chromium'];
const LINUX_CHROME = ['google-chrome', 'google-chrome-stable', 'chromium', 'chromium-browser'];
// Chrome installs per machine under either Program Files, or per user under LOCALAPPDATA.
const WINDOWS_CHROME_ROOTS = ['ProgramFiles', 'ProgramFiles(x86)', 'LOCALAPPDATA'];

// Paths follow the platform being asked about, not the one running, so each branch behaves the same anywhere.
function findChrome({ env = process.env, platform = process.platform, exists = fs.existsSync } = {}) {
  if (env.BRISK_CHROME) {
    if (!exists(env.BRISK_CHROME)) throw new PasskeyError(`BRISK_CHROME does not exist: ${env.BRISK_CHROME}`);
    return env.BRISK_CHROME;
  }
  let found;
  if (platform === 'darwin') {
    found = [...MAC_CHROME, path.posix.join(env.HOME || os.homedir(), MAC_CHROME[0].slice(1))].find((p) => exists(p));
  } else if (platform === 'linux') {
    for (const name of LINUX_CHROME) {
      found = (env.PATH || '').split(':').filter(Boolean).map((d) => path.posix.join(d, name)).find((p) => exists(p));
      if (found) break;
    }
  } else if (platform === 'win32') {
    found = WINDOWS_CHROME_ROOTS.map((name) => env[name]).filter(Boolean)
      .map((root) => path.win32.join(root, 'Google', 'Chrome', 'Application', 'chrome.exe')).find((p) => exists(p));
  } else {
    throw new PasskeyError(`Passkey login supports macOS, Linux and Windows, not ${platform}`);
  }
  if (!found) throw new PasskeyError('Google Chrome (or Chromium) was not found; install it or set BRISK_CHROME to its executable');
  return found;
}

// The DevTools protocol over Chrome's --remote-debugging-pipe: JSON messages, each ending in a NUL byte.
class Cdp {
  constructor(writable, readable) {
    this.writable = writable;
    this.nextId = 0;
    this.pending = new Map();
    this.handlers = new Map();
    this.waiters = new Set();
    this.buffer = '';
    this.closed = false;
    readable.setEncoding('utf8');
    readable.on('data', (chunk) => this.#feed(chunk));
    readable.on('close', () => this.#fail(new PasskeyError('Chrome closed the debugging pipe')));
    readable.on('error', () => this.#fail(new PasskeyError('The debugging pipe to Chrome failed')));
    writable.on('error', () => this.#fail(new PasskeyError('The debugging pipe to Chrome failed')));
  }

  #feed(chunk) {
    this.buffer += chunk;
    for (let end = this.buffer.indexOf('\0'); end >= 0; end = this.buffer.indexOf('\0')) {
      const raw = this.buffer.slice(0, end);
      this.buffer = this.buffer.slice(end + 1);
      if (!raw) continue;
      let message;
      try { message = JSON.parse(raw); } catch { continue; }
      this.#dispatch(message);
    }
  }

  #dispatch(message) {
    if (message.id !== undefined) {
      const entry = this.pending.get(message.id);
      if (!entry) return;
      this.pending.delete(message.id);
      if (message.error) entry.reject(new PasskeyError(`${entry.method}: ${message.error.message}`));
      else entry.resolve(message.result || {});
      return;
    }
    for (const handler of this.handlers.get(message.method) || []) handler(message.params || {}, message.sessionId);
  }

  #fail(error) {
    if (this.closed) return;
    this.closed = true;
    for (const entry of this.pending.values()) entry.reject(error);
    this.pending.clear();
    for (const waiter of this.waiters) waiter(error);
    this.waiters.clear();
  }

  send(method, params = {}, sessionId) {
    if (this.closed) return Promise.reject(new PasskeyError('Chrome closed the debugging pipe'));
    const id = ++this.nextId;
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject, method });
      const message = { id, method, params };
      if (sessionId) message.sessionId = sessionId;
      this.writable.write(`${JSON.stringify(message)}\0`);
    });
  }

  on(method, handler) {
    if (!this.handlers.has(method)) this.handlers.set(method, new Set());
    this.handlers.get(method).add(handler);
    return () => this.handlers.get(method).delete(handler);
  }

  // Resolves with the params of the next matching event; rejects on timeout or when Chrome goes away.
  waitFor(method, predicate = () => true, timeoutMs = 30_000) {
    return new Promise((resolve, reject) => {
      let off;
      const finish = (result) => {
        clearTimeout(timer);
        off();
        this.waiters.delete(finish);
        if (result instanceof Error) reject(result); else resolve(result);
      };
      const timer = setTimeout(() => finish(new PasskeyError(`Timed out waiting for ${method}`)), timeoutMs);
      off = this.on(method, (params, sessionId) => { if (predicate(params, sessionId)) finish(params); });
      if (this.closed) finish(new PasskeyError('Chrome closed the debugging pipe'));
      else this.waiters.add(finish);
    });
  }
}

const PROFILE_PREFIX = 'brisk-passkey-';
const OWNER_FILE = 'brisk-owner.pid';

function processAlive(pid) {
  try { process.kill(pid, 0); return true; } catch (error) { return error.code === 'EPERM'; }
}

// A force-killed helper cannot delete its temporary profile, which holds the broker's logged-in session.
// Each profile records its owner, so the next start removes the ones whose owner is gone.
function sweepStaleProfiles({ tmp = os.tmpdir(), now = Date.now(), alive = processAlive } = {}) {
  let names;
  try { names = fs.readdirSync(tmp); } catch { return []; }
  const removed = [];
  for (const name of names.filter((n) => n.startsWith(PROFILE_PREFIX))) {
    const dir = path.join(tmp, name);
    let pid = NaN;
    try { pid = Number(fs.readFileSync(path.join(dir, OWNER_FILE), 'utf8')); } catch { /* no owner recorded yet */ }
    if (Number.isInteger(pid) && pid > 0) {
      if (alive(pid)) continue;
    } else {
      // No owner file: a profile still being created by a starting helper. Leave it a minute.
      try { if (now - fs.statSync(dir).mtimeMs < 60_000) continue; } catch { continue; }
    }
    try { fs.rmSync(dir, { recursive: true, force: true, maxRetries: 8, retryDelay: 200 }); removed.push(name); } catch { /* locked: next time */ }
  }
  return removed;
}

async function launchChrome({ chrome, profileDir, headless, spawnImpl = spawn }) {
  const ephemeral = !profileDir;
  if (ephemeral) sweepStaleProfiles();
  const dir = ephemeral ? fs.mkdtempSync(path.join(os.tmpdir(), PROFILE_PREFIX)) : profileDir;
  fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
  if (ephemeral) fs.writeFileSync(path.join(dir, OWNER_FILE), String(process.pid));
  const args = ['--remote-debugging-pipe', `--user-data-dir=${dir}`, '--no-first-run', '--no-default-browser-check',
    '--disable-sync', '--password-store=basic', '--use-mock-keychain'];
  if (headless) args.push('--headless=new');
  if (typeof process.getuid === 'function' && process.getuid() === 0) args.push('--no-sandbox');
  args.push('about:blank');
  const child = spawnImpl(chrome, args, { stdio: ['ignore', 'ignore', 'ignore', 'pipe', 'pipe'] });
  const exited = new Promise((resolve) => child.once('exit', resolve));
  // Windows can keep a file in the profile locked for a moment after Chrome exits, so deleting retries.
  const discard = () => { if (ephemeral) fs.rmSync(dir, { recursive: true, force: true, maxRetries: 20, retryDelay: 250 }); };
  try {
    await new Promise((resolve, reject) => { child.once('spawn', resolve); child.once('error', reject); });
  } catch (error) {
    discard();
    throw new PasskeyError(`Could not start Chrome (${error.code || error.message})`);
  }
  const cdp = new Cdp(child.stdio[3], child.stdio[4]);
  const close = async () => {
    if (child.exitCode === null && child.signalCode === null) {
      await Promise.race([cdp.send('Browser.close').catch(() => {}), sleep(3000)]);
      const timer = setTimeout(() => child.kill('SIGKILL'), 5000);
      await exited;
      clearTimeout(timer);
    }
    discard();
  };
  return { cdp, close, exited };
}

// Cookies of exactly one host (a leading dot is how Chrome writes domain cookies); nothing else is returned.
function pickCookies(cookies, host) {
  const mine = {};
  for (const cookie of cookies) {
    if (String(cookie.domain).replace(/^\./, '').toLowerCase() === host.toLowerCase()) mine[cookie.name] = cookie.value;
  }
  return mine;
}

const FIND_BUTTON = (text) => `(() => {
  const want = ${JSON.stringify(text)};
  const label = (e) => (e.innerText || e.value || e.getAttribute('aria-label') || '').replace(/\\s+/g, ' ').trim();
  const visible = (e) => { const r = e.getBoundingClientRect(); const s = getComputedStyle(e);
    return r.width > 0 && r.height > 0 && s.visibility !== 'hidden' && s.display !== 'none'; };
  const hits = [...document.querySelectorAll('button, a, input[type=button], input[type=submit], [role=button]')]
    .filter((e) => visible(e) && label(e).includes(want)).sort((a, b) => label(a).length - label(b).length);
  if (!hits.length) return null;
  hits[0].scrollIntoView({ block: 'center', inline: 'center' });
  const r = hits[0].getBoundingClientRect();
  return { x: r.left + r.width / 2, y: r.top + r.height / 2 };
})()`;

// One Chrome with a virtual authenticator on every page it opens (popups and new tabs included).
class Browser {
  constructor({ cdp, credential = null, onCredential = () => {} }) {
    this.cdp = cdp;
    this.credential = credential;
    this.onCredential = onCredential;
    this.configured = new Set();
    this.authenticators = new Map();
    this.primary = null;
    this.latest = credential;
    this.stopped = false;
  }

  async start() {
    const { cdp } = this;
    cdp.on('Target.attachedToTarget', (params) => { this.#attached(params).catch(() => {}); });
    for (const event of ['Added', 'Asserted', 'Updated']) {
      cdp.on(`WebAuthn.credential${event}`, (params) => this.#changed(params.credential));
    }
    await cdp.send('Target.setAutoAttach', { autoAttach: true, waitForDebuggerOnStart: true, flatten: true,
      filter: [{ type: 'page', exclude: false }, { exclude: true }] });
    for (let attempt = 0; attempt < 75 && !this.primary; attempt++) {
      const { targetInfos } = await cdp.send('Target.getTargets');
      for (const info of targetInfos.filter((t) => t.type === 'page' && !this.configured.has(t.targetId))) {
        const { sessionId } = await cdp.send('Target.attachToTarget', { targetId: info.targetId, flatten: true });
        await this.#configure(info.targetId, sessionId);
      }
      if (!this.primary) await sleep(200);
    }
    if (!this.primary) throw new PasskeyError('Chrome did not open a page');
  }

  async #attached({ sessionId, targetInfo, waitingForDebugger }) {
    try {
      if (targetInfo.type === 'page') await this.#configure(targetInfo.targetId, sessionId);
    } finally {
      if (waitingForDebugger) await this.cdp.send('Runtime.runIfWaitingForDebugger', {}, sessionId).catch(() => {});
    }
  }

  async #configure(targetId, sessionId) {
    if (this.configured.has(targetId)) return;
    this.configured.add(targetId);
    const { cdp } = this;
    await cdp.send('Page.enable', {}, sessionId);
    await cdp.send('WebAuthn.enable', {}, sessionId);
    const { authenticatorId } = await cdp.send('WebAuthn.addVirtualAuthenticator', { options: AUTHENTICATOR }, sessionId);
    if (this.credential) await cdp.send('WebAuthn.addCredential', { authenticatorId, credential: this.credential }, sessionId);
    this.authenticators.set(sessionId, authenticatorId);
    if (!this.primary) this.primary = sessionId;
  }

  #changed(credential) {
    if (!credential || typeof credential.credentialId !== 'string') return;
    this.latest = credential;
    this.onCredential(credential);
  }

  // The authenticator's own record, which is authoritative for the sign counter.
  async refresh() {
    for (const [sessionId, authenticatorId] of this.authenticators) {
      const { credentials } = await this.cdp.send('WebAuthn.getCredentials', { authenticatorId }, sessionId).catch(() => ({}));
      const found = (credentials || []).find((c) => !this.latest || c.credentialId === this.latest.credentialId);
      if (found && (!this.latest || found.signCount >= this.latest.signCount)) this.#changed(found);
    }
    return this.latest;
  }

  async navigate(url) {
    const loaded = this.cdp.waitFor('Page.loadEventFired', (_, sessionId) => sessionId === this.primary, 30_000).catch(() => null);
    const result = await this.cdp.send('Page.navigate', { url }, this.primary);
    if (result.errorText) throw new PasskeyError(`Could not open ${originOf(url)}: ${result.errorText}`);
    await loaded;
  }

  async #evaluate(expression) {
    try {
      const { result, exceptionDetails } = await this.cdp.send('Runtime.evaluate', { expression, returnByValue: true }, this.primary);
      return exceptionDetails ? null : result.value;
    } catch { return null; }
  }

  // Click the first visible control whose text contains `text`, with real (trusted) mouse events.
  async clickWhenPresent(text, timeoutMs) {
    const deadline = Date.now() + timeoutMs;
    while (!this.stopped && Date.now() < deadline) {
      const spot = await this.#evaluate(FIND_BUTTON(text));
      if (spot) {
        try {
          for (const event of [{ type: 'mouseMoved' }, { type: 'mousePressed', button: 'left', clickCount: 1 },
            { type: 'mouseReleased', button: 'left', clickCount: 1 }]) {
            await this.cdp.send('Input.dispatchMouseEvent', { ...event, x: spot.x, y: spot.y }, this.primary);
          }
        } catch { return false; }
        return true;
      }
      await sleep(400);
    }
    return false;
  }

  async cookies(host) {
    const { cookies } = await this.cdp.send('Storage.getCookies');
    return pickCookies(cookies, host);
  }

  async waitForCookies({ host, prefix, timeoutMs }) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      let mine = await this.cookies(host);
      if (Object.keys(mine).some((name) => name.startsWith(prefix))) {
        await sleep(500); // The other cookies of the same response arrive with it.
        mine = await this.cookies(host);
        return mine;
      }
      await sleep(500);
    }
    return null;
  }
}

async function withBrowser(options, credential, ctx, work) {
  const chrome = options.chrome || ctx.findChrome();
  const handle = await ctx.launch({ chrome, profileDir: options.profileDir, headless: options.headless });
  ctx.setCleanup(handle.close);
  const browser = new Browser({ cdp: handle.cdp, credential, onCredential: (c) => ctx.emit({ type: 'credential', credential: c }) });
  try {
    await browser.start();
    return await work(browser, handle);
  } finally {
    browser.stopped = true;
    // Whatever happened, the latest sign counter must reach the caller before Chrome goes away.
    await browser.refresh().catch(() => {});
    await handle.close();
    ctx.setCleanup(null);
  }
}

async function login(request, ctx) {
  const options = resolveOptions(request, 'login');
  validateCredential(request.credential);
  return withBrowser(options, request.credential, ctx, async (browser) => {
    log(`Chrome started; signing in at ${originOf(options.loginUrl)}`);
    const asserted = browser.cdp.waitFor('WebAuthn.credentialAsserted', () => true, options.assertTimeoutMs);
    asserted.catch(() => {});
    await browser.navigate(options.loginUrl);
    browser.clickWhenPresent(options.passkeyButton, options.clickWaitMs).then((clicked) => {
      if (!clicked && !browser.stopped) {
        log(options.headless
          ? `No "${options.passkeyButton}" control found on the login page`
          : `No "${options.passkeyButton}" control found; click the passkey login in the Chrome window`);
      }
    }, () => {});
    try { await asserted; } catch (error) {
      if (error.message.startsWith('Timed out')) throw new PasskeyError('The site never asked for the passkey; the passkey button text or the login URL may need changing');
      throw error;
    }
    log('The site asked for the passkey and it was signed');
    if (options.launchUrl) {
      // Let the login response finish loading first: leaving now could abort the sign-in request.
      await browser.cdp.waitFor('Page.loadEventFired', (_, s) => s === browser.primary, 10_000).catch(() => null);
      log(`Opening BRiSK at ${originOf(options.launchUrl)}`);
      await browser.navigate(options.launchUrl);
    } else if (!options.headless) {
      log('Open BRiSK from the broker\'s site in the Chrome window');
    }
    const cookies = await browser.waitForCookies({ host: options.cookieHost, prefix: options.cookiePrefix, timeoutMs: options.cookieTimeoutMs });
    if (!cookies) {
      throw new PasskeyError(`No ${options.cookieHost} session cookie appeared. The login may have been refused`
        + `${options.launchUrl ? '' : ', or BRiSK was not opened (give a launch URL)'}`);
    }
    return { cookies };
  });
}

async function enroll(request, ctx) {
  const options = resolveOptions(request, 'enroll');
  return withBrowser(options, null, ctx, async (browser, handle) => {
    log(`Chrome started at ${originOf(options.loginUrl)}. Sign in, then register a passkey in the site's security settings`);
    const added = browser.cdp.waitFor('WebAuthn.credentialAdded', () => true, options.enrollTimeoutMs);
    added.catch(() => {});
    await browser.navigate(options.loginUrl);
    const { credential } = await added;
    ctx.emit({ type: 'registered' });
    log(`A passkey for ${credential.rpId} was captured. Finish the registration on the site, then confirm`);
    await Promise.race([ctx.done, handle.exited]);
    return {};
  });
}

async function main(argv = process.argv.slice(2), env = {}) {
  const emit = (message) => process.stdout.write(`${JSON.stringify(message)}\n`);
  let finished = false;
  const finish = (result) => { finished = true; emit({ type: 'result', ...result }); return result.ok ? 0 : 1; };
  if (process.stdout.isTTY || process.stdin.isTTY) {
    process.stderr.write('brisk passkey: run this through `brisk enroll` or `brisk login`; it prints credentials\n');
    return 2;
  }
  const lines = readline.createInterface({ input: process.stdin });
  const iterator = lines[Symbol.asyncIterator]();
  const first = await iterator.next();
  let done = null;
  const doneWait = new Promise((resolve) => { done = resolve; });
  let cleanup = null;
  const stop = async (code) => { try { if (cleanup) await cleanup(); } finally { process.exit(code); } };
  // After the request, stdin carries one control word, "done" (enroll: the registration is complete).
  // stdin closing without it means the parent is gone or is stopping us: close Chrome and exit. This
  // is the stop signal that works everywhere; Windows has no SIGTERM, only an abrupt TerminateProcess.
  (async () => {
    let confirmed = false;
    for await (const line of iterator) if (line.trim() === 'done') { confirmed = true; break; }
    if (confirmed) done();
    else if (!first.done && !finished) stop(143);
  })();
  process.once('SIGINT', () => stop(130));
  process.once('SIGTERM', () => stop(143));
  const ctx = { emit, launch: env.launch || launchChrome, findChrome: env.findChrome || findChrome, done: doneWait,
    setCleanup: (fn) => { cleanup = fn; } };
  try {
    if (first.done) throw new PasskeyError('No request on stdin');
    let request;
    try { request = JSON.parse(first.value); } catch { throw new PasskeyError('The request on stdin is not JSON'); }
    const mode = argv[0] || request.mode;
    if (mode !== 'enroll' && mode !== 'login') throw new PasskeyError('Usage: passkey.cjs enroll|login');
    const result = await (mode === 'login' ? login : enroll)(request, ctx);
    return finish({ ok: true, ...result });
  } catch (error) {
    return finish({ ok: false, error: error instanceof PasskeyError ? error.message : `Unexpected failure: ${error.message}` });
  }
}

module.exports = { PasskeyError, Cdp, DEFAULTS, AUTHENTICATOR, findChrome, launchChrome, sweepStaleProfiles, pickCookies, originOf,
  resolveOptions, validateCredential, Browser, login, enroll, main };

if (require.main === module) {
  main().then((code) => process.exit(code), (error) => { process.stderr.write(`brisk passkey: ${error.message}\n`); process.exit(1); });
}
