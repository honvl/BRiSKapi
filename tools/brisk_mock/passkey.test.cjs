'use strict';
// Passkey login host: unit tests for the DevTools pipe, Chrome discovery and cookie scoping, and
// end-to-end runs in a real Chrome against a fake SBI that verifies WebAuthn the way a real site does
// (ES256 signatures, user verification, and a sign counter that must increase). No SBI account needed.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const readline = require('node:readline');
const { PassThrough } = require('node:stream');
const { spawn, execFileSync } = require('node:child_process');
const { Cdp, findChrome, pickCookies, originOf, resolveOptions, validateCredential, DEFAULTS, PasskeyError } = require('../../briskapi/decoder/passkey.cjs');

const HELPER = path.join(__dirname, '..', '..', 'briskapi', 'decoder', 'passkey.cjs');

// ---- unit tests (no Chrome) ----

function pipePair() {
  const toChrome = new PassThrough();
  const fromChrome = new PassThrough();
  return { cdp: new Cdp(toChrome, fromChrome), toChrome, fromChrome };
}
const frame = (message) => `${JSON.stringify(message)}\0`;

test('the pipe sends NUL-terminated JSON and matches replies to requests', async () => {
  const { cdp, toChrome, fromChrome } = pipePair();
  const reply = cdp.send('Target.getTargets', { a: 1 }, 'sess');
  const sent = JSON.parse(toChrome.read().toString().replace(/\0$/, ''));
  assert.deepEqual(sent, { id: 1, method: 'Target.getTargets', params: { a: 1 }, sessionId: 'sess' });
  fromChrome.write(frame({ id: 1, result: { ok: true } }));
  assert.deepEqual(await reply, { ok: true });
});

test('replies split across chunks, with multibyte text cut in half, are reassembled', async () => {
  const { cdp, fromChrome } = pipePair();
  const reply = cdp.send('Runtime.evaluate');
  const bytes = Buffer.from(frame({ id: 1, result: { text: 'パスキー認証' } }));
  const cut = bytes.indexOf(Buffer.from('キ')) + 1; // inside a three-byte character
  fromChrome.write(bytes.subarray(0, cut));
  fromChrome.write(bytes.subarray(cut));
  assert.deepEqual(await reply, { text: 'パスキー認証' });
});

test('protocol errors name the method and events reach their handlers with the session', async () => {
  const { cdp, fromChrome } = pipePair();
  const failed = cdp.send('Page.navigate');
  fromChrome.write(frame({ id: 1, error: { code: -32000, message: 'No such target' } }));
  await assert.rejects(failed, /Page\.navigate: No such target/);
  const seen = [];
  cdp.on('WebAuthn.credentialAsserted', (params, sessionId) => seen.push([params.n, sessionId]));
  fromChrome.write(frame({ method: 'WebAuthn.credentialAsserted', params: { n: 1 }, sessionId: 's1' })
    + frame({ method: 'Other.event', params: {} }) + 'not json\0');
  await new Promise((resolve) => setImmediate(resolve));
  assert.deepEqual(seen, [[1, 's1']]);
});

test('waitFor resolves on a matching event, times out otherwise, and fails when Chrome goes away', async () => {
  const { cdp, fromChrome } = pipePair();
  const wanted = cdp.waitFor('X.event', (params) => params.keep, 1000);
  fromChrome.write(frame({ method: 'X.event', params: { keep: false } }) + frame({ method: 'X.event', params: { keep: true, n: 7 } }));
  assert.equal((await wanted).n, 7);
  await assert.rejects(cdp.waitFor('X.never', () => true, 30), /Timed out waiting for X\.never/);
  const pending = cdp.send('Slow.command');
  const waiting = cdp.waitFor('X.later', () => true, 5000);
  fromChrome.end();
  await assert.rejects(pending, /closed the debugging pipe/);
  await assert.rejects(waiting, /closed the debugging pipe/);
  await assert.rejects(cdp.send('After.close'), /closed the debugging pipe/);
  await assert.rejects(cdp.waitFor('X.after', () => true, 30), /closed the debugging pipe/);
});

test('Chrome is found by override, by the usual macOS paths, or on the Linux PATH', () => {
  const only = (...present) => (p) => present.includes(p);
  assert.equal(findChrome({ env: { BRISK_CHROME: '/x/chrome' }, platform: 'darwin', exists: only('/x/chrome') }), '/x/chrome');
  assert.throws(() => findChrome({ env: { BRISK_CHROME: '/x/missing' }, platform: 'darwin', exists: only() }), /BRISK_CHROME does not exist/);
  const mac = '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome';
  assert.equal(findChrome({ env: {}, platform: 'darwin', exists: only(mac) }), mac);
  assert.equal(findChrome({ env: { HOME: '/Users/u' }, platform: 'darwin', exists: only(`/Users/u${mac}`) }), `/Users/u${mac}`);
  assert.equal(findChrome({ env: { PATH: '/a:/b' }, platform: 'linux', exists: only('/b/chromium') }), '/b/chromium');
  assert.throws(() => findChrome({ env: { PATH: '/a' }, platform: 'linux', exists: only() }), /Chrome \(or Chromium\) was not found.*BRISK_CHROME/);
  assert.throws(() => findChrome({ env: {}, platform: 'win32', exists: only() }), /macOS and Linux, not win32/);
});

test('only cookies of exactly the BRiSK host are returned', () => {
  const cookies = [
    { name: 'session_a', value: '1', domain: 'sbi.brisk.jp' },
    { name: 'dotted', value: '2', domain: '.sbi.brisk.jp' },
    { name: 'main', value: '3', domain: '.sbisec.co.jp' },
    { name: 'parent', value: '4', domain: '.brisk.jp' },
    { name: 'lookalike', value: '5', domain: 'evil-sbi.brisk.jp' },
    { name: 'suffix', value: '6', domain: 'sbi.brisk.jp.evil.example' },
  ];
  assert.deepEqual(pickCookies(cookies, 'sbi.brisk.jp'), { session_a: '1', dotted: '2' });
  assert.deepEqual(pickCookies([], 'sbi.brisk.jp'), {});
});

test('options take defaults, accept overrides and refuse bad values', () => {
  assert.deepEqual(resolveOptions({}), DEFAULTS);
  const set = resolveOptions({ loginUrl: 'http://main.localhost:1/', launchUrl: null, headless: true, cookieTimeoutMs: 5 });
  assert.equal(set.loginUrl, 'http://main.localhost:1/');
  assert.equal(set.launchUrl, null);
  assert.equal(set.headless, true);
  assert.equal(set.cookieHost, 'sbi.brisk.jp');
  assert.throws(() => resolveOptions({ loginUrl: 'nope' }), /loginUrl is not a valid URL/);
  assert.throws(() => resolveOptions({ launchUrl: 'file:///etc/passwd' }), /launchUrl must be an http\(s\) URL/);
  assert.throws(() => resolveOptions({ assertTimeoutMs: 0 }), /assertTimeoutMs must be a positive number/);
  assert.equal(originOf('https://sbi.brisk.jp/path?token=secret#x'), 'https://sbi.brisk.jp');
  assert.equal(originOf('nope'), 'an invalid URL');
});

test('a stored passkey that is not a resident credential is refused before Chrome starts', () => {
  const good = { credentialId: 'a', privateKey: 'k', rpId: 'r', isResidentCredential: true, signCount: 1 };
  validateCredential(good);
  for (const bad of [null, {}, { ...good, privateKey: '' }, { ...good, isResidentCredential: false }, { ...good, signCount: '1' }]) {
    assert.throws(() => validateCredential(bad), PasskeyError);
  }
});

function runHelper(mode, request, { onMessage, timeoutMs = 90_000, raw } = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [HELPER, mode], { stdio: ['pipe', 'pipe', 'pipe'] });
    const messages = [];
    let stderr = '';
    child.stderr.on('data', (data) => { stderr += data; });
    readline.createInterface({ input: child.stdout }).on('line', (line) => {
      let message;
      try { message = JSON.parse(line); } catch { return; }
      messages.push(message);
      if (onMessage) onMessage(message, child);
    });
    const timer = setTimeout(() => { child.kill('SIGKILL'); reject(new Error(`helper timed out\n${stderr}`)); }, timeoutMs);
    child.on('close', (code) => {
      clearTimeout(timer);
      resolve({ code, messages, stderr, result: messages.find((m) => m.type === 'result'),
        credentials: messages.filter((m) => m.type === 'credential').map((m) => m.credential) });
    });
    if (raw !== undefined) child.stdin.end(raw);
    else child.stdin.write(`${JSON.stringify({ mode, ...request })}\n`);
  });
}

test('the stdin protocol reports a missing, malformed or unknown request as a failed result', async () => {
  const empty = await runHelper('login', {}, { raw: '' });
  assert.equal(empty.code, 1);
  assert.match(empty.result.error, /No request on stdin/);
  const garbled = await runHelper('login', {}, { raw: 'not json\n' });
  assert.match(garbled.result.error, /not JSON/);
  const unknown = await runHelper('dance', {});
  assert.match(unknown.result.error, /Usage: passkey\.cjs enroll\|login/);
  const invalid = await runHelper('login', { credential: { credentialId: 'x' } });
  assert.match(invalid.result.error, /not valid; run `brisk sbi enroll` again/);
});

test('a Chrome that cannot start is a clear failure and leaves no profile behind', async () => {
  const before = fs.readdirSync(os.tmpdir()).filter((n) => n.startsWith('brisk-passkey-'));
  const credential = { credentialId: 'a', privateKey: 'k', rpId: 'r', isResidentCredential: true, signCount: 1 };
  const run = await runHelper('login', { credential, chrome: '/nonexistent/chrome' });
  assert.equal(run.code, 1);
  assert.match(run.result.error, /Could not start Chrome \(ENOENT\)/);
  assert.deepEqual(fs.readdirSync(os.tmpdir()).filter((n) => n.startsWith('brisk-passkey-')), before);
  assert.doesNotMatch(run.stderr + JSON.stringify(run.messages), /"k"/);
});

// ---- end-to-end in a real Chrome ----

let chrome = null;
try { chrome = findChrome(); } catch { /* the end-to-end tests skip */ }
const e2e = { skip: chrome ? false : 'Chrome is not installed', timeout: 120_000 };

const { FakeSbi } = require('./fake_sbi_passkey.cjs');

const baseOptions = (site, extra = {}) => ({ headless: true, loginUrl: `${site.mainOrigin}/login`, cookieHost: 'brisk.localhost',
  cookiePrefix: 'session_', cookieTimeoutMs: 20_000, assertTimeoutMs: 20_000, clickWaitMs: 10_000, ...extra });

async function enrollOn(site) {
  const run = await runHelper('enroll', { headless: true, loginUrl: `${site.mainOrigin}/enroll`, enrollTimeoutMs: 30_000 }, {
    onMessage: (message, child) => {
      if (message.type !== 'registered') return;
      const wait = setInterval(() => { if (site.registered) { clearInterval(wait); child.stdin.write('done\n'); } }, 50);
    },
  });
  return run;
}

function leftovers() {
  const ps = execFileSync('ps', ['-axo', 'command']).toString();
  return ps.split('\n').filter((line) => line.includes('--user-data-dir=') && line.includes('brisk-passkey-')).length;
}

test('enrolling captures the passkey the site registered, and the profile and Chrome are gone afterwards', e2e, async () => {
  const site = new FakeSbi();
  await site.listen();
  try {
    const run = await enrollOn(site);
    assert.equal(run.code, 0, run.stderr);
    assert.deepEqual(run.result, { type: 'result', ok: true });
    assert.ok(run.messages.some((m) => m.type === 'registered'));
    const passkey = run.credentials.at(-1);
    assert.equal(passkey.rpId, 'main.localhost');
    assert.equal(passkey.isResidentCredential, true);
    assert.ok(passkey.privateKey.length > 100);
    assert.ok(site.registered);
    assert.equal(leftovers(), 0, 'Chrome with the ephemeral profile is still running');
    assert.doesNotMatch(run.stderr, new RegExp(passkey.privateKey.slice(0, 40)));
    assert.deepEqual(fs.readdirSync(os.tmpdir()).filter((n) => n.startsWith('brisk-passkey-')), []);
  } finally { site.close(); }
});

test('login signs the site in, returns only the BRiSK cookies and the advanced passkey, and leaks nothing', e2e, async () => {
  const site = new FakeSbi();
  await site.listen();
  try {
    const first = (await enrollOn(site)).credentials.at(-1);
    const run = await runHelper('login', { ...baseOptions(site), credential: first });
    assert.equal(run.code, 0, run.stderr);
    assert.equal(run.result.ok, true);
    assert.deepEqual(Object.keys(run.result.cookies).sort(), ['brisk_extra', 'session_fake']);
    assert.equal(run.result.cookies.session_fake, site.briskCookieValue);
    assert.ok(!('main_session' in run.result.cookies), 'a cookie of the main site must never be returned');
    const advanced = run.credentials.at(-1);
    assert.equal(advanced.credentialId, first.credentialId);
    assert.ok(advanced.signCount > first.signCount, 'the sign counter must advance');
    assert.equal(advanced.signCount, site.counter, 'the saved counter must equal the one the site saw');
    for (const secret of [first.privateKey, site.briskCookieValue]) assert.ok(!run.stderr.includes(secret), 'a secret reached the log');
    assert.doesNotMatch(run.stderr, /v2\.local|token=|ticket=/);
    assert.equal(leftovers(), 0);
  } finally { site.close(); }
});

test('a passkey that was not saved after a login is refused as a clone; the saved one still works', e2e, async () => {
  const site = new FakeSbi();
  await site.listen();
  try {
    const original = (await enrollOn(site)).credentials.at(-1);
    const one = await runHelper('login', { ...baseOptions(site), credential: original });
    assert.equal(one.result.ok, true, one.stderr);
    const saved = one.credentials.at(-1);

    const stale = await runHelper('login', { ...baseOptions(site, { cookieTimeoutMs: 3000 }), credential: original });
    assert.equal(stale.result.ok, false);
    assert.match(stale.result.error, /No brisk\.localhost session cookie appeared/);
    assert.match(site.refusals.at(-1), /sign counter did not increase.*cloned authenticator/);
    assert.ok(stale.credentials.length > 0, 'a failed run still reports the passkey so the caller can save it');

    const two = await runHelper('login', { ...baseOptions(site), credential: saved });
    assert.equal(two.result.ok, true, two.stderr);
    assert.ok(two.credentials.at(-1).signCount > saved.signCount);
    assert.equal(site.accepted, 2);
  } finally { site.close(); }
});

test('with launchUrl the helper opens BRiSK itself when the site stays on the main host', e2e, async () => {
  const site = new FakeSbi({ redirectToBrisk: false });
  await site.listen();
  try {
    const passkey = (await enrollOn(site)).credentials.at(-1);
    const without = await runHelper('login', { ...baseOptions(site, { cookieTimeoutMs: 3000 }), credential: passkey });
    assert.equal(without.result.ok, false);
    assert.match(without.result.error, /BRiSK was not opened \(set launchUrl\)/);
    const saved = without.credentials.at(-1);
    const run = await runHelper('login', { ...baseOptions(site, { launchUrl: `${site.mainOrigin}/launch` }), credential: saved });
    assert.equal(run.result.ok, true, run.stderr);
    assert.equal(run.result.cookies.session_fake, site.briskCookieValue);
    assert.match(run.stderr, /Opening BRiSK at http:\/\/main\.localhost:\d+\n/);
    assert.doesNotMatch(run.stderr, /\/launch/, 'only origins are logged, never paths');
  } finally { site.close(); }
});

test('a login page without the passkey control fails clearly instead of hanging', e2e, async () => {
  const site = new FakeSbi();
  await site.listen();
  try {
    const passkey = (await enrollOn(site)).credentials.at(-1);
    const run = await runHelper('login', { ...baseOptions(site, { passkeyButton: 'no such button', assertTimeoutMs: 3000, clickWaitMs: 1500 }), credential: passkey });
    assert.equal(run.result.ok, false);
    assert.match(run.result.error, /never asked for the passkey.*passkeyButton and loginUrl/);
    assert.match(run.stderr, /No "no such button" control found on the login page/);
  } finally { site.close(); }
});
