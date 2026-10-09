'use strict';
// A fake SBI for the passkey host: a main site with passkey registration and login, and a separate
// BRiSK host that is entered with a one-time ticket. It checks what a real relying party checks
// (ES256 signature, rp id hash, user presence and verification, a sign counter that must increase).
// Used by passkey.test.cjs and tests/test_passkey.py. Run directly it serves until stdin closes.
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const http = require('node:http');

const b64u = (buffer) => Buffer.from(buffer).toString('base64url');
const sha256 = (data) => crypto.createHash('sha256').update(data).digest();

const PAGE_HELPERS = `
const enc = (buf) => btoa(String.fromCharCode(...new Uint8Array(buf))).replace(/\\+/g, '-').replace(/\\//g, '_').replace(/=+$/, '');
const dec = (s) => Uint8Array.from(atob(s.replace(/-/g, '+').replace(/_/g, '/')), (c) => c.charCodeAt(0));`;

// A fake SBI: a main site with passkey registration and login, and a separate BRiSK host that is
// entered with a one-time ticket. It checks what a real relying party checks.
class FakeSbi {
  constructor({ redirectToBrisk = true } = {}) {
    this.redirectToBrisk = redirectToBrisk;
    this.credentials = new Map();
    this.challenges = new Set();
    this.tickets = new Set();
    this.sessions = new Set();
    this.registered = false;
    this.refusals = [];
    this.accepted = 0;
    this.briskCookieValue = null;
  }

  async listen() {
    this.main = http.createServer((req, res) => this.#mainRoute(req, res).catch((e) => { res.statusCode = 500; res.end(String(e)); }));
    this.brisk = http.createServer((req, res) => this.#briskRoute(req, res));
    await Promise.all([this.main, this.brisk].map((s) => new Promise((resolve) => s.listen(0, '127.0.0.1', resolve))));
    this.mainOrigin = `http://main.localhost:${this.main.address().port}`;
    this.briskOrigin = `http://brisk.localhost:${this.brisk.address().port}`;
  }

  close() { for (const s of [this.main, this.brisk]) { s.closeAllConnections(); s.close(); } }

  challenge() { const c = b64u(crypto.randomBytes(32)); this.challenges.add(c); return c; }

  async #body(req) { let text = ''; for await (const chunk of req) text += chunk; return text ? JSON.parse(text) : {}; }

  async #mainRoute(req, res) {
    const url = new URL(req.url, this.mainOrigin);
    const json = (value) => { res.setHeader('content-type', 'application/json'); res.end(JSON.stringify(value)); };
    const html = (body) => { res.setHeader('content-type', 'text/html; charset=utf-8'); res.end(`<!doctype html><meta charset=utf-8><title>fake sbi</title>${body}`); };
    if (url.pathname === '/enroll') {
      return html(`<script>${PAGE_HELPERS}
        (async () => {
          const opt = await (await fetch('/api/challenge')).json();
          const made = await navigator.credentials.create({ publicKey: { challenge: dec(opt.challenge),
            rp: { name: 'Fake SBI', id: location.hostname }, user: { id: new Uint8Array([1, 2, 3, 4]), name: 'trader', displayName: 'trader' },
            pubKeyCredParams: [{ type: 'public-key', alg: -7 }], authenticatorSelection: { residentKey: 'required', userVerification: 'required' } } });
          await fetch('/api/register', { method: 'POST', body: JSON.stringify({ id: made.id, publicKey: enc(made.response.getPublicKey()),
            clientDataJSON: enc(made.response.clientDataJSON) }) });
        })();</script>`);
    }
    if (url.pathname === '/login') {
      return html(`<a href="/help">パスキー認証でログインできない方はこちら</a>
        <button id="other">パスワードでログイン</button>
        <button id="go">  パスキー認証でログイン  </button>
        <script>${PAGE_HELPERS}
        document.getElementById('go').onclick = async () => {
          const opt = await (await fetch('/api/challenge')).json();
          const got = await navigator.credentials.get({ publicKey: { challenge: dec(opt.challenge), rpId: location.hostname, userVerification: 'required' } });
          const out = await (await fetch('/api/login', { method: 'POST', body: JSON.stringify({ id: got.id, clientDataJSON: enc(got.response.clientDataJSON),
            authenticatorData: enc(got.response.authenticatorData), signature: enc(got.response.signature) }) })).json();
          if (out.ok) location.href = out.next; else document.title = 'refused: ' + out.error;
        };</script>`);
    }
    if (url.pathname === '/__state') return json(this.state());
    if (url.pathname === '/home') return html('home');
    if (url.pathname === '/launch') {
      const session = /(?:^|; )main_session=([^;]+)/.exec(req.headers.cookie || '');
      if (!session || !this.sessions.has(session[1])) { res.statusCode = 401; return res.end('login first'); }
      res.statusCode = 302;
      res.setHeader('location', `${this.briskOrigin}/enter?ticket=${this.#ticket()}`);
      return res.end();
    }
    if (url.pathname === '/api/challenge') return json({ challenge: this.challenge() });
    if (url.pathname === '/api/register') return json(this.#register(await this.#body(req)));
    if (url.pathname === '/api/login') return this.#login(await this.#body(req), res, json);
    res.statusCode = 404;
    return res.end('missing');
  }

  #ticket() { const t = b64u(crypto.randomBytes(16)); this.tickets.add(t); return t; }

  #register(body) {
    const data = JSON.parse(Buffer.from(body.clientDataJSON, 'base64url'));
    assert.equal(data.type, 'webauthn.create');
    assert.ok(this.challenges.delete(data.challenge));
    this.credentials.set(body.id, { signCount: 0,
      publicKey: crypto.createPublicKey({ key: Buffer.from(body.publicKey, 'base64url'), format: 'der', type: 'spki' }) });
    this.registered = true;
    return { ok: true };
  }

  #login(body, res, json) {
    const refuse = (error) => { this.refusals.push(error); json({ ok: false, error }); };
    const clientData = Buffer.from(body.clientDataJSON, 'base64url');
    const data = JSON.parse(clientData);
    const auth = Buffer.from(body.authenticatorData, 'base64url');
    const known = this.credentials.get(body.id);
    if (!known) return refuse('unknown credential');
    if (data.type !== 'webauthn.get' || data.origin !== this.mainOrigin || !this.challenges.delete(data.challenge)) return refuse('bad client data');
    if (!auth.subarray(0, 32).equals(sha256('main.localhost'))) return refuse('wrong relying party');
    if ((auth[32] & 0x05) !== 0x05) return refuse('user presence and verification are required');
    if (!crypto.verify('sha256', Buffer.concat([auth, sha256(clientData)]), known.publicKey, Buffer.from(body.signature, 'base64url'))) return refuse('bad signature');
    const count = auth.readUInt32BE(33);
    if (count <= known.signCount) return refuse(`sign counter did not increase (${count} <= ${known.signCount}): cloned authenticator?`);
    known.signCount = count;
    this.accepted += 1;
    const session = b64u(crypto.randomBytes(16));
    this.sessions.add(session);
    res.setHeader('set-cookie', `main_session=${session}; Path=/; HttpOnly`);
    return json({ ok: true, next: this.redirectToBrisk ? `${this.briskOrigin}/enter?ticket=${this.#ticket()}` : '/home' });
  }

  #briskRoute(req, res) {
    const url = new URL(req.url, this.briskOrigin);
    if (url.pathname === '/enter' && this.tickets.delete(url.searchParams.get('ticket'))) {
      this.briskCookieValue = `v2.local.${b64u(crypto.randomBytes(24))}`;
      res.setHeader('set-cookie', [`session_fake=${this.briskCookieValue}; Path=/; HttpOnly`, 'brisk_extra=on; Path=/']);
      res.setHeader('content-type', 'text/html');
      return res.end('<!doctype html><title>brisk</title>BRiSK');
    }
    res.statusCode = 401;
    return res.end('login first');
  }

  get counter() { return [...this.credentials.values()][0].signCount; }

  state() {
    return { registered: this.registered, accepted: this.accepted, counter: this.credentials.size ? this.counter : null,
      refusals: this.refusals, briskCookieValue: this.briskCookieValue };
  }
}

module.exports = { FakeSbi, b64u, sha256 };

if (require.main === module) {
  const site = new FakeSbi({ redirectToBrisk: process.argv[2] !== 'stay' });
  site.listen().then(() => {
    process.stdout.write(`${JSON.stringify({ mainOrigin: site.mainOrigin, briskOrigin: site.briskOrigin })}\n`);
    process.stdin.on('data', () => {}).on('close', () => { site.close(); process.exit(0); });
  });
}
