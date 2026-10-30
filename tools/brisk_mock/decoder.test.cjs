'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { spawnSync } = require('node:child_process');
const { Decoder, QuoteHistory, frames, u64, looseU64, parseMasterFile, checkMasterFile, loadAssets, replay, main, parseStockView, stockViewShift,
  scheduleArgs, SCHEDULE_FIELDS, boardCalls, BOARD_KERNEL } = require('../../briskapi/decoder/decoder.cjs');
const cache = process.env.BRISK_MOCK_CACHE;

function frame(time, bytes = Buffer.from([1])) {
  const data = Buffer.alloc(8 + bytes.length);
  data.writeUInt32LE(time, 0); data.writeUInt32LE(bytes.length, 4); bytes.copy(data, 8);
  return data;
}

test('record container validates lengths, truncation and time order', () => {
  assert.equal([...frames(Buffer.concat([frame(9), frame(10)]))].length, 2);
  for (const bad of [Buffer.alloc(7), frame(9).subarray(0, 8), Buffer.alloc(8),
    Buffer.concat([frame(9), frame(8)])]) assert.throws(() => [...frames(bad)]);
  assert.equal(u64([0xffffffff, 1], 0), 8589934591);
  assert.throws(() => u64([0xffffffff, 0xffffffff], 0), /Unsafe/);
  assert.equal(looseU64([100, 0], 0), 100);
  assert.ok(Number.isNaN(looseU64([0xffffffff, 0xffffffff], 0)));
});

test('QR rollback and clone keep independent last prices', () => {
  const q = new QuoteHistory(); q.initialize(1);
  assert.equal(q.lastPrice10(1), 0);
  q.emplace(1, 0, 99, 700, 10, 100, 1); q.clone(2, 1);
  q.emplace(2, 0, 100, 710, 10, 100, 2);
  assert.equal(q.lastPrice10(1), 700); assert.equal(q.lastPrice10(2), 710);
  q.popBack(2); assert.equal(q.size(2), 1); q.deinitialize(2);
  assert.equal(q.qrs.has(2), false);
});

test('rejects unpinned cache and invalid CLI settings', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'brisk-hash-'));
  try { fs.writeFileSync(path.join(dir, 'fita.js'), 'bad'); assert.throws(() => loadAssets(dir), /hash mismatch/); }
  finally { fs.rmSync(dir, { recursive: true }); }
  await assert.rejects(main([]), /cache/);
  await assert.rejects(main(['--unknown', 'x']), /Usage/);
  await assert.rejects(main(['--cache']), /Usage/);
  await assert.rejects(replay({ speed: -1 }), /Speed/);
  await assert.rejects(replay({ speed: Infinity }), /Speed/);
  await assert.rejects(replay({ speed: 0, limitFrames: 0 }), /limit/);
});

test('decoder rejects malformed snapshots, gaps and overflows', () => {
  const d = Object.create(Decoder.prototype);
  d.w = { HEAPU8: new Uint8Array(4 * 1024 * 1024), _push() {}, _unserialize() {},
    _pushWs() {}, _applyBasePriceQueue() {}, _getTrace: () => -1, _clearTrace() {},
    _getStockView: () => 0 };
  d.buf = 0; d.aux = 8; d.master = [{ code: '7203' }];
  d.push(Buffer.alloc(4 * 1024 * 1024 + 1));
  for (const bad of [Buffer.alloc(3), Buffer.alloc(4), Buffer.from([9, 0, 0, 0])]) {
    assert.throws(() => d.unserialize(bad), /snapshot/);
  }
  assert.throws(() => d.feed(Buffer.alloc(4 * 1024 * 1024 + 1)), /buffer/);
  d.authError = true; assert.throws(() => d.feed(Buffer.alloc(1)), /session/); d.authError = false;
  assert.equal(d.start(Buffer.alloc(1)), false);  // waits for frame numbers
  d.initialFrames = [1, 2];
  assert.throws(() => d.start(Buffer.alloc(1)), /bootstrap/);
  d.initialFrames = [1]; d.w._getFrameNumbers = () => {};
  assert.throws(() => d.start(Buffer.alloc(1)), /catch-up/);
  assert.throws(() => d.changed(), /overflow/);
  d.w._getTrace = () => 1; d.w.HEAPU8[0] = 2;
  assert.throws(() => d.changed(), /issue ID/);
  assert.throws(() => d.quote(1), /Unknown/);
  assert.throws(() => d.quote(0), /unavailable/);
});

test('public mock pre-open snapshot and complete opening replay', { skip: !cache }, async () => {
  let bootstrap, previousSeq = -1, previousTime = 0, count = 0, sawOpeningTrade = false;
  const stats = await replay({ cache, speed: 0, emit: batch => {
    assert.equal(batch.seq, previousSeq + 1); previousSeq = batch.seq;
    assert.ok(batch.source_time_us >= previousTime); previousTime = batch.source_time_us;
    if (batch.type === 'bootstrap') {
      bootstrap = batch;
      assert.equal(batch.market_issue_count, 4131); assert.equal(batch.quotes.length, 4131);
      assert.equal(batch.source_time_us, 32399999955);
      assert.equal(batch.source_timestamp_origin, 'brisk_decoder_unverified');
      assert.equal(batch.exchange_delay_ms, null);
      const q = batch.quotes.find(q => q.code === '7203');
      assert.equal(q.indicative_price10, 101500); assert.equal(q.indicative_volume, 304700);
      assert.equal(q.market_buy_quantity, 201300); assert.equal(q.market_sell_quantity, 105700);
      assert.equal(q.volume, 0); assert.equal(q.last_price10, 0);
      assert.equal(batch.quotes.filter(q => q.indicative_price10 > 0).length, 3891);
    }
    if (batch.type === 'quotes') {
      assert.equal(batch.replay_lateness_ms, null);
      count += batch.quotes.length;
      if (batch.quotes.some(q => q.code === '7203' && q.open_price10 > 0 && q.volume > 0)) sawOpeningTrade = true;
    }
  } });
  assert.equal(bootstrap.trading_date, '20210927');
  assert.equal(stats.frames, 18001); assert.equal(count, 731251);
  assert.equal(stats.source_time_us, 32579999896); assert.ok(sawOpeningTrade);
});

test('filtering, replay pacing and partial-run termination', { skip: !cache }, async () => {
  const batches = [];
  await replay({ cache, codes: ['7203', '5659'], speed: 10, limitFrames: 10, emit: b => batches.push(b) });
  assert.equal(batches[0].quotes.length, 2);
  assert.ok(batches.every(b => !b.quotes || b.quotes.every(q => ['7203', '5659'].includes(q.code))));
  assert.equal(batches.at(-1).frames, 10);
  assert.ok(batches.filter(b => b.type === 'quotes').every(b => b.replay_lateness_ms >= 0));
  await assert.rejects(replay({ cache, codes: ['130A'], speed: 0, emit() {} }), /missing from 2021/);
});

test('CLI emits JSON only and reports failures', { skip: !cache }, () => {
  const command = path.join(__dirname, '../../briskapi/decoder/decoder.cjs');
  const run = spawnSync(process.execPath, [command, '--cache', cache, '--codes', '7203', '--speed', '0', '--limit-frames', '2'], { encoding: 'utf8' });
  assert.equal(run.status, 0, run.stderr);
  const records = run.stdout.trim().split('\n').map(JSON.parse);
  assert.equal(records.length, 3); assert.equal(records.at(-1).type, 'end');
  const bad = spawnSync(process.execPath, [command], { encoding: 'utf8' });
  assert.equal(bad.status, 1); assert.match(bad.stderr, /cache/);
});

// ---------------------------------------------------------------------------------------
// Stock view layout per protocol version. The two position tables below are written out
// literally, from two independent parsers, so they cross-check the shift used by the code:
//   16000: the vendor client's own getStockView (public Next demo bundle), n = 6 * ohlcLength
//   18000: an independent parser of SBI's current build, which reads the same
//          fields one word lower (the header word, the numeric code, is gone), as SBI's
//          March build's own _getStockView writes them (turnover and the closing side too)
const FIELDS_16000 = n => ({ last_price10: [1], open_price10: [2], volume: [n + 3, n + 4], turnover10: [n + 5, n + 6],
  bid_price10: [n + 7], ask_price10: [n + 8], indicative_price10: [n + 9], indicative_volume: [n + 10, n + 11],
  indicative_imbalance_side: [n + 12], closing_indicative_price10: [n + 13], closing_indicative_volume: [n + 14, n + 15],
  closing_indicative_imbalance_side: [n + 16], source_time_us: [n + 17, n + 18],
  frame: [n + 19], max_frame: [n + 20], quote_flag: [n + 21], quote_side: [n + 22],
  special_quote_time_us: [n + 23, n + 24], indicative_open_price10: [n + 32], reference_price10: [n + 35] });
const FIELDS_18000 = n => ({ last_price10: [0], open_price10: [1], volume: [n + 2, n + 3], turnover10: [n + 4, n + 5],
  bid_price10: [n + 6], ask_price10: [n + 7], indicative_price10: [n + 8], indicative_volume: [n + 9, n + 10],
  indicative_imbalance_side: [n + 11], closing_indicative_price10: [n + 12], closing_indicative_volume: [n + 13, n + 14],
  closing_indicative_imbalance_side: [n + 15], source_time_us: [n + 16, n + 17],
  frame: [n + 18], max_frame: [n + 19], quote_flag: [n + 20], quote_side: [n + 21],
  special_quote_time_us: [n + 22, n + 23], indicative_open_price10: [n + 31], reference_price10: [n + 34] });

function readWith(fields, words) {
  return Object.fromEntries(Object.entries(fields).map(([name, at]) =>
    [name, at.length === 2 ? words[at[0]] + 4294967296 * words[at[1]] : words[at[0]]]));
}

test('stock view layouts: both protocol versions read the same fields at their own positions', () => {
  for (const ohlc of [60, 5]) {
    const n = 6 * ohlc;
    for (const [version, table] of [[16000, FIELDS_16000(n)], [18000, FIELDS_18000(n)]]) {
      const words = new Uint32Array(37 + 6 * ohlc);
      let seed = 1000;
      const expected = {};
      for (const [name, at] of Object.entries(table)) {
        const value = at.length === 2 ? 5 * 4294967296 + (seed += 7) : (seed += 7);
        if (at.length === 2) { words[at[0]] = value % 4294967296; words[at[1]] = Math.floor(value / 4294967296); } else words[at[0]] = value;
        expected[name] = value;
      }
      assert.deepEqual(parseStockView(words, ohlc, stockViewShift(version)), expected, `protocol ${version}, ohlc ${ohlc}`);
    }
  }
  assert.throws(() => stockViewShift(17000), /layout for protocol 17000 is not known \(known: 16000, 18000\)/);
  assert.throws(() => stockViewShift(undefined), /is not known/);
});

test('the layout shift is the only difference between the two versions', () => {
  const n = 6 * 60;
  for (const [name, at16] of Object.entries(FIELDS_16000(n))) {
    assert.deepEqual(FIELDS_18000(n)[name], at16.map(i => i - 1), name);
  }
});

test('an unknown protocol version is refused before any frame is read', { skip: !cache }, async () => {
  await assert.rejects(Decoder.create(loadAssets(cache), { protocolVersion: 17000 }), /layout for protocol 17000 is not known/);
});

test('our parse equals the vendor client\'s own indices on real demo data', { skip: !cache }, async () => {
  const assets = loadAssets(cache);
  const decoder = await Decoder.create(assets);
  const it = frames(assets['ws.dat']);
  decoder.start(it.next().value.data);
  let checked = 0;
  const check = () => {
    const n = 6 * decoder.ohlc;
    for (let issue = 0; issue < decoder.master.length; issue += 17) {
      if (!decoder.w._getStockView(decoder.id, issue, decoder.buf)) continue;
      const words = new Uint32Array(decoder.w.HEAPU8.buffer, decoder.buf, decoder.viewWords).slice();
      const { issue_id, code, ...ours } = decoder.quote(issue);
      const { ...expected } = readWith(FIELDS_16000(n), words);
      const compared = Object.fromEntries(Object.keys(expected).map(key => [key, ours[key]]));
      assert.deepEqual(compared, expected, `issue ${issue}`);
      checked++;
    }
  };
  check();
  for (let i = 0; i < 1500; i++) decoder.feed(it.next().value.data);
  check();
  assert.ok(checked > 400, `checked ${checked} views`);
});

// ---------------------------------------------------------------------------------------
// The six callbacks and the heartbeat/ping loop, against the real decoder.

function pingFrame(ns) {
  const frame = Buffer.alloc(9);
  frame[0] = 0xf0;
  frame.writeBigUInt64LE(ns, 1);
  return frame;
}

test('the decoder turns a ping echo into a heartbeat and answers with its own ping', { skip: !cache }, async () => {
  const assets = loadAssets(cache);
  const beats = [], sends = [];
  const decoder = await Decoder.create(assets, { callbacks: { heartbeat: ns => beats.push(ns), send: b => sends.push(b) } });
  const it = frames(assets['ws.dat']);
  decoder.start(it.next().value.data);
  for (let i = 0; i < 300; i++) decoder.feed(it.next().value.data);
  decoder.feed(Buffer.from([0x38, 0x04, 0x00, 1, 2, 3]));                  // a server sync frame
  assert.deepEqual([beats.length, sends.length], [0, 0], 'market and sync frames neither beat nor ping');
  const clock = 1_791_475_200_123_456_789n;
  decoder.feed(pingFrame(clock));
  assert.deepEqual(beats, [clock]);
  assert.equal(decoder.lastHeartbeat, clock);
  assert.equal(sends.length, 1);
  assert.equal(sends[0].length, 9);
  assert.equal(sends[0][0], 0xf0);
  // The ping carries the server's clock as the decoder estimates it (the echo's time plus the
  // time elapsed since), not the local clock: the fed echo is hours away from Date.now().
  const sentClock = sends[0].readBigUInt64LE(1);
  assert.ok(Math.abs(Number(sentClock - clock) / 1e6) < 5000, 'the ping carries the server clock in nanoseconds');
  decoder.feed(pingFrame(clock + 1n));
  assert.equal(sends.length, 2, 'every echo is answered, so the loop sustains itself once a ping arrives');
});

test('all six WASM callbacks reach the host with the vendor\'s signatures', { skip: !cache }, async () => {
  const got = {};
  let wasm, pointers;
  const decoder = await Decoder.create(loadAssets(cache), {
    check: w => { wasm = w; const init = w._initialize; w._initialize = (...a) => { pointers = a.slice(0, 6); return init(...a); }; },
    callbacks: { send: b => got.send = b, heartbeat: n => got.heartbeat = n, basePrice: i => got.basePrice = i,
      authError: () => got.authError = true, marketFinished: () => got.marketFinished = true },
  });
  const ptr = wasm._malloc(16);
  wasm.HEAPU8.set([0xf0, 1, 2, 3], ptr);
  wasm.dynCall_viii(pointers[0], decoder.id, ptr, 4);
  wasm.dynCall_viii(pointers[2], decoder.id, -1, 5);   // a negative i32 half is unsigned on the wire
  wasm.dynCall_vii(pointers[3], decoder.id, 1424);
  wasm.dynCall_vii(pointers[4], decoder.id, 1);
  wasm.dynCall_vi(pointers[5], decoder.id);
  assert.equal(got.send.toString('hex'), 'f0010203');
  assert.equal(got.heartbeat, 0xFFFFFFFFn + (5n << 32n));
  assert.equal(got.basePrice, 1424);
  assert.ok(got.authError && got.marketFinished && decoder.authError && decoder.marketFinished);
  assert.ok(decoder.basePriceQueue.has(1424), 'a decoder-reported base price change is queued for the master re-read');
});

// ---------------------------------------------------------------------------------------
// Boot data (base prices, exceptional special quotes), applied as the vendor client does.

test('boot base prices change the master limits; counts are reported', { skip: !cache }, async () => {
  const assets = loadAssets(cache);
  const plain = await Decoder.create(assets);
  const toyota = plain.master.find(m => m.code === '7203');
  assert.deepEqual(plain.boot, { exceptional_sq: 0, base_prices: 0, applied: 0 });
  const boot = {
    base_prices: [{ issue_code: '7203', base_price10: 123450, limit_down10: 93450, limit_up10: 153450 }],
    exceptional_sq: [{ buy_sell: 'B', jump_range: 10, jump_sec: 180, issue_code: '7203', quote_limit_down: 9900, quote_limit_up: 10400 }],
  };
  const decoder = await Decoder.create(assets, { boot });
  const changed = decoder.master.find(m => m.code === '7203');
  assert.deepEqual(decoder.boot, { exceptional_sq: 1, base_prices: 1, applied: 1 });
  assert.equal(changed.limit_up10, 153450);
  assert.equal(changed.limit_down10, 93450);
  assert.notEqual(changed.limit_up10, toyota.limit_up10);
  const others = decoder.master.filter(m => m.code !== '7203').every((m, i) => m.limit_up10 === plain.master.filter(p => p.code !== '7203')[i].limit_up10);
  assert.ok(others, 'only the named issue changed');
  assert.equal(decoder.applyBasePrice(), 0, 'nothing is queued afterwards');
});

test('malformed or unknown boot data fails loudly; a special quote for an issue not in the master is listed, not sent',
  { skip: !cache }, async () => {
  const assets = loadAssets(cache);
  const base = { issue_code: '7203', base_price10: 1, limit_down10: 1, limit_up10: 2 };
  await assert.rejects(Decoder.create(assets, { boot: { base_prices: [{ ...base, issue_code: '0000' }] } }),
    /base_prices names issue code 0000, which is not in the master/);
  await assert.rejects(Decoder.create(assets, { boot: { base_prices: [{ ...base, limit_up10: undefined }] } }),
    /Boot limit_up10 is not a number/);
  // The demo's 2021 master has no 130A; failing would stop every session whose boot names a stale issue.
  const decoder = await Decoder.create(assets, { boot: { exceptional_sq: [{ buy_sell: 'S', jump_range: 1, jump_sec: 1,
    issue_code: '130A', quote_limit_down: 1, quote_limit_up: 2 }] } });
  assert.deepEqual(decoder.boot, { exceptional_sq: 0, base_prices: 0, applied: 0, skipped_exceptional_sq: ['130A'] });
});

test('a special quote names its issue by code in the demo\'s build and by index in the 2026 builds', () => {
  const master = [{ code: '1301', issue_id: 0 }, { code: '167A', issue_id: 1 }, { code: '7203', issue_id: 2 }];
  const sq = issue_code => ({ buy_sell: 'B', jump_range: 1, jump_sec: 180, issue_code, quote_limit_down: 1, quote_limit_up: 2 });
  const push = sqIssue => {
    const issues = [];
    const host = { master, id: 7, sqIssue, stderr: { invalidIssueCode: 0 }, basePriceQueue: new Set(), applyBasePrice: () => 0,
      w: { _pushSQ: (id, issue) => issues.push(issue) } };
    const boot = Decoder.prototype.applyBoot.call(host, { exceptional_sq: ['167A', '7203', '1301', '9999'].map(sq) });
    return { issues, skipped: boot.skipped_exceptional_sq };
  };
  // Given a code, a 2026 build attaches the special quote to the issue at that index (1301 to 8283's).
  assert.deepEqual(push('index'), { issues: [1, 2, 0], skipped: ['9999'] });
  assert.deepEqual(push('code'), { issues: [7203, 1301], skipped: ['167A', '9999'] });
});

test('a special quote the decoder can\'t place stops the session', { skip: !cache }, async () => {
  // The decoder reports it only on stderr ("Invalid Issue Code"); here the demo's build is given an index.
  const decoder = await Decoder.create(loadAssets(cache));
  decoder.sqIssue = 'index';
  assert.throws(() => decoder.applyBoot({ exceptional_sq: [{ buy_sell: 'B', jump_range: 1, jump_sec: 180, issue_code: '1301',
    quote_limit_down: 1, quote_limit_up: 2 }] }), /rejected 1 of the boot's special quotes as an invalid issue code/);
});

test('a frame the decoder can\'t parse becomes one readable error', { skip: !cache }, async () => {
  // A parse error escapes the decoder as a bare number (the address of the C++ exception).
  const assets = loadAssets(cache);
  const decoder = await Decoder.create(assets);
  decoder.start(frames(assets['ws.dat']).next().value.data);
  assert.throws(() => decoder.feed(Buffer.from('<html>')), /^Error: A 6-byte frame starting 3c68746d6c3e: the decoder failed \(Invalid Header\)$/);
});

test('a decoder that stops decoding stops the session, naming the frame after which it did', { skip: !cache }, async () => {
  // SBI's recording holds this frame every 40 s; after it the decoder's parser never finds a message again.
  const assets = loadAssets(cache);
  const decoder = await Decoder.create(assets);
  const stream = frames(assets['ws.dat']);
  decoder.start(stream.next().value.data);
  decoder.feed(Buffer.from('99eb2c6a07adca97a179aaed6de6ad', 'hex'));
  assert.throws(() => { for (let n = 0; n < 10; n++) decoder.feed(stream.next().value.data); },
    /The decoder stopped decoding after a 15-byte frame starting 99eb2c6a07adca97a179aaed6de6ad: the frames since apply no update/);
});

test('a decoder trap names the issue and leaves the decoder\'s stack where it was', { skip: !cache }, async () => {
  // Without its snapshot, an issue's updates take its order quantities below zero, and reading them traps.
  // (Its book, garbage too, can take the decoder seconds to read, and isn't what is tested here.)
  const assets = loadAssets(cache);
  const decoder = await Decoder.create({ ...assets, 'snapshot.dat': Buffer.alloc(0) });
  decoder.book = () => ({});
  const stream = frames(assets['ws.dat']);
  decoder.feed(stream.next().value.data);
  decoder.begin();
  const stack = decoder.w.stackSave();
  let error = null;
  for (let n = 0; n < 50 && !error; n++) {
    decoder.feed(stream.next().value.data);
    for (const id of decoder.changed()) { try { decoder.quote(id); } catch (e) { error = e; break; } }
  }
  // The trap's own words are the engine's: V8 says what the conversion did, JavaScriptCore (Bun) which operation failed.
  assert.match(error?.message, /^The quote for \d+: the decoder trapped \((?:float unrepresentable in integer range|Out of bounds Trunc operation.*)\); its state is inconsistent/);
  assert.equal(decoder.w.stackSave(), stack);
});

test('an issue whose quote traps is skipped, and read again only once its frame number moves', { skip: !cache }, async () => {
  const assets = loadAssets(cache);
  const decoder = await Decoder.create(assets);
  const stream = frames(assets['ws.dat']);
  decoder.start(stream.next().value.data);
  const toyota = decoder.master.find(m => m.code === '7203').issue_id;
  // A trap as the decoder raises it, on Toyota only, counting the attempts.
  let reads = 0;
  const readQuote = decoder.readQuote.bind(decoder);
  decoder.readQuote = (id, m) => {
    if (id !== toyota) return readQuote(id, m);
    reads++;
    throw Object.assign(new Error('float unrepresentable in integer range'), { name: 'RuntimeError' });
  };
  const { quotes, skipped } = decoder.quotes([toyota, 0, 1]);
  assert.deepEqual([quotes.map(q => q.issue_id), skipped, reads], [[0, 1], [toyota], 1]);
  assert.deepEqual(decoder.quotes([toyota]).skipped, [toyota]);
  assert.equal(reads, 1, 'not read again at the same frame: the decoder would return stale fields');
  const frame = decoder.frameNumbers()[toyota];
  while (decoder.frameNumbers()[toyota] === frame) decoder.feed(stream.next().value.data);
  decoder.readQuote = readQuote;
  assert.deepEqual(decoder.quotes([toyota]), { quotes: [decoder.quote(toyota)], skipped: [] }, 'read again once it updates');
  // Any other error still ends the session.
  decoder.readQuote = () => { throw new Error('Stock view unavailable: 7203'); };
  assert.throws(() => decoder.quotes([toyota]), /Stock view unavailable/);
});

test('a quote\'s book holds the best ten prices of each side, as reading the whole board gives them', { skip: !cache }, async () => {
  const assets = loadAssets(cache);
  const decoder = await Decoder.create(assets);
  const stream = frames(assets['ws.dat']);
  decoder.start(stream.next().value.data);
  for (let n = 0; n < 2500; n++) { decoder.feed(stream.next().value.data); decoder.changed(); }
  let crossed = 0, full = 0, closing = 0;
  for (let issue = 0; issue < decoder.master.length; issue += 7) {
    const m = decoder.master[issue], q = decoder.quote(issue);
    const index = price => decoder.w._getTickPrice10ToIndex(decoder.id, m.tick_type, price);
    const low = index(m.limit_down10);
    const { rows } = decoder.rows(issue, low, index(m.limit_up10) - low + 1, true);
    const best = (group, buy) => (buy ? rows.slice().reverse() : rows).filter(r => r[group][0]).slice(0, 10)
      .map(r => [r.price10, ...r[group]]);
    assert.deepEqual(q.bid_levels, best('bid', true), m.code);
    assert.deepEqual(q.ask_levels, best('ask', false), m.code);
    assert.deepEqual(q.bid_closing_levels, best('bid_closing', true), m.code);
    assert.deepEqual(q.ask_closing_levels, best('ask_closing', false), m.code);
    for (const group of ['bid', 'bid_closing', 'ask', 'ask_closing']) {
      const sum = k => rows.reduce((total, r) => total + r[group][k], 0);
      assert.deepEqual([q[`${group}_total_quantity`], q[`${group}_total_orders`]], [sum(0), sum(1)], `${m.code} ${group}`);
    }
    const at = rows.find(r => r.price10 === q.bid_price10);
    assert.deepEqual([q.bid_quantity, q.bid_orders], at ? at.bid : [0, 0], m.code);
    crossed += q.bid_levels.length > 0 && q.ask_levels.length > 0 && q.bid_levels[0][0] >= q.ask_levels[0][0];
    full += q.bid_levels.length === 10;
    closing += q.bid_closing_levels.length > 0 || q.ask_closing_levels.length > 0;
  }
  assert.ok(crossed > 10 && full > 100 && closing > 10, `crossed ${crossed}, full ${full}, closing ${closing}`);
});

test('the decoder\'s QR records are the trades: they account for every volume and turnover change', { skip: !cache }, async () => {
  const assets = loadAssets(cache);
  const decoder = await Decoder.create(assets);
  const stream = frames(assets['ws.dat']);
  decoder.start(stream.next().value.data);
  const all = new Set(decoder.master.map(m => m.issue_id));
  const base = new Map(decoder.quotes([...all]).quotes.map(q => [q.issue_id, [q.volume, q.turnover10]]));
  assert.deepEqual(decoder.trades(all), [], 'no trades before the first frame');
  const traded = new Map(), sides = new Map();
  let checked = 0;
  for (let n = 0; n < 3000; n++) {
    decoder.feed(stream.next().value.data);
    const changed = decoder.changed();
    for (const t of decoder.trades(all)) {
      const [volume, turnover] = traded.get(t.issue_id) ?? [0, 0];
      traded.set(t.issue_id, [volume + t.quantity, turnover + t.price10 * t.quantity]);
      sides.set(t.side, (sides.get(t.side) ?? 0) + 1);
      assert.ok(t.code === decoder.master[t.issue_id].code && t.quantity > 0 && t.price10 > 0, t.code);
    }
    for (const q of decoder.quotes(changed).quotes) {
      const [volume, turnover] = traded.get(q.issue_id) ?? [0, 0];
      const [v0, t0] = base.get(q.issue_id);
      assert.deepEqual([q.volume - v0, q.turnover10 - t0], [volume, turnover], q.code);
      checked++;
    }
  }
  assert.ok(checked > 50000 && traded.size > 1000, `${checked} quotes, ${traded.size} traded issues`);
  assert.deepEqual([...sides.keys()].sort(), [0, 1, 2]);
  decoder.qrm.withdrawn = 1;
  assert.throws(() => decoder.trades(all), /withdrew/);
});

test('trades a snapshot restores come apart, by issue, in batches', { skip: !cache }, async () => {
  const assets = loadAssets(cache);
  const decoder = await Decoder.create(assets);
  assert.deepEqual(decoder.history(), [], 'the demo\'s pre-open snapshot restores no trades');
  const stock = issue => decoder.w._getStockPointer(decoder.id, issue);
  const record = (issue, timestamp) => ({ p: stock(issue), type: 1, timestamp, price10: 1000, qty: 2, lot: 100, frame: 5 });
  decoder.restored = [record(3, 30), record(1, 10), record(3, 31), record(2, 20), record(1, 11)];
  const batches = decoder.history(new Set([1, 3]), 3);
  assert.deepEqual(batches.map(b => b.map(t => [t.issue_id, t.time_us, t.quantity])),
    [[[1, 10, 200], [1, 11, 200], [3, 30, 200]], [[3, 31, 200]]]);
  assert.deepEqual(decoder.history(), [], 'taken once');
});

test('a catch-up must bring exactly the lagging issues to the stream\'s first frame', () => {
  const lagging = [{ issue_id: 1, from: 10, to: 12 }, { issue_id: 3, from: 5, to: 9 }];
  const catchUp = after => {
    let pushed = false;
    const host = { push() { pushed = true; }, frameNumbers: () => (pushed ? after : [7, 10, 4, 5]) };
    return () => Decoder.prototype.catchUp.call(host, Buffer.alloc(0), lagging);
  };
  assert.doesNotThrow(catchUp([7, 12, 4, 9]));
  assert.throws(catchUp([7, 12, 4, 8]), /^Error: Catch-up left 1 issues behind the stream \(first: issue 3, frame 8, stream starts at 9\)$/);
  // The decoder takes a reply twice, or a stray update, without complaint.
  assert.throws(catchUp([7, 14, 4, 13]), /Catch-up took issue 1 to frame 14, but the stream starts at 12: the reply repeats or misplaces updates/);
  assert.throws(catchUp([7, 12, 5, 9]), /Catch-up took issue 2 to frame 5, but it was not behind \(frame 4\)/);
});

test('the schedule\'s sq_jump_interval reaches the decoder in microseconds', () => {
  const schedule = Object.fromEntries(SCHEDULE_FIELDS.map((key, k) => [key, (8 + k) * 3600e6]));
  assert.deepEqual(scheduleArgs({ ...schedule, sq_jump_interval: 180 }), [...SCHEDULE_FIELDS.slice(0, -1).map(key => schedule[key]), 180e6]);
  // A value already in microseconds (or none) is refused rather than scaled into days.
  for (const sq_jump_interval of [180e6, 0]) {
    assert.throws(() => scheduleArgs({ ...schedule, sq_jump_interval }), /sq_jump_interval .* is not a number of seconds/);
  }
});

test('a host\'s master check runs before boot data is matched to the master', { skip: !cache }, async () => {
  // A misread master would otherwise fail on its first boot base price, hiding the real problem.
  let checked = 0;
  const boot = { base_prices: [{ issue_code: '0000', base_price10: 1000, limit_down10: 500, limit_up10: 1500 }] };
  await assert.rejects(Decoder.create(loadAssets(cache), { boot, checkMaster: master => {
    checked = master.length;
    throw new Error('stock master layout not recognised');
  } }), /layout not recognised/);
  assert.ok(checked > 1000);
  await assert.rejects(Decoder.create(loadAssets(cache), { boot }), /issue code 0000, which is not in the master/);
});

// A master file record as the demo's and SBI's files hold them (self-authored values).
function masterRecord({ code, tick = 1, base, up, down, type = '0111', lot }) {
  const r = Buffer.alloc(180, 0x20);
  r.writeUInt32LE((0x18000018 | (tick << 25)) >>> 0, 0); r.writeUInt32LE(base, 4); r.writeUInt32LE(up, 8); r.writeUInt32LE(down, 12);
  r.write(type, 40, 'latin1'); r.write(code, 50, 'latin1');
  r.write(String(lot).padStart(15), 124, 'latin1');
  return r;
}

// A record as an 18000 decoder writes it: the code as eight bytes of text, then the demo's fields four bytes on.
function decoderRecord({ code, tick = 1, base, up, down, lot, type = 111, name = '' }) {
  const r = Buffer.alloc(148);
  r.write(code, 0, 'latin1'); r.writeUInt32LE(tick, 8);
  r.writeUInt32LE(base, 12); r.writeUInt32LE(up, 16); r.writeUInt32LE(down, 20);
  r.writeBigUInt64LE(BigInt(lot), 24); r.writeUInt32LE(type, 32);
  const text = Buffer.from(name, 'utf8'); r.writeUInt32LE(text.length, 68); text.copy(r, 72);
  return r;
}

test('master file records: header prices and fixed-width text', () => {
  const records = [
    masterRecord({ code: '167A', tick: 3, base: 1700, up: 2200, down: 1200, lot: 100 }),
    masterRecord({ code: '25935', tick: 0, base: 2350, up: 3050, down: 1650, type: '0115', lot: 1 }),
  ];
  const expected = [
    { issue_id: 0, code: '167A', tick_type: 3, base_price10: 1700, limit_up10: 2200, limit_down10: 1200, lot_size: 100, issue_type: 111 },
    { issue_id: 1, code: '25935', tick_type: 0, base_price10: 2350, limit_up10: 3050, limit_down10: 1650, lot_size: 1, issue_type: 115 },
  ];
  assert.deepEqual(parseMasterFile(Buffer.concat(records), 2), expected);
  // Records are counted from the end, so a preamble (the demo's file has one) is skipped.
  assert.deepEqual(parseMasterFile(Buffer.concat([Buffer.alloc(444, 7), ...records]), 2), expected);
  assert.ok(Number.isNaN(parseMasterFile(masterRecord({ code: '1301', base: 1, up: 2, down: 0, lot: 'x' }), 1)[0].lot_size));
  assert.throws(() => parseMasterFile(Buffer.concat(records), 3), /cannot hold 3 issues/);
  assert.throws(() => parseMasterFile(Buffer.concat(records), 0), /cannot hold 0 issues/);
});

test('the demo decoder\'s master agrees with its master file on every issue, and a disagreement is refused',
  { skip: !cache }, async () => {
  const assets = loadAssets(cache);
  const { master } = await Decoder.create(assets);
  assert.equal(master.length, 4131);
  assert.doesNotThrow(() => checkMasterFile(master, assets['master.dat']));
  const file = Buffer.from(assets['master.dat']);
  const at = file.length - (master.length - 5) * 180;   // issue 5's record
  file.writeUInt32LE(file.readUInt32LE(at + 4) + 10, at + 4);
  assert.throws(() => checkMasterFile(master, file),
    /disagrees with its master file on 1 of 4131 issues \(issue 5: base_price10 \d+, file \d+\); its record layout may have changed/);
  // The tick type is the first word's bits 25-26 in the file and its own field in the decoder's record.
  const ticks = Buffer.from(assets['master.dat']);
  ticks.writeUInt32LE((ticks.readUInt32LE(at) ^ (1 << 26)) >>> 0, at);
  assert.throws(() => checkMasterFile(master, ticks), /on 1 of 4131 issues \(issue 5: tick_type \d, file \d\)/);
});

test('an 18000 master: text codes, the decoder\'s own names and tick types, limits re-read after a change', () => {
  // Just enough of a decoder to load a master: its records are the 18000 layout, at the buffer it is given.
  const issues = [
    { code: '1301', base: 52500, up: 62500, down: 42500, lot: 100, name: '極洋' },
    { code: '167A', tick: 3, base: 32300, up: 39300, down: 25300, lot: 100, name: 'リョーサン菱洋' },
    { code: '9418', base: 18140, up: 22140, down: 14140, lot: 100, name: 'Ｕ－ＮＥＸＴ' },
  ];
  const heap = new Uint8Array(4 * 1024 * 1024);
  const fake = { addFunction: () => 0, _initialize: () => 1, _malloc: () => 0, HEAPU8: heap, _push() {},
    _applyBasePriceQueue() {}, _stockCount: () => issues.length, _getDate: () => 20260311,
    _getStockMaster: (id, buf, size) => { assert.equal(size, issues.length * 148); heap.set(Buffer.concat(issues.map(decoderRecord)), buf); } };
  const file = Buffer.concat(issues.map(i => masterRecord(i)));
  const assets = { 'master.dat': file, 'snapshot.dat': Buffer.alloc(0) };
  const decoder = new Decoder(fake, assets, { protocolVersion: 18000 });
  assert.deepEqual(decoder.master[1], { issue_id: 1, code: '167A', tick_type: 3, base_price10: 32300, limit_up10: 39300,
    limit_down10: 25300, lot_size: 100, issue_type: 111, name: 'リョーサン菱洋' });
  assert.equal(decoder.master[2].name, 'Ｕ－ＮＥＸＴ', 'a name that starts like a market prefix is the decoder\'s, whole');
  issues[2].up += 1000; issues[2].down += 1000;
  decoder.basePriceQueue.add(2);
  assert.equal(decoder.applyBasePrice(), 1);
  assert.deepEqual([decoder.master[2].limit_down10, decoder.master[2].limit_up10], [15140, 23140]);
  // A file that does not line up with the decoder's issues is refused, not paired with the wrong codes.
  issues[2].up -= 1000; issues[2].down -= 1000;
  assert.throws(() => new Decoder(fake, { ...assets, 'master.dat': Buffer.concat([file.subarray(180), file.subarray(0, 180)]) },
    { protocolVersion: 18000 }), /disagrees with its master file on 3 of 3 issues \(issue 0: code "1301", file "167A"\)/);
  assert.throws(() => new Decoder(fake, assets, { protocolVersion: 17000 }), /layout for protocol 17000 is not known/);
});


// The book of `issue` as the stock view's quote has it, both ways: from the ladder, and through _getItaRows.
function bothBooks(decoder, issue) {
  const m = decoder.master[issue];
  if (!decoder.w._getStockView(decoder.id, issue, decoder.buf)) return null;
  const view = parseStockView(new Uint32Array(decoder.w.HEAPU8.buffer, decoder.buf, decoder.viewWords), decoder.ohlc, decoder.layoutShift);
  let walked;
  try { walked = decoder.walkBook(issue, m, view); } catch (error) { if (error.trap) return null; throw error; }
  return [decoder.board.book(issue, m, view), walked];
}

test('a quote\'s book is read from the decoder\'s ladder, as _getItaRows gives it, through the opening auction', { skip: !cache }, async () => {
  const assets = loadAssets(cache);
  const decoder = await Decoder.create(assets);
  assert.ok(decoder.board, decoder.boardSkipped);
  assert.deepEqual(decoder.board.calls, { lookup: [39, 34112], flag: 2556, rebuild: 218 });
  assert.deepEqual(decoder.board.layout, { map: 92, start: 104, size: 108, base: 0, market: 16 });
  const stream = frames(assets['ws.dat']);
  decoder.start(stream.next().value.data);
  let books = 0, levels = 0, closing = 0;
  for (let n = 0; n < 600; n++) {
    decoder.feed(stream.next().value.data);
    for (const issue of decoder.changed()) {
      const both = bothBooks(decoder, issue);
      if (!both) continue;
      assert.deepEqual(both[0], both[1], decoder.master[issue].code);
      books++; levels += both[0].bid_levels.length + both[0].ask_levels.length;
      closing += both[0].bid_closing_levels.length + both[0].ask_closing_levels.length;
    }
  }
  assert.ok(books > 30000 && levels > 300000 && closing > 0, `${books} books, ${levels} levels, ${closing} closing`);
});

test('a decoder reads books through _getItaRows when its ladder reader is off, gone wrong or given a bad value', { skip: !cache }, async () => {
  const assets = loadAssets(cache);
  const [on, off] = [await Decoder.create(assets), await Decoder.create(assets, { board: false })];
  assert.ok(on.board && !off.board);
  for (let issue = 0; issue < on.master.length; issue += 13) assert.deepEqual(on.quote(issue), off.quote(issue));
  // A check against _getItaRows that fails ends the reader, and the book read through _getItaRows is used.
  const toyota = on.master.findIndex(m => m.code === '7203');
  on.board.layout.market += 8;
  on.board.reads = 4095;   // the next read is checked
  const quote = on.quote(toyota);
  assert.equal(on.board, null);
  assert.match(on.boardSkipped, /its book of 7203 differed from _getItaRows' after 4096 reads/);
  assert.deepEqual(quote, off.quote(toyota));
  // A quantity _getItaRows could not convert is a trap here as there.
  const fresh = await Decoder.create(assets), board = fresh.board;
  const p = board.board(toyota);
  new Float64Array(fresh.w.HEAPU8.buffer)[(p + board.layout.market + 8) >> 3] = -5;
  assert.throws(() => fresh.quote(toyota), error => error.trap === true && /float unrepresentable/.test(error.message));
});

test('the board kernel embedded is board.wat assembled, and boardCalls finds what _getItaRows calls', () => {
  const kernel = Buffer.from(BOARD_KERNEL, 'base64');
  assert.throws(() => boardCalls(kernel), /_getItaRows or the invoke imports not found/);
  const wat2wasm = spawnSync('wat2wasm', ['--version']);
  if (wat2wasm.error) return;   // WABT not installed: the bytes stand as committed
  const out = path.join(os.tmpdir(), `board-${process.pid}.wasm`);
  const built = spawnSync('wat2wasm', [path.join(__dirname, '../../briskapi/decoder/board.wat'), '-o', out]);
  assert.equal(built.status, 0, String(built.stderr));
  assert.deepEqual(fs.readFileSync(out), kernel);
  fs.rmSync(out);
});

test('a snapshot that restores the whole day\'s trades so far (millions, after the open) is taken whole', { skip: !cache }, async () => {
  const decoder = await Decoder.create(loadAssets(cache));
  const before = decoder.restored.length, restored = Array.from({ length: 500000 }, (_, k) => ({ k }));
  decoder.qrm.take = () => restored;   // what the decoder recorded while a day's snapshot loaded
  decoder.unserialize(Buffer.alloc(0));
  assert.equal(decoder.restored.length, before + restored.length);
});

test('trade records: an issue\'s count and last price, withdrawals as before, and only a few records kept', () => {
  const qrm = new QuoteHistory();
  qrm.initialize(1); qrm.initialize(2);
  for (let k = 1; k <= 200; k++) qrm.emplace(1, 1, k, 1000 + k, 1, 100, k);
  assert.equal(qrm.size(1), 200);
  assert.equal(qrm.lastPrice10(1), 1200);
  qrm.popBack(1);   // still pending: withdrawn from it
  assert.deepEqual([qrm.size(1), qrm.lastPrice10(1), qrm.pending.length, qrm.withdrawn], [199, 1199, 199, 0]);
  assert.equal(qrm.take().length, 199);
  for (let k = 1; k <= 100; k++) qrm.emplace(1, 1, k, 2000 + k, 1, 100, k);
  const records = qrm.qrs.get(1).records;
  assert.ok(records.filter(r => r.round < qrm.taken).length <= 64 && records.length <= 164, 'taken records are let go');
  // Withdrawing past the records kept: they were taken, so each is a withdrawal of a recorded trade.
  for (let k = 0; k < 150; k++) qrm.popBack(1);
  assert.deepEqual([qrm.size(1), qrm.lastPrice10(1)], [149, 1149]);
  assert.equal(qrm.pending.length, 0);
  assert.equal(qrm.withdrawn, 50);
  qrm.popBack(2);   // nothing to withdraw
  assert.deepEqual([qrm.size(2), qrm.lastPrice10(2), qrm.withdrawn], [0, 0, 50]);
  qrm.clone(3, 1);
  assert.deepEqual([qrm.size(3), qrm.lastPrice10(3)], [149, 1149]);
  qrm.emplace(2, 2, 1, 500, 1, 100, 1);
  qrm.compact();
  qrm.deinitialize(2);
  assert.equal(qrm.pending.length, 0);
});

test('a quote worker\'s history keeps each issue\'s count and last price, and no trade records', () => {
  const full = new QuoteHistory(), lean = new QuoteHistory(false);
  for (const qrm of [full, lean]) {
    qrm.initialize(1);
    for (let k = 1; k <= 2000; k++) qrm.emplace(1, 1, k, 1000 + k, 1, 100, k);
    qrm.popBack(1);   // a withdrawn trade: the count and the last price both step back
    qrm.emplace(1, 2, 3000, 7777, 1, 100, 3000);
  }
  assert.deepEqual([lean.size(1), lean.lastPrice10(1)], [full.size(1), full.lastPrice10(1)], 'what the decoder asks of it');
  assert.deepEqual([lean.size(1), lean.lastPrice10(1)], [2000, 7777]);
  assert.equal(lean.pending.length, 0);
  assert.equal(lean.qrs.get(1).records.length, 0, 'no record of any trade');
  assert.deepEqual([lean.take().length, lean.withdrawn], [0, 0]);
  lean.clone(2, 1);
  assert.deepEqual([lean.size(2), lean.lastPrice10(2)], [2000, 7777]);
  assert.equal(full.take().length, 2000, 'the host\'s own history still reports its trades');
});

test('book changes replay to every level of all four books, the lists their best ten', { skip: !cache }, async () => {
  const assets = loadAssets(cache);
  const decoder = await Decoder.create(assets, { bookChanges: true });
  const plain = await Decoder.create(assets);
  const stream = frames(assets['ws.dat']), first = stream.next().value.data;
  decoder.start(first);
  plain.start(first);
  const BOOKS = ['bid_levels', 'ask_levels', 'bid_closing_levels', 'ask_closing_levels'];
  const TOTALS = ['bid_total', 'ask_total', 'bid_closing_total', 'ask_closing_total'];
  const books = new Map();
  let quotes = 0, removed = 0, deep = 0;
  const check = (q, same) => {
    quotes++;
    assert.deepEqual({ ...q, book_changes: undefined }, { ...same, book_changes: undefined }, 'the quote is the plain decoder\'s');
    const book = books.get(q.issue_id) ?? BOOKS.map(() => new Map());
    if (!books.has(q.issue_id)) assert.ok(q.book_changes.every(([, , n]) => n), 'a first quote holds its whole book');
    books.set(q.issue_id, book);
    let last = null;
    for (const [k, p, n, c] of q.book_changes) {
      assert.ok(!last || k > last[0] || (k === last[0] && (k % 2 === 0 ? p < last[1] : p > last[1])), 'each book best first');
      last = [k, p];
      if (n) book[k].set(p, [n, c]);
      else { assert.ok(book[k].delete(p) && c === 0, 'an emptied level the book had'); removed++; }
    }
    BOOKS.forEach((name, k) => {
      const levels = [...book[k]].map(([p, [n, c]]) => [p, n, c]).sort((x, y) => (k % 2 === 0 ? y[0] - x[0] : x[0] - y[0]));
      assert.deepEqual(levels.slice(0, 10), q[name], name);
      deep += levels.length > 10;
      assert.equal(levels.reduce((sum, level) => sum + level[1], 0), q[`${TOTALS[k]}_quantity`], `${name}: every share on the board`);
      assert.equal(levels.reduce((sum, level) => sum + level[2], 0), q[`${TOTALS[k]}_orders`], `${name}: every order`);
    });
  };
  const all = decoder.master.map(m => m.issue_id);
  const pair = (a, b) => a.quotes.forEach((q, i) => check(q, b.quotes[i]));
  pair(decoder.quotes(all), plain.quotes(all));
  let n = 0;
  for (const frame of stream) {
    decoder.feed(frame.data);
    plain.feed(frame.data);
    const changed = decoder.changed();
    assert.deepEqual(changed, plain.changed());
    pair(decoder.quotes(changed), plain.quotes(changed));
    if (++n === 3000) break;   // the opening's first 30 seconds: every security opens or is in its auction
  }
  assert.ok(quotes > 100000 && removed > 1000 && deep > 10000, JSON.stringify({ quotes, removed, deep }));
  const toyota = decoder.master.find(m => m.code === '7203').issue_id;
  assert.ok([...books.get(toyota)[0].keys()].length > 500, 'Toyota\'s bids before the open: hundreds of levels');
});

test('book changes are the same from the board kernel and from the _getItaRows walk, and across a switch between them', { skip: !cache }, async () => {
  const assets = loadAssets(cache);
  const create = () => Decoder.create(assets, { bookChanges: true });
  const [walk, kernel, off, on] = await Promise.all([create(), create(), create(), create()]);
  const stream = frames(assets['ws.dat']), first = stream.next().value.data;
  for (const d of [walk, kernel, off, on]) d.start(first);
  for (const d of [walk, on]) [d.board, d.boardRetry] = [null, null];   // the walk only (on: until it is found)
  assert.ok(kernel.board && off.board);
  const all = walk.master.map(m => m.issue_id);
  const read = ids => [walk, kernel, off, on].map(d => d.quotes(ids).quotes);
  const same = ([w, ...others]) => others.forEach(o => assert.deepEqual(o, w));
  same(read(all));
  let n = 0;
  for (const frame of stream) {
    for (const d of [walk, kernel, off, on]) d.feed(frame.data);
    const changed = walk.changed();
    for (const d of [kernel, off, on]) assert.deepEqual(d.changed(), changed);
    same(read(changed));
    if (++n === 300) {
      off.board = null;   // its last ladders, kept in the decoder's memory, are compared in JavaScript from now on
      assert.ok(on.discoverBoard());   // those it kept in arrays go into the decoder's memory for the kernel
    }
    if (n === 900) break;
  }
  assert.ok(kernel.board && on.board && !off.board && !walk.board);
});
