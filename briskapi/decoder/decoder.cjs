'use strict';
// Adapter for the pinned Next demo's public WASM ABI, not a guessed wire decoder.
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const crypto = require('node:crypto');
const { once } = require('node:events');
const { setTimeout: delay } = require('node:timers/promises');
const manifest = require('./assets.json');
const { verifyAsset, loadWebAssets } = require('./web.cjs');
const BUFFER = 4 * 1024 * 1024;
const LEVELS = 10;   // price levels kept on each side of a quote's book
// With bookChanges, the most levels a side is read to, all its nonempty ones (a side spans fewer ticks than this between
// its daily limits); the quote keeps LEVELS and says what changed in book_changes.
const MAX_LEVELS = 65536;
// The books of book_changes, by their index in it: bid, ask, and those of the closing-condition orders.
const BOOKS = ['bid_levels', 'ask_levels', 'bid_closing_levels', 'ask_closing_levels'];
const WINDOW = 64;   // book rows either side of the best prices read per call (a call costs the same for 1 row or 400)
const HISTORY = 50000;   // trades per history batch (those a snapshot restores)
const BOARD_CHECK_EVERY = 4096;   // ladder reads between checks against _getItaRows (see BoardReader)
const BOARD_RETRY_EVERY = 20000;  // books read before a BoardReader is looked for again, while no board had orders
// board.wat, assembled (wat2wasm): the board totals summed in the decoder's memory, its book read whole (compact) and
// compared with the last read (diff).
const BOARD_KERNEL = 'AGFzbQEAAAABHQNgBH9/f38AYAd/f39/f39/AX9gBn9/f398fwF/Ag8BA2VudgZtZW1vcnkCAAEDBQQAAQIBBxgDA3N1bQAAB2NvbXBhY3QAAQRkaWZmAAMKkQ0EswMEBn8BfAR7BH9BfyEPQX8hEEF/IRFBfyESAkADQCAEIAJPDQEgASAEaiEFQTggBUE4cGshBiAGIAIgBGtLBEAgAiAEayEGCyAAIAVBOG5BAnRqKAIAIAVBOHBByABsaiEHIAcgBkHIAGxqIQggBCEJA0AgCyAH/QADCP3wASELIAwgB/0AAyj98AEhDCANIAf9XQIY/ckB/c4BIQ0gDiAH/V0COP3JAf3OASEOIAcrAwghCiAJIA8gCkQAAAAAAAAAAGQgCkQAAAAAAAAAAGNyGyEPIAcrAxAhCiAJIBAgCkQAAAAAAAAAAGQgCkQAAAAAAAAAAGNyGyEQIAcrAyghCiAJIBEgEUEASCAKRAAAAAAAAAAAZCAKRAAAAAAAAAAAY3JxGyERIAcrAzAhCiAJIBIgEkEASCAKRAAAAAAAAAAAZCAKRAAAAAAAAAAAY3JxGyESIAlBAWohCSAHQcgAaiEHIAcgCEkNAAsgBCAGaiEEDAALCyADIAv9CwMAIAMgDP0LAxAgAyAN/QsDICADIA79CwMwIAMgDzYCQCADIBA2AkQgAyARNgJIIAMgEjYCTAuiBQQLfwR8BHsEf0F/IRpBfyEbQX8hHEF/IR0gBCAFQQJ0aiEOIA4gBUEFdGohDwJAA0AgByACTw0BIAEgB2ohCEE4IAhBOHBrIQkgCSACIAdrSwRAIAIgB2shCQsgACAIQThuQQJ0aigCACAIQThwQcgAbGohCiAKIAlByABsaiELIAchDANAIBYgCv0AAwj98AEhFiAXIAr9AAMo/fABIRcgGCAK/V0CGP3JAf3OASEYIBkgCv1dAjj9yQH9zgEhGSAKKwMIIRIgCisDECEUIAorAyghEyAKKwMwIRUgDCAaIBJEAAAAAAAAAABkIBJEAAAAAAAAAABjchshGiAMIBsgFEQAAAAAAAAAAGQgFEQAAAAAAAAAAGNyGyEbIAwgHCAcQQBIIBNEAAAAAAAAAABkIBNEAAAAAAAAAABjcnEbIRwgDCAdIB1BAEggFUQAAAAAAAAAAGQgFUQAAAAAAAAAAGNycRshHSASRAAAAAAAAAAAZCASRAAAAAAAAAAAY3IgE0QAAAAAAAAAAGQgE0QAAAAAAAAAAGNyciAURAAAAAAAAAAAZCAURAAAAAAAAAAAY3IgFUQAAAAAAAAAAGQgFUQAAAAAAAAAAGNycnIEQCAEIA1BAnRqIAMgDGo2AgAgDiANQQV0aiEQIBAgEjkDACAQIBM5AwggECAUOQMQIBAgFTkDGCAPIA1BBHRqIREgESAKKAIYNgIAIBEgCigCODYCBCARIAooAhw2AgggESAKKAI8NgIMIA1BAWohDQsgDEEBaiEMIApByABqIQogCiALSQ0ACyAHIAlqIQcMAAsLIAYgFv0LAwAgBiAX/QsDECAGIBj9CwMgIAYgGf0LAzAgBiAaNgJAIAYgGzYCRCAGIBw2AkggBiAdNgJMIA0LLwEBfyAAIAFBGGxqIQYgBiACNgIAIAYgAzYCBCAGIAQ5AwggBiAFNgIQIAFBAWoLhQQCDn8BeyAAIAFBAnRqIQ0gDSABQQV0aiEOIAMgBEECdGohDyAPIARBBXRqIRACQANAIAcgAk8gCCAFT3ENASAAIAdBAnRqKAIAQf////8HIAcgAkkbIQogAyAIQQJ0aigCAEH/////ByAIIAVJGyELIA0gB0EFdGohESAOIAdBBHRqIRMgDyAIQQV0aiESIBAgCEEEdGohFCAKIAtGBEAgEf0AAwAgEv0AAwD91gEgEf0AAxAgEv0AAxD91gH9TiAT/QACACAU/QACAP3WAf1OIRUgFf3DAUUEQEEAIQwDQCARIAxBA3RqKwMAIBIgDEEDdGorAwBiIBMgDEECdGooAgAgFCAMQQJ0aigCAEdyBEAgBiAJIAogDCARIAxBA3RqKwMAIBMgDEECdGooAgAQAiEJCyAMQQFqIQwgDEEESQ0ACwsgB0EBaiEHIAhBAWohCAUgCiALSARAQQAhDANAIBEgDEEDdGorAwBEAAAAAAAAAABiBEAgBiAJIAogDCARIAxBA3RqKwMAIBMgDEECdGooAgAQAiEJCyAMQQFqIQwgDEEESQ0ACyAHQQFqIQcFQQAhDANAIBIgDEEDdGorAwBEAAAAAAAAAABiBEAgBiAJIAsgDEQAAAAAAAAAAEEAEAIhCQsgDEEBaiEMIAxBBEkNAAsgCEEBaiEICwsMAAsLIAkL';
// The order groups of a _getItaRows row (204 bytes): quantity (u64) and order count (u64) word offsets.
const GROUPS = {
  bid: { name: 'bid', buy: true, closing: false, quantity: 5, orders: 9 },
  bid_closing: { name: 'bid_closing', buy: true, closing: true, quantity: 16, orders: 20 },
  ask: { name: 'ask', buy: false, closing: false, quantity: 28, orders: 32 },
  ask_closing: { name: 'ask_closing', buy: false, closing: true, quantity: 39, orders: 43 },
};

function u64(words, i) {
  const value = words[i] + 4294967296 * words[i + 1];
  if (!Number.isSafeInteger(value)) throw new Error('Unsafe 64-bit quantity/timestamp');
  return value;
}

// A master read with the wrong layout can hold any bits; NaN keeps them for the host's layout check.
function looseU64(words, i) {
  const value = words[i] + 4294967296 * words[i + 1];
  return Number.isSafeInteger(value) ? value : NaN;
}

const head = data => `${data.length}-byte frame starting ${Buffer.from(data.subarray(0, 16)).toString('hex')}`;

// One call into the decoder, with its failures made readable. A C++ exception that isn't a
// std::exception (a parse error such as "Invalid Header") escapes as a bare number, the address of
// { vtable, message }. A WebAssembly trap, such as "float unrepresentable in integer range" (an order
// quantity gone negative, converted to an unsigned one), leaves the decoder's stack 16 bytes down;
// std::exceptions arrive as Errors. The stack is restored either way.
function call(w, what, fn) {
  const stack = w.stackSave?.();
  try {
    return fn();
  } catch (error) {
    if (stack !== undefined) w.stackRestore(stack);
    if (typeof error === 'number') throw new Error(`${what}: the decoder failed (${exceptionText(w.HEAPU8, error)})`);
    if (error?.name === 'RuntimeError') {
      throw Object.assign(new Error(`${what}: the decoder trapped (${error.message}); its state is inconsistent, as `
        + 'after a lost, repeated or reordered update or one applied without the snapshot under it'), { trap: true });
    }
    throw error;
  }
}

function exceptionText(heap, address) {
  const view = new DataView(heap.buffer);
  const text = address > 0 && address + 8 <= heap.length ? view.getUint32(address + 4, true) : 0;
  if (!text || text >= heap.length) return `C++ exception at ${address}`;
  const end = heap.indexOf(0, text);
  return Buffer.from(heap.subarray(text, Math.min(end < 0 ? heap.length : end, text + 200))).toString('latin1');
}

function* frames(data) {
  let previous = -1;
  for (let offset = 0; offset < data.length;) {
    if (offset + 8 > data.length) throw new Error('Truncated mock frame header');
    const time = data.readUInt32LE(offset);
    const size = data.readUInt32LE(offset + 4);
    if (!size || size > BUFFER || offset + 8 + size > data.length) throw new Error('Invalid mock frame length');
    if (time < previous) throw new Error('Mock replay times regressed');
    previous = time;
    yield { time, data: data.subarray(offset + 8, offset + 8 + size) };
    offset += 8 + size;
  }
}

// The decoder's QR records are its trades (歩み値), one per execution, kept by the host per stock object
// (`p` is the issue's _getStockPointer): type (1 buyer-initiated, 2 seller-initiated, 0 most opening
// auction prints), time in microseconds after JST midnight, price, quantity in lots of `lot` shares and
// the issue's frame number. The vendor's rollback/last-price callbacks need the whole history; new
// trades also wait in `pending` until Decoder.trades() takes them. A trade withdrawn (popped) after it
// was taken is counted, and ends the session there.
// An issue keeps the prices of all its records, for their count and the last one's price (also after one is
// withdrawn), but the records themselves only while one could still be withdrawn from `pending`, and a few more:
// a snapshot taken during the session restores the day's trades so far, millions, in every decoder.
// A quote worker's (`trades` false) reads no trades, so it keeps neither the records nor `pending`: only each issue's
// prices. A snapshot taken during the session restores the day's trades so far into every decoder, the workers' too,
// and a record is some 130 bytes: with millions of trades, gigabytes at once across the host's decoders.
class QuoteHistory {
  constructor(trades = true) { this.qrs = new Map(); this.pending = []; this.withdrawn = 0; this.taken = 0; this.trades = trades; }
  initialize(p) { this.qrs.set(p, { prices: [], records: [] }); }
  deinitialize(p) { this.qrs.delete(p); this.pending = this.pending.filter(trade => trade.p !== p); }
  clone(dst, src) { const q = this.qrs.get(src); this.qrs.set(dst, { prices: q.prices.slice(), records: q.records.slice() }); }
  emplace(p, type, timestamp, price10, qty, lot, frame) {
    const q = this.qrs.get(p);
    q.prices.push(price10);
    if (!this.trades) return;
    const trade = { p, type, timestamp, price10, qty, lot, frame, round: this.taken };
    q.records.push(trade);
    // Records are taken in order, so once the 48th is, the 47 before it are too: none can be in `pending`.
    if (q.records.length > 64 && q.records[47].round < this.taken) q.records = q.records.slice(48);
    this.pending.push(trade);
  }
  popBack(p) {
    const q = this.qrs.get(p);
    if (!q.prices.length) return;
    q.prices.pop();
    if (!this.trades) return;
    const trade = q.records.pop();   // none: one taken long ago
    const at = trade ? this.pending.lastIndexOf(trade) : -1;
    if (at >= 0) this.pending.splice(at, 1);
    else this.withdrawn++;
  }
  take() { const trades = this.pending; this.pending = []; this.taken++; return trades; }
  // After taking a snapshot's records, keep each issue's last few only.
  compact() {
    for (const q of this.qrs.values()) if (q.records.length > 16) q.records = q.records.slice(-16);
  }
  size(p) { return this.qrs.get(p).prices.length; }
  lastPrice10(p) { return this.qrs.get(p).prices.at(-1) || 0; }
}

function loadAssets(cache) {
  const assets = {};
  for (const name of Object.keys(manifest.assets)) {
    const data = fs.readFileSync(path.join(cache, name));
    assets[name] = verifyAsset(name, data);
  }
  return assets;
}

// Position of the stock view's fields, relative to a version's base. Protocol 16000 (the 2021
// demo, and the vendor client's own parse of it) puts the numeric security code first. 18000 (SBI's
// build of March 2026, whose code is text) drops that word, so every index is one lower: its
// _getStockView writes the same fields in the same order, and with the demo's snapshot all 4,131
// issues read the demo's words one lower. The fields and their order are otherwise identical. Any
// other version is refused rather than read at a guessed offset.
const STOCK_VIEW_SHIFT = { 16000: 0, 18000: -1 };

// What _pushSQ takes as its issue. The demo's build takes the security code as a number and looks
// it up; the 2026 builds take the issue index and read the code (167A too) from their own record.
// Given a code, a 2026 build attaches the special quote to whichever issue has that index.
const SQ_ISSUE = { 16000: 'code', 18000: 'index' };

function stockViewShift(protocolVersion) {
  if (!(protocolVersion in STOCK_VIEW_SHIFT)) {
    throw new Error(`Stock view layout for protocol ${protocolVersion} is not known (known: ${Object.keys(STOCK_VIEW_SHIFT).join(', ')})`);
  }
  return STOCK_VIEW_SHIFT[protocolVersion];
}

// `words` is the raw _getStockView buffer; `ohlc` the number of OHLC bars ahead of the quote fields.
// The imbalance sides are 1 when the sell quantity at that indicative price exceeds the buy quantity
// (or nothing matches), else 0. The reference price is the one the decoder's indicative matching
// starts from (see ARCHITECTURE.md); in continuous trading it is the last price.
function parseStockView(words, ohlc, shift) {
  const a = words.subarray(6 * ohlc + shift);
  return { frame: a[19], max_frame: a[20], source_time_us: u64(a, 17), last_price10: words[1 + shift],
    open_price10: words[2 + shift], bid_price10: a[7], ask_price10: a[8], indicative_price10: a[9],
    indicative_volume: u64(a, 10), indicative_imbalance_side: a[12], closing_indicative_price10: a[13],
    closing_indicative_volume: u64(a, 14), closing_indicative_imbalance_side: a[16], quote_flag: a[21],
    quote_side: a[22], special_quote_time_us: u64(a, 23), indicative_open_price10: a[32], reference_price10: a[35],
    volume: u64(a, 3), turnover10: u64(a, 5) };
}

// The decoder's own master records (_getStockMaster), in issue order. 16000 (the demo) is the vendor
// client's layout. 18000 widens the code from a number to eight bytes of text, which must hold codes
// such as 167A, so every later field is four bytes on (`shift`): found by running SBI's decoder of
// March 2026 (from pybrisk's research) on the master file it served, where all 4,445 records agree
// with the file on code, prices, lot size and issue type. Any other version is refused.
const MASTER_LAYOUT = { 16000: { size: 144, shift: 0 }, 18000: { size: 148, shift: 4 } };

// The 2026 decoders take an eighth _initialize argument: the session schedule, which the
// vendor client builds with _createSchedule from the boot response's schedule_info. Without
// it they keep no price bars (_ohlcLength 0), their stock views can't be read and a snapshot fails
// ("deque"); a snapshot whose bar count differs from the schedule's is refused ("Invalid
// unserialize"). The arguments are schedule_info's fields in the boot's own order, as integers in
// microseconds after midnight; _createSchedule counts (morning close - open) / 5 min + (afternoon
// close - open) / 5 min bars (66 for 9:00-11:30 and 12:30-15:30). sq_jump_interval comes in seconds
// (180), but the decoder adds it to a special quote's time in microseconds, as the demo's build
// does with its fixed 180,000,000, so it is passed in microseconds.
const SCHEDULE_FIELDS = ['morning_session_pre_open_time', 'morning_session_open_time', 'morning_session_close_time',
  'afternoon_session_pre_open_time', 'afternoon_session_open_time', 'afternoon_session_pre_close_time',
  'afternoon_session_close_time', 'sq_jump_interval'];

function scheduleArgs(schedule) {
  if (schedule === null || typeof schedule !== 'object') throw new Error('The boot response has no schedule_info, which this decoder needs');
  // _createSchedule truncates each value to an unsigned integer: a negative or missing one traps the WASM.
  const missing = SCHEDULE_FIELDS.filter(key => !Number.isFinite(schedule[key]) || schedule[key] < 0);
  if (missing.length) throw new Error(`The boot schedule_info has no usable ${missing.join(', ')}`);
  if (!(schedule.sq_jump_interval >= 1 && schedule.sq_jump_interval <= 3600)) {
    throw new Error(`The boot schedule_info's sq_jump_interval (${schedule.sq_jump_interval}) is not a number of seconds`);
  }
  return SCHEDULE_FIELDS.map(key => (key === 'sq_jump_interval' ? schedule[key] * 1e6 : schedule[key]));
}

function masterLayout(protocolVersion) {
  if (!(protocolVersion in MASTER_LAYOUT)) {
    throw new Error(`Master layout for protocol ${protocolVersion} is not known (known: ${Object.keys(MASTER_LAYOUT).join(', ')})`);
  }
  return MASTER_LAYOUT[protocolVersion];
}

// One record at byte `at` of `heap`. Past the code, field indices are the demo's.
// The name never runs past its record, however a misread length says.
function readMasterRecord(heap, at, issue_id, { size, shift }) {
  const a = new Uint32Array(heap.buffer, heap.byteOffset + at + shift, 36);
  const code = shift ? Buffer.from(heap.subarray(at, at + 8)).toString('latin1').replace(/\0[^]*$/, '') : String(a[0]);
  return { issue_id, code, tick_type: a[1], base_price10: a[2], limit_up10: a[3], limit_down10: a[4],
    lot_size: looseU64(a, 5), issue_type: a[7],
    name: Buffer.from(heap.subarray(at + shift + 68, at + Math.min(size, shift + 68 + a[16]))).toString('utf8') };
}

// The master file the decoder loads: fixed 180-byte records in issue order, after a preamble (444
// bytes in the 2021 demo, none in SBI's of 2026-03-11). A 16-byte binary header (a word of unknown
// use, then base price, upper and lower limit in tenths of a yen) and fixed-width text: issue type at
// 40, code at 50 (five wide), lot size right-aligned up to 139. pybrisk's research documents the
// header and the code; the rest agrees with the vendor decoder's own master on every issue of both
// files. Names and tick types are not read here: the decoder's differ (it keeps "Ｕ－ＮＥＸＴ" whole
// but drops a market prefix such as "Ｍ－", and SBI's tick types follow no single column).
const MASTER_RECORD = 180;

function parseMasterFile(data, count) {
  const start = data.length - count * MASTER_RECORD;
  if (!(count > 0) || start < 0) throw new Error(`A master file of ${data.length} bytes cannot hold ${count} issues`);
  const text = (r, from, to) => r.toString('latin1', from, to).trim();
  const integer = value => (/^\d+$/.test(value) ? Number(value) : NaN);
  return Array.from({ length: count }, (_, issue_id) => {
    const r = data.subarray(start + MASTER_RECORD * issue_id, start + MASTER_RECORD * (issue_id + 1));
    // The first word is the record's message tag (0x18) and flags; bits 25-26 are the tick type.
    return { issue_id, code: text(r, 50, 55), tick_type: (r.readUInt32LE(0) >>> 25) & 3, base_price10: r.readUInt32LE(4),
      limit_up10: r.readUInt32LE(8), limit_down10: r.readUInt32LE(12), lot_size: integer(text(r, 124, 139)),
      issue_type: integer(text(r, 40, 44)) };
  });
}

// The decoder's master must agree with the file it loaded. A record layout that no longer fits a
// build shows here as a disagreement, and is refused rather than read.
const CHECKED = ['code', 'tick_type', 'base_price10', 'limit_up10', 'limit_down10', 'lot_size', 'issue_type'];

function checkMasterFile(master, data) {
  const file = parseMasterFile(data, master.length);
  const wrong = master.filter((m, i) => CHECKED.some(key => m[key] !== file[i][key]));
  if (wrong.length) {
    const [m] = wrong, f = file[m.issue_id];
    const key = CHECKED.find(k => m[k] !== f[k]);
    throw new Error(`The decoder's master disagrees with its master file on ${wrong.length} of ${master.length} issues `
      + `(issue ${m.issue_id}: ${key} ${JSON.stringify(m[key])}, file ${JSON.stringify(f[key])}); its record layout may have changed`);
  }
}

class Decoder {
  // options.protocolVersion: 16000 for the Next demo (default), 18000 for the brokers' BRiSK.
  // options.callbacks: { send(Buffer), heartbeat(ns BigInt), frameNumbers(Array), basePrice(issueIndex),
  //   authError(), marketFinished() }, the vendor client's six WASM callbacks.
  // options.keepTrades: false for a decoder whose trades nothing reads (a quote worker's): see QuoteHistory.
  // options.bookChanges: each quote reads every level of its four books and says, in book_changes, which differ from
  //   its issue's previous quote read by this decoder (see changes()). For the decoders whose quotes a recording keeps:
  //   a decoder whose reads go anywhere else (the fast lane's) would leave the changes of the next recorded one short.
  // options.boot: { exceptional_sq, base_prices } from the app boot response, applied in the
  //   vendor client's order: after the master is loaded, before the snapshot.
  // options.checkMaster(master): throws if the master read does not look like a market, before
  //   anything else uses it. Without it, a lot size beyond 2^53 is an error. Either way the
  //   master is then checked against the master file.
  static async create(assets, options = {}) {
    // Isolate the legacy glue's globals and process exception handlers. No UI,
    // network, filesystem access or account state is needed inside this VM.
    const context = vm.createContext({
      module: { exports: {} }, exports: {}, qrm: new QuoteHistory(options.keepTrades !== false),
      console: { log() {}, warn() {}, error() {} },
      crypto: crypto.webcrypto, TextDecoder, TextEncoder, setTimeout, clearTimeout,
    });
    vm.runInContext(assets['fita.js'].toString('utf8'), context, { filename: 'brisk-demo-fita.js' });
    // The memory the glue would make (its import's size), made here, so that BoardReader can share it.
    let memory;
    try { memory = new WebAssembly.Memory(moduleOf(assets['fita.wasm']).memory); } catch { memory = undefined; }
    // A special quote the decoder can't place is reported only on stderr, so that line is counted.
    const stderr = { invalidIssueCode: 0 };
    const { wasm } = await new Promise((resolve, reject) => {
      context.module.exports({ wasmBinary: assets['fita.wasm'], ...(memory && { wasmMemory: memory }), print() {},
        printErr: line => { if (String(line).includes('Invalid Issue Code')) stderr.invalidIssueCode++; }, onAbort: reject,
      }).then(wasm => resolve({ wasm }));
    });
    options.check?.(wasm);
    const decoder = new Decoder(wasm, assets, { ...options, stderr, qrm: context.qrm });
    if (options.board !== false) {
      Object.assign(decoder, { memory, wasmBytes: assets['fita.wasm'] });
      decoder.discoverBoard();
    }
    return decoder;
  }

  // options.schedule: the boot's schedule_info. Used only by decoders that take one (the 2026
  //   builds export _createSchedule; the 2021 demo's doesn't); null there is an error.
  constructor(w, assets, { protocolVersion = manifest.protocol_version, callbacks = {}, boot = {}, checkMaster, schedule,
    stderr = { invalidIssueCode: 0 }, qrm = new QuoteHistory(), bookChanges = false } = {}) {
    this.w = w;
    this.bookChanges = bookChanges;
    this.levelLimit = bookChanges ? MAX_LEVELS : LEVELS;
    this.books = new Map();   // with bookChanges, each issue's book as its last quote read it (a ladder: see changes())
    this.prices = [];         // by tick type, the price of each tick index (0: not looked up yet): see priceOf()
    this.stderr = stderr;
    this.board = null;         // the BoardReader that reads books from the ladder, if it could be used
    this.boardSkipped = null;  // why it is not (or no longer)
    this.boardRetry = null;    // books read since it was last looked for, while that might still succeed
    this.qrm = qrm;
    this.stocks = new Map();   // stock object (QR container) -> issue
    this.restored = [];        // QR records a snapshot restored
    this.layoutShift = stockViewShift(protocolVersion);
    this.layout = masterLayout(protocolVersion);
    this.sqIssue = SQ_ISSUE[protocolVersion];
    this.basePriceQueue = new Set();
    this.unreadable = new Map();   // issue -> frame number at which its quote trapped
    this.authError = false;
    this.marketFinished = false;
    this.initialFrames = null;
    // A schedule is created before _initialize copies it in; the 2021 demo's build takes none.
    this.scheduled = typeof w._createSchedule === 'function' && schedule !== undefined;
    const scheduleArg = this.scheduled ? [w._createSchedule(...scheduleArgs(schedule))] : [];
    this.lastHeartbeat = null;
    // The signatures are the vendor client's own (an indirect call with another type traps).
    const add = (fn, sig) => w.addFunction(fn, sig);
    this.id = w._initialize(
      // send(id, ptr, len): bytes the client must write to the server. The WASM decides
      // when (the vendor client has no ping timer of its own), so a host must forward them.
      add((id, ptr, len) => callbacks.send?.(Buffer.from(w.HEAPU8.slice(ptr, ptr + len))), 'viii'),
      // updateNumber(id, ptr, count): the first frame number of every issue on the stream.
      add((id, p, count) => {
        this.initialFrames = Array.from(new Uint32Array(w.HEAPU8.buffer, p, count));
        callbacks.frameNumbers?.(this.initialFrames);
      }, 'viii'),
      // heartbeat(id, low, high): the server's clock, nanoseconds since the Unix epoch.
      add((id, low, high) => {
        this.lastHeartbeat = BigInt(low >>> 0) + (BigInt(high >>> 0) << 32n);
        callbacks.heartbeat?.(this.lastHeartbeat);
      }, 'viii'),
      // basePrice(id, issueIndex): the decoder changed an issue's base price or limits.
      add((id, issueIndex) => { this.basePriceQueue.add(issueIndex); callbacks.basePrice?.(issueIndex); }, 'vii'),
      add(() => { this.authError = true; callbacks.authError?.(); }, 'vii'),
      add(() => { this.marketFinished = true; callbacks.marketFinished?.(); }, 'vi'),
      protocolVersion, ...scheduleArg);
    this.buf = w._malloc(BUFFER);
    this.aux = w._malloc(64);
    this.push(assets['master.dat']);
    w._applyBasePriceQueue(this.id);
    const count = w._stockCount(this.id);
    if (count <= 0 || count * this.layout.size > BUFFER) throw new Error('Invalid master count');
    w._getStockMaster(this.id, this.buf, count * this.layout.size);
    this.master = Array.from({ length: count },
      (_, issue_id) => readMasterRecord(w.HEAPU8, this.buf + this.layout.size * issue_id, issue_id, this.layout));
    if (checkMaster) checkMaster(this.master);
    else if (this.master.some(m => Number.isNaN(m.lot_size))) throw new Error('Unsafe 64-bit quantity/timestamp');
    checkMasterFile(this.master, assets['master.dat']);
    this.boot = this.applyBoot(boot);
    this.unserialize(assets['snapshot.dat']);
    this.date = String(w._getDate(this.id));
  }

  // The vendor client pushes the boot response's exceptional special quotes and base-price
  // changes into the decoder right after the master, then re-reads the master's limits.
  applyBoot({ exceptional_sq = [], base_prices = [] } = {}) {
    const byCode = new Map(this.master.map(m => [m.code, m.issue_id]));
    const number = (value, what) => {
      if (!Number.isFinite(value)) throw new Error(`Boot ${what} is not a number: ${JSON.stringify(value)}`);
      return value;
    };
    // A special quote names its issue by code or by index (SQ_ISSUE). One whose code is not in the
    // master, or by code isn't a number, is left out and listed: the decoder would drop it or misplace it.
    const skipped = [];
    const rejectedBefore = this.stderr.invalidIssueCode;
    for (const sq of exceptional_sq) {
      const code = String(sq.issue_code);
      const issue = byCode.get(code);
      if (issue === undefined || (this.sqIssue === 'code' && !/^\d+$/.test(code))) {
        skipped.push(code);
        continue;
      }
      this.w._pushSQ(this.id, this.sqIssue === 'index' ? issue : Number(code), sq.buy_sell === 'B' ? 0 : 1, 10 * number(sq.jump_range, 'jump_range'),
        number(sq.jump_sec, 'jump_sec'), 10 * number(sq.quote_limit_down, 'quote_limit_down'),
        10 * number(sq.quote_limit_up, 'quote_limit_up'));
    }
    // Every special quote sent names an issue in the master. If the decoder still can't place one,
    // this build names issues another way, and the others may sit on the wrong issue.
    const rejected = this.stderr.invalidIssueCode - rejectedBefore;
    if (rejected) {
      throw new Error(`The decoder rejected ${rejected} of the boot's special quotes as an invalid issue code; `
        + `this build may name a special quote's issue differently`);
    }
    for (const change of base_prices) {
      const issue = byCode.get(String(change.issue_code));
      if (issue === undefined) throw new Error(`Boot base_prices names issue code ${change.issue_code}, which is not in the master`);
      this.w._pushBasePrice(this.id, issue, number(change.base_price10, 'base_price10'),
        number(change.limit_down10, 'limit_down10'), number(change.limit_up10, 'limit_up10'));
      this.basePriceQueue.add(issue);
    }
    return { exceptional_sq: exceptional_sq.length - skipped.length, base_prices: base_prices.length, applied: this.applyBasePrice(),
      ...(skipped.length && { skipped_exceptional_sq: skipped }) };
  }

  // Re-read the master's price limits for issues whose base price changed (the vendor client
  // does this after boot and after every frame); cheap when nothing is queued.
  applyBasePrice() {
    if (!this.basePriceQueue.size) return 0;
    const { size } = this.layout;
    this.w._getStockMaster(this.id, this.buf, this.master.length * size);
    let applied = 0;
    for (const issue of this.basePriceQueue) {
      const m = this.master[issue];
      if (!m) throw new Error(`Base price change for unknown issue index ${issue}`);
      const { base_price10, limit_up10, limit_down10 } = readMasterRecord(this.w.HEAPU8, this.buf + size * issue, issue, this.layout);
      Object.assign(m, { base_price10, limit_up10, limit_down10 });
      applied++;
    }
    this.basePriceQueue.clear();
    return applied;
  }

  push(data) {
    for (let offset = 0; offset < data.length; offset += BUFFER) {
      const part = data.subarray(offset, offset + BUFFER);
      this.w.HEAPU8.set(part, this.buf);
      call(this.w, `Data pushed to the decoder (${data.length} bytes)`, () => this.w._push(this.id, this.buf, part.length));
    }
  }

  // Push the catch-up reply for `lagging` (from laggingIssues) and check what it did: each issue in
  // it must now be exactly at the stream's first frame, and every other issue where it was. The
  // decoder applies whatever it is given, so a reply pushed twice, a repeated or stray update or
  // one for other issues would otherwise pass, leaving quotes off by those updates.
  catchUp(reply, lagging) {
    const before = this.frameNumbers();
    this.push(reply);
    const after = this.frameNumbers();
    const behind = lagging.filter(({ issue_id, to }) => after[issue_id] < to);
    if (behind.length) {
      const [first] = behind;
      throw new Error(`Catch-up left ${behind.length} issues behind the stream (first: issue ${first.issue_id}, `
        + `frame ${after[first.issue_id]}, stream starts at ${first.to})`);
    }
    const to = new Map(lagging.map(l => [l.issue_id, l.to]));
    const moved = after.findIndex((frame, issue) => frame !== (to.has(issue) ? to.get(issue) : before[issue]));
    if (moved >= 0) {
      throw new Error(`Catch-up took issue ${moved} to frame ${after[moved]}, but ${to.has(moved)
        ? `the stream starts at ${to.get(moved)}` : `it was not behind (frame ${before[moved]})`}: the reply repeats or misplaces updates`);
    }
  }

  unserialize(data) {
    let start = 0, size = 0, first = 0, count = 0;
    const flush = () => {
      this.w.HEAPU8.set(data.subarray(start, start + size), this.buf);
      call(this.w, 'The snapshot', () => this.w._unserialize(this.id, this.buf, size, first, first + count));
      start += size; first += count; size = 0; count = 0;
    };
    for (let offset = 0; offset < data.length;) {
      if (offset + 4 > data.length) throw new Error('Truncated snapshot header');
      const length = data.readUInt32LE(offset);
      if (length < 4 || length > BUFFER || offset + length > data.length) throw new Error('Invalid snapshot record');
      if (size + length > BUFFER) flush();
      size += length; count++; offset += length;
    }
    if (size) flush();
    // Trades a snapshot restores (a live session's could be the whole day so far) predate the stream;
    // its quotes already count them. They are kept apart, for history().
    // One at a time: a snapshot taken during the session restores the day's trades so far, millions of them, more
    // than a call can take as arguments.
    for (const trade of this.qrm.take()) this.restored.push(trade);
    this.qrm.compact();
  }

  // Bars of OHLC history ahead of the quote fields in a stock view. SBI's build
  // reports it once its stream is initialized; the demo's comes from the manifest.
  get ohlc() {
    this._ohlc ??= typeof this.w._ohlcLength === 'function' ? this.w._ohlcLength(this.id) : manifest.ohlc_length;
    return this._ohlc;
  }

  get viewWords() {
    return 37 + 6 * this.ohlc;
  }

  time() {
    this.w._getTime(this.id, this.aux);
    return u64(new Uint32Array(this.w.HEAPU8.buffer, this.aux, 2), 0);
  }

  // _pushWs writes the latest source time of the updates it applied (u64 at aux) and how many it
  // applied (u32 at aux + 8): none for pings, clock messages and frames held until begin().
  feed(data) {
    if (data.length > BUFFER) throw new Error('Frame exceeds decoder buffer');
    this.w.HEAPU8.set(data, this.buf);
    call(this.w, `A ${head(data)}`, () => this.w._pushWs(this.id, this.buf, data.length, this.aux, this.aux + 8));
    this.w._applyBasePriceQueue(this.id);
    if (this.authError) throw new Error('Decoder rejected session');
    if (this.began && data[0] & 1) this.watch(data);
  }

  // After begin(), every frame of stock updates (odd first byte) applies at least one: so it is through
  // the demo's recording and SBI's. One that applies none leaves its bytes held for the next frame;
  // when the frames after it apply none either, the decoder's parser has lost the messages' boundaries
  // and will decode nothing more, with no error or callback (nor ping replies). In SBI's recording a
  // 15-byte frame does this every 40 s, though the browser's decoder kept going; whether a live
  // stream carries one is not known, so the session stops at once and names it.
  watch(data) {
    if (new DataView(this.w.HEAPU8.buffer).getUint32(this.aux + 8, true)) {
      this.idleFrames = 0;
      if (++this.goodFrames >= 3) this.suspect = null;
      return;
    }
    this.goodFrames = 0;
    this.suspect ??= head(data);
    if (++this.idleFrames >= 2) {
      throw new Error(`The decoder stopped decoding after a ${this.suspect}: the frames since apply no update `
        + '(run with --trace-protocol to see what the server sent)');
    }
  }

  // Feed one frame; true once the decoder has reported its initial frame numbers
  // and quote tracing has started (the demo's first frame always does this).
  start(frame) {
    this.feed(frame);
    if (!this.initialFrames) return false;
    if (this.laggingIssues().length) throw new Error('Snapshot needs unavailable catch-up data');
    this.begin();
    return true;
  }

  // Frame number of every issue as the decoder holds it now (the snapshot's, before catch-up).
  frameNumbers() {
    this.w._getFrameNumbers(this.id, this.buf, this.master.length);
    return Array.from(new Uint32Array(this.w.HEAPU8.buffer, this.buf, this.master.length));
  }

  // Issues whose snapshot is older than the stream's first frame: the ranges a live
  // client must fetch before the state is consistent. Empty for the demo's cut recording.
  laggingIssues() {
    if (!this.initialFrames) throw new Error('Frame numbers not received yet');
    if (this.initialFrames.length !== this.master.length) throw new Error('Missing frame-number bootstrap');
    const current = this.frameNumbers();
    const lagging = [];
    this.initialFrames.forEach((to, issue_id) => { if (current[issue_id] < to) lagging.push({ issue_id, from: current[issue_id], to }); });
    return lagging;
  }

  // Mark the API data complete and start tracing quote changes. The decoder then applies the frames
  // it held, skipping updates the snapshot already has.
  begin() {
    call(this.w, 'The frames received before the catch-up', () => this.w._apiRecieved(this.id));
    this.trace = this.w._addTraceUpdate(this.id);
    Object.assign(this, { began: true, idleFrames: 0, goodFrames: 0, suspect: null });
  }

  changed() {
    const count = this.w._getTrace(this.id, this.trace, this.buf, 8192);
    if (count < 0 || count > 8192) throw new Error('Decoder trace overflow');
    const ids = Array.from(new Uint16Array(this.w.HEAPU8.buffer, this.buf, count));
    this.w._clearTrace(this.id, this.trace);
    if (ids.some(id => id >= this.master.length)) throw new Error('Invalid trace issue ID');
    return ids;
  }

  // The trades recorded since the last call, in the decoder's order: those of `ids` (a Set), or all.
  trades(ids) {
    if (this.qrm.withdrawn) throw new Error(`The decoder withdrew ${this.qrm.withdrawn} trade(s) already recorded`);
    return this.issueTrades(this.qrm.take(), ids);
  }

  // The trades the snapshot restored, by issue (each issue's in the decoder's order), in batches of
  // `size`; taken once.
  history(ids, size = HISTORY) {
    const trades = this.issueTrades(this.restored, ids).sort((a, b) => a.issue_id - b.issue_id);
    this.restored = [];
    return Array.from({ length: Math.ceil(trades.length / size) }, (_, k) => trades.slice(k * size, (k + 1) * size));
  }

  issueTrades(records, ids) {
    const trades = [];
    for (const t of records) {
      // Stock objects are replaced when a snapshot loads, so the map is rebuilt when one is new.
      if (!this.stocks.has(t.p)) this.stocks = new Map(this.master.map(m => [this.w._getStockPointer(this.id, m.issue_id), m.issue_id]));
      const issue_id = this.stocks.get(t.p);
      if (issue_id === undefined) throw new Error('A trade for an unknown stock object');
      if (!Number.isSafeInteger(t.timestamp) || !(t.qty > 0) || !(t.lot > 0)) {
        throw new Error(`Trade not recognised: ${this.master[issue_id].code} at frame ${t.frame}`);
      }
      if (ids && !ids.has(issue_id)) continue;
      trades.push({ issue_id, code: this.master[issue_id].code, frame: t.frame, time_us: t.timestamp, price10: t.price10,
        quantity: t.qty * t.lot, side: t.type });
    }
    return trades;
  }

  // Quotes for `ids`. An issue whose quote traps the decoder (its state for that issue is inconsistent)
  // is skipped and listed, and not read again until its frame number moves: read again at the same
  // frame, the decoder returns stale derived fields instead of trapping.
  quotes(ids) {
    const quotes = [], skipped = [];
    let frames = null;
    const frame = id => (frames ??= this.frameNumbers())[id];
    for (const id of ids) {
      if (this.unreadable.has(id) && this.unreadable.get(id) === frame(id)) {
        skipped.push(id);
        continue;
      }
      try {
        quotes.push(this.quote(id));
        this.unreadable.delete(id);
      } catch (error) {
        if (!error.trap) throw error;
        this.unreadable.set(id, frame(id));
        skipped.push(id);
      }
    }
    return { quotes, skipped };
  }

  quote(issue_id) {
    const m = this.master[issue_id];
    if (!m) throw new Error(`Unknown issue ID: ${issue_id}`);
    return call(this.w, `The quote for ${m.code}`, () => this.readQuote(issue_id, m));
  }

  readQuote(issue_id, m) {
    if (!this.w._getStockView(this.id, issue_id, this.buf)) throw new Error(`Stock view unavailable: ${m.code}`);
    const words = new Uint32Array(this.w.HEAPU8.buffer, this.buf, this.viewWords);
    const quote = { issue_id, code: m.code, ...parseStockView(words, this.ohlc, this.layoutShift) };
    // The demo's build gives the issue status through _getPortfolio. SBI's has none; its view holds the
    // status one word after the demo's last, plus 8 (the same values once 8 is taken off).
    if (this.w._getPortfolio) {
      if (!this.w._getPortfolio(this.id, Number(m.code), this.buf)) throw new Error(`Portfolio unavailable: ${m.code}`);
      quote.issue_status = new Uint32Array(this.w.HEAPU8.buffer, this.buf, 22 + this.ohlc)[this.ohlc + 15];
    } else {
      quote.issue_status = words[6 * this.ohlc + this.layoutShift + 37] - 8;
      if (!(quote.issue_status >= 0 && quote.issue_status <= 7)) throw new Error(`Issue status unrecognised: ${m.code}`);
    }
    if (!this.bookChanges) return Object.assign(quote, this.book(issue_id, m, quote));
    const { book, ladder } = this.wholeBook(issue_id, m, quote);
    Object.assign(quote, book);
    quote.book_changes = this.changes(issue_id, ladder, m);
    return quote;
  }

  // With bookChanges: the quote's book (its lists the best LEVELS) and its ladder, every level that holds anything, as
  // BoardReader reads it whole or (when it cannot be used) the walk. A ladder is the levels' tick indices (ascending),
  // their four quantities and their four order counts (bid, ask, closing-condition bid and ask, as BOOKS), and how many.
  wholeBook(issue_id, m, quote) {
    if (!this.board && this.boardRetry !== null && ++this.boardRetry % BOARD_RETRY_EVERY === 0) this.discoverBoard();
    if (!this.board) return this.walkedWhole(issue_id, m, quote);
    const read = this.board.book(issue_id, m, quote, true);
    // Now and then checked against the decoder's own walk, every level of it: a difference ends it for this decoder.
    if (++this.board.reads % BOARD_CHECK_EVERY === 0) {
      const walked = this.walkedWhole(issue_id, m, quote);
      if (JSON.stringify(walked.book) !== JSON.stringify(read.book) || !sameLadder(walked.ladder, read.ladder)) {
        this.boardSkipped = `its book of ${m.code} differed from _getItaRows' after ${this.board.reads} reads`;
        this.board = null;
        return walked;
      }
    }
    return read;
  }

  walkedWhole(issue_id, m, quote) {
    const book = this.walkBook(issue_id, m, quote), at = new Map();
    BOOKS.forEach((name, k) => {
      for (const [price, quantity, orders] of book[name]) {
        const index = this.w._getTickPrice10ToIndex(this.id, m.tick_type, price);
        if (!at.has(index)) at.set(index, [0, 0, 0, 0, 0, 0, 0, 0]);
        at.get(index)[k] = quantity;
        at.get(index)[4 + k] = orders;
      }
      book[name] = book[name].slice(0, LEVELS);
    });
    const indices = [...at.keys()].sort((a, b) => a - b), n = indices.length;
    const ladder = { n, idx: Int32Array.from(indices), q: new Float64Array(4 * n), c: new Uint32Array(4 * n) };
    indices.forEach((index, j) => {
      const v = at.get(index);
      for (let k = 0; k < 4; k++) [ladder.q[4 * j + k], ladder.c[4 * j + k]] = [v[k], v[4 + k]];
    });
    return { book, ladder };
  }

  // The price of a tick index for this tick type: the lowest price _getTickPrice10ToIndex gives it, remembered.
  priceOf(m, index) {
    let cache = this.prices[m.tick_type];
    if (!cache || index >= cache.length) {
      const grown = new Int32Array(Math.max(index + 1, 2 * (cache?.length ?? 4096)));
      if (cache) grown.set(cache);
      this.prices[m.tick_type] = cache = grown;
    }
    let price = cache[index];
    if (!price) {
      let lo = 0, hi = 2 * m.limit_up10 + 1;   // a level with orders lies between the limits
      while (lo < hi) {
        const mid = Math.floor((lo + hi) / 2);
        if (this.w._getTickPrice10ToIndex(this.id, m.tick_type, mid) < index) lo = mid + 1; else hi = mid;
      }
      cache[index] = price = lo;
    }
    return price;
  }

  // The levels of a quote's four books that differ from its issue's previous quote read here, from their ladders (see
  // wholeBook()): [book, price10, quantity, orders], book the index in BOOKS, each book's best first, and 0 shares and
  // 0 orders for a level that emptied. An issue's first quote holds its whole book. Keeps the ladder for the next.
  changes(issue_id, ladder, m) {
    let before = this.books.get(issue_id);
    const byBook = [[], [], [], []];
    const emit = (k, index, value, orders) => byBook[k].push(value
      ? [k, this.priceOf(m, index), quantity(value, m), orders] : [k, this.priceOf(m, index), 0, 0]);
    const best = () => [...byBook[0].reverse(), ...byBook[1], ...byBook[2].reverse(), ...byBook[3]];
    const reader = ladder.ptr !== undefined ? this.board : null;
    if (reader) {   // read by the kernel: compared by it (diff), the last ladder kept in the decoder's memory
      if (before && before.ptr === undefined) before = reader.place(before);
      for (const [index, k, value, orders] of reader.changesOf(ladder, before)) emit(k, index, value, orders);
      this.books.set(issue_id, reader.keep(ladder, before));
      return best();
    }
    if (before && before.ptr !== undefined) {   // kept by a reader no longer used: its arrays, read here
      const buffer = this.w.HEAPU8.buffer;
      before = { n: before.n, idx: new Int32Array(buffer, before.ptr, before.n),
        q: new Float64Array(buffer, before.ptr + 4 * before.cap, 4 * before.n),
        c: new Uint32Array(buffer, before.ptr + 36 * before.cap, 4 * before.n) };
      this.books.delete(issue_id);
    }
    const { n, idx, q, c } = ladder, pn = before ? before.n : 0;
    for (let i = 0, j = 0; i < n || j < pn;) {
      const a = i < n ? idx[i] : Infinity, b = j < pn ? before.idx[j] : Infinity;
      if (a === b) {
        for (let k = 0; k < 4; k++) {
          if (q[4 * i + k] !== before.q[4 * j + k] || c[4 * i + k] !== before.c[4 * j + k]) emit(k, a, q[4 * i + k], c[4 * i + k]);
        }
        i++; j++;
      } else if (a < b) {
        for (let k = 0; k < 4; k++) if (q[4 * i + k]) emit(k, a, q[4 * i + k], c[4 * i + k]);
        i++;
      } else {
        for (let k = 0; k < 4; k++) if (before.q[4 * j + k]) emit(k, b, 0, 0);
        j++;
      }
    }
    let kept = this.books.get(issue_id);
    if (!kept || kept.idx.length < n) {
      const cap = Math.max(16, 2 * n);
      kept = { n: 0, idx: new Int32Array(cap), q: new Float64Array(4 * cap), c: new Uint32Array(4 * cap) };
      this.books.set(issue_id, kept);
    }
    kept.idx.set(idx.subarray(0, n));
    kept.q.set(q.subarray(0, 4 * n));
    kept.c.set(c.subarray(0, 4 * n));
    kept.n = n;
    return best();   // bids best first are the highest indices first; asks the lowest
  }

  // The book of a quote: the market orders, each order group's totals over the whole board, the quantity and
  // orders at the quote's bid and ask prices, and each side's best LEVELS prices that hold orders, best first, as
  // [price10, quantity, orders]: the bids from the top of the board down, the asks from the bottom up. The
  // closing-condition (引け条件付) limit orders make a book of their own, read the same way, where the totals
  // show any. Row indices step one tick between the issue's limits (_getTickPrice10ToIndex takes the tick type,
  // not the issue).
  //
  // _getItaRows(id, issue, first row index, rows, price rows, market row, OVER, UNDER, light) writes 204-byte
  // rows: the price rows asked for, the market row, whose buy and sell groups (quantity at +20 and +112) and
  // closing groups (+64, +156) are the market orders, and OVER and UNDER, each group summed over the rows above
  // and below those read. It always returns 1; a window off the board throws. light = 0 keeps the closing
  // groups. Every call sums the whole board for OVER and UNDER, which costs more than the rows it returns, so a
  // book is read in as few calls as it can be: one window from WINDOW rows below the bid to WINDOW above the
  // ask, with the closing groups, usually holds all of it (and with OVER and UNDER, the board's totals).
  // Use a BoardReader if one can be (see there); books are read through _getItaRows meanwhile. With no orders
  // on any board yet (a session started before the pre-open), it is tried again as books are read.
  discoverBoard() {
    try {
      this.board = BoardReader.discover(this, this.wasmBytes, this.memory);
      this.boardSkipped = this.boardRetry = null;
    } catch (error) {
      this.boardSkipped = error.message;
      this.boardRetry = error instanceof NoOrdersYet ? 0 : null;
    }
    return Boolean(this.board);
  }

  book(issue_id, m, quote) {
    if (!this.board && this.boardRetry !== null && ++this.boardRetry % BOARD_RETRY_EVERY === 0) this.discoverBoard();
    if (!this.board) return this.walkBook(issue_id, m, quote);
    const book = this.board.book(issue_id, m, quote);
    // Now and then the ladder read is checked against the decoder's own: a difference ends it for this decoder.
    if (++this.board.reads % BOARD_CHECK_EVERY === 0) {
      const walked = this.walkBook(issue_id, m, quote);
      if (JSON.stringify(walked) !== JSON.stringify(book)) {
        this.boardSkipped = `its book of ${m.code} differed from _getItaRows' after ${this.board.reads} reads`;
        this.board = null;
        return walked;
      }
    }
    return book;
  }

  // The book as _getItaRows gives it (see book()): the way every book was read before BoardReader, and is read
  // when it cannot be used.
  walkBook(issue_id, m, quote) {
    const index = price => this.w._getTickPrice10ToIndex(this.id, m.tick_type, price);
    const low = index(m.limit_down10), high = index(m.limit_up10);
    const inside = price => price >= m.limit_down10 && price <= m.limit_up10;
    const bidHint = inside(quote.bid_price10) ? index(quote.bid_price10) : high;
    const askHint = inside(quote.ask_price10) ? index(quote.ask_price10) : low;
    // A side far from the other (a wide spread, or no price on it) gets a window of its own when it needs one.
    const near = Math.abs(bidHint - askHint) <= 2 * WINDOW;
    const window = this.read(issue_id, Math.max(low, (near ? Math.min(bidHint, askHint) : bidHint) - WINDOW + 1),
      Math.min(high, (near ? Math.max(bidHint, askHint) : bidHint) + WINDOW - 1), true);
    const u = window.u, count = window.last - window.first + 1;
    const book = { market_buy_quantity: u64(u, 5), market_sell_quantity: u64(u, 28),
      closing_market_buy_quantity: u64(u, 16), closing_market_sell_quantity: u64(u, 39) };
    // Limit orders on the whole board, each group: OVER (words 51 on) + the rows read (153 on) + UNDER (102).
    const totals = {};
    for (const [name, group] of Object.entries(GROUPS)) {
      for (const [key, word] of [['quantity', group.quantity], ['orders', group.orders]]) {
        let total = u64(u, 51 + word) + u64(u, 102 + word);
        for (let k = 0; k < count; k++) total += u64(u, 153 + 51 * k + word);
        totals[`${name}_total_${key}`] = total;
      }
    }
    // The side's own window where the shared one does not hold its best price (or lacks its groups).
    const anchor = (hint, group) => (hint >= window.first && hint <= window.last ? window
      : group.buy ? this.read(issue_id, Math.max(low, hint - WINDOW + 1), hint, group.closing)
        : this.read(issue_id, hint, Math.min(high, hint + WINDOW - 1), group.closing));
    const limit = this.levelLimit;
    const side = (group, hint) => this.levels(issue_id, low, high, group, anchor(hint, group), limit);
    const bid_levels = side(GROUPS.bid, bidHint);
    const ask_levels = side(GROUPS.ask, askHint);
    const bid_closing_levels = totals.bid_closing_total_quantity ? side(GROUPS.bid_closing, bidHint) : [];
    const ask_closing_levels = totals.ask_closing_total_quantity ? side(GROUPS.ask_closing, askHint) : [];
    const at = (levels, price, group) => {
      const level = levels.find(([p]) => p === price);
      if (level) return level.slice(1);
      // A price the walk passed, or every order on the side having been read, means none there.
      const passed = levels.length < limit || (group.buy ? price >= levels.at(-1)[0] : price <= levels.at(-1)[0]);
      if (!price || !inside(price) || passed) return [0, 0];
      const i = index(price);
      const r = i >= window.first && i <= window.last ? window : this.read(issue_id, i, i, false);
      const b = 153 + 51 * (i - r.first);
      return [u64(r.u, b + group.quantity), u64(r.u, b + group.orders)];
    };
    const [bid_quantity, bid_orders] = at(bid_levels, quote.bid_price10, GROUPS.bid);
    const [ask_quantity, ask_orders] = at(ask_levels, quote.ask_price10, GROUPS.ask);
    return { ...book, bid_quantity, bid_orders, ask_quantity, ask_orders, bid_levels, ask_levels, bid_closing_levels,
      ask_closing_levels, ...totals };
  }

  // One side's best LEVELS prices with orders, best first. `anchor` is a read that holds the side's best price
  // where the quote says it is. The rows beyond it on the better side are read, in one call, only if its OVER
  // (bids) or UNDER (asks) has orders there, as in a crossed auction book; the worse side then a WINDOW at a
  // time, until there are enough levels or nothing lies beyond.
  levels(issue_id, low, high, group, anchor, limit = LEVELS) {
    const { buy, closing, quantity, orders } = group;
    const over = r => u64(r.u, 51 + quantity), under = r => u64(r.u, 102 + quantity);
    const better = buy ? over : under, worse = buy ? under : over;
    const levels = [];
    const take = r => {
      const count = r.last - r.first + 1;
      for (let j = 0; j < count && levels.length < limit; j++) {
        const b = 153 + 51 * (buy ? count - 1 - j : j);
        const q = u64(r.u, b + quantity);
        if (q) levels.push([r.u[b], q, u64(r.u, b + orders)]);
      }
    };
    if (better(anchor) && (buy ? anchor.last < high : anchor.first > low)) {
      // In reads the buffer holds, as a crossed side's better rows may span more ticks than one (a whole book's).
      const rows = Math.floor((BUFFER - 612) / 204), end = buy ? anchor.last : anchor.first;
      for (let first = buy ? high : low; levels.length < limit && (buy ? first > end : first < end);) {
        const last = buy ? Math.max(end + 1, first - rows + 1) : Math.min(end - 1, first + rows - 1);
        take(this.read(issue_id, Math.min(first, last), Math.max(first, last), closing));
        first = last + (buy ? -1 : 1);
      }
    }
    for (let r = anchor; ;) {
      take(r);
      if (levels.length >= limit || !worse(r) || (buy ? r.first <= low : r.last >= high)) return levels;
      r = buy ? this.read(issue_id, Math.max(low, r.first - WINDOW), r.first - 1, closing)
        : this.read(issue_id, r.last + 1, Math.min(high, r.last + WINDOW), closing);
    }
  }

  // Rows first..last of an issue's book, copied out of the decoder's buffer (the next read reuses it): the price
  // rows from word 153, 51 words each, the market row at 0, OVER at 51 and UNDER at 102. Without `closing`,
  // light = 1: the closing groups, traded volume and cumulative columns aren't computed.
  read(issue_id, first, last, closing) {
    const count = last - first + 1;
    if (612 + 204 * count > BUFFER) throw new Error(`A book read of ${count} rows exceeds the decoder buffer`);
    this.w._getItaRows(this.id, issue_id, first, count, this.buf + 612, this.buf, this.buf + 204, this.buf + 408, closing ? 0 : 1);
    return { first, last, u: new Uint32Array(this.w.HEAPU8.buffer, this.buf, 153 + 51 * count).slice() };
  }

  // Price rows first..first + count - 1 of an issue's book as objects (the tests read whole boards with it), each { price10, bid: [quantity, orders],
  // ask: [...] } (and bid_closing, ask_closing with `closing`), and each group's quantity on the rows
  // above (OVER) and below (UNDER) them. Without `closing`, light = 1: the closing groups, traded
  // volume and cumulative columns aren't computed.
  rows(issue_id, first, count, closing = false) {
    const buf = this.buf;
    // Boards run to a few thousand rows (3,521 at most in the demo's market); the buffer holds 20,000.
    if (612 + 204 * count > BUFFER) throw new Error(`A book read of ${count} rows exceeds the decoder buffer`);
    this.w._getItaRows(this.id, issue_id, first, count, buf + 612, buf, buf + 204, buf + 408, closing ? 0 : 1);
    const u = new Uint32Array(this.w.HEAPU8.buffer, buf, 153 + 51 * count);
    const groups = Object.values(GROUPS).filter(g => closing || !g.closing);
    const row = b => Object.fromEntries([['price10', u[b]], ...groups.map(g => [g.name, [u64(u, b + g.quantity), u64(u, b + g.orders)]])]);
    const sums = b => Object.fromEntries(groups.map(g => [g.name, u64(u, b + g.quantity)]));
    return { rows: Array.from({ length: count }, (_, k) => row(153 + 51 * k)), over: sums(51), under: sums(102) };
  }
}

async function replay({ cache, web = false, codes = [], speed = 1, limitFrames = Infinity, emit }) {
  if (!Number.isFinite(speed) || speed < 0) throw new Error('Speed must be >= 0 (0 means unpaced)');
  if (!(limitFrames > 0)) throw new Error('Frame limit must be positive');
  if (Boolean(cache) === Boolean(web)) throw new Error('Select exactly one of --cache DIR or --web');
  const { assets, input_transport } = web ? await loadWebAssets()
    : { assets: loadAssets(cache), input_transport: { kind: 'local_cache' } };
  const decoder = await Decoder.create(assets);
  if (decoder.date !== manifest.trading_date.replaceAll('-', '')) throw new Error('Unexpected mock trading date');
  const selected = new Set(codes);
  const master = decoder.master.filter(m => !selected.size || selected.has(m.code));
  const missing = codes.filter(code => !master.some(m => m.code === code));
  if (missing.length) throw new Error(`Codes missing from 2021 mock: ${missing.join(',')}`);
  const ids = new Set(master.map(m => m.issue_id));
  const iter = frames(assets['ws.dat']);
  const first = iter.next().value;
  if (!first) throw new Error('Empty mock recording');
  if (!decoder.start(first.data)) throw new Error('Missing frame-number bootstrap');
  let seq = 0;
  const skippedIssues = skipped => (skipped.length ? { skipped_issues: skipped.sort((a, b) => a - b) } : {});
  const bootstrap = decoder.quotes(master.map(m => m.issue_id));
  await emit({ type: 'bootstrap', seq: seq++, source: 'historical_mock', trading_date: decoder.date, input_transport,
    source_timestamp_origin: 'brisk_decoder_unverified', exchange_delay_ms: null,
    source_time_us: decoder.time(), market_issue_count: decoder.master.length, master,
    quotes: bootstrap.quotes, trades: decoder.trades(ids), ...skippedIssues(bootstrap.skipped) });
  // Trades the snapshot restored follow the bootstrap, at its clock.
  for (const trades of decoder.history(ids)) await emit({ type: 'history', seq: seq++, source_time_us: decoder.time(), trades });
  const start = performance.now();
  let frameCount = 1, updateCount = 0;
  for (const frame of iter) {
    if (frameCount >= limitFrames) break;
    if (speed) {
      const wait = (frame.time - first.time) / speed - (performance.now() - start);
      if (wait > 0) await delay(wait);
    }
    const received_unix_ms = Date.now();
    // Compare local monotonic elapsed time with the replay schedule. The
    // recording clock is historical and cannot measure exchange delay.
    const replay_lateness_ms = speed ? Math.max(0,
      performance.now() - start - (frame.time - first.time) / speed) : null;
    const began = process.hrtime.bigint();
    decoder.feed(frame.data);
    const { quotes, skipped } = decoder.quotes(decoder.changed().filter(id => ids.has(id)));
    const trades = decoder.trades(ids);
    const decode_ns = Number(process.hrtime.bigint() - began);
    // Include empty batches: sequence continuity and feed health differ from a
    // quiet security's last update. No fixed polling interval or dropped deltas.
    await emit({ type: 'quotes', seq: seq++, source_time_us: decoder.time(),
      received_unix_ms, decode_ns, replay_lateness_ms, quotes, trades, ...skippedIssues(skipped) });
    updateCount += quotes.length; frameCount++;
  }
  const summary = { type: 'end', seq, source_time_us: decoder.time(), frames: frameCount,
    quote_updates: updateCount, replay_wall_ms: performance.now() - start };
  await emit(summary);
  return summary;
}

async function main(argv) {
  const args = {};
  for (let i = 0; i < argv.length;) {
    if (argv[i] === '--web') {
      if (args['--web']) throw new Error('Duplicate --web');
      args['--web'] = true; i++; continue;
    }
    if (!['--cache', '--codes', '--speed', '--limit-frames'].includes(argv[i]) || argv[i + 1] === undefined || argv[i + 1].startsWith('--')) {
      throw new Error('Usage: decoder.cjs (--cache DIR | --web) [--codes 7203,6758] [--speed 1] [--limit-frames N]');
    }
    if (args[argv[i]] !== undefined) throw new Error(`Duplicate ${argv[i]}`);
    args[argv[i]] = argv[i + 1]; i += 2;
  }
  if (Boolean(args['--cache']) === Boolean(args['--web'])) throw new Error('Select exactly one of --cache DIR or --web');
  await replay({ cache: args['--cache'], web: args['--web'] || false, codes: args['--codes']?.split(',') || [],
    speed: Number(args['--speed'] ?? 1), limitFrames: args['--limit-frames'] ? Number(args['--limit-frames']) : Infinity,
    emit: async record => {
      if (!process.stdout.write(JSON.stringify(record) + '\n')) await once(process.stdout, 'drain');
    } });
}

// Read a module's code just enough to find what _getItaRows calls: the board object it looks up (an indirect
// call through invoke_iiii, with the table slot and the registry as constants, then the decoder ID and the
// issue), and the rebuild it runs (through invoke_vi, on that object) when a byte flag in the object is clear.
// A WebAssembly binary: sections of (id, size), the code section's bodies of (size, locals, instructions).
function moduleOf(bytes) {
  let at = 0;
  const byte = () => bytes[at++];
  const uleb = () => { let r = 0, shift = 0, b; do { b = bytes[at++]; r += (b & 0x7f) * 2 ** shift; shift += 7; } while (b & 0x80); return r; };
  const sleb = () => {
    let r = 0n, shift = 0n, b;
    do { b = BigInt(bytes[at++]); r |= (b & 0x7fn) << shift; shift += 7n; } while (b & 0x80n);
    if (b & 0x40n) r -= 1n << shift;
    return Number(r);
  };
  const name = () => { const n = uleb(); const s = Buffer.from(bytes.subarray(at, at + n)).toString(); at += n; return s; };
  if (Buffer.from(bytes.subarray(0, 4)).toString('latin1') !== '\0asm') throw new Error('not a WebAssembly module');
  at = 8;
  const imported = new Map(), exported = new Map();
  let functions = 0, code = null, memory = null;
  while (at < bytes.length) {
    const id = byte(), size = uleb(), end = at + size;
    if (id === 2) {
      for (let n = uleb(); n--;) {
        name(); const field = name(), kind = byte();
        if (kind === 0) { uleb(); imported.set(field, functions++); }
        else if (kind === 1) { byte(); const flags = uleb(); uleb(); if (flags & 1) uleb(); }
        else if (kind === 2) { const flags = uleb(); memory = { initial: uleb() }; if (flags & 1) memory.maximum = uleb(); }
        else if (kind === 3) { byte(); byte(); }
      }
    } else if (id === 7) {
      for (let n = uleb(); n--;) { const field = name(), kind = byte(), index = uleb(); if (kind === 0) exported.set(field, index); }
    } else if (id === 10) {
      code = at;
    }
    at = end;
  }
  return { imported, exported, functions, code, memory };
}

function boardCalls(bytes) {
  const { imported, exported, functions, code } = moduleOf(bytes);
  let at = 0;
  const byte = () => bytes[at++];
  const uleb = () => { let r = 0, shift = 0, b; do { b = bytes[at++]; r += (b & 0x7f) * 2 ** shift; shift += 7; } while (b & 0x80); return r; };
  const sleb = () => {
    let r = 0n, shift = 0n, b;
    do { b = BigInt(bytes[at++]); r |= (b & 0x7fn) << shift; shift += 7n; } while (b & 0x80n);
    if (b & 0x40n) r -= 1n << shift;
    return Number(r);
  };
  const target = exported.get('_getItaRows'), lookup = imported.get('invoke_iiii'), rebuild = imported.get('invoke_vi');
  if (target === undefined || code === null || lookup === undefined || rebuild === undefined) {
    throw new Error('_getItaRows or the invoke imports not found');
  }
  at = code;
  for (let n = uleb(), i = 0; i < n; i++) {
    const size = uleb();
    if (i !== target - functions) { at += size; continue; }
    const end = at + size;
    for (let groups = uleb(); groups--;) { uleb(); byte(); }
    // The MVP instruction set's immediates, enough to step through an Emscripten body.
    const ops = [];
    while (at < end) {
      const op = byte(), ins = { op };
      if (op === 0x02 || op === 0x03 || op === 0x04) { if (bytes[at] === 0x40 || bytes[at] >= 0x7b) byte(); else sleb(); }
      else if (op === 0x0c || op === 0x0d || (op >= 0x20 && op <= 0x24) || op === 0x10) ins.arg = uleb();
      else if (op === 0x0e) { for (let k = uleb(); k--;) uleb(); uleb(); }
      else if (op === 0x11) { uleb(); uleb(); }
      else if (op >= 0x28 && op <= 0x3e) { uleb(); ins.offset = uleb(); }
      else if (op === 0x3f || op === 0x40) byte();
      else if (op === 0x41 || op === 0x42) ins.arg = sleb();
      else if (op === 0x43) at += 4;
      else if (op === 0x44) at += 8;
      else if (op === 0xfc) { const sub = uleb(); if (sub >= 8 && sub <= 17) { uleb(); if (sub === 8 || sub === 10 || sub === 12 || sub === 14) uleb(); } }
      else if (op === 0xfd) throw new Error('SIMD in _getItaRows: not the build this reads');
      ops.push(ins);
    }
    const is = (k, op, arg) => ops[k]?.op === op && (arg === undefined || ops[k].arg === arg);
    let found = null, flag = null, rebuildSlot = null, board = null;
    for (let k = 0; k < ops.length; k++) {
      // i32.const slot; i32.const registry; local.get 0; local.get 1; call invoke_iiii; local.set board
      if (!found && is(k, 0x41) && is(k + 1, 0x41) && is(k + 2, 0x20, 0) && is(k + 3, 0x20, 1) && is(k + 4, 0x10, lookup)) {
        found = [ops[k].arg, ops[k + 1].arg];
        const set = ops[k + 5];
        if (set?.op === 0x21 || set?.op === 0x22) board = set.arg;
      }
      // local.get board; i32.const F; i32.add; i32.load8_s
      if (found && flag === null && is(k, 0x20, board) && is(k + 1, 0x41) && is(k + 2, 0x6a) && is(k + 3, 0x2c)) {
        flag = ops[k + 1].arg + ops[k + 3].offset;
      }
      // i32.const slot; local.get board; call invoke_vi
      if (flag !== null && rebuildSlot === null && is(k, 0x41) && is(k + 1, 0x20, board) && is(k + 2, 0x10, rebuild)) {
        rebuildSlot = ops[k].arg;
      }
    }
    if (!found || flag === null || rebuildSlot === null) throw new Error('_getItaRows does not read its board as expected');
    return { lookup: found, flag, rebuild: rebuildSlot };
  }
  throw new Error('_getItaRows has no code');
}

// A quote's book read from the decoder's own price ladder, without _getItaRows (see book()). _getItaRows looks
// up the issue's board, rebuilds it if a frame changed it (cheap), then walks every tick of the ladder for the
// totals above and below the rows asked for, converting each to a price and adding notional columns on the way:
// most of a quote's cost. Here the board is looked up and rebuilt as it does (boardCalls), the ladder is read
// where it lies, a std::deque of 72-byte levels found in the board object by matching _getItaRows' rows, and
// its totals are summed by board.wat's kernel in the decoder's memory. Only the 72-byte level and the 56-level
// block are assumed: discover() checks the book of every issue against _getItaRows before it is used.
class NoOrdersYet extends Error {}

// A ladder quantity as _getItaRows converts it (i64.trunc_f64_u, then u64()).
function quantity(value, m) {
  if (!(value >= 0) || value >= 2 ** 64) {
    throw Object.assign(new Error(`The book of ${m.code}: the decoder trapped (float unrepresentable in integer range); its state `
      + 'is inconsistent, as after a lost, repeated or reordered update or one applied without the snapshot under it'), { trap: true });
  }
  const integer = Math.trunc(value);
  if (!Number.isSafeInteger(integer)) throw new Error('Unsafe 64-bit quantity/timestamp');
  return integer;
}

class BoardReader {
  static discover(decoder, wasmBytes, memory) {
    if (!(memory instanceof WebAssembly.Memory) || memory.buffer !== decoder.w.HEAPU8.buffer) {
      throw new Error('the decoder\'s memory is not reachable');
    }
    const w = decoder.w;
    if (typeof w.dynCall_iiii !== 'function' || typeof w.dynCall_vi !== 'function') throw new Error('no dynCall exports');
    const kernel = new WebAssembly.Instance(new WebAssembly.Module(Buffer.from(BOARD_KERNEL, 'base64')), { env: { memory } });
    const reader = new BoardReader(decoder, boardCalls(wasmBytes), memory, kernel.exports);
    reader.locate();
    reader.check();
    return reader;
  }

  constructor(decoder, calls, memory, { sum, compact, diff }) {
    Object.assign(this, { decoder, calls, memory, sum, compact, diff, reads: 0, sums: decoder.w._malloc(80), out: 0, cap: 0,
      changed: 0, changedCap: 0 });
  }

  // Memory of the decoder's for kernel output, `bytes` at least, in place of `at` (`room`: how much it holds): [at, room].
  // A decoder's memory can grow when it is taken, and views on it go with the old buffer (see views()).
  room(at, room, bytes) {
    if (bytes <= room) return [at, room];
    if (at) this.decoder.w._free(at);
    return [this.decoder.w._malloc(bytes), bytes];
  }

  // The changes of a ladder this reader read (cur) from an issue's last, kept in the decoder's memory (prev: { ptr, cap,
  // n }, or none), as diff() writes them: [index, book, quantity, orders] records. prev then holds cur, grown if need be.
  changesOf(cur, prev) {
    const pn = prev ? prev.n : 0;
    [this.changed, this.changedCap] = this.room(this.changed, this.changedCap, 96 * (cur.n + pn) + 8);
    const count = this.diff(cur.ptr, cur.cap, cur.n, prev ? prev.ptr : 0, prev ? prev.cap : 0, pn, this.changed);
    const out = [], buffer = this.memory.buffer, words = new Int32Array(buffer, this.changed, 6 * count);
    const values = new Float64Array(buffer, this.changed, 3 * count);
    for (let r = 0; r < count; r++) out.push([words[6 * r], words[6 * r + 1], values[3 * r + 1], words[6 * r + 4] >>> 0]);
    return out;
  }

  // An issue's last ladder in the decoder's memory, `cur` copied in: { ptr, cap, n }.
  keep(cur, prev) {
    let kept = prev;
    if (!kept || kept.cap < cur.n) {
      if (kept) this.decoder.w._free(kept.ptr);
      const cap = (Math.max(16, 2 * cur.n) + 1) & ~1;
      kept = { ptr: this.decoder.w._malloc(52 * cap), cap, n: 0 };
    }
    const bytes = new Uint8Array(this.memory.buffer);
    bytes.copyWithin(kept.ptr, cur.ptr, cur.ptr + 4 * cur.n);
    bytes.copyWithin(kept.ptr + 4 * kept.cap, cur.ptr + 4 * cur.cap, cur.ptr + 4 * cur.cap + 32 * cur.n);
    bytes.copyWithin(kept.ptr + 36 * kept.cap, cur.ptr + 36 * cur.cap, cur.ptr + 36 * cur.cap + 16 * cur.n);
    kept.n = cur.n;
    return kept;
  }

  // A ladder in the decoder's memory as views (`kept` from keep(), or one this reader read).
  views_of(kept) {
    const buffer = this.memory.buffer;
    return { n: kept.n, idx: new Int32Array(buffer, kept.ptr, kept.n), q: new Float64Array(buffer, kept.ptr + 4 * kept.cap, 4 * kept.n),
      c: new Uint32Array(buffer, kept.ptr + 36 * kept.cap, 4 * kept.n) };
  }

  // A ladder held in arrays (the walk's) copied into the decoder's memory, for diff().
  place(ladder) {
    const cap = (Math.max(16, 2 * ladder.n) + 1) & ~1, ptr = this.decoder.w._malloc(52 * cap), buffer = this.memory.buffer;
    new Int32Array(buffer, ptr, ladder.n).set(ladder.idx.subarray(0, ladder.n));
    new Float64Array(buffer, ptr + 4 * cap, 4 * ladder.n).set(ladder.q.subarray(0, 4 * ladder.n));
    new Uint32Array(buffer, ptr + 36 * cap, 4 * ladder.n).set(ladder.c.subarray(0, 4 * ladder.n));
    return { ptr, cap, n: ladder.n };
  }

  // A board's ladder whole, as compact() writes it into this reader's buffer (a ladder, as Decoder.wholeBook() has
  // them, viewing that buffer: good until the next read).
  ladder(map, start, size, base) {
    if (size > this.cap) {
      if (this.out) this.decoder.w._free(this.out);
      this.cap = (size + 1) & ~1;
      this.out = this.decoder.w._malloc(52 * this.cap);
    }
    const n = this.compact(map, start, size, base, this.out, this.cap, this.sums);
    return { ...this.views_of({ ptr: this.out, cap: this.cap, n }), ptr: this.out, cap: this.cap };
  }

  views() {
    if (this.u?.buffer !== this.memory.buffer) {
      this.u = new Uint32Array(this.memory.buffer);
      this.i = new Int32Array(this.memory.buffer);
      this.f = new Float64Array(this.memory.buffer);
    }
  }

  // The issue's board, rebuilt if a frame changed it, as _getItaRows has it.
  board(issue_id) {
    const { w, id } = this.decoder, p = w.dynCall_iiii(this.calls.lookup[0], this.calls.lookup[1], id, issue_id);
    if (!w.HEAPU8[p + this.calls.flag]) w.dynCall_vi(this.calls.rebuild, p);
    return p;
  }

  // Where the board object keeps the ladder (the deque's block map, first slot and length, which follow one
  // another, and the first level's tick index) and its market orders: found from issues with orders, by
  // matching their levels to _getItaRows' rows.
  locate() {
    const d = this.decoder, refs = [];
    for (const m of d.master) {
      const ladder = this.walked(m);
      if (ladder && ladder.rows.size >= 3) refs.push(ladder);
      if (refs.length >= 8) break;
    }
    if (!refs.length) throw new NoOrdersYet('no issue with orders to locate the ladder by');
    // A board left inconsistent (as by updates applied without the snapshot under them) may not read as a
    // ladder; the layout is taken from one that does and must hold for every other that does, two at least.
    for (const first of refs) {
      this.views();
      const { u, i } = this, p = this.board(first.m.issue_id);
      for (let at = 0; at < 4096 && !this.layout; at += 4) {
        const map = u[(p + at) >> 2], end = u[(p + at + 4) >> 2], start = u[(p + at + 12) >> 2], size = u[(p + at + 16) >> 2];
        const blocks = (end - map) / 4;
        if (!map || !Number.isInteger(blocks) || blocks < 1 || blocks > 65536 || size < 1 || start + size > blocks * 56) continue;
        if (end >= this.memory.buffer.byteLength) continue;
        const layout = { map: at, start: at + 12, size: at + 16 };
        const base = this.baseOf(p, layout, first);
        if (base === null) continue;
        for (let b = 0; b < 4096; b += 4) {
          if (i[(p + b) >> 2] !== base) continue;
          const candidate = { ...layout, base: b, market: at - 4 - 72 };
          const readable = refs.filter(ref => this.levelsOf(this.board(ref.m.issue_id), candidate) !== null);
          if (readable.length >= 2 && readable.every(ref => this.matches(ref, candidate))) { this.layout = candidate; break; }
        }
      }
      if (this.layout) break;
    }
    if (!this.layout) throw new Error('the ladder was not found in the board object');
  }

  // An issue's whole ladder as _getItaRows gives it: the nonzero rows by tick index, and the market row.
  walked(m) {
    const d = this.decoder, w = d.w;
    const index = price => w._getTickPrice10ToIndex(d.id, m.tick_type, price);
    const low = index(m.limit_down10), count = index(m.limit_up10) - low + 1;
    if (!(count > 0) || 612 + 204 * count > BUFFER) return null;
    let rows;
    try {
      rows = call(w, `The ladder of ${m.code}`, () => d.rows(m.issue_id, low, count, true).rows);
    } catch (error) {
      if (error.trap) return null;
      throw error;
    }
    const market = new Uint32Array(w.HEAPU8.buffer, d.buf, 51);
    const groups = row => Object.values(GROUPS).map(g => row[g.name]).flat();
    const nonzero = new Map();
    rows.forEach((row, k) => { if (groups(row).some(Boolean)) nonzero.set(low + k, row); });
    return { m, rows: nonzero, market: [u64(market, 5), u64(market, 16), u64(market, 28), u64(market, 39)] };
  }

  // The tick index of the deque's first level, if its nonzero levels are the walked ladder's rows.
  baseOf(p, layout, ref) {
    const levels = this.levelsOf(p, layout);
    if (levels === null || levels.length !== ref.rows.size) return null;
    const ticks = [...ref.rows.keys()];
    const base = ticks[0] - levels[0].slot;
    return levels.every((level, k) => level.slot + base === ticks[k]) ? base : null;
  }

  // The deque's nonzero levels, in order (null if it does not read as one).
  levelsOf(p, layout) {
    this.views();
    const { u, f } = this, map = u[(p + layout.map) >> 2], start = u[(p + layout.start) >> 2], size = u[(p + layout.size) >> 2];
    if (size > 100000) return null;
    const levels = [];
    for (let s = 0; s < size; s++) {
      const e = this.level(map, start, s);
      if (!e || e + 72 > this.memory.buffer.byteLength) return null;
      const values = [f[(e + 8) >> 3], u[(e + 24) >> 2], f[(e + 16) >> 3], u[(e + 28) >> 2], f[(e + 40) >> 3], u[(e + 56) >> 2],
        f[(e + 48) >> 3], u[(e + 60) >> 2]];
      if (values.some(v => !Number.isFinite(v) || v < 0)) return null;
      if (values.some(Boolean)) levels.push({ slot: s, values });
    }
    return levels;
  }

  level(map, start, s) {
    const k = start + s;
    return this.u[(map >> 2) + Math.floor(k / 56)] + (k % 56) * 72;
  }

  matches(ref, layout) {
    const p = this.board(ref.m.issue_id), levels = this.levelsOf(p, layout);
    if (levels === null || levels.length !== ref.rows.size) return false;
    const base = this.i[(p + layout.base) >> 2];
    const sameLevels = levels.every(({ slot, values }) => {
      const row = ref.rows.get(base + slot);
      return row && JSON.stringify(values) === JSON.stringify([...row.bid, ...row.bid_closing, ...row.ask, ...row.ask_closing]);
    });
    const { f } = this, mk = p + layout.market;
    return sameLevels && JSON.stringify([f[(mk + 8) >> 3], f[(mk + 16) >> 3], f[(mk + 40) >> 3], f[(mk + 48) >> 3]]) === JSON.stringify(ref.market);
  }

  // Every issue's book against _getItaRows', as it stands (the snapshot's): any difference and it is not used.
  check() {
    const d = this.decoder, w = d.w;
    for (const m of d.master) {
      if (!w._getStockView(d.id, m.issue_id, d.buf)) continue;
      const view = parseStockView(new Uint32Array(w.HEAPU8.buffer, d.buf, d.viewWords), d.ohlc, d.layoutShift);
      let walked;
      try {
        walked = d.walkBook(m.issue_id, m, view);
      } catch (error) {
        if (error.trap) continue;
        throw error;
      }
      if (JSON.stringify(this.book(m.issue_id, m, view)) !== JSON.stringify(walked)) {
        throw new Error(`its book of ${m.code} differs from _getItaRows'`);
      }
    }
  }

  price(m, index) {
    return this.decoder.priceOf(m, index);
  }

  // As walkBook() gives it, key for key. A quantity _getItaRows could not convert to an integer (a board in an
  // inconsistent state) is a trap here as there, and one beyond 2^53 the same error.
  book(issue_id, m, quote, whole = false) {
    const p = this.board(issue_id);
    this.views();
    const { u, i, f, layout } = this, q = at => quantity(f[at >> 3], m);
    const map = u[(p + layout.map) >> 2], start = u[(p + layout.start) >> 2], size = u[(p + layout.size) >> 2];
    const base = i[(p + layout.base) >> 2], mk = p + layout.market;
    const book = { market_buy_quantity: q(mk + 8), market_sell_quantity: q(mk + 40),
      closing_market_buy_quantity: q(mk + 16), closing_market_sell_quantity: q(mk + 48) };
    // With `whole`, the ladder in one pass (compact, which sums it too) and the lists its best LEVELS, so a book of
    // hundreds of levels is not read level by level here; the ladder goes with the book.
    const ladder = whole ? this.ladder(map, start, size, base) : (this.sum(map, start, size, this.sums), null);
    if (whole && u.buffer !== this.memory.buffer) return this.book(issue_id, m, quote, whole);   // its buffer grew memory
    const sums = this.sums, orders = k => u[(sums + 32 + 8 * k) >> 2] + 4294967296 * u[(sums + 36 + 8 * k) >> 2];
    const totals = { bid_total_quantity: q(sums), bid_total_orders: orders(0), bid_closing_total_quantity: q(sums + 8),
      bid_closing_total_orders: orders(1), ask_total_quantity: q(sums + 16), ask_total_orders: orders(2),
      ask_closing_total_quantity: q(sums + 24), ask_closing_total_orders: orders(3) };
    // A side's best LEVELS prices with orders: the bids from the highest with orders down, the asks from the
    // lowest up (the kernel found where they begin; every level beyond is empty).
    const levels = (quantity, count, buy, from) => {
      const out = [];
      for (let slot = from; slot >= 0 && slot < size && out.length < this.decoder.levelLimit; slot += buy ? -1 : 1) {
        const e = this.level(map, start, slot);
        if (f[(e + quantity) >> 3]) out.push([this.price(m, base + slot), q(e + quantity), u[(e + count) >> 2]]);
      }
      return out;
    };
    const from = k => this.i[(sums + 64 + 4 * k) >> 2];
    const top = (k, buy) => {
      const { n, idx, q: v, c } = ladder, out = [];
      for (let j = buy ? n - 1 : 0; j >= 0 && j < n && out.length < LEVELS; j += buy ? -1 : 1) {
        if (v[4 * j + k]) out.push([this.price(m, idx[j]), quantity(v[4 * j + k], m), c[4 * j + k]]);
      }
      return out;
    };
    const side = (k, quantity, count, buy) => (whole ? top(k, buy) : levels(quantity, count, buy, from(k === 0 ? 0 : k === 1 ? 2 : k === 2 ? 1 : 3)));
    const bid_levels = side(0, 8, 24, true), ask_levels = side(1, 40, 56, false);
    const bid_closing_levels = totals.bid_closing_total_quantity ? side(2, 16, 28, true) : [];
    const ask_closing_levels = totals.ask_closing_total_quantity ? side(3, 48, 60, false) : [];
    const limit = whole ? LEVELS : this.decoder.levelLimit;
    const inside = price => price >= m.limit_down10 && price <= m.limit_up10;
    const at = (side, price, quantity, count, buy) => {
      const level = side.find(([x]) => x === price);
      if (level) return level.slice(1);
      // A price the walk passed, or every order on the side having been read, means none there.
      const passed = side.length < limit || (buy ? price >= side.at(-1)[0] : price <= side.at(-1)[0]);
      if (!price || !inside(price) || passed) return [0, 0];
      const slot = this.decoder.w._getTickPrice10ToIndex(this.decoder.id, m.tick_type, price) - base;
      if (slot < 0 || slot >= size) return [0, 0];
      const e = this.level(map, start, slot);
      return [q(e + quantity), u[(e + count) >> 2]];
    };
    const [bid_quantity, bid_orders] = at(bid_levels, quote.bid_price10, 8, 24, true);
    const [ask_quantity, ask_orders] = at(ask_levels, quote.ask_price10, 40, 56, false);
    const read = { ...book, bid_quantity, bid_orders, ask_quantity, ask_orders, bid_levels, ask_levels, bid_closing_levels,
      ask_closing_levels, ...totals };
    return whole ? { book: read, ladder } : read;
  }
}

// Whether two ladders (see Decoder.wholeBook()) hold the same levels.
function sameLadder(a, b) {
  if (a.n !== b.n) return false;
  for (let j = 0; j < a.n; j++) if (a.idx[j] !== b.idx[j]) return false;
  for (let j = 0; j < 4 * a.n; j++) if (a.q[j] !== b.q[j] || a.c[j] !== b.c[j]) return false;
  return true;
}

module.exports = { Decoder, BoardReader, boardCalls, BOARD_KERNEL, QuoteHistory, frames, u64, looseU64, readMasterRecord, masterLayout, MASTER_LAYOUT, SCHEDULE_FIELDS, scheduleArgs,
  parseMasterFile, checkMasterFile, loadAssets, replay, main, parseStockView, stockViewShift, STOCK_VIEW_SHIFT };
if (require.main === module) main(process.argv.slice(2)).catch(error => {
  console.error(error.message); process.exitCode = 1;
});
