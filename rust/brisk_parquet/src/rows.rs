//! The quotes (`order_book_depths`) and trades of a recording, checked a row group at a time: each row group must be
//! the canonical encoding of what it holds, and its rows must obey the rules `verify_broker` states in
//! `briskapi/_parquet.py`. Nothing is re-encoded: every derived column is compared with what it derives from.
use arrow_array::cast::AsArray;
use arrow_array::types::{
    Decimal128Type, Int8Type, TimestampNanosecondType, UInt8Type, UInt32Type, UInt64Type,
};
use arrow_array::{Array, ArrayRef, ListArray, PrimitiveArray, RecordBatch, StringArray};

use crate::batches::{BOOTSTRAP, Batches, HISTORY, QUOTES};
use crate::error::{Failure, Res, refuse};
use crate::master::Master;
use crate::require;
use crate::schema::{self, BOOKS, COUNTS, LEVELS, PRICES, TRADE_KEYS, VENUE_SUFFIX};
use crate::units::{TWO_53, digits, tenths, whole};

const DEPTHS_BAD: &str = "Non-canonical data/order_book_depths.parquet";
const TRADES_BAD: &str = "Non-canonical data/trades.parquet";

/// What the rows are checked against.
pub struct Context<'a> {
    pub batches: &'a Batches,
    pub master: &'a [Master],
    /// Each master entry's identifier, "CODE.XTKS".
    pub identifiers: Vec<Vec<u8>>,
    /// Canonical JSON bytes of a quote's and a trade's keys and punctuation.
    quote_fixed: u64,
    trade_fixed: u64,
    /// The tables are in v2's format (`ts_init`).
    legacy: bool,
    /// The quotes have book changes (the depths table has `changes` and `checkpoint` columns).
    pub changes: bool,
}

impl<'a> Context<'a> {
    pub fn new(batches: &'a Batches, master: &'a [Master], legacy: bool, changes: bool) -> Self {
        Context {
            batches,
            master,
            identifiers: master
                .iter()
                .map(|m| format!("{}{}", m.code, VENUE_SUFFIX).into_bytes())
                .collect(),
            quote_fixed: schema::object_length(&schema::QUOTE_KEYS),
            trade_fixed: schema::object_length(&TRADE_KEYS),
            legacy,
            changes,
        }
    }

    fn locate(&self, issue: u64, hint: &mut usize) -> Option<usize> {
        if self.master.get(*hint).is_some_and(|m| m.issue_id == issue) {
            return Some(*hint);
        }
        let found = self
            .master
            .binary_search_by_key(&issue, |m| m.issue_id)
            .ok()?;
        *hint = found;
        Some(found)
    }
}

fn bad_schema(name: &str) -> Failure {
    Failure::Refused(format!("Unexpected schema in {name}"))
}

fn prim<'a, T: arrow_array::types::ArrowPrimitiveType>(
    batch: &'a RecordBatch,
    name: &str,
) -> Res<&'a PrimitiveArray<T>> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_primitive_opt::<T>())
        .ok_or_else(|| bad_schema(name))
}

fn strings<'a>(batch: &'a RecordBatch, name: &str) -> Res<&'a StringArray> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_string_opt::<i32>())
        .ok_or_else(|| bad_schema(name))
}

fn micros(ts: &[i64], midnight: i64, what: &str) -> Res<Vec<i64>> {
    let mut out = Vec::with_capacity(ts.len());
    for &t in ts {
        match t.checked_sub(midnight) {
            Some(d) if d >= 0 && d % 1000 == 0 => out.push(d / 1000),
            _ => return refuse(what.to_string()),
        }
    }
    Ok(out)
}

/// The first and last quote rows of a row group, to join the groups' boundaries.
#[derive(Clone, Copy, Default)]
pub struct Edge {
    pub issue: u64,
    pub seq: u64,
    pub frame: u64,
    pub time: i64,
}

pub struct Group {
    pub rows: usize,
    pub first: Edge,
    pub last: Edge,
    pub bytes: u64,
}

/// What a book side's rows break once they decode: raised after the checks that come before them.
#[derive(Default, Clone, Copy)]
struct Deferred {
    too_long: bool,
    unordered: bool,
}

/// One book side's rows, decoded: no order IDs and whole non-negative prices and sizes; adds the canonical JSON bytes
/// of the side's lists. A side with more than LEVELS levels, or levels out of order, is told for later.
fn side(list: &ListArray, descending: bool, bytes: &mut u64) -> Res<Deferred> {
    let offsets = list.value_offsets();
    let levels = list.values().as_struct();
    let (price, size, count, order) = (
        levels.column(0).as_primitive_opt::<Decimal128Type>(),
        levels.column(1).as_primitive_opt::<Decimal128Type>(),
        levels.column(2).as_primitive_opt::<UInt32Type>(),
        levels.column(3).as_primitive_opt::<UInt64Type>(),
    );
    let (Some(price), Some(size), Some(count), Some(order)) = (price, size, count, order) else {
        return refuse("Unexpected schema in data/order_book_depths.parquet");
    };
    let (price, size, count, order) = (
        price.values(),
        size.values(),
        count.values(),
        order.values(),
    );
    require!(order.iter().all(|&o| o == 0), "Invalid book level");
    let (mut total, mut deferred) = (0u64, Deferred::default());
    for window in offsets.windows(2) {
        let (lo, hi) = (window[0] as usize, window[1] as usize);
        deferred.too_long |= hi - lo > LEVELS;
        total += 2 + hi.saturating_sub(lo + 1) as u64;
        let mut previous: Option<u64> = None;
        for j in lo..hi {
            let (Some(p), Some(q)) = (tenths(price[j]), whole(size[j])) else {
                return refuse("Invalid decimal value");
            };
            if let Some(before) = previous {
                deferred.unordered |= if descending { p >= before } else { p <= before };
            }
            previous = Some(p);
            total += digits(p) + digits(q) + digits(count[j] as u64) + 4;
        }
    }
    *bytes += total;
    Ok(deferred)
}

fn u64s<'a>(batch: &'a RecordBatch, name: &str) -> Res<&'a [u64]> {
    Ok(prim::<UInt64Type>(batch, name)?.values())
}

/// The quotes' book changes, decoded: whole prices and sizes, and books that are books; adds their canonical JSON bytes. The
/// rules they obey (order, emptied levels, the books they give) are `replay`'s.
fn changes(batch: &RecordBatch, bytes: &mut u64) -> Res<()> {
    let list = batch
        .column_by_name(schema::CHANGES)
        .and_then(|c| c.as_list_opt::<i32>())
        .ok_or_else(|| bad_schema(schema::CHANGES))?;
    let levels = list.values().as_struct();
    let (Some(book), Some(price), Some(size)) = (
        levels.column(0).as_primitive_opt::<UInt8Type>(),
        levels.column(1).as_primitive_opt::<Decimal128Type>(),
        levels.column(2).as_primitive_opt::<Decimal128Type>(),
    ) else {
        return Err(bad_schema(schema::CHANGES));
    };
    let offsets = list.value_offsets();
    let used = offsets[0] as usize..offsets[offsets.len() - 1] as usize;
    require!(
        used.clone()
            .all(|j| tenths(price.value(j)).is_some() && whole(size.value(j)).is_some()),
        "Invalid decimal value"
    );
    require!(
        used.clone().all(|j| (book.value(j) as usize) < BOOKS.len()),
        "Invalid book change"
    );
    *bytes += crate::sizes::changes_bytes(list);
    Ok(())
}

/// A quote row group: the canonical encoding of its content and the rules that hold row by row, checked in the order
/// `verify_broker` does (what cannot be decoded, what is not canonical, then the rules), so a recording is refused for the
/// same reason by both. `counts` is told how many quotes each batch holds.
pub fn check_depths(batch: &RecordBatch, context: &Context, counts: &mut [u32]) -> Res<Group> {
    let n = batch.num_rows();
    let batches = context.batches;
    let midnight = batches.midnight;
    let (issue, seq, frame) = (
        u64s(batch, "issue_id")?,
        u64s(batch, "seq")?,
        u64s(batch, "sequence")?,
    );
    let mut bytes = 0u64;
    // Decoding: integers below 2^53, times, decimals, identifiers, books (their digits are in the canonical JSON).
    for column in [seq, issue, frame] {
        require!(column.iter().all(|&v| v < TWO_53), "Invalid integer value");
    }
    for column in [issue, frame] {
        bytes += column.iter().map(|&v| digits(v)).sum::<u64>();
    }
    let time = micros(
        prim::<TimestampNanosecondType>(batch, "ts_event")?.values(),
        midnight,
        "Invalid quote time",
    )?;
    bytes += time.iter().map(|&t| digits(t as u64)).sum::<u64>();
    let special = prim::<TimestampNanosecondType>(batch, "special_quote_time")?;
    let mut special_at_midnight = false;
    for i in 0..n {
        if special.is_valid(i) {
            match special.value(i).checked_sub(midnight) {
                Some(d) if d >= 0 && d % 1000 == 0 => {
                    special_at_midnight |= d == 0;
                    bytes += digits((d / 1000) as u64);
                }
                _ => return refuse("Invalid special quote time"),
            }
        } else {
            bytes += 1;
        }
    }
    let turnover = prim::<Decimal128Type>(batch, "turnover")?;
    for &v in turnover.values().iter() {
        let Some(t) = tenths(v) else {
            return refuse("Invalid decimal value");
        };
        bytes += digits(t);
    }
    let mut zero_price = false;
    for name in PRICES {
        let prices = prim::<Decimal128Type>(batch, name)?;
        for i in 0..n {
            if prices.is_valid(i) {
                let Some(t) = tenths(prices.value(i)) else {
                    return refuse("Invalid decimal value");
                };
                zero_price |= t == 0;
                bytes += digits(t);
            } else {
                bytes += 1;
            }
        }
    }
    for name in COUNTS {
        let column = u64s(batch, name)?;
        require!(column.iter().all(|&v| v < TWO_53), "Invalid integer value");
        bytes += column.iter().map(|&v| digits(v)).sum::<u64>();
    }
    let identifiers = strings(batch, "identifier")?;
    require!(
        (0..n).all(|i| identifiers
            .value(i)
            .as_bytes()
            .ends_with(VENUE_SUFFIX.as_bytes())),
        "Invalid identifier"
    );
    let books: Vec<&ListArray> = BOOKS
        .iter()
        .map(|(name, _)| {
            batch
                .column_by_name(name)
                .and_then(|c| c.as_list_opt::<i32>())
                .ok_or_else(|| bad_schema(name))
        })
        .collect::<Res<_>>()?;
    let mut deferred = Vec::with_capacity(4);
    for (list, (_, descending)) in books.iter().zip(BOOKS) {
        deferred.push(side(list, descending, &mut bytes)?);
    }
    if context.changes {
        changes(batch, &mut bytes)?;
    }
    // Canonical: the table is what the decoded content encodes to.
    let flags = prim::<UInt8Type>(batch, "flags")?.values();
    require!(
        !zero_price
            && !special_at_midnight
            && (!context.legacy || crate::files::clock_copied(batch))
            && (0..n).all(|i| flags[i]
                == if seq[i] == 0 {
                    schema::F_MBP | schema::F_SNAPSHOT
                } else {
                    schema::F_MBP
                }),
        DEPTHS_BAD
    );
    // Identity: a known issue, and its code as the master has it.
    let (mut hint, mut which) = (0usize, Vec::with_capacity(n));
    for &id in issue {
        match context.locate(id, &mut hint) {
            Some(index) => which.push(index),
            None => return refuse("Unknown issue"),
        }
    }
    for i in 0..n {
        require!(
            identifiers.value(i).as_bytes() == context.identifiers[which[i]].as_slice(),
            "Quote identity mismatch/duplicate"
        );
        bytes += context.master[which[i]].code.len() as u64 + 2;
    }
    // Sorted by issue, then batch.
    require!(
        (1..n).all(|i| (issue[i - 1], seq[i - 1]) < (issue[i], seq[i])),
        "Quotes not sorted by issue, then batch"
    );
    // In their batches: bootstraps' and quotes' only, none after their batch's clock.
    let (kinds, clocks) = (&batches.kinds, &batches.clocks);
    require!(
        seq.iter().all(|&s| (s as usize) < kinds.len() && matches!(kinds[s as usize], BOOTSTRAP | QUOTES)),
        "Quotes outside their batches"
    );
    require!(
        (0..n).all(|i| time[i] <= clocks[seq[i] as usize]),
        "Quotes after their batch clock"
    );
    if !batches.skipped.is_empty() {
        require!(
            (0..n).all(|i| batches
                .skipped
                .binary_search(&(issue[i] << 32 | seq[i]))
                .is_err()),
            "Invalid skipped issues"
        );
    }
    require!(
        frame.iter().all(|&f| f < 1 << 32) && issue.iter().all(|&i| i < 1 << 32),
        "Invalid unsigned integer"
    );
    for i in 1..n {
        if issue[i] == issue[i - 1] {
            require!(
                frame[i] >= frame[i - 1] && time[i] >= time[i - 1],
                "Quote frame/time regression"
            );
        }
    }
    for d in &deferred {
        require!(!d.too_long, "Invalid book side");
        require!(!d.unordered, "Book prices out of order");
    }
    for &s in seq {
        counts[s as usize] += 1;
    }
    bytes += context.quote_fixed * n as u64;
    let edge = |i: usize| Edge {
        issue: issue[i],
        seq: seq[i],
        frame: frame[i],
        time: time[i],
    };
    Ok(Group {
        rows: n,
        first: if n > 0 { edge(0) } else { Edge::default() },
        last: if n > 0 { edge(n - 1) } else { Edge::default() },
        bytes,
    })
}

/// The trades' state from one row group to the next.
#[derive(Default)]
pub struct Trades {
    last: Option<(u64, u64)>,
    carry: Option<(u64, u64, u64)>,
    pub rows: usize,
    pub bytes: u64,
}

fn write_id(buffer: &mut [u8; 48], frame: u64, place: u64) -> usize {
    let mut at = 0;
    for (value, tail) in [(frame, Some(b'-')), (place, None)] {
        let mut digits = [0u8; 20];
        let mut count = 0;
        let mut v = value;
        loop {
            digits[count] = b'0' + (v % 10) as u8;
            count += 1;
            v /= 10;
            if v == 0 {
                break;
            }
        }
        while count > 0 {
            count -= 1;
            buffer[at] = digits[count];
            at += 1;
        }
        if let Some(t) = tail {
            buffer[at] = t;
            at += 1;
        }
    }
    at
}

impl Trades {
    /// A trade row group, in order (the carry joins trade IDs across groups), checked as `verify_broker` does. `counts`
    /// is told how many trades each batch holds.
    pub fn check(&mut self, batch: &RecordBatch, context: &Context, counts: &mut [u32]) -> Res<()> {
        let n = batch.num_rows();
        let batches = context.batches;
        let (seq, issue, frame) = (
            u64s(batch, "seq")?,
            u64s(batch, "issue_id")?,
            u64s(batch, "frame")?,
        );
        for column in [seq, issue, frame] {
            require!(column.iter().all(|&v| v < TWO_53), "Invalid integer value");
        }
        let side = prim::<UInt8Type>(batch, "side")?.values();
        let time = micros(
            prim::<TimestampNanosecondType>(batch, "ts_event")?.values(),
            batches.midnight,
            "Invalid trade time",
        )?;
        let (price, size) = (
            prim::<Decimal128Type>(batch, "price")?.values(),
            prim::<Decimal128Type>(batch, "size")?.values(),
        );
        let mut prices = Vec::with_capacity(n);
        for i in 0..n {
            let (Some(p), Some(q)) = (tenths(price[i]), whole(size[i])) else {
                return refuse("Invalid decimal value");
            };
            prices.push((p, q));
        }
        let identifiers = strings(batch, "identifier")?;
        require!(
            (0..n).all(|i| identifiers
                .value(i)
                .as_bytes()
                .ends_with(VENUE_SUFFIX.as_bytes())),
            "Invalid identifier"
        );
        // Canonical: the aggressor sides are the decoder's trade types as v2's AggressorSide, and the trade IDs the issue's
        // frame number with the trade's place among that frame's trades.
        let aggressor = batch
            .column_by_name("aggressor_side")
            .and_then(|c| c.as_dictionary_opt::<Int8Type>())
            .ok_or_else(|| bad_schema("aggressor_side"))?;
        let names = aggressor
            .values()
            .as_string_opt::<i32>()
            .ok_or_else(|| bad_schema("aggressor_side"))?;
        let keys = aggressor.keys();
        for i in 0..n {
            let want = match side[i] {
                1 => "BUY",
                2 => "SELL",
                _ => "NO_AGGRESSOR",
            };
            require!(
                keys.is_valid(i)
                    && (keys.value(i) as usize) < names.len()
                    && names.value(keys.value(i) as usize) == want,
                TRADES_BAD
            );
        }
        require!(
            !context.legacy || crate::files::clock_copied(batch),
            TRADES_BAD
        );
        let ids = strings(batch, "trade_id")?;
        let mut buffer = [0u8; 48];
        let mut key: Option<(u64, u64)> = None;
        let mut place = 0u64;
        for i in 0..n {
            let (id, fr) = (issue[i], frame[i]);
            if key == Some((id, fr)) {
                place += 1;
            } else {
                place = match self.carry {
                    Some((ci, cf, cp)) if key.is_none() && ci == id && cf == fr => cp + 1,
                    _ => 0,
                };
            }
            key = Some((id, fr));
            let length = write_id(&mut buffer, fr, place);
            require!(ids.value(i).as_bytes() == &buffer[..length], TRADES_BAD);
        }
        if n > 0 {
            self.carry = Some((issue[n - 1], frame[n - 1], place));
        }
        // Identity.
        let (mut hint, mut which) = (0usize, Vec::with_capacity(n));
        for &id in issue {
            match context.locate(id, &mut hint) {
                Some(index) => which.push(index),
                None => return refuse("Unknown issue"),
            }
        }
        let mut bytes = 0u64;
        for i in 0..n {
            require!(
                identifiers.value(i).as_bytes() == context.identifiers[which[i]].as_slice(),
                "Quote identity mismatch/duplicate"
            );
            bytes += context.master[which[i]].code.len() as u64 + 2;
        }
        // Sorted by issue, then batch (trades of one batch keep the decoder's order).
        let mut last = self.last;
        for i in 0..n {
            let this = (issue[i], seq[i]);
            require!(
                last.is_none_or(|l| l <= this),
                "Trades not sorted by issue, then batch"
            );
            last = Some(this);
        }
        self.last = last;
        let (kinds, clocks) = (&batches.kinds, &batches.clocks);
        require!(
            seq.iter().all(|&s| (s as usize) < kinds.len()
                && matches!(kinds[s as usize], BOOTSTRAP | QUOTES | HISTORY)),
            "Trades outside their batches"
        );
        require!(
            (0..n).all(|i| time[i] <= clocks[seq[i] as usize]),
            "Trades after their batch clock"
        );
        require!(
            (0..n).all(|i| prices[i].0 > 0 && prices[i].1 > 0 && frame[i] < 1 << 32),
            "Invalid trade"
        );
        for i in 0..n {
            counts[seq[i] as usize] += 1;
            bytes += digits(issue[i])
                + digits(frame[i])
                + digits(time[i] as u64)
                + digits(prices[i].0)
                + digits(prices[i].1)
                + digits(side[i] as u64);
        }
        self.rows += n;
        self.bytes += bytes + context.trade_fixed * n as u64;
        Ok(())
    }
}

#[allow(dead_code)]
fn _unused(_: &ArrayRef) {}
