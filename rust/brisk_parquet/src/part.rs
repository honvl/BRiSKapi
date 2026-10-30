//! A spool part: the decoder's JSON lines of a few seconds of a session, written as the depths, trades and batch tables of
//! a recording (`write_part` of `briskapi/_parquet.py`). The lines are read straight into column buffers, in pieces on
//! several threads, so that a quote is never an object; the rows are sorted by issue, then batch; a row group of depths
//! holds a block of issues, as `package` reads them a range at a time.
use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::builder::StringBuilder;
use arrow_array::{
    ArrayRef, Decimal128Array, DictionaryArray, ListArray, RecordBatch, StringArray, StructArray,
    TimestampNanosecondArray, UInt8Array, UInt32Array, UInt64Array, new_null_array,
};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field};
use rayon::prelude::*;
use serde::de::{DeserializeSeed, Error as _, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde_json::{Value, json};

use crate::batches::{BOOTSTRAP, END, HISTORY, QUOTES};
use crate::error::{Failure, Res};
use crate::require;
use crate::schema::{
    self, BATCHES, BOOK_CHANGES, COUNTS, DEPTHS, F_MBP, F_SNAPSHOT, PRICES, QUOTE_KEYS, TRADE_KEYS,
    TRADES, VENUE_SUFFIX,
};
use crate::sink::{Sink, Stage};
use crate::units::{tenths_raw, whole_raw};
use crate::writer;

/// Issues whose rows share a row group of a part.
const PART_ISSUES: i64 = 32;

pub struct PartConfig {
    pub out: PathBuf,
    pub midnight: i64,
    pub threads: usize,
}

pub struct PartSummary {
    pub batches: usize,
    pub quotes: usize,
    pub trades: usize,
}

fn slot_of(key: &str) -> usize {
    QUOTE_KEYS
        .iter()
        .position(|k| *k == key)
        .expect("a quote key")
}

/// A quote's keys, and (last) the one a broker's quote may have for its book changes.
const PART_QUOTE_KEYS: [&str; 44] = {
    let mut keys = [BOOK_CHANGES; 44];
    let mut i = 0;
    while i < QUOTE_KEYS.len() {
        keys[i] = QUOTE_KEYS[i];
        i += 1;
    }
    keys
};
const CHANGES_SLOT: usize = 43;
const CODE_SLOT: usize = 1;
const BOOK_SLOTS: [usize; 4] = [31, 32, 33, 34];
const QUOTE_ALL: u64 = (1 << 43) - 1;
const TRADE_ALL: u32 = (1 << 7) - 1;

// ---- what a chunk of lines holds

#[derive(Default)]
struct Book {
    counts: Vec<u32>,
    /// price10, quantity, orders, level after level.
    levels: Vec<i64>,
}

struct QuoteCols {
    ints: Vec<Vec<i64>>,
    codes: Vec<u64>,
    seq: Vec<u64>,
    books: [Book; 4],
    /// Whether the quotes have book changes (all do, or none), once there is a quote to say; and the changes, as a book's but
    /// with four numbers to a level: book, price10, quantity, orders.
    with_changes: Option<bool>,
    changes: Book,
}

impl QuoteCols {
    fn new() -> Self {
        QuoteCols {
            ints: vec![Vec::new(); QUOTE_KEYS.len()],
            codes: Vec::new(),
            seq: Vec::new(),
            books: Default::default(),
            with_changes: None,
            changes: Book::default(),
        }
    }
    fn len(&self) -> usize {
        self.codes.len()
    }
    fn append(&mut self, other: QuoteCols) -> Res<()> {
        require!(
            self.with_changes
                .zip(other.with_changes)
                .is_none_or(|(a, b)| a == b),
            "Quotes with and without book changes"
        );
        self.with_changes = self.with_changes.or(other.with_changes);
        for (a, b) in self.ints.iter_mut().zip(other.ints) {
            a.extend(b);
        }
        self.codes.extend(other.codes);
        self.seq.extend(other.seq);
        for (a, b) in self.books.iter_mut().zip(other.books) {
            a.counts.extend(b.counts);
            a.levels.extend(b.levels);
        }
        self.changes.counts.extend(other.changes.counts);
        self.changes.levels.extend(other.changes.levels);
        Ok(())
    }
}

#[derive(Default)]
struct TradeCols {
    issue: Vec<i64>,
    code: Vec<u64>,
    frame: Vec<i64>,
    time: Vec<i64>,
    price: Vec<i64>,
    quantity: Vec<i64>,
    side: Vec<i64>,
    seq: Vec<u64>,
}

impl TradeCols {
    fn len(&self) -> usize {
        self.issue.len()
    }
    fn append(&mut self, other: TradeCols) {
        self.issue.extend(other.issue);
        self.code.extend(other.code);
        self.frame.extend(other.frame);
        self.time.extend(other.time);
        self.price.extend(other.price);
        self.quantity.extend(other.quantity);
        self.side.extend(other.side);
        self.seq.extend(other.seq);
    }
}

#[derive(Default, Clone)]
struct BatchRow {
    kind: u8,
    seq: u64,
    source_time_us: i64,
    received_unix_ms: Option<u64>,
    decode_ns: Option<u64>,
    replay_lateness_ms: Option<f64>,
    skipped: Option<Vec<u64>>,
    frames: Option<u64>,
    quote_updates: Option<u64>,
    replay_wall_ms: Option<f64>,
    source: Option<String>,
    trading_date: Option<String>,
    input_transport: Option<String>,
    origin: Option<String>,
    exchange_delay_ms: Option<f64>,
    market_issue_count: Option<u64>,
}

struct Chunk {
    batches: Vec<BatchRow>,
    quotes: QuoteCols,
    trades: TradeCols,
}

fn pack(code: &str) -> Result<u64, String> {
    let bytes = code.as_bytes();
    if bytes.len() > 8 {
        return Err("Invalid security code".into());
    }
    let mut padded = [0u8; 8];
    padded[..bytes.len()].copy_from_slice(bytes);
    Ok(u64::from_le_bytes(padded))
}

fn unpack(code: u64) -> String {
    let bytes = code.to_le_bytes();
    let end = bytes.iter().position(|b| *b == 0).unwrap_or(8);
    String::from_utf8_lossy(&bytes[..end]).into_owned()
}

// ---- serde visitors that fill the buffers

struct Key<const N: usize>(&'static [&'static str; N]);

impl<'de, const N: usize> DeserializeSeed<'de> for Key<N> {
    type Value = usize;
    fn deserialize<D: serde::Deserializer<'de>>(self, deserializer: D) -> Result<usize, D::Error> {
        struct V<'a, const N: usize>(&'a [&'static str; N]);
        impl<'de, const N: usize> Visitor<'de> for V<'_, N> {
            type Value = usize;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a known key")
            }
            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<usize, E> {
                self.0
                    .iter()
                    .position(|k| *k == v)
                    .ok_or_else(|| E::custom(format!("Unknown or missing fields: {v}")))
            }
        }
        deserializer.deserialize_identifier(V(self.0))
    }
}

struct Code;

impl<'de> DeserializeSeed<'de> for Code {
    type Value = u64;
    fn deserialize<D: serde::Deserializer<'de>>(self, deserializer: D) -> Result<u64, D::Error> {
        struct V;
        impl Visitor<'_> for V {
            type Value = u64;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a security code")
            }
            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<u64, E> {
                pack(v).map_err(E::custom)
            }
        }
        deserializer.deserialize_str(V)
    }
}

struct Levels<'a>(&'a mut Book);

impl<'de> DeserializeSeed<'de> for Levels<'_> {
    type Value = ();
    fn deserialize<D: serde::Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        deserializer.deserialize_seq(self)
    }
}

impl<'de> Visitor<'de> for Levels<'_> {
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a book side")
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<(), A::Error> {
        let mut count = 0u32;
        while seq.next_element_seed(Level(&mut self.0.levels))?.is_some() {
            count += 1;
        }
        self.0.counts.push(count);
        Ok(())
    }
}

struct Level<'a>(&'a mut Vec<i64>);

impl<'de> DeserializeSeed<'de> for Level<'_> {
    type Value = ();
    fn deserialize<D: serde::Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        deserializer.deserialize_seq(self)
    }
}

impl<'de> Visitor<'de> for Level<'_> {
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a book level of three numbers")
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<(), A::Error> {
        for _ in 0..3 {
            match seq.next_element::<i64>()? {
                Some(v) => self.0.push(v),
                None => return Err(A::Error::custom("Invalid book level")),
            }
        }
        if seq.next_element::<IgnoredAny>()?.is_some() {
            return Err(A::Error::custom("Invalid book level"));
        }
        Ok(())
    }
}

/// A quote's book changes: a list of [book, price10, quantity, orders].
struct Changes<'a>(&'a mut Book);

impl<'de> DeserializeSeed<'de> for Changes<'_> {
    type Value = ();
    fn deserialize<D: serde::Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        deserializer.deserialize_seq(self)
    }
}

impl<'de> Visitor<'de> for Changes<'_> {
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("book changes")
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<(), A::Error> {
        let mut count = 0u32;
        while seq.next_element_seed(Change(&mut self.0.levels))?.is_some() {
            count += 1;
        }
        self.0.counts.push(count);
        Ok(())
    }
}

struct Change<'a>(&'a mut Vec<i64>);

impl<'de> DeserializeSeed<'de> for Change<'_> {
    type Value = ();
    fn deserialize<D: serde::Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        deserializer.deserialize_seq(self)
    }
}

impl<'de> Visitor<'de> for Change<'_> {
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a book change of four numbers")
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<(), A::Error> {
        for _ in 0..4 {
            match seq.next_element::<i64>()? {
                Some(v) => self.0.push(v),
                None => return Err(A::Error::custom("Invalid book change")),
            }
        }
        if seq.next_element::<IgnoredAny>()?.is_some() {
            return Err(A::Error::custom("Invalid book change"));
        }
        Ok(())
    }
}

struct Quotes<'a>(&'a mut QuoteCols);

impl<'de> DeserializeSeed<'de> for Quotes<'_> {
    type Value = ();
    fn deserialize<D: serde::Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        deserializer.deserialize_seq(self)
    }
}

impl<'de> Visitor<'de> for Quotes<'_> {
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a list of quotes")
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<(), A::Error> {
        while seq.next_element_seed(OneQuote(self.0))?.is_some() {}
        Ok(())
    }
}

struct OneQuote<'a>(&'a mut QuoteCols);

impl<'de> DeserializeSeed<'de> for OneQuote<'_> {
    type Value = ();
    fn deserialize<D: serde::Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        deserializer.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for OneQuote<'_> {
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a quote")
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
        let cols = self.0;
        let mut seen = 0u64;
        while let Some(slot) = map.next_key_seed(Key(&PART_QUOTE_KEYS))? {
            if seen & (1 << slot) != 0 {
                return Err(A::Error::custom("Unknown or missing quote fields"));
            }
            seen |= 1 << slot;
            if slot == CHANGES_SLOT {
                map.next_value_seed(Changes(&mut cols.changes))?;
            } else if slot == CODE_SLOT {
                cols.codes.push(map.next_value_seed(Code)?);
            } else if let Some(side) = BOOK_SLOTS.iter().position(|b| *b == slot) {
                map.next_value_seed(Levels(&mut cols.books[side]))?;
            } else {
                let value = map.next_value::<i64>()?;
                cols.ints[slot].push(value);
            }
        }
        // A chunk's quotes all have book changes, or none do.
        let with = seen & (1 << CHANGES_SLOT) != 0;
        if seen & QUOTE_ALL != QUOTE_ALL || *cols.with_changes.get_or_insert(with) != with {
            return Err(A::Error::custom("Unknown or missing quote fields"));
        }
        Ok(())
    }
}

struct Trades<'a>(&'a mut TradeCols);

impl<'de> DeserializeSeed<'de> for Trades<'_> {
    type Value = ();
    fn deserialize<D: serde::Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        deserializer.deserialize_seq(self)
    }
}

impl<'de> Visitor<'de> for Trades<'_> {
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a list of trades")
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<(), A::Error> {
        while seq.next_element_seed(OneTrade(self.0))?.is_some() {}
        Ok(())
    }
}

struct OneTrade<'a>(&'a mut TradeCols);

impl<'de> DeserializeSeed<'de> for OneTrade<'_> {
    type Value = ();
    fn deserialize<D: serde::Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        deserializer.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for OneTrade<'_> {
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a trade")
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
        let t = self.0;
        let mut seen = 0u32;
        while let Some(slot) = map.next_key_seed(Key(&TRADE_KEYS))? {
            if seen & (1 << slot) != 0 {
                return Err(A::Error::custom("Unknown or missing trade fields"));
            }
            seen |= 1 << slot;
            match slot {
                0 => t.issue.push(map.next_value()?),
                1 => t.code.push(map.next_value_seed(Code)?),
                2 => t.frame.push(map.next_value()?),
                3 => t.time.push(map.next_value()?),
                4 => t.price.push(map.next_value()?),
                5 => t.quantity.push(map.next_value()?),
                _ => t.side.push(map.next_value()?),
            }
        }
        if seen != TRADE_ALL {
            return Err(A::Error::custom("Unknown or missing trade fields"));
        }
        Ok(())
    }
}

const BATCH_KEYS: [&str; 19] = [
    "type",
    "seq",
    "source_time_us",
    "received_unix_ms",
    "decode_ns",
    "replay_lateness_ms",
    "quotes",
    "trades",
    "master",
    "source",
    "trading_date",
    "input_transport",
    "source_timestamp_origin",
    "exchange_delay_ms",
    "market_issue_count",
    "frames",
    "quote_updates",
    "replay_wall_ms",
    "skipped_issues",
];

/// The keys a kind of batch has (SKIPPED aside), as `schema.BATCH_KEYS`.
fn required(kind: u8) -> u32 {
    let bits = |names: &[&str]| {
        names.iter().fold(0u32, |m, n| {
            m | 1 << BATCH_KEYS.iter().position(|k| k == n).expect("a batch key")
        })
    };
    match kind {
        BOOTSTRAP => bits(&[
            "type",
            "seq",
            "source",
            "trading_date",
            "input_transport",
            "source_timestamp_origin",
            "exchange_delay_ms",
            "source_time_us",
            "market_issue_count",
            "master",
            "quotes",
            "trades",
        ]),
        QUOTES => bits(&[
            "type",
            "seq",
            "source_time_us",
            "received_unix_ms",
            "decode_ns",
            "replay_lateness_ms",
            "quotes",
            "trades",
        ]),
        HISTORY => bits(&["type", "seq", "source_time_us", "trades"]),
        _ => bits(&[
            "type",
            "seq",
            "source_time_us",
            "frames",
            "quote_updates",
            "replay_wall_ms",
        ]),
    }
}

/// Microsecond precision, as `canonical_batches` rounds the local timings.
fn round3(v: f64) -> f64 {
    format!("{v:.3}").parse().unwrap_or(v)
}

/// The fixed transport of the site a bootstrap's transport matches, or the transport as it is (timings rounded).
fn canonical_transport(source: &str, value: Value) -> Value {
    for fixed in crate::batches::transports(source) {
        if let (Value::Object(want), Value::Object(have)) = (&fixed, &value)
            && want.iter().all(|(k, v)| have.get(k) == Some(v))
        {
            return fixed;
        }
    }
    match value {
        Value::Object(mut map) => {
            if let Some(Value::Number(n)) = map.get("asset_fetch_ms").cloned()
                && n.is_f64()
            {
                map.insert(
                    "asset_fetch_ms".into(),
                    json!(round3(n.as_f64().unwrap_or(0.0))),
                );
            }
            Value::Object(map)
        }
        other => other,
    }
}

struct Batch<'a>(&'a mut Chunk);

impl<'de> DeserializeSeed<'de> for Batch<'_> {
    type Value = ();
    fn deserialize<D: serde::Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        deserializer.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for Batch<'_> {
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a batch")
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
        let chunk = self.0;
        let (quote_start, trade_start) = (chunk.quotes.len(), chunk.trades.len());
        let (mut row, mut seen, mut kind) = (BatchRow::default(), 0u32, None::<String>);
        let mut transport: Option<Value> = None;
        while let Some(slot) = map.next_key_seed(Key(&BATCH_KEYS))? {
            if slot != 18 && seen & (1 << slot) != 0 {
                return Err(A::Error::custom("Unknown or missing batch fields"));
            }
            seen |= 1 << slot;
            match slot {
                0 => kind = Some(map.next_value::<String>()?),
                1 => row.seq = map.next_value()?,
                2 => row.source_time_us = map.next_value()?,
                3 => row.received_unix_ms = Some(map.next_value()?),
                4 => row.decode_ns = Some(map.next_value()?),
                5 => row.replay_lateness_ms = map.next_value::<Option<f64>>()?.map(round3),
                6 => map.next_value_seed(Quotes(&mut chunk.quotes))?,
                7 => map.next_value_seed(Trades(&mut chunk.trades))?,
                8 => {
                    map.next_value::<IgnoredAny>()?;
                }
                9 => row.source = Some(map.next_value()?),
                10 => row.trading_date = Some(map.next_value()?),
                11 => transport = Some(map.next_value()?),
                12 => row.origin = Some(map.next_value()?),
                13 => row.exchange_delay_ms = map.next_value()?,
                14 => row.market_issue_count = Some(map.next_value()?),
                15 => row.frames = Some(map.next_value()?),
                16 => row.quote_updates = Some(map.next_value()?),
                17 => row.replay_wall_ms = Some(round3(map.next_value()?)),
                _ => {
                    let mut issues: Vec<u64> = map.next_value()?;
                    issues.sort_unstable();
                    row.skipped = Some(issues);
                }
            }
        }
        let code = match kind.as_deref() {
            Some("bootstrap") => BOOTSTRAP,
            Some("quotes") => QUOTES,
            Some("history") => HISTORY,
            Some("end") => END,
            _ => return Err(A::Error::custom("Unknown or missing batch fields")),
        };
        if seen & !(1 << 18) != required(code) {
            return Err(A::Error::custom("Unknown or missing batch fields"));
        }
        row.kind = code;
        if let Some(value) = transport {
            let canonical = canonical_transport(row.source.as_deref().unwrap_or(""), value);
            row.input_transport =
                Some(serde_json::to_string(&canonical).map_err(A::Error::custom)?);
        }
        // The rows of this batch are numbered with it.
        let seq = row.seq;
        chunk.quotes.seq.resize(chunk.quotes.len(), seq);
        debug_assert!(chunk.quotes.seq.len() >= quote_start);
        chunk.trades.seq.resize(chunk.trades.len(), seq);
        debug_assert!(chunk.trades.seq.len() >= trade_start);
        chunk.batches.push(row);
        Ok(())
    }
}

fn parse_lines(lines: &[&[u8]]) -> Res<Chunk> {
    let mut chunk = Chunk {
        batches: Vec::with_capacity(lines.len()),
        quotes: QuoteCols::new(),
        trades: TradeCols::default(),
    };
    for line in lines {
        let mut deserializer = serde_json::Deserializer::from_slice(line);
        Batch(&mut chunk)
            .deserialize(&mut deserializer)
            .map_err(|e| Failure::Refused(format!("Invalid batch: {e}")))?;
        deserializer
            .end()
            .map_err(|e| Failure::Refused(format!("Invalid batch: {e}")))?;
    }
    Ok(chunk)
}

// ---- the tables

fn decimal(values: impl Iterator<Item = Option<i128>>) -> Res<ArrayRef> {
    let array: Decimal128Array = values.collect();
    Ok(Arc::new(
        array
            .with_precision_and_scale(38, 16)
            .map_err(Failure::from)?,
    ))
}

fn unsigned(values: &[i64], order: &[u32]) -> Res<ArrayRef> {
    let mut out = Vec::with_capacity(order.len());
    for &i in order {
        let v = values[i as usize];
        require!(v >= 0, "Invalid negative value");
        out.push(v as u64);
    }
    Ok(Arc::new(UInt64Array::from(out)))
}

fn tenths(values: &[i64], order: &[u32], none_is_zero: bool) -> Res<ArrayRef> {
    let mut out = Vec::with_capacity(order.len());
    for &i in order {
        let v = values[i as usize];
        require!(v >= 0, "Invalid negative value");
        out.push(if none_is_zero && v == 0 {
            None
        } else {
            Some(tenths_raw(v as u64))
        });
    }
    decimal(out.into_iter())
}

fn clock(values: &[i64], order: &[u32], midnight: i64, none_is_zero: bool) -> ArrayRef {
    let out: Vec<Option<i64>> = order
        .iter()
        .map(|&i| {
            let v = values[i as usize];
            if none_is_zero && v == 0 {
                None
            } else {
                Some(midnight + v * 1000)
            }
        })
        .collect();
    Arc::new(TimestampNanosecondArray::from(out).with_timezone("UTC"))
}

fn identifiers(codes: &[u64], order: &[u32]) -> ArrayRef {
    let mut names: HashMap<u64, String> = HashMap::new();
    let mut builder = StringBuilder::with_capacity(order.len(), order.len() * 9);
    for &i in order {
        let code = codes[i as usize];
        let name = names
            .entry(code)
            .or_insert_with(|| format!("{}{}", unpack(code), VENUE_SUFFIX));
        builder.append_value(name);
    }
    Arc::new(builder.finish())
}

fn book(book: &Book, order: &[u32], starts: &[usize]) -> Res<ArrayRef> {
    let mut offsets = Vec::with_capacity(order.len() + 1);
    offsets.push(0i32);
    let (mut price, mut size, mut count) = (Vec::new(), Vec::new(), Vec::new());
    for &i in order {
        let i = i as usize;
        let n = book.counts[i] as usize;
        let at = starts[i] * 3;
        for level in book.levels[at..at + n * 3].as_chunks::<3>().0 {
            require!(
                level[0] >= 0 && level[1] >= 0 && level[2] >= 0,
                "Invalid negative value"
            );
            require!(level[2] <= u32::MAX as i64, "Invalid book level");
            price.push(tenths_raw(level[0] as u64));
            size.push(whole_raw(level[1] as u64));
            count.push(level[2] as u32);
        }
        offsets.push(offsets[offsets.len() - 1] + n as i32);
    }
    let rows = price.len();
    let decimals = |v: Vec<i128>| -> Res<ArrayRef> {
        Ok(Arc::new(
            Decimal128Array::from(v)
                .with_precision_and_scale(38, 16)
                .map_err(Failure::from)?,
        ))
    };
    let items = StructArray::new(
        schema::level_fields(),
        vec![
            decimals(price)?,
            decimals(size)?,
            Arc::new(UInt32Array::from(count)),
            Arc::new(UInt64Array::from(vec![0u64; rows])),
        ],
        None,
    );
    let field = Arc::new(Field::new(
        "item",
        DataType::Struct(schema::level_fields()),
        false,
    ));
    Ok(Arc::new(ListArray::new(
        field,
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        Arc::new(items),
        None,
    )))
}

/// Where each row's levels start in a book's flat levels, from the counts.
fn level_starts(book: &Book) -> Vec<usize> {
    let mut starts = Vec::with_capacity(book.counts.len());
    let mut at = 0usize;
    for &n in &book.counts {
        starts.push(at);
        at += n as usize;
    }
    starts
}

/// The quotes' book changes of the rows `order`, as the `changes` column. `starts`: where each row's changes begin.
fn change_list(changes: &Book, order: &[u32], starts: &[usize]) -> Res<ArrayRef> {
    let mut offsets = Vec::with_capacity(order.len() + 1);
    offsets.push(0i32);
    let (mut book, mut price, mut size, mut count) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for &i in order {
        let i = i as usize;
        let n = changes.counts[i] as usize;
        let at = starts[i] * 4;
        for change in changes.levels[at..at + n * 4].as_chunks::<4>().0 {
            require!(
                (0..schema::BOOKS.len() as i64).contains(&change[0])
                    && (0..=u32::MAX as i64).contains(&change[3]),
                "Invalid book change"
            );
            require!(change[1] >= 0 && change[2] >= 0, "Invalid negative value");
            book.push(change[0] as u8);
            price.push(tenths_raw(change[1] as u64));
            size.push(whole_raw(change[2] as u64));
            count.push(change[3] as u32);
        }
        offsets.push(offsets[offsets.len() - 1] + n as i32);
    }
    let decimals = |v: Vec<i128>| -> Res<ArrayRef> {
        Ok(Arc::new(
            Decimal128Array::from(v)
                .with_precision_and_scale(38, 16)
                .map_err(Failure::from)?,
        ))
    };
    let items = StructArray::new(
        schema::change_fields(),
        vec![
            Arc::new(UInt8Array::from(book)),
            decimals(price)?,
            decimals(size)?,
            Arc::new(UInt32Array::from(count)),
        ],
        None,
    );
    let field = Arc::new(Field::new(
        "item",
        DataType::Struct(schema::change_fields()),
        false,
    ));
    Ok(Arc::new(ListArray::new(
        field,
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        Arc::new(items),
        None,
    )))
}

fn depth_batch(
    cols: &QuoteCols,
    starts: &[Vec<usize>; 4],
    change_starts: &[usize],
    order: &[u32],
    midnight: i64,
) -> Res<RecordBatch> {
    let ints = |name: &str| unsigned(&cols.ints[slot_of(name)], order);
    let seq: Vec<u64> = order.iter().map(|&i| cols.seq[i as usize]).collect();
    let flags: Vec<u8> = seq
        .iter()
        .map(|&s| if s == 0 { F_MBP | F_SNAPSHOT } else { F_MBP })
        .collect();
    let mut columns: Vec<ArrayRef> = vec![
        book(&cols.books[0], order, &starts[0])?,
        book(&cols.books[1], order, &starts[1])?,
        Arc::new(UInt8Array::from(flags)),
        ints("frame")?,
        clock(
            &cols.ints[slot_of("source_time_us")],
            order,
            midnight,
            false,
        ),
        identifiers(&cols.codes, order),
        ints("issue_id")?,
        Arc::new(UInt64Array::from(seq)),
    ];
    for name in PRICES {
        columns.push(tenths(
            &cols.ints[slot_of(&format!("{name}10"))],
            order,
            true,
        )?);
    }
    for name in COUNTS {
        columns.push(ints(name)?);
    }
    columns.push(clock(
        &cols.ints[slot_of("special_quote_time_us")],
        order,
        midnight,
        true,
    ));
    columns.push(tenths(&cols.ints[slot_of("turnover10")], order, false)?);
    columns.push(book(&cols.books[2], order, &starts[2])?);
    columns.push(book(&cols.books[3], order, &starts[3])?);
    let changed = cols.with_changes == Some(true);
    if changed {
        // The checkpoints are the packaging's: a part holds none.
        columns.push(change_list(&cols.changes, order, change_starts)?);
        columns.push(new_null_array(&schema::changes_type(), order.len()));
    }
    RecordBatch::try_new(Arc::new(schema::depths(changed)), columns).map_err(Failure::from)
}

fn trade_batch(cols: &TradeCols, order: &[u32], places: &[u64], midnight: i64) -> Res<RecordBatch> {
    let pick = |v: &[i64]| -> Vec<i64> { order.iter().map(|&i| v[i as usize]).collect() };
    let mut prices = Vec::with_capacity(order.len());
    let mut sizes = Vec::with_capacity(order.len());
    for &i in order {
        let (p, q) = (cols.price[i as usize], cols.quantity[i as usize]);
        require!(p >= 0 && q >= 0, "Invalid negative value");
        prices.push(Some(tenths_raw(p as u64)));
        sizes.push(Some(whole_raw(q as u64)));
    }
    let sides = pick(&cols.side);
    require!(
        sides.iter().all(|s| (0..256).contains(s)),
        "Invalid trade side"
    );
    let keys: Vec<i8> = sides
        .iter()
        .map(|&s| {
            if s == 1 {
                1
            } else if s == 2 {
                2
            } else {
                0
            }
        })
        .collect();
    let names: ArrayRef = Arc::new(StringArray::from(vec!["NO_AGGRESSOR", "BUY", "SELL"]));
    let aggressor: ArrayRef = Arc::new(
        DictionaryArray::<arrow_array::types::Int8Type>::try_new(
            arrow_array::Int8Array::from(keys),
            names,
        )
        .map_err(Failure::from)?,
    );
    // Trade IDs: the issue's frame number and the trade's place among that frame's trades.
    let frames = pick(&cols.frame);
    let mut ids = StringBuilder::with_capacity(order.len(), order.len() * 10);
    for i in 0..order.len() {
        ids.append_value(format!("{}-{}", frames[i], places[i]));
    }
    let seq: Vec<u64> = order.iter().map(|&i| cols.seq[i as usize]).collect();
    let columns: Vec<ArrayRef> = vec![
        decimal(prices.into_iter())?,
        decimal(sizes.into_iter())?,
        aggressor,
        Arc::new(ids.finish()),
        clock(&cols.time, order, midnight, false),
        identifiers(&cols.code, order),
        unsigned(&cols.issue, order)?,
        Arc::new(UInt64Array::from(seq)),
        unsigned(&cols.frame, order)?,
        Arc::new(UInt8Array::from(
            sides.iter().map(|&s| s as u8).collect::<Vec<u8>>(),
        )),
    ];
    RecordBatch::try_new(Arc::new(schema::trade_schema()), columns).map_err(Failure::from)
}

fn batch_table(rows: &[BatchRow], midnight: i64) -> Res<RecordBatch> {
    let n = rows.len();
    let u64s = |f: &dyn Fn(&BatchRow) -> Option<u64>| -> ArrayRef {
        Arc::new(UInt64Array::from(rows.iter().map(f).collect::<Vec<_>>()))
    };
    let f64s = |f: &dyn Fn(&BatchRow) -> Option<f64>| -> ArrayRef {
        Arc::new(arrow_array::Float64Array::from(
            rows.iter().map(f).collect::<Vec<_>>(),
        ))
    };
    let strings = |f: &dyn Fn(&BatchRow) -> Option<String>| -> ArrayRef {
        Arc::new(StringArray::from(rows.iter().map(f).collect::<Vec<_>>()))
    };
    let field = Arc::new(Field::new("item", DataType::UInt64, false));
    let skipped: ArrayRef = if rows.iter().any(|r| r.skipped.is_some()) {
        let mut offsets = vec![0usize];
        let mut values = Vec::new();
        let mut valid = Vec::with_capacity(n);
        for row in rows {
            if let Some(list) = &row.skipped {
                values.extend_from_slice(list);
            }
            valid.push(row.skipped.is_some());
            offsets.push(values.len());
        }
        let lengths: Vec<usize> = offsets.windows(2).map(|w| w[1] - w[0]).collect();
        Arc::new(ListArray::new(
            field,
            OffsetBuffer::from_lengths(lengths),
            Arc::new(UInt64Array::from(values)),
            Some(arrow_buffer::NullBuffer::from(valid)),
        ))
    } else {
        new_null_array(&DataType::List(field), n)
    };
    let kinds = ["bootstrap", "quotes", "history", "end"];
    let ts: Vec<i64> = rows
        .iter()
        .map(|r| midnight + r.source_time_us * 1000)
        .collect();
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from(
            rows.iter().map(|r| r.seq).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|r| kinds[r.kind as usize])
                .collect::<Vec<_>>(),
        )),
    ];
    columns.push(u64s(&|r| r.received_unix_ms));
    columns.push(u64s(&|r| r.decode_ns));
    columns.push(f64s(&|r| r.replay_lateness_ms));
    columns.push(skipped);
    columns.push(u64s(&|r| r.frames));
    columns.push(u64s(&|r| r.quote_updates));
    columns.push(f64s(&|r| r.replay_wall_ms));
    columns.push(strings(&|r| r.source.clone()));
    columns.push(strings(&|r| r.trading_date.clone()));
    columns.push(strings(&|r| r.input_transport.clone()));
    columns.push(strings(&|r| r.origin.clone()));
    columns.push(f64s(&|r| r.exchange_delay_ms));
    columns.push(u64s(&|r| r.market_issue_count));
    columns.push(Arc::new(StringArray::from(vec!["BriskBatch"; n])));
    columns.push(Arc::new(
        TimestampNanosecondArray::from(ts).with_timezone("UTC"),
    ));
    RecordBatch::try_new(Arc::new(schema::batch_schema()), columns).map_err(Failure::from)
}

/// The groups of a sorted order whose rows share a block of issues (and are at most `limit` rows).
fn groups(issues: &[i64], order: &[u32], limit: usize) -> Vec<(usize, usize)> {
    let mut out = Vec::new();
    let mut start = 0usize;
    for i in 1..=order.len() {
        let split = i == order.len()
            || issues[order[i] as usize].div_euclid(PART_ISSUES)
                != issues[order[start] as usize].div_euclid(PART_ISSUES)
            || i - start >= limit;
        if split {
            out.push((start, i));
            start = i;
        }
    }
    out
}

pub fn write_part(input: &[u8], config: &PartConfig) -> Res<PartSummary> {
    let pool = Arc::new(
        rayon::ThreadPoolBuilder::new()
            .num_threads(config.threads.max(1))
            .build()
            .map_err(|e| Failure::Fault(e.into()))?,
    );
    let lines: Vec<&[u8]> = input
        .split(|b| *b == b'\n')
        .filter(|l| !l.is_empty())
        .collect();
    require!(!lines.is_empty(), "Recording has no bootstrap");
    let size = lines.len().div_ceil(config.threads.max(1) * 3).max(1);
    let parsed: Vec<Res<Chunk>> =
        pool.install(|| lines.par_chunks(size).map(parse_lines).collect());
    let mut whole = Chunk {
        batches: Vec::new(),
        quotes: QuoteCols::new(),
        trades: TradeCols::default(),
    };
    for chunk in parsed {
        let chunk = chunk?;
        whole.batches.extend(chunk.batches);
        whole.quotes.append(chunk.quotes)?;
        whole.trades.append(chunk.trades);
    }
    let midnight = config.midnight;
    let (n_quotes, n_trades) = (whole.quotes.len(), whole.trades.len());
    require!(
        whole.quotes.seq.len() == n_quotes && whole.trades.seq.len() == n_trades,
        "Invalid batch"
    );
    let out = &config.out;
    std::fs::create_dir_all(out)?;

    let properties = |name: &str| writer::part_properties(name, schema::ROW_GROUP);
    let plain = || Stage::plain();
    // Batches
    let mut sink = Sink::create_with(
        out,
        BATCHES,
        schema::expected(BATCHES, false),
        properties(BATCHES),
        schema::ROW_GROUP,
        pool.clone(),
        plain(),
    )?;
    sink.push_group(batch_table(&whole.batches, midnight)?)?;
    // Depths: by issue, then batch.
    let mut depths = Sink::create_with(
        out,
        DEPTHS,
        schema::depths(whole.quotes.with_changes == Some(true)),
        properties(DEPTHS),
        schema::ROW_GROUP,
        pool.clone(),
        plain(),
    )?;
    let issues = &whole.quotes.ints[slot_of("issue_id")];
    let mut order: Vec<u32> = (0..n_quotes as u32).collect();
    order.sort_unstable_by_key(|&i| (issues[i as usize], whole.quotes.seq[i as usize], i));
    let starts: [Vec<usize>; 4] = [0, 1, 2, 3].map(|b| level_starts(&whole.quotes.books[b]));
    let change_starts = level_starts(&whole.quotes.changes);
    for (a, b) in groups(issues, &order, schema::ROW_GROUP) {
        depths.push_group(depth_batch(
            &whole.quotes,
            &starts,
            &change_starts,
            &order[a..b],
            midnight,
        )?)?;
    }
    // Trades: by issue, then batch, then the decoder's order.
    let mut trades = Sink::create_with(
        out,
        TRADES,
        schema::expected(TRADES, false),
        properties(TRADES),
        schema::ROW_GROUP,
        pool.clone(),
        plain(),
    )?;
    let mut order: Vec<u32> = (0..n_trades as u32).collect();
    order.sort_unstable_by_key(|&i| {
        (
            whole.trades.issue[i as usize],
            whole.trades.seq[i as usize],
            i,
        )
    });
    // The place of each trade among its issue's trades of one frame, over the whole table (a group may cut a run).
    let mut places = Vec::with_capacity(n_trades);
    let (mut key, mut place) = (None, 0u64);
    for &i in &order {
        let this = (
            whole.trades.issue[i as usize],
            whole.trades.frame[i as usize],
        );
        place = if key == Some(this) { place + 1 } else { 0 };
        key = Some(this);
        places.push(place);
    }
    for (a, b) in groups(&whole.trades.issue, &order, schema::ROW_GROUP) {
        trades.push_group(trade_batch(
            &whole.trades,
            &order[a..b],
            &places[a..b],
            midnight,
        )?)?;
    }
    sink.finish()?;
    depths.finish()?;
    trades.finish()?;
    Ok(PartSummary {
        batches: whole.batches.len(),
        quotes: n_quotes,
        trades: n_trades,
    })
}
