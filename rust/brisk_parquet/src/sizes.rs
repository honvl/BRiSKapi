//! What a recording's content comes to as the archive's canonical JSON (the summary's `expanded_bytes`): the decoder's
//! batches, quotes and trades, one object each, sorted keys and no spaces. Counted from the tables' columns, never written.
use arrow_array::cast::AsArray;
use arrow_array::types::{
    Decimal128Type, Float64Type, TimestampNanosecondType, UInt32Type, UInt64Type,
};
use arrow_array::{Array, RecordBatch};
use serde_json::{Map, Value, json};

use crate::batches::{BOOTSTRAP, END, HISTORY, QUOTES};
use crate::master::Master;
use crate::schema::{self, BOOKS, COUNTS, PRICES, SKIPPED, TRADE_KEYS};
use crate::units::{digits, tenths, whole};

/// A quote's bytes in a batch's list but for its numbers, and a trade's.
pub fn quote_fixed() -> u64 {
    schema::object_length(&schema::QUOTE_KEYS)
}

pub fn trade_fixed() -> u64 {
    schema::object_length(&TRADE_KEYS)
}

fn u64s<'a>(batch: &'a RecordBatch, name: &str) -> &'a [u64] {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_primitive_opt::<UInt64Type>())
        .map_or(&[], |a| a.values())
}

fn times<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Option<&'a arrow_array::TimestampNanosecondArray> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_primitive_opt::<TimestampNanosecondType>())
}

/// The canonical JSON bytes of the quotes in a depths batch (`midnight`: the day's start, for their clocks).
pub fn quote_bytes(batch: &RecordBatch, midnight: i64) -> u64 {
    let n = batch.num_rows();
    let mut bytes = quote_fixed() * n as u64;
    for name in ["issue_id", "sequence"].into_iter().chain(COUNTS) {
        bytes += u64s(batch, name).iter().map(|&v| digits(v)).sum::<u64>();
    }
    if let Some(ts) = times(batch, "ts_event") {
        bytes += ts
            .values()
            .iter()
            .map(|&t| digits(((t - midnight) / 1000) as u64))
            .sum::<u64>();
    }
    if let Some(special) = times(batch, "special_quote_time") {
        for i in 0..n {
            bytes += if special.is_valid(i) {
                digits(((special.value(i) - midnight) / 1000) as u64)
            } else {
                1
            };
        }
    }
    for name in PRICES {
        if let Some(prices) = batch
            .column_by_name(name)
            .and_then(|c| c.as_primitive_opt::<Decimal128Type>())
        {
            for i in 0..n {
                bytes += if prices.is_valid(i) {
                    digits(tenths(prices.value(i)).unwrap_or(0))
                } else {
                    1
                };
            }
        }
    }
    if let Some(turnover) = batch
        .column_by_name("turnover")
        .and_then(|c| c.as_primitive_opt::<Decimal128Type>())
    {
        bytes += turnover
            .values()
            .iter()
            .map(|&v| digits(tenths(v).unwrap_or(0)))
            .sum::<u64>();
    }
    if let Some(identifiers) = batch
        .column_by_name("identifier")
        .and_then(|c| c.as_string_opt::<i32>())
    {
        // The code and its two quotes: the identifier without ".XTKS".
        bytes += (0..n)
            .map(|i| identifiers.value(i).len() as u64 - schema::VENUE_SUFFIX.len() as u64 + 2)
            .sum::<u64>();
    }
    for (name, _) in BOOKS {
        let Some(list) = batch
            .column_by_name(name)
            .and_then(|c| c.as_list_opt::<i32>())
        else {
            continue;
        };
        let levels = list.values().as_struct();
        let (price, size, count) = (
            levels.column(0).as_primitive::<Decimal128Type>().values(),
            levels.column(1).as_primitive::<Decimal128Type>().values(),
            levels.column(2).as_primitive::<UInt32Type>().values(),
        );
        for window in list.value_offsets().windows(2) {
            let (lo, hi) = (window[0] as usize, window[1] as usize);
            bytes += 2 + hi.saturating_sub(lo + 1) as u64;
            for j in lo..hi {
                bytes += digits(tenths(price[j]).unwrap_or(0))
                    + digits(whole(size[j]).unwrap_or(0))
                    + digits(count[j] as u64)
                    + 4;
            }
        }
    }
    if let Some(list) = batch
        .column_by_name(schema::CHANGES)
        .and_then(|c| c.as_list_opt::<i32>())
    {
        bytes += changes_bytes(list);
    }
    bytes
}

/// The canonical JSON bytes a depths batch's quotes take for their book changes: the key and its comma, the list of
/// [book, price10, quantity, orders], and the commas between its elements.
pub fn changes_bytes(list: &arrow_array::ListArray) -> u64 {
    let levels = list.values().as_struct();
    let (book, price, size, count) = (
        levels
            .column(0)
            .as_primitive::<arrow_array::types::UInt8Type>()
            .values(),
        levels.column(1).as_primitive::<Decimal128Type>().values(),
        levels.column(2).as_primitive::<Decimal128Type>().values(),
        levels.column(3).as_primitive::<UInt32Type>().values(),
    );
    let mut bytes = (schema::BOOK_CHANGES.len() as u64 + 4) * list.len() as u64;
    for window in list.value_offsets().windows(2) {
        let (lo, hi) = (window[0] as usize, window[1] as usize);
        bytes += 2 + hi.saturating_sub(lo + 1) as u64;
        for j in lo..hi {
            bytes += digits(book[j] as u64)
                + digits(tenths(price[j]).unwrap_or(0))
                + digits(whole(size[j]).unwrap_or(0))
                + digits(count[j] as u64)
                + 5;
        }
    }
    bytes
}

/// The canonical JSON bytes of the trades in a trades batch.
pub fn trade_bytes(batch: &RecordBatch, midnight: i64) -> u64 {
    let n = batch.num_rows();
    let mut bytes = trade_fixed() * n as u64;
    for name in ["issue_id", "frame"] {
        bytes += u64s(batch, name).iter().map(|&v| digits(v)).sum::<u64>();
    }
    if let Some(ts) = times(batch, "ts_event") {
        bytes += ts
            .values()
            .iter()
            .map(|&t| digits(((t - midnight) / 1000) as u64))
            .sum::<u64>();
    }
    if let (Some(price), Some(size)) = (
        batch
            .column_by_name("price")
            .and_then(|c| c.as_primitive_opt::<Decimal128Type>()),
        batch
            .column_by_name("size")
            .and_then(|c| c.as_primitive_opt::<Decimal128Type>()),
    ) {
        for i in 0..n {
            bytes += digits(tenths(price.value(i)).unwrap_or(0))
                + digits(whole(size.value(i)).unwrap_or(0));
        }
    }
    if let Some(side) = batch
        .column_by_name("side")
        .and_then(|c| c.as_primitive_opt::<arrow_array::types::UInt8Type>())
    {
        bytes += side.values().iter().map(|&v| digits(v as u64)).sum::<u64>();
    }
    if let Some(identifiers) = batch
        .column_by_name("identifier")
        .and_then(|c| c.as_string_opt::<i32>())
    {
        bytes += (0..n)
            .map(|i| identifiers.value(i).len() as u64 - schema::VENUE_SUFFIX.len() as u64 + 2)
            .sum::<u64>();
    }
    bytes
}

/// The canonical line of batch row `i` (kind `code`, batch number `seq`, clock `now` in microseconds) without its quotes' and
/// trades' own objects (`batch_lengths`). Its shape is fixed by its kind once the other tables are checked: a bootstrap or
/// quotes batch has both lists, history has trades, an end neither. The lists' elements and commas are added when the rows
/// are counted. `transport` is the bootstrap's provenance as an object, `master` its master.
pub fn line_length(
    batch: &RecordBatch,
    i: usize,
    code: u8,
    seq: u64,
    now: i64,
    transport: &Value,
    master: &[Master],
) -> u32 {
    let mut o = Map::new();
    o.insert("seq".into(), json!(seq));
    o.insert("source_time_us".into(), json!(now));
    let get_u64 = |name: &str| -> Value {
        match batch
            .column_by_name(name)
            .and_then(|c| c.as_primitive_opt::<UInt64Type>())
        {
            Some(a) if !a.is_null(i) => json!(a.value(i)),
            _ => Value::Null,
        }
    };
    let get_f64 = |name: &str| -> Value {
        match batch
            .column_by_name(name)
            .and_then(|c| c.as_primitive_opt::<Float64Type>())
        {
            Some(a) if !a.is_null(i) => {
                serde_json::Number::from_f64(a.value(i)).map_or(Value::Null, Value::Number)
            }
            _ => Value::Null,
        }
    };
    let get_str = |name: &str| -> Value {
        match batch
            .column_by_name(name)
            .and_then(|c| c.as_string_opt::<i32>())
        {
            Some(a) if !a.is_null(i) => json!(a.value(i)),
            _ => Value::Null,
        }
    };
    let (kind, lists) = match code {
        BOOTSTRAP => {
            o.insert("exchange_delay_ms".into(), get_f64("exchange_delay_ms"));
            o.insert("input_transport".into(), transport.clone());
            o.insert("market_issue_count".into(), get_u64("market_issue_count"));
            o.insert("source".into(), get_str("source"));
            o.insert(
                "source_timestamp_origin".into(),
                get_str("source_timestamp_origin"),
            );
            o.insert("trading_date".into(), get_str("trading_date"));
            o.insert(
                "master".into(),
                Value::Array(master.iter().map(Master::json).collect()),
            );
            ("bootstrap", 3)
        }
        QUOTES => {
            for name in ["decode_ns", "received_unix_ms"] {
                o.insert(name.into(), get_u64(name));
            }
            o.insert("replay_lateness_ms".into(), get_f64("replay_lateness_ms"));
            ("quotes", 3)
        }
        HISTORY => ("history", 2),
        END => {
            o.insert("frames".into(), get_u64("frames"));
            o.insert("quote_updates".into(), get_u64("quote_updates"));
            o.insert("replay_wall_ms".into(), get_f64("replay_wall_ms"));
            ("end", 0)
        }
        _ => ("end", 0),
    };
    o.insert("type".into(), json!(kind));
    if lists & 1 != 0 {
        o.insert("quotes".into(), Value::Array(Vec::new()));
    }
    if lists & 2 != 0 {
        o.insert("trades".into(), Value::Array(Vec::new()));
    }
    if let Some(list) = batch
        .column_by_name(SKIPPED)
        .and_then(|c| c.as_list_opt::<i32>())
        .filter(|l| !l.is_null(i))
    {
        let values: Vec<u64> = list
            .value(i)
            .as_primitive_opt::<UInt64Type>()
            .map(|p| p.values().to_vec())
            .unwrap_or_default();
        o.insert(SKIPPED.into(), json!(values));
    }
    serde_json::to_vec(&Value::Object(o))
        .map(|v| v.len() + 1)
        .unwrap_or(0) as u32
}
