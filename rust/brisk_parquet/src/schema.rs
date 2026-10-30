//! The recording's tables (brisk-nautilus-parquet-v3): their Arrow schemas, names and limits, as
//! `briskapi/_parquet.py` defines them. Both must stay equal; `tests/test_native.py` checks it.
use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Fields, Schema, TimeUnit};

pub const FORMAT: &str = "brisk-nautilus-parquet-v3";
/// Before 1.5.0 the tables of updates held `ts_init`, a copy of `ts_event`, right after it.
pub const LEGACY_FORMAT: &str = "brisk-nautilus-parquet-v2";
pub const VENUE_SUFFIX: &str = ".XTKS";
pub const EQUITY: &str = "data/equity.parquet";
pub const DEPTHS: &str = "data/order_book_depths.parquet";
pub const TRADES: &str = "data/trades.parquet";
pub const BATCHES: &str = "data/custom/BriskBatch.parquet";
pub const FILES: [&str; 4] = [EQUITY, DEPTHS, TRADES, BATCHES];

pub const ROW_GROUP: usize = 65_536;
pub const LEVELS: usize = 10;
/// A book's nonempty levels, at most (`MAX_BOOK_LEVELS`), and how many book changes of an issue make a checkpoint
/// (`CHECKPOINT_CHANGES`; see `checkpoint_changes`).
pub const MAX_BOOK_LEVELS: usize = 65_536;
pub const CHECKPOINT_CHANGES: usize = 1000;
/// The depths table's extra columns when its quotes have book changes.
pub const CHANGES: &str = "changes";
pub const CHECKPOINT: &str = "checkpoint";
/// A quote's key for its book changes, in the decoder's JSON.
pub const BOOK_CHANGES: &str = "book_changes";
pub const F_MBP: u8 = 16;
pub const F_SNAPSHOT: u8 = 32;

/// Rows a broker's recording may hold, per table (`BROKER_MAX_ROWS`), and the bytes its row groups may decode to.
pub fn max_rows(name: &str) -> i64 {
    match name {
        EQUITY => 20_000,
        DEPTHS => 100_000_000,
        TRADES => 50_000_000,
        _ => 10_000_000,
    }
}
pub const MAX_DECODED: i64 = 256 * 1024 * 1024 * 1024;

/// The decoder's quote, in its own order (`QUOTE_ORDER`): the keys of its canonical JSON.
pub const QUOTE_KEYS: [&str; 43] = [
    "issue_id",
    "code",
    "frame",
    "max_frame",
    "source_time_us",
    "last_price10",
    "open_price10",
    "bid_price10",
    "ask_price10",
    "indicative_price10",
    "indicative_volume",
    "indicative_imbalance_side",
    "closing_indicative_price10",
    "closing_indicative_volume",
    "closing_indicative_imbalance_side",
    "quote_flag",
    "quote_side",
    "special_quote_time_us",
    "indicative_open_price10",
    "reference_price10",
    "volume",
    "turnover10",
    "issue_status",
    "market_buy_quantity",
    "market_sell_quantity",
    "closing_market_buy_quantity",
    "closing_market_sell_quantity",
    "bid_quantity",
    "bid_orders",
    "ask_quantity",
    "ask_orders",
    "bid_levels",
    "ask_levels",
    "bid_closing_levels",
    "ask_closing_levels",
    "bid_total_quantity",
    "bid_total_orders",
    "bid_closing_total_quantity",
    "bid_closing_total_orders",
    "ask_total_quantity",
    "ask_total_orders",
    "ask_closing_total_quantity",
    "ask_closing_total_orders",
];
pub const TRADE_KEYS: [&str; 7] = [
    "issue_id", "code", "frame", "time_us", "price10", "quantity", "side",
];

/// Price columns (tenths of a yen in the decoder, `decimal128(38, 16)` here); null means none.
pub const PRICES: [&str; 8] = [
    "last_price",
    "open_price",
    "bid_price",
    "ask_price",
    "indicative_price",
    "closing_indicative_price",
    "indicative_open_price",
    "reference_price",
];
/// The quote's plain counters, `max_frame` first (it is delta encoded).
pub const COUNTS: [&str; 25] = [
    "max_frame",
    "indicative_volume",
    "indicative_imbalance_side",
    "closing_indicative_volume",
    "closing_indicative_imbalance_side",
    "quote_flag",
    "quote_side",
    "volume",
    "issue_status",
    "market_buy_quantity",
    "market_sell_quantity",
    "closing_market_buy_quantity",
    "closing_market_sell_quantity",
    "bid_quantity",
    "bid_orders",
    "ask_quantity",
    "ask_orders",
    "bid_total_quantity",
    "bid_total_orders",
    "bid_closing_total_quantity",
    "bid_closing_total_orders",
    "ask_total_quantity",
    "ask_total_orders",
    "ask_closing_total_quantity",
    "ask_closing_total_orders",
];
/// A quote's four book sides, bids first: the columns, and whether prices fall (a bid side) or rise.
pub const BOOKS: [(&str, bool); 4] = [
    ("bids", true),
    ("asks", false),
    ("closing_bids", true),
    ("closing_asks", false),
];

/// The batch table's columns by kind: what each kind of batch carries beyond its sequence and clock.
pub const BOOTSTRAP_FIELDS: [&str; 6] = [
    "exchange_delay_ms",
    "input_transport",
    "market_issue_count",
    "source",
    "source_timestamp_origin",
    "trading_date",
];
pub const QUOTES_FIELDS: [&str; 3] = ["decode_ns", "received_unix_ms", "replay_lateness_ms"];
pub const END_FIELDS: [&str; 3] = ["frames", "quote_updates", "replay_wall_ms"];
pub const SKIPPED: &str = "skipped_issues";

pub fn timestamp() -> DataType {
    DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
}

pub fn decimal() -> DataType {
    DataType::Decimal128(38, 16)
}

pub fn level_fields() -> Fields {
    Fields::from(vec![
        Field::new("price", decimal(), false),
        Field::new("size", decimal(), false),
        Field::new("count", DataType::UInt32, false),
        Field::new("order_id", DataType::UInt64, false),
    ])
}

pub fn book() -> DataType {
    DataType::List(Arc::new(Field::new(
        "item",
        DataType::Struct(level_fields()),
        false,
    )))
}

fn metadata(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

/// A book change (or a level of a checkpoint): `book` is the index in `BOOKS`.
pub fn change_fields() -> Fields {
    Fields::from(vec![
        Field::new("book", DataType::UInt8, false),
        Field::new("price", decimal(), false),
        Field::new("size", decimal(), false),
        Field::new("count", DataType::UInt32, false),
    ])
}

pub fn changes_type() -> DataType {
    DataType::List(Arc::new(Field::new(
        "item",
        DataType::Struct(change_fields()),
        false,
    )))
}

/// How many book changes of an issue make a checkpoint. A test may ask for fewer, by the environment variable
/// `BRISK_PARQUET_CHECKPOINT_CHANGES` (the checkpoints a recording holds follow it, and so does its verifying).
pub fn checkpoint_changes() -> i64 {
    static COUNT: std::sync::OnceLock<i64> = std::sync::OnceLock::new();
    *COUNT.get_or_init(|| {
        std::env::var("BRISK_PARQUET_CHECKPOINT_CHANGES")
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .filter(|&v| v > 0)
            .unwrap_or(CHECKPOINT_CHANGES as i64)
    })
}

pub fn depth_schema() -> Schema {
    depths(false)
}

/// The depths table's schema; with `changes`, for quotes that have book changes: their own, and a checkpoint (the issue's
/// whole books, null but now and then).
pub fn depths(changes: bool) -> Schema {
    let mut fields = vec![
        Field::new("bids", book(), false),
        Field::new("asks", book(), false),
        Field::new("flags", DataType::UInt8, false),
        Field::new("sequence", DataType::UInt64, false),
        Field::new("ts_event", timestamp(), false),
        Field::new("identifier", DataType::Utf8, false),
        Field::new("issue_id", DataType::UInt64, false),
        Field::new("seq", DataType::UInt64, false),
    ];
    fields.extend(PRICES.iter().map(|name| Field::new(*name, decimal(), true)));
    fields.extend(
        COUNTS
            .iter()
            .map(|name| Field::new(*name, DataType::UInt64, false)),
    );
    fields.push(Field::new("special_quote_time", timestamp(), true));
    fields.push(Field::new("turnover", decimal(), false));
    fields.push(Field::new("closing_bids", book(), false));
    fields.push(Field::new("closing_asks", book(), false));
    if changes {
        fields.push(Field::new(CHANGES, changes_type(), false));
        fields.push(Field::new(CHECKPOINT, changes_type(), true));
    }
    Schema::new_with_metadata(
        fields,
        metadata(&[
            ("type_name", "OrderBookDepth"),
            ("price_precision", "1"),
            ("size_precision", "0"),
            ("brisk_format", FORMAT),
        ]),
    )
}

pub fn trade_schema() -> Schema {
    Schema::new_with_metadata(
        vec![
            Field::new("price", decimal(), false),
            Field::new("size", decimal(), false),
            Field::new(
                "aggressor_side",
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                false,
            ),
            Field::new("trade_id", DataType::Utf8, false),
            Field::new("ts_event", timestamp(), false),
            Field::new("identifier", DataType::Utf8, false),
            Field::new("issue_id", DataType::UInt64, false),
            Field::new("seq", DataType::UInt64, false),
            Field::new("frame", DataType::UInt64, false),
            Field::new("side", DataType::UInt8, false),
        ],
        metadata(&[
            ("type_name", "TradeTick"),
            ("price_precision", "1"),
            ("size_precision", "0"),
            ("brisk_format", FORMAT),
        ]),
    )
}

pub fn equity_schema() -> Schema {
    let mut fields = vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("raw_symbol", DataType::Utf8, false),
        Field::new("currency", DataType::Utf8, false),
        Field::new("price_precision", DataType::UInt8, false),
        Field::new("price_increment", DataType::Utf8, false),
    ];
    for name in [
        "lot_size",
        "isin",
        "max_quantity",
        "min_quantity",
        "max_price",
        "min_price",
    ] {
        fields.push(Field::new(name, DataType::Utf8, true));
    }
    fields.push(Field::new("margin_init", DataType::Utf8, false));
    fields.push(Field::new("margin_maint", DataType::Utf8, false));
    fields.push(Field::new("tick_scheme", DataType::Utf8, true));
    fields.push(Field::new("info", DataType::Utf8, true));
    fields.push(Field::new("ts_event", timestamp(), false));
    fields.push(Field::new("ts_init", timestamp(), false));
    for name in ["issue_id", "tick_type", "issue_type"] {
        fields.push(Field::new(name, DataType::UInt64, false));
    }
    fields.push(Field::new("name", DataType::Utf8, false));
    fields.push(Field::new("base_price", decimal(), true));
    Schema::new_with_metadata(
        fields,
        metadata(&[("type_name", "Equity"), ("brisk_format", FORMAT)]),
    )
}

pub fn batch_schema() -> Schema {
    let skipped = DataType::List(Arc::new(Field::new("item", DataType::UInt64, false)));
    let mut fields = vec![
        Field::new("seq", DataType::UInt64, false),
        Field::new("kind", DataType::Utf8, false),
    ];
    let optional = [
        ("received_unix_ms", DataType::UInt64),
        ("decode_ns", DataType::UInt64),
        ("replay_lateness_ms", DataType::Float64),
        (SKIPPED, skipped),
        ("frames", DataType::UInt64),
        ("quote_updates", DataType::UInt64),
        ("replay_wall_ms", DataType::Float64),
        ("source", DataType::Utf8),
        ("trading_date", DataType::Utf8),
        ("input_transport", DataType::Utf8),
        ("source_timestamp_origin", DataType::Utf8),
        ("exchange_delay_ms", DataType::Float64),
        ("market_issue_count", DataType::UInt64),
    ];
    fields.extend(
        optional
            .into_iter()
            .map(|(name, kind)| Field::new(name, kind, true)),
    );
    fields.push(Field::new("type", DataType::Utf8, false));
    fields.push(Field::new("ts_event", timestamp(), false));
    Schema::new_with_metadata(
        fields,
        metadata(&[("type_name", "BriskBatch"), ("brisk_format", FORMAT)]),
    )
}

/// The schema of a table in the current format, or (`legacy`) in v2's.
pub fn expected(name: &str, legacy: bool) -> Schema {
    let schema = match name {
        EQUITY => equity_schema(),
        DEPTHS => depth_schema(),
        TRADES => trade_schema(),
        _ => batch_schema(),
    };
    if !legacy {
        return schema;
    }
    let mut fields: Vec<Arc<Field>> = schema.fields().iter().cloned().collect();
    if schema.field_with_name("ts_init").is_err() {
        let at = fields
            .iter()
            .position(|f| f.name() == "ts_event")
            .map_or(fields.len(), |i| i + 1);
        fields.insert(at, Arc::new(Field::new("ts_init", timestamp(), false)));
    }
    let mut metadata = schema.metadata().clone();
    metadata.insert("brisk_format".into(), LEGACY_FORMAT.into());
    Schema::new_with_metadata(fields, metadata)
}

/// The columns of `name` written delta encoded: within an issue (rows are sorted by issue, then batch) clocks and
/// frame and batch numbers only rise.
pub fn deltas(name: &str) -> &'static [&'static str] {
    match name {
        DEPTHS => &["ts_event", "sequence", "seq", "max_frame"],
        TRADES => &["ts_event", "seq", "frame"],
        BATCHES => &["ts_event", "seq"],
        _ => &[],
    }
}

/// Length of an object's canonical JSON with these keys, their values aside: braces, two quotes and a colon
/// per key, and the commas between.
pub fn object_length(keys: &[&str]) -> u64 {
    2 + keys.iter().map(|k| k.len() as u64 + 3).sum::<u64>() + keys.len() as u64 - 1
}
