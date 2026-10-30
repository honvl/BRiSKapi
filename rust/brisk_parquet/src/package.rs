//! Spooled parts, packaged: consecutive parts of a session (and the book where a previous piece left it) written as one
//! recording, a range of issues at a time, so that no day is ever whole in memory. It is `write_piece` of
//! `briskapi/_parquet.py`, and writes the same tables: tests/test_native.py compares them.
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::builder::StringBuilder;
use arrow_array::cast::AsArray;
use arrow_array::types::{TimestampNanosecondType, UInt64Type};
use arrow_array::{
    Array, ArrayRef, ListArray, RecordBatch, StringArray, TimestampNanosecondArray, UInt8Array,
    UInt64Array, new_null_array,
};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{DataType, Field};
use rayon::prelude::*;
use serde_json::{Value, json};

use crate::batches::{BOOTSTRAP, END, HISTORY, QUOTES};
use crate::error::{Failure, Res};
use crate::files;
use crate::master;
use crate::parts::PartTable;
use crate::replay::{IssueState, Lists, Rows};
use crate::require;
use crate::schema::{
    self, BATCHES, CHANGES, CHECKPOINT, DEPTHS, EQUITY, F_MBP, F_SNAPSHOT, SKIPPED, TRADES,
};
use crate::sink::{Sink, Stage};
use crate::sizes::{line_length, quote_bytes, trade_bytes};
use crate::writer;

/// About how many rows a range of issues holds, 1.2 GB decoded.
const PIECE_ROWS: u64 = 1_000_000;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum End {
    /// The parts' own end.
    Parts,
    /// A piece cut at a lunch break or a size: it gets an end.
    Cut,
    /// A session cut short gets the end its host never wrote, if it has none.
    Repair,
}

pub struct Cut {
    /// Each issue's last quote before the piece, as a depths table.
    pub state: PathBuf,
    pub clock: i64,
}

pub struct Config {
    /// The segment's folder: `data/equity.parquet` is its master.
    pub spool: PathBuf,
    pub parts: Vec<PathBuf>,
    pub out: Option<PathBuf>,
    pub midnight: i64,
    /// The first part of the day: its first batch is the bootstrap a piece after a cut starts like.
    pub day_start: Option<PathBuf>,
    pub cut: Option<Cut>,
    pub end: End,
    /// Where each issue's last quote after the piece goes.
    pub state_out: Option<PathBuf>,
    pub threads: usize,
    /// About how many quote rows a range of issues holds in memory (PIECE_ROWS by default).
    pub range_rows: Option<u64>,
}

pub struct Outcome {
    pub summary: Option<Value>,
    pub clock: i64,
    pub repaired: bool,
}

fn fault<E: Into<anyhow::Error>>(error: E) -> Failure {
    Failure::Fault(error.into())
}

fn code_of(kind: &str) -> u8 {
    match kind {
        "bootstrap" => BOOTSTRAP,
        "quotes" => QUOTES,
        "history" => HISTORY,
        _ => END,
    }
}

fn u64s<'a>(batch: &'a RecordBatch, name: &str) -> Res<&'a [u64]> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_primitive_opt::<UInt64Type>())
        .map(|a| a.values().as_ref())
        .ok_or_else(|| Failure::Refused(format!("Unexpected schema: {name}")))
}

fn replace(batch: &RecordBatch, name: &str, array: ArrayRef) -> Res<RecordBatch> {
    let schema = batch.schema();
    let index = schema.index_of(name).map_err(Failure::from)?;
    let mut columns = batch.columns().to_vec();
    columns[index] = array;
    RecordBatch::try_new(schema, columns).map_err(Failure::from)
}

/// What the plan needs of the parts' batch tables.
struct Scan {
    kinds: Vec<u8>,
    first_seq: u64,
    first_kind: u8,
    first_clock: i64,
    received: (Option<u64>, Option<u64>),
    last_kind: u8,
    last_seq: u64,
    last_clock: i64,
}

fn scan_batches(tables: &[PartTable], midnight: i64) -> Res<Scan> {
    let mut scan = Scan {
        kinds: Vec::new(),
        first_seq: 0,
        first_kind: END,
        first_clock: 0,
        received: (None, None),
        last_kind: END,
        last_seq: 0,
        last_clock: 0,
    };
    for table in tables {
        for batch in table.read_all()? {
            let kinds = batch
                .column_by_name("kind")
                .and_then(|c| c.as_string_opt::<i32>())
                .ok_or_else(|| Failure::Refused("Unexpected schema: kind".into()))?;
            let seqs = u64s(&batch, "seq")?;
            let events = batch
                .column_by_name("ts_event")
                .and_then(|c| c.as_primitive_opt::<TimestampNanosecondType>())
                .ok_or_else(|| Failure::Refused("Unexpected schema: ts_event".into()))?;
            let received = batch
                .column_by_name("received_unix_ms")
                .and_then(|c| c.as_primitive_opt::<UInt64Type>())
                .ok_or_else(|| Failure::Refused("Unexpected schema: received_unix_ms".into()))?;
            for i in 0..batch.num_rows() {
                let code = code_of(kinds.value(i));
                if scan.kinds.is_empty() {
                    scan.first_seq = seqs[i];
                    scan.first_kind = code;
                    scan.first_clock = (events.value(i) - midnight) / 1000;
                }
                if code == QUOTES && received.is_valid(i) {
                    scan.received.0 = scan.received.0.or(Some(received.value(i)));
                    scan.received.1 = Some(received.value(i));
                }
                scan.kinds.push(code);
                scan.last_kind = code;
                scan.last_seq = seqs[i];
                scan.last_clock = (events.value(i) - midnight) / 1000;
            }
        }
    }
    require!(!scan.kinds.is_empty(), "Recording has no bootstrap");
    Ok(scan)
}

/// A batch row: the table's columns, null but where given.
fn batch_row(
    seq: u64,
    kind: &str,
    clock_us: i64,
    midnight: i64,
    fields: &[(&str, ArrayRef)],
) -> Res<RecordBatch> {
    let schema = Arc::new(schema::batch_schema());
    let mut columns: Vec<ArrayRef> = Vec::new();
    for field in schema.fields() {
        let given = fields
            .iter()
            .find(|(name, _)| name == field.name())
            .map(|(_, a)| a.clone());
        columns.push(match field.name().as_str() {
            "seq" => Arc::new(UInt64Array::from(vec![seq])),
            "kind" => Arc::new(StringArray::from(vec![kind])),
            "type" => Arc::new(StringArray::from(vec!["BriskBatch"])),
            "ts_event" => Arc::new(
                TimestampNanosecondArray::from(vec![midnight + clock_us * 1000])
                    .with_timezone("UTC"),
            ),
            _ => given.unwrap_or_else(|| new_null_array(field.data_type(), 1)),
        });
    }
    RecordBatch::try_new(schema, columns).map_err(Failure::from)
}

fn skipped_array(issues: &[u64]) -> ArrayRef {
    if issues.is_empty() {
        return new_null_array(
            &DataType::List(Arc::new(Field::new("item", DataType::UInt64, false))),
            1,
        );
    }
    let field = Arc::new(Field::new("item", DataType::UInt64, false));
    Arc::new(ListArray::new(
        field,
        OffsetBuffer::from_lengths([issues.len()]),
        Arc::new(UInt64Array::from(issues.to_vec())),
        None,
    ))
}

/// Rows of one table, in a range of issues, as pieces of read batches: an issue's rows from one batch.
#[derive(Clone, Copy)]
struct Run {
    issue: u64,
    /// 0 for the book before the piece, then the part.
    rank: usize,
    batch: usize,
    start: usize,
    len: usize,
}

fn runs(
    batch_index: usize,
    batch: &RecordBatch,
    low: u64,
    high: u64,
    rank: usize,
) -> Res<Vec<Run>> {
    let issues = u64s(batch, "issue_id")?;
    let from = issues.partition_point(|&v| v < low);
    let to = issues.partition_point(|&v| v < high);
    let mut out = Vec::new();
    let mut i = from;
    while i < to {
        let mut j = i + 1;
        while j < to && issues[j] == issues[i] {
            j += 1;
        }
        out.push(Run {
            issue: issues[i],
            rank,
            batch: batch_index,
            start: i,
            len: j - i,
        });
        i = j;
    }
    Ok(out)
}

/// The issue ID ranges [low, high) of about PIECE_ROWS rows each, from the rows of each issue (an issue is never split).
fn ranges(counts: &BTreeMap<u64, u64>, piece_rows: u64) -> Vec<(u64, u64)> {
    let (mut out, mut low, mut rows) = (Vec::new(), 0u64, 0u64);
    for (&issue, &count) in counts {
        rows += count;
        if rows >= piece_rows {
            out.push((low, issue + 1));
            low = issue + 1;
            rows = 0;
        }
    }
    out.push((low, u64::MAX));
    out
}

/// A batch's rows of issues in [low, high), copied if it has others (so that it does not hold their memory too).
fn trim(batch: RecordBatch, low: u64, high: u64) -> Res<Option<RecordBatch>> {
    let issues = u64s(&batch, "issue_id")?;
    let (from, to) = (
        issues.partition_point(|&v| v < low),
        issues.partition_point(|&v| v < high),
    );
    if from == to {
        return Ok(None);
    }
    if from == 0 && to == issues.len() {
        return Ok(Some(batch));
    }
    let indices = arrow_array::UInt32Array::from_iter_values(from as u32..to as u32);
    arrow_select::take::take_record_batch(&batch, &indices)
        .map(Some)
        .map_err(Failure::from)
}

/// `seq` less `shift`, and the opening snapshot's flag where the batch is 0.
fn renumber(batch: &RecordBatch, shift: u64) -> Res<RecordBatch> {
    let seqs: Vec<u64> = u64s(batch, "seq")?.iter().map(|&s| s - shift).collect();
    let flags: Vec<u8> = seqs
        .iter()
        .map(|&s| if s == 0 { F_MBP | F_SNAPSHOT } else { F_MBP })
        .collect();
    let out = replace(batch, "seq", Arc::new(UInt64Array::from(seqs)))?;
    replace(&out, "flags", Arc::new(UInt8Array::from(flags)))
}

/// Trade IDs: the issue's frame number and the trade's place among that frame's trades, in this order of rows.
fn trade_ids(batch: &RecordBatch) -> Res<ArrayRef> {
    let (issues, frames) = (u64s(batch, "issue_id")?, u64s(batch, "frame")?);
    let mut builder = StringBuilder::with_capacity(batch.num_rows(), batch.num_rows() * 10);
    let (mut key, mut place) = (None, 0u64);
    for i in 0..batch.num_rows() {
        if key == Some((issues[i], frames[i])) {
            place += 1;
        } else {
            place = 0;
        }
        key = Some((issues[i], frames[i]));
        builder.append_value(format!("{}-{}", frames[i], place));
    }
    Ok(Arc::new(builder.finish()))
}

/// One issue's rows of a range, walked in order: the rows with their checkpoints (if `write`), and the row the next piece
/// starts from, the last with its issue's whole books as its changes.
struct Walked {
    rows: Vec<RecordBatch>,
    last: RecordBatch,
}

fn walk_issue(sources: &[(usize, RecordBatch)], runs: &[Run], write: bool) -> Res<Walked> {
    let mut state = IssueState::default();
    let mut rows_out = Vec::with_capacity(runs.len());
    let mut last = None;
    for run in runs {
        let rows = sources[run.batch].1.slice(run.start, run.len);
        let mut points = Lists::new();
        {
            let view = Rows::new(&rows)?;
            for i in 0..rows.num_rows() {
                let point = state.apply(&view, i)?;
                if write {
                    if point {
                        points.books(&state.books);
                    } else {
                        points.null();
                    }
                }
            }
        }
        rows_out.push(if write {
            replace(&rows, CHECKPOINT, points.finish()?)?
        } else {
            rows.clone()
        });
        last = Some(rows);
    }
    let last = last.expect("an issue has rows");
    let last = last.slice(last.num_rows() - 1, 1);
    let mut whole = Lists::new();
    whole.books(&state.books);
    let last = replace(&last, CHANGES, whole.finish()?)?;
    let last = replace(
        &last,
        CHECKPOINT,
        new_null_array(&schema::changes_type(), 1),
    )?;
    Ok(Walked {
        rows: rows_out,
        last,
    })
}

fn trace(label: &str, since: &std::time::Instant) {
    if std::env::var_os("BRISK_PARQUET_TRACE").is_some() {
        let rss = std::process::Command::new("ps")
            .args(["-o", "rss=", "-p", &std::process::id().to_string()])
            .output()
            .ok()
            .and_then(|o| {
                String::from_utf8_lossy(&o.stdout)
                    .trim()
                    .parse::<u64>()
                    .ok()
            })
            .unwrap_or(0)
            / 1024;
        eprintln!(
            "trace: {label} {:.1}s rss {rss} MB",
            since.elapsed().as_secs_f64()
        );
    }
}

/// The least time between two row counts of a table.
const PROGRESS_EVERY: std::time::Duration = std::time::Duration::from_millis(500);

/// What a caller is told while a recording is written, as `package: STAGE` (preparing, finalizing) and `package: TABLE
/// DONE/TOTAL` for the rows of a table (batches, quotes, trades). A table's count comes at most every PROGRESS_EVERY, and
/// its last always, once. Only a package that writes tables says anything: the book alone (`state`) is not a recording.
struct Pace<'a> {
    tell: Option<&'a (dyn Fn(&str) + Sync)>,
    /// When each table's count was last told, and what it was.
    last: HashMap<&'static str, (std::time::Instant, u64)>,
}

impl Pace<'_> {
    fn stage(&self, name: &str) {
        if let Some(tell) = self.tell {
            tell(&format!("package: {name}"));
        }
    }

    fn rows(&mut self, table: &'static str, done: u64, total: u64) {
        let Some(tell) = self.tell else { return };
        let told = self.last.get(table);
        let due = told.is_none_or(|(at, _)| at.elapsed() >= PROGRESS_EVERY) || done >= total;
        if total > 0 && due && told.is_none_or(|(_, was)| *was != done) {
            self.last.insert(table, (std::time::Instant::now(), done));
            tell(&format!("package: {table} {done}/{total}"));
        }
    }
}

pub fn package(config: &Config) -> Res<Outcome> {
    package_with(config, &|_| {})
}

/// `package`, telling `progress` how it goes (see Pace).
pub fn package_with(config: &Config, progress: &(dyn Fn(&str) + Sync)) -> Res<Outcome> {
    let mut pace = Pace {
        tell: config.out.as_ref().map(|_| progress),
        last: HashMap::new(),
    };
    pace.stage("preparing");
    let started = std::time::Instant::now();
    let pool = Arc::new(
        rayon::ThreadPoolBuilder::new()
            .num_threads(config.threads.max(1))
            .build()
            .map_err(fault)?,
    );
    let midnight = config.midnight;
    require!(!config.parts.is_empty(), "Recording has no bootstrap");

    let equity = files::open(&config.spool, EQUITY, schema::ROW_GROUP, false)?;
    let (_, master) = master::read(&config.spool, &equity)?;
    let batch_tables: Vec<PartTable> = config
        .parts
        .iter()
        .map(|p| PartTable::open(p, BATCHES))
        .collect::<Res<_>>()?;
    let depth_tables: Vec<PartTable> = config
        .parts
        .iter()
        .map(|p| PartTable::open(p, DEPTHS))
        .collect::<Res<_>>()?;
    let trade_tables: Vec<PartTable> = config
        .parts
        .iter()
        .map(|p| PartTable::open(p, TRADES))
        .collect::<Res<_>>()?;

    // Whether the quotes have book changes: all the parts' do, or none (a part with no quotes does not say).
    let mut changes = None::<bool>;
    let mut agree = |found: bool| -> Res<()> {
        require!(
            changes.is_none_or(|before| before == found),
            "Quotes with and without book changes"
        );
        changes = Some(found);
        Ok(())
    };
    for table in depth_tables.iter().filter(|t| t.rows() > 0) {
        agree(table.changes)?;
    }

    // The plan: the batches' kinds and how many quotes and trades each holds, from the small tables; where the piece starts and
    // ends, and the end it needs.
    trace("opened", &started);
    let scan = scan_batches(&batch_tables, midnight)?;
    trace("scanned", &started);
    let n = scan.kinds.len();
    let tally = |tables: &[PartTable]| -> Res<Vec<u64>> {
        let per_part: Vec<Res<(u64, Vec<u32>)>> = pool.install(|| {
            tables
                .par_iter()
                .map(|table| {
                    let seqs = table.seqs()?;
                    if seqs.is_empty() {
                        return Ok((scan.first_seq, Vec::new()));
                    }
                    let (low, high) = (
                        seqs.iter().copied().min().unwrap_or(0),
                        seqs.iter().copied().max().unwrap_or(0),
                    );
                    require!(
                        low >= scan.first_seq && high < scan.first_seq + n as u64,
                        "Quotes or trades outside their batches"
                    );
                    let mut counts = vec![0u32; (high - low) as usize + 1];
                    for s in seqs {
                        counts[(s - low) as usize] += 1;
                    }
                    Ok((low, counts))
                })
                .collect()
        });
        let mut total = vec![0u64; n];
        for part in per_part {
            let (low, counts) = part?;
            for (offset, count) in counts.iter().enumerate() {
                total[(low - scan.first_seq) as usize + offset] += *count as u64;
            }
        }
        Ok(total)
    };
    let (mut quotes, mut trades) = (tally(&depth_tables)?, tally(&trade_tables)?);
    trace("tallied", &started);

    let template_table;
    let template_tables = match &config.day_start {
        Some(part) if config.cut.is_some() => {
            template_table = PartTable::open(part, BATCHES)?;
            &template_table
        }
        _ => &batch_tables[0],
    };
    let template = template_tables
        .read(&[0])?
        .into_iter()
        .next()
        .ok_or_else(|| Failure::Refused("Recording has no bootstrap".into()))?;
    let template = template.slice(0, 1);
    let template_kind = template
        .column_by_name("kind")
        .and_then(|c| c.as_string_opt::<i32>())
        .map(|a| a.value(0).to_string())
        .unwrap_or_default();
    require!(template_kind == "bootstrap", "Recording has no bootstrap");
    let text = |name: &str| {
        template
            .column_by_name(name)
            .and_then(|c| c.as_string_opt::<i32>())
            .filter(|a| !a.is_null(0))
            .map(|a| a.value(0).to_string())
    };
    let (source, trading_date) = (
        text("source").unwrap_or_default(),
        text("trading_date").unwrap_or_default(),
    );
    let transport: Value = text("input_transport")
        .and_then(|t| serde_json::from_str(&t).ok())
        .unwrap_or(Value::Null);

    let mut kinds = scan.kinds.clone();
    let (mut shift, mut head) = (0u64, None::<RecordBatch>);
    let state_batch: Option<RecordBatch> = match &config.cut {
        Some(cut) => {
            let table = PartTable::open_file(&cut.state, DEPTHS)?;
            let batches = table.read_all()?;
            if batches.is_empty() {
                None
            } else {
                agree(table.changes)?;
                Some(
                    arrow_select::concat::concat_batches(&batches[0].schema(), &batches)
                        .map_err(Failure::from)?,
                )
            }
        }
        None => None,
    };
    if let Some(cut) = &config.cut {
        shift = scan.first_seq - 1;
        let state_ids: Vec<u64> = state_batch
            .as_ref()
            .map(|b| u64s(b, "issue_id").map(|s| s.to_vec()))
            .transpose()?
            .unwrap_or_default();
        let skipped: Vec<u64> = master
            .iter()
            .map(|m| m.issue_id)
            .filter(|id| state_ids.binary_search(id).is_err())
            .collect();
        let row = replace(&template, "seq", Arc::new(UInt64Array::from(vec![0u64])))?;
        let row = replace(
            &row,
            "ts_event",
            Arc::new(
                TimestampNanosecondArray::from(vec![midnight + cut.clock * 1000])
                    .with_timezone("UTC"),
            ),
        )?;
        head = Some(replace(&row, SKIPPED, skipped_array(&skipped))?);
        kinds.insert(0, BOOTSTRAP);
        quotes.insert(0, state_ids.len() as u64);
        trades.insert(0, 0);
    } else {
        require!(scan.first_kind == BOOTSTRAP, "Recording has no bootstrap");
    }
    let updates: u64 = kinds
        .iter()
        .zip(&quotes)
        .filter(|(k, _)| **k == QUOTES)
        .map(|(_, q)| *q)
        .sum();
    let last_seq = scan.last_seq - shift;
    let (mut tail, mut recount, mut repaired) = (None::<RecordBatch>, false, false);
    if config.end == End::Cut || (config.end == End::Repair && scan.last_kind != END) {
        let wall = match scan.received {
            (Some(first), Some(last)) => last as f64 - first as f64,
            _ => 0.0,
        };
        tail = Some(batch_row(
            last_seq + 1,
            "end",
            scan.last_clock,
            midnight,
            &[
                ("frames", Arc::new(UInt64Array::from(vec![last_seq]))),
                ("quote_updates", Arc::new(UInt64Array::from(vec![updates]))),
                (
                    "replay_wall_ms",
                    Arc::new(arrow_array::Float64Array::from(vec![wall])),
                ),
            ],
        )?);
        quotes.push(0);
        trades.push(0);
        kinds.push(END);
        repaired = config.end == End::Repair;
    } else if config.cut.is_some() && scan.last_kind == END {
        recount = true;
    }
    let final_kind = *kinds.last().unwrap_or(&BOOTSTRAP);
    // The book alone (no tables) is wanted of parts that are not over.
    require!(
        config.out.is_none() || final_kind == END,
        "Recording has no clean end"
    );
    let total = kinds.len();
    let from_parts = total - head.is_some() as usize - tail.is_some() as usize;
    let first_clock = match &config.cut {
        Some(cut) => cut.clock,
        None => scan.first_clock,
    };

    let state_rows_for = |low: u64, high: u64| -> Res<Option<RecordBatch>> {
        let Some(batch) = &state_batch else {
            return Ok(None);
        };
        let issues = u64s(batch, "issue_id")?;
        let (from, to) = (
            issues.partition_point(|&v| v < low),
            issues.partition_point(|&v| v < high),
        );
        if from == to {
            return Ok(None);
        }
        let rows = batch.slice(from, to - from);
        let zeros = vec![0u64; rows.num_rows()];
        let rows = replace(&rows, "seq", Arc::new(UInt64Array::from(zeros)))?;
        Ok(Some(replace(
            &rows,
            "flags",
            Arc::new(UInt8Array::from(vec![F_MBP | F_SNAPSHOT; rows.num_rows()])),
        )?))
    };

    let changes = changes.unwrap_or(false);
    let (mut depths_sink, mut trades_sink, mut batches_sink) = (None, None, None);
    if let Some(out) = &config.out {
        std::fs::create_dir_all(out)?;
        let mut equity_writer = writer::create(out, EQUITY, schema::ROW_GROUP)?;
        equity_writer
            .write(&master::table(&master, midnight)?)
            .map_err(fault)?;
        equity_writer.close().map_err(fault)?;
        batches_sink = Some(Sink::create(
            out,
            BATCHES,
            schema::expected(BATCHES, false),
            schema::ROW_GROUP,
            pool.clone(),
            Stage::plain(),
        )?);
        let measure_midnight = midnight;
        depths_sink = Some(Sink::create(
            out,
            DEPTHS,
            schema::depths(changes),
            schema::ROW_GROUP,
            pool.clone(),
            Stage {
                transform: Box::new(Ok),
                measure: Box::new(move |b| quote_bytes(b, measure_midnight)),
            },
        )?);
        trades_sink = Some(Sink::create(
            out,
            TRADES,
            schema::expected(TRADES, false),
            schema::ROW_GROUP,
            pool.clone(),
            Stage {
                transform: Box::new(Ok),
                measure: Box::new(move |b| trade_bytes(b, measure_midnight)),
            },
        )?);
    }

    // The batches, a part at a time: renumbered after a cut, with the head and the end a piece needs.
    let mut line_bytes = 0u64;
    if let Some(sink) = batches_sink.as_mut() {
        let extras = |at: usize| -> u64 {
            let mut bytes = 0;
            if kinds[at] <= QUOTES {
                bytes += quotes[at].saturating_sub(1);
            }
            if kinds[at] != END {
                bytes += trades[at].saturating_sub(1);
            }
            bytes
        };
        let measure = |batch: &RecordBatch, at: usize| -> u64 {
            let rows: Vec<usize> = (0..batch.num_rows()).collect();
            let seqs: Vec<u64> = u64s(batch, "seq").map(|s| s.to_vec()).unwrap_or_default();
            let events = batch
                .column_by_name("ts_event")
                .and_then(|c| c.as_primitive_opt::<TimestampNanosecondType>());
            pool.install(|| {
                rows.par_iter()
                    .map(|&i| {
                        let now = events.map_or(0, |e| (e.value(i) - midnight) / 1000);
                        line_length(batch, i, kinds[at + i], seqs[i], now, &transport, &master)
                            as u64
                            + extras(at + i)
                    })
                    .sum()
            })
        };
        let mut at = 0usize;
        if let Some(row) = &head {
            line_bytes += measure(row, at);
            sink.push(row.clone())?;
            at += 1;
        }
        for table in &batch_tables {
            for batch in table.read_all()? {
                let mut batch = if shift > 0 {
                    let seqs: Vec<u64> = u64s(&batch, "seq")?.iter().map(|&s| s - shift).collect();
                    replace(&batch, "seq", Arc::new(UInt64Array::from(seqs)))?
                } else {
                    batch
                };
                if recount && at + batch.num_rows() == from_parts + head.is_some() as usize {
                    let rows = batch.num_rows();
                    let mut updates_column: Vec<Option<u64>> = batch
                        .column_by_name("quote_updates")
                        .and_then(|c| c.as_primitive_opt::<UInt64Type>())
                        .map(|a| a.iter().collect())
                        .unwrap_or_default();
                    updates_column[rows - 1] = Some(updates);
                    batch = replace(
                        &batch,
                        "quote_updates",
                        Arc::new(UInt64Array::from(updates_column)),
                    )?;
                }
                line_bytes += measure(&batch, at);
                at += batch.num_rows();
                sink.push(batch)?;
                if at < total {
                    pace.rows("batches", at as u64, total as u64);
                }
            }
        }
        if let Some(row) = &tail {
            line_bytes += measure(row, at);
            sink.push(row.clone())?;
            at += 1;
        }
        require!(at == total, "Quotes or trades outside their batches");
        pace.rows("batches", at as u64, total as u64);
    }

    trace("batches written", &started);
    // The quotes and trades, a range of issues at a time.
    let mut counts: BTreeMap<u64, u64> = BTreeMap::new();
    let per_part: Vec<Res<Vec<(u64, u64)>>> =
        pool.install(|| depth_tables.par_iter().map(|t| t.issue_runs()).collect());
    for runs in per_part {
        for (issue, count) in runs? {
            *counts.entry(issue).or_default() += count;
        }
    }
    trace("counted", &started);
    // The rows to write, for the progress lines: the book before the piece goes into the first ranges' quotes.
    let quotes_total =
        counts.values().sum::<u64>() + state_batch.as_ref().map_or(0, |b| b.num_rows() as u64);
    let trades_total: u64 = trades.iter().sum();
    let (mut quotes_done, mut trades_done) = (0u64, 0u64);
    let mut latest: Vec<RecordBatch> = Vec::new();
    let mut read_time = std::time::Duration::ZERO;
    for (low, high) in ranges(&counts, config.range_rows.unwrap_or(PIECE_ROWS)) {
        let range_started = std::time::Instant::now();
        let read = |tables: &[PartTable]| -> Res<Vec<(usize, RecordBatch)>> {
            let jobs: Vec<(usize, usize)> = tables
                .iter()
                .enumerate()
                .flat_map(|(rank, table)| {
                    table
                        .spans
                        .iter()
                        .filter(|s| s.high >= low && s.low < high)
                        .map(move |s| (rank + 1, s.group))
                })
                .collect();
            let read: Vec<Res<(usize, Vec<RecordBatch>)>> = pool.install(|| {
                jobs.par_iter()
                    .map(|&(rank, group)| {
                        let mut kept = Vec::new();
                        for batch in tables[rank - 1].read(&[group])? {
                            kept.extend(trim(batch, low, high)?);
                        }
                        Ok((rank, kept))
                    })
                    .collect()
            });
            let mut out = Vec::new();
            for item in read {
                let (rank, batches) = item?;
                out.extend(batches.into_iter().map(|b| (rank, b)));
            }
            Ok(out)
        };
        // Quotes: the book before the piece first, then the parts' rows, renumbered; merged by issue.
        let mut sources: Vec<(usize, RecordBatch)> = Vec::new();
        if let Some(state) = state_rows_for(low, high)? {
            sources.push((0, state));
        }
        let loaded = read(&depth_tables)?;
        read_time += range_started.elapsed();
        for (rank, batch) in loaded {
            sources.push((rank, renumber(&batch, shift)?));
        }
        let mut all: Vec<Run> = Vec::new();
        for (index, (rank, batch)) in sources.iter().enumerate() {
            all.extend(runs(index, batch, low, high, *rank)?);
        }
        all.sort_by_key(|r| (r.issue, r.rank, r.batch, r.start));
        if std::env::var_os("BRISK_PARQUET_TRACE").is_some() {
            let bytes: usize = sources.iter().map(|(_, b)| b.get_array_memory_size()).sum();
            let rows: usize = sources.iter().map(|(_, b)| b.num_rows()).sum();
            eprintln!(
                "trace:   range {low}..{high}: {} batches, {rows} rows, {} MB",
                sources.len(),
                bytes >> 20
            );
        }
        let mut last_of: Vec<RecordBatch> = Vec::new();
        if changes {
            // Each issue's rows are walked on their own (its books replayed, its checkpoints told), issues on the pool at once.
            let mut issues: Vec<&[Run]> = Vec::new();
            let mut from = 0;
            for i in 1..=all.len() {
                if i == all.len() || all[i].issue != all[from].issue {
                    issues.push(&all[from..i]);
                    from = i;
                }
            }
            let write = depths_sink.is_some();
            let walked: Vec<Res<Walked>> = pool.install(|| {
                issues
                    .par_iter()
                    .map(|runs| walk_issue(&sources, runs, write))
                    .collect()
            });
            for item in walked {
                let item = item?;
                if let Some(sink) = depths_sink.as_mut() {
                    for rows in item.rows {
                        quotes_done += rows.num_rows() as u64;
                        sink.push(rows)?;
                    }
                }
                last_of.push(item.last);
            }
        } else {
            for (i, run) in all.iter().enumerate() {
                let rows = sources[run.batch].1.slice(run.start, run.len);
                if all.get(i + 1).is_none_or(|next| next.issue != run.issue) {
                    last_of.push(rows.slice(rows.num_rows() - 1, 1));
                }
                if let Some(sink) = depths_sink.as_mut() {
                    quotes_done += rows.num_rows() as u64;
                    sink.push(rows)?;
                }
            }
        }
        pace.rows("quotes", quotes_done, quotes_total);
        if !last_of.is_empty() {
            let schema = last_of[0].schema();
            latest.push(
                arrow_select::concat::concat_batches(&schema, &last_of).map_err(Failure::from)?,
            );
        }
        drop((sources, all, last_of));
        trace(&format!("range {low}..{high} read"), &started);
        // Trades: every part's rows in the range, by issue, then batch, then the decoder's order.
        let mut sources: Vec<(usize, RecordBatch)> = Vec::new();
        for (rank, batch) in read(&trade_tables)? {
            sources.push((rank, batch));
        }
        let mut all: Vec<Run> = Vec::new();
        for (index, (rank, batch)) in sources.iter().enumerate() {
            all.extend(runs(index, batch, low, high, *rank)?);
        }
        all.sort_by_key(|r| (r.issue, r.rank, r.batch, r.start));
        if let (Some(sink), false) = (trades_sink.as_mut(), all.is_empty()) {
            let slices: Vec<RecordBatch> = all
                .iter()
                .map(|r| sources[r.batch].1.slice(r.start, r.len))
                .collect();
            let merged = arrow_select::concat::concat_batches(&slices[0].schema(), &slices)
                .map_err(Failure::from)?;
            let seqs: Vec<u64> = u64s(&merged, "seq")?.iter().map(|&s| s - shift).collect();
            let merged = replace(&merged, "seq", Arc::new(UInt64Array::from(seqs)))?;
            let ids = trade_ids(&merged)?;
            trades_done += merged.num_rows() as u64;
            sink.push(replace(&merged, "trade_id", ids)?)?;
        }
        pace.rows("trades", trades_done, trades_total);
    }
    trace(
        &format!("ranges done (reading {:.1}s)", read_time.as_secs_f64()),
        &started,
    );
    pace.stage("finalizing");
    let mut summary = None;
    if let (Some(depths), Some(trades), Some(batches)) = (depths_sink, trades_sink, batches_sink) {
        let (quote_rows, quote_total) = depths.finish()?;
        let (_, trade_total) = trades.finish()?;
        batches.finish()?;
        let mut codes: Vec<&str> = master.iter().map(|m| m.code.as_str()).collect();
        codes.sort_unstable();
        summary = Some(json!({
            "source": source, "trading_date": trading_date,
            "first_source_time_us": first_clock, "last_source_time_us": scan.last_clock,
            "codes": codes, "batches": total, "quote_updates": quote_rows,
            "expanded_bytes": line_bytes + quote_total + trade_total,
        }));
    }
    trace("tables closed", &started);
    // The book after the piece, for the next one.
    if let Some(path) = &config.state_out {
        let mut file =
            writer::create_file(path, DEPTHS, schema::depths(changes), schema::ROW_GROUP)?;
        for batch in &latest {
            file.write(batch).map_err(fault)?;
        }
        file.close().map_err(fault)?;
    }
    Ok(Outcome {
        summary,
        clock: scan.last_clock,
        repaired,
    })
}
