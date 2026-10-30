//! Verify a broker's recording (all four tables, in a directory) a row group at a time, never whole in memory
//! (`verify_broker` in `briskapi/_parquet.py` states the rules): exactly the expected tables, each row group the canonical
//! encoding of its content, the batches' rules, and of every quote and trade its identity, order, batch, clock and value
//! ranges, and its books' order. Not checked, the market's own consistency. Returns the summary the archive compares with
//! the manifest.
use std::path::Path;
use std::sync::Mutex;

use arrow_array::RecordBatch;
use arrow_array::cast::AsArray;
use arrow_array::types::UInt64Type;
use rayon::prelude::*;
use serde_json::{Value, json};

use crate::batches::{self, END, HISTORY, QUOTES};
use crate::error::{Failure, Res};
use crate::files;
use crate::master;
use crate::replay::{IssueState, Rows};
use crate::require;
use crate::rows::{Context, Edge, Group, Trades, check_depths};
use crate::schema::{self, ROW_GROUP};

pub struct Options {
    /// Rows of a full row group.
    pub row_group: usize,
    pub threads: usize,
    /// The format the tables must be in; the one their depths table says if none.
    pub format: Option<String>,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            row_group: ROW_GROUP,
            threads: std::thread::available_parallelism().map_or(1, |n| n.get()),
            format: None,
        }
    }
}

const MAX_HISTORY: u64 = 50_000;

/// An issue's rows in a wave: (group, first row, end row) of each run of them.
type Chain = (u64, Vec<(usize, usize, usize)>);

/// A wave's row groups' book changes replayed (`replay`): each issue's rows, across the groups of the wave, are one chain,
/// and chains are replayed on the pool's threads at once. The wave's first chain goes on from `carry`, the last issue of the
/// wave before it if it is the same; the last chain's state is carried on.
fn replay_wave(
    pool: &rayon::ThreadPool,
    batches: &[RecordBatch],
    carry: Option<(u64, IssueState)>,
) -> Res<Option<(u64, IssueState)>> {
    let rows: Vec<Rows> = batches.iter().map(Rows::new).collect::<Res<_>>()?;
    let mut chains: Vec<Chain> = Vec::new();
    for (g, batch) in batches.iter().enumerate() {
        let issues = batch
            .column_by_name("issue_id")
            .and_then(|c| c.as_primitive_opt::<UInt64Type>())
            .map(|a| a.values())
            .ok_or_else(|| Failure::Refused("Unexpected schema in issue_id".into()))?;
        let mut i = 0;
        while i < issues.len() {
            let mut j = i + 1;
            while j < issues.len() && issues[j] == issues[i] {
                j += 1;
            }
            match chains.last_mut() {
                Some((issue, runs)) if *issue == issues[i] => runs.push((g, i, j)),
                _ => chains.push((issues[i], vec![(g, i, j)])),
            }
            i = j;
        }
    }
    if chains.is_empty() {
        return Ok(carry);
    }
    let mut starts: Vec<Option<IssueState>> = (0..chains.len()).map(|_| None).collect();
    if let Some((issue, state)) = carry
        && issue == chains[0].0
    {
        starts[0] = Some(state);
    }
    let walked: Vec<Res<IssueState>> = pool.install(|| {
        chains
            .par_iter()
            .zip(starts.par_iter_mut())
            .map(|((_, runs), start)| {
                let mut state = start.take().unwrap_or_default();
                for &(g, from, to) in runs {
                    for i in from..to {
                        let point = state.apply(&rows[g], i)?;
                        require!(
                            rows[g].checkpoint_is(i, &state.books, point),
                            format!("Non-canonical {}", schema::DEPTHS)
                        );
                    }
                }
                Ok(state)
            })
            .collect()
    });
    let mut last = None;
    for ((issue, _), state) in chains.iter().zip(walked) {
        last = Some((*issue, state?));
    }
    Ok(last)
}

pub fn verify_broker(
    directory: &Path,
    options: &Options,
    progress: &(dyn Fn(&str) + Sync),
) -> Res<Value> {
    let format = match &options.format {
        Some(format) => Some(format.clone()),
        None => files::format_of(directory)?,
    };
    let legacy = match format.as_deref() {
        Some(schema::FORMAT) => false,
        Some(schema::LEGACY_FORMAT) => true,
        _ => return crate::error::refuse("Unexpected format"),
    };
    let tables: Vec<files::Table> = schema::FILES
        .iter()
        .map(|name| files::open(directory, name, options.row_group, legacy))
        .collect::<Res<_>>()?;
    let [equity, depths, trades, batch_table] = <[files::Table; 4]>::try_from(tables)
        .ok()
        .expect("four tables");
    let (equity_rows, master) = master::read(directory, &equity)?;
    let batches = batches::verify(directory, &batch_table, &master, legacy)?;
    master::check_canonical(&equity_rows, &master, batches.midnight)?;
    progress("verify: batches");
    let n = batches.kinds.len();
    let context = Context::new(&batches, &master, legacy, depths.changes);
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(options.threads.max(1))
        .build()
        .map_err(|e| Failure::Fault(e.into()))?;

    // Quotes: row groups in parallel, a wave of them at a time, each counting its quotes per batch into its thread's own
    // array. The quotes' book changes are replayed, issue by issue, as each wave is done (see `replay_wave`).
    let tallies: Vec<Mutex<Vec<u32>>> = (0..pool.current_num_threads())
        .map(|_| Mutex::new(vec![0u32; n]))
        .collect();
    let (wave, mut groups) = (pool.current_num_threads() * 4, Vec::<Group>::new());
    let (mut carry, mut previous) = (None, None::<Edge>);
    for from in (0..depths.groups).step_by(wave) {
        let done: Vec<(Group, Option<RecordBatch>)> = pool.install(|| {
            (from..(from + wave).min(depths.groups))
                .into_par_iter()
                .map(|g| {
                    let batch = depths.read(directory, g)?;
                    let index = rayon::current_thread_index().unwrap_or(0);
                    let mut counts = tallies[index].lock().unwrap_or_else(|e| e.into_inner());
                    let group = check_depths(&batch, &context, &mut counts)?;
                    Ok((group, depths.changes.then_some(batch)))
                })
                .collect::<Res<Vec<_>>>()
        })?;
        for (group, _) in &done {
            if let Some(a) = previous {
                let b = group.first;
                require!(
                    (a.issue, a.seq) < (b.issue, b.seq),
                    "Quotes not sorted by issue, then batch"
                );
                require!(
                    a.issue != b.issue || (b.frame >= a.frame && b.time >= a.time),
                    "Quote frame/time regression"
                );
            }
            previous = Some(group.last);
        }
        if depths.changes {
            let batches: Vec<RecordBatch> = done.iter().filter_map(|(_, b)| b.clone()).collect();
            carry = replay_wave(&pool, &batches, carry)?;
        }
        groups.extend(done.into_iter().map(|(group, _)| group));
    }
    let quote_rows: usize = groups.iter().map(|g| g.rows).sum();
    require!(
        quote_rows == depths.rows,
        format!("Non-canonical {}", schema::DEPTHS)
    );
    let quote_bytes: u64 = groups.iter().map(|g| g.bytes).sum();
    let mut quotes = vec![0u64; n];
    for tally in &tallies {
        for (total, count) in quotes
            .iter_mut()
            .zip(tally.lock().unwrap_or_else(|e| e.into_inner()).iter())
        {
            *total += *count as u64;
        }
    }
    drop(tallies);
    progress("verify: depths");

    // Trades: in order, as their IDs count on from one row group to the next.
    let mut counts = vec![0u32; n];
    let mut state = Trades::default();
    for g in 0..trades.groups {
        let batch = trades.read(directory, g)?;
        state.check(&batch, &context, &mut counts)?;
    }
    require!(
        state.rows == trades.rows,
        format!("Non-canonical {}", schema::TRADES)
    );
    let trade_counts: Vec<u64> = counts.iter().map(|&c| c as u64).collect();
    progress("verify: trades");

    require!(
        quotes[0] + batches.first_skipped as u64 == master.len() as u64,
        "Incomplete bootstrap"
    );
    for i in 0..n {
        if batches.kinds[i] == HISTORY {
            require!(
                trade_counts[i] > 0 && trade_counts[i] <= MAX_HISTORY,
                "Invalid trades"
            );
        }
    }
    let updates: u64 = (0..n)
        .filter(|&i| batches.kinds[i] == QUOTES)
        .map(|i| quotes[i])
        .sum();
    require!(batches.end_quote_updates == updates, "End summary mismatch");
    let mut expanded = quote_bytes + state.bytes;
    for i in 0..n {
        expanded += batches.lengths[i] as u64;
        if batches.kinds[i] != END {
            expanded += trade_counts[i].saturating_sub(1);
        }
        if batches.kinds[i] <= QUOTES {
            expanded += quotes[i].saturating_sub(1);
        }
    }
    let mut codes: Vec<&str> = master.iter().map(|m| m.code.as_str()).collect();
    codes.sort_unstable();
    Ok(json!({
        "source": batches.source, "trading_date": batches.trading_date,
        "first_source_time_us": batches.first_source_time_us, "last_source_time_us": batches.last_source_time_us,
        "codes": codes, "batches": n, "quote_updates": quotes.iter().sum::<u64>(), "expanded_bytes": expanded,
    }))
}
