//! `brisk-parquet`: the archive's and the recorder's fast Parquet work.
//!
//!     brisk-parquet verify DIRECTORY [--row-group ROWS] [--threads N] [--format FORMAT]
//!     brisk-parquet package --spool DIR --midnight NS --out DIR [--day-start PART] [--cut-state FILE --cut-clock US]
//!                           [--end parts|cut|repair] [--state-out FILE] [--threads N] [--range-rows N] [--parts-file FILE] [PART...]
//!     brisk-parquet state --spool DIR --midnight NS --state-out FILE [--cut-state FILE --cut-clock US] [--day-start PART]
//!                         [--parts-file FILE] [PART...]
//!     brisk-parquet part --out DIR --midnight NS [--threads N]    (the decoder's JSON lines on stdin)
//!     brisk-parquet schemas DIRECTORY
//!
//! The parts of `package` and `state` are those `--parts-file` names (one path per line, UTF-8; `-` reads them from stdin), then
//! the positional ones: a day has thousands, more than a Windows command line holds (32,767 characters), so briskapi sends them
//! on stdin. Each prints a JSON result on stdout, and `progress: ...` lines on stderr (`verify`: the table it checks; `package`: its stage,
//! `package: preparing|finalizing`, and the rows written of a table, `package: batches|quotes|trades DONE/TOTAL`). A recording
//! that breaks a rule exits 2 with `refused: MESSAGE` on stderr; any other failure exits 1.
use std::collections::HashMap;
use std::path::PathBuf;
use std::process::ExitCode;

use brisk_parquet::error::{Failure, Res};
use brisk_parquet::package::{Config, Cut, End, package_with};
use brisk_parquet::part::{PartConfig, write_part};
use brisk_parquet::parts::listed;
use brisk_parquet::verify::{Options, verify_broker};
use serde_json::json;

// Row groups are decoded and freed over and over; musl's allocator, the archive's build target, hands the memory back to the
// system each time.
#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

const USAGE: &str = "usage: brisk-parquet verify DIRECTORY [--row-group ROWS] [--threads N] [--format FORMAT]\n       \
    brisk-parquet package --spool DIR --midnight NS --out DIR [--day-start PART] [--cut-state FILE --cut-clock US] \
    [--end parts|cut|repair] [--state-out FILE] [--threads N] [--range-rows N] [--parts-file FILE] [PART...]\n       \
    brisk-parquet state --spool DIR --midnight NS --state-out FILE [--cut-state FILE --cut-clock US] [--day-start PART] \
    [--parts-file FILE] [PART...]\n       \
    brisk-parquet part --out DIR --midnight NS [--threads N] < LINES\n       \
    brisk-parquet schemas DIRECTORY";

/// Arguments: `--flag value` pairs and the positional words.
struct Args {
    flags: HashMap<String, String>,
    words: Vec<String>,
}

fn parse(args: impl Iterator<Item = String>) -> Option<Args> {
    let (mut flags, mut words) = (HashMap::new(), Vec::new());
    let mut args = args;
    while let Some(arg) = args.next() {
        if let Some(name) = arg.strip_prefix("--") {
            flags.insert(name.to_string(), args.next()?);
        } else {
            words.push(arg);
        }
    }
    Some(Args { flags, words })
}

impl Args {
    fn number<T: std::str::FromStr>(&self, name: &str) -> Option<Option<T>> {
        match self.flags.get(name) {
            None => Some(None),
            Some(v) => v.parse().ok().map(Some),
        }
    }
    fn path(&self, name: &str) -> Option<PathBuf> {
        self.flags.get(name).map(PathBuf::from)
    }
}

fn threads() -> usize {
    std::thread::available_parallelism().map_or(1, |n| n.get())
}

/// The parts of `package` and `state`: those `--parts-file` names (one path per line; `-` for standard input), then the
/// positional ones.
fn parts(args: &Args) -> Res<Vec<PathBuf>> {
    let mut parts = match args.flags.get("parts-file").map(String::as_str) {
        None => Vec::new(),
        Some("-") => listed(&std::io::read_to_string(std::io::stdin().lock())?),
        Some(file) => listed(&std::fs::read_to_string(file).map_err(|error| {
            Failure::Fault(anyhow::anyhow!(
                "cannot read the parts file {file}: {error}"
            ))
        })?),
    };
    parts.extend(args.words.iter().map(PathBuf::from));
    Ok(parts)
}

fn configuration(args: &Args, write: bool, parts: Vec<PathBuf>) -> Option<Config> {
    let cut = match (args.path("cut-state"), args.number::<i64>("cut-clock")?) {
        (Some(state), Some(clock)) => Some(Cut { state, clock }),
        (None, None) => None,
        _ => return None,
    };
    let end = match args.flags.get("end").map(String::as_str) {
        None | Some("parts") => End::Parts,
        Some("cut") => End::Cut,
        Some("repair") => End::Repair,
        _ => return None,
    };
    Some(Config {
        spool: args.path("spool")?,
        parts,
        out: if write { Some(args.path("out")?) } else { None },
        midnight: args.number::<i64>("midnight")??,
        day_start: args.path("day-start"),
        cut,
        end,
        state_out: args.path("state-out"),
        threads: args.number::<usize>("threads")?.unwrap_or_else(threads),
        range_rows: args.number::<u64>("range-rows")?,
    })
}

fn finish(result: Res<serde_json::Value>) -> ExitCode {
    match result {
        Ok(value) => {
            println!("{value}");
            ExitCode::SUCCESS
        }
        Err(Failure::Refused(message)) => {
            eprintln!("refused: {message}");
            ExitCode::from(2)
        }
        Err(Failure::Fault(error)) => {
            eprintln!("error: {error:#}");
            ExitCode::from(1)
        }
    }
}

fn usage() -> ExitCode {
    eprintln!("{USAGE}");
    ExitCode::from(64)
}

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let command = args.next();
    let Some(args) = parse(args) else {
        return usage();
    };
    match command.as_deref() {
        Some("verify") => {
            let (Some(rows), Some(threads_flag)) = (
                args.number::<usize>("row-group"),
                args.number::<usize>("threads"),
            ) else {
                return usage();
            };
            let Some(directory) = args.words.first() else {
                return usage();
            };
            let mut options = Options::default();
            options.row_group = rows.unwrap_or(options.row_group);
            options.threads = threads_flag.unwrap_or(options.threads);
            options.format = args.flags.get("format").cloned();
            let progress = |label: &str| eprintln!("progress: {label}");
            finish(verify_broker(
                &PathBuf::from(directory),
                &options,
                &progress,
            ))
        }
        Some(kind @ ("package" | "state")) => {
            let parts = match parts(&args) {
                Ok(parts) => parts,
                Err(error) => return finish(Err(error)),
            };
            let Some(config) = configuration(&args, kind == "package", parts) else {
                return usage();
            };
            let progress = |label: &str| eprintln!("progress: {label}");
            finish(package_with(&config, &progress).map(|outcome| {
                json!({"summary": outcome.summary, "clock": outcome.clock, "repaired": outcome.repaired})
            }))
        }
        Some("part") => {
            let (Some(out), Ok(Some(midnight)), Ok(threads_flag)) = (
                args.path("out"),
                args.number::<i64>("midnight").ok_or(()),
                args.number::<usize>("threads").ok_or(()),
            ) else {
                return usage();
            };
            let mut input = Vec::new();
            if let Err(error) = std::io::Read::read_to_end(&mut std::io::stdin().lock(), &mut input)
            {
                eprintln!("error: {error}");
                return ExitCode::from(1);
            }
            let config = PartConfig {
                out,
                midnight,
                threads: threads_flag.unwrap_or_else(threads),
            };
            finish(
                write_part(&input, &config)
                    .map(|s| json!({"batches": s.batches, "quotes": s.quotes, "trades": s.trades})),
            )
        }
        Some("schemas") => match args.words.first() {
            Some(directory) => {
                finish(brisk_parquet::writer::empty(&PathBuf::from(directory)).map(|()| json!({})))
            }
            None => usage(),
        },
        _ => usage(),
    }
}
