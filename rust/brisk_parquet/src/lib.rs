//! BRiSK recordings' Parquet tables, for the archive's function and the recorder: verifying a broker's session and
//! packaging it. `briskapi/_parquet.py` defines the format; this crate is its fast implementation.
// Rows are checked column by column through parallel slices, indexed by row: iterators would only hide that.
#![allow(clippy::needless_range_loop)]

pub mod batches;
pub mod error;
pub mod files;
pub mod master;
pub mod package;
pub mod part;
pub mod parts;
pub mod replay;
pub mod rows;
pub mod schema;
pub mod sink;
pub mod sizes;
pub mod units;
pub mod verify;
pub mod writer;
