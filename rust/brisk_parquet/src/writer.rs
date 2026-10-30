//! How the tables are written: the properties every table of a recording has (zstd, delta encoded rising columns, a
//! dictionary elsewhere, full row groups), as `options()` in `briskapi/_parquet.py` has them.
use std::fs::File;
use std::io::BufWriter;
use std::path::Path;
use std::sync::Arc;

use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use parquet::schema::types::ColumnPath;

use arrow_schema::Schema;

use crate::error::{Failure, Res};
use crate::schema;

const ZSTD_LEVEL: i32 = 3;

pub fn properties(name: &str, row_group: usize) -> WriterProperties {
    // The schema's own metadata (the table's type and format) is a file key-value pair too, as pyarrow writes it.
    let mut pairs: Vec<KeyValue> = schema::expected(name, false)
        .metadata()
        .iter()
        .map(|(k, v)| KeyValue::new(k.clone(), v.clone()))
        .collect();
    pairs.sort_by(|a, b| a.key.cmp(&b.key));
    let mut builder = WriterProperties::builder()
        .set_key_value_metadata(Some(pairs))
        .set_compression(Compression::ZSTD(
            ZstdLevel::try_new(ZSTD_LEVEL).expect("a valid zstd level"),
        ))
        .set_writer_version(WriterVersion::PARQUET_1_0)
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_dictionary_enabled(true)
        // Nothing but the row groups' column chunks between the magic and the footer: the archive checks for it.
        .set_offset_index_disabled(true)
        .set_max_row_group_row_count(Some(row_group));
    for column in schema::deltas(name) {
        let path = ColumnPath::from(*column);
        builder = builder
            .set_column_encoding(path.clone(), Encoding::DELTA_BINARY_PACKED)
            .set_column_dictionary_enabled(path, false);
    }
    builder.build()
}

/// How a spool part's tables are written: quickly (zstd level 1), with the statistics of the issue alone, as the part is read
/// again for each range of issues and then thrown away.
pub fn part_properties(name: &str, row_group: usize) -> WriterProperties {
    let pairs: Vec<KeyValue> = {
        let mut pairs: Vec<KeyValue> = schema::expected(name, false)
            .metadata()
            .iter()
            .map(|(k, v)| KeyValue::new(k.clone(), v.clone()))
            .collect();
        pairs.sort_by(|a, b| a.key.cmp(&b.key));
        pairs
    };
    let mut builder = WriterProperties::builder()
        .set_key_value_metadata(Some(pairs))
        .set_compression(Compression::ZSTD(
            ZstdLevel::try_new(1).expect("a valid zstd level"),
        ))
        .set_writer_version(WriterVersion::PARQUET_1_0)
        .set_statistics_enabled(EnabledStatistics::None)
        .set_dictionary_enabled(true)
        .set_offset_index_disabled(true)
        .set_max_row_group_row_count(Some(row_group));
    builder = builder
        .set_column_statistics_enabled(ColumnPath::from("issue_id"), EnabledStatistics::Chunk);
    for column in schema::deltas(name) {
        let path = ColumnPath::from(*column);
        builder = builder
            .set_column_encoding(path.clone(), Encoding::DELTA_BINARY_PACKED)
            .set_column_dictionary_enabled(path, false);
    }
    builder.build()
}

pub type TableWriter = ArrowWriter<BufWriter<File>>;

/// A writer for the table `name` under `directory` (its folders made).
pub fn create(directory: &Path, name: &str, row_group: usize) -> Res<TableWriter> {
    create_file(
        &directory.join(name),
        name,
        schema::expected(name, false),
        row_group,
    )
}

/// A writer for a table of the kind `name` and schema `schema` (the kind's own, but for a depths table with book changes) in
/// the file `path`.
pub fn create_file(path: &Path, name: &str, schema: Schema, row_group: usize) -> Res<TableWriter> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file = BufWriter::with_capacity(1 << 20, File::create(path)?);
    ArrowWriter::try_new(file, Arc::new(schema), Some(properties(name, row_group)))
        .map_err(|e| Failure::Fault(e.into()))
}

/// An empty table of each kind, to see the schemas as a reader sees them; the depths table with its quotes' book changes if
/// `changes`.
pub fn empty(directory: &Path, changes: bool) -> Res<()> {
    for name in schema::FILES {
        let schema = if changes && name == schema::DEPTHS {
            schema::depths(true)
        } else {
            schema::expected(name, false)
        };
        create_file(&directory.join(name), name, schema, schema::ROW_GROUP)?
            .close()
            .map_err(|e| Failure::Fault(e.into()))?;
    }
    Ok(())
}
