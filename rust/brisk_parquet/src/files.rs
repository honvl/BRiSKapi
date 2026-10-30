//! A table's file: its structure as the archive requires it (`check` in `briskapi/_parquet.py`), and its row groups
//! read one at a time.
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};
use parquet::basic::Compression;
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};

use crate::error::{Res, refuse};
use crate::require;
use crate::schema;

/// What a table's footer says, once the file passes the structural checks.
pub struct Table {
    pub name: &'static str,
    pub rows: usize,
    pub groups: usize,
    /// A depths table whose quotes have book changes (`changes` and `checkpoint` columns).
    pub changes: bool,
    reader: ArrowReaderMetadata,
}

fn created_by_ok(text: &str) -> bool {
    // "parquet-cpp-arrow version 23.0.1" (pyarrow) or "parquet-rs version 59.0.0" (this crate): three numbers of up to four digits.
    let rest = ["parquet-cpp-arrow version ", "parquet-rs version "]
        .iter()
        .find_map(|p| text.strip_prefix(p));
    rest.is_some_and(|numbers| {
        let parts: Vec<&str> = numbers.split('.').collect();
        parts.len() == 3
            && parts
                .iter()
                .all(|p| (1..=4).contains(&p.len()) && p.bytes().all(|b| b.is_ascii_digit()))
    })
}

/// Open a table, refusing a file that is not exactly the canonical structure: the schema, its metadata, full row
/// groups, zstd, and column chunks that fill the file with nothing between them. `row_group` is the rows of a full group;
/// `legacy` the format of 1.4 and before (v2). A depths table in the current format may have its quotes' book changes.
pub fn open(directory: &Path, name: &'static str, row_group: usize, legacy: bool) -> Res<Table> {
    let path = directory.join(name);
    require!(path.is_file(), format!("Missing {name}"));
    let mut file = File::open(&path)?;
    let size = file.metadata()?.len();
    require!(size >= 12, format!("Unexpected bytes in {name}"));
    let mut tail = [0u8; 8];
    file.seek(SeekFrom::End(-8))?;
    file.read_exact(&mut tail)?;
    require!(
        &tail[4..] == b"PAR1",
        format!("Unreadable table: {name} is not Parquet")
    );
    let footer = u32::from_le_bytes(tail[..4].try_into().unwrap()) as u64;
    let meta: ParquetMetaData = ParquetMetaDataReader::new().parse_and_finish(&file)?;
    let reader =
        ArrowReaderMetadata::try_new(std::sync::Arc::new(meta.clone()), ArrowReaderOptions::new())?;

    let mut expected = schema::expected(name, legacy);
    let changes = name == schema::DEPTHS
        && !legacy
        && !same_schema(reader.schema(), &expected)
        && same_schema(reader.schema(), &schema::depths(true));
    if changes {
        expected = schema::depths(true);
    }
    require!(
        same_schema(reader.schema(), &expected),
        format!("Unexpected schema in {name}")
    );
    let file_meta = meta.file_metadata();
    let keys = file_meta
        .key_value_metadata()
        .map(|v| v.as_slice())
        .unwrap_or(&[]);
    let canonical = file_meta.created_by().is_some_and(created_by_ok)
        && keys.len() == expected.metadata().len() + 1
        && keys.iter().any(|kv| kv.key == "ARROW:schema")
        && expected.metadata().iter().all(|(k, v)| {
            keys.iter()
                .any(|kv| &kv.key == k && kv.value.as_deref() == Some(v.as_str()))
        });
    require!(canonical, format!("Unexpected file metadata in {name}"));
    let rows = meta.file_metadata().num_rows();
    require!(
        rows <= schema::max_rows(name),
        format!("Too many rows in {name}")
    );
    let groups = meta.num_row_groups();
    require!(
        (0..groups.saturating_sub(1)).all(|i| meta.row_group(i).num_rows() == row_group as i64),
        format!("Unexpected row groups in {name}")
    );
    require!(
        meta.row_groups().iter().all(|g| g
            .columns()
            .iter()
            .all(|c| matches!(c.compression(), Compression::ZSTD(_)))),
        format!("Unexpected compression in {name}")
    );
    let mut chunks: Vec<(i64, i64)> = meta
        .row_groups()
        .iter()
        .flat_map(|g| g.columns().iter())
        .map(|c| {
            (
                c.dictionary_page_offset()
                    .filter(|o| *o > 0)
                    .unwrap_or(c.data_page_offset()),
                c.compressed_size(),
            )
        })
        .collect();
    chunks.sort_unstable();
    let mut end = 4i64;
    for (start, length) in chunks {
        require!(start == end, format!("Unexpected bytes in {name}"));
        end = start + length;
    }
    require!(
        end as u64 == size.saturating_sub(8 + footer),
        format!("Unexpected bytes in {name}")
    );
    let mut decoded = 0i64;
    let limit_rows = if name == schema::BATCHES {
        schema::max_rows(schema::EQUITY)
    } else {
        schema::LEVELS as i64
    };
    for group in meta.row_groups() {
        decoded = decoded.saturating_add(group.total_byte_size());
        // Book sides hold at most LEVELS entries, book changes and checkpoints whole books; skipped issues at most the master.
        let rows = group.num_rows().max(1);
        let limit = limit_rows.saturating_mul(rows);
        let whole = ((schema::BOOKS.len() * schema::MAX_BOOK_LEVELS) as i64).saturating_mul(rows);
        require!(
            group.columns().iter().all(|c| {
                let held = matches!(
                    c.column_path().parts().first().map(String::as_str),
                    Some(schema::CHANGES | schema::CHECKPOINT)
                );
                c.num_values() <= if held { whole } else { limit }
            }),
            format!("Oversized column in {name}")
        );
    }
    require!(
        decoded <= schema::MAX_DECODED,
        format!("{name} exceeds decode limit")
    );
    Ok(Table {
        name,
        rows: rows as usize,
        groups,
        changes,
        reader,
    })
}

fn same_schema(found: &Schema, expected: &Schema) -> bool {
    found.fields().len() == expected.fields().len()
        && found.fields().iter().zip(expected.fields()).all(|(a, b)| {
            a.name() == b.name()
                && a.data_type() == b.data_type()
                && a.is_nullable() == b.is_nullable()
                && a.metadata().is_empty()
        })
        && found.metadata() == expected.metadata()
}

impl Table {
    /// Rows in a row group.
    pub fn group_rows(&self, group: usize) -> usize {
        self.reader.metadata().row_group(group).num_rows() as usize
    }

    /// One row group, whole.
    pub fn read(&self, directory: &Path, group: usize) -> Res<RecordBatch> {
        let file = File::open(directory.join(self.name))?;
        let rows = self.group_rows(group).max(1);
        let builder = ParquetRecordBatchReaderBuilder::new_with_metadata(file, self.reader.clone());
        let mut reader = builder
            .with_row_groups(vec![group])
            .with_batch_size(rows)
            .build()?;
        let first = match reader.next() {
            Some(batch) => batch?,
            None => {
                return refuse(format!(
                    "Unreadable table: {} has an empty row group",
                    self.name
                ));
            }
        };
        // A group is read as one batch (the reader's batch size is its rows); a second would be a split.
        match reader.next() {
            None => Ok(first),
            Some(_) => refuse(format!(
                "Unreadable table: {} has an unexpected batch split",
                self.name
            )),
        }
    }
}

/// The format a recording's tables say they are in: its depths table's `brisk_format`.
pub fn format_of(directory: &Path) -> Res<Option<String>> {
    let path = directory.join(schema::DEPTHS);
    require!(path.is_file(), format!("Missing {}", schema::DEPTHS));
    let file = File::open(path)?;
    let meta = match ParquetMetaDataReader::new().parse_and_finish(&file) {
        Ok(meta) => meta,
        Err(_) => return Ok(None),
    };
    let keys = meta
        .file_metadata()
        .key_value_metadata()
        .map(|v| v.as_slice())
        .unwrap_or(&[]);
    Ok(keys
        .iter()
        .find(|kv| kv.key == "brisk_format")
        .and_then(|kv| kv.value.clone()))
}

/// v2's `ts_init` is a copy of `ts_event`: whether it is, in every row.
pub fn clock_copied(batch: &RecordBatch) -> bool {
    use arrow_array::Array;
    use arrow_array::cast::AsArray;
    use arrow_array::types::TimestampNanosecondType;
    let column = |name: &str| {
        batch
            .column_by_name(name)
            .and_then(|c| c.as_primitive_opt::<TimestampNanosecondType>())
    };
    match (column("ts_event"), column("ts_init")) {
        (Some(event), Some(init)) => event.values() == init.values() && init.null_count() == 0,
        _ => false,
    }
}
