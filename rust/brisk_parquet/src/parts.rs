//! A spool part (`briskapi/_spool.py`): the depths, trades and batch tables of a few seconds of a session, read back for
//! packaging. A part's tables hold rows sorted by issue, then batch; a row group holds a block of issues (and its footer says
//! which), so a piece of the day is read a range of issues at a time.
use std::fs::File;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;

use arrow_array::RecordBatch;
use arrow_array::cast::AsArray;
use arrow_array::types::UInt64Type;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};
use parquet::basic::Compression;
use parquet::errors::ParquetError;
use parquet::file::metadata::{
    ColumnChunkMetaData, FileMetaData, ParquetMetaData, ParquetStatisticsPolicy, RowGroupMetaData,
};
use parquet::file::reader::{ChunkReader, Length};
use parquet::file::statistics::Statistics;
use parquet::schema::types::SchemaDescPtr;

use crate::error::{Failure, Res, refuse};
use crate::require;
use crate::schema;

/// The parts a `--parts-file` names: one path per line, UTF-8, blank lines and a trailing carriage return ignored. A day has
/// thousands of parts, more than a Windows command line (32,767 characters) holds, so `package` and `state` take them this way.
pub fn listed(text: &str) -> Vec<PathBuf> {
    text.strip_prefix('\u{feff}')
        .unwrap_or(text)
        .lines()
        .map(|line| line.trim_end_matches('\r'))
        .filter(|line| !line.is_empty())
        .map(PathBuf::from)
        .collect()
}

/// Where a row group's issues lie: the lowest and highest issue ID in it.
#[derive(Clone, Copy, Debug)]
pub struct Span {
    pub group: usize,
    pub rows: usize,
    pub low: u64,
    pub high: u64,
}

pub struct PartTable {
    path: PathBuf,
    options: ArrowReaderOptions,
    pub spans: Vec<Span>,
    file_len: u64,
    /// What reading a row group takes of the footer, kept in a few bytes per column chunk: a footer parsed whole is some 4 MB for a
    /// part of a hundred row groups, a day has a thousand parts, and parsing one again for each row group read took half the time.
    file_meta: FileMetaData,
    descr: SchemaDescPtr,
    chunks: Vec<Chunk>,
    groups: Vec<GroupRecord>,
}

#[derive(Clone, Copy)]
struct Chunk {
    dictionary: i64,
    data: i64,
    compressed: i64,
    uncompressed: i64,
    values: i64,
}

#[derive(Clone, Copy)]
struct GroupRecord {
    rows: i64,
    bytes: i64,
}

/// A file's bytes that are in memory, as a parquet reader sees the file: reading a row group takes one read of the file for all
/// its columns, not the four system calls a column parquet's own file reader makes.
#[derive(Clone)]
struct Region {
    len: u64,
    parts: Arc<Vec<(u64, Bytes)>>,
}

impl Length for Region {
    fn len(&self) -> u64 {
        self.len
    }
}

impl Region {
    fn find(&self, start: u64, length: usize) -> Option<Bytes> {
        self.parts
            .iter()
            .find(|(at, bytes)| start >= *at && start + length as u64 <= at + bytes.len() as u64)
            .map(|(at, bytes)| {
                let from = (start - at) as usize;
                bytes.slice(from..from + length)
            })
    }
}

impl ChunkReader for Region {
    type T = Cursor<Bytes>;

    fn get_read(&self, start: u64) -> Result<Self::T, ParquetError> {
        let part = self
            .parts
            .iter()
            .find(|(at, bytes)| start >= *at && start < at + bytes.len() as u64);
        match part {
            Some((at, bytes)) => Ok(Cursor::new(bytes.slice((start - at) as usize..))),
            None => Err(ParquetError::General(format!("byte {start} was not read"))),
        }
    }

    fn get_bytes(&self, start: u64, length: usize) -> Result<Bytes, ParquetError> {
        self.find(start, length)
            .ok_or_else(|| ParquetError::General(format!("bytes {start}+{length} were not read")))
    }
}

/// The footer of `file` (its length is `file_len`), as a region of the file in memory, and its metadata.
fn footer(file: &File, options: &ArrowReaderOptions) -> Res<(Region, ArrowReaderMetadata, Bytes)> {
    let file_len = file.metadata()?.len();
    let end = read_at(file, file_len - 8, 8)?;
    let footer = u32::from_le_bytes(end[..4].try_into().unwrap()) as u64;
    let tail = Bytes::from(read_at(file, file_len - 8 - footer, footer as usize + 8)?);
    let region = Region {
        len: file_len,
        parts: Arc::new(vec![(file_len - tail.len() as u64, tail.clone())]),
    };
    let reader = ArrowReaderMetadata::load(&region, options.clone())?;
    Ok((region, reader, tail))
}

/// `length` bytes of `file` from `offset`, by one system call where the system has one.
fn read_at(file: &File, offset: u64, length: usize) -> std::io::Result<Vec<u8>> {
    let mut buffer = vec![0u8; length];
    #[cfg(unix)]
    {
        std::os::unix::fs::FileExt::read_exact_at(file, &mut buffer, offset)?;
    }
    #[cfg(windows)]
    {
        let mut done = 0;
        while done < length {
            let n = std::os::windows::fs::FileExt::seek_read(
                file,
                &mut buffer[done..],
                offset + done as u64,
            )?;
            if n == 0 {
                return Err(std::io::ErrorKind::UnexpectedEof.into());
            }
            done += n;
        }
    }
    Ok(buffer)
}

/// How a part's footer is read: without the statistics of its 51 columns but the issue's. A day has a thousand parts of a
/// hundred row groups each; their footers whole would take gigabytes, so they are read again for each range of issues.
fn light(leaf: Option<usize>) -> ArrowReaderOptions {
    let columns = match leaf {
        Some(leaf) => ParquetStatisticsPolicy::skip_except(&[leaf]),
        None => ParquetStatisticsPolicy::SkipAll,
    };
    ArrowReaderOptions::new()
        .with_encoding_stats_policy(ParquetStatisticsPolicy::SkipAll)
        .with_size_stats_policy(ParquetStatisticsPolicy::SkipAll)
        .with_column_stats_policy(columns)
}

impl PartTable {
    pub fn open(part: &Path, name: &'static str) -> Res<PartTable> {
        PartTable::open_file(&part.join(name), name)
    }

    /// A table kept in a file of its own (of the kind `name`).
    pub fn open_file(path: &Path, name: &'static str) -> Res<PartTable> {
        let path = path.to_path_buf();
        require!(path.is_file(), format!("Missing {name}"));
        let expected = schema::expected(name, false);
        let descriptor = parquet::arrow::ArrowSchemaConverter::new().convert(&expected)?;
        let leaf = descriptor
            .columns()
            .iter()
            .position(|c| c.path().string() == "issue_id");
        let options = light(leaf);
        let file = File::open(&path)?;
        let file_len = file.metadata()?.len();
        require!(
            file_len >= 12,
            format!("Unreadable table: {name} is not Parquet")
        );
        let end = read_at(&file, file_len - 8, 8)?;
        require!(
            &end[4..] == b"PAR1",
            format!("Unreadable table: {name} is not Parquet")
        );
        let footer = u32::from_le_bytes(end[..4].try_into().unwrap()) as u64;
        require!(
            footer + 8 <= file_len,
            format!("Unreadable table: {name} is not Parquet")
        );
        let tail = Bytes::from(read_at(&file, file_len - 8 - footer, footer as usize + 8)?);
        let region = Region {
            len: file_len,
            parts: Arc::new(vec![(file_len - tail.len() as u64, tail)]),
        };
        let reader = ArrowReaderMetadata::load(&region, options.clone())?;
        let found = reader.schema();
        require!(
            found.fields().len() == expected.fields().len()
                && found
                    .fields()
                    .iter()
                    .zip(expected.fields())
                    .all(|(a, b)| a.name() == b.name() && a.data_type() == b.data_type()),
            format!("Unexpected schema in {name}")
        );
        let meta = reader.metadata();
        let mut spans = Vec::with_capacity(meta.num_row_groups());
        for group in 0..meta.num_row_groups() {
            let rows = meta.row_group(group).num_rows() as usize;
            let (mut low, mut high) = (0, u64::MAX);
            if let Some(Statistics::Int64(stats)) =
                leaf.and_then(|c| meta.row_group(group).column(c).statistics())
                && let (Some(min), Some(max)) = (stats.min_opt(), stats.max_opt())
            {
                (low, high) = (*min as u64, *max as u64);
            }
            spans.push(Span {
                group,
                rows,
                low,
                high,
            });
        }
        let descr = meta.file_metadata().schema_descr_ptr();
        let mut chunks = Vec::with_capacity(meta.num_row_groups() * descr.num_columns());
        let mut groups = Vec::with_capacity(meta.num_row_groups());
        for group in meta.row_groups() {
            groups.push(GroupRecord {
                rows: group.num_rows(),
                bytes: group.total_byte_size(),
            });
            chunks.extend(group.columns().iter().map(|c| Chunk {
                dictionary: c.dictionary_page_offset().filter(|o| *o > 0).unwrap_or(0),
                data: c.data_page_offset(),
                compressed: c.compressed_size(),
                uncompressed: c.uncompressed_size(),
                values: c.num_values(),
            }));
        }
        let file_meta = FileMetaData::new(
            meta.file_metadata().version(),
            meta.file_metadata().num_rows(),
            None,
            meta.file_metadata().key_value_metadata().cloned(),
            descr.clone(),
            None,
        );
        Ok(PartTable {
            path,
            options,
            spans,
            file_len,
            file_meta,
            descr,
            chunks,
            groups,
        })
    }

    /// The row groups `groups`, each as a batch: a row group's bytes are read at once, and decoded from memory.
    pub fn read(&self, groups: &[usize]) -> Res<Vec<RecordBatch>> {
        let file = File::open(&self.path)?;
        let width = self.descr.num_columns();
        let mut out = Vec::with_capacity(groups.len());
        for &group in groups {
            let chunks = &self.chunks[group * width..(group + 1) * width];
            let mut columns = Vec::with_capacity(width);
            for (index, chunk) in chunks.iter().enumerate() {
                columns.push(
                    ColumnChunkMetaData::builder(self.descr.column(index))
                        .set_compression(Compression::ZSTD(Default::default()))
                        .set_num_values(chunk.values)
                        .set_total_compressed_size(chunk.compressed)
                        .set_total_uncompressed_size(chunk.uncompressed)
                        .set_data_page_offset(chunk.data)
                        .set_dictionary_page_offset(
                            (chunk.dictionary > 0).then_some(chunk.dictionary),
                        )
                        .build()?,
                );
            }
            let record = self.groups[group];
            let row_group = RowGroupMetaData::builder(self.descr.clone())
                .set_num_rows(record.rows)
                .set_total_byte_size(record.bytes)
                .set_column_metadata(columns)
                .build()?;
            let metadata = ParquetMetaData::new(self.file_meta.clone(), vec![row_group]);
            let reader =
                ArrowReaderMetadata::try_new(Arc::new(metadata), ArrowReaderOptions::new())?;
            let start = chunks
                .iter()
                .map(|c| {
                    if c.dictionary > 0 {
                        c.dictionary
                    } else {
                        c.data
                    }
                })
                .min()
                .unwrap_or(0);
            let end = chunks
                .iter()
                .map(|c| {
                    (if c.dictionary > 0 {
                        c.dictionary
                    } else {
                        c.data
                    }) + c.compressed
                })
                .max()
                .unwrap_or(0);
            let bytes = Bytes::from(read_at(&file, start as u64, (end - start) as usize)?);
            let region = Region {
                len: self.file_len,
                parts: Arc::new(vec![(start as u64, bytes)]),
            };
            let rows = record.rows as usize;
            let builder = ParquetRecordBatchReaderBuilder::new_with_metadata(region, reader);
            for batch in builder
                .with_row_groups(vec![0])
                .with_batch_size(rows.max(1))
                .build()?
            {
                out.push(batch?);
            }
        }
        Ok(out)
    }

    /// The table whole.
    pub fn read_all(&self) -> Res<Vec<RecordBatch>> {
        let groups: Vec<usize> = self.spans.iter().map(|s| s.group).collect();
        self.read(&groups)
    }

    /// Only the batch numbers of every row (the `seq` column).
    pub fn seqs(&self) -> Res<Vec<u64>> {
        let file = File::open(&self.path)?;
        let reader = ArrowReaderMetadata::load(&file, self.options.clone())?;
        let descr = reader.metadata().file_metadata().schema_descr_ptr();
        let leaf = descr
            .columns()
            .iter()
            .position(|c| c.path().string() == "seq");
        let Some(leaf) = leaf else {
            return refuse("Unexpected schema: no seq");
        };
        let mask = parquet::arrow::ProjectionMask::leaves(&descr, [leaf]);
        let builder = ParquetRecordBatchReaderBuilder::new_with_metadata(file, reader);
        let reader = builder
            .with_projection(mask)
            .with_batch_size(1 << 16)
            .build()?;
        let mut out = Vec::with_capacity(self.spans.iter().map(|s| s.rows).sum());
        for batch in reader {
            let batch = batch?;
            let column = batch
                .column(0)
                .as_primitive_opt::<UInt64Type>()
                .ok_or_else(|| Failure::Refused("Unexpected schema: seq".into()))?;
            out.extend_from_slice(column.values());
        }
        Ok(out)
    }

    /// Every row's issue ID (the `issue_id` column alone), as the runs it comes in: (issue, rows). Rows are sorted by issue.
    pub fn issue_runs(&self) -> Res<Vec<(u64, u64)>> {
        let file = File::open(&self.path)?;
        let (region, reader, tail) = footer(&file, &self.options)?;
        let descr = reader.metadata().file_metadata().schema_descr_ptr();
        let leaf = descr
            .columns()
            .iter()
            .position(|c| c.path().string() == "issue_id");
        let Some(leaf) = leaf else {
            return refuse("Unexpected schema: no issue_id");
        };
        // The column's chunks, from every row group, in one read.
        let file_len = region.len;
        let mut parts = vec![(file_len - tail.len() as u64, tail)];
        for group in 0..reader.metadata().num_row_groups() {
            let (start, length) = reader.metadata().row_group(group).column(leaf).byte_range();
            parts.push((start, Bytes::from(read_at(&file, start, length as usize)?)));
        }
        let region = Region {
            len: file_len,
            parts: Arc::new(parts),
        };
        let mask = parquet::arrow::ProjectionMask::leaves(&descr, [leaf]);
        let builder = ParquetRecordBatchReaderBuilder::new_with_metadata(region, reader);
        let mut runs: Vec<(u64, u64)> = Vec::new();
        for batch in builder
            .with_projection(mask)
            .with_batch_size(1 << 16)
            .build()?
        {
            let batch = batch?;
            let column = batch
                .column(0)
                .as_primitive_opt::<UInt64Type>()
                .ok_or_else(|| Failure::Refused("Unexpected schema: issue_id".into()))?;
            for &issue in column.values().iter() {
                match runs.last_mut() {
                    Some((last, count)) if *last == issue => *count += 1,
                    _ => runs.push((issue, 1)),
                }
            }
        }
        Ok(runs)
    }

    pub fn rows(&self) -> usize {
        self.spans.iter().map(|s| s.rows).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_parts_file_names_a_path_a_line_however_long() {
        let paths: Vec<String> = (1..=20_000)
            .map(|n| format!(r"C:\bd\brisk\spool\segment-422k3fvs\parts\{n:06}"))
            .collect();
        let text = paths.join("\n");
        assert!(text.len() > 32_767, "longer than a Windows command line");
        let parts = listed(&text);
        assert_eq!(parts.len(), 20_000);
        assert_eq!(parts[0], PathBuf::from(&paths[0]));
        assert_eq!(parts[19_999], PathBuf::from(&paths[19_999]));
    }

    #[test]
    fn line_endings_blank_lines_and_a_byte_order_mark_are_not_parts() {
        let parts = listed("\u{feff}a/1\r\n\nb/2\r\n\r\nc/3\r");
        assert_eq!(
            parts,
            [
                PathBuf::from("a/1"),
                PathBuf::from("b/2"),
                PathBuf::from("c/3")
            ]
        );
        assert!(listed("").is_empty() && listed("\n\r\n").is_empty());
    }

    #[test]
    fn paths_keep_their_unicode() {
        assert_eq!(
            listed("/data/録画/parts/000001\n"),
            [PathBuf::from("/data/録画/parts/000001")]
        );
    }
}
