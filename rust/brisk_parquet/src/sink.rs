//! A table written a row group at a time, the row groups encoded on a pool of threads and appended in order: rows
//! come in batches of any size and leave in full row groups (the last shorter), as the archive requires.
use std::collections::VecDeque;
use std::fs::File;
use std::io::BufWriter;
use std::path::Path;
use std::sync::Arc;
use std::sync::mpsc::{Receiver, channel};

use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_writer::{ArrowColumnChunk, ArrowRowGroupWriterFactory, compute_leaves};
use parquet::column::writer::ColumnCloseResult;
use parquet::data_type::{ByteArray, FixedLenByteArray};
use parquet::file::properties::WriterProperties;
use parquet::file::statistics::{Statistics, ValueStatistics};
use parquet::file::writer::SerializedFileWriter;

use crate::error::{Failure, Res};
use crate::writer;

/// What is done to a row group's rows before it is encoded (on a pool thread), and what is counted of them.
pub struct Stage {
    pub transform: Box<dyn Fn(RecordBatch) -> Res<RecordBatch> + Send + Sync>,
    pub measure: Box<dyn Fn(&RecordBatch) -> u64 + Send + Sync>,
}

impl Stage {
    pub fn plain() -> Stage {
        Stage {
            transform: Box::new(Ok),
            measure: Box::new(|_| 0),
        }
    }
}

struct Encoded {
    chunks: Vec<ArrowColumnChunk>,
    measured: u64,
}

pub struct Sink {
    file: SerializedFileWriter<BufWriter<File>>,
    factory: Arc<ArrowRowGroupWriterFactory>,
    schema: SchemaRef,
    pool: Arc<rayon::ThreadPool>,
    stage: Arc<Stage>,
    row_group: usize,
    in_flight: VecDeque<Receiver<Res<Encoded>>>,
    limit: usize,
    carry: VecDeque<RecordBatch>,
    carried: usize,
    index: usize,
    /// Rows written so far, and the sum of what the stage measured of them.
    pub rows: u64,
    pub measured: u64,
}

fn fault<E: Into<anyhow::Error>>(error: E) -> Failure {
    Failure::Fault(error.into())
}

impl Sink {
    /// A sink for the table `name` of schema `schema` (the table's own, but for a depths table with book changes).
    pub fn create(
        directory: &Path,
        name: &str,
        schema: Schema,
        row_group: usize,
        pool: Arc<rayon::ThreadPool>,
        stage: Stage,
    ) -> Res<Sink> {
        Sink::create_with(
            directory,
            name,
            schema,
            writer::properties(name, row_group),
            row_group,
            pool,
            stage,
        )
    }

    /// A sink with its own writer properties (`row_group` rows to a row group, unless rows are given by `push_group`).
    pub fn create_with(
        directory: &Path,
        name: &str,
        schema: Schema,
        properties: WriterProperties,
        row_group: usize,
        pool: Arc<rayon::ThreadPool>,
        stage: Stage,
    ) -> Res<Sink> {
        let path = directory.join(name);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let schema: SchemaRef = Arc::new(schema);
        let file = BufWriter::with_capacity(1 << 20, File::create(path)?);
        let writer = ArrowWriter::try_new(file, schema.clone(), Some(properties)).map_err(fault)?;
        let (file, factory) = writer.into_serialized_writer().map_err(fault)?;
        // Each job in flight holds its rows twice (the pieces it was cut from, and its own copy): as many as there are threads.
        let limit = pool.current_num_threads().max(2);
        Ok(Sink {
            file,
            factory: Arc::new(factory),
            schema,
            pool,
            stage: Arc::new(stage),
            row_group,
            in_flight: VecDeque::new(),
            limit,
            carry: VecDeque::new(),
            carried: 0,
            index: 0,
            rows: 0,
            measured: 0,
        })
    }

    /// Rows to be written, in order.
    pub fn push(&mut self, batch: RecordBatch) -> Res<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        self.rows += batch.num_rows() as u64;
        self.carried += batch.num_rows();
        self.carry.push_back(batch);
        while self.carried >= self.row_group {
            let mut slices = Vec::new();
            let mut need = self.row_group;
            while need > 0 {
                let front = self.carry.pop_front().expect("rows carried");
                if front.num_rows() <= need {
                    need -= front.num_rows();
                    slices.push(front);
                } else {
                    slices.push(front.slice(0, need));
                    self.carry
                        .push_front(front.slice(need, front.num_rows() - need));
                    need = 0;
                }
            }
            self.carried -= self.row_group;
            self.submit(slices)?;
        }
        Ok(())
    }

    /// These rows as a row group of their own, whatever their number.
    pub fn push_group(&mut self, batch: RecordBatch) -> Res<()> {
        if batch.num_rows() > 0 {
            self.rows += batch.num_rows() as u64;
            self.submit(vec![batch])?;
        }
        Ok(())
    }

    fn submit(&mut self, slices: Vec<RecordBatch>) -> Res<()> {
        let (sender, receiver) = channel();
        let (factory, schema, stage, index) = (
            self.factory.clone(),
            self.schema.clone(),
            self.stage.clone(),
            self.index,
        );
        self.index += 1;
        self.pool.spawn(move || {
            let _ = sender.send(encode(&factory, &schema, &stage, index, &slices));
        });
        self.in_flight.push_back(receiver);
        while self.in_flight.len() > self.limit {
            self.append_next()?;
        }
        Ok(())
    }

    fn append_next(&mut self) -> Res<()> {
        let Some(receiver) = self.in_flight.pop_front() else {
            return Ok(());
        };
        let encoded = receiver.recv().map_err(fault)??;
        self.measured += encoded.measured;
        let mut group = self.file.next_row_group().map_err(fault)?;
        for chunk in encoded.chunks {
            chunk.append_to_row_group(&mut group).map_err(fault)?;
        }
        group.close().map_err(fault)?;
        Ok(())
    }

    /// The rows left over are the last row group; then the footer. Returns the rows written and the sum of what the stage
    /// measured of them.
    pub fn finish(mut self) -> Res<(u64, u64)> {
        if self.carried > 0 {
            let slices: Vec<RecordBatch> = self.carry.drain(..).collect();
            self.submit(slices)?;
        }
        while !self.in_flight.is_empty() {
            self.append_next()?;
        }
        self.file.close().map_err(fault)?;
        Ok((self.rows, self.measured))
    }
}

fn encode(
    factory: &ArrowRowGroupWriterFactory,
    schema: &SchemaRef,
    stage: &Stage,
    index: usize,
    slices: &[RecordBatch],
) -> Res<Encoded> {
    let batch = if slices.len() == 1 {
        slices[0].clone()
    } else {
        arrow_select_concat(schema, slices)?
    };
    let batch = (stage.transform)(batch)?;
    let measured = (stage.measure)(&batch);
    let mut writers = factory.create_column_writers(index).map_err(fault)?;
    let mut next = writers.iter_mut();
    for (field, column) in schema.fields().iter().zip(batch.columns()) {
        for leaf in compute_leaves(field, column).map_err(fault)? {
            next.next()
                .expect("a writer for every leaf")
                .write(&leaf)
                .map_err(fault)?;
        }
    }
    let mut chunks = Vec::with_capacity(writers.len());
    for writer in writers {
        let mut chunk = writer.close().map_err(fault)?;
        own_statistics(chunk.close_mut());
        chunks.push(chunk);
    }
    Ok(Encoded { chunks, measured })
}

/// A column chunk's min and max of byte values (the decimals and strings) point into the very arrays the rows came from, so that
/// every row group's footer entry would keep its rows in memory until the file was closed: copy them out.
fn own_statistics(close: &mut ColumnCloseResult) {
    let own = |bytes: &ByteArray| ByteArray::from(bytes.data().to_vec());
    let owned = match close.metadata.statistics() {
        Some(Statistics::ByteArray(v)) => Statistics::ByteArray(
            ValueStatistics::new(
                v.min_opt().map(own),
                v.max_opt().map(own),
                v.distinct_count(),
                v.null_count_opt(),
                false,
            )
            .with_min_is_exact(v.min_is_exact())
            .with_max_is_exact(v.max_is_exact()),
        ),
        Some(Statistics::FixedLenByteArray(v)) => {
            let own = |bytes: &FixedLenByteArray| {
                FixedLenByteArray::from(ByteArray::from(bytes.data().to_vec()))
            };
            Statistics::FixedLenByteArray(
                ValueStatistics::new(
                    v.min_opt().map(own),
                    v.max_opt().map(own),
                    v.distinct_count(),
                    v.null_count_opt(),
                    false,
                )
                .with_min_is_exact(v.min_is_exact())
                .with_max_is_exact(v.max_is_exact()),
            )
        }
        _ => return,
    };
    close.metadata = close
        .metadata
        .clone()
        .into_builder()
        .set_statistics(owned)
        .build()
        .expect("metadata with owned statistics");
}

fn arrow_select_concat(schema: &SchemaRef, slices: &[RecordBatch]) -> Res<RecordBatch> {
    arrow_select::concat::concat_batches(schema, slices).map_err(Failure::from)
}
