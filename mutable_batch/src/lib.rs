#![warn(missing_docs)]

//! A mutable data structure for a collection of writes.
//!
//! Can be viewed as a mutable version of [`RecordBatch`] that remains the exclusive
//! owner of its buffers, permitting mutability. The in-memory layout is similar, however,
//! permitting fast conversion to [`RecordBatch`].

// Workaround for "unused crate" lint false positives.
#[cfg(test)]
use partition as _;
#[cfg(test)]
use pretty_assertions as _;
#[cfg(test)]
use rand as _;
use workspace_hack as _;

use crate::column::{Column, ColumnData};
use arrow::record_batch::RecordBatch;
use data_types::StatValues;
use hashbrown::HashMap;
use iox_time::Time;
use schema::Projection;
use schema::{builder::SchemaBuilder, Schema, TIME_COLUMN_NAME};
use snafu::{OptionExt, ResultExt, Snafu};
use std::{collections::BTreeSet, ops::Range};

pub mod column;
pub mod payload;
pub mod writer;

pub use payload::*;

#[allow(missing_docs)]
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Column error on column {}: {}", column, source))]
    ColumnError {
        column: String,
        source: column::Error,
    },

    #[snafu(display("arrow conversion error: {}", source))]
    ArrowError { source: arrow::error::ArrowError },

    #[snafu(display("Internal error converting schema: {}", source))]
    InternalSchema { source: schema::builder::Error },

    #[snafu(display("Column not found: {}", column))]
    ColumnNotFound { column: String },

    #[snafu(context(false))]
    WriterError { source: writer::Error },
}

/// A specialized `Error` for [`MutableBatch`] errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Represents a mutable batch of rows (a horizontal subset of a table) which
/// can be appended to and converted into an Arrow `RecordBatch`
#[derive(Debug, Default, Clone)]
pub struct MutableBatch {
    /// Map of column name to index in `MutableBatch::columns`
    column_names: HashMap<String, usize>,

    /// Columns contained within this MutableBatch
    columns: Vec<Column>,

    /// The number of rows in this MutableBatch
    row_count: usize,
}

impl MutableBatch {
    /// Create a new empty batch
    pub fn new() -> Self {
        Self {
            column_names: Default::default(),
            columns: Default::default(),
            row_count: 0,
        }
    }

    /// Returns the schema for a given selection
    ///
    /// If Selection::All the returned columns are sorted by name
    pub fn schema(&self, selection: Projection<'_>) -> Result<Schema> {
        let mut schema_builder = SchemaBuilder::new();
        let schema = match selection {
            Projection::All => {
                for (column_name, column_idx) in &self.column_names {
                    let column = &self.columns[*column_idx];
                    schema_builder.influx_column(column_name, column.influx_type());
                }

                schema_builder
                    .build()
                    .context(InternalSchemaSnafu)?
                    .sort_fields_by_name()
            }
            Projection::Some(cols) => {
                for col in cols {
                    let column = self.column(col)?;
                    schema_builder.influx_column(*col, column.influx_type());
                }
                schema_builder.build().context(InternalSchemaSnafu)?
            }
        };

        Ok(schema)
    }

    /// Convert all the data in this `MutableBatch` into a `RecordBatch`
    pub fn to_arrow(&self, selection: Projection<'_>) -> Result<RecordBatch> {
        let schema = self.schema(selection)?;
        let columns = schema
            .iter()
            .map(|(_, field)| {
                let column = self
                    .column(field.name())
                    .expect("schema contains non-existent column");

                column.to_arrow().context(ColumnSnafu {
                    column: field.name(),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        RecordBatch::try_new(schema.into(), columns).context(ArrowSnafu {})
    }

    /// Returns an iterator over the columns in this batch in no particular order
    pub fn columns(&self) -> impl ExactSizeIterator<Item = (&String, &Column)> + '_ {
        self.column_names
            .iter()
            .map(move |(name, idx)| (name, &self.columns[*idx]))
    }

    /// Return the set of column names for this table. Used in combination with a write operation's
    /// column names to determine whether a write would exceed the max allowed columns.
    pub fn column_names(&self) -> BTreeSet<&str> {
        self.column_names.keys().map(|name| name.as_str()).collect()
    }

    /// Return the number of rows in this chunk
    pub fn rows(&self) -> usize {
        self.row_count
    }

    /// Returns a summary of the write timestamps in this chunk if a
    /// time column exists
    pub fn timestamp_summary(&self) -> Option<TimestampSummary> {
        let col_data = self.time_column().ok()?;
        let mut summary = TimestampSummary::default();

        for t in col_data {
            summary.record_nanos(*t)
        }

        Some(summary)
    }

    /// Extend this [`MutableBatch`] with the contents of `other`
    pub fn extend_from(&mut self, other: &Self) -> Result<()> {
        let mut writer = writer::Writer::new(self, other.row_count);
        writer.write_batch(other)?;
        writer.commit();
        Ok(())
    }

    /// Extend this [`MutableBatch`] with `range` rows from `other`
    pub fn extend_from_range(&mut self, other: &Self, range: Range<usize>) -> Result<()> {
        let mut writer = writer::Writer::new(self, range.end - range.start);
        writer.write_batch_range(other, range)?;
        writer.commit();
        Ok(())
    }

    /// Extend this [`MutableBatch`] with `ranges` rows from `other`
    pub fn extend_from_ranges(&mut self, other: &Self, ranges: &[Range<usize>]) -> Result<()> {
        let to_insert = ranges.iter().map(|x| x.end - x.start).sum();

        let mut writer = writer::Writer::new(self, to_insert);
        writer.write_batch_ranges(other, ranges)?;
        writer.commit();
        Ok(())
    }

    /// Returns a reference to the specified column
    pub fn column(&self, column: &str) -> Result<&Column> {
        let idx = self
            .column_names
            .get(column)
            .context(ColumnNotFoundSnafu { column })?;

        Ok(&self.columns[*idx])
    }

    /// Returns a reference to the column at the specified index
    pub fn column_by_index(&self, idx: usize) -> Result<&Column> {
        self.columns.get(idx).with_context(|| ColumnNotFoundSnafu {
            column: format!("index {}", idx),
        })
    }

    /// Return the values in the time column in this batch. Returns an error if the batch has no
    /// time column.
    ///
    /// # Panics
    ///
    /// If a time column exists but its data isn't of type `i64`, this function will panic.
    fn time_column(&self) -> Result<&[i64]> {
        let time_column = self.column(TIME_COLUMN_NAME)?;
        match &time_column.data {
            ColumnData::I64(col_data, _) => Ok(col_data),
            x => unreachable!("expected i64 got {} for time column", x),
        }
    }

    /// Return the approximate memory size of the batch, in bytes.
    ///
    /// This includes `Self`.
    pub fn size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self
                .column_names
                .iter()
                .map(|(k, v)| std::mem::size_of_val(k) + k.capacity() + std::mem::size_of_val(v))
                .sum::<usize>()
            + self.columns.iter().map(|c| c.size()).sum::<usize>()
    }

    /// Return the approximate memory size of the data in the batch, in bytes.
    pub fn size_data(&self) -> usize {
        self.columns.iter().map(|c| c.size_data()).sum::<usize>()
    }

    /// Split this [`MutableBatch`] at the specified row boundary, such that
    /// after this call, `self` contains the range of rows indexed from `[0, n)`
    /// and the returned value contains `[n, len)`.
    ///
    /// # Panics
    ///
    /// Panics if `n > self.rows()`.
    ///
    /// # Performance
    ///
    /// This implementation is heavily optimised towards splitting `self` at a
    /// `n` value skewed towards the high end of the row count - see [`Column`].
    pub fn split_off(&mut self, n: usize) -> Self {
        assert!(n <= self.row_count);

        let right_row_count = self.row_count - n;
        self.row_count = n;

        Self {
            column_names: self.column_names.clone(),
            columns: self.columns.iter_mut().map(|v| v.split_off(n)).collect(),
            row_count: right_row_count,
        }
    }
}

/// A description of the distribution of timestamps in a
/// set of writes, bucketed based on minute within the hour
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TimestampSummary {
    /// Stores the count of how many rows in the set of writes have a timestamp
    /// with a minute matching a given index
    ///
    /// E.g. a row with timestamp 12:31:12 would store a count at index 31
    pub counts: [u32; 60],

    /// Standard timestamp statistics
    pub stats: StatValues<i64>,
}

impl Default for TimestampSummary {
    fn default() -> Self {
        Self {
            counts: [0; 60],
            stats: Default::default(),
        }
    }
}

impl TimestampSummary {
    /// Records a timestamp value
    pub fn record(&mut self, timestamp: Time) {
        self.counts[timestamp.minute() as usize] += 1;
        self.stats.update(&timestamp.timestamp_nanos())
    }

    /// Records a timestamp value from nanos
    pub fn record_nanos(&mut self, timestamp_nanos: i64) {
        self.record(Time::from_timestamp_nanos(timestamp_nanos))
    }
}

#[cfg(test)]
mod tests {
    use arrow_util::assert_batches_eq;
    use mutable_batch_lp::lines_to_batches;
    use schema::Projection;

    #[test]
    fn size_data_without_nulls() {
        let batches = lines_to_batches(
            "cpu,t1=hello,t2=world f1=1.1,f2=1i 1234\ncpu,t1=h,t2=w f1=2.2,f2=2i 1234",
            0,
        )
        .unwrap();
        let batch = batches.get("cpu").unwrap();

        assert_eq!(batch.size_data(), 128);
        assert_eq!(batch.columns().len(), 5);

        let batches = lines_to_batches(
            "cpu,t1=hellomore,t2=world f1=1.1,f2=1i 1234\ncpu,t1=h,t2=w f1=2.2,f2=2i 1234",
            0,
        )
        .unwrap();
        let batch = batches.get("cpu").unwrap();
        assert_eq!(batch.size_data(), 138);
        assert_eq!(batch.columns().len(), 5);
    }

    #[test]
    fn size_data_with_nulls() {
        let batches = lines_to_batches(
            "cpu,t1=hello,t2=world f1=1.1 1234\ncpu,t2=w f1=2.2,f2=2i 1234",
            0,
        )
        .unwrap();
        let batch = batches.get("cpu").unwrap();

        assert_eq!(batch.size_data(), 124);
        assert_eq!(batch.columns().len(), 5);
    }

    /// Assert the correct row index is split off using
    /// [`MutableBatch::split_off()`].
    ///
    /// Correctness of the [`Column`] splitting is handled by tests against the
    /// [`Column`] itself.
    #[test]
    fn test_split_off() {
        let mut batches = lines_to_batches(
            "\
            cpu,t1=hello,t2=world f1=1.1 1234\n\
            cpu,t2=w f1=2.2,f2=2i 1234\n\
            ",
            0,
        )
        .unwrap();
        let mut batch = batches.remove("cpu").unwrap();
        assert_eq!(batch.rows(), 2);
        assert_eq!(batch.column_names().len(), 5);

        let got = batch.split_off(1);

        assert_batches_eq!(
            &[
                "+-----+----+-------+-------+--------------------------------+",
                "| f1  | f2 | t1    | t2    | time                           |",
                "+-----+----+-------+-------+--------------------------------+",
                "| 1.1 |    | hello | world | 1970-01-01T00:00:00.000001234Z |",
                "+-----+----+-------+-------+--------------------------------+",
            ],
            &[batch.to_arrow(Projection::All).unwrap()]
        );
        assert_batches_eq!(
            &[
                "+-----+----+----+----+--------------------------------+",
                "| f1  | f2 | t1 | t2 | time                           |",
                "+-----+----+----+----+--------------------------------+",
                "| 2.2 | 2  |    | w  | 1970-01-01T00:00:00.000001234Z |",
                "+-----+----+----+----+--------------------------------+",
            ],
            &[got.to_arrow(Projection::All).unwrap()]
        );

        assert_eq!(batch.rows(), 1);
        assert_eq!(got.rows(), 1);

        // Actual Column instances
        assert_eq!(got.columns().len(), batch.columns().len());

        // Column name map
        assert_eq!(got.column_names().len(), 5);
        assert_eq!(got.column_names(), batch.column_names());
        assert_eq!(got.column_names().len(), got.columns().len());

        // Schema
        assert_eq!(
            got.schema(Projection::All).unwrap(),
            batch.schema(Projection::All).unwrap()
        );
        assert_eq!(
            got.schema(Projection::All).unwrap().len(),
            got.columns().len()
        );
    }

    #[test]
    fn test_split_off_n_0() {
        let mut batches = lines_to_batches(
            "\
            cpu,t1=hello,t2=world f1=1.1 1234\n\
            cpu,t2=w f1=2.2,f2=2i 1234\n\
            ",
            0,
        )
        .unwrap();
        let mut batch = batches.remove("cpu").unwrap();
        assert_eq!(batch.rows(), 2);
        assert_eq!(batch.column_names().len(), 5);

        let got = batch.split_off(0);

        assert_batches_eq!(
            &[
                "+-----+----+-------+-------+--------------------------------+",
                "| f1  | f2 | t1    | t2    | time                           |",
                "+-----+----+-------+-------+--------------------------------+",
                "| 1.1 |    | hello | world | 1970-01-01T00:00:00.000001234Z |",
                "| 2.2 | 2  |       | w     | 1970-01-01T00:00:00.000001234Z |",
                "+-----+----+-------+-------+--------------------------------+",
            ],
            &[got.to_arrow(Projection::All).unwrap()]
        );

        assert_eq!(batch.rows(), 0);
        assert_eq!(got.rows(), 2);

        // Actual Column instances
        assert_eq!(got.columns().len(), batch.columns().len());

        // Column name map
        assert_eq!(got.column_names().len(), 5);
        assert_eq!(got.column_names(), batch.column_names());
        assert_eq!(got.column_names().len(), got.columns().len());

        // Schema
        assert_eq!(
            got.schema(Projection::All).unwrap(),
            batch.schema(Projection::All).unwrap()
        );
        assert_eq!(
            got.schema(Projection::All).unwrap().len(),
            got.columns().len()
        );
    }

    #[test]
    fn test_split_off_none() {
        let mut batches = lines_to_batches(
            "\
            cpu,t1=hello,t2=world f1=1.1 1234\n\
            cpu,t2=w f1=2.2,f2=2i 1234\n\
            ",
            0,
        )
        .unwrap();
        let mut batch = batches.remove("cpu").unwrap();
        assert_eq!(batch.rows(), 2);
        assert_eq!(batch.column_names().len(), 5);

        let got = batch.split_off(2);

        assert_batches_eq!(
            &[
                "+-----+----+-------+-------+--------------------------------+",
                "| f1  | f2 | t1    | t2    | time                           |",
                "+-----+----+-------+-------+--------------------------------+",
                "| 1.1 |    | hello | world | 1970-01-01T00:00:00.000001234Z |",
                "| 2.2 | 2  |       | w     | 1970-01-01T00:00:00.000001234Z |",
                "+-----+----+-------+-------+--------------------------------+",
            ],
            &[batch.to_arrow(Projection::All).unwrap()]
        );

        assert_eq!(batch.rows(), 2);
        assert_eq!(got.rows(), 0);

        // Actual Column instances
        assert_eq!(got.columns().len(), batch.columns().len());

        // Column name map
        assert_eq!(got.column_names().len(), 5);
        assert_eq!(got.column_names(), batch.column_names());
        assert_eq!(got.column_names().len(), got.columns().len());

        // Schema
        assert_eq!(
            got.schema(Projection::All).unwrap(),
            batch.schema(Projection::All).unwrap()
        );
        assert_eq!(
            got.schema(Projection::All).unwrap().len(),
            got.columns().len()
        );
    }
}

/// Test helpers for randomised testing.
#[cfg(any(test, feature = "arbitrary"))]
pub mod arbitrary {
    use proptest::prelude::*;

    use crate::{
        column::{arbitrary::arbitrary_column, Column},
        MutableBatch,
    };

    /// Deterministically generate a name for `c` derived from the column type.
    fn column_name(c: &Column) -> String {
        format!("col-{}", c.influx_type())
    }

    /// Instantiate a [`MutableBatch`] containing random columns and data.
    ///
    /// The batch will contain at most one column of each data type.
    pub fn arbitrary_mutable_batch() -> impl Strategy<Value = MutableBatch> {
        prop::collection::vec(arbitrary_column(), 0..20).prop_map(|v| {
            v.into_iter().fold(MutableBatch::new(), |mut acc, v| {
                // Convert this column into a single-column batch, and then
                // merge it into the accumulator.
                //
                // This allows us to build batches that contain null ranges,
                // correct batch statistics, etc.
                let c = MutableBatch {
                    column_names: [(column_name(&v), 0)].into_iter().collect(),
                    row_count: v.len(),
                    columns: vec![v],
                };

                acc.extend_from(&c).expect("must merge batch");
                acc
            })
        })
    }
}
