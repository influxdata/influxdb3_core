//! This module contains the definition of a "SeriesSet" a plan that when run
//! produces rows that can be logically divided into "Series"
//!
//! Specifically, a SeriesSet wraps a "table", and each table is
//! sorted on a set of "tag" columns, meaning the data the series
//! series will be contiguous.
//!
//! For example, the output columns of such a plan would be:
//! (tag col0) (tag col1) ... (tag colN) (field val1) (field val2) ... (field
//! valN) .. (timestamps)
//!
//! Note that the data will come out ordered by the tag keys (ORDER BY
//! (tag col0) (tag col1) ... (tag colN))
//!
//! NOTE: The InfluxDB classic storage engine not only returns
//! series sorted by the tag values, but the order of the tag columns
//! (and thus the actual sort order) is also lexographically
//! sorted. So for example, if you have `region`, `host`, and
//! `service` as tags, the columns would be ordered `host`, `region`,
//! and `service` as well.

pub mod converter;
pub mod series;

use arrow::{self, record_batch::RecordBatch};

use std::sync::Arc;

use super::field::FieldIndexes;

#[derive(Debug)]
/// Information to map a slice of rows in a [`RecordBatch`] sorted by
/// tags and timestamps to several timeseries that share the same
/// tag keys and timestamps.
///
/// The information in a [`SeriesSet`] can be used to "unpivot" a
/// [`RecordBatch`] into one or more Time Series as [`series::Series`]
///
/// For example, given the following set of rows from a [`RecordBatch`]
/// which must be sorted by `(TagA, TagB, time)`:
//
/// TagA | TagB | Field1 | Field2 | time
/// -----+------+--------+--------+-------
///   a  |  b   |  1     | 10     | 100
///   a  |  b   |  2     | 20     | 200
///   a  |  b   |  3     | 30     | 300
///   a  |  x   |  11    |        | 100
///   a  |  x   |  12    |        | 200
///
/// Would be represented as
/// * `SeriesSet` 1: For {TagA='a', TagB='b'}
/// * `SeriesSet` 2: For {TagA='a', TagB='x'}
///
/// `SeriesSet` 1 would produce 2 series (one for each field):
///
/// {_field=Field1, TagA=a, TagB=b} timestamps = {100, 200, 300} values = {1, 2, 3}
/// {_field=Field2, TagA=a, TagB=b} timestamps = {100, 200, 300} values = {100, 200, 300}
///
/// `SeriesSet` 2 would produce a single series for `Field1` (no
/// series is created for `Field2` because there are no values for
/// `Field2` where TagA=a, and TagB=x)
///
/// {_field=Field1, TagA=a, TagB=x} timestamps = {100, 200} values = {11, 12}
///
/// NB: The heavy use of `Arc` is to avoid many duplicated Strings given
/// the the fact that many SeriesSets share the same tag keys and
/// table name.
pub struct SeriesSet {
    /// The table name this series came from
    pub table_name: Arc<str>,

    /// key = value pairs that define this series
    pub tags: Vec<(Arc<str>, Arc<str>)>,

    /// the column index of each "field" of the time series. For
    /// example, if there are two field indexes then this series set
    /// would result in two distinct series being sent back, one for
    /// each field.
    pub field_indexes: FieldIndexes,

    // The row in the record batch where the data starts (inclusive)
    pub start_row: usize,

    // The number of rows in the record batch that the data goes to
    pub num_rows: usize,

    // The underlying record batch data
    pub batch: RecordBatch,
}

impl SeriesSet {
    // Amount of memory required to store this series set, in bytes.
    pub fn memory_size(&self) -> usize {
        std::mem::size_of::<Self>()
            + std::mem::size_of_val(self.field_indexes.as_slice())
            + self.batch.get_array_memory_size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use super::super::field::FieldIndex;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    #[test]
    fn size() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new("tag1", DataType::Utf8, false),
            Field::new("tag2", DataType::Utf8, false),
            Field::new("field1", DataType::Int64, false),
            Field::new("field2", DataType::Int64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow::array::Int64Array::from(vec![100, 200, 300])),
                Arc::new(arrow::array::StringArray::from(vec![
                    Some("a"),
                    Some("a"),
                    Some("a"),
                ])),
                Arc::new(arrow::array::StringArray::from(vec![
                    Some("b"),
                    Some("b"),
                    Some("b"),
                ])),
                Arc::new(arrow::array::Int64Array::from(vec![1, 2, 3])),
                Arc::new(arrow::array::Int64Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();
        let batch_size = batch.get_array_memory_size();

        let series_set = SeriesSet {
            table_name: Arc::from("table_name"),
            tags: vec![
                (Arc::from("tag1"), Arc::from("a")),
                (Arc::from("tag2"), Arc::from("b")),
            ],
            field_indexes: FieldIndexes::from_slice(&[(3, 0), (4, 0)]),
            start_row: 0,
            num_rows: 3,
            batch,
        };

        // The size of the series set should be the size of the struct
        // plus the size of the field indexes plus the size of the
        // underlying record batch.
        let expected_size =
            std::mem::size_of::<SeriesSet>() + std::mem::size_of::<FieldIndex>() * 2 + batch_size;

        assert_eq!(series_set.memory_size(), expected_size);
    }
}
