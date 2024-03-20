//! Tools to set up DataFusion statistics.

use std::{collections::HashMap, sync::Arc};

use data_types::TimestampMinMax;
use datafusion::common::stats::Precision;
use datafusion::{
    physical_plan::{ColumnStatistics, Statistics},
    scalar::ScalarValue,
};
use datafusion_util::{option_to_precision, timestamptz_nano};
use schema::{InfluxColumnType, Schema};

use crate::pruning_oracle::{
    BucketInfo, BucketPartitionPruningOracle, BucketPartitionPruningOracleBuilder,
};

/// Represent known min/max values for a specific column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnRange {
    pub min_value: Arc<Precision<ScalarValue>>,
    pub max_value: Arc<Precision<ScalarValue>>,
    /// The server-side bucketing bucket ID of the column for this partition.
    ///
    /// None if this column or this table is not bucketed. e.g. the partition
    /// template of this table does not contain `TemplatePart::Bucket`.
    pub bucket_info: Option<Arc<BucketInfo>>,
}

impl ColumnRange {
    pub fn size(&self) -> usize {
        std::mem::size_of::<Self>()
            + size_of_precision(&self.min_value)
            + size_of_precision(&self.max_value)
            + self
                .bucket_info
                .as_ref()
                .map_or(0, |_| std::mem::size_of::<BucketInfo>())
    }
}

/// Calculate the size of `datafusion::common::stats::Precision<Scalar>` in bytes.
fn size_of_precision(v: &Precision<ScalarValue>) -> usize {
    let size_of_value = std::mem::size_of_val(v);

    // account avoid double counting size of scalar
    let size_of_precision = size_of_value - std::mem::size_of::<ScalarValue>();

    match v {
        Precision::Exact(scalar) => size_of_precision + scalar.size(),
        Precision::Inexact(scalar) => size_of_precision + scalar.size(),
        Precision::Absent => size_of_value,
    }
}

/// Represents the known min/max values for a subset (not all) of the columns in a partition.
///
/// The values may not actually in any row.
///
/// These ranges apply to ALL rows (esp. in ALL files and ingester chunks) within in given partition.
pub type ColumnRanges = Arc<HashMap<Arc<str>, ColumnRange>>;

/// Returns the min/max values for the range, if present
fn range_to_min_max_stats(
    range: Option<&ColumnRange>,
) -> (Precision<ScalarValue>, Precision<ScalarValue>) {
    let Some(range) = range else {
        return (Precision::Absent, Precision::Absent);
    };

    let min_value = range.min_value.as_ref().clone();

    let max_value = range.max_value.as_ref().clone();

    (min_value, max_value)
}

/// A container type to bundle the statistics and the server-side bucketing information
/// for a chunk.
///
/// This is a workaround on the limitation of DataFusion's `Statistics` type, which does not
/// has the entry to store the server-side bucketing information.
#[derive(Debug)]
pub struct ChunkStatistics {
    statistics: Arc<Statistics>,
    bucket_pruning_oracle: Option<Arc<BucketPartitionPruningOracle>>,
}

impl ChunkStatistics {
    pub fn new(
        statistics: Arc<Statistics>,
        bucket_pruning_oracle: Option<Arc<BucketPartitionPruningOracle>>,
    ) -> Self {
        Self {
            statistics,
            bucket_pruning_oracle,
        }
    }

    pub fn statistics(&self) -> Arc<Statistics> {
        Arc::clone(&self.statistics)
    }

    pub fn bucket_pruning_oracle(&self) -> Option<Arc<BucketPartitionPruningOracle>> {
        self.bucket_pruning_oracle.as_ref().map(Arc::clone)
    }
}

/// Create chunk [statistics](Statistics).
pub fn create_chunk_statistics(
    row_count: Option<usize>,
    schema: &Schema,
    ts_min_max: Option<TimestampMinMax>,
    ranges: Option<&ColumnRanges>,
) -> ChunkStatistics {
    let mut columns = Vec::with_capacity(schema.len());
    let mut pruning_oracle_builder = BucketPartitionPruningOracleBuilder::default();

    for (t, field) in schema.iter() {
        let stats = match t {
            InfluxColumnType::Timestamp => {
                // prefer explicitely given time range but fall back to column ranges
                let (min_value, max_value) = match ts_min_max {
                    Some(ts_min_max) => (
                        Precision::Exact(timestamptz_nano(ts_min_max.min)),
                        Precision::Exact(timestamptz_nano(ts_min_max.max)),
                    ),
                    None => {
                        let range =
                            ranges.and_then(|ranges| ranges.get::<str>(field.name().as_ref()));

                        range_to_min_max_stats(range)
                    }
                };
                ColumnStatistics {
                    null_count: Precision::Exact(0),
                    min_value,
                    max_value,
                    distinct_count: Precision::Absent,
                }
            }
            _ => {
                let range = ranges.and_then(|ranges| ranges.get::<str>(field.name().as_ref()));

                let (min_value, max_value) = range_to_min_max_stats(range);

                // If the column is a tag, and there is any server-side bucketing information,
                // add the bucket info to the pruning oracle builder.
                if t == InfluxColumnType::Tag {
                    if let Some(bucket_info) = range.and_then(|range| range.bucket_info.clone()) {
                        pruning_oracle_builder
                            .insert(Arc::from(field.name().to_string()), *bucket_info.as_ref());
                    }
                }

                ColumnStatistics {
                    null_count: Precision::Absent,
                    min_value,
                    max_value,
                    distinct_count: Precision::Absent,
                }
            }
        };
        columns.push(stats)
    }

    let num_rows = option_to_precision(row_count);

    // If there is no server-side bucketing information,
    // don't include it in the statistics, i.e. None.
    let bucket_pruning_oracle = if pruning_oracle_builder.is_empty() {
        None
    } else {
        Some(Arc::new(pruning_oracle_builder.build(None)))
    };

    ChunkStatistics::new(
        Arc::new(Statistics {
            num_rows,
            total_byte_size: Precision::Absent,
            column_statistics: columns,
        }),
        bucket_pruning_oracle,
    )
}

#[cfg(test)]
mod tests {
    use schema::{InfluxFieldType, SchemaBuilder, TIME_COLUMN_NAME};

    use super::*;

    #[test]
    fn test_create_chunk_statistics_no_columns_no_rows() {
        let schema = SchemaBuilder::new().build().unwrap();
        let row_count = 0;

        let actual = create_chunk_statistics(Some(row_count), &schema, None, None).statistics();
        let expected = Statistics {
            num_rows: Precision::Exact(row_count),
            total_byte_size: Precision::Absent,
            column_statistics: vec![],
        };
        assert_eq!(actual, Arc::new(expected));
    }

    #[test]
    fn test_create_chunk_statistics_no_columns_null_rows() {
        let schema = SchemaBuilder::new().build().unwrap();

        let actual = create_chunk_statistics(None, &schema, None, None).statistics();
        let expected = Statistics {
            num_rows: Precision::Absent,
            total_byte_size: Precision::Absent,
            column_statistics: vec![],
        };
        assert_eq!(actual, Arc::new(expected));
    }

    #[test]
    fn test_create_chunk_statistics() {
        let schema = full_schema();
        let ts_min_max = TimestampMinMax { min: 10, max: 20 };
        let ranges = Arc::new(HashMap::from([
            (
                Arc::from("tag1"),
                ColumnRange {
                    min_value: Arc::new(Precision::Exact(ScalarValue::from("aaa"))),
                    max_value: Arc::new(Precision::Exact(ScalarValue::from("bbb"))),
                    bucket_info: None,
                },
            ),
            (
                Arc::from("tag3"), // does not exist in schema
                ColumnRange {
                    min_value: Arc::new(Precision::Exact(ScalarValue::from("ccc"))),
                    max_value: Arc::new(Precision::Exact(ScalarValue::from("ddd"))),
                    bucket_info: None,
                },
            ),
            (
                Arc::from("field_integer"),
                ColumnRange {
                    min_value: Arc::new(Precision::Exact(ScalarValue::from(10i64))),
                    max_value: Arc::new(Precision::Exact(ScalarValue::from(20i64))),
                    bucket_info: None,
                },
            ),
        ]));

        for row_count in [0usize, 1337usize] {
            let actual =
                create_chunk_statistics(Some(row_count), &schema, Some(ts_min_max), Some(&ranges))
                    .statistics();
            let expected = Statistics {
                num_rows: Precision::Exact(row_count),
                total_byte_size: Precision::Absent,
                column_statistics: vec![
                    // tag1
                    ColumnStatistics {
                        null_count: Precision::Absent,
                        min_value: Precision::Exact(ScalarValue::from("aaa")),
                        max_value: Precision::Exact(ScalarValue::from("bbb")),
                        distinct_count: Precision::Absent,
                    },
                    // tag2
                    ColumnStatistics::default(),
                    // field_bool
                    ColumnStatistics::default(),
                    // field_float
                    ColumnStatistics::default(),
                    // field_integer
                    ColumnStatistics {
                        null_count: Precision::Absent,
                        min_value: Precision::Exact(ScalarValue::from(10i64)),
                        max_value: Precision::Exact(ScalarValue::from(20i64)),
                        distinct_count: Precision::Absent,
                    },
                    // field_string
                    ColumnStatistics::default(),
                    // field_uinteger
                    ColumnStatistics::default(),
                    // time
                    ColumnStatistics {
                        null_count: Precision::Exact(0),
                        min_value: Precision::Exact(timestamptz_nano(10)),
                        max_value: Precision::Exact(timestamptz_nano(20)),
                        distinct_count: Precision::Absent,
                    },
                ],
            };
            assert_eq!(actual, Arc::new(expected));
        }
    }

    #[test]
    fn test_create_chunk_statistics_ts_min_max_overrides_column_range() {
        let schema = full_schema();
        let row_count = 42usize;
        let ts_min_max = TimestampMinMax { min: 10, max: 20 };
        let ranges = Arc::new(HashMap::from([(
            Arc::from(TIME_COLUMN_NAME),
            ColumnRange {
                min_value: Arc::new(Precision::Exact(timestamptz_nano(12))),
                max_value: Arc::new(Precision::Exact(timestamptz_nano(22))),
                bucket_info: None,
            },
        )]));

        let actual =
            create_chunk_statistics(Some(row_count), &schema, Some(ts_min_max), Some(&ranges))
                .statistics();
        let expected = Statistics {
            num_rows: Precision::Exact(row_count),
            total_byte_size: Precision::Absent,
            column_statistics: vec![
                ColumnStatistics::default(),
                ColumnStatistics::default(),
                ColumnStatistics::default(),
                ColumnStatistics::default(),
                ColumnStatistics::default(),
                ColumnStatistics::default(),
                ColumnStatistics::default(),
                ColumnStatistics {
                    null_count: Precision::Exact(0),
                    min_value: Precision::Exact(timestamptz_nano(10)),
                    max_value: Precision::Exact(timestamptz_nano(20)),
                    distinct_count: Precision::Absent,
                },
            ],
        };
        assert_eq!(actual, Arc::new(expected));
    }

    #[test]
    fn test_create_chunk_statistics_ts_min_max_none_so_fallback_to_column_range() {
        let schema = full_schema();
        let row_count = 42usize;
        let ranges = Arc::new(HashMap::from([(
            Arc::from(TIME_COLUMN_NAME),
            ColumnRange {
                min_value: Arc::new(Precision::Exact(timestamptz_nano(12))),
                max_value: Arc::new(Precision::Exact(timestamptz_nano(22))),
                bucket_info: None,
            },
        )]));

        let actual =
            create_chunk_statistics(Some(row_count), &schema, None, Some(&ranges)).statistics();
        let expected = Statistics {
            num_rows: Precision::Exact(row_count),
            total_byte_size: Precision::Absent,
            column_statistics: vec![
                ColumnStatistics::default(),
                ColumnStatistics::default(),
                ColumnStatistics::default(),
                ColumnStatistics::default(),
                ColumnStatistics::default(),
                ColumnStatistics::default(),
                ColumnStatistics::default(),
                ColumnStatistics {
                    null_count: Precision::Exact(0),
                    min_value: Precision::Exact(timestamptz_nano(12)),
                    max_value: Precision::Exact(timestamptz_nano(22)),
                    distinct_count: Precision::Absent,
                },
            ],
        };
        assert_eq!(actual, Arc::new(expected));
    }

    fn full_schema() -> Schema {
        SchemaBuilder::new()
            .tag("tag1")
            .tag("tag2")
            .influx_field("field_bool", InfluxFieldType::Boolean)
            .influx_field("field_float", InfluxFieldType::Float)
            .influx_field("field_integer", InfluxFieldType::Integer)
            .influx_field("field_string", InfluxFieldType::String)
            .influx_field("field_uinteger", InfluxFieldType::UInteger)
            .timestamp()
            .build()
            .unwrap()
    }
}
