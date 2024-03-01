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

use crate::pruning_oracle::BucketInfo;

/// Represent known min/max values for a specific column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnRange {
    pub min_value: Option<Arc<ScalarValue>>,
    pub max_value: Option<Arc<ScalarValue>>,
    /// The server-side bucketing bucket ID of the column for this partition.
    ///
    /// None if this column or this table is not bucketed. e.g. the partition
    /// template of this table does not contain `TemplatePart::Bucket`.
    pub bucket_info: Option<BucketInfo>,
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

    let min_value = range
        .min_value
        .as_ref()
        .map(|min_value| Precision::Exact(min_value.as_ref().clone()))
        .unwrap_or(Precision::Absent);

    let max_value = range
        .max_value
        .as_ref()
        .map(|max_value| Precision::Exact(max_value.as_ref().clone()))
        .unwrap_or(Precision::Absent);

    (min_value, max_value)
}

/// Create chunk [statistics](Statistics).
pub fn create_chunk_statistics(
    row_count: Option<usize>,
    schema: &Schema,
    ts_min_max: Option<TimestampMinMax>,
    ranges: Option<&ColumnRanges>,
) -> Statistics {
    let mut columns = Vec::with_capacity(schema.len());

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

    Statistics {
        num_rows,
        total_byte_size: Precision::Absent,
        column_statistics: columns,
    }
}

#[cfg(test)]
mod tests {
    use schema::{InfluxFieldType, SchemaBuilder, TIME_COLUMN_NAME};

    use super::*;

    #[test]
    fn test_create_chunk_statistics_no_columns_no_rows() {
        let schema = SchemaBuilder::new().build().unwrap();
        let row_count = 0;

        let actual = create_chunk_statistics(Some(row_count), &schema, None, None);
        let expected = Statistics {
            num_rows: Precision::Exact(row_count),
            total_byte_size: Precision::Absent,
            column_statistics: vec![],
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_create_chunk_statistics_no_columns_null_rows() {
        let schema = SchemaBuilder::new().build().unwrap();

        let actual = create_chunk_statistics(None, &schema, None, None);
        let expected = Statistics {
            num_rows: Precision::Absent,
            total_byte_size: Precision::Absent,
            column_statistics: vec![],
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_create_chunk_statistics() {
        let schema = full_schema();
        let ts_min_max = TimestampMinMax { min: 10, max: 20 };
        let ranges = Arc::new(HashMap::from([
            (
                Arc::from("tag1"),
                ColumnRange {
                    min_value: Some(Arc::new(ScalarValue::from("aaa"))),
                    max_value: Some(Arc::new(ScalarValue::from("bbb"))),
                    bucket_info: None,
                },
            ),
            (
                Arc::from("tag3"), // does not exist in schema
                ColumnRange {
                    min_value: Some(Arc::new(ScalarValue::from("ccc"))),
                    max_value: Some(Arc::new(ScalarValue::from("ddd"))),
                    bucket_info: None,
                },
            ),
            (
                Arc::from("field_integer"),
                ColumnRange {
                    min_value: Some(Arc::new(ScalarValue::from(10i64))),
                    max_value: Some(Arc::new(ScalarValue::from(20i64))),
                    bucket_info: None,
                },
            ),
        ]));

        for row_count in [0usize, 1337usize] {
            let actual =
                create_chunk_statistics(Some(row_count), &schema, Some(ts_min_max), Some(&ranges));
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
            assert_eq!(actual, expected);
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
                min_value: Some(Arc::new(timestamptz_nano(12))),
                max_value: Some(Arc::new(timestamptz_nano(22))),
                bucket_info: None,
            },
        )]));

        let actual =
            create_chunk_statistics(Some(row_count), &schema, Some(ts_min_max), Some(&ranges));
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
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_create_chunk_statistics_ts_min_max_none_so_fallback_to_column_range() {
        let schema = full_schema();
        let row_count = 42usize;
        let ranges = Arc::new(HashMap::from([(
            Arc::from(TIME_COLUMN_NAME),
            ColumnRange {
                min_value: Some(Arc::new(timestamptz_nano(12))),
                max_value: Some(Arc::new(timestamptz_nano(22))),
                bucket_info: None,
            },
        )]));

        let actual = create_chunk_statistics(Some(row_count), &schema, None, Some(&ranges));
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
        assert_eq!(actual, expected);
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
