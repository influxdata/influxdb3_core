use arrow::datatypes::{DataType, Field, Schema, SchemaRef, UnionMode};
use datafusion::common::{Column, DFSchema, DFSchemaRef};
use datafusion::logical_expr::{lit, Expr, SortExpr};
use datafusion::scalar::ScalarValue;
use generated_types::influxdata::platform::storage::read_response::DataType as StorageDataType;
use schema::TIME_DATA_TYPE;
use std::collections::BTreeSet;
use std::iter::once;
use std::sync::Arc;
use std::sync::LazyLock;

use super::{FieldExt, SeriesColumnType};

/// The field definition for the `_measurement` field. This field is added
/// to the schema when pivoting the data for use in the "influxrpc" protocol.
static MEASUREMENT_FIELD: LazyLock<Arc<Field>> = LazyLock::new(|| {
    Arc::new(
        Field::new_dictionary("_measurement", DataType::Int32, DataType::Utf8, false)
            .with_series_column_type(SeriesColumnType::Measurement),
    )
});

/// The field definition for the `_field` field. This field is added to the
/// schema when pivoting the data for use in the "influxrpc" protocol.
static FIELD_FIELD: LazyLock<Arc<Field>> = LazyLock::new(|| {
    Arc::new(
        Field::new_dictionary("_field", DataType::Int32, DataType::Utf8, false)
            .with_series_column_type(SeriesColumnType::Field),
    )
});

/// The field definition for the `_time` field. This field will be the "time"
/// field in the pivoted schema.
static TIME_FIELD: LazyLock<Arc<Field>> = LazyLock::new(|| {
    Arc::new(
        Field::new("_time", TIME_DATA_TYPE(), false)
            .with_series_column_type(SeriesColumnType::Timestamp),
    )
});

/// The field definition for the `_value` field. This field will be the "value"
/// field in the pivoted schema.
static VALUE_FIELD: LazyLock<Arc<Field>> = LazyLock::new(|| {
    Arc::new(
        Field::new_union(
            "_value",
            vec![
                StorageDataType::Float as i8,
                StorageDataType::Integer as i8,
                StorageDataType::Unsigned as i8,
                StorageDataType::Boolean as i8,
                StorageDataType::String as i8,
            ],
            vec![
                Field::new("float", DataType::Float64, true),
                Field::new("integer", DataType::Int64, true),
                Field::new("unsigned", DataType::UInt64, true),
                Field::new("boolean", DataType::Boolean, true),
                Field::new("string", DataType::Utf8, true),
            ],
            UnionMode::Sparse,
        )
        .with_series_column_type(SeriesColumnType::Value),
    )
});

/// Schema for a data that has been pivoted into a series representation.
/// The pivoted schema uses a determininstic schema for all fields in the
/// measurements. The schema has this general form:
///
///  - `_measurement` (tag)
///  - ... tag fields
///  - `_field` (tag)
///  - `_time` (timestamp)
///  - `_value` (union)
///
/// The tags fields are always ordered lexically.
#[derive(Debug, Clone)]
pub(crate) struct SeriesSchema {
    inner: SchemaRef,
}

impl SeriesSchema {
    /// Create a new series schema with the provided tags.
    /// The tags must have had their series column type set.
    fn new(tags: impl IntoIterator<Item = Arc<Field>>) -> Self {
        let fields = once(Arc::clone(&*MEASUREMENT_FIELD))
            .chain(tags)
            .chain(once(Arc::clone(&*FIELD_FIELD)))
            .chain(once(Arc::clone(&*TIME_FIELD)))
            .chain(once(Arc::clone(&*VALUE_FIELD)))
            .collect::<Vec<_>>();
        Self {
            inner: Arc::new(Schema::new(fields)),
        }
    }

    pub(crate) fn new_tags(tags: impl IntoIterator<Item = Field>) -> Self {
        Self::new(
            tags.into_iter()
                .map(|f| Arc::new(f.with_series_column_type(SeriesColumnType::Tag))),
        )
    }

    /// Create the schema for an empty series.
    pub(crate) fn empty() -> Self {
        Self::new(vec![])
    }

    /// Merge two series schemata together. This will combine the
    /// tags columns and ensure they are lexically ordered.
    pub(crate) fn merge(&self, other: &Self) -> Self {
        let tags: BTreeSet<Arc<Field>> = self
            .inner
            .fields()
            .iter()
            .chain(other.inner.fields())
            .filter(|f| f.series_column_type() == Some(SeriesColumnType::Tag))
            .cloned()
            .collect();
        Self::new(tags)
    }

    /// The projection expressions required to make the provided schema
    /// match this one.
    pub(crate) fn projection(&self, other: &Self) -> Vec<Expr> {
        self.inner
            .fields()
            .iter()
            .map(|f| {
                if f.series_column_type().unwrap() == SeriesColumnType::Tag
                    && other.inner.column_with_name(f.name()).is_none()
                {
                    //cast(lit(ScalarValue::Null), f.data_type().clone()).alias(f.name())
                    //Expr::ScalarFunction(ScalarFunction {
                    //    func: Arc::new(ScalarUDF::new_from_impl(NullTag::new())),
                    //    args: vec![],
                    //})
                    //.alias(f.name())
                    lit(ScalarValue::Dictionary(
                        Box::new(DataType::Int32),
                        Box::new(ScalarValue::Utf8(None)),
                    ))
                    .alias(f.name())
                } else {
                    Expr::Column(Column {
                        relation: None,
                        name: f.name().to_string(),
                    })
                }
            })
            .collect()
    }

    /// The sort expressions required to sort the series.
    pub(crate) fn sort(&self, partition_key: &[Arc<str>]) -> Vec<SortExpr> {
        partition_key
            .iter()
            .filter_map(|key| {
                self.inner.column_with_name(key).map(|_| {
                    Expr::Column(Column {
                        relation: None,
                        name: key.to_string(),
                    })
                    .sort(true, false)
                })
            })
            .chain(
                self.inner
                    .fields()
                    .iter()
                    .filter(|f| !partition_key.iter().any(|key| key.as_ref() == f.name()))
                    .filter(|f| {
                        if let Some(series_column_type) = f.series_column_type() {
                            series_column_type != SeriesColumnType::Value
                        } else {
                            false
                        }
                    })
                    .map(|f| {
                        Expr::Column(Column {
                            relation: None,
                            name: f.name().to_string(),
                        })
                        .sort(true, false)
                    }),
            )
            .collect()
    }
}

impl TryFrom<&SchemaRef> for SeriesSchema {
    type Error = crate::InfluxRpcError;

    fn try_from(schema: &SchemaRef) -> Result<Self, Self::Error> {
        // Check that this schema represents a series.

        let mut has_measurement = false;
        let mut has_field = false;
        let mut has_timestamp = false;
        let mut has_value = false;
        for field in schema.fields() {
            observability_deps::tracing::debug!(?field, "field");
            match field.series_column_type() {
                Some(SeriesColumnType::Measurement) => {
                    has_measurement = true;
                }
                Some(SeriesColumnType::Field) => {
                    has_field = true;
                }
                Some(SeriesColumnType::Timestamp) => {
                    has_timestamp = true;
                }
                Some(SeriesColumnType::Value) => {
                    has_value = true;
                }
                _ => {}
            }
        }
        let mut missing = Vec::with_capacity(4);
        if !has_measurement {
            missing.push("_measurement");
        }
        if !has_field {
            missing.push("_field");
        }
        if !has_timestamp {
            missing.push("_time");
        }
        if !has_value {
            missing.push("_value");
        }

        if !missing.is_empty() {
            observability_deps::tracing::debug!(
                has_measurement,
                has_field,
                has_timestamp,
                has_value,
                "invalid schema"
            );
            return Err(crate::InfluxRpcError::internal(format!(
                "invalid series schema, missing: {}",
                missing.join(",")
            )));
        }

        Ok(Self {
            inner: Arc::clone(schema),
        })
    }
}

impl TryFrom<SchemaRef> for SeriesSchema {
    type Error = crate::InfluxRpcError;

    fn try_from(schema: SchemaRef) -> Result<Self, Self::Error> {
        Self::try_from(&schema)
    }
}

impl TryFrom<&Schema> for SeriesSchema {
    type Error = <Self as TryFrom<SchemaRef>>::Error;

    fn try_from(schema: &Schema) -> Result<Self, Self::Error> {
        Self::try_from(Arc::new(schema.clone()))
    }
}

impl TryFrom<&DFSchemaRef> for SeriesSchema {
    type Error = <Self as TryFrom<SchemaRef>>::Error;

    fn try_from(schema: &DFSchemaRef) -> Result<Self, Self::Error> {
        Self::try_from(schema.as_arrow())
    }
}

impl TryFrom<SeriesSchema> for DFSchema {
    type Error = <Self as TryFrom<SchemaRef>>::Error;

    fn try_from(value: SeriesSchema) -> Result<Self, Self::Error> {
        <Self as TryFrom<SchemaRef>>::try_from(value.inner)
    }
}

impl From<SeriesSchema> for SchemaRef {
    fn from(value: SeriesSchema) -> Self {
        value.inner
    }
}

impl AsRef<SchemaRef> for SeriesSchema {
    fn as_ref(&self) -> &SchemaRef {
        &self.inner
    }
}
