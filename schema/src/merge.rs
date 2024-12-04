use arrow::{datatypes::Field, record_batch::RecordBatch};
use hashbrown::hash_map::RawEntryMut;
use hashbrown::HashMap;
use snafu::Snafu;

use crate::interner::SchemaInterner;

use super::{InfluxColumnType, Schema};

/// Namespace schema creation / validation errors.
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("No schemas found when building merged schema"))]
    NoSchemas,

    #[snafu(display(
        "Schema Merge Error: Incompatible measurement names. Existing measurement name '{}', new measurement name '{}'",
        existing_measurement, new_measurement
    ))]
    TryMergeDifferentMeasurementNames {
        existing_measurement: String,
        new_measurement: String,
    },

    #[snafu(display(
        "Schema Merge Error: Incompatible column type for '{}'. Existing type {:?}, new type {:?}",
        field_name,
        existing_column_type,
        new_column_type
    ))]
    TryMergeBadColumnType {
        field_name: String,
        existing_column_type: InfluxColumnType,
        new_column_type: InfluxColumnType,
    },

    #[cfg(feature = "v3")]
    #[snafu(display(
        "Schema Merge Error: Incompatible series keys when merging schema. Existing key: [{}], new key: [{}]",
        existing_key.join(", "),
        new_key.join(", ")
    ))]
    TryMergeIncompatibleSeriesKey {
        existing_key: Vec<String>,
        new_key: Vec<String>,
    },

    #[cfg(feature = "v3")]
    #[snafu(display(
        "Schema Merge Error: tried to merge a schema that has no series key with one that does"
    ))]
    TryMergeNonSeriesKey,

    #[cfg(feature = "v3")]
    #[snafu(display(
        "Schema Merge Error: tried to merge a schema that has a series key with one that does not"
    ))]
    TryMergeIntoNonSeriesKey,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Return the merged schema for RecordBatches
///
/// This is infallable because the schemas of chunks within a
/// partition are assumed to be compatible because that schema was
/// enforced as part of writing into the partition
pub fn merge_record_batch_schemas(batches: &[RecordBatch]) -> Schema {
    let mut merger = SchemaMerger::new();
    for batch in batches {
        let schema = Schema::try_from(batch.schema()).expect("Schema conversion error");
        merger = merger.merge(&schema).expect("Schemas compatible");
    }
    merger.build()
}

/// Schema Merger
///
/// The usecase for merging schemas is when different chunks have
/// different schemas. This struct can be used to build a combined
/// schema by merging Schemas together according to the following
/// rules:
///
/// 1. New columns may be added in subsequent schema, but the types of
///    the columns (including any metadata) must be the same
///
/// 2. The measurement names must be consistent: one or both can be
///    `None`, or they can both be `Some(name`)
#[derive(Debug, Default)]
pub struct SchemaMerger<'a> {
    /// Maps column names to their definition
    fields: HashMap<String, (Field, InfluxColumnType)>,
    /// The measurement name if any
    measurement: Option<String>,
    /// Interner, if any.
    interner: Option<&'a mut SchemaInterner>,
    /// The series key if any
    #[cfg(feature = "v3")]
    series_key: Option<Vec<String>>,
}

impl SchemaMerger<'static> {
    pub fn new() -> Self {
        Self::default()
    }
}

impl SchemaMerger<'_> {
    pub fn with_interner(self, interner: &mut SchemaInterner) -> SchemaMerger<'_> {
        SchemaMerger {
            fields: self.fields,
            measurement: self.measurement,
            interner: Some(interner),
            #[cfg(feature = "v3")]
            series_key: None,
        }
    }

    /// Appends the schema to the merged schema being built,
    /// validating that no columns are added.
    pub fn merge(mut self, other: &Schema) -> Result<Self> {
        // Verify measurement name is compatible
        match (self.measurement.as_ref(), other.measurement()) {
            (Some(existing_measurement), Some(new_measurement)) => {
                if existing_measurement != new_measurement {
                    return TryMergeDifferentMeasurementNamesSnafu {
                        existing_measurement,
                        new_measurement,
                    }
                    .fail();
                }
            }
            (None, Some(other)) => self.measurement = Some(other.clone()),
            _ => {}
        }

        #[cfg(feature = "v3")]
        self.merge_series_key(other.series_key())?;

        // Merge fields
        for (column_type, field) in other.iter() {
            self.merge_field(field, column_type)?;
        }

        Ok(self)
    }

    #[cfg(feature = "v3")]
    pub fn merge_series_key(&mut self, other: Option<Vec<&str>>) -> Result<&mut Self> {
        let this = self.series_key.as_deref();

        match (this, other) {
            (None, None) => (),
            (None, Some(new)) => {
                if self.fields.is_empty() {
                    // this is the first merge, since the fields are empty, so set the
                    // series key this time:
                    self.series_key = Some(new.into_iter().map(|v| v.to_string()).collect());
                } else {
                    return TryMergeIntoNonSeriesKeySnafu.fail();
                }
            }
            (Some(_), None) => return TryMergeNonSeriesKeySnafu.fail(),
            (Some(a), Some(b)) => {
                if a != b {
                    return TryMergeIncompatibleSeriesKeySnafu {
                        existing_key: a.iter().map(Into::into).collect::<Vec<String>>(),
                        new_key: b.into_iter().map(Into::into).collect::<Vec<String>>(),
                    }
                    .fail();
                }
            }
        }

        Ok(self)
    }

    pub fn merge_field(
        &mut self,
        field: &Field,
        column_type: InfluxColumnType,
    ) -> Result<&mut Self> {
        let field_name = field.name();
        match self.fields.raw_entry_mut().from_key(field_name) {
            RawEntryMut::Vacant(vacant) => {
                // Purposefully don't propagate metadata to avoid blindly propagating
                // information such as sort key, etc... that SchemaMerger cannot guarantee
                // to preserve the semantics of
                let field = Field::new(field_name, field.data_type().clone(), field.is_nullable());
                vacant.insert(field_name.clone(), (field, column_type));
            }
            RawEntryMut::Occupied(occupied) => {
                let (existing_field, existing_column_type) = occupied.get();

                // for now, insist the types are exactly the same
                // (e.g. None and Some(..) don't match). We could
                // consider relaxing this constraint
                if existing_column_type != &column_type {
                    return Err(Error::TryMergeBadColumnType {
                        field_name: field_name.to_string(),
                        existing_column_type: *existing_column_type,
                        new_column_type: column_type,
                    });
                }

                // both are valid schemas, so this should always hold
                assert_eq!(field.is_nullable(), existing_field.is_nullable());
                assert_eq!(field.data_type(), existing_field.data_type());
            }
        }

        Ok(self)
    }

    /// Returns the schema that was built, the columns are always sorted in lexicographic order
    pub fn build(mut self) -> Schema {
        let schema = Schema::new_from_parts(
            self.measurement.take(),
            self.fields.drain().map(|x| x.1),
            true,
            #[cfg(feature = "v3")]
            self.series_key.take(),
        )
        .expect("failed to build merged schema");

        if let Some(interner) = self.interner.as_mut() {
            interner.intern(schema)
        } else {
            schema
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::builder::SchemaBuilder;
    use crate::InfluxFieldType::Integer;

    use super::*;

    #[test]
    fn test_merge_same_schema() {
        let schema1 = SchemaBuilder::new()
            .influx_field("int_field", Integer)
            .tag("the_tag")
            .build()
            .unwrap();

        let schema2 = SchemaBuilder::new()
            .influx_field("int_field", Integer)
            .tag("the_tag")
            .build()
            .unwrap();

        let merged_schema = SchemaMerger::new()
            .merge(&schema1)
            .unwrap()
            .merge(&schema2)
            .unwrap()
            .build();

        assert_eq!(merged_schema, schema1);
        assert_eq!(merged_schema, schema2);
    }

    #[test]
    fn test_merge_compatible_schema() {
        let schema1 = SchemaBuilder::new()
            .tag("the_tag")
            .influx_field("int_field", Integer)
            .build()
            .unwrap()
            .sort_fields_by_name();

        // has some of the same and some new, different fields
        let schema2 = SchemaBuilder::new()
            .measurement("my_measurement")
            .tag("the_other_tag")
            .influx_field("int_field", Integer)
            .influx_field("another_field", Integer)
            .build()
            .unwrap()
            .sort_fields_by_name();

        let merged_schema = SchemaMerger::new()
            .merge(&schema1)
            .unwrap()
            .merge(&schema2)
            .unwrap()
            .build();

        let expected_schema = SchemaBuilder::new()
            .measurement("my_measurement")
            .tag("the_tag")
            .influx_field("int_field", Integer)
            .tag("the_other_tag")
            .influx_field("another_field", Integer)
            .build()
            .unwrap()
            .sort_fields_by_name();

        assert_eq!(
            expected_schema, merged_schema,
            "\nExpected:\n{expected_schema:#?}\nActual:\n{merged_schema:#?}"
        );
    }

    #[test]
    fn test_merge_compatible_schema_no_names() {
        let schema1 = SchemaBuilder::new().tag("the_tag").build().unwrap();

        // has some different fields
        let schema2 = SchemaBuilder::new().tag("the_other_tag").build().unwrap();

        // ensure the merge is not optimized away
        let merged_schema = SchemaMerger::new()
            .merge(&schema1)
            .unwrap()
            .merge(&schema2)
            .unwrap()
            .build();

        let expected_schema = SchemaBuilder::new()
            .tag("the_other_tag")
            .tag("the_tag")
            .build()
            .unwrap();

        assert_eq!(
            expected_schema, merged_schema,
            "\nExpected:\n{expected_schema:#?}\nActual:\n{merged_schema:#?}"
        );
    }

    #[test]
    fn test_merge_compatible_schema_only_measurement() {
        let schema1 = SchemaBuilder::new()
            .tag("the_tag")
            .measurement("the_measurement")
            .build()
            .unwrap();

        // schema has same fields but not measurement name
        let schema2 = SchemaBuilder::new().tag("the_tag").build().unwrap();

        // ensure the merge is not optimized away
        let merged_schema = SchemaMerger::new()
            .merge(&schema1)
            .unwrap()
            .merge(&schema2)
            .unwrap()
            .build();

        let expected_schema = SchemaBuilder::new()
            .tag("the_tag")
            .measurement("the_measurement")
            .build()
            .unwrap();

        assert_eq!(
            expected_schema, merged_schema,
            "\nExpected:\n{expected_schema:#?}\nActual:\n{merged_schema:#?}"
        );
    }

    #[test]
    fn test_merge_measurement_names() {
        let schema1 = SchemaBuilder::new().tag("the_tag").build().unwrap();

        // has some of the same and some different fields
        let schema2 = SchemaBuilder::new()
            .measurement("my_measurement")
            .build()
            .unwrap();

        let merged_schema = SchemaMerger::new()
            .merge(&schema1)
            .unwrap()
            .merge(&schema2)
            .unwrap()
            .build();

        let expected_schema = SchemaBuilder::new()
            .measurement("my_measurement")
            .tag("the_tag")
            .build()
            .unwrap();

        assert_eq!(
            expected_schema, merged_schema,
            "\nExpected:\n{expected_schema:#?}\nActual:\n{merged_schema:#?}"
        );
    }

    #[test]
    fn test_merge_incompatible_schema_measurement_names() {
        let schema1 = SchemaBuilder::new()
            .tag("the_tag")
            .measurement("measurement1")
            .build()
            .unwrap();

        // different measurement name, same otherwise
        let schema2 = SchemaBuilder::new()
            .tag("the_tag")
            .measurement("measurement2")
            .build()
            .unwrap();

        let merged_schema_error = SchemaMerger::new()
            .merge(&schema1)
            .unwrap()
            .merge(&schema2)
            .unwrap_err();

        assert_eq!(
            merged_schema_error.to_string(),
            "Schema Merge Error: Incompatible measurement names. Existing measurement name 'measurement1', new measurement name 'measurement2'"
        );
    }

    #[test]
    fn test_merge_incompatible_column_types() {
        let schema1 = SchemaBuilder::new().tag("the_tag").build().unwrap();

        // same field name with different type
        let schema2 = SchemaBuilder::new()
            .influx_field("the_tag", Integer)
            .build()
            .unwrap();

        let merged_schema_error = SchemaMerger::new()
            .merge(&schema1)
            .unwrap()
            .merge(&schema2)
            .unwrap_err();

        assert_eq!(merged_schema_error.to_string(), "Schema Merge Error: Incompatible column type for 'the_tag'. Existing type Tag, new type Field(Integer)");
    }

    #[test]
    fn test_interning() {
        let schema_1a = SchemaBuilder::new()
            .influx_field("int_field", Integer)
            .tag("the_tag")
            .build()
            .unwrap();

        let schema_1b = SchemaBuilder::new()
            .influx_field("int_field", Integer)
            .tag("the_tag")
            .build()
            .unwrap();

        let schema_2 = SchemaBuilder::new()
            .influx_field("float_field", crate::InfluxFieldType::Float)
            .tag("the_tag")
            .build()
            .unwrap();

        let mut interner = SchemaInterner::new();

        let merged_schema_a = SchemaMerger::new()
            .with_interner(&mut interner)
            .merge(&schema_1a)
            .unwrap()
            .merge(&schema_2)
            .unwrap()
            .build();

        let merged_schema_b = SchemaMerger::new()
            .with_interner(&mut interner)
            .merge(&schema_1b)
            .unwrap()
            .merge(&schema_2)
            .unwrap()
            .build();

        assert_eq!(merged_schema_a, merged_schema_b);
        assert!(Arc::ptr_eq(
            merged_schema_a.inner(),
            merged_schema_b.inner()
        ));
    }

    #[cfg(feature = "v3")]
    #[test]
    fn test_series_key_merge_success() {
        use crate::InfluxFieldType;
        use InfluxColumnType::*;
        use InfluxFieldType::*;

        let s1 = SchemaBuilder::new()
            .with_series_key(["a", "b"])
            .tag("a")
            .tag("b")
            .influx_field("f1", Float)
            .timestamp()
            .measurement("foo")
            .build()
            .unwrap();
        let s2 = SchemaBuilder::new()
            .with_series_key(["a", "b"])
            .tag("a")
            .tag("b")
            .influx_field("f2", String)
            .timestamp()
            .measurement("foo")
            .build()
            .unwrap();
        let s_merged = SchemaMerger::new()
            .merge(&s1)
            .unwrap()
            .merge(&s2)
            .unwrap()
            .build();
        assert_eq!(vec!["a", "b", "time"], s_merged.primary_key());
        assert!(s_merged
            .iter()
            .any(|(t, f)| f.name() == "a" && matches!(t, Tag)));
        assert!(s_merged
            .iter()
            .any(|(t, f)| f.name() == "b" && matches!(t, Tag)));
        assert!(s_merged
            .iter()
            .any(|(t, f)| f.name() == "time" && matches!(t, Timestamp)));
        assert!(s_merged
            .iter()
            .any(|(t, f)| f.name() == "f1" && matches!(t, Field(Float))));
        assert!(s_merged
            .iter()
            .any(|(t, f)| f.name() == "f2" && matches!(t, Field(String))));
        assert_eq!(5, s_merged.len());
    }

    #[cfg(feature = "v3")]
    #[test]
    fn test_series_key_merge_failures() {
        use crate::InfluxFieldType;
        use InfluxFieldType::*;

        // base schema with two tag columns (a, b) in the series key:
        let s1 = SchemaBuilder::new()
            .with_series_key(["a", "b"])
            .tag("a")
            .tag("b")
            .influx_field("f1", Float)
            .timestamp()
            .measurement("foo")
            .build()
            .unwrap();
        // a similar schema with one differing tag column (c vs. b):
        let s2 = SchemaBuilder::new()
            .with_series_key(["a", "c"])
            .tag("a")
            .tag("c")
            .influx_field("f1", Float)
            .timestamp()
            .measurement("foo")
            .build()
            .unwrap();
        // a schema that does not have a series key:
        let s3 = SchemaBuilder::new()
            .tag("a")
            .tag("b")
            .influx_field("f1", Float)
            .timestamp()
            .measurement("foo")
            .build()
            .unwrap();
        SchemaMerger::new()
            .merge(&s1)
            .unwrap()
            .merge(&s2)
            .expect_err("should not be able to merge schema with mismatched series keys");
        SchemaMerger::new()
            .merge(&s1)
            .unwrap()
            .merge(&s3)
            .expect_err(
                "should not be able to merge schema using series key with schema using tags",
            );
    }
}
