//! This module contains the schema definition for IOx

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

use std::{
    cmp::Ordering,
    collections::HashMap,
    convert::{TryFrom, TryInto},
    fmt,
    mem::{size_of, size_of_val},
    sync::Arc,
};

use arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, FieldRef as ArrowFieldRef, Fields,
    Schema as ArrowSchema, SchemaRef as ArrowSchemaRef, TimeUnit,
};
use hashbrown::HashSet;

use crate::sort::SortKey;
use snafu::{OptionExt, Snafu};

/// The name of the timestamp column in the InfluxDB datamodel
pub const TIME_COLUMN_NAME: &str = "time";

/// The name of the column specifying the source measurement for a row for an InfluxQL query.
pub const INFLUXQL_MEASUREMENT_COLUMN_NAME: &str = "iox::measurement";
/// The key identifying the schema-level metadata.
pub const INFLUXQL_METADATA_KEY: &str = "iox::influxql::group_key::metadata";

/// The Timezone to use for InfluxDB timezone (should be a constant)
// TODO: Start Epic Add timezone support to IOx #18154
// https://github.com/influxdata/idpe/issues/18154
#[expect(non_snake_case)]
pub fn TIME_DATA_TIMEZONE() -> Option<Arc<str>> {
    None
}

/// the [`ArrowDataType`] to use for InfluxDB timestamps
#[expect(non_snake_case)]
pub fn TIME_DATA_TYPE() -> ArrowDataType {
    ArrowDataType::Timestamp(TimeUnit::Nanosecond, TIME_DATA_TIMEZONE())
}

pub mod builder;
pub mod interner;
pub mod merge;
mod projection;
pub mod sort;

pub use builder::SchemaBuilder;
pub use projection::Projection;

/// Namespace schema creation / validation errors.
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Internal Error: Duplicate column name found in schema: '{}'",
        column_name,
    ))]
    DuplicateColumnName { column_name: String },

    #[snafu(display(
        "Internal Error: Incompatible metadata type found in schema for column '{}'. Metadata specified {:?} which is incompatible with actual type {:?}",
        column_name,
        influxdb_column_type,
        actual_type
    ))]
    IncompatibleMetadata {
        column_name: String,
        influxdb_column_type: InfluxColumnType,
        actual_type: ArrowDataType,
    },

    #[snafu(display(
        "Internal Error: Invalid metadata type found in schema for column '{}'. Metadata specifies {:?} which requires the nullable flag to be set to {}, got {}",
        column_name,
        influxdb_column_type,
        expected,
        nullable,
    ))]
    Nullability {
        column_name: String,
        influxdb_column_type: InfluxColumnType,
        nullable: bool,
        expected: bool,
    },

    #[snafu(display("Column not found '{}'", column_name))]
    ColumnNotFound { column_name: String },

    #[snafu(display("Sort column not found '{}'", column_name))]
    SortColumnNotFound { column_name: String },

    #[snafu(display(
        "Internal Error: Invalid InfluxDB column type for column '{}', cannot parse metadata: {:?}",
        column_name,
        md
    ))]
    InvalidInfluxColumnType {
        column_name: String,
        md: Option<String>,
    },

    #[snafu(display(
        "Internal Error: Time column should be named '{}' but is named '{}'",
        TIME_COLUMN_NAME,
        column_name
    ))]
    WrongTimeColumnName { column_name: String },

    #[cfg(feature = "v3")]
    #[snafu(display(
        "The series key contains a column that was not present in fields when building"
    ))]
    MissingSeriesKeyColumn,

    #[cfg(feature = "v3")]
    #[snafu(display("The series key does not contain all tag columns present in the schema"))]
    TagsNotInSeriesKey,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Schema for an IOx table.
///
/// This structure can be copied / cloned cheaply
///
/// Holds an Arrow [`SchemaRef`] that stores IOx schema information in
/// the "user defined metadata".
///
/// The metadata can be used to map back and forth between the Arrow data model
/// (e.g. [`DataType`]) and the to the InfluxDB
/// data model, which is described in the
/// [documentation](https://docs.influxdata.com/influxdb/v2.0/reference/syntax/line-protocol/).
///
/// Specifically, each column in the Arrow schema has a corresponding
/// InfluxDB data model type of `Tag`, `Field` or `Timestamp` which is stored in
/// the metadata field of the [`SchemaRef`].
///
/// [`SchemaRef`]: arrow::datatypes::SchemaRef
/// [`DataType`]: arrow::datatypes::DataType
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Schema {
    /// Reference-counted pointer to underlying Arrow Schema
    ///
    /// All the actual data lives on the metadata structure in
    /// `ArrowSchemaRef` and this structure knows how to access that
    /// metadata
    inner: ArrowSchemaRef,
}

impl From<Schema> for ArrowSchemaRef {
    fn from(s: Schema) -> Self {
        s.inner
    }
}

impl From<&Schema> for ArrowSchemaRef {
    fn from(s: &Schema) -> Self {
        s.as_arrow()
    }
}

impl TryFrom<ArrowSchemaRef> for Schema {
    type Error = Error;

    fn try_from(value: ArrowSchemaRef) -> Result<Self, Self::Error> {
        Self::try_from_arrow(value)
    }
}

const MEASUREMENT_METADATA_KEY: &str = "iox::measurement::name";
const COLUMN_METADATA_KEY: &str = "iox::column::type";
#[cfg(feature = "v3")]
const SERIES_KEY_METADATA_KEY: &str = "iox::series::key";
#[cfg(feature = "v3")]
const SERIES_KEY_METADATA_SEPARATOR: &str = "/";

impl Schema {
    /// Create a new Schema wrapper over the schema
    ///
    /// All metadata validation is done on creation (todo maybe offer
    /// a fallible version where the checks are done on access)?
    fn try_from_arrow(inner: ArrowSchemaRef) -> Result<Self> {
        // Validate fields
        {
            // All column names must be unique
            let mut field_names = HashSet::with_capacity(inner.fields().len());

            for field in inner.fields() {
                let column_name = field.name();
                if !field_names.insert(column_name.as_str()) {
                    return Err(Error::DuplicateColumnName {
                        column_name: column_name.to_string(),
                    });
                }

                // for each field, ensure any type specified by the metadata
                // is compatible with the actual type of the field
                let influxdb_column_type =
                    get_influx_type(field).map_err(|md| Error::InvalidInfluxColumnType {
                        column_name: column_name.to_string(),
                        md,
                    })?;
                let actual_type = field.data_type();
                if !influxdb_column_type.valid_arrow_type(actual_type) {
                    return Err(Error::IncompatibleMetadata {
                        column_name: column_name.to_string(),
                        influxdb_column_type,
                        actual_type: actual_type.clone(),
                    });
                }

                let expected_nullable = match influxdb_column_type {
                    InfluxColumnType::Tag => true,
                    InfluxColumnType::Field(_) => true,
                    InfluxColumnType::Timestamp => false,
                };

                if field.is_nullable() != expected_nullable {
                    return Err(Error::Nullability {
                        column_name: column_name.to_string(),
                        influxdb_column_type,
                        nullable: field.is_nullable(),
                        expected: expected_nullable,
                    });
                }

                if (influxdb_column_type == InfluxColumnType::Timestamp)
                    && (column_name != TIME_COLUMN_NAME)
                {
                    return Err(Error::WrongTimeColumnName {
                        column_name: column_name.to_string(),
                    });
                }
            }
        }

        Ok(Self { inner })
    }

    /// Return a valid Arrow `SchemaRef` representing this `Schema`
    pub fn as_arrow(&self) -> ArrowSchemaRef {
        Arc::clone(&self.inner)
    }

    /// Create and validate a new Schema, creating metadata to
    /// represent the the various parts. This method is intended to be
    /// used only by the SchemaBuilder.
    pub(crate) fn new_from_parts(
        measurement: Option<String>,
        fields: impl Iterator<Item = (ArrowField, InfluxColumnType)>,
        sort_columns: bool,
        #[cfg(feature = "v3")] series_key: Option<impl IntoIterator<Item: AsRef<str>>>,
    ) -> Result<Self> {
        let mut metadata = HashMap::new();

        if let Some(measurement) = measurement {
            metadata.insert(MEASUREMENT_METADATA_KEY.to_string(), measurement);
        }

        #[cfg(feature = "v3")]
        if let Some(sk) = series_key {
            metadata.insert(
                SERIES_KEY_METADATA_KEY.to_string(),
                sk.into_iter()
                    .map(|k| k.as_ref().to_string())
                    .collect::<Vec<_>>()
                    .join(SERIES_KEY_METADATA_SEPARATOR),
            );
        }

        let mut fields: Vec<ArrowField> = fields
            .map(|(mut field, column_type)| {
                set_field_metadata(&mut field, column_type);
                field
            })
            .collect();

        if sort_columns {
            fields.sort_unstable_by(|a, b| a.name().cmp(b.name()));
        }

        // Call new_from_arrow to do normal, additional validation
        // (like dupe column detection)
        let record =
            ArrowSchemaRef::new(ArrowSchema::new_with_metadata(fields, metadata)).try_into()?;

        Ok(record)
    }

    #[cfg(feature = "v3")]
    pub fn series_key(&self) -> Option<Vec<&str>> {
        self.inner
            .metadata
            .get(SERIES_KEY_METADATA_KEY)
            .map(|v| v.split(SERIES_KEY_METADATA_SEPARATOR).collect())
    }

    /// Returns true if the sort_key includes all primary key cols
    pub fn is_sorted_on_pk(&self, sort_key: &SortKey) -> bool {
        self.primary_key().iter().all(|col| sort_key.contains(col))
    }

    /// Provide a reference to the underlying Arrow Schema object
    pub fn inner(&self) -> &ArrowSchemaRef {
        &self.inner
    }

    /// Return the InfluxDB data model type, if any, and underlying arrow
    /// schema field for the column at index `idx`. Panics if `idx` is
    /// greater than or equal to self.len()
    ///
    /// if there is no corresponding influx metadata,
    /// returns None for the influxdb_column_type
    pub fn field(&self, idx: usize) -> (InfluxColumnType, &ArrowField) {
        let field = self.inner.field(idx);
        (
            get_influx_type(field).expect("was checked during creation"),
            field,
        )
    }

    /// Return the InfluxDB data model type, if any, and underlying arrow
    /// schema field for the column identified by `name`.
    pub fn field_by_name(&self, name: &str) -> Option<(InfluxColumnType, &ArrowField)> {
        self.find_index_of(name).map(|index| self.field(index))
    }

    /// Return the [`InfluxColumnType`] for the field identified by `name`.
    pub fn field_type_by_name(&self, name: &str) -> Option<InfluxColumnType> {
        self.field_by_name(name).map(|(t, _)| t)
    }

    /// Find the index of the column with the given name, if any.
    pub fn find_index_of(&self, name: &str) -> Option<usize> {
        self.inner.index_of(name).ok()
    }

    /// Provides the InfluxDB data model measurement name for this schema, if
    /// any
    pub fn measurement(&self) -> Option<&String> {
        self.inner.metadata().get(MEASUREMENT_METADATA_KEY)
    }

    /// Returns the number of columns defined in this schema
    pub fn len(&self) -> usize {
        self.inner.fields().len()
    }

    /// Returns `true` if the schema contains no fields.
    pub fn is_empty(&self) -> bool {
        self.inner.fields().is_empty()
    }

    /// Returns an iterator of `(Option<InfluxColumnType>, &Field)` for
    /// all the columns of this schema, in order
    pub fn iter(&self) -> SchemaIter<'_> {
        SchemaIter::new(self)
    }

    /// Returns an iterator of `&Field` for all the tag columns of
    /// this schema, in order
    pub fn tags_iter(&self) -> impl Iterator<Item = &ArrowField> {
        self.iter().filter_map(|(influx_column_type, field)| {
            if matches!(influx_column_type, InfluxColumnType::Tag) {
                Some(field)
            } else {
                None
            }
        })
    }

    /// Returns an iterator of `&Field` for all the field columns of
    /// this schema, in order
    pub fn fields_iter(&self) -> impl Iterator<Item = &ArrowField> {
        self.iter().filter_map(|(influx_column_type, field)| {
            if matches!(influx_column_type, InfluxColumnType::Field(_)) {
                Some(field)
            } else {
                None
            }
        })
    }

    /// Returns an iterator of `&Field` for all the timestamp columns
    /// of this schema, in order. At the time of writing there should
    /// be only one or 0 such columns
    pub fn time_iter(&self) -> impl Iterator<Item = &ArrowField> {
        self.iter().filter_map(|(influx_column_type, field)| {
            if matches!(influx_column_type, InfluxColumnType::Timestamp) {
                Some(field)
            } else {
                None
            }
        })
    }

    /// Resort order of our columns lexicographically by name
    pub fn sort_fields_by_name(self) -> Self {
        // pairs of (orig_index, field_ref)
        let mut sorted_fields: Vec<(usize, &ArrowFieldRef)> =
            self.inner.fields().iter().enumerate().collect();
        sorted_fields.sort_by(|a, b| a.1.name().cmp(b.1.name()));

        let is_sorted = sorted_fields
            .iter()
            .enumerate()
            .all(|(index, pair)| index == pair.0);

        if is_sorted {
            self
        } else {
            // No way at present to destructure an existing Schema so
            // we have to copy :(
            let new_fields: Fields = sorted_fields.iter().map(|pair| pair.1).cloned().collect();

            let new_meta = self.inner.metadata().clone();
            let new_schema = ArrowSchema::new_with_metadata(new_fields, new_meta);

            Self {
                inner: Arc::new(new_schema),
            }
        }
    }

    /// Returns a Schema that represents selecting some of the columns
    /// in this schema. An error is returned if the selection refers to
    /// columns that do not exist.
    pub fn select(&self, selection: Projection<'_>) -> Result<Self> {
        Ok(match self.df_projection(selection)? {
            None => self.clone(),
            Some(indicies) => self.select_by_indices(&indicies),
        })
    }

    /// Return names of the columns of given indexes with all PK columns (tags and time)
    /// If the columns are not provided, return all columns
    pub fn select_given_and_pk_columns(&self, cols: Option<&Vec<usize>>) -> Vec<String> {
        match cols {
            Some(cols) => {
                let mut columns = cols
                    .iter()
                    .map(|i| self.field(*i).1.name().to_string())
                    .collect::<HashSet<_>>();

                // Add missing PK columnns as they are needed for deduplication
                let pk = self.primary_key();
                for col in pk {
                    columns.insert(col.to_string());
                }
                let mut columns = columns.into_iter().collect::<Vec<String>>();
                columns.sort();
                columns
            }
            None => {
                // Use all table columns
                self.iter().map(|(_, f)| f.name().to_string()).collect()
            }
        }
    }

    /// Returns a DataFusion style "projection" when the selection is
    /// applied to this schema.
    ///
    /// * `None` means "all columns"
    /// * `Some(indicies)` means the subset
    pub fn df_projection(&self, selection: Projection<'_>) -> Result<Option<Vec<usize>>> {
        Ok(match selection {
            Projection::All => None,
            Projection::Some(columns) => {
                let projection = columns
                    .iter()
                    .map(|&column_name| {
                        self.find_index_of(column_name)
                            .context(ColumnNotFoundSnafu { column_name })
                    })
                    .collect::<Result<Vec<_>>>()?;
                Some(projection)
            }
        })
    }

    /// Returns a Schema for the given (sub)set of column projects
    pub fn select_by_indices(&self, selection: &[usize]) -> Self {
        let mut fields = Vec::with_capacity(selection.len());
        for idx in selection {
            let field = self.inner.field(*idx);
            fields.push(field.clone());
        }

        let mut metadata = HashMap::with_capacity(1);
        if let Some(measurement) = self.inner.metadata().get(MEASUREMENT_METADATA_KEY).cloned() {
            metadata.insert(MEASUREMENT_METADATA_KEY.to_string(), measurement);
        }

        Self {
            inner: Arc::new(ArrowSchema::new_with_metadata(fields, metadata)),
        }
    }

    /// Returns a Schema for a given (sub)set of named columns
    pub fn select_by_names(&self, selection: &[&str]) -> Result<Self> {
        self.select(Projection::Some(selection))
    }

    /// Return columns used for the "primary key" in this table.
    ///
    /// This will use the series key if present in the schema, otherwise will revert to
    /// the InfluxDB data model annotations for what columns to include
    #[cfg(feature = "v3")]
    pub fn primary_key(&self) -> Vec<&str> {
        use InfluxColumnType::*;
        self.inner
            .metadata
            .get(SERIES_KEY_METADATA_KEY)
            .map_or_else(
                // use tags in lexicographical order
                || {
                    let mut primary_keys: Vec<_> = self
                        .iter()
                        .filter_map(|(column_type, field)| match column_type {
                            Tag => Some((Tag, field)),
                            Field(_) => None,
                            Timestamp => Some((Timestamp, field)),
                        })
                        .collect();

                    // Now, sort lexographically (but put timestamp last)
                    primary_keys.sort_by(|(a_column_type, a), (b_column_type, b)| {
                        match (a_column_type, b_column_type) {
                            (Tag, Tag) => a.name().cmp(b.name()),
                            (Timestamp, Tag) => Ordering::Greater,
                            (Tag, Timestamp) => Ordering::Less,
                            (Timestamp, Timestamp) => panic!("multiple timestamps in summary"),
                            _ => panic!("Unexpected types in key summary"),
                        }
                    });

                    // Take just the names
                    primary_keys
                        .into_iter()
                        .map(|(_column_type, field)| field.name().as_str())
                        .collect()
                },
                // use the series key
                |v| {
                    v.split(SERIES_KEY_METADATA_SEPARATOR)
                        .chain(self.time_iter().map(|f| f.name().as_str()))
                        .collect()
                },
            )
    }

    /// Return columns used for the "primary key" in this table.
    ///
    /// This will use the series key if present in the schema, otherwise will revert to
    /// the InfluxDB data model annotations for what columns to include
    #[cfg(not(feature = "v3"))]
    pub fn primary_key(&self) -> Vec<&str> {
        use InfluxColumnType::*;
        let mut primary_keys: Vec<_> = self
            .iter()
            .filter_map(|(column_type, field)| match column_type {
                Tag => Some((Tag, field)),
                Field(_) => None,
                Timestamp => Some((Timestamp, field)),
            })
            .collect();

        // Now, sort lexographically (but put timestamp last)
        primary_keys.sort_by(|(a_column_type, a), (b_column_type, b)| {
            match (a_column_type, b_column_type) {
                (Tag, Tag) => a.name().cmp(b.name()),
                (Timestamp, Tag) => Ordering::Greater,
                (Tag, Timestamp) => Ordering::Less,
                (Timestamp, Timestamp) => panic!("multiple timestamps in summary"),
                _ => panic!("Unexpected types in key summary"),
            }
        });

        // Take just the names
        primary_keys
            .into_iter()
            .map(|(_column_type, field)| field.name().as_str())
            .collect()
    }

    /// Estimate memory consumption in bytes of the schema.
    ///
    /// This includes the size of `Self` as well as the inner [`Arc`]ed arrow schema.
    pub fn estimate_size(&self) -> usize {
        let size_self = size_of_val(self);

        let size_inner = size_of_val(self.inner.as_ref());

        let size_fields = self.inner.fields().size();

        let metadata = self.inner.metadata();
        let size_metadata = metadata.capacity() * size_of::<(String, String)>()
            + metadata
                .iter()
                .map(|(k, v)| k.capacity() + v.capacity())
                .sum::<usize>();

        size_self + size_inner + size_fields + size_metadata
    }
}

/// Gets the influx type for a field
pub(crate) fn get_influx_type(field: &ArrowField) -> Result<InfluxColumnType, Option<String>> {
    let md = field
        .metadata()
        .get(COLUMN_METADATA_KEY)
        .ok_or(None)?
        .as_str();

    md.try_into().map_err(|_| Some(md.to_owned()))
}

/// Sets the metadata for a field - replacing any existing metadata
pub(crate) fn set_field_metadata(field: &mut ArrowField, column_type: InfluxColumnType) {
    field.set_metadata(HashMap::from([(
        COLUMN_METADATA_KEY.to_string(),
        column_type.to_string(),
    )]));
}

/// Field value types for InfluxDB 2.0 data model, as defined in
/// [the documentation]: <https://docs.influxdata.com/influxdb/v2.0/reference/syntax/line-protocol/>
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum InfluxFieldType {
    /// 64-bit floating point number (TDB if NULLs / Nans are allowed)
    Float,
    /// 64-bit signed integer
    Integer,
    /// Unsigned 64-bit integers
    UInteger,
    /// UTF-8 encoded string
    String,
    /// true or false
    Boolean,
}

impl From<InfluxFieldType> for ArrowDataType {
    fn from(t: InfluxFieldType) -> Self {
        match t {
            InfluxFieldType::Float => Self::Float64,
            InfluxFieldType::Integer => Self::Int64,
            InfluxFieldType::UInteger => Self::UInt64,
            InfluxFieldType::String => Self::Utf8,
            InfluxFieldType::Boolean => Self::Boolean,
        }
    }
}

impl TryFrom<ArrowDataType> for InfluxFieldType {
    type Error = &'static str;

    fn try_from(value: ArrowDataType) -> Result<Self, Self::Error> {
        match value {
            ArrowDataType::Float64 => Ok(Self::Float),
            ArrowDataType::Int64 => Ok(Self::Integer),
            ArrowDataType::UInt64 => Ok(Self::UInteger),
            ArrowDataType::Utf8 => Ok(Self::String),
            ArrowDataType::Boolean => Ok(Self::Boolean),
            _ => Err("No corresponding type in the InfluxDB data model"),
        }
    }
}

impl TryFrom<&String> for InfluxFieldType {
    type Error = &'static str;

    fn try_from(s: &String) -> Result<Self, Self::Error> {
        Ok(match s.as_str() {
            "Float" => Self::Float,
            "Integer" => Self::Integer,
            "UnsignedInteger" => Self::UInteger,
            "Boolean" => Self::Boolean,
            "String" => Self::String,
            _ => {
                return Err("No corresponding type in the InfluxDB data model");
            }
        })
    }
}

/// Column types.
///
/// Includes types for tags and fields in the InfluxDB data model, as described in the
/// [documentation](https://docs.influxdata.com/influxdb/v2.0/reference/syntax/line-protocol/).
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum InfluxColumnType {
    /// Tag
    ///
    /// Note: tags are always stored as a Utf8, but eventually this
    /// should allow for both Utf8 and Dictionary
    Tag,

    /// Field: Data of type in InfluxDB Data model
    Field(InfluxFieldType),

    /// Timestamp
    ///
    /// 64 bit timestamp "UNIX timestamps" representing nanoseconds
    /// since the UNIX epoch (00:00:00 UTC on 1 January 1970).
    Timestamp,
}

impl InfluxColumnType {
    /// returns true if `arrow_type` can validly store this column type
    pub fn valid_arrow_type(&self, data_type: &ArrowDataType) -> bool {
        match self {
            Self::Tag => match data_type {
                ArrowDataType::Utf8 => true,
                ArrowDataType::Dictionary(key, value) => {
                    key.as_ref() == &ArrowDataType::Int32 && value.as_ref() == &ArrowDataType::Utf8
                }
                _ => false,
            },
            Self::Field(_) => {
                let default_type: ArrowDataType = self.into();
                data_type == &default_type
            }
            Self::Timestamp => match data_type {
                ArrowDataType::Timestamp(TimeUnit::Nanosecond, None) => true,
                ArrowDataType::Timestamp(TimeUnit::Nanosecond, Some(tz))
                    if tz.as_ref() == "UTC" =>
                {
                    true
                }
                _ => false,
            },
        }
    }
}

/// "serialization" to strings that are stored in arrow metadata
impl From<&InfluxColumnType> for &'static str {
    fn from(t: &InfluxColumnType) -> Self {
        match t {
            InfluxColumnType::Tag => "iox::column_type::tag",
            InfluxColumnType::Field(InfluxFieldType::Float) => "iox::column_type::field::float",
            InfluxColumnType::Field(InfluxFieldType::Integer) => "iox::column_type::field::integer",
            InfluxColumnType::Field(InfluxFieldType::UInteger) => {
                "iox::column_type::field::uinteger"
            }
            InfluxColumnType::Field(InfluxFieldType::String) => "iox::column_type::field::string",
            InfluxColumnType::Field(InfluxFieldType::Boolean) => "iox::column_type::field::boolean",
            InfluxColumnType::Timestamp => "iox::column_type::timestamp",
        }
    }
}

impl std::fmt::Display for InfluxColumnType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s: &str = self.into();
        write!(f, "{s}")
    }
}

/// "deserialization" from strings that are stored in arrow metadata
impl TryFrom<&str> for InfluxColumnType {
    type Error = String;
    /// this is the inverse of converting to &str
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s {
            "iox::column_type::tag" => Ok(Self::Tag),
            "iox::column_type::field::float" => Ok(Self::Field(InfluxFieldType::Float)),
            "iox::column_type::field::integer" => Ok(Self::Field(InfluxFieldType::Integer)),
            "iox::column_type::field::uinteger" => Ok(Self::Field(InfluxFieldType::UInteger)),
            "iox::column_type::field::string" => Ok(Self::Field(InfluxFieldType::String)),
            "iox::column_type::field::boolean" => Ok(Self::Field(InfluxFieldType::Boolean)),
            "iox::column_type::timestamp" => Ok(Self::Timestamp),
            _ => Err(format!("Unknown column type in metadata: {s:?}")),
        }
    }
}

impl From<&InfluxColumnType> for ArrowDataType {
    /// What arrow type is used for this column type?
    fn from(t: &InfluxColumnType) -> Self {
        match t {
            InfluxColumnType::Tag => Self::Dictionary(Box::new(Self::Int32), Box::new(Self::Utf8)),
            InfluxColumnType::Field(influxdb_field_type) => (*influxdb_field_type).into(),
            InfluxColumnType::Timestamp => TIME_DATA_TYPE(),
        }
    }
}

/// Thing that implements iterator over a Schema's columns.
pub struct SchemaIter<'a> {
    schema: &'a Schema,
    idx: usize,
}

impl<'a> SchemaIter<'a> {
    fn new(schema: &'a Schema) -> Self {
        Self { schema, idx: 0 }
    }
}

impl fmt::Debug for SchemaIter<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SchemaIter<{}>", self.idx)
    }
}

impl<'a> Iterator for SchemaIter<'a> {
    type Item = (InfluxColumnType, &'a ArrowField);

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx < self.schema.len() {
            let ret = self.schema.field(self.idx);
            self.idx += 1;
            Some(ret)
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, Some(self.schema.len()))
    }
}

/// Asserts that the result of calling Schema:field(i) is as expected:
///
/// example
///   assert_column_eq!(schema, 0, InfluxColumnType::Tag, "host");
#[macro_export]
macro_rules! assert_column_eq {
    ($schema:expr, $i:expr, $expected_influxdb_column_type:expr, $expected_field_name:expr) => {
        let (influxdb_column_type, arrow_field) = $schema.field($i);
        assert_eq!(
            influxdb_column_type, $expected_influxdb_column_type,
            "Line protocol column mismatch for column {}, field {:?}, in schema {:#?}",
            $i, arrow_field, $schema
        );
        assert_eq!(
            arrow_field.name(),
            $expected_field_name,
            "expected field name mismatch for column {}, field {:?}, in schema {:#?}",
            $i,
            arrow_field,
            $schema
        )
    };
}

#[cfg(test)]
pub(crate) mod test_util {
    use super::*;

    pub(crate) fn make_field(
        name: &str,
        data_type: arrow::datatypes::DataType,
        nullable: bool,
        column_type: &str,
    ) -> ArrowField {
        let mut field = ArrowField::new(name, data_type, nullable);
        field.set_metadata(
            vec![(COLUMN_METADATA_KEY.to_string(), column_type.to_string())]
                .into_iter()
                .collect(),
        );
        field
    }
}

#[cfg(test)]
mod test {
    use InfluxColumnType::*;
    use InfluxFieldType::*;

    use crate::test_util::make_field;

    use super::{builder::SchemaBuilder, *};

    #[test]
    fn new_from_arrow_metadata_good() {
        let fields = vec![
            make_field(
                "tag_col",
                ArrowDataType::Utf8,
                true,
                "iox::column_type::tag",
            ),
            make_field(
                "int_col",
                ArrowDataType::Int64,
                true,
                "iox::column_type::field::integer",
            ),
            make_field(
                "uint_col",
                ArrowDataType::UInt64,
                true,
                "iox::column_type::field::uinteger",
            ),
            make_field(
                "float_col",
                ArrowDataType::Float64,
                true,
                "iox::column_type::field::float",
            ),
            make_field(
                "str_col",
                ArrowDataType::Utf8,
                true,
                "iox::column_type::field::string",
            ),
            make_field(
                "bool_col",
                ArrowDataType::Boolean,
                true,
                "iox::column_type::field::boolean",
            ),
            make_field(
                TIME_COLUMN_NAME,
                TIME_DATA_TYPE(),
                false,
                "iox::column_type::timestamp",
            ),
        ];

        let metadata: HashMap<_, _> = vec![(
            "iox::measurement::name".to_string(),
            "the_measurement".to_string(),
        )]
        .into_iter()
        .collect();

        let arrow_schema = ArrowSchemaRef::new(ArrowSchema::new_with_metadata(fields, metadata));

        let schema: Schema = arrow_schema.try_into().unwrap();
        assert_column_eq!(schema, 0, Tag, "tag_col");
        assert_column_eq!(schema, 1, Field(Integer), "int_col");
        assert_column_eq!(schema, 2, Field(UInteger), "uint_col");
        assert_column_eq!(schema, 3, Field(Float), "float_col");
        assert_column_eq!(schema, 4, Field(String), "str_col");
        assert_column_eq!(schema, 5, Field(Boolean), "bool_col");
        assert_column_eq!(schema, 6, Timestamp, TIME_COLUMN_NAME);
        assert_eq!(schema.len(), 7);

        assert_eq!(schema.measurement().unwrap(), "the_measurement");
    }

    // mismatched metadata / arrow types
    #[test]
    fn new_from_arrow_metadata_mismatched_tag() {
        let fields = vec![
            make_field(
                "tag_col",
                ArrowDataType::Int64,
                false,
                "iox::column_type::tag",
            ), // not a valid tag type
        ];

        let arrow_schema = ArrowSchemaRef::new(ArrowSchema::new(fields));

        let res = Schema::try_from_arrow(arrow_schema);
        assert_eq!(
            res.unwrap_err().to_string(),
            "Internal Error: Incompatible metadata type found in schema for column 'tag_col'. Metadata specified Tag which is incompatible with actual type Int64"
        );
    }

    // mismatched metadata / arrow types
    #[test]
    fn new_from_arrow_metadata_mismatched_field() {
        let fields = vec![make_field(
            "int_col",
            ArrowDataType::Int64,
            false,
            "iox::column_type::field::float",
        )];
        let arrow_schema = ArrowSchemaRef::new(ArrowSchema::new(fields));

        let res = Schema::try_from_arrow(arrow_schema);
        assert_eq!(
            res.unwrap_err().to_string(),
            "Internal Error: Incompatible metadata type found in schema for column 'int_col'. Metadata specified Field(Float) which is incompatible with actual type Int64"
        );
    }

    // mismatched metadata / arrow types
    #[test]
    fn new_from_arrow_metadata_mismatched_timestamp() {
        let fields = vec![
            make_field(
                "time",
                ArrowDataType::Utf8,
                false,
                "iox::column_type::timestamp",
            ), // timestamp can't be strings
        ];

        let arrow_schema = ArrowSchemaRef::new(ArrowSchema::new(fields));

        let res = Schema::try_from_arrow(arrow_schema);
        assert_eq!(
            res.unwrap_err().to_string(),
            "Internal Error: Incompatible metadata type found in schema for column 'time'. Metadata specified Timestamp which is incompatible with actual type Utf8"
        );
    }

    #[test]
    fn new_from_arrow_replicated_columns() {
        // arrow allows duplicated colum names
        let fields = vec![
            make_field(
                "the_column",
                ArrowDataType::Utf8,
                true,
                "iox::column_type::tag",
            ),
            make_field(
                "another_columng",
                ArrowDataType::Utf8,
                true,
                "iox::column_type::tag",
            ),
            make_field(
                "the_column",
                ArrowDataType::Utf8,
                true,
                "iox::column_type::tag",
            ),
        ];

        let arrow_schema = ArrowSchemaRef::new(ArrowSchema::new(fields));

        let res = Schema::try_from_arrow(arrow_schema);
        assert_eq!(
            res.unwrap_err().to_string(),
            "Internal Error: Duplicate column name found in schema: 'the_column'"
        );
    }

    #[test]
    fn new_from_arrow_nullable_wrong_tag() {
        let fields = vec![make_field(
            "tag_col",
            ArrowDataType::Utf8,
            false,
            "iox::column_type::tag",
        )];

        let arrow_schema = ArrowSchemaRef::new(ArrowSchema::new(fields));

        let res = Schema::try_from_arrow(arrow_schema);
        assert_eq!(
            res.unwrap_err().to_string(),
            "Internal Error: Invalid metadata type found in schema for column 'tag_col'. Metadata specifies Tag which requires the nullable flag to be set to true, got false"
        );
    }

    #[test]
    fn new_from_arrow_nullable_wrong_field() {
        let fields = vec![make_field(
            "field_col",
            ArrowDataType::Utf8,
            false,
            "iox::column_type::field::string",
        )];

        let arrow_schema = ArrowSchemaRef::new(ArrowSchema::new(fields));

        let res = Schema::try_from_arrow(arrow_schema);
        assert_eq!(
            res.unwrap_err().to_string(),
            "Internal Error: Invalid metadata type found in schema for column 'field_col'. Metadata specifies Field(String) which requires the nullable flag to be set to true, got false"
        );
    }

    #[test]
    fn new_from_arrow_nullable_wrong_timestamp() {
        let fields = vec![make_field(
            "time",
            TIME_DATA_TYPE(),
            true,
            "iox::column_type::timestamp",
        )];

        let arrow_schema = ArrowSchemaRef::new(ArrowSchema::new(fields));

        let res = Schema::try_from_arrow(arrow_schema);
        assert_eq!(
            res.unwrap_err().to_string(),
            "Internal Error: Invalid metadata type found in schema for column 'time'. Metadata specifies Timestamp which requires the nullable flag to be set to false, got true"
        );
    }

    #[test]
    fn new_from_arrow_no_metadata() {
        let arrow_schema = ArrowSchemaRef::new(ArrowSchema::new(vec![ArrowField::new(
            "col1",
            ArrowDataType::Int64,
            false,
        )]));

        let res = Schema::try_from_arrow(arrow_schema);
        assert_eq!(
            res.unwrap_err().to_string(),
            "Internal Error: Invalid InfluxDB column type for column 'col1', cannot parse metadata: None"
        );
    }

    #[test]
    fn new_from_arrow_metadata_invalid_md() {
        let fields = vec![make_field(
            "tag_col",
            ArrowDataType::Utf8,
            false,
            "something_other_than_iox",
        )];

        let arrow_schema = ArrowSchemaRef::new(ArrowSchema::new(fields));

        let res = Schema::try_from_arrow(arrow_schema);
        assert_eq!(
            res.unwrap_err().to_string(),
            "Internal Error: Invalid InfluxDB column type for column 'tag_col', cannot parse metadata: Some(\"something_other_than_iox\")"
        );
    }

    #[test]
    fn new_from_arrow_metadata_invalid_field() {
        let fields = vec![make_field(
            "int_col",
            ArrowDataType::Int64,
            false,
            "iox::column_type::field::some_new_exotic_type",
        )];

        let arrow_schema = ArrowSchemaRef::new(ArrowSchema::new(fields));

        let res = Schema::try_from_arrow(arrow_schema);
        assert_eq!(
            res.unwrap_err().to_string(),
            "Internal Error: Invalid InfluxDB column type for column 'int_col', cannot parse metadata: Some(\"iox::column_type::field::some_new_exotic_type\")"
        );
    }

    #[test]
    fn new_from_arrow_wrong_time_column_name() {
        let fields = vec![make_field(
            "foo",
            TIME_DATA_TYPE(),
            false,
            "iox::column_type::timestamp",
        )];

        let arrow_schema = ArrowSchemaRef::new(ArrowSchema::new(fields));

        let res = Schema::try_from_arrow(arrow_schema);
        assert_eq!(
            res.unwrap_err().to_string(),
            "Internal Error: Time column should be named 'time' but is named 'foo'"
        );
    }

    #[test]
    fn test_round_trip() {
        let schema1 = SchemaBuilder::new()
            .influx_field("the_field", String)
            .tag("the_tag")
            .timestamp()
            .measurement("the_measurement")
            .build()
            .unwrap();

        // Make a new schema via ArrowSchema (serialized metadata) to ensure that
        // the metadata makes it through a round trip

        let arrow_schema_1: ArrowSchemaRef = schema1.clone().into();
        let schema2 = Schema::try_from_arrow(arrow_schema_1).unwrap();

        for s in &[schema1, schema2] {
            assert_eq!(s.measurement().unwrap(), "the_measurement");
            assert_column_eq!(s, 0, Field(String), "the_field");
            assert_column_eq!(s, 1, Tag, "the_tag");
            assert_column_eq!(s, 2, Timestamp, "time");
            assert_eq!(3, s.len());
        }
    }

    /// Build an empty iterator
    fn empty_schema() -> Schema {
        SchemaBuilder::new().build().unwrap()
    }

    #[test]
    fn test_iter_empty() {
        assert_eq!(empty_schema().iter().count(), 0);
    }

    #[test]
    fn test_tags_iter_empty() {
        assert_eq!(empty_schema().tags_iter().count(), 0);
    }

    #[test]
    fn test_fields_iter_empty() {
        assert_eq!(empty_schema().fields_iter().count(), 0);
    }

    #[test]
    fn test_time_iter_empty() {
        assert_eq!(empty_schema().time_iter().count(), 0);
    }

    /// Build a schema for testing iterators
    fn iter_schema() -> Schema {
        SchemaBuilder::new()
            .influx_field("field1", Float)
            .tag("tag1")
            .timestamp()
            .influx_field("field2", String)
            .influx_field("field3", String)
            .tag("tag2")
            .build()
            .unwrap()
    }

    #[test]
    fn test_iter() {
        let schema = iter_schema();

        // test schema iterator and field accessor match up
        for (i, (iter_col_type, iter_field)) in schema.iter().enumerate() {
            let (col_type, field) = schema.field(i);
            assert_eq!(iter_col_type, col_type);
            assert_eq!(iter_field, field);
        }
        assert_eq!(schema.iter().count(), 6);
    }

    #[test]
    fn test_tags_iter() {
        let schema = iter_schema();

        let mut iter = schema.tags_iter();
        assert_eq!(iter.next().unwrap().name(), "tag1");
        assert_eq!(iter.next().unwrap().name(), "tag2");
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_fields_iter() {
        let schema = iter_schema();

        let mut iter = schema.fields_iter();
        assert_eq!(iter.next().unwrap().name(), "field1");
        assert_eq!(iter.next().unwrap().name(), "field2");
        assert_eq!(iter.next().unwrap().name(), "field3");
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_time_iter() {
        let schema = iter_schema();

        let mut iter = schema.time_iter();
        assert_eq!(iter.next().unwrap().name(), "time");
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_sort_fields_by_name_already_sorted() {
        let schema = SchemaBuilder::new()
            .field("field_a", ArrowDataType::Int64)
            .unwrap()
            .field("field_b", ArrowDataType::Int64)
            .unwrap()
            .field("field_c", ArrowDataType::Int64)
            .unwrap()
            .build()
            .unwrap();

        let sorted_schema = schema.clone().sort_fields_by_name();

        assert_eq!(
            schema, sorted_schema,
            "\nExpected:\n{schema:#?}\nActual:\n{sorted_schema:#?}"
        );
    }

    #[test]
    fn test_sort_fields_by_name() {
        let schema = SchemaBuilder::new()
            .field("field_b", ArrowDataType::Int64)
            .unwrap()
            .field("field_a", ArrowDataType::Int64)
            .unwrap()
            .field("field_c", ArrowDataType::Int64)
            .unwrap()
            .build()
            .unwrap();

        let sorted_schema = schema.sort_fields_by_name();

        let expected_schema = SchemaBuilder::new()
            .field("field_a", ArrowDataType::Int64)
            .unwrap()
            .field("field_b", ArrowDataType::Int64)
            .unwrap()
            .field("field_c", ArrowDataType::Int64)
            .unwrap()
            .build()
            .unwrap();

        assert_eq!(
            expected_schema, sorted_schema,
            "\nExpected:\n{expected_schema:#?}\nActual:\n{sorted_schema:#?}"
        );
    }

    #[test]
    fn test_select() {
        let schema1 = SchemaBuilder::new()
            .influx_field("the_field", String)
            .tag("the_tag")
            .timestamp()
            .measurement("the_measurement")
            .build()
            .unwrap();

        let schema2 = schema1.select_by_names(&[TIME_COLUMN_NAME]).unwrap();
        let schema3 = Schema::try_from_arrow(Arc::clone(&schema2.inner)).unwrap();

        assert_eq!(schema1.measurement(), schema2.measurement());
        assert_eq!(schema1.measurement(), schema3.measurement());

        assert_eq!(schema1.len(), 3);
        assert_eq!(schema2.len(), 1);
        assert_eq!(schema3.len(), 1);

        assert_eq!(schema1.inner.fields().len(), 3);
        assert_eq!(schema2.inner.fields().len(), 1);
        assert_eq!(schema3.inner.fields().len(), 1);

        let get_type = |x: &Schema, field: &str| -> InfluxColumnType {
            let idx = x.find_index_of(field).unwrap();
            x.field(idx).0
        };

        assert_eq!(
            get_type(&schema1, TIME_COLUMN_NAME),
            InfluxColumnType::Timestamp
        );
        assert_eq!(
            get_type(&schema2, TIME_COLUMN_NAME),
            InfluxColumnType::Timestamp
        );
        assert_eq!(get_type(&schema1, "the_tag"), InfluxColumnType::Tag);
        assert_eq!(
            get_type(&schema1, "the_field"),
            InfluxColumnType::Field(InfluxFieldType::String)
        );
        assert_eq!(
            get_type(&schema2, TIME_COLUMN_NAME),
            InfluxColumnType::Timestamp
        );
        assert_eq!(
            get_type(&schema3, TIME_COLUMN_NAME),
            InfluxColumnType::Timestamp
        );
    }

    #[test]
    fn test_df_projection() {
        let schema = SchemaBuilder::new()
            .influx_field("the_field", String)
            .tag("the_tag")
            .timestamp()
            .measurement("the_measurement")
            .build()
            .unwrap();

        assert_eq!(schema.df_projection(Projection::All).unwrap(), None);
        assert_eq!(
            schema
                .df_projection(Projection::Some(&["the_tag"]))
                .unwrap(),
            Some(vec![1])
        );
        assert_eq!(
            schema
                .df_projection(Projection::Some(&["the_tag", "the_field"]))
                .unwrap(),
            Some(vec![1, 0])
        );

        let res = schema.df_projection(Projection::Some(&["the_tag", "unknown_field"]));
        assert_eq!(
            res.unwrap_err().to_string(),
            "Column not found 'unknown_field'"
        );
    }

    #[test]
    fn test_is_sort_on_pk() {
        // Sort key the same as pk
        let sort_key =
            SortKey::from_columns(vec!["tag4", "tag3", "tag2", "tag1", TIME_COLUMN_NAME]);

        let schema = SchemaBuilder::new()
            .influx_field("the_field", String)
            .tag("tag1")
            .tag("tag2")
            .tag("tag3")
            .tag("tag4")
            .timestamp()
            .measurement("the_measurement")
            .build()
            .unwrap();
        assert!(schema.is_sorted_on_pk(&sort_key));

        // Sort key does not include all pk cols
        let sort_key = SortKey::from_columns(vec!["tag3", "tag1", TIME_COLUMN_NAME]);

        let schema = SchemaBuilder::new()
            .influx_field("the_field", String)
            .tag("tag1")
            .tag("tag2")
            .tag("tag3")
            .tag("tag4")
            .timestamp()
            .measurement("the_measurement")
            .build()
            .unwrap();
        assert!(!schema.is_sorted_on_pk(&sort_key));

        // No PK, sort key on non pk
        let sort_key = SortKey::from_columns(vec!["the_field"]);

        let schema = SchemaBuilder::new()
            .influx_field("the_field", String)
            .tag("tag1")
            .tag("tag2")
            .tag("tag3")
            .tag("tag4")
            .timestamp()
            .measurement("the_measurement")
            .build()
            .unwrap();
        assert!(!schema.is_sorted_on_pk(&sort_key));
    }

    #[test]
    fn test_estimate_size() {
        let schema = SchemaBuilder::new()
            .influx_field("the_field", String)
            .tag("the_tag")
            .timestamp()
            .measurement("the_measurement")
            .build()
            .unwrap();

        // this is mostly a smoke test
        assert_eq!(schema.estimate_size(), 1243);
    }

    #[cfg(feature = "v3")]
    #[test]
    fn test_series_key_as_pk() {
        let schema = SchemaBuilder::new()
            .with_series_key(["a", "b"])
            .tag("a")
            .tag("b")
            .influx_field("f1", Float)
            .timestamp()
            .measurement("foo")
            .build()
            .unwrap();
        assert_eq!(vec!["a", "b", "time"], schema.primary_key());
    }
}
