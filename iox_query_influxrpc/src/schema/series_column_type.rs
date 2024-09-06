use arrow::datatypes::Field;

/// Metadata key used to store the series column type.
const SERIES_COLUMN_TYPE_KEY: &str = "influxrpc::series_column_type";

/// The type of a particular column in a series schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeriesColumnType {
    /// Column that contains the measuement name for the series.
    Measurement,
    /// Column that contains an arbitrary tag in the series.
    Tag,
    /// Column that contains the field name for the series.
    Field,
    /// Column that contains the timestamp for the point.
    Timestamp,
    /// Column that contains the value of the point.
    Value,
}

impl TryFrom<&str> for SeriesColumnType {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "influxrpc::series_column_type::measurement" => Ok(Self::Measurement),
            "influxrpc::series_column_type::tag" => Ok(Self::Tag),
            "influxrpc::series_column_type::field" => Ok(Self::Field),
            "influxrpc::series_column_type::timestamp" => Ok(Self::Timestamp),
            "influxrpc::series_column_type::value" => Ok(Self::Value),
            _ => Err(format!("Unknown series column type: {value}")),
        }
    }
}

impl From<SeriesColumnType> for String {
    fn from(value: SeriesColumnType) -> Self {
        match value {
            SeriesColumnType::Measurement => "influxrpc::series_column_type::measurement".into(),
            SeriesColumnType::Tag => "influxrpc::series_column_type::tag".into(),
            SeriesColumnType::Field => "influxrpc::series_column_type::field".into(),
            SeriesColumnType::Timestamp => "influxrpc::series_column_type::timestamp".into(),
            SeriesColumnType::Value => "influxrpc::series_column_type::value".into(),
        }
    }
}

impl std::fmt::Display for SeriesColumnType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Measurement => write!(f, "MEASUREMENT"),
            Self::Tag => write!(f, "TAG"),
            Self::Field => write!(f, "FIELD"),
            Self::Timestamp => write!(f, "TIMESTAMP"),
            Self::Value => write!(f, "VALUE"),
        }
    }
}

/// Extension trait for `Field` to add series column type information.
pub trait FieldExt {
    ///Adds a series column type to a field.
    fn with_series_column_type(self, series_column_type: SeriesColumnType) -> Self;

    /// Returns the series column type of the field, if it has one.
    fn series_column_type(&self) -> Option<SeriesColumnType>;
}

impl FieldExt for Field {
    fn with_series_column_type(self, series_column_type: SeriesColumnType) -> Self {
        let mut metadata = self.metadata().clone();
        metadata.insert(SERIES_COLUMN_TYPE_KEY.into(), series_column_type.into());
        self.with_metadata(metadata)
    }

    fn series_column_type(&self) -> Option<SeriesColumnType> {
        self.metadata()
            .get(SERIES_COLUMN_TYPE_KEY)
            .and_then(|value| SeriesColumnType::try_from(value.as_str()).ok())
    }
}
