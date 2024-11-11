use datafusion::error::DataFusionError;

#[derive(Debug, Clone)]
pub enum InfluxRpcError {
    Internal {
        message: String,
    },
    UnknownTag {
        name: String,
        known_tags: Vec<String>,
    },
}

impl InfluxRpcError {
    /// Create an internal error with the provided message.
    pub(crate) fn internal(message: impl Into<String>) -> Self {
        Self::Internal {
            message: message.into(),
        }
    }
}

impl std::error::Error for InfluxRpcError {}

impl std::fmt::Display for InfluxRpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Internal { message } => write!(f, "InfluxRpc internal error: {message}"),
            Self::UnknownTag { name, known_tags } => {
                write!(
                    f,
                    "InfluxRpc unknown tag {name}. Known tags: {}",
                    known_tags.join(",")
                )
            }
        }
    }
}

impl From<InfluxRpcError> for DataFusionError {
    fn from(value: InfluxRpcError) -> Self {
        Self::External(Box::new(value))
    }
}
