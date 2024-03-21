use std::sync::Arc;

use arrow::datatypes::DataType;
use arrow::datatypes::TimeUnit;
use datafusion::common::internal_err;
use datafusion::error::Result;
use datafusion::logical_expr::ScalarUDFImpl;
use datafusion::logical_expr::Signature;
use datafusion::{
    logical_expr::{ScalarUDF, Volatility},
    physical_plan::ColumnarValue,
};
use once_cell::sync::Lazy;

/// The name of the function
pub const TO_TIMESTAMP_FUNCTION_NAME: &str = "to_timestamp";

/// Implementation of `to_timestamp` function that
/// overrides the built in version in DataFusion because the semantics changed
/// upstream: <https://github.com/apache/arrow-datafusion/pull/7844>
///
/// See <https://github.com/influxdata/influxdb_iox/issues/9164> for more details
#[derive(Debug)]
struct ToTimestampUDF {
    signature: Signature,
    /// Fall back to DataFusion's built in to_timestamp implementation
    fallback: Arc<ScalarUDF>,
}

impl ToTimestampUDF {
    fn new() -> Self {
        Self {
            signature: Signature::uniform(
                1,
                vec![
                    DataType::Int64,
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    DataType::Timestamp(TimeUnit::Second, None),
                    DataType::Utf8,
                ],
                Volatility::Immutable,
            ),
            // Find DataFusion's built in to_timestamp implementation
            // Use `to_timestamp` directly when available:
            // https://github.com/apache/arrow-datafusion/issues/9584
            fallback: datafusion::functions::datetime::functions()
                .into_iter()
                .find(|f| f.name() == "to_timestamp")
                .expect("to_timestamp function not found"),
        }
    }
}

impl ScalarUDFImpl for ToTimestampUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        TO_TIMESTAMP_FUNCTION_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Timestamp(TimeUnit::Nanosecond, None))
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.len() != 1 {
            return internal_err!("to_timestamp expected 1 argument, got {}", args.len());
        }

        match args[0].data_type() {
            // call through to arrow cast kernel
            DataType::Int64 | DataType::Timestamp(_, _) => {
                args[0].cast_to(&DataType::Timestamp(TimeUnit::Nanosecond, None), None)
            }
            DataType::Utf8 => self.fallback.invoke(args),
            dt => internal_err!("to_timestamp does not support argument type '{dt}'"),
        }
    }
}

/// Implementation of to_timestamp
pub(crate) static TO_TIMESTAMP_UDF: Lazy<Arc<ScalarUDF>> =
    Lazy::new(|| Arc::new(ScalarUDF::from(ToTimestampUDF::new())));
