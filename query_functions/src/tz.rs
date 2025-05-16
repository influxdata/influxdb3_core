use std::str::FromStr;
use std::sync::{Arc, LazyLock};

use arrow::array::timezone::Tz;
use arrow::datatypes::{DataType, TimeUnit};

use datafusion::common::cast::as_timestamp_nanosecond_array;
use datafusion::common::{ExprSchema, internal_err, plan_err};
use datafusion::error::Result;
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, TIMEZONE_WILDCARD, TypeSignature,
    Volatility,
};
use datafusion::prelude::Expr;
use datafusion::scalar::ScalarValue;

pub(crate) const TZ_UDF_NAME: &str = "tz";

/// A static instance of the `tz` scalar function.
pub static TZ_UDF: LazyLock<Arc<ScalarUDF>> =
    LazyLock::new(|| Arc::new(ScalarUDF::from(TzUDF::default())));

/// A scalar UDF that converts a timestamp to a different timezone.
/// This function understands that input timestamps are in UTC despite
/// the lack of time zone information.
#[derive(Debug)]
pub struct TzUDF {
    signature: Signature,
}

impl TzUDF {
    fn is_valid_timezone(tz: &str) -> Result<()> {
        let iserr = Tz::from_str(tz);
        if iserr.is_err() {
            plan_err!("TZ requires a valid timezone string, got {:?}", tz)
        } else {
            Ok(())
        }
    }
}

impl Default for TzUDF {
    fn default() -> Self {
        let signatures = vec![
            TypeSignature::Exact(vec![DataType::Timestamp(TimeUnit::Nanosecond, None)]),
            TypeSignature::Exact(vec![DataType::Timestamp(
                TimeUnit::Nanosecond,
                Some(Arc::from(TIMEZONE_WILDCARD)),
            )]),
            TypeSignature::Exact(vec![
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                DataType::Utf8,
            ]),
            TypeSignature::Exact(vec![
                DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from(TIMEZONE_WILDCARD))),
                DataType::Utf8,
            ]),
        ];
        Self {
            signature: Signature::one_of(signatures, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for TzUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        TZ_UDF_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("tz should call return_type_from_exprs")
    }

    fn return_type_from_exprs(
        &self,
        args: &[Expr],
        _schema: &dyn ExprSchema,
        _arg_types: &[DataType],
    ) -> Result<DataType> {
        match args.len() {
            1 => Ok(DataType::Timestamp(
                TimeUnit::Nanosecond,
                Some(Arc::from("UTC")),
            )),
            2 => {
                let Expr::Literal(ScalarValue::Utf8(Some(tz))) = &args[1] else {
                    return plan_err!(
                        "tz requires its second argument to be a timezone string, got {:?}",
                        &args[1]
                    );
                };

                Self::is_valid_timezone(tz)?;

                Ok(DataType::Timestamp(
                    TimeUnit::Nanosecond,
                    Some(Arc::from(tz.as_str())),
                ))
            }
            _ => {
                plan_err!("tz expects 1 or 2 arguments, got {:?}", args.len())
            }
        }
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let new_tz = match args.len() {
            1 => "UTC",
            2 => match &args[1] {
                ColumnarValue::Scalar(ScalarValue::Utf8(tz)) => match tz {
                    Some(tz) => {
                        Self::is_valid_timezone(tz)?;
                        tz.as_str()
                    }
                    None => "UTC",
                },
                _ => {
                    return plan_err!(
                        "TZ expects the second argument to be a string defining the desired timezone"
                    );
                }
            },
            _ => return plan_err!("TZ expects one or two arguments"),
        };

        match &args[0] {
            ColumnarValue::Scalar(scalar) => match scalar {
                ScalarValue::TimestampNanosecond(epoch_nanos, _) => Ok(ColumnarValue::Scalar(
                    ScalarValue::TimestampNanosecond(*epoch_nanos, Some(Arc::from(new_tz))),
                )),
                scalar_value => plan_err!(
                    "TZ expects a nanosecond timestamp as the first parameter, got {:?}",
                    scalar_value.data_type()
                ),
            },
            ColumnarValue::Array(array) => {
                let dt = array.data_type();
                match dt {
                    DataType::Timestamp(_, _) => (), // This is what we want
                    _ => {
                        return plan_err!("TZ expects nanosecond timestamp data, got {:?}", dt);
                    }
                };

                Ok(ColumnarValue::Array(Arc::new(
                    as_timestamp_nanosecond_array(array.as_ref())?
                        .clone()
                        .with_timezone(new_tz),
                )))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Int64Array, TimestampNanosecondArray, TimestampNanosecondBuilder};

    use super::*;

    const TODAY: i64 = 1728668794000000000;

    #[test]
    fn default_args() {
        let udf = TzUDF::default();
        let args = vec![ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
            Some(TODAY),
            None,
        ))];
        let result = match udf.invoke_batch(&args, args.len()).unwrap() {
            ColumnarValue::Scalar(scalar) => scalar,
            _ => panic!("Expected scalar value"),
        };
        assert_eq!(
            result,
            ScalarValue::TimestampNanosecond(Some(TODAY), Some(Arc::from("UTC"))),
        );
    }

    #[test]
    fn utc() {
        let udf = TzUDF::default();
        let args = vec![ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
            Some(TODAY),
            Some(Arc::from("UTC")),
        ))];
        let result = match udf.invoke_batch(&args, args.len()).unwrap() {
            ColumnarValue::Scalar(scalar) => scalar,
            _ => panic!("Expected scalar value"),
        };
        assert_eq!(
            result,
            ScalarValue::TimestampNanosecond(Some(TODAY), Some(Arc::from("UTC"))),
        );

        let args = vec![
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(TODAY), None)),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("UTC".into()))),
        ];
        let result = match udf.invoke_batch(&args, args.len()).unwrap() {
            ColumnarValue::Scalar(scalar) => scalar,
            _ => panic!("Expected scalar value"),
        };
        assert_eq!(
            result,
            ScalarValue::TimestampNanosecond(Some(TODAY), Some(Arc::from("UTC"))),
        );
    }

    #[test]
    fn timezone() {
        let udf = TzUDF::default();
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(TODAY), None)),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("America/New_York".into()))),
        ];
        let result = match udf.invoke_batch(&args, args.len()).unwrap() {
            ColumnarValue::Scalar(scalar) => scalar,
            _ => panic!("Expected scalar value"),
        };
        assert_eq!(
            result,
            ScalarValue::TimestampNanosecond(Some(TODAY), Some(Arc::from("America/New_York"))),
        );

        let args = vec![
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
                Some(TODAY),
                Some(Arc::from("America/Denver")),
            )),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("America/New_York".into()))),
        ];
        let result = match udf.invoke_batch(&args, args.len()).unwrap() {
            ColumnarValue::Scalar(scalar) => scalar,
            _ => panic!("Expected scalar value"),
        };
        assert_eq!(
            result,
            ScalarValue::TimestampNanosecond(Some(TODAY), Some(Arc::from("America/New_York"))),
        );
    }

    #[test]
    fn offset() {
        let udf = TzUDF::default();
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(TODAY), None)),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("+05:00".into()))),
        ];
        let result = match udf.invoke_batch(&args, args.len()).unwrap() {
            ColumnarValue::Scalar(scalar) => scalar,
            _ => panic!("Expected scalar value"),
        };
        assert_eq!(
            result,
            ScalarValue::TimestampNanosecond(Some(TODAY), Some(Arc::from("+05:00"))),
        );

        let args = vec![
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
                Some(TODAY),
                Some(Arc::from("America/Denver")),
            )),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("+05:00".into()))),
        ];
        let result = match udf.invoke_batch(&args, args.len()).unwrap() {
            ColumnarValue::Scalar(scalar) => scalar,
            _ => panic!("Expected scalar value"),
        };
        assert_eq!(
            result,
            ScalarValue::TimestampNanosecond(Some(TODAY), Some(Arc::from("+05:00"))),
        );
    }

    #[test]
    fn arrays() {
        let udf = TzUDF::default();
        let arr = Arc::new(TimestampNanosecondArray::from(vec![
            Some(TODAY + 1_000_000_000),
            Some(TODAY + 2 * 1_000_000_000),
        ]));
        let args = vec![
            ColumnarValue::Array(arr),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("Europe/Brussels".to_owned()))),
        ];
        // let result = udf.invoke_batch(&args, args.len());
        let result = match udf.invoke_batch(&args, args.len()).unwrap() {
            ColumnarValue::Array(array) => as_timestamp_nanosecond_array(&array).unwrap().clone(),
            _ => panic!("Expected scalar value"),
        };

        let mut expected =
            TimestampNanosecondBuilder::new().with_timezone_opt(Some("Europe/Brussels"));
        expected.append_option(Some(TODAY + 1_000_000_000));
        expected.append_option(Some(TODAY + 2 * 1_000_000_000));

        assert_eq!(result, expected.finish());
    }

    #[test]
    fn wrong_timezone_error() {
        let udf = TzUDF::default();
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(TODAY), None)),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("NewYork".into()))),
        ];
        let result = udf.invoke_batch(&args, args.len());
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("TZ requires a valid timezone string, got \"NewYork\"")
        );
    }

    #[test]
    fn wrong_type_error() {
        let udf = TzUDF::default();
        let args = vec![ColumnarValue::Scalar(ScalarValue::TimestampSecond(
            Some(100),
            None,
        ))];
        let result = udf.invoke_batch(&args, args.len());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains(
            "TZ expects a nanosecond timestamp as the first parameter, got Timestamp(Second, None)"
        ));

        let udf = TzUDF::default();
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::TimestampSecond(Some(100), None)),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(100))),
        ];
        let result = udf.invoke_batch(&args, args.len());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains(
            "TZ expects the second argument to be a string defining the desired timezone"
        ));

        let udf = TzUDF::default();
        let arr = Arc::new(Int64Array::from(vec![
            Some(TODAY + 1_000_000_000),
            Some(TODAY + 2 * 1_000_000_000),
        ]));
        let args = vec![ColumnarValue::Array(arr)];

        let result = udf.invoke_batch(&args, args.len());
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("TZ expects nanosecond timestamp data, got Int64")
        );
    }
}
