use arrow::datatypes::{DataType, Field, Fields, TimeUnit};
use datafusion::common::scalar::ScalarStructBuilder;
use datafusion::{
    error::{DataFusionError, Result as DataFusionResult},
    scalar::ScalarValue,
};
use std::sync::Arc;

/// Name of the output struct field that holds the value that was the main input into the selector, i.e. from which we
/// have selected the first/last/min/max value.
const STRUCT_FIELD_VALUE: &str = "value";

/// Name of the output struct field that holds the time.
const STRUCT_FIELD_TIME: &str = "time";

/// Name of the output struct field that holds other values that just point to the selected row but for which we do NOT
/// evaluate first/last/min/max.
fn struct_field_other(idx: usize) -> String {
    format!("other_{}", idx + 1)
}

/// Create [`Fields`] for the output struct.
fn make_struct_fields<'a>(
    value_type: &'a DataType,
    time_type: &'a DataType,
    other_types: impl IntoIterator<Item = &'a DataType>,
) -> Fields {
    let fields = [
        Field::new(STRUCT_FIELD_VALUE, value_type.clone(), true),
        Field::new(STRUCT_FIELD_TIME, time_type.clone(), true),
    ]
    .into_iter()
    .chain(
        other_types
            .into_iter()
            .enumerate()
            .map(|(i, dt)| Field::new(struct_field_other(i), (*dt).clone(), true)),
    )
    .collect::<Vec<_>>();

    fields.into()
}

/// Create output struct [`DataType`].
///
/// This will be a struct with the following fields:
///
/// - `value`
/// - `time`
/// - `other_{1..}` (depending on the other input to the selector function).
pub fn make_struct_datatype<'a>(
    value_type: &'a DataType,
    time_type: &'a DataType,
    other_types: impl IntoIterator<Item = &'a DataType>,
) -> DataType {
    DataType::Struct(make_struct_fields(value_type, time_type, other_types))
}

/// Create output struct [`ScalarValue`].
///
/// This will be a struct with the following fields:
///
/// - `value`
/// - `time`
/// - `other_{1..}` (depending on the other input to the selector function).
///
/// If `value` is `NULL`, the whole struct will be `NULL`. Neither the
/// time-based, nor value-based selectors will select a `value` that is
/// `NULL` so this is used as a proxy to indicate that no row met the
/// criteria to be selected.
pub fn make_struct_scalar<'a>(
    value: &'a ScalarValue,
    time: &'a ScalarValue,
    other: impl IntoIterator<Item = &'a ScalarValue>,
) -> DataFusionResult<ScalarValue> {
    let scalars: Vec<_> = [value.clone(), time.clone()]
        .into_iter()
        .chain(other.into_iter().cloned())
        .collect();

    let other_types: Vec<_> = scalars[2..].iter().map(|s| s.data_type()).collect();
    let fields = make_struct_fields(&value.data_type(), &time.data_type(), &other_types);

    if value.is_null() {
        Ok(ScalarStructBuilder::new_null(fields))
    } else {
        let mut builder = ScalarStructBuilder::new();

        for (field, scalar) in fields.iter().zip(scalars) {
            builder = builder.with_scalar(Arc::clone(field), scalar);
        }

        builder.build()
    }
}

/// Contains types of the aggregator.
#[derive(Debug)]
pub struct AggType<'a> {
    /// Type of the value that is fed into the selector and for which we select the row that satisfies first/last/min/max.
    pub value_type: &'a DataType,

    /// Type of the time that is fed into the selector and for which we select the row that satisfies first/last/min/max.
    pub time_type: &'a DataType,

    /// Types of the other values that are picked for the same row for which [value](Self::value_type) was selected. The do
    /// NOT influence the row selection in any way.
    pub other_types: Box<[&'a DataType]>,
}

impl<'a> AggType<'a> {
    /// Return type of the aggregator.
    ///
    /// See [`make_struct_datatype`].
    pub fn return_type(&self) -> DataType {
        make_struct_datatype(
            self.value_type,
            self.time_type,
            self.other_types.iter().copied(),
        )
    }

    /// Return the state in which the arguments are stored
    pub fn state_datatypes(&self) -> Vec<DataType> {
        [self.value_type.clone(), self.time_type.clone()]
            .into_iter()
            .chain(self.other_types.iter().copied().cloned())
            .collect()
    }

    /// Try to exract types from [`return_type`](Self::return_type).
    pub fn try_from_return_type(return_type: &'a DataType) -> DataFusionResult<Self> {
        if let DataType::Struct(fields) = return_type {
            if fields.len() < 2 {
                return Err(DataFusionError::Plan(format!(
                    "requires at least 2 arguments, got {}",
                    fields.len()
                )));
            }

            let value_type = fields[0].data_type();
            let time_type = fields[1].data_type();
            let other_types = fields[2..].iter().map(|f| f.data_type()).collect();

            match time_type {
                DataType::Timestamp(TimeUnit::Nanosecond, _) => Ok(Self {
                    value_type,
                    time_type,
                    other_types,
                }),
                _ => Err(DataFusionError::Plan(format!(
                    "second argument must be a timestamp, but got {time_type}"
                ))),
            }
        } else {
            Err(DataFusionError::Execution(format!(
                "Cannot create selector type from non-struct return type: {return_type}"
            )))
        }
    }

    /// Try to extract type from argument types that where passed into the aggregator UDF.
    ///
    /// The `name` is used to generated better error messages.
    pub fn try_from_arg_types(arg_types: &'a [DataType], name: &str) -> DataFusionResult<Self> {
        if arg_types.len() < 2 {
            return Err(DataFusionError::Plan(format!(
                "{} requires at least 2 arguments, got {}",
                name,
                arg_types.len()
            )));
        }

        let value_type = &arg_types[0];
        let time_type = &arg_types[1];
        let other_types = arg_types[2..].iter().collect();
        match time_type {
            DataType::Timestamp(TimeUnit::Nanosecond, _) => Ok(Self {
                value_type,
                time_type,
                other_types,
            }),
            _ => Err(DataFusionError::Plan(format!(
                "{name} second argument must be a timestamp, but got {time_type}"
            ))),
        }
    }
}
