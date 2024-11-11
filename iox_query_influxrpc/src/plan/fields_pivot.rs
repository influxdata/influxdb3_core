use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{DFSchemaRef, DataFusionError, Result, ToDFSchema};
use datafusion::logical_expr::{expr_vec_fmt, Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion_util::{AsExpr, ThenWithOpt};
use schema::Schema as IoxSchema;
use std::cmp::Ordering;
use std::sync::Arc;

/// A plan which outputs one row for each non-null field in the input.
/// The first time a field is seen to have a non-null value an output
/// row will be generated containing the key (field name), the type,
/// and the timestamp.
///
/// The type column contains the i32 representation for the [schema::InfluxFieldType]
/// enum.
///
/// The timestamp column contains the i64 reprepsentation of the input time
/// column for the first non-null value of the field.
///
/// It is expected that the input will be sorted in descending time order.
#[derive(Hash, PartialEq, Eq)]
pub(crate) struct FieldsPivot {
    input: LogicalPlan,
    schema: DFSchemaRef,
    exprs: Vec<Expr>,
}

// Manual impl because FieldsPivot has a Range and is not PartialOrd
impl PartialOrd for FieldsPivot {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.input
            .partial_cmp(&other.input)
            .then_with_opt(|| self.exprs.partial_cmp(&other.exprs))
    }
}

impl FieldsPivot {
    pub(crate) fn new(input: LogicalPlan) -> Result<Self> {
        let exprs = Self::expressions_for_input(&input)?;
        Ok(Self {
            input,
            schema: make_schema(),
            exprs,
        })
    }

    fn expressions_for_input(input: &LogicalPlan) -> Result<Vec<Expr>> {
        let schema = IoxSchema::try_from(Arc::clone(input.schema().as_ref().as_ref()))
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(schema
            .time_iter()
            .chain(schema.fields_iter())
            .map(|field| field.name().as_expr().is_not_null())
            .collect())
    }
}

impl UserDefinedLogicalNodeCore for FieldsPivot {
    fn name(&self) -> &str {
        "FieldsPivot"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        self.exprs.clone()
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FieldsPivot: exprs=({})", expr_vec_fmt!(self.exprs))
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        Ok(Self {
            input: inputs.into_iter().next().unwrap(),
            schema: Arc::clone(&self.schema),
            exprs,
        })
    }
}

impl std::fmt::Debug for FieldsPivot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.fmt_for_explain(f)
    }
}

/// Create a schema for the output of the FieldsPivotExec node.
pub(crate) fn make_schema() -> DFSchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("type", DataType::Int32, true),
        Field::new("timestamp", DataType::Int64, true),
    ]))
    .to_dfschema_ref()
    .unwrap()
}
