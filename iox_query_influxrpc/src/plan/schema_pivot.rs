//! This module contains code for the "SchemaPivot" DataFusion
//! extension plan node
//!
//! A SchemaPivot node takes an arbitrary input like
//!
//!  ColA | ColB | ColC
//! ------+------+------
//!   1   | NULL | NULL
//!   2   | 2    | NULL
//!   3   | 2    | NULL
//!
//! And pivots it to a table with a single string column for any
//! columns that had non null values.
//!
//!   non_null_column
//!  -----------------
//!   "ColA"
//!   "ColB"
//!
//! This operation can be used to implement the tag_keys metadata query

use std::cmp::Ordering;
use std::fmt::{self, Debug};

use arrow::datatypes::{DataType, Field, Schema};
use datafusion::{
    common::{DFSchemaRef, ToDFSchema},
    error::Result,
    logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore},
};
use datafusion_util::ThenWithOpt;

/// Implements the SchemaPivot operation described in `make_schema_pivot`
#[derive(Hash, PartialEq, Eq)]
pub(crate) struct SchemaPivot {
    input: LogicalPlan,
    schema: DFSchemaRef,
    // these expressions represent what columns are "used" by this
    // node (in this case all of them) -- columns that are not used
    // are optimized away by datafusion.
    exprs: Vec<Expr>,
}

// Manual impl because SchemaPivot has a Range and is not PartialOrd
impl PartialOrd for SchemaPivot {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.input
            .partial_cmp(&other.input)
            .then_with_opt(|| self.exprs.partial_cmp(&other.exprs))
    }
}

impl SchemaPivot {
    pub(crate) fn new(input: LogicalPlan) -> Self {
        let schema = make_schema_pivot_output_schema();

        // Form exprs that refer to all of our input columns (so that
        // datafusion knows not to opimize them away)
        let exprs = input
            .schema()
            .iter()
            .map(|(qualifier, field)| {
                Expr::Column(datafusion::common::Column::from((
                    qualifier,
                    field.as_ref(),
                )))
            })
            .collect::<Vec<_>>();

        Self {
            input,
            schema,
            exprs,
        }
    }
}

impl Debug for SchemaPivot {
    /// Use explain format for the Debug format.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl UserDefinedLogicalNodeCore for SchemaPivot {
    fn name(&self) -> &str {
        "SchemaPivot"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    /// Schema for Pivot is a single string
    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        self.exprs.clone()
    }

    /// For example: `SchemaPivot`
    fn fmt_for_explain(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        assert_eq!(inputs.len(), 1, "SchemaPivot: input sizes inconistent");
        assert_eq!(
            exprs.len(),
            self.exprs.len(),
            "SchemaPivot: expression sizes inconistent"
        );
        Ok(Self::new(inputs[0].clone()))
    }
}

/// Create the schema describing the output
pub(crate) fn make_schema_pivot_output_schema() -> DFSchemaRef {
    let nullable = false;
    Schema::new(vec![Field::new(
        "non_null_column",
        DataType::Utf8,
        nullable,
    )])
    .to_dfschema_ref()
    .unwrap()
}
