use crate::schema::{FieldExt, SeriesColumnType, SeriesSchema};
use datafusion::common::{DFSchemaRef, Result};
use datafusion::logical_expr::{
    expr_vec_fmt, Expr, ExprSchemable, LogicalPlan, UserDefinedLogicalNodeCore,
};
use datafusion_util::ThenWithOpt;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::sync::Arc;

/// Pivot a wide table into a set of narrow tables, one for each field.
/// Any field which contains a null value is considered not present, and
/// will not be included in the output.
///
/// An input table of the form:
///
/// ```text
/// +------+------+------+--------+--------+
/// | time | tag1 | tag2 | field1 | field2 |
/// +------+------+------+--------+--------+
/// | t1   | a    | x    | 1      | 2      |
/// | t2   | b    | y    | 3      | null   |
/// | t3   | a    | z    | null   | 4      |
/// +------+------+------+--------+--------+
/// ```
///
/// Will be pivoted into:
///
/// ```text
/// +--------------+------+------+---------+-------+---------+
/// | _measurement | tag1 | tag2 | _field  | _time | _value  |
/// +--------------+------+------+---------+-------+---------+
/// | table        | a    | x    | field1  | t1    | 1       |
/// | table        | b    | y    | field1  | t2    | 3       |
/// | table        | a    | x    | field2  | t1    | 2       |
/// | table        | a    | z    | field2  | t3    | 4       |
/// +--------------+------+------+---------+-------+---------+
/// ```
///
/// The names `_measurement`, `_field`, `_time` and `_value` are
/// chosen as they match the names flux uses for these columns.
/// InfluxDB line protocol reserves all names beginning with an `_`
/// so there is no chance of these clashing with tag names.
///
/// The `_value` column is a union containing the 5 possible value
/// types that a field can have.
#[derive(Hash, PartialEq, Eq)]
pub(crate) struct SeriesPivot {
    pub(crate) input: LogicalPlan,
    pub(crate) schema: DFSchemaRef,
    pub(crate) measurement_expr: Expr,
    pub(crate) tag_exprs: Vec<Expr>,
    pub(crate) time_exprs: Vec<Expr>,
    pub(crate) field_exprs: Vec<Expr>,
}

// Manual impl because FieldsPivot has a Range and is not PartialOrd
impl PartialOrd for SeriesPivot {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.input
            .partial_cmp(&other.input)
            .then_with_opt(|| self.measurement_expr.partial_cmp(&other.measurement_expr))
            .then_with_opt(|| self.tag_exprs.partial_cmp(&other.tag_exprs))
            .then_with_opt(|| self.time_exprs.partial_cmp(&other.time_exprs))
            .then_with_opt(|| self.field_exprs.partial_cmp(&other.field_exprs))
    }
}

impl SeriesPivot {
    pub(crate) fn new(
        input: LogicalPlan,
        measurement_expr: Expr,
        tag_exprs: Vec<Expr>,
        time_exprs: Vec<Expr>,
        field_exprs: Vec<Expr>,
    ) -> Result<Self> {
        let schema = Self::schema(&input, &tag_exprs)?;
        Ok(Self {
            input,
            schema,
            measurement_expr,
            tag_exprs,
            time_exprs,
            field_exprs,
        })
    }

    fn schema(input: &LogicalPlan, tag_exprs: &[Expr]) -> Result<DFSchemaRef> {
        let tags = tag_exprs
            .iter()
            .map(|expr| {
                expr.to_field(input.schema())
                    .map(|(_, f)| f.as_ref().clone())
            })
            .collect::<Result<Vec<_>>>()?;
        let series_schema = SeriesSchema::new_tags(tags);
        Ok(Arc::new(series_schema.try_into()?))
    }
}

impl std::fmt::Debug for SeriesPivot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl std::fmt::Display for SeriesPivot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl UserDefinedLogicalNodeCore for SeriesPivot {
    fn name(&self) -> &str {
        "SeriesPivot"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        let mut exprs = vec![self.measurement_expr.clone()];
        exprs.extend_from_slice(&self.tag_exprs);
        exprs.extend_from_slice(&self.time_exprs);
        exprs.extend_from_slice(&self.field_exprs);
        exprs
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SeriesPivot: measurement_expr={}, tag_exprs=({}), time_exprs=({}), field_exprs=({})",
            self.measurement_expr,
            expr_vec_fmt!(&self.tag_exprs),
            expr_vec_fmt!(&self.time_exprs),
            expr_vec_fmt!(&self.field_exprs)
        )
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        let input = inputs.into_iter().next().unwrap();
        let mut exprs = exprs.into_iter();
        let measurement_expr = exprs.next().unwrap();
        let tag_exprs = (&mut exprs).take(self.tag_exprs.len()).collect::<Vec<_>>();
        let time_exprs = (&mut exprs).take(self.time_exprs.len()).collect::<Vec<_>>();
        let field_exprs = exprs.collect();
        let schema = Self::schema(&input, &tag_exprs)?;
        Ok(Self {
            input,
            schema,
            measurement_expr,
            tag_exprs,
            time_exprs,
            field_exprs,
        })
    }

    fn prevent_predicate_push_down_columns(&self) -> HashSet<String> {
        self.schema()
            .fields()
            .iter()
            .filter_map(|f| match f.series_column_type() {
                Some(SeriesColumnType::Tag) => None,
                _ => Some(f.name().to_string()),
            })
            .collect()
    }
}
