use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{DFSchema, DFSchemaRef, Result, ToDFSchema, plan_err};
use datafusion::logical_expr::expr::AggregateFunction;
use datafusion::logical_expr::{
    AggregateUDF, EmptyRelation, Expr, Extension, LogicalPlan, LogicalPlanBuilder, Projection,
    Union, Values,
};
use datafusion::sql::sqlparser::ast::NullTreatment;
use std::sync::Arc;

mod fields_pivot;
mod schema_pivot;
mod series_pivot;

pub(crate) use fields_pivot::FieldsPivot;
pub(crate) use schema_pivot::SchemaPivot;
pub(crate) use series_pivot::SeriesPivot;

pub(crate) use fields_pivot::make_schema as fields_pivot_schema;
#[cfg(test)]
pub(crate) use schema_pivot::make_schema_pivot_output_schema as schema_pivot_schema;

use crate::STRING_VALUE_COLUMN_NAME;
use crate::schema::SeriesSchema;

/// The type of aggregation to perform on all fields in a
/// aggregate_field operation.
///
/// Selector variants take a time expression as a second
/// input and return a union value containing the
/// selected value and the time associated with that value.
pub(crate) enum FieldAggregator {
    Aggregator(Arc<AggregateUDF>),
    Selector(Arc<AggregateUDF>),
}

impl FieldAggregator {
    fn aggr_expr(&self, field_expr: impl Into<Expr>, time_expr: impl Into<Expr>) -> Expr {
        let (udaf, args) = match self {
            Self::Aggregator(udaf) => (Arc::clone(udaf), vec![field_expr.into()]),
            Self::Selector(udaf) => (Arc::clone(udaf), vec![field_expr.into(), time_expr.into()]),
        };
        Expr::AggregateFunction(AggregateFunction {
            func: udaf,
            args,
            distinct: false,
            filter: None,
            order_by: None,
            null_treatment: Some(NullTreatment::IgnoreNulls),
        })
    }
}

/// Extention trait for LogicalPlanBuilder which adds influxrpc
/// specific nodes to the plan.
pub(crate) trait LogicalPlanBuilderExt: Sized {
    /// Create a new logical plan node that represents the known
    /// values using the provided schema. The values may be empty.
    fn known_values(values: Vec<Vec<Expr>>, schema: DFSchemaRef) -> Result<Self>;

    /// Pivot the input fields into a single row per field.
    /// Each row contains the field name, the data type enum
    /// value and the value of the time column when the field
    /// is first seen to have a non-null value.
    fn fields_pivot(self) -> Result<Self>;

    /// Create the union of two plans that have been pivoted into the
    /// series representation. The resulting plan will contain the union
    /// of all the fields in the two input plans.
    fn union_series(plans: Vec<LogicalPlan>) -> Result<Self>;

    /// Create a logical plan node that represents an empty series. The schema
    /// will include the required `_measurement`, `_field`, `_time` and `_value`
    /// columns.
    fn empty_series() -> Self;

    /// Pivot the input into a series representation with a row per non-null
    /// field in the input.
    fn series_pivot(
        self,
        measurement_expr: impl Into<Expr>,
        tag_exprs: impl IntoIterator<Item = impl Into<Expr>>,
        time_exprs: impl IntoIterator<Item = impl Into<Expr>>,
        field_exprs: impl IntoIterator<Item = impl Into<Expr>>,
    ) -> Result<Self>;

    /// Aggregate the fields in the input plan into a single row per group
    /// using the specified aggregate function.
    fn aggregate_fields(
        self,
        group_exprs: impl IntoIterator<Item = impl Into<Expr>>,
        time_expr: impl Into<Expr>,
        time_aggregator: Option<Arc<AggregateUDF>>,
        field_exprs: impl IntoIterator<Item = impl Into<Expr>>,
        field_aggregator: FieldAggregator,
    ) -> Result<Self>;

    /// Attach a SchemaPivot node to a builder. A SchemaPivot
    /// node takes an arbitrary input like
    ///  ColA | ColB | ColC
    /// ------+------+------
    ///   1   | NULL | NULL
    ///   2   | 2    | NULL
    ///   3   | 2    | NULL
    ///
    /// And pivots it to a table with a single string column for any
    /// columns that had non null values.
    ///
    ///   non_null_column
    ///  -----------------
    ///   "ColA"
    ///   "ColB"
    fn schema_pivot(self) -> Result<Self>;
}

impl LogicalPlanBuilderExt for LogicalPlanBuilder {
    fn known_values(values: Vec<Vec<Expr>>, schema: DFSchemaRef) -> Result<Self> {
        for (i, row) in values.iter().enumerate() {
            if row.len() != schema.fields().len() {
                return plan_err!(
                    "Values list in row {i} has length {} but schema has length {}",
                    row.len(),
                    schema.fields().len()
                );
            }
        }

        let node = if values.is_empty() {
            LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema,
            })
        } else {
            LogicalPlan::Values(Values { schema, values })
        };

        Ok(Self::from(node))
    }

    fn fields_pivot(self) -> Result<Self> {
        self.build()
            .and_then(FieldsPivot::new)
            .map(Arc::new)
            .map(|node| LogicalPlan::Extension(Extension { node }))
            .map(Self::from)
    }

    fn union_series(plans: Vec<LogicalPlan>) -> Result<Self> {
        if plans.is_empty() {
            return Ok(Self::empty_series());
        }
        if plans.len() == 1 {
            return Ok(Self::from(plans.into_iter().next().unwrap()));
        }
        let mut schema = SeriesSchema::empty();
        let plan_schemas = plans
            .iter()
            .map(|p| {
                SeriesSchema::try_from(p.schema().as_arrow()).inspect(|s| schema = schema.merge(s))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let projections = plan_schemas
            .into_iter()
            .map(|s| schema.projection(&s))
            .collect::<Vec<_>>();

        let schema = Arc::new(schema.try_into()?);
        let plans = plans
            .into_iter()
            .zip(projections)
            .map(|(p, exprs)| {
                Ok(Arc::new(LogicalPlan::Projection(
                    Projection::try_new_with_schema(exprs, Arc::new(p), Arc::clone(&schema))?,
                )))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Self::from(LogicalPlan::Union(Union {
            inputs: plans,
            schema,
        })))
    }

    fn empty_series() -> Self {
        Self::from(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::try_from(SeriesSchema::empty()).unwrap()),
        }))
    }

    fn series_pivot(
        self,
        measurement_expr: impl Into<Expr>,
        tag_exprs: impl IntoIterator<Item = impl Into<Expr>>,
        time_expr: impl IntoIterator<Item = impl Into<Expr>>,
        field_exprs: impl IntoIterator<Item = impl Into<Expr>>,
    ) -> Result<Self> {
        let input = self.build()?;
        let node = Arc::new(SeriesPivot::new(
            input,
            measurement_expr.into(),
            tag_exprs.into_iter().map(|expr| expr.into()).collect(),
            time_expr.into_iter().map(|expr| expr.into()).collect(),
            field_exprs.into_iter().map(|expr| expr.into()).collect(),
        )?);
        let plan = LogicalPlan::Extension(Extension { node });
        Ok(Self::from(plan))
    }

    fn aggregate_fields(
        self,
        group_exprs: impl IntoIterator<Item = impl Into<Expr>>,
        time_expr: impl Into<Expr>,
        time_aggregator: Option<Arc<AggregateUDF>>,
        field_exprs: impl IntoIterator<Item = impl Into<Expr>>,
        field_aggregator: FieldAggregator,
    ) -> Result<Self> {
        let time_expr = time_expr.into();

        let mut aggr_expr = field_exprs
            .into_iter()
            .map(|expr| {
                let expr = expr.into();
                let alias = expr.name_for_alias()?;
                Ok(field_aggregator
                    .aggr_expr(expr, time_expr.clone())
                    .alias(alias))
            })
            .collect::<Result<Vec<_>>>()?;

        if let Some(time_aggregator) = time_aggregator {
            let alias = time_expr.name_for_alias()?;
            aggr_expr.push(
                Expr::AggregateFunction(AggregateFunction {
                    func: time_aggregator,
                    args: vec![time_expr],
                    distinct: false,
                    filter: None,
                    order_by: None,
                    null_treatment: Some(NullTreatment::IgnoreNulls),
                })
                .alias(alias),
            );
        }

        self.aggregate(group_exprs, aggr_expr)
    }

    fn schema_pivot(self) -> Result<Self> {
        self.build()
            .map(SchemaPivot::new)
            .map(Arc::new)
            .map(|node| LogicalPlan::Extension(Extension { node }))
            .map(Self::from)
    }
}

/// Schema for a single column that contains string values.
pub(crate) fn string_value_schema() -> DFSchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        STRING_VALUE_COLUMN_NAME,
        DataType::Utf8,
        false,
    )]))
    .to_dfschema_ref()
    .unwrap()
}
