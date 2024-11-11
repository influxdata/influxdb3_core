//! This module contains code that implements
//! a gap-filling extension to DataFusion

mod algo;
mod buffered_input;
mod date_bin_gap_expander;
mod date_bin_wallclock_gap_expander;
#[cfg(test)]
mod exec_tests;
mod gap_expander;
mod params;
mod stream;

use self::stream::GapFillStream;
use arrow::{compute::SortOptions, datatypes::SchemaRef};
use datafusion::{
    common::DFSchemaRef,
    error::{DataFusionError, Result},
    execution::{
        context::{SessionState, TaskContext},
        memory_pool::MemoryConsumer,
    },
    logical_expr::{LogicalPlan, ScalarUDF, UserDefinedLogicalNodeCore},
    physical_expr::{EquivalenceProperties, PhysicalSortExpr, PhysicalSortRequirement},
    physical_plan::{
        expressions::Column,
        metrics::{BaselineMetrics, ExecutionPlanMetricsSet},
        DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, ExecutionPlanProperties,
        Partitioning, PhysicalExpr, PlanProperties, SendableRecordBatchStream, Statistics,
    },
    prelude::Expr,
    scalar::ScalarValue,
};
use datafusion_util::ThenWithOpt;
pub use gap_expander::{ExpandedValue, GapExpander};
use std::cmp::Ordering;
use std::{
    convert::Infallible,
    fmt::{self, Debug},
    ops::{Bound, Range},
    sync::Arc,
};

/// A logical node that represents the gap filling operation.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct GapFill {
    /// The incoming logical plan
    pub input: Arc<LogicalPlan>,
    /// Grouping expressions
    pub group_expr: Vec<Expr>,
    /// Aggregate expressions
    pub aggr_expr: Vec<Expr>,
    /// Parameters to configure the behavior of the
    /// gap-filling operation
    pub params: GapFillParams,
}

// Manual impl because GapFillParams has a Range and is not PartialOrd
impl PartialOrd for GapFill {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        other
            .input
            .partial_cmp(&self.input)
            .then_with_opt(|| self.group_expr.partial_cmp(&other.group_expr))
            .then_with_opt(|| self.aggr_expr.partial_cmp(&other.aggr_expr))
    }
}

/// Parameters to the GapFill operation
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct GapFillParams {
    /// The name of the UDF that provides the DATE_BIN like functionality.
    pub date_bin_udf: Arc<str>,
    /// The stride argument from the call to DATE_BIN_GAPFILL
    pub stride: Expr,
    /// The source time column
    pub time_column: Expr,
    /// The origin argument from the call to DATE_BIN_GAPFILL
    pub origin: Option<Expr>,
    /// The time range of the time column inferred from predicates
    /// in the overall query. The lower bound may be [`Bound::Unbounded`]
    /// which implies that gap-filling should just start from the
    /// first point in each series.
    pub time_range: Range<Bound<Expr>>,
    /// What to do when filling aggregate columns.
    /// The first item in the tuple will be the column
    /// reference for the aggregate column.
    pub fill_strategy: Vec<(Expr, FillStrategy)>,
}

/// Describes how to fill gaps in an aggregate column.
#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd)]
pub enum FillStrategy {
    /// Fill with the given 'default value' - this includes a default value of Null.
    Default(ScalarValue),
    /// Fill with the most recent value in the input column.
    /// Null values in the input are preserved.
    PrevNullAsIntentional,
    /// Fill with the most recent non-null value in the input column.
    /// This is the InfluxQL behavior for `FILL(PREVIOUS)`.
    PrevNullAsMissing,
    /// Fill the gaps between points linearly.
    /// Null values will not be considered as missing, so two non-null values
    /// with a null in between will not be filled.
    LinearInterpolate,
}

impl FillStrategy {
    // used with both `Expr` and `PhysicalExpr`, normally to display this within `explain` queries
    fn display_with_expr(&self, expr: &impl fmt::Display) -> String {
        match self {
            Self::PrevNullAsIntentional => format!("LOCF(null-as-intentional, {expr})"),
            Self::PrevNullAsMissing => format!("LOCF({expr})"),
            Self::LinearInterpolate => format!("INTERPOLATE({expr})"),
            Self::Default(scalar) if scalar.is_null() => expr.to_string(),
            Self::Default(val) => format!("COALESCE({val}, {expr})"),
        }
    }
}

impl GapFillParams {
    // Extract the expressions so they can be optimized.
    fn expressions(&self) -> Vec<Expr> {
        let mut exprs = vec![self.stride.clone(), self.time_column.clone()];
        if let Some(e) = self.origin.as_ref() {
            exprs.push(e.clone())
        }
        if let Some(start) = bound_extract(&self.time_range.start) {
            exprs.push(start.clone());
        }
        exprs.push(
            bound_extract(&self.time_range.end)
                .unwrap_or_else(|| panic!("upper time bound is required"))
                .clone(),
        );
        exprs
    }

    #[allow(clippy::wrong_self_convention)] // follows convention of UserDefinedLogicalNode
    fn from_template(&self, exprs: &[Expr], aggr_expr: &[Expr]) -> Self {
        let mut e_iter = exprs.iter().cloned();

        // we only need the third item in the iter if `Some(_) == self.origin` so that's why we
        // match against `None | Some(Some(_))` - that ensures either origin is None, or origin is
        // Some and e_iter.next() is Some
        let (Some(stride), Some(time_column), origin @ (None | Some(Some(_)))) = (
            e_iter.next(),
            e_iter.next(),
            self.origin.as_ref().map(|_| e_iter.next()),
        ) else {
            panic!("`exprs` should contain at least a stride, source, and origin");
        };

        let origin = origin.flatten();

        let time_range = match try_map_range(&self.time_range, |b| {
            try_map_bound(b.as_ref(), |_| {
                Ok::<_, Infallible>(e_iter.next().expect("expr count should match template"))
            })
        }) {
            Ok(tr) => tr,
            Err(infallible) => match infallible {},
        };

        let fill_strategy = aggr_expr
            .iter()
            .cloned()
            .zip(
                self.fill_strategy
                    .iter()
                    .map(|(_expr, fill_strategy)| fill_strategy.clone()),
            )
            .collect();

        Self {
            date_bin_udf: Arc::clone(&self.date_bin_udf),
            stride,
            time_column,
            origin,
            time_range,
            fill_strategy,
        }
    }

    // Find the expression that matches `e` and replace its fill strategy.
    // If such an expression is found, return the old strategy, and `None` otherwise.
    fn replace_fill_strategy(&mut self, e: &Expr, mut fs: FillStrategy) -> Option<FillStrategy> {
        for expr_fs in &mut self.fill_strategy {
            if &expr_fs.0 == e {
                std::mem::swap(&mut fs, &mut expr_fs.1);
                return Some(fs);
            }
        }
        None
    }
}

impl GapFill {
    /// Create a new gap-filling operator.
    pub fn try_new(
        input: Arc<LogicalPlan>,
        group_expr: Vec<Expr>,
        aggr_expr: Vec<Expr>,
        params: GapFillParams,
    ) -> Result<Self> {
        if params.time_range.end == Bound::Unbounded {
            return Err(DataFusionError::Internal(
                "missing upper bound in GapFill time range".to_string(),
            ));
        }

        Ok(Self {
            input,
            group_expr,
            aggr_expr,
            params,
        })
    }

    // Find the expression that matches `e` and replace its fill strategy.
    // If such an expression is found, return the old strategy, and `None` otherwise.
    pub(crate) fn replace_fill_strategy(
        &mut self,
        e: &Expr,
        fs: FillStrategy,
    ) -> Option<FillStrategy> {
        self.params.replace_fill_strategy(e, fs)
    }
}

impl UserDefinedLogicalNodeCore for GapFill {
    fn name(&self) -> &str {
        "GapFill"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.input.as_ref()]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        self.group_expr
            .iter()
            .chain(&self.aggr_expr)
            .chain(&self.params.expressions())
            .cloned()
            .collect()
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let aggr_expr: String = self
            .params
            .fill_strategy
            .iter()
            .map(|(e, fs)| fs.display_with_expr(e))
            .collect::<Vec<String>>()
            .join(", ");

        let group_expr = self
            .group_expr
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<String>>()
            .join(", ");

        write!(
            f,
            "{}: groupBy=[{group_expr}], aggr=[[{aggr_expr}]], time_column={}, stride={}, range={:?}",
            self.name(),
            self.params.time_column,
            self.params.stride,
            self.params.time_range,
        )
    }

    fn with_exprs_and_inputs(
        &self,
        mut group_expr: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        let plan = inputs[0].clone();
        let mut aggr_expr = group_expr.split_off(self.group_expr.len());
        let param_expr = aggr_expr.split_off(self.aggr_expr.len());
        let params = self.params.from_template(&param_expr, &aggr_expr);
        Self::try_new(Arc::new(plan), group_expr, aggr_expr, params)
    }

    /// Projection pushdown is an optmization that pushes a `Projection` node further down
    /// into the query plan.
    ///
    /// So instead of:
    ///
    /// ```text
    /// Projection: a, b, c
    ///   GapFill: ...
    ///     Aggregate: ...
    ///       TableScan: table
    /// ```
    ///
    /// You will get:
    ///
    /// ```text
    /// GapFill: ...
    ///   Aggregate: ...
    ///     TableScan: table, projection=[a, b, c]
    /// ```
    /// By default, DataFusion will not optimize projection pushdown through a user defined node.
    /// Overriding this trait method allows projection pushdown.
    ///
    /// When `HandleGapfill` was an [`OptimizerRule`] this was not needed, because
    /// the [`GapFill`] node was inserted *after* projection pushdown.
    ///
    /// Now that `HandleGapFill` is an [`AnalyzerRule`], it inserts the [`GapFill`] node
    /// *before* projection pushdown.
    ///
    /// [`OptimizerRule`]: datafusion::optimizer::OptimizerRule
    /// [`AnalyzerRule`]: datafusion::optimizer::AnalyzerRule
    fn necessary_children_exprs(&self, output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        Some(vec![Vec::from(output_columns)])
    }
}

/// Called by the extension planner to plan a [GapFill] node.
pub(crate) fn plan_gap_fill(
    session_state: &SessionState,
    gap_fill: &GapFill,
    logical_inputs: &[&LogicalPlan],
    physical_inputs: &[Arc<dyn ExecutionPlan>],
) -> Result<GapFillExec> {
    let input_dfschema = match logical_inputs {
        [input] => input.schema().as_ref(),
        _ => {
            return Err(DataFusionError::Internal(format!(
                "GapFillExec: wrong number of logical inputs; expect 1, found {}",
                logical_inputs.len()
            )))
        }
    };

    let phys_input = match physical_inputs {
        [input] => input,
        _ => {
            return Err(DataFusionError::Internal(format!(
                "GapFillExec: wrong number of physical inputs; expected 1, found {}",
                physical_inputs.len()
            )))
        }
    };

    let input_schema = phys_input.schema();
    let input_schema = input_schema.as_ref();

    let group_expr: Result<Vec<_>> = gap_fill
        .group_expr
        .iter()
        .map(|e| session_state.create_physical_expr(e.clone(), input_dfschema))
        .collect();
    let group_expr = group_expr?;

    let aggr_expr: Result<Vec<_>> = gap_fill
        .aggr_expr
        .iter()
        .map(|e| session_state.create_physical_expr(e.clone(), input_dfschema))
        .collect();
    let aggr_expr = aggr_expr?;

    let Some(logical_time_column) = gap_fill.params.time_column.try_as_col() else {
        return Err(DataFusionError::Internal(
            "GapFillExec: time column must be a `Column` expression".to_string(),
        ));
    };
    let time_column = Column::new_with_schema(&logical_time_column.name, input_schema)?;

    let stride =
        session_state.create_physical_expr(gap_fill.params.stride.clone(), input_dfschema)?;

    let time_range = &gap_fill.params.time_range;
    let time_range = try_map_range(time_range, |b| {
        try_map_bound(b.as_ref(), |e| {
            session_state.create_physical_expr(e.clone(), input_dfschema)
        })
    })?;

    let origin = gap_fill
        .params
        .origin
        .as_ref()
        .map(|e| session_state.create_physical_expr(e.clone(), input_dfschema))
        .transpose()?;

    let fill_strategy = gap_fill
        .params
        .fill_strategy
        .iter()
        .map(|(e, fs)| {
            Ok((
                session_state.create_physical_expr(e.clone(), input_dfschema)?,
                fs.clone(),
            ))
        })
        .collect::<Result<Vec<(Arc<dyn PhysicalExpr>, FillStrategy)>>>()?;

    let date_bin_udf = session_state
        .scalar_functions()
        .get(gap_fill.params.date_bin_udf.as_ref())
        .cloned()
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "ScalarUDF {} not found",
                gap_fill.params.date_bin_udf
            ))
        })?;

    let params = GapFillExecParams {
        date_bin_udf,
        stride,
        time_column,
        origin,
        time_range,
        fill_strategy,
    };
    GapFillExec::try_new(Arc::clone(phys_input), group_expr, aggr_expr, params)
}

fn try_map_range<T, U, E, F>(tr: &Range<T>, mut f: F) -> Result<Range<U>, E>
where
    F: FnMut(&T) -> Result<U, E>,
{
    Ok(Range {
        start: f(&tr.start)?,
        end: f(&tr.end)?,
    })
}

fn try_map_bound<T, U, E, F>(bt: Bound<T>, mut f: F) -> Result<Bound<U>, E>
where
    F: FnMut(T) -> Result<U, E>,
{
    Ok(match bt {
        Bound::Excluded(t) => Bound::Excluded(f(t)?),
        Bound::Included(t) => Bound::Included(f(t)?),
        Bound::Unbounded => Bound::Unbounded,
    })
}

fn bound_extract<T>(b: &Bound<T>) -> Option<&T> {
    match b {
        Bound::Included(t) | Bound::Excluded(t) => Some(t),
        Bound::Unbounded => None,
    }
}

/// A physical node for the gap-fill operation.
pub struct GapFillExec {
    input: Arc<dyn ExecutionPlan>,
    // The group by expressions from the original aggregation node.
    group_expr: Vec<Arc<dyn PhysicalExpr>>,
    // The aggregate expressions from the original aggregation node.
    aggr_expr: Vec<Arc<dyn PhysicalExpr>>,
    // The sort expressions for the required sort order of the input:
    // all of the group exressions, with the time column being last.
    sort_expr: Vec<PhysicalSortExpr>,
    // Parameters (besides streaming data) to gap filling
    params: GapFillExecParams,
    /// Metrics reporting behavior during execution.
    metrics: ExecutionPlanMetricsSet,
    /// Cache holding plan properties like equivalences, output partitioning, output ordering etc.
    cache: PlanProperties,
}

#[derive(Clone, Debug)]
struct GapFillExecParams {
    /// The scalar function used to bin the timestamps.
    date_bin_udf: Arc<ScalarUDF>,
    /// The uniform interval of incoming timestamps
    stride: Arc<dyn PhysicalExpr>,
    /// The timestamp column produced by date_bin
    time_column: Column,
    /// The origin argument from the all to DATE_BIN_GAPFILL
    origin: Option<Arc<dyn PhysicalExpr>>,
    /// The time range of source input to DATE_BIN_GAPFILL.
    /// Inferred from predicates in the overall query.
    time_range: Range<Bound<Arc<dyn PhysicalExpr>>>,
    /// What to do when filling aggregate columns.
    /// The 0th element in each tuple is the aggregate column.
    fill_strategy: Vec<(Arc<dyn PhysicalExpr>, FillStrategy)>,
}

impl GapFillExec {
    fn try_new(
        input: Arc<dyn ExecutionPlan>,
        group_expr: Vec<Arc<dyn PhysicalExpr>>,
        aggr_expr: Vec<Arc<dyn PhysicalExpr>>,
        params: GapFillExecParams,
    ) -> Result<Self> {
        let sort_expr = {
            let mut sort_expr: Vec<_> = group_expr
                .iter()
                .map(|expr| PhysicalSortExpr {
                    expr: Arc::clone(expr),
                    options: SortOptions::default(),
                })
                .collect();

            // Ensure that the time column is the last component in the sort
            // expressions.
            let time_idx = group_expr
                .iter()
                .enumerate()
                .find(|(_i, e)| {
                    e.as_any()
                        .downcast_ref::<Column>()
                        .map_or(false, |c| c.index() == params.time_column.index())
                })
                .map(|(i, _)| i);

            if let Some(time_idx) = time_idx {
                let last_elem = sort_expr.len() - 1;
                sort_expr.swap(time_idx, last_elem);
            } else {
                return Err(DataFusionError::Internal(
                    "could not find time column for GapFillExec".to_string(),
                ));
            }

            sort_expr
        };

        let cache = Self::compute_properties(&input);

        Ok(Self {
            input,
            group_expr,
            aggr_expr,
            sort_expr,
            params,
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
        })
    }

    /// This function creates the cache object that stores the plan properties such as equivalence properties, partitioning, ordering, etc.
    fn compute_properties(input: &Arc<dyn ExecutionPlan>) -> PlanProperties {
        let schema = input.schema();
        let eq_properties = match input.properties().output_ordering() {
            None => EquivalenceProperties::new(schema),
            Some(output_ordering) => {
                EquivalenceProperties::new_with_orderings(schema, &[output_ordering.to_vec()])
            }
        };

        let output_partitioning = Partitioning::UnknownPartitioning(1);

        PlanProperties::new(eq_properties, output_partitioning, input.execution_mode())
    }
}

impl Debug for GapFillExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "GapFillExec")
    }
}

impl ExecutionPlan for GapFillExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        // It seems like it could be possible to partition on all the
        // group keys except for the time expression. For now, keep it simple.
        vec![Distribution::SinglePartition]
    }

    fn required_input_ordering(&self) -> Vec<Option<Vec<PhysicalSortRequirement>>> {
        vec![Some(PhysicalSortRequirement::from_sort_exprs(
            &self.sort_expr,
        ))]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match children.as_slice() {
            [child] => Ok(Arc::new(Self::try_new(
                Arc::clone(child),
                self.group_expr.clone(),
                self.aggr_expr.clone(),
                self.params.clone(),
            )?)),
            _ => Err(DataFusionError::Internal(format!(
                "GapFillExec wrong number of children: expected 1, found {}",
                children.len()
            ))),
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "GapFillExec invalid partition {partition}, there can be only one partition"
            )));
        }

        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        let output_batch_size = context.session_config().batch_size();
        let reservation = MemoryConsumer::new(format!("GapFillExec[{partition}]"))
            .register(context.memory_pool());
        let input_stream = self.input.execute(partition, context)?;
        Ok(Box::pin(GapFillStream::try_new(
            self,
            output_batch_size,
            input_stream,
            reservation,
            baseline_metrics,
        )?))
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema()))
    }
}

impl DisplayAs for GapFillExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let group_expr: Vec<_> = self.group_expr.iter().map(|e| e.to_string()).collect();
                let aggr_expr: Vec<_> = self
                    .params
                    .fill_strategy
                    .iter()
                    .map(|(e, fs)| fs.display_with_expr(e))
                    .collect();

                let time_range = match try_map_range(&self.params.time_range, |b| {
                    try_map_bound(b.as_ref(), |e| Ok::<_, Infallible>(e.to_string()))
                }) {
                    Ok(tr) => tr,
                    Err(infallible) => match infallible {},
                };

                write!(
                    f,
                    "GapFillExec: group_expr=[{}], aggr_expr=[{}], stride={}, time_range={:?}",
                    group_expr.join(", "),
                    aggr_expr.join(", "),
                    self.params.stride,
                    time_range
                )
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::ops::{Bound, Range};

    use crate::{
        exec::Executor,
        test::{format_execution_plan, format_logical_plan},
    };

    use super::*;
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::{
        common::DFSchema,
        datasource::empty::EmptyTable,
        error::Result,
        logical_expr::{logical_plan, ExprSchemable, Extension, UserDefinedLogicalNode},
        prelude::{col, lit},
        scalar::ScalarValue,
    };
    use datafusion_util::lit_timestamptz_nano;

    use test_helpers::assert_error;

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("loc", DataType::Utf8, false),
            Field::new("temp", DataType::Float64, false),
        ])
    }

    fn table_scan() -> Result<LogicalPlan> {
        let schema = schema();
        logical_plan::table_scan(Some("temps"), &schema, None)?.build()
    }

    fn fill_strategy_null(cols: Vec<Expr>, schema: &DFSchema) -> Vec<(Expr, FillStrategy)> {
        cols.into_iter()
            .map(|e| {
                e.get_type(schema)
                    .and_then(|dt| dt.try_into())
                    .map(|null| (e, FillStrategy::Default(null)))
            })
            .collect::<Result<Vec<_>>>()
            .unwrap()
    }

    #[test]
    fn test_try_new_errs() {
        let scan = table_scan().unwrap();
        let schema = Arc::clone(scan.schema());
        let result = GapFill::try_new(
            Arc::new(scan),
            vec![col("loc"), col("time")],
            vec![col("temp")],
            GapFillParams {
                date_bin_udf: Arc::from("date_bin"),
                stride: lit(ScalarValue::new_interval_dt(0, 60_000)),
                time_column: col("time"),
                origin: None,
                time_range: Range {
                    start: Bound::Included(lit_timestamptz_nano(1000)),
                    end: Bound::Unbounded,
                },
                fill_strategy: fill_strategy_null(vec![col("temp")], schema.as_ref()),
            },
        );

        assert_error!(result, DataFusionError::Internal(ref msg) if msg == "missing upper bound in GapFill time range");
    }

    fn assert_gapfill_from_template_roundtrip(gapfill: &GapFill) {
        let gapfill_as_node: &dyn UserDefinedLogicalNode = gapfill;
        let scan = table_scan().unwrap();
        let exprs = gapfill_as_node.expressions();
        let want_exprs = gapfill.group_expr.len()
            + gapfill.aggr_expr.len()
            + 2 // stride, time
            + gapfill.params.origin.iter().count()
            + bound_extract(&gapfill.params.time_range.start).iter().count()
            + bound_extract(&gapfill.params.time_range.end).iter().count();
        assert_eq!(want_exprs, exprs.len());
        let gapfill_ft = gapfill_as_node
            .with_exprs_and_inputs(exprs, vec![scan])
            .expect("should be able to create a new `UserDefinedLogicalNode` node");
        let gapfill_ft = gapfill_ft
            .as_any()
            .downcast_ref::<GapFill>()
            .expect("should be a GapFill");
        assert_eq!(gapfill.group_expr, gapfill_ft.group_expr);
        assert_eq!(gapfill.aggr_expr, gapfill_ft.aggr_expr);
        assert_eq!(gapfill.params, gapfill_ft.params);
    }

    #[test]
    fn test_from_template() {
        let schema = schema().try_into().unwrap();

        for params in vec![
            // no origin, no start bound
            GapFillParams {
                date_bin_udf: Arc::from("date_bin"),
                stride: lit(ScalarValue::new_interval_dt(0, 60_000)),
                time_column: col("time"),
                origin: None,
                time_range: Range {
                    start: Bound::Unbounded,
                    end: Bound::Excluded(lit_timestamptz_nano(2000)),
                },
                fill_strategy: fill_strategy_null(vec![col("temp")], &schema),
            },
            // no origin, yes start bound
            GapFillParams {
                date_bin_udf: Arc::from("date_bin"),
                stride: lit(ScalarValue::new_interval_dt(0, 60_000)),
                time_column: col("time"),
                origin: None,
                time_range: Range {
                    start: Bound::Included(lit_timestamptz_nano(1000)),
                    end: Bound::Excluded(lit_timestamptz_nano(2000)),
                },
                fill_strategy: fill_strategy_null(vec![col("temp")], &schema),
            },
            // yes origin, no start bound
            GapFillParams {
                date_bin_udf: Arc::from("date_bin"),
                stride: lit(ScalarValue::new_interval_dt(0, 60_000)),
                time_column: col("time"),
                origin: Some(lit_timestamptz_nano(1_000_000_000)),
                time_range: Range {
                    start: Bound::Unbounded,
                    end: Bound::Excluded(lit_timestamptz_nano(2000)),
                },
                fill_strategy: fill_strategy_null(vec![col("temp")], &schema),
            },
            // yes origin, yes start bound
            GapFillParams {
                date_bin_udf: Arc::from("date_bin"),
                stride: lit(ScalarValue::new_interval_dt(0, 60_000)),
                time_column: col("time"),
                origin: Some(lit_timestamptz_nano(1_000_000_000)),
                time_range: Range {
                    start: Bound::Included(lit_timestamptz_nano(1000)),
                    end: Bound::Excluded(lit_timestamptz_nano(2000)),
                },
                fill_strategy: fill_strategy_null(vec![col("temp")], &schema),
            },
        ] {
            let scan = table_scan().unwrap();
            let gapfill = GapFill::try_new(
                Arc::new(scan.clone()),
                vec![col("loc"), col("time")],
                vec![col("temp")],
                params,
            )
            .unwrap();
            assert_gapfill_from_template_roundtrip(&gapfill);
        }
    }

    #[test]
    fn fmt_logical_plan() -> Result<()> {
        // This test case does not make much sense but
        // just verifies we can construct a logical gapfill node
        // and show its plan.
        let scan = table_scan()?;
        let schema = Arc::clone(scan.schema());
        let gapfill = GapFill::try_new(
            Arc::new(scan),
            vec![col("loc"), col("time")],
            vec![col("temp")],
            GapFillParams {
                date_bin_udf: Arc::from("date_bin"),
                stride: lit(ScalarValue::new_interval_dt(0, 60_000)),
                time_column: col("time"),
                origin: None,
                time_range: Range {
                    start: Bound::Included(lit_timestamptz_nano(1000)),
                    end: Bound::Excluded(lit_timestamptz_nano(2000)),
                },
                fill_strategy: fill_strategy_null(vec![col("temp")], &schema),
            },
        )?;
        let plan = LogicalPlan::Extension(Extension {
            node: Arc::new(gapfill),
        });

        insta::assert_yaml_snapshot!(
            format_logical_plan(&plan),
            @r#"
        - " GapFill: groupBy=[loc, time], aggr=[[temp]], time_column=time, stride=IntervalDayTime(\"IntervalDayTime { days: 0, milliseconds: 60000 }\"), range=Included(Literal(TimestampNanosecond(1000, None)))..Excluded(Literal(TimestampNanosecond(2000, None)))"
        - "   TableScan: temps"
        "#
        );
        Ok(())
    }

    async fn format_explain(sql: &str) -> Result<Vec<String>> {
        let executor = Executor::new_testing();
        let context = executor.new_context();
        context
            .inner()
            .register_table("temps", Arc::new(EmptyTable::new(Arc::new(schema()))))?;
        let physical_plan = context.sql_to_physical_plan(sql).await?;
        Ok(format_execution_plan(&physical_plan))
    }

    #[tokio::test]
    async fn plan_gap_fill() -> Result<()> {
        // show that the optimizer rule can fire and that physical
        // planning will succeed.
        let sql = "SELECT date_bin_gapfill(interval '1 minute', time, timestamp '1970-01-01T00:00:00Z') AS minute, avg(temp)\
                   \nFROM temps\
                   \nWHERE time >= '1980-01-01T00:00:00Z' and time < '1981-01-01T00:00:00Z'\
                   \nGROUP BY minute;";

        let explain = format_explain(sql).await?;
        insta::assert_yaml_snapshot!(
            explain,
            @r#"
        - " ProjectionExec: expr=[date_bin_gapfill(IntervalMonthDayNano(\"IntervalMonthDayNano { months: 0, days: 0, nanoseconds: 60000000000 }\"),temps.time,Utf8(\"1970-01-01T00:00:00Z\"))@0 as minute, avg(temps.temp)@1 as avg(temps.temp)]"
        - "   GapFillExec: group_expr=[date_bin_gapfill(IntervalMonthDayNano(\"IntervalMonthDayNano { months: 0, days: 0, nanoseconds: 60000000000 }\"),temps.time,Utf8(\"1970-01-01T00:00:00Z\"))@0], aggr_expr=[avg(temps.temp)@1], stride=IntervalMonthDayNano { months: 0, days: 0, nanoseconds: 60000000000 }, time_range=Included(\"315532800000000000\")..Excluded(\"347155200000000000\")"
        - "     SortExec: expr=[date_bin_gapfill(IntervalMonthDayNano(\"IntervalMonthDayNano { months: 0, days: 0, nanoseconds: 60000000000 }\"),temps.time,Utf8(\"1970-01-01T00:00:00Z\"))@0 ASC], preserve_partitioning=[false]"
        - "       AggregateExec: mode=Single, gby=[date_bin(IntervalMonthDayNano { months: 0, days: 0, nanoseconds: 60000000000 }, time@0, 0) as date_bin_gapfill(IntervalMonthDayNano(\"IntervalMonthDayNano { months: 0, days: 0, nanoseconds: 60000000000 }\"),temps.time,Utf8(\"1970-01-01T00:00:00Z\"))], aggr=[avg(temps.temp)]"
        - "         EmptyExec"
        "#
        );
        Ok(())
    }

    #[tokio::test]
    async fn gap_fill_exec_sort_order() -> Result<()> {
        // The call to `date_bin_gapfill` should be last in the SortExec
        // expressions, even though it was not last on the SELECT list
        // or the GROUP BY clause.
        let sql = "SELECT \
           \n  loc,\
           \n  date_bin_gapfill(interval '1 minute', time, timestamp '1970-01-01T00:00:00Z') AS minute,\
           \n  concat('zz', loc) AS loczz,\
           \n  avg(temp)\
           \nFROM temps\
           \nWHERE time >= '1980-01-01T00:00:00Z' and time < '1981-01-01T00:00:00Z'
           \nGROUP BY loc, minute, loczz;";

        let explain = format_explain(sql).await?;
        insta::assert_yaml_snapshot!(
            explain,
            @r#"
        - " ProjectionExec: expr=[loc@0 as loc, date_bin_gapfill(IntervalMonthDayNano(\"IntervalMonthDayNano { months: 0, days: 0, nanoseconds: 60000000000 }\"),temps.time,Utf8(\"1970-01-01T00:00:00Z\"))@1 as minute, concat(Utf8(\"zz\"),temps.loc)@2 as loczz, avg(temps.temp)@3 as avg(temps.temp)]"
        - "   GapFillExec: group_expr=[loc@0, date_bin_gapfill(IntervalMonthDayNano(\"IntervalMonthDayNano { months: 0, days: 0, nanoseconds: 60000000000 }\"),temps.time,Utf8(\"1970-01-01T00:00:00Z\"))@1, concat(Utf8(\"zz\"),temps.loc)@2], aggr_expr=[avg(temps.temp)@3], stride=IntervalMonthDayNano { months: 0, days: 0, nanoseconds: 60000000000 }, time_range=Included(\"315532800000000000\")..Excluded(\"347155200000000000\")"
        - "     SortExec: expr=[loc@0 ASC,concat(Utf8(\"zz\"),temps.loc)@2 ASC,date_bin_gapfill(IntervalMonthDayNano(\"IntervalMonthDayNano { months: 0, days: 0, nanoseconds: 60000000000 }\"),temps.time,Utf8(\"1970-01-01T00:00:00Z\"))@1 ASC], preserve_partitioning=[false]"
        - "       AggregateExec: mode=Single, gby=[loc@1 as loc, date_bin(IntervalMonthDayNano { months: 0, days: 0, nanoseconds: 60000000000 }, time@0, 0) as date_bin_gapfill(IntervalMonthDayNano(\"IntervalMonthDayNano { months: 0, days: 0, nanoseconds: 60000000000 }\"),temps.time,Utf8(\"1970-01-01T00:00:00Z\")), concat(zz, loc@1) as concat(Utf8(\"zz\"),temps.loc)], aggr=[avg(temps.temp)]"
        - "         EmptyExec"
        "#
        );
        Ok(())
    }
}
