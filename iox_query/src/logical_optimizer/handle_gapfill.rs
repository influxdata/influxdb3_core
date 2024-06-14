//! An optimizer rule that transforms a plan
//! to fill gaps in time series data.

pub mod range_predicate;

use crate::exec::gapfill::{FillStrategy, GapFill, GapFillParams};
use datafusion::functions::datetime::functions as datafusion_datetime_udfs;
use datafusion::{
    common::tree_node::{Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter},
    config::ConfigOptions,
    error::{DataFusionError, Result},
    logical_expr::{
        expr::{Alias, ScalarFunction},
        utils::expr_to_columns,
        Aggregate, Extension, LogicalPlan, Projection, ScalarUDF,
    },
    optimizer::AnalyzerRule,
    prelude::{col, Column, Expr},
};
use hashbrown::{hash_map, HashMap};
use query_functions::gapfill::{DATE_BIN_GAPFILL_UDF_NAME, INTERPOLATE_UDF_NAME, LOCF_UDF_NAME};
use std::{
    collections::HashSet,
    ops::{Bound, Range},
    sync::Arc,
};

/// This optimizer rule enables gap-filling semantics for SQL queries
/// that contain calls to `DATE_BIN_GAPFILL()` and related functions
/// like `LOCF()`.
///
/// In SQL a typical gap-filling query might look like this:
/// ```sql
/// SELECT
///   location,
///   DATE_BIN_GAPFILL(INTERVAL '1 minute', time, '1970-01-01T00:00:00Z') AS minute,
///   LOCF(AVG(temp))
/// FROM temps
/// WHERE time > NOW() - INTERVAL '6 hours' AND time < NOW()
/// GROUP BY LOCATION, MINUTE
/// ```
///
/// The initial logical plan will look like this:
///
/// ```text
///   Projection: location, date_bin_gapfill(...) as minute, LOCF(AVG(temps.temp))
///     Aggregate: groupBy=[[location, date_bin_gapfill(...)]], aggr=[[AVG(temps.temp)]]
///       ...
/// ```
///
/// This optimizer rule transforms it to this:
///
/// ```text
///   Projection: location, date_bin_gapfill(...) as minute, AVG(temps.temp)
///     GapFill: groupBy=[[location, date_bin_gapfill(...))]], aggr=[[LOCF(AVG(temps.temp))]], start=..., stop=...
///       Aggregate: groupBy=[[location, date_bin(...))]], aggr=[[AVG(temps.temp)]]
///         ...
/// ```
///
/// For `Aggregate` nodes that contain calls to `DATE_BIN_GAPFILL`, this rule will:
/// - Convert `DATE_BIN_GAPFILL()` to `DATE_BIN()`
/// - Create a `GapFill` node that fills in gaps in the query
/// - The range for gap filling is found by analyzing any preceding `Filter` nodes
///
/// If there is a `Projection` above the `GapFill` node that gets created:
/// - Look for calls to gap-filling functions like `LOCF`
/// - Push down these functions into the `GapFill` node, updating the fill strategy for the column.
///
/// Note: both `DATE_BIN_GAPFILL` and `LOCF` are functions that don't have implementations.
/// This rule must rewrite the plan to get rid of them.
pub struct HandleGapFill;

impl HandleGapFill {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for HandleGapFill {
    fn default() -> Self {
        Self::new()
    }
}

impl AnalyzerRule for HandleGapFill {
    fn name(&self) -> &str {
        "handle_gap_fill"
    }
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up(handle_gap_fill).map(|t| t.data)
    }
}

fn handle_gap_fill(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let res = match plan {
        LogicalPlan::Aggregate(aggr) => {
            handle_aggregate(aggr).map_err(|e| e.context("handle_aggregate"))?
        }
        LogicalPlan::Projection(proj) => {
            handle_projection(proj).map_err(|e| e.context("handle_projection"))?
        }
        _ => Transformed::no(plan),
    };

    if !res.transformed {
        // no transformation was applied,
        // so make sure the plan is not using gap filling
        // functions in an unsupported way.
        check_node(&res.data)?;
    }

    Ok(res)
}

fn handle_aggregate(aggr: Aggregate) -> Result<Transformed<LogicalPlan>> {
    match replace_date_bin_gapfill(aggr).map_err(|e| e.context("replace_date_bin_gapfill"))? {
        // No change: return as-is
        RewriteInfo::Unchanged(aggr) => Ok(Transformed::no(LogicalPlan::Aggregate(aggr))),
        // Changed: new_aggr has DATE_BIN_GAPFILL replaced with DATE_BIN.
        RewriteInfo::Changed {
            new_aggr,
            date_bin_gapfill_index,
            date_bin_gapfill_args,
        } => {
            let new_aggr_plan = {
                let new_aggr_plan = LogicalPlan::Aggregate(new_aggr);
                check_node(&new_aggr_plan).map_err(|e| e.context("check_node"))?;
                new_aggr_plan
            };

            let new_gap_fill_plan =
                build_gapfill_node(new_aggr_plan, date_bin_gapfill_index, date_bin_gapfill_args)
                    .map_err(|e| e.context("build_gapfill_node"))?;
            Ok(Transformed::yes(new_gap_fill_plan))
        }
    }
}

fn build_gapfill_node(
    new_aggr_plan: LogicalPlan,
    date_bin_gapfill_index: usize,
    date_bin_gapfill_args: Vec<Expr>,
) -> Result<LogicalPlan> {
    match date_bin_gapfill_args.len() {
        2 | 3 => (),
        nargs => {
            return Err(DataFusionError::Plan(format!(
                "DATE_BIN_GAPFILL expects 2 or 3 arguments, got {nargs}",
            )));
        }
    }

    let mut args_iter = date_bin_gapfill_args.into_iter();

    // Ensure that stride argument is a scalar
    let stride = args_iter.next().unwrap();
    validate_scalar_expr("stride argument to DATE_BIN_GAPFILL", &stride)
        .map_err(|e| e.context("validate_scalar_expr"))?;

    fn get_column(expr: Expr) -> Result<Column> {
        match expr {
            Expr::Column(c) => Ok(c),
            Expr::Cast(c) => get_column(*c.expr),
            _ => Err(DataFusionError::Plan(
                "DATE_BIN_GAPFILL requires a column as the source argument".to_string(),
            )),
        }
    }

    // Ensure that the source argument is a column
    let time_col =
        get_column(args_iter.next().unwrap()).map_err(|e| e.context("get time column"))?;

    // Ensure that a time range was specified and is valid for gap filling
    let time_range = range_predicate::find_time_range(new_aggr_plan.inputs()[0], &time_col)
        .map_err(|e| e.context("find time range"))?;
    validate_time_range(&time_range).map_err(|e| e.context("validate time range"))?;

    // Ensure that origin argument is a scalar
    let origin = args_iter.next();
    if let Some(ref origin) = origin {
        validate_scalar_expr("origin argument to DATE_BIN_GAPFILL", origin)
            .map_err(|e| e.context("validate origin"))?;
    }

    // Make sure the time output to the gapfill node matches what the
    // aggregate output was.
    let time_column = col(datafusion::common::Column::from(
        new_aggr_plan
            .schema()
            .qualified_field(date_bin_gapfill_index),
    ));

    let LogicalPlan::Aggregate(aggr) = &new_aggr_plan else {
        return Err(DataFusionError::Internal(format!(
            "Expected Aggregate plan, got {}",
            new_aggr_plan.display()
        )));
    };
    let mut new_group_expr: Vec<_> = aggr
        .schema
        .iter()
        .map(|(qualifier, field)| {
            Expr::Column(datafusion::common::Column::from((
                qualifier,
                field.as_ref(),
            )))
        })
        .collect();
    let aggr_expr = new_group_expr.split_off(aggr.group_expr.len());

    let fill_behavior = aggr_expr
        .iter()
        .cloned()
        .map(|e| (e, FillStrategy::Null))
        .collect();

    Ok(LogicalPlan::Extension(Extension {
        node: Arc::new(
            GapFill::try_new(
                Arc::new(new_aggr_plan),
                new_group_expr,
                aggr_expr,
                GapFillParams {
                    stride,
                    time_column,
                    origin,
                    time_range,
                    fill_strategy: fill_behavior,
                },
            )
            .map_err(|e| e.context("GapFill::try_new"))?,
        ),
    }))
}

fn validate_time_range(range: &Range<Bound<Expr>>) -> Result<()> {
    let Range { ref start, ref end } = range;
    let (start, end) = match (start, end) {
        (Bound::Unbounded, Bound::Unbounded) => {
            return Err(DataFusionError::Plan(
                "gap-filling query is missing both upper and lower time bounds".to_string(),
            ))
        }
        (Bound::Unbounded, _) => Err(DataFusionError::Plan(
            "gap-filling query is missing lower time bound".to_string(),
        )),
        (_, Bound::Unbounded) => Err(DataFusionError::Plan(
            "gap-filling query is missing upper time bound".to_string(),
        )),
        (
            Bound::Included(start) | Bound::Excluded(start),
            Bound::Included(end) | Bound::Excluded(end),
        ) => Ok((start, end)),
    }?;
    validate_scalar_expr("lower time bound", start)?;
    validate_scalar_expr("upper time bound", end)
}

fn validate_scalar_expr(what: &str, e: &Expr) -> Result<()> {
    let mut cols = HashSet::new();
    expr_to_columns(e, &mut cols)?;
    if !cols.is_empty() {
        Err(DataFusionError::Plan(format!(
            "{what} for gap fill query must evaluate to a scalar"
        )))
    } else {
        Ok(())
    }
}

enum RewriteInfo {
    // Group expressions were unchanged
    Unchanged(Aggregate),
    // Group expressions were changed
    Changed {
        // Group expressions with DATE_BIN_GAPFILL rewritten to DATE_BIN.
        new_aggr: Aggregate,
        // The index of the group expression that contained the call to DATE_BIN_GAPFILL.
        date_bin_gapfill_index: usize,
        // The arguments to the call to DATE_BIN_GAPFILL.
        date_bin_gapfill_args: Vec<Expr>,
    },
}

// Iterate over the group expression list.
// If it finds no occurrences of date_bin_gapfill, it will return None.
// If it finds more than one occurrence it will return an error.
// Otherwise it will return a RewriteInfo for the analyzer rule to use.
fn replace_date_bin_gapfill(aggr: Aggregate) -> Result<RewriteInfo> {
    let mut date_bin_gapfill_count = 0;
    let mut dbg_idx = None;
    aggr.group_expr
        .iter()
        .enumerate()
        .try_for_each(|(i, e)| -> Result<()> {
            let fn_cnt = count_udf(e, DATE_BIN_GAPFILL_UDF_NAME)?;
            date_bin_gapfill_count += fn_cnt;
            if fn_cnt > 0 {
                dbg_idx = Some(i);
            }
            Ok(())
        })?;
    match date_bin_gapfill_count {
        0 => return Ok(RewriteInfo::Unchanged(aggr)),
        1 => {
            // Make sure that the call to DATE_BIN_GAPFILL is root expression
            // excluding aliases.
            let dbg_idx = dbg_idx.expect("should have found exactly one call");
            if !matches_udf(
                unwrap_alias(&aggr.group_expr[dbg_idx]),
                DATE_BIN_GAPFILL_UDF_NAME,
            ) {
                return Err(DataFusionError::Plan(
                    "DATE_BIN_GAPFILL must be a top-level expression in the GROUP BY clause when gap filling. It cannot be part of another expression or cast".to_string(),
                ));
            }
        }
        _ => {
            return Err(DataFusionError::Plan(
                "DATE_BIN_GAPFILL specified more than once".to_string(),
            ))
        }
    }

    let date_bin_gapfill_index = dbg_idx.expect("should be found exactly one call");

    let date_bin = datafusion_datetime_udfs()
        .iter()
        .find(|fun| fun.name().eq("date_bin"))
        .ok_or(DataFusionError::Execution("no date_bin UDF found".into()))?
        .to_owned();

    let mut rewriter = DateBinGapfillRewriter {
        args: None,
        date_bin,
    };
    let new_group_expr = aggr
        .group_expr
        .into_iter()
        .enumerate()
        .map(|(i, e)| {
            if i == date_bin_gapfill_index {
                e.rewrite(&mut rewriter).map(|t| t.data)
            } else {
                Ok(e)
            }
        })
        .collect::<Result<Vec<_>>>()?;
    let date_bin_gapfill_args = rewriter.args.expect("should have found args");

    // Create the aggregate node with the same output schema as the original
    // one. This means that there will be an output column called `date_bin_gapfill(...)`
    // even though the actual expression populating that column will be `date_bin(...)`.
    // This seems acceptable since it avoids having to deal with renaming downstream.
    let new_aggr =
        Aggregate::try_new_with_schema(aggr.input, new_group_expr, aggr.aggr_expr, aggr.schema)
            .map_err(|e| e.context("Aggregate::try_new_with_schema"))?;
    Ok(RewriteInfo::Changed {
        new_aggr,
        date_bin_gapfill_index,
        date_bin_gapfill_args,
    })
}

fn unwrap_alias(mut e: &Expr) -> &Expr {
    loop {
        match e {
            Expr::Alias(Alias { expr, .. }) => e = expr.as_ref(),
            e => break e,
        }
    }
}

struct DateBinGapfillRewriter {
    args: Option<Vec<Expr>>,
    date_bin: Arc<ScalarUDF>,
}

impl TreeNodeRewriter for DateBinGapfillRewriter {
    type Node = Expr;
    fn f_down(&mut self, expr: Expr) -> Result<Transformed<Expr>> {
        match &expr {
            Expr::ScalarFunction(fun) if fun.name() == DATE_BIN_GAPFILL_UDF_NAME => {
                Ok(Transformed::new(expr, true, TreeNodeRecursion::Jump))
            }
            _ => Ok(Transformed::no(expr)),
        }
    }

    fn f_up(&mut self, expr: Expr) -> Result<Transformed<Expr>> {
        // We need to preserve the name of the original expression
        // so that everything stays wired up.
        let orig_name = expr.display_name()?;
        match expr {
            Expr::ScalarFunction(ScalarFunction { func, args })
                if func.name() == DATE_BIN_GAPFILL_UDF_NAME =>
            {
                self.args = Some(args.clone());
                Ok(Transformed::yes(
                    Expr::ScalarFunction(ScalarFunction {
                        func: Arc::clone(&self.date_bin),
                        args,
                    })
                    .alias(orig_name),
                ))
            }
            _ => Ok(Transformed::no(expr)),
        }
    }
}

fn udf_to_fill_strategy(name: &str) -> Option<FillStrategy> {
    match name {
        LOCF_UDF_NAME => Some(FillStrategy::PrevNullAsMissing),
        INTERPOLATE_UDF_NAME => Some(FillStrategy::LinearInterpolate),
        _ => None,
    }
}

fn fill_strategy_to_udf(fs: &FillStrategy) -> Result<&'static str> {
    match fs {
        FillStrategy::PrevNullAsMissing => Ok(LOCF_UDF_NAME),
        FillStrategy::LinearInterpolate => Ok(INTERPOLATE_UDF_NAME),
        _ => Err(DataFusionError::Internal(format!(
            "unknown UDF for fill strategy {fs:?}"
        ))),
    }
}

fn handle_projection(mut proj: Projection) -> Result<Transformed<LogicalPlan>> {
    let Some(child_gapfill) = (match proj.input.as_ref() {
        LogicalPlan::Extension(Extension { node }) => node.as_any().downcast_ref::<GapFill>(),
        _ => None,
    }) else {
        // If this is not a projection that is a parent to a GapFill node,
        // then there is nothing to do.
        return Ok(Transformed::no(LogicalPlan::Projection(proj)));
    };

    let mut fill_fn_rewriter = FillFnRewriter {
        aggr_col_fill_map: HashMap::new(),
    };
    let new_proj_exprs = proj
        .expr
        .iter()
        .map(|expr| {
            expr.clone()
                .rewrite(&mut fill_fn_rewriter)
                .map(|t| t.data)
                .map_err(|e| e.context(format!("rewrite: {expr}")))
        })
        .collect::<Result<Vec<Expr>>>()?;
    let FillFnRewriter { aggr_col_fill_map } = fill_fn_rewriter;
    if aggr_col_fill_map.is_empty() {
        return Ok(Transformed::no(LogicalPlan::Projection(proj)));
    }

    // Clone the existing GapFill node, then modify it in place
    // to reflect the new fill strategy.
    let mut new_gapfill = child_gapfill.clone();
    for (e, fs) in aggr_col_fill_map {
        let udf = fill_strategy_to_udf(&fs).map_err(|e| e.context("fill_strategy_to_udf"))?;
        if new_gapfill.replace_fill_strategy(&e, fs).is_none() {
            // There was a gap filling function called on a non-aggregate column.
            return Err(DataFusionError::Plan(format!(
                "{udf} must be called on an aggregate column in a gap-filling query",
            )));
        }
    }
    proj.expr = new_proj_exprs;
    proj.input = Arc::new(LogicalPlan::Extension(Extension {
        node: Arc::new(new_gapfill),
    }));
    Ok(Transformed::yes(LogicalPlan::Projection(proj)))
}

/// Implements `TreeNodeRewriter`:
/// - Traverses over the expressions in a projection node
/// - If it finds `locf(col)` or `interpolate(col)`,
///   it replaces them with `col AS <original name>`
/// - Collects into [`Self::aggr_col_fill_map`] which correlates
///   aggregate columns to their [`FillStrategy`].
struct FillFnRewriter {
    aggr_col_fill_map: HashMap<Expr, FillStrategy>,
}

impl TreeNodeRewriter for FillFnRewriter {
    type Node = Expr;
    fn f_down(&mut self, expr: Expr) -> Result<Transformed<Expr>> {
        match &expr {
            Expr::ScalarFunction(fun) if udf_to_fill_strategy(fun.name()).is_some() => {
                Ok(Transformed::new(expr, true, TreeNodeRecursion::Jump))
            }
            _ => Ok(Transformed::no(expr)),
        }
    }

    fn f_up(&mut self, expr: Expr) -> Result<Transformed<Expr>> {
        let orig_name = expr.display_name()?;
        match expr {
            Expr::ScalarFunction(ref fun) if udf_to_fill_strategy(fun.name()).is_none() => {
                Ok(Transformed::no(expr))
            }
            Expr::ScalarFunction(mut fun) => {
                let fs = udf_to_fill_strategy(fun.name()).expect("must be a fill fn");
                let arg = fun.args.remove(0);
                self.add_fill_strategy(arg.clone(), fs)?;
                Ok(Transformed::yes(arg.alias(orig_name)))
            }
            _ => Ok(Transformed::no(expr)),
        }
    }
}

impl FillFnRewriter {
    fn add_fill_strategy(&mut self, e: Expr, fs: FillStrategy) -> Result<()> {
        match self.aggr_col_fill_map.entry(e) {
            hash_map::Entry::Occupied(_) => Err(DataFusionError::NotImplemented(
                "multiple fill strategies for the same column".to_string(),
            )),
            hash_map::Entry::Vacant(ve) => {
                ve.insert(fs);
                Ok(())
            }
        }
    }
}

fn count_udf(e: &Expr, name: &str) -> Result<usize> {
    let mut count = 0;
    e.apply(|expr| {
        if matches_udf(expr, name) {
            count += 1;
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(count)
}

fn matches_udf(e: &Expr, name: &str) -> bool {
    matches!(
        e,
        Expr::ScalarFunction(fun) if fun.name() == name
    )
}

fn check_node(node: &LogicalPlan) -> Result<()> {
    node.expressions().iter().try_for_each(|expr| {
        let dbg_count = count_udf(expr, DATE_BIN_GAPFILL_UDF_NAME)?;
        if dbg_count > 0 {
            return Err(DataFusionError::Plan(format!(
                "{DATE_BIN_GAPFILL_UDF_NAME} may only be used as a GROUP BY expression"
            )));
        }

        for fn_name in [LOCF_UDF_NAME, INTERPOLATE_UDF_NAME] {
            if count_udf(expr, fn_name)? > 0 {
                return Err(DataFusionError::Plan(format!(
                    "{fn_name} may only be used in the SELECT list of a gap-filling query"
                )));
            }
        }
        Ok(())
    })
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use super::HandleGapFill;

    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::config::ConfigOptions;
    use datafusion::error::Result;
    use datafusion::logical_expr::builder::table_scan_with_filters;
    use datafusion::logical_expr::{logical_plan, LogicalPlan, LogicalPlanBuilder};
    use datafusion::optimizer::{Analyzer, AnalyzerRule};
    use datafusion::prelude::{avg, case, col, lit, min, Expr};
    use datafusion::scalar::ScalarValue;
    use datafusion_util::lit_timestamptz_nano;
    use query_functions::gapfill::{
        DATE_BIN_GAPFILL_UDF_NAME, INTERPOLATE_UDF_NAME, LOCF_UDF_NAME,
    };

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new(
                "time2",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("loc", DataType::Utf8, false),
            Field::new("temp", DataType::Float64, false),
        ])
    }

    fn table_scan() -> Result<LogicalPlan> {
        logical_plan::table_scan(Some("temps"), &schema(), None)?.build()
    }

    fn date_bin_gapfill(interval: Expr, time: Expr) -> Result<Expr> {
        date_bin_gapfill_with_origin(interval, time, None)
    }

    fn date_bin_gapfill_with_origin(
        interval: Expr,
        time: Expr,
        origin: Option<Expr>,
    ) -> Result<Expr> {
        let mut args = vec![interval, time];
        if let Some(origin) = origin {
            args.push(origin)
        }

        Ok(query_functions::registry()
            .udf(DATE_BIN_GAPFILL_UDF_NAME)?
            .call(args))
    }

    fn locf(arg: Expr) -> Result<Expr> {
        Ok(query_functions::registry()
            .udf(LOCF_UDF_NAME)?
            .call(vec![arg]))
    }

    fn interpolate(arg: Expr) -> Result<Expr> {
        Ok(query_functions::registry()
            .udf(INTERPOLATE_UDF_NAME)?
            .call(vec![arg]))
    }

    fn observe(_plan: &LogicalPlan, _rule: &dyn AnalyzerRule) {}

    fn analyze(plan: LogicalPlan) -> Result<LogicalPlan> {
        let analyzer = Analyzer::with_rules(vec![Arc::new(HandleGapFill)]);
        analyzer.execute_and_check(plan, &ConfigOptions::new(), observe)
    }

    fn assert_analyzer_err(plan: LogicalPlan, expected: &str) {
        match analyze(plan) {
            Ok(plan) => assert_eq!(format!("{}", plan.display_indent()), "an error"),
            Err(ref e) => {
                let actual = e.to_string();
                if expected.is_empty() || !actual.contains(expected) {
                    assert_eq!(actual, expected)
                }
            }
        }
    }

    fn assert_analyzer_skipped(plan: LogicalPlan) -> Result<()> {
        let new_plan = analyze(plan.clone())?;
        assert_eq!(
            format!("{}", plan.display_indent()),
            format!("{}", new_plan.display_indent())
        );
        Ok(())
    }

    fn format_analyzed_plan(plan: LogicalPlan) -> Result<Vec<String>> {
        let plan = analyze(plan)?.display_indent().to_string();
        Ok(plan.split('\n').map(|s| s.to_string()).collect())
    }

    #[test]
    fn misplaced_dbg_err() -> Result<()> {
        // date_bin_gapfill used in a filter should produce an error
        let scan = table_scan()?;
        let plan = LogicalPlanBuilder::from(scan)
            .filter(
                date_bin_gapfill(
                    lit(ScalarValue::IntervalDayTime(Some(600_000))),
                    col("temp"),
                )?
                .gt(lit(100.0)),
            )?
            .build()?;
        assert_analyzer_err(
            plan,
            "Error during planning: date_bin_gapfill may only be used as a GROUP BY expression",
        );
        Ok(())
    }

    /// calling LOCF in a WHERE predicate is not valid
    #[test]
    fn misplaced_locf_err() -> Result<()> {
        // date_bin_gapfill used in a filter should produce an error
        let scan = table_scan()?;
        let plan = LogicalPlanBuilder::from(scan)
            .filter(locf(col("temp"))?.gt(lit(100.0)))?
            .build()?;
        assert_analyzer_err(
            plan,
            "Error during planning: locf may only be used in the SELECT list of a gap-filling query",
        );
        Ok(())
    }

    /// calling INTERPOLATE in a WHERE predicate is not valid
    #[test]
    fn misplaced_interpolate_err() -> Result<()> {
        // date_bin_gapfill used in a filter should produce an error
        let scan = table_scan()?;
        let plan = LogicalPlanBuilder::from(scan)
            .filter(interpolate(col("temp"))?.gt(lit(100.0)))?
            .build()?;
        assert_analyzer_err(
            plan,
            "Error during planning: interpolate may only be used in the SELECT list of a gap-filling query",
        );
        Ok(())
    }
    /// calling LOCF on the SELECT list but not on an aggregate column is not valid.
    #[test]
    fn misplaced_locf_non_agg_err() -> Result<()> {
        let plan = LogicalPlanBuilder::from(table_scan()?)
            .filter(
                col("time")
                    .gt_eq(lit_timestamptz_nano(1000))
                    .and(col("time").lt(lit_timestamptz_nano(2000))),
            )?
            .aggregate(
                vec![
                    col("loc"),
                    date_bin_gapfill(lit(ScalarValue::IntervalDayTime(Some(60_000))), col("time"))?,
                ],
                vec![avg(col("temp")), min(col("temp"))],
            )?
            .project(vec![
                locf(col("loc"))?,
                col("date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time)"),
                locf(col("AVG(temps.temp)"))?,
                locf(col("MIN(temps.temp)"))?,
            ])?
            .build()?;
        assert_analyzer_err(
            plan,
            "locf must be called on an aggregate column in a gap-filling query",
        );
        Ok(())
    }

    #[test]
    fn different_fill_strategies_one_col() -> Result<()> {
        let plan = LogicalPlanBuilder::from(table_scan()?)
            .filter(
                col("time")
                    .gt_eq(lit_timestamptz_nano(1000))
                    .and(col("time").lt(lit_timestamptz_nano(2000))),
            )?
            .aggregate(
                vec![
                    col("loc"),
                    date_bin_gapfill(lit(ScalarValue::IntervalDayTime(Some(60_000))), col("time"))?,
                ],
                vec![avg(col("temp")), min(col("temp"))],
            )?
            .project(vec![
                locf(col("loc"))?,
                col("date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time)"),
                locf(col("AVG(temps.temp)"))?,
                interpolate(col("AVG(temps.temp)"))?,
            ])?
            .build()?;
        assert_analyzer_err(
            plan,
            "This feature is not implemented: multiple fill strategies for the same column",
        );
        Ok(())
    }

    #[test]
    fn nonscalar_origin() -> Result<()> {
        let plan = LogicalPlanBuilder::from(table_scan()?)
            .filter(
                col("time")
                    .gt_eq(lit_timestamptz_nano(1000))
                    .and(col("time").lt(lit_timestamptz_nano(2000))),
            )?
            .aggregate(
                vec![date_bin_gapfill_with_origin(
                    lit(ScalarValue::IntervalDayTime(Some(60_000))),
                    col("time"),
                    Some(col("time2")),
                )?],
                vec![avg(col("temp"))],
            )?
            .build()?;
        assert_analyzer_err(
            plan,
            "Error during planning: origin argument to DATE_BIN_GAPFILL for gap fill query must evaluate to a scalar",
        );
        Ok(())
    }

    #[test]
    fn nonscalar_stride() -> Result<()> {
        let stride = case(col("loc"))
            .when(
                lit("kitchen"),
                lit(ScalarValue::IntervalDayTime(Some(60_000))),
            )
            .otherwise(lit(ScalarValue::IntervalDayTime(Some(30_000))))
            .unwrap();

        let plan = LogicalPlanBuilder::from(table_scan()?)
            .filter(
                col("time")
                    .gt_eq(lit_timestamptz_nano(1000))
                    .and(col("time").lt(lit_timestamptz_nano(2000))),
            )?
            .aggregate(
                vec![date_bin_gapfill(stride, col("time"))?],
                vec![avg(col("temp"))],
            )?
            .build()?;
        assert_analyzer_err(
            plan,
            "Error during planning: stride argument to DATE_BIN_GAPFILL for gap fill query must evaluate to a scalar",
        );
        Ok(())
    }

    #[test]
    fn time_range_errs() -> Result<()> {
        let cases = vec![
            (
                lit(true),
                "Error during planning: gap-filling query is missing both upper and lower time bounds",
            ),
            (
                col("time").gt_eq(lit_timestamptz_nano(1000)),
                "Error during planning: gap-filling query is missing upper time bound",
            ),
            (
                col("time").lt(lit_timestamptz_nano(2000)),
                "Error during planning: gap-filling query is missing lower time bound",
            ),
            (
                col("time").gt_eq(col("time2")).and(
                    col("time").lt(lit_timestamptz_nano(2000))),
                "Error during planning: lower time bound for gap fill query must evaluate to a scalar",
            ),
            (
                col("time").gt_eq(lit_timestamptz_nano(2000)).and(
                    col("time").lt(col("time2"))),
                "Error during planning: upper time bound for gap fill query must evaluate to a scalar",
            )
        ];
        for c in cases {
            let plan = LogicalPlanBuilder::from(table_scan()?)
                .filter(c.0)?
                .aggregate(
                    vec![date_bin_gapfill(
                        lit(ScalarValue::IntervalDayTime(Some(60_000))),
                        col("time"),
                    )?],
                    vec![avg(col("temp"))],
                )?
                .build()?;
            assert_analyzer_err(plan, c.1);
        }
        Ok(())
    }

    #[test]
    fn no_change() -> Result<()> {
        let plan = LogicalPlanBuilder::from(table_scan()?)
            .aggregate(vec![col("loc")], vec![avg(col("temp"))])?
            .build()?;
        assert_analyzer_skipped(plan)?;
        Ok(())
    }

    #[test]
    fn date_bin_gapfill_simple() -> Result<()> {
        let plan = LogicalPlanBuilder::from(table_scan()?)
            .filter(
                col("time")
                    .gt_eq(lit_timestamptz_nano(1000))
                    .and(col("time").lt(lit_timestamptz_nano(2000))),
            )?
            .aggregate(
                vec![date_bin_gapfill(
                    lit(ScalarValue::IntervalDayTime(Some(60_000))),
                    col("time"),
                )?],
                vec![avg(col("temp"))],
            )?
            .build()?;

        insta::assert_yaml_snapshot!(
            format_analyzed_plan(plan)?,
            @r###"
        ---
        - "GapFill: groupBy=[date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time)], aggr=[[AVG(temps.temp)]], time_column=date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time), stride=IntervalDayTime(\"60000\"), range=Included(Literal(TimestampNanosecond(1000, Some(\"UTC\"))))..Excluded(Literal(TimestampNanosecond(2000, Some(\"UTC\"))))"
        - "  Aggregate: groupBy=[[date_bin(IntervalDayTime(\"60000\"), temps.time) AS date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time)]], aggr=[[AVG(temps.temp)]]"
        - "    Filter: temps.time >= TimestampNanosecond(1000, Some(\"UTC\")) AND temps.time < TimestampNanosecond(2000, Some(\"UTC\"))"
        - "      TableScan: temps"
        "###);
        Ok(())
    }

    #[test]
    fn date_bin_gapfill_origin() -> Result<()> {
        let plan = LogicalPlanBuilder::from(table_scan()?)
            .filter(
                col("time")
                    .gt_eq(lit_timestamptz_nano(1000))
                    .and(col("time").lt(lit_timestamptz_nano(2000))),
            )?
            .aggregate(
                vec![date_bin_gapfill_with_origin(
                    lit(ScalarValue::IntervalDayTime(Some(60_000))),
                    col("time"),
                    Some(lit_timestamptz_nano(7)),
                )?],
                vec![avg(col("temp"))],
            )?
            .build()?;

        insta::assert_yaml_snapshot!(
            format_analyzed_plan(plan)?,
            @r###"
        ---
        - "GapFill: groupBy=[date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time,TimestampNanosecond(7, Some(\"UTC\")))], aggr=[[AVG(temps.temp)]], time_column=date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time,TimestampNanosecond(7, Some(\"UTC\"))), stride=IntervalDayTime(\"60000\"), range=Included(Literal(TimestampNanosecond(1000, Some(\"UTC\"))))..Excluded(Literal(TimestampNanosecond(2000, Some(\"UTC\"))))"
        - "  Aggregate: groupBy=[[date_bin(IntervalDayTime(\"60000\"), temps.time, TimestampNanosecond(7, Some(\"UTC\"))) AS date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time,TimestampNanosecond(7, Some(\"UTC\")))]], aggr=[[AVG(temps.temp)]]"
        - "    Filter: temps.time >= TimestampNanosecond(1000, Some(\"UTC\")) AND temps.time < TimestampNanosecond(2000, Some(\"UTC\"))"
        - "      TableScan: temps"
        "###);
        Ok(())
    }
    #[test]
    fn two_group_exprs() -> Result<()> {
        // grouping by date_bin_gapfill(...), loc
        let plan = LogicalPlanBuilder::from(table_scan()?)
            .filter(
                col("time")
                    .gt_eq(lit_timestamptz_nano(1000))
                    .and(col("time").lt(lit_timestamptz_nano(2000))),
            )?
            .aggregate(
                vec![
                    date_bin_gapfill(lit(ScalarValue::IntervalDayTime(Some(60_000))), col("time"))?,
                    col("loc"),
                ],
                vec![avg(col("temp"))],
            )?
            .build()?;

        insta::assert_yaml_snapshot!(
            format_analyzed_plan(plan)?,
            @r###"
        ---
        - "GapFill: groupBy=[date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time), temps.loc], aggr=[[AVG(temps.temp)]], time_column=date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time), stride=IntervalDayTime(\"60000\"), range=Included(Literal(TimestampNanosecond(1000, Some(\"UTC\"))))..Excluded(Literal(TimestampNanosecond(2000, Some(\"UTC\"))))"
        - "  Aggregate: groupBy=[[date_bin(IntervalDayTime(\"60000\"), temps.time) AS date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time), temps.loc]], aggr=[[AVG(temps.temp)]]"
        - "    Filter: temps.time >= TimestampNanosecond(1000, Some(\"UTC\")) AND temps.time < TimestampNanosecond(2000, Some(\"UTC\"))"
        - "      TableScan: temps"
        "###);
        Ok(())
    }

    #[test]
    fn double_date_bin_gapfill() -> Result<()> {
        let plan = LogicalPlanBuilder::from(table_scan()?)
            .aggregate(
                vec![
                    date_bin_gapfill(lit(ScalarValue::IntervalDayTime(Some(60_000))), col("time"))?,
                    date_bin_gapfill(lit(ScalarValue::IntervalDayTime(Some(30_000))), col("time"))?,
                ],
                vec![avg(col("temp"))],
            )?
            .build()?;
        assert_analyzer_err(
            plan,
            "Error during planning: DATE_BIN_GAPFILL specified more than once",
        );
        Ok(())
    }

    #[test]
    fn with_projection() -> Result<()> {
        let plan = LogicalPlanBuilder::from(table_scan()?)
            .filter(
                col("time")
                    .gt_eq(lit_timestamptz_nano(1000))
                    .and(col("time").lt(lit_timestamptz_nano(2000))),
            )?
            .aggregate(
                vec![date_bin_gapfill(
                    lit(ScalarValue::IntervalDayTime(Some(60_000))),
                    col("time"),
                )?],
                vec![avg(col("temp"))],
            )?
            .project(vec![
                col("date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time)"),
                col("AVG(temps.temp)"),
            ])?
            .build()?;

        insta::assert_yaml_snapshot!(
            format_analyzed_plan(plan)?,
            @r###"
        ---
        - "Projection: date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time), AVG(temps.temp)"
        - "  GapFill: groupBy=[date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time)], aggr=[[AVG(temps.temp)]], time_column=date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time), stride=IntervalDayTime(\"60000\"), range=Included(Literal(TimestampNanosecond(1000, Some(\"UTC\"))))..Excluded(Literal(TimestampNanosecond(2000, Some(\"UTC\"))))"
        - "    Aggregate: groupBy=[[date_bin(IntervalDayTime(\"60000\"), temps.time) AS date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time)]], aggr=[[AVG(temps.temp)]]"
        - "      Filter: temps.time >= TimestampNanosecond(1000, Some(\"UTC\")) AND temps.time < TimestampNanosecond(2000, Some(\"UTC\"))"
        - "        TableScan: temps"
        "###);
        Ok(())
    }

    #[test]
    fn with_locf() -> Result<()> {
        let plan = LogicalPlanBuilder::from(table_scan()?)
            .filter(
                col("time")
                    .gt_eq(lit_timestamptz_nano(1000))
                    .and(col("time").lt(lit_timestamptz_nano(2000))),
            )?
            .aggregate(
                vec![date_bin_gapfill(
                    lit(ScalarValue::IntervalDayTime(Some(60_000))),
                    col("time"),
                )?],
                vec![avg(col("temp")), min(col("temp"))],
            )?
            .project(vec![
                col("date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time)"),
                locf(col("AVG(temps.temp)"))?,
                locf(col("MIN(temps.temp)"))?,
            ])?
            .build()?;

        insta::assert_yaml_snapshot!(
            format_analyzed_plan(plan)?,
            @r###"
        ---
        - "Projection: date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time), AVG(temps.temp) AS locf(AVG(temps.temp)), MIN(temps.temp) AS locf(MIN(temps.temp))"
        - "  GapFill: groupBy=[date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time)], aggr=[[LOCF(AVG(temps.temp)), LOCF(MIN(temps.temp))]], time_column=date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time), stride=IntervalDayTime(\"60000\"), range=Included(Literal(TimestampNanosecond(1000, Some(\"UTC\"))))..Excluded(Literal(TimestampNanosecond(2000, Some(\"UTC\"))))"
        - "    Aggregate: groupBy=[[date_bin(IntervalDayTime(\"60000\"), temps.time) AS date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time)]], aggr=[[AVG(temps.temp), MIN(temps.temp)]]"
        - "      Filter: temps.time >= TimestampNanosecond(1000, Some(\"UTC\")) AND temps.time < TimestampNanosecond(2000, Some(\"UTC\"))"
        - "        TableScan: temps"
        "###);
        Ok(())
    }

    #[test]
    fn with_locf_aliased() -> Result<()> {
        let plan = LogicalPlanBuilder::from(table_scan()?)
            .filter(
                col("time")
                    .gt_eq(lit_timestamptz_nano(1000))
                    .and(col("time").lt(lit_timestamptz_nano(2000))),
            )?
            .aggregate(
                vec![date_bin_gapfill(
                    lit(ScalarValue::IntervalDayTime(Some(60_000))),
                    col("time"),
                )?],
                vec![avg(col("temp")), min(col("temp"))],
            )?
            .project(vec![
                col("date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time)"),
                locf(col("MIN(temps.temp)"))?.alias("locf_min_temp"),
            ])?
            .build()?;

        insta::assert_yaml_snapshot!(
            format_analyzed_plan(plan)?,
            @r###"
        ---
        - "Projection: date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time), MIN(temps.temp) AS locf(MIN(temps.temp)) AS locf_min_temp"
        - "  GapFill: groupBy=[date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time)], aggr=[[AVG(temps.temp), LOCF(MIN(temps.temp))]], time_column=date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time), stride=IntervalDayTime(\"60000\"), range=Included(Literal(TimestampNanosecond(1000, Some(\"UTC\"))))..Excluded(Literal(TimestampNanosecond(2000, Some(\"UTC\"))))"
        - "    Aggregate: groupBy=[[date_bin(IntervalDayTime(\"60000\"), temps.time) AS date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time)]], aggr=[[AVG(temps.temp), MIN(temps.temp)]]"
        - "      Filter: temps.time >= TimestampNanosecond(1000, Some(\"UTC\")) AND temps.time < TimestampNanosecond(2000, Some(\"UTC\"))"
        - "        TableScan: temps"
        "###);
        Ok(())
    }

    #[test]
    fn with_interpolate() -> Result<()> {
        let plan = LogicalPlanBuilder::from(table_scan()?)
            .filter(
                col("time")
                    .gt_eq(lit_timestamptz_nano(1000))
                    .and(col("time").lt(lit_timestamptz_nano(2000))),
            )?
            .aggregate(
                vec![date_bin_gapfill(
                    lit(ScalarValue::IntervalDayTime(Some(60_000))),
                    col("time"),
                )?],
                vec![avg(col("temp")), min(col("temp"))],
            )?
            .project(vec![
                col("date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time)"),
                interpolate(col("AVG(temps.temp)"))?,
                interpolate(col("MIN(temps.temp)"))?,
            ])?
            .build()?;

        insta::assert_yaml_snapshot!(
            format_analyzed_plan(plan)?,
            @r###"
        ---
        - "Projection: date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time), AVG(temps.temp) AS interpolate(AVG(temps.temp)), MIN(temps.temp) AS interpolate(MIN(temps.temp))"
        - "  GapFill: groupBy=[date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time)], aggr=[[INTERPOLATE(AVG(temps.temp)), INTERPOLATE(MIN(temps.temp))]], time_column=date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time), stride=IntervalDayTime(\"60000\"), range=Included(Literal(TimestampNanosecond(1000, Some(\"UTC\"))))..Excluded(Literal(TimestampNanosecond(2000, Some(\"UTC\"))))"
        - "    Aggregate: groupBy=[[date_bin(IntervalDayTime(\"60000\"), temps.time) AS date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time)]], aggr=[[AVG(temps.temp), MIN(temps.temp)]]"
        - "      Filter: temps.time >= TimestampNanosecond(1000, Some(\"UTC\")) AND temps.time < TimestampNanosecond(2000, Some(\"UTC\"))"
        - "        TableScan: temps"
        "###);
        Ok(())
    }

    #[test]
    fn scan_filter_not_part_of_projection() {
        let schema = schema();
        let plan = table_scan_with_filters(
            Some("temps"),
            &schema,
            Some(vec![schema.index_of("time").unwrap()]),
            vec![
                col("temps.time").gt_eq(lit_timestamptz_nano(1000)),
                col("temps.time").lt(lit_timestamptz_nano(2000)),
                col("temps.loc").eq(lit("foo")),
            ],
        )
        .unwrap()
        .aggregate(
            vec![
                date_bin_gapfill(lit(ScalarValue::IntervalDayTime(Some(60_000))), col("time"))
                    .unwrap(),
            ],
            std::iter::empty::<Expr>(),
        )
        .unwrap()
        .build()
        .unwrap();

        insta::assert_yaml_snapshot!(
            format_analyzed_plan(plan).unwrap(),
            @r###"
        ---
        - "GapFill: groupBy=[date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time)], aggr=[[]], time_column=date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time), stride=IntervalDayTime(\"60000\"), range=Included(Literal(TimestampNanosecond(1000, Some(\"UTC\"))))..Excluded(Literal(TimestampNanosecond(2000, Some(\"UTC\"))))"
        - "  Aggregate: groupBy=[[date_bin(IntervalDayTime(\"60000\"), temps.time) AS date_bin_gapfill(IntervalDayTime(\"60000\"),temps.time)]], aggr=[[]]"
        - "    TableScan: temps projection=[time], full_filters=[temps.time >= TimestampNanosecond(1000, Some(\"UTC\")), temps.time < TimestampNanosecond(2000, Some(\"UTC\")), temps.loc = Utf8(\"foo\")]"
        "###);
    }
}
