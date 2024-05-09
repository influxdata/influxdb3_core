use std::sync::Arc;

use crate::{
    provider::progressive_eval::ProgressiveEvalExec,
    statistics::{column_statistics_min_max, compute_stats_column_min_max, overlap},
};
use arrow::compute::{rank, SortOptions};
use datafusion::{
    error::Result,
    physical_expr::PhysicalSortExpr,
    physical_plan::{
        expressions::{Column, Literal},
        projection::ProjectionExec,
        sorts::{sort::SortExec, sort_preserving_merge::SortPreservingMergeExec},
        union::UnionExec,
        ExecutionPlan,
    },
    scalar::ScalarValue,
};
use observability_deps::tracing::trace;

/// Compute statistics for the given plans on a given column name
/// Return none if the statistics are not available
pub(crate) fn collect_statistics_min_max(
    plans: &[Arc<dyn ExecutionPlan>],
    col_name: &str,
) -> Result<Option<Vec<(ScalarValue, ScalarValue)>>> {
    // temp solution while waiting for DF's statistics to get mature
    // Compute min max stats for all inputs of UnionExec on the sorted column
    // https://github.com/apache/arrow-datafusion/issues/8078
    let col_stats = plans
        .iter()
        .map(|plan| compute_stats_column_min_max(&**plan, col_name))
        .collect::<Result<Vec<_>>>()?;

    // If min and max not available, return none
    let mut value_ranges = Vec::with_capacity(col_stats.len());
    for stats in &col_stats {
        let Some((min, max)) = column_statistics_min_max(stats) else {
            trace!("-------- min_max not available");
            return Ok(None);
        };

        value_ranges.push((min, max));
    }

    // todo: use this when DF satistics is ready
    // // Get statistics for the inputs of UnionExec on the sorted column
    // let Some(value_ranges) = statistics_min_max(plans, col_name)
    // else {
    //     return Ok(None);
    // };

    Ok(Some(value_ranges))
}

/// Plans and their corresponding value ranges
pub(crate) struct PlansValueRanges {
    pub plans: Vec<Arc<dyn ExecutionPlan>>,
    // Min and max values of the plan on a specific column
    pub value_ranges: Vec<(ScalarValue, ScalarValue)>,
}

/// Sort the given plans by value ranges
/// Return none if
///    . the number of plans is not the same as the number of value ranges
///    . the value ranges overlap
pub(crate) fn sort_by_value_ranges(
    plans: Vec<Arc<dyn ExecutionPlan>>,
    value_ranges: Vec<(ScalarValue, ScalarValue)>,
    sort_options: SortOptions,
) -> Result<Option<PlansValueRanges>> {
    if plans.len() != value_ranges.len() {
        trace!(
            plans.len = plans.len(),
            value_ranges.len = value_ranges.len(),
            "--------- number of plans is not the same as the number of value ranges"
        );
        return Ok(None);
    }

    if overlap(&value_ranges)? {
        trace!("--------- value ranges overlap");
        return Ok(None);
    }

    // get the min value of each value range
    let min_iter = value_ranges.iter().map(|(min, _)| min.clone());
    let mins = ScalarValue::iter_to_array(min_iter)?;

    // rank the min values
    let ranks = rank(&*mins, Some(sort_options))?;

    // no need to sort if it is already sorted
    if ranks.iter().zip(ranks.iter().skip(1)).all(|(a, b)| a <= b) {
        return Ok(Some(PlansValueRanges {
            plans,
            value_ranges,
        }));
    }

    // sort the plans by the ranks of their min values
    let mut plan_rank_zip: Vec<(Arc<dyn ExecutionPlan>, u32)> =
        plans.into_iter().zip(ranks.clone()).collect::<Vec<_>>();
    plan_rank_zip.sort_by(|(_, min1), (_, min2)| min1.cmp(min2));
    let plans = plan_rank_zip
        .into_iter()
        .map(|(plan, _)| plan)
        .collect::<Vec<_>>();

    // Sort the value ranges by the ranks of their min values
    let mut value_range_rank_zip: Vec<((ScalarValue, ScalarValue), u32)> =
        value_ranges.into_iter().zip(ranks).collect::<Vec<_>>();
    value_range_rank_zip.sort_by(|(_, min1), (_, min2)| min1.cmp(min2));
    let value_ranges = value_range_rank_zip
        .into_iter()
        .map(|(value_range, _)| value_range)
        .collect::<Vec<_>>();

    Ok(Some(PlansValueRanges {
        plans,
        value_ranges,
    }))
}

/// Check if every sort expression is on a column
pub(crate) fn all_physical_sort_exprs_on_column(sort_exprs: &[PhysicalSortExpr]) -> bool {
    sort_exprs
        .iter()
        .all(|sort_expr| sort_expr.expr.as_any().downcast_ref::<Column>().is_some())
}

/// Check if every SortExec is on the same sort_exprs starting at a given position
/// sort_execs: Given SortExec plans
/// sort_exprs: Given sort expressions
/// start_position: The start position in the the sort expressions that we want to compare with the sort expressions of the SortExec
pub(crate) fn all_sort_execs_on_same_sort_exprs_from_starting_position(
    sort_execs: &[&SortExec],
    sort_exprs: &[PhysicalSortExpr],
    start_position: usize,
) -> bool {
    if sort_exprs.len() < start_position + 1 {
        return false;
    }

    let sort_exprs = &sort_exprs[start_position..];
    sort_execs
        .iter()
        .all(|sort_exec| sort_exec.expr() == sort_exprs)
}

/// This struct is mainly used to support the work described in OrderUnionSortedInputsForConstants
/// It will hold 2 vectors:
///  . value_ranges: The min and max values of a string which in the case is a concatenation of
///       the first and the second columns of a sort expression to be used to sort
///       the branches of the plan
/// . column_values: The first column values to be used as value ranges for the ProgressiveEval
pub(crate) struct ValueRangeAndColValues {
    pub value_ranges: Vec<(ScalarValue, ScalarValue)>,
    pub column_values: Vec<String>,
}

impl ValueRangeAndColValues {
    fn new(value_ranges: Vec<(ScalarValue, ScalarValue)>, column_values: Vec<String>) -> Self {
        Self {
            value_ranges,
            column_values,
        }
    }

    /// Check if under every SortExec is a ProjectionExec that includes the first 2 columns as constants
    /// And if so, collect those constants and build ValueRangeAndColValues with 2 vectors:
    ///   . First vector (value_ranges) is the concatenation of the first and the second so we can sort them correctly
    ///   . Second vector (column_values) is the first column constant values to be used as value ranges for the ProgressiveEval
    pub(crate) fn try_new(sort_execs: &[&SortExec], first_sort_col_name: &str) -> Result<Self> {
        let mut two_first_col_val_ranges: Vec<(ScalarValue, ScalarValue)> =
            Vec::with_capacity(sort_execs.len());
        let mut first_col_vals = Vec::with_capacity(sort_execs.len());
        for sort_exec in sort_execs {
            let sort_exec_input = sort_exec.input();
            let Some(projection) = sort_exec_input.as_any().downcast_ref::<ProjectionExec>() else {
                return Ok(Self::new(vec![], vec![]));
            };

            // Check if the projection includes the first column as a constant
            let projection_expr = projection.expr();
            if projection_expr.len() < 2 {
                return Ok(Self::new(vec![], vec![]));
            }

            // Check if all these conditions meet:
            //   . first expr's name is the same as the first_sort_col_name and expr is a constant/litertal
            //   . second expr is a constant/litertal
            // Example:
            //     expr.0 : Literal { value: Dictionary(Int32, Utf8("m0")) }    <-- constant
            //     expr.1 : "iox::measurement"                                  <-- column name
            if projection_expr[0].1 == first_sort_col_name   // first expr's name is the same as the first_sort_col_name
                // first two exprs are constants/litertals
                && projection_expr[0].0.as_any().is::<Literal>() && projection_expr[1].0.as_any().is::<Literal>()
            {
                // collect & concatenate 2 constants
                let mut const_values = Vec::with_capacity(2);
                for expr in projection_expr.iter().take(2) {
                    let constant_value = expr
                        .0
                        .as_any()
                        .downcast_ref::<Literal>()
                        .unwrap()
                        .value()
                        .to_string();
                    const_values.push(constant_value);
                }

                // keep first column costant value
                first_col_vals.push(const_values[0].clone());

                // concatenate 2 constants used to sort the streams later
                let consts = const_values.iter().fold(String::new(), |acc, x| acc + x);
                // Since it is a constant, min and max will be the same
                let scarlar_value = ScalarValue::from(consts);
                two_first_col_val_ranges.push((scarlar_value.clone(), scarlar_value));
            } else {
                return Ok(Self::new(vec![], vec![]));
            }
        }

        Ok(Self::new(two_first_col_val_ranges, first_col_vals))
    }

    /// Group plans with the same value of first column (aka measurement/table name in `show tag value``)
    /// Add ProgressiveEval on top of each group if it includes more than  one plan.
    /// The assumption is that every plan is sorted by the first column and return only one partition
    /// Return the grouped plans
    pub(crate) fn group_plans_by_value(
        plans: Vec<Arc<dyn ExecutionPlan>>,
        plan_tables: Vec<String>,
    ) -> PlansValueRanges {
        assert!(plans.len() == plan_tables.len());

        let mut grouped_plans = Vec::with_capacity(plans.len());
        let mut value_ranges = Vec::with_capacity(plans.len());
        let mut current_group = Vec::new();
        let mut current_table = None;
        for (plan, table) in plans.into_iter().zip(plan_tables) {
            if Some(&table) == current_table.as_ref() {
                current_group.push(plan);
            } else {
                if !current_group.is_empty() {
                    let cur_table = current_table.clone().unwrap();
                    value_ranges.push((
                        ScalarValue::from(cur_table.clone()),
                        ScalarValue::from(cur_table.clone()),
                    ));

                    let new_plan = match current_group.len() {
                        1 => current_group.pop().unwrap(),
                        _ => add_union_and_progressive_eval(
                            current_group,
                            ScalarValue::from(cur_table.clone()),
                            None,
                        ),
                    };
                    grouped_plans.push(new_plan);
                }

                current_group = vec![plan];
                current_table = Some(table);
            }
        }

        // last group
        if !current_group.is_empty() {
            let cur_table = current_table.clone().unwrap();
            value_ranges.push((
                ScalarValue::from(cur_table.clone()),
                ScalarValue::from(cur_table.clone()),
            ));

            let new_plan = match current_group.len() {
                1 => current_group.pop().unwrap(),
                _ => add_union_and_progressive_eval(
                    current_group,
                    ScalarValue::from(cur_table.clone()),
                    None,
                ),
            };
            grouped_plans.push(new_plan);
        }

        PlansValueRanges {
            plans: grouped_plans,
            value_ranges,
        }
    }
}

/// Add SortPreservingMerge to the plan with many partitions to ensure the order is preserved
pub(crate) fn add_sort_preserving_merge(
    plans: Vec<Arc<dyn ExecutionPlan>>,
    sort_exprs: &[PhysicalSortExpr],
    fetch_number: Option<usize>,
) -> Result<Vec<Arc<dyn ExecutionPlan>>> {
    plans
        .iter()
        .map(|input| {
            if input.properties().output_partitioning().partition_count() > 1 {
                // Add SortPreservingMergeExec on top of this input
                let sort_preserving_merge_exec = Arc::new(
                    SortPreservingMergeExec::new(sort_exprs.to_vec(), Arc::clone(input))
                        .with_fetch(fetch_number),
                );
                Ok(sort_preserving_merge_exec as _)
            } else {
                Ok(Arc::clone(input))
            }
        })
        .collect::<Result<Vec<_>>>()
}

/// Add union and then progressive eval on top of the plans
pub(crate) fn add_union_and_progressive_eval(
    plans: Vec<Arc<dyn ExecutionPlan>>,
    value_for_ranges: ScalarValue,
    fetch_number: Option<usize>,
) -> Arc<dyn ExecutionPlan> {
    assert!(plans.len() > 1);

    // make value ranges
    let value_ranges = (0..plans.len())
        .map(|_| (value_for_ranges.clone(), value_for_ranges.clone()))
        .collect::<Vec<_>>();

    // Create a union of all the plans
    let union_exec = Arc::new(UnionExec::new(plans));
    // Create a ProgressiveEval on top of the union
    let progressive_eval = ProgressiveEvalExec::new(union_exec, Some(value_ranges), fetch_number);

    Arc::new(progressive_eval) as Arc<dyn ExecutionPlan>
}
