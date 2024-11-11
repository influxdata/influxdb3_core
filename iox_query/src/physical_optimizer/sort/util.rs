use std::{collections::HashMap, sync::Arc};

use crate::{
    provider::{progressive_eval::ProgressiveEvalExec, DeduplicateExec},
    statistics::{column_statistics_min_max, compute_stats_column_min_max, overlap},
};
use arrow::compute::{rank, SortOptions};
use datafusion::{
    common::tree_node::{Transformed, TreeNode},
    datasource::physical_plan::{parquet::ParquetExecBuilder, ParquetExec},
    error::{DataFusionError, Result},
    physical_expr::PhysicalSortExpr,
    physical_plan::{
        expressions::{Column, Literal},
        limit::LocalLimitExec,
        projection::ProjectionExec,
        sorts::{sort::SortExec, sort_preserving_merge::SortPreservingMergeExec},
        union::UnionExec,
        visit_execution_plan, ExecutionPlan, ExecutionPlanVisitor,
    },
    scalar::ScalarValue,
};
use observability_deps::tracing::{trace, warn};

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
pub(crate) fn sort_plans_by_value_ranges(
    plans: Vec<Arc<dyn ExecutionPlan>>,
    value_ranges: Vec<(ScalarValue, ScalarValue)>,
    sort_options: SortOptions,
) -> Result<Option<PlansValueRanges>> {
    let Some(plans_and_value_ranges) = sort_by_value_ranges(plans, value_ranges, sort_options)?
    else {
        return Ok(None);
    };

    Ok(Some(PlansValueRanges {
        plans: plans_and_value_ranges.sorted_vec,
        value_ranges: plans_and_value_ranges.sorted_value_ranges,
    }))
}

/// A sorted vector and their corresponding sorted value ranges
struct SortedVecAndValueRanges<T> {
    pub sorted_vec: Vec<T>,
    // Min and max values of the plan on a specific column
    pub sorted_value_ranges: Vec<(ScalarValue, ScalarValue)>,
}

impl<T> SortedVecAndValueRanges<T> {
    fn new(sorted_vec: Vec<T>, sorted_value_ranges: Vec<(ScalarValue, ScalarValue)>) -> Self {
        Self {
            sorted_vec,
            sorted_value_ranges,
        }
    }
}

/// Sort the given vector by the given value ranges
/// Return none if
///    . the number of plans is not the same as the number of value ranges
///    . the value ranges overlap
/// Return the sorted plans and the sorted vallue ranges
fn sort_by_value_ranges<T>(
    plans: Vec<T>,
    value_ranges: Vec<(ScalarValue, ScalarValue)>,
    sort_options: SortOptions,
) -> Result<Option<SortedVecAndValueRanges<T>>> {
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
        return Ok(Some(SortedVecAndValueRanges::new(plans, value_ranges)));
    }

    // sort the plans by the ranks of their min values
    let mut plan_rank_zip: Vec<(T, u32)> = plans
        .into_iter()
        .zip(ranks.iter().copied())
        .collect::<Vec<_>>();
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

    Ok(Some(SortedVecAndValueRanges::new(plans, value_ranges)))
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

                // keep first column constant value
                first_col_vals.push(const_values[0].clone());

                // concatenate 2 constants used to sort the streams later
                let consts = const_values.iter().fold(String::new(), |acc, x| acc + x);
                // Since it is a constant, min and max will be the same
                let scalar_value = ScalarValue::from(consts);
                two_first_col_val_ranges.push((scalar_value.clone(), scalar_value));
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
                    let cur_table = std::mem::take(&mut current_table).unwrap();
                    value_ranges.push((
                        ScalarValue::from(cur_table.clone()),
                        ScalarValue::from(cur_table.clone()),
                    ));

                    let new_plan = match current_group.len() {
                        1 => current_group.pop().unwrap(),
                        _ => add_union_and_progressive_eval(
                            current_group,
                            ScalarValue::from(cur_table),
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
            let cur_table = std::mem::take(&mut current_table).unwrap();
            value_ranges.push((
                ScalarValue::from(cur_table.clone()),
                ScalarValue::from(cur_table.clone()),
            ));

            let new_plan = match current_group.len() {
                1 => current_group.pop().unwrap(),
                _ => add_union_and_progressive_eval(
                    current_group,
                    ScalarValue::from(cur_table),
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

// This is an optimization for the most recent value query `ORDER BY time DESC/ASC LIMIT n`
// See: https://github.com/influxdata/influxdb_iox/issues/12205
// The observation is recent data is mostly in first file,  so the plan should avoid reading the others unless necessary
//
/// This function is to split non-overlapped files in the same ParquetExec into different groups/DF partitions and
///  set the `preserve_partitioning` so they will be executed sequentially. Note that files in same group will are read sequentially.
/// If we cannot split them, we have to add SortPreservingMerge to keep data outputed in the right sort order
pub(crate) fn split_files_or_add_sort_preserving_merge(
    plans: Vec<Arc<dyn ExecutionPlan>>,
    sort_col_name: &str,
    sort_options: SortOptions,
    sort_exprs: &[PhysicalSortExpr],
    fetch_number: Option<usize>,
) -> Result<Vec<Arc<dyn ExecutionPlan>>> {
    plans
        .iter()
        .map(|input| {
            let mut new_input = Ok(Arc::clone(input));
            let mut consider_merging_inputs = true;

            // This is a subplan of single ParquetExec of all non-overlapped files
            if !has_deduplicate(input)? && only_one_parquet_exec(input)? {
                // Transform the ParquetExec to have one file each group so they can be executed sequentially
                new_input = transform_parquet_exec_single_file_each_group(
                    Arc::clone(input),
                    sort_col_name,
                    sort_options,
                );

                if new_input.is_ok() {
                    consider_merging_inputs = false;
                }
            }

            if consider_merging_inputs {
                // cannot split files, add SortPreservingMerge for multiple input partitions
                if input.properties().output_partitioning().partition_count() > 1 {
                    let sort_preserving_merge_exec = Arc::new(
                        SortPreservingMergeExec::new(sort_exprs.to_vec(), Arc::clone(input))
                            .with_fetch(fetch_number),
                    );
                    new_input = Ok(sort_preserving_merge_exec as _)
                } else {
                    new_input = Ok(Arc::clone(input))
                }
            }

            new_input
        })
        .collect::<Result<Vec<_>>>()
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

/// Transform a ParquetExec with N non-overlapped files of the ParquetExec into N groups each include one file.
///
/// The function is only called when the plan does not include DeduplicateExec and includes only one ParquetExec.
///
/// This function will return error if
///   - There are no statsitics for the given column (including the when the column is missing from the file
///     and produce null values that leads to absent statistics)
///   - Some files overlap (the min/max time ranges are disjoint)
///   - There is a DeduplicateExec in the plan which means the data of the plan overlaps
///
/// The output ParquetExec's are ordered such that the file with the most recent time ranges is read first
///
/// For example
/// ```text
/// ParquetExec(groups=[[file1, file2],[file3]])
/// ```
/// Is rewritten so each file is in its own group and the files are ordered by time range
/// ```text
/// ParquetExec(groups=[[file1], [file2],[file3]])
/// ```
pub(crate) fn transform_parquet_exec_single_file_each_group(
    plan: Arc<dyn ExecutionPlan>,
    sort_col_name: &str,
    sort_options: SortOptions,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut new_plan = plan
        .transform_up(|plan| {
            let Some(parquet_exec) = plan.as_any().downcast_ref::<ParquetExec>() else {
                return Ok(Transformed::no(plan));
            };

            // Extract parittioned files from the ParquetExec
            let base_config = parquet_exec.base_config();
            let schema = Arc::clone(&base_config.file_schema);
            let col_idx = schema.index_of(sort_col_name).map_err(|err| {
                DataFusionError::Plan(format!(
                    "Column {} not found in the schema {}: {}",
                    sort_col_name, schema, err
                ))
            })?;

            let mut new_base_config = base_config.clone();
            let partitioned_file_groups = &base_config.file_groups;

            // All partitioned files of the same file will be replaced with one partitioned file
            // Hashmap: Path --> PartitionedFile
            let mut new_partitioned_files = HashMap::new();
            for group in partitioned_file_groups {
                for partitioned_file in group {
                    // if the file is already in the new_partitioned_files, skip it
                    if new_partitioned_files.contains_key(&partitioned_file.object_meta.location) {
                        continue;
                    } else {
                        let mut partitioned_file = partitioned_file.clone();
                        partitioned_file.range = None;
                        new_partitioned_files.insert(
                            partitioned_file.object_meta.location.clone(),
                            partitioned_file,
                        );
                    }
                }
            }

            // convert the hashmap to a vector
            let new_partitioned_files = new_partitioned_files.into_values().collect::<Vec<_>>();

            // Sort paritioned files by their statistics min
            let value_ranges = {
                new_partitioned_files.iter().map(|file| {
                    let col_stats = &file
                        .statistics
                        .as_ref()
                        .ok_or_else(|| {
                            DataFusionError::Plan(format!(
                                "File statistics not available for file: {}",
                                file.path()
                            ))
                        })?
                        .column_statistics[col_idx];

                    let min = col_stats
                        .min_value
                        .get_value()
                        .ok_or_else(|| {
                            DataFusionError::Plan(format!(
                                "Min column ({}) statistics not available for file: {}",
                                sort_col_name,
                                file.path()
                            ))
                        })?
                        .clone();

                    let max = col_stats
                        .max_value
                        .get_value()
                        .ok_or_else(|| {
                            DataFusionError::Plan(format!(
                                "Max column ({}) statistics not available for file: {}",
                                sort_col_name,
                                file.path()
                            ))
                        })?
                        .clone();

                    Ok((min, max))
                })
            }
            .collect::<Result<Vec<_>>>()?;

            let Some(sorted_files_and_value_ranges) =
                sort_by_value_ranges(new_partitioned_files, value_ranges, sort_options)?
            else {
                warn!("Cannot sort non-overlapped files by their value ranges");
                return Err(datafusion::error::DataFusionError::Internal(
                    "Cannot sort non-overlapped files by their value ranges".to_string(),
                ));
            };

            let new_partitioned_files = sorted_files_and_value_ranges.sorted_vec;

            // Each partitioned file is in its own group
            let new_partitioned_file_groups = new_partitioned_files
                .into_iter()
                .map(|file| vec![file])
                .collect::<Vec<_>>();

            // Assigned new partitioned file groups to the new base config
            new_base_config.file_groups = new_partitioned_file_groups;

            // Make a ParquetExecBuilder from the existing ParquetExec
            // TODO: replace the below with simpler DF's new API when available
            //       https://github.com/apache/datafusion/issues/12737
            let mut builder = ParquetExecBuilder::new_with_options(
                new_base_config,
                parquet_exec.table_parquet_options().clone(),
            );
            if let Some(predicate) = parquet_exec.predicate() {
                builder = builder.with_predicate(Arc::clone(predicate));
            }
            let new_parquet_exec = builder.build();

            Ok(Transformed::yes(
                Arc::new(new_parquet_exec) as Arc<dyn ExecutionPlan>
            ))
        })
        .map(|t| t.data);

    // If new_plan is SortExec, set its preserve_partitioning to true to keep the transformed files above output sequentially
    if let Some(sort_exec) = new_plan
        .as_ref()
        .ok()
        .and_then(|plan| plan.as_any().downcast_ref::<SortExec>())
    {
        // Set preserve_partitioning to true if its ParquetExec has many output partitions
        if !sort_exec.preserve_partitioning()
            && sort_exec
                .input()
                .properties()
                .output_partitioning()
                .partition_count()
                > 1
        {
            new_plan = Ok(Arc::new(
                SortExec::new(sort_exec.expr().to_vec(), Arc::clone(sort_exec.input()))
                    .with_fetch(sort_exec.fetch())
                    .with_preserve_partitioning(true),
            ) as _);
        };
    }

    new_plan
}

// Recursively check if there is a `DeduplicateExec` in the plan
pub(crate) fn has_deduplicate(plan: &Arc<dyn ExecutionPlan>) -> Result<bool> {
    plan.exists(|plan| Ok(plan.as_any().downcast_ref::<DeduplicateExec>().is_some()))
}

/// Recursively check if there is only one ParquetExec
pub(crate) fn only_one_parquet_exec(plan: &Arc<dyn ExecutionPlan>) -> Result<bool> {
    let mut visitor = CountParquetExecVisitor { count: 0 };
    visit_execution_plan(&**plan, &mut visitor)?;
    Ok(visitor.count == 1)
}

#[derive(Debug)]
struct CountParquetExecVisitor {
    count: usize,
}

impl ExecutionPlanVisitor for CountParquetExecVisitor {
    type Error = datafusion::error::DataFusionError;

    fn pre_visit(&mut self, plan: &dyn ExecutionPlan) -> Result<bool, Self::Error> {
        if plan.as_any().downcast_ref::<ParquetExec>().is_some() {
            self.count += 1;
        }
        Ok(true)
    }
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

/// Find the [`UnionExec`] located as input to the [`SortPreservingMergeExec`].
///
/// This accepts specific locations for the union, in relation to the sort merge input.
pub(crate) fn accepted_union_exec(merge_node: &SortPreservingMergeExec) -> Option<&UnionExec> {
    if let Some(union_exec) = merge_node.input().as_any().downcast_ref::<UnionExec>() {
        return Some(union_exec);
    }

    // LIMITs may be applied locally, by pushing down closer to the data source.
    //
    // When a limit is pushed down, it will return a subset of the source rows.
    // However, it will not broaden the range of the data used (in crate::ValueRangeAndColValue)
    // to determine non-overlapping streams for ordering.
    //
    // The limit may narrow the range, but this is acceptable since the non-overlapping property
    // will still be retained.
    if let Some(union_exec) = merge_node
        .input()
        .as_any()
        .downcast_ref::<LocalLimitExec>()
        .and_then(|local_limit_exec| {
            local_limit_exec
                .input()
                .as_any()
                .downcast_ref::<UnionExec>()
        })
    {
        return Some(union_exec);
    }
    None
}
