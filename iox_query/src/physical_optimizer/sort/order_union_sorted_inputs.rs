use std::sync::Arc;

use datafusion::{
    common::tree_node::{Transformed, TreeNode},
    config::ConfigOptions,
    error::Result,
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{
        displayable, expressions::Column, sorts::sort_preserving_merge::SortPreservingMergeExec,
        union::UnionExec, ExecutionPlan,
    },
};
use observability_deps::tracing::{trace, warn};

use crate::{
    physical_optimizer::sort::util::{
        accepted_union_exec, collect_statistics_min_max, sort_plans_by_value_ranges,
        split_files_or_add_sort_preserving_merge,
    },
    provider::progressive_eval::ProgressiveEvalExec,
};

use super::util::{
    has_deduplicate, only_one_parquet_exec, transform_parquet_exec_single_file_each_group,
};

/// IOx specific optimization that eliminates a `SortPreservingMerge` by reordering inputs in terms
/// of their value ranges. If all inputs are non overlapping and ordered by value range, they can
/// be concatenated by `ProgressiveEval`  while maintaining the desired output order without
/// actually merging.
///
/// Find this structure:
///     SortPreservingMergeExec - on one column (DESC or ASC)
///         UnionExec
/// and if:
///
/// - all inputs of UnionExec are already sorted (or has SortExec) with sortExpr also on time DESC
///   or ASC accordingly and
/// - the streams do not overlap in values of the sorted column
///
/// do:
///
/// - order them by the sorted column DESC or ASC accordingly and
/// - replace SortPreservingMergeExec with ProgressiveEvalExec
///
/// Notes: The difference between SortPreservingMergeExec & ProgressiveEvalExec:
///
/// - SortPreservingMergeExec do the merge of sorted input streams. It needs each stream sorted but
///   the streams themselves can be in any random order and they can also overlap in values of
///   sorted columns.
/// - ProgressiveEvalExec only outputs data in their input order of the streams and not do any
///   merges. Thus in order to output data in the right sort order, these three conditions must be
///   true:
///     1. Each input stream must sorted on the same column DESC or ASC accordingly
///     2. The streams must be sorted on the column DESC or ASC accordingly
///     3. The streams must not overlap in the values of that column.
///
/// Example: for col_name ranges:
///   |--- r1---|-- r2 ---|-- r3 ---|-- r4 --|
///
/// Here is what the input look like:
/// (Note this optimization is focusing on query `order by time DESC/ASC limit n` without aggregation):
///
///   SortPreservingMergeExec: time@2 DESC, fetch=1
///     UnionExec
///       SortExec: expr=col_name@2 DESC  <--- input stream with col_name range r3
///         ...
///       SortExec: expr=col_name@2 DESC  <--- input stream with col_name range r1
///         ...
///       SortExec: expr=col_name@2 DESC  <--- input stream with col_name range r4
///         ...
///       SortExec: expr=col_name@2 DESC  <--- input stream with col_name range r2  -- assuming this SortExec has 2 output sorted streams
///          ...
///
/// The streams do not overlap in time, and they are already sorted by time DESC.
///
/// The output will be the same except that all the input streams will be sorted by time DESC too and looks like
///
///   SortPreservingMergeExec: time@2 DESC, fetch=1
///     UnionExec
///       SortExec: expr=col_name@2 DESC  <--- input stream with col_name range r1
///         ...
///       SortPreservingMergeExec:                                                  -- need this extra to merge the 2 streams into one
///          SortExec: expr=col_name@2 DESC  <--- input stream with col_name range r2
///             ...
///       SortExec: expr=col_name@2 DESC  <--- input stream with col_name range r3
///         ...
///       SortExec: expr=col_name@2 DESC  <--- input stream with col_name range r4
///          ...
///

#[derive(Debug)]
pub(crate) struct OrderUnionSortedInputs;

impl PhysicalOptimizerRule for OrderUnionSortedInputs {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|plan| {
            // Find SortPreservingMergeExec
            let Some(sort_preserving_merge_exec) =
                plan.as_any().downcast_ref::<SortPreservingMergeExec>()
            else {
                return Ok(Transformed::no(plan));
            };

            // Check if the sortExpr is only on one column
            let sort_expr = sort_preserving_merge_exec.expr();
            if sort_expr.len() != 1 {
                trace!(
                    ?sort_expr,
                    "sortExpr is not on one column. No optimization"
                );
                return Ok(Transformed::no(plan));
            };
            let Some(sorted_col) = sort_expr[0].expr.as_any().downcast_ref::<Column>() else {
                trace!(
                    ?sort_expr,
                    "sortExpr is not on pure column but expression. No optimization"
                );
                return Ok(Transformed::no(plan));
            };
            let sort_options = sort_expr[0].options;

            let mut input_value_ranges = None;
            // Find UnionExec
            let transformed_input_plan: Option<Arc<dyn ExecutionPlan>> = if let Some(union_exec) = accepted_union_exec(sort_preserving_merge_exec) {
                // Check all inputs of UnionExec must be already sorted and on the same sort_expr of SortPreservingMergeExec
                let Some(union_output_ordering) = union_exec.properties().output_ordering() else {
                    warn!(plan=%displayable(plan.as_ref()).indent(false), "Union input to SortPreservingMerge is not sorted");
                    return Ok(Transformed::no(plan));
                };

                // Check if the first PhysicalSortExpr is the same as the sortExpr[0] in SortPreservingMergeExec
                if sort_expr[0] != union_output_ordering[0] {
                    // this happens in production https://github.com/influxdata/influxdb_iox/issues/10641
                    trace!(?sort_expr, ?union_output_ordering, plan=%displayable(plan.as_ref()).indent(false),
                        "Sort order of SortPreservingMerge and its children are different");
                    return Ok(Transformed::no(plan));
                }

                let Some(value_ranges) = collect_statistics_min_max(union_exec.inputs(), sorted_col.name())?
                else {
                    return Ok(Transformed::no(plan));
                };

                // Sort the inputs by their value ranges
                trace!("value_ranges: {:?}", value_ranges);
                let Some(plans_value_ranges) =
                    sort_plans_by_value_ranges(union_exec.inputs().to_vec(), value_ranges, sort_options)?
                else {
                    trace!("inputs are not sorted by value ranges. No optimization");
                    return Ok(Transformed::no(plan));
                };

                // If each input of UnionExec outputs many sorted streams, data of different streams may overlap and
                // even if they do not overlapped, their streams can be in any order. We need to (sort) merge them first
                // to have a single output stream out to guarantee the output is sorted.
                let new_inputs = split_files_or_add_sort_preserving_merge(plans_value_ranges.plans, sorted_col.name(), sort_options, sort_expr, sort_preserving_merge_exec.fetch())?;
                input_value_ranges = Some(plans_value_ranges.value_ranges);

                Some(Arc::new(UnionExec::new(new_inputs)))

            } else { // No union under SortPreservingMergeExec

                // No union means underneath SortPreservingMergeExec is one input. Still optimize using ProgressiveEval if
                // it is a LIMIT plan of all non-overlapped parquet files. It means we need to check for:
                //   1. limit
                //   2. no DeduplicateExec
                //   3. one ParquetExec
                //   4. and all files of that ParquetExec are non-overlapped

                let input = sort_preserving_merge_exec.input();
                let is_limit_query = sort_preserving_merge_exec.fetch().is_some();

                // three top conditions meet
                let try_transform = is_limit_query && !has_deduplicate(input)? && only_one_parquet_exec(input)?;

                let transformed_input_plan = if try_transform {
                    // get statistics of the files
                    input_value_ranges = collect_statistics_min_max(&[Arc::clone(input)], sorted_col.name())?;
                    // transform the files if the files are non-overlapped
                    let transformed_input_plan = transform_parquet_exec_single_file_each_group(Arc::clone(input), sorted_col.name(), sort_options);

                    if let Ok(transformed_input_plan) = transformed_input_plan {
                        Some(transformed_input_plan)
                    } else {
                        None
                    }
                } else {
                    None
                };

                transformed_input_plan
            };

            let Some(transformed_input_plan) = transformed_input_plan else {
                return Ok(Transformed::no(plan));
            };

            // Replace SortPreservingMergeExec with ProgressiveEvalExec
            let progresive_eval_exec = Arc::new(ProgressiveEvalExec::new(
                transformed_input_plan,
                input_value_ranges,
                sort_preserving_merge_exec.fetch(),
            ));

            Ok(Transformed::yes(progresive_eval_exec))
        }).map(|t| t.data)
    }

    fn name(&self) -> &str {
        "order_union_sorted_inputs"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::{compute::SortOptions, datatypes::SchemaRef};
    use datafusion::{
        datasource::provider_as_source,
        logical_expr::{LogicalPlanBuilder, Operator},
        physical_expr::PhysicalSortExpr,
        physical_plan::{
            expressions::{BinaryExpr, Column},
            limit::GlobalLimitExec,
            projection::ProjectionExec,
            repartition::RepartitionExec,
            sorts::{sort::SortExec, sort_preserving_merge::SortPreservingMergeExec},
            union::UnionExec,
            ExecutionPlan, Partitioning, PhysicalExpr,
        },
        prelude::{col, lit},
        scalar::ScalarValue,
    };
    use executor::DedicatedExecutor;
    use schema::{sort::SortKey, InfluxFieldType, SchemaBuilder as IOxSchemaBuilder};

    use crate::{
        exec::{Executor, ExecutorConfig},
        physical_optimizer::{
            sort::{
                order_union_sorted_inputs::OrderUnionSortedInputs,
                util::{has_deduplicate, only_one_parquet_exec},
            },
            test_util::OptimizationTest,
        },
        provider::{chunks_to_physical_nodes, DeduplicateExec, ProviderBuilder, RecordBatchesExec},
        statistics::{column_statistics_min_max, compute_stats_column_min_max},
        test::{format_execution_plan, TestChunk},
        QueryChunk, CHUNK_ORDER_COLUMN_NAME,
    };

    // ------------------------------------------------------------------
    // Test only one ParquetExec & no deduplicate
    // ------------------------------------------------------------------
    #[test]
    fn test_one_parquet_exec_and_has_dedup() {
        test_helpers::maybe_start_logging();

        let schema = schema();

        let final_sort_exprs = [("time", SortOp::Desc)];

        let target_partition = 3;
        let plan_parquet_1 = PlanBuilder::parquet_exec_non_overlapped_chunks(
            &schema,
            5,
            1000,
            200,
            target_partition,
        );
        insta::assert_yaml_snapshot!(
            plan_parquet_1.formatted(),
            @r#"- " ParquetExec: file_groups={3 groups: [[0.parquet, 3.parquet], [1.parquet, 4.parquet], [2.parquet]]}, projection=[col1, col2, field1, time, __chunk_order]""#
        );

        //   - "       ParquetExec: file_groups={3 groups: [[0.parquet, 3.parquet], [1.parquet, 4.parquet], [2.parquet]]}, projection=[col1, col2, field1, time, __chunk_order]"
        let plan = plan_parquet_1.clone().build();
        assert!(only_one_parquet_exec(&plan).unwrap());
        assert!(!has_deduplicate(&plan).unwrap());

        //   - "     SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[true]"
        //   - "       ParquetExec: file_groups={3 groups: [[0.parquet, 3.parquet], [1.parquet, 4.parquet], [2.parquet]]}, projection=[col1, col2, field1, time, __chunk_order]"
        let plan_sort_1 = plan_parquet_1.sort_with_preserve_partitioning(final_sort_exprs);
        let plan = plan_sort_1.clone().build();
        assert!(only_one_parquet_exec(&plan).unwrap());
        assert!(!has_deduplicate(&plan).unwrap());

        // ------------------------------------------------------------------
        // Sort plan of a parquet overlapped files
        // Two overlapped files [2001, 2202] and [2201, 2402]
        let plan_parquet_2 = PlanBuilder::parquet_exec_overlapped_chunks(&schema, 3, 2001, 200, 2);
        insta::assert_yaml_snapshot!(
            plan_parquet_2.formatted(),
            @r#"- " ParquetExec: file_groups={2 groups: [[0.parquet, 2.parquet], [1.parquet]]}, projection=[col1, col2, field1, time, __chunk_order]""#
        );

        //   - "           ParquetExec: file_groups={2 groups: [[0.parquet, 2.parquet], [1.parquet]]}, projection=[col1, col2, field1, time, __chunk_order]"
        let plan = plan_parquet_2.clone().build();
        assert!(only_one_parquet_exec(&plan).unwrap());
        assert!(!has_deduplicate(&plan).unwrap());

        // sort expression for deduplication
        let sort_exprs = [
            ("col2", SortOp::Asc),
            ("col1", SortOp::Asc),
            ("time", SortOp::Asc),
        ];
        //   - "         SortPreservingMergeExec: [col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
        //   - "           ParquetExec: file_groups={2 groups: [[0.parquet, 2.parquet], [1.parquet]]}, projection=[col1, col2, field1, time, __chunk_order]"
        let plan_spm_2 = plan_parquet_2.sort_preserving_merge(sort_exprs);
        let plan = plan_spm_2.clone().build();
        assert!(only_one_parquet_exec(&plan).unwrap());
        assert!(!has_deduplicate(&plan).unwrap());

        //   - "       DeduplicateExec: [col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
        //   - "         SortPreservingMergeExec: [col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
        //   - "           ParquetExec: file_groups={2 groups: [[0.parquet, 2.parquet], [1.parquet]]}, projection=[col1, col2, field1, time, __chunk_order]"
        let plan_dedup_2 = plan_spm_2.deduplicate(sort_exprs, false);
        let plan = plan_dedup_2.clone().build();
        assert!(only_one_parquet_exec(&plan).unwrap());
        assert!(has_deduplicate(&plan).unwrap());

        //   - "     SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
        //   - "       DeduplicateExec: [col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
        //   - "         SortPreservingMergeExec: [col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
        //   - "           ParquetExec: file_groups={2 groups: [[0.parquet, 2.parquet], [1.parquet]]}, projection=[col1, col2, field1, time, __chunk_order]"
        let plan_sort_2 = plan_dedup_2.sort(final_sort_exprs);
        let plan = plan_sort_2.clone().build();
        assert!(only_one_parquet_exec(&plan).unwrap());
        assert!(has_deduplicate(&plan).unwrap());

        // union 2 plan
        //   - "   UnionExec"
        //   - "     SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[true]"
        //   - "       ParquetExec: file_groups={3 groups: [[0.parquet, 3.parquet], [1.parquet, 4.parquet], [2.parquet]]}, projection=[col1, col2, field1, time, __chunk_order]"
        //   - "     SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
        //   - "       DeduplicateExec: [col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
        //   - "         SortPreservingMergeExec: [col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
        //   - "           ParquetExec: file_groups={2 groups: [[0.parquet, 2.parquet], [1.parquet]]}, projection=[col1, col2, field1, time, __chunk_order]"
        let plan_union = plan_sort_1.union(plan_sort_2);
        let plan = plan_union.clone().build();
        assert!(!only_one_parquet_exec(&plan).unwrap());
        assert!(has_deduplicate(&plan).unwrap());

        //   - " SortPreservingMergeExec: [time@3 DESC NULLS LAST]"
        //   - "   UnionExec"
        //   - "     SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[true]"
        //   - "       ParquetExec: file_groups={3 groups: [[0.parquet, 3.parquet], [1.parquet, 4.parquet], [2.parquet]]}, projection=[col1, col2, field1, time, __chunk_order]"
        //   - "     SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
        //   - "       DeduplicateExec: [col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
        //   - "         SortPreservingMergeExec: [col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
        //   - "           ParquetExec: file_groups={2 groups: [[0.parquet, 2.parquet], [1.parquet]]}, projection=[col1, col2, field1, time, __chunk_order]"
        let plan_spm = plan_union.sort_preserving_merge(final_sort_exprs);
        let plan = plan_spm.clone().build();
        assert!(!only_one_parquet_exec(&plan).unwrap());
        assert!(has_deduplicate(&plan).unwrap());
    }

    // ------------------------------------------------------------------
    // Positive tests: the right structure found -> plan optimized
    // ------------------------------------------------------------------

    #[test]
    fn test_limit_mix_record_batch_parquet_2_desc() {
        test_helpers::maybe_start_logging();

        let schema = schema();
        let sort_exprs = [
            ("col2", SortOp::Asc),
            ("col1", SortOp::Asc),
            ("time", SortOp::Asc),
        ];

        let plan_parquet = PlanBuilder::parquet_exec(&schema, 1000, 2000);
        let plan_parquet2 = PlanBuilder::parquet_exec(&schema, 2001, 3000);
        let plan_batches = PlanBuilder::record_batches_exec(2, 2500, 3500);

        let plan_sort1 = plan_batches.sort(sort_exprs);
        let plan_union_1 = plan_sort1.union(plan_parquet2);
        let plan_spm_for_dedupe = plan_union_1.sort_preserving_merge(sort_exprs);
        let plan_dedupe = plan_spm_for_dedupe.deduplicate(sort_exprs, false);

        let sort_exprs = [("time", SortOp::Desc)];
        let plan_sort2 = plan_parquet.sort(sort_exprs);
        let plan_sort3 = plan_dedupe.sort(sort_exprs);

        let plan_union_2 = plan_sort2.union(plan_sort3);

        let plan_spm = plan_union_2.sort_preserving_merge(sort_exprs);

        let plan_limit = plan_spm.limit(0, Some(1));

        let opt = OrderUnionSortedInputs;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_limit.build(), opt),
            @r#"
        input:
          - " GlobalLimitExec: skip=0, fetch=1"
          - "   SortPreservingMergeExec: [time@3 DESC NULLS LAST]"
          - "     UnionExec"
          - "       SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
          - "         ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
          - "       SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
          - "         DeduplicateExec: [col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
          - "           SortPreservingMergeExec: [col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
          - "             UnionExec"
          - "               SortExec: expr=[col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST], preserve_partitioning=[false]"
          - "                 RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
          - "               ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
        output:
          Ok:
            - " GlobalLimitExec: skip=0, fetch=1"
            - "   ProgressiveEvalExec: input_ranges=[(TimestampNanosecond(2001, None), TimestampNanosecond(3500, None)), (TimestampNanosecond(1000, None), TimestampNanosecond(2000, None))]"
            - "     UnionExec"
            - "       SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
            - "         DeduplicateExec: [col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
            - "           SortPreservingMergeExec: [col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
            - "             UnionExec"
            - "               SortExec: expr=[col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST], preserve_partitioning=[false]"
            - "                 RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
            - "               ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
            - "       SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
            - "         ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
        "#
        );
    }

    // test on non-time column & order desc
    #[test]
    fn test_limit_mix_record_batch_parquet_non_time_sort_desc() {
        test_helpers::maybe_start_logging();

        let schema = schema();
        let sort_exprs = [
            ("col2", SortOp::Asc),
            ("col1", SortOp::Asc),
            ("time", SortOp::Asc),
        ];

        let plan_parquet = PlanBuilder::parquet_exec(&schema, 1000, 2000);
        let plan_parquet2 = PlanBuilder::parquet_exec(&schema, 2001, 3000);
        let plan_batches = PlanBuilder::record_batches_exec(2, 2500, 3500);

        let plan_sort1 = plan_batches.sort(sort_exprs);
        let plan_union_1 = plan_sort1.union(plan_parquet2);

        let plan_spm_for_dedupe = plan_union_1.sort_preserving_merge(sort_exprs);
        let plan_dedupe = plan_spm_for_dedupe.deduplicate(sort_exprs, false);

        let sort_exprs = [("field1", SortOp::Desc)];

        let plan_sort2 = plan_parquet.sort(sort_exprs);
        let plan_sort3 = plan_dedupe.sort(sort_exprs);

        let plan_union_2 = plan_sort2.union(plan_sort3);

        let plan_spm = plan_union_2.sort_preserving_merge(sort_exprs);

        let plan_limit = plan_spm.limit(0, Some(1));

        // Output plan: the 2 SortExecs will be swapped the order
        let opt = OrderUnionSortedInputs;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_limit.build(), opt),
            @r#"
        input:
          - " GlobalLimitExec: skip=0, fetch=1"
          - "   SortPreservingMergeExec: [field1@2 DESC NULLS LAST]"
          - "     UnionExec"
          - "       SortExec: expr=[field1@2 DESC NULLS LAST], preserve_partitioning=[false]"
          - "         ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
          - "       SortExec: expr=[field1@2 DESC NULLS LAST], preserve_partitioning=[false]"
          - "         DeduplicateExec: [col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
          - "           SortPreservingMergeExec: [col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
          - "             UnionExec"
          - "               SortExec: expr=[col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST], preserve_partitioning=[false]"
          - "                 RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
          - "               ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
        output:
          Ok:
            - " GlobalLimitExec: skip=0, fetch=1"
            - "   ProgressiveEvalExec: input_ranges=[(Int64(2001), Int64(3500)), (Int64(1000), Int64(2000))]"
            - "     UnionExec"
            - "       SortExec: expr=[field1@2 DESC NULLS LAST], preserve_partitioning=[false]"
            - "         DeduplicateExec: [col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
            - "           SortPreservingMergeExec: [col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
            - "             UnionExec"
            - "               SortExec: expr=[col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST], preserve_partitioning=[false]"
            - "                 RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
            - "               ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
            - "       SortExec: expr=[field1@2 DESC NULLS LAST], preserve_partitioning=[false]"
            - "         ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
        "#
        );
    }

    // test on non-time column & order asc
    #[test]
    fn test_limit_mix_record_batch_parquet_non_time_sort_asc() {
        test_helpers::maybe_start_logging();

        let schema = schema();
        let sort_exprs = [
            ("col2", SortOp::Asc),
            ("col1", SortOp::Asc),
            ("time", SortOp::Asc),
        ];

        let plan_parquet = PlanBuilder::parquet_exec(&schema, 1000, 2000);
        let plan_parquet2 = PlanBuilder::parquet_exec(&schema, 2001, 3000);
        let plan_batches = PlanBuilder::record_batches_exec(2, 2500, 3500);

        let plan_sort1 = plan_batches.sort(sort_exprs);
        let plan_union_1 = plan_sort1.union(plan_parquet2);
        let plan_spm_for_dedupe = plan_union_1.sort_preserving_merge(sort_exprs);
        let plan_dedupe = plan_spm_for_dedupe.deduplicate(sort_exprs, false);

        let sort_exprs = [("field1", SortOp::Asc)];

        let plan_sort2 = plan_parquet.sort(sort_exprs);
        let plan_sort3 = plan_dedupe.sort(sort_exprs);

        let plan_union_2 = plan_sort2.union(plan_sort3);

        let plan_spm = plan_union_2.sort_preserving_merge(sort_exprs);

        let plan_limit = plan_spm.limit(0, Some(1));

        // input and output are the same
        let opt = OrderUnionSortedInputs;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_limit.build(), opt),
            @r#"
        input:
          - " GlobalLimitExec: skip=0, fetch=1"
          - "   SortPreservingMergeExec: [field1@2 ASC NULLS LAST]"
          - "     UnionExec"
          - "       SortExec: expr=[field1@2 ASC NULLS LAST], preserve_partitioning=[false]"
          - "         ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
          - "       SortExec: expr=[field1@2 ASC NULLS LAST], preserve_partitioning=[false]"
          - "         DeduplicateExec: [col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
          - "           SortPreservingMergeExec: [col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
          - "             UnionExec"
          - "               SortExec: expr=[col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST], preserve_partitioning=[false]"
          - "                 RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
          - "               ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
        output:
          Ok:
            - " GlobalLimitExec: skip=0, fetch=1"
            - "   ProgressiveEvalExec: input_ranges=[(Int64(1000), Int64(2000)), (Int64(2001), Int64(3500))]"
            - "     UnionExec"
            - "       SortExec: expr=[field1@2 ASC NULLS LAST], preserve_partitioning=[false]"
            - "         ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
            - "       SortExec: expr=[field1@2 ASC NULLS LAST], preserve_partitioning=[false]"
            - "         DeduplicateExec: [col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
            - "           SortPreservingMergeExec: [col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
            - "             UnionExec"
            - "               SortExec: expr=[col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST], preserve_partitioning=[false]"
            - "                 RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
            - "               ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
        "#
        );
    }

    // No limit & but the input is in the right sort preserving merge struct --> optimize
    #[test]
    fn test_spm_time_desc() {
        test_helpers::maybe_start_logging();

        let schema = schema();
        let sort_exprs = [
            ("col2", SortOp::Asc),
            ("col1", SortOp::Asc),
            ("time", SortOp::Asc),
        ];

        let plan_parquet = PlanBuilder::parquet_exec(&schema, 1000, 2000);
        let plan_parquet2 = PlanBuilder::parquet_exec(&schema, 2001, 3000);
        let plan_batches = PlanBuilder::record_batches_exec(2, 2500, 3500);

        let plan_sort1 = plan_batches.sort(sort_exprs);
        let plan_union_1 = plan_sort1.union(plan_parquet2);
        let plan_spm_for_dedupe = plan_union_1.sort_preserving_merge(sort_exprs);
        let plan_dedupe = plan_spm_for_dedupe.deduplicate(sort_exprs, false);

        let sort_exprs = [("time", SortOp::Desc)];
        let plan_sort1 = plan_parquet.sort(sort_exprs);
        let plan_sort2 = plan_dedupe.sort(sort_exprs);

        let plan_union_2 = plan_sort1.union(plan_sort2);

        let plan_spm = plan_union_2.sort_preserving_merge(sort_exprs);

        // Output plan: the 2 SortExecs will be swapped the order
        let opt = OrderUnionSortedInputs;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_spm.build(), opt),
            @r#"
        input:
          - " SortPreservingMergeExec: [time@3 DESC NULLS LAST]"
          - "   UnionExec"
          - "     SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
          - "       ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
          - "     SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
          - "       DeduplicateExec: [col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
          - "         SortPreservingMergeExec: [col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
          - "           UnionExec"
          - "             SortExec: expr=[col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST], preserve_partitioning=[false]"
          - "               RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
          - "             ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
        output:
          Ok:
            - " ProgressiveEvalExec: input_ranges=[(TimestampNanosecond(2001, None), TimestampNanosecond(3500, None)), (TimestampNanosecond(1000, None), TimestampNanosecond(2000, None))]"
            - "   UnionExec"
            - "     SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
            - "       DeduplicateExec: [col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
            - "         SortPreservingMergeExec: [col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
            - "           UnionExec"
            - "             SortExec: expr=[col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST], preserve_partitioning=[false]"
            - "               RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
            - "             ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
            - "     SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
            - "       ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
        "#
        );
    }

    // No limit & but the input is in the right sort preserving merge struct --> optimize
    #[test]
    fn test_spm_non_time_desc() {
        test_helpers::maybe_start_logging();

        // plan:
        //  SortPreservingMerge: [field1@2 DESC]
        //    UnionExec
        //      SortExec: expr=[field1@2 DESC]
        //        ParquetExec
        //      SortExec: expr=[field1@2 DESC]
        //        UnionExec
        //          RecordBatchesExec
        //          ParquetExec
        //
        // Output: 2 SortExec are swapped

        let schema = schema();
        let sort_exprs = [
            ("col2", SortOp::Asc),
            ("col1", SortOp::Asc),
            ("time", SortOp::Asc),
        ];

        let plan_parquet = PlanBuilder::parquet_exec(&schema, 1000, 2000);
        let plan_parquet2 = PlanBuilder::parquet_exec(&schema, 2001, 3000);
        let plan_batches = PlanBuilder::record_batches_exec(2, 2500, 3500);

        let plan_sort1 = plan_batches.sort(sort_exprs);
        let plan_union_1 = plan_sort1.union(plan_parquet2);
        let plan_spm_for_dedupe = plan_union_1.sort_preserving_merge(sort_exprs);
        let plan_dedupe = plan_spm_for_dedupe.deduplicate(sort_exprs, false);

        let sort_exprs = [("field1", SortOp::Desc)];
        let plan_sort1 = plan_parquet.sort(sort_exprs);
        let plan_sort2 = plan_dedupe.sort(sort_exprs);

        let plan_union_2 = plan_sort1.union(plan_sort2);

        let plan_spm = plan_union_2.sort_preserving_merge(sort_exprs);

        // Output plan: the 2 SortExecs will be swapped the order
        let opt = OrderUnionSortedInputs;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_spm.build(), opt),
            @r#"
        input:
          - " SortPreservingMergeExec: [field1@2 DESC NULLS LAST]"
          - "   UnionExec"
          - "     SortExec: expr=[field1@2 DESC NULLS LAST], preserve_partitioning=[false]"
          - "       ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
          - "     SortExec: expr=[field1@2 DESC NULLS LAST], preserve_partitioning=[false]"
          - "       DeduplicateExec: [col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
          - "         SortPreservingMergeExec: [col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
          - "           UnionExec"
          - "             SortExec: expr=[col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST], preserve_partitioning=[false]"
          - "               RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
          - "             ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
        output:
          Ok:
            - " ProgressiveEvalExec: input_ranges=[(Int64(2001), Int64(3500)), (Int64(1000), Int64(2000))]"
            - "   UnionExec"
            - "     SortExec: expr=[field1@2 DESC NULLS LAST], preserve_partitioning=[false]"
            - "       DeduplicateExec: [col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
            - "         SortPreservingMergeExec: [col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
            - "           UnionExec"
            - "             SortExec: expr=[col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST], preserve_partitioning=[false]"
            - "               RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
            - "             ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
            - "     SortExec: expr=[field1@2 DESC NULLS LAST], preserve_partitioning=[false]"
            - "       ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
        "#
        );
    }

    // No limit & but the input is in the right sort preserving merge struct --> optimize
    #[test]
    fn test_spm_non_time_asc() {
        test_helpers::maybe_start_logging();

        let schema = schema();
        let sort_exprs = [
            ("col2", SortOp::Asc),
            ("col1", SortOp::Asc),
            ("time", SortOp::Asc),
        ];

        let plan_parquet = PlanBuilder::parquet_exec(&schema, 1000, 2000);
        let plan_parquet2 = PlanBuilder::parquet_exec(&schema, 2001, 3000);
        let plan_batches = PlanBuilder::record_batches_exec(2, 2500, 3500);

        let plan_sort1 = plan_batches.sort(sort_exprs);
        let plan_union_1 = plan_sort1.union(plan_parquet2);
        let plan_spm_for_dedupe = plan_union_1.sort_preserving_merge(sort_exprs);
        let plan_dedupe = plan_spm_for_dedupe.deduplicate(sort_exprs, false);

        let sort_exprs = [("field1", SortOp::Asc)];
        let plan_sort1 = plan_parquet.sort(sort_exprs);
        let plan_sort2 = plan_dedupe.sort(sort_exprs);

        let plan_union_2 = plan_sort1.union(plan_sort2);

        let plan_spm = plan_union_2.sort_preserving_merge(sort_exprs);

        // output stays the same as input
        let opt = OrderUnionSortedInputs;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_spm.build(), opt),
            @r#"
        input:
          - " SortPreservingMergeExec: [field1@2 ASC NULLS LAST]"
          - "   UnionExec"
          - "     SortExec: expr=[field1@2 ASC NULLS LAST], preserve_partitioning=[false]"
          - "       ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
          - "     SortExec: expr=[field1@2 ASC NULLS LAST], preserve_partitioning=[false]"
          - "       DeduplicateExec: [col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
          - "         SortPreservingMergeExec: [col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
          - "           UnionExec"
          - "             SortExec: expr=[col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST], preserve_partitioning=[false]"
          - "               RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
          - "             ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
        output:
          Ok:
            - " ProgressiveEvalExec: input_ranges=[(Int64(1000), Int64(2000)), (Int64(2001), Int64(3500))]"
            - "   UnionExec"
            - "     SortExec: expr=[field1@2 ASC NULLS LAST], preserve_partitioning=[false]"
            - "       ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
            - "     SortExec: expr=[field1@2 ASC NULLS LAST], preserve_partitioning=[false]"
            - "       DeduplicateExec: [col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
            - "         SortPreservingMergeExec: [col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
            - "           UnionExec"
            - "             SortExec: expr=[col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST], preserve_partitioning=[false]"
            - "               RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
            - "             ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
        "#
        );
    }

    #[test]
    fn test_spm_time_desc_with_dedupe_and_proj() {
        test_helpers::maybe_start_logging();

        // plan:
        //  SortPreservingMerge: [time@2 DESC]
        //    UnionExec
        //      SortExec: expr=[time@2 DESC]   -- time range [1000, 2000]
        //        ProjectionExec: expr=[time]
        //          ParquetExec                           -- [1000, 2000]
        //      SortExec: expr=[time@2 DESC]   -- time range [2001, 3500] from combine time range of record batches & parquet
        //        ProjectionExec: expr=[time]
        //          DeduplicateExec: [col1, col2, time]
        //              SortPreservingMergeExec: [col1 ASC, col2 ASC, time ASC]
        //                  UnionExec
        //                      SortExec: expr=[col1 ASC, col2 ASC, time ASC]
        //                          RecordBatchesExec           -- 2 chunks [2500, 3500]
        //                      SortExec: expr=[col1 ASC, col2 ASC, time ASC]
        //                          ParquetExec                     -- [2001, 3000]
        //
        // Output: 2 SortExec are swapped

        let schema = schema();

        let final_sort_exprs = [("time", SortOp::Desc)];

        // Sort plan of the first parquet:
        //      SortExec: expr=[time@2 DESC]   -- time range [1000, 2000]
        //        ProjectionExec: expr=[time]
        //          ParquetExec
        let plan_parquet_1 = PlanBuilder::parquet_exec(&schema, 1000, 2000);
        let plan_projection_1 = plan_parquet_1.project(["time"]);
        let plan_sort1 = plan_projection_1.sort(final_sort_exprs);

        // Sort plan of the second parquet and the record batch
        //      SortExec: expr=[time@2 DESC]   -- time range [2001, 3500] from combine time range of record batches & parquet
        //        ProjectionExec: expr=[time]
        //          DeduplicateExec: [col1, col2, time]
        //              SortPreservingMergeExec: [col1 ASC, col2 ASC, time ASC]
        //                  UnionExec
        //                      SortExec: expr=[col1 ASC, col2 ASC, time ASC]
        //                          RecordBatchesExec           -- 2 chunks [2500, 3500]
        //                      SortExec: expr=[col1 ASC, col2 ASC, time ASC]
        //                          ParquetExec                     -- [2001, 3000]
        let plan_parquet_2 = PlanBuilder::parquet_exec(&schema, 2001, 3000);
        let plan_batches = PlanBuilder::record_batches_exec(2, 2500, 3500);
        let dedupe_sort_exprs = [
            ("col1", SortOp::Asc),
            ("col2", SortOp::Asc),
            ("time", SortOp::Asc),
        ];
        let plan_sort_rb = plan_batches.sort(dedupe_sort_exprs);
        let plan_sort_pq = plan_parquet_2.sort(dedupe_sort_exprs);
        let plan_union_1 = plan_sort_rb.union(plan_sort_pq);
        let plan_spm_1 = plan_union_1.sort_preserving_merge(dedupe_sort_exprs);

        let plan_dedupe = plan_spm_1.deduplicate(dedupe_sort_exprs, false);
        let plan_projection_2 = plan_dedupe.project(["time"]);
        let plan_sort2 = plan_projection_2.sort(final_sort_exprs);

        // Union them together
        let plan_union_2 = plan_sort1.union(plan_sort2);

        // SortPreservingMerge them
        let plan_spm = plan_union_2.sort_preserving_merge(final_sort_exprs);

        // compute statistics
        let min_max_spm = compute_stats_column_min_max(plan_spm.inner(), "time").unwrap();
        let min_max = column_statistics_min_max(&min_max_spm).unwrap();
        assert_eq!(
            min_max,
            (
                ScalarValue::TimestampNanosecond(Some(1000), None),
                ScalarValue::TimestampNanosecond(Some(3500), None)
            )
        );

        // Output plan: the 2 SortExecs will be swapped the order
        let opt = OrderUnionSortedInputs;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_spm.build(), opt),
            @r#"
        input:
          - " SortPreservingMergeExec: [time@0 DESC NULLS LAST]"
          - "   UnionExec"
          - "     SortExec: expr=[time@0 DESC NULLS LAST], preserve_partitioning=[false]"
          - "       ProjectionExec: expr=[time@3 as time]"
          - "         ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
          - "     SortExec: expr=[time@0 DESC NULLS LAST], preserve_partitioning=[false]"
          - "       ProjectionExec: expr=[time@3 as time]"
          - "         DeduplicateExec: [col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST]"
          - "           SortPreservingMergeExec: [col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST]"
          - "             UnionExec"
          - "               SortExec: expr=[col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST], preserve_partitioning=[false]"
          - "                 RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
          - "               SortExec: expr=[col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST], preserve_partitioning=[false]"
          - "                 ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
        output:
          Ok:
            - " ProgressiveEvalExec: input_ranges=[(TimestampNanosecond(2001, None), TimestampNanosecond(3500, None)), (TimestampNanosecond(1000, None), TimestampNanosecond(2000, None))]"
            - "   UnionExec"
            - "     SortExec: expr=[time@0 DESC NULLS LAST], preserve_partitioning=[false]"
            - "       ProjectionExec: expr=[time@3 as time]"
            - "         DeduplicateExec: [col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST]"
            - "           SortPreservingMergeExec: [col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST]"
            - "             UnionExec"
            - "               SortExec: expr=[col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST], preserve_partitioning=[false]"
            - "                 RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
            - "               SortExec: expr=[col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST], preserve_partitioning=[false]"
            - "                 ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
            - "     SortExec: expr=[time@0 DESC NULLS LAST], preserve_partitioning=[false]"
            - "       ProjectionExec: expr=[time@3 as time]"
            - "         ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
        "#
        );
    }

    // Test split non-overlapped parquet files in the same parquet exec
    // 5 non-overlapped files split into 3 DF partitions
    #[test]
    fn test_split_partitioned_files_in_multiple_groups() {
        test_helpers::maybe_start_logging();

        let schema = schema();

        let final_sort_exprs = [("time", SortOp::Desc)];

        // ------------------------------------------------------------------
        // Sort plan of the first parquet of 5 non-overlapped files split into 3 DF partitions:
        //      SortExec: expr=[time@2 DESC]   -- time range [1000, 1999]
        //          ParquetExec: file_groups={3 groups: [[0.parquet, 3.parquet], [1.parquet, 4.parquet], [2.parquet]]}, ..."

        // 5 non-overlapped files split into 3 DF partitions that cover the time range [1000, 1999]
        // Larger the file name more recent their time range
        //   . 0.parquet: [1000, 1199]
        //   . 1.parquet: [1200, 1399]
        //   . 2.parquet: [1400, 1599]
        //   . 3.parquet: [1600, 1799]
        //   . 4.parquet: [1800, 1999]
        let target_partition = 3;
        let plan_parquet_1 = PlanBuilder::parquet_exec_non_overlapped_chunks(
            &schema,
            5,
            1000,
            200,
            target_partition,
        );
        insta::assert_yaml_snapshot!(
            plan_parquet_1.formatted(),
            @r#"- " ParquetExec: file_groups={3 groups: [[0.parquet, 3.parquet], [1.parquet, 4.parquet], [2.parquet]]}, projection=[col1, col2, field1, time, __chunk_order]""#
        );

        let plan_sort1 = plan_parquet_1.sort_with_preserve_partitioning(final_sort_exprs);

        let min_max_sort1 = compute_stats_column_min_max(plan_sort1.inner(), "time").unwrap();
        let min_max = column_statistics_min_max(&min_max_sort1).unwrap();
        assert_eq!(
            min_max,
            (
                ScalarValue::TimestampNanosecond(Some(1000), None),
                ScalarValue::TimestampNanosecond(Some(1999), None)
            )
        );

        // ------------------------------------------------------------------
        // Sort plan of a parquet overlapped with the record batch:
        let plan_parquet_2 = PlanBuilder::parquet_exec(&schema, 2001, 3000);
        let plan_batches = PlanBuilder::record_batches_exec(2, 2500, 3500);
        let dedupe_sort_exprs = [
            ("col1", SortOp::Asc),
            ("col2", SortOp::Asc),
            ("time", SortOp::Asc),
        ];
        let plan_sort_rb = plan_batches.sort(dedupe_sort_exprs);
        let plan_union_1 = plan_sort_rb.union(plan_parquet_2);
        let plan_spm_1 = plan_union_1.sort_preserving_merge(dedupe_sort_exprs);

        let plan_dedupe = plan_spm_1.deduplicate(dedupe_sort_exprs, false);
        let plan_sort2 = plan_dedupe.sort(final_sort_exprs);

        let plan_union_2 = plan_sort1.union(plan_sort2);

        let plan_spm = plan_union_2.sort_preserving_merge(final_sort_exprs);

        // Differences between the input and output plan:
        //   1. The top SortPreservingMergeExec is replaced with ProgressiveEvalExec to get the
        //      benefits of reading its inputs sequentailly and stop when the limit hits
        //   2. The two SortExecs are swap order to have latest time range as first input to
        //      garuantee correct results
        //   3. Five non-overlapped parquet files are now in 5 different groups one each and
        //      sorted by final_sort_exprs which is time DESC. This is needed to ensure files are
        //      executed sequentially, one by one, and the latest time range is read first by the ProgressiveEvalExec above
        //      - File are sorted in time descending order: 4.parquet, 3.parquet, 2.parquet, 1.parquet, 0.parquet
        //      - Larger the file name represents more recent their time range
        let opt = OrderUnionSortedInputs;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_spm.build(), opt),
            @r#"
        input:
          - " SortPreservingMergeExec: [time@3 DESC NULLS LAST]"
          - "   UnionExec"
          - "     SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[true]"
          - "       ParquetExec: file_groups={3 groups: [[0.parquet, 3.parquet], [1.parquet, 4.parquet], [2.parquet]]}, projection=[col1, col2, field1, time, __chunk_order]"
          - "     SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
          - "       DeduplicateExec: [col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST]"
          - "         SortPreservingMergeExec: [col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST]"
          - "           UnionExec"
          - "             SortExec: expr=[col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST], preserve_partitioning=[false]"
          - "               RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
          - "             ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
        output:
          Ok:
            - " ProgressiveEvalExec: input_ranges=[(TimestampNanosecond(2001, None), TimestampNanosecond(3500, None)), (TimestampNanosecond(1000, None), TimestampNanosecond(1999, None))]"
            - "   UnionExec"
            - "     SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
            - "       DeduplicateExec: [col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST]"
            - "         SortPreservingMergeExec: [col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST]"
            - "           UnionExec"
            - "             SortExec: expr=[col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST], preserve_partitioning=[false]"
            - "               RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
            - "             ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
            - "     SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[true]"
            - "       ParquetExec: file_groups={5 groups: [[4.parquet], [3.parquet], [2.parquet], [1.parquet], [0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
        "#
        );
    }

    // Test split non-overlapped parquet files in the same parquet exec
    // 5 non-overlapped files all in the same DF partition/group and preserve_partitioning is set to true
    #[test]
    fn test_split_partitioned_files_in_one_group() {
        test_helpers::maybe_start_logging();

        let schema = schema();

        let final_sort_exprs = [("time", SortOp::Desc)];

        // ------------------------------------------------------------------
        // Sort plan of the first parquet of 5 non-overlapped files all in one group:
        //      SortExec: expr=[time@2 DESC]   -- time range [1000, 1999]
        //          ParquetExec: file_groups={1 group: [[0.parquet, 1.parquet, 2.parquet, 3.parquet, 4.parquet]]}, ..."

        // 5 non-overlapped files all in one DF partitions that cover the time range [1000, 1999]
        // Larger the file name more recent their time range
        //   . 0.parquet: [1000, 1199]
        //   . 1.parquet: [1200, 1399]
        //   . 2.parquet: [1400, 1599]
        //   . 3.parquet: [1600, 1799]
        //   . 4.parquet: [1800, 1999]
        let target_partition = 1;
        let plan_parquet_1 = PlanBuilder::parquet_exec_non_overlapped_chunks(
            &schema,
            5,
            1000,
            200,
            target_partition,
        );
        insta::assert_yaml_snapshot!(
            plan_parquet_1.formatted(),
            @r#"- " ParquetExec: file_groups={1 group: [[0.parquet, 1.parquet, 2.parquet, 3.parquet, 4.parquet]]}, projection=[col1, col2, field1, time, __chunk_order]""#
        );

        let plan_sort1 = plan_parquet_1.sort_with_preserve_partitioning(final_sort_exprs);
        // verify preserve_partitioning is set to true from the function above
        insta::assert_yaml_snapshot!(
            plan_sort1.formatted(),
            @r#"
        - " SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[true]"
        - "   ParquetExec: file_groups={1 group: [[0.parquet, 1.parquet, 2.parquet, 3.parquet, 4.parquet]]}, projection=[col1, col2, field1, time, __chunk_order]"
        "#
        );

        let min_max_sort1 = compute_stats_column_min_max(plan_sort1.inner(), "time").unwrap();
        let min_max = column_statistics_min_max(&min_max_sort1).unwrap();
        assert_eq!(
            min_max,
            (
                ScalarValue::TimestampNanosecond(Some(1000), None),
                ScalarValue::TimestampNanosecond(Some(1999), None)
            )
        );

        // ------------------------------------------------------------------
        // Sort plan of a parquet overlapped with the record batch:
        let plan_parquet_2 = PlanBuilder::parquet_exec(&schema, 2001, 3000);
        let plan_batches = PlanBuilder::record_batches_exec(2, 2500, 3500);
        let dedupe_sort_exprs = [
            ("col1", SortOp::Asc),
            ("col2", SortOp::Asc),
            ("time", SortOp::Asc),
        ];
        let plan_sort_rb = plan_batches.sort(dedupe_sort_exprs);
        let plan_sort_pq = plan_parquet_2.sort(dedupe_sort_exprs);
        let plan_union_1 = plan_sort_rb.union(plan_sort_pq);
        let plan_spm_1 = plan_union_1.sort_preserving_merge(dedupe_sort_exprs);

        let plan_dedupe = plan_spm_1.deduplicate(dedupe_sort_exprs, false);
        let plan_sort2 = plan_dedupe.sort(final_sort_exprs);

        let plan_union_2 = plan_sort1.union(plan_sort2);

        let plan_spm = plan_union_2.sort_preserving_merge(final_sort_exprs);

        // Differences between the input and output plan:
        //   1. The top SortPreservingMergeExec is replaced with ProgressiveEvalExec
        //   2. The two SortExecs are swap order to have latest time range first
        //   3. Five non-overlapped parquet files are now in 5 different groups one eachand sorted by final_sort_exprs which is time DESC
        //      File are sorted in time descending order: 4.parquet, 3.parquet, 2.parquet, 1.parquet, 0.parquet
        //      Larger the file name more recent their time range
        let opt = OrderUnionSortedInputs;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_spm.build(), opt),
            @r#"
        input:
          - " SortPreservingMergeExec: [time@3 DESC NULLS LAST]"
          - "   UnionExec"
          - "     SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[true]"
          - "       ParquetExec: file_groups={1 group: [[0.parquet, 1.parquet, 2.parquet, 3.parquet, 4.parquet]]}, projection=[col1, col2, field1, time, __chunk_order]"
          - "     SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
          - "       DeduplicateExec: [col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST]"
          - "         SortPreservingMergeExec: [col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST]"
          - "           UnionExec"
          - "             SortExec: expr=[col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST], preserve_partitioning=[false]"
          - "               RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
          - "             SortExec: expr=[col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST], preserve_partitioning=[false]"
          - "               ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
        output:
          Ok:
            - " ProgressiveEvalExec: input_ranges=[(TimestampNanosecond(2001, None), TimestampNanosecond(3500, None)), (TimestampNanosecond(1000, None), TimestampNanosecond(1999, None))]"
            - "   UnionExec"
            - "     SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
            - "       DeduplicateExec: [col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST]"
            - "         SortPreservingMergeExec: [col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST]"
            - "           UnionExec"
            - "             SortExec: expr=[col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST], preserve_partitioning=[false]"
            - "               RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
            - "             SortExec: expr=[col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST], preserve_partitioning=[false]"
            - "               ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
            - "     SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[true]"
            - "       ParquetExec: file_groups={5 groups: [[4.parquet], [3.parquet], [2.parquet], [1.parquet], [0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
        "#
        );
    }

    // Reproducer of https://github.com/influxdata/influxdb_iox/issues/12584
    // Test split non-overlapped parquet files in the same parquet exec
    // 5 non-overlapped files all in the same DF partition/group and preserve_partitioning is set to false
    #[test]
    fn test_split_partitioned_files_in_one_group_and_preserve_partitioning_is_false() {
        test_helpers::maybe_start_logging();

        let schema = schema();

        let final_sort_exprs = [("time", SortOp::Desc)];

        // ------------------------------------------------------------------
        // Sort plan of the first parquet of 5 non-overlapped files all in one group:
        //      SortExec: expr=[time@2 DESC]   -- time range [1000, 1999]
        //          ParquetExec: file_groups={1 group: [[0.parquet, 1.parquet, 2.parquet, 3.parquet, 4.parquet]]}, ..."

        // 5 non-overlapped files all in one DF partitions that cover the time range [1000, 1999]
        // Larger the file name more recent their time range
        //   . 0.parquet: [1000, 1199]
        //   . 1.parquet: [1200, 1399]
        //   . 2.parquet: [1400, 1599]
        //   . 3.parquet: [1600, 1799]
        //   . 4.parquet: [1800, 1999]
        let target_partition = 1;
        let plan_parquet_1 = PlanBuilder::parquet_exec_non_overlapped_chunks(
            &schema,
            5,
            1000,
            200,
            target_partition,
        );
        insta::assert_yaml_snapshot!(
            plan_parquet_1.formatted(),
            @r#"- " ParquetExec: file_groups={1 group: [[0.parquet, 1.parquet, 2.parquet, 3.parquet, 4.parquet]]}, projection=[col1, col2, field1, time, __chunk_order]""#
        );

        let plan_sort1 =
            plan_parquet_1.sort_with_preserve_partitioning_setting(final_sort_exprs, false);
        // verify preserve_partitioning is false
        insta::assert_yaml_snapshot!(
            plan_sort1.formatted(),
            @r#"
        - " SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
        - "   ParquetExec: file_groups={1 group: [[0.parquet, 1.parquet, 2.parquet, 3.parquet, 4.parquet]]}, projection=[col1, col2, field1, time, __chunk_order]"
        "#
        );

        let min_max_sort1 = compute_stats_column_min_max(plan_sort1.inner(), "time").unwrap();
        let min_max = column_statistics_min_max(&min_max_sort1).unwrap();
        assert_eq!(
            min_max,
            (
                ScalarValue::TimestampNanosecond(Some(1000), None),
                ScalarValue::TimestampNanosecond(Some(1999), None)
            )
        );

        // ------------------------------------------------------------------
        // Sort plan of a parquet overlapped with the record batch
        // There must be SortPreservingMergeExec and DeduplicateExec in this subplan to deduplicate data of
        //   the record batch and the parquet file
        let plan_parquet_2 = PlanBuilder::parquet_exec(&schema, 2001, 3000);
        let plan_batches = PlanBuilder::record_batches_exec(2, 2500, 3500);
        let dedupe_sort_exprs = [
            ("col1", SortOp::Asc),
            ("col2", SortOp::Asc),
            ("time", SortOp::Asc),
        ];
        let plan_sort_rb = plan_batches.sort(dedupe_sort_exprs);
        let plan_sort_pq = plan_parquet_2.sort(dedupe_sort_exprs);
        let plan_union_1 = plan_sort_rb.union(plan_sort_pq);
        let plan_spm_1 = plan_union_1.sort_preserving_merge(dedupe_sort_exprs);

        let plan_dedupe = plan_spm_1.deduplicate(dedupe_sort_exprs, false);
        let plan_sort2 = plan_dedupe.sort(final_sort_exprs);

        // Union them together
        let plan_union_2 = plan_sort1.union(plan_sort2);

        // SortPreservingMerge them
        let plan_spm = plan_union_2.sort_preserving_merge(final_sort_exprs);

        // Differences between the input and output plan:
        //   1. The top SortPreservingMergeExec is replaced with ProgressiveEvalExec
        //   2. The two SortExecs are swap order to have latest time range first
        //   3. Five non-overlapped parquet files are now in 5 different groups one eachand sorted by final_sort_exprs which is time DESC
        //      File are sorted in time descending order: 4.parquet, 3.parquet, 2.parquet, 1.parquet, 0.parquet
        //      Larger the file name more recent their time range
        //   4. The  SortExec of the non-overlapped parquet files is now have preserve_partitioning set to true
        let opt = OrderUnionSortedInputs;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_spm.build(), opt),
            @r#"
        input:
          - " SortPreservingMergeExec: [time@3 DESC NULLS LAST]"
          - "   UnionExec"
          - "     SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
          - "       ParquetExec: file_groups={1 group: [[0.parquet, 1.parquet, 2.parquet, 3.parquet, 4.parquet]]}, projection=[col1, col2, field1, time, __chunk_order]"
          - "     SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
          - "       DeduplicateExec: [col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST]"
          - "         SortPreservingMergeExec: [col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST]"
          - "           UnionExec"
          - "             SortExec: expr=[col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST], preserve_partitioning=[false]"
          - "               RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
          - "             SortExec: expr=[col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST], preserve_partitioning=[false]"
          - "               ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
        output:
          Ok:
            - " ProgressiveEvalExec: input_ranges=[(TimestampNanosecond(2001, None), TimestampNanosecond(3500, None)), (TimestampNanosecond(1000, None), TimestampNanosecond(1999, None))]"
            - "   UnionExec"
            - "     SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
            - "       DeduplicateExec: [col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST]"
            - "         SortPreservingMergeExec: [col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST]"
            - "           UnionExec"
            - "             SortExec: expr=[col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST], preserve_partitioning=[false]"
            - "               RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
            - "             SortExec: expr=[col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST], preserve_partitioning=[false]"
            - "               ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
            - "     SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[true]"
            - "       ParquetExec: file_groups={5 groups: [[4.parquet], [3.parquet], [2.parquet], [1.parquet], [0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
        "#
        );
    }

    // Test mix of split non-overlapped parquet files in the same parquet exec & non split on overllaped parquet files
    //  . One parquet exec with 5 non-overlapped files split into 3 DF partitions
    //  . One parquet exec with 2 overlapped files
    #[test]
    fn test_split_mix() {
        test_helpers::maybe_start_logging();

        let schema = schema();

        let final_sort_exprs = [("time", SortOp::Desc)];

        // ------------------------------------------------------------------
        // Sort plan of the first parquet of 5 non-overlapped files split into 3 DF partitions:
        //      SortExec: expr=[time@2 DESC]   -- time range [1000, 1999]
        //          ParquetExec: file_groups={3 groups: [[0.parquet, 3.parquet], [1.parquet, 4.parquet], [2.parquet]]}, ..."

        // 5 non-overlapped files split into 3 DF partitions that cover the time range [1000, 1999]
        // Larger the file name more recent their time range
        //   . 0.parquet: [1000, 1199]
        //   . 1.parquet: [1200, 1399]
        //   . 2.parquet: [1400, 1599]
        //   . 3.parquet: [1600, 1799]
        //   . 4.parquet: [1800, 1999]
        let target_partition = 3;
        let plan_parquet_1 = PlanBuilder::parquet_exec_non_overlapped_chunks(
            &schema,
            5,
            1000,
            200,
            target_partition,
        );
        insta::assert_yaml_snapshot!(
            plan_parquet_1.formatted(),
            @r#"- " ParquetExec: file_groups={3 groups: [[0.parquet, 3.parquet], [1.parquet, 4.parquet], [2.parquet]]}, projection=[col1, col2, field1, time, __chunk_order]""#
        );

        let plan_sort_1 = plan_parquet_1.sort_with_preserve_partitioning(final_sort_exprs);
        let min_max_sort1 = compute_stats_column_min_max(plan_sort_1.inner(), "time").unwrap();
        let min_max = column_statistics_min_max(&min_max_sort1).unwrap();
        assert_eq!(
            min_max,
            (
                ScalarValue::TimestampNanosecond(Some(1000), None),
                ScalarValue::TimestampNanosecond(Some(1999), None)
            )
        );

        // ------------------------------------------------------------------
        // Sort plan of a parquet overlapped files
        // Two overlapped files [2001, 2202] and [2201, 2402]
        let plan_parquet_2 = PlanBuilder::parquet_exec_overlapped_chunks(&schema, 3, 2001, 200, 2);
        insta::assert_yaml_snapshot!(
            plan_parquet_2.formatted(),
            @r#"- " ParquetExec: file_groups={2 groups: [[0.parquet, 2.parquet], [1.parquet]]}, projection=[col1, col2, field1, time, __chunk_order]""#
        );

        // sort expression for deduplication
        let sort_exprs = [
            ("col2", SortOp::Asc),
            ("col1", SortOp::Asc),
            ("time", SortOp::Asc),
        ];
        let plan_spm_2 = plan_parquet_2.sort_preserving_merge(sort_exprs);
        let plan_dedup_2 = plan_spm_2.deduplicate(sort_exprs, false);
        let plan_sort_2 = plan_dedup_2.sort(final_sort_exprs);

        // union 2 plan
        let plan_union = plan_sort_1.union(plan_sort_2);

        let plan_spm = plan_union.sort_preserving_merge(final_sort_exprs);

        // Differences between the input and output plan:
        //   1. The top SortPreservingMergeExec is replaced with ProgressiveEvalExec
        //   2. The two SortExecs are swap order to have latest time range first
        //   3. Five non-overlapped parquet files are now in 5 different groups one eachand sorted by final_sort_exprs which is time DESC
        //      File are sorted in time descending order: 4.parquet, 3.parquet, 2.parquet, 1.parquet, 0.parquet
        //      Larger the file name more recent their time range
        //  Note that the other parquet exec with 2 non-overlapped files are not split
        let opt = OrderUnionSortedInputs;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_spm.build(), opt),
            @r#"
        input:
          - " SortPreservingMergeExec: [time@3 DESC NULLS LAST]"
          - "   UnionExec"
          - "     SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[true]"
          - "       ParquetExec: file_groups={3 groups: [[0.parquet, 3.parquet], [1.parquet, 4.parquet], [2.parquet]]}, projection=[col1, col2, field1, time, __chunk_order]"
          - "     SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
          - "       DeduplicateExec: [col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
          - "         SortPreservingMergeExec: [col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
          - "           ParquetExec: file_groups={2 groups: [[0.parquet, 2.parquet], [1.parquet]]}, projection=[col1, col2, field1, time, __chunk_order]"
        output:
          Ok:
            - " ProgressiveEvalExec: input_ranges=[(TimestampNanosecond(2001, None), TimestampNanosecond(2602, None)), (TimestampNanosecond(1000, None), TimestampNanosecond(1999, None))]"
            - "   UnionExec"
            - "     SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
            - "       DeduplicateExec: [col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
            - "         SortPreservingMergeExec: [col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
            - "           ParquetExec: file_groups={2 groups: [[0.parquet, 2.parquet], [1.parquet]]}, projection=[col1, col2, field1, time, __chunk_order]"
            - "     SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[true]"
            - "       ParquetExec: file_groups={5 groups: [[4.parquet], [3.parquet], [2.parquet], [1.parquet], [0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
        "#
        );
    }

    // ------------------------------------------------------------------
    // Negative tests: the right structure not found -> nothing optimized
    // ------------------------------------------------------------------

    // Right stucture but sort on 2 columns --> plan stays the same
    #[test]
    fn test_negative_spm_2_column_sort_desc() {
        test_helpers::maybe_start_logging();

        // plan:
        //  SortPreservingMerge: [time@3 DESC, field1@2 DESC]
        //    UnionExec
        //      SortExec: expr=[time@3 DESC, field1@2 DESC]
        //        ParquetExec
        //      SortExec: expr=[time@3 DESC, field1@2 DESC]
        //        UnionExec
        //          RecordBatchesExec
        //          ParquetExec
        //
        // Output: same as input

        let schema = schema();

        let plan_parquet = PlanBuilder::parquet_exec(&schema, 1000, 2000);
        let plan_parquet2 = PlanBuilder::parquet_exec(&schema, 2001, 3000);
        let plan_batches = PlanBuilder::record_batches_exec(2, 2500, 3500);

        let plan_union_1 = plan_batches.union(plan_parquet2);

        let sort_exprs = [("time", SortOp::Desc), ("field1", SortOp::Desc)];
        let plan_sort1 = plan_parquet.sort(sort_exprs);
        let plan_sort2 = plan_union_1.sort(sort_exprs);

        let plan_union_2 = plan_sort1.union(plan_sort2);

        let plan_spm = plan_union_2.sort_preserving_merge(sort_exprs);

        // input and output are the same
        let opt = OrderUnionSortedInputs;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_spm.build(), opt),
            @r#"
        input:
          - " SortPreservingMergeExec: [time@3 DESC NULLS LAST,field1@2 DESC NULLS LAST]"
          - "   UnionExec"
          - "     SortExec: expr=[time@3 DESC NULLS LAST,field1@2 DESC NULLS LAST], preserve_partitioning=[false]"
          - "       ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
          - "     SortExec: expr=[time@3 DESC NULLS LAST,field1@2 DESC NULLS LAST], preserve_partitioning=[false]"
          - "       UnionExec"
          - "         RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
          - "         ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
        output:
          Ok:
            - " SortPreservingMergeExec: [time@3 DESC NULLS LAST,field1@2 DESC NULLS LAST]"
            - "   UnionExec"
            - "     SortExec: expr=[time@3 DESC NULLS LAST,field1@2 DESC NULLS LAST], preserve_partitioning=[false]"
            - "       ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
            - "     SortExec: expr=[time@3 DESC NULLS LAST,field1@2 DESC NULLS LAST], preserve_partitioning=[false]"
            - "       UnionExec"
            - "         RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
            - "         ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
        "#
        );
    }

    // No limit  & random plan --> plan stay the same
    #[test]
    fn test_negative_no_limit() {
        test_helpers::maybe_start_logging();

        let schema = schema();
        let sort_exprs = [
            ("col2", SortOp::Asc),
            ("col1", SortOp::Asc),
            ("time", SortOp::Asc),
        ];

        let plan_parquet = PlanBuilder::parquet_exec(&schema, 1000, 2000);
        let plan_batches = PlanBuilder::record_batches_exec(2, 1500, 2500);

        let plan = plan_batches
            .union(plan_parquet)
            .round_robin_repartition(8)
            .hash_repartition(vec!["col2", "col1", "time"], 8)
            .sort(sort_exprs)
            .deduplicate(sort_exprs, true);

        // input and output are the same
        let opt = OrderUnionSortedInputs;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan.build(), opt),
            @r#"
        input:
          - " DeduplicateExec: [col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
          - "   SortExec: expr=[col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST], preserve_partitioning=[false]"
          - "     RepartitionExec: partitioning=Hash([col2@1, col1@0, time@3], 8), input_partitions=8"
          - "       RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=3"
          - "         UnionExec"
          - "           RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
          - "           ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
        output:
          Ok:
            - " DeduplicateExec: [col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST]"
            - "   SortExec: expr=[col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST], preserve_partitioning=[false]"
            - "     RepartitionExec: partitioning=Hash([col2@1, col1@0, time@3], 8), input_partitions=8"
            - "       RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=3"
            - "         UnionExec"
            - "           RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
            - "           ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
        "#
        );
    }

    // has limit but no sort preserving merge --> plan stay the same
    #[test]
    fn test_negative_limit_no_preserving_merge() {
        test_helpers::maybe_start_logging();

        let plan_batches1 = PlanBuilder::record_batches_exec(1, 1000, 2000);
        let plan_batches2 = PlanBuilder::record_batches_exec(3, 2001, 3000);
        let plan_batches3 = PlanBuilder::record_batches_exec(2, 2500, 3500);

        let plan_union_1 = plan_batches2.union(plan_batches3);

        let sort_exprs = [("time", SortOp::Desc)];
        let plan_sort1 = plan_batches1.sort(sort_exprs);
        let plan_sort2 = plan_union_1.sort(sort_exprs);

        let plan_union_2 = plan_sort1.union(plan_sort2);

        let plan_limit = plan_union_2.limit(0, Some(1));

        // input and output are the same
        let opt = OrderUnionSortedInputs;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_limit.build(), opt),
            @r#"
        input:
          - " GlobalLimitExec: skip=0, fetch=1"
          - "   UnionExec"
          - "     SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
          - "       RecordBatchesExec: chunks=1, projection=[col1, col2, field1, time, __chunk_order]"
          - "     SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
          - "       UnionExec"
          - "         RecordBatchesExec: chunks=3, projection=[col1, col2, field1, time, __chunk_order]"
          - "         RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
        output:
          Ok:
            - " GlobalLimitExec: skip=0, fetch=1"
            - "   UnionExec"
            - "     SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
            - "       RecordBatchesExec: chunks=1, projection=[col1, col2, field1, time, __chunk_order]"
            - "     SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
            - "       UnionExec"
            - "         RecordBatchesExec: chunks=3, projection=[col1, col2, field1, time, __chunk_order]"
            - "         RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
        "#
        );
    }

    // right structure and same sort order but inputs of uion overlap --> plan stay the same
    #[test]
    fn test_negative_overlap() {
        test_helpers::maybe_start_logging();

        // Input plan:
        //
        // GlobalLimitExec: skip=0, fetch=2
        //  SortPreservingMerge: [time@2 DESC]
        //    UnionExec
        //      SortExec: expr=[time@2 DESC]   -- time range [1000, 2000]  that overlaps with the other SorExec
        //        ParquetExec                         -- [1000, 2000]
        //      SortExec: expr=[time@2 DESC]   -- time range [2000, 3500] from combine time range of two record batches
        //        UnionExec
        //           SortExec: expr=[time@2 DESC]
        //              RecordBatchesExec             -- 2 chunks [2500, 3500]
        //           ParquetExec                      -- [2000, 3000]

        let schema = schema();
        let sort_exprs = [
            ("col2", SortOp::Asc),
            ("col1", SortOp::Asc),
            ("time", SortOp::Asc),
        ];

        let plan_parquet = PlanBuilder::parquet_exec(&schema, 1000, 2000);
        let plan_parquet2 = PlanBuilder::parquet_exec(&schema, 2000, 3000);
        let plan_batches = PlanBuilder::record_batches_exec(2, 2500, 3500);

        let plan_sort1 = plan_batches.sort(sort_exprs);
        let plan_union_1 = plan_sort1.union(plan_parquet2);

        let sort_exprs = [("time", SortOp::Desc)];
        let plan_sort2 = plan_parquet.sort(sort_exprs);
        let plan_sort3 = plan_union_1.sort(sort_exprs);

        let plan_union_2 = plan_sort2.union(plan_sort3);

        let plan_spm = plan_union_2.sort_preserving_merge(sort_exprs);

        let plan_limit = plan_spm.limit(0, Some(1));

        // input and output are the same
        let opt = OrderUnionSortedInputs;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_limit.build(), opt),
            @r#"
        input:
          - " GlobalLimitExec: skip=0, fetch=1"
          - "   SortPreservingMergeExec: [time@3 DESC NULLS LAST]"
          - "     UnionExec"
          - "       SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
          - "         ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
          - "       SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
          - "         UnionExec"
          - "           SortExec: expr=[col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST], preserve_partitioning=[false]"
          - "             RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
          - "           ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
        output:
          Ok:
            - " GlobalLimitExec: skip=0, fetch=1"
            - "   SortPreservingMergeExec: [time@3 DESC NULLS LAST]"
            - "     UnionExec"
            - "       SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
            - "         ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
            - "       SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
            - "         UnionExec"
            - "           SortExec: expr=[col2@1 ASC NULLS LAST,col1@0 ASC NULLS LAST,time@3 ASC NULLS LAST], preserve_partitioning=[false]"
            - "             RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
            - "           ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
        "#
        );
    }

    // No limit & but the input is in the right union struct --> plan stay the same
    #[test]
    fn test_negative_no_sortpreservingmerge_input_union() {
        test_helpers::maybe_start_logging();

        // plan:
        //    UnionExec
        //      SortExec: expr=[time@2 DESC]
        //        ParquetExec
        //      SortExec: expr=[time@2 DESC]
        //        UnionExec
        //          RecordBatchesExec
        //          ParquetExec

        let schema = schema();

        let plan_parquet = PlanBuilder::parquet_exec(&schema, 1000, 2000);
        let plan_parquet2 = PlanBuilder::parquet_exec(&schema, 2001, 3000);
        let plan_batches = PlanBuilder::record_batches_exec(2, 2500, 3500);

        let plan_union_1 = plan_batches.union(plan_parquet2);

        let sort_exprs = [("time", SortOp::Desc)];

        let plan_sort1 = plan_parquet.sort(sort_exprs);
        let plan_sort2 = plan_union_1.sort(sort_exprs);

        let plan_union_2 = plan_sort1.union(plan_sort2);

        // input and output are the same
        let opt = OrderUnionSortedInputs;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_union_2.build(), opt),
            @r#"
        input:
          - " UnionExec"
          - "   SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
          - "     ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
          - "   SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
          - "     UnionExec"
          - "       RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
          - "       ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
        output:
          Ok:
            - " UnionExec"
            - "   SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
            - "     ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
            - "   SortExec: expr=[time@3 DESC NULLS LAST], preserve_partitioning=[false]"
            - "     UnionExec"
            - "       RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
            - "       ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
        "#
        );
    }

    // Projection expression (field + field) ==> not optimze. Plan stays the same
    #[test]
    fn test_negative_spm_time_desc_with_dedupe_and_proj_on_expr() {
        test_helpers::maybe_start_logging();

        // plan:
        //  SortPreservingMerge: [time@2 DESC]
        //    UnionExec
        //      SortExec: expr=[time@2 DESC]   -- time range [1000, 2000]
        //        ProjectionExec: expr=[field1 + field1, time]                                <-- NOTE: has expresssion col1+col2
        //          ParquetExec                           -- [1000, 2000]
        //      SortExec: expr=[time@2 DESC]   -- time range [2001, 3500] from combine time range of record batches & parquet
        //        ProjectionExec: expr=[field1 + field1, time]                                <-- NOTE: has expresssion col1+col2
        //          DeduplicateExec: [col1, col2, time]
        //              SortPreservingMergeExec: [col1 ASC, col2 ASC, time ASC]
        //                  UnionExec
        //                      SortExec: expr=[col1 ASC, col2 ASC, time ASC]
        //                          RecordBatchesExec           -- 2 chunks [2500, 3500]
        //                      SortExec: expr=[col1 ASC, col2 ASC, time ASC]
        //                          ParquetExec                     -- [2001, 3000]

        let schema = schema();

        let final_sort_exprs = [("time", SortOp::Desc)];

        // Sort plan of the first parquet:
        //      SortExec: expr=[time@2 DESC]   -- time range [1000, 2000]
        //        ProjectionExec: expr=[field1 + field1, time]
        //          ParquetExec
        let plan_parquet_1 = PlanBuilder::parquet_exec(&schema, 1000, 2000);

        let field_expr = Arc::new(BinaryExpr::new(
            Arc::new(Column::new_with_schema("field1", &schema).unwrap()),
            Operator::Plus,
            Arc::new(Column::new_with_schema("field1", &schema).unwrap()),
        )) as Arc<dyn PhysicalExpr>;
        let project_exprs = vec![
            (Arc::clone(&field_expr), String::from("field")),
            (
                expr_col("time", &plan_parquet_1.schema()),
                String::from("time"),
            ),
        ];
        let plan_projection_1 = plan_parquet_1.project_with_exprs(project_exprs);

        let plan_sort1 = plan_projection_1.sort(final_sort_exprs);

        // Sort plan of the second parquet and the record batch
        //      SortExec: expr=[time@2 DESC]   -- time range [2001, 3500] from combine time range of record batches & parquet
        //        ProjectionExec: expr=[field1 + field1, time]
        //          DeduplicateExec: [col1, col2, time]
        //              SortPreservingMergeExec: [col1 ASC, col2 ASC, time ASC]
        //                  UnionExec
        //                      SortExec: expr=[col1 ASC, col2 ASC, time ASC]
        //                          RecordBatchesExec           -- 2 chunks [2500, 3500]
        //                      SortExec: expr=[col1 ASC, col2 ASC, time ASC]
        //                          ParquetExec                     -- [2001, 3000]
        let plan_parquet_2 = PlanBuilder::parquet_exec(&schema, 2001, 3000);
        let plan_batches = PlanBuilder::record_batches_exec(2, 2500, 3500);
        let dedupe_sort_exprs = [
            ("col1", SortOp::Asc),
            ("col2", SortOp::Asc),
            ("time", SortOp::Asc),
        ];
        let plan_sort_rb = plan_batches.sort(dedupe_sort_exprs);
        let plan_sort_pq = plan_parquet_2.sort(dedupe_sort_exprs);
        let plan_union_1 = plan_sort_rb.union(plan_sort_pq);
        let plan_spm_1 = plan_union_1.sort_preserving_merge(dedupe_sort_exprs);

        let plan_dedupe = plan_spm_1.deduplicate(dedupe_sort_exprs, false);
        let project_exprs = vec![
            (field_expr, String::from("field")),
            (
                expr_col("time", &plan_dedupe.schema()),
                String::from("time"),
            ),
        ];
        let plan_projection_2 = plan_dedupe.project_with_exprs(project_exprs);
        let plan_sort2 = plan_projection_2.sort(final_sort_exprs);

        // Union them together
        let plan_union_2 = plan_sort1.union(plan_sort2);

        // SortPreservingMerge them
        let plan_spm = plan_union_2.sort_preserving_merge(final_sort_exprs);

        // compute statistics: no stats becasue the ProjectionExec includes expression
        let min_max_spm = compute_stats_column_min_max(plan_spm.inner(), "time").unwrap();
        let min_max = column_statistics_min_max(&min_max_spm);
        assert!(min_max.is_none());

        // output plan stays the same
        let opt = OrderUnionSortedInputs;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_spm.build(), opt),
            @r#"
        input:
          - " SortPreservingMergeExec: [time@1 DESC NULLS LAST]"
          - "   UnionExec"
          - "     SortExec: expr=[time@1 DESC NULLS LAST], preserve_partitioning=[false]"
          - "       ProjectionExec: expr=[field1@2 + field1@2 as field, time@3 as time]"
          - "         ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
          - "     SortExec: expr=[time@1 DESC NULLS LAST], preserve_partitioning=[false]"
          - "       ProjectionExec: expr=[field1@2 + field1@2 as field, time@3 as time]"
          - "         DeduplicateExec: [col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST]"
          - "           SortPreservingMergeExec: [col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST]"
          - "             UnionExec"
          - "               SortExec: expr=[col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST], preserve_partitioning=[false]"
          - "                 RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
          - "               SortExec: expr=[col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST], preserve_partitioning=[false]"
          - "                 ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
        output:
          Ok:
            - " SortPreservingMergeExec: [time@1 DESC NULLS LAST]"
            - "   UnionExec"
            - "     SortExec: expr=[time@1 DESC NULLS LAST], preserve_partitioning=[false]"
            - "       ProjectionExec: expr=[field1@2 + field1@2 as field, time@3 as time]"
            - "         ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
            - "     SortExec: expr=[time@1 DESC NULLS LAST], preserve_partitioning=[false]"
            - "       ProjectionExec: expr=[field1@2 + field1@2 as field, time@3 as time]"
            - "         DeduplicateExec: [col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST]"
            - "           SortPreservingMergeExec: [col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST]"
            - "             UnionExec"
            - "               SortExec: expr=[col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST], preserve_partitioning=[false]"
            - "                 RecordBatchesExec: chunks=2, projection=[col1, col2, field1, time, __chunk_order]"
            - "               SortExec: expr=[col1@0 ASC NULLS LAST,col2@1 ASC NULLS LAST,time@3 ASC NULLS LAST], preserve_partitioning=[false]"
            - "                 ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[__chunk_order@4 ASC]"
        "#
        );
    }

    // Reproduce of https://github.com/influxdata/influxdb_iox/issues/12461#issuecomment-2430196754
    // The reproducer needs big non-overlapped files so its first physical plan will have ParquetExec with multiple
    // file groups, each file group has multiple partitioned files.
    // The  OrderUnionSortedInputs optimizer step will merge those partitioned files of the same file into one partitioned file
    // and each will be in its own file group
    #[tokio::test]
    async fn test_many_partition_files() {
        // DF session setup
        let config = ExecutorConfig {
            target_query_partitions: 4.try_into().unwrap(),
            ..ExecutorConfig::testing()
        };
        let exec = Executor::new_with_config_and_executor(config, DedicatedExecutor::new_testing());
        let ctx = exec.new_context();
        let state = ctx.inner().state();

        // chunks
        let c = TestChunk::new("t").with_tag_column("tag");

        // Ingester data time[90, 100]
        let c_mem = c
            .clone()
            .with_may_contain_pk_duplicates(true)
            .with_time_column_with_full_stats(Some(90), Some(100), 100_000, None);

        // Two files overlapping with each other and with c_mem
        //
        // File 1: time[90, 100] and overlaps with c_mem
        let c_file_1 = c
            .clone()
            .with_time_column_with_full_stats(Some(90), Some(100), 100_000, None)
            .with_dummy_parquet_file_and_size(1000)
            .with_may_contain_pk_duplicates(false)
            .with_sort_key(SortKey::from_columns([Arc::from("tag"), Arc::from("time")]));
        // File 2: overlaps with c_file_1 and c_mem
        let c_file_2 = c_file_1.clone();

        // Five files that are not overlapped with any
        let overlapped_c = c
            .clone()
            .with_sort_key(SortKey::from_columns([Arc::from("tag"), Arc::from("time")]))
            .with_dummy_parquet_file_and_size(100000000)
            .with_may_contain_pk_duplicates(false);
        //
        // File 3: time[65, 69] that is not overlapped any
        let c_file_3 = overlapped_c.clone().with_time_column_with_full_stats(
            Some(65),
            Some(69),
            1_000_000,
            None,
        );
        let c_file_4 = overlapped_c.clone().with_time_column_with_full_stats(
            Some(60),
            Some(64),
            1_000_000,
            None,
        );
        let c_file_5 = overlapped_c.clone().with_time_column_with_full_stats(
            Some(55),
            Some(58),
            1_000_000,
            None,
        );
        let c_file_6 = overlapped_c.clone().with_time_column_with_full_stats(
            Some(50),
            Some(54),
            1_000_000,
            None,
        );
        let c_file_7 = overlapped_c.clone().with_time_column_with_full_stats(
            Some(45),
            Some(49),
            1_000_000,
            None,
        );

        // Schema & provider
        let schema = c_mem.schema().clone();
        let provider = ProviderBuilder::new("t".into(), schema)
            .add_chunk(Arc::new(c_mem.clone().with_id(1).with_order(i64::MAX)))
            .add_chunk(Arc::new(c_file_1.with_id(2).with_order(2)))
            .add_chunk(Arc::new(c_file_2.with_id(3).with_order(3)))
            // add non-overlapped chunks in random order
            .add_chunk(Arc::new(c_file_7.with_id(8).with_order(8)))
            .add_chunk(Arc::new(c_file_3.with_id(4).with_order(4)))
            .add_chunk(Arc::new(c_file_5.with_id(6).with_order(6)))
            .add_chunk(Arc::new(c_file_6.with_id(7).with_order(7)))
            .add_chunk(Arc::new(c_file_4.with_id(5).with_order(5)))
            .build()
            .unwrap();

        // expression: time > 0
        let expr = col("time").gt(lit(ScalarValue::TimestampNanosecond(Some(0), None)));

        // logical plan: select * from t where time > 0 order by time desc limit 1
        let plan = LogicalPlanBuilder::scan(
            "t".to_owned(),
            provider_as_source(Arc::new(provider.clone())),
            None,
        )
        .unwrap()
        .filter(expr.clone())
        .unwrap()
        // order by time DESC
        .sort(vec![col("time").sort(false, true)])
        .unwrap()
        // limit
        .limit(0, Some(1))
        .unwrap()
        .build()
        .unwrap();

        // create physical plan
        let plan = state.create_physical_plan(&plan).await.unwrap();

        // Since this is time DESC, the ProgressiveEvalExec must reflect the correct input ranges from largest to smallest
        // The LAST ParquetExec must include non-overlapped files sorted from smallest file name: 4.parquet, 5.parquet, 6.parquet, 7.parquet, 8.parquet
        //   Note: larger file name includes older time range
        insta::assert_yaml_snapshot!(
            format_execution_plan(&plan),
            @r#"
        - " ProgressiveEvalExec: fetch=1, input_ranges=[(TimestampNanosecond(90, None), TimestampNanosecond(100, None)), (TimestampNanosecond(45, None), TimestampNanosecond(69, None))]"
        - "   UnionExec"
        - "     SortExec: TopK(fetch=1), expr=[time@1 DESC], preserve_partitioning=[false]"
        - "       ProjectionExec: expr=[tag@0 as tag, time@1 as time]"
        - "         DeduplicateExec: [tag@0 ASC,time@1 ASC]"
        - "           SortPreservingMergeExec: [tag@0 ASC,time@1 ASC,__chunk_order@2 ASC]"
        - "             UnionExec"
        - "               SortExec: expr=[tag@0 ASC,time@1 ASC,__chunk_order@2 ASC], preserve_partitioning=[true]"
        - "                 CoalesceBatchesExec: target_batch_size=8192"
        - "                   FilterExec: time@1 > 0"
        - "                     RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=1"
        - "                       RecordBatchesExec: chunks=1, projection=[tag, time, __chunk_order]"
        - "               ParquetExec: file_groups={4 groups: [[2.parquet:0..500], [3.parquet:0..500], [2.parquet:500..1000], [3.parquet:500..1000]]}, projection=[tag, time, __chunk_order], output_ordering=[tag@0 ASC, time@1 ASC, __chunk_order@2 ASC], predicate=time@1 > 0, pruning_predicate=CASE WHEN time_null_count@1 = time_row_count@2 THEN false ELSE time_max@0 > 0 END, required_guarantees=[]"
        - "     SortExec: TopK(fetch=1), expr=[time@1 DESC], preserve_partitioning=[true]"
        - "       ParquetExec: file_groups={5 groups: [[4.parquet], [5.parquet], [6.parquet], [7.parquet], [8.parquet]]}, projection=[tag, time], output_ordering=[tag@0 ASC, time@1 ASC], predicate=time@1 > 0, pruning_predicate=CASE WHEN time_null_count@1 = time_row_count@2 THEN false ELSE time_max@0 > 0 END, required_guarantees=[]"
        "#
        );

        // logical plan: select * from t where time > 0 order by time ASC limit 1
        let plan =
            LogicalPlanBuilder::scan("t".to_owned(), provider_as_source(Arc::new(provider)), None)
                .unwrap()
                .filter(expr)
                .unwrap()
                // order by time ASC
                .sort(vec![col("time").sort(true, true)])
                .unwrap()
                // limit
                .limit(0, Some(1))
                .unwrap()
                .build()
                .unwrap();

        // create physical plan
        let plan = state.create_physical_plan(&plan).await.unwrap();

        // Since this is time ASC, the ProgressiveEvalExec must reflect the correct input ranges from smallest to largest
        // The FRIST ParquetExec must include non-overlapped files sorted from largest file name: 8.parquet, 7.parquet, 6.parquet, 5.parquet, 4.parquet
        //   Note: larger file name includes older time range
        insta::assert_yaml_snapshot!(
            format_execution_plan(&plan),
            @r#"
        - " ProgressiveEvalExec: fetch=1, input_ranges=[(TimestampNanosecond(45, None), TimestampNanosecond(69, None)), (TimestampNanosecond(90, None), TimestampNanosecond(100, None))]"
        - "   UnionExec"
        - "     SortExec: TopK(fetch=1), expr=[time@1 ASC], preserve_partitioning=[true]"
        - "       ParquetExec: file_groups={5 groups: [[8.parquet], [7.parquet], [6.parquet], [5.parquet], [4.parquet]]}, projection=[tag, time], output_ordering=[tag@0 ASC, time@1 ASC], predicate=time@1 > 0, pruning_predicate=CASE WHEN time_null_count@1 = time_row_count@2 THEN false ELSE time_max@0 > 0 END, required_guarantees=[]"
        - "     SortExec: TopK(fetch=1), expr=[time@1 ASC], preserve_partitioning=[false]"
        - "       ProjectionExec: expr=[tag@0 as tag, time@1 as time]"
        - "         DeduplicateExec: [tag@0 ASC,time@1 ASC]"
        - "           SortPreservingMergeExec: [tag@0 ASC,time@1 ASC,__chunk_order@2 ASC]"
        - "             UnionExec"
        - "               SortExec: expr=[tag@0 ASC,time@1 ASC,__chunk_order@2 ASC], preserve_partitioning=[true]"
        - "                 CoalesceBatchesExec: target_batch_size=8192"
        - "                   FilterExec: time@1 > 0"
        - "                     RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=1"
        - "                       RecordBatchesExec: chunks=1, projection=[tag, time, __chunk_order]"
        - "               ParquetExec: file_groups={4 groups: [[2.parquet:0..500], [3.parquet:0..500], [2.parquet:500..1000], [3.parquet:500..1000]]}, projection=[tag, time, __chunk_order], output_ordering=[tag@0 ASC, time@1 ASC, __chunk_order@2 ASC], predicate=time@1 > 0, pruning_predicate=CASE WHEN time_null_count@1 = time_row_count@2 THEN false ELSE time_max@0 > 0 END, required_guarantees=[]"
        "#
        );
    }

    #[tokio::test]
    async fn test_many_partition_files_all_non_overlapped_files_no_ingester_data() {
        // DF session setup
        let config = ExecutorConfig {
            target_query_partitions: 4.try_into().unwrap(),
            ..ExecutorConfig::testing()
        };
        let exec = Executor::new_with_config_and_executor(config, DedicatedExecutor::new_testing());
        let ctx = exec.new_context();
        let state = ctx.inner().state();

        // chunks
        let c = TestChunk::new("t").with_tag_column("tag");

        // Five files that are not overlapped with any
        let overlapped_c = c
            .clone()
            .with_sort_key(SortKey::from_columns([Arc::from("tag"), Arc::from("time")]))
            .with_dummy_parquet_file_and_size(100000000)
            .with_may_contain_pk_duplicates(false);
        //
        let c_file_1 = overlapped_c.clone().with_time_column_with_full_stats(
            Some(65),
            Some(69),
            1_000_000,
            None,
        );
        let c_file_2 = overlapped_c.clone().with_time_column_with_full_stats(
            Some(60),
            Some(64),
            1_000_000,
            None,
        );
        let c_file_3 = overlapped_c.clone().with_time_column_with_full_stats(
            Some(55),
            Some(58),
            1_000_000,
            None,
        );
        let c_file_4 = overlapped_c.clone().with_time_column_with_full_stats(
            Some(50),
            Some(54),
            1_000_000,
            None,
        );
        let c_file_5 = overlapped_c.clone().with_time_column_with_full_stats(
            Some(45),
            Some(49),
            1_000_000,
            None,
        );

        // Schema & provider
        let schema = c_file_1.schema().clone();
        let provider = ProviderBuilder::new("t".into(), schema)
            // add non-overlapped chunks in random order
            .add_chunk(Arc::new(c_file_4.with_id(4).with_order(4)))
            .add_chunk(Arc::new(c_file_3.with_id(3).with_order(3)))
            .add_chunk(Arc::new(c_file_1.with_id(1).with_order(1)))
            .add_chunk(Arc::new(c_file_2.with_id(2).with_order(2)))
            .add_chunk(Arc::new(c_file_5.with_id(5).with_order(5)))
            .build()
            .unwrap();

        // expression: time > 0
        let expr = col("time").gt(lit(ScalarValue::TimestampNanosecond(Some(0), None)));

        // logical plan: select * from t where time > 0 order by time desc limit 1
        let plan = LogicalPlanBuilder::scan(
            "t".to_owned(),
            provider_as_source(Arc::new(provider.clone())),
            None,
        )
        .unwrap()
        .filter(expr.clone())
        .unwrap()
        // order by time DESC
        .sort(vec![col("time").sort(false, true)])
        .unwrap()
        // limit
        .limit(0, Some(1))
        .unwrap()
        .build()
        .unwrap();

        // create physical plan
        let plan = state.create_physical_plan(&plan).await.unwrap();

        // Since this is time DESC, the ProgressiveEvalExec must reflect the correct input ranges from largest to smallest
        // The LAST ParquetExec must include non-overlapped files sorted from smallest file name: 1.parquet, 2.parquet, 3.parquet, 4.parquet, 5.parquet
        //   Note: larger file name includes older time range
        insta::assert_yaml_snapshot!(
            format_execution_plan(&plan),
            @r#"
        - " ProgressiveEvalExec: fetch=1, input_ranges=[(TimestampNanosecond(45, None), TimestampNanosecond(69, None))]"
        - "   SortExec: TopK(fetch=1), expr=[time@1 DESC], preserve_partitioning=[true]"
        - "     ParquetExec: file_groups={5 groups: [[1.parquet], [2.parquet], [3.parquet], [4.parquet], [5.parquet]]}, projection=[tag, time], output_ordering=[tag@0 ASC, time@1 ASC], predicate=time@1 > 0, pruning_predicate=CASE WHEN time_null_count@1 = time_row_count@2 THEN false ELSE time_max@0 > 0 END, required_guarantees=[]"
        "#
        );

        // logical plan: select * from t where time > 0 order by time ASC limit 1
        let plan =
            LogicalPlanBuilder::scan("t".to_owned(), provider_as_source(Arc::new(provider)), None)
                .unwrap()
                .filter(expr)
                .unwrap()
                // order by time ASC
                .sort(vec![col("time").sort(true, true)])
                .unwrap()
                // limit
                .limit(0, Some(1))
                .unwrap()
                .build()
                .unwrap();

        // create physical plan
        let plan = state.create_physical_plan(&plan).await.unwrap();

        // Since this is time ASC, the ProgressiveEvalExec must reflect the correct input ranges from smallest to largest
        // The FRIST ParquetExec must include non-overlapped files sorted from largest file name: 5.parquet, 4.parquet, 3.parquet, 2.parquet, 1.parquet
        //   Note: larger file name includes older time range
        insta::assert_yaml_snapshot!(
            format_execution_plan(&plan),
            @r#"
        - " ProgressiveEvalExec: fetch=1, input_ranges=[(TimestampNanosecond(45, None), TimestampNanosecond(69, None))]"
        - "   SortExec: TopK(fetch=1), expr=[time@1 ASC], preserve_partitioning=[true]"
        - "     ParquetExec: file_groups={5 groups: [[5.parquet], [4.parquet], [3.parquet], [2.parquet], [1.parquet]]}, projection=[tag, time], output_ordering=[tag@0 ASC, time@1 ASC], predicate=time@1 > 0, pruning_predicate=CASE WHEN time_null_count@1 = time_row_count@2 THEN false ELSE time_max@0 > 0 END, required_guarantees=[]"
        "#
        );
    }

    // ------------------------------------------------------------------
    // Helper functions
    // ------------------------------------------------------------------

    /// Builder for plans that uses the correct schemas are used when
    /// constructing embedded expressions.
    #[derive(Debug, Clone)]
    struct PlanBuilder {
        /// The current plan being constructed
        inner: Arc<dyn ExecutionPlan>,
    }

    impl PlanBuilder {
        /// Create a new builder to scan the parquet file with the specified range
        fn parquet_exec(schema: &SchemaRef, min: i64, max: i64) -> Self {
            let chunk = test_chunk(min, max, true);
            let plan = chunks_to_physical_nodes(schema, None, vec![chunk], 1);

            Self::remove_union(plan)
        }

        // Create a parquet_exec with a given number of chunks
        fn parquet_exec_non_overlapped_chunks(
            schema: &SchemaRef,
            n_chunks: usize,
            min: i64,
            duration: usize,
            target_partition: usize,
        ) -> Self {
            let mut chunks = Vec::with_capacity(n_chunks);
            for i in 0..n_chunks {
                let min = min + (duration * i) as i64;
                let max = min + duration as i64 - 1;
                let chunk = test_chunk_with_id(min, max, true, i as u128);
                chunks.push(chunk);
            }

            let inner = chunks_to_physical_nodes(schema, None, chunks, target_partition);

            Self::remove_union(inner)
        }

        // Create a parquet_exec with a given number of chunks
        fn parquet_exec_overlapped_chunks(
            schema: &SchemaRef,
            n_chunks: usize,
            min: i64,
            duration: usize,
            target_partition: usize,
        ) -> Self {
            let mut chunks = Vec::with_capacity(n_chunks);
            for i in 0..n_chunks {
                let min = min + (duration * i) as i64;
                let max = min + duration as i64 + 1; // overlap by 2
                let chunk = test_chunk_with_id(min, max, true, i as u128);
                chunks.push(chunk);
            }

            let inner = chunks_to_physical_nodes(schema, None, chunks, target_partition);

            Self::remove_union(inner)
        }

        fn remove_union(plan: Arc<dyn ExecutionPlan>) -> Self {
            let inner = if let Some(union_exec) = plan.as_any().downcast_ref::<UnionExec>() {
                if union_exec.inputs().len() == 1 {
                    Arc::clone(&union_exec.inputs()[0])
                } else {
                    plan
                }
            } else {
                plan
            };
            Self { inner }
        }

        /// Create a builder for scanning record batches with the specified value range
        fn record_batches_exec(n_chunks: usize, min: i64, max: i64) -> Self {
            let chunks = std::iter::repeat(test_chunk(min, max, false))
                .take(n_chunks)
                .collect::<Vec<_>>();

            Self {
                inner: Arc::new(RecordBatchesExec::new(chunks, schema(), None)),
            }
        }

        /// Create a union of this plan with another plan
        fn union(self, other: Self) -> Self {
            let inner = Arc::new(UnionExec::new(vec![self.inner, other.inner]));
            Self { inner }
        }

        /// Sort the output of this plan with the specified expressions
        fn sort<'a>(self, cols: impl IntoIterator<Item = (&'a str, SortOp)>) -> Self {
            Self {
                inner: Arc::new(SortExec::new(self.sort_exprs(cols), self.inner)),
            }
        }

        /// Sort the output of this plan with the specified expressions
        fn sort_with_preserve_partitioning<'a>(
            self,
            cols: impl IntoIterator<Item = (&'a str, SortOp)>,
        ) -> Self {
            Self::sort_with_preserve_partitioning_setting(self, cols, true)
        }

        /// Sort the output of this plan with the specified expressions &
        fn sort_with_preserve_partitioning_setting<'a>(
            self,
            cols: impl IntoIterator<Item = (&'a str, SortOp)>,
            preserve_partitioning: bool,
        ) -> Self {
            Self {
                inner: Arc::new(
                    SortExec::new(self.sort_exprs(cols), self.inner)
                        .with_preserve_partitioning(preserve_partitioning),
                ),
            }
        }

        /// Deduplicate the output of this plan with the specified sort expressions
        fn deduplicate<'a>(
            self,
            cols: impl IntoIterator<Item = (&'a str, SortOp)>,
            use_chunk_order_col: bool,
        ) -> Self {
            let sort_exprs = self.sort_exprs(cols);
            Self {
                inner: Arc::new(DeduplicateExec::new(
                    self.inner,
                    sort_exprs,
                    use_chunk_order_col,
                )),
            }
        }

        /// adds a ProjectionExec node to the plan with the specified columns
        fn project<'a>(self, cols: impl IntoIterator<Item = &'a str>) -> Self {
            let schema = self.inner.schema();
            let project_exprs = cols
                .into_iter()
                .map(|col| {
                    let expr: Arc<dyn PhysicalExpr> =
                        Arc::new(Column::new_with_schema(col, &schema).unwrap());
                    (expr, col.to_string())
                })
                .collect::<Vec<_>>();

            self.project_with_exprs(project_exprs)
        }

        /// adds a ProjectionExec node to the plan with the specified exprs
        fn project_with_exprs(self, project_exprs: Vec<(Arc<dyn PhysicalExpr>, String)>) -> Self {
            Self {
                inner: Arc::new(ProjectionExec::try_new(project_exprs, self.inner).unwrap()),
            }
        }

        /// Create a sort_preserving_merge with the specified sort order
        fn sort_preserving_merge<'a>(
            self,
            cols: impl IntoIterator<Item = (&'a str, SortOp)>,
        ) -> Self {
            Self {
                inner: Arc::new(SortPreservingMergeExec::new(
                    self.sort_exprs(cols),
                    self.inner,
                )),
            }
        }

        /// round robin repartition into the specified number of partitions
        fn round_robin_repartition(self, n_partitions: usize) -> Self {
            Self {
                inner: Arc::new(
                    RepartitionExec::try_new(
                        self.inner,
                        Partitioning::RoundRobinBatch(n_partitions),
                    )
                    .unwrap(),
                ),
            }
        }

        /// hash repartition into the specified number of partitions
        fn hash_repartition<'a>(
            self,
            cols: impl IntoIterator<Item = &'a str>,
            n_partitions: usize,
        ) -> Self {
            let schema = self.inner.schema();
            let hash_exprs = cols
                .into_iter()
                .map(|col| {
                    Arc::new(Column::new_with_schema(col, &schema).unwrap())
                        as Arc<dyn PhysicalExpr>
                })
                .collect();
            Self {
                inner: Arc::new(
                    RepartitionExec::try_new(
                        self.inner,
                        Partitioning::Hash(hash_exprs, n_partitions),
                    )
                    .unwrap(),
                ),
            }
        }

        /// create a `Vec<PhysicalSortExpr>` from the specified columns for the current node
        fn sort_exprs<'a>(
            &self,
            cols: impl IntoIterator<Item = (&'a str, SortOp)>,
        ) -> Vec<PhysicalSortExpr> {
            // sort expressions are based on the schema of the input
            let schema = self.inner.schema();
            cols.into_iter()
                .map(|col| PhysicalSortExpr {
                    expr: Arc::new(Column::new_with_schema(col.0, schema.as_ref()).unwrap()),
                    options: SortOptions {
                        descending: col.1 == SortOp::Desc,
                        nulls_first: false,
                    },
                })
                .collect()
        }

        fn limit(self, skip: usize, fetch: Option<usize>) -> Self {
            Self {
                inner: Arc::new(GlobalLimitExec::new(self.inner, skip, fetch)),
            }
        }

        /// Return the current plan as formatted strings
        fn formatted(&self) -> Vec<String> {
            format_execution_plan(&self.inner)
        }

        /// return the inner plan
        fn build(self) -> Arc<dyn ExecutionPlan> {
            self.inner
        }

        /// return the schema of the inner plan
        fn schema(&self) -> SchemaRef {
            self.inner.schema()
        }

        /// return a reference to the inner plan
        fn inner(&self) -> &dyn ExecutionPlan {
            self.inner.as_ref()
        }
    }

    fn schema() -> SchemaRef {
        IOxSchemaBuilder::new()
            .tag("col1")
            .tag("col2")
            .influx_field("field1", InfluxFieldType::Float)
            .timestamp()
            .influx_field(CHUNK_ORDER_COLUMN_NAME, InfluxFieldType::Integer)
            .build()
            .unwrap()
            .into()
    }

    fn expr_col(name: &str, schema: &SchemaRef) -> Arc<dyn PhysicalExpr> {
        Arc::new(Column::new_with_schema(name, schema).unwrap())
    }

    // test chunk with time range and field1's value range
    fn test_chunk(min: i64, max: i64, parquet_data: bool) -> Arc<dyn QueryChunk> {
        test_chunk_with_id(min, max, parquet_data, 0)
    }

    // test chunk with time range and field1's value range and with a given chunk id
    fn test_chunk_with_id(
        min: i64,
        max: i64,
        parquet_data: bool,
        chunk_id: u128,
    ) -> Arc<dyn QueryChunk> {
        let chunk = TestChunk::new("t")
            .with_id(chunk_id)
            .with_time_column_with_stats(Some(min), Some(max))
            .with_tag_column_with_stats("col1", Some("AL"), Some("MT"))
            .with_tag_column_with_stats("col2", Some("MA"), Some("VY"))
            .with_i64_field_column_with_stats("field1", Some(min), Some(max));

        let chunk = if parquet_data {
            chunk.with_dummy_parquet_file()
        } else {
            chunk
        };

        Arc::new(chunk) as Arc<dyn QueryChunk>
    }

    #[derive(Debug, PartialEq, Clone, Copy)]
    enum SortOp {
        Asc,
        Desc,
    }
}
