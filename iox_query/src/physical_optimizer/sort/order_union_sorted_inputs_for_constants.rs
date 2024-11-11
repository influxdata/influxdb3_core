use std::sync::Arc;

use datafusion::{
    common::tree_node::{Transformed, TreeNode},
    config::ConfigOptions,
    error::Result,
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{
        expressions::Column,
        sorts::{sort::SortExec, sort_preserving_merge::SortPreservingMergeExec},
        union::UnionExec,
        ExecutionPlan,
    },
};

use crate::{
    physical_optimizer::sort::util::{
        all_sort_execs_on_same_sort_exprs_from_starting_position, sort_plans_by_value_ranges,
    },
    provider::progressive_eval::ProgressiveEvalExec,
};

use super::util::{
    accepted_union_exec, add_sort_preserving_merge, all_physical_sort_exprs_on_column,
    ValueRangeAndColValues,
};

/// IOx specific optimization rule to replace SortPreservingMerge with ProgressiveEval if
/// below it is a union of other sorts of projections on same columns.
/// This optiization is specifically for improving `show table values` of 2 related tickets
///   `<https://github.com/influxdata/influxdb_iox/issues/10359>`
///   `<https://github.com/influxdata/influxdb_iox/issues/10042>`
///
/// Example:
/// Input plan:
///   SortPreservingMerge (col1, col2, col3)
///     Union
///       Sort (col3)   <-- no col1 and col2 becasue they are constants and removed in an earlier optimization step
///          Projection (col1 is a constant = "m1", col2 as constant = "tag0", col3)
///       Sort (col3)
///          Projection (col1 is a constant = "m0", col2 as constant = "tag1", col3)
///       Sort (col3)
///          Projection (col1 is a constant = "m0", col2 as constant = "tag0", col3)
///       Sort (ccol3)
///          Projection (col1 is a constant = "m2", col2 as constant = "tag1", col3)
///
/// Output plan:
///  ProgressiveEval         <-- top operator is ProgressiveEval to output data in order: m0 tag0, m0 tag1, m1 tag0, m2 tag1
///    Union
///      ProgressiveEval     <-- ProgressiveEval to union different branches of the same "m0"
///         Sort (col3)
///            Projection (col1 is a constant = "m0", col2 as constant = "tag0", col3)
///         Sort (col3)
///            Projection (col1 is a constant = "m0", col2 as constant = "tag1", col3)
///      Sort (col3)
///         Projection (col1 is a constant = "m1", col2 as constant = "tag0", col3)
///      Sort (col3)
///        Projection (col1 is a constant = "m2", col2 as constant = "tag1", col3)
///
///  Note: the Sort branches are sorted to guarantee the output is sorted

pub(crate) struct OrderUnionSortedInputsForConstants;

impl PhysicalOptimizerRule for OrderUnionSortedInputsForConstants {
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

            // Check if the sortExpr is on many expressions
            let sort_expr = sort_preserving_merge_exec.expr();
            if sort_expr.is_empty() {
                return Ok(Transformed::no(plan));
            };
            // Check if each expression is on a column
            if !all_physical_sort_exprs_on_column(sort_expr) {
                return Ok(Transformed::no(plan));
            };
            // Get column name of the first sort expression and its sort option
            let first_sort_col_name = sort_expr[0]
                .expr
                .as_any()
                .downcast_ref::<Column>()
                .unwrap()
                .name();
            let sort_options = sort_expr[0].options;

            // Check if under the SortPreservingMergeExec is UnionnExec
            let Some(union_exec) = accepted_union_exec(sort_preserving_merge_exec) else {
                return Ok(Transformed::no(plan));
            };

            // Check if all inputs of Union are SortExec
            let union_inputs = union_exec.inputs();
            let mut sort_exec_plans = Vec::with_capacity(union_inputs.len());
            for union_input_plan in union_inputs {
                let Some(union_input_sort_exec) =
                    union_input_plan.as_any().downcast_ref::<SortExec>()
                else {
                    return Ok(Transformed::no(plan));
                };
                sort_exec_plans.push(union_input_sort_exec);
            }

            // Check if each sort_exec_plans has its sortExpr the same as the sort_expr from the start position
            //   . The sort_expr will 2 constansts vlaue first for `iox::measurement` and `key`. SOmething like:
            //         Sort: iox::measurement ASC NULLS LAST, key ASC NULLS LAST, value ASC NULLS LAST
            //      that is equivalent for the ones in projection (see below)
            //         Projection: m0 as iox::measurement, tag0 as key, tag0@1 as value  - notes: m0 and tag0 are constants
            //   . The sort_exec_plans will have sort expression without cosntants and looks like
            //         SortExec: expr=[value@2 ASC NULLS LAST]
            if !all_sort_execs_on_same_sort_exprs_from_starting_position(
                &sort_exec_plans,
                sort_expr,
                2,
            ) {
                return Ok(Transformed::no(plan));
            }

            // Check if under each SortExec is ProjectionExec that includes 2 first column as constants
            // And if so, collect those constants. The result includes 2 vectors:
            //  . vector 1: ranges of a contatenation of first and second cols
            //      e.g: [(m0tag0, m0tag0), (m0tag1, m0tag1), (m1tag0, m1tag0), (m1tag3, m1tag3)]
            //      This will be used to sort the streams each represent a (table & a tag)
            //  . second vector value of the first column: [m0, m0, m1, m1]. This will be used to group streams of the same table.
            let mut val_ranges_and_col_vals =
                ValueRangeAndColValues::try_new(&sort_exec_plans, first_sort_col_name)?;
            if val_ranges_and_col_vals.value_ranges.is_empty() {
                return Ok(Transformed::no(plan));
            }

            // Sort the stream by the two-first-column-value-ranges
            let Some(plans_value_ranges) = sort_plans_by_value_ranges(
                union_inputs.to_vec(),
                val_ranges_and_col_vals.value_ranges,
                sort_options,
            )?
            else {
                return Ok(Transformed::no(plan));
            };
            // sort first_col_vals used to group streams of the same table. This sort is to guarantee
            // val_ranges_and_col_vals.column_values and plans_value_ranges.value_ranges are in the same order
            val_ranges_and_col_vals.column_values.sort();

            // If each input of UnionExec outputs many sorted streams, data of different streams may overlap and
            // even if they do not overlapped, their streams can be in any order. We need to (sort) merge them first
            // to have a single output stream out to guarantee the output is sorted.
            let new_inputs = add_sort_preserving_merge(
                plans_value_ranges.plans,
                sort_expr,
                sort_preserving_merge_exec.fetch(),
            )?;

            // Since the streams are sorted on 2 columns (table_name, tag_name), we group streams
            // with the same table_name and add ProgressiveEvalExec on top of them
            // to have them return in one stream
            let new_plans_value_ranges = ValueRangeAndColValues::group_plans_by_value(
                new_inputs,
                val_ranges_and_col_vals.column_values,
            );

            // Make new union with sorted streams
            let new_union_exec = Arc::new(UnionExec::new(new_plans_value_ranges.plans));

            // Replace SortPreservingMergeExec with ProgressiveEvalExec
            let progresive_eval_exec = Arc::new(ProgressiveEvalExec::new(
                new_union_exec,
                Some(new_plans_value_ranges.value_ranges),
                None,
            ));

            Ok(Transformed::yes(progresive_eval_exec))
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "order_union_sorted_input_for_constants"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::{
        compute::SortOptions,
        datatypes::{DataType, SchemaRef},
    };
    use datafusion::{
        physical_expr::PhysicalSortExpr,
        physical_plan::{
            expressions::{Column, Literal},
            projection::ProjectionExec,
            sorts::{sort::SortExec, sort_preserving_merge::SortPreservingMergeExec},
            union::UnionExec,
            ExecutionPlan, PhysicalExpr,
        },
        scalar::ScalarValue,
    };
    use schema::{InfluxFieldType, SchemaBuilder as IOxSchemaBuilder};

    use crate::{
        physical_optimizer::{
            sort::order_union_sorted_inputs_for_constants::OrderUnionSortedInputsForConstants,
            test_util::OptimizationTest,
        },
        provider::{chunks_to_physical_nodes, RecordBatchesExec},
        test::TestChunk,
        QueryChunk, CHUNK_ORDER_COLUMN_NAME,
    };

    // ------------------------------------------------------------------
    // Positive tests: the right structure found -> plan optimized
    // ------------------------------------------------------------------

    #[test]
    fn test_replace_spm_with_pe() {
        test_helpers::maybe_start_logging();

        let schema = schema();
        let sort_order = sort_order_for_sort();

        // First sort on parquet file
        let plan_parquet = parquet_exec_with_value_range(&schema, 1000, 2000);
        let plan_projection_1 = Arc::new(
            ProjectionExec::try_new(
                projection_expr_with_2_constants("m1", "tag0", &schema),
                plan_parquet,
            )
            .unwrap(),
        );
        let plan_sort1 = Arc::new(SortExec::new(sort_order.clone(), plan_projection_1));

        // Second sort on record batch
        let plan_batches = record_batches_exec_with_value_range(1, 2001, 3000);
        let plan_projection_2 = Arc::new(
            ProjectionExec::try_new(
                projection_expr_with_2_constants("m0", "tag0", &schema),
                plan_batches,
            )
            .unwrap(),
        );
        let plan_sort2 = Arc::new(SortExec::new(sort_order.clone(), plan_projection_2));

        // union the 2 sorts
        let plan_union = Arc::new(UnionExec::new(vec![plan_sort1, plan_sort2]));

        // add sort preserving merge on top
        let plan_spm = Arc::new(SortPreservingMergeExec::new(
            sort_order_for_sort_preserving_merge(),
            plan_union,
        ));

        // Output plan:
        //   . SortPreservingMergeExec will be replaced with ProgressiveEvalExec
        //   . the 2 SortExecs will be swapped the order to have "m0" first
        let opt = OrderUnionSortedInputsForConstants;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_spm, opt),
            @r#"
        input:
          - " SortPreservingMergeExec: [iox::measurement@0 ASC NULLS LAST,key@1 ASC NULLS LAST,value@2 ASC NULLS LAST]"
          - "   UnionExec"
          - "     SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
          - "       ProjectionExec: expr=[m1 as iox::measurement, tag0 as key, tag0@1 as value]"
          - "         ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[tag2, tag0, tag1, field1, time, __chunk_order], output_ordering=[__chunk_order@5 ASC]"
          - "     SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
          - "       ProjectionExec: expr=[m0 as iox::measurement, tag0 as key, tag0@1 as value]"
          - "         RecordBatchesExec: chunks=1, projection=[tag2, tag0, tag1, field1, time, __chunk_order]"
        output:
          Ok:
            - " ProgressiveEvalExec: input_ranges=[(Utf8(\"m0\"), Utf8(\"m0\")), (Utf8(\"m1\"), Utf8(\"m1\"))]"
            - "   UnionExec"
            - "     SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
            - "       ProjectionExec: expr=[m0 as iox::measurement, tag0 as key, tag0@1 as value]"
            - "         RecordBatchesExec: chunks=1, projection=[tag2, tag0, tag1, field1, time, __chunk_order]"
            - "     SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
            - "       ProjectionExec: expr=[m1 as iox::measurement, tag0 as key, tag0@1 as value]"
            - "         ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[tag2, tag0, tag1, field1, time, __chunk_order], output_ordering=[__chunk_order@5 ASC]"
        "#
        );
    }

    #[test]
    fn test_replace_many_spm_with_pe() {
        test_helpers::maybe_start_logging();

        let schema = schema();
        let sort_order = sort_order_for_sort();

        // First sort: first col: constant m1, second column: constant tag0, third column: value of seconnd column (tag0)
        let plan_parquet = parquet_exec_with_value_range(&schema, 1000, 2000);
        let plan_projection_1 = Arc::new(
            ProjectionExec::try_new(
                projection_expr_with_2_constants("m1", "tag0", &schema),
                plan_parquet,
            )
            .unwrap(),
        );
        let plan_sort1 = Arc::new(SortExec::new(sort_order.clone(), plan_projection_1));

        // Second sort: first col: constant m0, second column: constant tag1, third column: value of seconnd column (tag1)
        let plan_batches = record_batches_exec_with_value_range(1, 2001, 3000);
        let plan_projection_2 = Arc::new(
            ProjectionExec::try_new(
                projection_expr_with_2_constants("m0", "tag1", &schema),
                plan_batches,
            )
            .unwrap(),
        );
        let plan_sort2 = Arc::new(SortExec::new(sort_order.clone(), plan_projection_2));

        // Third sort: first col: constant m0, second column: constant tag0, third column: value of seconnd column (tag0)
        let plan_batches2 = record_batches_exec_with_value_range(1, 2001, 3000);
        let plan_projection_3 = Arc::new(
            ProjectionExec::try_new(
                projection_expr_with_2_constants("m0", "tag0", &schema),
                plan_batches2,
            )
            .unwrap(),
        );
        let plan_sort3 = Arc::new(SortExec::new(sort_order.clone(), plan_projection_3));

        // union the 3 sorts
        let plan_union = Arc::new(UnionExec::new(vec![plan_sort1, plan_sort2, plan_sort3]));

        // add sort preserving merge on top
        let plan_spm = Arc::new(SortPreservingMergeExec::new(
            sort_order_for_sort_preserving_merge(),
            plan_union,
        ));

        // Output plan: There are 2 ProgressiveEvalExecs
        //  . One on top repalcing the top SortPreservingMergeExec
        //  . One on top of a new UnionExec that unions the tow SortExecs on m0
        // All the streams are sorted accordingly to have output data sorted : m0tag0, m0tag1, m1tag0
        let opt = OrderUnionSortedInputsForConstants;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_spm, opt),
            @r#"
        input:
          - " SortPreservingMergeExec: [iox::measurement@0 ASC NULLS LAST,key@1 ASC NULLS LAST,value@2 ASC NULLS LAST]"
          - "   UnionExec"
          - "     SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
          - "       ProjectionExec: expr=[m1 as iox::measurement, tag0 as key, tag0@1 as value]"
          - "         ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[tag2, tag0, tag1, field1, time, __chunk_order], output_ordering=[__chunk_order@5 ASC]"
          - "     SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
          - "       ProjectionExec: expr=[m0 as iox::measurement, tag1 as key, tag1@2 as value]"
          - "         RecordBatchesExec: chunks=1, projection=[tag2, tag0, tag1, field1, time, __chunk_order]"
          - "     SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
          - "       ProjectionExec: expr=[m0 as iox::measurement, tag0 as key, tag0@1 as value]"
          - "         RecordBatchesExec: chunks=1, projection=[tag2, tag0, tag1, field1, time, __chunk_order]"
        output:
          Ok:
            - " ProgressiveEvalExec: input_ranges=[(Utf8(\"m0\"), Utf8(\"m0\")), (Utf8(\"m1\"), Utf8(\"m1\"))]"
            - "   UnionExec"
            - "     ProgressiveEvalExec: input_ranges=[(Utf8(\"m0\"), Utf8(\"m0\")), (Utf8(\"m0\"), Utf8(\"m0\"))]"
            - "       UnionExec"
            - "         SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
            - "           ProjectionExec: expr=[m0 as iox::measurement, tag0 as key, tag0@1 as value]"
            - "             RecordBatchesExec: chunks=1, projection=[tag2, tag0, tag1, field1, time, __chunk_order]"
            - "         SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
            - "           ProjectionExec: expr=[m0 as iox::measurement, tag1 as key, tag1@2 as value]"
            - "             RecordBatchesExec: chunks=1, projection=[tag2, tag0, tag1, field1, time, __chunk_order]"
            - "     SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
            - "       ProjectionExec: expr=[m1 as iox::measurement, tag0 as key, tag0@1 as value]"
            - "         ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[tag2, tag0, tag1, field1, time, __chunk_order], output_ordering=[__chunk_order@5 ASC]"
        "#
        );
    }

    // The contants are integers
    #[test]
    fn test_replace_spm_with_pe_integer_constants() {
        test_helpers::maybe_start_logging();

        let schema = schema();
        let sort_order = sort_order_for_sort();

        // First sort on parquet file
        let plan_parquet = parquet_exec_with_value_range(&schema, 1000, 2000);
        let plan_projection_1 = Arc::new(
            ProjectionExec::try_new(
                projection_expr_with_2_constant_integers(1, 10, &schema),
                plan_parquet,
            )
            .unwrap(),
        );
        let plan_sort1 = Arc::new(SortExec::new(sort_order.clone(), plan_projection_1));

        // Second sort on record batch
        let plan_batches = record_batches_exec_with_value_range(1, 2001, 3000);
        let plan_projection_2 = Arc::new(
            ProjectionExec::try_new(
                projection_expr_with_2_constant_integers(2, 20, &schema),
                plan_batches,
            )
            .unwrap(),
        );
        let plan_sort2 = Arc::new(SortExec::new(sort_order.clone(), plan_projection_2));

        // union the 2 sorts
        let plan_union = Arc::new(UnionExec::new(vec![plan_sort1, plan_sort2]));

        // add sort preserving merge on top
        let plan_spm = Arc::new(SortPreservingMergeExec::new(
            sort_order_for_sort_preserving_merge(),
            plan_union,
        ));

        // Output plan:
        //   . SortPreservingMergeExec will be replaced with ProgressiveEvalExec
        //   . the 2 SortExecs will be swapped the order to have "m0" first
        let opt = OrderUnionSortedInputsForConstants;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_spm, opt),
            @r#"
        input:
          - " SortPreservingMergeExec: [iox::measurement@0 ASC NULLS LAST,key@1 ASC NULLS LAST,value@2 ASC NULLS LAST]"
          - "   UnionExec"
          - "     SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
          - "       ProjectionExec: expr=[1 as iox::measurement, 10 as key, tag0@1 as value]"
          - "         ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[tag2, tag0, tag1, field1, time, __chunk_order], output_ordering=[__chunk_order@5 ASC]"
          - "     SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
          - "       ProjectionExec: expr=[2 as iox::measurement, 20 as key, tag0@1 as value]"
          - "         RecordBatchesExec: chunks=1, projection=[tag2, tag0, tag1, field1, time, __chunk_order]"
        output:
          Ok:
            - " ProgressiveEvalExec: input_ranges=[(Utf8(\"1\"), Utf8(\"1\")), (Utf8(\"2\"), Utf8(\"2\"))]"
            - "   UnionExec"
            - "     SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
            - "       ProjectionExec: expr=[1 as iox::measurement, 10 as key, tag0@1 as value]"
            - "         ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[tag2, tag0, tag1, field1, time, __chunk_order], output_ordering=[__chunk_order@5 ASC]"
            - "     SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
            - "       ProjectionExec: expr=[2 as iox::measurement, 20 as key, tag0@1 as value]"
            - "         RecordBatchesExec: chunks=1, projection=[tag2, tag0, tag1, field1, time, __chunk_order]"
        "#
        );
    }

    // Three contants
    #[test]
    fn test_replace_spm_with_pe_three_constants() {
        test_helpers::maybe_start_logging();

        let schema = schema();
        let sort_order = sort_order_for_sort();

        // First sort on parquet file
        let plan_parquet = parquet_exec_with_value_range(&schema, 1000, 2000);
        let plan_projection_1 = Arc::new(
            ProjectionExec::try_new(
                projection_expr_with_3_constants("m0", "tag0", "tag1", &schema),
                plan_parquet,
            )
            .unwrap(),
        );
        let plan_sort1 = Arc::new(SortExec::new(sort_order.clone(), plan_projection_1));

        // Second sort on record batch
        let plan_batches = record_batches_exec_with_value_range(1, 2001, 3000);
        let plan_projection_2 = Arc::new(
            ProjectionExec::try_new(
                projection_expr_with_3_constants("m1", "tag1", "tag2", &schema),
                plan_batches,
            )
            .unwrap(),
        );
        let plan_sort2 = Arc::new(SortExec::new(sort_order.clone(), plan_projection_2));

        // union the 2 sorts
        let plan_union = Arc::new(UnionExec::new(vec![plan_sort1, plan_sort2]));

        // add sort preserving merge on top
        let plan_spm = Arc::new(SortPreservingMergeExec::new(
            sort_order_for_sort_preserving_merge(),
            plan_union,
        ));

        // Output plan:
        //   . SortPreservingMergeExec will be replaced with ProgressiveEvalExec
        //   . the 2 SortExecs will be swapped the order to have "m0" first
        let opt = OrderUnionSortedInputsForConstants;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_spm, opt),
            @r#"
        input:
          - " SortPreservingMergeExec: [iox::measurement@0 ASC NULLS LAST,key@1 ASC NULLS LAST,value@2 ASC NULLS LAST]"
          - "   UnionExec"
          - "     SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
          - "       ProjectionExec: expr=[m0 as iox::measurement, tag0 as key, tag1 as another_key, tag0@1 as value]"
          - "         ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[tag2, tag0, tag1, field1, time, __chunk_order], output_ordering=[__chunk_order@5 ASC]"
          - "     SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
          - "       ProjectionExec: expr=[m1 as iox::measurement, tag1 as key, tag2 as another_key, tag0@1 as value]"
          - "         RecordBatchesExec: chunks=1, projection=[tag2, tag0, tag1, field1, time, __chunk_order]"
        output:
          Ok:
            - " ProgressiveEvalExec: input_ranges=[(Utf8(\"m0\"), Utf8(\"m0\")), (Utf8(\"m1\"), Utf8(\"m1\"))]"
            - "   UnionExec"
            - "     SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
            - "       ProjectionExec: expr=[m0 as iox::measurement, tag0 as key, tag1 as another_key, tag0@1 as value]"
            - "         ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[tag2, tag0, tag1, field1, time, __chunk_order], output_ordering=[__chunk_order@5 ASC]"
            - "     SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
            - "       ProjectionExec: expr=[m1 as iox::measurement, tag1 as key, tag2 as another_key, tag0@1 as value]"
            - "         RecordBatchesExec: chunks=1, projection=[tag2, tag0, tag1, field1, time, __chunk_order]"
        "#
        );
    }

    // ------------------------------------------------------------------
    // Negative tests: wrong structure -> not optimized
    // ------------------------------------------------------------------

    // Sort expressions each is not on a column
    #[test]
    fn test_negative_sort_not_on_column() {
        test_helpers::maybe_start_logging();

        let schema = schema();
        let sort_order = sort_order_not_on_column();

        // First sort on parquet file
        let plan_parquet = parquet_exec_with_value_range(&schema, 1000, 2000);
        let plan_projection_1 = Arc::new(
            ProjectionExec::try_new(
                projection_expr_with_2_constants("m1", "tag0", &schema),
                plan_parquet,
            )
            .unwrap(),
        );
        let plan_sort1 = Arc::new(SortExec::new(sort_order.clone(), plan_projection_1));

        // Second sort on record batch
        let plan_batches = record_batches_exec_with_value_range(1, 2001, 3000);
        let plan_projection_2 = Arc::new(
            ProjectionExec::try_new(
                projection_expr_with_2_constants("m0", "tag0", &schema),
                plan_batches,
            )
            .unwrap(),
        );
        let plan_sort2 = Arc::new(SortExec::new(sort_order.clone(), plan_projection_2));

        // union the 2 sorts
        let plan_union = Arc::new(UnionExec::new(vec![plan_sort1, plan_sort2]));

        // add sort preserving merge on top
        let plan_spm = Arc::new(SortPreservingMergeExec::new(sort_order, plan_union));

        // input and output are the same
        let opt = OrderUnionSortedInputsForConstants;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_spm, opt),
            @r#"
        input:
          - " SortPreservingMergeExec: [iox::measurement ASC NULLS LAST]"
          - "   UnionExec"
          - "     SortExec: expr=[iox::measurement ASC NULLS LAST], preserve_partitioning=[false]"
          - "       ProjectionExec: expr=[m1 as iox::measurement, tag0 as key, tag0@1 as value]"
          - "         ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[tag2, tag0, tag1, field1, time, __chunk_order], output_ordering=[__chunk_order@5 ASC]"
          - "     SortExec: expr=[iox::measurement ASC NULLS LAST], preserve_partitioning=[false]"
          - "       ProjectionExec: expr=[m0 as iox::measurement, tag0 as key, tag0@1 as value]"
          - "         RecordBatchesExec: chunks=1, projection=[tag2, tag0, tag1, field1, time, __chunk_order]"
        output:
          Ok:
            - " SortPreservingMergeExec: [iox::measurement ASC NULLS LAST]"
            - "   UnionExec"
            - "     SortExec: expr=[iox::measurement ASC NULLS LAST], preserve_partitioning=[false]"
            - "       ProjectionExec: expr=[m1 as iox::measurement, tag0 as key, tag0@1 as value]"
            - "         ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[tag2, tag0, tag1, field1, time, __chunk_order], output_ordering=[__chunk_order@5 ASC]"
            - "     SortExec: expr=[iox::measurement ASC NULLS LAST], preserve_partitioning=[false]"
            - "       ProjectionExec: expr=[m0 as iox::measurement, tag0 as key, tag0@1 as value]"
            - "         RecordBatchesExec: chunks=1, projection=[tag2, tag0, tag1, field1, time, __chunk_order]"
        "#
        );
    }

    // Under sort preserving merge is not UnionExec
    #[test]
    fn test_negative_no_union_under_spm() {
        test_helpers::maybe_start_logging();

        let schema = schema();
        let sort_order = sort_order_for_sort();

        // First sort on parquet file
        let plan_parquet = parquet_exec_with_value_range(&schema, 1000, 2000);
        let plan_projection_1 = Arc::new(
            ProjectionExec::try_new(
                projection_expr_with_2_constants("m1", "tag0", &schema),
                plan_parquet,
            )
            .unwrap(),
        );
        let plan_sort1 = Arc::new(SortExec::new(sort_order.clone(), plan_projection_1));

        // add sort preserving merge on top
        let plan_spm = Arc::new(SortPreservingMergeExec::new(
            sort_order_for_sort_preserving_merge(),
            plan_sort1,
        ));

        // input and output are the same
        let opt = OrderUnionSortedInputsForConstants;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_spm, opt),
            @r#"
        input:
          - " SortPreservingMergeExec: [iox::measurement@0 ASC NULLS LAST,key@1 ASC NULLS LAST,value@2 ASC NULLS LAST]"
          - "   SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
          - "     ProjectionExec: expr=[m1 as iox::measurement, tag0 as key, tag0@1 as value]"
          - "       ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[tag2, tag0, tag1, field1, time, __chunk_order], output_ordering=[__chunk_order@5 ASC]"
        output:
          Ok:
            - " SortPreservingMergeExec: [iox::measurement@0 ASC NULLS LAST,key@1 ASC NULLS LAST,value@2 ASC NULLS LAST]"
            - "   SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
            - "     ProjectionExec: expr=[m1 as iox::measurement, tag0 as key, tag0@1 as value]"
            - "       ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[tag2, tag0, tag1, field1, time, __chunk_order], output_ordering=[__chunk_order@5 ASC]"
        "#
        );
    }

    // Under Union is not all SortExec
    #[test]
    fn test_negative_not_all_sorts_under_union() {
        let schema = schema();
        let sort_order = sort_order_for_sort();

        // First branch: sort on parquet file
        let plan_parquet = parquet_exec_with_value_range(&schema, 1000, 2000);
        let plan_projection_1 = Arc::new(
            ProjectionExec::try_new(
                projection_expr_with_2_constants("m1", "tag0", &schema),
                plan_parquet,
            )
            .unwrap(),
        );
        let plan_sort1 = Arc::new(SortExec::new(sort_order.clone(), plan_projection_1));

        // Second branch: projection on record batch
        let plan_batches = record_batches_exec_with_value_range(1, 2001, 3000);
        let plan_projection_2 = Arc::new(
            ProjectionExec::try_new(
                projection_expr_with_2_constants("m0", "tag0", &schema),
                plan_batches,
            )
            .unwrap(),
        );

        // union the 2 sorts
        let plan_union = Arc::new(UnionExec::new(vec![plan_sort1, plan_projection_2]));

        // add sort preserving merge on top
        let plan_spm = Arc::new(SortPreservingMergeExec::new(
            sort_order_for_sort_preserving_merge(),
            plan_union,
        ));

        // input and output are the same
        let opt = OrderUnionSortedInputsForConstants;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_spm, opt),
            @r#"
        input:
          - " SortPreservingMergeExec: [iox::measurement@0 ASC NULLS LAST,key@1 ASC NULLS LAST,value@2 ASC NULLS LAST]"
          - "   UnionExec"
          - "     SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
          - "       ProjectionExec: expr=[m1 as iox::measurement, tag0 as key, tag0@1 as value]"
          - "         ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[tag2, tag0, tag1, field1, time, __chunk_order], output_ordering=[__chunk_order@5 ASC]"
          - "     ProjectionExec: expr=[m0 as iox::measurement, tag0 as key, tag0@1 as value]"
          - "       RecordBatchesExec: chunks=1, projection=[tag2, tag0, tag1, field1, time, __chunk_order]"
        output:
          Ok:
            - " SortPreservingMergeExec: [iox::measurement@0 ASC NULLS LAST,key@1 ASC NULLS LAST,value@2 ASC NULLS LAST]"
            - "   UnionExec"
            - "     SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
            - "       ProjectionExec: expr=[m1 as iox::measurement, tag0 as key, tag0@1 as value]"
            - "         ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[tag2, tag0, tag1, field1, time, __chunk_order], output_ordering=[__chunk_order@5 ASC]"
            - "     ProjectionExec: expr=[m0 as iox::measurement, tag0 as key, tag0@1 as value]"
            - "       RecordBatchesExec: chunks=1, projection=[tag2, tag0, tag1, field1, time, __chunk_order]"
        "#
        );
    }

    // Sort expressions of SortExecs are not the same and/or not the same with the parent's sort_expr
    #[test]
    fn test_negative_not_same_sort_exprs() {
        test_helpers::maybe_start_logging();

        let schema = schema();
        let sort_order_3_cols = sort_order_for_sort_preserving_merge();
        let sort_order_2_cols = sort_order_two_cols();
        // First sort on parquet file
        let plan_parquet = parquet_exec_with_value_range(&schema, 1000, 2000);
        let plan_projection_1 = Arc::new(
            ProjectionExec::try_new(
                projection_expr_with_2_constants("m1", "tag0", &schema),
                plan_parquet,
            )
            .unwrap(),
        );
        let plan_sort1 = Arc::new(SortExec::new(sort_order_2_cols, plan_projection_1));

        // Second sort on record batch
        let plan_batches = record_batches_exec_with_value_range(1, 2001, 3000);
        let plan_projection_2 = Arc::new(
            ProjectionExec::try_new(
                projection_expr_with_2_constants("m0", "tag0", &schema),
                plan_batches,
            )
            .unwrap(),
        );
        let plan_sort2 = Arc::new(SortExec::new(sort_order_3_cols.clone(), plan_projection_2));

        // union the 2 sorts
        let plan_union = Arc::new(UnionExec::new(vec![plan_sort1, plan_sort2]));

        // add sort preserving merge on top
        let plan_spm = Arc::new(SortPreservingMergeExec::new(sort_order_3_cols, plan_union));

        // input and output are the same
        let opt = OrderUnionSortedInputsForConstants;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_spm, opt),
            @r#"
        input:
          - " SortPreservingMergeExec: [iox::measurement@0 ASC NULLS LAST,key@1 ASC NULLS LAST,value@2 ASC NULLS LAST]"
          - "   UnionExec"
          - "     SortExec: expr=[iox::measurement@0 ASC NULLS LAST,key@1 ASC NULLS LAST], preserve_partitioning=[false]"
          - "       ProjectionExec: expr=[m1 as iox::measurement, tag0 as key, tag0@1 as value]"
          - "         ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[tag2, tag0, tag1, field1, time, __chunk_order], output_ordering=[__chunk_order@5 ASC]"
          - "     SortExec: expr=[iox::measurement@0 ASC NULLS LAST,key@1 ASC NULLS LAST,value@2 ASC NULLS LAST], preserve_partitioning=[false]"
          - "       ProjectionExec: expr=[m0 as iox::measurement, tag0 as key, tag0@1 as value]"
          - "         RecordBatchesExec: chunks=1, projection=[tag2, tag0, tag1, field1, time, __chunk_order]"
        output:
          Ok:
            - " SortPreservingMergeExec: [iox::measurement@0 ASC NULLS LAST,key@1 ASC NULLS LAST,value@2 ASC NULLS LAST]"
            - "   UnionExec"
            - "     SortExec: expr=[iox::measurement@0 ASC NULLS LAST,key@1 ASC NULLS LAST], preserve_partitioning=[false]"
            - "       ProjectionExec: expr=[m1 as iox::measurement, tag0 as key, tag0@1 as value]"
            - "         ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[tag2, tag0, tag1, field1, time, __chunk_order], output_ordering=[__chunk_order@5 ASC]"
            - "     SortExec: expr=[iox::measurement@0 ASC NULLS LAST,key@1 ASC NULLS LAST,value@2 ASC NULLS LAST], preserve_partitioning=[false]"
            - "       ProjectionExec: expr=[m0 as iox::measurement, tag0 as key, tag0@1 as value]"
            - "         RecordBatchesExec: chunks=1, projection=[tag2, tag0, tag1, field1, time, __chunk_order]"
        "#
        );
    }

    // bad projection under sort
    #[test]
    fn test_negative_bad_projection_under_sort() {
        test_helpers::maybe_start_logging();

        let schema = schema();
        let sort_order = sort_order_for_sort();

        // First sort on parquet file
        let plan_parquet = parquet_exec_with_value_range(&schema, 1000, 2000);
        let plan_projection_1 = Arc::new(
            ProjectionExec::try_new(
                projection_expr_with_2_constants("m1", "tag0", &schema),
                plan_parquet,
            )
            .unwrap(),
        );
        let plan_sort1 = Arc::new(SortExec::new(sort_order.clone(), plan_projection_1));

        // Second sort on record batch
        let plan_batches = record_batches_exec_with_value_range(1, 2001, 3000);
        // swap order of projection expressions
        let plan_projection_1 = Arc::new(
            ProjectionExec::try_new(
                projection_expr_with_2_constants("tag0", "tag1", &schema),
                plan_batches,
            )
            .unwrap(),
        );
        let plan_sort2 = Arc::new(SortExec::new(sort_order_two_cols(), plan_projection_1));

        // union the 2 sorts
        let plan_union = Arc::new(UnionExec::new(vec![plan_sort1, plan_sort2]));

        // add sort preserving merge on top
        let plan_spm = Arc::new(SortPreservingMergeExec::new(
            sort_order_for_sort_preserving_merge(),
            plan_union,
        ));

        // input and output are the same
        let opt = OrderUnionSortedInputsForConstants;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_spm, opt),
            @r#"
        input:
          - " SortPreservingMergeExec: [iox::measurement@0 ASC NULLS LAST,key@1 ASC NULLS LAST,value@2 ASC NULLS LAST]"
          - "   UnionExec"
          - "     SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
          - "       ProjectionExec: expr=[m1 as iox::measurement, tag0 as key, tag0@1 as value]"
          - "         ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[tag2, tag0, tag1, field1, time, __chunk_order], output_ordering=[__chunk_order@5 ASC]"
          - "     SortExec: expr=[iox::measurement@0 ASC NULLS LAST,key@1 ASC NULLS LAST], preserve_partitioning=[false]"
          - "       ProjectionExec: expr=[tag0 as iox::measurement, tag1 as key, tag1@2 as value]"
          - "         RecordBatchesExec: chunks=1, projection=[tag2, tag0, tag1, field1, time, __chunk_order]"
        output:
          Ok:
            - " SortPreservingMergeExec: [iox::measurement@0 ASC NULLS LAST,key@1 ASC NULLS LAST,value@2 ASC NULLS LAST]"
            - "   UnionExec"
            - "     SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
            - "       ProjectionExec: expr=[m1 as iox::measurement, tag0 as key, tag0@1 as value]"
            - "         ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[tag2, tag0, tag1, field1, time, __chunk_order], output_ordering=[__chunk_order@5 ASC]"
            - "     SortExec: expr=[iox::measurement@0 ASC NULLS LAST,key@1 ASC NULLS LAST], preserve_partitioning=[false]"
            - "       ProjectionExec: expr=[tag0 as iox::measurement, tag1 as key, tag1@2 as value]"
            - "         RecordBatchesExec: chunks=1, projection=[tag2, tag0, tag1, field1, time, __chunk_order]"
        "#
        );
    }

    // Projection does not have the first column as a constant value
    #[test]
    fn test_negative_proj_without_first_col_as_const() {
        test_helpers::maybe_start_logging();

        let schema = schema();
        let sort_order = sort_order_for_sort();

        // First sort on parquet file
        let plan_parquet = parquet_exec_with_value_range(&schema, 1000, 2000);
        let plan_projection_1 = Arc::new(
            ProjectionExec::try_new(
                vec![
                    (expr_col("tag0", &schema), String::from("value")), // tag value -- not constant
                    (
                        expr_dict_const("m1".to_string()),
                        String::from("iox::measurement"),
                    ), // constant table name
                    (expr_dict_const("tag0".to_string()), String::from("key")), // constant tag name
                ],
                plan_parquet,
            )
            .unwrap(),
        );
        let plan_sort1 = Arc::new(SortExec::new(sort_order.clone(), plan_projection_1));

        // Second sort on record batch
        let plan_batches = record_batches_exec_with_value_range(1, 2001, 3000);
        let plan_projection_2 = Arc::new(
            ProjectionExec::try_new(
                projection_expr_with_2_constants("m0", "tag0", &schema),
                plan_batches,
            )
            .unwrap(),
        );
        let plan_sort2 = Arc::new(SortExec::new(sort_order.clone(), plan_projection_2));

        // union the 2 sorts
        let plan_union = Arc::new(UnionExec::new(vec![plan_sort1, plan_sort2]));

        // add sort preserving merge on top
        let plan_spm = Arc::new(SortPreservingMergeExec::new(
            sort_order_for_sort_preserving_merge(),
            plan_union,
        ));

        // input and output are the same
        let opt = OrderUnionSortedInputsForConstants;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_spm, opt),
            @r#"
        input:
          - " SortPreservingMergeExec: [iox::measurement@0 ASC NULLS LAST,key@1 ASC NULLS LAST,value@2 ASC NULLS LAST]"
          - "   UnionExec"
          - "     SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
          - "       ProjectionExec: expr=[tag0@1 as value, m1 as iox::measurement, tag0 as key]"
          - "         ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[tag2, tag0, tag1, field1, time, __chunk_order], output_ordering=[__chunk_order@5 ASC]"
          - "     SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
          - "       ProjectionExec: expr=[m0 as iox::measurement, tag0 as key, tag0@1 as value]"
          - "         RecordBatchesExec: chunks=1, projection=[tag2, tag0, tag1, field1, time, __chunk_order]"
        output:
          Ok:
            - " SortPreservingMergeExec: [iox::measurement@0 ASC NULLS LAST,key@1 ASC NULLS LAST,value@2 ASC NULLS LAST]"
            - "   UnionExec"
            - "     SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
            - "       ProjectionExec: expr=[tag0@1 as value, m1 as iox::measurement, tag0 as key]"
            - "         ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[tag2, tag0, tag1, field1, time, __chunk_order], output_ordering=[__chunk_order@5 ASC]"
            - "     SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
            - "       ProjectionExec: expr=[m0 as iox::measurement, tag0 as key, tag0@1 as value]"
            - "         RecordBatchesExec: chunks=1, projection=[tag2, tag0, tag1, field1, time, __chunk_order]"
        "#
        );
    }

    // Projection only has the first column as constant
    #[test]
    fn test_negative_proj_with_only_first_col_as_const() {
        test_helpers::maybe_start_logging();

        let schema = schema();
        let sort_order = sort_order_for_sort();

        // First sort on parquet file
        let plan_parquet = parquet_exec_with_value_range(&schema, 1000, 2000);
        let plan_projection_1 = Arc::new(
            ProjectionExec::try_new(
                vec![
                    (
                        expr_dict_const("m1".to_string()),
                        String::from("iox::measurement"),
                    ), // constant table name
                    (expr_col("tag0", &schema), String::from("key")), // tag value -- not constant
                    (expr_col("tag1", &schema), String::from("value")), // tag value -- not constant
                ],
                // ProjectionExec::try_new(projection_expr_with_one_constant("m1", &schema),
                plan_parquet,
            )
            .unwrap(),
        );
        let plan_sort1 = Arc::new(SortExec::new(sort_order.clone(), plan_projection_1));

        // Second sort on record batch
        let plan_batches = record_batches_exec_with_value_range(1, 2001, 3000);
        let plan_projection_2 = Arc::new(
            ProjectionExec::try_new(
                projection_expr_with_2_constants("m0", "tag0", &schema),
                plan_batches,
            )
            .unwrap(),
        );
        let plan_sort2 = Arc::new(SortExec::new(sort_order.clone(), plan_projection_2));

        // union the 2 sorts
        let plan_union = Arc::new(UnionExec::new(vec![plan_sort1, plan_sort2]));

        // add sort preserving merge on top
        let plan_spm = Arc::new(SortPreservingMergeExec::new(
            sort_order_for_sort_preserving_merge(),
            plan_union,
        ));

        // input and output are the same
        let opt = OrderUnionSortedInputsForConstants;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan_spm, opt),
            @r#"
        input:
          - " SortPreservingMergeExec: [iox::measurement@0 ASC NULLS LAST,key@1 ASC NULLS LAST,value@2 ASC NULLS LAST]"
          - "   UnionExec"
          - "     SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
          - "       ProjectionExec: expr=[m1 as iox::measurement, tag0@1 as key, tag1@2 as value]"
          - "         ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[tag2, tag0, tag1, field1, time, __chunk_order], output_ordering=[__chunk_order@5 ASC]"
          - "     SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
          - "       ProjectionExec: expr=[m0 as iox::measurement, tag0 as key, tag0@1 as value]"
          - "         RecordBatchesExec: chunks=1, projection=[tag2, tag0, tag1, field1, time, __chunk_order]"
        output:
          Ok:
            - " SortPreservingMergeExec: [iox::measurement@0 ASC NULLS LAST,key@1 ASC NULLS LAST,value@2 ASC NULLS LAST]"
            - "   UnionExec"
            - "     SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
            - "       ProjectionExec: expr=[m1 as iox::measurement, tag0@1 as key, tag1@2 as value]"
            - "         ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[tag2, tag0, tag1, field1, time, __chunk_order], output_ordering=[__chunk_order@5 ASC]"
            - "     SortExec: expr=[value@2 ASC NULLS LAST], preserve_partitioning=[false]"
            - "       ProjectionExec: expr=[m0 as iox::measurement, tag0 as key, tag0@1 as value]"
            - "         RecordBatchesExec: chunks=1, projection=[tag2, tag0, tag1, field1, time, __chunk_order]"
        "#
        );
    }

    // ------------------------------------------------------------------
    // Helper functions
    // ------------------------------------------------------------------

    fn schema() -> SchemaRef {
        IOxSchemaBuilder::new()
            .tag("tag2")
            .tag("tag0")
            .tag("tag1")
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

    fn expr_dict_const(val: String) -> Arc<dyn PhysicalExpr> {
        Arc::new(Literal::new(ScalarValue::Dictionary(
            Box::new(DataType::Int32),
            Box::new(ScalarValue::new_utf8(val)),
        )))
    }

    // test chunk with time range and field1's value range
    fn test_chunk(min: i64, max: i64, parquet_data: bool) -> Arc<dyn QueryChunk> {
        let chunk = TestChunk::new("t")
            .with_time_column_with_stats(Some(min), Some(max))
            .with_tag_column_with_stats("tag1", Some("AL"), Some("MT"))
            .with_tag_column_with_stats("tag2", Some("MA"), Some("MT"))
            .with_tag_column_with_stats("tag3", Some("NM"), Some("VY"))
            .with_i64_field_column_with_stats("field1", Some(min), Some(max));

        let chunk = if parquet_data {
            chunk.with_dummy_parquet_file()
        } else {
            chunk
        };

        Arc::new(chunk) as Arc<dyn QueryChunk>
    }

    fn record_batches_exec_with_value_range(
        n_chunks: usize,
        min: i64,
        max: i64,
    ) -> Arc<dyn ExecutionPlan> {
        let chunks = std::iter::repeat(test_chunk(min, max, false))
            .take(n_chunks)
            .collect::<Vec<_>>();

        Arc::new(RecordBatchesExec::new(chunks, schema(), None))
    }

    fn parquet_exec_with_value_range(
        schema: &SchemaRef,
        min: i64,
        max: i64,
    ) -> Arc<dyn ExecutionPlan> {
        let chunk = test_chunk(min, max, true);
        let plan = chunks_to_physical_nodes(schema, None, vec![chunk], 1);

        if let Some(union_exec) = plan.as_any().downcast_ref::<UnionExec>() {
            if union_exec.inputs().len() == 1 {
                Arc::clone(&union_exec.inputs()[0])
            } else {
                plan
            }
        } else {
            plan
        }
    }

    // projection with 2 dictionary constants and an normal column
    fn projection_expr_with_2_constants(
        first_constant: &str,
        second_constant: &str,
        schema: &SchemaRef,
    ) -> Vec<(Arc<dyn PhysicalExpr>, String)> {
        vec![
            (
                // constant table name
                expr_dict_const(first_constant.to_string()),
                String::from("iox::measurement"),
            ),
            (
                // constant tag name
                expr_dict_const(second_constant.to_string()),
                String::from("key"),
            ),
            // tag value
            (expr_col(second_constant, schema), String::from("value")),
        ]
    }

    // projection with 2 constant integers,  one as an literal integer and the other is converted to string
    // and used as a dictionary value. These are done on purpose to ensure they work even if different data types
    fn projection_expr_with_2_constant_integers(
        first_constant: i32,
        second_constant: i32,
        schema: &SchemaRef,
    ) -> Vec<(Arc<dyn PhysicalExpr>, String)> {
        vec![
            (
                // Make a literal of a integer for the first constant
                Arc::new(Literal::new(ScalarValue::Int32(Some(first_constant)))),
                String::from("iox::measurement"),
            ),
            (
                // Convert the second integer to string and use it as a dictionary value
                expr_dict_const(second_constant.to_string()),
                String::from("key"),
            ),
            // tag value
            (expr_col("tag0", schema), String::from("value")),
        ]
    }

    // projection with 3 dictionnary constants and an normal column
    fn projection_expr_with_3_constants(
        first_constant: &str,
        second_constant: &str,
        third_constant: &str,
        schema: &SchemaRef,
    ) -> Vec<(Arc<dyn PhysicalExpr>, String)> {
        vec![
            (
                // constant table name
                expr_dict_const(first_constant.to_string()),
                String::from("iox::measurement"),
            ),
            (
                // constant tag name
                expr_dict_const(second_constant.to_string()),
                String::from("key"),
            ),
            (
                // constant tag name
                expr_dict_const(third_constant.to_string()),
                String::from("another_key"),
            ),
            // tag value
            (expr_col("tag0", schema), String::from("value")),
        ]
    }

    fn sort_order_for_sort_preserving_merge() -> Vec<PhysicalSortExpr> {
        let measurement = Arc::new(Column::new("iox::measurement", 0)) as Arc<dyn PhysicalExpr>;
        let key = Arc::new(Column::new("key", 1)) as Arc<dyn PhysicalExpr>;
        let value = Arc::new(Column::new("value", 2)) as Arc<dyn PhysicalExpr>;

        vec![
            PhysicalSortExpr {
                expr: measurement,
                options: SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            },
            PhysicalSortExpr {
                expr: key,
                options: SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            },
            PhysicalSortExpr {
                expr: value,
                options: SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            },
        ]
    }

    fn sort_order_for_sort() -> Vec<PhysicalSortExpr> {
        let value = Arc::new(Column::new("value", 2)) as Arc<dyn PhysicalExpr>;

        vec![PhysicalSortExpr {
            expr: value,
            options: SortOptions {
                descending: false,
                nulls_first: false,
            },
        }]
    }

    fn sort_order_two_cols() -> Vec<PhysicalSortExpr> {
        let measurement = Arc::new(Column::new("iox::measurement", 0)) as Arc<dyn PhysicalExpr>;
        let key = Arc::new(Column::new("key", 1)) as Arc<dyn PhysicalExpr>;

        vec![
            PhysicalSortExpr {
                expr: measurement,
                options: SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            },
            PhysicalSortExpr {
                expr: key,
                options: SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            },
        ]
    }

    fn sort_order_not_on_column() -> Vec<PhysicalSortExpr> {
        let measurement =
            Arc::new(Literal::new(ScalarValue::from("iox::measurement"))) as Arc<dyn PhysicalExpr>;

        vec![PhysicalSortExpr {
            expr: measurement,
            options: SortOptions {
                descending: false,
                nulls_first: false,
            },
        }]
    }
}
