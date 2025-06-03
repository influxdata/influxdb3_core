//! Extract [`NonOverlappingOrderedLexicalRanges`] from different sources.

use arrow::compute::SortOptions;
use arrow::datatypes::Schema;
use datafusion::common::{
    internal_datafusion_err, ColumnStatistics, Result, ScalarValue, Statistics,
};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::LexOrdering;
use datafusion::physical_plan::{displayable, ExecutionPlan, ExecutionPlanProperties};
use itertools::FoldWhile;
use itertools::Itertools;
use observability_deps::tracing::trace;
use std::sync::Arc;

use crate::physical_optimizer::sort::lexical_range::{
    LexicalRange, NonOverlappingOrderedLexicalRanges,
};
use crate::statistics::partition_statistics::util::merge_col_stats;
use crate::statistics::{column_statistics_min_max, statistics_by_partition};

/// Attempt to extract LexicalRanges for the given sort keys and input plan
///
/// Output will have N ranges where N is the number of output partitions
///
/// Returns None if not possible to determine ranges.
#[cfg_attr(not(test), expect(unused))]
pub(crate) fn extract_disjoint_ranges_from_plan(
    exprs: &LexOrdering,
    input_plan: &Arc<dyn ExecutionPlan>,
) -> Result<Option<NonOverlappingOrderedLexicalRanges>> {
    trace!(
        "Extracting lexical ranges for input plan: \n{}",
        displayable(input_plan.as_ref()).indent(true)
    );

    // if the ordering does not match, then we cannot confirm proper ranges
    if !input_plan
        .properties()
        .equivalence_properties()
        .ordering_satisfy(exprs)
    {
        return Ok(None);
    }

    let num_input_partitions = partition_count(input_plan);

    // One builder for each output partition.
    // Each builder will contain multiple sort keys
    let mut builders = vec![LexicalRange::builder(); num_input_partitions];

    // get partitioned stats for the plan
    let partitioned_stats = statistics_by_partition(input_plan.as_ref())?;

    // add per sort key
    for sort_expr in exprs.iter() {
        let Some(column) = sort_expr.expr.as_any().downcast_ref::<Column>() else {
            return Ok(None);
        };
        let Ok(col_idx) = input_plan.schema().index_of(column.name()) else {
            return Ok(None);
        };

        // add per partition
        for (builder, stats_for_partition) in builders.iter_mut().zip(&partitioned_stats) {
            let Some((min, max)) = min_max_for_stats_with_sort_options(
                col_idx,
                &sort_expr.options,
                stats_for_partition,
            )?
            else {
                return Ok(None);
            };
            builder.push(min, max);
        }
    }

    let sort_options = exprs.iter().map(|e| e.options).collect::<Vec<_>>();
    let ranges_per_partition = builders
        .into_iter()
        .map(|builder| builder.build())
        .collect::<Vec<_>>();

    trace!("Found lexical ranges: {:?}", ranges_per_partition);

    NonOverlappingOrderedLexicalRanges::try_new(&sort_options, ranges_per_partition)
}

fn partition_count(plan: &Arc<dyn ExecutionPlan>) -> usize {
    plan.output_partitioning().partition_count()
}

/// Returns the min and max value for the [`Statistics`],
/// and handles null values based on [`SortOptions`].
///
/// Returns None if statistics are absent/unknown.
fn min_max_for_stats_with_sort_options(
    col_idx: usize,
    sort_options: &SortOptions,
    stats: &Statistics,
) -> Result<Option<(ScalarValue, ScalarValue)>> {
    // Check if the column is a constant value according to the equivalence properties (TODO)

    let Some(ColumnStatistics {
        max_value,
        min_value,
        null_count,
        ..
    }) = stats.column_statistics.get(col_idx)
    else {
        return Err(internal_datafusion_err!(
            "extracted statistics is missing @{:?}, only has {:?} columns",
            col_idx,
            stats.column_statistics.len()
        ));
    };

    let (Some(min), Some(max)) = (min_value.get_value(), max_value.get_value()) else {
        return Ok(None);
    };

    let mut min = min.clone();
    let mut max = max.clone();
    if *null_count.get_value().unwrap_or(&0) > 0 {
        let nulls_as_min = !sort_options.descending && sort_options.nulls_first // ASC nulls first
    || sort_options.descending && !sort_options.nulls_first; // DESC nulls last

        // Get the typed null value for the data type of min/max
        let null: ScalarValue = min.data_type().try_into()?;

        if nulls_as_min {
            min = null;
        } else {
            max = null;
        }
    }

    Ok(Some((min, max)))
}

/// Attempt to extract LexicalRanges for the given sort keys and partitioned file groups.
///
/// Since the goal is to have as many non-overlapping ranges as possible, by determinining
/// the set of ranges. Each range may contain >1 file.
///
/// Therefore we expect M disjoint ranges for N files.
///
/// Returns None if not possible to determine disjoint ranges.
pub(crate) fn extract_ranges_from_files(
    exprs: &LexOrdering,
    file_groups: &[Vec<PartitionedFile>],
    schema: Arc<Schema>,
) -> Result<Option<NonOverlappingOrderedLexicalRanges>> {
    trace!(
        "Extracting lexical ranges for {:?} file groups",
        file_groups.len()
    );
    let num_input_partitions = file_groups.len();

    // one builder for each output partition
    let mut builders = vec![LexicalRange::builder(); num_input_partitions];
    for sort_expr in exprs.iter() {
        let Some(column) = sort_expr.expr.as_any().downcast_ref::<Column>() else {
            return Ok(None);
        };
        let col_name = column.name();

        for (grouped_partitioned_files, builder) in file_groups.iter().zip(builders.iter_mut()) {
            let Some((min, max)) =
                min_max_for_partitioned_filegroup(col_name, grouped_partitioned_files, &schema)?
            else {
                return Ok(None);
            };
            builder.push(min, max);
        }
    }

    let sort_options = exprs.iter().map(|e| e.options).collect::<Vec<_>>();
    let ranges_per_partition = builders
        .into_iter()
        .map(|builder| builder.build())
        .collect::<Vec<_>>();

    trace!("Found lexical ranges: {:?}", ranges_per_partition);

    NonOverlappingOrderedLexicalRanges::try_new(&sort_options, ranges_per_partition)
}

/// Return the min and max value for the specified group of partitioned files.
pub(crate) fn min_max_for_partitioned_filegroup(
    col_name: &str,
    filegroup: &[PartitionedFile],
    schema: &Arc<Schema>,
) -> Result<Option<(ScalarValue, ScalarValue)>> {
    let Some((col_idx, _)) = schema.fields().find(col_name) else {
        return Ok(None);
    };

    // from all files in group
    let Some(merged_col_stats) = filegroup
        .iter()
        .map(|file| {
            file.statistics
                .as_ref()
                .map(|has_stats| has_stats.column_statistics[col_idx].clone())
        })
        .fold_while(None, |acc, maybe_col_stats| match (acc, maybe_col_stats) {
            (_, None) => FoldWhile::Done(None),
            (None, Some(col_stats)) => FoldWhile::Continue(Some(col_stats)),
            (Some(acc_col_stats), Some(col_stats)) => {
                FoldWhile::Continue(Some(merge_col_stats(acc_col_stats, &col_stats)))
            }
        })
        .into_inner()
    else {
        // missing col_stats on one of the partitioned files in the group
        return Ok(None);
    };

    // `column_statistics_min_max` considers precision
    Ok(column_statistics_min_max(merged_col_stats))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::test_utils::{
        parquet_exec_with_sort_with_statistics, parquet_exec_with_sort_with_statistics_and_schema,
        sort_exec, union_exec, SortKeyRange,
    };
    use std::fmt::{Debug, Display, Formatter};

    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field};
    use datafusion::{
        physical_expr::{LexOrdering, PhysicalSortExpr},
        physical_plan::expressions::col,
    };
    use insta::assert_snapshot;

    /// test with three partition ranges that are disjoint (non overlapping)
    fn test_case_disjoint_3() -> TestCaseBuilder {
        TestCaseBuilder::new()
            .with_key_range(SortKeyRange {
                min: Some(1000),
                max: Some(2000),
                null_count: 0,
            })
            .with_key_range(SortKeyRange {
                min: Some(2001),
                max: Some(3000),
                null_count: 0,
            })
            .with_key_range(SortKeyRange {
                min: Some(3001),
                max: Some(3500),
                null_count: 0,
            })
    }

    /// test with three partition ranges that are NOT disjoint (are overlapping)
    fn test_case_overlapping_3() -> TestCaseBuilder {
        TestCaseBuilder::new()
            .with_key_range(SortKeyRange {
                min: Some(1000),
                max: Some(2010),
                null_count: 0,
            })
            .with_key_range(SortKeyRange {
                min: Some(2001),
                max: Some(3000),
                null_count: 0,
            })
            .with_key_range(SortKeyRange {
                min: Some(3001),
                max: Some(3500),
                null_count: 0,
            })
    }

    #[test]
    fn test_union_sort_union_disjoint_ranges_asc() {
        assert_snapshot!(
        test_case_disjoint_3()
            .with_descending(false)
            .with_sort_expr("a")
            .union_sort_union_plan(),
         @r"
        UnionExec
          ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[a], output_ordering=[a@0 ASC]
          SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
            UnionExec
              ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[a], output_ordering=[a@0 ASC]
              ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[a], output_ordering=[a@0 ASC]

        Input Ranges
          (Some(1000))->(Some(2000))
          (Some(2001))->(Some(3000))
          (Some(3001))->(Some(3500))
        Output Ranges: [0, 1]
          (1000)->(2000)
          (2001)->(3500)
        ");
    }

    #[test]
    fn test_union_sort_union_overlapping_ranges_asc() {
        assert_snapshot!(
         test_case_overlapping_3()
            .with_descending(false)
            .with_sort_expr("a")
            .union_sort_union_plan(),
         @r"
        UnionExec
          ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[a], output_ordering=[a@0 ASC]
          SortExec: expr=[a@0 ASC], preserve_partitioning=[false]
            UnionExec
              ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[a], output_ordering=[a@0 ASC]
              ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[a], output_ordering=[a@0 ASC]

        Input Ranges
          (Some(1000))->(Some(2010))
          (Some(2001))->(Some(3000))
          (Some(3001))->(Some(3500))
        No disjoint ranges found
        ");
    }

    #[test]
    fn test_union_sort_union_disjoint_ranges_desc_nulls_first() {
        assert_snapshot!(
        test_case_disjoint_3()
            .with_descending(true)
            .with_sort_expr("a")
            .union_sort_union_plan(),
         @r"
        UnionExec
          ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[a], output_ordering=[a@0 DESC]
          SortExec: expr=[a@0 DESC], preserve_partitioning=[false]
            UnionExec
              ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[a], output_ordering=[a@0 DESC]
              ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[a], output_ordering=[a@0 DESC]

        Input Ranges
          (Some(1000))->(Some(2000))
          (Some(2001))->(Some(3000))
          (Some(3001))->(Some(3500))
        Output Ranges: [1, 0]
          (2001)->(3500)
          (1000)->(2000)
        ");
    }

    #[test]
    fn test_union_sort_union_overlapping_ranges_desc_nulls_first() {
        assert_snapshot!(
         test_case_overlapping_3()
            .with_descending(true)
            .with_sort_expr("a")
            .union_sort_union_plan(),
         @r"
        UnionExec
          ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[a], output_ordering=[a@0 DESC]
          SortExec: expr=[a@0 DESC], preserve_partitioning=[false]
            UnionExec
              ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[a], output_ordering=[a@0 DESC]
              ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[a], output_ordering=[a@0 DESC]

        Input Ranges
          (Some(1000))->(Some(2010))
          (Some(2001))->(Some(3000))
          (Some(3001))->(Some(3500))
        No disjoint ranges found
        ");
    }

    // default is NULLS FIRST so try NULLS LAST
    #[test]
    fn test_union_sort_union_disjoint_ranges_asc_nulls_last() {
        assert_snapshot!(
        test_case_disjoint_3()
            .with_descending(false)
            .with_nulls_first(false)
            .with_sort_expr("a")
            .union_sort_union_plan(),
         @r"
        UnionExec
          ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[a], output_ordering=[a@0 ASC NULLS LAST]
          SortExec: expr=[a@0 ASC NULLS LAST], preserve_partitioning=[false]
            UnionExec
              ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[a], output_ordering=[a@0 ASC NULLS LAST]
              ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[a], output_ordering=[a@0 ASC NULLS LAST]

        Input Ranges
          (Some(1000))->(Some(2000))
          (Some(2001))->(Some(3000))
          (Some(3001))->(Some(3500))
        Output Ranges: [0, 1]
          (1000)->(2000)
          (2001)->(3500)
        ");
    }

    // default is NULLS FIRST so try NULLS LAST
    #[test]
    fn test_union_sort_union_overlapping_ranges_asc_nulls_last() {
        assert_snapshot!(
         test_case_overlapping_3()
            .with_descending(false)
            .with_nulls_first(false)
            .with_sort_expr("a")
            .union_sort_union_plan(),
         @r"
        UnionExec
          ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[a], output_ordering=[a@0 ASC NULLS LAST]
          SortExec: expr=[a@0 ASC NULLS LAST], preserve_partitioning=[false]
            UnionExec
              ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[a], output_ordering=[a@0 ASC NULLS LAST]
              ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[a], output_ordering=[a@0 ASC NULLS LAST]

        Input Ranges
          (Some(1000))->(Some(2010))
          (Some(2001))->(Some(3000))
          (Some(3001))->(Some(3500))
        No disjoint ranges found
        ");
    }

    #[test]
    fn test_union_disjoint_ranges_asc() {
        assert_snapshot!(
         test_case_disjoint_3()
            .with_descending(false)
            .with_sort_expr("a")
            .union_plan(),
         @r"
        UnionExec
          ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[a], output_ordering=[a@0 ASC]
          ParquetExec: file_groups={2 groups: [[0.parquet], [1.parquet]]}, projection=[a], output_ordering=[a@0 ASC]

        Input Ranges
          (Some(1000))->(Some(2000))
          (Some(2001))->(Some(3000))
          (Some(3001))->(Some(3500))
        Output Ranges: [0, 1, 2]
          (1000)->(2000)
          (2001)->(3000)
          (3001)->(3500)
        ");
    }

    #[test]
    fn test_union_overlapping_ranges_asc() {
        assert_snapshot!(
         test_case_overlapping_3()
            .with_descending(false)
            .with_sort_expr("a")
            .union_plan(),
         @r"
        UnionExec
          ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[a], output_ordering=[a@0 ASC]
          ParquetExec: file_groups={2 groups: [[0.parquet], [1.parquet]]}, projection=[a], output_ordering=[a@0 ASC]

        Input Ranges
          (Some(1000))->(Some(2010))
          (Some(2001))->(Some(3000))
          (Some(3001))->(Some(3500))
        No disjoint ranges found
        ");
    }

    #[test]
    fn test_union_disjoint_ranges_desc() {
        assert_snapshot!(
         test_case_disjoint_3()
            .with_descending(true)
            .with_sort_expr("a")
            .union_plan(),
         @r"
        UnionExec
          ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[a], output_ordering=[a@0 DESC]
          ParquetExec: file_groups={2 groups: [[0.parquet], [1.parquet]]}, projection=[a], output_ordering=[a@0 DESC]

        Input Ranges
          (Some(1000))->(Some(2000))
          (Some(2001))->(Some(3000))
          (Some(3001))->(Some(3500))
        Output Ranges: [2, 1, 0]
          (3001)->(3500)
          (2001)->(3000)
          (1000)->(2000)
        ");
    }

    #[test]
    fn test_union_overlapping_ranges_desc() {
        assert_snapshot!(
         test_case_overlapping_3()
            .with_descending(true)
            .with_sort_expr("a")
            .union_plan(),
         @r"
        UnionExec
          ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[a], output_ordering=[a@0 DESC]
          ParquetExec: file_groups={2 groups: [[0.parquet], [1.parquet]]}, projection=[a], output_ordering=[a@0 DESC]

        Input Ranges
          (Some(1000))->(Some(2010))
          (Some(2001))->(Some(3000))
          (Some(3001))->(Some(3500))
        No disjoint ranges found
        ");
    }

    /// Helper for building up patterns for testing statistics extraction from
    /// ExecutionPlans
    #[derive(Debug)]
    struct TestCaseBuilder {
        input_ranges: Vec<SortKeyRange>,
        sort_options: SortOptions,
        sort_exprs: Vec<PhysicalSortExpr>,
        schema: Arc<Schema>,
    }

    impl TestCaseBuilder {
        /// Creates a new `TestCaseBuilder` instance with default values.
        fn new() -> Self {
            let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));

            Self {
                input_ranges: vec![],
                sort_options: SortOptions::default(),
                sort_exprs: vec![],
                schema,
            }
        }

        /// Add a key range
        pub fn with_key_range(mut self, key_range: SortKeyRange) -> Self {
            self.input_ranges.push(key_range);
            self
        }

        /// set SortOptions::descending flag
        pub fn with_descending(mut self, descending: bool) -> Self {
            self.sort_options.descending = descending;
            self
        }

        /// set SortOptions::nulls_first flag
        pub fn with_nulls_first(mut self, nulls_first: bool) -> Self {
            self.sort_options.nulls_first = nulls_first;
            self
        }

        /// Add a sort expression to the ordering (created with the current SortOptions)
        pub fn with_sort_expr(mut self, column_name: &str) -> Self {
            let expr =
                PhysicalSortExpr::new(col(column_name, &self.schema).unwrap(), self.sort_options);
            self.sort_exprs.push(expr);
            self
        }

        /// Build a test physical plan like the following, and extract disjoint ranges from it:
        ///
        /// ```text
        /// UNION
        ///     ParquetExec (key_ranges[0])            (range_a)
        ///     SORT
        ///         UNION
        ///             ParquetExec (key_ranges[1])    (range_b_1)
        ///             ParquetExec (key_ranges[2])    (range_b_2)
        /// ```
        fn union_sort_union_plan(self) -> TestResult {
            let Self {
                input_ranges,
                sort_options: _, // used to create sort exprs, nothere
                sort_exprs,
                schema,
            } = self;
            let lex_ordering = LexOrdering::new(sort_exprs);

            assert_eq!(input_ranges.len(), 3);
            let range_a = &input_ranges[0];
            let range_b_1 = &input_ranges[1];
            let range_b_2 = &input_ranges[2];

            let datasrc_a = parquet_exec_with_sort_with_statistics_and_schema(
                &schema,
                vec![lex_ordering.clone()],
                &[range_a],
            );

            let datasrc_b1 = parquet_exec_with_sort_with_statistics_and_schema(
                &schema,
                vec![lex_ordering.clone()],
                &[range_b_1],
            );
            let datasrc_b2 = parquet_exec_with_sort_with_statistics_and_schema(
                &schema,
                vec![lex_ordering.clone()],
                &[range_b_2],
            );
            let b = sort_exec(
                &lex_ordering,
                &union_exec(vec![datasrc_b1, datasrc_b2]),
                false,
            );

            let plan = union_exec(vec![datasrc_a, b]);

            let actual = extract_disjoint_ranges_from_plan(&lex_ordering, &plan)
                .expect("Error extracting disjoint ranges from plan");
            TestResult {
                input_ranges,
                plan,
                actual,
            }
        }

        /// Build a test physical plan like the following, and extract disjoint ranges from it:
        ///
        /// ```text
        /// UNION
        ///     ParquetExec (key_ranges[0])                (range_a)
        ///     ParquetExec (key_ranges[1], key_ranges[2]) (range_b_1, range_b_2)
        /// ```
        fn union_plan(self) -> TestResult {
            let Self {
                input_ranges,
                sort_options: _, // used to create sort exprs, nothere
                sort_exprs,
                schema,
            } = self;

            let lex_ordering = LexOrdering::new(sort_exprs);

            assert_eq!(input_ranges.len(), 3);
            let range_a = &input_ranges[0];
            let range_b_1 = &input_ranges[1];
            let range_b_2 = &input_ranges[2];
            let datasrc_a =
                parquet_exec_with_sort_with_statistics(vec![lex_ordering.clone()], &[range_a]);
            let datasrc_b = parquet_exec_with_sort_with_statistics_and_schema(
                &schema,
                vec![lex_ordering.clone()],
                &[range_b_1, range_b_2],
            );

            let plan = union_exec(vec![datasrc_a, datasrc_b]);

            let actual = extract_disjoint_ranges_from_plan(&lex_ordering, &plan)
                .expect("Error extracting disjoint ranges from plan");
            TestResult {
                input_ranges,
                plan,
                actual,
            }
        }
    }

    /// Result of running a test case, including the input ranges, the execution
    /// plan, and the actual disjoint ranges found.
    ///
    /// This struct implements `Display` to provide a formatted output of the
    /// test case results that can be easily compared using `insta` snapshots.
    struct TestResult {
        input_ranges: Vec<SortKeyRange>,
        plan: Arc<dyn ExecutionPlan>,
        actual: Option<NonOverlappingOrderedLexicalRanges>,
    }

    impl TestResult {}

    impl Display for TestResult {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            let displayable_plan = displayable(self.plan.as_ref()).indent(false);
            writeln!(f, "{}", displayable_plan)?;

            writeln!(f, "Input Ranges")?;
            for range in &self.input_ranges {
                writeln!(f, "  {}", range)?;
            }

            match self.actual.as_ref() {
                Some(actual) => {
                    writeln!(f, "Output Ranges: {:?}", actual.indices())?;
                    for range in actual.ordered_ranges() {
                        writeln!(f, "  {}", range)?;
                    }
                }
                None => {
                    writeln!(f, "No disjoint ranges found")?;
                }
            }
            Ok(())
        }
    }
}
