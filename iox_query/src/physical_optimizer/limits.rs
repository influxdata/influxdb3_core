use crate::config::IoxConfigExt;
use crate::provider::DeduplicateExec;
use datafusion::config::ConfigOptions;
use datafusion::datasource::physical_plan::ParquetExec;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use object_store::path::Path;
use std::collections::HashSet;
use std::sync::Arc;

pub struct CheckLimits;

impl PhysicalOptimizerRule for CheckLimits {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let ParquetFileMetrics {
            partitions,
            parquet_files,
            deduplicated_parquet_files: _,
            deduplicated_partitions: _,
        } = ParquetFileMetrics::plan_metrics(plan.as_ref());

        let iox_config = config
            .extensions
            .get::<IoxConfigExt>()
            .cloned()
            .unwrap_or_default();
        if let Some(partition_limit) = iox_config.partition_limit_opt() {
            if partitions > partition_limit {
                return Err(DataFusionError::ResourcesExhausted(format!(
                    "Query would process more than {} partitions",
                    partition_limit
                )));
            }
        }
        if let Some(parquet_file_limit) = iox_config.parquet_file_limit_opt() {
            if parquet_files > parquet_file_limit {
                return Err(DataFusionError::ResourcesExhausted(format!(
                    "Query would process more than {} parquet files",
                    parquet_file_limit
                )));
            }
        }
        Ok(plan)
    }

    fn name(&self) -> &str {
        "check-limits"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Metrics information about parquet files.
#[derive(Debug, Clone, PartialEq, Default)]
pub(crate) struct ParquetFileMetrics {
    pub(crate) parquet_files: usize,
    pub(crate) deduplicated_parquet_files: usize,
    pub(crate) partitions: usize,
    pub(crate) deduplicated_partitions: usize,
}

impl ParquetFileMetrics {
    /// Calculate metrics for a given plan.
    pub(crate) fn plan_metrics(plan: &dyn ExecutionPlan) -> Self {
        let mut metrics = ParquetFileMetricsVisitor::default();

        metrics.collect_for_plan(plan, false);

        Self {
            parquet_files: metrics.parquet_files,
            deduplicated_parquet_files: metrics.deduplicated_parquet_files,
            partitions: metrics.partitions.len(),
            deduplicated_partitions: metrics.deduplicated_partitions.len(),
        }
    }
}

#[derive(Default)]
struct ParquetFileMetricsVisitor<'plan> {
    pub(crate) parquet_files: usize,
    pub(crate) deduplicated_parquet_files: usize,
    pub(crate) partitions: HashSet<&'plan str>,
    pub(crate) deduplicated_partitions: HashSet<&'plan str>,
}

impl<'plan> ParquetFileMetricsVisitor<'plan> {
    // This could be implemented via a visitor pattern, specifically ExecutionPlanVisitor, but that
    // doesn't have lifetime bounds, so the visitor can't borrow from the plan (like we want to do
    // here), which would require us to allocate much more than necessary.
    fn collect_for_plan(
        &mut self,
        plan: &'plan (dyn ExecutionPlan + 'plan),
        mut under_dedup: bool,
    ) {
        if let Some(parquet_exec) = plan.as_any().downcast_ref::<ParquetExec>() {
            let file_iter = parquet_exec
                .base_config()
                .file_groups
                .iter()
                .flatten()
                .map(|p| p.path());

            if under_dedup {
                Self::add_parts_and_files_from_iter(
                    file_iter.clone(),
                    &mut self.deduplicated_parquet_files,
                    &mut self.deduplicated_partitions,
                );
            }

            Self::add_parts_and_files_from_iter(
                file_iter,
                &mut self.parquet_files,
                &mut self.partitions,
            );
        } else if plan.as_any().downcast_ref::<DeduplicateExec>().is_some() {
            under_dedup = true;
        }

        for child in plan.children() {
            self.collect_for_plan(child.as_ref(), under_dedup);
        }
    }

    fn add_parts_and_files_from_iter<I>(iter: I, files: &mut usize, parts: &mut HashSet<&'plan str>)
    where
        I: Iterator<Item = &'plan Path> + Clone,
    {
        *files += iter.clone().count();
        parts.extend(iter.flat_map(|path| {
            path.as_ref()
                .rsplit_once(object_store::path::DELIMITER)
                .map(|(part, _)| part)
        }));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::datatypes::Schema;
    use datafusion::common::stats::Precision;
    use datafusion::datasource::listing::PartitionedFile;
    use datafusion::datasource::physical_plan::parquet::ParquetExecBuilder;
    use datafusion::datasource::physical_plan::FileScanConfig;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::physical_plan::union::UnionExec;
    use datafusion::physical_plan::Statistics;
    use datafusion_util::config::table_parquet_options;
    use std::sync::Arc;

    #[test]
    fn test_metrics() {
        let execs: Vec<Arc<dyn ExecutionPlan>> = vec![
            parquet_exec(&[
                "1/2/partition1/file11",
                "1/2/partition1/file12",
                "1/2/partition1/file13",
                "1/2/partition1/file14",
            ]),
            parquet_exec(&[
                "1/2/partition2/file21",
                "1/2/partition2/file22",
                "1/2/partition2/file23",
                "1/2/partition2/file24",
            ]),
            parquet_exec(&[
                "1/2/partition3/file31",
                "1/2/partition3/file32",
                "1/2/partition3/file33",
                "1/2/partition3/file34",
            ]),
            parquet_exec(&[
                "1/2/partition4/file41",
                "1/2/partition4/file42",
                "1/2/partition4/file43",
                "1/2/partition4/file44",
            ]),
        ];
        let plan = UnionExec::new(execs);
        let metrics = ParquetFileMetrics::plan_metrics(&plan);
        assert_eq!(
            metrics,
            ParquetFileMetrics {
                parquet_files: 16,
                deduplicated_parquet_files: 0,
                partitions: 4,
                deduplicated_partitions: 0
            }
        );
    }

    #[test]
    fn test_single_partition() {
        let execs: Vec<Arc<dyn ExecutionPlan>> = vec![
            parquet_exec(&[
                "1/2/partition1/file11",
                "1/2/partition1/file12",
                "1/2/partition1/file13",
                "1/2/partition1/file14",
            ]),
            parquet_exec(&[
                "1/2/partition1/file21",
                "1/2/partition1/file22",
                "1/2/partition1/file23",
                "1/2/partition1/file24",
            ]),
            parquet_exec(&[
                "1/2/partition1/file31",
                "1/2/partition1/file32",
                "1/2/partition1/file33",
                "1/2/partition1/file34",
            ]),
            parquet_exec(&[
                "1/2/partition1/file41",
                "1/2/partition1/file42",
                "1/2/partition1/file43",
                "1/2/partition1/file44",
            ]),
        ];
        let plan = UnionExec::new(execs);
        let metrics = ParquetFileMetrics::plan_metrics(&plan);
        assert_eq!(
            metrics,
            ParquetFileMetrics {
                parquet_files: 16,
                deduplicated_parquet_files: 0,
                partitions: 1,
                deduplicated_partitions: 0
            }
        );
    }

    #[test]
    fn test_deduplicate_exec() {
        let execs = vec![
            parquet_exec(&[
                "1/2/partition1/file01",
                "1/2/partition2/file01",
                "1/2/partition1/file02",
                "1/2/partition1/file03",
            ]),
            Arc::new(DeduplicateExec::new(
                parquet_exec(&[
                    "1/2/partition2/file02",
                    "1/3/partition1/file01",
                    "1/3/partition1/file02",
                    "1/4/partition1/file01",
                ]),
                Vec::new(),
                false,
            )),
        ];

        let plan = UnionExec::new(execs);
        let metrics = ParquetFileMetrics::plan_metrics(&plan);

        assert_eq!(
            metrics,
            ParquetFileMetrics {
                parquet_files: 8,
                deduplicated_parquet_files: 4,
                partitions: 4,
                deduplicated_partitions: 3
            }
        );
    }

    fn parquet_exec(files: &[&str]) -> Arc<dyn ExecutionPlan> {
        let base_config = FileScanConfig {
            object_store_url: ObjectStoreUrl::local_filesystem(),
            file_schema: Arc::new(Schema {
                fields: Default::default(),
                metadata: Default::default(),
            }),
            file_groups: files
                .iter()
                .map(|f| vec![PartitionedFile::new(*f, 0)])
                .collect(),
            statistics: Statistics {
                num_rows: Precision::Absent,
                total_byte_size: Precision::Absent,
                column_statistics: vec![],
            },
            projection: None,
            limit: None,
            table_partition_cols: vec![],
            output_ordering: vec![],
        };
        let builder = ParquetExecBuilder::new_with_options(base_config, table_parquet_options());
        builder.build_arc()
    }
}
