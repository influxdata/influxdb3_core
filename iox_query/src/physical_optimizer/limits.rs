use crate::config::IoxConfigExt;
use datafusion::config::ConfigOptions;
use datafusion::datasource::physical_plan::ParquetExec;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::{accept, ExecutionPlan, ExecutionPlanVisitor};
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
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ParquetFileMetrics {
    pub(crate) partitions: usize,
    pub(crate) parquet_files: usize,
}

impl ParquetFileMetrics {
    /// Calculate metrics for a given plan.
    pub(crate) fn plan_metrics(plan: &dyn ExecutionPlan) -> Self {
        let mut partitions: std::collections::HashSet<String> = Default::default();
        let mut parquet_files = 0;
        for parquet_file in list_parquet_files(plan) {
            parquet_files += 1;
            // The partition is identified by all the path elements before the file name.
            if let Some((partition, _)) = parquet_file.rsplit_once(object_store::path::DELIMITER) {
                partitions.insert(partition.to_string());
            }
        }
        Self {
            partitions: partitions.len(),
            parquet_files,
        }
    }
}

fn list_parquet_files(plan: &dyn ExecutionPlan) -> Vec<String> {
    let mut visitor = ParquetFileVisitor {
        parquet_files: Vec::new(),
    };
    accept(plan, &mut visitor).unwrap();
    visitor.parquet_files
}

struct ParquetFileVisitor {
    parquet_files: Vec<String>,
}

impl ExecutionPlanVisitor for ParquetFileVisitor {
    type Error = DataFusionError;

    fn pre_visit(&mut self, plan: &dyn ExecutionPlan) -> std::result::Result<bool, Self::Error> {
        if let Some(parquet_exec) = plan.as_any().downcast_ref::<ParquetExec>() {
            self.parquet_files.extend(
                parquet_exec
                    .base_config()
                    .file_groups
                    .iter()
                    .flatten()
                    .map(|pf| pf.path().to_string()),
            );
        }
        Ok(true)
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
                partitions: 4,
                parquet_files: 16
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
                partitions: 1,
                parquet_files: 16
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
