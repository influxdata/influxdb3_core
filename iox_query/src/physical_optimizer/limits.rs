use crate::config::IoxConfigExt;
use crate::physical_optimizer::chunk_extraction::extract_chunks;
use datafusion::config::ConfigOptions;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use std::collections::HashSet;
use std::sync::Arc;

pub struct CheckLimits;

impl PhysicalOptimizerRule for CheckLimits {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut partitions: HashSet<String> = Default::default();
        let mut parquet_files = 0;
        if let Some((_, chunks, _)) = extract_chunks(plan.as_ref()) {
            for chunk in chunks {
                partitions.insert(chunk.partition_id().to_string());
                if chunk.chunk_type() == "parquet" {
                    parquet_files += 1;
                }
            }
        }
        let iox_config = config
            .extensions
            .get::<IoxConfigExt>()
            .cloned()
            .unwrap_or_default();
        if let Some(partition_limit) = iox_config.partition_limit_opt() {
            if partitions.len() > partition_limit {
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
