use crate::extension_planner::InfluxRpcExtensionPlanner;
use crate::physical_optimizer::SeriesPivotPushdown;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_planner::ExtensionPlanner;
use iox_query::Extension;
use std::sync::Arc;

/// Extension for the InfluxRPC query specifiv functionality.
#[derive(Debug, Default, Copy, Clone)]
pub struct InfluxRpcExtension {}

impl Extension for InfluxRpcExtension {
    fn planner(&self) -> Option<Arc<dyn ExtensionPlanner + Send + Sync>> {
        Some(Arc::new(InfluxRpcExtensionPlanner {}))
    }

    fn extend_session_state(&self, mut state: SessionStateBuilder) -> SessionStateBuilder {
        let mut rules = if let Some(physical_optimizers) = state.physical_optimizers() {
            std::mem::take(&mut physical_optimizers.rules)
        } else {
            vec![]
        };
        let idx = if let Some(idx) = rules
            .iter()
            .position(|rule| rule.as_ref().name() == "projection_pushdown")
        {
            idx + 1
        } else {
            rules.len()
        };
        // Insert the new rules after the projection_pushdown rule.
        rules.insert(idx, Arc::new(SeriesPivotPushdown::default()));

        state.with_physical_optimizer_rules(rules)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::LogicalPlanBuilderExt;
    use arrow::{
        array::{ArrayRef, RecordBatch, StringArray},
        datatypes::SchemaRef,
    };
    use datafusion::{
        datasource::{provider_as_source, MemTable},
        logical_expr::LogicalPlanBuilder,
    };
    use futures::stream::TryStreamExt;
    use iox_query::exec::Executor;
    use std::collections::BTreeSet;

    #[tokio::test]
    async fn schema_pivot_is_planned() {
        // Test that all the planning logic is wired up and that we
        // can make a plan using a SchemaPivot node
        let batch = RecordBatch::try_from_iter_with_nullable(vec![
            ("f1", to_string_array(&["foo", "bar"]), true),
            ("f2", to_string_array(&["baz", "bzz"]), true),
        ])
        .expect("created new record batch");

        let builder = make_plan_builder(batch.schema(), vec![batch]);
        let pivot = builder.schema_pivot().unwrap().build().unwrap();

        let exec = Executor::new_testing();
        let ctx = exec
            .new_session_config()
            .with_query_extension(Arc::new(InfluxRpcExtension {}))
            .build();
        let physical_plan = ctx
            .create_physical_plan(&pivot)
            .await
            .expect("Created physical plan");
        let mut stream = ctx
            .execute_stream(physical_plan)
            .await
            .expect("Executed plan");

        let mut results = BTreeSet::<String>::default();
        while let Some(batch) = stream.try_next().await.expect("Got next batch") {
            let arr = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Cast to string array");
            results.extend(arr.into_iter().map(|x| String::from(x.expect("No nulls"))));
        }

        assert_eq!(results, to_set(&["f1", "f2"]));
    }

    /// return a set for testing
    fn to_set(strs: &[&str]) -> BTreeSet<String> {
        strs.iter().map(|s| s.to_string()).collect()
    }

    fn to_string_array(strs: &[&str]) -> ArrayRef {
        let array: StringArray = strs.iter().map(|s| Some(*s)).collect();
        Arc::new(array)
    }

    // creates a DataFusion plan that reads the RecordBatches into memory
    fn make_plan_builder(schema: SchemaRef, data: Vec<RecordBatch>) -> LogicalPlanBuilder {
        let partitions = vec![data];

        let projection = None;

        // model one partition,
        let table = MemTable::try_new(schema, partitions).unwrap();
        let source = provider_as_source(Arc::new(table));

        LogicalPlanBuilder::scan("memtable", source, projection).unwrap()
    }
}
