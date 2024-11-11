use crate::exec::{FieldsPivotExec, SchemaPivotExec, SeriesPivotExec};
use crate::plan::{FieldsPivot, SchemaPivot, SeriesPivot};
use datafusion::common::Result;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{ExprSchemable, LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use std::sync::Arc;

#[derive(Debug, Default, Copy, Clone)]
pub(crate) struct InfluxRpcExtensionPlanner {}

#[async_trait::async_trait]
impl ExtensionPlanner for InfluxRpcExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let node = node.as_any();
        if let Some(node) = node.downcast_ref::<FieldsPivot>() {
            let input = Arc::clone(&physical_inputs[0]);
            let schema = Arc::new(node.schema().as_arrow().clone());
            let exec = FieldsPivotExec::new(&schema, input)?;
            Ok(Some(Arc::new(exec)))
        } else if let Some(node) = node.downcast_ref::<SeriesPivot>() {
            let input = Arc::clone(&physical_inputs[0]);
            let schema = Arc::new(node.schema().as_arrow().clone());
            let input_schema = logical_inputs[0].schema();
            let measurement_expr = create_physical_expr(
                &node.measurement_expr,
                input_schema,
                session_state.execution_props(),
            )?;
            let tag_exprs = node
                .tag_exprs
                .iter()
                .map(|expr| {
                    create_physical_expr(expr, input_schema, session_state.execution_props())
                })
                .collect::<Result<Vec<_>>>()?;
            let time_exprs = node
                .time_exprs
                .iter()
                .map(|expr| {
                    create_physical_expr(expr, input_schema, session_state.execution_props())
                })
                .collect::<Result<Vec<_>>>()?;
            let field_exprs = node
                .field_exprs
                .iter()
                .map(|expr| {
                    expr.to_field(input_schema).and_then(|(_, field)| {
                        create_physical_expr(expr, input_schema, session_state.execution_props())
                            .map(|expr| (expr, Arc::from(field.name().as_str())))
                    })
                })
                .collect::<Result<Vec<_>>>()?;

            let exec = SeriesPivotExec::new(
                input,
                schema,
                measurement_expr,
                tag_exprs,
                time_exprs,
                field_exprs,
            );
            Ok(Some(Arc::new(exec)))
        } else if let Some(schema_pivot) = node.downcast_ref::<SchemaPivot>() {
            assert_eq!(physical_inputs.len(), 1, "Inconsistent number of inputs");
            Ok(Some(Arc::new(SchemaPivotExec::new(
                Arc::clone(&physical_inputs[0]),
                schema_pivot.schema().as_ref().clone().into(),
            ))))
        } else {
            Ok(None)
        }
    }
}
