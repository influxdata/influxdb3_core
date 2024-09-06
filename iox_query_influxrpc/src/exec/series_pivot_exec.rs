use arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PhysicalExpr, PlanProperties,
};
use std::sync::Arc;

use super::series_pivot_stream::{FieldExpr, SeriesPivotStream};

pub(crate) struct SeriesPivotExec {
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    properties: PlanProperties,
    metrics: ExecutionPlanMetricsSet,
    measurement_expr: Arc<dyn PhysicalExpr>,
    tag_exprs: Vec<Arc<dyn PhysicalExpr>>,
    time_exprs: Vec<Arc<dyn PhysicalExpr>>,
    field_exprs: Vec<(Arc<dyn PhysicalExpr>, Arc<str>)>,
}

impl SeriesPivotExec {
    pub(crate) fn new(
        input: Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
        measurement_expr: Arc<dyn PhysicalExpr>,
        tag_exprs: Vec<Arc<dyn PhysicalExpr>>,
        time_exprs: Vec<Arc<dyn PhysicalExpr>>,
        field_exprs: Vec<(Arc<dyn PhysicalExpr>, Arc<str>)>,
    ) -> Self {
        let properties = Self::calculate_properties(&input, &schema);
        let metrics = ExecutionPlanMetricsSet::new();
        Self {
            input,
            schema,
            properties,
            metrics,
            measurement_expr,
            tag_exprs,
            time_exprs,
            field_exprs,
        }
    }

    fn calculate_properties(input: &Arc<dyn ExecutionPlan>, schema: &SchemaRef) -> PlanProperties {
        let input_properties = input.properties();
        let partitioning = input_properties.output_partitioning().clone();
        let execution_mode = input_properties.execution_mode();
        PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(schema)),
            partitioning,
            execution_mode,
        )
    }
}

impl ExecutionPlan for SeriesPivotExec {
    fn name(&self) -> &str {
        "SeriesPivotExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input = children.into_iter().next().unwrap();
        let properties = Self::calculate_properties(&input, &self.schema);
        Ok(Arc::new(Self {
            input,
            schema: Arc::clone(&self.schema),
            properties,
            metrics: self.metrics.clone(),
            measurement_expr: Arc::clone(&self.measurement_expr),
            tag_exprs: self.tag_exprs.clone(),
            time_exprs: self.time_exprs.clone(),
            field_exprs: self.field_exprs.clone(),
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, Arc::clone(&context))?;
        let measurement_expr = Arc::clone(&self.measurement_expr);
        let tag_exprs = self.tag_exprs.clone();
        let field_exprs = self
            .field_exprs
            .iter()
            .zip(&self.time_exprs)
            .map(|((field_expr, name), time_expr)| {
                FieldExpr::new(
                    Arc::clone(field_expr),
                    Arc::clone(time_expr),
                    Arc::clone(name),
                )
            })
            .collect();
        let metrics = BaselineMetrics::new(&self.metrics, partition);
        Ok(Box::pin(SeriesPivotStream::new(
            input,
            Arc::clone(&self.schema),
            measurement_expr,
            tag_exprs,
            field_exprs,
            context.memory_pool(),
            metrics,
        )))
    }
}

impl std::fmt::Debug for SeriesPivotExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.fmt_as(DisplayFormatType::Verbose, f)
    }
}

impl DisplayAs for SeriesPivotExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let tag_exprs = self
            .tag_exprs
            .iter()
            .map(|e| format!("{e}"))
            .collect::<Vec<_>>();
        let time_exprs = self
            .time_exprs
            .iter()
            .map(|e| format!("{e}"))
            .collect::<Vec<_>>();
        let field_exprs = self
            .field_exprs
            .iter()
            .map(|(e, key)| format!("{e} as {key}"))
            .collect::<Vec<_>>();
        write!(
            f,
            "SeriesPivotExec: measurement_expr={}, tag_exprs=[{}], time_exprs=({}), field_exprs=[{}]",
            self.measurement_expr,
            tag_exprs.join(", "),
            time_exprs.join(", "),
            field_exprs.join(", ")
        )
    }
}
