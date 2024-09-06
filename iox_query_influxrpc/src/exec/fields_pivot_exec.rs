use super::fields_pivot_stream::FieldsPivotStream;

use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning, PhysicalSortRequirement};
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use hashbrown::HashSet;
use schema::TIME_DATA_TYPE;
use snafu::Snafu;
use std::sync::Arc;

/// Error type for FieldsPivotExec.
#[derive(Debug, Snafu)]
enum Error {
    #[snafu(display("No inputs provided"))]
    NoInputs,

    #[snafu(display("Time column not found in input schema"))]
    TimeColumnNotFound,
}

pub(crate) struct FieldsPivotExec {
    schema: SchemaRef,

    input: Arc<dyn ExecutionPlan>,
    properties: PlanProperties,
    physical_sort_requirement: PhysicalSortRequirement,
    metrics: ExecutionPlanMetricsSet,
}

impl FieldsPivotExec {
    pub(crate) fn new(schema: &SchemaRef, input: Arc<dyn ExecutionPlan>) -> Result<Self> {
        let properties = Self::make_properties(schema, &input);
        let physical_sort_requirement = Self::make_physical_sort_requirement(&input)?;
        Ok(Self {
            schema: Arc::clone(schema),
            properties,
            input,
            physical_sort_requirement,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    fn make_properties(schema: &SchemaRef, input: &Arc<dyn ExecutionPlan>) -> PlanProperties {
        let equivalence_properties = EquivalenceProperties::new(Arc::clone(schema));
        let output_partitioning = Partitioning::UnknownPartitioning(1);
        PlanProperties::new(
            equivalence_properties,
            output_partitioning,
            input.execution_mode(),
        )
    }

    fn make_physical_sort_requirement(
        input: &Arc<dyn ExecutionPlan>,
    ) -> Result<PhysicalSortRequirement> {
        let col = input
            .schema()
            .fields()
            .iter()
            .enumerate()
            .find(|(_, f)| f.data_type() == &TIME_DATA_TYPE())
            .map(|(n, f)| Column::new(f.name(), n))
            .ok_or_else(|| DataFusionError::External(Box::new(Error::TimeColumnNotFound)))?;

        Ok(PhysicalSortRequirement::new(
            Arc::new(col),
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
        ))
    }
}

impl ExecutionPlan for FieldsPivotExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn static_name() -> &'static str {
        "FieldsPivotExec"
    }

    fn as_any(&self) -> &(dyn std::any::Any + 'static) {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input = children
            .into_iter()
            .next()
            .ok_or_else(|| DataFusionError::External(Box::new(Error::NoInputs)))?;
        let properties = Self::make_properties(&self.schema, &input);
        let physical_sort_requirement = Self::make_physical_sort_requirement(&input)?;
        Ok(Arc::new(Self {
            schema: Arc::clone(&self.schema),
            input,
            properties,
            physical_sort_requirement,
            metrics: self.metrics.clone(),
        }))
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, Arc::clone(&context))?;

        let memory_consumer = MemoryConsumer::new("FieldsPivotExec");
        let mut memory_reservation = memory_consumer.register(context.memory_pool());

        let fields = input
            .schema()
            .fields()
            .iter()
            .filter_map(|f| {
                if !matches!(f.data_type(), &DataType::Timestamp(TimeUnit::Nanosecond, _)) {
                    Some(f.name().clone())
                } else {
                    None
                }
            })
            .inspect(|s| memory_reservation.grow(std::mem::size_of_val(s)))
            .collect::<HashSet<String>>();

        memory_reservation.try_grow(std::mem::size_of::<HashSet<String>>())?;

        let stream = FieldsPivotStream::new(
            input,
            Arc::clone(&self.schema),
            fields,
            memory_reservation,
            BaselineMetrics::new(&self.metrics, partition),
        );

        Ok(Box::pin(stream))
    }

    fn required_input_distribution(&self) -> Vec<datafusion::physical_plan::Distribution> {
        vec![datafusion::physical_plan::Distribution::SinglePartition]
    }

    fn required_input_ordering(&self) -> Vec<Option<Vec<PhysicalSortRequirement>>> {
        vec![Some(vec![self.physical_sort_requirement.clone()])]
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

impl std::fmt::Debug for FieldsPivotExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.fmt_as(DisplayFormatType::Verbose, f)
    }
}

impl DisplayAs for FieldsPivotExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FieldsPivotExec")
    }
}
