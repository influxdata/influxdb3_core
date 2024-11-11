use crate::exec::SeriesPivotExec;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::Result;
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

/// Optimizer rule that pushes the SeriesPivotExec operation to be as
/// early in the process as possible. The results of the SeriesPivotExec
/// have to be sorted following this operation and by pushing it down
/// there are fewer rows that need to be sorted.
///
/// This transforms plans like:
///
/// ```text
///   SeriesPivotExec
///     UnionExec
/// ```
///
/// into:
///
/// ```text
///   UnionExec
///       SeriesPivotExec
/// ```
#[derive(Debug, Default)]
pub(crate) struct SeriesPivotPushdown {}

impl PhysicalOptimizerRule for SeriesPivotPushdown {
    fn name(&self) -> &str {
        "series_pivot_push_down"
    }

    fn schema_check(&self) -> bool {
        true
    }

    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let transformed = plan.transform_down(|plan| {
            if let Some(series_pivot) = plan.as_any().downcast_ref::<SeriesPivotExec>() {
                let child = series_pivot.input().as_any();
                if let Some(union) = child.downcast_ref::<UnionExec>() {
                    let children = union
                        .children()
                        .into_iter()
                        .map(|child| {
                            Arc::new(SeriesPivotExec::new(
                                Arc::clone(child),
                                series_pivot.schema(),
                                Arc::clone(series_pivot.measurement_expr()),
                                series_pivot.tag_exprs().clone(),
                                series_pivot.time_exprs().clone(),
                                series_pivot.field_exprs().clone(),
                            )) as Arc<dyn ExecutionPlan>
                        })
                        .collect();
                    Ok(Transformed::yes(Arc::new(UnionExec::new(children))))
                } else if child.is::<EmptyExec>() {
                    // An empty input will result in empty output, so we can elide the SeriesPivotExec.
                    let schema = series_pivot.schema();
                    Ok(Transformed::yes(Arc::new(EmptyExec::new(schema))))
                } else {
                    Ok(Transformed::no(plan))
                }
            } else {
                Ok(Transformed::no(plan))
            }
        })?;
        Ok(transformed.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::physical_expr::expressions::{Column, Literal};
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::values::ValuesExec;
    use datafusion::physical_plan::{displayable, PhysicalExpr};
    use datafusion::scalar::ScalarValue;
    use insta::assert_snapshot;
    use schema::{TIME_DATA_TIMEZONE, TIME_DATA_TYPE};

    #[test]
    fn seriespivot_pushdown() {
        let (input_plan, output_schema) = union_plan();
        let optimizer = SeriesPivotPushdown::default();
        let options = ConfigOptions::default();
        let output_plan = optimizer
            .optimize(Arc::clone(&input_plan), &options)
            .unwrap();

        let compare = format!(
            "input:\n{}\noutput:\n{}",
            displayable(input_plan.as_ref()).indent(false),
            displayable(output_plan.as_ref()).indent(false),
        );

        assert_snapshot!(compare, @r"
        input:
        SeriesPivotExec: measurement_expr=test, tag_exprs=[tag1@1, tag2@2], time_exprs=(time@0), field_exprs=[field1@3 as field1, field2@4 as field2]
          UnionExec
            ValuesExec
            EmptyExec

        output:
        UnionExec
          SeriesPivotExec: measurement_expr=test, tag_exprs=[tag1@1, tag2@2], time_exprs=(time@0), field_exprs=[field1@3 as field1, field2@4 as field2]
            ValuesExec
          EmptyExec
        ");
        assert!(output_plan.schema() == output_schema);
    }

    fn union_plan() -> (Arc<dyn ExecutionPlan>, SchemaRef) {
        let input_schema = Arc::new(Schema::new([
            Arc::new(Field::new("time", TIME_DATA_TYPE(), false)),
            Arc::new(Field::new(
                "tag1",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            )),
            Arc::new(Field::new(
                "tag2",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            )),
            Arc::new(Field::new("field1", DataType::Float64, true)),
            Arc::new(Field::new("field2", DataType::Float64, true)),
        ]));
        let output_schema = Arc::new(Schema::new([
            Arc::new(Field::new(
                "_measurement",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                false,
            )),
            Arc::new(Field::new(
                "tag1",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            )),
            Arc::new(Field::new(
                "tag2",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            )),
            Arc::new(Field::new(
                "_field",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                false,
            )),
            Arc::new(Field::new("_time", TIME_DATA_TYPE(), false)),
            Arc::new(Field::new("_value", DataType::Float64, true)),
        ]));

        (
            Arc::new(SeriesPivotExec::new(
                Arc::new(UnionExec::new(vec![
                    Arc::new(
                        ValuesExec::try_new(
                            Arc::clone(&input_schema),
                            vec![vec![
                                Arc::new(Literal::new(ScalarValue::TimestampNanosecond(
                                    Some(0),
                                    TIME_DATA_TIMEZONE(),
                                ))),
                                Arc::new(Literal::new(ScalarValue::Dictionary(
                                    Box::new(DataType::Int32),
                                    Box::new(ScalarValue::Utf8(Some(String::from("test")))),
                                ))),
                                Arc::new(Literal::new(ScalarValue::Dictionary(
                                    Box::new(DataType::Int32),
                                    Box::new(ScalarValue::Utf8(None)),
                                ))),
                                Arc::new(Literal::new(ScalarValue::Float64(Some(1.0)))),
                                Arc::new(Literal::new(ScalarValue::Float64(None))),
                            ]],
                        )
                        .unwrap(),
                    ),
                    Arc::new(EmptyExec::new(Arc::clone(&input_schema))),
                ])),
                Arc::clone(&output_schema),
                Arc::new(Literal::new(ScalarValue::Utf8(Some(String::from("test")))))
                    as Arc<dyn PhysicalExpr>,
                vec![
                    Arc::new(Column::new("tag1", 1)),
                    Arc::new(Column::new("tag2", 2)),
                ],
                vec![Arc::new(Column::new("time", 0))],
                vec![
                    (Arc::new(Column::new("field1", 3)), Arc::from("field1")),
                    (Arc::new(Column::new("field2", 4)), Arc::from("field2")),
                ],
            )),
            output_schema,
        )
    }
}
