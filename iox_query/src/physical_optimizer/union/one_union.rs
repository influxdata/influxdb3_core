use std::sync::Arc;

use datafusion::{
    common::tree_node::{Transformed, TreeNode},
    config::ConfigOptions,
    error::Result,
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{union::UnionExec, ExecutionPlan},
};

/// Optimizer that replaces [`UnionExec`] with a single child node w/ the child note itself.
///
/// # Example
/// ```yaml
/// ---
/// UnionExec:
///  - SomeExec1
///
/// ---
/// SomeExec1
/// ```
#[derive(Debug, Default)]
pub struct OneUnion;

impl PhysicalOptimizerRule for OneUnion {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|plan| {
            let plan_any = plan.as_any();

            if let Some(union_exec) = plan_any.downcast_ref::<UnionExec>() {
                let mut children = union_exec.children();
                if children.len() == 1 {
                    return Ok(Transformed::yes(Arc::clone(children.remove(0))));
                }
            }

            Ok(Transformed::no(plan))
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "one_union"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::physical_plan::empty::EmptyExec;

    use crate::physical_optimizer::test_util::OptimizationTest;

    use super::*;

    #[test]
    #[should_panic(expected = "index out of bounds")]
    fn test_union_empty() {
        // empty UnionExecs cannot be created in the first place
        UnionExec::new(vec![]);
    }

    #[test]
    fn test_union_one() {
        let plan = Arc::new(UnionExec::new(vec![other_node()]));
        let opt = OneUnion;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " UnionExec"
          - "   EmptyExec"
        output:
          Ok:
            - " EmptyExec"
        "###
        );
    }

    #[test]
    fn test_union_two() {
        let plan = Arc::new(UnionExec::new(vec![other_node(), other_node()]));
        let opt = OneUnion;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " UnionExec"
          - "   EmptyExec"
          - "   EmptyExec"
        output:
          Ok:
            - " UnionExec"
            - "   EmptyExec"
            - "   EmptyExec"
        "###
        );
    }

    #[test]
    fn test_other_node() {
        let plan = other_node();
        let opt = OneUnion;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " EmptyExec"
        output:
          Ok:
            - " EmptyExec"
        "###
        );
    }

    fn other_node() -> Arc<dyn ExecutionPlan> {
        Arc::new(EmptyExec::new(schema()))
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("c", DataType::UInt32, false)]))
    }
}
