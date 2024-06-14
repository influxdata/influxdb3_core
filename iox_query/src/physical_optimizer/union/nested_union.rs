use std::sync::Arc;

use datafusion::{
    common::tree_node::{Transformed, TreeNode},
    config::ConfigOptions,
    error::Result,
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{union::UnionExec, ExecutionPlan},
};

/// Optimizer that replaces nested [`UnionExec`]s with a single level.
///
/// # Example
/// ```yaml
/// ---
/// UnionExec:
///  - UnionExec:
///      - SomeExec1
///      - SomeExec2
///  - SomeExec3
///
/// ---
/// UnionExec:
///  - SomeExec1
///  - SomeExec2
///  - SomeExec3
/// ```
#[derive(Debug, Default)]
pub struct NestedUnion;

impl PhysicalOptimizerRule for NestedUnion {
    #[allow(clippy::map_clone)]
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|plan| {
            let plan_any = plan.as_any();

            if let Some(union_exec) = plan_any.downcast_ref::<UnionExec>() {
                let children = union_exec.children();

                let mut children_new: Vec<Arc<dyn ExecutionPlan>> =
                    Vec::with_capacity(children.len());
                let mut found_union = false;
                for child in children {
                    if let Some(union_child) = child.as_any().downcast_ref::<UnionExec>() {
                        found_union = true;
                        let mut union_children = union_child
                            .children()
                            .iter()
                            .map(|c| Arc::clone(c))
                            .collect::<Vec<Arc<dyn ExecutionPlan>>>();
                        children_new.append(&mut union_children);
                    } else {
                        children_new.push(Arc::clone(child))
                    }
                }

                if found_union {
                    return Ok(Transformed::yes(Arc::new(UnionExec::new(children_new))));
                }
            }

            Ok(Transformed::no(plan))
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "nested_union"
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
    fn test_union_not_nested() {
        let plan = Arc::new(UnionExec::new(vec![other_node()]));
        let opt = NestedUnion;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " UnionExec"
          - "   EmptyExec"
        output:
          Ok:
            - " UnionExec"
            - "   EmptyExec"
        "###
        );
    }

    #[test]
    fn test_union_nested() {
        let plan = Arc::new(UnionExec::new(vec![
            Arc::new(UnionExec::new(vec![other_node(), other_node()])),
            other_node(),
        ]));
        let opt = NestedUnion;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " UnionExec"
          - "   UnionExec"
          - "     EmptyExec"
          - "     EmptyExec"
          - "   EmptyExec"
        output:
          Ok:
            - " UnionExec"
            - "   EmptyExec"
            - "   EmptyExec"
            - "   EmptyExec"
        "###
        );
    }

    #[test]
    fn test_union_deeply_nested() {
        let plan = Arc::new(UnionExec::new(vec![
            Arc::new(UnionExec::new(vec![
                other_node(),
                Arc::new(UnionExec::new(vec![other_node()])),
            ])),
            other_node(),
        ]));
        let opt = NestedUnion;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " UnionExec"
          - "   UnionExec"
          - "     EmptyExec"
          - "     UnionExec"
          - "       EmptyExec"
          - "   EmptyExec"
        output:
          Ok:
            - " UnionExec"
            - "   EmptyExec"
            - "   EmptyExec"
            - "   EmptyExec"
        "###
        );
    }

    #[test]
    fn test_other_node() {
        let plan = other_node();
        let opt = NestedUnion;
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
