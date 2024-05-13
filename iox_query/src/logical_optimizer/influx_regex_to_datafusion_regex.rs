use datafusion::common::tree_node::Transformed;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::{
    common::{tree_node::TreeNodeRewriter, DFSchema},
    error::DataFusionError,
    logical_expr::{expr_rewriter::rewrite_preserving_name, LogicalPlan, Operator},
    optimizer::{OptimizerConfig, OptimizerRule},
    prelude::{binary_expr, lit, Expr},
    scalar::ScalarValue,
};
use query_functions::{clean_non_meta_escapes, REGEX_MATCH_UDF_NAME, REGEX_NOT_MATCH_UDF_NAME};

/// Replaces InfluxDB-specific regex operator with DataFusion regex operator.
///
/// InfluxDB has a special regex operator that is especially used by Flux/InfluxQL and that excepts certain escape
/// sequences that are normal Rust regex crate does NOT support. If the pattern is already known at planning time (i.e.
/// it is a constant), then we can clean the escape sequences and just use the ordinary DataFusion regex operator. This
/// is desired because the ordinary DataFusion regex operator can be optimized further (e.g. to cheaper `LIKE` expressions).
#[derive(Debug, Clone)]
pub struct InfluxRegexToDataFusionRegex {}

impl InfluxRegexToDataFusionRegex {
    /// Create new optimizer rule.
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for InfluxRegexToDataFusionRegex {
    fn name(&self) -> &str {
        "influx_regex_to_datafusion_regex"
    }

    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> datafusion::error::Result<Option<LogicalPlan>> {
        optimize(plan).map(Some)
    }
}

fn optimize(plan: &LogicalPlan) -> Result<LogicalPlan, DataFusionError> {
    let new_inputs = plan
        .inputs()
        .iter()
        .map(|input| optimize(input))
        .collect::<Result<Vec<_>, DataFusionError>>()?;

    let mut schema =
        new_inputs
            .iter()
            .map(|input| input.schema())
            .fold(DFSchema::empty(), |mut lhs, rhs| {
                lhs.merge(rhs);
                lhs
            });

    schema.merge(plan.schema());

    let mut expr_rewriter = InfluxRegexToDataFusionRegex {};

    let new_exprs = plan
        .expressions()
        .into_iter()
        .map(|expr| rewrite_preserving_name(expr, &mut expr_rewriter))
        .collect::<Result<Vec<_>, DataFusionError>>()?;
    plan.with_new_exprs(new_exprs, new_inputs)
}

impl TreeNodeRewriter for InfluxRegexToDataFusionRegex {
    type Node = Expr;

    fn f_up(&mut self, expr: Expr) -> Result<Transformed<Expr>, DataFusionError> {
        match expr {
            Expr::ScalarFunction(ScalarFunction { func, mut args }) => {
                let name = func.name();
                if (args.len() == 2)
                    && ((name == REGEX_MATCH_UDF_NAME) || (name == REGEX_NOT_MATCH_UDF_NAME))
                {
                    if let Expr::Literal(ScalarValue::Utf8(Some(s))) = &args[1] {
                        let s = clean_non_meta_escapes(s);
                        let op = match name {
                            REGEX_MATCH_UDF_NAME => Operator::RegexMatch,
                            REGEX_NOT_MATCH_UDF_NAME => Operator::RegexNotMatch,
                            _ => unreachable!(),
                        };
                        return Ok(Transformed::yes(binary_expr(args.remove(0), op, lit(s))));
                    }
                }

                Ok(Transformed::yes(Expr::ScalarFunction(ScalarFunction {
                    func,
                    args,
                })))
            }
            _ => Ok(Transformed::no(expr)),
        }
    }
}
