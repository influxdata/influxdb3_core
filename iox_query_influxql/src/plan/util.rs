use crate::error;
use arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::scalar::ScalarStructBuilder;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchemaRef, Result};
use datafusion::logical_expr::utils::expr_as_column_expr;
use datafusion::logical_expr::{lit, Expr, ExprSchemable, LogicalPlan, Operator};
use datafusion::scalar::ScalarValue;
use influxdb_influxql_parser::expression::BinaryOperator;
use influxdb_influxql_parser::literal::Number;
use influxdb_influxql_parser::string::Regex;
use query_functions::clean_non_meta_escapes;
use query_functions::coalesce_struct::coalesce_struct;
use schema::InfluxColumnType;
use std::sync::Arc;

use super::ir::{DataSourceSchema, Field};

pub(in crate::plan) fn binary_operator_to_df_operator(op: BinaryOperator) -> Operator {
    match op {
        BinaryOperator::Add => Operator::Plus,
        BinaryOperator::Sub => Operator::Minus,
        BinaryOperator::Mul => Operator::Multiply,
        BinaryOperator::Div => Operator::Divide,
        BinaryOperator::Mod => Operator::Modulo,
        BinaryOperator::BitwiseAnd => Operator::BitwiseAnd,
        BinaryOperator::BitwiseOr => Operator::BitwiseOr,
        BinaryOperator::BitwiseXor => Operator::BitwiseXor,
    }
}

/// Container for the DataFusion schema as well as
/// info on which columns are tags.
pub(in crate::plan) struct IQLSchema<'a> {
    pub(in crate::plan) df_schema: DFSchemaRef,
    tag_info: TagInfo<'a>,
}

impl<'a> IQLSchema<'a> {
    /// Create a new IQLSchema from a [`DataSourceSchema`] from the
    /// FROM clause of a query or subquery.
    pub(in crate::plan) fn new_from_ds_schema(
        df_schema: &DFSchemaRef,
        ds_schema: DataSourceSchema<'a>,
    ) -> Result<Self> {
        Ok(Self {
            df_schema: Arc::clone(df_schema),
            tag_info: TagInfo::DataSourceSchema(ds_schema),
        })
    }

    /// Create a new IQLSchema from a list of [`Field`]s on the SELECT list
    /// of a subquery.
    pub(in crate::plan) fn new_from_fields(
        df_schema: &DFSchemaRef,
        fields: &'a [Field],
    ) -> Result<Self> {
        Ok(Self {
            df_schema: Arc::clone(df_schema),
            tag_info: TagInfo::FieldList(fields),
        })
    }

    /// Returns `true` if the schema contains a tag column with the specified name.
    pub(crate) fn is_tag_field(&self, name: &str) -> bool {
        match self.tag_info {
            TagInfo::DataSourceSchema(ref ds_schema) => ds_schema.is_tag_field(name),
            TagInfo::FieldList(fields) => fields
                .iter()
                .any(|f| f.name == name && f.data_type == Some(InfluxColumnType::Tag)),
        }
    }

    /// Returns `true` if the schema contains a tag column with the specified name.
    /// If the underlying data source is a subquery, it will apply any aliases in the
    /// projection that represents the SELECT list.
    pub(crate) fn is_projected_tag_field(&self, name: &str) -> bool {
        match self.tag_info {
            TagInfo::DataSourceSchema(ref ds_schema) => ds_schema.is_projected_tag_field(name),
            _ => self.is_tag_field(name),
        }
    }
}

pub(in crate::plan) enum TagInfo<'a> {
    DataSourceSchema(DataSourceSchema<'a>),
    FieldList(&'a [Field]),
}

/// Sanitize an InfluxQL regular expression and create a compiled [`regex::Regex`].
pub(crate) fn parse_regex(re: &Regex) -> Result<regex::Regex> {
    let pattern = clean_non_meta_escapes(re.as_str());
    regex::Regex::new(&pattern)
        .map_err(|e| error::map::query(format!("invalid regular expression '{re}': {e}")))
}

/// Returns `n` as a scalar value of the specified `data_type`.
fn number_to_scalar(n: &Number, data_type: &DataType) -> Result<ScalarValue> {
    Ok(match (n, data_type) {
        (Number::Integer(v), DataType::Int64) => ScalarValue::from(*v),
        (Number::Integer(v), DataType::Float64) => ScalarValue::from(*v as f64),
        (Number::Integer(v), DataType::UInt64) => ScalarValue::from(*v as u64),
        (Number::Integer(v), DataType::Timestamp(TimeUnit::Nanosecond, tz)) => {
            ScalarValue::TimestampNanosecond(Some(*v), tz.clone())
        }
        (Number::Float(v), DataType::Int64) => ScalarValue::from(*v as i64),
        (Number::Float(v), DataType::Float64) => ScalarValue::from(*v),
        (Number::Float(v), DataType::UInt64) => ScalarValue::from(*v as u64),
        (Number::Float(v), DataType::Timestamp(TimeUnit::Nanosecond, tz)) => {
            ScalarValue::TimestampNanosecond(Some(*v as i64), tz.clone())
        }
        (n, DataType::Struct(fields)) => {
            let mut builder = ScalarStructBuilder::new();
            for field in fields {
                let value = number_to_scalar(n, field.data_type())?;
                builder = builder.with_scalar(field, value);
            }
            builder.build()?
        }
        (_, DataType::Null) => ScalarValue::Null,
        (n, data_type) => {
            // The only output data types expected are Int64, Float64 or UInt64
            return error::internal(format!("no conversion from {n} to {data_type}"));
        }
    })
}

/// Rebuilds an `Expr` as a projection on top of a collection of `Expr`'s.
///
/// For example, the expression `a + b < 1` would require, as input, the 2
/// individual columns, `a` and `b`. But, if the base expressions already
/// contain the `a + b` result, then that may be used in lieu of the `a` and
/// `b` columns.
///
/// This is useful in the context of a query like:
///
/// SELECT a + b < 1 ... GROUP BY a + b
///
/// where post-aggregation, `a + b` need not be a projection against the
/// individual columns `a` and `b`, but rather it is a projection against the
/// `a + b` found in the GROUP BY.
///
/// `fill_if_null` will be used to coalesce any expressions from `NULL`.
/// This is used with the `FILL(<value>)` strategy.
pub(crate) fn rebase_expr(
    expr: &Expr,
    base_exprs: &[Expr],
    fill_if_null: &Option<Number>,
    plan: &LogicalPlan,
) -> Result<Transformed<Expr>> {
    if let Some(value) = fill_if_null {
        expr.clone().transform_up(&|nested_expr| {
            Ok(if base_exprs.contains(&nested_expr) {
                let col_expr = expr_as_column_expr(&nested_expr, plan)?;
                let data_type = col_expr.get_type(plan.schema())?;
                Transformed::yes(coalesce_struct(vec![
                    col_expr,
                    lit(number_to_scalar(value, &data_type)?),
                ]))
            } else {
                Transformed::no(nested_expr)
            })
        })
    } else {
        expr.clone().transform_up(&|nested_expr| {
            Ok(if base_exprs.contains(&nested_expr) {
                Transformed::yes(expr_as_column_expr(&nested_expr, plan)?)
            } else {
                Transformed::no(nested_expr)
            })
        })
    }
}

pub(crate) fn contains_expr(expr: &Expr, needle: &Expr) -> bool {
    let mut found = false;
    expr.apply(|expr| {
        if expr == needle {
            found = true;
            Ok(TreeNodeRecursion::Stop)
        } else {
            Ok(TreeNodeRecursion::Continue)
        }
    })
    .expect("cannot fail");
    found
}

/// Search the provided `Expr`'s, and all of their nested `Expr`, for any that
/// pass the provided test. The returned `Expr`'s are deduplicated and returned
/// in order of appearance (depth first).
///
/// # NOTE
///
/// Copied from DataFusion
pub(crate) fn find_exprs_in_exprs<F>(exprs: &[Expr], test_fn: &F) -> Vec<Expr>
where
    F: Fn(&Expr) -> bool,
{
    exprs
        .iter()
        .flat_map(|expr| find_exprs_in_expr(expr, test_fn))
        .fold(vec![], |mut acc, expr| {
            if !acc.contains(&expr) {
                acc.push(expr)
            }
            acc
        })
}

/// Search an `Expr`, and all of its nested `Expr`'s, for any that pass the
/// provided test. The returned `Expr`'s are deduplicated and returned in order
/// of appearance (depth first).
///
/// # NOTE
///
/// Copied from DataFusion
fn find_exprs_in_expr<F>(expr: &Expr, test_fn: &F) -> Vec<Expr>
where
    F: Fn(&Expr) -> bool,
{
    let mut exprs = vec![];
    expr.apply(|expr| {
        if test_fn(expr) {
            if !(exprs.contains(expr)) {
                exprs.push(expr.clone())
            }
            // stop recursing down this expr once we find a match
            return Ok(TreeNodeRecursion::Jump);
        }

        Ok(TreeNodeRecursion::Continue)
    })
    // pre_visit always returns OK, so this will always too
    .expect("no way to return error during recursion");
    exprs
}
