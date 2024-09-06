mod column_rewrite;
mod field_rewrite;
mod measurement_rewrite;
mod rewrite;
mod value_rewrite;

use crate::rpc_predicate::column_rewrite::missing_tag_to_null;
use crate::Predicate;

use async_trait::async_trait;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::ToDFSchema;
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::context::{ExecutionProps, SessionContext};
use datafusion::optimizer::simplify_expressions::{ExprSimplifier, SimplifyContext};
use datafusion::prelude::{lit, Expr};
use futures::{StreamExt, TryStreamExt};
use observability_deps::tracing::{debug, trace};
use schema::Schema;
use std::collections::BTreeSet;
use std::sync::Arc;

use self::field_rewrite::FieldProjectionRewriter;
use self::measurement_rewrite::rewrite_measurement_references;
use self::value_rewrite::rewrite_field_value_references;

pub use self::rewrite::{iox_expr_rewrite, simplify_predicate};

/// Any column references to this name are rewritten to be
/// the actual table name by the Influx gRPC planner.
///
/// This is required to support predicates like
/// `_measurement = "foo" OR tag1 = "bar"`
///
/// The plan for each table will have the value of `_measurement`
/// filled in with a literal for the respective name of that field
pub const MEASUREMENT_COLUMN_NAME: &str = "_measurement";

/// A reference to a field's name which is used to represent column
/// projections in influx RPC predicates.
///
/// For example, a predicate like
/// ```text
/// _field = temperature
/// ```
///
/// Means to select only the (field) column named "temperature"
///
/// Any equality expressions using this column name are removed and
/// replaced with projections on the specified column.
pub const FIELD_COLUMN_NAME: &str = "_field";

/// Any column references to this name are rewritten to be a disjunctive set of
/// expressions to all field columns for the table schema.
///
/// This is required to support predicates like
/// `_value` = 1.77
///
/// The plan for each table will have expression containing `_value` rewritten
/// into multiple expressions (one for each field column).
pub const VALUE_COLUMN_NAME: &str = "_value";

/// Special group key for `read_group` requests.
///
/// Treat these specially and use `""` as a placeholder value (instead of a real column) to mirror what TSM does.
/// See <https://github.com/influxdata/influxdb_iox/issues/2693#issuecomment-947695442>
/// for more details.
///
/// See also [`GROUP_KEY_SPECIAL_STOP`].
pub const GROUP_KEY_SPECIAL_START: &str = "_start";

/// Special group key for `read_group` requests.
///
/// Treat these specially and use `""` as a placeholder value (instead of a real column) to mirror what TSM does.
/// See <https://github.com/influxdata/influxdb_iox/issues/2693#issuecomment-947695442>
/// for more details.
///
/// See also [`GROUP_KEY_SPECIAL_START`].
pub const GROUP_KEY_SPECIAL_STOP: &str = "_stop";

/// [`InfluxRpcPredicate`] implements the semantics of the InfluxDB
/// Storage gRPC and handles mapping details such as `_field` and
/// `_measurement` predicates into the corresponding IOx structures.
#[derive(Debug, Clone, Default)]
pub struct InfluxRpcPredicate {
    /// Optional table restriction. If present, restricts the results
    /// to only tables whose names are in `table_names`
    table_names: Option<BTreeSet<String>>,

    /// The inner predicate
    inner: Predicate,
}

impl InfluxRpcPredicate {
    /// Create a new [`InfluxRpcPredicate`]
    pub fn new(table_names: Option<BTreeSet<String>>, predicate: Predicate) -> Self {
        Self {
            table_names,
            inner: predicate,
        }
    }

    /// Create a new [`InfluxRpcPredicate`] for the given table
    pub fn new_table(table: impl Into<String>, predicate: Predicate) -> Self {
        Self::new(Some(std::iter::once(table.into()).collect()), predicate)
    }

    /// Removes the timestamp range from this predicate, if the range
    /// is for the entire min/max valid range.
    ///
    /// This is used in certain cases to retain compatibility with the
    /// existing storage engine which uses the max range to mean "all
    /// the data for all time"
    pub fn clear_timestamp_if_max_range(self) -> Self {
        Self {
            inner: self.inner.with_clear_timestamp_if_max_range(),
            ..self
        }
    }

    /// Since InfluxRPC predicates may have references to
    /// `_measurement` columns or other table / table schema specific
    /// restrictions, a predicate must specialized for each table
    /// prior to being applied by IOx to a specific table.
    ///
    /// See [`normalize_predicate`] for more details on the
    /// transformations applied.
    ///
    /// Returns a list of (TableName, [`Predicate`])
    pub async fn table_predicates(
        &self,
        session_ctx: &SessionContext,
        table_info: &dyn QueryNamespaceMeta,
        concurrent_jobs: usize,
    ) -> DataFusionResult<Vec<(Arc<str>, Option<Schema>, Predicate)>> {
        let table_names = match &self.table_names {
            Some(table_names) => itertools::Either::Left(table_names.iter().cloned()),
            None => itertools::Either::Right(table_info.table_names().into_iter()),
        };

        futures::stream::iter(table_names)
            .map(|table| async move {
                let schema = table_info.table_schema(&table).await;
                let predicate = match &schema {
                    Some(schema) => normalize_predicate(session_ctx, &table, schema, &self.inner)?,
                    None => {
                        // if we don't know about this table, we can't
                        // do any predicate specialization. This can
                        // happen if there is a request for
                        // "measurement fields" for a non existent
                        // measurement, for example
                        self.inner.clone()
                    }
                };

                Ok((Arc::from(table), schema, predicate))
            })
            .buffered(concurrent_jobs)
            .try_collect()
            .await
    }

    /// Returns the table names this predicate is restricted to if any
    pub fn table_names(&self) -> Option<&BTreeSet<String>> {
        self.table_names.as_ref()
    }

    /// Returns true if ths predicate evaluates to true for all rows
    pub fn is_empty(&self) -> bool {
        self.table_names.is_none() && self.inner.is_empty()
    }
}

/// Information required to normalize predicates
#[async_trait]
pub trait QueryNamespaceMeta: Send + Sync {
    /// Returns a list of table names in this namespace
    fn table_names(&self) -> Vec<String>;

    /// Schema for a specific table if the table exists.
    async fn table_schema(&self, table_name: &str) -> Option<Schema>;
}

/// Predicate that has been "specialized" / normalized for a
/// particular table. Specifically:
///
/// * all references to the [MEASUREMENT_COLUMN_NAME] column in any `Exprs` are rewritten with the
///   actual table name
/// * any expression on the [VALUE_COLUMN_NAME] column is rewritten to be applied across all field
///   columns.
/// * any expression on the [FIELD_COLUMN_NAME] is rewritten to be applied as a projection to
///   specific columns.
///
/// For example if the original predicate was
/// ```text
/// _measurement = "some_table"
/// ```
///
/// When evaluated on table "cpu" then the predicate is rewritten to
/// ```text
/// "cpu" = "some_table"
/// ```
///
/// if the original predicate contained
/// ```text
/// _value > 34.2
/// ```
///
/// When evaluated on table "cpu" then the expression is rewritten as a
/// collection of disjunctive expressions against all field columns
/// ```text
/// ("field1" > 34.2 OR "field2" > 34.2 OR "fieldn" > 34.2)
/// ```
fn normalize_predicate(
    session_ctx: &SessionContext,
    table_name: &str,
    schema: &Schema,
    predicate: &Predicate,
) -> DataFusionResult<Predicate> {
    let mut predicate = predicate.clone();

    let mut field_projections = FieldProjectionRewriter::new(schema.clone());

    let mut field_value_exprs = vec![];

    let props = ExecutionProps::new();
    let df_schema = schema.as_arrow().to_dfschema_ref()?;
    let simplify_context = SimplifyContext::new(&props).with_schema(Arc::clone(&df_schema));
    let simplifier = ExprSimplifier::new(simplify_context);

    predicate.exprs = predicate
        .exprs
        .into_iter()
        .map(|e| {
            debug!(?e, "rewriting expr");

            let e = e
                .transform(&|e| rewrite_measurement_references(table_name, e))
                .map(|e| log_rewrite(e, "rewrite_measurement_references"))
                // Rewrite any references to `_value = some_value` to literal true values.
                // Keeps track of these expressions, which can then be used to
                // augment field projections with conditions using `CASE` statements.
                .and_then(|e| rewrite_field_value_references(&mut field_value_exprs, e.data))
                .map(|e| log_rewrite(e, "rewrite_field_value_references"))
                // Rewrite any references to `_field` with a literal
                // and keep track of referenced field names to add to
                // the field column projection set.
                .and_then(|e| field_projections.rewrite_field_exprs(e.data))
                .map(|e| log_rewrite(e, "field_projections"))
                // Any column references that exist in the RPC predicate must exist
                // in the table's schema as tags. Replace any column references that
                // do not exist, or that are not tags, with NULL.
                // Field values always use `_value` as a name and are handled above.
                .and_then(|e| e.data.transform(&|e| missing_tag_to_null(schema, e)))
                .map(|e| log_rewrite(e, "missing_columums"))
                // apply IOx specific rewrites (that unlock other simplifications)
                .and_then(|e| rewrite::iox_expr_rewrite(e.data))
                .map(|e| log_rewrite(e, "rewrite"))
                // apply type_coercing so datafuson simplification can deal with this
                .and_then(|e| simplifier.coerce(e.data, &df_schema).map(Transformed::yes))
                .map(|e| log_rewrite(e, "coerce_expr"))
                // Call DataFusion simplification logic
                .and_then(|e| simplifier.simplify(e.data).map(Transformed::yes))
                .map(|e| log_rewrite(e, "simplify_expr"))
                .and_then(|e| rewrite::simplify_predicate(e.data))
                .map(|e| log_rewrite(e, "simplify_predicate"));

            debug!(?e, "rewritten expr");
            e
        })
        // Filter out literal true so is_empty works correctly
        .filter_map(|f| match f {
            Err(e) => Some(Err(e)),
            Ok(expr) if expr.data != lit(true) => Some(Ok(expr.data)),
            _ => None,
        })
        .collect::<DataFusionResult<Vec<_>>>()?;

    // Store any field value (`_value`) expressions on the `Predicate`.
    predicate.value_expr = field_value_exprs;

    // save any field projections
    field_projections.add_to_predicate(session_ctx, predicate)
}

fn log_rewrite(expr: Transformed<Expr>, description: &str) -> Transformed<Expr> {
    trace!(expr=?expr.data, transformed=expr.transformed, %description, "After rewrite");
    expr
}

#[cfg(test)]
mod tests {
    use crate::Predicate;

    use super::*;
    use arrow::datatypes::DataType;
    use datafusion::{
        execution::context::SessionContext,
        prelude::{col, lit},
        scalar::ScalarValue,
    };
    use datafusion_util::lit_dict;
    use test_helpers::assert_contains;

    #[test]
    fn test_normalize_predicate_coerced() {
        let predicate = normalize_predicate(
            &SessionContext::new(),
            "table",
            &schema(),
            &Predicate::new().with_expr(col("t1").eq(lit("f1"))),
        )
        .unwrap();

        let expected = Predicate::new().with_expr(col("t1").eq(lit_dict("f1")));

        assert_eq!(predicate, expected);
    }

    #[test]
    fn test_normalize_predicate_field_rewrite() {
        let predicate = normalize_predicate(
            &SessionContext::new(),
            "table",
            &schema(),
            &Predicate::new().with_expr(col("_field").eq(lit("f1"))),
        )
        .unwrap();

        let expected = Predicate::new().with_field_columns(vec!["f1"]).unwrap();

        assert_eq!(predicate, expected);
    }

    #[test]
    fn test_normalize_predicate_field_rewrite_multi_field() {
        let predicate = normalize_predicate(
            &SessionContext::new(),
            "table",
            &schema(),
            &Predicate::new()
                .with_expr(col("_field").eq(lit("f1")).or(col("_field").eq(lit("f2")))),
        )
        .unwrap();

        let expected = Predicate::new()
            .with_field_columns(vec!["f1", "f2"])
            .unwrap();

        assert_eq!(predicate, expected);
    }

    #[test]
    fn test_normalize_predicate_field_non_existent_field() {
        let predicate = normalize_predicate(
            &SessionContext::new(),
            "table",
            &schema(),
            &Predicate::new().with_expr(col("_field").eq(lit("not_a_field"))),
        )
        .unwrap();

        let expected = Predicate::new()
            .with_field_columns(vec![] as Vec<String>)
            .unwrap();
        assert_eq!(&expected.field_columns, &Some(BTreeSet::new()));
        assert_eq!(predicate, expected);
    }

    #[test]
    fn test_normalize_predicate_field_non_tag() {
        // should treat
        let predicate = normalize_predicate(
            &SessionContext::new(),
            "table",
            &schema(),
            &Predicate::new().with_expr(col("not_a_tag").eq(lit("blarg"))),
        )
        .unwrap();

        let expected = Predicate::new().with_expr(lit(ScalarValue::Boolean(None)));
        assert_eq!(predicate, expected);
    }

    #[test]
    fn test_normalize_predicate_field_rewrite_multi_field_unsupported() {
        let err = normalize_predicate(
            &SessionContext::new(),
            "table",
            &schema(),
            &Predicate::new()
                // predicate refers to a column other than _field which is not supported
                .with_expr(
                    col("t1")
                        .eq(lit("my_awesome_tag_value"))
                        .or(col("_field").eq(lit("f2"))),
                ),
        )
        .unwrap_err();

        let expected = r#"Error during planning: Unsupported _field predicate: t1 = Utf8("my_awesome_tag_value") OR _field = Utf8("f2")"#;

        assert_contains!(err.to_string(), expected);
    }

    #[test]
    fn test_normalize_predicate_field_rewrite_not_eq() {
        let predicate = normalize_predicate(
            &SessionContext::new(),
            "table",
            &schema(),
            &Predicate::new().with_expr(col("_field").not_eq(lit("f1"))),
        )
        .unwrap();

        let expected = Predicate::new().with_field_columns(vec!["f2"]).unwrap();

        assert_eq!(predicate, expected);
    }

    #[test]
    fn test_normalize_predicate_field_rewrite_field_multi_expressions() {
        let predicate = normalize_predicate(
            &SessionContext::new(),
            "table",
            &schema(),
            &Predicate::new()
                // put = and != predicates in *different* exprs
                .with_expr(col("_field").eq(lit("f1")))
                .with_expr(col("_field").not_eq(lit("f2"))),
        )
        .unwrap();

        let expected = Predicate::new().with_field_columns(vec!["f1"]).unwrap();

        assert_eq!(predicate, expected);
    }

    fn schema() -> Schema {
        schema::builder::SchemaBuilder::new()
            .tag("t1")
            .tag("t2")
            .field("f1", DataType::Int64)
            .unwrap()
            .field("f2", DataType::Int64)
            .unwrap()
            .build()
            .unwrap()
    }

    #[allow(dead_code)]
    const fn assert_send<T: Send>() {}

    // `InfluxRpcPredicate` shall be `Send`, otherwise we will have problems constructing plans for InfluxRPC
    // concurrently.
    const _: () = assert_send::<InfluxRpcPredicate>();
}
