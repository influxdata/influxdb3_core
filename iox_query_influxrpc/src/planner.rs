use crate::STRING_VALUE_COLUMN_NAME;
use crate::error::InfluxRpcError;
use crate::plan::{
    FieldAggregator, LogicalPlanBuilderExt, fields_pivot_schema, string_value_schema,
};
use crate::schema::SeriesSchema;
use arrow::datatypes::{Field, Schema as ArrowSchema};
use datafusion::{
    catalog::{SchemaProvider, TableProvider},
    common::DFSchema,
    datasource::provider_as_source,
    error::{DataFusionError, Result},
    execution::context::SessionContext,
    functions::core::get_field,
    functions_aggregate::{
        average::avg_udaf, count::count_udaf, expr_fn::max, min_max::max_udaf, sum::sum_udaf,
    },
    logical_expr::{LogicalPlan, LogicalPlanBuilder, Projection, col, expr::ScalarFunction, lit},
    prelude::{Column, Expr, when},
};
use schema::{InfluxColumnType, InfluxFieldType, Schema, TIME_COLUMN_NAME};

use datafusion_util::{
    AsExpr,
    config::{DEFAULT_CATALOG, DEFAULT_SCHEMA},
};
use hashbrown::HashSet;
use iox_query::exec::IOxSessionContext;
use observability_deps::tracing::debug;
use predicate::{
    Predicate,
    rpc_predicate::{
        FIELD_COLUMN_NAME, GROUP_KEY_SPECIAL_START, GROUP_KEY_SPECIAL_STOP, InfluxRpcPredicate,
        MEASUREMENT_COLUMN_NAME,
    },
};
use query_functions::{
    group_by::{Aggregate, WindowDuration},
    make_window_bound_expr,
    selectors::{selector_first, selector_last, selector_max, selector_min},
};
use std::collections::{BTreeMap, BTreeSet};
use std::iter::repeat_n;
use std::sync::Arc;

const CONCURRENT_TABLE_JOBS: usize = 10;

/// Plans queries that originate from the InfluxDB Storage gRPC
/// interface, which are in terms of the InfluxDB Data model (e.g.
/// `ParsedLine`). The query methods on this trait such as
/// `tag_keys` are specific to this data model.
///
/// The IOx storage engine implements this trait to provide time-series
/// specific queries, but also provides more generic access to the
/// same underlying data via other frontends (e.g. SQL).
///
/// The InfluxDB data model can be thought of as a relational
/// database table where each column has both a type as well as one of
/// the following categories:
///
/// * Tag (always String type)
/// * Field (Float64, Int64, UInt64, String, or Bool)
/// * Time (Int64)
///
/// While the underlying storage is the same for columns in different
/// categories with the same data type, columns of different
/// categories are treated differently in the different query types.
pub struct InfluxRpcPlanner {
    /// Optional executor currently only used to provide span context for tracing.
    ctx: IOxSessionContext,

    /// SchemaProvider for the database.
    schema_provider: Arc<dyn SchemaProvider>,
}

impl InfluxRpcPlanner {
    /// Create a new instance of the RPC planner using the [`IOxSessionContext`].
    /// InfluxRpc plans use the tables in the catalog and schema identified by
    /// [`DEFAULT_CATALOG`] and [`DEFAULT_SCHEMA`] respectively.
    pub fn new(ctx: IOxSessionContext) -> Self {
        let schema_provider = ctx
            .inner()
            .catalog(DEFAULT_CATALOG)
            .expect("default catalog exists")
            .schema(DEFAULT_SCHEMA)
            .expect("default schema exists");
        Self {
            ctx,
            schema_provider,
        }
    }

    /// Returns a builder that includes
    ///   . A set of table names got from meta data that will participate
    ///      in the requested `predicate`
    ///   . A set of plans of tables of either
    ///       . chunks with deleted data or
    ///       . chunks without deleted data but cannot be decided from meta data
    pub async fn table_names(&self, rpc_predicate: InfluxRpcPredicate) -> Result<LogicalPlan> {
        let ctx = self.ctx.child_ctx("table_names planning");
        debug!(?rpc_predicate, "planning table_names");

        // Special case predicates that span the entire valid timestamp range
        let rpc_predicate = rpc_predicate.clear_timestamp_if_max_range();

        let mut names = vec![];
        let mut plans = vec![];
        for table in table_iter(
            ctx.inner(),
            Arc::clone(&self.schema_provider),
            rpc_predicate,
        )
        .await?
        {
            if table.predicate().is_empty() {
                names.push(vec![lit(table.name())]);
            } else {
                plans.push(Self::table_name_plan(&table)?);
            }
        }

        let mut builder = LogicalPlanBuilder::known_values(names, string_value_schema())?;
        for p in plans {
            builder = builder.union(p)?;
        }
        builder = builder.sort(vec![col(STRING_VALUE_COLUMN_NAME).sort(true, false)])?;

        builder.build()
    }

    /// Returns a set of plans that produces the names of "tag" columns (as defined in the InfluxDB
    /// data model) names in this namespace that have more than zero rows which pass the conditions
    /// specified by `predicate`.
    pub async fn tag_keys(&self, rpc_predicate: InfluxRpcPredicate) -> Result<LogicalPlan> {
        let ctx = self.ctx.child_ctx("tag_keys planning");
        debug!(?rpc_predicate, "planning tag_keys");

        // Special case predicates that span the entire valid timestamp range
        let rpc_predicate = rpc_predicate.clear_timestamp_if_max_range();

        let mut need_plan = vec![];
        let mut keys = BTreeSet::new();
        for table in table_iter(
            ctx.inner(),
            Arc::clone(&self.schema_provider),
            rpc_predicate,
        )
        .await?
        {
            if table.predicate().is_empty() {
                // special case - return the columns from metadata only.
                // Note that columns with all rows deleted will still show here
                keys.extend(table.schema().tags_iter().map(|f| f.name().clone()));
            } else {
                need_plan.push(table);
            }
        }

        let mut plans = vec![];
        // At this point, we have a set of column names we know pass
        // in `known_columns`, and potentially some tables that we need
        // to run a plan to know if they pass the predicate.
        for table in need_plan {
            // Filter out any tables where all the possible tags are already known.
            if table.schema().tags_iter().all(|f| keys.contains(f.name())) {
                continue;
            }
            if let Some(plan) = Self::tag_keys_plan(&table, &keys)? {
                plans.push(plan);
            }
        }

        let mut builder = LogicalPlanBuilder::known_values(
            keys.into_iter().map(|v| vec![lit(v)]).collect(),
            string_value_schema(),
        )?;
        for p in plans {
            builder = builder.union_distinct(p)?;
        }
        builder = builder.sort(vec![col(STRING_VALUE_COLUMN_NAME).sort(true, false)])?;

        builder.build()
    }

    /// Returns a plan which finds the distinct, non-null tag values in the specified `tag_name`
    /// column of this namespace which pass the conditions specified by `predicate`.
    pub async fn tag_values(
        &self,
        tag_name: &str,
        rpc_predicate: InfluxRpcPredicate,
    ) -> Result<LogicalPlan> {
        let ctx = self.ctx.child_ctx("tag_values planning");
        debug!(?rpc_predicate, tag_name, "planning tag_values");

        // The basic algorithm is:
        //
        // 1. Find all the potential tables
        //
        // 2. For each table, figure out which have distinct
        // values that can be found from only metadata and
        // which need full plans

        let plans = table_iter(
            ctx.inner(),
            Arc::clone(&self.schema_provider),
            rpc_predicate,
        )
        .await?
        .filter(|table| table.schema().tags_iter().any(|tag| tag.name() == tag_name))
        .inspect(|table| {
            debug!(
                table_name = %table.name(),
                "need full plan to find tag values"
            )
        })
        .map(|table| Self::tag_values_plan(&table, tag_name))
        .collect::<Result<Vec<_>>>()?;

        let mut builder = LogicalPlanBuilder::known_values(vec![], string_value_schema())?;
        for p in plans {
            builder = builder.union_distinct(p)?;
        }
        builder = builder.sort(vec![STRING_VALUE_COLUMN_NAME.as_expr().sort(true, false)])?;
        builder.build()
    }

    /// Returns a plan that produces a list of columns and their datatypes (as defined in the data
    /// written via `write_lines`), and which have more than zero rows which pass the conditions
    /// specified by `predicate`.
    pub async fn field_columns(&self, rpc_predicate: InfluxRpcPredicate) -> Result<LogicalPlan> {
        let ctx = self.ctx.child_ctx("field_columns planning");
        debug!(?rpc_predicate, "planning field_columns");

        // Special case predicates that span the entire valid timestamp range
        let rpc_predicate = rpc_predicate.clear_timestamp_if_max_range();

        // Algorithm is to run a "select field_cols from table where
        // <predicate> type plan for each table"
        //
        // The executor then figures out which columns have non-null
        // values and stops the plan executing once it has them

        // optimization: just get the field columns from metadata.
        // note this both ignores field keys, and sets the timestamp data 'incorrectly'.
        let mut fields: BTreeMap<String, InfluxFieldType> = BTreeMap::new();
        let mut plans = vec![];
        for table in table_iter(
            ctx.inner(),
            Arc::clone(&self.schema_provider),
            rpc_predicate,
        )
        .await?
        {
            if table.predicate().is_empty() {
                for (r#type, field) in table.schema().iter() {
                    if let InfluxColumnType::Field(r#type) = r#type {
                        fields.insert(field.name().into(), r#type);
                    }
                }
            } else {
                plans.push(Self::field_columns_plan(&table)?);
            }
        }

        let mut builder = LogicalPlanBuilder::known_values(
            fields
                .into_iter()
                .map(|(name, r#type)| vec![lit(name), lit(r#type as i32), lit(0_i64)])
                .collect::<Vec<_>>(),
            fields_pivot_schema(),
        )?;
        for plan in plans {
            builder = builder.union(plan)?
        }
        builder = builder
            .aggregate(
                vec![col("key"), col("type")],
                vec![max(col("timestamp")).alias("timestamp")],
            )?
            .sort(vec![col("key").sort(true, false)])?;

        builder.build()
    }

    /// Returns a plan that finds all rows which pass the
    /// conditions specified by `predicate` in the form of logical
    /// time series.
    ///
    /// A time series is defined by the unique values in a set of
    /// "tag_columns" for each field in the "field_columns", ordered by
    /// the time column.
    ///
    /// The output looks like:
    /// ```text
    /// (tag_col1, tag_col2, ... field1, field2, ... timestamp)
    /// ```
    ///
    /// The  tag_columns are ordered by name.
    ///
    /// The data is sorted on (tag_col1, tag_col2, ...) so that all
    /// rows for a particular series (groups where all tags are the
    /// same) occur together in the plan
    pub async fn read_filter(&self, rpc_predicate: InfluxRpcPredicate) -> Result<LogicalPlan> {
        let ctx = self.ctx.child_ctx("planning_read_filter");
        debug!(?rpc_predicate, "planning read_filter");

        let mut tables = table_iter(
            ctx.inner(),
            Arc::clone(&self.schema_provider),
            rpc_predicate,
        )
        .await?
        .collect::<Vec<_>>();

        // The measurement (table) name is the first element of the sort key.
        // Let's generate them in the correct order to avoid needing a sort later.
        tables
            .as_mut_slice()
            .sort_unstable_by(|t1, t2| t1.name().cmp(t2.name()));

        let plans = tables
            .iter()
            .map(Self::read_filter_plan)
            .collect::<Result<Vec<_>, DataFusionError>>()?;

        let mut builder = LogicalPlanBuilder::union_series(plans)?;
        let schema = SeriesSchema::try_from(builder.schema())?;
        builder = builder.sort(schema.sort(&[]))?;
        builder.build()
    }

    /// Creates one or more GroupedSeriesSet plans that produces an
    /// output table with rows grouped according to group_columns and
    /// an aggregate function which is applied to each *series* (aka
    /// distinct set of tag value). Note the aggregate is not applied
    /// across series within the same group.
    ///
    /// Specifically the data that is output from the plans is
    /// guaranteed to be sorted such that:
    ///
    ///   1. The group_columns are a prefix of the sort key
    ///
    ///   2. All remaining tags appear in the sort key, in order,
    ///      after the prefix (as the tag key may also appear as a
    ///      group key)
    ///
    /// Schematically, the plan looks like:
    ///
    /// (order by {group_coumns, remaining tags})
    ///   (aggregate by group -- agg, gby_exprs=tags)
    ///      (apply filters)
    pub async fn read_group(
        &self,
        rpc_predicate: InfluxRpcPredicate,
        agg: Aggregate,
        group_columns: &[impl AsRef<str> + Send + Sync],
    ) -> Result<LogicalPlan> {
        let ctx = self.ctx.child_ctx("read_group planning");
        debug!(?rpc_predicate, ?agg, "planning read_group");

        // Note always group (which will resort the frames)
        // by tag, even if there are 0 columns
        let group_columns = group_columns
            .iter()
            .map(|s| Arc::from(s.as_ref()))
            .collect::<Vec<Arc<str>>>();
        let mut group_columns_set: HashSet<Arc<str>> = HashSet::with_capacity(group_columns.len());
        for group_col in &group_columns {
            match group_columns_set.entry(Arc::clone(group_col)) {
                hashbrown::hash_set::Entry::Occupied(_) => {
                    return Err(DataFusionError::Plan(format!(
                        "Duplicate group column: {group_col}"
                    )));
                }
                hashbrown::hash_set::Entry::Vacant(v) => {
                    v.insert();
                }
            }
        }

        let plans = table_iter(
            ctx.inner(),
            Arc::clone(&self.schema_provider),
            rpc_predicate,
        )
        .await?
        .map(|table| {
            // check group_columns for unknown columns
            let known_tags_vec = table
                .schema()
                .tags_iter()
                .map(|f| f.name().clone())
                .collect::<Vec<_>>();
            let known_tags_set = known_tags_vec
                .iter()
                .map(|s| s.as_str())
                .collect::<HashSet<_>>();
            for group_col in &group_columns {
                if (group_col.as_ref() == FIELD_COLUMN_NAME)
                    || (group_col.as_ref() == MEASUREMENT_COLUMN_NAME)
                    || (group_col.as_ref() == GROUP_KEY_SPECIAL_START)
                    || (group_col.as_ref() == GROUP_KEY_SPECIAL_STOP)
                {
                    continue;
                }

                if !known_tags_set.contains(group_col.as_ref()) {
                    return Err(InfluxRpcError::UnknownTag {
                        name: group_col.to_string(),
                        known_tags: known_tags_vec.clone(),
                    }
                    .into());
                }
            }
            Self::read_group_plan(&table, agg)
        })
        .collect::<Result<Vec<_>>>()?;

        let mut builder = LogicalPlanBuilder::union_series(plans)?;
        let schema = SeriesSchema::try_from(builder.schema())?;
        builder = builder.sort(schema.sort(&group_columns))?;

        builder.build()
    }

    /// Creates a GroupedSeriesSet plan that produces an output table with rows
    /// that are grouped by window definitions
    pub async fn read_window_aggregate(
        &self,
        rpc_predicate: InfluxRpcPredicate,
        agg: Aggregate,
        every: WindowDuration,
        offset: WindowDuration,
    ) -> Result<LogicalPlan> {
        let ctx = self.ctx.child_ctx("read_window_aggregate planning");
        debug!(
            ?rpc_predicate,
            ?agg,
            ?every,
            ?offset,
            "planning read_window_aggregate"
        );

        let plans = table_iter(
            ctx.inner(),
            Arc::clone(&self.schema_provider),
            rpc_predicate,
        )
        .await?
        .map(|table| Self::read_window_aggregate_plan(&table, agg, every, offset))
        .collect::<Result<Vec<_>, DataFusionError>>()?;

        let mut builder = LogicalPlanBuilder::union_series(plans)?;
        let schema = SeriesSchema::try_from(builder.schema())?;
        builder = builder.sort(schema.sort(&[]))?;

        builder.build()
    }

    /// Creates a DataFusion LogicalPlan that returns column *names* as a
    /// single column of Strings for a specific table
    ///
    /// The created plan looks like:
    ///
    /// ```text
    ///  Extension(PivotSchema)
    ///    Filter(predicate)
    ///      TableScan (of chunks)
    /// ```
    fn tag_keys_plan(
        table: &InfluxRpcTable,
        known_keys: &BTreeSet<String>,
    ) -> Result<Option<LogicalPlan>, DataFusionError> {
        // select only the tag columns that we don't already know about.
        let select_exprs = table
            .schema()
            .tags_iter()
            .filter(|field| !known_keys.contains(field.name()))
            .map(|field| field.name().as_expr())
            .collect::<Vec<_>>();

        // If the projection is empty then there is no plan to execute.
        if select_exprs.is_empty() {
            return Ok(None);
        }

        let mut builder = table.builder()?;
        builder = table.filter_null_fields(builder)?.project(select_exprs)?;
        // And finally pivot the plan
        let plan = builder
            .schema_pivot()?
            .project(vec![col("non_null_column").alias(STRING_VALUE_COLUMN_NAME)])?
            .build()?;

        debug!(table_name=table.name(), plan=%plan.display_indent_schema(),
               "created column_name plan for table");

        Ok(Some(plan))
    }

    /// Creates a DataFusion LogicalPlan that returns the distinct values
    /// for a specific tag column in a table.
    fn tag_values_plan(table: &InfluxRpcTable, key: &str) -> Result<LogicalPlan, DataFusionError> {
        let mut builder = table.builder()?;
        builder = table.filter_null_fields(builder)?;
        builder = builder.project(vec![key.as_expr().alias(STRING_VALUE_COLUMN_NAME)])?;
        builder = builder.filter(STRING_VALUE_COLUMN_NAME.as_expr().is_not_null())?;
        builder = builder.distinct()?;
        builder.build()
    }

    /// Creates a DataFusion LogicalPlan that returns the timestamp
    /// and all field columns for a specified table:
    ///
    /// The output looks like (field0, field1, ..., time)
    ///
    /// The data is not sorted in any particular order
    ///
    /// The created plan looks like:
    ///
    /// ```text
    ///  Projection (select the field columns needed)
    ///      Filter(predicate) [optional]
    ///        Scan
    /// ```
    fn field_columns_plan(table: &InfluxRpcTable) -> Result<LogicalPlan, DataFusionError> {
        let mut builder = table.builder()?;
        builder = table.filter_null_fields(builder)?;
        let schema = Schema::try_from(Arc::clone(builder.schema().inner()))
            .map_err(|err| DataFusionError::External(Box::new(err)))?;
        // Selection of only fields and time
        let select_exprs = schema
            .time_iter()
            .chain(table.schema.fields_iter())
            .map(|field| field.name().as_expr());

        let builder = builder.project(select_exprs)?;
        builder
            .sort([col(TIME_COLUMN_NAME).sort(false, false)])?
            .fields_pivot()?
            .build()
    }

    /// Creates a DataFusion LogicalPlan that returns the values in
    /// the fields for a specified table:
    ///
    /// The output produces the table name as a single string if any
    /// non null values are passed in.
    ///
    /// The data is not sorted in any particular order
    ///
    /// returns `None` if the table contains no rows that would pass
    /// the predicate.
    ///
    /// The created plan looks like:
    ///
    /// ```text
    ///  NonNullChecker
    ///    Projection (select fields)
    ///      Filter(predicate) [optional]
    ///        Scan
    /// ```
    fn table_name_plan(table: &InfluxRpcTable) -> Result<LogicalPlan, DataFusionError> {
        debug!(table_name = table.name(), "Creating table_name full plan");
        table
            .filter_null_fields(table.builder()?)?
            .sort([col(TIME_COLUMN_NAME).sort(false, false)])?
            .limit(0, Some(1))?
            .project(vec![lit(table.name()).alias(STRING_VALUE_COLUMN_NAME)])?
            .build()
    }

    /// Creates a DataFusion LogicalPlan that produces the pivoted data
    /// from the table provider.
    ///
    /// The plan has the schema like:
    ///
    /// - _measurement: Dictionary(Int32, Utf8)
    /// - ...tags: Dictionary(Int32, Utf8)
    /// - _field: Dictionary(Int32, Utf8)
    /// - _time: Timestamp(Nanosecond, None)
    /// - _value: Union
    ///
    fn read_filter_plan(table: &InfluxRpcTable) -> Result<LogicalPlan, DataFusionError> {
        Self::read_group_plan(table, Aggregate::None)
    }

    /// Creates a DataFusion LogicalPlan that produces the pivoted data
    /// from the table provider. If an aggregate is specified it will be
    /// applied to the data before pivoting.
    ///
    /// The plan has the schema like:
    ///
    /// - _measurement: Dictionary(Int32, Utf8)
    /// - ...tags: Dictionary(Int32, Utf8)
    /// - _field: Dictionary(Int32, Utf8)
    /// - _time: Timestamp(Nanosecond, None)
    /// - _value: Union
    ///
    fn read_group_plan(
        table: &InfluxRpcTable,
        aggregate: Aggregate,
    ) -> Result<LogicalPlan, DataFusionError> {
        let measurement_expr = lit(table.name());

        let mut builder = table.builder()?;
        builder = table.filter_field_values(builder)?;
        let schema = Schema::try_from(Arc::clone(builder.schema().inner()))
            .map_err(|err| DataFusionError::External(Box::new(err)))?;
        let mut tag_names = schema
            .tags_iter()
            .map(|tag| tag.name().clone())
            .collect::<Vec<String>>();
        tag_names.sort_unstable();
        let tag_exprs: Vec<_> = tag_names
            .into_iter()
            .map(|s| {
                Expr::Column(Column {
                    relation: None,
                    name: s,
                })
            })
            .collect();
        let time_expr = table.schema().time_iter().map(field_col).next().unwrap();
        let field_exprs = schema
            .fields_iter()
            .map(|field| field.name().as_expr())
            .collect::<Vec<_>>();

        let (builder, time_exprs, field_exprs) = match aggregate {
            agg @ (Aggregate::Sum | Aggregate::Count | Aggregate::Mean) => {
                let agg = match agg {
                    Aggregate::Sum => FieldAggregator::Aggregator(sum_udaf()),
                    Aggregate::Count => FieldAggregator::Aggregator(count_udaf()),
                    Aggregate::Mean => FieldAggregator::Aggregator(avg_udaf()),
                    _ => unreachable!(),
                };

                (
                    builder.aggregate_fields(
                        tag_exprs.clone(),
                        time_expr.clone(),
                        Some(max_udaf()),
                        field_exprs.clone(),
                        agg,
                    )?,
                    repeat_n(&time_expr, field_exprs.len())
                        .cloned()
                        .collect::<Vec<_>>(),
                    field_exprs,
                )
            }
            agg @ (Aggregate::Min | Aggregate::Max | Aggregate::First | Aggregate::Last) => {
                let agg = match agg {
                    Aggregate::Min => FieldAggregator::Selector(Arc::new(selector_min())),
                    Aggregate::Max => FieldAggregator::Selector(Arc::new(selector_max())),
                    Aggregate::First => FieldAggregator::Selector(Arc::new(selector_first())),
                    Aggregate::Last => FieldAggregator::Selector(Arc::new(selector_last())),
                    _ => unreachable!(),
                };
                (
                    builder.aggregate_fields(
                        tag_exprs.clone(),
                        time_expr.clone(),
                        None,
                        field_exprs.clone(),
                        agg,
                    )?,
                    field_exprs
                        .iter()
                        .map(|expr| {
                            Expr::ScalarFunction(ScalarFunction {
                                func: get_field(),
                                args: vec![expr.clone(), lit("time")],
                            })
                        })
                        .collect(),
                    field_exprs
                        .iter()
                        .map(|expr| {
                            expr.name_for_alias().map(|alias| {
                                Expr::ScalarFunction(ScalarFunction {
                                    func: get_field(),
                                    args: vec![expr.clone(), lit("value")],
                                })
                                .alias(alias)
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                )
            }
            Aggregate::None => (
                table.filter_null_fields(builder)?,
                repeat_n(&time_expr, field_exprs.len())
                    .cloned()
                    .collect::<Vec<_>>(),
                field_exprs,
            ),
        };
        builder
            .series_pivot(measurement_expr, tag_exprs, time_exprs, field_exprs)?
            .build()
    }

    /// Creates a DataFusion LogicalPlan that produces the pivoted data
    /// from the table provider. The data is aggregated into time windows
    /// using the requested aggregate function.
    ///
    /// The plan has the schema like:
    ///
    /// - _measurement: Dictionary(Int32, Utf8)
    /// - ...tags: Dictionary(Int32, Utf8)
    /// - _field: Dictionary(Int32, Utf8)
    /// - _time: Timestamp(Nanosecond, None)
    /// - _value: Union
    ///
    fn read_window_aggregate_plan(
        table: &InfluxRpcTable,
        aggregate: Aggregate,
        every: WindowDuration,
        offset: WindowDuration,
    ) -> Result<LogicalPlan, DataFusionError> {
        let measurement_expr = lit(table.name());

        let builder = table.builder()?;
        let builder = table.filter_field_values(builder)?;
        let schema = Schema::try_from(Arc::clone(builder.schema().inner()))
            .map_err(|err| DataFusionError::External(Box::new(err)))?;
        let mut tag_names = schema
            .tags_iter()
            .map(|tag| tag.name().clone())
            .collect::<Vec<String>>();
        tag_names.sort_unstable();
        let tag_exprs: Vec<_> = tag_names.into_iter().map(|s| s.as_expr()).collect();
        let time_expr = table.schema().time_iter().map(field_col).next().unwrap();
        let field_exprs = schema
            .fields_iter()
            .map(|field| field.name().as_expr())
            .collect::<Vec<_>>();
        // Group by all tag columns and the window bounds
        let window_bound = make_window_bound_expr(TIME_COLUMN_NAME.as_expr(), every, offset)
            .alias(TIME_COLUMN_NAME);

        let mut group_expr = tag_exprs.clone();
        group_expr.push(window_bound);

        let (builder, time_exprs, field_exprs) = match aggregate {
            agg @ (Aggregate::Sum | Aggregate::Count | Aggregate::Mean) => {
                let agg = match agg {
                    Aggregate::Sum => FieldAggregator::Aggregator(sum_udaf()),
                    Aggregate::Count => FieldAggregator::Aggregator(count_udaf()),
                    Aggregate::Mean => FieldAggregator::Aggregator(avg_udaf()),
                    _ => unreachable!(),
                };
                (
                    builder.aggregate_fields(
                        group_expr,
                        time_expr.clone(),
                        None,
                        field_exprs.clone(),
                        agg,
                    )?,
                    repeat_n(&time_expr, field_exprs.len())
                        .cloned()
                        .collect::<Vec<_>>(),
                    field_exprs,
                )
            }
            agg @ (Aggregate::Min | Aggregate::Max | Aggregate::First | Aggregate::Last) => {
                let agg = match agg {
                    Aggregate::Min => FieldAggregator::Selector(Arc::new(selector_min())),
                    Aggregate::Max => FieldAggregator::Selector(Arc::new(selector_max())),
                    Aggregate::First => FieldAggregator::Selector(Arc::new(selector_first())),
                    Aggregate::Last => FieldAggregator::Selector(Arc::new(selector_last())),
                    _ => unreachable!(),
                };
                (
                    builder.aggregate_fields(
                        group_expr,
                        time_expr.clone(),
                        None,
                        field_exprs.clone(),
                        agg,
                    )?,
                    field_exprs
                        .iter()
                        .map(|expr| {
                            Expr::ScalarFunction(ScalarFunction {
                                func: get_field(),
                                args: vec![expr.clone(), lit("time")],
                            })
                        })
                        .collect(),
                    field_exprs
                        .iter()
                        .map(|expr| {
                            expr.name_for_alias().map(|alias| {
                                Expr::ScalarFunction(ScalarFunction {
                                    func: get_field(),
                                    args: vec![expr.clone(), lit("value")],
                                })
                                .alias(alias)
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                )
            }
            Aggregate::None => {
                return Err(DataFusionError::Plan(
                    "read_window_aggregate requires an aggregate type".to_string(),
                ));
            }
        };
        builder
            .series_pivot(measurement_expr, tag_exprs, time_exprs, field_exprs)?
            .build()
    }
}

impl std::fmt::Debug for InfluxRpcPlanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InfluxRpcPlanner")
            .field("ctx", &self.ctx)
            .field("schema_provider", &"<SchemaProvider>")
            .finish()
    }
}

/// The representation of an IOx table required for planning
/// influxrpc queries.
struct InfluxRpcTable {
    /// The name of the table
    name: Arc<str>,
    /// The IOx schema of the table
    schema: Schema,
    /// The provider of the table
    provider: Arc<dyn TableProvider>,
    /// The predicate that filters the table
    predicate: Predicate,
}

impl InfluxRpcTable {
    fn name(&self) -> &str {
        &self.name
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn predicate(&self) -> &Predicate {
        &self.predicate
    }

    /// Create a LogicalPlanBuilder for the table. This builder will scan the table
    /// and apply any filters specified by the predicate.
    fn builder(&self) -> Result<LogicalPlanBuilder, DataFusionError> {
        let mut builder = LogicalPlanBuilder::scan(
            self.name.as_ref(),
            provider_as_source(Arc::clone(&self.provider)),
            None,
        )?;
        if let Some(expr) = self.predicate.filter_expr() {
            builder = builder.filter(expr)?;
        }
        Ok(builder)
    }

    fn filter_field_values(
        &self,
        builder: LogicalPlanBuilder,
    ) -> Result<LogicalPlanBuilder, DataFusionError> {
        let input = builder.build()?;
        let field_exprs = filtered_fields_iter(&self.schema, &self.predicate)
            .map(|fe| fe.expr)
            .collect::<Vec<_>>();
        let expr = self
            .schema
            .time_iter()
            .chain(self.schema.tags_iter())
            .map(|field| field.name().as_expr())
            .chain(field_exprs)
            .collect::<Vec<_>>();
        let fields = expr
            .iter()
            .map(|e| {
                Ok(self
                    .schema
                    .inner()
                    .field_with_name(e.name_for_alias()?.as_ref())?
                    .clone())
            })
            .collect::<Result<Vec<_>, DataFusionError>>()?;
        let schema = ArrowSchema::new(fields);
        let dfschema = DFSchema::try_from(schema)?;

        Ok(LogicalPlanBuilder::from(LogicalPlan::Projection(
            Projection::try_new_with_schema(expr, Arc::new(input), Arc::new(dfschema))?,
        )))
    }

    fn filter_null_fields(
        &self,
        builder: LogicalPlanBuilder,
    ) -> Result<LogicalPlanBuilder, DataFusionError> {
        let fields = filtered_fields_iter(&self.schema, &self.predicate)
            .map(|fe| fe.name.as_expr())
            .collect::<Vec<_>>();
        if let Some(expr) = fields
            .into_iter()
            .map(|expr| expr.is_not_null())
            .reduce(|l, r| l.or(r))
        {
            builder.filter(expr)
        } else {
            Ok(builder)
        }
    }
}

/// Iterator over tables that match the given predicate.
async fn table_iter(
    ctx: &SessionContext,
    schema_provider: Arc<dyn SchemaProvider>,
    rpc_predicate: InfluxRpcPredicate,
) -> Result<impl Iterator<Item = InfluxRpcTable>, DataFusionError> {
    let iter = rpc_predicate
        .table_predicates(ctx, schema_provider, CONCURRENT_TABLE_JOBS)
        .await?
        .into_iter()
        .filter_map(|(name, table, predicate)| {
            table.map(|table| InfluxRpcTable {
                name,
                schema: table.1,
                provider: table.0,
                predicate,
            })
        })
        .filter(|table| {
            // If there is a field_columns predicate then check that the table
            // includes at least one of those columms.
            match &table.predicate().field_columns {
                Some(field_columns) => field_columns
                    .iter()
                    .any(|field| table.schema().field_by_name(field).is_some()),
                None => true,
            }
        });
    Ok(iter)
}

// Encapsulates a field column projection as an expression. In the simplest case
// the expression is a `Column` expression. In more complex cases it might be
// a predicate that filters rows for the projection.
#[derive(Clone)]
struct FieldExpr<'a> {
    expr: Expr,
    name: &'a str,
}

// Returns an iterator of fields from schema that pass the predicate. If there
// are expressions associated with field column projections then these are
// applied to the column via `CASE` statements.
//
// TODO(edd): correctly support multiple `_value` expressions. Right now they
// are OR'd together, which makes sense for equality operators like `_value == xyz`.
fn filtered_fields_iter<'a>(
    schema: &'a Schema,
    predicate: &'a Predicate,
) -> impl Iterator<Item = FieldExpr<'a>> {
    schema.fields_iter().filter_map(move |f| {
        if !predicate.should_include_field(f.name()) {
            return None;
        }

        // For example, assume two fields (`field1` and `field2`) along with
        // a predicate like: `_value = 1.32 OR _value = 2.87`. The projected
        // field columns become:
        //
        // SELECT
        //  CASE WHEN #field1 = Float64(1.32) OR #field1 = Float64(2.87) THEN #field1 END AS field1,
        //  CASE WHEN #field2 = Float64(1.32) OR #field2 = Float64(2.87) THEN #field2 END AS field2
        //
        let expr = predicate
            .value_expr
            .iter()
            .map(|value_expr| value_expr.replace_col(f.name()))
            .reduce(|a, b| a.or(b))
            .map(|when_expr| when(when_expr, f.name().as_expr()).end())
            .unwrap_or_else(|| Ok(f.name().as_expr()))
            .unwrap();

        Some(FieldExpr {
            expr: expr.alias(f.name()),
            name: f.name(),
        })
    })
}

/// Create an expression that references a field in a table.
fn field_col(field: &Field) -> Expr {
    Expr::Column(Column {
        relation: None,
        name: field.name().clone(),
    })
}

#[cfg(test)]
mod tests {
    use datafusion::prelude::lit;

    use predicate::Predicate;

    use iox_query::{
        QueryNamespace,
        exec::Executor,
        test::{TestChunk, TestDatabase},
    };
    use test_helpers::maybe_start_logging;

    use super::*;

    /// Fix to address [IDPE issue #17144][17144]
    ///
    /// [17144]: https://github.com/influxdata/idpe/issues/17144
    #[tokio::test]
    async fn test_idpe_issue_17144() {
        let chunk0 = Arc::new(
            TestChunk::new("h2o")
                .with_id(0)
                .with_tag_column("foo")
                .with_f64_field_column("foo.bar") // with period
                .with_time_column(),
        );

        let executor = Arc::new(Executor::new_testing());
        let test_db = Arc::new(TestDatabase::new(Arc::clone(&executor)));
        test_db.add_chunk("my_partition_key", Arc::clone(&chunk0));

        // verify that _field = 'foo.bar' is rewritten correctly
        let predicate = Predicate::new().with_expr(
            "_field"
                .as_expr()
                .eq(lit("foo.bar"))
                .and("_value".as_expr().eq(lit(1.2))),
        );

        let rpc_predicate = InfluxRpcPredicate::new(None, predicate);

        let agg = Aggregate::None;
        let group_columns = &[String::from("foo")];
        let res = InfluxRpcPlanner::new(test_db.new_query_context(None, None))
            .read_group(rpc_predicate, agg, group_columns)
            .await
            .expect("creating plan");
        insta::assert_snapshot!(res.display_indent_schema().to_string(), @r#"
        Sort: foo ASC NULLS LAST, _measurement ASC NULLS LAST, _field ASC NULLS LAST, _time ASC NULLS LAST [_measurement:Dictionary(Int32, Utf8), foo:Dictionary(Int32, Utf8);N, _field:Dictionary(Int32, Utf8), _time:Timestamp(Nanosecond, None), _value:Union([(0, Field { name: "float", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), (1, Field { name: "integer", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), (2, Field { name: "unsigned", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), (3, Field { name: "boolean", data_type: Boolean, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), (4, Field { name: "string", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} })], Sparse)]
          SeriesPivot: measurement_expr=Utf8("h2o"), tag_exprs=(foo), time_exprs=(time), field_exprs=(foo.bar) [_measurement:Dictionary(Int32, Utf8), foo:Dictionary(Int32, Utf8);N, _field:Dictionary(Int32, Utf8), _time:Timestamp(Nanosecond, None), _value:Union([(0, Field { name: "float", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), (1, Field { name: "integer", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), (2, Field { name: "unsigned", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), (3, Field { name: "boolean", data_type: Boolean, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), (4, Field { name: "string", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} })], Sparse)]
            Filter: foo.bar IS NOT NULL [time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, foo.bar:Float64;N]
              Projection: time, foo, CASE WHEN foo.bar = Float64(1.2) THEN foo.bar END AS foo.bar [time:Timestamp(Nanosecond, None), foo:Dictionary(Int32, Utf8);N, foo.bar:Float64;N]
                TableScan: h2o [foo:Dictionary(Int32, Utf8);N, foo.bar:Float64;N, time:Timestamp(Nanosecond, None)]
        "#);
    }

    #[tokio::test]
    async fn test_issue_7848() {
        maybe_start_logging();

        let chunk = Arc::new(
            TestChunk::new("table")
                .with_id(0)
                .with_tag_column("tag")
                .with_f64_field_column("field")
                .with_time_column()
                .with_one_row_of_data(),
        );

        let executor = Arc::new(Executor::new_testing());
        let test_db =
            Arc::new(TestDatabase::new(Arc::clone(&executor)).with_retention_time_ns(Some(1)));
        test_db.add_chunk("my_partition_key", Arc::clone(&chunk));

        let predicate = Predicate::new().with_expr("tag".as_expr().eq(lit("MA")));

        let rpc_predicate = InfluxRpcPredicate::new(None, predicate);

        let plan = InfluxRpcPlanner::new(test_db.new_query_context(None, None))
            .read_filter(rpc_predicate)
            .await
            .expect("creating plan");

        // Note: The retention policy (i.e. a time predicate) does NOT occur within the logical plan because it is an
        //       implementation detail of the table itself and will only be manifested when the `TableScan` is converted
        //       into a physical plan (which uses the IOx table provider code).
        insta::assert_snapshot!(plan.display_indent_schema().to_string(), @r#"
        Sort: _measurement ASC NULLS LAST, tag ASC NULLS LAST, _field ASC NULLS LAST, _time ASC NULLS LAST [_measurement:Dictionary(Int32, Utf8), tag:Dictionary(Int32, Utf8);N, _field:Dictionary(Int32, Utf8), _time:Timestamp(Nanosecond, None), _value:Union([(0, Field { name: "float", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), (1, Field { name: "integer", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), (2, Field { name: "unsigned", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), (3, Field { name: "boolean", data_type: Boolean, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), (4, Field { name: "string", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} })], Sparse)]
          SeriesPivot: measurement_expr=Utf8("table"), tag_exprs=(tag), time_exprs=(time), field_exprs=(field) [_measurement:Dictionary(Int32, Utf8), tag:Dictionary(Int32, Utf8);N, _field:Dictionary(Int32, Utf8), _time:Timestamp(Nanosecond, None), _value:Union([(0, Field { name: "float", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), (1, Field { name: "integer", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), (2, Field { name: "unsigned", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), (3, Field { name: "boolean", data_type: Boolean, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), (4, Field { name: "string", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} })], Sparse)]
            Filter: field IS NOT NULL [time:Timestamp(Nanosecond, None), tag:Dictionary(Int32, Utf8);N, field:Float64;N]
              Projection: time, tag, field AS field [time:Timestamp(Nanosecond, None), tag:Dictionary(Int32, Utf8);N, field:Float64;N]
                Filter: table.tag = Dictionary(Int32, Utf8("MA")) [field:Float64;N, tag:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
                  TableScan: table [field:Float64;N, tag:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
        "#);
    }

    #[tokio::test]
    async fn test_table_names_plan_empty() {
        maybe_start_logging();

        let executor = Arc::new(Executor::new_testing());
        let test_db = Arc::new(TestDatabase::new(Arc::clone(&executor)));
        let predicate = Predicate::new();
        let rpc_predicate = InfluxRpcPredicate::new(None, predicate);

        let plan = InfluxRpcPlanner::new(test_db.new_query_context(None, None))
            .table_names(rpc_predicate)
            .await
            .expect("creating plan");

        insta::assert_snapshot!(plan.display_indent_schema().to_string(), @r"
        Sort: string_value ASC NULLS LAST [string_value:Utf8]
          EmptyRelation [string_value:Utf8]
        ");

        let physical_plan = executor
            .new_context()
            .create_physical_plan(&plan)
            .await
            .expect("physical plan");
        let physical_plan = datafusion::physical_plan::displayable(physical_plan.as_ref());
        insta::assert_snapshot!(physical_plan.indent(true), @"EmptyExec");
    }

    #[tokio::test]
    async fn test_table_names_plan_one_table() {
        maybe_start_logging();

        let executor = Arc::new(Executor::new_testing());
        let test_db = Arc::new(TestDatabase::new(Arc::clone(&executor)));

        let chunk = Arc::new(
            TestChunk::new("table")
                .with_id(0)
                .with_tag_column("tag")
                .with_f64_field_column("field")
                .with_time_column()
                .with_one_row_of_data(),
        );
        test_db.add_chunk("my_partition_key", Arc::clone(&chunk));

        let predicate = Predicate::new();
        let rpc_predicate = InfluxRpcPredicate::new(None, predicate);

        let plan = InfluxRpcPlanner::new(test_db.new_query_context(None, None))
            .table_names(rpc_predicate)
            .await
            .expect("creating plan");

        insta::assert_snapshot!(plan.display_indent_schema().to_string(), @r#"
        Sort: string_value ASC NULLS LAST [string_value:Utf8]
          Values: (Utf8("table")) [string_value:Utf8]
        "#);

        let physical_plan = executor
            .new_context()
            .create_physical_plan(&plan)
            .await
            .expect("physical plan");
        let physical_plan = datafusion::physical_plan::displayable(physical_plan.as_ref());
        insta::assert_snapshot!(physical_plan.indent(true), @r"
        SortExec: expr=[string_value@0 ASC NULLS LAST], preserve_partitioning=[false]
          ValuesExec
        ");
    }

    #[tokio::test]
    async fn test_table_names_plan_two_tables() {
        maybe_start_logging();

        let executor = Arc::new(Executor::new_testing());
        let test_db = Arc::new(TestDatabase::new(Arc::clone(&executor)));

        let chunk = Arc::new(
            TestChunk::new("table")
                .with_id(0)
                .with_tag_column("tag")
                .with_f64_field_column("field")
                .with_time_column()
                .with_one_row_of_data(),
        );
        test_db.add_chunk("my_partition_key_1", Arc::clone(&chunk));

        let chunk = Arc::new(
            TestChunk::new("other_table")
                .with_id(0)
                .with_tag_column("tag")
                .with_f64_field_column("other_field")
                .with_time_column()
                .with_one_row_of_data(),
        );
        test_db.add_chunk("my_partition_key_2", Arc::clone(&chunk));

        let predicate = Predicate::new();
        let rpc_predicate = InfluxRpcPredicate::new(None, predicate);

        let plan = InfluxRpcPlanner::new(test_db.new_query_context(None, None))
            .table_names(rpc_predicate)
            .await
            .expect("creating plan");

        insta::assert_snapshot!(plan.display_indent_schema().to_string(), @r#"
        Sort: string_value ASC NULLS LAST [string_value:Utf8]
          Values: (Utf8("table")), (Utf8("other_table")) [string_value:Utf8]
        "#);

        let physical_plan = executor
            .new_context()
            .create_physical_plan(&plan)
            .await
            .expect("physical plan");
        let physical_plan = datafusion::physical_plan::displayable(physical_plan.as_ref());
        insta::assert_snapshot!(physical_plan.indent(true), @r"
        SortExec: expr=[string_value@0 ASC NULLS LAST], preserve_partitioning=[false]
          ValuesExec
        ");
    }

    #[tokio::test]
    async fn test_table_names_plan_two_tables_with_predicate() {
        maybe_start_logging();

        let executor = Arc::new(Executor::new_testing());
        let test_db = Arc::new(TestDatabase::new(Arc::clone(&executor)));

        let chunk = Arc::new(
            TestChunk::new("table")
                .with_id(0)
                .with_tag_column("tag")
                .with_f64_field_column("field")
                .with_time_column()
                .with_one_row_of_data(),
        );
        test_db.add_chunk("my_partition_key_1", Arc::clone(&chunk));

        let chunk = Arc::new(
            TestChunk::new("other_table")
                .with_id(0)
                .with_tag_column("other_tag")
                .with_f64_field_column("other_field")
                .with_time_column()
                .with_one_row_of_data(),
        );
        test_db.add_chunk("my_partition_key_2", Arc::clone(&chunk));

        let predicate = Predicate::new().with_expr("tag".as_expr().eq(lit("MA")));
        let rpc_predicate = InfluxRpcPredicate::new(None, predicate);

        let plan = InfluxRpcPlanner::new(test_db.new_query_context(None, None))
            .table_names(rpc_predicate)
            .await
            .expect("creating plan");

        insta::assert_snapshot!(plan.display_indent_schema().to_string(), @r#"
        Sort: string_value ASC NULLS LAST [string_value:Utf8]
          Union [string_value:Utf8]
            Union [string_value:Utf8]
              EmptyRelation [string_value:Utf8]
              Projection: Utf8("table") AS string_value [string_value:Utf8]
                Limit: skip=0, fetch=1 [field:Float64;N, tag:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
                  Sort: table.time DESC NULLS LAST [field:Float64;N, tag:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
                    Filter: table.field IS NOT NULL [field:Float64;N, tag:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
                      Filter: table.tag = Dictionary(Int32, Utf8("MA")) [field:Float64;N, tag:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
                        TableScan: table [field:Float64;N, tag:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
            Projection: Utf8("other_table") AS string_value [string_value:Utf8]
              Limit: skip=0, fetch=1 [other_field:Float64;N, other_tag:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
                Sort: other_table.time DESC NULLS LAST [other_field:Float64;N, other_tag:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
                  Filter: other_table.other_field IS NOT NULL [other_field:Float64;N, other_tag:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
                    Filter: Boolean(NULL) [other_field:Float64;N, other_tag:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
                      TableScan: other_table [other_field:Float64;N, other_tag:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, None)]
        "#);

        let physical_plan = executor
            .new_context()
            .create_physical_plan(&plan)
            .await
            .expect("physical plan");
        let physical_plan = datafusion::physical_plan::displayable(physical_plan.as_ref());
        insta::assert_snapshot!(physical_plan.indent(true), @r"
        ProjectionExec: expr=[table as string_value]
          SortExec: TopK(fetch=1), expr=[time@0 DESC NULLS LAST], preserve_partitioning=[false]
            CoalesceBatchesExec: target_batch_size=8192
              FilterExec: field@1 IS NOT NULL AND tag@2 = MA, projection=[time@0]
                RecordBatchesExec: chunks=1, projection=[time, field, tag]
        ");
    }
}
