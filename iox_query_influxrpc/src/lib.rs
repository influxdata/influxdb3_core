//! Query frontend for InfluxDB Storage gRPC requests

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

use crate::schema::SeriesSchema;
use ::schema::{InfluxColumnType, InfluxFieldType, Projection, Schema, TIME_COLUMN_NAME};
use arrow::datatypes::{DataType, Field};
use async_trait::async_trait;
use data_types::ChunkId;
use datafusion::{
    catalog::SchemaProvider,
    error::DataFusionError,
    functions::core::get_field,
    functions_aggregate::{
        average::avg_udaf, count::count_udaf, expr_fn::max, min_max::max_udaf, sum::sum_udaf,
    },
    logical_expr::{col, expr::ScalarFunction, lit, LogicalPlan, LogicalPlanBuilder},
    prelude::{when, Column, Expr},
};
use plan::{string_value_schema, FieldAggregator, LogicalPlanBuilderExt};

use datafusion_util::{
    config::{DEFAULT_CATALOG, DEFAULT_SCHEMA},
    AsExpr,
};
use futures::{Stream, StreamExt, TryStreamExt};
use hashbrown::HashSet;
use iox_query::{
    exec::{build_schema_pivot, IOxSessionContext},
    provider::{ChunkTableProvider, ProviderBuilder},
    QueryChunk, QueryNamespace,
};
use observability_deps::tracing::{debug, warn};
use predicate::{
    rpc_predicate::{
        InfluxRpcPredicate, QueryNamespaceMeta, FIELD_COLUMN_NAME, GROUP_KEY_SPECIAL_START,
        GROUP_KEY_SPECIAL_STOP, MEASUREMENT_COLUMN_NAME,
    },
    Predicate,
};
use query_functions::{
    group_by::{Aggregate, WindowDuration},
    make_window_bound_expr,
    selectors::{selector_first, selector_last, selector_max, selector_min},
};
use snafu::{ensure, OptionExt, ResultExt, Snafu};
use std::collections::{BTreeMap, BTreeSet, HashSet as StdHashSet};
use std::iter::repeat;
use std::sync::Arc;

use crate::plan::fields_pivot_schema;

mod exec;
mod extension;
mod extension_planner;
mod physical_optimizer;
mod plan;
pub mod schema;

pub use extension::InfluxRpcExtension;

const CONCURRENT_TABLE_JOBS: usize = 10;

/// Name of the single column that is produced by queries
/// that form a set of string values.
const STRING_VALUE_COLUMN_NAME: &str = "string_value";

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "gRPC planner got error fetching chunks for table '{}': {}",
        table_name,
        source
    ))]
    GettingChunks {
        table_name: String,
        source: DataFusionError,
    },

    #[snafu(display(
        "gRPC planner got error checking if chunk {} could pass predicate: {}",
        chunk_id,
        source
    ))]
    CheckingChunkPredicate {
        chunk_id: ChunkId,
        source: DataFusionError,
    },

    #[snafu(display("gRPC planner got error creating predicates: {}", source))]
    CreatingPredicates { source: DataFusionError },

    #[snafu(display("gRPC planner got error building plan: {}", source))]
    BuildingPlan { source: DataFusionError },

    #[snafu(display("gRPC planner got error building table provider: {}", source))]
    BuildingTableProvider { source: iox_query::provider::Error },

    #[snafu(display("gRPC planner got error reading columns from expression: {}", source))]
    ReadColumns {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display(
        "gRPC planner error: column '{}' is not a tag, it is {:?}",
        tag_name,
        influx_column_type
    ))]
    InvalidTagColumn {
        tag_name: String,
        influx_column_type: Option<InfluxColumnType>,
    },

    #[snafu(display(
        "Internal error: tag column '{}' is not Utf8 type, it is {:?} ",
        tag_name,
        data_type
    ))]
    InternalInvalidTagType {
        tag_name: String,
        data_type: DataType,
    },

    #[snafu(display("Duplicate group column '{}'", column_name))]
    DuplicateGroupColumn { column_name: String },

    #[snafu(display(
        "Group column '{}' not found in tag columns: {:?}",
        column_name,
        all_tag_column_names
    ))]
    GroupColumnNotFound {
        column_name: String,
        all_tag_column_names: Vec<String>,
    },

    #[snafu(display("Error creating aggregate expression:  {}", source))]
    CreatingAggregates {
        source: query_functions::group_by::Error,
    },

    #[snafu(display(
        "gRPC planner got error casting aggregate {:?} for {}: {}",
        agg,
        field_name,
        source
    ))]
    CastingAggregates {
        agg: Aggregate,
        field_name: String,
        source: DataFusionError,
    },

    #[snafu(display("Internal error: unexpected aggregate request for None aggregate",))]
    InternalUnexpectedNoneAggregate {},

    #[snafu(display("Internal error: aggregate {:?} is not a selector", agg))]
    InternalAggregateNotSelector { agg: Aggregate },

    #[snafu(display("Table was removed while planning query: {}", table_name))]
    TableRemoved { table_name: String },

    #[snafu(display("Internal error: invalid schema for series"))]
    InternalInvalidArrowSchema {},
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    pub fn to_df_error(self, method: &'static str) -> DataFusionError {
        let msg = self.to_string();

        match self {
            Self::GettingChunks { source, .. }
            | Self::CreatingPredicates { source, .. }
            | Self::BuildingPlan { source, .. }
            | Self::ReadColumns { source, .. }
            | Self::CheckingChunkPredicate { source, .. }
            | Self::CastingAggregates { source, .. } => {
                DataFusionError::Context(format!("{method}: {msg}"), Box::new(source))
            }
            Self::TableRemoved { .. }
            | Self::InvalidTagColumn { .. }
            | Self::DuplicateGroupColumn { .. }
            | Self::BuildingTableProvider { .. }
            | Self::GroupColumnNotFound { .. } => DataFusionError::Plan(msg),
            e @ (Self::InternalInvalidTagType { .. }
            | Self::CreatingAggregates { .. }
            | Self::InternalUnexpectedNoneAggregate {}
            | Self::InternalAggregateNotSelector { .. }
            | Self::InternalInvalidArrowSchema {}) => DataFusionError::External(Box::new(e)),
        }
    }
}

impl From<DataFusionError> for Error {
    fn from(source: DataFusionError) -> Self {
        Self::BuildingPlan { source }
    }
}

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
#[derive(Debug)]
pub struct InfluxRpcPlanner {
    /// Optional executor currently only used to provide span context for tracing.
    ctx: IOxSessionContext,

    /// Namespace metadata.
    meta: Arc<NamespaceMeta>,
}

impl InfluxRpcPlanner {
    /// Create a new instance of the RPC planner
    pub fn new(ctx: IOxSessionContext) -> Self {
        let meta = Arc::new(NamespaceMeta::new(&ctx));
        Self { ctx, meta }
    }

    /// Returns a builder that includes
    ///   . A set of table names got from meta data that will participate
    ///      in the requested `predicate`
    ///   . A set of plans of tables of either
    ///       . chunks with deleted data or
    ///       . chunks without deleted data but cannot be decided from meta data
    pub async fn table_names(
        &self,
        namespace: Arc<dyn QueryNamespace>,
        rpc_predicate: InfluxRpcPredicate,
    ) -> Result<LogicalPlan> {
        let ctx = self.ctx.child_ctx("table_names planning");
        debug!(?rpc_predicate, "planning table_names");

        // Special case predicates that span the entire valid timestamp range
        let rpc_predicate = rpc_predicate.clear_timestamp_if_max_range();

        let table_predicates = rpc_predicate
            .table_predicates(ctx.inner(), self.meta.as_ref(), CONCURRENT_TABLE_JOBS)
            .await
            .context(CreatingPredicatesSnafu)?;
        let tables: Vec<_> =
            table_chunk_stream(Arc::clone(&namespace), false, &table_predicates, &ctx)
                .try_filter_map(|(table_name, schema, table_predicate, chunks)| async move {
                    Ok((!chunks.is_empty())
                        .then_some((table_name, Some((schema, table_predicate, chunks)))))
                })
                .try_collect()
                .await?;

        // Feed builder
        let mut names: Vec<Vec<Expr>> = vec![];
        let mut plans: Vec<LogicalPlan> = vec![];
        for (table_name, maybe_full_plan) in tables {
            match maybe_full_plan {
                None => {
                    names.push(vec![lit(table_name.to_string())]);
                }
                Some((schema, predicate, chunks)) => {
                    let provider = table_provider(Arc::clone(table_name), schema, chunks)?;
                    plans.push(Self::table_name_plan(Arc::new(provider), &predicate)?);
                }
            }
        }

        let mut builder = LogicalPlanBuilder::known_values(names, string_value_schema())?;
        for p in plans {
            builder = builder.union(p).context(BuildingPlanSnafu)?;
        }
        builder = builder
            .sort(vec![col(STRING_VALUE_COLUMN_NAME).sort(true, false)])
            .context(BuildingPlanSnafu)?;

        builder.build().context(BuildingPlanSnafu)
    }

    /// Returns a set of plans that produces the names of "tag" columns (as defined in the InfluxDB
    /// data model) names in this namespace that have more than zero rows which pass the conditions
    /// specified by `predicate`.
    pub async fn tag_keys(
        &self,
        namespace: Arc<dyn QueryNamespace>,
        rpc_predicate: InfluxRpcPredicate,
    ) -> Result<LogicalPlan> {
        let ctx = self.ctx.child_ctx("tag_keys planning");
        debug!(?rpc_predicate, "planning tag_keys");

        // Special case predicates that span the entire valid timestamp range
        let rpc_predicate = rpc_predicate.clear_timestamp_if_max_range();

        // The basic algorithm is:
        //
        // 1. Find all the potential tables in the chunks
        //
        // 2. For each table/chunk pair, figure out which can be found from only metadata and which
        //    need full plans

        let table_predicates = rpc_predicate
            .table_predicates(ctx.inner(), self.meta.as_ref(), CONCURRENT_TABLE_JOBS)
            .await
            .context(CreatingPredicatesSnafu)?;

        let mut table_predicates_need_chunks = vec![];
        let mut keys = BTreeSet::new();
        for (table_name, schema, predicate) in table_predicates {
            if predicate.is_empty() {
                let schema = schema.context(TableRemovedSnafu {
                    table_name: table_name.as_ref(),
                })?;

                // special case - return the columns from metadata only.
                // Note that columns with all rows deleted will still show here
                keys.extend(schema.tags_iter().map(|f| f.name().clone()));
            } else {
                table_predicates_need_chunks.push((table_name, schema, predicate));
            }
        }

        let tables: Vec<_> = table_chunk_stream(
            Arc::clone(&namespace),
            false,
            &table_predicates_need_chunks,
            &ctx,
        )
        .and_then(|(table_name, schema, predicate, chunks)| {
            let mut ctx = ctx.child_ctx("table");
            ctx.set_metadata("table", table_name.to_string());

            async move {
                let mut chunks_full = vec![];
                let mut known_columns = BTreeSet::new();

                for chunk in chunks {
                    // get only tag columns from metadata
                    let schema = chunk.schema();

                    let column_names: Vec<&str> = schema
                        .tags_iter()
                        .map(|f| f.name().as_str())
                        .collect::<Vec<&str>>();

                    let selection = Projection::Some(&column_names);

                    // filter the columns further from the predicate
                    let maybe_names = chunk_column_names(&chunk, &predicate, selection);

                    match maybe_names {
                        Some(mut names) => {
                            debug!(
                                %table_name,
                                names=?names,
                                chunk_id=%chunk.id().get(),
                                "column names found from metadata",
                            );
                            known_columns.append(&mut names);
                        }
                        None => {
                            debug!(
                                %table_name,
                                chunk_id=%chunk.id().get(),
                                "column names need full plan"
                            );
                            chunks_full.push(chunk);
                        }
                    }
                }

                Ok((table_name, schema, predicate, chunks_full, known_columns))
            }
        })
        .try_collect()
        .await?;

        let mut plans = vec![];
        // At this point, we have a set of column names we know pass
        // in `known_columns`, and potentially some tables in chunks
        // that we need to run a plan to know if they pass the
        // predicate.
        for (table_name, schema, predicate, chunks_full, mut known_columns) in tables {
            keys.append(&mut known_columns);

            if !chunks_full.is_empty() {
                // TODO an additional optimization here would be to filter
                // out chunks (and tables) where all columns in that chunk
                // were already known to have data (based on the contents of known_columns)

                let provider = table_provider(Arc::clone(table_name), schema.clone(), chunks_full)?;
                if let Some(plan) = Self::tag_keys_plan(Arc::new(provider), &predicate)? {
                    plans.push(plan);
                }
            }
        }

        let mut builder = LogicalPlanBuilder::known_values(
            keys.into_iter().map(|v| vec![lit(v)]).collect(),
            string_value_schema(),
        )?;
        for p in plans {
            builder = builder.union_distinct(p).context(BuildingPlanSnafu)?;
        }
        builder = builder
            .sort(vec![col(STRING_VALUE_COLUMN_NAME).sort(true, false)])
            .context(BuildingPlanSnafu)?;

        builder.build().context(BuildingPlanSnafu)
    }

    /// Returns a plan which finds the distinct, non-null tag values in the specified `tag_name`
    /// column of this namespace which pass the conditions specified by `predicate`.
    pub async fn tag_values(
        &self,
        namespace: Arc<dyn QueryNamespace>,
        tag_name: &str,
        rpc_predicate: InfluxRpcPredicate,
    ) -> Result<LogicalPlan> {
        let ctx = self.ctx.child_ctx("tag_values planning");
        debug!(?rpc_predicate, tag_name, "planning tag_values");

        // The basic algorithm is:
        //
        // 1. Find all the potential tables in the chunks
        //
        // 2. For each table/chunk pair, figure out which have
        // distinct values that can be found from only metadata and
        // which need full plans

        let table_predicates = rpc_predicate
            .table_predicates(ctx.inner(), self.meta.as_ref(), CONCURRENT_TABLE_JOBS)
            .await
            .context(CreatingPredicatesSnafu)?;

        // filter out tables that do NOT contain `tag_name` early, esp. before performing any chunk
        // scan (which includes ingester RPC)
        let mut table_predicates_filtered = Vec::with_capacity(table_predicates.len());
        for (table_name, schema, predicate) in table_predicates {
            let schema = schema.context(TableRemovedSnafu {
                table_name: table_name.as_ref(),
            })?;

            // Skip this table if the tag_name is not a column in this table
            if schema.find_index_of(tag_name).is_none() {
                continue;
            };

            table_predicates_filtered.push((table_name, Some(schema), predicate));
        }

        let tables: Vec<_> = table_chunk_stream(
            Arc::clone(&namespace),
            false,
            &table_predicates_filtered,
            &ctx,
        )
        .and_then(|(table_name, schema, predicate, chunks)| async move {
            let mut chunks_full = vec![];

            for chunk in chunks {
                // use schema to validate column type
                let schema = chunk.schema();

                // Skip this table if the tag_name is not a column in this chunk
                // Note: This may happen even when the table contains the tag_name, because some
                // chunks may not contain all columns.
                let idx = if let Some(idx) = schema.find_index_of(tag_name) {
                    idx
                } else {
                    continue;
                };

                // Validate that this really is a Tag column
                let (influx_column_type, field) = schema.field(idx);
                ensure!(
                    influx_column_type == InfluxColumnType::Tag,
                    InvalidTagColumnSnafu {
                        tag_name,
                        influx_column_type,
                    }
                );
                ensure!(
                    influx_column_type.valid_arrow_type(field.data_type()),
                    InternalInvalidTagTypeSnafu {
                        tag_name,
                        data_type: field.data_type().clone(),
                    }
                );

                debug!(
                    %table_name,
                    chunk_id=%chunk.id().get(),
                    "need full plan to find tag values"
                );
                chunks_full.push(chunk);
            }

            Ok((table_name, schema, predicate, chunks_full))
        })
        .try_collect()
        .await?;

        // At this point, we have a set of tables in chunks that we
        // need to run a plan to find what values pass the predicate.
        let plans = tables
            .into_iter()
            .filter(|(_, _, _, chunks)| !chunks.is_empty())
            .map(|(table_name, schema, predicate, chunks)| {
                let provider = table_provider(Arc::clone(table_name), schema.clone(), chunks)?;
                Self::tag_values_plan(Arc::new(provider), &predicate, tag_name)
                    .context(BuildingPlanSnafu)
            })
            .collect::<Result<Vec<_>>>()?;

        let mut builder = LogicalPlanBuilder::known_values(vec![], string_value_schema())?;
        for p in plans {
            builder = builder.union_distinct(p).context(BuildingPlanSnafu)?;
        }
        builder = builder.sort(vec![STRING_VALUE_COLUMN_NAME.as_expr().sort(true, false)])?;
        builder.build().context(BuildingPlanSnafu)
    }

    /// Returns a plan that produces a list of columns and their datatypes (as defined in the data
    /// written via `write_lines`), and which have more than zero rows which pass the conditions
    /// specified by `predicate`.
    pub async fn field_columns(
        &self,
        namespace: Arc<dyn QueryNamespace>,
        rpc_predicate: InfluxRpcPredicate,
    ) -> Result<LogicalPlan> {
        let ctx = self.ctx.child_ctx("field_columns planning");
        debug!(?rpc_predicate, "planning field_columns");

        // Special case predicates that span the entire valid timestamp range
        let rpc_predicate = rpc_predicate.clear_timestamp_if_max_range();

        // Algorithm is to run a "select field_cols from table where
        // <predicate> type plan for each table in the chunks"
        //
        // The executor then figures out which columns have non-null
        // values and stops the plan executing once it has them

        let table_predicates = rpc_predicate
            .table_predicates(ctx.inner(), self.meta.as_ref(), CONCURRENT_TABLE_JOBS)
            .await
            .context(CreatingPredicatesSnafu)?;

        // optimization: just get the field columns from metadata.
        // note this both ignores field keys, and sets the timestamp data 'incorrectly'.
        let mut fields: BTreeMap<String, InfluxFieldType> = BTreeMap::new();
        let mut table_predicates_need_chunks = Vec::with_capacity(table_predicates.len());
        for (table_name, schema, predicate) in table_predicates {
            if predicate.is_empty() {
                let schema = schema.context(TableRemovedSnafu {
                    table_name: table_name.as_ref(),
                })?;
                for (r#type, field) in schema.iter() {
                    if let InfluxColumnType::Field(r#type) = r#type {
                        fields.insert(field.name().into(), r#type);
                    }
                }
            } else {
                table_predicates_need_chunks.push((table_name, schema, predicate));
            }
        }

        // full scans
        let plans = create_plans(
            namespace,
            &table_predicates_need_chunks,
            ctx,
            |table_name, predicate, chunks, schema| {
                let provider = table_provider(table_name, schema.clone(), chunks)?;
                Self::field_columns_plan(Arc::new(provider), predicate).context(BuildingPlanSnafu)
            },
        )
        .await?;

        let mut builder = LogicalPlanBuilder::known_values(
            fields
                .into_iter()
                .map(|(name, r#type)| vec![lit(name), lit(r#type as i32), lit(0_i64)])
                .collect::<Vec<_>>(),
            fields_pivot_schema(),
        )
        .context(BuildingPlanSnafu)?;
        for plan in plans {
            builder = builder.union(plan).context(BuildingPlanSnafu)?
        }
        builder = builder
            .aggregate(
                vec![col("key"), col("type")],
                vec![max(col("timestamp")).alias("timestamp")],
            )
            .context(BuildingPlanSnafu)?
            .sort(vec![col("key").sort(true, false)])
            .context(BuildingPlanSnafu)?;

        builder.build().context(BuildingPlanSnafu)
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
    pub async fn read_filter(
        &self,
        namespace: Arc<dyn QueryNamespace>,
        rpc_predicate: InfluxRpcPredicate,
    ) -> Result<LogicalPlan> {
        let ctx = self.ctx.child_ctx("planning_read_filter");
        debug!(?rpc_predicate, "planning read_filter");

        let mut table_predicates = rpc_predicate
            .table_predicates(ctx.inner(), self.meta.as_ref(), CONCURRENT_TABLE_JOBS)
            .await
            .context(CreatingPredicatesSnafu)?;

        // The measurement (table) name is the first element of the sort key.
        // Let's generate them in the correct order to avoid needing a sort later.
        table_predicates
            .as_mut_slice()
            .sort_unstable_by(|(name1, _, _), (name2, _, _)| name1.cmp(name2));

        let plans = create_plans(
            namespace,
            &table_predicates,
            ctx,
            |table_name, predicate, chunks, schema| {
                let provider = table_provider(table_name, schema.clone(), chunks)?;
                Self::read_filter_plan(Arc::new(provider), predicate)
            },
        )
        .await?;

        let mut builder = LogicalPlanBuilder::union_series(plans).context(BuildingPlanSnafu)?;
        let schema = SeriesSchema::try_from(builder.schema())?;
        builder = builder.sort(schema.sort(&[])).context(BuildingPlanSnafu)?;
        builder.build().context(BuildingPlanSnafu)
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
        namespace: Arc<dyn QueryNamespace>,
        rpc_predicate: InfluxRpcPredicate,
        agg: Aggregate,
        group_columns: &[impl AsRef<str> + Send + Sync],
    ) -> Result<LogicalPlan> {
        let ctx = self.ctx.child_ctx("read_group planning");
        debug!(?rpc_predicate, ?agg, "planning read_group");

        let table_predicates = rpc_predicate
            .table_predicates(ctx.inner(), self.meta.as_ref(), CONCURRENT_TABLE_JOBS)
            .await
            .context(CreatingPredicatesSnafu)?;

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
                    return Err(Error::DuplicateGroupColumn {
                        column_name: group_col.to_string(),
                    });
                }
                hashbrown::hash_set::Entry::Vacant(v) => {
                    v.insert();
                }
            }
        }

        let plans = create_plans(
            namespace,
            &table_predicates,
            ctx,
            |table_name, predicate, chunks, schema| {
                // check group_columns for unknown columns
                let known_tags_vec = schema
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

                    ensure!(
                        known_tags_set.contains(group_col.as_ref()),
                        GroupColumnNotFoundSnafu {
                            column_name: group_col.as_ref(),
                            all_tag_column_names: known_tags_vec.clone(),
                        }
                    );
                }
                let provider = table_provider(table_name, schema.clone(), chunks)?;
                Self::read_group_plan(Arc::new(provider), predicate, agg)
            },
        )
        .await?;
        let mut builder = LogicalPlanBuilder::union_series(plans)?;
        let schema = SeriesSchema::try_from(builder.schema())?;
        builder = builder.sort(schema.sort(&group_columns))?;

        Ok(builder.build()?)
    }

    /// Creates a GroupedSeriesSet plan that produces an output table with rows
    /// that are grouped by window definitions
    pub async fn read_window_aggregate(
        &self,
        namespace: Arc<dyn QueryNamespace>,
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

        let table_predicates = rpc_predicate
            .table_predicates(ctx.inner(), self.meta.as_ref(), CONCURRENT_TABLE_JOBS)
            .await
            .context(CreatingPredicatesSnafu)?;

        let plans = create_plans(
            namespace,
            &table_predicates,
            ctx,
            |table_name, predicate, chunks, schema| {
                let provider = table_provider(table_name, schema.clone(), chunks)?;
                Self::read_window_aggregate_plan(Arc::new(provider), predicate, agg, every, offset)
            },
        )
        .await?;
        let mut builder = LogicalPlanBuilder::union_series(plans)?;
        let schema = SeriesSchema::try_from(builder.schema())?;
        builder = builder.sort(schema.sort(&[]))?;

        Ok(builder.build()?)
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
        provider: Arc<ChunkTableProvider>,
        predicate: &Predicate,
    ) -> Result<Option<LogicalPlan>, DataFusionError> {
        // select only the tag columns
        let select_exprs = provider
            .iox_schema()
            .iter()
            .filter_map(|(influx_column_type, field)| {
                if influx_column_type == InfluxColumnType::Tag {
                    Some(field.name().as_expr())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        // If the projection is empty then there is no plan to execute.
        if select_exprs.is_empty() {
            return Ok(None);
        }

        let mut builder = Arc::clone(&provider).into_logical_plan_builder()?;
        if let Some(predicate) = predicate.filter_expr() {
            builder = builder.filter(predicate.clone())?;
        }
        builder = builder.project(select_exprs)?;
        // And finally pivot the plan
        let plan = build_schema_pivot(builder)?
            .project(vec![col("non_null_column").alias(STRING_VALUE_COLUMN_NAME)])?
            .build()?;

        debug!(table_name=provider.table_name(), plan=%plan.display_indent_schema(),
               "created column_name plan for table");

        Ok(Some(plan))
    }

    /// Creates a DataFusion LogicalPlan that returns the distinct values
    /// for a specific tag column in a table.
    fn tag_values_plan(
        provider: Arc<ChunkTableProvider>,
        predicate: &Predicate,
        key: &str,
    ) -> Result<LogicalPlan, DataFusionError> {
        let mut builder = provider.into_logical_plan_builder()?;
        if let Some(predicate) = predicate.filter_expr() {
            builder = builder.filter(predicate)?;
        }
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
    /// returns `None` if the table contains no rows that would pass
    /// the predicate.
    ///
    /// The created plan looks like:
    ///
    /// ```text
    ///  Projection (select the field columns needed)
    ///      Filter(predicate) [optional]
    ///        Scan
    /// ```
    fn field_columns_plan(
        provider: Arc<ChunkTableProvider>,
        predicate: &Predicate,
    ) -> Result<LogicalPlan, DataFusionError> {
        let mut builder = Arc::clone(&provider).into_logical_plan_builder()?;
        if let Some(predicate) = predicate.filter_expr() {
            builder = builder.filter(predicate.clone())?;
        }
        // Selection of only fields and time
        let select_exprs =
            provider
                .iox_schema()
                .iter()
                .filter_map(|(influx_column_type, field)| match influx_column_type {
                    InfluxColumnType::Field(_) => Some(field.name().as_expr()),
                    InfluxColumnType::Timestamp => Some(field.name().as_expr()),
                    InfluxColumnType::Tag => None,
                });
        builder = builder.project(select_exprs)?;
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
    fn table_name_plan(
        provider: Arc<ChunkTableProvider>,
        predicate: &Predicate,
    ) -> Result<LogicalPlan, DataFusionError> {
        debug!(
            table_name = provider.table_name(),
            "Creating table_name full plan"
        );

        let mut builder = Arc::clone(&provider).into_logical_plan_builder()?;
        if let Some(predicate) = predicate.filter_expr() {
            builder = builder.filter(predicate.clone())?;
        }
        if let Some(field_exprs) = filtered_fields_iter(provider.iox_schema(), predicate)
            .map(|fe| fe.expr.is_not_null())
            .reduce(|left, right| left.or(right))
        {
            builder = builder.filter(field_exprs)?;
        }
        builder = builder.sort([col(TIME_COLUMN_NAME).sort(false, false)])?;
        builder = builder.limit(0, Some(1))?;
        builder = builder.project(vec![
            lit(String::from(provider.table_name())).alias(STRING_VALUE_COLUMN_NAME)
        ])?;
        builder.build()
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
    fn read_filter_plan(
        provider: Arc<ChunkTableProvider>,
        predicate: &Predicate,
    ) -> Result<LogicalPlan> {
        Self::read_group_plan(provider, predicate, Aggregate::None)
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
        provider: Arc<ChunkTableProvider>,
        predicate: &Predicate,
        aggregate: Aggregate,
    ) -> Result<LogicalPlan> {
        let schema = provider.iox_schema();

        let measurement_expr = lit(provider.table_name());
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
        let time_expr = schema.time_iter().map(field_col).next().unwrap();
        let (field_exprs, field_columns) = filtered_fields_iter(schema, predicate)
            .map(|fe| {
                (
                    fe.expr,
                    Expr::Column(Column {
                        relation: None,
                        name: fe.name.to_string(),
                    }),
                )
            })
            .unzip::<_, _, Vec<_>, Vec<_>>();

        let mut builder = Arc::clone(&provider).into_logical_plan_builder()?;
        if let Some(expr) = predicate.filter_expr() {
            builder = builder.filter(expr)?;
        }

        let (mut builder, time_exprs, field_exprs) = match aggregate {
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
                    repeat(&time_expr)
                        .take(field_columns.len())
                        .cloned()
                        .collect::<Vec<_>>(),
                    field_columns,
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
                    field_columns
                        .iter()
                        .map(|expr| {
                            Expr::ScalarFunction(ScalarFunction {
                                func: get_field(),
                                args: vec![expr.clone(), lit("time")],
                            })
                        })
                        .collect(),
                    field_columns
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
                builder,
                repeat(&time_expr)
                    .take(field_columns.len())
                    .cloned()
                    .collect::<Vec<_>>(),
                field_exprs,
            ),
        };
        builder = builder.series_pivot(measurement_expr, tag_exprs, time_exprs, field_exprs)?;
        Ok(builder.build()?)
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
    #[allow(clippy::too_many_arguments)]
    fn read_window_aggregate_plan(
        provider: Arc<ChunkTableProvider>,
        predicate: &Predicate,
        aggregate: Aggregate,
        every: WindowDuration,
        offset: WindowDuration,
    ) -> Result<LogicalPlan> {
        let schema = provider.iox_schema();

        let measurement_expr = lit(provider.table_name());
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
        let time_expr = schema.time_iter().map(field_col).next().unwrap();
        let (field_exprs, field_columns) = filtered_fields_iter(schema, predicate)
            .map(|fe| {
                (
                    fe.expr,
                    Expr::Column(Column {
                        relation: None,
                        name: fe.name.to_string(),
                    }),
                )
            })
            .unzip::<_, _, Vec<_>, Vec<_>>();
        // Group by all tag columns and the window bounds
        let window_bound = make_window_bound_expr(TIME_COLUMN_NAME.as_expr(), every, offset)
            .alias(TIME_COLUMN_NAME);

        let mut builder = Arc::clone(&provider).into_logical_plan_builder()?;
        if let Some(expr) = predicate.filter_expr() {
            builder = builder.filter(expr)?;
        }
        let mut group_expr = tag_exprs.clone();
        group_expr.push(window_bound);

        let (mut builder, time_exprs, field_exprs) = match aggregate {
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
                    repeat(&time_expr)
                        .take(field_columns.len())
                        .cloned()
                        .collect::<Vec<_>>(),
                    field_columns,
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
                    field_columns
                        .iter()
                        .map(|expr| {
                            Expr::ScalarFunction(ScalarFunction {
                                func: get_field(),
                                args: vec![expr.clone(), lit("time")],
                            })
                        })
                        .collect(),
                    field_columns
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
            Aggregate::None => InternalUnexpectedNoneAggregateSnafu.fail()?,
        };
        builder = builder.series_pivot(measurement_expr, tag_exprs, time_exprs, field_exprs)?;
        Ok(builder.build()?)
    }
}

/// Stream of chunks for table predicates.
/// This function is used by influx grpc meta queries that want to know which table/tags/fields
/// that match the given predicates.
/// `need_fields` means the grpc queries will need to return field columns. If  `need_fields`
/// is false, the grpc query does not need to return field columns but it still filters data on the
/// field columns in the predicate
///
/// This function is directly invoked by `table_name, `tag_keys` and `tag_values` where need_fields should be false.
/// This function is indirectly invoked by `field_columns`, `read_filter`, `read_group` and `read_window_aggregate`
/// through the function `create_plans` where need_fields should be true.
fn table_chunk_stream<'a>(
    namespace: Arc<dyn QueryNamespace>,
    need_fields: bool,
    table_predicates: &'a [(Arc<str>, Option<Schema>, Predicate)],
    ctx: &'a IOxSessionContext,
) -> impl Stream<Item = Result<(&'a Arc<str>, Schema, Predicate, Vec<Arc<dyn QueryChunk>>)>> + 'a {
    futures::stream::iter(table_predicates)
        .filter_map(move |(table_name, table_schema, predicate)| async move {
            let table_schema = table_schema.as_ref()?.clone();
            Some((table_name, table_schema, predicate))
        })
        .map(move |(table_name, table_schema, predicate)| {
            let mut ctx = ctx.child_ctx("table");
            ctx.set_metadata("table", table_name.to_string());

            let namespace = Arc::clone(&namespace);

            async move {
                let predicate = match namespace.retention_time_ns() {
                    Some(ret) => predicate.clone().with_retention(ret),
                    None => predicate.clone(),
                };
                let filters = predicate.filter_expr().into_iter().collect::<Vec<_>>();
                let projection =
                    columns_in_predicates(need_fields, &table_schema, table_name, &predicate);

                let mut chunks = namespace
                    .chunks(
                        table_name,
                        &filters,
                        projection.as_ref(),
                        ctx.child_ctx("table chunks"),
                    )
                    .await
                    .context(GettingChunksSnafu {
                        table_name: table_name.as_ref(),
                    })?;

                // if there is a field restriction on the predicate, only
                // chunks with that field should be returned. If the chunk has
                // none of the fields specified, then it doesn't match
                // TODO: test this branch
                if let Some(field_columns) = &predicate.field_columns {
                    chunks.retain(|chunk| {
                        let schema = chunk.schema();
                        // keep chunk if it has any of the columns requested
                        field_columns
                            .iter()
                            .any(|col| schema.find_index_of(col).is_some())
                    })
                }

                Ok((table_name, table_schema.clone(), predicate, chunks))
            }
        })
        .buffered(CONCURRENT_TABLE_JOBS)
}

// Return all columns in predicate's field_columns, exprs and val_exprs.
// Return None means nothing is filtered in this function and all field columns should be used.
// None is returned when:
//   - we cannot determine at least one column in the predicate
//   - need_fields is true and the predicate does not have any field_columns.
//     This signal that all fields are needed.
// Note that the returned columns can also include tag and time columns if they happen to be
// in the predicate.
fn columns_in_predicates(
    need_fields: bool,
    table_schema: &Schema,
    table_name: &str,
    predicate: &Predicate,
) -> Option<Vec<usize>> {
    // columns in field_columns
    let columns: StdHashSet<Column> = match &predicate.field_columns {
        Some(field_columns) => field_columns
            .iter()
            .map(Column::from_name)
            .collect::<StdHashSet<_>>(),
        None => {
            if need_fields {
                // fields wanted and `field_columns` is empty mean al fields will be needed
                return None;
            } else {
                StdHashSet::new()
            }
        }
    };

    let mut columns: StdHashSet<&Column> = columns.iter().collect::<StdHashSet<_>>();

    // columns in exprs
    predicate
        .exprs
        .iter()
        .for_each(|expr| expr.add_column_refs(&mut columns));

    // columns in val_exprs
    let exprs: Vec<Expr> = predicate
        .value_expr
        .iter()
        .map(|e| Expr::from((*e).clone()))
        .collect();
    exprs
        .iter()
        .for_each(|expr| expr.add_column_refs(&mut columns));

    // convert the column names into their corresponding indexes in the schema
    if columns.is_empty() {
        return None;
    }

    let mut indices = Vec::with_capacity(columns.len());
    for c in columns {
        if let Some(idx) = table_schema.find_index_of(&c.name) {
            indices.push(idx);
        } else {
            warn!(
                table_name,
                column=c.name.as_str(),
                table_columns=?table_schema.iter().map(|(_t, f)| f.name()).collect::<Vec<_>>(),
                "cannot find predicate column (field column, value expr, filter expression) table schema",
            );
            return None;
        }
    }
    Some(indices)
}

/// Create plans that fetch the data specified in table_predicates.
///
/// table_predicates contains `(table_name, predicate_specialized_for_that_table)`
///
/// The plans are created in parallel as different `async` Tasks to reduce
/// query latency due to planning
///
/// `f(ctx, table_name, table_predicate, chunks, table_schema)` is
///  invoked on the chunks for each table to produce a plan for each
async fn create_plans<F, P>(
    namespace: Arc<dyn QueryNamespace>,
    table_predicates: &[(Arc<str>, Option<Schema>, Predicate)],
    ctx: IOxSessionContext,
    f: F,
) -> Result<Vec<P>>
where
    F: for<'a> Fn(&'a str, &'a Predicate, Vec<Arc<dyn QueryChunk>>, &'a Schema) -> Result<P>
        + Clone
        + Send
        + Sync,
    P: Send,
{
    table_chunk_stream(namespace, true, table_predicates, &ctx)
        .try_filter_map(|(table_name, schema, predicate, chunks)| async move {
            Ok((!chunks.is_empty()).then_some((table_name, schema, predicate, chunks)))
                as Result<Option<(&Arc<str>, Schema, Predicate, Vec<_>)>>
        })
        .and_then(|(table_name, schema, predicate, chunks)| {
            let mut ctx = ctx.child_ctx("table");
            ctx.set_metadata("table", table_name.to_string());

            let f = f.clone();

            async move { f(table_name, &predicate, chunks, &schema) }
        })
        .try_collect()
        .await
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

fn chunk_column_names(
    chunk: &Arc<dyn QueryChunk>,
    predicate: &Predicate,
    columns: Projection<'_>,
) -> Option<BTreeSet<String>> {
    if !predicate.is_empty() {
        // if there is anything in the predicate, bail for now and force a full plan
        return None;
    }

    let fields = chunk.schema().inner().fields().iter();

    Some(match columns {
        Projection::Some(cols) => fields
            .filter_map(|x| {
                if cols.contains(&x.name().as_str()) {
                    Some(x.name().clone())
                } else {
                    None
                }
            })
            .collect(),
        Projection::All => fields.map(|x| x.name().clone()).collect(),
    })
}

struct NamespaceMeta {
    schema_provider: Arc<dyn SchemaProvider>,
}

impl NamespaceMeta {
    fn new(ctx: &IOxSessionContext) -> Self {
        let schema_provider = ctx
            .inner()
            .catalog(DEFAULT_CATALOG)
            .expect("default catalog exists")
            .schema(DEFAULT_SCHEMA)
            .expect("default schema exists");

        Self { schema_provider }
    }
}

impl std::fmt::Debug for NamespaceMeta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NamespaceMeta")
            .field("schema_provider", &"<Provider>")
            .finish()
    }
}

/// Create a table provider for the specified table name, schema, and chunks.
fn table_provider(
    table_name: impl Into<Arc<str>>,
    schema: Schema,
    chunks: impl IntoIterator<Item = Arc<dyn QueryChunk>>,
) -> Result<ChunkTableProvider> {
    let mut builder = ProviderBuilder::new(table_name.into(), schema);
    for chunk in chunks {
        builder = builder.add_chunk(chunk);
    }
    builder.build().context(BuildingTableProviderSnafu)
}

#[async_trait]
impl QueryNamespaceMeta for NamespaceMeta {
    fn table_names(&self) -> Vec<String> {
        self.schema_provider.table_names()
    }

    async fn table_schema(&self, table_name: &str) -> Option<Schema> {
        let table_provider = self
            .schema_provider
            .table(table_name)
            .await
            .expect("get table provider")?;
        let schema: Schema = table_provider
            .schema()
            .try_into()
            .expect("valid IOx schema");
        Some(schema)
    }
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
    use datafusion::{
        common::ScalarValue,
        prelude::{col, lit},
    };
    use datafusion_util::lit_dict;
    use futures::{future::BoxFuture, FutureExt};
    use predicate::Predicate;

    use iox_query::{
        exec::Executor,
        test::{TestChunk, TestDatabase},
    };
    use test_helpers::maybe_start_logging;

    use super::*;

    #[test]
    fn test_columns_in_predicates() {
        // setup a db
        let chunk0 = Arc::new(
            TestChunk::new("h2o")
                .with_id(0)
                .with_tag_column("foo")
                .with_tag_column("bar")
                .with_i64_field_column("i64_field")
                .with_i64_field_column("i64_field_2")
                .with_time_column()
                .with_one_row_of_data(),
        );
        // index of columns in the above chunk: [bar, foo, i64_field, i64_field_2, time]
        let executor = Arc::new(Executor::new_testing());
        let test_db = Arc::new(TestDatabase::new(Arc::clone(&executor)));
        test_db.add_chunk("my_partition_key", Arc::clone(&chunk0));
        let table = "h2o";
        let schema = chunk0.schema();

        // test 1: empty predicate without need_fields
        let predicate = Predicate::new();
        let need_fields = false;
        let projection = columns_in_predicates(need_fields, schema, table, &predicate);
        assert_eq!(projection, None);

        // test 2: empty predicate with need_fields
        let need_fields = true;
        let projection = columns_in_predicates(need_fields, schema, table, &predicate);
        assert_eq!(projection, None);

        // test 3: predicate on tag without need_fields
        let predicate = Predicate::new().with_expr(col("foo").eq(lit("some_thing")));
        let need_fields = false;
        let projection = columns_in_predicates(need_fields, schema, table, &predicate).unwrap();
        // return index of foo
        assert_eq!(projection, vec![1]);

        // test 4: predicate on tag with need_fields
        let need_fields = true;
        let projection = columns_in_predicates(need_fields, schema, table, &predicate);
        // return None means all fields
        assert_eq!(projection, None);

        // test 5: predicate on tag with field_columns without need_fields
        let predicate = Predicate::new()
            .with_expr(col("foo").eq(lit("some_thing")))
            .with_field_columns(vec!["i64_field".to_string()])
            .unwrap();
        let need_fields = false;
        let mut projection = columns_in_predicates(need_fields, schema, table, &predicate).unwrap();
        projection.sort();
        // return indexes of i64_field and foo
        assert_eq!(projection, vec![1, 2]);

        // test 6: predicate on tag with field_columns with need_fields
        let need_fields = true;
        let mut projection = columns_in_predicates(need_fields, schema, table, &predicate).unwrap();
        projection.sort();
        // return indexes of foo and index of i64_field
        assert_eq!(projection, vec![1, 2]);

        // test 7: predicate on tag and field with field_columns without need_fields
        let predicate = Predicate::new()
            .with_expr(col("bar").eq(lit(1)).and(col("i64_field").eq(lit(1))))
            .with_field_columns(vec!["i64_field".to_string()])
            .unwrap();
        let need_fields = false;
        let mut projection = columns_in_predicates(need_fields, schema, table, &predicate).unwrap();
        projection.sort();
        // return indexes of bard and i64_field
        assert_eq!(projection, vec![0, 2]);

        // test 7: predicate on tag and field with field_columns with need_fields
        let need_fields = true;
        let mut projection = columns_in_predicates(need_fields, schema, table, &predicate).unwrap();
        projection.sort();
        // return indexes of bard and i64_field
        assert_eq!(projection, vec![0, 2]);
    }

    #[tokio::test]
    async fn test_table_chunk_stream_no_field_columns() {
        let chunk0 = Arc::new(
            TestChunk::new("h2o")
                .with_id(0)
                .with_tag_column("foo")
                .with_tag_column("bar")
                .with_i64_field_column("i64_field")
                .with_i64_field_column("i64_field_2")
                .with_time_column()
                .with_one_row_of_data(),
        );

        let executor = Arc::new(Executor::new_testing());
        let test_db = Arc::new(TestDatabase::new(Arc::clone(&executor)));
        test_db.add_chunk("my_partition_key", Arc::clone(&chunk0));
        let ctx = test_db.new_query_context(None, None);

        // predicate has no field_columns
        // predicate on a tag column `foo`
        let expr = col("foo").eq(lit("some_thing"));
        let predicate = Predicate::new().with_expr(expr);
        let table_predicates = vec![(Arc::from("h2o"), Some(chunk0.schema().clone()), predicate)];

        ////////////////////////////
        // Test 1: need_fields --> all columns will be selected
        let need_fields = true;

        let result = table_chunk_stream(test_db, need_fields, &table_predicates, &ctx)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert!(!result.is_empty());
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0.as_ref(), "h2o"); // table name
        assert_eq!(result[0].3.len(), 1); // returned chunks

        // chunk schema includes  all 5 columns of the table because we asked it return all fileds (and implicit PK) even though the predicate is on `foo` only
        let chunk = &result[0].3[0];
        let chunk_schema = (*chunk.schema()).clone();
        assert_eq!(chunk_schema.len(), 5);
        let chunk_schema = chunk_schema.sort_fields_by_name();
        assert_eq!(chunk_schema.field(0).1.name(), "bar");
        assert_eq!(chunk_schema.field(1).1.name(), "foo");
        assert_eq!(chunk_schema.field(2).1.name(), "i64_field");
        assert_eq!(chunk_schema.field(3).1.name(), "i64_field_2");
        assert_eq!(chunk_schema.field(4).1.name(), TIME_COLUMN_NAME);

        ////////////////////////////
        // Test 2: no need_fields
        let need_fields = false;

        let test_db = Arc::new(TestDatabase::new(Arc::clone(&executor)));
        test_db.add_chunk("my_partition_key", Arc::clone(&chunk0));
        let ctx = test_db.new_query_context(None, None);
        let result = table_chunk_stream(test_db, need_fields, &table_predicates, &ctx)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert!(!result.is_empty());
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0.as_ref(), "h2o"); // table name
        assert_eq!(result[0].3.len(), 1); // returned chunks

        // chunk schema includes still includes everything (the test table implementation does NOT project chunks)
        let chunk = &result[0].3[0];
        let chunk_schema = (*chunk.schema()).clone();
        assert_eq!(chunk_schema.len(), 5);
        let chunk_schema = chunk_schema.sort_fields_by_name();
        assert_eq!(chunk_schema.field(0).1.name(), "bar");
        assert_eq!(chunk_schema.field(1).1.name(), "foo");
        assert_eq!(chunk_schema.field(2).1.name(), "i64_field");
        assert_eq!(chunk_schema.field(3).1.name(), "i64_field_2");
    }

    #[tokio::test]
    async fn test_table_chunk_stream_empty_pred() {
        let chunk0 = Arc::new(
            TestChunk::new("h2o")
                .with_id(0)
                .with_tag_column("foo")
                .with_tag_column("bar")
                .with_i64_field_column("i64_field")
                .with_i64_field_column("i64_field_2")
                .with_time_column()
                .with_one_row_of_data(),
        );

        let executor = Arc::new(Executor::new_testing());
        let test_db = Arc::new(TestDatabase::new(Arc::clone(&executor)));
        test_db.add_chunk("my_partition_key", Arc::clone(&chunk0));
        let ctx = test_db.new_query_context(None, None);

        // empty predicate
        let predicate = Predicate::new();
        let table_predicates = vec![(Arc::from("h2o"), Some(chunk0.schema().clone()), predicate)];

        /////////////
        // Test 1: empty predicate with need_fields
        let need_fields = true;
        let result = table_chunk_stream(test_db, need_fields, &table_predicates, &ctx)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert!(!result.is_empty());
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0.as_ref(), "h2o"); // table name
        assert_eq!(result[0].3.len(), 1); // returned chunks

        // chunk schema includes  all 5 columns of the table because the preidcate is empty
        let chunk = &result[0].3[0];
        let chunk_schema = (*chunk.schema()).clone();
        assert_eq!(chunk_schema.len(), 5);
        let chunk_schema = chunk_schema.sort_fields_by_name();
        assert_eq!(chunk_schema.field(0).1.name(), "bar");
        assert_eq!(chunk_schema.field(1).1.name(), "foo");
        assert_eq!(chunk_schema.field(2).1.name(), "i64_field");
        assert_eq!(chunk_schema.field(3).1.name(), "i64_field_2");
        assert_eq!(chunk_schema.field(4).1.name(), TIME_COLUMN_NAME);

        /////////////
        // Test 2: empty predicate without need_fields
        let need_fields = false;
        let test_db = Arc::new(TestDatabase::new(Arc::clone(&executor)));
        test_db.add_chunk("my_partition_key", Arc::clone(&chunk0));
        let ctx = test_db.new_query_context(None, None);
        let result = table_chunk_stream(test_db, need_fields, &table_predicates, &ctx)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert!(!result.is_empty());
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0.as_ref(), "h2o"); // table name
        assert_eq!(result[0].3.len(), 1); // returned chunks

        // chunk schema includes  all 5 columns of the table because the preidcate is empty
        let chunk = &result[0].3[0];
        let chunk_schema = (*chunk.schema()).clone();
        assert_eq!(chunk_schema.len(), 5);
    }

    #[tokio::test]
    async fn test_table_chunk_stream_pred_on_tag_no_data() {
        let chunk0 = Arc::new(
            TestChunk::new("h2o")
                .with_id(0)
                .with_tag_column("foo")
                .with_tag_column("bar")
                .with_i64_field_column("i64_field")
                .with_i64_field_column("i64_field_2")
                .with_time_column(), // no row added for this chunk on purpose
        );

        let executor = Arc::new(Executor::new_testing());
        let test_db = Arc::new(TestDatabase::new(Arc::clone(&executor)));
        test_db.add_chunk("my_partition_key", Arc::clone(&chunk0));
        let ctx = test_db.new_query_context(None, None);

        // predicate on a tag column `foo`
        let expr = col("foo").eq(lit("some_thing"));
        let predicate = Predicate::new().with_expr(expr);
        let table_predicates = vec![(Arc::from("h2o"), Some(chunk0.schema().clone()), predicate)];

        let need_fields = false;
        let result = table_chunk_stream(test_db, need_fields, &table_predicates, &ctx)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert!(!result.is_empty());
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0.as_ref(), "h2o"); // table name
        assert_eq!(result[0].3.len(), 1); // returned chunks

        // Since no data, we do not do pushdown in the test chunk.
        let chunk = &result[0].3[0];
        let chunk_schema = (*chunk.schema()).clone();
        assert_eq!(chunk_schema.len(), 5);
        let chunk_schema = chunk_schema.sort_fields_by_name();
        assert_eq!(chunk_schema.field(0).1.name(), "bar");
        assert_eq!(chunk_schema.field(1).1.name(), "foo");
        assert_eq!(chunk_schema.field(2).1.name(), "i64_field");
        assert_eq!(chunk_schema.field(3).1.name(), "i64_field_2");
        assert_eq!(chunk_schema.field(4).1.name(), TIME_COLUMN_NAME);
    }

    #[tokio::test]
    async fn test_table_chunk_stream_pred_and_field_columns() {
        let chunk0 = Arc::new(
            TestChunk::new("h2o")
                .with_id(0)
                .with_tag_column("foo")
                .with_tag_column("bar")
                .with_i64_field_column("i64_field")
                .with_i64_field_column("i64_field_2")
                .with_time_column()
                .with_one_row_of_data(),
        );

        let executor = Arc::new(Executor::new_testing());
        let test_db = Arc::new(TestDatabase::new(Arc::clone(&executor)));
        test_db.add_chunk("my_partition_key", Arc::clone(&chunk0));
        let ctx = test_db.new_query_context(None, None);

        let need_fields = false;

        /////////////
        // Test 1: predicate on field `i64_field_2` and `field_columns` is empty
        // predicate on field column
        let expr = col("i64_field_2").eq(lit(10));
        let predicate = Predicate::new().with_expr(expr);
        let table_predicates = vec![(Arc::from("h2o"), Some(chunk0.schema().clone()), predicate)];

        let result = table_chunk_stream(test_db, need_fields, &table_predicates, &ctx)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert!(!result.is_empty());
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0.as_ref(), "h2o"); // table name
        assert_eq!(result[0].3.len(), 1); // returned chunks

        // chunk schema includes everything (test table does NOT perform any projection)
        let chunk = &result[0].3[0];
        let chunk_schema = (*chunk.schema()).clone();
        assert_eq!(chunk_schema.len(), 5);
        let chunk_schema = chunk_schema.sort_fields_by_name();
        assert_eq!(chunk_schema.field(0).1.name(), "bar");
        assert_eq!(chunk_schema.field(1).1.name(), "foo");
        assert_eq!(chunk_schema.field(2).1.name(), "i64_field");
        assert_eq!(chunk_schema.field(3).1.name(), "i64_field_2");
        assert_eq!(chunk_schema.field(4).1.name(), TIME_COLUMN_NAME);

        /////////////
        // Test 2: predicate on tag `foo` and `field_columns` is not empty
        let expr = col("bar").eq(lit(10));
        let predicate = Predicate::new()
            .with_expr(expr)
            .with_field_columns(vec!["i64_field".to_string()])
            .unwrap();
        let table_predicates = vec![(Arc::from("h2o"), Some(chunk0.schema().clone()), predicate)];

        let test_db = Arc::new(TestDatabase::new(Arc::clone(&executor)));
        test_db.add_chunk("my_partition_key", Arc::clone(&chunk0));
        let ctx = test_db.new_query_context(None, None);
        let result = table_chunk_stream(test_db, need_fields, &table_predicates, &ctx)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert!(!result.is_empty());
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0.as_ref(), "h2o"); // table name
        assert_eq!(result[0].3.len(), 1); // returned chunks

        // chunk schema includes everything (test table does NOT perform any projection)
        let chunk = &result[0].3[0];
        let chunk_schema = (*chunk.schema()).clone();
        assert_eq!(chunk_schema.len(), 5);
        let chunk_schema = chunk_schema.sort_fields_by_name();
        assert_eq!(chunk_schema.field(0).1.name(), "bar");
        assert_eq!(chunk_schema.field(1).1.name(), "foo");
        assert_eq!(chunk_schema.field(2).1.name(), "i64_field");
        assert_eq!(chunk_schema.field(3).1.name(), "i64_field_2");
        assert_eq!(chunk_schema.field(4).1.name(), TIME_COLUMN_NAME);
    }

    #[tokio::test]
    async fn test_table_chunk_stream_pred_on_unknown_field() {
        let chunk0 = Arc::new(
            TestChunk::new("h2o")
                .with_id(0)
                .with_tag_column("foo")
                .with_tag_column("bar")
                .with_i64_field_column("i64_field")
                .with_i64_field_column("i64_field_2")
                .with_time_column()
                .with_one_row_of_data(),
        );

        let executor = Arc::new(Executor::new_testing());
        let test_db = Arc::new(TestDatabase::new(Arc::clone(&executor)));
        test_db.add_chunk("my_partition_key", Arc::clone(&chunk0));
        let ctx = test_db.new_query_context(None, None);

        // predicate on unknown column
        let expr = col("unknown_name").eq(lit(10));
        let predicate = Predicate::new().with_expr(expr);
        let table_predicates = vec![(Arc::from("h2o"), Some(chunk0.schema().clone()), predicate)];

        let need_fields = false;
        let result = table_chunk_stream(test_db, need_fields, &table_predicates, &ctx)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert!(!result.is_empty());
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0.as_ref(), "h2o"); // table name
        assert_eq!(result[0].3.len(), 1); // returned chunks

        // chunk schema includes all 5 columns since we hit the unknown column
        let chunk = &result[0].3[0];
        let chunk_schema = (*chunk.schema()).clone();
        assert_eq!(chunk_schema.len(), 5);
        let chunk_schema = chunk_schema.sort_fields_by_name();
        assert_eq!(chunk_schema.field(0).1.name(), "bar");
        assert_eq!(chunk_schema.field(1).1.name(), "foo");
        assert_eq!(chunk_schema.field(2).1.name(), "i64_field");
        assert_eq!(chunk_schema.field(3).1.name(), "i64_field_2");
        assert_eq!(chunk_schema.field(4).1.name(), TIME_COLUMN_NAME);
    }

    #[tokio::test]
    async fn test_predicate_rewrite_table_names() {
        run_test(|test_db, rpc_predicate| {
            async move {
                InfluxRpcPlanner::new(test_db.new_query_context(None, None))
                    .table_names(test_db, rpc_predicate)
                    .await
                    .expect("creating plan");
            }
            .boxed()
        })
        .await
    }

    #[tokio::test]
    async fn test_predicate_rewrite_tag_keys() {
        run_test(|test_db, rpc_predicate| {
            async move {
                InfluxRpcPlanner::new(test_db.new_query_context(None, None))
                    .tag_keys(test_db, rpc_predicate)
                    .await
                    .expect("creating plan");
            }
            .boxed()
        })
        .await
    }

    #[tokio::test]
    async fn test_predicate_rewrite_tag_values() {
        run_test(|test_db, rpc_predicate| {
            async move {
                InfluxRpcPlanner::new(test_db.new_query_context(None, None))
                    .tag_values(test_db, "foo", rpc_predicate)
                    .await
                    .expect("creating plan");
            }
            .boxed()
        })
        .await
    }

    #[tokio::test]
    async fn test_predicate_rewrite_field_columns() {
        run_test(|test_db, rpc_predicate| {
            async move {
                InfluxRpcPlanner::new(test_db.new_query_context(None, None))
                    .field_columns(test_db, rpc_predicate)
                    .await
                    .expect("creating plan");
            }
            .boxed()
        })
        .await
    }

    #[tokio::test]
    async fn test_predicate_rewrite_read_filter() {
        run_test(|test_db, rpc_predicate| {
            async move {
                InfluxRpcPlanner::new(test_db.new_query_context(None, None))
                    .read_filter(test_db, rpc_predicate)
                    .await
                    .expect("creating plan");
            }
            .boxed()
        })
        .await
    }

    #[tokio::test]
    async fn test_predicate_read_group() {
        run_test(|test_db, rpc_predicate| {
            async move {
                let agg = Aggregate::None;
                let group_columns = &[String::from("foo")];
                InfluxRpcPlanner::new(test_db.new_query_context(None, None))
                    .read_group(test_db, rpc_predicate, agg, group_columns)
                    .await
                    .expect("creating plan");
            }
            .boxed()
        })
        .await
    }

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
            .read_group(Arc::clone(&test_db) as _, rpc_predicate, agg, group_columns)
            .await
            .expect("creating plan");
        insta::assert_snapshot!(res.display_indent_schema().to_string(), @r###"
        Sort: foo ASC NULLS LAST, _measurement ASC NULLS LAST, _field ASC NULLS LAST, _time ASC NULLS LAST [_measurement:Dictionary(Int32, Utf8), foo:Dictionary(Int32, Utf8);N, _field:Dictionary(Int32, Utf8), _time:Timestamp(Nanosecond, Some("UTC")), _value:Union([(0, Field { name: "float", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), (1, Field { name: "integer", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), (2, Field { name: "unsigned", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), (3, Field { name: "boolean", data_type: Boolean, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), (4, Field { name: "string", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} })], Sparse)]
          SeriesPivot: measurement_expr=Utf8("h2o"), tag_exprs=(foo), time_exprs=(time), field_exprs=(CASE WHEN foo.bar = Float64(1.2) THEN foo.bar END AS foo.bar) [_measurement:Dictionary(Int32, Utf8), foo:Dictionary(Int32, Utf8);N, _field:Dictionary(Int32, Utf8), _time:Timestamp(Nanosecond, Some("UTC")), _value:Union([(0, Field { name: "float", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), (1, Field { name: "integer", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), (2, Field { name: "unsigned", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), (3, Field { name: "boolean", data_type: Boolean, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), (4, Field { name: "string", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} })], Sparse)]
            TableScan: h2o [foo:Dictionary(Int32, Utf8);N, foo.bar:Float64;N, time:Timestamp(Nanosecond, Some("UTC"))]
        "###);
    }

    #[tokio::test]
    async fn test_predicate_read_window_aggregate() {
        run_test(|test_db, rpc_predicate| {
            async move {
                let agg = Aggregate::First;
                let every = WindowDuration::from_months(1, false);
                let offset = WindowDuration::from_months(1, false);
                InfluxRpcPlanner::new(test_db.new_query_context(None, None))
                    .read_window_aggregate(test_db, rpc_predicate, agg, every, offset)
                    .await
                    .expect("creating plan");
            }
            .boxed()
        })
        .await
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
            .read_filter(Arc::clone(&test_db) as _, rpc_predicate)
            .await
            .expect("creating plan");

        // Note: The retention policy (i.e. a time predicate) does NOT occur within the logical plan because it is an
        //       implementation detail of the table itself and will only be manifested when the `TableScan` is converted
        //       into a physical plan (which uses the IOx table provider code).
        insta::assert_snapshot!(plan.display_indent_schema().to_string(), @r###"
        Sort: _measurement ASC NULLS LAST, tag ASC NULLS LAST, _field ASC NULLS LAST, _time ASC NULLS LAST [_measurement:Dictionary(Int32, Utf8), tag:Dictionary(Int32, Utf8);N, _field:Dictionary(Int32, Utf8), _time:Timestamp(Nanosecond, Some("UTC")), _value:Union([(0, Field { name: "float", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), (1, Field { name: "integer", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), (2, Field { name: "unsigned", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), (3, Field { name: "boolean", data_type: Boolean, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), (4, Field { name: "string", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} })], Sparse)]
          SeriesPivot: measurement_expr=Utf8("table"), tag_exprs=(tag), time_exprs=(time), field_exprs=(field AS field) [_measurement:Dictionary(Int32, Utf8), tag:Dictionary(Int32, Utf8);N, _field:Dictionary(Int32, Utf8), _time:Timestamp(Nanosecond, Some("UTC")), _value:Union([(0, Field { name: "float", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), (1, Field { name: "integer", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), (2, Field { name: "unsigned", data_type: UInt64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), (3, Field { name: "boolean", data_type: Boolean, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), (4, Field { name: "string", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} })], Sparse)]
            Filter: table.tag = Dictionary(Int32, Utf8("MA")) AND table.time > TimestampNanosecond(1, Some("UTC")) [field:Float64;N, tag:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, Some("UTC"))]
              TableScan: table [field:Float64;N, tag:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, Some("UTC"))]
        "###);
    }

    #[tokio::test]
    async fn test_table_names_plan_empty() {
        maybe_start_logging();

        let executor = Arc::new(Executor::new_testing());
        let test_db = Arc::new(TestDatabase::new(Arc::clone(&executor)));
        let predicate = Predicate::new();
        let rpc_predicate = InfluxRpcPredicate::new(None, predicate);

        let plan = InfluxRpcPlanner::new(test_db.new_query_context(None, None))
            .table_names(Arc::clone(&test_db) as _, rpc_predicate)
            .await
            .expect("creating plan");

        insta::assert_snapshot!(plan.display_indent_schema().to_string(), @r###"
        Sort: string_value ASC NULLS LAST [string_value:Utf8]
          EmptyRelation [string_value:Utf8]
        "###);

        let physical_plan = executor
            .new_context()
            .create_physical_plan(&plan)
            .await
            .expect("physical plan");
        let physical_plan = datafusion::physical_plan::displayable(physical_plan.as_ref());
        insta::assert_snapshot!(physical_plan.indent(true), @r###"
        EmptyExec
        "###);
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
            .table_names(Arc::clone(&test_db) as _, rpc_predicate)
            .await
            .expect("creating plan");

        insta::assert_snapshot!(plan.display_indent_schema().to_string(), @r###"
        Sort: string_value ASC NULLS LAST [string_value:Utf8]
          Union [string_value:Utf8]
            EmptyRelation [string_value:Utf8]
            Projection: Utf8("table") AS string_value [string_value:Utf8]
              Limit: skip=0, fetch=1 [field:Float64;N, tag:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, Some("UTC"))]
                Sort: table.time DESC NULLS LAST [field:Float64;N, tag:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, Some("UTC"))]
                  Filter: table.field IS NOT NULL [field:Float64;N, tag:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, Some("UTC"))]
                    TableScan: table [field:Float64;N, tag:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, Some("UTC"))]
        "###);

        let physical_plan = executor
            .new_context()
            .create_physical_plan(&plan)
            .await
            .expect("physical plan");
        let physical_plan = datafusion::physical_plan::displayable(physical_plan.as_ref());
        insta::assert_snapshot!(physical_plan.indent(true), @r###"
        ProjectionExec: expr=[table as string_value]
          SortExec: TopK(fetch=1), expr=[time@0 DESC NULLS LAST], preserve_partitioning=[false]
            ProjectionExec: expr=[time@1 as time]
              CoalesceBatchesExec: target_batch_size=8192
                FilterExec: field@0 IS NOT NULL
                  RecordBatchesExec: chunks=1, projection=[field, time]
        "###);
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
            .table_names(Arc::clone(&test_db) as _, rpc_predicate)
            .await
            .expect("creating plan");

        insta::assert_snapshot!(plan.display_indent_schema().to_string(), @r###"
        Sort: string_value ASC NULLS LAST [string_value:Utf8]
          Union [string_value:Utf8]
            Union [string_value:Utf8]
              EmptyRelation [string_value:Utf8]
              Projection: Utf8("table") AS string_value [string_value:Utf8]
                Limit: skip=0, fetch=1 [field:Float64;N, tag:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, Some("UTC"))]
                  Sort: table.time DESC NULLS LAST [field:Float64;N, tag:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, Some("UTC"))]
                    Filter: table.field IS NOT NULL [field:Float64;N, tag:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, Some("UTC"))]
                      TableScan: table [field:Float64;N, tag:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, Some("UTC"))]
            Projection: Utf8("other_table") AS string_value [string_value:Utf8]
              Limit: skip=0, fetch=1 [other_field:Float64;N, tag:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, Some("UTC"))]
                Sort: other_table.time DESC NULLS LAST [other_field:Float64;N, tag:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, Some("UTC"))]
                  Filter: other_table.other_field IS NOT NULL [other_field:Float64;N, tag:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, Some("UTC"))]
                    TableScan: other_table [other_field:Float64;N, tag:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, Some("UTC"))]
        "###);

        let physical_plan = executor
            .new_context()
            .create_physical_plan(&plan)
            .await
            .expect("physical plan");
        let physical_plan = datafusion::physical_plan::displayable(physical_plan.as_ref());
        insta::assert_snapshot!(physical_plan.indent(true), @r###"
        SortPreservingMergeExec: [string_value@0 ASC NULLS LAST]
          UnionExec
            ProjectionExec: expr=[table as string_value]
              SortExec: TopK(fetch=1), expr=[time@0 DESC NULLS LAST], preserve_partitioning=[false]
                ProjectionExec: expr=[time@1 as time]
                  CoalesceBatchesExec: target_batch_size=8192
                    FilterExec: field@0 IS NOT NULL
                      RecordBatchesExec: chunks=1, projection=[field, time]
            ProjectionExec: expr=[other_table as string_value]
              SortExec: TopK(fetch=1), expr=[time@0 DESC NULLS LAST], preserve_partitioning=[false]
                ProjectionExec: expr=[time@1 as time]
                  CoalesceBatchesExec: target_batch_size=8192
                    FilterExec: other_field@0 IS NOT NULL
                      RecordBatchesExec: chunks=1, projection=[other_field, time]
        "###);
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
            .table_names(Arc::clone(&test_db) as _, rpc_predicate)
            .await
            .expect("creating plan");

        insta::assert_snapshot!(plan.display_indent_schema().to_string(), @r###"
        Sort: string_value ASC NULLS LAST [string_value:Utf8]
          Union [string_value:Utf8]
            Union [string_value:Utf8]
              EmptyRelation [string_value:Utf8]
              Projection: Utf8("table") AS string_value [string_value:Utf8]
                Limit: skip=0, fetch=1 [field:Float64;N, tag:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, Some("UTC"))]
                  Sort: table.time DESC NULLS LAST [field:Float64;N, tag:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, Some("UTC"))]
                    Filter: table.field IS NOT NULL [field:Float64;N, tag:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, Some("UTC"))]
                      Filter: table.tag = Dictionary(Int32, Utf8("MA")) [field:Float64;N, tag:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, Some("UTC"))]
                        TableScan: table [field:Float64;N, tag:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, Some("UTC"))]
            Projection: Utf8("other_table") AS string_value [string_value:Utf8]
              Limit: skip=0, fetch=1 [other_field:Float64;N, other_tag:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, Some("UTC"))]
                Sort: other_table.time DESC NULLS LAST [other_field:Float64;N, other_tag:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, Some("UTC"))]
                  Filter: other_table.other_field IS NOT NULL [other_field:Float64;N, other_tag:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, Some("UTC"))]
                    Filter: Boolean(NULL) [other_field:Float64;N, other_tag:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, Some("UTC"))]
                      TableScan: other_table [other_field:Float64;N, other_tag:Dictionary(Int32, Utf8);N, time:Timestamp(Nanosecond, Some("UTC"))]
        "###);

        let physical_plan = executor
            .new_context()
            .create_physical_plan(&plan)
            .await
            .expect("physical plan");
        let physical_plan = datafusion::physical_plan::displayable(physical_plan.as_ref());
        insta::assert_snapshot!(physical_plan.indent(true), @r###"
        ProjectionExec: expr=[table as string_value]
          SortExec: TopK(fetch=1), expr=[time@0 DESC NULLS LAST], preserve_partitioning=[false]
            ProjectionExec: expr=[time@2 as time]
              CoalesceBatchesExec: target_batch_size=8192
                FilterExec: field@0 IS NOT NULL AND tag@1 = MA
                  RecordBatchesExec: chunks=1, projection=[field, tag, time]
        "###);
    }

    /// Runs func() and checks that predicates are simplified prior to
    /// sending them down to the chunks for processing.
    async fn run_test<T>(func: T)
    where
        T: Fn(Arc<TestDatabase>, InfluxRpcPredicate) -> BoxFuture<'static, ()> + Send + Sync,
    {
        test_helpers::maybe_start_logging();
        // ------------- Test 1 ----------------

        // this is what happens with a grpc predicate on a tag
        //
        // tag(foo) = 'bar' becomes
        //
        // CASE WHEN foo IS NULL then '' ELSE foo END = 'bar'
        //
        // It is critical to be rewritten foo = 'bar' correctly so
        // that it can be evaluated quickly
        let expr = when(col("foo").is_null(), lit(""))
            .otherwise(col("foo"))
            .unwrap();
        let silly_predicate = Predicate::new().with_expr(expr.eq(lit("bar")));

        // verify that the predicate was rewritten to `foo = 'bar'`
        let expected_predicate = vec![col("foo").eq(lit_dict("bar"))];

        run_test_with_predicate(&func, silly_predicate, expected_predicate).await;

        // ------------- Test 2 ----------------
        // Validate that _measurement predicates are translated
        //
        // https://github.com/influxdata/influxdb_iox/issues/3601
        // _measurement = 'foo'
        let silly_predicate = Predicate::new().with_expr(col("_measurement").eq(lit("foo")));

        // verify that the predicate was rewritten to `false` as the
        // measurement name is `h20`
        let expected_predicate = vec![lit(false)];
        run_test_with_predicate(&func, silly_predicate, expected_predicate).await;

        // ------------- Test 3 ----------------
        // more complicated _measurement predicates are translated
        //
        // https://github.com/influxdata/influxdb_iox/issues/3601
        // (_measurement = 'foo' or measurement = 'h2o') AND foo = 'bar'
        let silly_predicate = Predicate::new().with_expr(
            col("_measurement")
                .eq(lit("foo"))
                .or(col("_measurement").eq(lit("h2o")))
                .and(col("foo").eq(lit("bar"))),
        );

        // verify that the predicate was rewritten to foo = 'bar'
        let dict = ScalarValue::Dictionary(
            Box::new(DataType::Int32),
            Box::new(ScalarValue::Utf8(Some("bar".to_string()))),
        );
        let expected_predicate = vec![col("foo").eq(lit(dict))];
        run_test_with_predicate(&func, silly_predicate, expected_predicate).await;
    }

    /// Runs func() with the specified predicate and verifies
    /// `expected_predicate` is received by the chunk
    async fn run_test_with_predicate<T>(
        func: &T,
        predicate: Predicate,
        expected_predicate: Vec<Expr>,
    ) where
        T: Fn(Arc<TestDatabase>, InfluxRpcPredicate) -> BoxFuture<'static, ()> + Send + Sync,
    {
        let chunk0 = Arc::new(
            TestChunk::new("h2o")
                .with_id(0)
                .with_tag_column("foo")
                .with_f64_field_column("my_field")
                .with_time_column(),
        );

        let executor = Arc::new(Executor::new_testing());
        let test_db = Arc::new(TestDatabase::new(Arc::clone(&executor)));
        test_db.add_chunk("my_partition_key", Arc::clone(&chunk0));

        let rpc_predicate = InfluxRpcPredicate::new(None, predicate);

        // run the function
        func(Arc::clone(&test_db), rpc_predicate).await;

        let actual_predicate = test_db.get_chunks_predicate();

        assert_eq!(
            actual_predicate, expected_predicate,
            "\nActual: {actual_predicate:?}\nExpected: {expected_predicate:?}"
        );
    }
}
