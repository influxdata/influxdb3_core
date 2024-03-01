//! Snapshot definition for namespaces
use bytes::Bytes;
use generated_types::influxdata::iox::catalog_cache::v1 as proto;
use generated_types::influxdata::iox::partition_template::v1::PartitionTemplate;
use snafu::{ResultExt, Snafu};

use crate::{
    Namespace, NamespaceId, NamespacePartitionTemplateOverride, NamespaceVersion,
    ServiceLimitError, Table, TableId, Timestamp,
};

use super::{
    hash::{HashBuckets, HashBucketsEncoder},
    list::MessageList,
};

/// Error for [`NamespaceSnapshot`]
#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum Error {
    #[snafu(display("Invalid table name: {source}"))]
    NamespaceName { source: std::str::Utf8Error },

    #[snafu(display("Invalid max tables: {source}"))]
    MaxTables { source: ServiceLimitError },

    #[snafu(display("Invalid max columns per table: {source}"))]
    MaxColumnsPerTable { source: ServiceLimitError },

    #[snafu(display("Invalid partition template: {source}"))]
    PartitionTemplate {
        source: crate::partition_template::ValidationError,
    },

    #[snafu(display("Error encoding tables: {source}"))]
    TableEncode {
        source: crate::snapshot::list::Error,
    },

    #[snafu(display("Error decoding tables: {source}"))]
    TableDecode {
        source: crate::snapshot::list::Error,
    },

    #[snafu(display("Error decoding table names: {source}"))]
    TableNamesDecode {
        source: crate::snapshot::hash::Error,
    },

    #[snafu(display("Table name hash lookup resulted in out of bounds. Wanted index {wanted} but there are only {entries} entries."))]
    TableNameHashOutOfBounds { wanted: usize, entries: usize },
}

/// Result for [`NamespaceSnapshot`]
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A snapshot of a namespace
#[derive(Debug, Clone)]
pub struct NamespaceSnapshot {
    id: NamespaceId,
    name: Bytes,
    retention_period_ns: Option<i64>,
    max_tables: i32,
    max_columns_per_table: i32,
    deleted_at: Option<Timestamp>,
    partition_template: Option<PartitionTemplate>,
    router_version: NamespaceVersion,
    tables: MessageList<proto::NamespaceTable>,
    table_names: HashBuckets,
    generation: u64,
}

impl NamespaceSnapshot {
    /// Create a new [`NamespaceSnapshot`] from the provided state
    pub fn encode(
        namespace: Namespace,
        tables: impl IntoIterator<Item = Table>,
        generation: u64,
    ) -> Result<Self> {
        let mut tables: Vec<_> = tables
            .into_iter()
            .map(|t| proto::NamespaceTable {
                id: t.id.get(),
                name: t.name.into(),
            })
            .collect();
        // TODO(marco): wire up binary search to find table by ID
        tables.sort_unstable_by_key(|t| t.id);

        let mut table_names = HashBucketsEncoder::new(tables.len());
        for table in &tables {
            table_names.push(&table.name);
        }

        Ok(Self {
            id: namespace.id,
            name: namespace.name.into(),
            retention_period_ns: namespace.retention_period_ns,
            max_tables: namespace.max_tables.get_i32(),
            max_columns_per_table: namespace.max_columns_per_table.get_i32(),
            deleted_at: namespace.deleted_at,
            partition_template: namespace.partition_template.as_proto().cloned(),
            router_version: namespace.router_version,
            tables: MessageList::encode(&tables).context(TableEncodeSnafu)?,
            table_names: table_names.finish(),
            generation,
        })
    }

    /// Create a new [`NamespaceSnapshot`] from a `proto` and generation
    pub fn decode(proto: proto::Namespace, generation: u64) -> Result<Self> {
        Ok(Self {
            id: NamespaceId::new(proto.id),
            name: proto.name,
            retention_period_ns: proto.retention_period_ns,
            max_tables: proto.max_tables,
            max_columns_per_table: proto.max_columns_per_table,
            deleted_at: proto.deleted_at.map(Timestamp::new),
            partition_template: proto.partition_template,
            router_version: NamespaceVersion::new(proto.router_version),
            tables: MessageList::from(proto.tables.unwrap_or_default()),
            table_names: proto
                .table_names
                .unwrap_or_default()
                .try_into()
                .context(TableNamesDecodeSnafu)?,
            generation,
        })
    }

    /// Get namespace.
    pub fn namespace(&self) -> Result<Namespace> {
        let name = std::str::from_utf8(&self.name)
            .context(NamespaceNameSnafu)?
            .to_owned();
        let max_tables = self.max_tables.try_into().context(MaxTablesSnafu)?;
        let max_columns_per_table = self
            .max_columns_per_table
            .try_into()
            .context(MaxColumnsPerTableSnafu)?;
        let partition_template = match self.partition_template.clone() {
            Some(t) => t.try_into().context(PartitionTemplateSnafu)?,
            None => NamespacePartitionTemplateOverride::const_default(),
        };

        Ok(Namespace {
            id: self.id,
            name,
            retention_period_ns: self.retention_period_ns,
            max_tables,
            max_columns_per_table,
            deleted_at: self.deleted_at,
            partition_template,
            router_version: self.router_version,
        })
    }

    /// Returns an iterator of the [`NamespaceSnapshotTable`]s in this namespace
    pub fn tables(&self) -> impl Iterator<Item = Result<NamespaceSnapshotTable>> + '_ {
        (0..self.tables.len()).map(|idx| {
            let t = self.tables.get(idx).context(TableDecodeSnafu)?;
            Ok(t.into())
        })
    }

    /// Lookup a [`NamespaceSnapshotTable`] by name
    pub fn lookup_table_by_name(&self, name: &str) -> Result<Option<NamespaceSnapshotTable>> {
        for idx in self.table_names.lookup(name.as_bytes()) {
            let table = self.tables.get(idx).context(TableEncodeSnafu)?;
            if table.name == name.as_bytes() {
                return Ok(Some(table.into()));
            }
        }

        Ok(None)
    }

    /// Returns the generation of this snapshot
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Get namespace ID.
    pub fn namespace_id(&self) -> NamespaceId {
        self.id
    }
}

/// Table information stored within [`NamespaceSnapshot`]
#[derive(Debug)]
pub struct NamespaceSnapshotTable {
    id: TableId,
    name: Bytes,
}

impl NamespaceSnapshotTable {
    /// Returns the [`TableId`] for this table
    pub fn id(&self) -> TableId {
        self.id
    }

    /// Returns the name for this table
    pub fn name(&self) -> &[u8] {
        &self.name
    }
}

impl From<proto::NamespaceTable> for NamespaceSnapshotTable {
    fn from(value: proto::NamespaceTable) -> Self {
        Self {
            id: TableId::new(value.id),
            name: value.name,
        }
    }
}

impl From<NamespaceSnapshot> for proto::Namespace {
    fn from(value: NamespaceSnapshot) -> Self {
        Self {
            tables: Some(value.tables.into()),
            table_names: Some(value.table_names.into()),
            deleted_at: value.deleted_at.map(|d| d.get()),
            id: value.id.get(),
            name: value.name,
            retention_period_ns: value.retention_period_ns,
            max_tables: value.max_tables,
            max_columns_per_table: value.max_columns_per_table,
            partition_template: value.partition_template,
            router_version: value.router_version.get(),
        }
    }
}
