//! Snapshot definition for root
use bytes::Bytes;
use generated_types::influxdata::iox::catalog_cache::v1 as proto;
use snafu::{ResultExt, Snafu};

use crate::{Namespace, NamespaceId};

use super::{
    hash::{HashBuckets, HashBucketsEncoder},
    list::MessageList,
};

/// Error for [`RootSnapshot`]
#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum Error {
    #[snafu(display("Error decoding namespace names: {source}"))]
    NamespaceNamesDecode {
        source: crate::snapshot::hash::Error,
    },

    #[snafu(display("Error encoding namespaces: {source}"))]
    NamespaceEncode {
        source: crate::snapshot::list::Error,
    },

    #[snafu(display("Error decoding namespaces: {source}"))]
    NamespaceDecode {
        source: crate::snapshot::list::Error,
    },
}

/// Result for [`RootSnapshot`]
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A snapshot of root.
///
/// # Soft Deletion
/// This snapshot also contains soft-deleted namespaces.
#[derive(Debug, Clone)]
pub struct RootSnapshot {
    namespaces: MessageList<proto::RootNamespace>,
    namespace_names: HashBuckets,
    generation: u64,
}

impl RootSnapshot {
    /// Create a new [`RootSnapshot`] from the provided state
    pub fn encode(
        namespaces: impl IntoIterator<Item = Namespace>,
        generation: u64,
    ) -> Result<Self> {
        let mut namespaces: Vec<_> = namespaces
            .into_iter()
            .map(|ns| proto::RootNamespace {
                id: ns.id.get(),
                name: ns.name.into(),
            })
            .collect();
        // TODO(marco): wire up binary search to find namespace by ID
        namespaces.sort_unstable_by_key(|ns| ns.id);

        let mut namespace_names = HashBucketsEncoder::new(namespaces.len());
        for ns in &namespaces {
            namespace_names.push(&ns.name);
        }

        Ok(Self {
            namespaces: MessageList::encode(&namespaces).context(NamespaceEncodeSnafu)?,
            namespace_names: namespace_names.finish(),
            generation,
        })
    }

    /// Create a new [`RootSnapshot`] from a `proto` and generation
    pub fn decode(proto: proto::Root, generation: u64) -> Result<Self> {
        Ok(Self {
            namespaces: MessageList::from(proto.namespaces.unwrap_or_default()),
            namespace_names: proto
                .namespace_names
                .unwrap_or_default()
                .try_into()
                .context(NamespaceNamesDecodeSnafu)?,
            generation,
        })
    }

    /// Returns an iterator of the [`RootSnapshotNamespace`]s in this root snapshot
    pub fn namespaces(&self) -> impl Iterator<Item = Result<RootSnapshotNamespace>> + '_ {
        (0..self.namespaces.len()).map(|idx| {
            let t = self.namespaces.get(idx).context(NamespaceDecodeSnafu)?;
            Ok(t.into())
        })
    }

    /// Lookup a [`RootSnapshotNamespace`] by name
    pub fn lookup_namespace_by_name(&self, name: &str) -> Result<Option<RootSnapshotNamespace>> {
        for idx in self.namespace_names.lookup(name.as_bytes()) {
            let ns = self.namespaces.get(idx).context(NamespaceDecodeSnafu)?;
            if ns.name == name.as_bytes() {
                return Ok(Some(ns.into()));
            }
        }

        Ok(None)
    }

    /// Returns the generation of this snapshot
    pub fn generation(&self) -> u64 {
        self.generation
    }
}

/// Namespace information stored within [`RootSnapshot`]
#[derive(Debug)]
pub struct RootSnapshotNamespace {
    id: NamespaceId,
    name: Bytes,
}

impl RootSnapshotNamespace {
    /// Returns the [`NamespaceId`] for this namespace
    pub fn id(&self) -> NamespaceId {
        self.id
    }

    /// Returns the name for this namespace
    pub fn name(&self) -> &[u8] {
        &self.name
    }
}

impl From<proto::RootNamespace> for RootSnapshotNamespace {
    fn from(value: proto::RootNamespace) -> Self {
        Self {
            id: NamespaceId::new(value.id),
            name: value.name,
        }
    }
}

impl From<RootSnapshot> for proto::Root {
    fn from(value: RootSnapshot) -> Self {
        Self {
            namespaces: Some(value.namespaces.into()),
            namespace_names: Some(value.namespace_names.into()),
        }
    }
}
