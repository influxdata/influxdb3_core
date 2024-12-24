//! Snapshot type abstraction.
use std::future::Future;

use catalog_cache::{CacheKey, CacheValue};
use data_types::snapshot::namespace::NamespaceSnapshot;
use data_types::snapshot::root::RootSnapshot;
use data_types::{
    snapshot::partition::PartitionSnapshot, snapshot::table::TableSnapshot, NamespaceId,
    PartitionId, TableId,
};

use generated_types::influxdata::iox::catalog_cache::v1 as proto;
use generated_types::prost::bytes::Bytes;
use generated_types::prost::Message;

use crate::interface::{Catalog, Result};

pub(crate) trait SnapshotKey:
    Copy + std::fmt::Debug + Eq + std::hash::Hash + Send + Sync
{
    fn get(&self) -> Option<i64>;
    fn to_key(&self) -> CacheKey;
}

pub(crate) trait Snapshot: Clone + std::fmt::Debug + Send + Sync + 'static {
    /// Machine- & humand-readable name.
    const NAME: &'static str;

    /// Key.
    type Key: SnapshotKey;

    /// Create new snapshot.
    fn snapshot(backing: &dyn Catalog, key: Self::Key)
        -> impl Future<Output = Result<Self>> + Send;

    /// Get the generation number of a snapshot for a given key. This
    /// can be used as an optimization where creating a snapshot is
    /// expensive.
    fn snapshot_generation(
        backing: &dyn Catalog,
        key: Self::Key,
    ) -> impl Future<Output = Result<u64>> + Send {
        async move {
            let snapshot = Self::snapshot(backing, key).await?;
            Ok(snapshot.generation())
        }
    }

    /// Snapshot generation.
    fn generation(&self) -> u64;

    /// Convert snapshot to bytes.
    fn to_bytes(&self) -> Bytes;

    /// Decode snapshot from [`CacheValue`].
    fn from_cache_value(val: CacheValue) -> Result<Self>;
}

impl SnapshotKey for PartitionId {
    fn get(&self) -> Option<i64> {
        Some(self.get())
    }

    fn to_key(&self) -> CacheKey {
        CacheKey::Partition(self.get())
    }
}

impl Snapshot for PartitionSnapshot {
    const NAME: &'static str = "partition";
    type Key = PartitionId;

    async fn snapshot(backing: &dyn Catalog, key: Self::Key) -> Result<Self> {
        let snapshot = backing.repositories().partitions().snapshot(key).await?;
        assert_eq!(snapshot.partition_id(), key);
        Ok(snapshot)
    }

    async fn snapshot_generation(backing: &dyn Catalog, key: Self::Key) -> Result<u64> {
        backing
            .repositories()
            .partitions()
            .snapshot_generation(key)
            .await
    }

    fn generation(&self) -> u64 {
        self.generation()
    }

    fn to_bytes(&self) -> Bytes {
        let proto: proto::Partition = self.clone().into();
        proto.encode_to_vec().into()
    }

    fn from_cache_value(val: CacheValue) -> Result<Self> {
        let proto = val.data().map_or(Ok(Default::default()), |data| {
            proto::Partition::decode(data.clone())
        })?;
        Ok(Self::decode(proto, val.generation()))
    }
}

impl SnapshotKey for TableId {
    fn get(&self) -> Option<i64> {
        Some(self.get())
    }

    fn to_key(&self) -> CacheKey {
        CacheKey::Table(self.get())
    }
}

impl Snapshot for TableSnapshot {
    const NAME: &'static str = "table";
    type Key = TableId;

    async fn snapshot(backing: &dyn Catalog, key: Self::Key) -> Result<Self> {
        let snapshot = backing.repositories().tables().snapshot(key).await?;
        assert_eq!(snapshot.table_id(), key);
        Ok(snapshot)
    }

    fn generation(&self) -> u64 {
        self.generation()
    }

    fn to_bytes(&self) -> Bytes {
        let proto: proto::Table = self.clone().into();
        proto.encode_to_vec().into()
    }

    fn from_cache_value(val: CacheValue) -> Result<Self> {
        let proto = val.data().map_or(Ok(Default::default()), |data| {
            proto::Table::decode(data.clone())
        })?;
        Ok(Self::decode(proto, val.generation()))
    }
}

impl SnapshotKey for NamespaceId {
    fn get(&self) -> Option<i64> {
        Some(self.get())
    }

    fn to_key(&self) -> CacheKey {
        CacheKey::Namespace(self.get())
    }
}

impl Snapshot for NamespaceSnapshot {
    const NAME: &'static str = "namespace";
    type Key = NamespaceId;

    async fn snapshot(backing: &dyn Catalog, key: Self::Key) -> Result<Self> {
        let snapshot = backing.repositories().namespaces().snapshot(key).await?;
        assert_eq!(snapshot.namespace_id(), key);
        Ok(snapshot)
    }

    fn generation(&self) -> u64 {
        self.generation()
    }

    fn to_bytes(&self) -> Bytes {
        let proto: proto::Namespace = self.clone().into();
        proto.encode_to_vec().into()
    }

    fn from_cache_value(val: CacheValue) -> Result<Self> {
        let proto = val.data().map_or(Ok(Default::default()), |data| {
            proto::Namespace::decode(data.clone())
        })?;
        Ok(Self::decode(proto, val.generation())?)
    }
}

/// Symbolic key for [`RootSnapshot`] since this one has no ID.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub(crate) struct RootKey;

impl SnapshotKey for RootKey {
    fn get(&self) -> Option<i64> {
        None
    }

    fn to_key(&self) -> CacheKey {
        CacheKey::Root
    }
}

impl Snapshot for RootSnapshot {
    const NAME: &'static str = "root";
    type Key = RootKey;

    async fn snapshot(backing: &dyn Catalog, _key: Self::Key) -> Result<Self> {
        let snapshot = backing.repositories().root().snapshot().await?;
        Ok(snapshot)
    }

    fn generation(&self) -> u64 {
        self.generation()
    }

    fn to_bytes(&self) -> Bytes {
        let proto: proto::Root = self.clone().into();
        proto.encode_to_vec().into()
    }

    fn from_cache_value(val: CacheValue) -> Result<Self> {
        let proto = val.data().map_or(Ok(Default::default()), |data| {
            proto::Root::decode(data.clone())
        })?;
        Ok(Self::decode(proto, val.generation())?)
    }
}
