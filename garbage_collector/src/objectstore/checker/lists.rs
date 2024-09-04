use std::vec;

use data_types::ObjectStoreId;
use object_store::ObjectMeta;
use observability_deps::tracing::{trace, warn};

/// A set of files that are considered eligible for immediate deletion without
/// incurring further checks (unknown / invalid / non-IOx files).
#[derive(Debug, Default)]
pub(crate) struct ImmediateDeletionList(Vec<ObjectMeta>);

impl ImmediateDeletionList {
    pub(super) fn push(&mut self, object: ObjectMeta) {
        warn!(?object, "scheduling file for unconditional deletion");
        self.0.push(object);
    }
}

impl std::iter::IntoIterator for ImmediateDeletionList {
    type Item = ObjectMeta;
    type IntoIter = vec::IntoIter<ObjectMeta>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

/// The set of files that are eligible for deletion subject to further checks
/// (critically catalog presence & soft deletion markers).
#[derive(Debug, Default)]
pub(crate) struct DeletionCandidateList(Vec<(ObjectStoreId, ObjectMeta)>);

impl DeletionCandidateList {
    pub(super) fn push(&mut self, id: ObjectStoreId, object: ObjectMeta) {
        trace!(
            ?id,
            ?object,
            "enqueuing deletion candidate for verification"
        );
        self.0.push((id, object));
    }

    /// Convert this [`DeletionCandidateList`] into a [`Vec`] catalog UUID
    /// [`ObjectStoreId`] instances and object storage [`ObjectMeta`] tuples.
    pub(crate) fn into_candidate_vec(self) -> Vec<(ObjectStoreId, ObjectMeta)> {
        self.0
    }
}
