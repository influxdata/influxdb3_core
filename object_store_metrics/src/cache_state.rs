//! Attributes for [`object_store`] response to signal cache state.

use std::borrow::Cow;

use object_store::{Attribute, AttributeValue};

/// Attribute that encodes [`CacheState`].
pub const ATTR_CACHE_STATE: Attribute = Attribute::Metadata(Cow::Borrowed("cache_state"));

/// State that provides more information about `get_or_fetch` operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CacheState {
    /// Entry was already part of the cache and fully fetched..
    WasCached,

    /// Entry was already part of the cache but did not finish loading.
    AlreadyLoading,

    /// A new entry was created.
    NewEntry,
}

impl From<CacheState> for AttributeValue {
    fn from(state: CacheState) -> Self {
        let s = match state {
            CacheState::AlreadyLoading => "already_loading",
            CacheState::NewEntry => "new_entry",
            CacheState::WasCached => "was_cached",
        };
        Self::from(s)
    }
}

impl TryFrom<&AttributeValue> for CacheState {
    type Error = String;

    fn try_from(value: &AttributeValue) -> Result<Self, Self::Error> {
        match value.as_ref() {
            "already_loading" => Ok(Self::AlreadyLoading),
            "new_entry" => Ok(Self::NewEntry),
            "was_cached" => Ok(Self::WasCached),
            other => Err(format!("unknown cache state: {other}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_state_roundtrip() {
        for cache_state in [
            CacheState::AlreadyLoading,
            CacheState::NewEntry,
            CacheState::WasCached,
        ] {
            let val = AttributeValue::from(cache_state);
            let cache_state_2 = CacheState::try_from(&val).unwrap();
            assert_eq!(cache_state, cache_state_2);
        }

        assert_eq!(
            CacheState::try_from(&AttributeValue::from("foo")).unwrap_err(),
            "unknown cache state: foo",
        );
    }
}
