//! A few helper types.

use std::sync::Arc;

/// Provides size estimations for an immutable object.
pub(crate) trait HasSize {
    /// Size in bytes.
    fn size(&self) -> usize;
}

/// Dynamic error type.
pub(crate) type DynError = Arc<dyn std::error::Error + Send + Sync>;

/// Result type with value wrapped into [`Arc`]s.
pub(crate) type ArcResult<T> = Result<Arc<T>, DynError>;
