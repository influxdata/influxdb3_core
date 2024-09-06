//! A few helper types.

use std::sync::Arc;

/// Provides size estimations for an immutable object.
pub trait HasSize {
    /// Size in bytes.
    fn size(&self) -> usize;
}

/// Dynamic error type.
pub type DynError = Arc<dyn std::error::Error + Send + Sync>;

/// Result type with value wrapped into [`Arc`]s.
pub type ArcResult<T> = Result<Arc<T>, DynError>;
