//! Tools to make working with [`object_store`] a bit easier.
use object_store::{path::Error as PathError, Error, GetOptions};

use crate::cache_system::interfaces::DynError;

/// Returns `true` if ANY options are set.
pub fn any_options_set(options: &GetOptions) -> bool {
    let GetOptions {
        if_match,
        if_none_match,
        if_modified_since,
        if_unmodified_since,
        range,
        version,
        head,
    } = options;

    if if_match.is_some() {
        return true;
    }

    if if_none_match.is_some() {
        return true;
    }

    if if_modified_since.is_some() {
        return true;
    }

    if if_unmodified_since.is_some() {
        return true;
    }

    if range.is_some() {
        return true;
    }

    if version.is_some() {
        return true;
    }

    if *head {
        return true;
    }

    false
}

/// Convert [`DynError`] to [`object_store::Error`].
pub fn dyn_error_to_object_store_error(e: DynError, store_name: &'static str) -> Error {
    let Some(e_os) = e.downcast_ref::<Error>() else {
        return Error::Generic {
            store: store_name,
            source: Box::new(e),
        };
    };

    match e_os {
        Error::Generic { store, .. } => Error::Generic {
            store,
            source: Box::new(e),
        },
        Error::NotFound { path, .. } => Error::NotFound {
            path: path.clone(),
            source: Box::new(e),
        },
        Error::InvalidPath { source } => {
            let source = match source {
                PathError::EmptySegment { path } => PathError::EmptySegment { path: path.clone() },
                PathError::BadSegment { .. } => {
                    // can't clone
                    return Error::Generic {
                        store: store_name,
                        source: Box::new(e),
                    };
                }
                PathError::Canonicalize { path, source } => PathError::Canonicalize {
                    path: path.clone(),
                    source: std::io::Error::new(source.kind(), e),
                },
                PathError::InvalidPath { path } => PathError::InvalidPath { path: path.clone() },
                PathError::NonUnicode { path, source } => PathError::NonUnicode {
                    path: path.clone(),
                    source: *source,
                },
                PathError::PrefixMismatch { path, prefix } => PathError::PrefixMismatch {
                    path: path.clone(),
                    prefix: prefix.clone(),
                },
            };

            Error::InvalidPath { source }
        }
        Error::JoinError { .. } => {
            // can't clone
            Error::Generic {
                store: store_name,
                source: Box::new(e),
            }
        }
        Error::NotSupported { .. } => Error::NotSupported {
            source: Box::new(e),
        },
        Error::AlreadyExists { path, .. } => Error::AlreadyExists {
            path: path.clone(),
            source: Box::new(e),
        },
        Error::Precondition { path, .. } => Error::Precondition {
            path: path.clone(),
            source: Box::new(e),
        },
        Error::NotModified { path, .. } => Error::NotModified {
            path: path.clone(),
            source: Box::new(e),
        },
        Error::NotImplemented => Error::NotImplemented,
        Error::UnknownConfigurationKey { store, key } => Error::UnknownConfigurationKey {
            store,
            key: key.clone(),
        },
    }
}
