//! An InfluxDB IOx API client.

#![warn(missing_docs)]
#![allow(clippy::missing_docs_in_private_items)]

pub use generated_types::{google, protobuf_type_url, protobuf_type_url_eq};

pub use client::*;

pub use client_util::connection;
pub use client_util::namespace_translation;

#[cfg(feature = "format")]
/// Output formatting utilities
pub mod format;

mod client;
