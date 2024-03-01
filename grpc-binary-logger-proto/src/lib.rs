#![allow(clippy::derive_partial_eq_without_eq, clippy::use_self)]

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

pub mod proto {
    tonic::include_proto!("grpc.binarylog.v1");
}
pub use proto::*;
