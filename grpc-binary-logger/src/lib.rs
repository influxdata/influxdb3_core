//! Implements a gRPC binary logger. See <https://github.com/grpc/grpc/blob/master/doc/binary-logging.md>

#![warn(missing_docs)]
#[allow(unreachable_pub)]
// Workaround for "unused crate" lint false positives.
#[cfg(test)]
use assert_matches as _;
#[cfg(test)]
use grpc_binary_logger_test_proto as _;
#[cfg(test)]
use tokio_stream as _;
use workspace_hack as _;

mod predicate;
pub use self::predicate::{NoReflection, Predicate};
pub mod sink;
pub use self::sink::{DebugSink, FileSink, Sink};

mod middleware;
pub use middleware::BinaryLoggerLayer;

pub use grpc_binary_logger_proto as proto;
