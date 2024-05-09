//! Synchronous reader & writer implementations of the IOx WAL.

mod reader;
pub(crate) use reader::{ClosedSegmentReader, Error as ReaderError};

mod writer;
pub(crate) use writer::{Error as WriterError, OpenSegmentFileWriter};
