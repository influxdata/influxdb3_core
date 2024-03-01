//! IOx test utils and tests

#![warn(missing_docs)]

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

mod catalog;
pub use catalog::{
    TestCatalog, TestNamespace, TestParquetFile, TestParquetFileBuilder, TestPartition, TestTable,
};

mod builders;
pub use builders::{
    ColumnBuilder, ParquetFileBuilder, PartitionBuilder, SkippedCompactionBuilder, TableBuilder,
};
