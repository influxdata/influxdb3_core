//! Utility functions for working with arrow

use std::iter::FromIterator;
use std::sync::Arc;

use arrow::{
    array::{new_null_array, ArrayRef, StringArray},
    datatypes::SchemaRef,
    error::ArrowError,
    record_batch::RecordBatch,
};

/// Returns a single column record batch of type Utf8 from the
/// contents of something that can be turned into an iterator over
/// `Option<&str>`
pub fn str_iter_to_batch<Ptr, I>(field_name: &str, iter: I) -> Result<RecordBatch, ArrowError>
where
    I: IntoIterator<Item = Option<Ptr>>,
    Ptr: AsRef<str>,
{
    let array = StringArray::from_iter(iter);

    RecordBatch::try_from_iter(vec![(field_name, Arc::new(array) as ArrayRef)])
}

/// Create a new [`RecordBatch`] that has the specified schema, adding null values for columns that
/// don't appear in the batch.
pub fn ensure_schema(
    output_schema: &SchemaRef,
    batch: &RecordBatch,
) -> Result<RecordBatch, ArrowError> {
    // Go over all columns of output_schema
    let batch_output_columns = output_schema
        .fields()
        .iter()
        .map(|output_field| {
            // If the field is available in the batch, use it. Otherwise, add a column with all
            // null values.
            batch
                .column_by_name(output_field.name())
                .map(Arc::clone)
                .unwrap_or_else(|| new_null_array(output_field.data_type(), batch.num_rows()))
        })
        .collect();

    RecordBatch::try_new(Arc::clone(output_schema), batch_output_columns)
}
