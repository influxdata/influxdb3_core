//! Implementations of pruning oracle for Server-side Bucketing.

use arrow::array::BooleanArray;
use data_types::partition_template::{
    bucket_for_tag_value, TablePartitionTemplateOverride, TemplatePart,
};
use datafusion::{prelude::Column, scalar::ScalarValue};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

/// An abstraction that provides a side channel for determining if a column could
/// contain a set of values.
pub trait PruningOracle {
    /// An implementation must return an array that indicates, for each summary
    /// associated with this pruning oracle, if to its knowledge `column`
    /// contains ONLY the provided `values`.
    ///
    /// The implementation must adhere to the contract specified by [`PruningStatistics::contained()`](https://docs.rs/datafusion/latest/datafusion/physical_optimizer/pruning/trait.PruningStatistics.html#tymethod.contained):
    ///
    /// The returned array has one row for each summary, with the following meanings:
    ///
    /// - `true` if the values in `column` ONLY contain values from `values`
    /// - `false` if the values in `column` are NOT ANY of `values`
    /// - `null` if the neither of the above holds or is unknown.
    ///
    fn could_contain_values(
        &self,
        column: &Column,
        values: &HashSet<ScalarValue>,
    ) -> Option<BooleanArray>;
}

impl<T> PruningOracle for Arc<T>
where
    T: PruningOracle,
{
    fn could_contain_values(
        &self,
        column: &Column,
        values: &HashSet<ScalarValue>,
    ) -> Option<BooleanArray> {
        T::could_contain_values(self, column, values)
    }
}

#[derive(Copy, Clone, Debug)]
pub struct NoopPruningOracle;

impl PruningOracle for NoopPruningOracle {
    fn could_contain_values(
        &self,
        _column: &Column,
        _values: &HashSet<ScalarValue>,
    ) -> Option<BooleanArray> {
        None
    }
}

/// Creates a map of tag name to total bucket number.
///
/// If we have a partition with the following given information:
///
///   Partition template:
///   ```rs
///   TemplatePart::Bucket("banana", 50), TemplatePart::Bucket("uuid", 10)
///   ```
///
/// The `create_num_buckets_per_column_map` would create a map look like:
///   ```rs
///   {"banana": 50, "uuid": 10}
///   ```
///
///  # Example
///
/// ```rs
/// use data_types::partition_template::{TablePartitionTemplateOverride, TemplatePart};
/// use generated_types::influxdata::iox::partition_template::v1::template_part::Part;
/// use iox_query::pruning_oracle::get_num_buckets_per_tag;
///
/// let table_partition_template = TablePartitionTemplateOverride::try_new(
///     Some(PartitionTemplate {
///         parts: vec![
///             TemplatePart {
///                 part: Some(Part::Bucket(
///                     influxdb_iox_client::table::generated_types::Bucket {
///                         tag_name: "banana",
///                         num_buckets: 50,
///                     },
///                 )),
///             },
///             TemplatePart {
///                 part: Some(Part::Bucket(
///                     influxdb_iox_client::table::generated_types::Bucket {
///                         tag_name: "uuid",
///                         num_buckets: 10,
///                     },
///                 )),
///             },
///         ],
///     }),
///     &NamespacePartitionTemplateOverride::default(),
/// )
/// .expect("valid partition template");
///
/// let num_buckets_per_column = get_num_buckets_per_tag(&table_partition_template);
///
/// assert_eq!(num_buckets_per_tag.get(&Arc::from("banana")), Some(&50));
/// assert_eq!(num_buckets_per_tag.get(&Arc::from("uuid")), Some(&10));
/// assert_eq!(num_buckets_per_tag.get(&Arc::from("not_exist")), None);
/// ```
pub fn get_num_buckets_per_tag(
    table_partition_template: &TablePartitionTemplateOverride,
) -> HashMap<Arc<str>, u32> {
    let mut num_buckets_per_column = table_partition_template
        .parts()
        .filter_map(|part| match part {
            TemplatePart::Bucket(tag_name, num_buckets) => Some((Arc::from(tag_name), num_buckets)),
            _ => None,
        })
        .collect::<HashMap<_, _>>();
    num_buckets_per_column.shrink_to_fit();
    num_buckets_per_column
}

/// A container type to bundle a bucket ID with the number of
/// buckets for that column. This is used for pruning the server-side
/// bucketing later on.
///
/// If we have a partition with the following given information:
///
///   Partition template:
///   ```rs
///   TemplatePart::Bucket("banana", 50), TemplatePart::Bucket("uuid", 2000), TemplatePart::Bucket("apple", 20)
///   ```
///
///   Partition keys:
///   ```rs
///   "42|1010|!"
///   ```
///
///   Then the bucket info for each column would be:
///   ```rs
///   "banana" -> BucketInfo { id: Some(42), num_buckets: 50 }
///   "uuid" -> BucketInfo { id: Some(1010), num_buckets: 2000 }
///   "apple" -> BucketInfo { id: None, num_buckets: 20 }
///   ```
///
/// When do pruning, we can use `BucketInfo::may_contain_value` to check if a given
/// tag value belongs to the bucket:
///
/// # Example
///
/// ```rs
/// use data_types::partition_template::bucket_for_tag_value;
/// use iox_query::pruning_oracle::BucketInfo;
///
/// const NUM_BUCKETS: u32 = 50;
/// let bucket_id = bucket_for_tag_value("foo", NUM_BUCKETS);
/// let bucket_info = BucketInfo {
///   id: Some(bucket_id),
///   num_buckets: NUM_BUCKETS,
/// };
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BucketInfo {
    /// Bucket ID for this partition. If `None`, it means the
    /// partition key of this partition is empty, i.e. "!".
    pub id: Option<u32>,

    /// Number of buckets for the related column, specified in the partition template:
    /// [`TemplatePart::Bucket(tag_name, num_buckets)`](data_types::partition_template::TemplatePart::Bucket).
    pub num_buckets: u32,
}

impl BucketInfo {
    /// returns true if `tag_value` maps to this bucket id. Note that there are
    /// many tag values that map to the same bucket id.
    ///
    /// returns false if `tag_value` does not map to this bucket, or if this
    /// partition does not belong to a bucket (i.e. partition key is "!").
    pub fn may_contain_value(&self, tag_value: &str) -> bool {
        self.id.map_or(false, |id| {
            id == bucket_for_tag_value(tag_value, self.num_buckets)
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::pruning_oracle::BucketInfo;
    use data_types::partition_template::bucket_for_tag_value;

    #[test]
    fn test_bucket_info() {
        const COLUMN_VALUE: &str = "foo";
        const NUM_BUCKETS: u32 = 50;

        let bucket_id = bucket_for_tag_value(COLUMN_VALUE, NUM_BUCKETS);
        let bucket_info = BucketInfo {
            id: Some(bucket_id),
            num_buckets: NUM_BUCKETS,
        };

        assert!(bucket_info.may_contain_value("foo"));
        assert!(!bucket_info.may_contain_value("bar"));
    }

    #[test]
    fn test_bucket_info_different_col_value_same_bucket_id() {
        const COLUMN_VALUE: &str = "foo";
        const NUM_BUCKETS: u32 = 2;

        let bucket_id_1 = bucket_for_tag_value(COLUMN_VALUE, NUM_BUCKETS);
        let bucket_info = BucketInfo {
            id: Some(bucket_id_1),
            num_buckets: NUM_BUCKETS,
        };
        assert!(bucket_info.may_contain_value("foo"));

        // "bar" maps to a different bucket than "foo"
        let bucket_id_2 = bucket_for_tag_value("bar", NUM_BUCKETS);
        assert_ne!(bucket_id_1, bucket_id_2);
        assert!(!bucket_info.may_contain_value("bar"));

        // "baz" maps to the same bucket as "foo"
        let bucket_id_3 = bucket_for_tag_value("baz", NUM_BUCKETS);
        assert_eq!(bucket_id_1, bucket_id_3);
        assert!(bucket_info.may_contain_value("baz"));
    }

    #[test]
    fn test_bucket_info_null_bucket() {
        const NUM_BUCKETS: u32 = 50;

        let bucket_info = BucketInfo {
            id: None,
            num_buckets: NUM_BUCKETS,
        };

        // Since no bucket id info, it should return false for any tag value
        assert!(!bucket_info.may_contain_value("foo"));
        assert!(!bucket_info.may_contain_value("bar"));
        assert!(!bucket_info.may_contain_value("baz"));
    }
}
