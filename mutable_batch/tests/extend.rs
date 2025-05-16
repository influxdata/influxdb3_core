// Tests and benchmarks don't use all the crate dependencies and that's all right.
#![expect(unused_crate_dependencies)]

use arrow_util::assert_batches_eq;
use data_types::{StatValues, Statistics};
use mutable_batch::{MutableBatch, writer::Writer};
use schema::Projection;
use std::{collections::BTreeMap, num::NonZeroU64};

#[test]
fn test_extend() {
    let mut a = MutableBatch::new();
    let mut writer = Writer::new(&mut a, 5);

    writer
        .write_tag(
            "tag1",
            Some(&[0b00010101]),
            vec!["v1", "v1", "v2"].into_iter(),
        )
        .unwrap();

    writer
        .write_tag(
            "tag2",
            Some(&[0b00001101]),
            vec!["v2", "v1", "v1"].into_iter(),
        )
        .unwrap();

    writer
        .write_time("time", vec![0, 1, 2, 3, 4].into_iter())
        .unwrap();

    writer.commit();

    let mut b = MutableBatch::new();
    let mut writer = Writer::new(&mut b, 8);

    writer
        .write_tag(
            "tag1",
            Some(&[0b10010011]),
            vec!["v1", "v1", "v3", "v1"].into_iter(),
        )
        .unwrap();

    writer
        .write_tag(
            "tag3",
            None,
            vec!["v2", "v1", "v3", "v1", "v3", "v5", "v5", "v5"].into_iter(),
        )
        .unwrap();

    writer
        .write_time("time", vec![5, 6, 7, 8, 9, 10, 11, 12].into_iter())
        .unwrap();

    writer.commit();

    let a_before = a.clone().try_into_arrow(Projection::All).unwrap();

    a.extend_from(&b).unwrap();

    assert_batches_eq!(
        &[
            "+------+------+--------------------------------+",
            "| tag1 | tag2 | time                           |",
            "+------+------+--------------------------------+",
            "| v1   | v2   | 1970-01-01T00:00:00Z           |",
            "|      |      | 1970-01-01T00:00:00.000000001Z |",
            "| v1   | v1   | 1970-01-01T00:00:00.000000002Z |",
            "|      | v1   | 1970-01-01T00:00:00.000000003Z |",
            "| v2   |      | 1970-01-01T00:00:00.000000004Z |",
            "+------+------+--------------------------------+",
        ],
        &[a_before]
    );

    assert_batches_eq!(
        &[
            "+------+------+--------------------------------+",
            "| tag1 | tag3 | time                           |",
            "+------+------+--------------------------------+",
            "| v1   | v2   | 1970-01-01T00:00:00.000000005Z |",
            "| v1   | v1   | 1970-01-01T00:00:00.000000006Z |",
            "|      | v3   | 1970-01-01T00:00:00.000000007Z |",
            "|      | v1   | 1970-01-01T00:00:00.000000008Z |",
            "| v3   | v3   | 1970-01-01T00:00:00.000000009Z |",
            "|      | v5   | 1970-01-01T00:00:00.000000010Z |",
            "|      | v5   | 1970-01-01T00:00:00.000000011Z |",
            "| v1   | v5   | 1970-01-01T00:00:00.000000012Z |",
            "+------+------+--------------------------------+",
        ],
        &[b.clone().try_into_arrow(Projection::All).unwrap()]
    );

    assert_batches_eq!(
        &[
            "+------+------+------+--------------------------------+",
            "| tag1 | tag2 | tag3 | time                           |",
            "+------+------+------+--------------------------------+",
            "| v1   | v2   |      | 1970-01-01T00:00:00Z           |",
            "|      |      |      | 1970-01-01T00:00:00.000000001Z |",
            "| v1   | v1   |      | 1970-01-01T00:00:00.000000002Z |",
            "|      | v1   |      | 1970-01-01T00:00:00.000000003Z |",
            "| v2   |      |      | 1970-01-01T00:00:00.000000004Z |",
            "| v1   |      | v2   | 1970-01-01T00:00:00.000000005Z |",
            "| v1   |      | v1   | 1970-01-01T00:00:00.000000006Z |",
            "|      |      | v3   | 1970-01-01T00:00:00.000000007Z |",
            "|      |      | v1   | 1970-01-01T00:00:00.000000008Z |",
            "| v3   |      | v3   | 1970-01-01T00:00:00.000000009Z |",
            "|      |      | v5   | 1970-01-01T00:00:00.000000010Z |",
            "|      |      | v5   | 1970-01-01T00:00:00.000000011Z |",
            "| v1   |      | v5   | 1970-01-01T00:00:00.000000012Z |",
            "+------+------+------+--------------------------------+",
        ],
        &[a.clone().try_into_arrow(Projection::All).unwrap()]
    );

    let stats: BTreeMap<_, _> = a.columns().map(|(k, v)| (k.as_str(), v.stats())).collect();

    assert_eq!(
        stats["tag1"],
        Statistics::String(StatValues {
            min: Some("v1".to_string()),
            max: Some("v3".to_string()),
            total_count: 13,
            null_count: Some(6),
            distinct_count: Some(NonZeroU64::new(4).unwrap())
        })
    );

    assert_eq!(
        stats["tag2"],
        Statistics::String(StatValues {
            min: Some("v1".to_string()),
            max: Some("v2".to_string()),
            total_count: 13,
            null_count: Some(10),
            distinct_count: Some(NonZeroU64::new(3).unwrap())
        })
    );

    assert_eq!(
        stats["tag3"],
        Statistics::String(StatValues {
            min: Some("v1".to_string()),
            max: Some("v5".to_string()),
            total_count: 13,
            null_count: Some(5),
            distinct_count: Some(NonZeroU64::new(5).unwrap())
        })
    );

    assert_eq!(
        stats["time"],
        Statistics::I64(StatValues {
            min: Some(0),
            max: Some(12),
            total_count: 13,
            null_count: Some(0),
            distinct_count: None
        })
    )
}
