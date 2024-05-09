//! The setups available for any `TestCase` to use by specifying the test name in a comment at the
//! start of the `.sql` file in the form of:
//!
//! ```text
//! -- IOX_SETUP: [test name]
//! ```

use crate::{Step, StepTestState};
use futures_util::FutureExt;
use influxdb_iox_client::table::generated_types::{Bucket, Part, PartitionTemplate, TemplatePart};
use iox_time::{SystemProvider, Time, TimeProvider};
use once_cell::sync::Lazy;
use std::{collections::HashMap, env};

/// The string value that will appear in `.sql` files.
pub type SetupName = &'static str;
/// The steps that should be run when this setup is chosen.
pub type SetupSteps = Vec<Step>;

/// timestamps for the retention test
static RETENTION_SETUP: Lazy<RetentionSetup> = Lazy::new(RetentionSetup::new);

/// nanoseonds of a day
static DAY_NANO: i64 = 86_400_000_000_000;

/// All possible setups for the [`TestCase`][super::TestCase]s to use, indexed by name
pub static SETUPS: Lazy<HashMap<SetupName, SetupSteps>> = Lazy::new(|| {
    HashMap::from([
        (
            "Bugs",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        r#"checks,id=1,method=POST,name=Writes,url="https://example.com",user_id=1 elapsed=66909i,status=204i 1678578528255989730"#,
                        r#"checks,id=1,method=POST,name=Writes,url="https://example.com",user_id=1 elapsed=112928i,status=204i 1678578532192972168"#,
                        r#"checks,id=1,method=POST,name=Writes,url="https://example.com",user_id=1 elapsed=147683i,status=204i 1678578588416052133"#,
                        r#"checks,id=1,method=POST,name=Writes,url="https://example.com",user_id=1 elapsed=78000i,status=204i 1678578592147893402"#,
                        // Anonymized data from https://github.com/influxdata/influxdb_iox/issues/9622
                        r#"m1,tag_id=1000 f1=32,f2="foo",f3="True",f4=1.0 1703030400000000000"#,
                        r#"m1,tag_id=1000 f1=32,f2="foo",f3="True",f4=2.0 1703031000000000000"#,
                        r#"m1,tag_id=1000 f1=32,f2="foo",f3="True",f4=3.0 1703031600000000000"#,
                        r#"m1,tag_id=1000 f1=32,f2="foo",f3="True",f4=4.0 1703032200000000000"#,
                        r#"m1,tag_id=1000 f1=32,f2="foo",f3="True",f4=5.0 1703032800000000000"#,
                        r#"m1,tag_id=1000 f1=32,f2="foo",f3="True",f4=6.0 1703033400000000000"#,
                        r#"m1,tag_id=1000 f1=32,f2="foo",f3="True",f4=7.0 1703034000000000000"#,
                        r#"m1,tag_id=1000 f1=32,f2="foo",f3="True",f4=8.0 1703034600000000000"#,
                        r#"m1,tag_id=1000 f1=32,f2="foo",f3="True",f4=9.0 1703035200000000000"#,
                        r#"m1,tag_id=1000 f1=32,f2="foo",f3="True",f4=10.0 1703035800000000000"#,
                        r#"m2,type=active,tag_id=1000 f5=100 1701648000000000000"#,
                        r#"m2,type=active,tag_id=1000 f5=200 1701648600000000000"#,
                        r#"m2,type=active,tag_id=1000 f5=300 1701649200000000000"#,
                        r#"m2,type=active,tag_id=1000 f5=400 1701649800000000000"#,
                        r#"m2,type=active,tag_id=1000 f5=500 1701650400000000000"#,
                        r#"m2,type=active,tag_id=1000 f5=600 1701651000000000000"#,
                        r#"m2,type=active,tag_id=1000 f5=700 1701651600000000000"#,
                        r#"m2,type=active,tag_id=1000 f5=800 1701652200000000000"#,
                        r#"m2,type=active,tag_id=1000 f5=900 1701652800000000000"#,
                        r#"m2,type=active,tag_id=1000 f5=1000 1701653400000000000"#,
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
            ],
        ),
        (
            "TwoMeasurements",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "cpu,region=west user=23.2 100",
                        "cpu,region=west user=21.0 150",
                        "disk,region=east bytes=99i 200",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 2,
                },
            ],
        ),
        (
            "TwoChunksDedupWeirdnessParquet",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol("table,tag=A foo=1,bar=1 0".into()),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(["table,tag=A bar=2 0", "table,tag=B foo=1 0"].join("\n")),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
            ],
        ),
        (
            "TwoChunksDedupWeirdnessParquetIngester",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol("table,tag=A foo=1,bar=1 0".into()),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
                Step::WriteLineProtocol(["table,tag=A bar=2 0", "table,tag=B foo=1 0"].join("\n")),
            ],
        ),
        (
            // Ideal leading edge testing scenario with 5 chunks:
            //   . 2 non-overlapped parquet files with smal time range
            //   . 2 overlapped parquet files with larger time range
            //   . 1 ingester chunk with largest time range and also overlaps with the largest time-range parquet file
            "LeadingEdgeFiveChunks",
            vec![
                Step::RecordNumParquetFiles,
                // Chunk 1: parquet stage
                //  . time range: 50-249
                //  . no duplicates in its own chunk
                //  . no overlap with any other chunks
                Step::WriteLineProtocol(
                    [
                        "h2o,state=MA,city=Bedford min_temp=71.59 150",
                        "h2o,state=MA,city=Boston min_temp=70.4, 50",
                        "h2o,state=MA,city=Andover max_temp=69.2, 249",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
                Step::RecordNumParquetFiles,
                // Chunk 2: parquet stage
                //  . time range: 250 - 349
                //  . no duplicates in its own chunk
                //  . no overlap with any other chunks
                Step::WriteLineProtocol(
                    [
                        // new field (area)
                        "h2o,state=CA,city=SF min_temp=79.0,max_temp=87.2,area=500u 300",
                        "h2o,state=CA,city=SJ min_temp=75.5,max_temp=84.08 349",
                        "h2o,state=MA,city=Bedford max_temp=78.75,area=742u 300",
                        "h2o,state=MA,city=Boston min_temp=65.4 250",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
                Step::RecordNumParquetFiles,
                // Chunk 3: parquet stage
                //  . time range: 350 - 500
                //  . no duplicates in its own chunk
                //  . overlaps with chunk 4
                Step::WriteLineProtocol(
                    [
                        "h2o,state=CA,city=SJ min_temp=77.0,max_temp=90.7 450",
                        "h2o,state=CA,city=SJ min_temp=69.5,max_temp=88.2 500",
                        "h2o,state=MA,city=Boston min_temp=68.4 350",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
                Step::RecordNumParquetFiles,
                // Chunk 4: parquet stage
                //  . time range: 400 - 600
                //  . no duplicates in its own chunk
                //  . overlaps with chunk 3
                Step::WriteLineProtocol(
                    [
                        "h2o,state=CA,city=SF min_temp=68.4,max_temp=85.7,area=500u 600",
                        "h2o,state=CA,city=SJ min_temp=69.5,max_temp=89.2 600",  // duplicate with chunk 5
                        "h2o,state=MA,city=Bedford max_temp=80.75,area=742u 400",
                        "h2o,state=MA,city=Boston min_temp=65.40,max_temp=82.67 400",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
                // Chunk 5: ingester stage
                //  . time range: 550 - 700
                //  . overlaps & duplicates with Chunk 4
                Step::WriteLineProtocol(
                    [
                        "h2o,state=MA,city=Bedford max_temp=88.75,area=742u 600",
                        "h2o,state=CA,city=SF min_temp=68.4,max_temp=85.7,area=500u 650",
                        "h2o,state=CA,city=SJ min_temp=68.5,max_temp=90.0 600", // duplicate with second row of Chunk 4
                        "h2o,state=CA,city=SJ min_temp=75.5,max_temp=84.08 700",
                        "h2o,state=MA,city=Boston min_temp=67.4 550",
                    ]
                    .join("\n"),
                ),
            ],
        ),
        (
            // non-leading edge with overlapped chunks in the middle of the time range
            // use case for https://github.com/influxdata/influxdb_iox/issues/9535
            // There are 4 chunks in total: Chunk 1 and chunk 4 do not overlap with any other chunks
            // while chunk 2 and chunk 3 are middle time ranges and overlap with each other.
            "NonLeadingEdgeOverlappedMiddleChunks",
            vec![
                Step::RecordNumParquetFiles,
                // Chunk 1: parquet stage
                //  . time range: 50-249
                //  . no duplicates in its own chunk
                //  . no overlap with any other chunks
                Step::WriteLineProtocol(
                    [
                        "h2o,state=MA,city=Boston min_temp=70.4 50",
                        "h2o,state=MA,city=Andover max_temp=69.2, 249",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },

                Step::RecordNumParquetFiles,
                // Chunk 2: parquet stage
                //  . time range: 250 - 300
                //  . no duplicates in its own chunk
                //  . overlaps with chunk 3
                Step::WriteLineProtocol(
                    [
                        "h2o,state=MA,city=Bedford max_temp=78.75,area=742u 250",
                        "h2o,state=MA,city=Boston min_temp=65.4 250",
                        "h2o,state=CA,city=SF min_temp=79.0,max_temp=87.2,area=500u 300",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },

                Step::RecordNumParquetFiles,
                // Chunk 3: parquet stage
                //  . time range: 250 - 400
                //  . no duplicates in its own chunk
                //  . overlaps with chunk 2
                Step::WriteLineProtocol(
                    [
                        "h2o,state=MA,city=Reading min_temp=53.4 250",
                        "h2o,state=CA,city=SJ min_temp=78.5,max_temp=88.0 300",
                        "h2o,state=CA,city=SJ min_temp=75.5,max_temp=84.08 350",
                        "h2o,state=MA,city=Boston max_temp=75.4 400",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },

                // Chunk 4: ingester stage
                //  . time range: 401 - 500
                //  . no duplicates in its own chunk
                //  . no overlap with any other chunks
                Step::WriteLineProtocol(
                    [
                        "h2o,state=MA,city=Bedford max_temp=80.75,area=742u 401",
                        "h2o,state=CA,city=SJ min_temp=75.5,max_temp=84.08 500",
                    ]
                    .join("\n"),
                ),
            ]
        ),
        (
            // Note that the field `no_overlap` is added one row per chunk
            // with non-overlap values bweteen chunks on purpose. Please
            // keep them non-overlap in case you want to add more or change them
            "OneMeasurementFourChunksWithDuplicatesWithIngester",
            vec![
                Step::RecordNumParquetFiles,
                // Chunk 1:
                //  . time range: 50-250
                //  . no duplicates in its own chunk
                Step::WriteLineProtocol(
                    [
                        "h2o,state=MA,city=Boston min_temp=70.4,no_overlap=10 50",
                        "h2o,state=MA,city=Bedford min_temp=71.59 150",
                        "h2o,state=MA,city=Boston max_temp=75.4 250",
                        "h2o,state=MA,city=Andover max_temp=69.2, 250",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
                Step::RecordNumParquetFiles,
                // Chunk 2: overlaps with chunk 1
                //  . time range: 150 - 300
                //  . no duplicates in its own chunk
                Step::WriteLineProtocol(
                    [
                        // new field (area) and update available NULL (max_temp)
                        "h2o,state=MA,city=Bedford max_temp=78.75,area=742u 150",
                        "h2o,state=MA,city=Boston min_temp=65.4 250", // update min_temp from NULL
                        "h2o,state=MA,city=Reading min_temp=53.4,no_overlap=20 250",
                        "h2o,state=CA,city=SF min_temp=79.0,max_temp=87.2,area=500u 300",
                        "h2o,state=CA,city=SJ min_temp=78.5,max_temp=88.0 300",
                        "h2o,state=CA,city=SJ min_temp=75.5,max_temp=84.08 350",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
                Step::RecordNumParquetFiles,
                // Chunk 3: no overlap
                //  . time range: 400 - 500
                //  . duplicates in its own chunk
                Step::WriteLineProtocol(
                    [
                        "h2o,state=MA,city=Bedford max_temp=80.75,area=742u 400",
                        "h2o,state=MA,city=Boston min_temp=68.4 400",
                        "h2o,state=MA,city=Bedford min_temp=65.22,area=750u 400", // duplicate
                        "h2o,state=MA,city=Boston min_temp=65.40,max_temp=82.67 400", // duplicate
                        "h2o,state=CA,city=SJ min_temp=77.0,max_temp=90.7 450",
                        "h2o,state=CA,city=SJ min_temp=69.5,max_temp=88.2,no_overlap=100 500",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
                // Chunk 4: no overlap
                //  . time range: 600 - 700
                //  . no duplicates
                Step::WriteLineProtocol(
                    [
                        "h2o,state=MA,city=Bedford max_temp=88.75,area=742u,no_overlap=50 600",
                        "h2o,state=MA,city=Boston min_temp=67.4 600",
                        "h2o,state=MA,city=Reading min_temp=60.4, 600",
                        "h2o,state=CA,city=SF min_temp=68.4,max_temp=85.7,area=500u 650",
                        "h2o,state=CA,city=SJ min_temp=69.5,max_temp=89.2 650",
                        "h2o,state=CA,city=SJ min_temp=75.5,max_temp=84.08 700",
                    ]
                    .join("\n"),
                ),
            ],
        ),
        (
            "OneMeasurementFourChunksWithDuplicatesParquetOnly",
            vec![
                Step::RecordNumParquetFiles,
                // Chunk 1:
                //  . time range: 50-250
                //  . no duplicates in its own chunk
                Step::WriteLineProtocol(
                    [
                        "h2o,state=MA,city=Boston min_temp=70.4 50",
                        "h2o,state=MA,city=Bedford min_temp=71.59 150",
                        "h2o,state=MA,city=Boston max_temp=75.4 250",
                        "h2o,state=MA,city=Andover max_temp=69.2, 250",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
                Step::RecordNumParquetFiles,
                // Chunk 2: overlaps with chunk 1
                //  . time range: 150 - 300
                //  . no duplicates in its own chunk
                Step::WriteLineProtocol(
                    [
                        // new field (area) and update available NULL (max_temp)
                        "h2o,state=MA,city=Bedford max_temp=78.75,area=742u 150",
                        "h2o,state=MA,city=Boston min_temp=65.4 250", // update min_temp from NULL
                        "h2o,state=MA,city=Reading min_temp=53.4, 250",
                        "h2o,state=CA,city=SF min_temp=79.0,max_temp=87.2,area=500u 300",
                        "h2o,state=CA,city=SJ min_temp=78.5,max_temp=88.0 300",
                        "h2o,state=CA,city=SJ min_temp=75.5,max_temp=84.08 350",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
                Step::RecordNumParquetFiles,
                // Chunk 3: no overlap
                //  . time range: 400 - 500
                //  . duplicates in its own chunk
                Step::WriteLineProtocol(
                    [
                        "h2o,state=MA,city=Bedford max_temp=80.75,area=742u 400",
                        "h2o,state=MA,city=Boston min_temp=68.4 400",
                        "h2o,state=MA,city=Bedford min_temp=65.22,area=750u 400", // duplicate
                        "h2o,state=MA,city=Boston min_temp=65.40,max_temp=82.67 400", // duplicate
                        "h2o,state=CA,city=SJ min_temp=77.0,max_temp=90.7 450",
                        "h2o,state=CA,city=SJ min_temp=69.5,max_temp=88.2 500",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
                Step::RecordNumParquetFiles,
                // Chunk 4: no overlap
                //  . time range: 600 - 700
                //  . no duplicates
                Step::WriteLineProtocol(
                    [
                        "h2o,state=MA,city=Bedford max_temp=88.75,area=742u 600",
                        "h2o,state=MA,city=Boston min_temp=67.4 600",
                        "h2o,state=MA,city=Reading min_temp=60.4, 600",
                        "h2o,state=CA,city=SF min_temp=68.4,max_temp=85.7,area=500u 650",
                        "h2o,state=CA,city=SJ min_temp=69.5,max_temp=89.2 650",
                        "h2o,state=CA,city=SJ min_temp=75.5,max_temp=84.08 700",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
            ],
        ),
        (
            "TwentySortedParquetFiles",
            (0..20)
                .flat_map(|i| {
                    let write = if i % 2 == 0 {
                        Step::WriteLineProtocol(format!(
                            "m,tag=A f=1 {}\nm,tab=B f=2 {}",
                            1000 - i, // unique in this chunk
                            1000 - i, // unique in this chunk (not plus i!)
                        ))
                    } else {
                        Step::WriteLineProtocol(
                            "m,tag=A f=3 2001".into(), // duplicated across all chunks
                        )
                    };
                    [
                        Step::RecordNumParquetFiles,
                        write,
                        Step::Persist,
                        Step::WaitForPersisted {
                            expected_increase: 1,
                        },
                    ]
                    .into_iter()
                })
                .collect::<Vec<_>>(),
        ),
        (
            "TwentySortedParquetFilesAndIngester",
            (0..20)
                .flat_map(|i| {
                    let write = if i % 2 == 0 {
                        Step::WriteLineProtocol(format!(
                            "m,tag=A f=1 {}\nm,tab=B f=2 {}",
                            1000 - i, // unique in this chunk
                            1000 - i, // unique in this chunk (not plus i!)
                        ))
                    } else {
                        Step::WriteLineProtocol(
                            "m,tag=A f=3 2001".into(), // duplicated across all chunks
                        )
                    };
                    [
                        Step::RecordNumParquetFiles,
                        write,
                        Step::Persist,
                        Step::WaitForPersisted {
                            expected_increase: 1,
                        },
                    ]
                    .into_iter()
                }).chain([
                        Step::WriteLineProtocol(
                            "m,tag=A f=3 2001".into(), // duplicated across all chunks
                        )
                ])
                .collect::<Vec<_>>(),
        ),
        (
            "FiftySortedSameParquetFiles",
            (0..50)
                .flat_map(|_i| {

                    let write = Step::WriteLineProtocol(
                        "m,tag1=A,tag2=B,tag3=C,tag4=D f1=1,f2=2 2001".into(), // duplicated across all chunks
                    );

                    [
                        Step::RecordNumParquetFiles,
                        write,
                        Step::Persist,
                        Step::WaitForPersisted {
                            expected_increase: 1,
                        },
                    ]
                    .into_iter()
                })
                .collect::<Vec<_>>(),
        ),

        (
            "OneMeasurementManyFields",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "h2o,tag1=foo,tag2=bar field1=70.6,field3=2 100",
                        "h2o,tag1=foo,tag2=bar field1=70.4,field2=\"ss\" 100",
                        "h2o,tag1=foo,tag2=bar field1=70.5,field2=\"ss\" 100",
                        "h2o,tag1=foo,tag2=bar field1=70.6,field4=true 1000",
                        "h2o,tag1=foo,tag2=bar field1=70.3,field5=false 3000",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
            ],
        ),
        (
            "TwoMeasurementsManyFields",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "h2o,state=MA,city=Boston temp=70.4 50",
                        "h2o,state=MA,city=Boston other_temp=70.4 250",
                        "h2o,state=CA,city=Boston other_temp=72.4 350",
                        "o2,state=MA,city=Boston temp=53.4,reading=51 50",
                        "o2,state=CA temp=79.0 300",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 2,
                },
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    "h2o,state=MA,city=Boston temp=70.4,moisture=43.0 100000".into(),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
            ],
        ),
        (
            "TwoMeasurementsManyFieldsTwoChunks",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "h2o,state=MA,city=Boston temp=70.4 50",
                        "h2o,state=MA,city=Boston other_temp=70.4 250",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
                Step::WriteLineProtocol(
                    [
                        "h2o,state=CA,city=Boston other_temp=72.4 150",
                        "o2,state=MA,city=Boston temp=53.4,reading=51 50",
                        "o2,state=CA temp=79.0 300",
                    ]
                    .join("\n"),
                ),
                // The system tables test looks for queries, so the setup needs to run this query.
                Step::Query {
                    sql: "SELECT 1;".into(),
                    expected: vec![
                        "+----------+",
                        "| Int64(1) |",
                        "+----------+",
                        "| 1        |",
                        "+----------+",
                    ],
                },
            ],
        ),
        (
            "TwoMeasurementsManyNulls",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "h2o,state=CA,city=LA,county=LA temp=70.4 100",
                        "h2o,state=MA,city=Boston,county=Suffolk temp=72.4 250",
                        "o2,state=MA,city=Boston temp=50.4 200",
                        "o2,state=CA temp=79.0 300",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 2,
                },
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "o2,state=NY temp=60.8 400",
                        "o2,state=NY,city=NYC temp=61.0 500",
                        "o2,state=NY,city=NYC,borough=Brooklyn temp=61.0 600",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
            ],
        ),
        (
            "OneMeasurementWithTags",
            vec![Step::WriteLineProtocol(
                [
                    "cpu,foo=me bar=1 10",
                    "cpu,foo=me bar=200 10",
                    "cpu,foo=you bar=2 20",
                    "cpu,foo=me bar=1 30",
                    "cpu,foo=me bar=1 40",
                ]
                .join("\n"),
            )],
        ),
        (
            "EqualInMeasurements",
            vec![Step::WriteLineProtocol(
                [
                    "measurement=one,tag=value field=1.0 \
                    1609459201000000001",
                    "measurement=one,tag=value2 field=1.0 \
                    1609459201000000002",
                ]
                .join("\n"),
            )],
        ),
        (
            "PeriodsInNames",
            vec![Step::WriteLineProtocol(
                [
                    "measurement.one,tag.one=value,tag.two=other field.one=1.0,field.two=t \
                    1609459201000000001",
                    "measurement.one,tag.one=value2,tag.two=other2 field.one=1.0,field.two=f \
                    1609459201000000002",
                ]
                .join("\n"),
            )],
        ),
        (
            "TwoMeasurementsPredicatePushDown",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "restaurant,town=andover count=40000u,system=5.0 100",
                        "restaurant,town=reading count=632u,system=5.0 120",
                        "restaurant,town=bedford count=189u,system=7.0 110",
                        "restaurant,town=tewsbury count=471u,system=6.0 110",
                        "restaurant,town=lexington count=372u,system=5.0 100",
                        "restaurant,town=lawrence count=872u,system=6.0 110",
                        "restaurant,town=reading count=632u,system=6.0 130",
                    ]
                    .join("\n"),
                ),
                Step::WriteLineProtocol(
                    [
                        "school,town=reading count=17u,system=6.0 150",
                        "school,town=andover count=25u,system=6.0 160",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 2,
                },
            ],
        ),
        (
            "AllTypes",
            vec![Step::WriteLineProtocol(
                [
                    "m,tag=row1 float_field=64.0 450",
                    "m,tag=row1 int_field=64 550",
                    "m,tag=row1 \
                        float_field=61.0,int_field=22,uint_field=25u,\
                        string_field=\"foo\",bool_field=t 500",
                    "m,tag=row1 \
                        float_field=62.0,int_field=21,uint_field=30u,\
                        string_field=\"ba\",bool_field=f 200",
                    "m,tag=row1 \
                        float_field=63.0,int_field=20,uint_field=35u,\
                        string_field=\"baz\",bool_field=f 300",
                    "m,tag=row1 \
                        float_field=64.0,int_field=19,uint_field=20u,\
                        string_field=\"bar\",bool_field=t 400",
                    "m,tag=row1 \
                        float_field=65.0,int_field=18,uint_field=40u,\
                        string_field=\"fruz\",bool_field=f 100",
                    "m,tag=row1 \
                        float_field=66.0,int_field=17,uint_field=10u,\
                        string_field=\"faa\",bool_field=t 600",
                ]
                .join("\n"),
            )],
        ),
        (
            "ManyFieldsSeveralChunks",
            vec![
                Step::RecordNumParquetFiles,
                // c1: parquet stage
                Step::WriteLineProtocol(
                    [
                        "h2o,state=MA,city=Boston temp=70.4 50",
                        // duplicate with a row in c4 and will be removed
                        "h2o,state=MA,city=Boston other_temp=70.4 250",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
                Step::RecordNumParquetFiles,
                // c2: parquet stage & overlaps with c1
                Step::WriteLineProtocol("h2o,state=CA,city=Andover other_temp=72.4 150".into()),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
                Step::RecordNumParquetFiles,
                // c3: parquet stage & doesn't overlap with any
                Step::WriteLineProtocol(
                    [
                        "h2o,state=MA,city=Boston temp=80.7 350",
                        "h2o,state=MA,city=Boston other_temp=68.2 450",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
                Step::RecordNumParquetFiles,
                // c4: parquet stage & overlap with c1
                Step::WriteLineProtocol(
                    [
                        "h2o,state=MA,city=Boston temp=88.6 230",
                        // duplicate with a row in c1 but more
                        // recent => this row is kept
                        "h2o,state=MA,city=Boston other_temp=80 250",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
                // c5: ingester stage & doesn't overlap with any
                Step::WriteLineProtocol("h2o,state=CA,city=Andover temp=67.3 500".into()),
            ],
        ),
        (
            "MeasurementWithMaxTime",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(format!(
                    "cpu,host=server01 value=100 {}",
                    // This is the maximum timestamp that can be represented in the InfluxDB data
                    // model:
                    // <https://github.com/influxdata/influxdb/blob/540bb66e1381a48a6d1ede4fc3e49c75a7d9f4af/models/time.go#L12-L34>
                    i64::MAX - 1, // 9223372036854775806
                )),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
            ],
        ),
        (
            "OneMeasurementRealisticTimes",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "cpu,region=west user=23.2 1626809330000000000",
                        "cpu,region=west user=21.0 1626809430000000000",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
            ],
        ),
        (
            "OneMeasurementTwoSeries",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "cpu,region=a user=23.2,idle=70.0 957529200000000000", // 2000-05-05T12:20:00Z
                        "cpu,region=b user=25.2           957529860000000000", // 2000-05-05T12:31:00Z
                        "cpu,region=b user=28.9,idle=60.0 957530340000000000", // 2000-05-05T12:39:00Z
                        "cpu,region=a user=21.0           957530400000000000", // 2000-05-05T12:40:00Z
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
            ],
        ),
        (
            "TwoChunksMissingColumns",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol("table,tag1=a,tag2=b field1=10,field2=11 100".into()),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol("table,tag1=a,tag3=c field1=20,field3=22 200".into()),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
            ],
        ),
        (
            "TwoMeasurementsMultiSeries",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        // Data is deliberately not in series order.
                        "h2o,state=CA,city=LA temp=90.0 200",
                        "h2o,state=MA,city=Boston temp=72.4 250",
                        "h2o,state=MA,city=Boston temp=70.4 100",
                        "h2o,state=CA,city=LA temp=90.0 350",
                        "o2,state=MA,city=Boston temp=53.4,reading=51 250",
                        "o2,state=MA,city=Boston temp=50.4,reading=50 100",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 2,
                },
            ],
        ),
        (
            // This recreates the test case for <https://github.com/influxdata/idpe/issues/16238>.
            "StringFieldWithNumericValue",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    ["m,tag0=foo fld=\"200\" 1000", "m,tag0=foo fld=\"404\" 1050"].join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
            ],
        ),
        (
            "MeasurementStatusCode",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "status_code,url=http://www.example.com value=404 1527018806000000000",
                        "status_code,url=https://influxdb.com value=418 1527018816000000000",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
            ],
        ),
        (
            "TwoMeasurementsMultiTagValue",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "h2o,state=MA,city=Boston temp=70.4 100",
                        "h2o,state=MA,city=Lowell temp=75.4 100",
                        "h2o,state=CA,city=LA temp=90.0 200",
                        "o2,state=MA,city=Boston temp=50.4,reading=50 100",
                        "o2,state=KS,city=Topeka temp=60.4,reading=60 100",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 2,
                },
            ],
        ),
        (
            "MeasurementsSortableTags",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "h2o,zz_tag=A,state=MA,city=Kingston temp=70.1 800",
                        "h2o,state=MA,city=Kingston,zz_tag=B temp=70.2 100",
                        "h2o,state=CA,city=Boston temp=70.3 250",
                        "h2o,state=MA,city=Boston,zz_tag=A temp=70.4 1000",
                        "h2o,state=MA,city=Boston temp=70.5,other=5.0 250",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
            ],
        ),
        (
            // See issue: https://github.com/influxdata/influxdb_iox/issues/2845
            "MeasurementsForDefect2845",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "system,host=host.local load1=1.83 1527018806000000000",
                        "system,host=host.local load1=1.63 1527018816000000000",
                        "system,host=host.local load3=1.72 1527018806000000000",
                        "system,host=host.local load4=1.77 1527018806000000000",
                        "system,host=host.local load4=1.78 1527018816000000000",
                        "system,host=host.local load4=1.77 1527018826000000000",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
            ],
        ),
        (
            "EndToEndTest",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "cpu_load_short,host=server01,region=us-west value=0.64 0000",
                        "cpu_load_short,host=server01 value=27.99 1000",
                        "cpu_load_short,host=server02,region=us-west value=3.89 2000",
                        "cpu_load_short,host=server01,region=us-east value=1234567.891011 3000",
                        "cpu_load_short,host=server01,region=us-west value=0.000003 4000",
                        "system,host=server03 uptime=1303385 5000",
                        "swap,host=server01,name=disk0 in=3,out=4 6000",
                        "status active=t 7000",
                        "attributes color=\"blue\" 8000",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 5,
                },
            ],
        ),
        (
            "OneMeasurementNoTags",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "h2o temp=70.4 100",
                        "h2o temp=72.4 250",
                        "h2o temp=50.4 200",
                        "h2o level=200.0 300",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
            ],
        ),
        (
            "OneMeasurementNoTags2",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(["m0 foo=1.0 1", "m0 foo=2.0 2"].join("\n")),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
            ],
        ),
        (
            "OneMeasurementForAggs",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "h2o,state=MA,city=Boston temp=70.4 100",
                        "h2o,state=MA,city=Boston temp=72.4 250",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "h2o,state=CA,city=LA temp=90.0 200",
                        "h2o,state=CA,city=LA temp=90.0 350",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
            ],
        ),
        (
            "TwoMeasurementForAggs",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "h2o,state=MA,city=Boston temp=70.4 100",
                        "h2o,state=MA,city=Boston temp=72.4 250",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "o2,state=CA,city=LA temp=90.0 200",
                        "o2,state=CA,city=LA temp=90.0 350",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
            ],
        ),
        (
            "AnotherMeasurementForAggs",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "h2o,state=MA,city=Cambridge temp=80 50",
                        "h2o,state=MA,city=Cambridge temp=81 100",
                        "h2o,state=MA,city=Cambridge temp=82 200",
                        "h2o,state=MA,city=Boston temp=70 300",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "h2o,state=MA,city=Boston temp=71 400",
                        "h2o,state=CA,city=LA temp=90,humidity=10 500",
                        "h2o,state=CA,city=LA temp=91,humidity=11 600",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
            ],
        ),
        (
            "MeasurementForSelectors",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    ["h2o,state=MA,city=Cambridge f=8.0,i=8i,b=true,s=\"d\" 1000"].join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "h2o,state=MA,city=Cambridge f=7.0,i=7i,b=true,s=\"c\" 2000",
                        "h2o,state=MA,city=Cambridge f=6.0,i=6i,b=false,s=\"b\" 3000",
                        "h2o,state=MA,city=Cambridge f=5.0,i=5i,b=false,s=\"a\" 4000",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
            ],
        ),
        (
            "MeasurementForMin",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "h2o,state=MA,city=Cambridge f=8.0,i=8i,b=false,s=\"c\" 1000",
                        "h2o,state=MA,city=Cambridge f=7.0,i=7i,b=true,s=\"a\" 2000",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "h2o,state=MA,city=Cambridge f=6.0,i=6i,b=true,s=\"z\" 3000",
                        "h2o,state=MA,city=Cambridge f=5.0,i=5i,b=false,s=\"c\" 4000",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
            ],
        ),
        (
            "MeasurementForMax",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "h2o,state=MA,city=Cambridge f=8.0,i=8i,b=true,s=\"c\" 1000",
                        "h2o,state=MA,city=Cambridge f=7.0,i=7i,b=false,s=\"d\" 2000",
                        "h2o,state=MA,city=Cambridge f=6.0,i=6i,b=true,s=\"a\" 3000",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    ["h2o,state=MA,city=Cambridge f=5.0,i=5i,b=false,s=\"z\" 4000"].join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
            ],
        ),
        (
            "MeasurementForGroupKeys",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "h2o,state=MA,city=Cambridge temp=80 50",
                        "h2o,state=MA,city=Cambridge temp=81 100",
                        "h2o,state=MA,city=Cambridge temp=82 200",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "h2o,state=MA,city=Boston temp=70 300",
                        "h2o,state=MA,city=Boston temp=71 400",
                        "h2o,state=CA,city=LA temp=90,humidity=10 500",
                        "h2o,state=CA,city=LA temp=91,humidity=11 600",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
            ],
        ),
        (
            "MeasurementForGroupByField",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "system,host=local,region=A load1=1.1,load2=2.1 100",
                        "system,host=local,region=A load1=1.2,load2=2.2 200",
                        "system,host=remote,region=B load1=10.1,load2=2.1 100",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "system,host=remote,region=B load1=10.2,load2=20.2 200",
                        "system,host=local,region=C load1=100.1,load2=200.1 100",
                        "aa_system,host=local,region=C load1=100.1,load2=200.1 100",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
            ],
        ),
        (
            "TwoMeasurementsManyFieldsOneChunk",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "h2o,state=MA,city=Boston temp=70.4 50",
                        "h2o,state=MA,city=Boston other_temp=70.4 250",
                        "h2o,state=CA,city=Boston other_temp=72.4 350",
                        "o2,state=MA,city=Boston temp=53.4,reading=51 50",
                        "o2,state=CA temp=79.0 300",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 2,
                },
            ],
        ),
        (
            "MeasurementForDefect2691",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "system,host=host.local load1=1.83 1527018806000000000",
                        "system,host=host.local load1=1.63 1527018816000000000",
                        "system,host=host.local load3=1.72 1527018806000000000",
                        "system,host=host.local load4=1.77 1527018806000000000",
                        "system,host=host.local load4=1.78 1527018816000000000",
                        "system,host=host.local load4=1.77 1527018826000000000",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
            ],
        ),
        (
            "MeasurementForWindowAggregate",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "h2o,state=MA,city=Boston temp=70.0 100",
                        "h2o,state=MA,city=Boston temp=71.0 200",
                        "h2o,state=MA,city=Boston temp=72.0 300",
                        "h2o,state=MA,city=Boston temp=73.0 400",
                        "h2o,state=MA,city=Boston temp=74.0 500",
                        "h2o,state=MA,city=Cambridge temp=80.0 100",
                        "h2o,state=MA,city=Cambridge temp=81.0 200",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "h2o,state=MA,city=Cambridge temp=82.0 300",
                        "h2o,state=MA,city=Cambridge temp=83.0 400",
                        "h2o,state=MA,city=Cambridge temp=84.0 500",
                        "h2o,state=CA,city=LA temp=90.0 100",
                        "h2o,state=CA,city=LA temp=91.0 200",
                        "h2o,state=CA,city=LA temp=92.0 300",
                        "h2o,state=CA,city=LA temp=93.0 400",
                        "h2o,state=CA,city=LA temp=94.0 500",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
            ],
        ),
        (
            // Test data to validate fix for
            // <https://github.com/influxdata/influxdb_iox/issues/2697>
            "MeasurementForDefect2697",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "mm,section=1a bar=5.0 1609459201000000011",
                        "mm,section=1a bar=0.28 1609459201000000031",
                        "mm,section=2b bar=4.0 1609459201000000009",
                        "mm,section=2b bar=6.0 1609459201000000015",
                        "mm,section=2b bar=1.2 1609459201000000022",
                        "mm,section=1a foo=1.0 1609459201000000001",
                        "mm,section=1a foo=3.0 1609459201000000005",
                        "mm,section=1a foo=11.24 1609459201000000024",
                        "mm,section=2b foo=2.0 1609459201000000002",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
            ],
        ),
        (
            "ThreeChunksWithRetention",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(RETENTION_SETUP.lp_partially_inside_1.clone()),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(RETENTION_SETUP.lp_fully_inside.clone()),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(RETENTION_SETUP.lp_fully_outside.clone()),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
                Step::WriteLineProtocol(RETENTION_SETUP.lp_partially_inside_2.clone()),
                Step::SetRetention(Some(RETENTION_SETUP.retention_period_ns)),
            ],
        ),
        (
            // Test data to validate fix for
            // <https://github.com/influxdata/influxdb_iox/issues/2890>
            "MeasurementForDefect2890",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "mm foo=2.0 1609459201000000001",
                        "mm foo=2.0 1609459201000000002",
                        "mm foo=3.0 1609459201000000005",
                        "mm foo=11.24 1609459201000000024",
                        "mm bar=4.0 1609459201000000009",
                        "mm bar=5.0 1609459201000000011",
                        "mm bar=6.0 1609459201000000015",
                        "mm bar=1.2 1609459201000000022",
                        "mm bar=2.8 1609459201000000031",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
            ],
        ),
        (
            // Single measurement that has several different chunks with different (but
            // compatible) schemas
            "MultiChunkSchemaMerge",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "cpu,region=west user=23.2,system=5.0 100",
                        "cpu,region=west user=21.0,system=6.0 150",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "cpu,region=east,host=foo user=23.2 100",
                        "cpu,region=west,host=bar user=21.0 250",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
            ],
        ),
        (
            "MultipleAggregates",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "cpu,region=east,city=Boston user=23.2,system=5.1 100",
                        "cpu,region=west,city=LA user=23.2,system=5.2 100",
                    ]
                        .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
                Step::RecordNumParquetFiles,
                // ensure the doesn't overlap in time so it doesn't get deduplicated
                // but it does have the same region values repeated
                Step::WriteLineProtocol(
                    [
                        "cpu,region=east,city=Chicago user=23.1,system=5.3 20000",
                        "cpu,region=west,city=Milwaukee user=23.1,system=5.4 20000",
                    ]
                        .join("\n"),
                ),
            ],
        ),
        (
            "TwoMeasurementsUnsignedType",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "restaurant,town=andover count=40000u 100",
                        "restaurant,town=reading count=632u 120",
                        "school,town=reading count=17u 150",
                        "school,town=andover count=25u 160",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
            ],
        ),
        (
            // This has two chunks with different tag/key sets for queries whose columns do not
            // include keys
            "OneMeasurementTwoChunksDifferentTagSet",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        // tag: state
                        "h2o,state=MA temp=70.4 50",
                        "h2o,state=MA other_temp=70.4 250",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        // tag: city
                        "h2o,city=Boston other_temp=72.4 350",
                        "h2o,city=Boston temp=53.4,reading=51 50",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
            ],
        ),
        (
            // This is the dataset defined by https://github.com/influxdata/influxdb_iox/issues/6112
            "InfluxQLSelectSupport",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    r#"
                    m1,tag0=val00 f64=100.5,i64=1001i,str="hi" 1667181600000000000
                    m1,tag0=val00 f64=200.6,i64=2001i,str="lo" 1667181610000000000
                    m1,tag0=val01 f64=101.7,i64=1011i,str="lo" 1667181600000000000
                    m0,tag0=val00 f64=10.1,i64=101i,str="hi" 1667181600000000000
                    m0,tag0=val00 f64=21.2,i64=211i,str="hi" 1667181610000000000
                    m0,tag0=val00 f64=11.2,i64=191i,str="lo" 1667181620000000000
                    m0,tag0=val00 f64=19.2,i64=392i,str="lo" 1667181630000000000
                    m0,tag0=val01 f64=11.3,i64=211i,str="lo" 1667181600000000000
                    m0,tag0=val02 f64=10.4,i64=101i,str="lo" 1667181600000000000
                    m0,tag0=val00,tag1=val10 f64=18.9,i64=211i,str="lo" 1667181610000000000
                    cpu,host=host1,cpu=cpu-total usage_idle=2.98,usage_system=2.2 1667181600000000000
                    cpu,host=host1,cpu=cpu-total usage_idle=2.99,usage_system=2.1 1667181610000000000
                    cpu,host=host1,cpu=cpu0 usage_idle=0.98,usage_system=0.2 1667181600000000000
                    cpu,host=host1,cpu=cpu0 usage_idle=0.99,usage_system=0.1 1667181610000000000
                    cpu,host=host1,cpu=cpu1 usage_idle=1.98,usage_system=1.2 1667181600000000000
                    cpu,host=host1,cpu=cpu1 usage_idle=1.99,usage_system=1.1 1667181610000000000
                    disk,host=host1,device=disk1s1 bytes_free=1234i,bytes_used=219838i 1667181600000000000
                    disk,host=host1,device=disk1s1 bytes_free=1239i,bytes_used=219833i 1667181610000000000
                    disk,host=host1,device=disk1s2 bytes_free=2234i,bytes_used=319838i 1667181600000000000
                    disk,host=host1,device=disk1s2 bytes_free=2239i,bytes_used=319833i 1667181610000000000
                    disk,host=host1,device=disk1s5 bytes_free=3234i,bytes_used=419838i 1667181600000000000
                    disk,host=host1,device=disk1s5 bytes_free=3239i,bytes_used=419833i 1667181610000000000
                    m2,tag0=val00 f64=0.98 1667181600000000000
                    m2,tag0=val02 f64=1.98 1667181600000000000
                    m2,tag0=val01 f64=2.98 1667181600000000000
                    m2,tag0=val05 f64=3.98 1667181600000000000
                    m2,tag0=val03 f64=4.98 1667181600000000000
                    m2,tag0=val09 f64=5.98 1667181600000000000
                    m2,tag0=val10 f64=6.98 1667181600000000000
                    m2,tag0=val08 f64=7.98 1667181600000000000
                    m2,tag0=val07 f64=8.98 1667181600000000000
                    m2,tag0=val04 f64=9.98 1667181600000000000
                    m3,tag0=a,tag1=b,tag2=c,tag3=d u64=1u 1667181600000000000
                    m4,tag.one=foo field.one=1 1667181600000000000
                    time_test,tt_tag=before_default_cutoff,tt_tag_before_default_cutoff=a tt_field="before_default_cutoff",tt_field_before_default_cutoff=1 631151999999999999
                    time_test,tt_tag=at_default_cutoff,tt_tag_at_default_cutoff=a tt_field="at_default_cutoff",tt_field_at_default_cutoff=1 631152000000000000
                    time_test,tt_tag=late,tt_tag_late=1 tt_field="late",tt_field_late=1 1667181600000000000
                    select_test,tag0=a,tag1=a,st_tag=aa,st_tag_aa=x st_field="aa",st_field_aa=1 1667181600000000000
                    select_test,tag0=a,tag1=b,st_tag=ab,st_tag_ab=x st_field="ab",st_field_ab=1 1667181600000000000
                    select_test,tag0=b,tag1=a,st_tag=ba,st_tag_ba=x st_field="ba",st_field_ba=1 1667181600000000000
                    select_test,tag0=b,tag1=b,st_tag=bb,st_tag_bb=x st_field="bb",st_field_bb=1 1667181600000000000
                    selector_test_1,tag1=a field1=1 1
                    selector_test_1,tag2=b field2=2 2
                    selector_test_1,tag3=c field3=3 3
                    selector_test_2,first=a f=1 1
                    "#
                    .to_string(),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 2,
                },
            ],
        ),
        (
            // Used for window-like function tests for InfluxQL
            "window_like",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    include_str!("data/window_like.lp").to_string()
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
            ],
        ),
        (
            // Used for top/bottom function tests for InfluxQL
            "top_bottom",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    include_str!("data/top_bottom.lp").to_string()
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
            ],
        ),
        (
            // Used for percentile function tests for InfluxQL
            "percentile",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    include_str!("data/percentile.lp").to_string()
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
            ],
        ),
        (
            // tests for InfluxQL issue 10608
            "issue_10608",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    include_str!("data/issue_10608.lp").to_string()
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 1,
                },
            ],
        ),
        (
            "DuplicateDifferentDomains",
            (0..2)
                .flat_map(|_| {
                    [
                        Step::RecordNumParquetFiles,
                        Step::WriteLineProtocol(
                            r#"
                            m,tag=A f=1 0
                            m,tag=A f=2 86400000000000
                            "#.into(),
                        ),
                        Step::Persist,
                        Step::WaitForPersisted {
                            expected_increase: 2,
                        },
                        Step::RecordNumParquetFiles,
                        Step::WriteLineProtocol(
                            r#"
                            m,tag=A f=3 1
                            "#.into(),
                        ),
                        Step::Persist,
                        Step::WaitForPersisted {
                            expected_increase: 1,
                        },
                    ]
                    .into_iter()
                })
                .collect::<Vec<_>>(),
        ),
        (
            "CustomPartitioning",
            [
                Step::Custom(Box::new(move |state: &mut StepTestState<'_>| {
                    async move {
                        let namespace_name = state.cluster().namespace();

                        let mut namespace_client = influxdb_iox_client::namespace::Client::new(
                            state.cluster().router().router_grpc_connection(),
                        );
                        namespace_client
                            .create_namespace(namespace_name, None, None, Some(PartitionTemplate{
                                parts: vec![
                                    TemplatePart{
                                        part: Some(Part::TagValue("tag1".into())),
                                    },
                                    TemplatePart{
                                        part: Some(Part::TagValue("tag3".into())),
                                    },
                                    // All new partitioning templates require a time format.
                                    TemplatePart{
                                        part: Some(Part::TimeFormat("%Y".into())),
                                    },
                                ],
                            }))
                            .await
                            .unwrap();

                        let mut table_client = influxdb_iox_client::table::Client::new(
                            state.cluster().router().router_grpc_connection(),
                        );

                        // table1: create implicitly by writing to it

                        // table2: do not override partition template => use namespace template
                        table_client.create_table(
                            namespace_name,
                            "table2",
                            None,
                        ).await.unwrap();

                        // table3: overide namespace template
                        table_client.create_table(
                            namespace_name,
                            "table3",
                            Some(PartitionTemplate{
                                parts: vec![
                                    TemplatePart{
                                        part: Some(Part::TagValue("tag2".into())),
                                    },
                                    // All new partitioning templates require a time format.
                                    TemplatePart{
                                        part: Some(Part::TimeFormat("%Y".into())),
                                    },
                                ],
                            }),
                        ).await.unwrap();
                    }
                    .boxed()
                })),
            ].into_iter()
            .chain(
                (1..=3).flat_map(|tid| {
                    [
                        Step::RecordNumParquetFiles,
                        Step::WriteLineProtocol(
                            [
                                format!("table{tid},tag1=v1a,tag2=v2a,tag3=v3a f=1 11"),
                                format!("table{tid},tag1=v1b,tag2=v2a,tag3=v3a f=1 11"),
                                format!("table{tid},tag1=v1a,tag2=v2b,tag3=v3a f=1 11"),
                                format!("table{tid},tag1=v1b,tag2=v2b,tag3=v3a f=1 11"),
                                format!("table{tid},tag1=v1a,tag2=v2a,tag3=v3b f=1 11"),
                                format!("table{tid},tag1=v1b,tag2=v2a,tag3=v3b f=1 11"),
                                format!("table{tid},tag1=v1a,tag2=v2b,tag3=v3b f=1 11"),
                                format!("table{tid},tag1=v1b,tag2=v2b,tag3=v3b f=1 11"),
                            ]
                            .join("\n"),
                        ),
                        Step::Persist,
                        Step::WaitForPersisted {
                            expected_increase: match tid {
                                1 => 4,
                                2 => 4,
                                3 => 2,
                                _ => unreachable!(),
                            },
                        },
                    ].into_iter()
                })
            )
            .collect(),
        ),
        (
            "CustomPartitioningWithoutTagsOfTemplate",
            [
                Step::Custom(Box::new(move |state: &mut StepTestState<'_>| {
                    async move {
                        let namespace_name = state.cluster().namespace();

                        let mut namespace_client = influxdb_iox_client::namespace::Client::new(
                            state.cluster().router().router_grpc_connection(),
                        );
                        namespace_client
                            .create_namespace(namespace_name, None, None, Some(PartitionTemplate{
                                parts: vec![
                                    TemplatePart{
                                        part: Some(Part::TagValue("tag1".into())),
                                    },
                                    TemplatePart{
                                        part: Some(Part::TagValue("tag3".into())),
                                    },
                                    // All new partitioning templates require a time format.
                                    TemplatePart{
                                        part: Some(Part::TimeFormat("%Y".into())),
                                    },
                                ],
                            }))
                            .await
                            .unwrap();

                        // table1: create implicitly by writing to it and will inherit the namespace's partition template

                    }
                    .boxed()
                })),
                // Four partitions are created using the template 'tag1|tag3'
                //
                // v1a|v3a
                // v1b|v3a
                // v1a|v3b
                // v1b|v3b
                // Each partition include 1 file
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "table1,tag1=v1a,tag2=v2a,tag3=v3a f=1 11",
                        "table1,tag1=v1b,tag2=v2a,tag3=v3a f=1 11",
                        "table1,tag1=v1a,tag2=v2b,tag3=v3a f=1 11",
                        "table1,tag1=v1b,tag2=v2b,tag3=v3a f=1 11",
                        "table1,tag1=v1a,tag2=v2a,tag3=v3b f=1 11",
                        "table1,tag1=v1b,tag2=v2a,tag3=v3b f=1 11",
                        "table1,tag1=v1a,tag2=v2b,tag3=v3b f=1 11",
                        "table1,tag1=v1b,tag2=v2b,tag3=v3b f=1 11",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 4,
                },
                // Two partitions are created using the template 'tag1|tag3'.
                // Becasue no tag1 in the data, its value is "!"
                //
                // !|v3a
                // !|v3b
                // Each partition include 1 file
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "table1,tag2=v2a,tag3=v3a f=1 11",
                        "table1,tag2=v2b,tag3=v3a f=1 11",
                        "table1,tag2=v2a,tag3=v3b f=1 11",
                        "table1,tag2=v2b,tag3=v3b f=1 11",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 2,
                },
            ].into_iter()
            .collect(),
        ),
        (
            "CustomPartitioningBucket",
            [
                Step::Custom(Box::new(move |state: &mut StepTestState<'_>| {
                    async move {
                        let namespace_name = state.cluster().namespace();

                        let mut namespace_client = influxdb_iox_client::namespace::Client::new(
                            state.cluster().router().router_grpc_connection(),
                        );
                        namespace_client
                            .create_namespace(namespace_name, None, None, Some(PartitionTemplate{
                                parts: vec![
                                    // Data will be partitioned into 10 buckets using formula hash("tag1_value") % 10
                                    TemplatePart{
                                        part: Some(Part::Bucket(Bucket { tag_name: "tag1".to_string(), num_buckets: 10 })),
                                    },
                                    // Data will be partitioned into 30 buckets using formula hash("tag3_value") % 30
                                    TemplatePart{
                                        part: Some(Part::Bucket(Bucket { tag_name: "tag3".to_string(), num_buckets: 30 })),
                                    },
                                    // All new partitioning templates require a time format.
                                    TemplatePart{
                                        part: Some(Part::TimeFormat("%Y".into())),
                                    },
                                ],
                            }))
                            .await
                            .unwrap();

                        // table1: create implicitly by writing to it
                    }
                    .boxed()
                })),
                // Let's say the bucket id for each tag values are the following:
                // hash(v1a) % 10 = 1
                // hash(v1b) % 10 = 2
                // hash(v3a) % 30 = 25
                // hash(v3b) % 30 = 26
                //
                // Four partitions are created using the template 'bucket_id_of_tag1|bucket_id_of_tag3'
                //
                // v1a|v3a --> 1|25
                // v1a|v3b --> 1|26
                // v1b|v3a --> 2|25
                // v1b|v3b --> 2|26
                // Each partition include 1 file
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "table1,tag1=v1a,tag2=v2a,tag3=v3a f=1 11",
                        "table1,tag1=v1b,tag2=v2a,tag3=v3a f=1 11",
                        "table1,tag1=v1a,tag2=v2b,tag3=v3a f=1 11",
                        "table1,tag1=v1b,tag2=v2b,tag3=v3a f=1 11",
                        "table1,tag1=v1a,tag2=v2a,tag3=v3b f=1 11",
                        "table1,tag1=v1b,tag2=v2a,tag3=v3b f=1 11",
                        "table1,tag1=v1a,tag2=v2b,tag3=v3b f=1 11",
                        "table1,tag1=v1b,tag2=v2b,tag3=v3b f=1 11",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 4
                },
                // Three partitions are created using the template 'bucket_id_of_tag1|bucket_id_of_tag3'
                // Becasue no tag1 or tag3 in the data, its value is "!"
                //
                //  ! |v3a --> !|25
                // v1a| !  --> 1|!
                //  ! | !  --> !|!
                // Each partition include 1 file
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "table1,tag2=v2a,tag3=v3a f=1 11",
                        "table1,tag2=v2b,tag3=v3a f=1 11",
                        "table1,tag1=v1a,tag2=v2a f=1 11",
                        "table1,tag1=v1a,tag2=v2b f=1 11",
                        "table1,tag2=v2a f=1 11",
                        "table1,tag2=v2b f=1 11",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 3,
                },
            ].into_iter()
            .collect(),

        ),
        (
            "CustomPartitioningWithTagValuesAsNumbers",
            [
                Step::Custom(Box::new(move |state: &mut StepTestState<'_>| {
                    async move {
                        let namespace_name = state.cluster().namespace();

                        let mut namespace_client = influxdb_iox_client::namespace::Client::new(
                            state.cluster().router().router_grpc_connection(),
                        );
                        namespace_client
                            .create_namespace(namespace_name, None, None, Some(PartitionTemplate{
                                parts: vec![
                                    TemplatePart{
                                        part: Some(Part::TagValue("tag1".into())),
                                    },
                                    // All new partitioning templates require a time format.
                                    TemplatePart{
                                        part: Some(Part::TimeFormat("%Y".into())),
                                    },
                                ],
                            }))
                            .await
                            .unwrap();

                        // table1: create implicitly by writing to it
                    }
                    .boxed()
                })),
                Step::RecordNumParquetFiles,
                // Has data in 2 differnet partitions
                Step::WriteLineProtocol(
                    [
                        "table1,tag1=1 f=10 0",          // will be in partition 1
                        "table1,tag1=2 f=20 0",          // will be in partition 2
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 2
                },
            ].into_iter()
            .collect(),

        ),
        (
            "CustomPartitioningWithTagValuesAsNumbersWithYear",
            [
                Step::Custom(Box::new(move |state: &mut StepTestState<'_>| {
                    async move {
                        let namespace_name = state.cluster().namespace();

                        let mut namespace_client = influxdb_iox_client::namespace::Client::new(
                            state.cluster().router().router_grpc_connection(),
                        );
                        namespace_client
                            .create_namespace(namespace_name, None, None, Some(PartitionTemplate{
                                parts: vec![
                                    TemplatePart {
                                        part: Some(Part::TimeFormat(String::from("%Y"))),
                                    },
                                    TemplatePart{
                                        part: Some(Part::TagValue("tag1".into())),
                                    }
                                ],
                            }))
                            .await
                            .unwrap();

                        // table1: create implicitly by writing to it
                    }
                    .boxed()
                })),
                Step::RecordNumParquetFiles,
                // Has data in 3 differnet partitions
                Step::WriteLineProtocol(
                    [
                        "table1,tag1=1 f=10 0",                  // will be in partition `1970| 1`
                        "table1,tag1=2 f=20 0",                  // will be in partition `1970| 2`
                        "table1,tag1=1 f=30 32000000000000000",  // will be in partition `1971| 1`
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 3
                },
            ].into_iter()
            .collect(),

        ),
        (
            "CustomPartitioningWithTagValuesAsNumbersWithYearAndBucketing",
            [
                Step::Custom(Box::new(move |state: &mut StepTestState<'_>| {
                    async move {
                        let namespace_name = state.cluster().namespace();

                        let mut namespace_client = influxdb_iox_client::namespace::Client::new(
                            state.cluster().router().router_grpc_connection(),
                        );
                        namespace_client
                            .create_namespace(namespace_name, None, None, Some(PartitionTemplate{
                                parts: vec![
                                    TemplatePart {
                                        part: Some(Part::TimeFormat(String::from("%Y"))),
                                    },
                                    // Data will be partitioned into 10 buckets using formula hash("tag1_value") % 10
                                    TemplatePart{
                                        part: Some(Part::Bucket(Bucket { tag_name: "tag1".to_string(), num_buckets: 10 })),
                                    },
                                ],
                            }))
                            .await
                            .unwrap();

                        // table1: create implicitly by writing to it
                    }
                    .boxed()
                })),
                Step::RecordNumParquetFiles,
                // Has data in 3 differnet partitions
                Step::WriteLineProtocol(
                    [
                        "table1,tag1=1 f=10 0",                  // will be in partition `1970|x`  x = hash("1") % 10
                        "table1,tag1=123456789 f=20 0",          // will be in partition `1970|y`  y = hash("123456789") % 10
                        "table1,tag1=1 f=30 32000000000000000",  // will be in partition `1971|x`  x = hash("1") % 10
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted {
                    expected_increase: 3
                },
            ].into_iter()
            .collect(),

        ),
        (
            "ManyPartitions",
            (0..1)
                .flat_map(|_i| {

                    let num_partitions = env::var("TEST_SETUP_MANY_PARTITIONS_NUM_PARTITIONS").map(|n| n.parse().unwrap()).unwrap_or(50);

                    let lines = (0..num_partitions)
                        .map(|i| format!("table1,tag=A f=1 {}", i * DAY_NANO + 100))
                        .collect::<Vec<_>>()
                        .join("\n");

                    // Each line of line protocol has time (i * DAY_NANO + 100)
                    // to ensure it is in different day partition.
                    // Each partition includes 1 file
                    let write = Step::WriteLineProtocol(lines);
                    [
                        Step::RecordNumParquetFiles,
                        write,
                        Step::Persist,
                        Step::WaitForPersisted {
                            // wait for all the files to persist
                            expected_increase: num_partitions as usize,
                        },
                    ]
                    .into_iter()
                })
                .collect::<Vec<_>>(),
        ),
    ])
});

/// Holds parameters for retention period. Tests based on this need to
/// be run within the hour of being created.
///
/// ```text                  (cut off)                    (now)
/// time: -----------------------|--------------------------|>
///
/// partially_inside:   |-----------------|
///
/// fully_inside:                 +1h |-----------|
///
/// fully_outside: |---------|
/// ```
///
/// Note this setup is only good for 1 hour after it was created.
struct RetentionSetup {
    /// the retention period, relative to now() that the three data
    /// chunks fall inside/outside
    retention_period_ns: i64,

    /// lineprotocol data partially inside retention
    lp_partially_inside_1: String,

    /// lineprotocol data partially inside retention
    lp_partially_inside_2: String,

    /// lineprotocol data fully inside (included in query)
    lp_fully_inside: String,

    /// lineprotocol data fully outside (excluded from query)
    lp_fully_outside: String,
}

impl RetentionSetup {
    fn new() -> Self {
        let retention_period_1_hour_ns = 3600 * 1_000_000_000;

        // Data is relative to this particular time stamp
        //
        // Use a cutoff date that is NOT at the start of the partition so that `lp_partially_inside` only spans a single
        // partition, not two. This is important because otherwise this will result in two chunks / files, not one.
        // However a partial inside/outside chunk is important for the query tests so that we can proof that it is not
        // sufficient to prune the chunks solely on statistics but that there needs to be an actual row-wise filter.
        let cutoff = Time::from_rfc3339("2022-01-01T10:00:00+00:00")
            .unwrap()
            .timestamp_nanos();
        // Timestamp 1 hour later than the cutoff, so the data will be retained for 1 hour
        let inside_retention = cutoff + retention_period_1_hour_ns;
        let outside_retention = cutoff - 10; // before retention

        let lp_partially_inside_1 = format!(
            "cpu,host=a load=1 {inside_retention}\n\
             cpu,host=aa load=11 {outside_retention}"
        );

        let lp_fully_inside = format!(
            "cpu,host=b load=2 {inside_retention}\n\
             cpu,host=bb load=21 {inside_retention}"
        );

        let lp_fully_outside = format!(
            "cpu,host=c load=3 {outside_retention}\n\
             cpu,host=cc load=31 {outside_retention}"
        );

        let lp_partially_inside_2 = format!(
            "cpu,host=d load=4 {inside_retention}\n\
             cpu,host=dd load=41 {outside_retention}"
        );

        // Set retention period to be at the cutoff date. Note that
        // since real world time advances, after 1 hour of real world
        // time the data that is inside the retention interval will
        // move outside (and thus not appear in queries).
        //
        // Thus this setup is only valid for 1 hour.
        let retention_period_ns = SystemProvider::new().now().timestamp_nanos() - cutoff;

        Self {
            // translate the retention period to be relative to now
            retention_period_ns,
            lp_partially_inside_1,
            lp_partially_inside_2,
            lp_fully_inside,
            lp_fully_outside,
        }
    }
}
