//! Writing generated points

use crate::measurement::LineToGenerate;
use bytes::Bytes;
use datafusion_util::{unbounded_memory_pool, MemoryStream};
use futures::stream;
use influxdb2_client::models::WriteDataPoint;
use mutable_batch_lp::lines_to_batches;
use parquet_file::{metadata::IoxMetadata, serialize};
use schema::Projection;
use snafu::{ensure, ResultExt, Snafu};
#[cfg(test)]
use std::{collections::BTreeMap, sync::Arc};
use std::{
    fs::{self, File, OpenOptions},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::Mutex,
};

/// Errors that may happen while writing points.
#[derive(Snafu, Debug)]
pub enum Error {
    /// Error that may happen when writing line protocol to a file
    #[snafu(display("Couldn't open line protocol file {}: {}", filename.display(), source))]
    CantOpenLineProtocolFile {
        /// The location of the file we tried to open
        filename: PathBuf,
        /// Underlying IO error that caused this problem
        source: std::io::Error,
    },

    /// Error that may happen when writing Parquet to a file
    #[snafu(display("Couldn't open Parquet file {}: {}", filename.display(), source))]
    CantOpenParquetFile {
        /// The location of the file we tried to open
        filename: PathBuf,
        /// Underlying IO error that caused this problem
        source: std::io::Error,
    },

    /// Error that may happen when writing line protocol to a no-op sink
    #[snafu(display("Could not generate line protocol: {}", source))]
    CantWriteToNoOp {
        /// Underlying IO error that caused this problem
        source: std::io::Error,
    },

    /// Error that may happen when writing line protocol to a file
    #[snafu(display("Could not write line protocol to file: {}", source))]
    CantWriteToLineProtocolFile {
        /// Underlying IO error that caused this problem
        source: std::io::Error,
    },

    /// Error that may happen when writing line protocol to a Vec of bytes
    #[snafu(display("Could not write to vec: {}", source))]
    WriteToVec {
        /// Underlying IO error that caused this problem
        source: std::io::Error,
    },

    /// Error that may happen when writing Parquet to a file
    #[snafu(display("Could not write Parquet: {}", source))]
    WriteToParquetFile {
        /// Underlying IO error that caused this problem
        source: std::io::Error,
    },

    /// Error that may happen when converting line protocol to a mutable batch
    #[snafu(display("Could not convert to a mutable batch: {}", source))]
    ConvertToMutableBatch {
        /// Underlying mutable_batch_lp error that caused this problem
        source: mutable_batch_lp::Error,
    },

    /// Error that may happen when converting a mutable batch to an Arrow RecordBatch
    #[snafu(display("Could not convert to a record batch: {}", source))]
    ConvertToArrow {
        /// Underlying mutable_batch error that caused this problem
        source: mutable_batch::Error,
    },

    /// Error that may happen when creating a directory to store files to write
    /// to
    #[snafu(display("Could not create directory: {}", source))]
    CantCreateDirectory {
        /// Underlying IO error that caused this problem
        source: std::io::Error,
    },

    /// Error that may happen when checking a path's metadata to see if it's a
    /// directory
    #[snafu(display("Could not get metadata: {}", source))]
    CantGetMetadata {
        /// Underlying IO error that caused this problem
        source: std::io::Error,
    },

    /// Error that may happen if the path given to the file-based writer isn't a
    /// directory
    #[snafu(display("Expected to get a directory"))]
    MustBeDirectory,

    /// Error that may happen while writing points to the API
    #[snafu(display("Could not write points to API: {}", source))]
    CantWriteToApi {
        /// Underlying Influx client request error that caused this problem
        source: influxdb2_client::RequestError,
    },

    /// Error that may happen while trying to create a bucket via the API
    #[snafu(display("Could not create bucket: {}", source))]
    CantCreateBucket {
        /// Underlying Influx client request error that caused this problem
        source: influxdb2_client::RequestError,
    },

    /// Error that may happen if attempting to create a bucket without
    /// specifying the org ID
    #[snafu(display("Could not create a bucket without an `org_id`"))]
    OrgIdRequiredToCreateBucket,

    /// Error that may happen when serializing to Parquet
    #[snafu(display("Could not serialize to Parquet"))]
    ParquetSerialization {
        /// Underlying `parquet_file` error that caused this problem
        source: parquet_file::serialize::CodecError,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Responsible for holding shared configuration needed to construct per-agent
/// points writers
#[derive(Debug)]
pub struct PointsWriterBuilder {
    config: PointsWriterConfig,
}

#[derive(Debug)]
enum PointsWriterConfig {
    Api(influxdb2_client::Client),
    Directory(PathBuf),
    ParquetFile(PathBuf),
    NoOp {
        perform_write: bool,
    },
    #[cfg(test)]
    Vector(BTreeMap<String, Arc<Mutex<Vec<u8>>>>),
    Stdout,
}

impl PointsWriterBuilder {
    /// Write points to the API at the specified host and put them in the
    /// specified org and bucket.
    pub async fn new_api(
        host: impl Into<String> + Send,
        token: impl Into<String> + Send,
        jaeger_debug: Option<&str>,
    ) -> Result<Self> {
        let host = host.into();

        // Be somewhat lenient on what we accept as far as host; the client expects the
        // protocol to be included. We could pull in the url crate and do more
        // verification here.
        let host = if host.starts_with("http") {
            host
        } else {
            format!("http://{host}")
        };

        let mut client = influxdb2_client::Client::new(host, token.into());
        if let Some(header) = jaeger_debug {
            client = client.with_jaeger_debug(header.to_string());
        }

        Ok(Self {
            config: PointsWriterConfig::Api(client),
        })
    }

    /// Write points to a file in the directory specified.
    pub fn new_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        fs::create_dir_all(&path).context(CantCreateDirectorySnafu)?;
        let metadata = fs::metadata(&path).context(CantGetMetadataSnafu)?;
        ensure!(metadata.is_dir(), MustBeDirectorySnafu);

        Ok(Self {
            config: PointsWriterConfig::Directory(PathBuf::from(path.as_ref())),
        })
    }

    /// Write points to a Parquet file in the directory specified.
    pub fn new_parquet<P: AsRef<Path>>(path: P) -> Result<Self> {
        fs::create_dir_all(&path).context(CantCreateDirectorySnafu)?;
        let metadata = fs::metadata(&path).context(CantGetMetadataSnafu)?;
        ensure!(metadata.is_dir(), MustBeDirectorySnafu);

        Ok(Self {
            config: PointsWriterConfig::ParquetFile(PathBuf::from(path.as_ref())),
        })
    }

    /// Write points to stdout
    pub fn new_std_out() -> Self {
        Self {
            config: PointsWriterConfig::Stdout,
        }
    }

    /// Generate points but do not write them anywhere
    pub fn new_no_op(perform_write: bool) -> Self {
        Self {
            config: PointsWriterConfig::NoOp { perform_write },
        }
    }

    /// Create a writer out of this writer's configuration for a particular
    /// agent that runs in a separate thread/task.
    pub fn build_for_agent(
        &mut self,
        name: impl Into<String>,
        org: impl Into<String>,
        bucket: impl Into<String>,
    ) -> Result<PointsWriter> {
        let inner_writer = match &mut self.config {
            PointsWriterConfig::Api(client) => InnerPointsWriter::Api {
                client: client.clone(),
                org: org.into(),
                bucket: bucket.into(),
            },
            PointsWriterConfig::Directory(dir_path) => {
                let mut filename = dir_path.clone();
                filename.push(name.into());
                filename.set_extension("txt");

                let file = OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open(&filename)
                    .context(CantOpenLineProtocolFileSnafu { filename })?;

                let file = Mutex::new(BufWriter::new(file));

                InnerPointsWriter::File { file }
            }

            PointsWriterConfig::ParquetFile(dir_path) => InnerPointsWriter::ParquetFile {
                dir_path: dir_path.clone(),
                agent_name: name.into(),
            },

            PointsWriterConfig::NoOp { perform_write } => InnerPointsWriter::NoOp {
                perform_write: *perform_write,
            },
            #[cfg(test)]
            PointsWriterConfig::Vector(ref mut agents_by_name) => {
                let v = agents_by_name
                    .entry(name.into())
                    .or_insert_with(|| Arc::new(Mutex::new(Vec::new())));
                InnerPointsWriter::Vec(Arc::clone(v))
            }
            PointsWriterConfig::Stdout => InnerPointsWriter::Stdout,
        };

        Ok(PointsWriter { inner_writer })
    }
}

/// Responsible for writing points to the location it's been configured for.
#[derive(Debug)]
pub struct PointsWriter {
    inner_writer: InnerPointsWriter,
}

impl PointsWriter {
    /// Write these points
    pub async fn write_points(
        &self,
        points: impl Iterator<Item = LineToGenerate> + Send + Sync + 'static,
    ) -> Result<()> {
        self.inner_writer.write_points(points).await
    }
}

#[derive(Debug)]
enum InnerPointsWriter {
    Api {
        client: influxdb2_client::Client,
        org: String,
        bucket: String,
    },
    File {
        file: Mutex<BufWriter<File>>,
    },
    ParquetFile {
        dir_path: PathBuf,
        agent_name: String,
    },
    NoOp {
        perform_write: bool,
    },
    #[cfg(test)]
    Vec(Arc<Mutex<Vec<u8>>>),
    Stdout,
}

impl InnerPointsWriter {
    async fn write_points(
        &self,
        points: impl Iterator<Item = LineToGenerate> + Send + Sync + 'static,
    ) -> Result<()> {
        match self {
            Self::Api {
                client,
                org,
                bucket,
            } => {
                client
                    .write(org, bucket, stream::iter(points))
                    .await
                    .context(CantWriteToApiSnafu)?;
            }
            Self::File { file } => {
                for point in points {
                    let mut file = file.lock().expect("Should be able to get lock");
                    point
                        .write_data_point_to(&mut *file)
                        .context(CantWriteToLineProtocolFileSnafu)?;
                }
            }

            Self::ParquetFile {
                dir_path,
                agent_name,
            } => {
                let mut raw_line_protocol = Vec::new();
                for point in points {
                    point
                        .write_data_point_to(&mut raw_line_protocol)
                        .context(WriteToVecSnafu)?;
                }
                let line_protocol = String::from_utf8(raw_line_protocol)
                    .expect("Generator should be creating valid UTF-8");

                let batches_by_measurement =
                    lines_to_batches(&line_protocol, 0).context(ConvertToMutableBatchSnafu)?;

                for (measurement, batch) in batches_by_measurement {
                    let record_batch = batch
                        .to_arrow(Projection::All)
                        .context(ConvertToArrowSnafu)?;
                    let stream = Box::pin(MemoryStream::new(vec![record_batch]));
                    let meta = IoxMetadata::external(crate::now_ns(), &*measurement);
                    let pool = unbounded_memory_pool();
                    let (data, _parquet_file_meta) =
                        serialize::to_parquet_bytes(stream, &meta, pool)
                            .await
                            .context(ParquetSerializationSnafu)?;
                    let data = Bytes::from(data);

                    let mut filename = dir_path.clone();
                    filename.push(format!("{agent_name}_{measurement}"));
                    filename.set_extension("parquet");

                    let file = OpenOptions::new()
                        .create(true)
                        .truncate(true)
                        .write(true)
                        .open(&filename)
                        .context(CantOpenParquetFileSnafu { filename })?;

                    let mut file = BufWriter::new(file);

                    file.write_all(&data).context(WriteToParquetFileSnafu)?;
                }
            }

            Self::NoOp { perform_write } => {
                if *perform_write {
                    let mut sink = std::io::sink();

                    for point in points {
                        point
                            .write_data_point_to(&mut sink)
                            .context(CantWriteToNoOpSnafu)?;
                    }
                }
            }
            #[cfg(test)]
            Self::Vec(vec) => {
                let vec_ref = Arc::clone(vec);
                let mut vec = vec_ref.lock().expect("Should be able to get lock");
                for point in points {
                    point
                        .write_data_point_to(&mut *vec)
                        .expect("Should be able to write to vec");
                }
            }
            Self::Stdout => {
                for point in points {
                    point
                        .write_data_point_to(std::io::stdout())
                        .expect("should be able to write to stdout");
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{generate, now_ns, specification::*};
    use std::str::FromStr;

    type Error = Box<dyn std::error::Error>;
    type Result<T = (), E = Error> = std::result::Result<T, E>;

    impl PointsWriterBuilder {
        fn new_vec() -> Self {
            Self {
                config: PointsWriterConfig::Vector(BTreeMap::new()),
            }
        }

        fn written_data(self, agent_name: &str) -> String {
            match self.config {
                PointsWriterConfig::Vector(agents_by_name) => {
                    let bytes_ref =
                        Arc::clone(agents_by_name.get(agent_name).expect(
                            "Should have written some data, did not find any for this agent",
                        ));
                    let bytes = bytes_ref
                        .lock()
                        .expect("Should have been able to get a lock");
                    String::from_utf8(bytes.to_vec()).expect("we should be generating valid UTF-8")
                }
                _ => unreachable!("this method is only valid when writing to a vector for testing"),
            }
        }
    }

    #[tokio::test]
    async fn test_generate() -> Result<()> {
        let toml = r#"
name = "demo_schema"

[[agents]]
name = "foo"

[[agents.measurements]]
name = "cpu"

[[agents.measurements.fields]]
name = "val"
i64_range = [3,3]

[[database_writers]]
agents = [{name = "foo", sampling_interval = "1s"}]
"#;

        let data_spec = DataSpec::from_str(toml).unwrap();
        let mut points_writer_builder = PointsWriterBuilder::new_vec();

        let now = now_ns();

        generate(
            &data_spec,
            vec!["foo_bar".to_string()],
            &mut points_writer_builder,
            Some(now),
            Some(now),
            now,
            false,
            1,
            false,
        )
        .await?;

        let line_protocol = points_writer_builder.written_data("foo");

        let expected_line_protocol = format!(
            r#"cpu val=3i {now}
"#
        );
        assert_eq!(line_protocol, expected_line_protocol);

        Ok(())
    }

    #[tokio::test]
    async fn test_generate_batches() -> Result<()> {
        let toml = r#"
name = "demo_schema"

[[agents]]
name = "foo"

[[agents.measurements]]
name = "cpu"

[[agents.measurements.fields]]
name = "val"
i64_range = [2, 2]

[[database_writers]]
agents = [{name = "foo", sampling_interval = "1s"}]
"#;

        let data_spec = DataSpec::from_str(toml).unwrap();
        let mut points_writer_builder = PointsWriterBuilder::new_vec();

        let now = now_ns();

        generate(
            &data_spec,
            vec!["foo_bar".to_string()],
            &mut points_writer_builder,
            Some(now - 1_000_000_000),
            Some(now),
            now,
            false,
            2,
            false,
        )
        .await?;

        let line_protocol = points_writer_builder.written_data("foo");

        let expected_line_protocol = format!(
            r#"cpu val=2i {}
cpu val=2i {}
"#,
            now - 1_000_000_000,
            now
        );
        assert_eq!(line_protocol, expected_line_protocol);

        Ok(())
    }
}
