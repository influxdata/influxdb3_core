use data_types::{
    NamespaceId, PartitionHashId, PartitionHashIdError, PartitionId, TableId, TransitionPartitionId,
};
use futures_util::TryStreamExt;
use influxdb_iox_client::{
    catalog::{
        self,
        generated_types::{ParquetFile, Partition, PartitionIdentifier, partition_identifier},
    },
    connection::Connection,
    store::{
        self,
        generated_types::{
            ListParquetFilesByPathFilterResponse, ObjectMetadata, ParquetFilePathFilter,
        },
    },
    table,
};
use object_store::{
    DynObjectStore,
    buffered::BufWriter,
    local::LocalFileSystem,
    path::{Path, PathPart},
};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use thiserror::Error;
use tokio::{
    fs,
    io::{self, AsyncWriteExt},
};
use tokio_util::compat::FuturesAsyncReadCompatExt;

#[derive(Debug, Error)]
pub enum ExportError {
    #[error("JSON Serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("IOx request failed: {0}")]
    Client(#[from] influxdb_iox_client::error::Error),

    #[error("IOx request failed: {0}")]
    StoreServer(#[from] influxdb_iox_client::store::Status),

    #[error("IOx request failed: {0}")]
    ObjectStore(#[from] object_store::Error),

    #[error("IOx request failed due to invalid path: {0}")]
    InvalidPath(#[from] object_store::path::Error),

    #[error("IOx request failed due to an invalid argument: {0}")]
    InvalidArg(String),

    #[error("IOx request failed due to an internal error: {0}")]
    Internal(String),

    #[error("Writing file: {0}")]
    File(#[from] std::io::Error),
}

type Result<T, E = ExportError> = std::result::Result<T, E>;

/// Location where data will be exported to. This can be thought of as
/// the directory where files will be written, but may be a object store
/// with a path prefix.
#[derive(Debug, Clone)]
pub struct Output {
    object_store: Arc<DynObjectStore>,
    path: Path,
}

impl Output {
    /// Create a new output with the specified object store and path.
    pub fn new(object_store: Arc<DynObjectStore>, path: Path) -> Self {
        Self { object_store, path }
    }

    pub async fn from_filesystem_path(
        path: impl AsRef<std::path::Path> + Send + Sync,
    ) -> Result<Self> {
        fs::create_dir_all(&path).await?;
        let object_store = LocalFileSystem::new_with_prefix(path)?;
        Ok(Self {
            object_store: Arc::new(object_store),
            path: Path::default(),
        })
    }

    /// Returns a new output with a child path.
    pub fn child<'a>(&self, child: impl Into<PathPart<'a>>) -> Self {
        Self {
            object_store: Arc::clone(&self.object_store),
            path: self.path.child(child),
        }
    }

    /// Open a buffered writer to the specified filename.
    pub fn writer(&self, filename: impl AsRef<str>) -> BufWriter {
        BufWriter::new(
            Arc::clone(&self.object_store),
            self.path.child(filename.as_ref()),
        )
    }

    /// writes the contents of a string to a file, overwriting the previous contents, if any
    pub async fn write_string(
        &self,
        filename: impl AsRef<str>,
        contents: impl AsRef<[u8]>,
    ) -> Result<()> {
        let mut w = self.writer(filename);
        w.write_all(contents.as_ref()).await?;
        w.shutdown().await?;
        Ok(())
    }

    /// Get the size of a file in the output directory.
    pub async fn size(&self, filename: impl AsRef<str>) -> Result<usize> {
        let metadata = self
            .object_store
            .head(&self.path.child(filename.as_ref()))
            .await?;
        Ok(metadata.size)
    }
}

enum DesiredFiles {
    /// Export files which are current
    AllCurrent,
    /// Export a subset of current files
    SubsetCurrent(Vec<ParquetFile>),
    /// Export files, including both current and soft deleted
    AllExisting,
    /// Export a subset of files, including both current and soft deleted
    SubsetExisting(HashSet<String /* object_store uuid */>),
}

/// Exports data from a remote IOx instance to local files.
///
/// Data is read using the clients in [`influxdb_iox_client`] (rather
/// than the catalog) so that this can be used to debug remote systems.
#[derive(Debug)]
pub struct RemoteExporter {
    catalog_client: catalog::Client,
    store_client: store::Client,
    table_client: table::Client,
}

impl RemoteExporter {
    pub fn new(connection: Connection) -> Self {
        Self {
            catalog_client: catalog::Client::new(connection.clone()),
            store_client: store::Client::new(connection.clone()),
            table_client: table::Client::new(connection),
        }
    }

    /// Exports all the tables in `namespace`. Each table is exported to
    /// a new subdirectory of the output location with the name of the
    /// table.
    pub async fn export_namespace(
        &mut self,
        output: &Output,
        namespace_name: String,
    ) -> Result<()> {
        let tables = self.table_client.get_tables(&namespace_name).await?;
        for table in tables {
            let output = output.child(table.name.clone());
            println!("exporting table {}...", &table.name);
            self.export_table(
                &output,
                namespace_name.clone(),
                table.name,
                None,
                None,
                false,
            )
            .await?;
        }

        Ok(())
    }

    /// Exports all data and metadata for `table_name` in
    /// `namespace` to local files.
    ///
    /// If a `partition` is specified, then it scopes results
    /// to the partition within the table.
    ///
    /// If `output_directory` is specified, all files are written
    /// there otherwise files are exported to a directory named
    /// `table_name`.
    ///
    /// If the `file_uuids` are specified, then only a subset of parquet files
    /// will be downloaded for the table. Otherwise, it downloads all
    /// files within the table.
    pub async fn export_table(
        &mut self,
        output: &Output,
        namespace_name: String,
        table_name: String,
        partition_id: Option<i64>,
        file_uuids: Option<Vec<String>>,
        include_deleted: bool,
    ) -> Result<()> {
        // Export the metadata for the table.
        let (namespace_id, table_id) = self
            .export_table_metadata(output, &table_name, &namespace_name)
            .await?;

        // Determine which files are sought.
        let desired_files = match (file_uuids, include_deleted) {
            (None, false) => DesiredFiles::AllCurrent,
            (None, true) => DesiredFiles::AllExisting,
            (Some(file_uuids), false) => {
                let len = file_uuids.len();
                let (in_catalog, _) = self
                    .determine_source_of_file_uuids(
                        &namespace_name,
                        &table_name,
                        &partition_id,
                        file_uuids,
                    )
                    .await?;

                assert_eq!(
                    len,
                    in_catalog.len(),
                    "The provided uuids do not all exist within the catalog for table {}",
                    table_name
                );

                DesiredFiles::SubsetCurrent(in_catalog)
            }
            (Some(file_uuids), true) => {
                let (in_catalog, not_in_catalog) = self
                    .determine_source_of_file_uuids(
                        &namespace_name,
                        &table_name,
                        &partition_id,
                        file_uuids.clone(),
                    )
                    .await?;

                if not_in_catalog.is_empty() {
                    // do the less expensive retrieval
                    DesiredFiles::SubsetCurrent(in_catalog)
                } else {
                    DesiredFiles::SubsetExisting(HashSet::from_iter(
                        file_uuids.into_iter().map(|uuid| format!("{uuid}.parquet")),
                    ))
                }
            }
        };

        self.export_files(
            output,
            desired_files,
            namespace_name,
            namespace_id,
            table_name,
            table_id,
            partition_id,
        )
        .await?;

        println!("Done.");
        Ok(())
    }

    /// Exports table and partition information for the specified
    /// table. Overwrites existing files, if any, to ensure it has the
    /// latest catalog information.
    ///
    /// 1. `<output_directory>/table.<partition_id>.json`: pbjson encoded data about the table
    ///    (minimal now)
    ///
    /// 2. `<output_directory>/partition.<partition_id>.json`: pbjson encoded data for each
    ///    partition
    async fn export_table_metadata(
        &mut self,
        output: &Output,
        table_name: &str,
        namespace_name: &str,
    ) -> Result<(NamespaceId, data_types::TableId)> {
        // write table metadata
        let table = self
            .table_client
            .get_table(namespace_name, table_name)
            .await?;
        let table_id = table.id;
        let table_json = serde_json::to_string_pretty(&table)?;
        output
            .write_string(format!("table.{table_id}.json"), &table_json)
            .await?;

        // write partition metadata for the table
        let partitions = self
            .catalog_client
            .get_partitions_by_table_id(table_id)
            .await?;

        for partition in partitions {
            let partition_id = to_partition_id(partition.identifier.as_ref());
            let partition_json = serde_json::to_string_pretty(&partition)?;
            output
                .write_string(format!("partition.{partition_id}.json"), &partition_json)
                .await?;
        }

        let columns = self
            .catalog_client
            .get_columns_by_table_id(table_id)
            .await?;
        let table_columns_json = serde_json::to_string_pretty(&columns)?;
        output
            .write_string(format!("columns.{table_id}.json"), &table_columns_json)
            .await?;

        Ok((
            NamespaceId::new(table.namespace_id),
            data_types::TableId::new(table.id),
        ))
    }

    /// Get all_files scoped to table or partition_id (take the narrowest scope provided).
    async fn list_files_in_catalog(
        &mut self,
        namespace_name: &String,
        table_name: &String,
        partition_id: &Option<i64>,
    ) -> Result<Vec<ParquetFile>> {
        if let Some(partition_id) = partition_id {
            Ok(self
                .catalog_client
                .get_parquet_files_by_partition_id(*partition_id)
                .await?)
        } else {
            Ok(self
                .catalog_client
                .get_parquet_files_by_namespace_table(namespace_name, table_name)
                .await?)
        }
    }

    /// Determine source of specifically requested uuids, delineated by
    /// whether or not exists in the catalog for a given table
    /// (or partition if narrowed to partition scope).
    ///
    /// If the files do NOT exist in the iox_catalog (e.g. mya be soft deleted),
    /// they will be presumed as existing within the store.
    async fn determine_source_of_file_uuids(
        &mut self,
        namespace_name: &String,
        table_name: &String,
        partition_id: &Option<i64>,
        desired_file_uuids: Vec<String>,
    ) -> Result<(
        Vec<ParquetFile>, // in catalog
        HashSet<String>,  // not in catalog
    )> {
        let all_catalog_files: HashMap<String, ParquetFile> = HashMap::from_iter(
            self.list_files_in_catalog(namespace_name, table_name, partition_id)
                .await?
                .into_iter()
                .map(|pf| (pf.object_store_id.clone(), pf)),
        );

        // split btwn in_catalog vs not_in_catalog
        let mut desired_uuids_in_catalog = Vec::with_capacity(desired_file_uuids.len());
        let mut desired_uuids_not_in_catalog = HashSet::new();
        for uuid in &desired_file_uuids {
            if let Some(pf) = all_catalog_files.get(uuid) {
                desired_uuids_in_catalog.push(pf.clone());
            } else {
                desired_uuids_not_in_catalog.insert(format!("{}.parquet", uuid));
            }
        }

        Ok((desired_uuids_in_catalog, desired_uuids_not_in_catalog))
    }

    /// Export based upon the [`DesiredFiles`].
    #[expect(clippy::too_many_arguments)]
    async fn export_files(
        &mut self,
        output: &Output,
        desired_files: DesiredFiles,
        namespace_name: String,
        namespace_id: NamespaceId,
        table_name: String,
        table_id: TableId,
        partition_id: Option<i64>,
    ) -> Result<()> {
        match desired_files {
            DesiredFiles::AllCurrent => {
                let files = self
                    .list_files_in_catalog(&namespace_name, &table_name, &partition_id)
                    .await?;
                let total_num_files = files.len();
                println!("found {total_num_files} current Parquet files, exporting...");

                for (index, parquet_file) in files.into_iter().enumerate() {
                    self.export_parquet_file(output, index, total_num_files, &parquet_file)
                        .await?;
                }
            }
            DesiredFiles::SubsetCurrent(files) => {
                let total_num_files = files.len();
                println!("found {total_num_files} current Parquet files, exporting...");

                for (index, parquet_file) in files.into_iter().enumerate() {
                    self.export_parquet_file(output, index, total_num_files, &parquet_file)
                        .await?;
                }
            }
            DesiredFiles::AllExisting => {
                println!("exporting all existing Parquet files (total count unknown)...");

                self.export_existing_objects_at_path(
                    output,
                    None,
                    namespace_id,
                    table_id,
                    partition_id,
                )
                .await?;
            }
            DesiredFiles::SubsetExisting(files) => {
                let total_num_files = files.len();
                println!("found {total_num_files} Parquet files, exporting...");

                self.export_existing_objects_at_path(
                    output,
                    Some(files),
                    namespace_id,
                    table_id,
                    partition_id,
                )
                .await?;
            }
        };
        Ok(())
    }

    /// Exports a remote [`ParquetFile`] to:
    ///
    /// 1. `<output_directory>/<uuid>.parquet`: The parquet bytes
    ///
    /// 2. `<output_directory>/<uuid>.parquet.json`: pbjson encoded `ParquetFile` metadata
    ///
    /// Performs a catalog request (within the object store grpc service), per file.
    async fn export_parquet_file(
        &mut self,
        output: &Output,
        index: usize,
        num_parquet_files: usize,
        parquet_file: &ParquetFile,
    ) -> Result<()> {
        let uuid = &parquet_file.object_store_id;
        let file_size_bytes = parquet_file.file_size_bytes as u64;

        // copy out the metadata as pbjson encoded data always (to
        // ensure we have the most up to date version)
        {
            let json = serde_json::to_string_pretty(&parquet_file)?;
            output
                .write_string(format!("{uuid}.parquet.json"), &json)
                .await?;
        }

        let filename = format!("{uuid}.parquet");
        if output
            .size(&filename)
            .await
            .is_ok_and(|size| (size as u64) == file_size_bytes)
        {
            println!(
                "skipping file {} of {num_parquet_files} ({filename} already exists with expected file size)",
                index + 1
            );
        } else {
            // scope to close files
            {
                println!(
                    "downloading file {} of {num_parquet_files} ({filename})...",
                    index + 1
                );

                let store_location = to_store_path(parquet_file)?;
                let mut response = self
                    .store_client
                    .get_parquet_file_by_object_store_path(store_location.to_string(), None)
                    .await?
                    .map_ok(|res| res.data)
                    .map_err(|err| io::Error::new(io::ErrorKind::Other, err))
                    .into_async_read()
                    .compat();

                let mut writer = output.writer(filename);
                io::copy(&mut response, &mut writer).await?;
                writer.shutdown().await?;
            }
        }

        Ok(())
    }

    /// From a given table, or [`Partition`], export a given set of filenames (`{uuid}.parquet`) to:
    ///
    /// 1. `<output_directory>/<uuid>.parquet`: The parquet bytes
    ///
    /// Exports if existing in the object store path, which catalog requests.
    async fn export_existing_objects_at_path(
        &mut self,
        output: &Output,
        filenames: Option<HashSet<String>>,
        namespace_id: NamespaceId,
        table_id: TableId,
        partition_id: Option<i64>,
    ) -> Result<(), ExportError> {
        let path_filter = if let Some(id) = partition_id {
            let Partition {
                table_id: partitions_table,
                identifier,
                key,
                ..
            } = self
                .catalog_client
                .get_partition_by_id(id)
                .await?
                .unwrap_or_else(|| panic!("partition_id {id} was not found in the catalog"));
            assert_eq!(
                table_id.get(),
                partitions_table,
                "expected partition to be a part of table_id={}, but instead it was table_id={}",
                table_id.get(),
                partitions_table
            );

            ParquetFilePathFilter {
                namespace_id: namespace_id.get(),
                table_id: Some(partitions_table),
                partition_id: identifier,
                partition_key: Some(key),
            }
        } else {
            ParquetFilePathFilter {
                namespace_id: namespace_id.get(),
                table_id: Some(table_id.get()),
                partition_id: None,
                partition_key: None,
            }
        };

        let mut existing_objects_exporter =
            ExportExistingObjects::try_new(filenames, path_filter, self.store_client.clone())
                .await?;

        let mut index = 1;
        while let Ok(Some(filename)) = existing_objects_exporter.export_next(output).await {
            println!("downloaded file {} ({filename})...", index);
            index += 1;
        }

        let not_found = existing_objects_exporter.objects_wanted();
        if !not_found.is_empty() {
            return Err(ExportError::InvalidArg(format!(
                "requested uuids which do not exist in remote store: {:?}",
                not_found
            )));
        }

        Ok(())
    }
}

fn to_partition_id(partition_identifier: Option<&PartitionIdentifier>) -> TransitionPartitionId {
    match partition_identifier
        .and_then(|pi| pi.id.as_ref())
        .expect("Catalog service should send the partition identifier")
    {
        partition_identifier::Id::HashId(bytes) => TransitionPartitionId::Hash(
            PartitionHashId::try_from(&bytes[..])
                .expect("Catalog service should send valid hash_id bytes"),
        ),
        partition_identifier::Id::CatalogId(id) => {
            TransitionPartitionId::Catalog(PartitionId::new(*id))
        }
    }
}

fn to_store_path(file: &ParquetFile) -> Result<Path> {
    let ParquetFile {
        namespace_id,
        table_id,
        partition_hash_id,
        partition_id,
        object_store_id,
        ..
    } = file;
    let partition_hash_id: PartitionHashId = partition_hash_id[..]
        .try_into()
        .map_err(|e: PartitionHashIdError| ExportError::Internal(e.to_string()))?;

    Ok(Path::from_iter([
        &format!("{}", namespace_id),
        &format!("{}", table_id),
        TransitionPartitionId::from_parts(PartitionId::new(*partition_id), Some(partition_hash_id))
            .to_string()
            .as_str(),
        &format!("{}.parquet", object_store_id),
    ]))
}

/// Export existing objects from a given store prefix.
///
/// This exporter collects all the existing objects once (scoped down to a given
/// prefix path), and determines if any are within the desired set.
///
/// Does not explicitly support versioned objects. Instead, it supports whichever
/// objects exist within the [`object_store::ObjectStore::list()`] request.
struct ExportExistingObjects {
    /// Given objects to export, if located at path_prefix.
    ///
    /// If none, then export all exisitng objects at path.
    objects_wanted: Option<HashSet<String /* filename = `uuid.parquet` */>>,

    /// objects available at path_prefix
    objects_available: Vec<Option<ObjectMetadata>>,

    /// access to store
    store_client: store::Client,
}

impl ExportExistingObjects {
    async fn try_new(
        objects_wanted: Option<HashSet<String /* filename = `uuid.parquet` */>>,
        path_prefix: ParquetFilePathFilter,
        mut store_client: store::Client,
    ) -> Result<Self, ExportError> {
        let objects_available = store_client
            .list_parquet_files_by_path_filter(path_prefix)
            .await?
            .map_ok(|ListParquetFilesByPathFilterResponse { metadata }| metadata)
            .try_collect::<Vec<Option<ObjectMetadata>>>()
            .await?;

        Ok(Self {
            objects_wanted,
            objects_available,
            store_client,
        })
    }

    /// Export next object, if available
    async fn export_next(
        &mut self,
        output: &Output,
    ) -> Result<Option<String /* filename */>, ExportError> {
        if self
            .objects_wanted
            .as_ref()
            .map(|wanted| wanted.is_empty())
            .unwrap_or(false)
        {
            return Ok(None);
        }

        while let Some(Some(ObjectMetadata {
            location, version, ..
        })) = self.objects_available.pop()
        {
            let available_path = Path::parse(&location)?;
            let available_filename = available_path
                .filename()
                .expect("path should have filename");

            if self
                .objects_wanted
                .as_mut()
                .map(|wanted| wanted.remove(available_filename))
                .unwrap_or(true)
            {
                let mut reader = self
                    .store_client
                    .get_parquet_file_by_object_store_path(available_path.to_string(), version)
                    .await?
                    .map_ok(|res| res.data)
                    .map_err(|err| io::Error::new(io::ErrorKind::Other, err))
                    .into_async_read()
                    .compat();

                let mut writer = output.writer(available_filename);
                io::copy(&mut reader, &mut writer).await?;
                writer.shutdown().await?;

                return Ok(Some(available_filename.to_string()));
            } else {
                continue;
            }
        }

        Ok(None)
    }

    /// Get objects_wanted.
    /// Note that objects are removed as they are exported.
    fn objects_wanted(&self) -> HashSet<String /* filename = `uuid.parquet` */> {
        self.objects_wanted.to_owned().unwrap_or_default()
    }
}
