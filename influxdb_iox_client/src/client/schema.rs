use self::generated_types::{schema_service_client::SchemaServiceClient, *};
use ::generated_types::google::OptionalField;
use ::generated_types::influxdata::iox::Target;
use client_util::connection::GrpcConnection;

use crate::connection::Connection;
use crate::error::Error;

/// Re-export generated_types
pub mod generated_types {
    pub use generated_types::influxdata::iox::{column_type::v1::ColumnType, schema::v1::*};
}

/// A basic client for fetching the Schema for a Namespace.
#[derive(Debug, Clone)]
pub struct Client {
    inner: SchemaServiceClient<GrpcConnection>,
}

impl Client {
    /// Creates a new client with the provided connection
    pub fn new(connection: Connection) -> Self {
        Self {
            inner: SchemaServiceClient::new(connection.into_grpc_connection()),
        }
    }

    /// Get the schema for a namespace and, optionally, one table within that namespace.
    pub async fn get_schema(
        &mut self,
        namespace: &str,
        table: Option<&str>,
    ) -> Result<NamespaceSchema, Error> {
        let response = self
            .inner
            .get_schema(GetSchemaRequest {
                namespace: namespace.to_string(),
                table: table.map(ToString::to_string),
            })
            .await?;

        Ok(response.into_inner().schema.unwrap_field("schema")?)
    }

    /// Upsert the schema for a namespace and table. Returns the schema for the specified table
    /// after applying the upsert.
    pub async fn upsert_schema(
        &mut self,
        namespace: impl Into<Target> + Send,
        table: &str,
        columns: impl Iterator<Item = (&str, ColumnType)> + Send,
    ) -> Result<NamespaceSchema, Error> {
        let columns = columns
            .map(|(name, column_type)| (name.to_string(), column_type as i32))
            .collect();

        let response = self
            .inner
            .upsert_schema(UpsertSchemaRequest {
                namespace_target: namespace.into().into(),
                table: table.to_string(),
                columns,
            })
            .await?;

        Ok(response.into_inner().schema.unwrap_field("schema")?)
    }
}
