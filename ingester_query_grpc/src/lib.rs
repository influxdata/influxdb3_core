// This crate deliberately does not use the same linting rules as the other
// crates because of all the generated code it contains that we don't have much
// control over.
#![expect(
    clippy::derive_partial_eq_without_eq,
    clippy::needless_borrows_for_generic_args,
    clippy::needless_lifetimes,
    clippy::allow_attributes
)]

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

pub use tonic;

use crate::influxdata::iox::ingester::v1 as proto;
use base64::{Engine, prelude::BASE64_STANDARD};
use data_types::{NamespaceId, TableId, TimestampMinMax, TimestampRange};
use datafusion::{common::DataFusionError, prelude::Expr};
use datafusion_proto::bytes::Serializeable;
use predicate::{Predicate, ValueExpr};
use prost::Message;
use snafu::{ResultExt, Snafu};

/// This module imports the generated protobuf code into a Rust module
/// hierarchy that matches the namespace hierarchy of the protobuf
/// definitions
#[expect(clippy::use_self, missing_copy_implementations)]
pub mod influxdata {
    pub mod iox {
        pub mod ingester {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/influxdata.iox.ingester.v1.rs"));
                include!(concat!(
                    env!("OUT_DIR"),
                    "/influxdata.iox.ingester.v1.serde.rs"
                ));
            }
        }
    }
}

/// Error returned if a request field has an invalid value. Includes
/// machinery to add parent field names for context -- thus it will
/// report `rules.write_timeout` than simply `write_timeout`.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct FieldViolation {
    pub field: String,
    pub description: String,
}

impl FieldViolation {
    pub fn required(field: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            description: "Field is required".to_string(),
        }
    }

    /// Re-scopes this error as the child of another field
    pub fn scope(self, field: impl Into<String>) -> Self {
        let field = if self.field.is_empty() {
            field.into()
        } else {
            [field.into(), self.field].join(".")
        };

        Self {
            field,
            description: self.description,
        }
    }
}

impl std::error::Error for FieldViolation {}

impl std::fmt::Display for FieldViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Violation for field \"{}\": {}",
            self.field, self.description
        )
    }
}

fn expr_to_bytes_violation(field: impl Into<String>, e: DataFusionError) -> FieldViolation {
    FieldViolation {
        field: field.into(),
        description: format!("Error converting Expr to bytes: {e}"),
    }
}

fn expr_from_bytes_violation(field: impl Into<String>, e: DataFusionError) -> FieldViolation {
    FieldViolation {
        field: field.into(),
        description: format!("Error creating Expr from bytes: {e}"),
    }
}

/// Request from the querier service to the ingester service
#[derive(Debug, PartialEq, Clone)]
pub struct IngesterQueryRequest {
    /// namespace to search
    pub namespace_id: NamespaceId,

    /// Table to search
    pub table_id: TableId,

    /// Columns the query service is interested in
    pub columns: Vec<String>,

    /// Predicate for filtering
    pub predicate: Option<Predicate>,
}

impl IngesterQueryRequest {
    /// Make a request to return data for a specified table
    pub fn new(
        namespace_id: NamespaceId,
        table_id: TableId,
        columns: Vec<String>,
        predicate: Option<Predicate>,
    ) -> Self {
        Self {
            namespace_id,
            table_id,
            columns,
            predicate,
        }
    }
}

impl TryFrom<proto::IngesterQueryRequest> for IngesterQueryRequest {
    type Error = FieldViolation;

    fn try_from(proto: proto::IngesterQueryRequest) -> Result<Self, Self::Error> {
        let proto::IngesterQueryRequest {
            namespace_id,
            table_id,
            columns,
            predicate,
        } = proto;

        let namespace_id = NamespaceId::new(namespace_id);
        let table_id = TableId::new(table_id);
        let predicate = predicate.map(TryInto::try_into).transpose()?;

        Ok(Self::new(namespace_id, table_id, columns, predicate))
    }
}

impl TryFrom<IngesterQueryRequest> for proto::IngesterQueryRequest {
    type Error = FieldViolation;

    fn try_from(query: IngesterQueryRequest) -> Result<Self, Self::Error> {
        let IngesterQueryRequest {
            namespace_id,
            table_id,
            columns,
            predicate,
        } = query;

        Ok(Self {
            namespace_id: namespace_id.get(),
            table_id: table_id.get(),
            columns,
            predicate: predicate.map(TryInto::try_into).transpose()?,
        })
    }
}

/// Request from the querier service to the ingester service
#[derive(Debug, PartialEq, Clone)]
pub struct IngesterQueryRequest2 {
    /// namespace to search
    pub namespace_id: NamespaceId,

    /// Table to search
    pub table_id: TableId,

    /// Columns the query service is interested in
    pub columns: Vec<String>,

    /// Predicate for filtering
    pub filters: Vec<Expr>,

    /// Time interval specified by the filters. This will be used by the
    /// ingestor for cheap early filtering.
    pub t_min_max: TimestampMinMax,
}

impl IngesterQueryRequest2 {
    /// Make a request to return data for a specified table
    pub fn new(
        namespace_id: NamespaceId,
        table_id: TableId,
        columns: Vec<String>,
        filters: Vec<Expr>,
        t_min_max: TimestampMinMax,
    ) -> Self {
        Self {
            namespace_id,
            table_id,
            columns,
            filters,
            t_min_max,
        }
    }
}

impl TryFrom<Predicate> for proto::Predicate {
    type Error = FieldViolation;

    fn try_from(pred: Predicate) -> Result<Self, Self::Error> {
        let Predicate {
            field_columns,
            range,
            exprs,
            value_expr,
        } = pred;

        let field_columns = field_columns.into_iter().flatten().collect();
        let range = range.map(|r| proto::TimestampRange {
            start: r.start(),
            end: r.end(),
        });

        let exprs = exprs
            .iter()
            .map(|expr| {
                expr.to_bytes()
                    .map(|bytes| bytes.to_vec())
                    .map_err(|e| expr_to_bytes_violation("exprs", e))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let value_expr = value_expr
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            field_columns,
            range,
            exprs,
            value_expr,
        })
    }
}

impl TryFrom<proto::Predicate> for Predicate {
    type Error = FieldViolation;

    fn try_from(proto: proto::Predicate) -> Result<Self, Self::Error> {
        let proto::Predicate {
            field_columns,
            range,
            exprs,
            value_expr,
        } = proto;

        let field_columns = if field_columns.is_empty() {
            None
        } else {
            Some(field_columns.into_iter().collect())
        };

        let range = range.map(|r| TimestampRange::new(r.start, r.end));

        let exprs = exprs
            .into_iter()
            .map(|bytes| {
                Expr::from_bytes_with_registry(&bytes, query_functions::registry())
                    .map_err(|e| expr_from_bytes_violation("exprs", e))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let value_expr = value_expr
            .into_iter()
            .map(|ve| {
                let expr = Expr::from_bytes_with_registry(&ve.expr, query_functions::registry())
                    .map_err(|e| expr_from_bytes_violation("value_expr.expr", e))?;
                // try to convert to ValueExpr
                expr.try_into().map_err(|e| FieldViolation {
                    field: "expr".into(),
                    description: format!("Internal: Serialized expr a valid ValueExpr: {e:?}"),
                })
            })
            .collect::<Result<Vec<ValueExpr>, FieldViolation>>()?;

        Ok(Self {
            field_columns,
            range,
            exprs,
            value_expr,
        })
    }
}

impl TryFrom<ValueExpr> for proto::ValueExpr {
    type Error = FieldViolation;

    fn try_from(value_expr: ValueExpr) -> Result<Self, Self::Error> {
        let expr: Expr = value_expr.into();

        let expr = expr
            .to_bytes()
            .map_err(|e| expr_to_bytes_violation("value_expr.expr", e))?
            .to_vec();

        Ok(Self { expr })
    }
}

#[derive(Debug, Snafu, Copy, Clone)]
pub enum EncodeProtoPredicateFromBase64Error {
    #[snafu(display("Cannot encode protobuf: {source}"))]
    ProtobufEncode { source: prost::EncodeError },
}

/// Encodes [`proto::Predicate`] as base64.
pub fn encode_proto_predicate_as_base64(
    predicate: &proto::Predicate,
) -> Result<String, EncodeProtoPredicateFromBase64Error> {
    let mut buf = vec![];
    predicate.encode(&mut buf).context(ProtobufEncodeSnafu)?;
    Ok(BASE64_STANDARD.encode(&buf))
}

#[derive(Debug, Snafu)]
pub enum DecodeProtoPredicateFromBase64Error {
    #[snafu(display("Cannot decode base64: {source}"))]
    Base64Decode { source: base64::DecodeError },

    #[snafu(display("Cannot decode protobuf: {source}"))]
    ProtobufDecode { source: prost::DecodeError },
}

/// Decodes [`proto::Predicate`] from base64 string.
pub fn decode_proto_predicate_from_base64(
    s: &str,
) -> Result<proto::Predicate, DecodeProtoPredicateFromBase64Error> {
    let predicate_binary = BASE64_STANDARD.decode(s).context(Base64DecodeSnafu)?;
    proto::Predicate::decode(predicate_binary.as_slice()).context(ProtobufDecodeSnafu)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use datafusion::prelude::*;

    #[test]
    fn query_round_trip() {
        let rust_predicate = predicate::Predicate::new()
            .with_range(1, 100)
            .with_expr(col("foo"))
            .with_value_expr(col("_value").eq(lit("bar")).try_into().unwrap());

        let rust_query = IngesterQueryRequest::new(
            NamespaceId::new(42),
            TableId::new(1337),
            vec!["usage".into(), "time".into()],
            Some(rust_predicate),
        );

        let proto_query: proto::IngesterQueryRequest = rust_query.clone().try_into().unwrap();

        let rust_query_converted: IngesterQueryRequest = proto_query.try_into().unwrap();

        assert_eq!(rust_query, rust_query_converted);
    }

    #[test]
    fn predicate_proto_base64_roundtrip() {
        let predicate = Predicate {
            field_columns: Some(BTreeSet::from([String::from("foo"), String::from("bar")])),
            range: Some(TimestampRange::new(13, 42)),
            exprs: vec![Expr::Wildcard {
                qualifier: None,
                options: Default::default(),
            }],
            value_expr: vec![col("_value").eq(lit("bar")).try_into().unwrap()],
        };
        let predicate: proto::Predicate = predicate.try_into().unwrap();
        let base64 = encode_proto_predicate_as_base64(&predicate).unwrap();
        let predicate2 = decode_proto_predicate_from_base64(&base64).unwrap();
        assert_eq!(predicate, predicate2);
    }
}
