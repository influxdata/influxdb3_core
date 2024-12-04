//! Helper methods to simplify serialization work.

use std::sync::Arc;

use error_reporting::DisplaySourceChain;
use generated_types::influxdata::iox::catalog::v2 as proto;

use crate::interface::{SoftDeletedRows, UnhandledError};

/// Error type dedicated for (de)serialization
///
/// This makes it easier to reuse serialization routines
/// that are used both ways (client->server, server->client),
/// especially when they are nested (i.e. a struct contains
/// another struct).
#[derive(Debug)]
pub struct Error {
    msg: String,
    path: Vec<&'static str>,
}

impl Error {
    /// Create a new (de)serialization error
    pub fn new<E>(e: E) -> Self
    where
        E: std::fmt::Display,
    {
        Self {
            msg: e.to_string(),
            path: vec![],
        }
    }

    /// Add context
    pub fn ctx(self, arg: &'static str) -> Self {
        let Self { msg, mut path } = self;
        path.insert(0, arg);
        Self { msg, path }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if !self.path.is_empty() {
            write!(f, "{}", self.path[0])?;
            for p in self.path.iter().skip(1) {
                write!(f, ".{}", p)?;
            }
            write!(f, ": ")?;
        }

        write!(f, "{}", self.msg)?;

        Ok(())
    }
}

impl std::error::Error for Error {}

impl From<Error> for crate::interface::Error {
    fn from(e: Error) -> Self {
        UnhandledError::GrpcSerialization {
            source: Arc::new(e),
        }
        .into()
    }
}

impl From<Error> for tonic::Status {
    fn from(e: Error) -> Self {
        Self::invalid_argument(e.to_string())
    }
}

pub(crate) trait ConvertExt<O> {
    fn convert(self) -> Result<O, Error>;
}

impl<T, O> ConvertExt<O> for T
where
    T: TryInto<O>,
    T::Error: std::fmt::Display,
{
    fn convert(self) -> Result<O, Error> {
        self.try_into().map_err(Error::new)
    }
}

pub(crate) trait ConvertOptExt<O> {
    fn convert_opt(self) -> Result<O, Error>;
}

impl<T, O> ConvertOptExt<Option<O>> for Option<T>
where
    T: TryInto<O>,
    T::Error: std::fmt::Display,
{
    fn convert_opt(self) -> Result<Option<O>, Error> {
        self.map(|x| x.convert()).transpose()
    }
}

pub(crate) trait RequiredExt<T> {
    fn required(self) -> Result<T, Error>;
}

impl<T> RequiredExt<T> for Option<T> {
    fn required(self) -> Result<T, Error> {
        self.ok_or_else(|| Error::new("required"))
    }
}

pub(crate) trait ContextExt<T> {
    fn ctx(self, path: &'static str) -> Result<T, Error>;
}

impl<T> ContextExt<T> for Result<T, Error> {
    fn ctx(self, path: &'static str) -> Self {
        self.map_err(|e| e.ctx(path))
    }
}

pub(crate) fn convert_status(status: tonic::Status) -> crate::interface::Error {
    use crate::interface::Error;

    match status.code() {
        tonic::Code::Internal => UnhandledError::GrpcRequest {
            source: Box::<dyn std::error::Error + Send + Sync>::from(status.message().to_owned())
                .into(),
        }
        .into(),
        tonic::Code::AlreadyExists => Error::AlreadyExists {
            descr: status.message().to_owned(),
        },
        tonic::Code::ResourceExhausted => Error::LimitExceeded {
            descr: status.message().to_owned(),
        },
        tonic::Code::NotFound => Error::NotFound {
            descr: status.message().to_owned(),
        },
        tonic::Code::InvalidArgument => Error::Malformed {
            descr: status.message().to_owned(),
        },
        tonic::Code::Unimplemented => Error::NotImplemented {
            descr: status.message().to_owned(),
        },
        _ => UnhandledError::GrpcRequest {
            source: Arc::new(status),
        }
        .into(),
    }
}

/// Converts the catalog error to tonic status
pub fn catalog_error_to_status(e: crate::interface::Error) -> tonic::Status {
    use crate::interface::Error;

    match e {
        Error::Unhandled { source } => {
            // walk cause chain to display full details
            // see https://github.com/influxdata/influxdb_iox/issues/12373
            tonic::Status::internal(DisplaySourceChain::new(source).to_string())
        }
        Error::AlreadyExists { descr } => tonic::Status::already_exists(descr),
        Error::LimitExceeded { descr } => tonic::Status::resource_exhausted(descr),
        Error::NotFound { descr } => tonic::Status::not_found(descr),
        Error::Malformed { descr } => tonic::Status::invalid_argument(descr),
        Error::NotImplemented { descr } => tonic::Status::unimplemented(descr),
    }
}

pub(crate) fn serialize_soft_deleted_rows(sdr: SoftDeletedRows) -> i32 {
    let sdr = match sdr {
        SoftDeletedRows::AllRows => proto::SoftDeletedRows::AllRows,
        SoftDeletedRows::ExcludeDeleted => proto::SoftDeletedRows::ExcludeDeleted,
        SoftDeletedRows::OnlyDeleted => proto::SoftDeletedRows::OnlyDeleted,
    };

    sdr.into()
}

pub(crate) fn deserialize_soft_deleted_rows(sdr: i32) -> Result<SoftDeletedRows, Error> {
    let sdr: proto::SoftDeletedRows = sdr.convert().ctx("soft deleted rows")?;
    let sdr = match sdr {
        proto::SoftDeletedRows::Unspecified => {
            return Err(Error::new("unspecified soft deleted rows"));
        }
        proto::SoftDeletedRows::AllRows => SoftDeletedRows::AllRows,
        proto::SoftDeletedRows::ExcludeDeleted => SoftDeletedRows::ExcludeDeleted,
        proto::SoftDeletedRows::OnlyDeleted => SoftDeletedRows::OnlyDeleted,
    };
    Ok(sdr)
}
