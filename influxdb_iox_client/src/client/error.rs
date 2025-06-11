use generated_types::{
    Code, Status,
    google::{AlreadyExists, FieldViolation, NotFound, PreconditionViolation},
};
use std::fmt::Debug;
use thiserror::Error;

/// A generic opaque error
pub type StdError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// A gRPC error payload with optional [details](https://cloud.google.com/apis/design/errors#error_details)
#[derive(Error, Debug, Clone)]
#[error("{message}")]
pub struct ServerError<D> {
    /// A human readable error message
    pub message: String,
    /// An optional machine-readable error
    pub details: Option<D>,
}

fn parse_status<D: ServerErrorDetails>(status: Status) -> ServerError<D> {
    ServerError {
        message: status.message().to_string(),
        details: D::try_decode(&status),
    }
}

trait ServerErrorDetails: Sized {
    fn try_decode(data: &Status) -> Option<Self>;
}

impl ServerErrorDetails for () {
    fn try_decode(_: &Status) -> Option<Self> {
        None
    }
}

impl ServerErrorDetails for FieldViolation {
    fn try_decode(status: &Status) -> Option<Self> {
        generated_types::google::decode_field_violation(status).next()
    }
}

impl ServerErrorDetails for AlreadyExists {
    fn try_decode(status: &Status) -> Option<Self> {
        generated_types::google::decode_already_exists(status).next()
    }
}

impl ServerErrorDetails for NotFound {
    fn try_decode(status: &Status) -> Option<Self> {
        generated_types::google::decode_not_found(status).next()
    }
}

impl ServerErrorDetails for PreconditionViolation {
    fn try_decode(status: &Status) -> Option<Self> {
        generated_types::google::decode_precondition_violation(status).next()
    }
}

/// The errors returned by this client
#[derive(Error, Debug)]
#[expect(missing_docs)]
pub enum Error {
    #[error("The operation was cancelled: {0}")]
    Cancelled(ServerError<()>),

    #[error("Unknown server error: {0}")]
    Unknown(ServerError<()>),

    #[error("Client specified an invalid argument: {0}")]
    InvalidArgument(Box<ServerError<FieldViolation>>),

    #[error("Deadline expired before operation could complete: {0}")]
    DeadlineExceeded(ServerError<()>),

    #[error("{0}")]
    NotFound(Box<ServerError<NotFound>>),

    #[error("Some entity that we attempted to create already exists: {0}")]
    AlreadyExists(Box<ServerError<AlreadyExists>>),

    #[error("The caller does not have permission to execute the specified operation: {0}")]
    PermissionDenied(ServerError<()>),

    #[error("Some resource has been exhausted: {0}")]
    ResourceExhausted(ServerError<()>),

    #[error("The system is not in a state required for the operation's execution: {0}")]
    FailedPrecondition(Box<ServerError<PreconditionViolation>>),

    #[error("The operation was aborted: {0}")]
    Aborted(ServerError<()>),

    #[error("Operation was attempted past the valid range: {0}")]
    OutOfRange(ServerError<()>),

    #[error("Operation is not implemented or supported: {0}")]
    Unimplemented(ServerError<()>),

    #[error("Internal error: {0}")]
    Internal(ServerError<()>),

    #[error("The service is currently unavailable: {0}")]
    Unavailable(ServerError<()>),

    #[error("Unrecoverable data loss or corruption: {0}")]
    DataLoss(ServerError<()>),

    #[error("The request does not have valid authentication credentials: {0}")]
    Unauthenticated(ServerError<()>),

    #[error("Received an invalid response from the server: {0}")]
    InvalidResponse(#[from] FieldViolation),

    #[error("An unexpected error occurred in the client library: {0}")]
    Client(StdError),
}

impl From<Status> for Error {
    fn from(s: Status) -> Self {
        match s.code() {
            Code::Ok => Self::Client("status is not an error".into()),
            Code::Cancelled => Self::Cancelled(parse_status(s)),
            Code::Unknown => Self::Unknown(parse_status(s)),
            Code::InvalidArgument => Self::InvalidArgument(Box::new(parse_status(s))),
            Code::DeadlineExceeded => Self::DeadlineExceeded(parse_status(s)),
            Code::NotFound => Self::NotFound(Box::new(parse_status(s))),
            Code::AlreadyExists => Self::AlreadyExists(Box::new(parse_status(s))),
            Code::PermissionDenied => Self::PermissionDenied(parse_status(s)),
            Code::ResourceExhausted => Self::ResourceExhausted(parse_status(s)),
            Code::FailedPrecondition => Self::FailedPrecondition(Box::new(parse_status(s))),
            Code::Aborted => Self::Aborted(parse_status(s)),
            Code::OutOfRange => Self::OutOfRange(parse_status(s)),
            Code::Unimplemented => Self::Unimplemented(parse_status(s)),
            Code::Internal => Self::Internal(parse_status(s)),
            Code::Unavailable => Self::Unavailable(parse_status(s)),
            Code::DataLoss => Self::DataLoss(parse_status(s)),
            Code::Unauthenticated => Self::Unauthenticated(parse_status(s)),
        }
    }
}

impl Error {
    /// Return a `Error::Unknown` variant with the specified message
    pub(crate) fn unknown(message: impl Into<String>) -> Self {
        Self::Unknown(ServerError {
            message: message.into(),
            details: None,
        })
    }

    /// Return a `Error::Internal` variant with the specified message
    pub(crate) fn internal(message: impl Into<String>) -> Self {
        Self::Internal(ServerError {
            message: message.into(),
            details: None,
        })
    }

    /// Return a `Error::Client` variant with the specified message
    pub(crate) fn client<E: std::error::Error + Send + Sync + 'static>(e: E) -> Self {
        Self::Client(Box::new(e))
    }

    /// Return `Error::InvalidArgument` specifing an error in `field_name`
    pub(crate) fn invalid_argument(
        field_name: impl Into<String>,
        description: impl Into<String>,
    ) -> Self {
        let field_name = field_name.into();
        let description = description.into();

        Self::InvalidArgument(Box::new(ServerError {
            message: format!("Invalid argument for '{field_name}': {description}"),
            details: Some(FieldViolation {
                field: field_name,
                description,
            }),
        }))
    }
}

/// Translates a reqwest response to an Error
pub(crate) async fn translate_response(response: reqwest::Response) -> Result<(), Error> {
    let status = response.status();

    if status.is_success() {
        Ok(())
    } else if status.is_server_error() {
        Err(Error::internal(response_description(response).await))
    } else {
        // todo would be nice to check for 404, etc and return more specific errors
        Err(Error::unknown(response_description(response).await))
    }
}

/// Makes as detailed error message as possible
async fn response_description(response: reqwest::Response) -> String {
    let status = response.status();

    // see if the response has any text we can include
    match response.text().await {
        Ok(text) => format!("(status {status}): {text}"),
        Err(_) => format!("status: {status}"),
    }
}
