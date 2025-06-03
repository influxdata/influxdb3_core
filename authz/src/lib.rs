//! IOx authorization client.
//!
//! Authorization client interface to be used by IOx components to
//! restrict access to authorized requests where required.

#![warn(missing_docs)]

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

use base64::{prelude::BASE64_STANDARD, Engine};
use generated_types::influxdata::iox::authz::v1::{self as proto};

mod authorization;
pub use authorization::Authorization;
mod authorizer;
pub use authorizer::Authorizer;
mod iox_authorizer;
pub use iox_authorizer::{Error, IoxAuthorizer};
mod instrumentation;
pub use instrumentation::AuthorizerInstrumentation;
mod permission;
pub use permission::{Action, Permission, Resource, Target};

#[cfg(feature = "http")]
pub mod http;

/// Extract a token from an HTTP header or gRPC metadata value.
pub fn extract_token<T: AsRef<[u8]> + ?Sized>(value: Option<&T>) -> Option<Vec<u8>> {
    let mut parts = value?.as_ref().splitn(2, |&v| v == b' ');
    let token = match parts.next()? {
        b"Token" | b"Bearer" => parts.next()?.to_vec(),
        b"Basic" => parts
            .next()
            .and_then(|v| BASE64_STANDARD.decode(v).ok())?
            .splitn(2, |&v| v == b':')
            .nth(1)?
            .to_vec(),
        _ => return None,
    };
    if token.is_empty() {
        None
    } else {
        Some(token)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use generated_types::Status;

    #[test]
    fn verify_error_from_tonic_status() {
        let s = Status::resource_exhausted("test error");
        let e = Error::from(s);
        assert_eq!(
            "token verification not possible: test error",
            format!("{e}")
        )
    }

    #[test]
    fn test_extract_token() {
        assert_eq!(None, extract_token::<&str>(None));
        assert_eq!(None, extract_token(Some("")));
        assert_eq!(None, extract_token(Some("Basic")));
        assert_eq!(None, extract_token(Some("Basic Og=="))); // ":"
        assert_eq!(None, extract_token(Some("Basic dXNlcm5hbWU6"))); // "username:"
        assert_eq!(None, extract_token(Some("Basic Og=="))); // ":"
        assert_eq!(
            Some(b"password".to_vec()),
            extract_token(Some("Basic OnBhc3N3b3Jk"))
        ); // ":password"
        assert_eq!(
            Some(b"password2".to_vec()),
            extract_token(Some("Basic dXNlcm5hbWU6cGFzc3dvcmQy"))
        ); // "username:password2"
        assert_eq!(None, extract_token(Some("Bearer")));
        assert_eq!(None, extract_token(Some("Bearer ")));
        assert_eq!(Some(b"token".to_vec()), extract_token(Some("Bearer token")));
        assert_eq!(None, extract_token(Some("Token")));
        assert_eq!(None, extract_token(Some("Token ")));
        assert_eq!(
            Some(b"token2".to_vec()),
            extract_token(Some("Token token2"))
        );
    }
}
