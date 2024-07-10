use async_trait::async_trait;
use generated_types::influxdata::iox::authz::v1::{self as proto, AuthorizeResponse};
use observability_deps::tracing::warn;
use snafu::Snafu;
use tonic::Response;

use super::{Authorizer, Permission};

/// Authorizer implementation using influxdata.iox.authz.v1 protocol.
#[derive(Clone, Debug)]
pub struct IoxAuthorizer {
    client:
        proto::iox_authorizer_service_client::IoxAuthorizerServiceClient<tonic::transport::Channel>,
}

impl IoxAuthorizer {
    /// Attempt to create a new client by connecting to a given endpoint.
    pub fn connect_lazy<D>(dst: D) -> Result<Self, Box<dyn std::error::Error>>
    where
        D: TryInto<tonic::transport::Endpoint> + Send,
        D::Error: Into<tonic::codegen::StdError>,
    {
        let ep = tonic::transport::Endpoint::new(dst)?;
        let client = proto::iox_authorizer_service_client::IoxAuthorizerServiceClient::new(
            ep.connect_lazy(),
        );
        Ok(Self { client })
    }

    async fn request(
        &self,
        token: Vec<u8>,
        requested_perms: &[Permission],
    ) -> Result<Response<AuthorizeResponse>, tonic::Status> {
        let req = proto::AuthorizeRequest {
            token,
            permissions: requested_perms
                .iter()
                .filter_map(|p| p.clone().try_into().ok())
                .collect(),
        };
        let mut client = self.client.clone();
        client.authorize(req).await
    }
}

#[async_trait]
impl Authorizer for IoxAuthorizer {
    async fn permissions(
        &self,
        token: Option<Vec<u8>>,
        requested_perms: &[Permission],
    ) -> Result<Vec<Permission>, Error> {
        let authz_rpc_result = self
            .request(token.ok_or(Error::NoToken)?, requested_perms)
            .await
            .map_err(|status| Error::Verification {
                msg: status.message().to_string(),
                source: Box::new(status),
            })?
            .into_inner();

        if !authz_rpc_result.valid {
            return Err(Error::InvalidToken);
        }

        let intersected_perms: Vec<Permission> = authz_rpc_result
            .permissions
            .into_iter()
            .filter_map(|p| match p.try_into() {
                Ok(p) => Some(p),
                Err(e) => {
                    warn!(error=%e, "authz service returned incompatible permission");
                    None
                }
            })
            .collect();

        if intersected_perms.is_empty() {
            return Err(Error::Forbidden);
        }
        Ok(intersected_perms)
    }
}

/// Authorization related error.
#[derive(Debug, Snafu)]
pub enum Error {
    /// Communication error when verifying a token.
    #[snafu(display("token verification not possible: {msg}"))]
    Verification {
        /// Message describing the error.
        msg: String,
        /// Source of the error.
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    /// Token is invalid.
    #[snafu(display("invalid token"))]
    InvalidToken,

    /// The token's permissions do not allow the operation.
    #[snafu(display("forbidden"))]
    Forbidden,

    /// No token has been supplied, but is required.
    #[snafu(display("no token"))]
    NoToken,
}

impl Error {
    /// Create new Error::Verification.
    pub fn verification(
        msg: impl Into<String>,
        source: impl Into<Box<dyn std::error::Error + Send + Sync + 'static>>,
    ) -> Self {
        Self::Verification {
            msg: msg.into(),
            source: source.into(),
        }
    }
}

impl From<tonic::Status> for Error {
    fn from(value: tonic::Status) -> Self {
        Self::verification(value.message(), value.clone())
    }
}

#[cfg(test)]
mod test {
    use std::{
        net::SocketAddr,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        time::Duration,
    };

    use assert_matches::assert_matches;
    use test_helpers_authz::Authorizer as AuthorizerServer;
    use tokio::{
        net::TcpListener,
        task::{spawn, JoinHandle},
    };
    use tonic::transport::{server::TcpIncoming, Server};

    use super::*;
    use crate::{Action, Authorizer, Permission, Resource};

    const NAMESPACE: &str = "bananas";

    macro_rules! test_iox_authorizer {
        (
            $name:ident,
            token_permissions = $token_permissions:expr,
            permissions_required = $permissions_required:expr,
            want = $want:pat
        ) => {
            paste::paste! {
                #[tokio::test]
                async fn [<test_iox_authorizer_ $name>]() {
                    let mut authz_server = AuthorizerServer::create().await;
                    let authz = IoxAuthorizer::connect_lazy(authz_server.addr())
                            .expect("Failed to create IoxAuthorizer client.");

                    let token = authz_server.create_token_for(NAMESPACE, $token_permissions);

                    let got = authz.permissions(
                        Some(token.as_bytes().to_vec()),
                        $permissions_required
                    ).await;

                    assert_matches!(got, $want);
                }
            }
        };
    }

    test_iox_authorizer!(
        ok,
        token_permissions = &["ACTION_WRITE"],
        permissions_required = &[Permission::ResourceAction(
            Resource::Database(NAMESPACE.to_string()),
            Action::Write,
        )],
        want = Ok(_)
    );

    test_iox_authorizer!(
        insufficient_perms,
        token_permissions = &["ACTION_READ"],
        permissions_required = &[Permission::ResourceAction(
            Resource::Database(NAMESPACE.to_string()),
            Action::Write,
        )],
        want = Err(Error::Forbidden)
    );

    test_iox_authorizer!(
        any_of_required_perms,
        token_permissions = &["ACTION_WRITE"],
        permissions_required = &[
            Permission::ResourceAction(Resource::Database(NAMESPACE.to_string()), Action::Write,),
            Permission::ResourceAction(Resource::Database(NAMESPACE.to_string()), Action::Create,)
        ],
        want = Ok(_)
    );

    #[tokio::test]
    async fn test_invalid_token() {
        let authz_server = AuthorizerServer::create().await;
        let authz = IoxAuthorizer::connect_lazy(authz_server.addr())
            .expect("Failed to create IoxAuthorizer client.");

        let invalid_token = b"UGLY";

        let got = authz
            .permissions(
                Some(invalid_token.to_vec()),
                &[Permission::ResourceAction(
                    Resource::Database(NAMESPACE.to_string()),
                    Action::Read,
                )],
            )
            .await;

        assert_matches!(got, Err(Error::InvalidToken));
    }

    #[tokio::test]
    async fn test_delayed_probe_response() {
        #[derive(Default, Debug)]
        struct DelayedAuthorizer(Arc<AtomicBool>);

        impl DelayedAuthorizer {
            fn start_countdown(&self) {
                let started = Arc::clone(&self.0);
                spawn(async move {
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    started.store(true, Ordering::Relaxed);
                });
            }
        }

        #[async_trait]
        impl proto::iox_authorizer_service_server::IoxAuthorizerService for DelayedAuthorizer {
            async fn authorize(
                &self,
                _request: tonic::Request<proto::AuthorizeRequest>,
            ) -> Result<tonic::Response<AuthorizeResponse>, tonic::Status> {
                let startup_done = self.0.load(Ordering::Relaxed);
                if !startup_done {
                    return Err(tonic::Status::deadline_exceeded("startup not done"));
                }

                Ok(tonic::Response::new(AuthorizeResponse {
                    valid: true,
                    subject: None,
                    permissions: vec![],
                }))
            }
        }

        #[derive(Debug)]
        struct DelayedServer {
            addr: SocketAddr,
            handle: JoinHandle<Result<(), tonic::transport::Error>>,
        }

        impl DelayedServer {
            async fn create() -> Self {
                let listener = TcpListener::bind("localhost:0").await.unwrap();
                let addr = listener.local_addr().unwrap();
                let incoming = TcpIncoming::from_listener(listener, false, None).unwrap();

                // start countdown mocking startup delay of sidecar
                let authz = DelayedAuthorizer::default();
                authz.start_countdown();

                let router = Server::builder().add_service(
                    proto::iox_authorizer_service_server::IoxAuthorizerServiceServer::new(authz),
                );
                let handle = spawn(router.serve_with_incoming(incoming));
                Self { addr, handle }
            }

            fn addr(&self) -> String {
                format!("http://{}", self.addr)
            }

            fn close(self) {
                self.handle.abort();
            }
        }

        let authz_server = DelayedServer::create().await;
        let authz_client = IoxAuthorizer::connect_lazy(authz_server.addr())
            .expect("Failed to create IoxAuthorizer client.");

        assert_matches!(
            authz_client.probe().await,
            Ok(()),
            "authz probe should work even with delay"
        );
        authz_server.close();
    }
}
