//! Auth related configs.

use std::path::PathBuf;

/// Configuration for the auth service.
#[derive(Debug, Clone, PartialEq, Eq, clap::Parser)]
pub struct AuthConfig {
    /// The path to the directory containing the token files
    #[arg(
        short,
        long,
        env = "INFLUXDB_IOX_AUTH_TOKEN_PATH",
        default_value = "/conf/authz"
    )]
    pub token_path: PathBuf,

    /// Check the token, but allow all requested permission anyway
    #[arg(
        long,
        hide = true,
        env = "INFLUXDB_IOX_AUTH_INSECURE_ALLOW_ALL_PERMISSIONS"
    )]
    pub insecure_mode: bool,
}
