//! CLI config for the controller command.

/// Config for the controller.
#[derive(Debug, Clone, clap::Parser)]
pub struct ControllerConfig {
    /// Restrict the watchers to this namespace.
    #[clap(
        long = "watch-namespace",
        env = "INFLUXDB_IOX_CONTROLLER_WATCH_NAMESPACE"
    )]
    pub watch_namespace: Option<String>,
}
