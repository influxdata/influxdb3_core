#![expect(unreachable_pub)]

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

use crate::export::AsyncExporter;
use crate::jaeger::JaegerAgentExporter;
use iox_time::SystemProvider;
use jaeger::JaegerTag;
use snafu::Snafu;
use std::num::{NonZeroU16, NonZeroU64};
use std::sync::Arc;

pub mod export;

mod jaeger;
mod rate_limiter;

/// Auto-generated thrift code
#[expect(
    dead_code,
    deprecated,
    clippy::redundant_field_names,
    clippy::unused_unit,
    clippy::use_self,
    clippy::too_many_arguments
)]
mod thrift {
    pub mod agent;

    pub mod zipkincore;

    pub mod jaeger;
}

/// Default header name used to export traces
pub const DEFAULT_JAEGER_TRACE_CONTEXT_HEADER_NAME: &str = "uber-trace-id";

/// Default header name for Influx Cloud
pub const DEFAULT_INFLUX_TRACE_CONTEXT_HEADER_NAME: &str = "influx-trace-id";

/// CLI config for distributed tracing options
#[derive(Debug, Clone, clap::Parser)]
pub struct TracingConfig {
    /// Tracing: exporter type
    ///
    /// Can be one of: none, jaeger
    #[clap(
        long = "traces-exporter",
        env = "TRACES_EXPORTER",
        default_value = "none",
        action
    )]
    pub traces_exporter: TracesExporter,

    /// Tracing: Jaeger agent network hostname
    ///
    /// Protocol is Thrift/Compact over UDP.
    ///
    /// Only used if `--traces-exporter` is "jaeger".
    #[clap(
        long = "traces-exporter-jaeger-agent-host",
        env = "TRACES_EXPORTER_JAEGER_AGENT_HOST",
        default_value = "0.0.0.0",
        action
    )]
    pub traces_exporter_jaeger_agent_host: String,

    /// Tracing: Jaeger agent network port
    ///
    /// Protocol is Thrift/Compact over UDP.
    ///
    /// Only used if `--traces-exporter` is "jaeger".
    #[clap(
        long = "traces-exporter-jaeger-agent-port",
        env = "TRACES_EXPORTER_JAEGER_AGENT_PORT",
        default_value = "6831",
        action
    )]
    pub traces_exporter_jaeger_agent_port: NonZeroU16,

    /// Tracing: Jaeger service name.
    ///
    /// Only used if `--traces-exporter` is "jaeger".
    #[clap(
        long = "traces-exporter-jaeger-service-name",
        env = "TRACES_EXPORTER_JAEGER_SERVICE_NAME",
        default_value = "iox-conductor",
        action
    )]
    pub traces_exporter_jaeger_service_name: String,

    /// Tracing: specifies the header name used for passing trace context
    ///
    /// Only used if `--traces-exporter` is "jaeger".
    #[clap(
        long = "traces-exporter-jaeger-trace-context-header-name",
        env = "TRACES_EXPORTER_JAEGER_TRACE_CONTEXT_HEADER_NAME",
        default_value = DEFAULT_JAEGER_TRACE_CONTEXT_HEADER_NAME,
        action,
    )]
    pub traces_jaeger_trace_context_header_name: String,

    /// Tracing: specifies the header name used for force sampling
    ///
    /// Only used if `--traces-exporter` is "jaeger".
    #[clap(
        long = "traces-jaeger-debug-name",
        env = "TRACES_EXPORTER_JAEGER_DEBUG_NAME",
        default_value = "jaeger-debug-id",
        action
    )]
    pub traces_jaeger_debug_name: String,

    /// Tracing: set of key=value pairs to annotate tracing spans with.
    ///
    /// Use a comma-delimited string to set multiple pairs: env=prod,region=eu-1
    ///
    /// Only used if `--traces-exporter` is "jaeger".
    #[clap(
        long = "traces-jaeger-tags",
        env = "TRACES_EXPORTER_JAEGER_TAGS",
        value_delimiter = ',',
        action
    )]
    pub traces_jaeger_tags: Option<Vec<JaegerTag>>,

    /// Tracing: Maximum number of message sent to a Jaeger service, per second.
    ///
    /// Only used if `--traces-exporter` is "jaeger".
    #[clap(
        long = "traces-jaeger-max-msgs-per-second",
        env = "TRACES_JAEGER_MAX_MSGS_PER_SECOND",
        default_value = "1000",
        action
    )]
    pub traces_jaeger_max_msgs_per_second: NonZeroU64,
}

impl TracingConfig {
    pub fn build(&self) -> Result<Option<Arc<AsyncExporter>>> {
        match self.traces_exporter {
            TracesExporter::None => Ok(None),
            TracesExporter::Jaeger => Ok(Some(jaeger_exporter(self)?)),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum TracesExporter {
    None,
    Jaeger,
}

impl std::str::FromStr for TracesExporter {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "none" => Ok(Self::None),
            "jaeger" => Ok(Self::Jaeger),
            _ => Err(format!(
                "Invalid traces exporter '{s}'. Valid options: none, jaeger"
            )),
        }
    }
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to resolve address: {}", address))]
    ResolutionError { address: String },

    #[snafu(context(false))]
    IOError { source: std::io::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

fn jaeger_exporter(config: &TracingConfig) -> Result<Arc<AsyncExporter>> {
    let agent_endpoint = format!(
        "{}:{}",
        config.traces_exporter_jaeger_agent_host.trim(),
        config.traces_exporter_jaeger_agent_port
    );

    let service_name = &config.traces_exporter_jaeger_service_name;
    let mut jaeger = JaegerAgentExporter::new(
        service_name.clone(),
        agent_endpoint,
        Arc::new(SystemProvider::new()),
        config.traces_jaeger_max_msgs_per_second,
    )?;

    // Use any specified static span tags.
    let mut tags = config
        .traces_jaeger_tags
        .as_ref()
        .cloned()
        .unwrap_or_default();

    // add hostname
    const TAG_HOSTNAME: &str = "hostname";
    if !tags.iter().any(|t| t.key() == TAG_HOSTNAME) {
        if let Ok(hostname) = std::env::var("HOSTNAME") {
            tags.push(JaegerTag::new(TAG_HOSTNAME, hostname));
        }
    }

    // commit tags
    if !tags.is_empty() {
        jaeger = jaeger.with_tags(&tags);
    }

    Ok(Arc::new(AsyncExporter::new(jaeger)))
}
