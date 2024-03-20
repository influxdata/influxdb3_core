//! Config for the tokio main (= IO) runtime.

use std::{
    num::{NonZeroU32, NonZeroUsize},
    time::Duration,
};

/// Tokio runtime type.
#[derive(Debug, Clone, Copy, Default, clap::ValueEnum)]
pub enum TokioRuntimeType {
    /// Current-thread runtime.
    CurrentThread,

    /// Multi-thread runtime.
    #[default]
    MultiThread,

    /// New, alternative multi-thread runtime.
    ///
    /// Requires `tokio_unstable` combile-time flag.
    MultiThreadAlt,
}

/// CLI config for tokio main (= IO) runtime.
#[derive(Debug, Clone, clap::Parser)]
#[allow(missing_copy_implementations)]
pub struct TokioMainConfig {
    /// Set the maximum number of main/IO runtime threads to use.
    ///
    /// Defaults to the number of logical cores on the system.
    #[clap(long = "num-threads", env = "INFLUXDB_IOX_NUM_THREADS", action)]
    pub num_threads: Option<NonZeroUsize>,

    /// Main tokio runtime type.
    #[clap(
        value_enum,
        long = "io-runtime-type",
        env = "INFLUXDB_IOX_IO_RUNTIME_TYPE",
        default_value_t = TokioRuntimeType::default(),
        action
    )]
    pub runtime_type: TokioRuntimeType,

    /// Disable LIFO slot of IO runtime.
    ///
    /// Requires `tokio_unstable` combile-time flag.
    #[clap(
        long = "io-runtime-disable-lifo-slot",
        env = "INFLUXDB_IOX_IO_RUNTIME_DISABLE_LIFO_SLOT",
        action
    )]
    pub disable_lifo: Option<bool>,

    /// Sets the number of scheduler ticks after which the scheduler will poll for
    /// external events (timers, I/O, and so on).
    #[clap(
        long = "io-runtime-event-interval",
        env = "INFLUXDB_IOX_IO_RUNTIME_EVENT_INTERVAL",
        action
    )]
    pub event_interval: Option<NonZeroU32>,

    /// Sets the number of scheduler ticks after which the scheduler will poll the global
    /// task queue.
    #[clap(
        long = "io-runtime-global-queue-interval",
        env = "INFLUXDB_IOX_IO_RUNTIME_GLOBAL_QUEUE_INTERVAL",
        action
    )]
    pub global_queue_interval: Option<NonZeroU32>,

    /// Specifies the limit for additional threads spawned by the Runtime.
    #[clap(
        long = "io-runtime-max-blocking-threads",
        env = "INFLUXDB_IOX_IO_RUNTIME_MAX_BLOCKING_THREADS",
        action
    )]
    pub max_blocking_threads: Option<NonZeroUsize>,

    /// Enables the I/O driver and configures the max number of events to be
    /// processed per tick.
    #[clap(
        long = "io-runtime-max-io-events-per-tick",
        env = "INFLUXDB_IOX_IO_RUNTIME_MAX_IO_EVENTS_PER_TICK",
        action
    )]
    pub max_io_events_per_tick: Option<NonZeroUsize>,

    /// Sets a custom timeout for a thread in the blocking pool.
    #[clap(long="io-runtime-thread-keep-alive", env = "INFLUXDB_IOX_IO_RUNTIME_THREAD_KEEP_ALIVE", value_parser = humantime::parse_duration)]
    pub thread_keep_alive: Option<Duration>,
}
