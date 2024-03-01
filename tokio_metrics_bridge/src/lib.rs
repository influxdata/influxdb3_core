//! Integrates tokio runtime stats into the IOx metric system.
//!
//! This is NOT called `tokio-metrics` since this name is already taken.
#![warn(missing_docs)]

#[cfg(not(tokio_unstable))]
mod not_tokio_unstable {
    use metric as _;
    use parking_lot as _;
    use tokio as _;
    use workspace_hack as _;
}

#[cfg(tokio_unstable)]
mod bridge;
#[cfg(tokio_unstable)]
pub use bridge::*;
