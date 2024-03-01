//! Flexible and modular cache system.
#![warn(missing_docs)]
#![allow(unreachable_pub)]

// Workaround for "unused crate" lint false positives.
#[cfg(test)]
use criterion as _;
use workspace_hack as _;

pub mod addressable_heap;
pub mod backend;
pub mod cache;
mod cancellation_safe_future;
pub mod loader;
pub mod resource_consumption;
#[cfg(test)]
mod test_util;
