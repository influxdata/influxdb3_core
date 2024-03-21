//! `iox_http`
//!
//! Core crate for storing shared HTTP functionality for InfluxDB 3.0 API.

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

pub mod write;
