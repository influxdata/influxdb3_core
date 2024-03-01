#![warn(clippy::str_to_string, clippy::string_to_string)]

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

pub mod predicate;
