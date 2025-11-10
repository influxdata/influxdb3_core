//! Crate holding an `authz::Authorizer` implementation for end-to-end tests

// Workaround for "unused crate" lint false positives.


mod authz;
pub use authz::*;
