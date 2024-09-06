//! Cache System.
//!
//! # Design
//! There are the following components:
//!
//! - [`Cache`]: The actual cache that maps keys to [shared futures] that return results.
//! - [`Hook`]: Can react to state changes of the cache state. Implements logging but also size limitations.
//! - [`Reactor`]: Drives pruning / garbage-collection decisions of the [`Cache`]. Implemented as background task so
//!       users don't need to drive that themselves.
//!
//! ```text
//!    +-------+                         +------+
//!    | Cache |----(informs & asks)---->| Hook |
//!    +-------+                         +------+
//!        ^                                |
//!        |                                |
//!        |                                |
//! (drives pruning)                        |
//!        |                                |
//!        |                                |
//!        |                                |
//!   +---------+                           |
//!   | Reactor |<-------(informs)----------+
//!   +---------+
//! ```
//!
//!
//! [`Cache`]: self::cache::Cache
//! [`Hook`]: self::hook::Hook
//! [`Reactor`]: self::reactor::Reactor
//! [shared futures]: futures::future::Shared
pub mod cache;
pub mod hook;
pub mod interfaces;
pub mod reactor;
pub mod utils;

#[cfg(test)]
pub(crate) mod test_utils;
