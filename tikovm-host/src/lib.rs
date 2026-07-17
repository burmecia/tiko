//! tikovm host library.
//!
//! Generic microVM management: lifecycle (`vmm`/`node`), control registry,
//! networking, storage, proxy/router, scheduler, and the control API.
//!
//! See `docs/tikovm-design.md`. This crate is being built incrementally;
//! modules land as the implementation progresses.

pub mod api;
pub mod config;
pub mod control;
pub mod guestlink;
pub mod node;
pub mod proxy;
pub mod scheduler;
pub mod store;
pub mod vmm;
