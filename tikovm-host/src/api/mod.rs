//! Control API (design §10). Generic only — no PG-specific routes.
//!
//! Workload-specific endpoints are exposed via the guest proxy
//! (`ANY /vms/{id}/guest/{path}`), which tunnels to the guest agent. Routing of
//! the dispatch logic is separated from the TCP server so it can be unit-tested
//! with a [`crate::vmm::mock::MockVmm`]-backed [`crate::node::Node`].

pub mod server;

pub use server::{ApiServer, Response, dispatch};
