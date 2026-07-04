//! `pgctl` — guest-side Postgres control agent.
//!
//! Library facade exposing the [`pgops`] (Postgres control) and [`server`]
//! (HTTP API) modules. The `pgctl` binary in `src/main.rs` wires these together.

pub mod pgops;
pub mod server;
