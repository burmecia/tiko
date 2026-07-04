//! `tikoguest` — guest-side agent for tikod.
//!
//! Library facade exposing the [`pgops`] (Postgres control) and [`server`]
//! (HTTP API) modules. The `tikoguest` binary in `src/main.rs` wires these
//! together.

pub mod pgops;
pub mod server;
