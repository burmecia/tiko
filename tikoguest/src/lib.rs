//! `tikoguest` — guest-side agent for tikod.
//!
//! Library facade exposing the [`pgops`] (Postgres control), [`server`]
//! (HTTP API), [`http`] (shared HTTP/1.1 primitives), and [`env`] (Tiko
//! identity) modules. The `tikoguest` binary in `src/main.rs` wires these
//! together.

pub mod env;
pub mod http;
pub mod observer;
pub mod pgmetrics;
pub mod pgops;
pub mod scaler;
pub mod server;
