//! Shared contract between [`tikovm-host`] and [`tikovm-guest`].
//!
//! This crate is deliberately lightweight (only `serde`, `serde_json`,
//! `thiserror`) and contains no async runtime: host and guest bring their own.
//! It defines the [`WorkloadManifest`](manifest::WorkloadManifest) schema, the
//! VM lifecycle types ([`VmState`](vm::VmState)), the vsock RPC message enums,
//! the routing/volume config, and a length-delimited JSON framing codec.
//!
//! See `docs/tikovm-design.md` for the full design.

pub mod codec;
pub mod error;
pub mod manifest;
pub mod routing;
pub mod rpc;
pub mod vm;
pub mod volume;

pub use error::ProtocolError;
