//! HTTP control API for VM lifecycle management.
//!
//! Exposes the [`Vmm`](crate::vmm::Vmm) trait and [`Node`](crate::node::Node)
//! orchestration over a Firecracker-style REST API. Like the Firecracker backend
//! client (`vmm::firecracker`), this uses raw HTTP/1.1 — no external HTTP
//! library is needed.
//!
//! ```text
//! Client ──HTTP/JSON──→ ApiServer ──→ Node / Control ──→ Vmm trait ──→ backend
//! ```
//!
//! The server side ([`server::ApiServer`]) shares the same `Node`/`Control`
//! used by the PG proxy. The client side ([`client::ApiClient`]) mirrors the
//! `Vmm` trait so existing lifecycle code can be switched from a direct backend
//! to HTTP with minimal changes.

pub mod client;
pub mod server;

pub use client::ApiClient;
pub use server::ApiServer;
