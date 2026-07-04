//! `tikod` — Tiko compute control plane.
//!
//! Single-binary process that combines:
//! - **proxy/** — PG wire-protocol proxy with wake-on-connect (scale-to-zero)
//! - **control/** — VM registry, idle policy, auto-pause enforcement
//! - **node/** — Firecracker/VZ lifecycle management, snapshot cache
//! - **vmm/** — VMM abstraction trait with platform-specific backends
//!
//! On macOS, uses Apple Virtualization Framework for development.
//! On Linux, uses Firecracker microVM for production.
//!
//! ```text
//! Client ──→ proxy ──→ control ──→ node ──→ Vmm trait ──→ backend
//!                                    │
//!                                    └── snapshot cache (local disk)
//! ```

pub mod api;
pub mod config;
pub mod control;
pub mod guestcontrol;
pub mod node;
pub mod proxy;
pub mod vmm;

pub use config::TikodConfig;
