//! tikovm guest library.
//!
//! The generic in-VM agent: reads the `WorkloadManifest`, supervises the
//! workload process, owns idle detection, and coordinates suspend/restore.
//!
//! See `docs/tikovm-design.md`. Modules land as the implementation progresses.

pub mod idle;
pub mod manifest;
pub mod supervisor;
