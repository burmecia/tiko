//! Volume declarations for the 2-tier storage model (see design §9).
//!
//! Volumes are declared in the rootfs manifest and read by the host at
//! provision time (the only manifest sections the host reads are `volumes`
//! and `schedule`). Both tiers are optional.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Storage tier for a declared volume.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VolumeTier {
    /// Per-VM ext4 image on host-local disk, attached as virtio-block.
    /// Fast, capped size, survives suspend. Lifetime across destroy is
    /// governed by [`VolumeDecl::persist_key`]: with a key the image lives in
    /// a shared local-fast store and **persists across destroy** (a later VM
    /// provisioned with the same key reattaches the same data); without one
    /// it is per-VM and **ephemeral on destroy**.
    #[default]
    LocalFast,
    /// ext4 image on a host-mounted remote FS (e.g. S3 Files via NFS), attached
    /// as virtio-block. Slow, **persists across destroy**, shared-capable.
    /// Firecracker has no virtio-fs, so the host owns the remote mount and the
    /// guest sees only a labeled block device.
    RemoteSlow,
}

impl std::fmt::Display for VolumeTier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VolumeTier::LocalFast => write!(f, "local_fast"),
            VolumeTier::RemoteSlow => write!(f, "remote_slow"),
        }
    }
}

/// A volume declared by a workload in its rootfs manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeDecl {
    /// Identifier for this volume (e.g. "data", "cache", "archive").
    pub name: String,
    /// Which tier this volume lives on.
    pub tier: VolumeTier,
    /// Where the guest mounts the volume (e.g. "/mnt/data").
    pub mount_path: PathBuf,
    /// Size cap in MiB for the ext4 image (sparse). Applies to both tiers:
    /// `local_fast` is capped by host disk; `remote_slow` is sparse on the
    /// remote mount (the backend capacity itself is effectively unlimited).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_mb: Option<u64>,
    /// Read-only mount.
    #[serde(default)]
    pub read_only: bool,
    /// `remote_slow` only: host path where the remote FS is mounted (the image
    /// is placed under `<source>/<vm_id>/<name>.ext4`). Ignored for `local_fast`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    /// `local_fast` only: stable identity that makes the volume **persist
    /// across VM destroy**. The host stores the image in a shared local-fast
    /// store keyed by this value instead of under the per-VM dir, so a later
    /// VM provisioned with the same key reattaches the same data (e.g. PGDATA
    /// + local cache for a serverless-Postgres endpoint). The key is
    /// operator-supplied (typically a tenant/endpoint id) because `vm_id` is
    /// ephemeral. When `None`, the volume is per-VM and deleted on destroy.
    /// Attaching the same key to two live VMs concurrently is a caller error
    /// (shared ext4 images are single-attach).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub persist_key: Option<String>,
}

impl VolumeDecl {
    pub fn local(name: impl Into<String>, mount_path: impl Into<PathBuf>, size_mb: u64) -> Self {
        Self {
            name: name.into(),
            tier: VolumeTier::LocalFast,
            mount_path: mount_path.into(),
            size_mb: Some(size_mb),
            read_only: false,
            source: None,
            persist_key: None,
        }
    }

    pub fn remote(
        name: impl Into<String>,
        mount_path: impl Into<PathBuf>,
        source: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            tier: VolumeTier::RemoteSlow,
            mount_path: mount_path.into(),
            size_mb: None,
            read_only: false,
            source: Some(source.into()),
            persist_key: None,
        }
    }
}
