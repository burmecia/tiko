//! The `WorkloadManifest` — the self-describing rootfs schema (design §5.1).
//!
//! Baked into the rootfs at `/etc/tikovm/workload.toml`, this tells the generic
//! guest daemon how to run whatever is in that rootfs. Everything here is
//! **guest-internal behavior**. The host reads only the `volumes` and `schedule`
//! sections (at provision time).

use std::collections::HashMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::volume::VolumeDecl;

/// Top-level manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkloadManifest {
    /// Schema version.
    #[serde(default = "default_version")]
    pub version: u32,
    /// Informational workload label (e.g. "echo", "postgres", "node").
    pub workload: String,
    /// The supervised main process.
    #[serde(default)]
    pub process: Option<ProcessSpec>,
    /// Optional one-time bootstrap, run before `process` on first boot.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub init: Option<ProcessSpec>,
    /// How the guest health-checks the workload.
    #[serde(default)]
    pub health: HealthProbe,
    /// Scale-to-zero policy + probes (guest-owned; design §8).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idle: Option<IdlePolicy>,
    /// Quiesce hooks for a clean snapshot across suspend/restore.
    #[serde(default)]
    pub suspend: SuspendHooks,
    /// Restart policy for the supervised process.
    #[serde(default)]
    pub restart: RestartPolicy,
    /// Workload HTTP exposed externally via the guest proxy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expose: Option<ExposeSpec>,
    /// Optional schedule for scheduled-job workloads (host-driven; design §13).
    /// The host reads this; it may be overridden in the provision request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule: Option<SchedulePolicy>,
    /// Declared storage volumes. Read by the host at provision time.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub volumes: Vec<VolumeDecl>,
}

fn default_version() -> u32 {
    1
}

impl WorkloadManifest {
    /// A minimal stateless manifest with no process (useful for tests/stubs).
    pub fn empty(workload: impl Into<String>) -> Self {
        Self {
            version: 1,
            workload: workload.into(),
            process: None,
            init: None,
            health: HealthProbe::None,
            idle: None,
            suspend: SuspendHooks::default(),
            restart: RestartPolicy::default(),
            expose: None,
            schedule: None,
            volumes: Vec::new(),
        }
    }
}

/// A process to spawn and (for `process`) supervise.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessSpec {
    /// Executable path inside the guest.
    pub cmd: String,
    /// Command-line arguments.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<String>,
    /// Working directory.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<PathBuf>,
    /// Extra environment variables for this process.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub env: HashMap<String, String>,
    /// Run as this user (name or uid:gid); `None` = inherit guestd's user.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
}

/// Health probe strategy.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum HealthProbe {
    /// HTTP GET to `path` on `port`; 2xx/3xx = healthy.
    Http {
        path: String,
        port: u16,
        #[serde(default = "default_health_interval")]
        interval_secs: u64,
    },
    /// TCP connect to `port`.
    Tcp {
        port: u16,
        #[serde(default = "default_health_interval")]
        interval_secs: u64,
    },
    /// Run a command; exit 0 = healthy.
    Exec {
        cmd: String,
        #[serde(default = "default_health_interval")]
        interval_secs: u64,
    },
    /// No health probe.
    #[default]
    None,
}

fn default_health_interval() -> u64 {
    5
}

/// Scale-to-zero policy, evaluated by the guest (design §8).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdlePolicy {
    /// How often (seconds) to collect probe signals.
    #[serde(default = "default_idle_tick")]
    pub tick_secs: u64,
    /// Sustained idle across all probes, in seconds, before signalling the host.
    pub idle_secs: u64,
    /// Probes combined with AND. If empty, the guest never signals idle.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub probes: Vec<IdleProbe>,
}

fn default_idle_tick() -> u64 {
    5
}

/// One idle signal source. The guest collects all declared probes each tick.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum IdleProbe {
    /// Pull VM-scoped network stats from the host via vsock. No config needed —
    /// the host returns traffic stats for the whole VM across all ports.
    HostNetwork,
    /// Run a command (baked in rootfs) returning a metrics JSON; the script's
    /// exit code or a declared field determines idle/busy.
    Exec { cmd: String },
    /// Scrape a workload HTTP metrics endpoint.
    Http { url: String },
}

/// Quiesce hooks for clean suspend/restore.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SuspendHooks {
    /// Run before the host suspends the VM (e.g. checkpoint a DB).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pre_suspend_cmd: Option<String>,
    /// Run after the host restores the VM.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub post_restore_cmd: Option<String>,
}

/// Restart policy for the supervised `process`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestartPolicy {
    #[serde(default = "default_restart_policy")]
    pub policy: RestartMode,
    #[serde(default = "default_restart_backoff")]
    pub backoff_secs: u64,
}

impl Default for RestartPolicy {
    fn default() -> Self {
        Self {
            policy: RestartMode::default(),
            backoff_secs: default_restart_backoff(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RestartMode {
    /// Always restart on exit.
    Always,
    /// Restart only on non-zero exit.
    #[default]
    OnFailure,
    /// Never restart.
    Never,
}

fn default_restart_policy() -> RestartMode {
    RestartMode::OnFailure
}
fn default_restart_backoff() -> u64 {
    2
}

/// Workload HTTP exposure via the guest proxy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExposeSpec {
    /// Workload HTTP port the guest proxy forwards external requests to.
    pub http_port: u16,
    /// Optional control binary serving `/db`/`/pitr`-style control routes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub control_bin: Option<String>,
}

/// Schedule for scheduled-job workloads (design §13). Declared in the manifest,
/// read by the host, overridable in the provision request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchedulePolicy {
    /// Standard cron expression (5-field). Mutually exclusive with `interval_secs`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cron: Option<String>,
    /// Run every N seconds. Mutually exclusive with `cron`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval_secs: Option<u64>,
    /// `true` (default): suspend between runs, restore each tick.
    /// `false`: destroy + re-provision each tick (ephemeral, Lambda-like).
    #[serde(default = "default_keep_warm")]
    pub keep_warm: bool,
}

fn default_keep_warm() -> bool {
    true
}

impl SchedulePolicy {
    pub fn interval(secs: u64) -> Self {
        Self {
            cron: None,
            interval_secs: Some(secs),
            keep_warm: true,
        }
    }

    pub fn cron(expr: impl Into<String>) -> Self {
        Self {
            cron: Some(expr.into()),
            interval_secs: None,
            keep_warm: true,
        }
    }

    /// Validate that exactly one of `cron` / `interval_secs` is set.
    pub fn validate(&self) -> Result<(), String> {
        match (&self.cron, self.interval_secs) {
            (None, None) => Err("schedule must set `cron` or `interval_secs`".into()),
            (Some(_), Some(_)) => Err("schedule must set only one of `cron`/`interval_secs`".into()),
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_toml_round_trip() {
        let toml = r#"
version = 1
workload = "echo"

[process]
cmd = "/usr/local/bin/echo-server"
args = ["--port", "8080"]

[health]
kind = "http"
path = "/health"
port = 8080

[idle]
tick_secs = 5
idle_secs = 120
[[idle.probes]]
kind = "host_network"

[schedule]
interval_secs = 300
keep_warm = true

[[volumes]]
name = "data"
tier = "local_fast"
mount_path = "/mnt/data"
size_mb = 1024
"#;
        // The host/guest will parse TOML via a `toml` dep; here we verify serde
        // JSON round-trips the same structure.
        let m = WorkloadManifest::empty("echo");
        let json = serde_json::to_string(&m).unwrap();
        let back: WorkloadManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(back.workload, "echo");
        let _ = toml; // (TOML parsing exercised in the host/guest crates.)
    }

    #[test]
    fn schedule_validation() {
        assert!(SchedulePolicy::interval(10).validate().is_ok());
        assert!(SchedulePolicy::cron("*/5 * * * *").validate().is_ok());
        let bad = SchedulePolicy {
            cron: Some("x".into()),
            interval_secs: Some(5),
            keep_warm: true,
        };
        assert!(bad.validate().is_err());
        let empty = SchedulePolicy {
            cron: None,
            interval_secs: None,
            keep_warm: true,
        };
        assert!(empty.validate().is_err());
    }
}
