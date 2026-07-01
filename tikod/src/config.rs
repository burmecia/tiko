//! Configuration for `tikod`.

use std::path::PathBuf;

use crate::control::IdlePolicy;
use crate::proxy::ProxyConfig;

/// Top-level configuration for the `tikod` process.
#[derive(Debug, Clone)]
pub struct TikodConfig {
    /// Directory for VM snapshots and runtime artifacts.
    pub data_dir: PathBuf,
    /// Proxy (client-facing) configuration.
    pub proxy: ProxyConfig,
    /// Idle (auto-pause) policy.
    pub idle_policy: IdlePolicy,
}

impl Default for TikodConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("/tmp/tikod"),
            proxy: ProxyConfig::default(),
            idle_policy: IdlePolicy::default(),
        }
    }
}
