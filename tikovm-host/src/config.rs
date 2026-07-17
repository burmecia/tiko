//! Host configuration (design §12). Loaded from a TOML file, with CLI flags
//! overriding file values. Replaces today's CLI-args-only model.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

/// Host daemon configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HostConfig {
    /// Data directory: snapshots, per-VM overlays, state DB, runtime artifacts.
    pub data_dir: PathBuf,
    /// Control API listen address.
    pub api_listen: String,
    /// Directory containing shared assets (kernel, base rootfs images).
    #[serde(default)]
    pub assets_dir: PathBuf,
    /// First guest CID to allocate from (each VM gets a unique vsock CID).
    #[serde(default = "default_base_cid")]
    pub vsock_base_cid: u32,
}

fn default_base_cid() -> u32 {
    3
}

impl HostConfig {
    /// Build config: start from file (if given), then override with the explicit
    /// `data_dir` / `api_listen` arguments.
    pub fn load(
        file: Option<&Path>,
        data_dir: &Path,
        api_listen: &str,
    ) -> Result<Self, ConfigError> {
        let mut cfg = if let Some(p) = file {
            let text = std::fs::read_to_string(p)
                .map_err(|e| ConfigError::Read(p.to_path_buf(), e.to_string()))?;
            toml::from_str(&text).map_err(|e| ConfigError::Parse(e.to_string()))?
        } else {
            HostConfig {
                data_dir: data_dir.to_path_buf(),
                api_listen: api_listen.to_string(),
                assets_dir: data_dir.join("assets"),
                vsock_base_cid: default_base_cid(),
            }
        };
        cfg.data_dir = data_dir.to_path_buf();
        cfg.api_listen = api_listen.to_string();
        if cfg.assets_dir.as_os_str().is_empty() {
            cfg.assets_dir = cfg.data_dir.join("assets");
        }
        Ok(cfg)
    }

    pub fn snapshots_dir(&self) -> PathBuf {
        self.data_dir.join("snapshots")
    }

    pub fn state_db_path(&self) -> PathBuf {
        self.data_dir.join("tikovm.db")
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to read config file {0}: {1}")]
    Read(PathBuf, String),
    #[error("failed to parse config: {0}")]
    Parse(String),
}
