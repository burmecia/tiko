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
    /// Storage provisioning (design §9): which `remote_slow` backing to use.
    #[serde(default)]
    pub storage: StorageConfig,
}

/// Storage provisioning configuration (`[storage]` section).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// `remote_slow` backing: "s3files_image" (ext4 image on a host-mounted
    /// remote FS — the legacy default) or "ublk" (tikoblkd chunk volumes).
    #[serde(default = "default_remote_slow_backing")]
    pub remote_slow_backing: String,
    /// tikoblkd control socket (ublk backing only).
    #[serde(default = "default_ublk_sock")]
    pub ublk_sock: PathBuf,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            remote_slow_backing: default_remote_slow_backing(),
            ublk_sock: default_ublk_sock(),
        }
    }
}

fn default_remote_slow_backing() -> String {
    "s3files_image".into()
}

fn default_ublk_sock() -> PathBuf {
    PathBuf::from("/run/tikoblk/daemon.sock")
}

impl StorageConfig {
    /// Parse/validate the backing name.
    pub fn backing(&self) -> Result<RemoteSlowBacking, ConfigError> {
        match self.remote_slow_backing.as_str() {
            "s3files_image" => Ok(RemoteSlowBacking::S3FilesImage),
            "ublk" => Ok(RemoteSlowBacking::Ublk),
            other => Err(ConfigError::Parse(format!(
                "unknown storage.remote_slow_backing {other:?} (want \"s3files_image\" or \"ublk\")"
            ))),
        }
    }
}

/// Which `remote_slow` backing is active.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteSlowBacking {
    /// ext4 image on a host-mounted remote FS (legacy).
    S3FilesImage,
    /// tikoblkd chunk volume on /dev/ublkbN.
    Ublk,
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
                storage: StorageConfig::default(),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_defaults_are_legacy() {
        let s = StorageConfig::default();
        assert_eq!(s.remote_slow_backing, "s3files_image");
        assert_eq!(s.ublk_sock, PathBuf::from("/run/tikoblk/daemon.sock"));
        assert_eq!(s.backing().unwrap(), RemoteSlowBacking::S3FilesImage);
    }

    #[test]
    fn storage_parses_from_toml_section() {
        let text = r#"
data_dir = "/x"
api_listen = "127.0.0.1:9000"

[storage]
remote_slow_backing = "ublk"
ublk_sock = "/run/tikoblk/other.sock"
"#;
        let cfg: HostConfig = toml::from_str(text).unwrap();
        assert_eq!(cfg.storage.remote_slow_backing, "ublk");
        assert_eq!(cfg.storage.ublk_sock, PathBuf::from("/run/tikoblk/other.sock"));
        assert_eq!(cfg.storage.backing().unwrap(), RemoteSlowBacking::Ublk);
    }

    #[test]
    fn storage_defaults_apply_when_section_absent() {
        let text = "data_dir = \"/x\"\napi_listen = \"127.0.0.1:9000\"\n";
        let cfg: HostConfig = toml::from_str(text).unwrap();
        assert_eq!(cfg.storage.backing().unwrap(), RemoteSlowBacking::S3FilesImage);
    }

    #[test]
    fn unknown_backing_rejected() {
        let s = StorageConfig {
            remote_slow_backing: "warp".into(),
            ..Default::default()
        };
        assert!(s.backing().is_err());
    }
}
