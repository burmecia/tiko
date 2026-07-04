//! Per-VM Tiko identity + agent identity.
//!
//! [`load_tiko_env`] resolves the env vars that every spawned `pg_ctl` /
//! `initdb` (and future in-guest service) must inherit so the in-guest
//! tikoworker extension sees the correct org/db/project + storage roots.
//!
//! Precedence for each var: **inherited process env > `tiko.env` file value >
//! default**. Inherited-wins lets the systemd unit (or
//! `TIKO_DB_ID=7 tikoguest`) override the file; otherwise the per-VM file
//! (written by `start_vm.sh`) is the source of truth; otherwise the
//! `tiko_env.sh` defaults apply.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use tracing::{debug, warn};

/// The Tiko identity keys managed by this module.
pub const TIKO_KEYS: &[&str] = &[
    "TIKO_ORG_ID",
    "TIKO_DB_ID",
    "TIKO_PROJECT_ID",
    "TIKO_STORAGE_ROOT",
    "TIKO_LOCAL_PATH",
];

/// Resolve the per-VM Tiko identity env map, mirroring `tiko_env.sh`.
///
/// See the module docs for precedence. The result is attached to every spawned
/// `pg_ctl` / `initdb` so the in-guest tikoworker extension reads the correct
/// org/db/project + storage roots.
pub fn load_tiko_env(tiko_env_path: &Path, data_dir: &Path) -> HashMap<String, String> {
    let file_values = parse_env_file(tiko_env_path);
    // PGHOME = the data dir's parent (matches tiko_env.sh's PGHOME).
    let pg_home = data_dir
        .parent()
        .unwrap_or_else(|| Path::new("/var/lib/postgresql"));
    let defaults: [(&str, String); 5] = [
        ("TIKO_ORG_ID", "12".into()),
        ("TIKO_DB_ID", "34".into()),
        ("TIKO_PROJECT_ID", "56".into()),
        (
            "TIKO_STORAGE_ROOT",
            "/mnt/s3files/tiko_root".into(), // tiko_env.sh: $S3FILES/tiko_root
        ),
        (
            "TIKO_LOCAL_PATH",
            pg_home.join("tiko_local").to_string_lossy().into_owned(),
        ),
    ];

    let mut out = HashMap::new();
    for (key, default) in defaults {
        let val = std::env::var(key)
            .ok()
            .filter(|v| !v.is_empty())
            .or_else(|| file_values.get(key).cloned())
            .unwrap_or(default);
        out.insert(key.to_string(), val);
    }
    // Also forward any *other* keys present in the file (forward-compat).
    // The file is tiko.env — all keys in it are Tiko-related, so we don't
    // filter by prefix. This catches e.g. TIKOD_ADDR (starts with "TIKOD_",
    // not "TIKO_").
    for (k, v) in file_values {
        if !out.contains_key(&k) {
            out.insert(k, v);
        }
    }
    if !tiko_env_path.exists() {
        debug!(path = ?tiko_env_path, "tiko.env absent; using identity defaults");
    } else {
        debug!(path = ?tiko_env_path, vars = ?out, "loaded tiko identity env");
    }
    out
}

/// Default path for the per-VM `tiko.env` file: `<data_dir_parent>/tiko.env`.
pub fn default_tiko_env_path(data_dir: &Path) -> PathBuf {
    data_dir
        .parent()
        .map(|p| p.join("tiko.env"))
        .unwrap_or_else(|| PathBuf::from("tiko.env"))
}

/// Look up a var with inherited-env > tiko-env-map > None precedence. For keys
/// not in the defaults list (e.g. `TIKO_VM_ID`, `TIKOD_ADDR`) — the map is the
/// result of [`load_tiko_env`], which already includes file-forwarded `TIKO_*`
/// keys. Returns `None` if the var is absent or empty in both sources.
pub fn lookup_optional(map: &HashMap<String, String>, key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .filter(|v| !v.is_empty())
        .or_else(|| map.get(key).cloned())
        .filter(|v| !v.is_empty())
}

/// Parse a `KEY=VALUE` env file (the format `start_vm.sh` writes for
/// `tiko.env`). Blank lines and `#` comments are skipped. Errors are logged and
/// yield an empty map rather than failing startup.
fn parse_env_file(path: &Path) -> HashMap<String, String> {
    let mut out = HashMap::new();
    let Ok(text) = std::fs::read_to_string(path) else {
        return out;
    };
    for (i, raw) in text.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((k, v)) = line.split_once('=') else {
            warn!(path = ?path, line = i + 1, "skipping malformed env line");
            continue;
        };
        let k = k.trim();
        let mut v = v.trim().to_string();
        if v.len() >= 2
            && ((v.starts_with('"') && v.ends_with('"'))
                || (v.starts_with('\'') && v.ends_with('\'')))
        {
            v = v[1..v.len() - 1].to_string();
        }
        if !k.is_empty() {
            out.insert(k.to_string(), v);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_env_file_basic() {
        let dir = std::env::temp_dir().join("tikoguest-env-test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("test.env");
        std::fs::write(&path, "# comment\nFOO=bar\nBAZ=\"quoted\"\n\n").unwrap();
        let map = parse_env_file(&path);
        assert_eq!(map.get("FOO").unwrap(), "bar");
        assert_eq!(map.get("BAZ").unwrap(), "quoted");
    }

    #[test]
    fn load_tiko_env_file_value_loaded() {
        let dir = std::env::temp_dir().join("tikoguest-env-test2");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("tiko.env");
        std::fs::write(&path, "TIKO_PROJECT_ID=99\n").unwrap();
        let data_dir = dir.join("data");
        std::fs::create_dir_all(&data_dir).unwrap();
        let map = load_tiko_env(&path, &data_dir);
        // If TIKO_PROJECT_ID is set in the process env, inherited wins; else the file value.
        let expected = std::env::var("TIKO_PROJECT_ID").unwrap_or_else(|_| "99".into());
        assert_eq!(map.get("TIKO_PROJECT_ID").unwrap(), &expected);
    }

    #[test]
    fn load_tiko_env_defaults_when_absent() {
        let dir = std::env::temp_dir().join("tikoguest-env-test3");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("nonexistent.env");
        let data_dir = dir.join("data");
        std::fs::create_dir_all(&data_dir).unwrap();
        let map = load_tiko_env(&path, &data_dir);
        // Defaults are used when the file doesn't exist (unless env overrides).
        assert!(map.contains_key("TIKO_ORG_ID"));
        assert!(map.contains_key("TIKO_DB_ID"));
    }
}
