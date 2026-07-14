//! Postgres control operations — thin wrappers around `pg_ctl` and the
//! `postgresql.tiko.conf` override file.
//!
//! The agent runs as the `postgres` user inside the guest, so `pg_ctl` works
//! directly (no sudo). Every path is overridable for testing (point `pg_ctl`
//! at a fake script and `data_dir` at a temp dir).

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};

use tokio::process::Command;
use tracing::{debug, info, warn};

/// Errors from a Postgres control operation.
#[derive(Debug, thiserror::Error)]
pub enum PgCtlError {
    /// `pg_ctl` exited non-zero (or with an unexpected status). `stderr` is
    /// captured for the HTTP error body so callers can diagnose.
    #[error("pg_ctl {cmd} failed (exit {code:?}): {stderr}")]
    CommandFailed {
        cmd: String,
        code: Option<i32>,
        stderr: String,
    },
    /// `initdb` exited non-zero. Kept separate so the server can report which
    /// stage failed.
    #[error("initdb failed (exit {code:?}): {stderr}")]
    InitdbFailed { code: Option<i32>, stderr: String },
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("config file is invalid: {0}")]
    ConfigParse(String),
    /// The data directory doesn't look initialized (no `PG_VERSION`).
    #[error("data directory not initialized: {0}")]
    NotInitialized(PathBuf),
    /// `init` was called on an existing cluster without `force`.
    #[error("data directory already initialized: {0} (pass force=true to wipe and re-init)")]
    AlreadyInitialized(PathBuf),
    /// `init` was called while Postgres is still running.
    #[error("cannot init: postgres is still running (stop it first)")]
    StillRunning,
}

pub type PgCtlResult<T> = Result<T, PgCtlError>;

/// How to shut down Postgres. Mirrors `pg_ctl -m` modes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StopMode {
    /// Wait for clients to disconnect and online checkpoints to finish.
    Smart,
    /// Active transactions are rolled back; existing connections are terminated
    /// cleanly. The default — fast but safe.
    #[default]
    Fast,
    /// Abort everything immediately; recovery runs on next start.
    Immediate,
}

impl StopMode {
    fn as_arg(self) -> &'static str {
        match self {
            Self::Smart => "smart",
            Self::Fast => "fast",
            Self::Immediate => "immediate",
        }
    }
}

/// Handles for the `pg_ctl` binary, data directory, log, and override config.
#[derive(Debug, Clone)]
pub struct PgCtl {
    /// `pg_ctl` executable (resolved from PATH by default; overridable).
    pub pg_ctl: PathBuf,
    /// `initdb` executable (resolved from PATH by default; overridable).
    pub initdb: PathBuf,
    /// `PGDATA` (e.g. `/var/lib/postgresql/tt`).
    pub data_dir: PathBuf,
    /// stdout/stderr capture file passed via `pg_ctl -l` (start/restart).
    pub log_path: PathBuf,
    /// The include-file written by tikod overrides (e.g.
    /// `postgresql.tiko.conf` inside the data dir).
    pub config_file: PathBuf,
    /// Source override-config template (e.g.
    /// `/var/lib/postgresql/postgresql.tiko.conf`) copied into a freshly
    /// initialized data dir during [`init`](Self::init). Defaults to the data
    /// dir's parent, matching `create_rootfs.sh`.
    pub config_template: PathBuf,
    /// CIDR granted `trust` auth in `pg_hba.conf` during `init`. Covers the
    /// per-VM subnets (172.16.N.0/24) by default.
    pub trust_cidr: String,
    /// Resolved Tiko identity env vars (TIKO_ORG_ID / TIKO_DB_ID /
    /// TIKO_PROJECT_ID / TIKO_STORAGE_ROOT / TIKO_LOCAL_PATH) loaded from
    /// `tiko.env` and passed through to every spawned `pg_ctl` / `initdb` so
    /// the in-guest tikoworker extension sees the right per-VM identity. See
    /// [`load_tiko_env`].
    pub tiko_env: HashMap<String, String>,
}

impl PgCtl {
    pub fn new(
        pg_ctl: PathBuf,
        data_dir: PathBuf,
        log_path: PathBuf,
        config_file: PathBuf,
    ) -> Self {
        // The override template ships in PGDATA's parent (PGHOME) per
        // create_rootfs.sh; fall back to the config file path if there's no
        // parent (e.g. a relative data dir in tests).
        let config_template = data_dir
            .parent()
            .map(|p| p.join("postgresql.tiko.conf"))
            .unwrap_or_else(|| config_file.clone());
        // The per-VM identity file (written by start_vm.sh) lives in PGHOME.
        let tiko_env_path = crate::env::default_tiko_env_path(&data_dir);
        let tiko_env = crate::env::load_tiko_env(&tiko_env_path, &data_dir);
        Self {
            pg_ctl,
            initdb: PathBuf::from("initdb"),
            data_dir,
            log_path,
            config_file,
            config_template,
            trust_cidr: "172.16.0.0/16".into(),
            tiko_env,
        }
    }

    /// Override the `initdb` binary path.
    pub fn with_initdb(mut self, initdb: impl Into<PathBuf>) -> Self {
        self.initdb = initdb.into();
        self
    }

    /// Override the override-config template path copied during `init`.
    pub fn with_config_template(mut self, template: impl Into<PathBuf>) -> Self {
        self.config_template = template.into();
        self
    }

    /// Override the `trust` CIDR appended to `pg_hba.conf` during `init`.
    pub fn with_trust_cidr(mut self, cidr: impl Into<String>) -> Self {
        self.trust_cidr = cidr.into();
        self
    }

    /// Reload Tiko identity env vars from a non-default `tiko.env` path. Use
    /// when `--tiko-env` points somewhere other than `<data_dir_parent>/tiko.env.
    pub fn with_tiko_env_path(mut self, path: impl AsRef<Path>) -> Self {
        self.tiko_env = crate::env::load_tiko_env(path.as_ref(), &self.data_dir);
        self
    }

    /// The resolved per-VM identity env vars that will be inherited by postgres.
    pub fn tiko_env(&self) -> &HashMap<String, String> {
        &self.tiko_env
    }

    /// Run `pg_ctl` with the given args. Returns captured stderr on failure.
    /// `description` labels the command in error messages and logs.
    async fn run(&self, args: &[&str], description: &str) -> PgCtlResult<String> {
        debug!(cmd = ?self.pg_ctl, args = ?args, "{description}");
        let output = Command::new(&self.pg_ctl)
            .args(args)
            .envs(&self.tiko_env)
            .output()
            .await
            .map_err(|e| PgCtlError::CommandFailed {
                cmd: description.to_string(),
                code: None,
                stderr: format!("spawn failed: {e}"),
            })?;

        let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
        if !output.status.success() {
            return Err(PgCtlError::CommandFailed {
                cmd: description.to_string(),
                code: output.status.code(),
                stderr,
            });
        }
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    }

    /// True if the data directory looks initialized (`PG_VERSION` present).
    pub fn is_initialized(&self) -> bool {
        self.data_dir.join("PG_VERSION").exists()
    }

    /// Postgres version string from `PG_VERSION` (e.g. `"17.x"`), if present.
    pub fn version(&self) -> Option<String> {
        let path = self.data_dir.join("PG_VERSION");
        let content = std::fs::read_to_string(&path).ok()?;
        // PG_VERSION's first non-empty line is the major.minor version.
        content
            .lines()
            .next()
            .map(|l| l.trim().to_string())
            .filter(|s| !s.is_empty())
    }

    /// Read the postmaster PID from `postmaster.pid` if the server is running.
    pub fn pid(&self) -> Option<i32> {
        let content = std::fs::read_to_string(self.data_dir.join("postmaster.pid")).ok()?;
        content.lines().next()?.trim().parse().ok()
    }

    /// `pg_ctl status`: whether the postmaster process is alive. Note this
    /// reports the *server process*, not whether it's accepting connections —
    /// see [`ready`](Self::ready).
    ///
    /// Returns `Ok(false)` (not an error) whenever the server isn't running:
    /// - if PGDATA isn't initialized (no `PG_VERSION`) we skip the spawn
    ///   entirely — a missing cluster obviously can't be running, and this also
    ///   avoids `pg_ctl`'s "directory does not exist" exit being mistaken for a
    ///   real failure (this is what `init` on a fresh VM relies on);
    /// - any non-zero `pg_ctl status` exit (3 = not running, 4 = invalid/missing
    ///   data dir) also maps to `false`. Only a spawn failure (the binary
    ///   couldn't run at all) surfaces as an `Err`.
    pub async fn running(&self) -> PgCtlResult<bool> {
        if !self.is_initialized() {
            return Ok(false);
        }
        match self
            .run(&["-D", &arg_path(&self.data_dir), "status"], "status")
            .await
        {
            Ok(_) => Ok(true),
            Err(PgCtlError::CommandFailed { code: Some(_), .. }) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Whether Postgres is accepting connections (lighter than a full SQL
    /// connect). Implemented as `pg_ctl status` for now; can be upgraded to
    /// `pg_isready` without changing the API.
    pub async fn ready(&self) -> PgCtlResult<bool> {
        self.running().await
    }

    /// `pg_ctl start`. A no-op (Ok) if already running.
    pub async fn start(&self) -> PgCtlResult<()> {
        if self.running().await? {
            info!("postgres already running — start is a no-op");
            return Ok(());
        }
        if !self.is_initialized() {
            return Err(PgCtlError::NotInitialized(self.data_dir.clone()));
        }
        self.run(
            &[
                "-D",
                &arg_path(&self.data_dir),
                "-l",
                &arg_path(&self.log_path),
                "-w",
                "start",
            ],
            "start",
        )
        .await?;
        info!("postgres started");
        Ok(())
    }

    /// `pg_ctl stop -m {mode}`. A no-op (Ok) if not running.
    pub async fn stop(&self, mode: StopMode) -> PgCtlResult<()> {
        if !self.running().await? {
            info!("postgres not running — stop is a no-op");
            return Ok(());
        }
        self.run(
            &[
                "-D",
                &arg_path(&self.data_dir),
                "-m",
                mode.as_arg(),
                "-w",
                "stop",
            ],
            "stop",
        )
        .await?;
        info!(mode = mode.as_arg(), "postgres stopped");
        Ok(())
    }

    /// `pg_ctl restart -m fast`.
    pub async fn restart(&self) -> PgCtlResult<()> {
        if !self.is_initialized() {
            return Err(PgCtlError::NotInitialized(self.data_dir.clone()));
        }
        self.run(
            &[
                "-D",
                &arg_path(&self.data_dir),
                "-l",
                &arg_path(&self.log_path),
                "-m",
                "fast",
                "-w",
                "restart",
            ],
            "restart",
        )
        .await?;
        info!("postgres restarted");
        Ok(())
    }

    /// `pg_ctl reload` — reload config without restarting.
    pub async fn reload(&self) -> PgCtlResult<()> {
        self.run(&["-D", &arg_path(&self.data_dir), "reload"], "reload")
            .await?;
        info!("postgres config reloaded");
        Ok(())
    }

    /// `initdb` — initialize a fresh cluster in the data directory.
    ///
    /// Mirrors `scripts/init_pg.sh`:
    ///   1. refuse if postgres is still running (caller must stop first)
    ///   2. refuse if already initialized unless `force` wipes it first
    ///   3. `initdb -D <data_dir> --auth=trust`
    ///   4. copy the override-config template (`postgresql.tiko.conf`) into the
    ///      data dir (or create an empty one)
    ///   5. append `include_if_exists='postgresql.tiko.conf'` to
    ///      `postgresql.conf`
    ///   6. append `host all all <trust_cidr> trust` to `pg_hba.conf`
    ///
    /// `--auth=trust` is added (the reference script relies on the appended
    /// `pg_hba.conf` line) so the agent never blocks on a password prompt — it
    /// runs unattended as a service.
    pub async fn init(&self, force: bool) -> PgCtlResult<()> {
        if self.running().await? {
            return Err(PgCtlError::StillRunning);
        }
        if self.is_initialized() && !force {
            return Err(PgCtlError::AlreadyInitialized(self.data_dir.clone()));
        }

        // 2. Wipe (matching the script's `rm -rf`). Guarded by the checks above.
        if self.data_dir.exists() {
            std::fs::remove_dir_all(&self.data_dir)?;
        }
        std::fs::create_dir_all(&self.data_dir)?;

        // 3. initdb.
        let output = Command::new(&self.initdb)
            .args(["-D", &arg_path(&self.data_dir), "--auth=trust"])
            .envs(&self.tiko_env)
            .output()
            .await
            .map_err(|e| PgCtlError::InitdbFailed {
                code: None,
                stderr: format!("spawn failed: {e}"),
            })?;
        if !output.status.success() {
            return Err(PgCtlError::InitdbFailed {
                code: output.status.code(),
                stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
            });
        }
        info!(data_dir = %self.data_dir.display(), "initdb completed");

        // 4. Override config: copy the template in, or seed an empty file so a
        // subsequent PUT /pg/config has a home.
        if self.config_template.exists() {
            std::fs::copy(&self.config_template, &self.config_file)?;
        } else {
            std::fs::write(&self.config_file, "")?;
        }

        // 5. Wire the override into postgresql.conf via include_if_exists (the
        // base file may already contain other directives; we append).
        append_line(
            &self.data_dir.join("postgresql.conf"),
            "include_if_exists='postgresql.tiko.conf'\n",
        )?;

        // 6. Trust the per-VM subnet (matches the script's pg_hba line).
        append_line(
            &self.data_dir.join("pg_hba.conf"),
            &format!("host all all {} trust\n", self.trust_cidr),
        )?;

        info!(trust_cidr = %self.trust_cidr, "cluster initialized");
        Ok(())
    }

    // ── Config file R/W (postgresql.tiko.conf) ─────────────────────────────

    /// Parse the override config file into an ordered `name → value` map.
    /// Format: `name = value` or `name=value`; `#` lines and blanks ignored.
    /// Quoted values (`'...'` / `"..."`) keep the inner text.
    pub fn read_config(&self) -> PgCtlResult<BTreeMap<String, String>> {
        if !self.config_file.exists() {
            return Ok(BTreeMap::new());
        }
        let text = std::fs::read_to_string(&self.config_file)?;
        parse_pg_config(&text)
    }

    /// Merge `settings` into the override config file, writing it back in a
    /// stable order. Existing keys are overwritten; new keys are appended.
    /// Does NOT reload — call [`reload`](Self::reload) after.
    pub fn write_config(&self, settings: &BTreeMap<String, String>) -> PgCtlResult<()> {
        let mut current = self.read_config()?;
        for (k, v) in settings {
            current.insert(k.clone(), v.clone());
        }
        let text = render_pg_config(&current);
        if let Some(parent) = self.config_file.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&self.config_file, text)?;
        debug!(config = ?self.config_file, "override config updated");
        Ok(())
    }
}

/// Render a path for a CLI argument. `OsStr` → lossy UTF-8 is fine here: all
/// paths in this crate are config-controlled ASCII.
fn arg_path(p: &Path) -> String {
    p.to_string_lossy().into_owned()
}

/// Append `line` to `path`, creating the file if missing. Used during `init`
/// to wire `include_if_exists` and the trust line into the freshly-initdb'd
/// config files.
fn append_line(path: &Path, line: &str) -> PgCtlResult<()> {
    use std::io::Write;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    file.write_all(line.as_bytes())?;
    Ok(())
}

/// Parse `postgresql.conf`-style text. Tolerant: lines that don't parse are
/// skipped with a warning rather than aborting the whole read.
fn parse_pg_config(text: &str) -> PgCtlResult<BTreeMap<String, String>> {
    let mut out = BTreeMap::new();
    for (lineno, raw) in text.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((name, value)) = line.split_once('=') else {
            warn!(lineno, "skipping unparseable config line");
            continue;
        };
        let name = name.trim().to_string();
        let mut value = value.trim().to_string();
        // Strip a trailing comment (postgres allows `name = value # comment`,
        // but not inside quotes — keep this simple, only strip outside quotes).
        if let Some((v, _)) = strip_trailing_comment(&value) {
            value = v.trim().to_string();
        }
        // Unquote.
        if value.len() >= 2
            && ((value.starts_with('\'') && value.ends_with('\''))
                || (value.starts_with('"') && value.ends_with('"')))
        {
            value = value[1..value.len() - 1].to_string();
        }
        if name.is_empty() {
            return Err(PgCtlError::ConfigParse(format!(
                "empty name on line {}",
                lineno + 1
            )));
        }
        out.insert(name, value);
    }
    Ok(out)
}

/// Render settings back to `name = value` text, single-quoting values that
/// contain anything other than digits/`.`/`-`.
fn render_pg_config(settings: &BTreeMap<String, String>) -> String {
    let mut out = String::new();
    out.push_str("# Managed by tikoguest (tikod). Hand-edits will be overwritten.\n");
    for (name, value) in settings {
        let needs_quote = !value
            .chars()
            .all(|c| c.is_ascii_digit() || c == '.' || c == '-');
        if needs_quote {
            let escaped = value.replace('\'', "''");
            out.push_str(&format!("{name} = '{escaped}'\n"));
        } else {
            out.push_str(&format!("{name} = {value}\n"));
        }
    }
    out
}

/// Split `value # comment` keeping only the value half, respecting simple
/// quotes (a `#` inside quotes is not a comment).
fn strip_trailing_comment(value: &str) -> Option<(String, String)> {
    let mut in_squote = false;
    let mut in_dquote = false;
    for (i, c) in value.char_indices() {
        match c {
            '\'' if !in_dquote => in_squote = !in_squote,
            '"' if !in_squote => in_dquote = !in_dquote,
            '#' if !in_squote && !in_dquote => {
                return Some((value[..i].to_string(), value[i..].to_string()));
            }
            _ => {}
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_basic_config() {
        let text = "# header\nlisten_addresses = '*'\nmax_connections=100\nport = 5432 # default\n";
        let m = parse_pg_config(text).unwrap();
        assert_eq!(m.get("listen_addresses").unwrap(), "*");
        assert_eq!(m.get("max_connections").unwrap(), "100");
        assert_eq!(m.get("port").unwrap(), "5432");
    }

    #[test]
    fn parse_strips_quotes_and_comments() {
        let m = parse_pg_config("shared_preload_libraries = 'libtikoworker' # load me\n").unwrap();
        assert_eq!(m.get("shared_preload_libraries").unwrap(), "libtikoworker");
    }

    #[test]
    fn render_round_trips() {
        let mut m = BTreeMap::new();
        m.insert("port".into(), "5432".into());
        m.insert("listen_addresses".into(), "*".into());
        let text = render_pg_config(&m);
        let parsed = parse_pg_config(&text).unwrap();
        assert_eq!(parsed.get("port").unwrap(), "5432");
        assert_eq!(parsed.get("listen_addresses").unwrap(), "*");
    }

    #[test]
    fn merge_keeps_existing_and_appends_new() {
        let text = "port = 5432\nlisten_addresses = '*'\n";
        let mut current = parse_pg_config(text).unwrap();
        current.insert("max_connections".into(), "100".into());
        assert_eq!(current.len(), 3);
        assert_eq!(current.get("port").unwrap(), "5432");
    }
}
