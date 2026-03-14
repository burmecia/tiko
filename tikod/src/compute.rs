//! Compute backend abstraction.
//!
//! All orchestration code issues `pg_ctl`/`psql` commands via `ComputeBackend::execute`
//! without caring whether they run locally or (future) over SSH into a Firecracker VM.

use std::path::Path;
use std::process::Command;

/// Shell command execution and liveness check for the compute environment.
pub trait ComputeBackend: Send + Sync {
    /// Execute a shell command in the compute environment.
    /// Returns combined stdout+stderr on success, error string on non-zero exit.
    fn execute(&self, cmd: &str) -> Result<String, String>;

    /// Returns true if a postgres process is currently running under `pgdata`.
    fn is_running(&self, pgdata: &str) -> bool;

    /// Pause the compute unit and write a memory snapshot to `snapshot_path`.
    /// `LocalProcess` returns `Err` (unsupported); callers fall back to cold-start.
    /// `FirecrackerVm` uses the Firecracker snapshot API.
    fn freeze(&self, snapshot_path: &Path) -> Result<(), String>;

    /// Resume from a previously written snapshot at `snapshot_path`.
    /// `LocalProcess` returns `Err` (unsupported); callers fall back to cold-start.
    /// `FirecrackerVm` uses the Firecracker resume API.
    fn thaw_from_snapshot(&self, snapshot_path: &Path) -> Result<(), String>;
}

/// Local subprocess backend — runs commands via `sh -c`.
pub struct LocalProcess;

impl ComputeBackend for LocalProcess {
    fn execute(&self, cmd: &str) -> Result<String, String> {
        let output = Command::new("sh")
            .arg("-c")
            .arg(cmd)
            .output()
            .map_err(|e| format!("failed to spawn shell: {e}"))?;

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        let combined = format!("{stdout}{stderr}");

        if output.status.success() {
            Ok(combined)
        } else {
            Err(combined)
        }
    }

    fn is_running(&self, pgdata: &str) -> bool {
        Command::new("pg_ctl")
            .args(["status", "-D", pgdata])
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
    }

    fn freeze(&self, _snapshot_path: &Path) -> Result<(), String> {
        Err("freeze/thaw not supported for LocalProcess; use FirecrackerVm".to_owned())
    }

    fn thaw_from_snapshot(&self, _snapshot_path: &Path) -> Result<(), String> {
        Err("freeze/thaw not supported for LocalProcess; use FirecrackerVm".to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn execute_returns_stdout() {
        let b = LocalProcess;
        let out = b.execute("echo hello").unwrap();
        assert!(out.contains("hello"));
    }

    #[test]
    fn execute_returns_err_on_nonzero_exit() {
        let b = LocalProcess;
        assert!(b.execute("exit 1").is_err());
    }

    #[test]
    fn is_running_false_when_no_postgres() {
        let b = LocalProcess;
        assert!(!b.is_running("/nonexistent/pgdata"));
    }

    #[test]
    fn freeze_unsupported_on_local_process() {
        let b = LocalProcess;
        assert!(b.freeze(Path::new("/tmp/snap")).is_err());
    }

    #[test]
    fn thaw_from_snapshot_unsupported_on_local_process() {
        let b = LocalProcess;
        assert!(b.thaw_from_snapshot(Path::new("/tmp/snap")).is_err());
    }
}
