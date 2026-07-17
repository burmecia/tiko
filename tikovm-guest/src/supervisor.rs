//! Workload process supervisor (design §4.3).
//!
//! The big upgrade over current `tikoguest`: a real supervisor with a restart
//! policy + backoff, graceful (SIGTERM) then forced (SIGKILL) stop, and
//! cooperative cancellation. It runs the manifest's `[process]` as a long-lived
//! child and revives it per [`RestartPolicy`].
//!
//! Cancellation uses an `AtomicBool` stop flag as the source of truth (a bare
//! `Notify` permit can be consumed by the wrong `notified()` call across the
//! multiple await points in the loop and get lost).

use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

use tikovm_protocol::manifest::{ProcessSpec, RestartMode, RestartPolicy};
use tokio::process::{Child, Command};
use tokio::sync::Notify;

/// Default grace period before escalating SIGTERM → SIGKILL.
const STOP_GRACE_SECS: u64 = 5;

/// Handle used to request a supervisor stop from another task.
#[derive(Clone)]
pub struct StopHandle {
    flag: Arc<AtomicBool>,
    notify: Arc<Notify>,
}

impl StopHandle {
    /// Request the supervisor stop the workload and return from `run`.
    pub fn stop(&self) {
        self.flag.store(true, Ordering::SeqCst);
        self.notify.notify_waiters(); // wake anyone parked in backoff/select
    }
}

/// Supervises one `[process]`, respawning it per the restart policy until
/// [`Supervisor::stop_handle`] is used, or the process exits cleanly in a way
/// the policy considers terminal.
pub struct Supervisor {
    spec: ProcessSpec,
    policy: RestartPolicy,
    flag: Arc<AtomicBool>,
    notify: Arc<Notify>,
    /// Number of times the process has been (re)spawned.
    pub spawns: Arc<AtomicU32>,
}

impl Supervisor {
    pub fn new(spec: ProcessSpec, policy: RestartPolicy) -> Self {
        Self {
            spec,
            policy,
            flag: Arc::new(AtomicBool::new(false)),
            notify: Arc::new(Notify::new()),
            spawns: Arc::new(AtomicU32::new(0)),
        }
    }

    /// A handle that can request this supervisor to stop.
    pub fn stop_handle(&self) -> StopHandle {
        StopHandle {
            flag: self.flag.clone(),
            notify: self.notify.clone(),
        }
    }

    fn stopped(&self) -> bool {
        self.flag.load(Ordering::SeqCst)
    }

    /// Run the supervision loop. Returns when the process has exited and the
    /// policy says not to restart, or when stop was requested.
    pub async fn run(&self) {
        loop {
            if self.stopped() {
                return;
            }
            let mut child = match self.spawn().await {
                Ok(c) => c,
                Err(e) => {
                    tracing::error!(error = %e, cmd = %self.spec.cmd, "failed to spawn workload");
                    if matches!(self.policy.policy, RestartMode::Never) {
                        return;
                    }
                    self.backoff().await;
                    continue;
                }
            };

            // Race the child exit against a stop request.
            tokio::select! {
                status = child.wait() => {
                    let code = status.ok().and_then(|s| s.code()).unwrap_or(-1);
                    tracing::info!(code, "workload exited");
                    let failed = code != 0;
                    if self.stopped() || !self.should_restart(failed) {
                        return;
                    }
                    self.backoff().await;
                }
                _ = self.notify.notified() => {
                    // Stop is the only notifier; if flagged, stop the child.
                    if self.stopped() {
                        self.graceful_stop(&mut child).await;
                        return;
                    }
                }
            }
        }
    }

    fn should_restart(&self, failed: bool) -> bool {
        match self.policy.policy {
            RestartMode::Always => true,
            RestartMode::OnFailure => failed,
            RestartMode::Never => false,
        }
    }

    async fn backoff(&self) {
        if self.stopped() {
            return;
        }
        let secs = self.policy.backoff_secs;
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(secs)) => {}
            _ = self.notify.notified() => {}
        }
    }

    async fn spawn(&self) -> std::io::Result<Child> {
        self.spawns.fetch_add(1, Ordering::Relaxed);
        let mut cmd = Command::new(&self.spec.cmd);
        cmd.args(&self.spec.args)
            .envs(&self.spec.env)
            .stdin(Stdio::null())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .kill_on_drop(true);
        if let Some(cwd) = &self.spec.cwd {
            cmd.current_dir(cwd);
        }
        cmd.spawn()
    }

    /// SIGTERM, wait the grace period, then SIGKILL.
    async fn graceful_stop(&self, child: &mut Child) {
        if let Some(pid) = child.id() {
            unsafe {
                libc::kill(pid as i32, libc::SIGTERM);
            }
        }
        tokio::select! {
            _ = child.wait() => {}
            _ = tokio::time::sleep(Duration::from_secs(STOP_GRACE_SECS)) => {
                if let Some(pid) = child.id() {
                    unsafe { libc::kill(pid as i32, libc::SIGKILL); }
                }
                let _ = child.wait().await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn spec(cmd: &str, args: &[&str]) -> ProcessSpec {
        ProcessSpec {
            cmd: cmd.to_string(),
            args: args.iter().map(|s| s.to_string()).collect(),
            cwd: None,
            env: HashMap::new(),
            user: None,
        }
    }

    #[tokio::test]
    async fn never_policy_stops_after_clean_exit() {
        let sup = Supervisor::new(spec("/bin/true", &[]), RestartPolicy::default());
        sup.run().await;
        assert_eq!(sup.spawns.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn on_failure_restarts_failing_process_then_stops() {
        // A command that always exits 1. With OnFailure it keeps restarting; we
        // request stop after observing >= 2 spawns.
        let mut policy = RestartPolicy::default();
        policy.backoff_secs = 0; // restart immediately for a fast test
        let sup = Supervisor::new(spec("/bin/sh", &["-c", "exit 1"]), policy);
        let stop = sup.stop_handle();
        let spawns = sup.spawns.clone();

        let handle = tokio::spawn(async move { sup.run().await });

        for _ in 0..500 {
            if spawns.load(Ordering::Relaxed) >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            spawns.load(Ordering::Relaxed) >= 2,
            "should have restarted at least twice"
        );
        stop.stop();
        tokio::time::timeout(Duration::from_secs(STOP_GRACE_SECS + 5), handle)
            .await
            .expect("supervisor stopped within timeout")
            .unwrap();
    }

    #[tokio::test]
    async fn stop_terminates_long_running_process() {
        let mut policy = RestartPolicy::default();
        policy.policy = RestartMode::Always;
        let sup = Supervisor::new(spec("/bin/sleep", &["30"]), policy);
        let stop = sup.stop_handle();
        let spawns = sup.spawns.clone();
        let handle = tokio::spawn(async move { sup.run().await });

        for _ in 0..500 {
            if spawns.load(Ordering::Relaxed) >= 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(spawns.load(Ordering::Relaxed), 1);
        stop.stop();
        tokio::time::timeout(Duration::from_secs(STOP_GRACE_SECS + 5), handle)
            .await
            .expect("stopped within grace+margin")
            .unwrap();
    }
}
