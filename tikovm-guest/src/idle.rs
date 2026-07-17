//! Idle evaluator (design §8) — the guest-owned scale-to-zero brain.
//!
//! Collects probe signals each tick, evaluates the manifest's `[idle]` policy,
//! and asks the host to suspend the VM when all probes have been idle for
//! `idle_secs`. The host is "dumb" about idleness: it only serves network stats
//! and obeys the resulting [`HostComm::request_suspend`].
//!
//! Probe kinds: `host_network` (pull VM-scoped stats from the host),
//! `exec` (run a rootfs script; exit 0 = idle). The PG specifics that today
//! live in `tikoguest/src/pgmetrics.rs:123-132` move into an `exec` probe.

use std::sync::Arc;
use std::time::Duration;

use tokio::process::Command;
use tikovm_protocol::manifest::{IdlePolicy, IdleProbe};
use tikovm_protocol::rpc::NetworkStats;

/// The guest's channel to the host: pull network stats, push suspend/health.
/// Real impl talks vsock; tests use a fake.
#[async_trait::async_trait]
pub trait HostComm: Send + Sync {
    fn vm_id(&self) -> &str;
    async fn network_stats(&self) -> NetworkStats;
    async fn request_suspend(&self);
    async fn report_health(&self, healthy: bool);
}

/// Evaluates the idle policy; fires `request_suspend` when sustained idle.
pub struct IdleEvaluator {
    policy: IdlePolicy,
    host: Arc<dyn HostComm>,
    /// Accumulated idle seconds across consecutive idle ticks.
    idle_for_secs: std::sync::Mutex<u64>,
}

impl IdleEvaluator {
    pub fn new(policy: IdlePolicy, host: Arc<dyn HostComm>) -> Self {
        Self {
            policy,
            host,
            idle_for_secs: std::sync::Mutex::new(0),
        }
    }

    /// Run the evaluator loop until cancelled.
    pub async fn run(self: Arc<Self>, cancel: tokio::sync::Notify) {
        let tick = Duration::from_secs(self.policy.tick_secs.max(1));
        loop {
            tokio::select! {
                _ = cancel.notified() => break,
                _ = tokio::time::sleep(tick) => {}
            }
            self.clone().tick().await;
        }
    }

    /// One evaluation pass.
    pub async fn tick(&self) {
        let all_idle = self.collect_all_idle().await;
        // Decide + update the accumulator under a short-lived lock, then release
        // before awaiting request_suspend (no guard held across an await).
        let fire = {
            let mut acc = self.idle_for_secs.lock().unwrap();
            if all_idle {
                *acc += self.policy.tick_secs.max(1);
                if *acc >= self.policy.idle_secs {
                    let elapsed = *acc;
                    *acc = 0;
                    tracing::info!(vm_id = self.host.vm_id(), idle_secs = elapsed, "idle threshold reached; requesting suspend");
                    true
                } else {
                    false
                }
            } else {
                *acc = 0;
                false
            }
        };
        if fire {
            self.host.request_suspend().await;
        }
    }

    /// True iff every declared probe reports idle this tick.
    async fn collect_all_idle(&self) -> bool {
        for probe in &self.policy.probes {
            if !probe_idle(probe, self.host.as_ref()).await {
                return false;
            }
        }
        // No probes at all => never idle (avoid suspending a VM with no policy).
        !self.policy.probes.is_empty()
    }
}

async fn probe_idle(probe: &IdleProbe, host: &dyn HostComm) -> bool {
    match probe {
        IdleProbe::HostNetwork => {
            let stats = host.network_stats().await;
            stats.is_idle()
        }
        IdleProbe::Exec { cmd } => {
            // exit 0 => idle
            match Command::new("sh").arg("-c").arg(cmd).output().await {
                Ok(out) => out.status.success(),
                Err(e) => {
                    tracing::warn!(error = %e, "idle exec probe failed; treating as busy");
                    false
                }
            }
        }
        IdleProbe::Http { url } => {
            // Minimal: a 2xx/3xx response means busy (active), anything else idle.
            // (Full HTTP probe lands with the HTTP client module.)
            let _ = url;
            tracing::warn!("http idle probe not yet implemented; treating as busy");
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    struct FakeHost {
        vm_id: String,
        net_idle: AtomicBool,
        suspends: AtomicU64,
    }

    #[async_trait::async_trait]
    impl HostComm for FakeHost {
        fn vm_id(&self) -> &str {
            &self.vm_id
        }
        async fn network_stats(&self) -> NetworkStats {
            if self.net_idle.load(Ordering::Relaxed) {
                NetworkStats { established_conns: 0, last_data_age_secs: 999, bytes_in: 0, bytes_out: 0 }
            } else {
                NetworkStats { established_conns: 1, last_data_age_secs: 0, bytes_in: 1, bytes_out: 0 }
            }
        }
        async fn request_suspend(&self) {
            self.suspends.fetch_add(1, Ordering::Relaxed);
        }
        async fn report_health(&self, _healthy: bool) {}
    }

    fn policy(idle_secs: u64, probes: Vec<IdleProbe>) -> IdlePolicy {
        IdlePolicy { tick_secs: 1, idle_secs, probes }
    }

    #[tokio::test]
    async fn fires_after_sustained_idle() {
        let host = Arc::new(FakeHost { vm_id: "vm-1".into(), net_idle: AtomicBool::new(true), suspends: AtomicU64::new(0) });
        let ev = IdleEvaluator::new(policy(3, vec![IdleProbe::HostNetwork]), host.clone());
        // idle_secs=3, tick_secs=1 => need 3 idle ticks.
        ev.tick().await; // acc=1
        ev.tick().await; // acc=2
        assert_eq!(host.suspends.load(Ordering::Relaxed), 0);
        ev.tick().await; // acc=3 >= 3 => fires + reset
        assert_eq!(host.suspends.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn resets_on_busy_tick() {
        let host = Arc::new(FakeHost { vm_id: "vm-2".into(), net_idle: AtomicBool::new(true), suspends: AtomicU64::new(0) });
        let ev = IdleEvaluator::new(policy(5, vec![IdleProbe::HostNetwork]), host.clone());
        ev.tick().await; // acc=1
        host.net_idle.store(false, Ordering::Relaxed);
        ev.tick().await; // busy => reset to 0
        host.net_idle.store(true, Ordering::Relaxed);
        ev.tick().await; // acc=1
        assert_eq!(host.suspends.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn exec_probe_exit_code_drives_idle() {
        let host = Arc::new(FakeHost { vm_id: "vm-3".into(), net_idle: AtomicBool::new(true), suspends: AtomicU64::new(0) });
        // `/bin/true` exits 0 => idle. With both probes idle, evaluator accumulates.
        let ev = IdleEvaluator::new(
            policy(1, vec![IdleProbe::HostNetwork, IdleProbe::Exec { cmd: "/bin/true".into() }]),
            host.clone(),
        );
        ev.tick().await;
        assert_eq!(host.suspends.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn exec_probe_busy_blocks_suspend() {
        let host = Arc::new(FakeHost { vm_id: "vm-4".into(), net_idle: AtomicBool::new(true), suspends: AtomicU64::new(0) });
        // `/bin/false` exits 1 => busy; suspends never fire.
        let ev = IdleEvaluator::new(
            policy(1, vec![IdleProbe::Exec { cmd: "/bin/false".into() }]),
            host.clone(),
        );
        ev.tick().await;
        ev.tick().await;
        assert_eq!(host.suspends.load(Ordering::Relaxed), 0);
    }
}
