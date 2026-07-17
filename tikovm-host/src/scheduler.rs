//! Scheduled-job triggers (design §13).
//!
//! Host-driven (only the host can wake a suspended VM on time). Sibling to idle
//! detection (§8, guest-driven) — both end in the same `suspend`/`restore`
//! machinery; only the trigger source differs. The scheduler is a long-running
//! tokio task that evaluates due schedules and invokes [`Node`].
//!
//! Run modes (`keep_warm`):
//! - `true` (default): the VM is restored each tick (`Suspended`→`Started`);
//!   the guest runs the job and re-`SuspendRequest`s when done.
//! - `false` (ephemeral): a fresh VM is provisioned each tick, runs, and is
//!   destroyed. (Requires the full provisioning pipeline.)

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use chrono::{DateTime, Utc};
use cron::Schedule;
use tikovm_protocol::manifest::SchedulePolicy;
use tikovm_protocol::vm::{VmId, VmState};

use crate::node::Node;

/// How often the scheduler re-evaluates due triggers.
const TICK_INTERVAL: Duration = Duration::from_secs(1);

pub struct Scheduler {
    node: Arc<Node>,
    /// In-memory next-fire time per VM (recomputed at boot; not persisted, since
    /// after a crash the next fire is relative to "now").
    next_fire: Mutex<HashMap<VmId, SystemTime>>,
}

impl Scheduler {
    pub fn new(node: Arc<Node>) -> Self {
        Self {
            node,
            next_fire: Mutex::new(HashMap::new()),
        }
    }

    /// Run the scheduler loop until the process exits.
    pub async fn run(self: Arc<Self>) {
        loop {
            self.tick().await;
            tokio::time::sleep(TICK_INTERVAL).await;
        }
    }

    /// One evaluation pass: for every VM with a schedule that is due, trigger it.
    pub async fn tick(&self) {
        let now = SystemTime::now();
        // Snapshot the ids to avoid holding registry guards across awaits.
        let ids: Vec<VmId> = self.node.control().ids();
        for vm_id in ids {
            let Some(rec) = self.node.control().get(&vm_id) else {
                continue;
            };
            let (schedule, state) = {
                let g = rec.read().unwrap();
                (schedule_of(&g.spec), g.state)
            };
            let Some(schedule) = schedule else { continue };
            if let Err(e) = schedule.validate() {
                tracing::warn!(%vm_id, error = %e, "invalid schedule, skipping");
                continue;
            }
            if !self.is_due(&vm_id, &schedule, now) {
                continue;
            }
            if let Err(e) = self.trigger(&vm_id, &schedule, state).await {
                tracing::warn!(%vm_id, error = %e, "scheduled trigger failed");
            }
        }
    }

    fn is_due(&self, vm_id: &VmId, schedule: &SchedulePolicy, now: SystemTime) -> bool {
        let mut nf = self.next_fire.lock().unwrap();
        let next = nf
            .entry(vm_id.clone())
            .or_insert_with(|| compute_next(schedule, now));
        if now >= *next {
            // advance to the next fire after now
            *next = compute_next(schedule, now);
            true
        } else {
            false
        }
    }

    async fn trigger(
        &self,
        vm_id: &VmId,
        schedule: &SchedulePolicy,
        state: VmState,
    ) -> Result<(), String> {
        if schedule.keep_warm {
            // Keep-warm: restore (wake) the VM if not already started. The guest
            // runs the job on wake and re-suspends when done (SuspendRequest).
            match state {
                VmState::Started => {
                    tracing::debug!(%vm_id, "scheduled tick: already running");
                }
                VmState::Suspended | VmState::Paused => {
                    tracing::info!(%vm_id, "scheduled tick: restoring (keep-warm)");
                    self.node
                        .ensure_running(vm_id)
                        .await
                        .map_err(|e| e.to_string())?;
                }
                VmState::Created => {
                    tracing::info!(%vm_id, "scheduled tick: starting (keep-warm, first run)");
                    self.node.start(vm_id).await.map_err(|e| e.to_string())?;
                }
                other => {
                    tracing::warn!(%vm_id, %other, "scheduled tick: VM not in a runnable state");
                }
            }
            Ok(())
        } else {
            // Ephemeral: provision a fresh VM each tick. Requires the full
            // provisioning pipeline (networking/storage), not yet wired here.
            tracing::warn!(%vm_id, "ephemeral scheduled provisioning not yet implemented");
            Ok(())
        }
    }
}

/// Extract the effective schedule for a VM: explicit override on the spec, else
/// the manifest's `[schedule]`.
pub fn schedule_of(spec: &tikovm_protocol::vm::VmSpec) -> Option<SchedulePolicy> {
    spec.schedule
        .clone()
        .or_else(|| spec.manifest.as_ref().and_then(|m| m.schedule.clone()))
}

/// Compute the next fire time for `schedule` strictly after `after`.
pub fn compute_next(schedule: &SchedulePolicy, after: SystemTime) -> SystemTime {
    if let Some(secs) = schedule.interval_secs {
        return after + Duration::from_secs(secs);
    }
    if let Some(expr) = &schedule.cron {
        let after_dt: DateTime<Utc> = DateTime::<Utc>::from(after);
        match expr.parse::<Schedule>() {
            Ok(s) => {
                if let Some(next) = s.after(&after_dt).next() {
                    return SystemTime::from(next);
                }
            }
            Err(e) => {
                tracing::warn!(cron = %expr, error = %e, "unparseable cron expression");
            }
        }
    }
    // Fallback: far future (effectively disabled).
    after + Duration::from_secs(365 * 24 * 3600)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::Control;
    use crate::vmm::mock::MockVmm;
    use tikovm_protocol::manifest::WorkloadManifest;

    fn make_node(tmp: &std::path::Path) -> (Arc<Node>, Arc<MockVmm>) {
        let vmm = Arc::new(MockVmm::new(tmp.join("snaps")));
        let node = Arc::new(Node::new(vmm.clone(), Arc::new(Control::new())));
        (node, vmm)
    }

    #[test]
    fn compute_next_interval() {
        let s = SchedulePolicy::interval(60);
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1000);
        let next = compute_next(&s, now);
        assert_eq!(next.duration_since(now).unwrap(), Duration::from_secs(60));
    }

    #[test]
    fn compute_next_cron_parses() {
        let s = SchedulePolicy::cron("0 * * * * *"); // every minute (6-field: sec min hr dom mon dow)
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1000);
        let next = compute_next(&s, now);
        // next must be in the future
        assert!(next > now);
    }

    #[tokio::test]
    async fn scheduler_triggers_keep_warm_restore() {
        let tmp = tempfile::tempdir().unwrap();
        let (node, _vmm) = make_node(tmp.path());
        // Provision + suspend a VM so the scheduler can restore it.
        let mut spec = tikovm_protocol::vm::VmSpec {
            vm_id: "vm-s".into(),
            rootfs: tikovm_protocol::vm::RootfsRef {
                path: "/r".into(),
                read_only_base: true,
            },
            resources: tikovm_protocol::vm::ResourceConfig::default(),
            kernel: tikovm_protocol::vm::KernelSpec {
                kernel_path: "/k".into(),
                kernel_cmdline: "".into(),
                initrd_path: None,
            },
            network: tikovm_protocol::vm::NetworkSpec::default(),
            routing: vec![],
            env: Default::default(),
            manifest: Some(WorkloadManifest::empty("job")),
            schedule: Some(SchedulePolicy::interval(1)),
        };
        let _ = &mut spec;
        let cfg = crate::vmm::VmConfig {
            vm_id: "vm-s".into(),
            kernel_path: "/k".into(),
            kernel_cmdline: "".into(),
            rootfs_path: "/r".into(),
            memory_mb: 512,
            vcpus: 1,
            drives: vec![],
            initrd_path: None,
            guest_cid: Some(3),
        };
        node.create(cfg, spec).await.unwrap();
        node.start(&"vm-s".to_string()).await.unwrap();
        node.pause(&"vm-s".to_string()).await.unwrap();
        node.suspend(&"vm-s".to_string()).await.unwrap();
        assert_eq!(node.state_of(&"vm-s".to_string()), Some(VmState::Suspended));

        // Force the next-fire to the past so the scheduler considers it due.
        let sched = Scheduler::new(node.clone());
        sched
            .next_fire
            .lock()
            .unwrap()
            .insert("vm-s".to_string(), SystemTime::UNIX_EPOCH);
        sched.tick().await;
        // The keep-warm tick should have restored it.
        assert_eq!(node.state_of(&"vm-s".to_string()), Some(VmState::Started));
    }
}
