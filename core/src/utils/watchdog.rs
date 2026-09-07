//! Wedge watchdog for shmem spin locks.
//!
//! A process that dies (or hangs) while holding an [`AtomicRWLock`] or
//! mid-seqlock-mutation leaves that state set forever and every other
//! process spins indefinitely. Self-release is unsafe — the interrupted
//! critical section is not idempotent — so the watchdog escalates instead:
//! PANIC on the process's PG thread, or from a Tokio runtime thread a poison
//! handoff to the tikoworker main loop, which PANICs on its PG thread. The
//! postmaster then crash-restarts the cluster and reinitialises shmem; the
//! surviving state is rebuilt by hydration plus the first commit's draft
//! drain. This mirrors PostgreSQL's own posture for a dead LWLock holder.
//!
//! [`AtomicRWLock`]: crate::utils::rw_lock::AtomicRWLock

use std::sync::Mutex;
use std::time::{Duration, Instant};

use pgsys::logging::{self, PANIC, pg_log_warning};

/// Spin this long before the first warning. Owner-liveness probing starts at
/// the same time, keeping syscalls out of short spins.
const WARN_AFTER: Duration = Duration::from_secs(30);
/// Spin this long before escalating even with a live holder — slack for
/// slow-but-legitimate I/O (NFS stalls) inside a critical section.
const ESCALATE_AFTER: Duration = Duration::from_secs(600);
/// Clock reads are vDSO-cheap but not free; sample every N spins.
const CHECK_EVERY_SPINS: u32 = 4096;

/// Set by an off-PG-thread escalation; drained by the tikoworker main loop.
static POISON: Mutex<Option<String>> = Mutex::new(None);

/// Take a pending poison message, if any. The tikoworker main loop polls
/// this every iteration and PANICs on its PG thread.
pub fn take_poison() -> Option<String> {
    POISON.lock().unwrap_or_else(|e| e.into_inner()).take()
}

fn poison(msg: &str) {
    let mut p = POISON.lock().unwrap_or_else(|e| e.into_inner());
    if p.is_none() {
        *p = Some(msg.to_owned());
    }
}

/// Liveness of a process by PID; EPERM counts as alive.
fn pid_alive(pid: i32) -> bool {
    if unsafe { libc::kill(pid, 0) } == 0 {
        return true;
    }
    std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Action {
    Continue,
    Warn,
    Escalate,
}

/// Pure policy, split out for tests. `owner_alive` is `None` when no writer
/// is identifiable (stuck reader count or a dead pending-writer).
pub(crate) fn decide(elapsed: Duration, owner_alive: Option<bool>, warned: bool) -> Action {
    if elapsed < WARN_AFTER {
        return Action::Continue;
    }
    // A dead owner never releases — escalate without waiting out the long
    // timeout. (The owner may exit between our load of its PID and this
    // probe, but only after ~WARN_AFTER of continuous contention; a false
    // positive costs a restart, not corruption.)
    if owner_alive == Some(false) || elapsed >= ESCALATE_AFTER {
        return Action::Escalate;
    }
    if !warned {
        return Action::Warn;
    }
    Action::Continue
}

/// Rate-limited spin watcher. Construct before a spin loop and [`tick`]
/// every iteration; costs nothing until the spin drags on.
///
/// [`tick`]: Self::tick
pub(crate) struct SpinWatch {
    /// Lazily set on the first check so uncontended acquisitions never
    /// touch the clock.
    start: Option<Instant>,
    spins: u32,
    warned: bool,
}

impl SpinWatch {
    pub(crate) fn new() -> Self {
        Self {
            start: None,
            spins: 0,
            warned: false,
        }
    }

    /// `what` names the spin site in log messages; `owner_pid` is the
    /// write-lock holder (0 = none identifiable).
    pub(crate) fn tick(&mut self, what: &'static str, owner_pid: i32) {
        self.spins += 1;
        if self.spins < CHECK_EVERY_SPINS {
            return;
        }
        self.spins = 0;
        let elapsed = self.start.get_or_insert_with(Instant::now).elapsed();
        let owner_alive = (owner_pid != 0).then(|| pid_alive(owner_pid));
        match decide(elapsed, owner_alive, self.warned) {
            Action::Continue => {}
            Action::Warn => {
                self.warned = true;
                pg_log_warning(format!(
                    "tiko: {what}: spinning for {elapsed:.0?}, owner pid {owner_pid} — slow holder or wedge"
                ));
            }
            Action::Escalate => escalate(&format!(
                "tiko: {what}: wedged for {elapsed:.0?}, owner pid {owner_pid} ({})",
                match owner_alive {
                    Some(false) => "dead",
                    Some(true) => "alive but no progress",
                    None => "not identifiable — stuck readers or pending writer",
                }
            )),
        }
    }
}

/// Terminally escalate a detected wedge. On the PG thread, PANIC (elog
/// PANIC aborts the process; the postmaster crash-restarts the cluster and
/// reinitialises shmem). Off the PG thread, poison for the tikoworker main
/// loop and park — it PANICs within one poll interval.
fn escalate(msg: &str) -> ! {
    if logging::on_pg_thread() {
        logging::pg_log(PANIC, msg);
        // A stub rust_pg_log (CLI binaries) returns even at PANIC level.
        std::process::abort();
    }
    poison(msg);
    // Relayed for context; the main loop drains it before PANICing.
    logging::pg_log(PANIC, msg);
    loop {
        std::thread::park();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decide_waits_below_warn_threshold() {
        for owner in [None, Some(true), Some(false)] {
            assert_eq!(
                decide(Duration::from_secs(5), owner, false),
                Action::Continue
            );
        }
    }

    #[test]
    fn decide_escalates_on_dead_owner_past_warn() {
        assert_eq!(
            decide(WARN_AFTER, Some(false), false),
            Action::Escalate,
            "dead owner escalates as soon as the warn window passes"
        );
        assert_eq!(
            decide(Duration::from_secs(120), Some(false), true),
            Action::Escalate
        );
    }

    #[test]
    fn decide_warns_once_for_live_holder() {
        assert_eq!(decide(WARN_AFTER, Some(true), false), Action::Warn);
        assert_eq!(decide(WARN_AFTER, None, false), Action::Warn);
        assert_eq!(
            decide(Duration::from_secs(120), Some(true), true),
            Action::Continue,
            "already warned, long timeout not reached"
        );
    }

    #[test]
    fn decide_escalates_at_long_timeout() {
        assert_eq!(decide(ESCALATE_AFTER, Some(true), true), Action::Escalate);
        assert_eq!(decide(ESCALATE_AFTER, None, true), Action::Escalate);
        assert_eq!(
            decide(ESCALATE_AFTER - Duration::from_secs(1), Some(true), true),
            Action::Continue
        );
    }

    #[test]
    fn pid_alive_detects_self_and_dead() {
        assert!(pid_alive(unsafe { libc::getpid() }));
        // A reaped child guarantees an unowned PID.
        let mut child = std::process::Command::new("true").spawn().unwrap();
        child.wait().unwrap();
        assert!(!pid_alive(child.id() as i32));
    }

    #[test]
    fn poison_is_set_once_and_taken() {
        assert!(take_poison().is_none());
        poison("first");
        poison("second");
        assert_eq!(take_poison().as_deref(), Some("first"));
        assert!(take_poison().is_none());
    }

    #[test]
    fn tick_below_threshold_is_quiet() {
        // Full CHECK_EVERY_SPINS rounds without ever reaching WARN_AFTER:
        // no warning, no escalation, no poison.
        let mut watch = SpinWatch::new();
        for _ in 0..CHECK_EVERY_SPINS * 3 {
            watch.tick("test", unsafe { libc::getpid() });
        }
        assert!(take_poison().is_none());
        assert!(watch.start.is_some(), "clock armed on first check");
    }
}
