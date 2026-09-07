//! Spin-based atomic reader-writer lock plus the wedge watchdog driving its
//! spin loops.
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

use std::sync::Mutex;
use std::sync::atomic::{AtomicI32, Ordering};
use std::time::{Duration, Instant};

use pgsys::common::Pid;
use pgsys::logging::{self, PANIC, pg_log_warning};

// Spin this long before the first warning. Owner-liveness probing starts at
// the same time, keeping syscalls out of short spins.
const WARN_AFTER: Duration = Duration::from_secs(30);
// Spin this long before escalating even with a live holder — slack for
// slow-but-legitimate I/O (NFS stalls) inside a critical section.
const ESCALATE_AFTER: Duration = Duration::from_secs(600);
// Clock reads are vDSO-cheap but not free; sample every N spins.
const CHECK_EVERY_SPINS: u32 = 4096;

// Set by an off-PG-thread escalation; drained by the tikoworker main loop.
static POISON: Mutex<Option<String>> = Mutex::new(None);

// Take a pending poison message, if any. The tikoworker main loop polls
// this every iteration and PANICs on its PG thread.
pub fn take_poison() -> Option<String> {
    POISON.lock().unwrap_or_else(|e| e.into_inner()).take()
}

fn poison(msg: &str) {
    let mut p = POISON.lock().unwrap_or_else(|e| e.into_inner());
    if p.is_none() {
        *p = Some(msg.to_owned());
    }
}

// Liveness of a process by PID; EPERM counts as alive.
fn pid_alive(pid: i32) -> bool {
    if unsafe { libc::kill(pid, 0) } == 0 {
        return true;
    }
    std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

#[derive(Debug, PartialEq, Eq)]
enum Action {
    Continue,
    Warn,
    Escalate,
}

impl Action {
    // Pure policy, split out for tests. `owner_alive` is `None` when no writer
    // is identifiable (stuck reader count or a dead pending-writer).
    fn decide(elapsed: Duration, owner_alive: Option<bool>, warned: bool) -> Self {
        if elapsed < WARN_AFTER {
            return Self::Continue;
        }
        // A dead owner never releases — escalate without waiting out the long
        // timeout. (The owner may exit between our load of its PID and this
        // probe, but only after ~WARN_AFTER of continuous contention; a false
        // positive costs a restart, not corruption.)
        if owner_alive == Some(false) || elapsed >= ESCALATE_AFTER {
            return Self::Escalate;
        }
        if !warned {
            return Self::Warn;
        }
        Self::Continue
    }
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
    pub(crate) fn tick(&mut self, what: &'static str, owner_pid: Pid) {
        self.spins += 1;
        if self.spins < CHECK_EVERY_SPINS {
            return;
        }
        self.spins = 0;
        let elapsed = self.start.get_or_insert_with(Instant::now).elapsed();
        let owner_alive = (owner_pid != 0).then(|| pid_alive(owner_pid));
        match Action::decide(elapsed, owner_alive, self.warned) {
            Action::Continue => {}
            Action::Warn => {
                self.warned = true;
                pg_log_warning(format!(
                    "tiko: {what}: spinning for {elapsed:.0?}, owner pid {owner_pid} — slow holder or wedge"
                ));
            }
            Action::Escalate => {
                let msg = format!(
                    "tiko: {what}: wedged for {elapsed:.0?}, owner pid {owner_pid} ({})",
                    match owner_alive {
                        Some(false) => "dead",
                        Some(true) => "alive but no progress",
                        None => "not identifiable — stuck readers or pending writer",
                    }
                );

                // Terminally escalate a detected wedge. On the PG thread, PANIC (elog
                // PANIC aborts the process; the postmaster crash-restarts the cluster and
                // reinitialises shmem). Off the PG thread, poison for the tikoworker main
                // loop and park — it PANICs within one poll interval.
                if logging::on_pg_thread() {
                    logging::pg_log(PANIC, &msg);
                    // A stub rust_pg_log (CLI binaries) returns even at PANIC level.
                    std::process::abort();
                }
                poison(&msg);
                // Relayed for context; the main loop drains it before PANICing.
                logging::pg_log(PANIC, &msg);
                loop {
                    std::thread::park();
                }
            }
        }
    }
}

/// Spin-based atomic reader-writer lock for hash table partitions.
/// Lives in PG shared memory. Used instead of PG LWLocks because Tokio
/// threads also access the hash table and LWLocks require per-process state.
///
/// State encoding (i32):
///   -1 (EXCLUSIVE)       — write-locked
///   bit 30 set           — a writer is pending (blocks new readers)
///   bits 0-29            — reader count
///
/// The WRITER_PENDING bit prevents new readers from entering while a writer
/// is waiting for existing readers to drain, eliminating writer starvation
/// under sustained read traffic.
///
/// `owner_pid` names the write holder so [`SpinWatch`] in the spin loops can
/// liveness-check it: a dead holder escalates to a PANIC/crash-restart
/// rather than spinning forever.
#[repr(C)]
pub(crate) struct AtomicRWLock {
    state: AtomicI32,
    owner_pid: AtomicI32,
}

const EXCLUSIVE: i32 = -1;
const WRITER_PENDING: i32 = 0x4000_0000; // bit 30
const READER_MASK: i32 = 0x3FFF_FFFF; // bits 0-29

/// RAII read guard. Releases the read lock on drop.
#[must_use = "lock guard dropped immediately — use `let _guard = lock.read()` to hold it"]
pub(crate) struct ReadGuard<'a> {
    lock: &'a AtomicRWLock,
}

impl Drop for ReadGuard<'_> {
    fn drop(&mut self) {
        self.lock.state.fetch_sub(1, Ordering::Release);
    }
}

/// RAII write guard. Releases the write lock on drop.
#[must_use = "lock guard dropped immediately — use `let _guard = lock.write()` to hold it"]
pub(crate) struct WriteGuard<'a> {
    lock: &'a AtomicRWLock,
}

impl Drop for WriteGuard<'_> {
    fn drop(&mut self) {
        self.lock.owner_pid.store(0, Ordering::Relaxed);
        self.lock.state.store(0, Ordering::Release);
    }
}

impl AtomicRWLock {
    pub(crate) fn init(&self) {
        self.state.store(0, Ordering::Relaxed);
        self.owner_pid.store(0, Ordering::Relaxed);
    }

    /// PID of the write holder, 0 when not write-locked (or a momentary
    /// window between acquisition and the store — spinners re-check).
    pub(crate) fn owner_pid(&self) -> i32 {
        self.owner_pid.load(Ordering::Relaxed)
    }

    pub(crate) fn read(&self) -> ReadGuard<'_> {
        let mut watch = SpinWatch::new();
        loop {
            let s = self.state.load(Ordering::Relaxed);
            // Only attempt CAS when not write-locked and no writer is pending.
            if s >= 0
                && (s & WRITER_PENDING) == 0
                && self
                    .state
                    .compare_exchange_weak(s, s + 1, Ordering::Acquire, Ordering::Relaxed)
                    .is_ok()
            {
                break;
            }
            watch.tick("AtomicRWLock read", self.owner_pid());
            std::hint::spin_loop();
        }

        ReadGuard { lock: self }
    }

    pub(crate) fn write(&self) -> WriteGuard<'_> {
        let mut watch = SpinWatch::new();
        loop {
            // Fast path: unlocked → exclusive.
            if self
                .state
                .compare_exchange_weak(0, EXCLUSIVE, Ordering::Acquire, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }

            let s = self.state.load(Ordering::Relaxed);

            if s == EXCLUSIVE {
                // Another writer holds the lock.
                watch.tick("AtomicRWLock write", self.owner_pid());
                std::hint::spin_loop();
                continue;
            }

            // Set WRITER_PENDING to prevent new readers from entering.
            if (s & WRITER_PENDING) == 0 {
                let _ = self.state.compare_exchange_weak(
                    s,
                    s | WRITER_PENDING,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                );
                std::hint::spin_loop();
                continue;
            }

            // WRITER_PENDING is set — check if all readers have drained.
            if (s & READER_MASK) == 0
                && self
                    .state
                    .compare_exchange_weak(
                        WRITER_PENDING,
                        EXCLUSIVE,
                        Ordering::Acquire,
                        Ordering::Relaxed,
                    )
                    .is_ok()
            {
                break;
            }

            // Stuck here means readers never drained — no owner to name.
            watch.tick("AtomicRWLock write (reader drain)", 0);
            std::hint::spin_loop();
        }

        self.owner_pid.store(current_pid(), Ordering::Relaxed);
        WriteGuard { lock: self }
    }

    pub(crate) fn is_in_write(&self) -> bool {
        self.state.load(Ordering::Relaxed) == EXCLUSIVE
    }

    /// Non-blocking write lock attempt. Returns `Some(guard)` if the lock
    /// was acquired immediately, `None` if any other thread holds the lock
    /// (read or write) or a writer is pending.
    pub(crate) fn try_write(&self) -> Option<WriteGuard<'_>> {
        if self
            .state
            .compare_exchange(0, EXCLUSIVE, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
        {
            self.owner_pid.store(current_pid(), Ordering::Relaxed);
            Some(WriteGuard { lock: self })
        } else {
            None
        }
    }
}

fn current_pid() -> Pid {
    unsafe { libc::getpid() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn new_lock() -> AtomicRWLock {
        let lock = AtomicRWLock {
            state: AtomicI32::new(0),
            owner_pid: AtomicI32::new(0),
        };
        lock.init();
        lock
    }

    #[test]
    fn initial_state_is_unlocked() {
        let lock = new_lock();
        assert_eq!(lock.state.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn single_reader() {
        let lock = new_lock();
        let guard = lock.read();
        assert_eq!(lock.state.load(Ordering::Relaxed), 1);
        drop(guard);
        assert_eq!(lock.state.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn multiple_readers() {
        let lock = new_lock();
        let g1 = lock.read();
        let g2 = lock.read();
        let g3 = lock.read();
        assert_eq!(lock.state.load(Ordering::Relaxed), 3);
        drop(g1);
        drop(g2);
        drop(g3);
        assert_eq!(lock.state.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn write_lock_sets_exclusive() {
        let lock = new_lock();
        let guard = lock.write();
        assert_eq!(lock.state.load(Ordering::Relaxed), EXCLUSIVE);
        drop(guard);
        assert_eq!(lock.state.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn write_lock_excludes_other_writers() {
        let lock = Arc::new(new_lock());

        let guard = lock.write();
        let lock2 = Arc::clone(&lock);
        let handle = std::thread::spawn(move || {
            for _ in 0..1_000 {
                let won = lock2
                    .state
                    .compare_exchange(0, EXCLUSIVE, Ordering::Acquire, Ordering::Relaxed)
                    .is_ok();
                assert!(!won, "second writer acquired the lock while first held it");
                std::hint::spin_loop();
            }
        });
        handle.join().unwrap();
        drop(guard);
    }

    #[test]
    fn readers_block_writer() {
        let lock = new_lock();
        let _guard = lock.read();
        let result =
            lock.state
                .compare_exchange(0, EXCLUSIVE, Ordering::Acquire, Ordering::Relaxed);
        assert!(result.is_err(), "writer acquired lock while reader held it");
    }

    #[test]
    fn writer_blocks_readers() {
        let lock = new_lock();
        let _guard = lock.write();
        let state = lock.state.load(Ordering::Relaxed);
        assert!(state < 0, "expected state < 0 while writer holds lock");
    }

    #[test]
    fn writer_pending_blocks_new_readers() {
        let lock = new_lock();
        let _reader = lock.read();
        // Simulate a pending writer by setting the WRITER_PENDING bit.
        let prev = lock.state.fetch_or(WRITER_PENDING, Ordering::Relaxed);
        assert_eq!(prev, 1, "expected 1 reader before setting WRITER_PENDING");
        let s = lock.state.load(Ordering::Relaxed);
        assert_eq!(s & READER_MASK, 1);
        assert_ne!(s & WRITER_PENDING, 0);
        // read() checks (s >= 0 && (s & WRITER_PENDING) == 0); since
        // WRITER_PENDING is set, a new reader would spin — verify the condition.
        assert!(
            !(s >= 0 && (s & WRITER_PENDING) == 0),
            "new readers should be blocked when WRITER_PENDING is set"
        );
        lock.state.store(0, Ordering::Relaxed);
    }

    #[test]
    fn read_guard_auto_unlocks() {
        let lock = new_lock();
        {
            let _guard = lock.read();
            assert_eq!(lock.state.load(Ordering::Relaxed), 1);
        }
        assert_eq!(lock.state.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn write_guard_auto_unlocks() {
        let lock = new_lock();
        {
            let _guard = lock.write();
            assert_eq!(lock.state.load(Ordering::Relaxed), EXCLUSIVE);
        }
        assert_eq!(lock.state.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn write_lock_tracks_owner_pid() {
        let lock = new_lock();
        assert_eq!(lock.owner_pid(), 0);
        {
            let _guard = lock.write();
            assert_eq!(lock.owner_pid(), unsafe { libc::getpid() });
        }
        assert_eq!(lock.owner_pid(), 0, "drop clears the owner");
    }

    #[test]
    fn try_write_tracks_owner_pid() {
        let lock = new_lock();
        {
            let guard = lock.try_write().expect("uncontended try_write");
            assert_eq!(lock.owner_pid(), unsafe { libc::getpid() });
            drop(guard);
        }
        assert_eq!(lock.owner_pid(), 0);
    }

    #[test]
    fn concurrent_readers_and_writer() {
        use std::sync::Barrier;

        let lock = Arc::new(new_lock());
        let barrier = Arc::new(Barrier::new(5));

        let readers: Vec<_> = (0..4)
            .map(|_| {
                let lock = Arc::clone(&lock);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    for _ in 0..1_000 {
                        let _guard = lock.read();
                        let s = lock.state.load(Ordering::Relaxed);
                        assert!(s > 0 && s != EXCLUSIVE);
                    }
                })
            })
            .collect();

        let lock_w = Arc::clone(&lock);
        let barrier_w = Arc::clone(&barrier);
        let writer = std::thread::spawn(move || {
            barrier_w.wait();
            for _ in 0..200 {
                let _guard = lock_w.write();
                assert_eq!(lock_w.state.load(Ordering::Relaxed), EXCLUSIVE);
            }
        });

        for r in readers {
            r.join().unwrap();
        }
        writer.join().unwrap();

        assert_eq!(lock.state.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn decide_waits_below_warn_threshold() {
        for owner in [None, Some(true), Some(false)] {
            assert_eq!(
                Action::decide(Duration::from_secs(5), owner, false),
                Action::Continue
            );
        }
    }

    #[test]
    fn decide_escalates_on_dead_owner_past_warn() {
        assert_eq!(
            Action::decide(WARN_AFTER, Some(false), false),
            Action::Escalate,
            "dead owner escalates as soon as the warn window passes"
        );
        assert_eq!(
            Action::decide(Duration::from_secs(120), Some(false), true),
            Action::Escalate
        );
    }

    #[test]
    fn decide_warns_once_for_live_holder() {
        assert_eq!(Action::decide(WARN_AFTER, Some(true), false), Action::Warn);
        assert_eq!(Action::decide(WARN_AFTER, None, false), Action::Warn);
        assert_eq!(
            Action::decide(Duration::from_secs(120), Some(true), true),
            Action::Continue,
            "already warned, long timeout not reached"
        );
    }

    #[test]
    fn decide_escalates_at_long_timeout() {
        assert_eq!(
            Action::decide(ESCALATE_AFTER, Some(true), true),
            Action::Escalate
        );
        assert_eq!(Action::decide(ESCALATE_AFTER, None, true), Action::Escalate);
        assert_eq!(
            Action::decide(ESCALATE_AFTER - Duration::from_secs(1), Some(true), true),
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
