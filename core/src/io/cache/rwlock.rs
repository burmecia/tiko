use std::sync::atomic::{AtomicI32, Ordering};

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
#[repr(C)]
pub(crate) struct AtomicRWLock {
    state: AtomicI32,
}

const EXCLUSIVE: i32 = -1;
const WRITER_PENDING: i32 = 0x4000_0000; // bit 30
const READER_MASK: i32 = 0x3FFF_FFFF; // bits 0-29

/// RAII read guard. Releases the read lock on drop.
#[must_use = "lock guard dropped immediately — use `let _guard = lock.read()` to hold it"]
pub(super) struct ReadGuard<'a> {
    lock: &'a AtomicRWLock,
}

impl Drop for ReadGuard<'_> {
    fn drop(&mut self) {
        self.lock.state.fetch_sub(1, Ordering::Release);
    }
}

/// RAII write guard. Releases the write lock on drop.
#[must_use = "lock guard dropped immediately — use `let _guard = lock.write()` to hold it"]
pub(super) struct WriteGuard<'a> {
    lock: &'a AtomicRWLock,
}

impl Drop for WriteGuard<'_> {
    fn drop(&mut self) {
        self.lock.state.store(0, Ordering::Release);
    }
}

impl AtomicRWLock {
    pub(super) fn init(&self) {
        self.state.store(0, Ordering::Relaxed);
    }

    pub(super) fn read(&self) -> ReadGuard<'_> {
        loop {
            let s = self.state.load(Ordering::Relaxed);
            // Only attempt CAS when not write-locked and no writer is pending.
            if s >= 0 && (s & WRITER_PENDING) == 0 {
                if self
                    .state
                    .compare_exchange_weak(s, s + 1, Ordering::Acquire, Ordering::Relaxed)
                    .is_ok()
                {
                    break;
                }
            }
            std::hint::spin_loop();
        }

        ReadGuard { lock: self }
    }

    pub(super) fn write(&self) -> WriteGuard<'_> {
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
            if (s & READER_MASK) == 0 {
                if self
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
            }

            std::hint::spin_loop();
        }

        WriteGuard { lock: self }
    }

    pub(super) fn is_in_write(&self) -> bool {
        self.state.load(Ordering::Relaxed) == EXCLUSIVE
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn new_lock() -> AtomicRWLock {
        let lock = AtomicRWLock {
            state: AtomicI32::new(0),
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
}
