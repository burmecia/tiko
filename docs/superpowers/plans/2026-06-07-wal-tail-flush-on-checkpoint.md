# WAL Tail Flush on Checkpoint Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `wal_receiver` flush its buffered partial WAL tail to storage whenever a PostgreSQL checkpoint completes, so archived WAL keeps up with the head (enabling PITR to recent points).

**Architecture:** The checkpoint (in the checkpointer process) already bumps `IoControl.timeline.generation` (an `AtomicU64`) on every commit. The `wal_receiver` Tokio task (in the separate `libtikoworker` worker process) self-polls that atomic on each message-loop iteration; when it advances and a buffered tail exists, the receiver PUTs the partial tail at its offset — without consuming the segment or advancing `chunks_uploaded`, so the next full-chunk PUT/seal at that offset supersedes it. No changes outside `wal_receiver.rs`.

**Tech Stack:** Rust (edition 2024), tokio, the project's `core` shared-memory `IoControl`/`TimelineState`.

**Reference spec:** `docs/superpowers/specs/2026-06-07-wal-tail-flush-on-checkpoint-design.md`

**Conventions (project memory / CLAUDE.md):**
- Build: `cargo build -p worker` must succeed cleanly.
- Tests: `cargo test -p worker` (the `worker` crate links as `cdylib`+`rlib` with `-undefined dynamic_lookup`; pure tests that don't call PG symbols run fine — verified).
- `cargo clippy` is blocked by pre-existing `pgsys` lint errors (unrelated); verify lint-cleanliness via a warning-free build.
- Commit after each task. Branch `pitr2` (already checked out). Commit messages end with: `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`

**Verified facts (rely on these; if one is wrong, report BLOCKED with the exact compiler error):**
- `core::io_control::IoControl` (re-exported via `core`'s `pub use io::{cache, io_control};`) has `pub fn try_get() -> Option<&'static Self>` and `pub timeline: TimelineState`.
- `core::io::timeline::TimelineState` has `pub generation: AtomicU64`.
- `wal_receiver.rs` already imports `core::io::store::Store`, `pgsys::timeline_id::TimelineId`, `pgsys::common::XLOG_SEG_SIZE`; defines `struct SegState { seg_no: u64, buf: Vec<u8>, chunks_uploaded: usize, chunk_tasks, partial }`; `type BoxError = Box<dyn std::error::Error + Send + Sync>`; helpers `wal_seg_name(timeline_id, seg_no) -> String` and `sim.locator().wal_chunk_key(timeline_id, &name, offset)`.
- `worker` depends on `core` (path `../core`) and `tokio`.

---

### Task 1: Add the pure `should_flush_tail` gate (TDD)

**Files:**
- Modify: `worker/src/tasks/wal_receiver.rs` (add a module-scope fn + a `#[cfg(test)] mod tests`)

- [ ] **Step 1: Write the failing test**

Append to the END of `worker/src/tasks/wal_receiver.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::should_flush_tail;

    #[test]
    fn should_flush_tail_gate() {
        assert!(should_flush_tail(1, 2, true)); // generation advanced + tail present
        assert!(!should_flush_tail(1, 2, false)); // advanced but nothing buffered
        assert!(!should_flush_tail(2, 2, true)); // no checkpoint since last flush
        assert!(!should_flush_tail(2, 2, false)); // unchanged + nothing buffered
    }
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p worker should_flush_tail_gate`
Expected: FAIL — `cannot find function 'should_flush_tail'`.

- [ ] **Step 3: Implement the gate**

In `worker/src/tasks/wal_receiver.rs`, add this at module scope (a good spot is just before the `// ── Utility` section, i.e. just before `fn wal_seg_name`):
```rust
/// Decide whether the checkpoint-triggered path should flush the WAL tail now.
///
/// Flush only when the shared-memory `generation` advanced since our last flush
/// (a checkpoint — or compaction — committed) AND there is buffered tail to push.
fn should_flush_tail(last_gen: u64, cur_gen: u64, has_tail: bool) -> bool {
    cur_gen != last_gen && has_tail
}
```

- [ ] **Step 4: Run the test**

Run: `cargo test -p worker should_flush_tail_gate`
Expected: PASS (1 passed).

- [ ] **Step 5: Commit**

```bash
git add worker/src/tasks/wal_receiver.rs
git commit -m "feat(worker): add should_flush_tail gate for checkpoint WAL flush"
```

(A `dead_code` warning for `should_flush_tail` under the plain `cargo build -p worker` is expected until Task 2 wires it in — do not add `#[allow(dead_code)]`; the test references it so `cargo test -p worker` is clean.)

---

### Task 2: Flush the tail when the checkpoint generation advances

Add the generation reader, the non-consuming tail flush, the per-iteration check, and wire it into the streaming message loop.

**Files:**
- Modify: `worker/src/tasks/wal_receiver.rs`

- [ ] **Step 1: Add imports**

In `worker/src/tasks/wal_receiver.rs`, add these two `use` lines alongside the existing imports near the top (after the existing `use core::io::store::Store;` line):
```rust
use core::io_control::IoControl;
use std::sync::atomic::Ordering;
```

- [ ] **Step 2: Add the generation reader + tail flush + per-iteration check**

Add these three functions at module scope. A good spot is immediately after `flush_partial_tail` (and before `join_chunks_and_flush_tail`), so the tail-flush helpers sit together:
```rust
/// Read the shared-memory checkpoint/commit generation. Returns 0 when
/// `IoControl` is not attached yet (very early startup) — treated as "no
/// checkpoint observed".
fn current_generation() -> u64 {
    IoControl::try_get()
        .map(|c| c.timeline.generation.load(Ordering::Acquire))
        .unwrap_or(0)
}

/// Checkpoint-triggered tail flush: if the shared-memory `generation` advanced
/// since `last_flushed_generation` and the current segment has buffered tail,
/// PUT that tail now. Always advances `last_flushed_generation` to the observed
/// value so an unchanged generation isn't re-evaluated.
async fn maybe_flush_on_checkpoint(
    cur_seg: &Option<SegState>,
    sim: &'static Store,
    timeline_id: TimelineId,
    last_flushed_generation: &mut u64,
) -> Result<(), BoxError> {
    let cur = current_generation();
    let has_tail = cur_seg
        .as_ref()
        .is_some_and(|s| s.buf.len() > s.chunks_uploaded);
    if should_flush_tail(*last_flushed_generation, cur, has_tail) {
        // has_tail implies cur_seg is Some.
        flush_tail_now(cur_seg.as_ref().unwrap(), sim, timeline_id).await?;
    }
    *last_flushed_generation = cur;
    Ok(())
}

/// PUT the current partial tail (`buf[chunks_uploaded..]`) at its byte offset,
/// WITHOUT consuming the segment or advancing `chunks_uploaded`.
///
/// Used by the checkpoint-triggered flush so archived WAL keeps up with the
/// head. Because `chunks_uploaded` is unchanged, the streaming path still owns
/// that offset: when the buffer later fills a full 256 KiB window (or the
/// segment is sealed), the normal PUT at the same key overwrites this partial
/// object. `tiko_restore` takes the latest object per offset, so reads stay
/// consistent. No-op if there is no buffered tail.
async fn flush_tail_now(
    state: &SegState,
    sim: &'static Store,
    timeline_id: TimelineId,
) -> Result<(), BoxError> {
    if state.buf.len() <= state.chunks_uploaded {
        return Ok(());
    }
    let name = wal_seg_name(timeline_id, state.seg_no);
    let offset = state.chunks_uploaded;
    let tail = state.buf[offset..].to_vec();
    let key = sim.locator().wal_chunk_key(timeline_id, &name, offset);
    tokio::task::spawn_blocking(move || sim.storage_put(&key, &tail))
        .await
        .map_err(|e| format!("checkpoint tail flush spawn_blocking panicked for {name}: {e}"))?
        .map_err(|e| format!("checkpoint tail PUT failed for {name}: {e}"))?;
    Ok(())
}
```

- [ ] **Step 3: Initialize the generation marker before the message loop**

In `run_streaming`, replace this block (around lines 227-229):
```rust
    // ── Message loop ──────────────────────────────────────────────────────────
    let mut confirmed_lsn: u64 = start_lsn;
    let mut cur_seg: Option<SegState> = None;
```
with:
```rust
    // ── Message loop ──────────────────────────────────────────────────────────
    let mut confirmed_lsn: u64 = start_lsn;
    let mut cur_seg: Option<SegState> = None;
    // Last checkpoint/commit generation we flushed the tail at. Initialized to
    // the current value so we don't flush spuriously right after connecting.
    let mut last_flushed_generation: u64 = current_generation();
```

- [ ] **Step 4: Call the check at the end of each loop iteration**

In `run_streaming`, the loop ends with the match's closing brace then the loop's closing brace:
```rust
            Err(_timeout) => {
                // No message — send proactive keepalive.
                send_standby_status(&mut conn, confirmed_lsn).await?;
            }
        }
    }
}
```
Insert the checkpoint-flush call between the match's closing `}` and the loop's closing `}`:
```rust
            Err(_timeout) => {
                // No message — send proactive keepalive.
                send_standby_status(&mut conn, confirmed_lsn).await?;
            }
        }

        // Checkpoint-triggered tail flush: if a checkpoint advanced the shared
        // generation since our last flush, push whatever WAL tail we've buffered
        // so archived WAL tracks the head. Cheap atomic read on a hit-or-miss
        // basis; the actual PUT only fires when there is a tail.
        maybe_flush_on_checkpoint(&cur_seg, sim, timeline_id, &mut last_flushed_generation)
            .await?;
    }
}
```
(The `CopyDone` arm `return`s before reaching this, so shutdown still flushes via `flush_partial_tail`; the `Ok(Err)` arm `return`s too. The empty-message `continue` at the top of the `'w'`/Some arm intentionally skips the check — nothing changed.)

- [ ] **Step 5: Build**

Run: `cargo build -p worker`
Expected: clean build, no warnings (the Task 1 `should_flush_tail` `dead_code` warning is now resolved — it's used by `maybe_flush_on_checkpoint`).

- [ ] **Step 6: Run tests**

Run: `cargo test -p worker`
Expected: PASS (the `should_flush_tail_gate` test plus the 0 pre-existing).

- [ ] **Step 7: Commit**

```bash
git add worker/src/tasks/wal_receiver.rs
git commit -m "feat(worker): flush WAL tail on checkpoint generation advance"
```

---

### Task 3: Full build + test gate

**Files:** none (verification only)

- [ ] **Step 1: Build the worker crate**

Run: `cargo build -p worker`
Expected: clean, no warnings.

- [ ] **Step 2: Build the wider workspace touched crates**

Run: `cargo build -p core -p worker -p cli`
Expected: clean (this change is confined to `worker`, but confirm nothing else broke).

- [ ] **Step 3: Run worker tests**

Run: `cargo test -p worker`
Expected: `should_flush_tail_gate` passes; no failures.

- [ ] **Step 4: Confirm no warnings introduced**

Run: `cargo build -p worker 2>&1 | grep -c warning`
Expected: `0`.

---

## Notes for the implementer

- **Integration testing (out of band):** the end-to-end behavior — run a workload, let a checkpoint fire, confirm the WAL archive (`$TIKO_ROOT/s3sim/{org}/{db}/wal/...`) grows to include the post-last-chunk tail, and `tiko_pitr` recovery to a recent time succeeds — is verified separately against a live instance, per project convention. `wal_receiver.rs` has no in-process integration tests.
- **Why non-blocking / flush-what-you-have:** the receiver lags the head by a small streaming delay, so a checkpoint flush may capture slightly less than the checkpoint LSN; the next checkpoint's flush closes the gap. This is intentional (see spec) and avoids coupling archiving latency to streaming lag or blocking the checkpointer.
- **Overwrite semantics:** re-flushing the tail at the same offset on successive checkpoints rewrites a growing (<256 KiB) partial chunk until the streaming path's full-chunk PUT (or `seal_segment`) supersedes it at that key. Do not advance `chunks_uploaded` in `flush_tail_now`.
