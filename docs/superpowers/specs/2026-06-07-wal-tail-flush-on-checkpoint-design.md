# WAL tail flush on checkpoint

**Date:** 2026-06-07
**Status:** Approved (design)

## Goal

Make the `wal_receiver` flush its buffered partial WAL tail to storage when a
PostgreSQL checkpoint completes, so archived WAL keeps up with the head instead
of lagging until a 256 KiB chunk fills, a segment switch, or a clean shutdown.

## Background / problem

`wal_receiver` (`worker/src/tasks/wal_receiver.rs`) streams physical WAL and
buffers it per segment in memory, PUTting a 256 KiB chunk object only once a
full 256 KiB window accumulates. The buffered remainder (the "tail") is flushed
to storage only on:
- a segment switch (`seal_segment`), or
- a clean shutdown / `CopyDone` (`flush_partial_tail`).

Consequently, on a low/moderate-activity database, recently-streamed WAL can sit
unarchived for a long time (up to a full 256 KiB of activity, a 16 MiB segment,
or until shutdown). This breaks PITR to recent points: base manifests are
anchored at checkpoints, but the WAL covering a recent base's redo→target span
may not be in storage yet. (Observed concretely: an archive containing a single
256 KiB chunk while six base manifests sat outside it.)

PostgreSQL solves the analogous problem with `archive_timeout` (force a segment
switch periodically). Here we tie the flush to **checkpoint completion**, which
is exactly where base manifests anchor, and is activity-driven (no flushing when
nothing is happening) — consistent with tiko's checkpoint-count retention model.

## Constraints / context

- The checkpoint runs in the **checkpointer** process (`tiko_perform_checkpoint`
  → `Store::run_commit_protocol`), which flushes dirty chunks and, under the
  timeline write lock, updates `IoControl.timeline.head_ckpt` / `redo_ckpt` and
  bumps `IoControl.timeline.generation` (`AtomicU64`, Release).
- `wal_receiver` is a Tokio task in a **different** process (the `libtikoworker`
  background worker). So, unlike dirty-chunk flush (a direct in-process call),
  the receiver cannot be called directly by the checkpointer — it must observe a
  shared-memory signal.
- Per the project's thread-safety rules, Tokio tasks **may** read/write
  shared-memory atomics (but not `LWLock`/`ereport`/`palloc`). Reading
  `generation` (an atomic) from the receiver task is therefore safe.

## Design decisions (resolved during brainstorming)

1. **Trigger:** the receiver **self-polls** `IoControl.timeline.generation` on
   its existing select-loop ticks; when it advances, flush the tail.
   Fully self-contained in `wal_receiver.rs` — no change to the checkpoint path
   or the worker main loop.
2. **Flush semantics:** **flush whatever is buffered, non-blocking.** PUT the
   current partial tail at its offset without consuming the segment or blocking
   the checkpointer. The small streaming lag self-heals on the next checkpoint;
   no attempt to guarantee "flushed exactly up to the checkpoint LSN."

## Mechanism

In `run_streaming` (`wal_receiver.rs`):

- Add a local `last_flushed_generation: u64`, initialized at stream start to the
  current `IoControl::try_get().map(|c| c.timeline.generation.load(Acquire))`
  (defaulting to 0 if `IoControl` is unavailable). Initializing to the current
  value avoids a spurious flush immediately on connect.
- On **each iteration** of the message loop, after handling the `recv_copy_data`
  outcome for the `'w'` (XLogData) and `'k'` (keepalive) / timeout cases — i.e.
  the cases that fall through and continue looping, **not** the `CopyDone`/`None`
  case (which already calls `flush_partial_tail` and returns) — check the
  generation:
  - `cur = IoControl::try_get().map(|c| c.timeline.generation.load(Acquire)).unwrap_or(last_flushed_generation)`
  - If `cur != last_flushed_generation` **and** the current segment has an
    unflushed tail (`buf.len() > chunks_uploaded`), flush the tail (below) and
    set `last_flushed_generation = cur`.
  - If `cur != last_flushed_generation` but there is no tail, still set
    `last_flushed_generation = cur` (nothing to flush; avoid rechecking).

Latency: checked on every WAL message (near-immediate after a checkpoint on a
busy DB) and at least every `KEEPALIVE_INTERVAL` (10 s) when idle. No new timer.

Note: `generation` is also bumped by compaction (`set_base_ckpt`), so the
receiver may occasionally flush when no new WAL-relevant checkpoint occurred —
a harmless extra PUT of the (possibly unchanged) tail.

## The tail flush

A new helper, e.g. `flush_tail_now(state: &SegState, sim, timeline_id)`, that:

```
if state.buf.len() > state.chunks_uploaded {
    let tail = state.buf[state.chunks_uploaded..].to_vec();
    let key  = wal_chunk_key(timeline_id, seg_name, state.chunks_uploaded);
    storage_put(key, tail)   // via spawn_blocking, awaited
}
```

- Takes `&SegState` (read-only): it does **not** consume the segment and does
  **not** advance `chunks_uploaded`.
- Because `chunks_uploaded` is unchanged, the streaming path still owns that
  offset: when the buffer later fills a full 256 KiB window, the normal
  full-chunk PUT **overwrites** the partial tail at the same key and advances
  `chunks_uploaded`. Re-flushing on successive checkpoints simply rewrites a
  growing (<256 KiB) partial chunk at the same offset until it is superseded.
  `tiko_restore` already takes the latest object per offset, so reads stay
  consistent.
- Does **not** join in-flight full-chunk PUTs: those cover earlier, already-full
  windows at lower offsets, so there is no key collision with the tail offset.
- The PUT is awaited (like the existing `join_chunks_and_flush_tail` tail PUT) so
  errors propagate to the reconnect loop. Storage PUT is local-FS today (fast);
  a brief pause in WAL reception is acceptable (the walsender buffers).

This also benefits the mid-stream **partial** first segment (the one that began
mid-segment and is never sealed): its tail now lands at each checkpoint rather
than only on shutdown.

### Relationship to existing tail logic

`seal_segment` and `flush_partial_tail` keep their current behavior. The shared
`join_chunks_and_flush_tail` already PUTs `buf[chunks_uploaded..]` at
`chunks_uploaded`; `flush_tail_now` is the non-consuming, non-joining,
mid-stream counterpart. If convenient, the tail-PUT body can be factored so both
share one small function that PUTs `buf[chunks_uploaded..]` at its offset; this
is an implementation detail, not a requirement.

## Error handling

- A failed tail PUT returns `Err` from `run_streaming`, which the
  `wal_receiver_task` loop logs and retries with backoff. No effect on the
  checkpointer (separate process); the checkpoint already committed.
- `IoControl` unavailable (e.g. very early startup) → `try_get()` is `None` →
  treat generation as unchanged → no flush. Harmless.

## Testing

- **Unit test** a small pure gate extracted for the decision:
  `should_flush_tail(last_gen: u64, cur_gen: u64, has_tail: bool) -> bool`
  returning `cur_gen != last_gen && has_tail`. Cases: advanced+tail → true;
  advanced+no-tail → false; unchanged+tail → false.
- The actual flush and loop integration are integration-verified via the PITR
  end-to-end test (run a workload, force/await a checkpoint, confirm the WAL
  archive grows to include the post-last-chunk tail, and recovery to a recent
  time succeeds), consistent with the rest of `wal_receiver.rs`, which has no
  unit tests.

## Out of scope

- A time-based (`archive_timeout`-style) flush independent of checkpoints. PG
  checkpoints occur at least every `checkpoint_timeout` even when idle, bounding
  staleness; a pure timer is a separate enhancement if needed later.
- Any guarantee that archived WAL reaches a specific checkpoint LSN before that
  checkpoint is considered durable (we flush what's buffered).
- Changes to the checkpointer / `run_commit_protocol` / worker main loop.
- Sealing partial segments as complete objects (unchanged — still chunks-only).
