# PITR Design — Point-in-Time Recovery for Pico

Pico's S3-backed storage gives it a structural advantage over standard
PostgreSQL for PITR: all relation data already flows through the smgr and
lands in S3. The only state outside Pico's control is a small set of
non-smgr files. This makes a full base backup unnecessary.

## Why Standard PostgreSQL PITR Does Not Apply Directly

Standard PostgreSQL PITR requires two things:

1. A **base backup** — a consistent snapshot of all data files at a known LSN
2. **WAL segments** from that backup's LSN forward to the recovery target

WAL records describe changes to pages that already exist. They do not
recreate pages from nothing. An empty `initdb` instance has none of the
relation pages from a past cluster, so WAL replay fails immediately — even
with `full_page_writes = on`, the first modification of a page only contains
the page image relative to the last checkpoint, not from the dawn of time.

## Pico's Structural Advantage

In Pico, all relation data (including system catalogs in `base/` and
`global/`) flows through the smgr and is stored in S3-backed chunk objects.
The only state that does **not** go through Pico is:

| File/Directory | Size | Content |
|---|---|---|
| `pg_control` | 8 KB | Checkpoint LSN, timeline, catalog version |
| `pg_xact/` | ~MB | Commit log (transaction status bits) |
| `pg_multixact/` | ~MB | Multixact state |
| `pg_filenode.map`, `global/pg_filenode.map` | KB | OID→filenode mapping |
| `pg_subtrans/` | small | Subtransaction state |

Archiving this non-smgr state at each checkpoint costs only a few megabytes.
The relation blocks themselves do not need to be copied — they already live
in S3. This replaces the traditional base backup entirely.

## Chosen Design: Append-Only LSN-Keyed Chunks + Delta Manifests

### Core Principle

Chunk objects in S3 are **never overwritten**. Each checkpoint flush writes
dirty chunks to new, immutable S3 objects whose keys embed the checkpoint
LSN. Historical versions accumulate naturally; GC removes them after the
retention window.

### S3 Layout

```
pitr/
  bases/
    {checkpoint_lsn}/
      manifest.json          ← full chunk_key → lsn_hex map (materialized)
      pg_control             ← 8 KB
      pg_xact/{segments}     ← CLOG segments
      pg_multixact/{files}   ← multixact state
      pg_filenode.map        ← OID mappings
  deltas/
    {checkpoint_lsn}/
      delta.json             ← dirty chunks at this checkpoint only
      pg_control             ← per-checkpoint non-smgr state
      pg_xact/{segments}
      pg_multixact/{files}
      pg_filenode.map

chunks/
  {spc_oid}/{db_oid}/{rel_number}.{fork}/{chunk_id}/
    {lsn_A_hex}             ← version written at checkpoint A
    {lsn_C_hex}             ← version written at checkpoint C
    ...                     ← latest version always kept; old versions GC'd
```

### Delta Manifests (Written at Each Checkpoint)

The checkpointer calls `s3_checkpoint_flush()` which already visits every
dirty chunk via `flush_all_dirty_chunks()`. As a side effect, it emits a
delta manifest containing only the chunks flushed at this checkpoint:

```json
{
  "checkpoint_lsn": "0/3A000028",
  "prev_checkpoint_lsn": "0/38000010",
  "timestamp": 1740480000,
  "dirty_chunks": {
    "1663/16384/16385.0/0": "0/3A000028",
    "1663/16384/16387.0/2": "0/3A000028"
  }
}
```

Each delta is **immutable once written** — the checkpointer never modifies it
after the PUT. This means the base materializer (see below) can read deltas
concurrently with the checkpointer writing new ones without any coordination.

Delta manifests are small. For a 100 GB database (≈400K chunks) with 1% of
chunks written per checkpoint interval, each delta is ≈200 KB vs ≈20 MB for
a full snapshot manifest.

### Who Writes Delta Manifests

The checkpointer process calls `s3_checkpoint_flush()` in s3smgr. This runs
outside s3worker (which is dead during the shutdown checkpoint). Therefore
delta manifests are written using a **lightweight blocking S3 client** in the
checkpointer process directly — mirroring the same fallback logic used for
`s3_ops` sync writes. This keeps correctness during shutdown without
depending on s3worker being alive.

## Rolling Base Materialization (Background Task in s3worker)

### Purpose

The rolling base is a **recovery speed optimization**, not a retention
boundary. It reduces the number of deltas that must be merged at recovery
time. It does not delete deltas within the retention window — those are
needed to support recovery to any arbitrary point in time.

### Task Structure

Spawned as a Tokio async task at s3worker startup. It performs only S3 I/O
and needs no PG process-local state, making it safe to run in the Tokio
thread pool.

```rust
// s3worker/src/pitr_task.rs

pub async fn pitr_background_task(s3: Arc<S3Client>, config: PitrConfig) {
    let mut interval = tokio::time::interval(config.materialization_interval);

    loop {
        interval.tick().await;

        if let Err(e) = materialize_base(&s3, &config).await {
            log_warning("pitr: base materialization failed: {e}");
            // non-fatal — deltas still exist, recovery still works
        }

        if let Err(e) = enforce_retention(&s3, &config).await {
            log_warning("pitr: retention GC failed: {e}");
        }
    }
}

async fn materialize_base(s3: &S3Client, config: &PitrConfig) -> Result<()> {
    // 1. Load current base manifest (if any)
    let base = fetch_latest_base(s3, config).await?;
    let base_lsn = base.as_ref().map(|b| b.checkpoint_lsn);

    // 2. Fetch all delta manifests newer than the current base
    let deltas = fetch_deltas_since(s3, config, base_lsn).await?;
    if deltas.is_empty() {
        return Ok(());
    }

    // 3. Merge: base.chunks + each delta in LSN order (later LSN wins)
    let mut merged: HashMap<ChunkKey, LsnHex> =
        base.map(|b| b.chunks).unwrap_or_default();
    for delta in &deltas {
        merged.extend(delta.dirty_chunks.clone());
    }

    let new_base = BaseManifest {
        checkpoint_lsn: deltas.last().unwrap().checkpoint_lsn,
        timestamp: now_unix(),
        chunks: merged,
    };

    // 4. Write new base atomically (single S3 PUT)
    put_base_manifest(s3, config, &new_base).await?;

    // NOTE: deltas are NOT deleted here — they remain for arbitrary-point
    // recovery within the retention window. Only enforce_retention() deletes
    // objects, and only beyond the 7-day cutoff.
    Ok(())
}
```

### Retention Enforcement

```rust
async fn enforce_retention(s3: &S3Client, config: &PitrConfig) -> Result<()> {
    let cutoff = lsn_older_than(config.retention_days); // e.g. 7 days

    // Delete base manifests, delta manifests, and non-smgr archives
    // older than the retention window
    delete_base_manifests_older_than(s3, config, cutoff).await?;
    delete_delta_manifests_older_than(s3, config, cutoff).await?;

    // Delete chunk LSN-keyed objects that are:
    //   - older than the retention window (lsn < cutoff)
    //   - NOT the latest version of their chunk key
    //
    // The latest version of every chunk is ALWAYS kept regardless of age
    // (it is live data, not history).
    gc_chunk_versions(s3, config, cutoff).await?;

    Ok(())
}
```

### Crash Safety

- The new base manifest is a single S3 PUT — atomic from the reader's perspective.
- Deltas are not deleted during materialization, so a crash mid-task leaves
  the previous base and all deltas intact.
- On restart, the task re-reads the latest confirmed base and re-merges any
  deltas written since it. Re-merging already-merged deltas is idempotent.

## Recovery to Any Point in Time Within the Retention Window

### Recovery Granularity

The granularity of recoverable points is the **checkpoint interval** (default
≈5 minutes), not the base materialization interval (e.g., 1–4 hours). The
rolling base only affects how many deltas must be merged at recovery time.

### Timeline Illustration

```
Base-A          Base-E (materialized by background task)
  |    δB  δC  δD  δE  |    δF  δG
──●────●───●───●───●───●────●───●──→ time
              ↑                 ↑
         recover here      recover here
         (LSN D)            (LSN G)
```

Recovery to **LSN D** (between Base-A and Base-E):
1. Find the latest base with `base_lsn ≤ D` → Base-A
2. Merge Base-A.chunks + δB + δC + δD (apply in LSN order)
3. Result: `chunk_key → lsn_hex` for every chunk at LSN D

Recovery to **LSN G** (after Base-E):
1. Find the latest base with `base_lsn ≤ G` → Base-E ← **fewer deltas to merge**
2. Merge Base-E.chunks + δF + δG
3. Result: `chunk_key → lsn_hex` for every chunk at LSN G

### Step-by-Step Recovery Procedure

Given an empty `initdb` instance and a target time T:

**Step 1 — Identify target checkpoint**

Search `pitr/deltas/` for the latest checkpoint with `timestamp ≤ T`.
Call its LSN `target_lsn`.

**Step 2 — Build chunk map at target_lsn**

```
latest_base = latest base with base_lsn ≤ target_lsn
deltas      = all deltas with delta_lsn in (latest_base.lsn, target_lsn]
chunk_map   = latest_base.chunks
for delta in deltas sorted by lsn:
    chunk_map.extend(delta.dirty_chunks)
```

`chunk_map` now gives: `chunk_key → lsn_hex` for every chunk at `target_lsn`.

**Step 3 — Restore non-smgr state**

Download from `pitr/deltas/{target_lsn}/`:
- `pg_control` → `$PGDATA/global/pg_control`
- `pg_xact/*` → `$PGDATA/pg_xact/`
- `pg_multixact/*` → `$PGDATA/pg_multixact/`
- `pg_filenode.map` → `$PGDATA/pg_filenode.map`

This tells PostgreSQL: "I am at LSN `target_lsn`, WAL replay starts here."

**Step 4 — Configure WAL recovery**

```ini
# postgresql.conf
restore_command = 'pico_restore %f %p'
recovery_target_time = '2026-02-20 10:00:00'
recovery_target_action = 'promote'
```

```bash
touch $PGDATA/recovery.signal
```

**Step 5 — Signal s3worker to use chunk_map**

Write `chunk_map` to a well-known path (e.g., `$PGDATA/pico_recovery_manifest.json`)
before starting PostgreSQL. On startup, s3worker detects `recovery.signal`
and loads this manifest. In recovery mode, block reads resolve `chunk_key`
to the specific `lsn_hex` from the manifest rather than the latest S3 object.

**Step 6 — Start PostgreSQL**

```
PostgreSQL reads pg_control → "last checkpoint at target_lsn"
WAL recovery begins, fetching WAL segments via restore_command
  For each WAL record, buffer manager reads a page:
    → s3worker in recovery mode: looks up chunk_map[chunk_key]
    → fetches chunks/{key}/{lsn_hex} from S3 (the checkpoint-era version)
    → returns page at checkpoint state
  WAL record is applied → s3_writev writes updated page to S3 (new version)
  ... repeat until WAL hits target time T
PostgreSQL promotes, recovery complete
```

**Step 7 — Post-recovery**

After promotion, s3worker exits recovery mode. New writes go to new LSN-keyed
S3 objects. The recovery manifest is removed.

## What Needs to Be Built

### New Files

| File | Purpose |
|---|---|
| `s3worker/src/pitr_task.rs` | Background Tokio task: base materialization + GC |
| `s3worker/src/manifest.rs` | `DeltaManifest` / `BaseManifest` types + merge logic |
| `s3worker/src/s3_client.rs` | S3 client initialization (shared by pitr_task and io_handler) |
| `s3worker/src/bin/pico_restore.rs` | WAL restore command binary |
| `s3smgr/src/wal_archive.rs` | Blocking S3 client for checkpointer-side delta + WAL writes |

### Modified Files

| File | Change |
|---|---|
| `s3worker/src/thread_pool.rs` | Spawn `pitr_background_task` after Tokio runtime starts |
| `s3worker/src/s3_ops.rs` | `write_blocks` writes to `chunks/{key}/{lsn_hex}` (append-only); add recovery-mode read path |
| `s3worker/src/lib.rs` | Export `manifest`, `pitr_task` modules |
| `s3smgr/src/checkpoint.rs` | After `flush_all_dirty_chunks`, write delta manifest + non-smgr files to S3 |
| `postgres/src/backend/access/transam/xlog.c` | Call `s3_archive_checkpoint_state()` from `CheckPointGuts()` |

### S3 Lifecycle Policy (7-Day Retention)

GC is handled by `enforce_retention()` in the background task. As a safety
net, configure S3 lifecycle rules to expire objects beyond the retention
window even if GC lags:

```json
{
  "Rules": [
    {
      "Id": "ExpireOldWAL",
      "Status": "Enabled",
      "Filter": { "Prefix": "wal/" },
      "Expiration": { "Days": 8 }
    },
    {
      "Id": "ExpireOldPitrManifests",
      "Status": "Enabled",
      "Filter": { "Prefix": "pitr/" },
      "Expiration": { "Days": 8 }
    },
    {
      "Id": "ExpireOldChunkVersions",
      "Status": "Enabled",
      "Filter": { "Prefix": "chunks/" },
      "NoncurrentVersionExpiration": { "NoncurrentDays": 8 }
    }
  ]
}
```

## Design Properties

| Property | Detail |
|---|---|
| No full backups | Non-smgr checkpoint state is a few MB; relation blocks are already in S3 |
| Recovery granularity | Checkpoint interval (≈5 min), not base materialization interval |
| Delta manifest size | Proportional to dirty chunks per checkpoint, not total database size |
| Base manifest size | Proportional to total number of live chunks (one entry per chunk) |
| Crash safety | All S3 PUTs are atomic; tasks are idempotent on restart |
| Background task isolation | Pure S3 I/O, no PG process-local state, safe in Tokio thread pool |
| Chunk version GC rule | Only superseded versions older than retention window; latest version always kept |
| Shutdown safety | Delta manifest written by checkpointer directly (no s3worker dependency) |
