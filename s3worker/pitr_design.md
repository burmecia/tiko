# PITR Design — Point-in-Time Recovery for Tiko

Tiko's S3-backed storage gives it a structural advantage over standard
PostgreSQL for PITR: all relation data already flows through the smgr and
lands in S3. The only state outside Tiko's control is a small set of
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

## Tiko's Structural Advantage

In Tiko, all relation data (including system catalogs in `base/` and
`global/`) flows through the smgr and is stored in S3-backed chunk objects.
The only state that does **not** go through Tiko is:

| File/Directory | Size | Content |
|---|---|---|
| `pg_control` | 8 KB | Checkpoint LSN, timeline, catalog version |
| `pg_xact/` | ~MB | Commit log (transaction status bits) |
| `pg_multixact/` | ~MB | Multixact state |
| `pg_filenode.map`, `global/pg_filenode.map` | KB | OID→filenode mapping |
| `pg_subtrans/` | small | Subtransaction state |

Before upload to S3, all pg state files for a checkpoint are bundled
into a single `pg_state.tar.zst` archive (tar + zstd). `pg_xact/`,
`pg_multixact/`, and `pg_subtrans/` are compact state files that usually
compress aggressively. Exact size is workload-dependent; in practice this
archive is typically small (often sub-MB), so upload cost remains low.
This replaces the traditional base backup entirely.

## Multi-Tenant and Database Branching

### Core Model

- **Org** (`org_id: u64`) — a tenant and the unit of S3 resource management.
  All chunk data for an org lives under `{org_id}/chunks/`. The org maintains
  a monotonically increasing `branch_id` counter starting at 0.

- **Project** (`project_id: u64`) — a single PostgreSQL database instance.
  `project_id` is globally unique across all orgs. Each project is assigned
  one org-scoped `branch_id` at creation time and writes all its chunk data
  to `{org_id}/chunks/{branch_id}/`. `branch_id` ≠ `project_id`: `project_id`
  is global, `branch_id` is local to the org.

- **Zero branch** (`branch_id = 0`) — created once when the org is
  provisioned by running `initdb` a single time. Holds built-in PostgreSQL
  DB state: system catalogs, template databases, and all other relation pages
  produced by `initdb`. All projects inherit these pages via their
  initial base manifest. No `initdb` is needed per project.

- **Database branch** — a new project created from a checkpoint of an
  existing project (the *parent*). The new project is assigned a fresh
  `branch_id`. Branch creation is a metadata-only control-plane operation:
  no chunk data is copied. After creation the two projects evolve
  independently on separate PostgreSQL timelines.

### Project Metadata

```
{org_id}/metadata/{project_id}/project.json
```

Any project branching from the zero branch (multiple projects can do this,
not just the first one; `branch_checkpoint_lsn` is the actual checkpoint LSN
produced by `initdb` when the zero branch was provisioned):

```json
{
  "project_id": 123456789,
  "org_id":     987654321,
  "branch_id":  1,
  "parent_project_id":    0,
  "parent_branch_id":     0,
  "branch_checkpoint_lsn": 22020096,
  "branch_timeline_id":    1,
  "created_at": 1740480000,
  "status": "active"
}
```

Child project (branches from another project):

```json
{
  "project_id": 234567890,
  "org_id":     987654321,
  "branch_id":  2,
  "parent_project_id":    123456789,
  "parent_branch_id":     1,
  "branch_checkpoint_lsn": 973078568,
  "branch_timeline_id":    1,
  "created_at": 1740490000,
  "status": "active"
}
```

### Initial Base Manifest

#### Root project (initdb)

For a root project the initial base is written automatically at the end of the
`initdb` shutdown checkpoint:

1. `cached_write_blocks` (initdb path, no S3IoControl) writes each block to
   SimStore express and appends the `ChunkTag` to the eviction log.
2. At shutdown, `s3_checkpoint_flush` skips `flush_all_dirty_chunks` (no shmem
   cache), runs `checkpoint_flush_inner` to process the eviction log into a
   delta manifest + pg_state archive, then calls `materialize_base` to merge
   all deltas into the first base at `checkpoint_lsn`.

After `initdb`, SimStore contains:
```
{org}/pitr/{project_id}/deltas/{checkpoint_lsn}/manifest.bin
{org}/pitr/{project_id}/deltas/{checkpoint_lsn}/pg_state.tar.zst
{org}/pitr/{project_id}/bases/{checkpoint_lsn}/manifest.bin   ← initial base
```

#### Branch project (branch creation)

At branch creation, the control plane builds the frozen chunk map at
`branch_checkpoint_lsn` and writes it directly as the project's first base
manifest:

```
{org_id}/pitr/{child_project_id}/bases/{branch_checkpoint_lsn}/manifest.bin
```

This object IS the branch base — there is no separate `branch_base.bin` file.
It serves two roles:

1. **Level 2 read fallback**: s3worker loads the latest base manifest from
   `pitr/{project_id}/bases/` at startup and keeps it in memory. On a cache
   miss, if express-bucket `latest` is absent, it looks up the chunk in this
   in-memory manifest and fetches the versioned object from standard-bucket.
   As the base materializer produces newer manifests, the in-memory copy is
   refreshed.

2. **Recovery starting point**: the standard base + delta merge algorithm
   (`latest_base ≤ target_lsn` then apply deltas) naturally picks up the
   initial manifest when recovering to any LSN near the branch point.

Entries are **flattened**: when building the chunk map from the parent's
manifests, each entry resolves to the true owning `{branch_id, lsn}` — the
lookup depth is always one. Chunks inherited from the zero branch carry
`branch_id: 0` (built-in DB pages); chunks inherited from ancestor projects
carry that ancestor's `branch_id`.

```json
// Logical structure of the initial manifest.bin (on-disk format: bincode + zstd)
{
  "checkpoint_lsn": 973078568,
  "timestamp": 1740490000,
  "chunks": {
    "1663/16384/16385.0/0": { "branch_id": 1, "lsn": 973078568 },
    "1663/16384/16387.0/2": { "branch_id": 1, "lsn": 956301328 },
    ...
  }
}
```

This is the same `Manifest` type used for both bases and deltas — the S3 path
(`bases/` vs `deltas/`) distinguishes them. No special file type is needed.

### Branch Creation Procedure (Zero-Copy)

Branch creation is a two-step control-plane operation followed by async
provisioning. No chunk data is copied at any point.

**Step 1 — Choose branch point**

The control plane picks a checkpoint LSN from the parent's `pitr/deltas/`
listing. Typically this is the latest checkpoint, but any checkpoint within
the retention window is valid.

**Step 2 — Build and write branch metadata (atomic)**

```
1. Build chunk_map at branch_checkpoint_lsn (same algorithm as PITR recovery):
     latest_base = latest pitr/bases/ entry with base_lsn ≤ branch_checkpoint_lsn
     deltas      = all pitr/deltas/ entries in (latest_base.lsn, branch_checkpoint_lsn]
     chunk_map   = latest_base.chunks
     for delta in deltas sorted by lsn:
         chunk_map.extend(delta.chunks)   // ChunkRef.branch_id = parent's branch_id

2. Flatten: for each chunk_map entry inherited from an ancestor branch,
   resolve to the true owning {branch_id, lsn} (already flat if parent
   maintained its manifests correctly).

3. Assign a new org-scoped branch_id to the child project (increment the
   org's branch_id counter). Write branch project metadata and initial base:
   PUT {org_id}/metadata/{child_project_id}/project.json                              ← branch descriptor
   PUT {org_id}/pitr/{child_project_id}/bases/{branch_checkpoint_lsn}/manifest.bin   ← frozen chunk map (initial base)
```

Steps 1–3 involve only small binary objects — typically a few hundred KB to
low MB (bincode + zstd) even for a large database. Wall-clock time is dominated by S3 round-trips, not
database size. There is no data copy, no separate `branch_base.bin`, and no
`branch_refs` marker is needed: org-level GC discovers live branch_ids by
scanning all project manifests.

**Step 3 — Provision new PostgreSQL instance (async)**

```
1. Pre-write project.json to SimStore with parent_project_id set (required
   before initdb so ProjectCtx::load() can detect is_branch() == true and
   skip root-only base compaction):
     PUT {org_id}/metadata/{child_project_id}/project.json

2. Run initdb to create the $PGDATA filesystem structure.
   - s3_checkpoint_flush at end of initdb detects is_branch() == true
     and skips materialize_base — no base manifest is written.

3. Download and extract pg state from parent's checkpoint:
     GET {org_id}/pitr/{parent_project_id}/deltas/{branch_checkpoint_lsn}/pg_state.tar.zst
    tar -xzf → $PGDATA/global/pg_control, $PGDATA/pg_xact/*, $PGDATA/pg_multixact/*,
           $PGDATA/pg_subtrans/*, $PGDATA/pg_filenode.map
   (Overwrites the pg_control etc. that initdb wrote — this is the restore step.)

4. Write tiko_recovery_manifest.bin from the initial base manifest
   at branch_checkpoint_lsn (fetched from pitr/{parent_project_id}/bases/).
5. Configure:
    restore_command = 'tiko_restore %f %p --project {parent_project_id} --org {org_id}'
     recovery_target_lsn = '{branch_checkpoint_lsn}'
     recovery_target_action = 'promote'
   Touch $PGDATA/recovery.signal.
6. Start PostgreSQL. WAL replay brings instance to branch_checkpoint_lsn,
   then promotes. s3worker exits recovery mode.
7. On promotion, PostgreSQL increments the timeline. The new project now
   has its own independent WAL archive at:
     {org_id}/pitr/{project_id}/wal/{new_timeline_id}/...
  At this point, switch runtime config from parent WAL bootstrap paths to
  child project paths (`archive_command` and any future `restore_command`).
```

### Read Path

All projects use the same two-level fallback. The express-bucket `latest`
only exists for chunks written by this project (i.e. after the branch point).
For inherited chunks, the in-memory base manifest provides the standard-bucket
location.

```
// Normal mode (not recovery.signal)
fn read_chunk(project: &ProjectCtx, key: &ChunkKey) -> Bytes {
    // Level 1: own express-bucket latest (written by this project after branch point)
    if let Ok(data) = get(express, &format!("{}/{}/chunks/{key}/latest",
                                            project.org_id, project.project_id))
    {
        return data;
    }

    // Level 2: base manifest lookup (loaded from pitr/{proj}/bases/ at startup;
    //           covers inherited chunks and own chunks from past checkpoints)
    if let Some(chunk_ref) = project.base_manifest.get(key) {
        // branch_id 0 → zero branch (built-in DB pages); otherwise → ancestor project
        return get(standard, &format!("{}/chunks/{}/{key}/{}",
                                      project.org_id, chunk_ref.branch_id,
                                      chunk_ref.lsn));
    }

    // Chunk did not exist at branch time — newly allocated on this project
    Err(NotFound)
}
```

`project.base_manifest` is loaded from the latest `pitr/{project_id}/bases/`
object at s3worker startup and refreshed in-memory each time the background
base materializer writes a newer one. The initial manifest (written by the
control plane at branch creation) serves as the starting value.

Recovery mode skips level 1 entirely and uses `tiko_recovery_manifest.bin`
to resolve every chunk to a specific standard-bucket object at the exact
recovery target LSN.

### Write Path

Every project writes exclusively to its own `branch_id`. The parent's chunk
namespace is never written by a child.

- **Express-bucket** (hot `latest`, per-project):
  `{org_id}/{project_id}/chunks/{key}/latest`
- **Standard-bucket** (immutable PITR archive, org-level):
  `{org_id}/chunks/{branch_id}/{key}/{lsn_hex}`

The project's delta manifests record only the chunks it dirtied; for all
other chunks, the base manifest remains authoritative. See
[Checkpoint Write Sequence](#checkpoint-write-sequence-per-dirty-chunk) for
the full 3-step PUT → COPY → Rename implementation and crash-safety analysis.

### GC: Org-Level Orphan Detection

Because all chunk data lives under `{org_id}/chunks/`, GC runs at the org
level with no cross-project coordination or `branch_refs` markers.

**Retention is checkpoint-count-based, not time-based.** A time-based cutoff
would incorrectly delete data from inactive projects (a project paused for
months still has a valid current state). Counting checkpoints ties retention
to database activity: a busy project accumulates 500 checkpoints quickly; an
idle one keeps all its history indefinitely until it generates enough new
checkpoints to trigger cleanup.

The policy: keep the last `max_checkpoints` (e.g. 500) delta manifests per
project, and keep enough base manifests to guarantee a valid recovery start
for every retained target checkpoint. In practice this means keeping the
newest base with `base_lsn ≤ cutoff_lsn` plus all newer bases.

```rust
async fn enforce_retention_org(
    s3: &S3Client,
    org_id: u64,
    max_checkpoints: usize,   // e.g. 500
) -> Result<()> {
    let mut live: HashSet<(u64 /*branch_id*/, ChunkKey, u64 /*lsn*/)> = HashSet::new();

    for project_id in list_projects(s3, org_id).await? {
        // Find the cutoff LSN: oldest delta to keep, based on count.
        // List all delta LSNs sorted ascending; cutoff is at position
        // (total - max_checkpoints). Fewer than max_checkpoints → keep all.
        let all_delta_lsns = list_delta_lsns(s3, org_id, project_id).await?;
        let cutoff_lsn = if all_delta_lsns.len() > max_checkpoints {
            all_delta_lsns[all_delta_lsns.len() - max_checkpoints]
        } else {
            0
        };

      // Find the base floor needed to recover any retained delta target:
      // keep the newest base with base_lsn <= cutoff_lsn.
      let base_floor_lsn = latest_base_lsn_leq(s3, org_id, project_id, cutoff_lsn)
        .await?
        .unwrap_or(0);

      // Protect all base manifests needed for recovery targets >= cutoff_lsn
      // (base_floor + newer bases, including latest/current state).
      for base in base_manifests_since(s3, org_id, project_id, base_floor_lsn).await? {
        for (key, chunk_ref) in &base.chunks {
          live.insert((chunk_ref.branch_id, key.clone(), chunk_ref.lsn));
        }
        }

        // Protect PITR history: chunks from deltas at or after cutoff_lsn.
        for chunk_ref in delta_chunk_refs_since(s3, org_id, project_id, cutoff_lsn).await? {
            live.insert((chunk_ref.branch_id, chunk_ref.key, chunk_ref.lsn));
        }

      // Delete manifests outside the retained window.
        delete_delta_manifests_before(s3, org_id, project_id, cutoff_lsn).await?;
      delete_base_manifests_before(s3, org_id, project_id, base_floor_lsn).await?;
    }

    // Delete versioned {lsn_hex} objects not in the live set.
    // Zero branch is permanent and never collected.
    for branch_id in list_branches(s3, org_id).await? {
        if branch_id == 0 { continue; }
        gc_branch(s3, org_id, branch_id, &live).await?;
    }

    Ok(())
}
```

Key properties:
- **Checkpoint-count retention** prevents data loss for inactive projects: a
  project with no new writes never accumulates enough checkpoints to trigger
  cleanup, so its current state is preserved indefinitely.
- No `branch_refs` markers needed. A deleted project's `branch_id` is
  naturally protected while any live child's base manifests reference
  it — the child's manifests appear in the org scan and keep it live.
- Zero branch (`branch_id = 0`) is never collected.
- **Orphan `.staging_*` sweep (express-bucket):** The algorithm above only
  covers standard-bucket `{lsn_hex}` objects. `.staging_{lsn_hex}` objects in
  express-bucket are left behind when a process crashes after Step 1 (PUT) but
  before Step 3 (Rename). These are cleaned by a separate per-project sweep:
  list all keys matching `{org}/{proj}/chunks/*/.staging_*` that are older than
  a fixed threshold (e.g. 1 hour — far longer than any checkpoint can take) and
  delete them unconditionally. No manifest cross-check is needed because a
  `.staging_*` object that survived a Rename always becomes `latest` and has no
  `.staging_*` name.

#### Scalability Note: Partitioned GC and S3 Inventory

The algorithm above scans all manifest objects for every project in the org on
each GC run. For large orgs (thousands of projects, billions of chunks) this
becomes expensive. **For large orgs this should be partitioned by `branch_id`
and driven by S3 Inventory.**

**Future optimization path:**

1. **S3 Inventory-driven GC**: Enable S3 Inventory on standard-bucket to get
   a daily CSV/ORC listing of all objects under `{org_id}/chunks/`. Instead of
   listing via `ListObjectsV2` (paginated API calls), the GC job reads the
   inventory file. This reduces LIST API cost from O(objects) round-trips to
   one flat file read.

2. **Partitioned by `branch_id`**: Shard the GC job so each worker handles a
   disjoint range of `branch_id` values. Each shard independently:
   - Reads the inventory slice for its `branch_id` range.
   - Scans only the manifests for projects whose `branch_id` falls in the range
     (plus base manifests of other projects that reference those branch_ids as
     inherited chunks).
   - Deletes orphaned objects within its range.
   Shards can run in parallel with no coordination beyond the shared inventory
   file.

3. **Incremental manifest scanning**: Rather than re-reading all manifests each
   run, maintain a per-org `gc_manifest_cursor.bin` that records the last-seen
   `checkpoint_lsn` per project. Each GC run only ingests delta manifests newer
   than the cursor, updating the live set incrementally. The live set itself can
   be persisted in S3 (e.g. a compact Bloom filter or sorted key file) between
   runs.

4. **Branch tombstones**: When a project is deleted, write a small tombstone
   object at `{org_id}/metadata/{project_id}/.deleted`. The GC job can skip
   scanning manifests for tombstoned projects entirely and immediately target
   their `branch_id` prefix for collection once no other live project's
   base manifests reference it.

When a project is deleted:

```
1. Delete {org_id}/pitr/{project_id}/  (manifests, WAL archive)
2. Delete {org_id}/metadata/{project_id}/  (project.json)
3. Delete {org_id}/{project_id}/  (express-bucket latest objects)
4. {org_id}/chunks/{branch_id}/ is collected by the next org GC run once
   no remaining project's manifests reference any of its {lsn_hex} objects.
```

---

## Storage Design: Append-Only LSN-Keyed Chunks + Delta Manifests

### Core Principle

Chunk objects in S3 are **never overwritten in place**. Each checkpoint flush
writes dirty chunks to new, immutable S3 objects whose keys embed the
checkpoint LSN. Historical versions accumulate naturally; GC removes them
after the retention window (subject to live manifest references from child branches).

Alongside each immutable versioned object, a `latest` object (full 256 KB
chunk data, not a pointer) is atomically promoted via `RenameObject` (S3
Express One Zone). This gives the normal operation read path a single S3 GET
per cache miss with no auxiliary index or pointer indirection. On a cache miss
the read path checks the project's own express-bucket `latest` first
(single-digit ms), then falls back to the standard-bucket via the in-memory
base manifest — see [Read Path](#read-path) in the Multi-Tenant section above.

### Dual-Bucket Architecture

The two objects written per checkpoint flush have different access profiles
and durability requirements, so they live in different S3 service types:

| Bucket type | Contents | Why |
|---|---|---|
| **S3 Express One Zone** (Directory Bucket) | `{org}/{proj}/chunks/{key}/latest` | Single-digit ms GET on every cache miss; `RenameObject` available |
| **Standard S3** (multi-AZ) | `{org}/chunks/{branch_id}/{key}/{lsn_hex}`, `{org}/pitr/{proj}/` manifests, WAL | 11-9s durability for PITR archive; Intelligent-Tiering; rarely read |

Express One Zone's single-AZ limitation is acceptable for `latest`: it is
always reconstructible from `{lsn_hex}` + WAL replay. Standard S3's multi-AZ
durability protects the PITR archive and base manifests, which are
the ground truth for recovery.

### S3 Layout

```
── express-bucket (S3 Express One Zone, per-project hot reads) ─────────────
{org_id}/{project_id}/
  chunks/
    {spc_oid}/{db_oid}/{rel_number}.{fork}/{chunk_id}/
      latest              ← full 256 KB chunk data at current checkpoint LSN
                            atomically replaced by RenameObject each checkpoint

── standard-bucket (Standard S3, multi-AZ) ─────────────────────────────────
{org_id}/
  chunks/
    0/                    ← zero branch: built-in DB state; permanent, never GC'd
      {spc_oid}/{db_oid}/{rel_number}.{fork}/{chunk_id}/
        {lsn_hex}
    {branch_id}/          ← one prefix per project (branch_id is org-scoped, ≠ project_id)
      {spc_oid}/{db_oid}/{rel_number}.{fork}/{chunk_id}/
        {lsn_A_hex}       ← immutable 256 KB version sealed at checkpoint A
        {lsn_C_hex}       ← immutable 256 KB version sealed at checkpoint C

  pitr/
    {project_id}/
      bases/
        {checkpoint_lsn}/
          manifest.bin              ← full chunk_key → ChunkRef map (materialized)
          pg_state.tar.zst    ← tar+zstd: pg_control, pg_xact/*, pg_multixact/*, pg_filenode.map
      deltas/
        {checkpoint_lsn}/
          manifest.bin                 ← dirty chunks at this checkpoint only
          pg_state.tar.zst    ← tar+zstd: pg_control, pg_xact/*, pg_multixact/*, pg_filenode.map
      wal/
        {timeline_id}/
          {wal_segment}   ← archived WAL segments

  metadata/
    {project_id}/
      project.json        ← project_id, org_id, branch_id, parent linkage
```

### Manifest Format

Both base and delta files use the filename `manifest.bin` under their
respective path prefixes. JSON below shows the **logical structure** only; the
on-disk format is always bincode + zstd.

#### Manifest File Structure

```json
// Logical structure (on-disk format: bincode + zstd)
{
  "checkpoint_lsn": 973078568,
  "timestamp": 1740480000,
  "chunks": {
    "1663/16384/16385.0/0": { "branch_id": 1, "lsn": 973078568 },
    "1663/16384/16387.0/2": { "branch_id": 1, "lsn": 956301328 }
  }
}
```

Both base and delta manifests share the same `Manifest` type. A **base**
(`bases/{lsn}/manifest.bin`) contains the full `chunk_key → ChunkRef` map —
every chunk the project can read, including inherited ones. A **delta**
(`deltas/{lsn}/manifest.bin`) contains only the chunks dirtied at that
checkpoint. The S3 path distinguishes them; no separate Rust type is needed.

#### ChunkRef

Both manifests carry `ChunkRef` values (not bare `lsn_hex` strings) so that
a child project can reference chunks that live in any ancestor's branch
namespace:

- `branch_id: u64` — org-scoped monotonically increasing integer. Identifies
  where the chunk data lives: `{org_id}/chunks/{branch_id}/`. `branch_id = 0`
  is the org zero branch (built-in DB state, never deleted).
- `lsn: u64` — PostgreSQL `XLogRecPtr`. Used as an S3 object key suffix,
  formatted as a zero-padded 16-character hex string (`format!("{:016X}", lsn)`)
  for lexicographic ordering. Human-readable display uses `HIGH/LOW_HEX`
  (e.g. `973078568` → `"0/3A000028"`).

For a project's own dirty chunks, `branch_id` is always its own branch_id.
For inherited chunks not yet rewritten on this project, `branch_id` may be
any ancestor's branch_id (including `0` for built-in DB pages). When the
rolling base materializer merges deltas — later LSN wins — the winning
entry's `branch_id` is preserved faithfully.

#### Binary Encoding: bincode + zstd

All manifest files are encoded as **bincode** serialised structs compressed
with **zstd** (level 3). `project.json` is the sole JSON file — written by
the control plane and kept human-readable.

- `bincode` serialises Rust `serde` types directly: zero schema management,
  zero allocation overhead on decode.
- `zstd` at level 3 exploits the highly repetitive chunk key prefixes
  (`{spc_oid}/{db_oid}/...`) to achieve 10–15× compression. A 400 K-chunk
  base manifest shrinks from ≈20 MB (JSON) to ≈1–2 MB (binary).
- Delta manifests compress even better: a 4 K-entry delta (≈200 KB JSON)
  becomes ≈15–30 KB binary, reducing PUT/GET cost and S3 request latency.

For debugging, `tiko manifest dump <file>` (future CLI) decodes any `.bin`
file to JSON for human inspection.

### Checkpoint Write Sequence (Per Dirty Chunk)

For each dirty chunk flushed during `flush_all_dirty_chunks()` at checkpoint
LSN X:

```
// Step 1: Upload chunk data to a unique staging key in Express One Zone (per-project).
PUT  express-bucket/{org}/{proj}/chunks/{key}/.staging_{lsn_X_hex}   ← 256 KB

// Step 2: Server-side cross-bucket copy to Standard S3 (org-level shared chunks).
//         Creates the immutable versioned object for PITR.
COPY express-bucket/{org}/{proj}/chunks/{key}/.staging_{lsn_X_hex}
  →  standard-bucket/{org}/chunks/{branch_id}/{key}/{lsn_X_hex}

// Step 3: Atomically replace latest in Express One Zone (per-project).
Rename express-bucket/{org}/{proj}/chunks/{key}/.staging_{lsn_X_hex}
    →  express-bucket/{org}/{proj}/chunks/{key}/latest
```

Steps 1 → 2 → 3 are strictly sequential for a single chunk: the COPY
requires the `.staging` object to exist, and the Rename must not remove
`.staging` until the COPY has finished. Total client-side upload: **1 × 256 KB**
per dirty chunk. The CopyObject and RenameObject are server-side operations
with no additional data transfer. Across multiple dirty chunks the three steps
can be pipelined — Step 2 for chunk A can run concurrently with Step 1 for
chunk B.

**Crash safety:**

| Crash point | State | Safe? |
|---|---|---|
| After PUT, before COPY | `.staging` exists; `{lsn_hex}` absent; `latest` is prior version | ✓ old `latest` consistent; orphan staging cleaned by GC |
| After COPY, before Rename | `.staging` + `{lsn_hex}` exist; `latest` is prior version | ✓ old `latest` consistent; orphan staging GC'd; `{lsn_hex}` valid for PITR |
| After Rename | `{lsn_hex}` + new `latest` both present | ✓ fully consistent |

### Delta Manifests (Written at Each Checkpoint)

#### The Eviction Gap

`flush_all_dirty_chunks()` only visits slots where `dirty_blocks != 0` in the
cache at checkpoint time. A dirty chunk evicted **before** the checkpoint has
already been `reset_slot()`'d — its slot has `valid_blocks = 0`,
`dirty_blocks = 0` and is invisible to the checkpoint scan. Its data reached
express-bucket `latest` during eviction, but:

- No versioned `{lsn_hex}` object was written to standard-bucket
- No entry was recorded in the delta manifest

During recovery, the chunk map would resolve to the stale version from the
previous checkpoint. WAL replay can compensate only if `full_page_writes = on`
embeds a full page image at the first post-checkpoint modification — subsequent
WAL records for the same page do not, so recovery would apply deltas over
incorrect base data.

#### Eviction Log File

When a dirty chunk is evicted, `flush_dirty_chunk()` is extended to:

1. Write chunk data to express-bucket `latest` with a plain PUT
   (`{org}/{proj}/chunks/{key}/latest`). No staging key, no standard-bucket
   copy — those happen at the next checkpoint.
2. Append a fixed-size 20-byte record to `$PGDATA/tiko/eviction_log` **after**
   the PUT succeeds.

```rust
#[repr(C)]
struct EvictionLogRecord {
    spc_oid:    u32,
    db_oid:     u32,
    rel_number: u32,
    fork:       u32,
    chunk_id:   u32,
}
// 20 bytes — write(2) with O_APPEND is atomic on local POSIX filesystems
// (kernel serialises appenders at the file level).
// Validate this assumption on each supported platform/filesystem (e.g. APFS,
// ext4/xfs) and fsync the log at checkpoint boundaries.
// pwrite(2) must NOT be used here: POSIX specifies that pwrite ignores O_APPEND.
```

Multiple processes evict concurrently. Fixed-size records with `O_APPEND` make
each append atomic — no locking required. Appending only after the PUT succeeds
means a crash before the append leaves no phantom log entry for a chunk not yet
uploaded to express-bucket.

#### Checkpoint Snapshot: Rename-Swap

The checkpointer cannot read-and-truncate the eviction log while new evictions
race to append. A rename-swap solves this atomically:

```
rename("$PGDATA/tiko/eviction_log",
       "$PGDATA/tiko/eviction_log.ckpt")
// New eviction_log created fresh for ongoing evictions.
// rename(2) is atomic — no record is lost or duplicated.
```

If the checkpointer crashes between the rename and the delta manifest write,
`eviction_log.ckpt` survives on disk and is re-processed at the next checkpoint.
Re-processing is idempotent: `latest` in express-bucket already has the correct
data; re-issuing the 3-step PUT→COPY→Rename for each chunk writes the same
versioned standard-bucket object, and the manifest is re-derived from the same
records.

#### Checkpoint Flush Sequence

```rust
pub extern "C-unwind" fn s3_checkpoint_flush(checkpoint_lsn: u64) {
    // --boot phase: checkpoint_lsn == 0; SimStore/ProjectCtx not yet
    // initialised. Nothing to do.
    if checkpoint_lsn == 0 { return; }

    let (sim, ctx) = match (SimStore::try_get(), ProjectCtx::try_get()) {
        (Some(s), Some(c)) => (s, c),
        _ => return,
    };

    if S3IoControl::is_initialized() {
        // Normal path (server running under postmaster):
        // 1. Evict all remaining in-cache dirty chunks: PUT each to
        //    express-bucket `latest` and append to eviction log.
        evict_all_dirty_chunks();
    }
    // Initdb path: cached_write_blocks already PUT to express + appended to
    // eviction log on every block write. No in-shmem cache to flush.

    // 2. Atomically snapshot the eviction log.
    rename("tiko/eviction_log", "tiko/eviction_log.ckpt");

    // 3. Read snapshot. Deduplicate by chunk_key (a chunk evicted multiple
    //    times in the interval is only uploaded once at checkpoint time).
    let dirty_chunks = dedup(read_eviction_log("tiko/eviction_log.ckpt"));

    // 4. For each dirty chunk, create the versioned standard-bucket object for
    //    PITR via the full 3-step sequence, keyed by checkpoint_lsn:
    //    PUT .staging_{checkpoint_lsn_hex} → COPY to standard-bucket → Rename to latest.
    for chunk_key in &dirty_chunks {
        upload_versioned_chunk(chunk_key, checkpoint_lsn);
    }

    // 5. Write delta manifest + pg_state archive to standard-bucket.
    write_delta_manifest(checkpoint_lsn, &dirty_chunks);
    upload_pg_state(checkpoint_lsn);

    // 6. Remove snapshot file.
    fs::remove("tiko/eviction_log.ckpt");

    // Initdb-only: after the shutdown checkpoint, bootstrap the initial base
    // manifest for root projects by materializing all deltas just produced.
    // Branch projects skip this — their initial base is created by the
    // restore-from-parent step.
    if !S3IoControl::is_initialized() && !ctx.is_branch() {
        materialize_base(sim, ctx.ns());
    }
}
```

#### Delta Manifest Format

All dirty chunks — whether evicted mid-interval or flushed at checkpoint step 1
— are stamped with the same `checkpoint_lsn` as the standard-bucket object key
suffix:

```json
// Logical structure (on-disk format: bincode + zstd)
{
  "checkpoint_lsn": 973078568,
  "timestamp": 1740480000,
  "chunks": {
    "1663/16384/16385.0/0": { "branch_id": 1, "lsn": 973078568 },
    "1663/16384/16387.0/2": { "branch_id": 1, "lsn": 973078568 }
  }
}
```

Both chunks resolve to standard-bucket objects keyed by `checkpoint_lsn`
under `{org_id}/chunks/{branch_id}/`. PITR recovery is checkpoint-granular;
WAL replay handles precision within the interval.

Each delta is **immutable once written** — the checkpointer never modifies it
after the PUT. This means the base materializer (see below) can read deltas
concurrently with the checkpointer writing new ones without any coordination.

Delta manifests are small. For a 100 GB database (≈400K chunks) with 1% of
chunks written per checkpoint interval, each delta is ≈15–30 KB (binary) vs
≈200 KB (JSON), and a full base manifest is ≈1–2 MB (binary) vs ≈20 MB (JSON).

#### Scenario Coverage

| Scenario | Result |
|---|---|
| Chunk dirtied → checkpoint (still in cache) | Evicted in checkpoint step 1, log entry included in delta |
| Chunk dirtied → evicted mid-interval → checkpoint | Eviction log entry included in delta |
| Chunk dirtied → evicted → re-dirtied → evicted → checkpoint | Two log entries; dedup collapses to one upload at checkpoint_lsn |
| Crash between eviction PUT and log append | No log entry; chunk absent from delta manifest. `latest` has the new data but no versioned standard-bucket object is created. WAL replay reconstructs the write during PITR recovery (requires `full_page_writes = on`) |
| Crash during checkpoint rename-swap | `eviction_log.ckpt` re-processed at next checkpoint |

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
    }
}

pub fn materialize_base(sim: &SimStore, ns: &ProjectNamespace) -> Result<()> {
    // 1. Load current base manifest, or start from empty if none exists yet.
    //    Lsn::INVALID == 0, so every real delta LSN passes the `lsn > base_lsn`
    //    filter when bootstrapping the first base from scratch.
    let (base, base_lsn) = if let Some(latest_lsn) = fetch_latest_base_lsn(sim, ns)? {
        (fetch_base_manifest(sim, ns, latest_lsn)?, latest_lsn)
    } else {
        (Manifest::empty(), Lsn::INVALID)  // bootstrap from scratch
    };

    // 2. Collect delta LSNs strictly newer than base_lsn.
    let delta_lsns = fetch_delta_lsns_after(sim, ns, base_lsn)?;
    if delta_lsns.is_empty() {
        return Ok(());  // nothing to merge
    }

    // 3. Apply each delta in LSN order (later LSN wins; branch_id preserved).
    let deltas = fetch_delta_manifests(sim, ns, &delta_lsns)?;
    base.apply_deltas(&deltas)?;

    // 4. Write new base atomically (single PUT).
    //    Key: {org}/pitr/{proj}/bases/{new_lsn}/manifest.bin
    let new_lsn = *delta_lsns.last().unwrap();
    sim.put_standard(&ns.base_manifest_key(new_lsn), &base.to_bytes()?)?;

    // NOTE: deltas are NOT deleted here — they remain for arbitrary-point
    // recovery within the retention window. Retention GC (manifest + chunk
    // cleanup) is handled by enforce_retention_org() on the control plane.
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
3. Result: `chunk_key → ChunkRef` for every chunk at LSN D

Recovery to **LSN G** (after Base-E):
1. Find the latest base with `base_lsn ≤ G` → Base-E ← **fewer deltas to merge**
2. Merge Base-E.chunks + δF + δG
3. Result: `chunk_key → ChunkRef` for every chunk at LSN G

### Step-by-Step Recovery Procedure

Given an empty `$PGDATA` directory (no `initdb` needed — built-in DB pages
come from the zero branch in S3) and a target time T:

**Step 1 — Identify target checkpoint**

Search `{org}/{proj}/pitr/deltas/` for the latest checkpoint with
`timestamp ≤ T`. If no delta qualifies (project has had no activity since
branch creation, or T is before the first delta), fall back to the latest
entry in `{org}/{proj}/pitr/bases/` with `timestamp ≤ T`. Record both
`target_lsn` and `target_kind ∈ {delta, base}`.

**Step 2 — Build chunk map at target_lsn**

```
latest_base = latest base with base_lsn ≤ target_lsn
deltas      = all deltas with delta_lsn in (latest_base.lsn, target_lsn]
chunk_map   = latest_base.chunks
for delta in deltas sorted by lsn:
    chunk_map.extend(delta.chunks)
```

`chunk_map` now gives: `chunk_key → ChunkRef{branch_id, lsn}` for every
chunk at `target_lsn`. For chunks written by this project, `branch_id` is
the project's own. For inherited chunks, `branch_id` may be any ancestor's
(including `0` for built-in pages). The S3 GET is always
`standard-bucket/{org}/chunks/{chunk_ref.branch_id}/{key}/{lsn}`.

**Step 3 — Restore pg state**

Download and extract from `{org}/{proj}/pitr/{target_kind}s/{target_lsn}/pg_state.tar.zst`:

```bash
# Single GET; decompress + untar in one pass (no temp file needed)
GET standard-bucket/{org}/{proj}/pitr/{target_kind}s/{target_lsn}/pg_state.tar.zst \
  | zstd -d | tar -xf - -C $PGDATA
# Extracts: global/pg_control, pg_xact/*, pg_multixact/*, pg_subtrans/*, pg_filenode.map
```

This tells PostgreSQL: "I am at LSN `target_lsn`, WAL replay starts here."

**Step 4 — Configure WAL recovery**

**Prerequisite:** WAL archiving must have been running during the period to be
recovered. Normal operation requires:

```ini
# postgresql.conf (on the source instance)
wal_level = replica
archive_mode = on
archive_command = 'tiko_archive %p %f --project {proj_id} --org {org_id}'
```

`tiko_archive` uploads each completed WAL segment to:
```
standard-bucket/{org}/pitr/{proj}/wal/{timeline_id}/{wal_segment}
```

During recovery, `restore_command` runs `tiko_restore` to download WAL
segments from that same path:

```ini
# postgresql.conf (on the recovery instance)
restore_command = 'tiko_restore %f %p --project {proj_id} --org {org_id}'
recovery_target_time = '<T from Step 1>'   # the target time requested
recovery_target_action = 'promote'
```

```bash
touch $PGDATA/recovery.signal
```

**Step 5 — Signal s3worker to use chunk_map**

Write `chunk_map` to `$PGDATA/tiko_recovery_manifest.bin` before starting
PostgreSQL. On startup, s3worker detects `recovery.signal` and loads this
manifest. In recovery mode, block reads resolve `chunk_key` to the specific
`ChunkRef` from the manifest and fetch from standard-bucket, rather than
reading `latest` from express-bucket.

**Step 6 — Start PostgreSQL**

```
PostgreSQL reads pg_control → "last checkpoint at target_lsn"
WAL recovery begins; restore_command (tiko_restore) downloads each WAL
  segment from standard-bucket/{org}/pitr/{proj}/wal/{timeline_id}/
  For each WAL record, buffer manager reads a page:
    → s3worker in recovery mode: looks up chunk_map[chunk_key]
    → fetches standard-bucket/{org}/chunks/{chunk_ref.branch_id}/{key}/{lsn}
    → returns page at checkpoint state
  WAL record is applied → page marked dirty in buffer pool
    (s3_writev is called lazily on buffer eviction, not per-WAL-record)
  ... repeat until WAL hits target time T
PostgreSQL promotes, recovery complete
```

**Step 7 — Post-recovery**

After promotion, s3worker exits recovery mode. New writes follow the normal
checkpoint flush sequence (PUT staging → COPY to standard-bucket → Rename to
express-bucket latest). The recovery manifest is removed.

## What Needs to Be Built

### New Files

| File | Purpose |
|---|---|
| `s3worker/src/pitr_task.rs` | Background Tokio task: rolling base materialization only (GC runs on control plane) |
| `s3worker/src/manifest.rs` | `ChunkRef`, `Manifest` type + merge logic |
| `s3worker/src/s3_client.rs` | S3 client init: express-bucket client + standard-bucket client |
| `s3worker/src/project.rs` | `ProjectCtx`: org_id, project_id, branch_id, base_manifest loaded at startup |
| `s3worker/src/bin/tiko_restore.rs` | WAL restore command: downloads WAL segments from standard-bucket during recovery |
| `s3worker/src/bin/tiko_archive.rs` | WAL archive command: uploads completed WAL segments to standard-bucket |
| `s3smgr/src/wal_archive.rs` | Blocking S3 client for checkpointer-side delta manifest + pg state writes |

### Modified Files

| File | Change |
|---|---|
| `s3worker/src/cache.rs` | `flush_dirty_chunk()` PUT chunk to express-bucket `latest` and append `ChunkTag` to eviction log. Added `pub append_chunk_tag_to_eviction_log(tag)` for use by the initdb write path |
| `s3worker/src/s3_ops.rs` | initdb path of `cached_write_blocks`: after express PUT, call `CacheControl::append_chunk_tag_to_eviction_log` (guarded by `!is_under_postmaster()`). Normal path: PUT to shmem cache (write-back). `read_blocks` normal: GET own express `latest`, fallback to base manifest → standard-bucket; recovery mode: GET standard-bucket `{lsn_hex}` via chunk_map |
| `s3worker/src/pitr_task.rs` | `materialize_base` made `pub`. "No base" early-return replaced: bootstrap from `Manifest::empty()` at `Lsn::INVALID`, merge all deltas, write first base |
| `s3worker/src/project.rs` | `ProjectCtx::load()` branch-with-no-base case changed from `Err` to empty manifest. `is_branch()` still returns `true` when `parent_project_id.is_some()`. Enables initdb to succeed for branch projects without a pre-existing base |
| `s3worker/src/io_handler.rs` | Pass project namespace to `read_blocks`/`write_blocks` |
| `s3worker/src/thread_pool.rs` | Spawn `pitr_background_task` after Tokio runtime starts |
| `s3worker/src/lib.rs` | Export `manifest`, `pitr_task`, `project` modules |
| `s3smgr/src/checkpoint.rs` | `s3_checkpoint_flush`: `checkpoint_lsn == 0` guard replaces `!S3IoControl::is_initialized()` guard. `flush_all_dirty_chunks()` only called when S3IoControl is initialized. `checkpoint_flush_inner` runs in both normal and initdb paths. After initdb checkpoint: calls `materialize_base` for root projects (`!is_initialized() && !is_branch()`) |
| `postgres/src/backend/access/transam/xlog.c` | Call `s3_checkpoint_flush(checkpoint_lsn)` from `CheckPointGuts()` |

### Control-Plane Responsibilities

The control plane (not Tiko itself) is responsible for:

| Operation | Action |
|---|---|
| Create root project | Run `initdb`; `s3_checkpoint_flush` automatically produces initial delta + base manifests. Write `metadata/project.json` |
| Create branch project | Write `metadata/project.json` (with `parent_project_id`) to SimStore **before** `initdb`. Run `initdb` (creates $PGDATA structure; skips base compaction because `is_branch()==true`). Build chunk map from parent manifests; write `pitr/{child}/bases/{lsn}/manifest.bin`. Restore pg_state from parent checkpoint. Start PG in recovery mode |
| Delete branch project | Delete express-bucket `{org}/{project_id}/`; delete `{org}/pitr/{project_id}/` and `{org}/metadata/{project_id}/`; standard-bucket `{org}/chunks/{branch_id}/` collected by next GC run |
| Delete zero-branch source project | Assert no live child project base manifests reference branch `0` objects that would be removed (normally branch 0 is permanent) |
| GC / retention enforcement | Run `enforce_retention_org` periodically (max_checkpoints cutoff); delete delta manifests beyond the limit; delete unreferenced standard-bucket chunk versions; delete superseded base manifests |

Branch creation is atomic at the metadata level: the critical write is
`pitr/{child}/bases/{lsn}/manifest.bin` (branch is valid once this exists).
A single S3 PUT.

## Design Properties

| Property | Detail |
|---|---|
| Multi-tenant isolation | All S3 objects namespaced under `{org_id}/`; projects further scoped by `project_id` or `branch_id` sub-prefix |
| Branch creation cost | Two S3 PUTs (project.json + initial manifest.bin at branch LSN); no chunk copy |
| Branch read overhead | One extra standard-bucket GET for chunks not yet written on branch; hot chunks quickly promoted to own `latest` |
| Branch base map size | Proportional to total live chunks in parent at branch time (one binary entry per chunk; ≈1–2 MB per 400 K chunks after zstd) |
| Branch GC safety | No `branch_refs` markers needed; GC scans all project manifests to find live `{branch_id, lsn}` references; cutoff is checkpoint-count-based (`max_checkpoints`) |
| Branch cascade depth | Always 1 — initial manifest.bin is flattened to true owning branch_id at creation time |
| No full backups | Non-smgr checkpoint state is a few MB; relation blocks are already in S3 |
| Normal read path | Single GET from Express One Zone (`latest`); no LSN index or pointer indirection |
| Checkpoint write | 1 × 256 KB upload per dirty chunk; CopyObject + RenameObject are server-side |
| Atomic `latest` update | `RenameObject` in Express One Zone; no torn reads; `full_page_writes = on` still required for WAL replay in crash recovery |
| Recovery granularity | Checkpoint interval (≈5 min), not base materialization interval |
| Delta manifest size | Proportional to dirty chunks per checkpoint, not total database size |
| Crash safety | All S3 PUTs/Renames are atomic; tasks are idempotent on restart |
| Background task isolation | Pure S3 I/O, no PG process-local state, safe in Tokio thread pool |
| Shutdown safety | Delta manifest written by checkpointer directly (no s3worker dependency) |
| Durability split | Express (single-AZ, fast) for hot `latest`; Standard (multi-AZ) for PITR archive and branch base maps |

## Future Optimisations

These S3 features are not required for correctness but improve performance,
cost, or operational simplicity at scale.

### Initial Manifest Pre-warming

On branch creation, the control plane can asynchronously copy the parent's
`latest` objects for the most-frequently-accessed chunks (e.g., system
catalog pages) into the branch's express-bucket namespace. This converts
subsequent standard-bucket level-2 reads into fast express-bucket reads for
hot data, at the cost of the copy. The pre-warming is best-effort and
entirely optional — the branch is functional without it.

### S3 Express One Zone: `CreateSession`

Checkpoint flush is a burst of potentially thousands of PUTs to express-bucket.
Normal SigV4 auth signs each request individually. `CreateSession` obtains a
5-minute session token that amortises authentication across all requests in
the flush. One `CreateSession` call at the start of `s3_checkpoint_flush`
covers the entire burst.

Also valuable for the s3worker Tokio path: cache-miss GETs from express-bucket
are frequent and the Express latency is already fast (1–10 ms); eliminating
per-request SigV4 overhead removes a consistent baseline cost from every read.

### CRC32C Checksums End-to-End

PostgreSQL computes CRC32C checksums per 8 KB page (when `data_checksums` is
enabled). S3 supports CRC32C as an additional object checksum, verified on
both upload and download.

Attach a CRC32C of the full 256 KB chunk when PUTting to express-bucket and
when the CopyObject lands in standard-bucket. Request checksum validation in
GET responses for cache-miss reads. This catches:

- Bit rot in Express One Zone (single-AZ; no silent corruption detection from
  cross-AZ redundancy)
- Data corruption during cross-bucket CopyObject (server-side but verifiable)
- Any mismatch between what was written at checkpoint time and what is served
  on a cache miss

CRC32C is hardware-accelerated (SSE4.2 / ARM CRC) and adds negligible CPU
overhead relative to the 256 KB I/O itself.

### Conditional Writes (`If-None-Match: *`) on `{lsn_hex}` Objects

`{lsn_hex}` objects in standard-bucket are supposed to be immutable once
written. S3 conditional PUT with `If-None-Match: *` fails with `412` if the
object already exists.

Apply this to the `CopyObject` destination in standard-bucket. If the
checkpointer crashes mid-flush and retries, it will attempt to re-copy chunks
already written. The `412` response is a safe success: the existing object is
identical (same checkpoint LSN, same data), and the delta manifest entry
remains valid.

### S3 Intelligent-Tiering on Standard S3

`{lsn_hex}` objects have a predictable access gradient within the retention
window: the most recent checkpoint versions are accessed during WAL replay
and recovery tests; older checkpoint versions are rarely accessed.

Configure S3 Intelligent-Tiering on the `chunks/` prefix in standard-bucket:

```json
{
  "Id": "ChunkVersionsTiering",
  "Status": "Enabled",
  "Filter": { "Prefix": "chunks/" },
  "Tierings": [
    { "Days": 1, "AccessTier": "ARCHIVE_INSTANT_ACCESS" }
  ]
}
```

Objects not accessed for 1 day drop to Archive Instant Access (lower storage
cost, still millisecond retrieval — no Glacier restore delay). Since PITR
recovery is rare, most `{lsn_hex}` objects transition quickly. Storage cost
for the PITR archive shrinks significantly without affecting availability.

This applies equally to inherited chunk objects (referenced via base manifests):
they are accessed only on branch provisioning and recovery, so they transition
quickly after branch creation.

### S3 Batch Operations + S3 Inventory for Large-Scale GC

`enforce_retention_org()` currently deletes objects individually. For large
orgs (millions of `{lsn_hex}` objects across the retention window),
per-request DELETE is expensive.

**S3 Inventory** provides scheduled daily reports (CSV or Parquet) of all
objects in standard-bucket, including key, size, and last-modified date,
delivered to a destination bucket.

**S3 Batch Operations** processes millions of object operations from an
Inventory report at ~$0.25 per million — far cheaper than individual DELETE
API calls.

Workflow:
1. Enable Inventory on standard-bucket for the `chunks/` prefix.
2. `enforce_retention_org()` reads the latest Inventory report to build
   the set of objects to delete: keys not referenced by any live base or
   delta manifest (as determined by the checkpoint-count-based live set).
3. Submit a Batch Delete job with the filtered object list.

The existing per-request GC loop handles day-to-day operation; Batch
Operations handles the large-scale periodic sweep for databases with many
millions of chunk versions.

### Parallel Checkpoint Flush

`flush_all_dirty_chunks()` currently iterates dirty chunks sequentially. With
the Tokio-based S3 client in s3worker, the flush can issue all dirty chunk
uploads concurrently via `tokio::spawn` or `FuturesUnordered`, bounded by a
semaphore to avoid overwhelming S3 (e.g., 64 concurrent requests). This pairs
naturally with Express One Zone's high per-bucket throughput and with
`CreateSession` amortising auth across all concurrent PUTs.

The checkpointer process (no Tokio runtime) would use a `rayon` thread pool
or a minimal `tokio::runtime::Builder::new_multi_thread` for its blocking S3
client, keeping the parallel flush benefit during normal checkpoints.

### Cross-Region Replication for Disaster Recovery

Express One Zone is intentionally single-AZ. Standard S3 is multi-AZ within
one region. For full geo-redundancy, enable S3 Cross-Region Replication (CRR)
on standard-bucket to a bucket in a secondary region.

CRR replicates all `{lsn_hex}` objects, `pitr/` manifests (including initial
base manifests), WAL segments, and `metadata/` objects (`project.json`) asynchronously. Recovery can proceed from the secondary region if the primary
region becomes unavailable. `latest` objects in express-bucket are not
replicated (they are reconstructible from `{lsn_hex}` + WAL replay); a new
express-bucket in the secondary region would be populated on first access.
