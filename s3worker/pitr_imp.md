# PITR Implementation Plan

This document is the executable implementation plan for PITR and database branching
support in Tiko. Each module is self-contained and independently testable.
Work in the order listed — each module's dependencies are noted explicitly.

**S3 sim policy:** All S3 I/O is simulated using the local filesystem.
Real `aws-sdk-s3` integration is deferred. The sim store mirrors the S3 key
layout exactly under `{DataDir}/tiko_pitr/{express,standard}/`, so migrating
to real S3 later is a drop-in replacement of `SimStore` only.

**Serialisation in sim:** Manifest files use `serde_json` for debuggability.
Production will use bincode + zstd — a drop-in change inside `SimStore` only.
The S3 key names (e.g. `manifest.bin`, `non_smgr_state.tar.zst`) match the
production layout even in the sim.

---

## Prerequisites

Add to workspace `Cargo.toml` (`[workspace.dependencies]`):
```toml
serde      = { version = "1", features = ["derive"] }
serde_json = "1"
```

Add to `s3worker/Cargo.toml`:
```toml
[dependencies]
serde      = { workspace = true }
serde_json = { workspace = true }

[[bin]]
name = "tiko_restore"
path = "src/bin/tiko_restore.rs"

[[bin]]
name = "tiko_archive"
path = "src/bin/tiko_archive.rs"
```

Add to `s3smgr/Cargo.toml`:
```toml
[dependencies]
serde      = { workspace = true }
serde_json = { workspace = true }
```

No AWS SDK dependency. No async runtime for I/O. `serde_json` is the only new
addition.

---

## Module 1 — Manifest Types & Merge Logic

**Status:** `[ ]`
**Depends on:** nothing
**New file:** `s3worker/src/manifest.rs`
**Modified file:** `s3worker/src/lib.rs` (add `pub mod manifest;`)

This is the pure data layer. No I/O, no PG. Every downstream module imports from here.

### Todos

- [ ] Define type alias `ChunkKey = String` with format `"{spc_oid}/{db_oid}/{rel_number}.{fork}/{chunk_id}"`

- [ ] Define `ChunkRef`:
  ```rust
  #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
  pub struct ChunkRef {
      pub branch_id: u64,   // org-scoped; identifies {org}/chunks/{branch_id}/ in standard-bucket
      pub lsn: u64,         // checkpoint LSN at which this chunk version was sealed
  }
  ```

- [ ] Define `Manifest` — single type for both base and delta manifests:
  ```rust
  #[derive(Serialize, Deserialize, Clone)]
  pub struct Manifest {
      pub checkpoint_lsn: u64,
      pub timestamp: i64,                          // unix seconds
      pub chunks: HashMap<ChunkKey, ChunkRef>,
  }
  ```
  The S3 path (`bases/` vs `deltas/`) distinguishes base from delta; no separate
  Rust type is needed.

- [ ] Implement `pub fn chunk_key_from_tag(tag: &ChunkTag) -> ChunkKey`
  — reuse `ChunkTag` fields from `s3worker/src/cache.rs`

- [ ] Implement LSN hex formatter:
  ```rust
  /// Format u64 LSN as fixed-width 16-char hex for use as S3 key suffix.
  /// e.g. 973078568u64 → "000000003A000028"
  pub fn lsn_to_hex(lsn: u64) -> String
  ```

- [ ] Implement merge function:
  ```rust
  /// Apply delta onto base (in-place). Later LSN (numeric u64) wins;
  /// the winning entry's branch_id is preserved faithfully.
  pub fn merge_delta(base: &mut HashMap<ChunkKey, ChunkRef>, delta: &HashMap<ChunkKey, ChunkRef>)
  ```

- [ ] Implement `build_chunk_map`:
  ```rust
  /// Fold deltas (ascending LSN order) onto base.
  /// Returns a new Manifest whose checkpoint_lsn = last delta's checkpoint_lsn.
  pub fn build_chunk_map(base: &Manifest, deltas: &[Manifest]) -> Manifest
  ```

- [ ] `#[cfg(test)]` unit tests (zero external deps, no tempdir needed):
  - Merge ordering: later LSN wins; winning `branch_id` preserved
  - Idempotent re-merge: applying same delta twice = same result
  - `build_chunk_map` with 3 deltas: each chunk resolves to correct `{branch_id, lsn}`
  - JSON round-trip for both types (`ChunkRef`, `Manifest`)
  - LSN comparison correctness: larger u64 wins in merge, equal LSN is idempotent
  - `lsn_to_hex` edge cases (`0u64`, `0x3A000028u64`, `u64::MAX`)

---

## Module 2 — S3-Sim Store Abstraction

**Status:** `[ ]`
**Depends on:** Module 1 (ChunkKey, ChunkRef)
**New file:** `s3worker/src/sim_store.rs`
**Modified file:** `s3worker/src/lib.rs` (add `pub mod sim_store;`)

`SimStore` replaces the S3 SDK. It implements the same logical operations using
local files under `{DataDir}/tiko_pitr/`. The filesystem layout mirrors the S3
key structure exactly so that migrating to real S3 later is a drop-in swap of
this module only.

### Directory layout

```
{DataDir}/tiko_pitr/
  express/                           ← sim for S3 Express One Zone (hot latest objects)
    {org_id}/{project_id}/
      chunks/{spc}/{db}/{rel}.{fork}/{chunk_id}/
        latest                       ← full 256 KB chunk at current checkpoint
        .staging_{lsn_hex}           ← staging file during three_step_write

  standard/                          ← sim for Standard S3 (versioned PITR archive)
    {org_id}/
      chunks/
        0/                           ← zero branch: built-in DB state; never GC'd
          {spc}/{db}/{rel}.{fork}/{chunk_id}/
            {lsn_hex}
        {branch_id}/                 ← one prefix per project (branch_id ≠ project_id)
          {spc}/{db}/{rel}.{fork}/{chunk_id}/
            {lsn_hex}                ← immutable versioned chunk
      pitr/
        {project_id}/
          bases/{lsn_hex}/
            manifest.bin             ← full chunk_key→ChunkRef map (JSON in sim)
            non_smgr_state.tar.zst   ← individual files in sim; see note below
          deltas/{lsn_hex}/
            manifest.bin             ← dirty chunks at this checkpoint only
            non_smgr_state.tar.zst
          wal/{timeline:08}/{segment}
      metadata/
        {project_id}/
          project.json
```

Note: In the sim, `non_smgr_state.tar.zst` is stored as individual files
under the same prefix (e.g. `non_smgr_state/pg_control`,
`non_smgr_state/pg_xact/...`) rather than a real tar+zstd archive.
Production replaces this with a single `tar | zstd` stream.

### Todos

- [ ] Define `SimStore`:
  ```rust
  pub struct SimStore {
      express_root:  PathBuf,   // {DataDir}/tiko_pitr/express
      standard_root: PathBuf,   // {DataDir}/tiko_pitr/standard
  }
  impl SimStore {
      pub fn new(data_dir: &Path) -> Self
      pub fn from_data_dir() -> Self   // reads DataDir from pgsys
  }
  ```

- [ ] Implement primitive file helpers (all synchronous, `std::fs` only):
  ```rust
  pub fn put_express(&self, key: &str, data: &[u8]) -> Result<()>
  pub fn put_standard(&self, key: &str, data: &[u8]) -> Result<()>
  pub fn get_express(&self, key: &str) -> Result<Option<Vec<u8>>>   // None if not found
  pub fn get_standard(&self, key: &str) -> Result<Option<Vec<u8>>> // None if not found
  pub fn copy_express_to_standard(&self, src_key: &str, dst_key: &str) -> Result<()>
  pub fn rename_express(&self, src_key: &str, dst_key: &str) -> Result<()>
  // rename(2) is atomic on POSIX — same semantics as S3 Express RenameObject
  pub fn delete_standard(&self, key: &str) -> Result<()>
  pub fn delete_express(&self, key: &str) -> Result<()>
  pub fn list_prefix_standard(&self, prefix: &str) -> Result<Vec<String>>
  pub fn list_prefix_express(&self, prefix: &str) -> Result<Vec<String>>
  ```
  Each helper creates parent directories as needed before writing.

- [ ] Define `ProjectNamespace` — stateless key formatter:
  ```rust
  pub struct ProjectNamespace {
      pub org_id:     u64,
      pub project_id: u64,
      pub branch_id:  u64,   // org-scoped; used for standard-bucket chunk paths
  }
  impl ProjectNamespace {
      pub fn new(org_id: u64, project_id: u64, branch_id: u64) -> Self

      // Express-bucket keys (per-project hot reads)
      pub fn chunk_latest_key(&self, key: &ChunkKey) -> String
      // → "{org}/{proj}/chunks/{key}/latest"
      pub fn chunk_staging_key(&self, key: &ChunkKey, lsn: u64) -> String
      // → "{org}/{proj}/chunks/{key}/.staging_{lsn_hex}"

      // Standard-bucket chunk keys (org-level, keyed by branch_id not project_id)
      pub fn chunk_versioned_key(&self, key: &ChunkKey, lsn: u64) -> String
      // → "{org}/chunks/{branch_id}/{key}/{lsn_hex}"

      // Standard-bucket PITR manifest keys (per-project)
      pub fn delta_manifest_key(&self, lsn: u64) -> String
      // → "{org}/pitr/{proj}/deltas/{lsn_hex}/manifest.bin"
      pub fn base_manifest_key(&self, lsn: u64) -> String
      // → "{org}/pitr/{proj}/bases/{lsn_hex}/manifest.bin"
      pub fn non_smgr_prefix(&self, lsn: u64) -> String
      // → "{org}/pitr/{proj}/deltas/{lsn_hex}/non_smgr_state/"

      // Standard-bucket WAL and metadata keys
      pub fn wal_key(&self, timeline: u32, segment: &str) -> String
      // → "{org}/pitr/{proj}/wal/{timeline:08}/{segment}"
      pub fn project_meta_key(&self) -> String
      // → "{org}/metadata/{proj}/project.json"

      // List prefixes (for scanning)
      pub fn delta_prefix(&self) -> String   // "{org}/pitr/{proj}/deltas/"
      pub fn base_prefix(&self) -> String    // "{org}/pitr/{proj}/bases/"
  }
  ```

- [ ] Implement `three_step_write` on `SimStore`:
  ```rust
  /// Checkpoint write: staged PUT → standard-bucket COPY → express-bucket atomic rename.
  /// Used ONLY at checkpoint time. Eviction uses put_express_latest() instead.
  pub fn three_step_write(
      &self, ns: &ProjectNamespace, key: &ChunkKey, checkpoint_lsn: u64, data: &[u8],
  ) -> Result<()> {
      let staging   = ns.chunk_staging_key(key, checkpoint_lsn);
      let versioned = ns.chunk_versioned_key(key, checkpoint_lsn);
      let latest    = ns.chunk_latest_key(key);
      self.put_express(&staging, data)?;                       // Step 1
      self.copy_express_to_standard(&staging, &versioned)?;   // Step 2
      self.rename_express(&staging, &latest)?;                 // Step 3: atomic
      Ok(())
  }
  ```

- [ ] Implement `put_express_latest` on `SimStore`:
  ```rust
  /// Eviction write: plain PUT to express-bucket `latest`.
  /// No staging, no standard-bucket copy — those happen at checkpoint.
  pub fn put_express_latest(&self, ns: &ProjectNamespace, key: &ChunkKey, data: &[u8]) -> Result<()> {
      self.put_express(&ns.chunk_latest_key(key), data)
  }
  ```

- [ ] `#[cfg(test)]` unit tests (use `tempfile::TempDir`):
  - PUT + GET round-trip on both express and standard
  - `put_express_latest`: verify `latest` key exists; no staging, no versioned object created
  - `three_step_write` full success: verify both `{lsn_hex}` in standard AND `latest` in express exist with correct content
  - Crash after step 1 only (staging exists, no versioned, no latest): verify old latest unchanged
  - Crash after step 2 only (staging + versioned exist, no latest rename): verify old latest unchanged, versioned is valid
  - Key prefix formatting: assert exact string for all key types; especially that `chunk_versioned_key` uses `branch_id`, not `project_id`
  - `list_prefix_standard`: returns correct subset of keys

  Note: add `tempfile = "3"` as a `[dev-dependency]` in `s3worker/Cargo.toml`.

---

## Module 3 — Project Context

**Status:** `[ ]`
**Depends on:** Modules 1, 2
**New file:** `s3worker/src/project.rs`
**Modified file:** `s3worker/src/lib.rs` (add `pub mod project;`)
**Modified file:** `s3worker/src/main_loop.rs` (init `GLOBAL_PROJECT_CTX` at startup)

Defines the runtime identity of the running project and provides the in-memory
base manifest used by the level-2 read fallback. Must be implemented before
the read path (Module 4) because `cached_read_blocks()` calls
`global_project_ctx()`.

Branch creation logic (which builds and writes the initial manifest to S3) is
deferred to Module 7 — it has no additional dependencies beyond this module.

### Todos

- [ ] Define `ProjectMeta` (mirrors `metadata/{project_id}/project.json`):
  ```rust
  #[derive(Serialize, Deserialize, Clone)]
  pub struct ProjectMeta {
      pub project_id:            u64,
      pub org_id:                u64,
      pub branch_id:             u64,   // org-scoped; identifies {org}/chunks/{branch_id}/
      pub parent_project_id:     Option<u64>,
      pub parent_branch_id:      Option<u64>,
      pub branch_checkpoint_lsn: Option<u64>,
      pub branch_timeline_id:    Option<u32>,
      pub created_at:            i64,
      pub status:                String,
  }
  ```

- [ ] Define `ProjectCtx`:
  ```rust
  pub struct ProjectCtx {
      pub meta:          ProjectMeta,
      pub ns:            ProjectNamespace,
      /// In-memory base manifest loaded from pitr/{proj}/bases/ at startup.
      /// Covers all chunks the project can read: own chunks from past checkpoints
      /// and inherited chunks from ancestor branches (including zero branch).
      /// Refreshed each time the base materializer writes a newer manifest.
      pub base_manifest: Manifest,
  }
  impl ProjectCtx {
      /// Load project.json, then the latest base manifest from pitr/{proj}/bases/.
      pub fn load(sim: &SimStore, ns: &ProjectNamespace) -> Result<Self>
      pub fn is_branch(&self) -> bool   // true if parent_project_id is Some
      /// Look up a chunk in the base manifest (level-2 read fallback).
      pub fn base_manifest_lookup(&self, key: &ChunkKey) -> Option<&ChunkRef>
  }
  ```

- [ ] Add `static GLOBAL_PROJECT_CTX: OnceLock<ProjectCtx>` and
  `pub fn global_project_ctx() -> &'static ProjectCtx`

- [ ] In `main_loop.rs` `s3worker_main()`: before entering the event loop call
  `init_sim_store(...)` (from Module 4) then `ProjectCtx::load()` to populate
  `GLOBAL_PROJECT_CTX`

- [ ] `#[cfg(test)]` (tempdir):
  - Load root `ProjectCtx` (no parent): `is_branch()` = false; `base_manifest_lookup`
    returns zero-branch chunks correctly
  - Load branch `ProjectCtx` with a synthetic base manifest in sim store:
    `base_manifest_lookup` returns correct `ChunkRef` for a known key, None for unknown
  - `ProjectCtx::load` when `bases/` is empty: returns an error (no manifest to start from)

---

## Module 4 — Versioned Chunk Read Path

**Status:** `[ ]`
**Depends on:** Modules 1, 2, 3
**Modified file:** `s3worker/src/s3_ops.rs`
**New file:** `s3worker/src/recovery.rs`
**Modified file:** `s3worker/src/lib.rs` (add `pub mod recovery;`)

Extends the existing local-file read path with the two-level branch fallback and
recovery mode. `write_blocks()` is **not** modified here — versioned S3 writes
happen in Module 5 (eviction) and Module 6 (checkpoint).

### Todos

**recovery.rs:**
- [ ] Define globals:
  ```rust
  static RECOVERY_MODE: AtomicBool = AtomicBool::new(false);
  static CHUNK_MAP: OnceLock<HashMap<ChunkKey, ChunkRef>> = OnceLock::new();
  ```
- [ ] `pub fn load_recovery_manifest(path: &Path) -> Result<()>`:
  Read JSON file at `path` (= `$PGDATA/tiko_recovery_manifest.bin`); deserialise
  into `HashMap<ChunkKey, ChunkRef>`; set `CHUNK_MAP`; set `RECOVERY_MODE = true`.
  (In sim, JSON; in production, bincode + zstd.)
- [ ] `pub fn clear_recovery_mode()` — set `RECOVERY_MODE = false` (after PG promotion)
- [ ] `pub fn is_recovery_mode() -> bool`
- [ ] `pub fn lookup_recovery_chunk(key: &ChunkKey) -> Option<ChunkRef>`

**s3_ops.rs additions:**
- [ ] Add `static SIM_STORE: OnceLock<SimStore>` — initialised at s3worker startup
- [ ] Add `static PROJECT_NS: OnceLock<ProjectNamespace>` — from env vars
  (`TIKO_ORG_ID`, `TIKO_PROJECT_ID`, `TIKO_BRANCH_ID`)
- [ ] Add `pub fn init_sim_store(data_dir: &Path, org_id: u64, project_id: u64, branch_id: u64)`
  — populates both statics; called once from `s3worker_main()` before `ProjectCtx::load()`
- [ ] Add `pub fn get_sim() -> &'static SimStore`
- [ ] Add `pub fn get_ns() -> &'static ProjectNamespace`

- [ ] Modify `cached_read_blocks()`: on cache miss, after existing pread attempt:
  1. **Recovery mode** (`is_recovery_mode()` = true):
     - `lookup_recovery_chunk(key)` → GET from standard sim at
       `{org}/chunks/{chunk_ref.branch_id}/{key}/{lsn_hex}`
     - If found, fill cache slot, continue
  2. **Normal, level 1** (own express-bucket `latest`):
     - GET `express/{org}/{proj}/chunks/{key}/latest`
     - If found, fill cache slot, continue
  3. **Normal, level 2** (base_manifest fallback for inherited chunks):
     - `global_project_ctx().base_manifest_lookup(key)` → `ChunkRef{branch_id, lsn}`
     - GET from standard sim at `{org}/chunks/{branch_id}/{key}/{lsn_hex}`
     - If found, fill cache slot, continue
  4. All misses: zero-fill (block beyond file extent — existing behaviour)

- [ ] `#[cfg(test)]` (tempdir):
  - Level-1 express hit: put data in express `latest`; delete backing file; read → correct data
  - Level-2 branch fallback: put data in standard sim under a different `branch_id`; configure
    base manifest with that `branch_id`; simulate level-1 miss; verify level-2 returns correct data
  - Level-2 GET key uses `branch_id` (not `project_id`) in the path
  - Recovery mode: write synthetic `tiko_recovery_manifest.bin`; put versioned data in standard
    sim; delete backing file; read → correct versioned data returned

---

## Module 5 — Cache Eviction Log

**Status:** `[ ]`
**Depends on:** Modules 1, 2
**Modified file:** `s3worker/src/cache.rs`

Can be developed in parallel with Module 4.

### Todos

- [ ] Define `EvictionLogRecord` (exactly 20 bytes — 5 × u32):
  ```rust
  #[repr(C)]
  pub struct EvictionLogRecord {
      pub spc_oid:    u32,
      pub db_oid:     u32,
      pub rel_number: u32,
      pub fork:       u32,
      pub chunk_id:   u32,
  }
  const _: () = assert!(std::mem::size_of::<EvictionLogRecord>() == 20);
  // write(2) with O_APPEND is atomic on local Linux filesystems
  // (kernel serialises concurrent appenders via the inode lock).
  // pwrite(2) must NOT be used: POSIX specifies that pwrite ignores O_APPEND.
  ```

- [ ] Add `fn eviction_log_path() -> PathBuf` → `$PGDATA/tiko/eviction_log`

- [ ] Add `fn open_eviction_log() -> File`:
  `OpenOptions::new().write(true).create(true).append(true).open(eviction_log_path())`

- [ ] Extend `flush_dirty_chunk(slot_index: u32)`:
  After existing write to `{DataDir}/tiko/` backing file:
  1. Read chunk data from the cache slot
  2. Call `get_sim().put_express_latest(get_ns(), &chunk_key, &chunk_data)`
     — plain PUT to express-bucket `latest`; no staging, no standard-bucket copy
  3. On PUT success only: `write(fd, &record)` one `EvictionLogRecord` with `O_APPEND`
     (no phantom log entry if PUT failed)

- [ ] Add `pub fn read_eviction_log(path: &Path) -> Vec<EvictionLogRecord>`:
  Read in 20-byte records; skip any incomplete trailing record (crash safety).

- [ ] `#[cfg(test)]` (tempdir):
  - Concurrent `flush_dirty_chunk` from N threads on N different slots: verify log has
    exactly N records; no corruption (aligned 20-byte reads)
  - `read_eviction_log` with truncated final record (write 30 bytes): verify partial record skipped
  - After `flush_dirty_chunk`: express `latest` exists; NO staging file; NO standard-bucket
    versioned object (those are checkpoint-only)

---

## Module 6 — Checkpoint Flush

**Status:** `[ ]`
**Depends on:** Modules 1, 2, 4, 5
**Rewritten file:** `s3smgr/src/checkpoint.rs` (expand from 20 lines)
**New file:** `s3smgr/src/wal_archive.rs`

The checkpointer runs as a plain PG process — no Tokio runtime. All sim I/O here
uses `std::fs` directly (synchronous), which is correct since `SimStore` is sync.

### Todos

**wal_archive.rs:**
- [ ] Implement `pub fn upload_delta_manifest(sim: &SimStore, ns: &ProjectNamespace, checkpoint_lsn: u64, manifest: &Manifest) -> Result<()>`:
  `serde_json::to_vec(manifest)` → `sim.put_standard(&ns.delta_manifest_key(checkpoint_lsn), &json)`

- [ ] Implement `pub fn upload_non_smgr_state(sim: &SimStore, ns: &ProjectNamespace, checkpoint_lsn: u64, pgdata: &Path) -> Result<()>`:
  Copy the following files under `{ns.non_smgr_prefix(checkpoint_lsn)}` in standard sim:
  - `pg_control` (8 KB)
  - `pg_xact/*` (all segment files)
  - `pg_multixact/members/*` and `pg_multixact/offsets/*`
  - `pg_filenode.map`
  Use `std::fs::copy` for each file. (Production: bundle into `non_smgr_state.tar.zst`.)

- [ ] Implement `pub fn upload_wal_segment(sim: &SimStore, ns: &ProjectNamespace, timeline: u32, segment: &str, path: &Path) -> Result<()>`:
  `std::fs::read(path)` → `sim.put_standard(&ns.wal_key(timeline, segment), &bytes)`
  Called by `tiko_archive` binary (Module 9).

- [ ] Implement `pub fn download_wal_segment(sim: &SimStore, ns: &ProjectNamespace, timeline: u32, segment: &str, dest: &Path) -> Result<bool>`:
  `sim.get_standard(&ns.wal_key(timeline, segment))` → write bytes to `dest`; returns
  `Ok(false)` if not found (caller tries parent namespace).

**checkpoint.rs:**
- [ ] Replace current stub with `s3_checkpoint_flush(checkpoint_lsn: u64)`:
  ```
  1. evict_all_dirty_chunks()
     // Flushes every remaining dirty cache slot: PUT to express-bucket `latest`
     // + append EvictionLogRecord. After this, the eviction log contains ALL
     // dirty chunks from this checkpoint interval (both mid-interval evictions
     // and those just flushed here).

  2. std::fs::rename("tiko/eviction_log", "tiko/eviction_log.ckpt")
     // Atomic snapshot. New evictions after this point write to a fresh log.

  3. records = read_eviction_log("tiko/eviction_log.ckpt")
     dirty_chunks = dedup_by_chunk_key(records)
     // Dedup: a chunk evicted multiple times in the interval is uploaded once.

  4. for chunk_key in &dirty_chunks {
         sim.three_step_write(ns, &chunk_key, checkpoint_lsn, &read_chunk_data(&chunk_key))
     }
     // Full 3-step PUT→COPY→Rename, all keyed by the same checkpoint_lsn.

  5. manifest = Manifest {
         checkpoint_lsn,
         timestamp: now_unix(),
         chunks: dirty_chunks.iter().map(|key| {
             (key.clone(), ChunkRef { branch_id: get_ns().branch_id, lsn: checkpoint_lsn })
         }).collect(),
     };
     upload_delta_manifest(sim, ns, checkpoint_lsn, &manifest)
     upload_non_smgr_state(sim, ns, checkpoint_lsn, pgdata)

  6. std::fs::remove_file("tiko/eviction_log.ckpt")
  ```

- [ ] Add `fn dedup_by_chunk_key(records: Vec<EvictionLogRecord>) -> Vec<ChunkKey>`:
  Build a `HashSet<ChunkKey>` from all records; return as `Vec`. Order doesn't matter.

- [ ] Keep existing guard: `if !S3IoControl::is_initialized() { return; }`

- [ ] Update C call site in `postgres/src/backend/access/transam/xlog.c`:
  `s3_checkpoint_flush(checkpoint_lsn)` — single argument, no `prev_lsn`

- [ ] `#[cfg(test)]` (tempdir):
  Synthesise all 5 eviction scenarios from the design doc:
  1. Chunk dirtied → checkpoint (still in cache) → `evict_all_dirty_chunks` flushes it to log
  2. Chunk dirtied → evicted mid-interval → checkpoint → eviction log has entry
  3. Chunk dirtied → evicted → re-dirtied → evicted → checkpoint → dedup collapses to one upload
  4. Crash between eviction PUT and log append → no log entry; WAL replay handles the gap
     (document as known gap; not testable as a unit test)
  5. Crash during rename-swap → `eviction_log.ckpt` exists, `eviction_log` absent → re-process
  - For each: run full `s3_checkpoint_flush` → assert delta manifest JSON has correct chunks
  - All chunks in manifest have `lsn == checkpoint_lsn` and `branch_id == own branch_id`
  - Idempotent: call again with `.ckpt` still present (simulated crash) → same manifest, no error
  - `eviction_log.ckpt` removed on success

---

## Module 7 — Branch Creation

**Status:** `[ ]`
**Depends on:** Modules 1, 2, 3
**Modified file:** `s3worker/src/project.rs` (add branch creation functions)

Can be developed in parallel with Module 6.

Branch creation is a control-plane operation that builds a frozen chunk map
from a parent's manifests and writes the child's initial base manifest. It
only needs `SimStore` and `Manifest` types — no dependency on the checkpoint
or eviction machinery.

### Todos

- [ ] Implement `pub fn build_initial_manifest(sim: &SimStore, parent_ns: &ProjectNamespace, branch_lsn: u64) -> Result<Manifest>`:
  1. List `{parent_ns.base_prefix()}` → parse LSN dirs → find latest with `base_lsn ≤ branch_lsn`
  2. GET + deserialise `Manifest` from `bases/{lsn_hex}/manifest.bin`
  3. List `{parent_ns.delta_prefix()}` → filter `(base_lsn, branch_lsn]` → GET + deserialise each delta
  4. `build_chunk_map(base, &deltas)` — result is flat (parent's `branch_id` values preserved)
  5. Return the merged `Manifest` as the child's initial base manifest

- [ ] Implement `pub fn create_branch(sim: &SimStore, parent_ns: &ProjectNamespace, child_ns: &ProjectNamespace, branch_lsn: u64) -> Result<()>`:
  ```
  1. initial_manifest = build_initial_manifest(sim, parent_ns, branch_lsn)
  2. meta = ProjectMeta {
         project_id: child_ns.project_id,
         org_id: child_ns.org_id,
         branch_id: child_ns.branch_id,
         parent_project_id: Some(parent_ns.project_id),
         parent_branch_id: Some(parent_ns.branch_id),
         branch_checkpoint_lsn: Some(branch_lsn),
         ...
     }
  3. sim.put_standard(&child_ns.project_meta_key(), &serde_json::to_vec(&meta)?)
  4. sim.put_standard(&child_ns.base_manifest_key(branch_lsn),
                      &serde_json::to_vec(&initial_manifest)?)
  ```
  Branch is valid once step 4 completes — a single S3 PUT.
  No `branch_base.json`, no `branch_refs` markers.

- [ ] Implement `pub fn delete_branch(sim: &SimStore, branch_ns: &ProjectNamespace) -> Result<()>`:
  1. List all keys under `express/{org}/{proj}/` → delete each (express-bucket hot data)
  2. List all keys under `standard/{org}/pitr/{proj}/` → delete each (manifests + WAL)
  3. List all keys under `standard/{org}/metadata/{proj}/` → delete each (project.json)
  4. Log: "standard-bucket {org}/chunks/{branch_id}/ will be collected by next GC run"

- [ ] `#[cfg(test)]` (tempdir):
  - `build_initial_manifest` from 3 synthetic deltas → correct merged chunk map with correct `branch_id` values per chunk
  - Cascaded branch (C from B from A): chunks written only on A have `branch_id = A`'s id in C's map
  - `create_branch` → exactly 2 files written to standard sim (`project.json` + `manifest.bin`); no `branch_base.json`, no `branch_refs`
  - `delete_branch` → express and pitr/metadata files removed; `{org}/chunks/{branch_id}/` untouched
  - `build_initial_manifest` when no base exists with `base_lsn ≤ branch_lsn`: returns error

---

## Module 8 — PITR Background Task

**Status:** `[ ]`
**Depends on:** Modules 1, 2, 3
**New file:** `s3worker/src/pitr_task.rs`
**Modified file:** `s3worker/src/thread_pool.rs` (spawn task after runtime starts)
**Modified file:** `s3worker/src/lib.rs` (add `mod pitr_task;`)

Can be developed in parallel with Modules 6 and 7.

The background task performs **base materialization only**. GC (retention
enforcement) runs exclusively on the control plane via `enforce_retention_org` —
it is not part of s3worker or this task.

The task runs on Tokio. Since `SimStore` is sync (`std::fs`), wrap sim calls
with `tokio::task::spawn_blocking` to avoid blocking Tokio workers, or call
them directly (acceptable for sim — local FS is fast).

### Todos

**pitr_task.rs:**
- [ ] Define `PitrConfig`:
  ```rust
  pub struct PitrConfig {
      pub materialization_interval: std::time::Duration, // TIKO_PITR_INTERVAL_SECS (default 3600)
  }
  impl PitrConfig { pub fn from_env() -> Self }
  ```

- [ ] Implement `pub async fn pitr_background_task(sim: Arc<SimStore>, ns: ProjectNamespace, config: PitrConfig)`:
  ```rust
  let mut interval = tokio::time::interval(config.materialization_interval);
  loop {
      interval.tick().await;
      // Non-fatal: log warning and continue. Deltas are the source of truth.
      if let Err(e) = materialize_base(&sim, &ns) { pg_log_warning(...) }
      // NOTE: no enforce_retention here — GC runs on the control plane only.
  }
  ```

- [ ] Implement `fn materialize_base(sim: &SimStore, ns: &ProjectNamespace) -> Result<()>`:
  1. `sim.list_prefix_standard(&ns.base_prefix())` → parse LSN dirs → GET latest `manifest.bin`
  2. `sim.list_prefix_standard(&ns.delta_prefix())` → filter newer than `base_lsn` → GET + deserialise each delta
  3. If no new deltas: return `Ok(())`
  4. `build_chunk_map(base, &deltas)` → new `Manifest`
  5. `sim.put_standard(&ns.base_manifest_key(new_lsn), &json)` — single atomic write
  6. Do NOT delete deltas — cleanup is `enforce_retention_org`'s responsibility

- [ ] `#[cfg(test)]` (tempdir, synchronous):
  - Seed sim with 10 delta manifests + a base at delta 3
  - `materialize_base`: verify new base matches expected merged map (base + deltas 4–10)
  - Idempotent: run again → no new base written (no new deltas since last base)
  - All 10 delta files still present after materialization

**thread_pool.rs:**
- [ ] After `init_tokio_runtime()`, spawn:
  ```rust
  let sim = Arc::new(SimStore::from_data_dir());
  let ns  = global_project_ctx().ns.clone();
  let cfg = PitrConfig::from_env();
  runtime.spawn(pitr_background_task(Arc::clone(&sim), ns, cfg));
  ```
- [ ] Store `Arc<SimStore>` in a `OnceLock` for access from Module 5's eviction path

---

## Module 9 — WAL Archive & Restore

**Status:** `[ ]`
**Depends on:** Modules 2, 6
**New files:** `s3worker/src/bin/tiko_restore.rs`, `s3worker/src/bin/tiko_archive.rs`
**Modified file:** `s3worker/Cargo.toml` (already has `[[bin]]` entries from prerequisites)

Can be developed in parallel with Modules 7 and 8.
Both binaries are short (<100 lines each) — pure `std::fs` + `SimStore`.

### Todos

**tiko_archive.rs:**
- [ ] Parse `archive_command = 'tiko_archive %p %f'` args: `argv[1]` = WAL path, `argv[2]` = WAL filename
- [ ] Build `SimStore::from_data_dir()` and `ProjectNamespace` from env
  (`TIKO_ORG_ID`, `TIKO_PROJECT_ID`, `TIKO_BRANCH_ID`)
- [ ] `timeline = parse_timeline_from_filename(argv[2])` (first 8 hex chars of segment name)
- [ ] Call `upload_wal_segment(sim, ns, timeline, filename, path)` from Module 6
- [ ] Exit 0 on success, exit 1 on any error (PG retries non-zero exit)

**tiko_restore.rs:**
- [ ] Parse `restore_command = 'tiko_restore %f %p'` args: `argv[1]` = WAL filename, `argv[2]` = dest path
- [ ] Build `SimStore::from_data_dir()`, own `ProjectNamespace` from
  `TIKO_ORG_ID`, `TIKO_PROJECT_ID`, `TIKO_BRANCH_ID`
- [ ] Read optional env vars `TIKO_PARENT_PROJECT_ID`, `TIKO_PARENT_BRANCH_ID`,
  `TIKO_BRANCH_CHECKPOINT_LSN` for branch WAL fallback
- [ ] `restore_segment(sim, own_ns, parent_ns_opt, branch_lsn_opt, filename, dest)`:
  1. Try `download_wal_segment(sim, own_ns, timeline, filename, dest)` → found: exit 0
  2. If not found AND branch context present AND `segment_lsn ≤ branch_checkpoint_lsn`:
     try `download_wal_segment(sim, parent_ns, timeline, filename, dest)` → found: exit 0
  3. All misses: exit 1
- [ ] Exit 0 on success, exit 1 on failure

**PostgreSQL configuration:**
```ini
wal_level         = replica
archive_mode      = on
archive_command   = '/path/to/tiko_archive %p %f'
restore_command   = '/path/to/tiko_restore %f %p'
archive_timeout   = 300   # force archive every 5 min
```

For branch provisioning:
```bash
export TIKO_ORG_ID=1001
export TIKO_PROJECT_ID=2002
export TIKO_BRANCH_ID=2
export TIKO_PARENT_PROJECT_ID=2001
export TIKO_PARENT_BRANCH_ID=1
export TIKO_BRANCH_CHECKPOINT_LSN=000000003A000028   # 16-char hex u64

touch $PGDATA/recovery.signal
```

- [ ] `#[cfg(test)]` (tempdir):
  - Archive a synthetic WAL file → verify sim standard `wal/` file exists with correct bytes
  - Restore it via `download_wal_segment` → byte equality
  - Branch fallback: segment only in parent's sim namespace → restore finds it
  - Segment beyond `branch_checkpoint_lsn` in parent WAL → not fetched (exit 1)
  - Segment absent from both → exit 1

---

## Dependency Graph

```
Module 1 (manifest types)     ← no deps; start here
Module 2 (sim store)          ← no deps; start here
         |
Module 3 (project context)    ← deps: 1, 2
         |
    ┌────┴────┐
    │         │
Module 4    Module 5           ← develop in parallel
(read path) (eviction log)
← 1, 2, 3   ← 1, 2
    │         │
    └────┬────┘
         │
Module 6 (checkpoint flush)   ← deps: 1, 2, 4, 5
         │
    ┌────┴────────────┐
    │        │        │
Module 7  Module 8  Module 9  ← develop in parallel
(branch   (bg task) (WAL)
creation) ← 1,2,3  ← 2, 6
← 1,2,3
```

---

## File Map

| Module | New files | Modified files |
|--------|-----------|----------------|
| Prereqs | — | `Cargo.toml`, `s3worker/Cargo.toml`, `s3smgr/Cargo.toml` |
| 1 | `s3worker/src/manifest.rs` | `s3worker/src/lib.rs` |
| 2 | `s3worker/src/sim_store.rs` | `s3worker/src/lib.rs` |
| 3 | `s3worker/src/project.rs` | `s3worker/src/lib.rs`, `s3worker/src/main_loop.rs` |
| 4 | `s3worker/src/recovery.rs` | `s3worker/src/s3_ops.rs`, `s3worker/src/lib.rs` |
| 5 | — | `s3worker/src/cache.rs` |
| 6 | `s3smgr/src/wal_archive.rs` | `s3smgr/src/checkpoint.rs`, `postgres/src/backend/access/transam/xlog.c` |
| 7 | — | `s3worker/src/project.rs` |
| 8 | `s3worker/src/pitr_task.rs` | `s3worker/src/thread_pool.rs`, `s3worker/src/lib.rs` |
| 9 | `s3worker/src/bin/tiko_restore.rs`, `s3worker/src/bin/tiko_archive.rs` | `s3worker/Cargo.toml` |

---

## Verification

### Unit + integration tests (no external services required)
```bash
cargo test -p s3worker    # Modules 1, 2, 3, 4, 5, 7, 8, 9 — all use tempdir
cargo test -p s3smgr      # Module 6 — uses tempdir
```
All tests run with `cargo test` only. No LocalStack, no Docker, no network.

### Full regression test
```bash
./run_test.sh
# Runs postgres/src/test/modules/test_tiko/ via pg_regress
```

### End-to-end branch scenario (manual, against a real PG instance)
```
1. Start root DB with archive_command = tiko_archive
2. Create test table, insert rows, force checkpoint (SELECT pg_checkpoint())
3. Call create_branch() for the checkpoint LSN
4. Start branch DB (recovery.signal + restore_command = tiko_restore)
5. Write different rows on root and branch; checkpoint both
6. Assert: root sim express/standard shows root-only changes (branch_id = root's)
7. Assert: branch sim express shows branch-only changes; level-2 reads resolve
   inherited chunks via base_manifest to root's branch_id in standard-bucket
8. PITR root to pre-branch checkpoint → state matches branch starting point
9. PITR branch to its own past checkpoint → correct isolation from root
```

### Future: real S3 migration
When real S3 is needed, replace `SimStore` in `s3worker/src/sim_store.rs` with
an `aws-sdk-s3`-backed implementation exposing the same method signatures.
Manifest serialisation changes from JSON to bincode + zstd inside `SimStore`.
All other modules are unchanged.
