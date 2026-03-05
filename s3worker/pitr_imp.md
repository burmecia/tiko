# PITR Implementation Plan

This document is the executable implementation plan for PITR and database branching
support in Tiko. Each module is self-contained and independently testable.
Work in the order listed — each module's dependencies are noted explicitly.

**S3 sim policy:** All S3 I/O is simulated using the local filesystem.
Real `aws-sdk-s3` integration is deferred. The sim store mirrors the S3 key
layout exactly under `{DataDir}/tiko_sim/{express,standard}/`, so migrating
to real S3 later is a drop-in replacement of `SimStore` only.

**Serialisation:** All structured data (manifests, project metadata) uses
MessagePack via `rmp-serde` + `zstd` compression as the S3/wire format.
Locally, manifests are stored as a fixed-size sorted binary file (TIKM format)
derived from the S3 bytes, enabling O(log N) binary search via direct `pread`.
WAL segments and chunk data are stored as raw bytes. `pg_state.tar.zst` is a
real `tar` archive compressed with `zstd` — no shortcuts.

---

## Prerequisites

Add to workspace `Cargo.toml` (`[workspace.dependencies]`):
```toml
serde      = { version = "1", features = ["derive"] }
rmp-serde  = "1"
zstd       = "0.13"
tar        = "0.4"
```

Add to `s3worker/Cargo.toml`:
```toml
[dependencies]
serde      = { workspace = true }
rmp-serde  = { workspace = true }
zstd       = { workspace = true }
tar        = { workspace = true }

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
rmp-serde  = { workspace = true }
zstd       = { workspace = true }
tar        = { workspace = true }
```

No AWS SDK dependency. No async runtime for I/O. `rmp-serde`, `zstd`, and `tar`
are the only new additions.

---

## Module 1 — Manifest Types & Merge Logic

**Status:** `[x]`
**Depends on:** nothing
**New file:** `s3worker/src/manifest.rs`
**Modified file:** `s3worker/src/lib.rs` (add `pub mod manifest;`)

`Manifest` is the unified type for both base and delta manifests. It is:
- **Stored on S3** as `manifest.bin` — a `zstd(msgpack(...))` blob.
- **Cached locally** as a fixed-size sorted binary file (TIKM format) that
  enables O(log N) binary search via direct `pread` calls (no in-memory page
  cache — the block cache in `cache.rs` already covers the hot path).

Both base and delta manifests use this same type, same local file format, and
same S3 wire format. The S3 path (`bases/` vs `deltas/`) distinguishes kind;
no separate Rust type is needed. The `materialize_base` function produces a
`Manifest` by merging deltas onto a base via `apply_deltas`.

### Local file format (TIKM — binary, random-access)

Path: caller-specified (e.g. `{DataDir}/tiko/base_manifest.bin`).

```
Header (32 bytes):
  magic:          [u8; 4] = b"TIKM"
  version:        u32     = 1
  checkpoint_lsn: u64
  timestamp:      i64     (unix seconds)
  entry_count:    u64

Body (entry_count × 36 bytes, sorted ascending by ChunkTag):
  ChunkTag  20 bytes  (spc_oid u32, db_oid u32, rel_number u32,
                        fork_number i32, chunk_id u32 — all little-endian)
  ChunkRef  16 bytes  (branch_id u64, lsn u64 — little-endian)
```

Entries are densely packed. Entry `i` starts at byte `32 + i * 36`.
Lookup makes at most `⌈log₂(N)⌉` `pread` calls (≈ 22 for 4 M entries).
An entry near a page boundary may straddle two 4096-byte aligned reads.

### S3 / wire format (compact, portable)

`manifest.bin` on S3 is `zstd(msgpack((checkpoint_lsn, timestamp, chunks)))` where
`chunks` is a sorted `Vec<(ChunkTag, ChunkRef)>`. `from_bytes` converts S3 bytes →
local TIKM file; `to_bytes` converts local TIKM file → S3 bytes.

### Todos

- [x] Reuse `ChunkTag` from `s3worker/src/cache.rs` as the manifest key type
  (derive `Eq`, `Hash`, `Serialize`, `Deserialize` on `ChunkTag` if not already present)

- [x] Add `#[derive(PartialOrd, Ord)]` to `ChunkTag` in `cache.rs` — required
  for sorting and binary search; field declaration order determines sort order:
  `spc_oid → db_oid → rel_number → fork_number → chunk_id`

- [x] Reuse `pgsys::Lsn` as the common LSN wrapper type across modules

- [x] Define `ChunkRef`:
  ```rust
  #[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
  pub struct ChunkRef {
      pub branch_id: u64,   // org-scoped; identifies {org}/chunks/{branch_id}/ in standard-bucket
      pub lsn: Lsn,         // checkpoint LSN at which this chunk version was sealed
  }
  ```

- [x] Define `Manifest` — single file-backed type for both base and delta manifests.
  All mutable state lives inside `Mutex<ManifestInner>` so that `apply_deltas`
  and `lookup` can be called through `&self` (required for use through
  `&'static ProjectCtx` obtained from `OnceLock`):
  ```rust
  pub struct Manifest {
      /// All mutable state; replaced atomically on apply_deltas.
      inner: Mutex<ManifestInner>,
  }

  struct ManifestInner {
      checkpoint_lsn: Lsn,
      timestamp: i64,
      /// Path to the local TIKM binary file.
      path: PathBuf,
      /// Read handle; replaced on apply_deltas (new file, same path after rename).
      file: File,
      /// Total number of 36-byte entries in the current file.
      entry_count: u64,
  }
  ```
  Invariant: the local TIKM file at `path` is always valid with entries sorted
  ascending by `ChunkTag`. Only `new_sorted`, `from_bytes`, and `apply_deltas`
  may create or overwrite this file.

- [x] Implement `pub fn new_sorted(checkpoint_lsn: Lsn, timestamp: i64, mut chunks: Vec<(ChunkTag, ChunkRef)>, path: &Path) -> io::Result<Self>`:
  Sort `chunks` by `ChunkTag`, write TIKM header + entries to `path`, open the
  file, return `Manifest`. Use this everywhere a manifest is constructed from
  scratch (checkpoint flush, `build_initial_manifest`).

- [x] Implement `pub fn open(path: &Path) -> io::Result<Self>`:
  Open an existing local TIKM file; validate `magic` + `version`; read
  `checkpoint_lsn`, `timestamp`, `entry_count` from header; return `Manifest`.
  Used at startup if the local file already exists and we want to avoid
  re-downloading from S3.

- [x] Implement `pub fn from_bytes(data: &[u8], path: &Path) -> io::Result<Self>`:
  `zstd::decode_all(data)` → `rmp_serde::from_slice` into a temp
  `(Lsn, i64, Vec<(ChunkTag, ChunkRef)>)` → call `new_sorted(lsn, ts, chunks, path)`.
  Used whenever an S3 `manifest.bin` is downloaded.

- [x] Implement `pub fn to_bytes(&self) -> io::Result<Vec<u8>>`:
  Acquire inner lock → `read_all_entries()` → collect into `Vec<(ChunkTag, ChunkRef)>` →
  `rmp_serde::to_vec(&(checkpoint_lsn, timestamp, &chunks))` → `zstd::encode_all(msgpack, 3)`.
  Used to upload the merged manifest back to S3.

- [x] Implement `pub fn checkpoint_lsn(&self) -> Lsn` and `pub fn timestamp(&self) -> i64`:
  Acquire inner lock, read field, release. Convenience accessors for callers
  that cannot destructure the inner lock directly.

- [x] Implement `pub fn lookup(&self, key: &ChunkTag) -> io::Result<Option<ChunkRef>>`:
  Acquire inner lock. Binary search on `entry_count`. For each candidate index `i`:
  1. Compute byte offset `off = 32 + i * 36`.
  2. `pread` 36 bytes at `off` (two aligned reads if the entry straddles a
     4096-byte boundary; this is rare and only adds one extra syscall).
  3. Compare `ChunkTag` (first 20 bytes) with target; adjust `lo`/`hi`.

- [x] Implement private `fn read_all_entries(inner: &ManifestInner) -> io::Result<Vec<(ChunkTag, ChunkRef)>>`:
  Sequential `pread` of all entries from offset 32; used by `to_bytes` and
  the merge step of `apply_deltas`.

- [x] Implement `pub fn apply_deltas(&self, deltas: &[Manifest]) -> io::Result<()>`:
  ```
  Acquire self's inner lock.
  For each delta in deltas (acquire each delta's lock briefly):
    read_all_entries(delta_inner) → append to combined_delta: Vec<(ChunkTag, ChunkRef)>
  Release each delta lock.
  Sort combined_delta by ChunkTag; dedup by keeping highest LSN per ChunkTag.
  Two-pointer merge of sequential scan over self's file + combined_delta:
    — on equal ChunkTag: keep entry with higher LSN (tie: keep self's entry)
    — write output sequentially to "{path}.tmp"
  Atomic fs::rename("{path}.tmp", path).
  Reopen file handle at path; update checkpoint_lsn and timestamp to the
  last non-empty delta's values; update entry_count.
  ```
  Collecting all deltas first then doing a single two-way merge avoids
  re-scanning the (potentially large) base file once per delta. Panics in
  debug builds if deltas are not in ascending LSN order.
  `apply_deltas` with an empty slice is a no-op (no file I/O).

- [x] Implement `pub fn chunk_tag_to_path(tag: &ChunkTag) -> String`
  — format as `"{spc_oid}/{db_oid}/{rel_number}.{fork}/{chunk_id}"` for S3 key paths

- [x] Use `Lsn` member utility functions for LSN formatting/parsing:
  - `Lsn::to_hex()` for fixed-width 16-char uppercase hex S3 key suffix
  - `Lsn::from_hex(...)` for parsing hex LSNs from env/config

- [x] `#[cfg(test)]` unit tests (use `tempfile` crate for tempdir):
  - `new_sorted` → header magic, version, entry_count correct; entries in ascending order
  - `new_sorted` → `lookup` hit: correct `ChunkRef` returned
  - `new_sorted` → `lookup` miss: `Ok(None)` for absent key
  - `from_bytes` → `to_bytes` round-trip: all entries preserved with correct values
  - `open` round-trip: write via `new_sorted`, `open` same path → same `checkpoint_lsn`,
    `timestamp`, `entry_count`
  - `apply_deltas` with 3 deltas: each chunk resolves to correct `{branch_id, lsn}`
  - `apply_deltas` with empty slice: manifest unchanged (entries, lsn, timestamp, no file I/O)
  - `apply_deltas` idempotent: applying same delta twice = same result
  - `apply_deltas` tie at equal LSN: existing self entry is kept (branch_id unchanged)
  - LSN comparison: larger u64 wins in merge, equal LSN keeps self entry
  - `Lsn::to_hex()` edge cases (`0u64`, `0x3A000028u64`, `u64::MAX`)

  Note: add `tempfile = "3"` as a `[dev-dependency]` in `s3worker/Cargo.toml`.

---

## Module 2 — S3-Sim Store Abstraction

**Status:** `[x]`
**Depends on:** Module 1 (ChunkTag, ChunkRef)
**New file:** `s3worker/src/sim_store.rs`
**Modified file:** `s3worker/src/lib.rs` (add `pub mod sim_store;`)

`SimStore` replaces the S3 SDK. It implements the same logical operations using
local files under `{DataDir}/tiko_sim/`. The filesystem layout mirrors the S3
key structure exactly so that migrating to real S3 later is a drop-in swap of
this module only.

### Directory layout

```
{DataDir}/tiko_sim/
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
            manifest.bin             ← full ChunkTag→ChunkRef map (msgpack + zstd)
            pg_state.tar.zst         ← tar archive of PG state files, zstd-compressed
          deltas/{lsn_hex}/
            manifest.bin             ← dirty chunks at this checkpoint only (same format)
            pg_state.tar.zst
          wal/{timeline:08}/{segment}
      metadata/
        {project_id}/
          project.json
```

`manifest.bin` at both `bases/` and `deltas/` paths uses the identical S3 wire
format: `zstd(msgpack((checkpoint_lsn, timestamp, chunks)))`. The `Manifest`
type handles both. `pg_state.tar.zst` is always a real tar+zstd archive
(built with the `tar` + `zstd` crates). No individual-file shortcut.

### Todos

- [x] Define `SimStore` with `new(data_dir)` and `from_data_dir()` constructors
- [x] Implement primitive file helpers (`put_*`, `get_*`, `copy_express_to_standard`,
  `rename_express`, `delete_*`, `list_prefix_*`); each creates parent dirs as needed
- [x] Define `ProjectNamespace` with all key formatters:
  `chunk_latest_key`, `chunk_staging_key`, `chunk_versioned_key`,
  `delta_manifest_key`, `base_manifest_key`, `pg_state_key`,
  `wal_key`, `project_meta_key`, `delta_prefix`, `base_prefix`
- [x] Implement `three_step_write` (staged PUT → standard COPY → express atomic rename)
- [x] Implement `put_express_latest` (plain PUT to express `latest`)
- [x] `#[cfg(test)]` unit tests:
  - PUT + GET round-trip on both express and standard
  - `put_express_latest`: `latest` exists; no staging, no versioned object
  - `three_step_write` full success: versioned in standard AND `latest` in express
  - Crash after step 1: old `latest` unchanged, no versioned
  - Crash after step 2: old `latest` unchanged, versioned is valid
  - Key format assertions for all key types; `chunk_versioned_key` uses `branch_id` not `project_id`
  - `list_prefix_standard`: returns correct subset; missing prefix returns empty

---

## Module 3 — Project Context

**Status:** `[x]`
**Depends on:** Modules 1, 2
**New file:** `s3worker/src/project.rs`
**Modified file:** `s3worker/src/lib.rs` (add `pub mod project;`)
**Modified file:** `s3worker/src/main_loop.rs` (init `GLOBAL_PROJECT_CTX` at startup)

Defines the runtime identity of the running project and provides the `Manifest`
used by the level-2 read fallback. Must be implemented before the read path
(Module 4) because `cached_read_blocks()` calls `global_project_ctx()`.

Branch creation logic (which builds and writes the initial manifest to S3) is
deferred to Module 7 — it has no additional dependencies beyond this module.

### Why `Manifest` is file-backed

The base manifest covers every chunk the project can read — own chunks plus
all inherited ancestor-branch chunks. For a large database this can be millions
of entries (≈ 100 bytes each in a HashMap → GBs of RSS for TB-scale databases).

`Manifest` keeps its data as a **sorted flat binary file** on local disk
(`base_manifest.bin`) and serves lookups via **binary search + direct `pread`**.
Each lookup makes at most `⌈log₂(N)⌉` `pread` calls; no in-memory page cache
is kept (the block cache in `cache.rs` already covers the hot path).
The level-2 fallback is only needed for chunks not found in the express-bucket
`latest` (level-1) — inherited ancestor-branch chunks that the current branch
has never overwritten — a read-heavy, write-cold pattern that suits a
file-backed index well.

### Todos

- [x] Add `serde_json = "1"` to `[workspace.dependencies]` in `Cargo.toml` and
  to `[dependencies]` in `s3worker/Cargo.toml` (needed to deserialise
  `project.json` into `ProjectMeta`).

- [x] Add `#[derive(Serialize, Deserialize, Clone, PartialEq)]` to
  `ProjectNamespace` in `sim_store.rs`:
  - `Serialize`/`Deserialize` required for `#[serde(flatten)]` in `ProjectMeta`.
  - `PartialEq` required for the identity check in `ProjectCtx::load`
    (`loaded_meta.ns != bootstrap_ns → error`).

- [x] Add `#[derive(PartialOrd, Ord)]` to `ChunkTag` in `cache.rs` if not done
  in Module 1 (required for sorting entries when building the manifest file;
  field order in the struct declaration determines sort order:
  `spc_oid → db_oid → rel_number → fork_number → chunk_id`).

- [x] Define `ProjectMeta` (mirrors `metadata/{project_id}/project.json`).
  The three identity fields (`org_id`, `project_id`, `branch_id`) live **only**
  inside the embedded `ProjectNamespace`; `#[serde(flatten)]` keeps the JSON
  representation flat so the on-disk format is unchanged:
  ```rust
  #[derive(Serialize, Deserialize, Clone)]
  pub struct ProjectMeta {
      #[serde(flatten)]
      pub ns:                    ProjectNamespace,  // org_id, project_id, branch_id
      pub parent_project_id:     Option<u64>,
      pub parent_branch_id:      Option<u64>,
      pub branch_checkpoint_lsn: Option<Lsn>,
      pub branch_timeline_id:    Option<u32>,
      pub created_at:            i64,
      pub status:                String,
  }
  ```

- [x] Define `ProjectCtx` — `base_manifest` is a `Manifest` (file-backed,
  with interior mutability) so it can be refreshed in-place through
  `&'static ProjectCtx`:
  ```rust
  pub struct ProjectCtx {
      pub meta:          ProjectMeta,
      /// File-backed sorted manifest for the level-2 chunk read fallback.
      /// Binary search via direct pread — no in-memory page cache.
      pub base_manifest: Manifest,
  }
  impl ProjectCtx {
      /// Load project.json from S3, download the latest base manifest,
      /// build the local TIKM file, populate PROJECT_CTX.
      /// Takes a `&ProjectNamespace` as the bootstrap key (constructed from env
      /// vars before project.json is fetched); the loaded `ProjectMeta` must
      /// agree on all three identity fields or load returns an error.
      pub fn load(sim: &SimStore, ns: &ProjectNamespace, data_dir: &Path) -> Result<Self>
      pub fn get() -> &'static Self   // panics if not initialised
      pub fn init(ctx: ProjectCtx)    // populates PROJECT_CTX; ignored if already set
      /// Convenience accessor.
      pub fn ns(&self) -> &ProjectNamespace { &self.meta.ns }
      pub fn is_branch(&self) -> bool   // true if parent_project_id is Some
      /// Level-2 chunk lookup: binary search into the on-disk Manifest.
      pub fn base_manifest_lookup(&self, key: &ChunkTag) -> io::Result<Option<ChunkRef>>
  }
  ```

- [x] In `ProjectCtx::load`:
  1. GET `metadata/{project_id}/project.json` from sim → deserialise to `ProjectMeta`.
  2. Validate `loaded_meta.ns == bootstrap_ns` or return error.
  3. List `{ns.base_prefix()}` → parse LSN dirs → find the latest base manifest key.
     - Root project with no bases yet: construct `Manifest::new_sorted(Lsn::ZERO, 0, vec![], path)`
       (zero-entry manifest; level-2 lookups return `Ok(None)` — correct for a fresh DB).
     - Branch project with no bases: return error (branch always has an initial base).
  4. GET `bases/{lsn_hex}/manifest.bin` → `Manifest::from_bytes(&bytes, &local_path)`
     where `local_path = {DataDir}/tiko/base_manifest.bin`.
  5. Return `ProjectCtx { meta, base_manifest }`.

- [x] Add `static PROJECT_CTX: OnceLock<ProjectCtx>` and
  `ProjectCtx::get() -> &'static ProjectCtx` / `ProjectCtx::init(ctx)`

- [x] In `main_loop.rs` `s3worker_main()`: before entering the event loop call
  `try_init_project_ctx()` which reads `TIKO_ORG_ID/PROJECT_ID/BRANCH_ID` from
  env, constructs `SimStore` + `ProjectNamespace`, and calls `ProjectCtx::load()`
  to populate `PROJECT_CTX` (best-effort; non-fatal on failure).

- [x] `#[cfg(test)]` (tempdir):
  - Load root `ProjectCtx` (no parent): `is_branch()` = false;
    `base_manifest_lookup` returns inherited chunks correctly
  - Load branch `ProjectCtx` with synthetic base manifest bytes in sim store:
    `base_manifest_lookup` returns correct `ChunkRef` for a known key, `Ok(None)` for unknown
  - `ProjectCtx::load` with empty `bases/` for a root project: succeeds with zero-entry manifest;
    `base_manifest_lookup` returns `Ok(None)` for any key
  - `ProjectCtx::load` with empty `bases/` for a branch project: returns error

---

## Module 4 — Versioned Chunk Read Path

**Status:** `[x]`
**Depends on:** Modules 1, 2, 3
**Modified file:** `s3worker/src/s3_ops.rs`
**New file:** `s3worker/src/recovery.rs`
**Modified file:** `s3worker/src/lib.rs` (add `pub mod recovery;`)

Extends the existing local-file read path with the two-level branch fallback and
recovery mode. `write_blocks()` is **not** modified here — versioned S3 writes
happen in Module 5 (eviction) and Module 6 (checkpoint).

### Todos

**recovery.rs:**
- [x] Define globals:
  ```rust
  static RECOVERY_MODE: AtomicBool = AtomicBool::new(false);
  /// Recovery manifest: a Manifest loaded from $PGDATA/tiko/recovery_manifest.bin.
  /// Populated by load_recovery_manifest; queried via lookup_recovery_chunk.
  static RECOVERY_MANIFEST: OnceLock<Manifest> = OnceLock::new();
  ```
- [x] `pub fn load_recovery_manifest(path: &Path) -> Result<()>`:
  Read file at `path` (= `$PGDATA/tiko/recovery_manifest.bin`);
  `Manifest::from_bytes(&bytes, &local_path)` where `local_path` is a fixed path
  like `{DataDir}/tiko/recovery_manifest_local.bin`; set `RECOVERY_MANIFEST`;
  set `RECOVERY_MODE = true`.
- [x] `pub fn clear_recovery_mode()` — set `RECOVERY_MODE = false` (after PG promotion)
- [x] `pub fn is_recovery_mode() -> bool`
- [x] `pub fn lookup_recovery_chunk(key: &ChunkTag) -> io::Result<Option<ChunkRef>>`:
  `RECOVERY_MANIFEST.get()?.lookup(key)` — returns `Ok(None)` if manifest not loaded.

**s3_ops.rs additions:**
- [x] Add `static SIM_STORE: OnceLock<SimStore>` — initialised at s3worker startup
- [x] Add `static PROJECT_NS: OnceLock<ProjectNamespace>` — from env vars
  (`TIKO_ORG_ID`, `TIKO_PROJECT_ID`, `TIKO_BRANCH_ID`)
- [x] Add `pub fn init_sim_store(data_dir: &Path, org_id: u64, project_id: u64, branch_id: u64)`
  — populates both statics; called once from `s3worker_main()` before `ProjectCtx::load()`
- [x] Add `pub fn get_sim() -> &'static SimStore`
- [x] Add `pub fn get_ns() -> &'static ProjectNamespace`

- [x] Modify `cached_read_blocks()`: on cache miss, after existing pread attempt:
  1. **Recovery mode** (`is_recovery_mode()` = true):
     - `lookup_recovery_chunk(key)?` → GET from standard sim at
       `{org}/chunks/{chunk_ref.branch_id}/{key}/{lsn_hex}`
     - If found, fill cache slot, continue
  2. **Normal, level 1** (own express-bucket `latest`):
     - GET `express/{org}/{proj}/chunks/{key}/latest`
     - If found, fill cache slot, continue
  3. **Normal, level 2** (base_manifest fallback for inherited chunks):
     - `global_project_ctx().base_manifest_lookup(key)?` → `Option<ChunkRef>`
       (binary search into on-disk Manifest; returns `io::Result`)
     - On `Some(ChunkRef{branch_id, lsn})`: GET from standard sim at
       `{org}/chunks/{branch_id}/{key}/{lsn_hex}`
     - If found, fill cache slot, continue
  4. All misses: zero-fill (block beyond file extent — existing behaviour)

- [x] `#[cfg(test)]` (tempdir):
  - Level-1 express hit: put data in express `latest`; delete backing file; read → correct data
  - Level-2 branch fallback: put data in standard sim under a different `branch_id`; configure
    base manifest with that `branch_id`; simulate level-1 miss; verify level-2 returns correct data
  - Level-2 GET key uses `branch_id` (not `project_id`) in the path
  - Recovery mode: write synthetic `tiko/recovery_manifest.bin` (msgpack+zstd); put versioned
    data in standard sim; delete backing file; read → correct versioned data returned

---

## Module 5 — Cache Eviction Log

**Status:** `[x]`
**Depends on:** Modules 1, 2
**Modified file:** `s3worker/src/cache.rs`

Can be developed in parallel with Module 4.

### Todos

- [x] Reuse `ChunkTag` (already `#[repr(C)]`, 5 × 4 bytes = 20 bytes) as the eviction log
  record type — no separate `EvictionLogRecord` struct needed:
  ```rust
  const _: () = assert!(std::mem::size_of::<ChunkTag>() == 20);
  // write(2) with O_APPEND is atomic on local Linux filesystems
  // (kernel serialises concurrent appenders via the inode lock).
  // pwrite(2) must NOT be used: POSIX specifies that pwrite ignores O_APPEND.
  ```

- [x] Add `fn eviction_log_path() -> PathBuf` → `$PGDATA/tiko/eviction_log`
  (implemented as `CacheControl::eviction_log_path()`)

- [x] Add `fn open_eviction_log() -> File`:
  `OpenOptions::new().write(true).create(true).append(true).open(eviction_log_path())`
  (implemented as `CacheControl::open_eviction_log()`)

- [x] Extend `flush_dirty_chunk(slot_index: u32)`:
  After existing write to `{DataDir}/tiko/` backing file:
  1. Read chunk data from the cache slot
  2. Call `SimStore::get().put_express_latest(ProjectCtx::try_get().ns(), &chunk_key, &chunk_data)`
     — plain PUT to express-bucket `latest`; no staging, no standard-bucket copy
  3. On PUT success only: `write(fd, &record)` one `ChunkTag` with `O_APPEND`
     (no phantom log entry if PUT failed)

- [x] Add `pub fn read_eviction_log(path: &Path) -> Vec<ChunkTag>`:
  Read in 20-byte records; skip any incomplete trailing record (crash safety).
  (implemented as `CacheControl::read_eviction_log(path)`)

- [x] `#[cfg(test)]` (tempdir):
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
- [ ] Implement `pub fn upload_delta_manifest(sim: &SimStore, ns: &ProjectNamespace, checkpoint_lsn: Lsn, manifest: &Manifest) -> Result<()>`:
  `manifest.to_bytes()?` → `sim.put_standard(&ns.delta_manifest_key(checkpoint_lsn), &bytes)`

- [ ] Implement `pub fn upload_pg_state(sim: &SimStore, ns: &ProjectNamespace, checkpoint_lsn: Lsn, pgdata: &Path) -> Result<()>`:
  Build a tar+zstd archive in memory containing the following PG state files:
  - `pg_control` (8 KB)
  - `pg_xact/*` (all segment files)
  - `pg_multixact/members/*` and `pg_multixact/offsets/*`
  - `pg_filenode.map`
  Use `tar::Builder` writing into a `zstd::Encoder` wrapping a `Vec<u8>`, then
  `sim.put_standard(&ns.non_smgr_key(checkpoint_lsn), &bytes)` where
  `non_smgr_key` returns `"{org}/pitr/{proj}/deltas/{lsn_hex}/pg_state.tar.zst"`.

- [ ] Implement `pub fn upload_wal_segment(sim: &SimStore, ns: &ProjectNamespace, timeline: u32, segment: &str, path: &Path) -> Result<()>`:
  `std::fs::read(path)` → `sim.put_standard(&ns.wal_key(timeline, segment), &bytes)`
  Called by `tiko_archive` binary (Module 9).

- [ ] Implement `pub fn download_wal_segment(sim: &SimStore, ns: &ProjectNamespace, timeline: u32, segment: &str, dest: &Path) -> Result<bool>`:
  `sim.get_standard(&ns.wal_key(timeline, segment))` → write bytes to `dest`; returns
  `Ok(false)` if not found (caller tries parent namespace).

**checkpoint.rs:**
- [ ] Replace current stub with `s3_checkpoint_flush(checkpoint_lsn: Lsn)`:
  ```
  1. evict_all_dirty_chunks()
     // Flushes every remaining dirty cache slot: PUT to express-bucket `latest`
     // + append EvictionLogRecord. After this, the eviction log contains ALL
     // dirty chunks from this checkpoint interval (both mid-interval evictions
     // and those just flushed here).

  2. std::fs::rename("tiko/eviction_log", "tiko/eviction_log.ckpt")
     // Atomic snapshot. New evictions after this point write to a fresh log.

  3. records = read_eviction_log("tiko/eviction_log.ckpt")
     dirty_chunks = dedup_by_chunk_tag(records)
     // Dedup: a chunk evicted multiple times in the interval is uploaded once.

  4. for chunk_key in &dirty_chunks {
         sim.three_step_write(ns, &chunk_key, checkpoint_lsn, &read_chunk_data(&chunk_key))
     }
     // Full 3-step PUT→COPY→Rename, all keyed by the same checkpoint_lsn.

  5. delta = Manifest::new_sorted(
         checkpoint_lsn,
         now_unix(),
         dirty_chunks.iter().map(|key| {
             (*key, ChunkRef { branch_id: get_ns().branch_id, lsn: checkpoint_lsn })
         }).collect(),
         &tmp_delta_path,
     )?;
     upload_delta_manifest(sim, ns, checkpoint_lsn, &delta)?;
     upload_pg_state(sim, ns, checkpoint_lsn, pgdata)?;

  6. std::fs::remove_file("tiko/eviction_log.ckpt")
  ```
  The delta `Manifest` is written to a temporary local path (e.g.
  `{DataDir}/tiko/delta_{lsn_hex}.bin`) for the duration of the checkpoint
  flush, then discarded after `upload_delta_manifest` converts it to S3 bytes.

- [ ] Add `fn dedup_by_chunk_tag(records: Vec<EvictionLogRecord>) -> Vec<ChunkTag>`:
  Build a `HashSet<ChunkTag>` from all records; return as `Vec`. Order doesn't matter.

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
  - For each: run full `s3_checkpoint_flush` → deserialise delta manifest from sim store
    (`Manifest::from_bytes`) → assert correct chunks present with correct `ChunkRef` values
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

- [ ] Implement `pub fn build_initial_manifest(sim: &SimStore, parent_ns: &ProjectNamespace, branch_lsn: Lsn, out_path: &Path) -> Result<Manifest>`:
  1. List `{parent_ns.base_prefix()}` → parse LSN dirs → find latest with `base_lsn ≤ branch_lsn`
  2. GET + `Manifest::from_bytes(&bytes, &base_local_path)` for the base manifest
  3. List `{parent_ns.delta_prefix()}` → filter `(base_lsn, branch_lsn]` → GET + `from_bytes`
     each delta into a temp local path
  4. `base.apply_deltas(&deltas)?` — result is flat (parent's `branch_id` values preserved)
  5. Return `base` — the merged `Manifest` written to `out_path`

- [ ] Implement `pub fn create_branch(sim: &SimStore, parent_ns: &ProjectNamespace, child_ns: &ProjectNamespace, branch_lsn: Lsn) -> Result<()>`:
  ```
  1. initial_manifest = build_initial_manifest(sim, parent_ns, branch_lsn,
                                               &child_initial_manifest_local_path)
  2. meta = ProjectMeta {
         ns: child_ns.clone(),
         parent_project_id: Some(parent_ns.project_id),
         parent_branch_id: Some(parent_ns.branch_id),
         branch_checkpoint_lsn: Some(branch_lsn),
         ...
     }
  3. sim.put_standard(&child_ns.project_meta_key(), &serde_json::to_vec(&meta)?)
  4. sim.put_standard(&child_ns.base_manifest_key(branch_lsn),
                      &initial_manifest.to_bytes()?)
  ```
  Branch is valid once step 4 completes — a single S3 PUT.
  No `branch_base.json`, no `branch_refs` markers.

- [ ] Implement `pub fn delete_branch(sim: &SimStore, branch_ns: &ProjectNamespace) -> Result<()>`:
  1. List all keys under `express/{org}/{proj}/` → delete each (express-bucket hot data)
  2. List all keys under `standard/{org}/pitr/{proj}/` → delete each (manifests + WAL)
  3. List all keys under `standard/{org}/metadata/{proj}/` → delete each (project.json)
  4. Log: "standard-bucket {org}/chunks/{branch_id}/ will be collected by next GC run"

- [ ] `#[cfg(test)]` (tempdir):
  - `build_initial_manifest` from 3 synthetic deltas → correct merged chunk map with correct `branch_id` values per chunk; result is a valid `Manifest` with correct `entry_count`
  - Cascaded branch (C from B from A): chunks written only on A have `branch_id = A`'s id in C's map
  - `create_branch` → exactly 2 files written to standard sim (`project.json` + `manifest.bin`);
    `manifest.bin` deserialises via `Manifest::from_bytes` to the expected entries
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
     → `Manifest::from_bytes(&bytes, &base_local_path)?`
  2. `sim.list_prefix_standard(&ns.delta_prefix())` → filter deltas newer than `base_lsn` →
     for each: GET + `Manifest::from_bytes(&bytes, &delta_local_path)?`
  3. If no new deltas: return `Ok(())`
  4. `base.apply_deltas(&deltas)?` — updates the base's local file in-place
  5. `sim.put_standard(&ns.base_manifest_key(new_lsn), &base.to_bytes()?)` — single atomic write
  6. `global_project_ctx().base_manifest.apply_deltas(&deltas)?` — refresh the global
     manifest in-place (rewrites the local TIKM file and updates `entry_count`);
     uses interior mutability so no `&mut` needed through `&'static ProjectCtx`.
     Must run after the S3 PUT so the on-disk index always reflects a committed base.
  7. Do NOT delete deltas — cleanup is `enforce_retention_org`'s responsibility

  Note: the `base` produced in step 1–4 and the global manifest updated in step 6
  use different local file paths to avoid a partial-write race. The global manifest
  is always updated via `apply_deltas` (not replaced wholesale), so any concurrent
  `lookup` calls see a consistent view under the manifest's internal `Mutex`.

- [ ] `#[cfg(test)]` (tempdir, synchronous):
  - Seed sim with 10 delta manifests + a base at delta 3 (all as msgpack+zstd blobs)
  - `materialize_base`: verify new base matches expected merged map (base + deltas 4–10);
    new base `manifest.bin` is in sim standard store at correct key
  - After materialization, global `base_manifest.lookup` returns correct `ChunkRef` for a
    chunk updated in delta 7 (would have returned old value if not refreshed)
  - Idempotent: run again → no new base written (no deltas newer than new base), global manifest unchanged
  - All 10 delta files still present after materialization

**thread_pool.rs:**
- [ ] After `init_tokio_runtime()`, spawn:
  ```rust
  let sim = Arc::new(SimStore::from_data_dir());
  let ns  = global_project_ctx().ns().clone();
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
export TIKO_BRANCH_CHECKPOINT_LSN=000000003A000028   # 16-char hex Lsn::to_hex()

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
| 1 | `s3worker/src/manifest.rs` | `s3worker/src/lib.rs`, `s3worker/src/cache.rs` |
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
Serialisation format (msgpack + zstd for S3 wire; TIKM binary for local) is
unchanged — all other modules are unaffected.
