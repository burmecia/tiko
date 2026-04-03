//! Manifest types and file-backed merge logic for PITR.
//!
//! `Manifest` is the unified type for both base and delta manifests. It is:
//! - **Stored on S3** as `manifest.bin` — a `msgpack(...)` blob (S3Sim applies zstd).
//! - **Cached locally** as a fixed-size sorted binary file (TIKM format) that
//!   enables O(log N) binary search via direct `pread` calls (no in-memory
//!   page cache — the block cache in `cache.rs` covers the hot path).
//!
//! Both base and delta manifests use this same type, same local file format,
//! and same S3 wire format. The S3 path (`bases/` vs `deltas/`) distinguishes
//! kind; no separate Rust type is needed.
//!
//! # TIKM file format
//!
//! ```text
//! Header (32 bytes):
//!   magic:          [u8; 4] = b"TIKM"
//!   version:        u32 = 1            (little-endian)
//!   checkpoint_lsn: u64                (little-endian)
//!   timestamp:      i64 (unix secs)    (little-endian)
//!   entry_count:    u64                (little-endian)
//!
//! Body (entry_count × 40 bytes, sorted ascending by ChunkTag):
//!   ChunkTag  20 bytes  (spc_oid u32, db_oid u32, rel_number u32,
//!                         fork_number i32, chunk_id u32 — all LE)
//!   ChunkRef  20 bytes  (branch_id u64, timeline_id u32, lsn u64 — all LE)
//! ```

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use pgsys::Lsn;
use serde::{Deserialize, Serialize};

use crate::chunk::{CHUNK_TAG_SIZE, ChunkTag, RelFork};
use crate::project::{ProjectCtx, ProjectNamespace};
use crate::s3_sim::S3Sim;

// ── TIKM constants ──

const TIKM_MAGIC: [u8; 4] = *b"TIKM";
const TIKM_VERSION: u32 = 1;
/// Header size in bytes.
const HEADER_SIZE: usize = 32;
/// Entry size in bytes (ChunkTag[20] + ChunkRef[20]).
const ENTRY_SIZE: usize = CHUNK_TAG_SIZE + CHUNK_REF_SIZE;

// ── ChunkRef ──

/// Reference to a specific version of a chunk stored in S3.
///
/// Note: no `#[repr(C)]` and no `size_of` assert here — `ChunkRef` is never
/// cast to raw bytes. Its in-memory size is 24 bytes (4-byte alignment padding
/// between `timeline_id: u32` and `lsn: u64`), while the wire encoding is 20
/// bytes. The wire size is enforced by `encode() -> [u8; 20]`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct ChunkRef {
    /// Branch-scoped id: selects `{org}/chunks/{branch_id}/` in the standard bucket.
    pub branch_id: u64,
    /// Timeline on which this chunk version was written.
    /// Together with `branch_id` and `lsn`, uniquely identifies the S3 object:
    /// `{org}/chunks/{branch_id}/{tag}/{timeline_id:08X}/{lsn_hex}`.
    pub timeline_id: u32,
    /// Checkpoint LSN at which this chunk version was sealed.
    pub lsn: Lsn,
}

impl ChunkRef {
    fn encode(&self) -> [u8; 20] {
        let mut buf = [0u8; 20];
        buf[0..8].copy_from_slice(&self.branch_id.to_le_bytes());
        buf[8..12].copy_from_slice(&self.timeline_id.to_le_bytes());
        buf[12..20].copy_from_slice(&self.lsn.as_u64().to_le_bytes());
        buf
    }

    fn decode(buf: &[u8; 20]) -> Self {
        ChunkRef {
            branch_id: u64::from_le_bytes(buf[0..8].try_into().unwrap()),
            timeline_id: u32::from_le_bytes(buf[8..12].try_into().unwrap()),
            lsn: Lsn::new(u64::from_le_bytes(buf[12..20].try_into().unwrap())),
        }
    }
}

/// Wire size of a serialised `ChunkRef` (u64 + u32 + u64 LE, no padding).
const CHUNK_REF_SIZE: usize = 20;
// In-memory size is 24 (4-byte padding after timeline_id:u32 before lsn:u64); wire
// encoding is 20 (explicit encode/decode, no padding). Catches accidental layout changes.
const _: () = assert!(std::mem::size_of::<ChunkRef>() == 24);

// ── ManifestInner ──

struct ManifestInner {
    checkpoint_lsn: Lsn,
    timestamp: i64,
    /// Path to the local TIKM binary file.
    path: PathBuf,
    /// Read handle; replaced on `apply_deltas` (new file, same path after rename).
    file: File,
    /// Total number of 36-byte entries in the current file.
    entry_count: u64,
    /// Block count per relation fork: `RelFork → nblocks`.
    /// Carried in the msgpack wire format only; not stored in the TIKM binary.
    fork_nblocks: HashMap<RelFork, u32>,
    /// Relation forks dropped during this checkpoint interval.
    /// Carried in the msgpack wire format only; always empty in a base manifest.
    deleted_forks: Vec<RelFork>,
}

// ── Manifest ──

/// File-backed sorted manifest for chunk lookup and PITR merge operations.
///
/// Invariant: the local TIKM file at `path` is always valid with entries sorted
/// ascending by `ChunkTag`. Only `new`, `from_bytes`, and `apply_deltas`
/// may create or overwrite this file.
pub struct Manifest {
    /// All mutable state; replaced atomically on `apply_deltas`.
    inner: Mutex<ManifestInner>,
}

// ── Low-level file I/O ──

/// `pread` exactly `buf.len()` bytes from `file` at `offset`.
fn pread_exact(file: &File, buf: &mut [u8], offset: u64) -> io::Result<()> {
    let mut done = 0;
    while done < buf.len() {
        let n = file.read_at(&mut buf[done..], offset + done as u64)?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "TIKM file truncated",
            ));
        }
        done += n;
    }
    Ok(())
}

/// Write a TIKM file from a **pre-sorted** `chunks` slice. Returns an open
/// read handle to the written file.
///
/// Creates parent directories as needed.
fn write_tikm(
    path: &Path,
    checkpoint_lsn: Lsn,
    timestamp: i64,
    chunks: &[(ChunkTag, ChunkRef)],
) -> io::Result<File> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let mut f = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)?;

    // Header (32 bytes): magic[4] + version[4] + checkpoint_lsn[8] +
    //                    timestamp[8] + entry_count[8]
    let mut header = [0u8; 32];
    header[0..4].copy_from_slice(&TIKM_MAGIC);
    header[4..8].copy_from_slice(&TIKM_VERSION.to_le_bytes());
    header[8..16].copy_from_slice(&checkpoint_lsn.as_u64().to_le_bytes());
    header[16..24].copy_from_slice(&timestamp.to_le_bytes());
    header[24..32].copy_from_slice(&(chunks.len() as u64).to_le_bytes());
    f.write_all(&header)?;

    // Entries (sorted ascending by ChunkTag)
    for (tag, cref) in chunks {
        f.write_all(&tag.encode())?;
        f.write_all(&cref.encode())?;
    }
    f.flush()?;
    drop(f);

    // Reopen read-only for the handle stored in ManifestInner
    File::open(path)
}

// ── Private helpers ──

/// Sequential `pread` of all entries starting at HEADER_SIZE.
fn read_all_entries(inner: &ManifestInner) -> io::Result<Vec<(ChunkTag, ChunkRef)>> {
    let n = inner.entry_count as usize;
    if n == 0 {
        return Ok(Vec::new());
    }
    let byte_len = n * ENTRY_SIZE;
    let mut buf = vec![0u8; byte_len];
    pread_exact(&inner.file, &mut buf, HEADER_SIZE as _)?;

    let mut entries = Vec::with_capacity(n);
    for i in 0..n {
        let off = i * ENTRY_SIZE;
        let tag = ChunkTag::decode(buf[off..off + CHUNK_TAG_SIZE].try_into().unwrap());
        let cref = ChunkRef::decode(
            buf[off + CHUNK_TAG_SIZE..off + ENTRY_SIZE]
                .try_into()
                .unwrap(),
        );
        entries.push((tag, cref));
    }
    Ok(entries)
}

// ── Manifest impl ──

impl Manifest {
    /// Construct a `Manifest` from an arbitrary list of chunks, writing the
    /// TIKM file at `path`. `chunks` need not be pre-sorted.
    ///
    /// `fork_nblocks` is carried in the msgpack wire format (`to_bytes`) but not
    /// in the local TIKM binary. Pass `HashMap::new()` when nblocks are unknown.
    /// Create a zero-entry manifest at `Lsn::INVALID` (used as a bootstrap
    /// starting point before the first real base exists).
    pub fn empty(path: &Path) -> io::Result<Self> {
        Self::new(Lsn::INVALID, 0, vec![], HashMap::new(), vec![], path)
    }

    pub fn new(
        checkpoint_lsn: Lsn,
        timestamp: i64,
        mut chunks: Vec<(ChunkTag, ChunkRef)>,
        fork_nblocks: HashMap<RelFork, u32>,
        deleted_forks: Vec<RelFork>,
        path: &Path,
    ) -> io::Result<Self> {
        chunks.sort_unstable_by_key(|(tag, _)| *tag);
        let file = write_tikm(path, checkpoint_lsn, timestamp, &chunks)?;
        Ok(Manifest {
            inner: Mutex::new(ManifestInner {
                checkpoint_lsn,
                timestamp,
                path: path.to_path_buf(),
                file,
                entry_count: chunks.len() as u64,
                fork_nblocks,
                deleted_forks,
            }),
        })
    }

    /// Open an existing local TIKM file; validate `magic` + `version`; read
    /// header metadata. Used at startup to avoid re-downloading from S3.
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = File::open(path)?;
        let mut header = [0u8; 32];
        pread_exact(&file, &mut header, 0)?;

        if header[0..4] != TIKM_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid TIKM magic",
            ));
        }
        let version = u32::from_le_bytes(header[4..8].try_into().unwrap());
        if version != TIKM_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported TIKM version: {version}"),
            ));
        }
        let checkpoint_lsn = Lsn::new(u64::from_le_bytes(header[8..16].try_into().unwrap()));
        let timestamp = i64::from_le_bytes(header[16..24].try_into().unwrap());
        let entry_count = u64::from_le_bytes(header[24..32].try_into().unwrap());

        Ok(Manifest {
            inner: Mutex::new(ManifestInner {
                checkpoint_lsn,
                timestamp,
                path: path.to_path_buf(),
                file,
                entry_count,
                fork_nblocks: HashMap::new(),
                deleted_forks: vec![],
            }),
        })
    }

    /// Deserialize from the S3 wire format (`msgpack(...)`).
    /// Writes the decoded entries to a local TIKM file at `path`.
    ///
    /// Wire format: 5-tuple `(lsn, timestamp, chunks, fork_nblocks, deleted_forks)`.
    pub fn from_bytes(data: &[u8], path: &Path) -> io::Result<Self> {
        let (checkpoint_lsn, timestamp, chunks, fork_nblocks, deleted_forks): (
            Lsn,
            i64,
            Vec<(ChunkTag, ChunkRef)>,
            HashMap<RelFork, u32>,
            Vec<RelFork>,
        ) = rmp_serde::from_slice(data)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        Self::new(
            checkpoint_lsn,
            timestamp,
            chunks,
            fork_nblocks,
            deleted_forks,
            path,
        )
    }

    /// Serialize to the S3 wire format (`msgpack(...)`).
    ///
    /// Format: 5-tuple `(checkpoint_lsn, timestamp, chunks, fork_nblocks, deleted_forks)`.
    pub fn to_bytes(&self) -> io::Result<Vec<u8>> {
        let inner = self.inner.lock().unwrap();
        let entries = read_all_entries(&inner)?;
        rmp_serde::to_vec(&(
            inner.checkpoint_lsn,
            inner.timestamp,
            &entries,
            &inner.fork_nblocks,
            &inner.deleted_forks,
        ))
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    /// Return the checkpoint LSN recorded in the manifest header.
    pub fn checkpoint_lsn(&self) -> Lsn {
        self.inner.lock().unwrap().checkpoint_lsn
    }

    /// Return the timestamp recorded in the manifest header.
    pub fn timestamp(&self) -> i64 {
        self.inner.lock().unwrap().timestamp
    }

    /// Return all `(ChunkTag, ChunkRef)` entries from the on-disk TIKM file.
    pub fn entries(&self) -> io::Result<Vec<(ChunkTag, ChunkRef)>> {
        let inner = self.inner.lock().unwrap();
        read_all_entries(&inner)
    }

    /// Return the `fork_nblocks` map (populated from the S3 wire format only;
    /// empty when opened from a local TIKM file via [`Manifest::open`]).
    pub fn fork_nblocks(&self) -> HashMap<RelFork, u32> {
        self.inner.lock().unwrap().fork_nblocks.clone()
    }

    /// Canonical local path for the base manifest TIKM file.
    pub fn local_manifest_path(root_dir: &Path) -> PathBuf {
        root_dir.join("base_manifest.bin")
    }

    /// Canonical local path for the recovery manifest TIKM file.
    pub fn recovery_manifest_path(root_dir: &Path) -> PathBuf {
        root_dir.join("recovery_manifest.bin")
    }

    /// Binary search for `key` in the sorted on-disk TIKM file.
    /// Returns `Ok(Some(ChunkRef))` on hit, `Ok(None)` on miss.
    pub fn lookup(&self, key: &ChunkTag) -> io::Result<Option<ChunkRef>> {
        let inner = self.inner.lock().unwrap();
        let entry_count = inner.entry_count;
        if entry_count == 0 {
            return Ok(None);
        }

        let mut lo: u64 = 0;
        let mut hi: u64 = entry_count;
        let mut buf = [0u8; 40];

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let offset = HEADER_SIZE as u64 + mid * ENTRY_SIZE as u64;
            pread_exact(&inner.file, &mut buf, offset)?;

            let tag = ChunkTag::decode(buf[0..CHUNK_TAG_SIZE].try_into().unwrap());
            match tag.cmp(key) {
                Ordering::Equal => {
                    let cref =
                        ChunkRef::decode(buf[CHUNK_TAG_SIZE..ENTRY_SIZE].try_into().unwrap());
                    return Ok(Some(cref));
                }
                Ordering::Less => lo = mid + 1,
                Ordering::Greater => hi = mid,
            }
        }
        Ok(None)
    }

    /// Apply a sequence of delta manifests onto `self`, updating the on-disk
    /// TIKM file in place via an atomic rename.
    ///
    /// - Delta entries with a higher LSN than the base entry for the same
    ///   `ChunkTag` win; on equal LSN the base (self) entry is kept.
    /// - `checkpoint_lsn` and `timestamp` are updated to the last non-empty
    ///   delta's values.
    /// - An empty `deltas` slice is a no-op (no file I/O).
    ///
    /// # Panics (debug only)
    /// Panics if `deltas` are not in ascending `checkpoint_lsn` order.
    pub fn apply_deltas(&self, deltas: &[Manifest]) -> io::Result<()> {
        if deltas.is_empty() {
            return Ok(());
        }

        debug_assert!(
            deltas
                .windows(2)
                .all(|w| w[0].checkpoint_lsn() <= w[1].checkpoint_lsn()),
            "deltas must be in ascending LSN order"
        );

        let mut inner = self.inner.lock().unwrap();

        // Collect all delta entries; track the last non-empty delta's metadata.
        let mut combined_delta: Vec<(ChunkTag, ChunkRef)> = Vec::new();
        let mut last_lsn = inner.checkpoint_lsn;
        let mut last_ts = inner.timestamp;
        // Start with self's fork_nblocks; delta wins per fork key.
        let mut merged_nblocks: HashMap<RelFork, u32> = inner.fork_nblocks.clone();
        let mut deleted_set: HashSet<RelFork> = HashSet::new();

        for delta in deltas {
            let delta_inner = delta.inner.lock().unwrap();
            let entries = read_all_entries(&delta_inner)?;
            if !entries.is_empty() {
                last_lsn = delta_inner.checkpoint_lsn;
                last_ts = delta_inner.timestamp;
            }
            combined_delta.extend(entries);
            // Delta's nblocks win over base's nblocks.
            for (&k, &v) in &delta_inner.fork_nblocks {
                merged_nblocks.insert(k, v);
            }
            for &rf in &delta_inner.deleted_forks {
                deleted_set.insert(rf);
            }
            // delta_inner lock released here
        }

        // Sort by (tag asc, lsn desc); dedup keeping the highest-LSN entry
        // per tag (dedup_by_key keeps the first element per key run, and the
        // first after descending-lsn sort has the highest LSN).
        combined_delta.sort_unstable_by(|a, b| a.0.cmp(&b.0).then_with(|| b.1.lsn.cmp(&a.1.lsn)));
        combined_delta.dedup_by_key(|(tag, _)| *tag);

        // Two-pointer merge: sequential scan of base + sorted combined_delta.
        let base_entries = read_all_entries(&inner)?;
        let mut output: Vec<(ChunkTag, ChunkRef)> =
            Vec::with_capacity(base_entries.len() + combined_delta.len());
        let mut bi = 0usize;
        let mut di = 0usize;

        while bi < base_entries.len() && di < combined_delta.len() {
            match base_entries[bi].0.cmp(&combined_delta[di].0) {
                Ordering::Less => {
                    output.push(base_entries[bi]);
                    bi += 1;
                }
                Ordering::Greater => {
                    output.push(combined_delta[di]);
                    di += 1;
                }
                Ordering::Equal => {
                    // Keep higher LSN; tie goes to self (base entry).
                    if combined_delta[di].1.lsn > base_entries[bi].1.lsn {
                        output.push(combined_delta[di]);
                    } else {
                        output.push(base_entries[bi]);
                    }
                    bi += 1;
                    di += 1;
                }
            }
        }
        while bi < base_entries.len() {
            output.push(base_entries[bi]);
            bi += 1;
        }
        while di < combined_delta.len() {
            output.push(combined_delta[di]);
            di += 1;
        }

        // Purge tombstoned forks from the merged output and nblocks map.
        if !deleted_set.is_empty() {
            output.retain(|(tag, _)| !deleted_set.contains(&tag.rel_fork()));
            merged_nblocks.retain(|rf, _| !deleted_set.contains(rf));
        }

        // Write to a tmp path then atomically rename over the live path.
        let tmp_path = PathBuf::from(format!("{}.tmp", inner.path.display()));
        write_tikm(&tmp_path, last_lsn, last_ts, &output)?;
        fs::rename(&tmp_path, &inner.path)?;

        // Reopen with a fresh read handle pointing at the renamed file.
        inner.file = File::open(&inner.path)?;
        inner.entry_count = output.len() as u64;
        inner.checkpoint_lsn = last_lsn;
        inner.timestamp = last_ts;
        inner.fork_nblocks = merged_nblocks;
        inner.deleted_forks = vec![];

        Ok(())
    }

    /// Look up the block count for a relation fork stored in this manifest.
    ///
    /// Returns `None` if no nblocks entry was recorded (e.g. legacy manifest
    /// or a relation that wasn't touched since the last checkpoint).
    pub fn lookup_nblocks(&self, rf: RelFork) -> Option<u32> {
        self.inner.lock().unwrap().fork_nblocks.get(&rf).copied()
    }

    /// Return the list of relation forks deleted during this checkpoint interval.
    /// Always empty in a base manifest (tombstones are consumed by `apply_deltas`).
    pub fn deleted_forks(&self) -> Vec<RelFork> {
        self.inner.lock().unwrap().deleted_forks.clone()
    }
}

// ── PITR base materialization ──

type MaterializeResultInner<T> = std::result::Result<T, Box<dyn std::error::Error>>;

/// Outcome of a single `materialize_base` call.
#[derive(Debug)]
pub enum MaterializeResult {
    /// No new deltas were found; the existing base is already up to date.
    NoNewDeltas { base_lsn: Lsn },
    /// A new base manifest was uploaded.
    Materialized {
        prev_base_lsn: Lsn,
        new_lsn: Lsn,
        delta_count: usize,
    },
}

/// Merge all delta manifests newer than the latest base into a new base and
/// upload it to the standard store.
///
/// Returns [`MaterializeResult::NoNewDeltas`] immediately if there are no new
/// deltas (idempotent). Does NOT delete delta manifests — cleanup is
/// enforce_retention_org's responsibility.
pub fn materialize_base(
    sim: &S3Sim,
    ns: &ProjectNamespace,
    timeline: u32,
) -> MaterializeResultInner<MaterializeResult> {
    let base_prefix = ns.base_prefix_for_timeline(timeline);
    let base_keys = sim.list_prefix_standard(&base_prefix)?;

    let mut base_lsns: Vec<Lsn> = base_keys
        .iter()
        .filter_map(|key| {
            let rest = key.strip_prefix(&base_prefix)?;
            let lsn_hex = rest.split('/').next()?;
            Lsn::from_hex(lsn_hex).ok()
        })
        .collect();
    base_lsns.sort();

    let base_local_path =
        std::env::temp_dir().join(format!("tiko_pitr_base_{}.tikm", ns.project_id));

    let (base, base_lsn) = if let Some(&lsn) = base_lsns.last() {
        let manifest_key = ns.base_manifest_key(timeline, lsn);
        let bytes = sim
            .get_standard(&manifest_key)?
            .ok_or_else(|| format!("base manifest not found: {manifest_key}"))?;
        (Manifest::from_bytes(&bytes, &base_local_path)?, lsn)
    } else {
        (Manifest::empty(&base_local_path)?, Lsn::INVALID)
    };

    let delta_prefix = ns.delta_prefix_for_timeline(timeline);
    let delta_keys = sim.list_prefix_standard(&delta_prefix)?;

    let mut delta_lsns: Vec<Lsn> = delta_keys
        .iter()
        .filter_map(|key| {
            let rest = key.strip_prefix(&delta_prefix)?;
            let lsn_hex = rest.split('/').next()?;
            Lsn::from_hex(lsn_hex).ok()
        })
        .filter(|&lsn| lsn > base_lsn)
        .collect();
    delta_lsns.sort();
    delta_lsns.dedup();

    if delta_lsns.is_empty() {
        tracing::debug!("tiko: pitr: no new deltas since base {base_lsn}");
        return Ok(MaterializeResult::NoNewDeltas { base_lsn });
    }

    let mut deltas = Vec::with_capacity(delta_lsns.len());
    for &delta_lsn in &delta_lsns {
        let key = ns.delta_manifest_key(timeline, delta_lsn);
        let delta_bytes = sim
            .get_standard(&key)?
            .ok_or_else(|| format!("delta manifest not found: {key}"))?;
        let delta_path = std::env::temp_dir().join(format!(
            "tiko_pitr_delta_{}_{}.tikm",
            ns.project_id,
            delta_lsn.to_hex()
        ));
        deltas.push(Manifest::from_bytes(&delta_bytes, &delta_path)?);
    }

    base.apply_deltas(&deltas)?;

    let new_lsn = *delta_lsns.last().unwrap();
    let delta_count = delta_lsns.len();
    tracing::debug!(
        "tiko: pitr: uploading new base manifest at lsn={} ({} delta(s) merged, prev_base={})",
        new_lsn.to_hex(),
        delta_count,
        base_lsn,
    );
    sim.put_standard(&ns.base_manifest_key(timeline, new_lsn), &base.to_bytes()?)?;

    if let Some(ctx) = ProjectCtx::try_get() {
        let mut all_updates = vec![base];
        all_updates.extend(deltas);
        ctx.base_manifest.apply_deltas(&all_updates)?;
    }

    Ok(MaterializeResult::Materialized {
        prev_base_lsn: base_lsn,
        new_lsn,
        delta_count,
    })
}

// ── Tests ──

#[cfg(test)]
mod tests {
    use super::*;
    use crate::project::ProjectNamespace;
    use crate::s3_sim::S3Sim;
    use pgsys::Lsn;
    use tempfile::{TempDir, tempdir};

    // ── Test helpers ──

    fn chunk(spc: u32, db: u32, rel: u32, fork: i32, id: u32) -> ChunkTag {
        ChunkTag {
            spc_oid: spc,
            db_oid: db,
            rel_number: rel,
            fork_number: fork,
            chunk_id: id,
        }
    }

    fn cref(branch_id: u64, lsn_val: u64) -> ChunkRef {
        ChunkRef {
            branch_id,
            timeline_id: 1,
            lsn: Lsn::new(lsn_val),
        }
    }

    fn setup_sim() -> (TempDir, S3Sim) {
        let dir = TempDir::new().unwrap();
        let store = S3Sim::new(dir.path());
        (dir, store)
    }

    fn ns_with_project(project_id: u64) -> ProjectNamespace {
        ProjectNamespace::new(1001, project_id, 1)
    }

    fn tag(rel: u32) -> ChunkTag {
        chunk(1663, 5, rel, 0, 0)
    }

    fn store_manifest(
        sim: &S3Sim,
        key: &str,
        lsn: Lsn,
        chunks: Vec<(ChunkTag, ChunkRef)>,
        tmp: &Path,
    ) {
        let m = Manifest::new(lsn, 0, chunks, HashMap::new(), vec![], tmp).unwrap();
        sim.put_standard(key, &m.to_bytes().unwrap()).unwrap();
    }

    /// Read and verify raw TIKM header fields directly from the file on disk.
    fn raw_header(path: &Path) -> (u32, Lsn, i64, u64) {
        let mut header = [0u8; 32];
        let f = File::open(path).unwrap();
        f.read_at(&mut header, 0).unwrap();
        assert_eq!(&header[0..4], b"TIKM", "magic mismatch");
        let version = u32::from_le_bytes(header[4..8].try_into().unwrap());
        let lsn = Lsn::new(u64::from_le_bytes(header[8..16].try_into().unwrap()));
        let ts = i64::from_le_bytes(header[16..24].try_into().unwrap());
        let count = u64::from_le_bytes(header[24..32].try_into().unwrap());
        (version, lsn, ts, count)
    }

    /// Read all entries from a TIKM file on disk without going through Manifest.
    fn raw_entries(path: &Path) -> Vec<(ChunkTag, ChunkRef)> {
        let data = fs::read(path).unwrap();
        let entry_count = u64::from_le_bytes(data[24..32].try_into().unwrap()) as usize;
        let mut result = Vec::with_capacity(entry_count);
        for i in 0..entry_count {
            let off = 32 + i * 40;
            let tag = ChunkTag::decode(data[off..off + 20].try_into().unwrap());
            let cref = ChunkRef::decode(data[off + 20..off + 40].try_into().unwrap());
            result.push((tag, cref));
        }
        result
    }

    // ── new_sorted ──

    #[test]
    fn new_sorted_writes_valid_header() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("m.tikm");
        let k1 = chunk(1663, 5, 1, 0, 0);
        let k2 = chunk(1663, 5, 2, 0, 0);
        let _m = Manifest::new(
            Lsn::new(0x100),
            42,
            vec![(k2, cref(2, 0x100)), (k1, cref(1, 0x100))],
            HashMap::new(),
            vec![],
            &path,
        )
        .unwrap();

        let (version, lsn, ts, count) = raw_header(&path);
        assert_eq!(version, 1);
        assert_eq!(lsn, Lsn::new(0x100));
        assert_eq!(ts, 42);
        assert_eq!(count, 2);
    }

    #[test]
    fn new_sorted_entries_in_ascending_order() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("m.tikm");
        // Provide in reverse order; new_sorted must sort them.
        let k3 = chunk(1663, 5, 3, 0, 0);
        let k1 = chunk(1663, 5, 1, 0, 0);
        let k2 = chunk(1663, 5, 2, 0, 0);
        let _m = Manifest::new(
            Lsn::new(1),
            0,
            vec![(k3, cref(1, 1)), (k1, cref(1, 1)), (k2, cref(1, 1))],
            HashMap::new(),
            vec![],
            &path,
        )
        .unwrap();

        let entries = raw_entries(&path);
        assert_eq!(entries.len(), 3);
        assert!(
            entries[0].0 < entries[1].0,
            "entries must be sorted ascending"
        );
        assert!(
            entries[1].0 < entries[2].0,
            "entries must be sorted ascending"
        );
        assert_eq!(entries[0].0, k1);
        assert_eq!(entries[1].0, k2);
        assert_eq!(entries[2].0, k3);
    }

    // ── lookup ──

    #[test]
    fn lookup_hit_returns_correct_chunk_ref() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("m.tikm");
        let k = chunk(1663, 5, 42, 0, 7);
        let r = cref(99, 0x300);
        let m = Manifest::new(
            Lsn::new(0x300),
            0,
            vec![(k, r)],
            HashMap::new(),
            vec![],
            &path,
        )
        .unwrap();
        assert_eq!(m.lookup(&k).unwrap(), Some(r));
    }

    #[test]
    fn lookup_miss_returns_none() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("m.tikm");
        let k = chunk(1663, 5, 1, 0, 0);
        let absent = chunk(1663, 5, 999, 0, 0);
        let m = Manifest::new(
            Lsn::new(1),
            0,
            vec![(k, cref(1, 1))],
            HashMap::new(),
            vec![],
            &path,
        )
        .unwrap();
        assert_eq!(m.lookup(&absent).unwrap(), None);
    }

    #[test]
    fn lookup_empty_manifest_returns_none() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("m.tikm");
        let m = Manifest::new(Lsn::new(0), 0, vec![], HashMap::new(), vec![], &path).unwrap();
        assert_eq!(m.lookup(&chunk(1663, 5, 1, 0, 0)).unwrap(), None);
    }

    #[test]
    fn lookup_first_and_last_entry() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("m.tikm");
        let keys: Vec<ChunkTag> = (0..20).map(|i| chunk(1663, 5, i, 0, 0)).collect();
        let chunks: Vec<_> = keys.iter().map(|k| (*k, cref(1, 100))).collect();
        let m = Manifest::new(Lsn::new(100), 0, chunks, HashMap::new(), vec![], &path).unwrap();

        assert!(
            m.lookup(&keys[0]).unwrap().is_some(),
            "first entry not found"
        );
        assert!(
            m.lookup(&keys[19]).unwrap().is_some(),
            "last entry not found"
        );
        assert_eq!(
            m.lookup(&chunk(1663, 5, 99, 0, 0)).unwrap(),
            None,
            "absent key should miss"
        );
    }

    // ── open ──

    #[test]
    fn open_round_trip_preserves_metadata() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("m.tikm");
        let k = chunk(1663, 5, 7, 0, 0);
        let _m = Manifest::new(
            Lsn::new(0x200),
            999,
            vec![(k, cref(5, 0x200))],
            HashMap::new(),
            vec![],
            &path,
        )
        .unwrap();

        let m2 = Manifest::open(&path).unwrap();
        assert_eq!(m2.checkpoint_lsn(), Lsn::new(0x200));
        assert_eq!(m2.timestamp(), 999);
        assert_eq!(m2.inner.lock().unwrap().entry_count, 1);
        assert_eq!(m2.lookup(&k).unwrap(), Some(cref(5, 0x200)));
    }

    #[test]
    fn open_rejects_bad_magic() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("bad.tikm");
        fs::write(&path, b"BOGUSDATA00000000000000000000000").unwrap();
        assert!(Manifest::open(&path).is_err());
    }

    // ── from_bytes / to_bytes ──

    #[test]
    fn from_bytes_to_bytes_round_trip() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("m.tikm");
        let path2 = tmp.path().join("m2.tikm");

        let k1 = chunk(1663, 5, 10, 0, 0);
        let k2 = chunk(1663, 5, 20, 0, 1);
        let m = Manifest::new(
            Lsn::new(0x400),
            1234,
            vec![(k1, cref(7, 0x400)), (k2, cref(8, 0x300))],
            HashMap::new(),
            vec![],
            &path,
        )
        .unwrap();

        let wire = m.to_bytes().unwrap();
        let m2 = Manifest::from_bytes(&wire, &path2).unwrap();

        assert_eq!(m2.checkpoint_lsn(), Lsn::new(0x400));
        assert_eq!(m2.timestamp(), 1234);
        assert_eq!(m2.lookup(&k1).unwrap(), Some(cref(7, 0x400)));
        assert_eq!(m2.lookup(&k2).unwrap(), Some(cref(8, 0x300)));
    }

    #[test]
    fn from_bytes_to_bytes_empty_manifest() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("m.tikm");
        let path2 = tmp.path().join("m2.tikm");

        let m = Manifest::new(Lsn::new(0), 0, vec![], HashMap::new(), vec![], &path).unwrap();
        let wire = m.to_bytes().unwrap();
        let m2 = Manifest::from_bytes(&wire, &path2).unwrap();

        assert_eq!(m2.inner.lock().unwrap().entry_count, 0);
        assert_eq!(m2.lookup(&chunk(1663, 5, 1, 0, 0)).unwrap(), None);
    }

    // ── apply_deltas ──

    #[test]
    fn apply_deltas_three_deltas_correct_chunk_refs() {
        let tmp = tempdir().unwrap();
        let base_path = tmp.path().join("base.tikm");
        let d1_path = tmp.path().join("d1.tikm");
        let d2_path = tmp.path().join("d2.tikm");
        let d3_path = tmp.path().join("d3.tikm");

        let k_a = chunk(1663, 5, 100, 0, 0);
        let k_b = chunk(1663, 5, 200, 0, 0);
        let k_c = chunk(1663, 5, 300, 0, 0);

        let base = Manifest::new(
            Lsn::new(0x100),
            1,
            vec![
                (k_a, cref(1, 0x100)),
                (k_b, cref(1, 0x100)),
                (k_c, cref(1, 0x100)),
            ],
            HashMap::new(),
            vec![],
            &base_path,
        )
        .unwrap();

        // d1 updates k_a to branch 2
        let d1 = Manifest::new(
            Lsn::new(0x200),
            2,
            vec![(k_a, cref(2, 0x200))],
            HashMap::new(),
            vec![],
            &d1_path,
        )
        .unwrap();
        // d2 updates k_b and k_c to branch 3
        let d2 = Manifest::new(
            Lsn::new(0x300),
            3,
            vec![(k_b, cref(3, 0x300)), (k_c, cref(3, 0x300))],
            HashMap::new(),
            vec![],
            &d2_path,
        )
        .unwrap();
        // d3 updates k_a again to branch 4 (highest)
        let d3 = Manifest::new(
            Lsn::new(0x400),
            4,
            vec![(k_a, cref(4, 0x400))],
            HashMap::new(),
            vec![],
            &d3_path,
        )
        .unwrap();

        base.apply_deltas(&[d1, d2, d3]).unwrap();

        assert_eq!(base.checkpoint_lsn(), Lsn::new(0x400));
        assert_eq!(base.lookup(&k_a).unwrap(), Some(cref(4, 0x400)));
        assert_eq!(base.lookup(&k_b).unwrap(), Some(cref(3, 0x300)));
        assert_eq!(base.lookup(&k_c).unwrap(), Some(cref(3, 0x300)));
    }

    #[test]
    fn apply_deltas_empty_slice_is_noop() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("m.tikm");
        let k = chunk(1663, 5, 42, 0, 0);
        let m = Manifest::new(
            Lsn::new(0x100),
            10,
            vec![(k, cref(1, 0x100))],
            HashMap::new(),
            vec![],
            &path,
        )
        .unwrap();

        // Capture mtime before to verify no file I/O happens.
        let mtime_before = fs::metadata(&path).unwrap().modified().unwrap();

        m.apply_deltas(&[]).unwrap();

        let mtime_after = fs::metadata(&path).unwrap().modified().unwrap();
        assert_eq!(mtime_before, mtime_after, "file must not be touched");
        assert_eq!(m.checkpoint_lsn(), Lsn::new(0x100));
        assert_eq!(m.timestamp(), 10);
        assert_eq!(m.lookup(&k).unwrap(), Some(cref(1, 0x100)));
    }

    #[test]
    fn apply_deltas_idempotent() {
        let tmp = tempdir().unwrap();
        let base_path = tmp.path().join("base.tikm");
        let d_path = tmp.path().join("d.tikm");

        let k = chunk(1663, 5, 7, 0, 0);
        let base = Manifest::new(
            Lsn::new(0x100),
            1,
            vec![(k, cref(1, 0x100))],
            HashMap::new(),
            vec![],
            &base_path,
        )
        .unwrap();
        let delta = Manifest::new(
            Lsn::new(0x200),
            2,
            vec![(k, cref(2, 0x200))],
            HashMap::new(),
            vec![],
            &d_path,
        )
        .unwrap();

        base.apply_deltas(&[delta]).unwrap();
        let after_first = base.lookup(&k).unwrap();

        // Re-open the delta (since apply_deltas consumed it) and apply again.
        let delta2 = Manifest::new(
            Lsn::new(0x200),
            2,
            vec![(k, cref(2, 0x200))],
            HashMap::new(),
            vec![],
            &d_path,
        )
        .unwrap();
        base.apply_deltas(&[delta2]).unwrap();
        let after_second = base.lookup(&k).unwrap();

        assert_eq!(
            after_first, after_second,
            "idempotent: applying same delta twice"
        );
    }

    #[test]
    fn apply_deltas_tie_at_equal_lsn_keeps_self_entry() {
        let tmp = tempdir().unwrap();
        let base_path = tmp.path().join("base.tikm");
        let d_path = tmp.path().join("d.tikm");

        let k = chunk(1663, 5, 55, 0, 0);
        // Base has branch_id=10 at LSN 0x300.
        let base = Manifest::new(
            Lsn::new(0x300),
            1,
            vec![(k, cref(10, 0x300))],
            HashMap::new(),
            vec![],
            &base_path,
        )
        .unwrap();
        // Delta has branch_id=99 at the same LSN 0x300 — tie.
        let delta = Manifest::new(
            Lsn::new(0x300),
            2,
            vec![(k, cref(99, 0x300))],
            HashMap::new(),
            vec![],
            &d_path,
        )
        .unwrap();

        base.apply_deltas(&[delta]).unwrap();

        // Self (base) entry must survive the tie.
        assert_eq!(
            base.lookup(&k).unwrap(),
            Some(cref(10, 0x300)),
            "tie: self entry must be kept"
        );
    }

    #[test]
    fn apply_deltas_higher_lsn_in_delta_wins() {
        let tmp = tempdir().unwrap();
        let base_path = tmp.path().join("base.tikm");
        let d_path = tmp.path().join("d.tikm");

        let k = chunk(1663, 5, 77, 0, 0);
        let base = Manifest::new(
            Lsn::new(0x100),
            1,
            vec![(k, cref(1, 0x100))],
            HashMap::new(),
            vec![],
            &base_path,
        )
        .unwrap();
        let delta = Manifest::new(
            Lsn::new(0x200),
            2,
            vec![(k, cref(22, 0x200))],
            HashMap::new(),
            vec![],
            &d_path,
        )
        .unwrap();

        base.apply_deltas(&[delta]).unwrap();

        assert_eq!(base.lookup(&k).unwrap(), Some(cref(22, 0x200)));
    }

    #[test]
    fn apply_deltas_older_lsn_in_delta_does_not_overwrite() {
        let tmp = tempdir().unwrap();
        let base_path = tmp.path().join("base.tikm");
        let d_path = tmp.path().join("d.tikm");

        let k = chunk(1663, 5, 88, 0, 0);
        // Base already has a newer LSN.
        let base = Manifest::new(
            Lsn::new(0x300),
            3,
            vec![(k, cref(10, 0x300))],
            HashMap::new(),
            vec![],
            &base_path,
        )
        .unwrap();
        let stale_delta = Manifest::new(
            Lsn::new(0x200),
            2,
            vec![(k, cref(99, 0x200))],
            HashMap::new(),
            vec![],
            &d_path,
        )
        .unwrap();

        base.apply_deltas(&[stale_delta]).unwrap();

        assert_eq!(base.lookup(&k).unwrap(), Some(cref(10, 0x300)));
    }

    // ── LSN utilities ──

    #[test]
    fn lsn_to_hex_edge_cases() {
        assert_eq!(Lsn::new(0u64).to_hex(), "0000000000000000");
        assert_eq!(Lsn::new(0x3A000028u64).to_hex(), "000000003A000028");
        assert_eq!(Lsn::new(u64::MAX).to_hex(), "FFFFFFFFFFFFFFFF");
    }

    #[test]
    fn lsn_from_hex_parses_expected_values() {
        assert_eq!(Lsn::from_hex("0000000000000000").unwrap(), Lsn::new(0));
        assert_eq!(
            Lsn::from_hex("000000003A000028").unwrap(),
            Lsn::new(0x3A000028)
        );
        assert_eq!(
            Lsn::from_hex("FFFFFFFFFFFFFFFF").unwrap(),
            Lsn::new(u64::MAX)
        );
    }

    // ── tombstone tests ──

    #[test]
    fn apply_deltas_tombstone_removes_chunks() {
        let tmp = tempdir().unwrap();
        let base_path = tmp.path().join("base.tikm");
        let d_path = tmp.path().join("d.tikm");

        let rf = RelFork {
            spc_oid: 1663,
            db_oid: 5,
            rel_number: 42,
            fork_number: 0,
        };
        let k = chunk(1663, 5, 42, 0, 0);
        let base = Manifest::new(
            Lsn::new(0x100),
            1,
            vec![(k, cref(1, 0x100))],
            HashMap::new(),
            vec![],
            &base_path,
        )
        .unwrap();

        // Delta carries a tombstone for the fork; no chunk entries.
        let delta = Manifest::new(
            Lsn::new(0x200),
            2,
            vec![],
            HashMap::new(),
            vec![rf],
            &d_path,
        )
        .unwrap();

        base.apply_deltas(&[delta]).unwrap();

        // The chunk belonging to the deleted fork must be purged from the base.
        assert_eq!(base.lookup(&k).unwrap(), None);
    }

    #[test]
    fn apply_deltas_tombstone_removes_nblocks() {
        let tmp = tempdir().unwrap();
        let base_path = tmp.path().join("base.tikm");
        let d_path = tmp.path().join("d.tikm");

        let rf = RelFork {
            spc_oid: 1663,
            db_oid: 5,
            rel_number: 99,
            fork_number: 0,
        };
        let mut nb = HashMap::new();
        nb.insert(rf, 10u32);
        let base = Manifest::new(Lsn::new(0x100), 1, vec![], nb, vec![], &base_path).unwrap();

        let delta = Manifest::new(
            Lsn::new(0x200),
            2,
            vec![],
            HashMap::new(),
            vec![rf],
            &d_path,
        )
        .unwrap();

        base.apply_deltas(&[delta]).unwrap();

        // The nblocks entry for the deleted fork must be removed.
        assert_eq!(base.lookup_nblocks(rf), None);
    }

    #[test]
    fn materialize_merges_new_deltas_onto_base() {
        let (dir, sim) = setup_sim();
        let ns = ns_with_project(20_001);

        let lsns: Vec<Lsn> = (1u64..=10).map(|i| Lsn::new(i * 0x100)).collect();

        let base_chunks: Vec<(ChunkTag, ChunkRef)> = (1u32..=3)
            .map(|i| {
                (
                    tag(i),
                    ChunkRef {
                        branch_id: 1,
                        timeline_id: 1,
                        lsn: lsns[i as usize - 1],
                    },
                )
            })
            .collect();
        store_manifest(
            &sim,
            &ns.base_manifest_key(1, lsns[2]),
            lsns[2],
            base_chunks,
            &dir.path().join("base.tikm"),
        );

        for i in 1u32..=10 {
            let delta_lsn = lsns[i as usize - 1];
            let chunks = vec![
                (
                    tag(i),
                    ChunkRef {
                        branch_id: 1,
                        timeline_id: 1,
                        lsn: delta_lsn,
                    },
                ),
                (
                    tag(0),
                    ChunkRef {
                        branch_id: 1,
                        timeline_id: 1,
                        lsn: delta_lsn,
                    },
                ),
            ];
            store_manifest(
                &sim,
                &ns.delta_manifest_key(1, delta_lsn),
                delta_lsn,
                chunks,
                &dir.path().join(format!("d{i}.tikm")),
            );
        }

        materialize_base(&sim, &ns, 1).unwrap();

        let bytes = sim
            .get_standard(&ns.base_manifest_key(1, lsns[9]))
            .unwrap()
            .expect("new base manifest must exist after materialization");
        let tmp = dir.path().join("verify.tikm");
        let merged = Manifest::from_bytes(&bytes, &tmp).unwrap();

        assert_eq!(
            merged.lookup(&tag(7)).unwrap(),
            Some(ChunkRef {
                branch_id: 1,
                timeline_id: 1,
                lsn: lsns[6],
            })
        );
        assert_eq!(
            merged.lookup(&tag(0)).unwrap(),
            Some(ChunkRef {
                branch_id: 1,
                timeline_id: 1,
                lsn: lsns[9],
            })
        );

        for i in 1u32..=10 {
            assert!(
                sim.get_standard(&ns.delta_manifest_key(1, lsns[i as usize - 1]))
                    .unwrap()
                    .is_some(),
                "delta {i} must not be deleted by materialize_base"
            );
        }
    }

    #[test]
    fn lookup_correct_chunk_ref_after_materialization() {
        let (dir, sim) = setup_sim();
        let ns = ns_with_project(20_002);

        let base_lsn = Lsn::new(0x100);
        let d4_lsn = Lsn::new(0x400);
        let d7_lsn = Lsn::new(0x700);

        store_manifest(
            &sim,
            &ns.base_manifest_key(1, Lsn::new(0x300)),
            Lsn::new(0x300),
            vec![
                (
                    tag(1),
                    ChunkRef {
                        branch_id: 1,
                        timeline_id: 1,
                        lsn: base_lsn,
                    },
                ),
                (
                    tag(2),
                    ChunkRef {
                        branch_id: 1,
                        timeline_id: 1,
                        lsn: base_lsn,
                    },
                ),
                (
                    tag(3),
                    ChunkRef {
                        branch_id: 1,
                        timeline_id: 1,
                        lsn: base_lsn,
                    },
                ),
            ],
            &dir.path().join("base.tikm"),
        );
        store_manifest(
            &sim,
            &ns.delta_manifest_key(1, d4_lsn),
            d4_lsn,
            vec![(
                tag(2),
                ChunkRef {
                    branch_id: 1,
                    timeline_id: 1,
                    lsn: d4_lsn,
                },
            )],
            &dir.path().join("d4.tikm"),
        );
        store_manifest(
            &sim,
            &ns.delta_manifest_key(1, d7_lsn),
            d7_lsn,
            vec![(
                tag(7),
                ChunkRef {
                    branch_id: 1,
                    timeline_id: 1,
                    lsn: d7_lsn,
                },
            )],
            &dir.path().join("d7.tikm"),
        );

        materialize_base(&sim, &ns, 1).unwrap();

        let bytes = sim
            .get_standard(&ns.base_manifest_key(1, d7_lsn))
            .unwrap()
            .expect("new base at d7_lsn must exist");
        let tmp = dir.path().join("verify.tikm");
        let m = Manifest::from_bytes(&bytes, &tmp).unwrap();

        assert_eq!(
            m.lookup(&tag(1)).unwrap(),
            Some(ChunkRef {
                branch_id: 1,
                timeline_id: 1,
                lsn: base_lsn,
            })
        );
        assert_eq!(
            m.lookup(&tag(2)).unwrap(),
            Some(ChunkRef {
                branch_id: 1,
                timeline_id: 1,
                lsn: d4_lsn,
            })
        );
        assert_eq!(
            m.lookup(&tag(7)).unwrap(),
            Some(ChunkRef {
                branch_id: 1,
                timeline_id: 1,
                lsn: d7_lsn,
            })
        );
    }

    #[test]
    fn materialize_idempotent() {
        let (dir, sim) = setup_sim();
        let ns = ns_with_project(20_003);

        let base_lsn = Lsn::new(0x100);
        let delta_lsn = Lsn::new(0x200);

        store_manifest(
            &sim,
            &ns.base_manifest_key(1, base_lsn),
            base_lsn,
            vec![(
                tag(1),
                ChunkRef {
                    branch_id: 1,
                    timeline_id: 1,
                    lsn: base_lsn,
                },
            )],
            &dir.path().join("base.tikm"),
        );
        store_manifest(
            &sim,
            &ns.delta_manifest_key(1, delta_lsn),
            delta_lsn,
            vec![(
                tag(1),
                ChunkRef {
                    branch_id: 1,
                    timeline_id: 1,
                    lsn: delta_lsn,
                },
            )],
            &dir.path().join("d1.tikm"),
        );

        materialize_base(&sim, &ns, 1).unwrap();
        materialize_base(&sim, &ns, 1).unwrap();

        let keys = sim
            .list_prefix_standard(&ns.base_prefix_for_timeline(1))
            .unwrap();
        assert_eq!(keys.len(), 2, "second run must not write another base");

        let bytes = sim
            .get_standard(&ns.base_manifest_key(1, delta_lsn))
            .unwrap()
            .expect("materialized base must exist");
        let tmp = dir.path().join("verify.tikm");
        let m = Manifest::from_bytes(&bytes, &tmp).unwrap();
        assert_eq!(
            m.lookup(&tag(1)).unwrap(),
            Some(ChunkRef {
                branch_id: 1,
                timeline_id: 1,
                lsn: delta_lsn,
            })
        );
    }

    #[test]
    fn materialize_no_new_deltas_is_noop() {
        let (dir, sim) = setup_sim();
        let ns = ns_with_project(20_004);

        let lsn = Lsn::new(0x100);
        store_manifest(
            &sim,
            &ns.base_manifest_key(1, lsn),
            lsn,
            vec![(
                tag(1),
                ChunkRef {
                    branch_id: 1,
                    timeline_id: 1,
                    lsn,
                },
            )],
            &dir.path().join("base.tikm"),
        );

        materialize_base(&sim, &ns, 1).unwrap();

        let keys = sim
            .list_prefix_standard(&ns.base_prefix_for_timeline(1))
            .unwrap();
        assert_eq!(
            keys.len(),
            1,
            "no new base should be written when no deltas"
        );
    }

    #[test]
    fn materialize_no_base_returns_ok() {
        let (_dir, sim) = setup_sim();
        let ns = ns_with_project(20_005);

        materialize_base(&sim, &ns, 1).unwrap();

        let keys = sim
            .list_prefix_standard(&ns.base_prefix_for_timeline(1))
            .unwrap();
        assert!(
            keys.is_empty(),
            "no base should be written when no deltas exist"
        );
    }

    #[test]
    fn materialize_no_base_with_deltas_creates_first_base() {
        let (dir, sim) = setup_sim();
        let ns = ns_with_project(20_006);

        let d1_lsn = Lsn::new(0x100);
        let d2_lsn = Lsn::new(0x200);

        store_manifest(
            &sim,
            &ns.delta_manifest_key(1, d1_lsn),
            d1_lsn,
            vec![(
                tag(1),
                ChunkRef {
                    branch_id: 1,
                    timeline_id: 1,
                    lsn: d1_lsn,
                },
            )],
            &dir.path().join("d1.tikm"),
        );
        store_manifest(
            &sim,
            &ns.delta_manifest_key(1, d2_lsn),
            d2_lsn,
            vec![(
                tag(2),
                ChunkRef {
                    branch_id: 1,
                    timeline_id: 1,
                    lsn: d2_lsn,
                },
            )],
            &dir.path().join("d2.tikm"),
        );

        materialize_base(&sim, &ns, 1).unwrap();

        let bytes = sim
            .get_standard(&ns.base_manifest_key(1, d2_lsn))
            .unwrap()
            .expect("initial base manifest must exist after materialization");
        let tmp = dir.path().join("verify.tikm");
        let m = Manifest::from_bytes(&bytes, &tmp).unwrap();

        assert_eq!(
            m.lookup(&tag(1)).unwrap(),
            Some(ChunkRef {
                branch_id: 1,
                timeline_id: 1,
                lsn: d1_lsn,
            })
        );
        assert_eq!(
            m.lookup(&tag(2)).unwrap(),
            Some(ChunkRef {
                branch_id: 1,
                timeline_id: 1,
                lsn: d2_lsn,
            })
        );
    }
}
