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
//! Header (48 bytes):
//!   magic:          [u8; 4] = b"TIKM"
//!   version:        u32 = 1            (little-endian)
//!   timeline_id:    u32                (little-endian)   ┐ base
//!   redo_timeline_id: u32              (little-endian)   │ checkpoint
//!   lsn:            u64                (little-endian)   ┘
//!   redo_lsn:       u64                (little-endian)   redo checkpoint
//!   timestamp:      i64 (unix secs)    (little-endian)
//!   entry_count:    u64                (little-endian)   chunk count
//!
//! Chunk body (entry_count × 40 bytes, sorted ascending by ChunkTag):
//!   ChunkTag  20 bytes  (spc_oid u32, db_oid u32, rel_number u32,
//!                         fork_number i32, chunk_id u32 — all LE)
//!   ChunkRef  20 bytes  (db_id u64, timeline_id u32, lsn u64 — all LE)
//!
//! Meta header (8 bytes):
//!   meta_count:     u64                (little-endian)
//!
//! Meta body (meta_count × 24 bytes, sorted ascending by RelFork):
//!   RelFork   16 bytes  (spc_oid u32, db_oid u32, rel_number u32,
//!                         fork_number i32 — all LE)
//!   nblocks   u32       (little-endian)
//!   deleted   u8        (0 = false, nonzero = true)
//!   _pad      3 bytes   (zero)
//!
//! pg_state trailer (8 + pg_state_len bytes):
//!   pg_state_len:   u64                (little-endian)
//!   pg_state:       [u8; pg_state_len] (pg_state.tar.zst archive bytes)
//! ```
//!
//! `redo_ckpt` and `pg_state` make a base manifest a self-contained base
//! backup for PITR: `pg_state` carries the `pg_control` + transaction-log
//! image at the base checkpoint, and `redo_ckpt` is the LSN from which WAL
//! replay begins. They are written by the compactor from the highest
//! [`SegmentCheckpoint`] folded into the base. The trailer sits after the
//! meta body and is never touched by lookups.
//!
//! Both lookups (chunks and relfork meta) are O(log N) `pread` binary
//! searches over the sorted on-disk sections — no in-memory copies.
//!
//! The file is published via per-PID tmp + atomic `rename`, so concurrent
//! writers from multiple processes never tear the visible file. Readers
//! attach to an existing file with [`Manifest::open_local`] without writing.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use pgsys::Lsn;
use pgsys::timeline_id::TimelineId;
use serde::{Deserialize, Serialize};

use crate::chunk::{CHUNK_TAG_SIZE, ChunkTag, RelFork};
use crate::error::{Error, Result};
use crate::io::timeline::{Checkpoint, SegmentCheckpoint};
use crate::relfork::{REL_FORK_SIZE, RelForkMeta};

// ── TIKM constants ──

const TIKM_MAGIC: [u8; 4] = *b"TIKM";
const TIKM_VERSION: u32 = 1;
/// Filename of the local TIKM cache file under the tiko root path.
const BASE_MANIFEST_FILE_NAME: &str = "base_manifest.tikm";
/// Header size in bytes.
const HEADER_SIZE: usize = 48;
/// Chunk-entry size in bytes (ChunkTag[20] + ChunkRef[20]).
const ENTRY_SIZE: usize = CHUNK_TAG_SIZE + CHUNK_REF_SIZE;
/// Meta-entry size in bytes (RelFork[16] + nblocks[4] + deleted[1] + pad[3]).
const META_ENTRY_SIZE: usize = REL_FORK_SIZE + 8;
/// Size of the meta-section header (`meta_count` u64).
const META_HEADER_SIZE: usize = 8;

// ── ChunkRef ──

/// Reference to a specific version of a chunk stored in S3.
///
/// Note: no `#[repr(C)]` and no `size_of` assert here — `ChunkRef` is never
/// cast to raw bytes. Its in-memory size is 24 bytes (4-byte alignment padding
/// between `timeline_id: u32` and `lsn: u64`), while the wire encoding is 20
/// bytes. The wire size is enforced by `encode() -> [u8; 20]`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub(crate) struct ChunkRef {
    /// Branch-scoped id: selects `{org}/chunks/{db_id}/` in the standard bucket.
    pub db_id: u64,
    /// Timeline on which this chunk version was written.
    /// Together with `db_id` and `lsn`, uniquely identifies the S3 object:
    /// `{org}/chunks/{db_id}/{tag}/{timeline_id:08X}/{lsn_hex}`.
    pub timeline_id: u32,
    /// Checkpoint LSN at which this chunk version was sealed.
    pub lsn: Lsn,
}

impl ChunkRef {
    fn encode(&self) -> [u8; 20] {
        let mut buf = [0u8; 20];
        buf[0..8].copy_from_slice(&self.db_id.to_le_bytes());
        buf[8..12].copy_from_slice(&self.timeline_id.to_le_bytes());
        buf[12..20].copy_from_slice(&self.lsn.as_u64().to_le_bytes());
        buf
    }

    fn decode(buf: &[u8; 20]) -> Self {
        ChunkRef {
            db_id: u64::from_le_bytes(buf[0..8].try_into().unwrap()),
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

// ── Manifest ──

/// File-backed sorted manifest for chunk lookup and PITR merge operations.
///
/// An immutable snapshot of one TIKM file. All public methods take `&self`
/// and read from `file` via `pread`, which is safe for concurrent use on a
/// shared FD. The compactor produces a new `Manifest` by calling
/// [`Self::apply_segments`] followed by [`Self::commit_applied`]; the caller
/// then swaps the new value in (typically behind an `Arc` on `Store`).
///
/// Invariant: the local TIKM file at `path` is always valid with entries
/// sorted ascending by `ChunkTag`.
pub(crate) struct Manifest {
    checkpoint: Checkpoint,
    /// Redo checkpoint of the highest [`SegmentCheckpoint`] folded into this
    /// base — the LSN from which WAL replay starts when this manifest is used
    /// as a PITR base backup. Default (`0/0`) on an empty/bootstrap manifest.
    redo_ckpt: Checkpoint,
    /// `pg_state.tar.zst` archive (pg_control + transaction logs) captured at
    /// the base checkpoint. Empty on an empty/bootstrap manifest. Stored in the
    /// TIKM trailer; never read on the lookup hot path.
    pg_state: Vec<u8>,
    timestamp: i64,
    /// Path to the local TIKM binary file.
    path: PathBuf,
    /// Read handle. Multiple `Arc<Manifest>` readers can `pread` the same
    /// FD concurrently. When a new `Manifest` replaces this one, this FD
    /// keeps pointing at the now-unlinked old inode for as long as any
    /// `Arc` reference is alive.
    file: File,
    /// Number of chunk entries in the chunk body.
    entry_count: u64,
    /// Number of meta entries in the meta body.
    meta_count: u64,
}

impl Manifest {
    /// Byte offset of the meta-section header inside the file.
    fn meta_header_offset(&self) -> u64 {
        HEADER_SIZE as u64 + self.entry_count * ENTRY_SIZE as u64
    }

    /// Byte offset of the first meta-entry in the file.
    fn meta_body_offset(&self) -> u64 {
        self.meta_header_offset() + META_HEADER_SIZE as u64
    }
}

/// Read the length-prefixed pg_state trailer at `offset` from `file`.
/// Returns an empty vector when the stored length is zero.
fn read_pg_state(file: &File, offset: u64) -> io::Result<Vec<u8>> {
    let mut len_buf = [0u8; 8];
    pread_exact(file, &mut len_buf, offset)?;
    let len = u64::from_le_bytes(len_buf) as usize;
    if len == 0 {
        return Ok(Vec::new());
    }
    let mut buf = vec![0u8; len];
    pread_exact(file, &mut buf, offset + 8)?;
    Ok(buf)
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

/// Write a TIKM file from pre-sorted `chunks` and `meta` slices. Returns an
/// open read handle to the published file.
///
/// Atomicity: writes to a per-PID tmp path, then renames over `path`.
/// Concurrent writers from other processes cannot tear the visible file.
/// Creates parent directories as needed.
fn write_tikm(
    path: &Path,
    checkpoint: Checkpoint,
    redo_ckpt: Checkpoint,
    timestamp: i64,
    chunks: &[(ChunkTag, ChunkRef)],
    meta: &[(RelFork, RelForkMeta)],
    pg_state: &[u8],
) -> Result<File> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let tmp_path = path.with_extension(format!("tikm.{}.tmp", std::process::id()));
    let mut f = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&tmp_path)?;

    // Header (48 bytes): magic[4] + version[4] + timeline_id[4] +
    //                    redo_timeline_id[4] + lsn[8] + redo_lsn[8] +
    //                    timestamp[8] + entry_count[8]
    let mut header = [0u8; HEADER_SIZE];
    header[0..4].copy_from_slice(&TIKM_MAGIC);
    header[4..8].copy_from_slice(&TIKM_VERSION.to_le_bytes());
    header[8..12].copy_from_slice(&checkpoint.timeline_id.as_u32().to_le_bytes());
    header[12..16].copy_from_slice(&redo_ckpt.timeline_id.as_u32().to_le_bytes());
    header[16..24].copy_from_slice(&checkpoint.lsn.as_u64().to_le_bytes());
    header[24..32].copy_from_slice(&redo_ckpt.lsn.as_u64().to_le_bytes());
    header[32..40].copy_from_slice(&timestamp.to_le_bytes());
    header[40..48].copy_from_slice(&(chunks.len() as u64).to_le_bytes());
    f.write_all(&header)?;

    // Chunk body (sorted ascending by ChunkTag).
    for (tag, cref) in chunks {
        f.write_all(&tag.encode())?;
        f.write_all(&cref.encode())?;
    }

    // Meta header: count.
    f.write_all(&(meta.len() as u64).to_le_bytes())?;
    // Meta body (sorted ascending by RelFork).
    let mut entry = [0u8; META_ENTRY_SIZE];
    for (rf, m) in meta {
        entry.fill(0);
        entry[0..REL_FORK_SIZE].copy_from_slice(&rf.encode());
        entry[REL_FORK_SIZE..REL_FORK_SIZE + 4].copy_from_slice(&m.nblocks.to_le_bytes());
        entry[REL_FORK_SIZE + 4] = m.deleted as u8;
        f.write_all(&entry)?;
    }

    // pg_state trailer: length-prefixed archive bytes. Sits after the meta
    // body and is never touched by binary-search lookups.
    f.write_all(&(pg_state.len() as u64).to_le_bytes())?;
    f.write_all(pg_state)?;

    f.flush()?;
    drop(f);

    fs::rename(&tmp_path, path)?;

    // Reopen read-only for the handle stored in ManifestInner.
    let file = File::open(path)?;
    Ok(file)
}

/// Decode one meta entry from a fixed-size buffer.
fn decode_meta_entry(buf: &[u8; META_ENTRY_SIZE]) -> (RelFork, RelForkMeta) {
    let rf = RelFork::decode(buf[0..REL_FORK_SIZE].try_into().unwrap());
    let nblocks = u32::from_le_bytes(buf[REL_FORK_SIZE..REL_FORK_SIZE + 4].try_into().unwrap());
    let deleted = buf[REL_FORK_SIZE + 4] != 0;
    (rf, RelForkMeta { nblocks, deleted })
}

// ── Private helpers ──

/// Sequential `pread` of all chunk entries starting at `HEADER_SIZE`.
fn read_all_entries(manifest: &Manifest) -> io::Result<Vec<(ChunkTag, ChunkRef)>> {
    let n = manifest.entry_count as usize;
    if n == 0 {
        return Ok(Vec::new());
    }
    let byte_len = n * ENTRY_SIZE;
    let mut buf = vec![0u8; byte_len];
    pread_exact(&manifest.file, &mut buf, HEADER_SIZE as _)?;

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

/// Sequential `pread` of all meta entries (sorted ascending by `RelFork`).
fn read_all_meta_entries(manifest: &Manifest) -> io::Result<Vec<(RelFork, RelForkMeta)>> {
    let n = manifest.meta_count as usize;
    if n == 0 {
        return Ok(Vec::new());
    }
    let byte_len = n * META_ENTRY_SIZE;
    let mut buf = vec![0u8; byte_len];
    pread_exact(&manifest.file, &mut buf, manifest.meta_body_offset())?;

    let mut entries = Vec::with_capacity(n);
    for i in 0..n {
        let off = i * META_ENTRY_SIZE;
        let slice: &[u8; META_ENTRY_SIZE] = buf[off..off + META_ENTRY_SIZE].try_into().unwrap();
        entries.push(decode_meta_entry(slice));
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
    /// Create a zero-entry manifest at the default checkpoint (used as a
    /// bootstrap starting point before the first real base exists).
    pub fn empty(root_path: &Path) -> Result<Self> {
        Self::new(
            Checkpoint::default(),
            Checkpoint::default(),
            0,
            vec![],
            HashMap::new(),
            Vec::new(),
            root_path,
        )
    }

    pub fn new(
        checkpoint: Checkpoint,
        redo_ckpt: Checkpoint,
        timestamp: i64,
        mut chunks: Vec<(ChunkTag, ChunkRef)>,
        meta_map: HashMap<RelFork, RelForkMeta>,
        pg_state: Vec<u8>,
        root_path: &Path,
    ) -> Result<Self> {
        chunks.sort_unstable_by_key(|(tag, _)| *tag);
        let mut meta: Vec<(RelFork, RelForkMeta)> = meta_map.into_iter().collect();
        meta.sort_unstable_by_key(|(rf, _)| *rf);
        let path = root_path.join(BASE_MANIFEST_FILE_NAME);
        let file = write_tikm(
            &path, checkpoint, redo_ckpt, timestamp, &chunks, &meta, &pg_state,
        )?;
        Ok(Manifest {
            checkpoint,
            redo_ckpt,
            pg_state,
            timestamp,
            path: path.to_path_buf(),
            file,
            entry_count: chunks.len() as u64,
            meta_count: meta.len() as u64,
        })
    }

    /// Open the existing local TIKM file under `root_path` in-place — read
    /// the header, validate magic + version, and attach to the file without
    /// rewriting it. Used by [`crate::io::store::Store::load_manifest_at`]
    /// when a sibling process already published the canonical file (e.g.
    /// the compactor).
    pub fn open_local(root_path: &Path) -> Result<Self> {
        let path = root_path.join(BASE_MANIFEST_FILE_NAME);
        let file = File::open(&path)?;
        let mut header = [0u8; HEADER_SIZE];
        pread_exact(&file, &mut header, 0)?;

        if header[0..4] != TIKM_MAGIC {
            return Err(Error::invalid_data("invalid TIKM magic"));
        }
        let version = u32::from_le_bytes(header[4..8].try_into().unwrap());
        if version != TIKM_VERSION {
            return Err(Error::invalid_data(format!(
                "unsupported TIKM version: {version}"
            )));
        }

        let timeline_id = TimelineId::new(u32::from_le_bytes(header[8..12].try_into().unwrap()));
        let redo_timeline_id =
            TimelineId::new(u32::from_le_bytes(header[12..16].try_into().unwrap()));
        let lsn = Lsn::new(u64::from_le_bytes(header[16..24].try_into().unwrap()));
        let redo_lsn = Lsn::new(u64::from_le_bytes(header[24..32].try_into().unwrap()));
        let timestamp = i64::from_le_bytes(header[32..40].try_into().unwrap());
        let entry_count = u64::from_le_bytes(header[40..48].try_into().unwrap());

        // Meta count lives in an 8-byte header immediately after the chunk body.
        let meta_header_offset = HEADER_SIZE as u64 + entry_count * ENTRY_SIZE as u64;
        let mut meta_count_buf = [0u8; META_HEADER_SIZE];
        pread_exact(&file, &mut meta_count_buf, meta_header_offset)?;
        let meta_count = u64::from_le_bytes(meta_count_buf);

        // pg_state trailer follows the meta body.
        let trailer_offset =
            meta_header_offset + META_HEADER_SIZE as u64 + meta_count * META_ENTRY_SIZE as u64;
        let pg_state = read_pg_state(&file, trailer_offset)?;

        Ok(Manifest {
            checkpoint: Checkpoint::new(timeline_id, lsn),
            redo_ckpt: Checkpoint::new(redo_timeline_id, redo_lsn),
            pg_state,
            timestamp,
            path,
            file,
            entry_count,
            meta_count,
        })
    }

    /// Deserialize from the S3 wire format (`msgpack(...)`).
    /// Writes the decoded entries to a local TIKM file at `path`.
    ///
    /// Wire format: 6-tuple
    /// `(checkpoint, redo_ckpt, timestamp, chunks, meta_map, pg_state)`.
    pub fn from_bytes(data: &[u8], root_path: &Path) -> Result<Self> {
        let (checkpoint, redo_ckpt, timestamp, chunks, meta_map, pg_state): (
            Checkpoint,
            Checkpoint,
            i64,
            Vec<(ChunkTag, ChunkRef)>,
            HashMap<RelFork, RelForkMeta>,
            Vec<u8>,
        ) = rmp_serde::from_slice(data)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        Self::new(
            checkpoint, redo_ckpt, timestamp, chunks, meta_map, pg_state, root_path,
        )
    }

    /// Return the checkpoint recorded in the manifest header.
    pub fn checkpoint(&self) -> Checkpoint {
        self.checkpoint
    }

    /// Return the TIKM header timestamp (unix seconds) — the time of this base
    /// manifest's checkpoint.
    #[allow(dead_code)]
    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }

    /// Return the redo checkpoint — the LSN from which WAL replay must begin
    /// when this base manifest anchors a PITR recovery. (The recovering smgr
    /// reads the manifest's chunk refs; WAL replay bounds come from the
    /// `backup/` tarball's `backup_label`, so this accessor is currently
    /// unused at runtime but retained as part of the manifest API.)
    #[allow(dead_code)]
    pub fn redo_ckpt(&self) -> Checkpoint {
        self.redo_ckpt
    }

    /// Return the `pg_state.tar.zst` archive captured at the base checkpoint.
    /// Empty on a bootstrap/empty manifest.
    ///
    /// Currently unused: PITR bases now come from `pg_basebackup` tarballs
    /// (see `Store::put_backup`), so checkpoints no longer build the
    /// `pg_state` archive (the trailer is always empty). The field + trailer
    /// are retained to keep the TIKM wire format stable.
    #[allow(dead_code)]
    pub fn pg_state(&self) -> &[u8] {
        &self.pg_state
    }

    /// Binary search for `key` in the sorted on-disk TIKM file.
    /// Returns `Ok(Some(ChunkRef))` on hit, `Ok(None)` on miss.
    pub fn lookup(&self, key: &ChunkTag) -> Result<Option<ChunkRef>> {
        if self.entry_count == 0 {
            return Ok(None);
        }

        let mut lo: u64 = 0;
        let mut hi: u64 = self.entry_count;
        let mut buf = [0u8; 40];

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let offset = HEADER_SIZE as u64 + mid * ENTRY_SIZE as u64;
            pread_exact(&self.file, &mut buf, offset)?;

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

    /// Binary search for `rf` in the sorted on-disk meta section. Returns
    /// `Ok(Some(meta))` on hit, `Ok(None)` on miss. Symmetric with
    /// [`Self::lookup`] for chunks — no in-memory `HashMap`.
    pub(crate) fn lookup_relfork_meta(&self, rf: &RelFork) -> Result<Option<RelForkMeta>> {
        if self.meta_count == 0 {
            return Ok(None);
        }
        let body_offset = self.meta_body_offset();
        let mut lo: u64 = 0;
        let mut hi: u64 = self.meta_count;
        let mut buf = [0u8; META_ENTRY_SIZE];

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let offset = body_offset + mid * META_ENTRY_SIZE as u64;
            pread_exact(&self.file, &mut buf, offset)?;
            let parsed_rf = RelFork::decode(buf[0..REL_FORK_SIZE].try_into().unwrap());
            match parsed_rf.cmp(rf) {
                Ordering::Equal => {
                    let nblocks = u32::from_le_bytes(
                        buf[REL_FORK_SIZE..REL_FORK_SIZE + 4].try_into().unwrap(),
                    );
                    let deleted = buf[REL_FORK_SIZE + 4] != 0;
                    return Ok(Some(RelForkMeta { nblocks, deleted }));
                }
                Ordering::Less => lo = mid + 1,
                Ordering::Greater => hi = mid,
            }
        }
        Ok(None)
    }

    /// Apply a sequence of [`SegmentCheckpoint`] summaries (in
    /// **ascending checkpoint LSN order** — oldest first) onto `self`,
    /// updating the on-disk TIKM file in place via an atomic rename.
    ///
    /// - For each chunk in `segment.chunks`, the resulting `ChunkRef` points
    ///   to the same checkpoint prefix used at write time
    ///   (`segment.prev_ckpt`), matching the S3 layout consumed by
    ///   [`crate::io::locator::Locator::chunk_base`].
    /// - On conflict (same `ChunkTag` appears in multiple segments), the
    ///   higher-LSN `ChunkRef` wins.
    /// - `meta_map` entries are last-write-wins by iteration order; segments
    ///   are processed in the order given, so the newest segment's
    ///   `RelForkMeta` per relfork wins.
    /// - `checkpoint_lsn` and `timestamp` advance to the newest non-empty
    ///   segment-checkpoint's values.
    /// - An empty `segments` slice is a no-op.
    /// Compute the merged manifest from `segments` applied on top of `self`.
    /// Pure: does not touch the local TIKM file or `self`'s internal state.
    /// Returns the merged state + S3 wire bytes for the caller to publish.
    ///
    /// The caller is expected to (in order):
    /// 1. PUT [`AppliedManifest::bytes`] to S3.
    /// 2. Call [`Self::commit_applied`] to atomically rewrite the local TIKM
    ///    file and advance internal state.
    ///
    /// Splitting the work lets the caller interleave the S3 PUT without
    /// holding the manifest lock, and ensures the local file is never
    /// published ahead of S3.
    pub fn apply_segments(&self, segments: &[SegmentCheckpoint]) -> Result<AppliedManifest> {
        debug_assert!(
            segments.windows(2).all(|w| w[0].ckpt <= w[1].ckpt),
            "segments must be in ascending Checkpoint order"
        );

        // 1. Collect (tag, ChunkRef) entries and merge relfork meta updates
        //    from every segment. Track the highest segment ckpt to advance
        //    manifest metadata.
        //
        //    The highest segment is checkpoint P — the point compaction
        //    advances the base to. Carry its `redo_ckpt` and `pg_state` so the
        //    new base manifest is a self-contained PITR base backup. With no
        //    segments, keep `self`'s existing values.
        let (redo_ckpt, pg_state) = match segments.last() {
            Some(p) => (p.redo_ckpt, p.pg_state.clone()),
            None => (self.redo_ckpt, self.pg_state.clone()),
        };
        let mut combined: Vec<(ChunkTag, ChunkRef)> = Vec::new();
        let mut new_meta: HashMap<RelFork, RelForkMeta> = HashMap::new();
        let mut last_ckpt = self.checkpoint;
        let mut last_ts = self.timestamp;
        for seg in segments {
            if !seg.chunks.is_empty() || !seg.relforks.is_empty() {
                last_ckpt = seg.ckpt;
            }
            let cref = ChunkRef {
                db_id: 0,
                timeline_id: seg.prev_ckpt.timeline_id.as_u32(),
                lsn: seg.prev_ckpt.lsn,
            };
            for &tag in &seg.chunks {
                combined.push((tag, cref));
            }
            // Last-write-wins for relfork meta (segments oldest-first).
            for (rf, meta) in &seg.relforks {
                new_meta.insert(*rf, meta.clone());
            }
        }
        last_ts = chrono::Utc::now().timestamp().max(last_ts);

        // 2. Sort by (tag asc, lsn desc); dedup keeping the highest-LSN entry.
        combined.sort_unstable_by(|a, b| a.0.cmp(&b.0).then_with(|| b.1.lsn.cmp(&a.1.lsn)));
        combined.dedup_by_key(|(tag, _)| *tag);

        // 3. Two-pointer merge of existing base chunks + new combined.
        let base_entries = read_all_entries(self)?;
        let mut output: Vec<(ChunkTag, ChunkRef)> =
            Vec::with_capacity(base_entries.len() + combined.len());
        let mut bi = 0usize;
        let mut ci = 0usize;
        while bi < base_entries.len() && ci < combined.len() {
            match base_entries[bi].0.cmp(&combined[ci].0) {
                Ordering::Less => {
                    output.push(base_entries[bi]);
                    bi += 1;
                }
                Ordering::Greater => {
                    output.push(combined[ci]);
                    ci += 1;
                }
                Ordering::Equal => {
                    if combined[ci].1.lsn > base_entries[bi].1.lsn {
                        output.push(combined[ci]);
                    } else {
                        output.push(base_entries[bi]);
                    }
                    bi += 1;
                    ci += 1;
                }
            }
        }
        while bi < base_entries.len() {
            output.push(base_entries[bi]);
            bi += 1;
        }
        while ci < combined.len() {
            output.push(combined[ci]);
            ci += 1;
        }

        // 4. Merge existing on-disk meta with `new_meta` (new wins).
        let base_meta = read_all_meta_entries(self)?;
        let mut merged_meta: HashMap<RelFork, RelForkMeta> = base_meta.into_iter().collect();
        for (rf, meta) in new_meta {
            merged_meta.insert(rf, meta);
        }

        // 5. Drop chunks belonging to forks marked deleted in merged meta.
        let deleted: HashSet<RelFork> = merged_meta
            .iter()
            .filter(|(_, m)| m.deleted)
            .map(|(rf, _)| *rf)
            .collect();
        if !deleted.is_empty() {
            output.retain(|(tag, _)| !deleted.contains(&tag.relfork()));
        }

        // 6. Sort meta by RelFork (required by on-disk binary search).
        let mut meta_sorted: Vec<(RelFork, RelForkMeta)> = merged_meta.into_iter().collect();
        meta_sorted.sort_unstable_by_key(|(rf, _)| *rf);

        // 7. Compute the S3 wire bytes from the in-memory state.
        let meta_map: HashMap<RelFork, RelForkMeta> = meta_sorted.iter().cloned().collect();
        let bytes =
            rmp_serde::to_vec(&(last_ckpt, redo_ckpt, last_ts, &output, &meta_map, &pg_state))
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        Ok(AppliedManifest {
            checkpoint: last_ckpt,
            redo_ckpt,
            timestamp: last_ts,
            chunks: output,
            meta: meta_sorted,
            pg_state,
            bytes,
        })
    }

    /// Atomically publish `applied` to the local TIKM file (tmp + rename)
    /// and return a fresh `Manifest` reflecting the new checkpoint. Call
    /// after [`Self::apply_segments`] and the caller's external publish
    /// (e.g. S3 PUT) have both succeeded — this is the step that makes the
    /// new state visible to other backends via [`Self::open_local`].
    ///
    /// `self` is unchanged. The caller (typically `Store`) swaps the
    /// returned `Manifest` in via `Arc` replacement; existing `Arc<Manifest>`
    /// holders keep reading the old state through their FD until they drop
    /// their `Arc`.
    pub fn commit_applied(&self, applied: AppliedManifest) -> Result<Self> {
        let file = write_tikm(
            &self.path,
            applied.checkpoint,
            applied.redo_ckpt,
            applied.timestamp,
            &applied.chunks,
            &applied.meta,
            &applied.pg_state,
        )?;
        Ok(Manifest {
            checkpoint: applied.checkpoint,
            redo_ckpt: applied.redo_ckpt,
            pg_state: applied.pg_state,
            timestamp: applied.timestamp,
            path: self.path.clone(),
            file,
            entry_count: applied.chunks.len() as u64,
            meta_count: applied.meta.len() as u64,
        })
    }
}

/// Result of [`Manifest::apply_segments`] — the merged state plus the S3
/// wire bytes, ready for the caller to publish externally before committing
/// via [`Manifest::commit_applied`].
pub(crate) struct AppliedManifest {
    pub checkpoint: Checkpoint,
    pub redo_ckpt: Checkpoint,
    pub timestamp: i64,
    pub chunks: Vec<(ChunkTag, ChunkRef)>,
    pub meta: Vec<(RelFork, RelForkMeta)>,
    pub pg_state: Vec<u8>,
    /// msgpack-encoded
    /// `(checkpoint, redo_ckpt, timestamp, chunks, meta_map, pg_state)` ready
    /// to PUT to S3 at the base-manifest key for `checkpoint`.
    pub bytes: Vec<u8>,
}

// ── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::timeline::Checkpoint;
    use pgsys::common::ForkNumber;
    use pgsys::timeline_id::TimelineId;
    use tempfile::tempdir;

    impl Manifest {
        /// Serialize to the S3 wire format (`msgpack(...)`).
        ///
        /// Format: 6-tuple
        /// `(checkpoint, redo_ckpt, timestamp, chunks, meta_map, pg_state)`.
        pub fn to_bytes(&self) -> io::Result<Vec<u8>> {
            let entries = read_all_entries(self)?;
            let meta_entries = read_all_meta_entries(self)?;
            let meta_map: HashMap<RelFork, RelForkMeta> = meta_entries.into_iter().collect();
            rmp_serde::to_vec(&(
                self.checkpoint,
                self.redo_ckpt,
                self.timestamp,
                &entries,
                &meta_map,
                &self.pg_state,
            ))
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
        }
    }

    fn tag(rel: u32, chunk_id: u32) -> ChunkTag {
        ChunkTag {
            spc_oid: 1,
            db_oid: 1,
            rel_number: rel,
            fork_number: 0 as ForkNumber,
            chunk_id,
        }
    }

    fn rf(rel: u32) -> RelFork {
        RelFork {
            spc_oid: 1,
            db_oid: 1,
            rel_number: rel,
            fork_number: 0 as ForkNumber,
        }
    }

    fn ckpt(lsn: u64) -> Checkpoint {
        Checkpoint::new(TimelineId::new(1), Lsn::new(lsn))
    }

    fn segment(
        ckpt_lsn: u64,
        prev_lsn: u64,
        tags: &[ChunkTag],
        rels: &[(RelFork, RelForkMeta)],
    ) -> SegmentCheckpoint {
        let mut s = SegmentCheckpoint::new(
            ckpt(ckpt_lsn),
            ckpt(prev_lsn),
            Checkpoint::default(),
            HashSet::new(),
            HashMap::new(),
            &vec![1, 2, 3, 4],
        );
        for t in tags {
            s.chunks.insert(*t);
        }
        for (rf, m) in rels {
            s.relforks.insert(*rf, m.clone());
        }
        s
    }

    #[test]
    fn apply_segments_merges_chunks_and_relforks() {
        let dir = tempdir().unwrap();
        let base = Manifest::empty(dir.path()).unwrap();

        let s1 = segment(
            100,
            0,
            &[tag(1, 0), tag(1, 1)],
            &[(rf(1), RelForkMeta::new(32, false))],
        );
        let s2 = segment(
            200,
            100,
            &[tag(1, 1), tag(2, 0)],
            &[
                (rf(1), RelForkMeta::new(48, false)),
                (rf(2), RelForkMeta::new(8, false)),
            ],
        );

        let applied = base.apply_segments(&[s1, s2]).unwrap();
        let base = base.commit_applied(applied).unwrap();

        // Chunk (1,0) only in s1 → prev_ckpt=0 → ChunkRef.lsn = 0.
        // Chunk (1,1) in both s1 and s2 → higher LSN wins → prev_ckpt=100.
        // Chunk (2,0) only in s2 → prev_ckpt=100.
        let r10 = base.lookup(&tag(1, 0)).unwrap().unwrap();
        assert_eq!(r10.lsn.as_u64(), 0);
        let r11 = base.lookup(&tag(1, 1)).unwrap().unwrap();
        assert_eq!(r11.lsn.as_u64(), 100);
        let r20 = base.lookup(&tag(2, 0)).unwrap().unwrap();
        assert_eq!(r20.lsn.as_u64(), 100);

        // Relfork meta: rf(1) overwritten by s2 → 48 blocks; rf(2) only in s2.
        assert_eq!(
            base.lookup_relfork_meta(&rf(1)).unwrap().unwrap().nblocks,
            48
        );
        assert_eq!(
            base.lookup_relfork_meta(&rf(2)).unwrap().unwrap().nblocks,
            8
        );

        // checkpoint_lsn advances to the newest applied segment.
        assert_eq!(base.checkpoint().lsn.as_u64(), 200);
    }

    #[test]
    fn apply_segments_deleted_relfork_drops_chunks() {
        let dir = tempdir().unwrap();
        let base = Manifest::empty(dir.path()).unwrap();

        let s1 = segment(
            100,
            0,
            &[tag(1, 0), tag(2, 0)],
            &[(rf(1), RelForkMeta::new(32, false))],
        );
        let s2 = segment(
            200,
            100,
            &[],
            &[(rf(1), RelForkMeta::new(0, true))], // relfork rf(1) dropped
        );
        let applied = base.apply_segments(&[s1, s2]).unwrap();
        let base = base.commit_applied(applied).unwrap();

        // Chunks for the deleted relfork were purged.
        assert!(base.lookup(&tag(1, 0)).unwrap().is_none());
        // Chunks for surviving relforks remain.
        assert!(base.lookup(&tag(2, 0)).unwrap().is_some());

        let m = base.lookup_relfork_meta(&rf(1)).unwrap().unwrap();
        assert!(m.deleted);
    }

    #[test]
    fn apply_segments_roundtrip_via_bytes() {
        let dir = tempdir().unwrap();
        let base = Manifest::empty(dir.path()).unwrap();

        let s = segment(
            100,
            0,
            &[tag(1, 0)],
            &[(rf(1), RelForkMeta::new(32, false))],
        );
        let applied = base.apply_segments(&[s]).unwrap();
        let base = base.commit_applied(applied).unwrap();

        let bytes = base.to_bytes().unwrap();
        let dir2 = tempdir().unwrap();
        let restored = Manifest::from_bytes(&bytes, dir2.path()).unwrap();

        assert_eq!(restored.checkpoint().lsn.as_u64(), 100);
        assert!(restored.lookup(&tag(1, 0)).unwrap().is_some());
        assert_eq!(
            restored
                .lookup_relfork_meta(&rf(1))
                .unwrap()
                .unwrap()
                .nblocks,
            32
        );
    }

    #[test]
    fn apply_segments_carries_redo_ckpt_and_pg_state_from_highest() {
        let dir = tempdir().unwrap();
        let base = Manifest::empty(dir.path()).unwrap();

        // Two segments with distinct redo_ckpt / pg_state. The highest
        // (checkpoint P = s2) is the one the base must inherit.
        let mut s1 = SegmentCheckpoint::new(
            ckpt(100),
            ckpt(0),
            ckpt(90),
            HashSet::new(),
            HashMap::new(),
            &[0xAA, 0xBB],
        );
        s1.chunks.insert(tag(1, 0));
        let mut s2 = SegmentCheckpoint::new(
            ckpt(200),
            ckpt(100),
            ckpt(190),
            HashSet::new(),
            HashMap::new(),
            &[1, 2, 3, 4, 5],
        );
        s2.chunks.insert(tag(2, 0));

        let applied = base.apply_segments(&[s1, s2]).unwrap();
        let base = base.commit_applied(applied).unwrap();

        // Inherited from the highest segment (s2 = checkpoint P).
        assert_eq!(base.redo_ckpt().lsn.as_u64(), 190);
        assert_eq!(base.pg_state(), &[1, 2, 3, 4, 5]);

        // Survives the S3 wire roundtrip.
        let bytes = base.to_bytes().unwrap();
        let dir2 = tempdir().unwrap();
        let restored = Manifest::from_bytes(&bytes, dir2.path()).unwrap();
        assert_eq!(restored.redo_ckpt().lsn.as_u64(), 190);
        assert_eq!(restored.pg_state(), &[1, 2, 3, 4, 5]);

        // Survives open_local (reads header + trailer from the TIKM file).
        let reopened = Manifest::open_local(dir2.path()).unwrap();
        assert_eq!(reopened.redo_ckpt().lsn.as_u64(), 190);
        assert_eq!(reopened.pg_state(), &[1, 2, 3, 4, 5]);
    }
}
