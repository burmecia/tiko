//! Host-local (data-dir, fast NVMe) cache tier: read cache + write journal.
//!
//! **Read cache** — `<data_dir>/cache/<chunk_id_hex>`: whole decompressed
//! chunks keyed by content id. Chunks are immutable, so cache entries are
//! never invalidated, only evicted by an in-memory size-capped LRU
//! (atime tracked in memory, nothing persisted — a cold cache after
//! restart is fine).
//!
//! **Write journal** — `<data_dir>/journal/<vol_id>/<seq>.j`: append-only
//! segment files of length-prefixed records:
//!
//! ```text
//! {chunk_idx u64, flags u8 (bit0 = zstd), payload_len u32, crc32 u32, payload}
//! ```
//!
//! crc32 covers the header fields and payload. This is the FLUSH
//! durability layer: `flush()` appends all dirty chunk payloads to the
//! current segment with one sequential write + fsync. On volume open the
//! segments are replayed into the dirty buffer (idempotent: whole-chunk
//! images, last writer wins). Replay stops at the first short/corrupt
//! record — the torn-tail crash case.

use std::collections::HashMap;
use std::io;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::chunkstore::{ChunkId, id_hex};
use crate::crc32::crc32;

// ---------------------------------------------------------------- read cache

struct CacheEntry {
    size: u64,
    atime: u64,
}

/// Size-capped LRU read cache for whole (decompressed) chunks.
pub struct ReadCache {
    dir: PathBuf,
    cap_bytes: u64,
    entries: Mutex<HashMap<String, CacheEntry>>,
    bytes: Mutex<u64>,
    tick: AtomicU64,
}

impl ReadCache {
    /// Create the cache under `dir` with the given capacity in bytes.
    pub fn new(dir: &Path, cap_bytes: u64) -> io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        Ok(Self {
            dir: dir.to_path_buf(),
            cap_bytes,
            entries: Mutex::new(HashMap::new()),
            bytes: Mutex::new(0),
            tick: AtomicU64::new(0),
        })
    }

    fn path(&self, id: &ChunkId) -> PathBuf {
        self.dir.join(id_hex(id))
    }

    /// Fetch a chunk from the cache (bumping its LRU atime).
    pub fn get(&self, id: &ChunkId) -> Option<Vec<u8>> {
        let hex = id_hex(id);
        if !self.entries.lock().unwrap().contains_key(&hex) {
            crate::metrics::inc(&crate::metrics::CACHE_MISSES_TOTAL);
            return None;
        }
        match std::fs::read(self.path(id)) {
            Ok(data) => {
                crate::metrics::inc(&crate::metrics::CACHE_HITS_TOTAL);
                if let Some(e) = self.entries.lock().unwrap().get_mut(&hex) {
                    e.atime = self.tick.fetch_add(1, Ordering::Relaxed);
                }
                Some(data)
            }
            // Entry/file mismatch (e.g. evicted concurrently): treat as miss.
            Err(_) => {
                crate::metrics::inc(&crate::metrics::CACHE_MISSES_TOTAL);
                None
            }
        }
    }

    /// Insert a chunk, evicting least-recently-used entries until the new
    /// total fits the cap. Chunks larger than the cap are not cached.
    pub fn insert(&self, id: &ChunkId, data: &[u8]) {
        let size = data.len() as u64;
        if size > self.cap_bytes {
            return;
        }
        let hex = id_hex(id);
        // Write the file first (tmp+rename so a concurrent get never reads
        // a torn entry), then account for it.
        let tmp = self.dir.join(format!("{hex}.tmp"));
        if std::fs::write(&tmp, data)
            .and_then(|_| std::fs::rename(&tmp, self.path(id)))
            .is_err()
        {
            let _ = std::fs::remove_file(&tmp);
            return;
        }
        let atime = self.tick.fetch_add(1, Ordering::Relaxed);
        {
            let mut entries = self.entries.lock().unwrap();
            let mut bytes = self.bytes.lock().unwrap();
            if let Some(old) = entries.insert(hex.clone(), CacheEntry { size, atime }) {
                *bytes -= old.size;
            }
            *bytes += size;
            // Evict LRU until within cap. (Entry count is small: cap/chunk.)
            while *bytes > self.cap_bytes {
                let Some((victim_hex, _)) = entries
                    .iter()
                    .filter(|(h, _)| **h != hex)
                    .min_by_key(|(_, e)| e.atime)
                    .map(|(h, e)| (h.clone(), e.atime))
                else {
                    break;
                };
                let victim = entries.remove(&victim_hex).expect("lru victim");
                *bytes -= victim.size;
                let _ = std::fs::remove_file(self.dir.join(&victim_hex));
            }
        }
    }

    /// Drop specific ids from the cache (volume delete).
    pub fn remove_ids(&self, ids: &[ChunkId]) {
        let mut entries = self.entries.lock().unwrap();
        let mut bytes = self.bytes.lock().unwrap();
        for id in ids {
            let hex = id_hex(id);
            if let Some(e) = entries.remove(&hex) {
                *bytes -= e.size;
                let _ = std::fs::remove_file(self.dir.join(&hex));
            }
        }
    }

    /// (entries, bytes, cap_bytes) for stats reporting.
    pub fn stats(&self) -> (usize, u64, u64) {
        (
            self.entries.lock().unwrap().len(),
            *self.bytes.lock().unwrap(),
            self.cap_bytes,
        )
    }
}

// ------------------------------------------------------------ write journal

/// One journal record's payload, ready to append.
pub struct JournalRecord {
    /// Volume chunk index.
    pub chunk_idx: u64,
    /// Bit 0: payload is zstd-compressed.
    pub flags: u8,
    /// Payload (compressed when flagged).
    pub payload: Vec<u8>,
}

/// Total bytes held by a volume's NVMe journal segments (metrics).
pub fn journal_bytes(jdir: &Path) -> u64 {
    list_segments(jdir)
        .map(|segs| {
            segs.iter()
                .filter_map(|(_, p)| std::fs::metadata(p).ok())
                .map(|m| m.len())
                .sum()
        })
        .unwrap_or(0)
}

/// Journal segment file name for a sequence number.
pub fn segment_name(seq: u64) -> String {
    format!("{seq:020}.j")
}

/// List journal segments in `jdir` as (seq, path), sorted by seq.
pub fn list_segments(jdir: &Path) -> io::Result<Vec<(u64, PathBuf)>> {

    let mut out = Vec::new();
    match std::fs::read_dir(jdir) {
        Ok(it) => {
            for e in it.flatten() {
                let name = e.file_name();
                let name = name.to_string_lossy();
                if let Some(stem) = name.strip_suffix(".j")
                    && let Ok(seq) = stem.parse::<u64>()
                {
                    out.push((seq, e.path()));
                }
            }
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    out.sort_by_key(|(seq, _)| *seq);
    Ok(out)
}

/// Append `records` to segment `seq` with ONE sequential write, then fsync
/// the file and the journal dir. This is the guest-FLUSH durability
/// boundary: after Ok(()) the payloads survive a daemon/host crash.
pub fn append_segment(jdir: &Path, seq: u64, records: &[JournalRecord]) -> io::Result<()> {
    if records.is_empty() {
        return Ok(());
    }
    std::fs::create_dir_all(jdir)?;
    let path = jdir.join(segment_name(seq));
    let mut buf = Vec::new();
    for r in records {
        let mut head = Vec::with_capacity(17);
        head.extend_from_slice(&r.chunk_idx.to_le_bytes());
        head.push(r.flags);
        head.extend_from_slice(&(r.payload.len() as u32).to_le_bytes());
        let crc = crc32(&[&head[..], &r.payload[..]].concat());
        buf.extend_from_slice(&head);
        buf.extend_from_slice(&crc.to_le_bytes());
        buf.extend_from_slice(&r.payload);
    }
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)?;
    f.write_all(&buf)?;
    f.sync_all()?;
    drop(f);
    crate::chunkstore::sync_dir(jdir)
}

/// Replay one segment file, invoking `apply` per valid record. Stops
/// silently at the first short or CRC-failing record (torn tail). Returns
/// the number of records applied.
pub fn replay_segment<F: FnMut(u64, Vec<u8>)>(path: &Path, mut apply: F) -> io::Result<usize> {
    let mut f = std::fs::File::open(path)?;
    let mut applied = 0;
    loop {
        let mut head = [0u8; 17]; // idx u64 + flags u8 + len u32 + crc u32
        if !read_exact_or_eof(&mut f, &mut head)? {
            break; // clean EOF
        }
        let idx = u64::from_le_bytes(head[..8].try_into().unwrap());
        let flags = head[8];
        let len = u32::from_le_bytes(head[9..13].try_into().unwrap()) as usize;
        let crc = u32::from_le_bytes(head[13..17].try_into().unwrap());
        if len > (64 << 20) {
            break; // implausible length: treat as torn/corrupt tail
        }
        let mut payload = vec![0u8; len];
        if !read_exact_or_eof(&mut f, &mut payload)? {
            break; // torn payload
        }
        let mut check = Vec::with_capacity(13);
        check.extend_from_slice(&idx.to_le_bytes());
        check.push(flags);
        check.extend_from_slice(&(len as u32).to_le_bytes());
        if crc32(&[&check[..], &payload[..]].concat()) != crc {
            break; // corrupt record: drop it and the rest of the tail
        }
        let data = if flags & 1 != 0 {
            zstd::decode_all(&payload[..])?
        } else {
            payload
        };
        apply(idx, data);
        applied += 1;
    }
    Ok(applied)
}

/// Read exactly buf.len() or report clean EOF at a record boundary.
/// Returns Ok(false) on EOF-before-first-byte or mid-record (caller
/// treats mid-record EOF as torn tail).
fn read_exact_or_eof(f: &mut std::fs::File, buf: &mut [u8]) -> io::Result<bool> {
    let mut n = 0;
    while n < buf.len() {
        match f.read(&mut buf[n..]) {
            Ok(0) => return Ok(false),
            Ok(k) => n += k,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunkstore::new_chunk_id;

    fn dir_at(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("tikoblk-cache-{tag}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn read_cache_lru_eviction() {
        let dir = dir_at("lru");
        let cap = 3 << 20; // 3 MiB
        let cache = ReadCache::new(&dir, cap).unwrap();
        let a = new_chunk_id().unwrap();
        let b = new_chunk_id().unwrap();
        let c = new_chunk_id().unwrap();
        let chunk = vec![7u8; 1 << 20];

        cache.insert(&a, &chunk);
        cache.insert(&b, &chunk);
        assert_eq!(cache.get(&a).unwrap(), chunk); // touch a: b is now LRU
        cache.insert(&c, &chunk); // 3 MiB total, at cap
        assert_eq!(cache.stats().0, 3);

        cache.insert(&new_chunk_id().unwrap(), &chunk); // forces eviction of b
        assert!(cache.get(&b).is_none(), "LRU victim evicted");
        assert!(cache.get(&a).is_some());
        assert!(cache.get(&c).is_some());
        assert!(cache.stats().1 <= cap);

        // Oversized entries are simply not cached.
        cache.insert(&new_chunk_id().unwrap(), &vec![0u8; (cap + 1) as usize]);
        assert!(cache.stats().1 <= cap);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn journal_roundtrip_replay_and_torn_tail() {
        let dir = dir_at("journal");
        let jd = dir.join("v");
        let recs = vec![
            JournalRecord { chunk_idx: 5, flags: 0, payload: b"raw-data".to_vec() },
            JournalRecord {
                chunk_idx: 9,
                flags: 1,
                payload: zstd::bulk::compress(&vec![1u8; 4096], 3).unwrap(),
            },
        ];
        append_segment(&jd, 1, &recs).unwrap();

        let mut got: Vec<(u64, Vec<u8>)> = Vec::new();
        let n = replay_segment(&jd.join(segment_name(1)), |i, d| got.push((i, d))).unwrap();
        assert_eq!(n, 2);
        assert_eq!(got[0], (5, b"raw-data".to_vec()));
        assert_eq!(got[1], (9, vec![1u8; 4096]));

        // Corrupt the second record's crc: replay applies the first only.
        let p = jd.join(segment_name(1));
        let mut buf = std::fs::read(&p).unwrap();
        // First record is 17 + 8 = 25 bytes; second record's crc starts at
        // 25 + 13 = 38.
        buf[38] ^= 0xFF;
        std::fs::write(&p, &buf).unwrap();
        let mut got2: Vec<(u64, Vec<u8>)> = Vec::new();
        let n2 = replay_segment(&p, |i, d| got2.push((i, d))).unwrap();
        assert_eq!(n2, 1, "corrupt record and its tail are dropped");
        assert_eq!(got2[0].0, 5);

        // Torn tail (truncate mid-record): first record still applies.
        let mut buf3 = std::fs::read(jd.join(segment_name(1))).unwrap();
        buf3.truncate(30);
        std::fs::write(jd.join(segment_name(2)), &buf3).unwrap();
        let n3 = replay_segment(&jd.join(segment_name(2)), |_, _| {}).unwrap();
        assert_eq!(n3, 1);

        // Appending to the same segment preserves earlier records.
        append_segment(&jd, 3, &[JournalRecord { chunk_idx: 1, flags: 0, payload: b"a".to_vec() }]).unwrap();
        append_segment(&jd, 3, &[JournalRecord { chunk_idx: 2, flags: 0, payload: b"b".to_vec() }]).unwrap();
        let mut cnt = 0;
        replay_segment(&jd.join(segment_name(3)), |_, _| cnt += 1).unwrap();
        assert_eq!(cnt, 2);
        std::fs::remove_dir_all(&dir).ok();
    }
}
