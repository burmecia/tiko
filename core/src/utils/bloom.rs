use crate::chunk::ChunkTag;

/// Per-active-checkpoint Bloom filter size in bytes. 16 KiB = 128 Ki bits.
/// At ~12 K dirty chunks per checkpoint and 7 hash functions, false-positive
/// rate is ~1 %. With K = 64 active slots, total Bloom footprint is ~1 MiB.
pub const CHUNK_BLOOM_BYTES: usize = 16 * 1024;
const CHUNK_BLOOM_BITS: u32 = (CHUNK_BLOOM_BYTES * 8) as u32;
const CHUNK_BLOOM_HASHES: u32 = 7;

/// Fixed-size Bloom filter living in shared memory. Stores the set of
/// [`ChunkTag`]s present in one active-window checkpoint.
///
/// False positives fall through to an on-disk segment lookup, so they only
/// affect read-path cost on a rare miss path, not correctness.
#[repr(C)]
pub struct ChunkBloom {
    bits: [u8; CHUNK_BLOOM_BYTES],
}

impl ChunkBloom {
    pub fn clear(&mut self) {
        self.bits.fill(0);
    }

    pub fn insert(&mut self, tag: &ChunkTag) {
        let (h1, h2) = double_hash(tag);
        for i in 0..CHUNK_BLOOM_HASHES {
            let bit = combined_hash(h1, h2, i) % CHUNK_BLOOM_BITS;
            self.bits[(bit / 8) as usize] |= 1u8 << (bit % 8);
        }
    }

    pub fn maybe_contains(&self, tag: &ChunkTag) -> bool {
        let (h1, h2) = double_hash(tag);
        for i in 0..CHUNK_BLOOM_HASHES {
            let bit = combined_hash(h1, h2, i) % CHUNK_BLOOM_BITS;
            if self.bits[(bit / 8) as usize] & (1u8 << (bit % 8)) == 0 {
                return false;
            }
        }
        true
    }
}

#[inline]
fn double_hash(tag: &ChunkTag) -> (u32, u32) {
    // FNV-1a from ChunkTag, mixed two ways to get two independent hashes
    // for the double-hashing Bloom scheme (Kirsch & Mitzenmacher).
    let h1 = tag.hash();
    let h2 = (h1 ^ 0x9E37_79B9_u32).wrapping_mul(0x85EB_CA6B_u32);
    (h1, h2)
}

#[inline]
fn combined_hash(h1: u32, h2: u32, i: u32) -> u32 {
    h1.wrapping_add(i.wrapping_mul(h2))
        .wrapping_add(i.wrapping_mul(i))
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgsys::common::ForkNumber;

    fn tag(rel: u32, chunk_id: u32) -> ChunkTag {
        ChunkTag {
            spc_oid: 1,
            db_oid: 1,
            rel_number: rel,
            fork_number: 0 as ForkNumber,
            chunk_id,
        }
    }

    fn new_bloom() -> Box<ChunkBloom> {
        // Heap-allocate zeroed bytes and reinterpret to avoid stack-allocating
        // a 16 KB array.
        let v = vec![0u8; CHUNK_BLOOM_BYTES].into_boxed_slice();
        let raw = Box::into_raw(v) as *mut ChunkBloom;
        unsafe { Box::from_raw(raw) }
    }

    #[test]
    fn bloom_empty_contains_nothing() {
        let b = new_bloom();
        assert!(!b.maybe_contains(&tag(1, 0)));
        assert!(!b.maybe_contains(&tag(99, 99)));
    }

    #[test]
    fn bloom_no_false_negatives() {
        let mut b = new_bloom();
        let mut inserted = Vec::new();
        for r in 0..50u32 {
            for c in 0..10u32 {
                let t = tag(r, c);
                b.insert(&t);
                inserted.push(t);
            }
        }
        for t in &inserted {
            assert!(
                b.maybe_contains(t),
                "false negative for {:?}: Bloom must report membership for everything inserted",
                t
            );
        }
    }

    #[test]
    fn bloom_false_positive_rate_is_reasonable() {
        // Insert N items, then probe M items that were NOT inserted.
        // With CHUNK_BLOOM_BITS=128Ki and 7 hashes, optimal load is around
        // 12700 items @ 1% FP. We test well below that capacity.
        let mut b = new_bloom();
        const INSERTED: u32 = 1_000;
        const PROBED: u32 = 10_000;
        for i in 0..INSERTED {
            b.insert(&tag(0, i));
        }
        let mut fp = 0u32;
        for i in INSERTED..(INSERTED + PROBED) {
            if b.maybe_contains(&tag(0, i)) {
                fp += 1;
            }
        }
        // At ~1k items / 128k bits / 7 hashes, FP rate is well under 0.1%.
        // Allow generous headroom for hash-quality variation.
        assert!(
            fp < PROBED / 100,
            "false-positive rate too high: {}/{} ({:.2}%)",
            fp,
            PROBED,
            fp as f64 * 100.0 / PROBED as f64
        );
    }

    #[test]
    fn bloom_clear_resets_state() {
        let mut b = new_bloom();
        let t = tag(7, 7);
        b.insert(&t);
        assert!(b.maybe_contains(&t));
        b.clear();
        assert!(!b.maybe_contains(&t));
    }
}
