use std::sync::atomic::{AtomicU64, Ordering};

#[repr(C)]
pub struct CacheStats {
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    pub evictions: AtomicU64,
    pub dirty_evictions: AtomicU64,
}

impl CacheStats {
    fn init(&self) {
        self.hits.store(0, Ordering::Relaxed);
        self.misses.store(0, Ordering::Relaxed);
        self.evictions.store(0, Ordering::Relaxed);
        self.dirty_evictions.store(0, Ordering::Relaxed);
    }

    fn summary(&self) -> String {
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let total = hits + misses;
        let hit_rate = if total > 0 {
            hits as f64 / total as f64 * 100.0
        } else {
            0.0
        };
        format!(
            "h={} m={} hr={:.1}% e={} de={}",
            hits,
            misses,
            hit_rate,
            self.evictions.load(Ordering::Relaxed),
            self.dirty_evictions.load(Ordering::Relaxed),
        )
    }

    pub(crate) fn inc_hits(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn inc_misses(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn inc_evictions(&self) {
        self.evictions.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn inc_dirty_evictions(&self) {
        self.dirty_evictions.fetch_add(1, Ordering::Relaxed);
    }
}

#[repr(C)]
pub struct BucketStats {
    pub gets: AtomicU64,
    pub puts: AtomicU64,
    pub lists: AtomicU64,
    pub get_bytes: AtomicU64,
    pub put_bytes: AtomicU64,
}

impl BucketStats {
    fn init(&self) {
        self.gets.store(0, Ordering::Relaxed);
        self.puts.store(0, Ordering::Relaxed);
        self.lists.store(0, Ordering::Relaxed);
        self.get_bytes.store(0, Ordering::Relaxed);
        self.put_bytes.store(0, Ordering::Relaxed);
    }

    fn summary(&self) -> String {
        format!(
            "r={} w={} l={} rb={} wb={}",
            self.gets.load(Ordering::Relaxed),
            self.puts.load(Ordering::Relaxed),
            self.lists.load(Ordering::Relaxed),
            self.get_bytes.load(Ordering::Relaxed),
            self.put_bytes.load(Ordering::Relaxed),
        )
    }

    pub(crate) fn inc_gets(&self, bytes: usize) {
        self.gets.fetch_add(1, Ordering::Relaxed);
        self.get_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub(crate) fn inc_puts(&self, bytes: usize) {
        self.puts.fetch_add(1, Ordering::Relaxed);
        self.put_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub(crate) fn inc_lists(&self) {
        self.lists.fetch_add(1, Ordering::Relaxed);
    }
}

#[repr(C)]
pub struct IoStats {
    pub chunk_cache: CacheStats,
    pub meta_cache: CacheStats,
    pub storage: BucketStats,
}

impl IoStats {
    pub(super) fn init(&self) {
        self.chunk_cache.init();
        self.meta_cache.init();
        self.storage.init();
    }

    /// Log a summary of cache performance stats.
    pub fn log_summary(&self) {
        pgsys::logging::pg_log_debug1(&format!(
            "tiko io stats: chunk_cache=({}) meta_cache=({}) storage=({})",
            self.chunk_cache.summary(),
            self.meta_cache.summary(),
            self.storage.summary(),
        ));
    }
}
