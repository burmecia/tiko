//! Prometheus-style metrics: global daemon counters (atomics) plus a
//! per-volume snapshot rendered by `control` at `GET /metrics`.
//!
//! Hand-rolled, no dependencies. Counters are `Relaxed` atomics — precise
//! ordering does not matter for gauges/counters scraped at second scale.

use std::sync::atomic::{AtomicU64, Ordering};

/// Guest FLUSH calls completed (durability boundary hits).
pub static FLUSHES_TOTAL: AtomicU64 = AtomicU64::new(0);
/// Chunk files written to the store (flusher + drain).
pub static CHUNKS_WRITTEN_TOTAL: AtomicU64 = AtomicU64::new(0);
/// Chunk files read from the store (cache misses).
pub static CHUNKS_READ_TOTAL: AtomicU64 = AtomicU64::new(0);
/// Read-cache hits.
pub static CACHE_HITS_TOTAL: AtomicU64 = AtomicU64::new(0);
/// Read-cache misses.
pub static CACHE_MISSES_TOTAL: AtomicU64 = AtomicU64::new(0);
/// Bytes reclaimed by GC passes.
pub static GC_RECLAIMED_BYTES_TOTAL: AtomicU64 = AtomicU64::new(0);
/// Journal records re-applied by startup replay.
pub static JOURNAL_REPLAYS_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Increment a counter by one.
pub fn inc(c: &'static AtomicU64) {
    c.fetch_add(1, Ordering::Relaxed);
}

/// Add to a counter.
pub fn add(c: &'static AtomicU64, n: u64) {
    c.fetch_add(n, Ordering::Relaxed);
}

fn get(c: &'static AtomicU64) -> u64 {
    c.load(Ordering::Relaxed)
}

/// Render the daemon counters in Prometheus text exposition format.
pub fn render_counters(out: &mut String) {
    let rows: [(&str, &str, &AtomicU64); 7] = [
        ("tikoblk_flushes_total", "Guest FLUSH completions", &FLUSHES_TOTAL),
        ("tikoblk_chunks_written_total", "Chunk files written to the store", &CHUNKS_WRITTEN_TOTAL),
        ("tikoblk_chunks_read_total", "Chunk files read from the store", &CHUNKS_READ_TOTAL),
        ("tikoblk_cache_hits_total", "Read-cache hits", &CACHE_HITS_TOTAL),
        ("tikoblk_cache_misses_total", "Read-cache misses", &CACHE_MISSES_TOTAL),
        ("tikoblk_gc_reclaimed_bytes_total", "Bytes reclaimed by GC", &GC_RECLAIMED_BYTES_TOTAL),
        ("tikoblk_journal_replays_total", "Journal records re-applied on open", &JOURNAL_REPLAYS_TOTAL),
    ];
    for (name, help, val) in rows {
        out.push_str(&format!(
            "# HELP {name} {help}\n# TYPE {name} counter\n{name} {}\n",
            get(val)
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_render_prometheus_format() {
        add(&GC_RECLAIMED_BYTES_TOTAL, 4096);
        let mut s = String::new();
        render_counters(&mut s);
        assert!(s.contains("# TYPE tikoblk_flushes_total counter"));
        // Values are cumulative across the test process; check >= what we added.
        let gc_line = s
            .lines()
            .find(|l| l.starts_with("tikoblk_gc_reclaimed_bytes_total "))
            .unwrap();
        let v: u64 = gc_line.rsplit(' ').next().unwrap().parse().unwrap();
        assert!(v >= 4096);
        // Every counter row is `name value` on its own line.
        for line in s.lines().filter(|l| !l.starts_with('#')) {
            let mut parts = line.split_whitespace();
            assert!(parts.next().unwrap().starts_with("tikoblk_"));
            assert!(parts.next().unwrap().parse::<u64>().is_ok());
            assert!(parts.next().is_none());
        }
    }
}
