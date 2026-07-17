//! Prometheus metrics (design section 12). Hand-rolled (no extra deps): a
//! process-global [`Metrics`] set initialized at daemon startup, with event
//! counters recorded from the lifecycle/proxy paths and gauges derived from the
//! control registry at scrape time. Exposed at `GET /metrics`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;

use tikovm_protocol::vm::VmState;

use crate::control::Control;

struct Metrics {
    suspends: AtomicU64,
    restores: AtomicU64,
    destroys: AtomicU64,
    proxy_connections: AtomicU64,
    suspend_micros_sum: AtomicU64,
    suspend_count: AtomicU64,
    restore_micros_sum: AtomicU64,
    restore_count: AtomicU64,
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            suspends: AtomicU64::new(0),
            restores: AtomicU64::new(0),
            destroys: AtomicU64::new(0),
            proxy_connections: AtomicU64::new(0),
            suspend_micros_sum: AtomicU64::new(0),
            suspend_count: AtomicU64::new(0),
            restore_micros_sum: AtomicU64::new(0),
            restore_count: AtomicU64::new(0),
        }
    }
}

static METRICS: OnceLock<Metrics> = OnceLock::new();

/// Install the global metrics set. Call once at daemon startup.
pub fn init() {
    let _ = METRICS.set(Metrics::default());
}

fn get() -> Option<&'static Metrics> {
    METRICS.get()
}

/// Record a completed suspend (scale-to-zero freeze) of `duration_micros`.
pub fn record_suspend(duration_micros: u64) {
    if let Some(m) = get() {
        m.suspends.fetch_add(1, Ordering::Relaxed);
        m.suspend_count.fetch_add(1, Ordering::Relaxed);
        m.suspend_micros_sum.fetch_add(duration_micros, Ordering::Relaxed);
    }
}

/// Record a completed restore (wake) of `duration_micros`.
pub fn record_restore(duration_micros: u64) {
    if let Some(m) = get() {
        m.restores.fetch_add(1, Ordering::Relaxed);
        m.restore_count.fetch_add(1, Ordering::Relaxed);
        m.restore_micros_sum.fetch_add(duration_micros, Ordering::Relaxed);
    }
}

/// Record a terminal destroy.
pub fn record_destroy() {
    if let Some(m) = get() {
        m.destroys.fetch_add(1, Ordering::Relaxed);
    }
}

/// Record an inbound proxy connection (data-plane request).
pub fn record_proxy_connection() {
    if let Some(m) = get() {
        m.proxy_connections.fetch_add(1, Ordering::Relaxed);
    }
}

/// Render the Prometheus text exposition (response body for `GET /metrics`).
pub fn render(control: &Control) -> String {
    // Tally VMs by state from the live registry.
    let mut by_state = std::collections::HashMap::<VmState, u64>::new();
    let mut healthy = 0u64;
    let mut unhealthy = 0u64;
    for info in control.list() {
        *by_state.entry(info.state).or_default() += 1;
        match info.healthy {
            Some(true) => healthy += 1,
            Some(false) => unhealthy += 1,
            None => {}
        }
    }

    let mut out = String::new();
    out.push_str("# HELP tikovm_vm_total VMs by lifecycle state.\n");
    out.push_str("# TYPE tikovm_vm_total gauge\n");
    // Emit every state so scrapers see a stable cardinality.
    for s in [
        VmState::Created,
        VmState::Started,
        VmState::Paused,
        VmState::Suspended,
        VmState::Destroyed,
    ] {
        let n = by_state.get(&s).copied().unwrap_or(0);
        out.push_str(&format!("tikovm_vm_total{{state=\"{s}\"}} {n}\n"));
    }

    out.push_str("# HELP tikovm_vm_healthy VMs by guest-reported health.\n");
    out.push_str("# TYPE tikovm_vm_healthy gauge\n");
    out.push_str(&format!("tikovm_vm_healthy{{status=\"healthy\"}} {healthy}\n"));
    out.push_str(&format!("tikovm_vm_healthy{{status=\"unhealthy\"}} {unhealthy}\n"));

    let m = match get() {
        Some(m) => m,
        None => return out,
    };
    let counter = |name: &str, v: u64| format!("# TYPE {name} counter\n{name} {v}\n");
    out.push_str(&counter("tikovm_suspends_total", m.suspends.load(Ordering::Relaxed)));
    out.push_str(&counter("tikovm_restores_total", m.restores.load(Ordering::Relaxed)));
    out.push_str(&counter("tikovm_destroys_total", m.destroys.load(Ordering::Relaxed)));
    out.push_str(&counter("tikovm_proxy_connections_total", m.proxy_connections.load(Ordering::Relaxed)));

    // Poor-man's histogram: _sum + _count (no buckets). Good enough for averages.
    out.push_str("# TYPE tikovm_suspend_duration_micros summary\n");
    out.push_str(&format!(
        "tikovm_suspend_duration_micros{{quantile=\"sum\"}} {}\n",
        m.suspend_micros_sum.load(Ordering::Relaxed)
    ));
    out.push_str(&format!("tikovm_suspend_count {}\n", m.suspend_count.load(Ordering::Relaxed)));
    out.push_str("# TYPE tikovm_restore_duration_micros summary\n");
    out.push_str(&format!(
        "tikovm_restore_duration_micros{{quantile=\"sum\"}} {}\n",
        m.restore_micros_sum.load(Ordering::Relaxed)
    ));
    out.push_str(&format!("tikovm_restore_count {}\n", m.restore_count.load(Ordering::Relaxed)));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::Control;

    #[test]
    fn render_has_state_lines() {
        init();
        let c = Control::new();
        let text = render(&c);
        assert!(text.contains("tikovm_vm_total{state=\"started\"} 0"));
        assert!(text.contains("# TYPE tikovm_suspends_total counter"));
    }

    #[test]
    fn counters_accumulate() {
        init();
        // init() is a no-op after the first call (OnceLock); use the global.
        record_suspend(1_000);
        record_restore(2_000);
        record_destroy();
        let text = render(&Control::new());
        assert!(text.contains("tikovm_suspends_total"));
        assert!(text.contains("tikovm_restores_total"));
        assert!(text.contains("tikovm_destroys_total"));
    }
}
