//! Postgres metrics collection via `tokio-postgres`.
//!
//! [`PgMetrics`] manages a connection to the local Postgres instance and
//! collects the metrics used by the observer loop (periodic push to tikod) and
//! the scaler loop (snapshot-eligibility evaluation).
//!
//! The agent runs as the `postgres` user (superuser) inside the guest, so
//! `pg_current_wal_lsn()` and `pg_stat_activity` are accessible without
//! additional grants. Trust auth is set up during `initdb --auth=trust`.
//!
//! Each [`collect`](PgMetrics::collect) call opens a fresh connection — simple
//! and naturally reconnects after a PG restart. The poll interval is 30s, so
//! the per-connect overhead is negligible.

use serde::Serialize;
use tokio_postgres::NoTls;
use tracing::{debug, warn};

/// Metrics collected from Postgres. Serialized and pushed to tikod by the
/// observer loop. When PG is unreachable, `available` is `false` and the rest
/// is zeroed — the observer still sends the report so tikod knows the agent is
/// alive but PG is down.
#[derive(Debug, Clone, Serialize)]
pub struct Metrics {
    /// Whether the collection succeeded. `false` when PG is down or a query
    /// failed.
    pub available: bool,
    /// Total backends connected to the `tt` database (`pg_stat_activity`).
    pub connections: i64,
    /// Backends currently executing a query (`state='active'`).
    pub active_backends: i64,
    /// Backends with a transaction started > 60s ago.
    pub long_running_tx: i64,
    /// `pg_database_size('tt')` in bytes.
    pub db_size_bytes: i64,
    /// `blks_hit / (blks_hit + blks_read)` — `None` when no reads yet.
    pub cache_hit_ratio: Option<f64>,
    /// Current WAL LSN as text (e.g. `"0/3000000"`). Tikod computes the rate
    /// from consecutive reports. `None` when unavailable.
    pub wal_lsn: Option<String>,
}

impl Metrics {
    /// Sent when PG is unreachable — zeros + `available: false`.
    pub fn unavailable() -> Self {
        Self {
            available: false,
            connections: 0,
            active_backends: 0,
            long_running_tx: 0,
            db_size_bytes: 0,
            cache_hit_ratio: None,
            wal_lsn: None,
        }
    }

    /// True when PG is up and the last collection succeeded.
    pub fn is_available(&self) -> bool {
        self.available
    }
}

/// Configuration for [`PgMetrics`].
#[derive(Clone, Debug)]
pub struct PgMetricsConfig {
    /// libpq-style connection string. Defaults to the unix socket as the
    /// `postgres` user (the agent runs as postgres), database `tt`.
    pub connection_string: String,
    /// Database name used in `pg_stat_activity` / `pg_database_size` filters.
    pub db_name: String,
}

impl Default for PgMetricsConfig {
    fn default() -> Self {
        Self {
            connection_string: "host=/var/run/postgresql user=postgres dbname=tt".into(),
            db_name: "tt".into(),
        }
    }
}

/// A Postgres metrics collector. Stateless between calls — each
/// [`collect`](Self::collect) opens a fresh connection.
pub struct PgMetrics {
    config: PgMetricsConfig,
}

impl PgMetrics {
    pub fn new(config: PgMetricsConfig) -> Self {
        Self { config }
    }

    pub fn config(&self) -> &PgMetricsConfig {
        &self.config
    }

    /// Collect a snapshot of Postgres metrics. If the connection or any query
    /// fails, returns [`Metrics::unavailable`] — never panics, never errors.
    pub async fn collect(&self) -> Metrics {
        match tokio_postgres::connect(&self.config.connection_string, NoTls).await {
            Ok((client, connection)) => {
                // The connection task drives the async protocol; dropped when
                // the client is dropped.
                tokio::spawn(async move {
                    if let Err(e) = connection.await {
                        warn!(error = %e, "postgres metrics connection closed");
                    }
                });
                self.query_metrics(&client).await
            }
            Err(e) => {
                debug!(error = %e, "failed to connect to postgres for metrics");
                Metrics::unavailable()
            }
        }
    }

    /// Run the combined metrics query. A single round-trip keeps latency low.
    async fn query_metrics(&self, client: &tokio_postgres::Client) -> Metrics {
        // One query: activity counts, db size, cache ratio, and WAL LSN.
        // Subqueries keep it to a single row / single round-trip. `$1` is the
        // db_name bind parameter (not a format! placeholder).
        let sql = "\
            SELECT \
              (SELECT count(*) FROM pg_stat_activity WHERE datname = $1) AS connections, \
              (SELECT count(*) FROM pg_stat_activity WHERE state = 'active') AS active_backends, \
              (SELECT count(*) FROM pg_stat_activity WHERE xact_start IS NOT NULL \
                AND xact_start < now() - interval '60 seconds') AS long_running_tx, \
              pg_database_size($1) AS db_size_bytes, \
              (SELECT sum(blks_hit)::float8 / nullif(sum(blks_hit) + sum(blks_read), 0) \
                FROM pg_stat_database WHERE datname = $1) AS cache_hit_ratio, \
              pg_current_wal_lsn()::text AS wal_lsn";

        match client.query_one(sql, &[&self.config.db_name]).await {
            Ok(row) => {
                let cache_hit_ratio: Option<f64> = row
                    .try_get("cache_hit_ratio")
                    .unwrap_or(None);
                let wal_lsn: Option<String> = row
                    .try_get("wal_lsn")
                    .unwrap_or(None);
                Metrics {
                    available: true,
                    connections: row.get("connections"),
                    active_backends: row.get("active_backends"),
                    long_running_tx: row.get("long_running_tx"),
                    db_size_bytes: row.get("db_size_bytes"),
                    cache_hit_ratio,
                    wal_lsn,
                }
            }
            Err(e) => {
                debug!(error = %e, "metrics query failed");
                Metrics::unavailable()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unavailable_is_marked_so() {
        let m = Metrics::unavailable();
        assert!(!m.is_available());
        assert_eq!(m.connections, 0);
        assert_eq!(m.active_backends, 0);
        assert_eq!(m.long_running_tx, 0);
        assert_eq!(m.db_size_bytes, 0);
        assert!(m.cache_hit_ratio.is_none());
        assert!(m.wal_lsn.is_none());
    }

    #[test]
    fn metrics_serialize() {
        let m = Metrics {
            available: true,
            connections: 3,
            active_backends: 1,
            long_running_tx: 0,
            db_size_bytes: 1048576,
            cache_hit_ratio: Some(0.99),
            wal_lsn: Some("0/3000000".into()),
        };
        let json = serde_json::to_string(&m).unwrap();
        assert!(json.contains("\"available\":true"));
        assert!(json.contains("\"connections\":3"));
        assert!(json.contains("\"wal_lsn\":\"0/3000000\""));
        assert!(json.contains("\"cache_hit_ratio\":0.99"));
    }

    #[test]
    fn unavailable_serializes_with_false() {
        let m = Metrics::unavailable();
        let json = serde_json::to_string(&m).unwrap();
        assert!(json.contains("\"available\":false"));
        assert!(json.contains("\"cache_hit_ratio\":null"));
        assert!(json.contains("\"wal_lsn\":null"));
    }

    #[test]
    fn config_defaults() {
        let c = PgMetricsConfig::default();
        assert!(c.connection_string.contains("dbname=tt"));
        assert_eq!(c.db_name, "tt");
    }
}
