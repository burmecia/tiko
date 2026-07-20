//! Minimal HTTP/1.1-over-Unix-socket control API, mirroring the style of
//! `tikovm-host/src/api/server.rs` but synchronous (no tokio — libublk
//! manages its own threads; one std thread per connection is plenty).
//!
//! Routes:
//! - `GET    /health`                        -> 200 `{"ok":true}`
//! - `POST   /gc`                            -> 200 `{scanned,reclaimed_count,reclaimed_bytes,...}`
//! - `POST   /volumes`                       -> 201 `{"vol_id":...}`
//!   (`{"vol_id","size_mb"?,"backend":"file"|"chunk"?,"chunk_size_kib":n?,`
//!   `"from_snapshot":"<src>/<snap>"?}`)
//! - `GET    /volumes`                       -> 200 `[{meta}...]`
//! - `GET    /volumes/{id}`                  -> 200 `{meta + backend/generation/epoch/has_data/stats/snapshots}`
//! - `POST   /volumes/{id}/attach`           -> 200 `{"device":"/dev/ublkbN","formatted":bool}`
//! - `POST   /volumes/{id}/detach`           -> 200 `{"ok":true}`
//! - `DELETE /volumes/{id}`                  -> 200 `{"ok":true}` (409 while snapshots exist)
//! - `POST   /volumes/{id}/snapshots`        -> 201 `{"snap_id":...}` (`{"name"?}`)
//! - `GET    /volumes/{id}/snapshots`        -> 200 `{"snapshots":[...]}`
//! - `DELETE /volumes/{id}/snapshots/{snap}` -> 200 `{"ok":true}`

use std::io::{Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use serde::Serialize;

use crate::volume::VolumeManager;
use crate::Error;

/// A simple JSON HTTP response.
#[derive(Debug, Clone)]
pub struct Response {
    /// HTTP status code.
    pub status: u16,
    /// Response body.
    pub body: Vec<u8>,
    /// Content-Type header value.
    pub content_type: &'static str,
}

impl Response {
    /// Serialize `val` as a JSON response with `status`.
    pub fn json<T: Serialize>(status: u16, val: &T) -> Self {
        Self {
            status,
            body: serde_json::to_vec(val).unwrap_or_else(|_| b"null".to_vec()),
            content_type: "application/json",
        }
    }

    /// JSON error envelope `{"error":{"kind","message"}}`.
    pub fn error(status: u16, kind: &str, message: impl Into<String>) -> Self {
        Self::json(
            status,
            &serde_json::json!({"error": {"kind": kind, "message": message.into()}}),
        )
    }

    /// 404 helper.
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::error(404, "not_found", message)
    }

    /// 400 helper.
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::error(400, "bad_request", message)
    }

    /// 409 helper.
    pub fn conflict(message: impl Into<String>) -> Self {
        Self::error(409, "conflict", message)
    }
}

#[derive(Debug, serde::Deserialize)]
struct CreateReq {
    vol_id: String,
    /// Volume size in MiB; optional (and must match) when cloning.
    size_mb: Option<u64>,
    /// "file" (default) or "chunk".
    backend: Option<String>,
    /// Chunk size in KiB for the chunk backend (256..=4096, power of two;
    /// default 1024).
    chunk_size_kib: Option<u32>,
    /// Zero-copy clone source: "<src_vol_id>/<snap_id>".
    from_snapshot: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
struct SnapshotReq {
    name: Option<String>,
}

/// Pure dispatch (testable without sockets).
pub fn dispatch(mgr: &VolumeManager, method: &str, path: &str, body: &[u8]) -> Response {
    let segs: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    match segs.as_slice() {
        ["health"] if method == "GET" => Response::json(200, &serde_json::json!({"ok": true})),
        ["metrics"] if method == "GET" => Response {
            status: 200,
            body: render_metrics(mgr).into_bytes(),
            content_type: "text/plain; version=0.0.4",
        },
        ["gc"] if method == "POST" => match mgr.gc_run() {
            Ok(stats) => Response::json(200, &stats),
            Err(e) => err_from(e),
        },
        ["volumes"] if method == "GET" => Response::json(200, &mgr.list()),
        ["volumes"] if method == "POST" => match serde_json::from_slice::<CreateReq>(body) {
            Ok(req) => {
                let size_bytes = req.size_mb.unwrap_or(0).saturating_mul(1 << 20);
                match create_opts(&req) {
                    Ok(opts) => match mgr.create(&req.vol_id, size_bytes, opts) {
                        Ok(meta) => {
                            Response::json(201, &serde_json::json!({"vol_id": meta.vol_id}))
                        }
                        Err(e) => err_from(e),
                    },
                    Err(e) => err_from(e),
                }
            }
            Err(e) => Response::bad_request(format!("invalid create body: {e}")),
        },
        ["volumes", id] if method == "GET" => match mgr.volume_detail(id) {
            Ok(detail) => Response::json(200, &detail),
            Err(e) => err_from(e),
        },
        ["volumes", id] if method == "DELETE" => match mgr.delete(id) {
            Ok(()) => Response::json(200, &serde_json::json!({"ok": true})),
            Err(e) => err_from(e),
        },
        ["volumes", id, "attach"] if method == "POST" => match mgr.attach(id) {
            Ok(info) => Response::json(
                200,
                &serde_json::json!({"device": info.device, "formatted": info.formatted}),
            ),
            Err(e) => err_from(e),
        },
        ["volumes", id, "detach"] if method == "POST" => match mgr.detach(id) {
            Ok(()) => Response::json(200, &serde_json::json!({"ok": true})),
            Err(e) => err_from(e),
        },
        ["volumes", id, "snapshots"] if method == "GET" => match mgr.list_snapshots(id) {
            Ok(snaps) => Response::json(200, &serde_json::json!({"snapshots": snaps})),
            Err(e) => err_from(e),
        },
        ["volumes", id, "snapshots"] if method == "POST" => {
            let name = match serde_json::from_slice::<SnapshotReq>(body) {
                Ok(r) => r.name,
                Err(e) => return Response::bad_request(format!("invalid snapshot body: {e}")),
            };
            match mgr.snapshot(id, name.as_deref()) {
                Ok(snap_id) => Response::json(201, &serde_json::json!({"snap_id": snap_id})),
                Err(e) => err_from(e),
            }
        }
        ["volumes", id, "snapshots", snap] if method == "DELETE" => {
            match mgr.delete_snapshot(id, snap) {
                Ok(()) => Response::json(200, &serde_json::json!({"ok": true})),
                Err(e) => err_from(e),
            }
        }
        _ => Response::not_found(format!("no route for {method} {path}")),
    }
}

fn create_opts(req: &CreateReq) -> crate::Result<crate::volume::CreateOpts> {
    use crate::registry::BackendKind;
    let backend = match req.backend.as_deref() {
        None | Some("file") => BackendKind::File,
        Some("chunk") => BackendKind::Chunk,
        Some(other) => {
            return Err(Error::InvalidInput(format!(
                "unknown backend {other:?} (want \"file\" or \"chunk\")"
            )));
        }
    };
    let chunk_size = req.chunk_size_kib.unwrap_or(1024) << 10;
    let from_snapshot = match &req.from_snapshot {
        Some(s) => {
            let (vol, snap) = s.split_once('/').ok_or_else(|| {
                Error::InvalidInput(format!(
                    "from_snapshot must be \"<src_vol_id>/<snap_id>\", got {s:?}"
                ))
            })?;
            Some((vol.to_string(), snap.to_string()))
        }
        None => None,
    };
    if from_snapshot.is_none() && req.size_mb.is_none() {
        return Err(Error::InvalidInput("size_mb is required".into()));
    }
    Ok(crate::volume::CreateOpts {
        backend,
        chunk_size,
        from_snapshot,
    })
}

fn err_from(e: Error) -> Response {
    match e {
        Error::NotFound(_) => Response::not_found(e.to_string()),
        Error::AlreadyExists(_) | Error::Busy(_) | Error::InvalidState(_) => {
            Response::conflict(e.to_string())
        }
        Error::InvalidInput(_) => Response::bad_request(e.to_string()),
        Error::InsufficientSpace { .. } => Response::error(507, "insufficient_space", e.to_string()),
        Error::Timeout(_) => Response::error(504, "timeout", e.to_string()),
        _ => Response::error(500, "internal", e.to_string()),
    }
}

/// Prometheus text exposition: per-volume gauges + daemon counters.
fn render_metrics(mgr: &VolumeManager) -> String {
    type Gauge = (&'static str, &'static str, fn(&crate::volume::VolMetrics) -> u64);
    let mut out = String::new();
    let rows = mgr.metrics_snapshot();
    let gauges: [Gauge; 5] = [
        ("tikoblk_volume_size_bytes", "Volume size in bytes", |v| v.size_bytes),
        ("tikoblk_volume_dirty_bytes", "Dirty-buffer bytes", |v| v.dirty_bytes),
        ("tikoblk_volume_journal_bytes", "NVMe journal bytes", |v| v.journal_bytes),
        ("tikoblk_volume_epoch", "Map epoch (lease counter)", |v| v.epoch),
        ("tikoblk_volume_attached", "Volume served by this daemon", |v| v.attached as u64),
    ];
    for (name, help, f) in gauges {
        out.push_str(&format!("# HELP {name} {help}\n# TYPE {name} gauge\n"));
        for v in &rows {
            let backend = match v.backend {
                crate::registry::BackendKind::File => "file",
                crate::registry::BackendKind::Chunk => "chunk",
            };
            out.push_str(&format!(
                "{name}{{vol_id=\"{}\",backend=\"{backend}\"}} {}\n",
                v.vol_id,
                f(v)
            ));
        }
    }
    crate::metrics::render_counters(&mut out);
    out
}

/// Run the accept loop until `shutdown` is set. The listener is
/// nonblocking with a short poll sleep, so shutdown is noticed within
/// ~50 ms regardless of which thread the signal handler ran on or whether
/// the socket path still belongs to us.
pub fn serve(listener: UnixListener, mgr: Arc<VolumeManager>, shutdown: Arc<AtomicBool>) {
    if let Err(e) = listener.set_nonblocking(true) {
        tracing::error!(error = %e, "set_nonblocking failed");
        return;
    }
    loop {
        if shutdown.load(Ordering::SeqCst) {
            break;
        }
        match listener.accept() {
            Ok((stream, _)) => {
                let mgr = mgr.clone();
                std::thread::spawn(move || {
                    if let Err(e) = handle_conn(stream, &mgr) {
                        tracing::debug!(error = %e, "connection closed");
                    }
                });
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => {
                tracing::warn!(error = %e, "accept failed");
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
        }
    }
}

fn handle_conn(mut stream: UnixStream, mgr: &VolumeManager) -> std::io::Result<()> {
    let mut buf = vec![0u8; 65536];
    let n = stream.read(&mut buf)?;
    if n == 0 {
        return Ok(());
    }
    let req = String::from_utf8_lossy(&buf[..n]);
    let Some((method, path, body_start)) = parse_request_line(&req) else {
        return write_response(&mut stream, Response::error(400, "bad_request", "bad request"));
    };
    // For simplicity we trust Content-Length when present; otherwise use the
    // remainder of the first read. Sufficient for JSON control calls.
    let body = extract_body(&req, body_start, &buf[..n]);
    let resp = dispatch(mgr, &method, &path, &body);
    write_response(&mut stream, resp)
}

// ---- minimal HTTP/1.1 helpers -------------------------------------------

fn parse_request_line(req: &str) -> Option<(String, String, usize)> {
    let line = req.lines().next()?;
    let mut parts = line.split_whitespace();
    let method = parts.next()?.to_string();
    let path = parts.next()?.to_string();
    // Header section ends at "\r\n\r\n"
    let body_start = req.find("\r\n\r\n").map(|i| i + 4)?;
    Some((method, path, body_start))
}

fn extract_body(req: &str, body_start: usize, raw: &[u8]) -> Vec<u8> {
    // Honor Content-Length when present.
    let len = req.lines().take_while(|l| !l.is_empty()).find_map(|line| {
        let (k, v) = line.split_once(':')?;
        (k.eq_ignore_ascii_case("content-length")).then(|| v.trim().parse::<usize>().ok())?
    });
    match len {
        Some(len) => {
            let start = body_start.min(raw.len());
            let end = (start + len).min(raw.len());
            raw[start..end].to_vec()
        }
        None => raw.get(body_start..).map(|s| s.to_vec()).unwrap_or_default(),
    }
}

fn write_response(stream: &mut UnixStream, resp: Response) -> std::io::Result<()> {
    let reason = match resp.status {
        200 => "OK",
        201 => "Created",
        400 => "Bad Request",
        404 => "Not Found",
        409 => "Conflict",
        500 => "Internal Server Error",
        504 => "Gateway Timeout",
        507 => "Insufficient Storage",
        _ => "OK",
    };
    let head = format!(
        "HTTP/1.1 {0} {reason}\r\nContent-Type: {1}\r\nContent-Length: {2}\r\nConnection: close\r\n\r\n",
        resp.status,
        resp.content_type,
        resp.body.len()
    );
    stream.write_all(head.as_bytes())?;
    stream.write_all(&resp.body)?;
    stream.flush()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::VolumeState;

    fn mgr_at(tag: &str) -> (std::path::PathBuf, VolumeManager) {
        let dir = std::env::temp_dir().join(format!("tikoblk-ctl-{tag}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let store = dir.join("store");
        let opts = crate::volume::ManagerOpts {
            cache_bytes: 8 << 20,
            gc_interval_secs: 0,
            ..Default::default()
        };
        let mgr = VolumeManager::new(&dir, &store, &opts).unwrap();
        (dir, mgr)
    }

    #[test]
    fn parse_request_line_and_body() {
        let raw = b"POST /volumes HTTP/1.1\r\nHost: x\r\nContent-Length: 26\r\n\r\n{\"vol_id\":\"a\",\"size_mb\":4}extra";
        let req = String::from_utf8_lossy(raw);
        let (m, p, start) = parse_request_line(&req).unwrap();
        assert_eq!((m.as_str(), p.as_str()), ("POST", "/volumes"));
        let body = extract_body(&req, start, raw);
        assert_eq!(body, b"{\"vol_id\":\"a\",\"size_mb\":4}");

        // No Content-Length: remainder of the read is the body.
        let raw = b"POST /volumes HTTP/1.1\r\n\r\n{}";
        let req = String::from_utf8_lossy(raw);
        let (_, _, start) = parse_request_line(&req).unwrap();
        assert_eq!(extract_body(&req, start, raw), b"{}");

        assert!(parse_request_line("garbage").is_none());
    }

    #[test]
    fn dispatch_routes() {
        let (dir, mgr) = mgr_at("routes");

        let r = dispatch(&mgr, "GET", "/health", &[]);
        assert_eq!(r.status, 200);
        assert!(r.body.windows(9).any(|w| w == b"\"ok\":true"));

        // Bad JSON -> 400.
        let r = dispatch(&mgr, "POST", "/volumes", b"not json");
        assert_eq!(r.status, 400);

        // Create -> 201; duplicate -> 409; list/get reflect it.
        let r = dispatch(&mgr, "POST", "/volumes", br#"{"vol_id":"x","size_mb":8}"#);
        assert_eq!(r.status, 201);
        let r = dispatch(&mgr, "POST", "/volumes", br#"{"vol_id":"x","size_mb":8}"#);
        assert_eq!(r.status, 409);
        let r = dispatch(&mgr, "GET", "/volumes", &[]);
        assert_eq!(r.status, 200);
        let r = dispatch(&mgr, "GET", "/volumes/x", &[]);
        assert_eq!(r.status, 200);
        let v: serde_json::Value = serde_json::from_slice(&r.body).unwrap();
        assert_eq!(v["state"], "created");
        assert_eq!(v["dev_id"], 1);
        assert_eq!(v["size_bytes"], 8 * (1 << 20));

        // Chunk-backend create + detail reporting.
        let r = dispatch(
            &mgr,
            "POST",
            "/volumes",
            br#"{"vol_id":"c","size_mb":8,"backend":"chunk","chunk_size_kib":1024}"#,
        );
        assert_eq!(r.status, 201);
        let r = dispatch(&mgr, "GET", "/volumes/c", &[]);
        assert_eq!(r.status, 200);
        let v: serde_json::Value = serde_json::from_slice(&r.body).unwrap();
        assert_eq!(v["backend"], "chunk");
        assert_eq!(v["chunk_size"], 1 << 20);
        assert_eq!(v["generation"], 0);
        // Unknown backend name -> 400.
        let r = dispatch(&mgr, "POST", "/volumes", br#"{"vol_id":"z","size_mb":1,"backend":"warp"}"#);
        assert_eq!(r.status, 400);
        dispatch(&mgr, "DELETE", "/volumes/c", &[]);

        // Unknown volume -> 404; unknown route -> 404.
        let r = dispatch(&mgr, "GET", "/volumes/nope", &[]);
        assert_eq!(r.status, 404);
        let r = dispatch(&mgr, "GET", "/bogus", &[]);
        assert_eq!(r.status, 404);

        // Detach (no-op, not attached) and delete -> 200.
        let r = dispatch(&mgr, "POST", "/volumes/x/detach", &[]);
        assert_eq!(r.status, 200);
        let r = dispatch(&mgr, "DELETE", "/volumes/x", &[]);
        assert_eq!(r.status, 200);
        let r = dispatch(&mgr, "DELETE", "/volumes/x", &[]);
        assert_eq!(r.status, 404);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn err_mapping() {
        let r = err_from(Error::InsufficientSpace { need: 10, have: 5 });
        assert_eq!(r.status, 507);
        let r = err_from(Error::Busy("v".into()));
        assert_eq!(r.status, 409);
    }

    #[test]
    fn metrics_endpoint_format() {
        let (dir, mgr) = mgr_at("metrics");
        dispatch(&mgr, "POST", "/volumes", br#"{"vol_id":"m1","size_mb":8}"#);
        dispatch(
            &mgr,
            "POST",
            "/volumes",
            br#"{"vol_id":"m2","size_mb":8,"backend":"chunk","chunk_size_kib":1024}"#,
        );
        crate::metrics::inc(&crate::metrics::FLUSHES_TOTAL);

        let r = dispatch(&mgr, "GET", "/metrics", &[]);
        assert_eq!(r.status, 200);
        assert_eq!(r.content_type, "text/plain; version=0.0.4");
        let body = String::from_utf8(r.body).unwrap();
        assert!(body.contains("tikoblk_volume_size_bytes{vol_id=\"m1\",backend=\"file\"} 8388608"));
        assert!(body.contains("tikoblk_volume_size_bytes{vol_id=\"m2\",backend=\"chunk\"} 8388608"));
        assert!(body.contains("tikoblk_volume_attached{vol_id=\"m2\",backend=\"chunk\"} 0"));
        assert!(body.contains("tikoblk_volume_journal_bytes{vol_id=\"m2\",backend=\"chunk\"} 0"));
        // Counters are cumulative across the test process; check >= what we added.
        let fl = body
            .lines()
            .find(|l| l.starts_with("tikoblk_flushes_total "))
            .unwrap();
        let v: u64 = fl.rsplit(' ').next().unwrap().parse().unwrap();
        assert!(v >= 1);
        // Valid exposition: non-comment lines are `name{labels} value` or `name value`.
        for line in body.lines().filter(|l| !l.starts_with('#') && !l.is_empty()) {
            let val = line.split_whitespace().next_back().unwrap();
            assert!(val.parse::<u64>().is_ok(), "bad metric line: {line}");
        }
        dispatch(&mgr, "DELETE", "/volumes/m1", &[]);
        dispatch(&mgr, "DELETE", "/volumes/m2", &[]);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn volume_state_serde_is_lowercase() {
        let s = serde_json::to_string(&VolumeState::Attached).unwrap();
        assert_eq!(s, "\"attached\"");
    }
}
