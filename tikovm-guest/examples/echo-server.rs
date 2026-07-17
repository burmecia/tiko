//! Minimal HTTP echo server — the demo "workload" baked into the echo rootfs.
//!
//! Listens on `--port` (default 8080) and answers any request with a short JSON
//! echo. `GET /health` returns 200. Intentionally dependency-free (stdlib only)
//! so it builds as a tiny static binary suitable for a minimal rootfs.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::thread;

fn main() {
    let port: u16 = std::env::args()
        .position(|a| a == "--port")
        .and_then(|i| std::env::args().nth(i + 1))
        .and_then(|v| v.parse().ok())
        .unwrap_or(8080);
    let addr = format!("0.0.0.0:{port}");
    let listener = TcpListener::bind(&addr).expect("bind");
    eprintln!("echo-server listening on {addr}");
    for stream in listener.incoming() {
        let Ok(mut stream) = stream else { continue };
        thread::spawn(move || {
            let mut buf = [0u8; 4096];
            let n = match stream.read(&mut buf) {
                Ok(n) if n > 0 => n,
                _ => return,
            };
            let req = String::from_utf8_lossy(&buf[..n]);
            let first_line = req.lines().next().unwrap_or("");
            let path = first_line.split_whitespace().nth(1).unwrap_or("/");
            let (status, body) = if path.starts_with("/health") {
                (200, "{\"status\":\"ok\"}".to_string())
            } else {
                (
                    200,
                    format!("{{\"echo\":true,\"path\":\"{path}\",\"method\":\"{first_line}\"}}"),
                )
            };
            let reason = if status == 200 { "OK" } else { "Error" };
            let resp = format!(
                "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(resp.as_bytes());
            let _ = stream.flush();
        });
    }
}
