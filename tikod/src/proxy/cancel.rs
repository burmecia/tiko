//! Cancel-request routing for multi-VM correctness.
//!
//! When a PostgreSQL client cancels a running query (e.g. `psql` Ctrl-C),
//! libpq opens a **new** TCP connection and sends a `CancelRequest` carrying
//! the `{pid, secret}` of the target backend connection (obtained from the
//! `BackendKeyData` ('K') message the backend emitted during startup).
//!
//! A multi-VM proxy must route that cancel to the backend that owns the PID —
//! PIDs are only unique per-VM. This module:
//!
//! 1. Records `{(pid, secret) → backend_addr}` by intercepting the
//!    `BackendKeyData` message on the backend→client byte stream during the
//!    handshake (until `ReadyForQuery`).
//! 2. Forwards a reconstructed `CancelRequest` to the recorded backend when a
//!    cancel connection arrives.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use dashmap::DashMap;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::debug;

/// Magic number for the `CancelRequest` packet (first word after the length).
const CANCEL_REQUEST: u32 = 80877102;

/// `(pid, secret)` → backend address, so cancel requests route to the owning VM.
#[derive(Debug, Default)]
pub struct CancelTable {
    inner: DashMap<(u32, u32), SocketAddr>,
}

impl CancelTable {
    pub fn new() -> Self {
        Self {
            inner: DashMap::new(),
        }
    }

    /// Wrap in `Arc` for sharing across connection tasks.
    pub fn shared() -> Arc<Self> {
        Arc::new(Self::new())
    }

    /// Record that `pid`/`secret` lives on `addr`.
    pub fn insert(&self, pid: u32, secret: u32, addr: SocketAddr) {
        self.inner.insert((pid, secret), addr);
    }

    /// Look up the backend owning `pid`/`secret`.
    pub fn get(&self, pid: u32, secret: u32) -> Option<SocketAddr> {
        self.inner.get(&(pid, secret)).map(|r| *r.value())
    }

    /// Drop a mapping (e.g. when the owning connection closes).
    pub fn remove(&self, pid: u32, secret: u32) {
        self.inner.remove(&(pid, secret));
    }
}

/// Copy framed backend messages to the client until (and including) the first
/// `ReadyForQuery` ('Z'), recording any `BackendKeyData` ('K') seen in
/// `cancel_table`. Returns once the handshake phase is over; the caller then
/// continues with a plain `io::copy` for the remainder of the connection.
///
/// On success returns the `(pid, secret)` of the intercepted `BackendKeyData`,
/// if any, so the caller can [`CancelTable::remove`] it when the connection
/// closes (bounding the table to active connections).
///
/// This bounds protocol-awareness to the handshake: after `ReadyForQuery`, the
/// proxy is a blind byte pipe.
pub async fn copy_until_ready<R, W>(
    backend_read: &mut R,
    client_write: &mut W,
    backend_addr: SocketAddr,
    cancel_table: &CancelTable,
) -> io::Result<Option<(u32, u32)>>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut captured: Option<(u32, u32)> = None;
    loop {
        // Header: type byte (1) + length (4, includes itself).
        let mut header = [0u8; 5];
        backend_read.read_exact(&mut header).await?;
        let ty = header[0];
        let len = u32::from_be_bytes(header[1..5].try_into().unwrap()) as usize;
        if len < 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("backend message length too small: {len}"),
            ));
        }
        let body_len = len - 4;
        let mut body = vec![0u8; body_len];
        backend_read.read_exact(&mut body).await?;

        // Intercept BackendKeyData ('K'): body is pid(4) + secret(4).
        if ty == b'K' && body_len == 8 {
            let pid = u32::from_be_bytes(body[0..4].try_into().unwrap());
            let secret = u32::from_be_bytes(body[4..8].try_into().unwrap());
            cancel_table.insert(pid, secret, backend_addr);
            captured = Some((pid, secret));
            debug!(pid, secret, %backend_addr, "registered backend key for cancel routing");
        }

        // Forward the message verbatim.
        client_write.write_all(&header).await?;
        client_write.write_all(&body).await?;

        if ty == b'Z' {
            // ReadyForQuery — handshake complete. Flush and hand off to plain copy.
            client_write.flush().await?;
            return Ok(captured);
        }
    }
}

/// Forward a cancel for `pid`/`secret` to the backend that owns it (if known).
/// Cancel connections are write-only: send the packet and close. Returns
/// `Ok(())` even if no backend is known (the cancel is silently dropped, same
/// as PostgreSQL's behaviour for a stale cancel key).
pub async fn forward_cancel(
    pid: u32,
    secret: u32,
    table: &CancelTable,
) -> io::Result<()> {
    let Some(addr) = table.get(pid, secret) else {
        debug!(pid, secret, "cancel for unknown backend key — dropping");
        return Ok(());
    };

    let mut stream = TcpStream::connect(addr).await?;
    let mut packet = Vec::with_capacity(16);
    packet.extend_from_slice(&16u32.to_be_bytes()); // length
    packet.extend_from_slice(&CANCEL_REQUEST.to_be_bytes()); // magic
    packet.extend_from_slice(&pid.to_be_bytes());
    packet.extend_from_slice(&secret.to_be_bytes());
    stream.write_all(&packet).await?;
    // No response expected; close immediately.
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

    /// A reader that yields bytes from a queued buffer, for framed-message tests.
    struct PipeReader(VecDeque<u8>);

    impl AsyncRead for PipeReader {
        fn poll_read(
            mut self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<io::Result<()>> {
            if self.0.is_empty() {
                return std::task::Poll::Ready(Ok(())); // EOF
            }
            let n = std::cmp::min(self.0.len(), buf.capacity());
            buf.put_slice(&self.0.drain(..n).collect::<Vec<_>>());
            std::task::Poll::Ready(Ok(()))
        }
    }

    fn backend_msg(ty: u8, body: &[u8]) -> Vec<u8> {
        let mut v = Vec::with_capacity(5 + body.len());
        v.push(ty);
        v.extend_from_slice(&(body.len() as u32 + 4).to_be_bytes());
        v.extend_from_slice(body);
        v
    }

    #[tokio::test]
    async fn intercepts_backend_key_until_ready() {
        // K(pid=11, secret=22) then Z(status=I)
        let mut bytes = Vec::new();
        bytes.extend(backend_msg(b'K', &[0, 0, 0, 11, 0, 0, 0, 22]));
        bytes.extend(backend_msg(b'Z', b"I"));
        let mut reader = PipeReader(VecDeque::from(bytes.drain(..).collect::<Vec<_>>()));
        let mut out: Vec<u8> = Vec::new();

        let table = CancelTable::new();
        let addr: SocketAddr = "127.0.0.1:9999".parse().unwrap();
        let captured = copy_until_ready(&mut reader, &mut out, addr, &table)
            .await
            .unwrap();

        // Cancel key recorded and returned.
        assert_eq!(captured, Some((11, 22)));
        assert_eq!(table.get(11, 22), Some(addr));
        // Both messages forwarded verbatim to the client.
        assert!(!out.is_empty());
        assert_eq!(out[0], b'K');
    }
}
