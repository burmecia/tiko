//! PostgreSQL wire-protocol startup message parsing (peek-then-splice).
//!
//! The proxy reads just enough of the client's opening bytes to:
//! - decline SSL/GSS encryption (v1 is plaintext-only),
//! - recognise a `CancelRequest` (handed off to the cancel router), and
//! - extract the `tiko.endpoint=<vm_id>` routing key from a `StartupMessage`.
//!
//! After the startup message is captured, the proxy replays its raw bytes to
//! the chosen backend and switches to a blind byte splice — no further parsing
//! of the data stream.
//!
//! # Wire format reference (protocol v3)
//!
//! The first message from a client is exactly one of:
//!
//! - `SSLRequest`  — length 8, magic `80877103`. Proxy replies `'N'`.
//! - `GSSENCRequest` — length 8, magic `80877104`. Proxy replies `'N'`.
//! - `CancelRequest` — length 16, magic `80877102`, then `pid: u32`, `secret: u32`.
//! - `StartupMessage` — length N, protocol version `0x00030000`, then
//!   NUL-terminated `key\0value\0` pairs terminated by an empty key (a single
//!   NUL byte).
//!
//! A client may send `SSLRequest` (or `GSSENCRequest`) *before* the real
//! `StartupMessage`; the proxy loops until it sees the startup or a cancel.

use std::io;

use tokio::io::{AsyncRead, AsyncReadExt};

/// Magic numbers for the special (non-startup) first messages.
const SSL_REQUEST: u32 = 80877103;
const GSS_ENC_REQUEST: u32 = 80877104;
const CANCEL_REQUEST: u32 = 80877102;

/// Protocol version 3.0 (major 3 in the high 16 bits).
const PROTO_VERSION_3: u32 = 0x0003_0000;

/// The discriminant of the client's first message.
#[derive(Debug)]
pub enum FirstMessage {
    /// Client asked for SSL. v1 proxy declines (replies `'N'`) and expects a
    /// `StartupMessage` to follow.
    SslRequest,
    /// Client asked for GSSAPI encryption. Same handling as `SslRequest`.
    GssEncRequest,
    /// Client wants to cancel a running query on an existing connection.
    /// Carries the `{pid, secret}` of the target backend connection.
    Cancel { pid: u32, secret: u32 },
    /// A normal startup packet. Carries the parsed parameters and the raw bytes
    /// (length prefix + payload) to replay verbatim to the backend.
    Startup(StartupMessage),
}

/// Parsed [`StartupMessage`](FirstMessage::Startup).
#[derive(Debug)]
pub struct StartupMessage {
    /// Raw message bytes including the 4-byte length prefix. Suitable for
    /// replaying to the backend without re-encoding.
    pub raw: Vec<u8>,
    /// The `options` startup parameter, if the client supplied one. This is the
    /// space-separated string of `-c key=value` tokens.
    pub options: Option<String>,
}

impl StartupMessage {
    /// Extract the `vm_id` routing key from the `options` string, if present.
    ///
    /// Accepts the standard two-token form (`-c tiko.endpoint=<vm_id>`) and any
    /// token containing `tiko.endpoint=...` (e.g. `--tiko.endpoint=...`). The
    /// first match wins.
    pub fn vm_id(&self) -> Option<&str> {
        let opts = self.options.as_deref()?;
        for tok in opts.split_whitespace() {
            if let Some(val) = tok.split("tiko.endpoint=").nth(1) {
                if !val.is_empty() {
                    return Some(val);
                }
            }
        }
        None
    }
}

/// Read exactly one framed message from `r`. Reads the 4-byte length prefix,
/// then the `(length - 4)`-byte payload.
async fn read_message<R: AsyncRead + Unpin>(r: &mut R) -> io::Result<(u32, Vec<u8>, Vec<u8>)> {
    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf);
    if len < 8 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("startup length too small: {len}"),
        ));
    }
    // Cap to defend against absurd values (the startup packet is small).
    if len > 1_000_000 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("startup length too large: {len}"),
        ));
    }
    let want = (len - 4) as usize;
    let mut payload = vec![0u8; want];
    r.read_exact(&mut payload).await?;

    let mut raw = Vec::with_capacity(4 + want);
    raw.extend_from_slice(&len_buf);
    raw.extend_from_slice(&payload);
    Ok((len, payload, raw))
}

/// Read the client's first meaningful message, transparently handling the
/// `SSLRequest`/`GSSENCRequest` prelude. The caller is responsible for
/// replying `'N'` to those (the proxy declines TLS) and looping.
///
/// Returns `Ok(None)` on clean EOF (client connected and closed without
/// sending anything) — the proxy treats this as a no-op.
pub async fn read_first_message<R: AsyncRead + Unpin>(
    r: &mut R,
) -> io::Result<Option<FirstMessage>> {
    let (len, payload, raw) = match read_message(r).await {
        Ok(t) => t,
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    };

    // Special requests all carry a 4-byte magic as their first word.
    let magic = u32::from_be_bytes(payload[0..4].try_into().unwrap());

    if len == 8 {
        match magic {
            SSL_REQUEST => return Ok(Some(FirstMessage::SslRequest)),
            GSS_ENC_REQUEST => return Ok(Some(FirstMessage::GssEncRequest)),
            _ => {}
        }
    }

    if len == 16 && magic == CANCEL_REQUEST {
        let pid = u32::from_be_bytes(payload[4..8].try_into().unwrap());
        let secret = u32::from_be_bytes(payload[8..12].try_into().unwrap());
        return Ok(Some(FirstMessage::Cancel { pid, secret }));
    }

    // Otherwise: a StartupMessage. The first payload word is the protocol
    // version (major in the high 16 bits).
    let version = magic;
    if version >> 16 != PROTO_VERSION_3 >> 16 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported protocol version: {version:#010x}"),
        ));
    }

    // Parse the trailing NUL-terminated key/value pairs (after the 4-byte
    // version word), stopping at the empty key.
    let mut options = None;
    let pairs = &payload[4..];
    let mut pos = 0;
    while pos < pairs.len() {
        // Read the key (C string).
        let Some(key_end) = pairs[pos..].iter().position(|&b| b == 0) else {
            break;
        };
        if key_end == 0 {
            break; // empty key → end of pairs
        }
        let key = &pairs[pos..pos + key_end];
        pos += key_end + 1;

        // Read the value (C string).
        let Some(val_end) = pairs[pos..].iter().position(|&b| b == 0) else {
            break;
        };
        let val = &pairs[pos..pos + val_end];
        pos += val_end + 1;

        if key == b"options" {
            options = std::str::from_utf8(val).ok().map(|s| s.to_string());
        }
    }

    Ok(Some(FirstMessage::Startup(StartupMessage { raw, options })))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a raw startup packet (length + payload) from key/value pairs.
    fn startup_packet(pairs: &[(&str, &str)]) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(&PROTO_VERSION_3.to_be_bytes());
        for (k, v) in pairs {
            payload.extend_from_slice(k.as_bytes());
            payload.push(0);
            payload.extend_from_slice(v.as_bytes());
            payload.push(0);
        }
        payload.push(0); // terminating empty key
        let mut packet = Vec::with_capacity(4 + payload.len());
        packet.extend_from_slice(&(payload.len() as u32 + 4).to_be_bytes());
        packet.extend_from_slice(&payload);
        packet
    }

    #[test]
    fn extracts_vm_id_from_options() {
        let msg = StartupMessage {
            raw: vec![],
            options: Some("-c tiko.endpoint=vm-42".into()),
        };
        assert_eq!(msg.vm_id(), Some("vm-42"));
    }

    #[test]
    fn extracts_vm_id_with_extra_options() {
        let msg = StartupMessage {
            raw: vec![],
            options: Some("-c application_name=app -c tiko.endpoint=ep-7 -c geqo=off".into()),
        };
        assert_eq!(msg.vm_id(), Some("ep-7"));
    }

    #[test]
    fn returns_none_without_endpoint_option() {
        let msg = StartupMessage {
            raw: vec![],
            options: Some("-c geqo=off".into()),
        };
        assert_eq!(msg.vm_id(), None);
    }

    #[test]
    fn returns_none_with_empty_endpoint_value() {
        let msg = StartupMessage {
            raw: vec![],
            options: Some("-c tiko.endpoint=".into()),
        };
        assert_eq!(msg.vm_id(), None);
    }

    #[tokio::test]
    async fn parses_startup_message_and_raw() {
        let packet = startup_packet(&[
            ("user", "alice"),
            ("database", "mydb"),
            ("options", "-c tiko.endpoint=vm-1"),
        ]);
        let mut cursor = std::io::Cursor::new(packet.clone());
        let msg = read_first_message(&mut cursor).await.unwrap().unwrap();
        match msg {
            FirstMessage::Startup(s) => {
                assert_eq!(s.options.as_deref(), Some("-c tiko.endpoint=vm-1"));
                assert_eq!(s.vm_id(), Some("vm-1"));
                assert_eq!(s.raw, packet); // raw is replayable verbatim
            }
            _ => panic!("expected Startup"),
        }
    }

    #[tokio::test]
    async fn parses_ssl_request() {
        let mut packet = Vec::new();
        packet.extend_from_slice(&8u32.to_be_bytes());
        packet.extend_from_slice(&SSL_REQUEST.to_be_bytes());
        let mut cursor = std::io::Cursor::new(packet);
        match read_first_message(&mut cursor).await.unwrap().unwrap() {
            FirstMessage::SslRequest => {}
            other => panic!("expected SslRequest, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn parses_cancel_request() {
        let mut packet = Vec::new();
        packet.extend_from_slice(&16u32.to_be_bytes());
        packet.extend_from_slice(&CANCEL_REQUEST.to_be_bytes());
        packet.extend_from_slice(&1234u32.to_be_bytes()); // pid
        packet.extend_from_slice(&5678u32.to_be_bytes()); // secret
        let mut cursor = std::io::Cursor::new(packet);
        match read_first_message(&mut cursor).await.unwrap().unwrap() {
            FirstMessage::Cancel { pid, secret } => {
                assert_eq!(pid, 1234);
                assert_eq!(secret, 5678);
            }
            other => panic!("expected Cancel, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn clean_eof_on_empty_connect() {
        let mut cursor = std::io::Cursor::new(Vec::new());
        assert!(read_first_message(&mut cursor).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn rejects_bad_protocol_version() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&0x0002_0000u32.to_be_bytes()); // protocol v2
        payload.push(0);
        let mut packet = Vec::new();
        packet.extend_from_slice(&(payload.len() as u32 + 4).to_be_bytes());
        packet.extend_from_slice(&payload);
        let mut cursor = std::io::Cursor::new(packet);
        let err = read_first_message(&mut cursor).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }
}
