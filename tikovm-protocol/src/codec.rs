//! Length-delimited JSON framing for the vsock control channel.
//!
//! Each frame is `[4-byte big-endian length][JSON payload]`. This module is
//! pure (no async): callers feed bytes into a [`FrameDecoder`] and receive back
//! complete decoded byte payloads, then (de)serialize messages with `serde_json`.
//! This keeps the protocol crate free of any runtime dependency.

use crate::error::{ProtocolError, ProtocolResult};

/// Maximum frame size (16 MiB). Guards against malformed/peer length prefixes.
const MAX_FRAME: usize = 16 * 1024 * 1024;

/// Encode a serializable message as a single length-prefixed frame.
pub fn encode_frame<T: serde::Serialize>(msg: &T) -> ProtocolResult<Vec<u8>> {
    let payload = serde_json::to_vec(msg)?;
    Ok(encode_frame_bytes(&payload))
}

/// Wrap an already-serialized payload with the length prefix.
pub fn encode_frame_bytes(payload: &[u8]) -> Vec<u8> {
    let len = payload.len() as u32;
    let mut out = Vec::with_capacity(4 + payload.len());
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(payload);
    out
}

/// Decode the next message from a length-prefixed frame buffer.
///
/// If a complete frame is available, returns `(decoded_message, bytes_consumed)`
/// and leaves the remainder in `buf`. Returns `Ok(None)` if more bytes are needed.
pub fn decode_frame<T: serde::de::DeserializeOwned>(buf: &[u8]) -> ProtocolResult<Option<(T, usize)>> {
    match try_take_frame(buf)? {
        Some((payload, n)) => {
            let msg = serde_json::from_slice(&payload)?;
            Ok(Some((msg, n)))
        }
        None => Ok(None),
    }
}

/// Extract one complete raw frame (payload + total bytes consumed) from `buf`,
/// or `None` if the buffer doesn't yet hold a full frame.
pub fn try_take_frame(buf: &[u8]) -> ProtocolResult<Option<(Vec<u8>, usize)>> {
    if buf.len() < 4 {
        return Ok(None);
    }
    let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    if len > MAX_FRAME {
        return Err(ProtocolError::FrameTooLarge(len, MAX_FRAME));
    }
    let total = 4 + len;
    if buf.len() < total {
        return Ok(None);
    }
    let payload = buf[4..total].to_vec();
    Ok(Some((payload, total)))
}

/// Incremental decoder: accumulate bytes from a stream, yield complete frames.
///
/// Typical use (host/guest side):
/// ```ignore
/// let mut dec = FrameDecoder::new();
/// // ...read n bytes from the vsock into `chunk`...
/// for payload in dec.push(&chunk)? {
///     let msg: MyMsg = serde_json::from_slice(&payload)?;
/// }
/// ```
#[derive(Default)]
pub struct FrameDecoder {
    buf: Vec<u8>,
}

impl FrameDecoder {
    pub fn new() -> Self {
        Self { buf: Vec::new() }
    }

    /// Append bytes and return all now-complete frame payloads (in order).
    pub fn push(&mut self, bytes: &[u8]) -> ProtocolResult<Vec<Vec<u8>>> {
        self.buf.extend_from_slice(bytes);
        let mut out = Vec::new();
        while let Some((payload, n)) = try_take_frame(&self.buf)? {
            out.push(payload);
            self.buf.drain(..n);
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
    struct Msg {
        v: u32,
        s: String,
    }

    #[test]
    fn round_trip_single_frame() {
        let msg = Msg { v: 7, s: "hi".into() };
        let frame = encode_frame(&msg).unwrap();
        let (decoded, n) = decode_frame::<Msg>(&frame).unwrap().unwrap();
        assert_eq!(decoded, msg);
        assert_eq!(n, frame.len());
    }

    #[test]
    fn decoder_handles_partial_and_multiple() {
        let mut dec = FrameDecoder::new();
        let a = encode_frame(&Msg { v: 1, s: "a".into() }).unwrap();
        let b = encode_frame(&Msg { v: 2, s: "bb".into() }).unwrap();

        // Feed first half of `a`.
        let split = a.len() / 2;
        assert!(dec.push(&a[..split]).unwrap().is_empty());
        // Feed rest of `a` plus all of `b`.
        let mut rest = Vec::new();
        rest.extend_from_slice(&a[split..]);
        rest.extend_from_slice(&b);
        let frames = dec.push(&rest).unwrap();
        assert_eq!(frames.len(), 2);
        let m0: Msg = serde_json::from_slice(&frames[0]).unwrap();
        let m1: Msg = serde_json::from_slice(&frames[1]).unwrap();
        assert_eq!(m0.v, 1);
        assert_eq!(m1.v, 2);
    }

    #[test]
    fn rejects_oversized_frame() {
        let mut bad = (0xFFFF_FFFFu32).to_be_bytes().to_vec();
        bad.extend_from_slice(&[0u8; 4]);
        assert!(try_take_frame(&bad).is_err());
    }
}
