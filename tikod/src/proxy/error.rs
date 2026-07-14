//! PostgreSQL `ErrorResponse` construction.
//!
//! Builds wire-format PG error packets so the proxy can reject or fail clients
//! with a clean message that libpq renders properly, instead of a bare TCP
//! close.
//!
//! Wire format (v3 protocol):
//!
//! ```text
//! 'E' | length: u32 BE | field(tag: u8, value: &str, NUL)* | NUL
//! ```
//!
//! where `length` counts itself (4 bytes) plus the field bytes plus the
//! terminating NUL. Each field is a one-byte tag followed by a C-string value.
//! The packet ends with an extra NUL (a zero tag). We emit the standard fields
//! `S` (severity), `V` (non-localized severity), `C` (SQLSTATE), `M` (message).

use crate::vmm::VmmError;

/// Severity for connection-rejection errors (libpq treats `FATAL` as terminal
/// for the connection).
const FATAL: &str = "FATAL";

/// Build a complete `'E'` error packet on the wire.
pub(crate) fn error_packet(severity: &str, sqlstate: &str, message: &str) -> Vec<u8> {
    let mut fields = Vec::with_capacity(64);
    for (tag, value) in [
        (b'S', severity),
        (b'V', severity),
        (b'C', sqlstate),
        (b'M', message),
    ] {
        fields.push(tag);
        fields.extend_from_slice(value.as_bytes());
        fields.push(0);
    }
    fields.push(0); // terminating NUL (zero tag)

    let mut packet = Vec::with_capacity(1 + 4 + fields.len());
    packet.push(b'E');
    packet.extend_from_slice(&(4 + fields.len() as u32).to_be_bytes());
    packet.extend_from_slice(&fields);
    packet
}

/// `tiko.endpoint` routing option not present in the startup packet.
pub fn fatal_missing_endpoint() -> Vec<u8> {
    error_packet(
        FATAL,
        "28000",
        "missing tiko.endpoint routing option (expected options=-c tiko.endpoint=<vm_id>)",
    )
}

/// `tiko.endpoint` referenced a vm_id that is not in the registry.
pub fn fatal_unknown_vm(vm_id: &str) -> Vec<u8> {
    error_packet(FATAL, "28000", &format!("unknown VM {vm_id}"))
}

/// VM is in a state that cannot be forwarded to (e.g. `Snapshotting`).
pub fn fatal_bad_state(vm_id: &str, state: &str) -> Vec<u8> {
    error_packet(
        FATAL,
        "08006",
        &format!("VM {vm_id} is in state {state}, cannot forward"),
    )
}

/// Wake (`Node::wake`) did not complete within `resume_timeout_secs`.
pub fn fatal_wake_timeout(vm_id: &str, secs: u64) -> Vec<u8> {
    error_packet(
        FATAL,
        "08006",
        &format!("VM {vm_id} did not start within {secs}s"),
    )
}

/// A stopped VM has no stored snapshot to restore from.
pub fn fatal_no_snapshot(vm_id: &str) -> Vec<u8> {
    error_packet(
        FATAL,
        "08006",
        &format!("VM {vm_id} has no snapshot; cannot restore"),
    )
}

/// Map a [`VmmError`] returned by `Node::wake` to the appropriate PG error
/// packet. `secs` is the configured `resume_timeout_secs`, used only if the
/// caller wants to report it (unused for non-timeout errors here).
pub fn wake_error_packet(vm_id: &str, err: &VmmError) -> Vec<u8> {
    match err {
        VmmError::SnapshotNotFound(_) => fatal_no_snapshot(vm_id),
        VmmError::InvalidState { current, .. } => fatal_bad_state(vm_id, &current.to_string()),
        _ => error_packet(FATAL, "08006", &format!("VM {vm_id}: {err}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_fields(packet: &[u8]) -> Vec<(u8, &str)> {
        assert_eq!(packet[0], b'E');
        let len = u32::from_be_bytes(packet[1..5].try_into().unwrap()) as usize;
        assert_eq!(len, packet.len() - 1);
        let mut fields = Vec::new();
        let mut i = 5;
        while i < packet.len() {
            let tag = packet[i];
            i += 1;
            if tag == 0 {
                break;
            }
            let end = packet[i..].iter().position(|&b| b == 0).unwrap() + i;
            let val = std::str::from_utf8(&packet[i..end]).unwrap();
            fields.push((tag, val));
            i = end + 1;
        }
        fields
    }

    #[test]
    fn missing_endpoint_packet_shape() {
        let p = fatal_missing_endpoint();
        let fields = parse_fields(&p);
        assert_eq!(fields[0], (b'S', FATAL));
        assert_eq!(fields[1], (b'V', FATAL));
        assert_eq!(fields[2], (b'C', "28000"));
        assert!(fields[3].1.contains("tiko.endpoint"));
    }

    #[test]
    fn unknown_vm_packet_message() {
        let p = fatal_unknown_vm("vm-7");
        let fields = parse_fields(&p);
        assert_eq!(fields[2], (b'C', "28000"));
        assert_eq!(fields[3], (b'M', "unknown VM vm-7"));
    }

    #[test]
    fn wake_no_snapshot_maps_to_fatal() {
        let err = VmmError::SnapshotNotFound("vm-9".into());
        let p = wake_error_packet("vm-9", &err);
        let fields = parse_fields(&p);
        assert_eq!(fields[2], (b'C', "08006"));
        assert!(fields[3].1.contains("no snapshot"));
    }
}
