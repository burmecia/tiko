//! `tiko_restore` — PostgreSQL `restore_command` helper for PITR.
//!
//! Invoked by PostgreSQL during archive recovery as
//! `restore_command = 'tiko_restore %f %p'`, where `%f` is the WAL segment
//! file name and `%p` is the destination path (relative to the data dir).
//!
//! Reads the WAL produced by `wal_receiver`. Two object shapes can exist for a
//! given segment (see `worker/src/tasks/wal_receiver.rs`):
//!
//!   1. A **sealed** object — a complete, zero-padded `XLOG_SEG_SIZE` segment.
//!      This is the authoritative copy and is preferred when present.
//!   2. **Chunks** — 256 KiB objects keyed by their byte offset within the
//!      segment, under a `{segment}.chunks/` prefix. These exist for a segment
//!      that was never sealed: the current in-flight tail, or the first
//!      segment of a stream that began mid-segment (its `[0, start_offset)`
//!      prefix is unknown and left zero).
//!
//! Exit codes follow the `restore_command` contract:
//!   * `0`  — file restored to `%p`.
//!   * `1`  — file not available in the archive (normal end-of-WAL / missing
//!            history file) or a hard error. PostgreSQL ends/redirects
//!            recovery accordingly.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::exit;

use clap::Parser;

use core::{error::Result, io::store::Store};
use pgsys::common::XLOG_SEG_SIZE;
use pgsys::timeline_id::TimelineId;

// `tiko_restore` runs as a standalone process (invoked by `restore_command`),
// not loaded into the postmaster, so the PG symbols that `core` transitively
// references are supplied by `cli::pg_stubs`. The `extern crate` below forces
// the `cli` lib onto the link line so its `#[no_mangle]` stubs resolve `core`'s
// undefined references. `TIKO_ROOT_PATH` is expected to be set in the
// environment so `DataDir` is never actually dereferenced.
extern crate cli;

#[derive(Parser)]
#[command(
    name = "tiko_restore",
    about = "Restore WAL segment files from Tiko storage for PITR"
)]
struct Args {
    /// `%f` — the WAL file name PostgreSQL is requesting (e.g.
    /// `000000010000000000000002`).
    wal_filename: String,
    /// `%p` — destination path the file must be written to.
    dest_path: PathBuf,
}

enum Outcome {
    /// File was written to the destination.
    Restored,
    /// File is not present in the archive (not an error — PG handles it).
    NotFound,
}

/// Restore one requested WAL file.
fn restore(store: &Store, args: &Args) -> Result<Outcome> {
    // We only archive regular 24-hex WAL segment files. Timeline history
    // files (`{tli:08X}.history`), `.backup`, and `.partial` files are never
    // uploaded by `wal_receiver`, so treat anything else as not-found and let
    // PostgreSQL fall back to its other recovery sources.
    let Some(timeline_id) = parse_wal_segment_name(&args.wal_filename) else {
        return Ok(Outcome::NotFound);
    };
    let name = args.wal_filename.as_str();
    let loc = store.locator();

    // 1. Prefer the sealed (complete) segment object.
    let seg_key = loc.wal_segment(timeline_id, name);
    match store.storage_get(&seg_key) {
        Ok(bytes) => {
            write_atomic(&args.dest_path, &bytes)?;
            return Ok(Outcome::Restored);
        }
        Err(e) if e.is_not_found() => {} // fall through to chunk assembly
        Err(e) => return Err(e),
    }

    // 2. Fall back to assembling the segment from its 256 KiB chunks.
    let prefix = loc.wal_chunk_prefix(timeline_id, name);
    let chunk_keys = store.storage_list_prefix(&prefix)?;
    if chunk_keys.is_empty() {
        return Ok(Outcome::NotFound);
    }

    // Assemble into a full-size, zero-initialized segment. Gaps (an unknown
    // mid-segment-start prefix, or bytes past the last streamed chunk) stay
    // zero; PostgreSQL replays valid records and stops at the first invalid
    // one, which is the correct end-of-WAL behavior.
    let mut buf = vec![0u8; XLOG_SEG_SIZE];
    for key in &chunk_keys {
        let offset = match chunk_offset(key) {
            Some(o) => o,
            None => continue, // ignore stray objects that aren't offset-keyed
        };
        let data = store.storage_get(key)?;
        let end = offset.saturating_add(data.len());
        if offset >= buf.len() || end > buf.len() {
            // A chunk that doesn't fit the segment means corruption upstream;
            // refuse to write a misleading file rather than silently truncate.
            return Err(core::error::Error::invalid_data(format!(
                "chunk {key} (offset {offset}, len {}) exceeds segment size {XLOG_SEG_SIZE}",
                data.len()
            )));
        }
        buf[offset..end].copy_from_slice(&data);
    }

    write_atomic(&args.dest_path, &buf)?;
    Ok(Outcome::Restored)
}

/// Validate that `name` is a 24-character WAL segment file name and return its
/// timeline id (the first 8 hex chars). Returns `None` for any other name.
fn parse_wal_segment_name(name: &str) -> Option<TimelineId> {
    if name.len() != 24 || !name.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    TimelineId::from_hex(&name[..8]).ok()
}

/// Parse the byte offset encoded in a chunk key's final path component
/// (`{...}.chunks/{offset:016X}`).
fn chunk_offset(key: &str) -> Option<usize> {
    let last = key.rsplit('/').next()?;
    usize::from_str_radix(last, 16).ok()
}

/// Write `data` to `dest` atomically: write to a sibling temp file, then
/// rename. A crash mid-write must never leave PostgreSQL a partial segment.
fn write_atomic(dest: &Path, data: &[u8]) -> Result<()> {
    let tmp = temp_sibling(dest);
    fs::write(&tmp, data)?;
    match fs::rename(&tmp, dest) {
        Ok(()) => Ok(()),
        Err(e) => {
            let _ = fs::remove_file(&tmp);
            Err(e.into())
        }
    }
}

/// Build a unique temp path next to `dest` (same directory → rename is atomic).
fn temp_sibling(dest: &Path) -> PathBuf {
    let mut file_name = dest
        .file_name()
        .map(|n| n.to_os_string())
        .unwrap_or_default();
    file_name.push(format!(".tiko_restore.{}.tmp", std::process::id()));
    match dest.parent() {
        Some(p) if !p.as_os_str().is_empty() => p.join(file_name),
        _ => PathBuf::from(file_name),
    }
}

fn main() {
    let args = Args::parse();
    let store = match Store::init() {
        Ok(s) => s,
        Err(e) => {
            eprintln!("tiko_restore: store init failed: {e}");
            exit(1);
        }
    };

    match restore(store, &args) {
        Ok(Outcome::Restored) => exit(0),
        Ok(Outcome::NotFound) => {
            // Not an error: PostgreSQL expects a nonzero exit for files that
            // aren't in the archive (end of WAL, missing history file).
            eprintln!("tiko_restore: {} not found in archive", args.wal_filename);
            exit(1);
        }
        Err(e) => {
            eprintln!("tiko_restore: error restoring {}: {e}", args.wal_filename);
            exit(1);
        }
    }
}
