//! Manifest viewer for Tiko TIKM and S3 wire-format manifests.
//!
//! Reads both local TIKM binary files and compressed S3 manifests, printing
//! a human-readable summary of the header and chunk entries.
//!
//! # Usage
//!
//! ```text
//! manifest_viewer <file>                   Auto-detect by magic bytes
//! manifest_viewer --s3 <key>               Read from SimStore standard bucket ($PGDATA required)
//! manifest_viewer --s3-express <key>       Read from SimStore express bucket ($PGDATA required)
//! manifest_viewer --no-entries [...]       Suppress rel_nblocks and entries table; show summary only
//! ```
//!
//! Auto-detection: if the file begins with the `TIKM` magic it is opened as a
//! local TIKM file; otherwise the raw bytes are treated as the S3 wire format
//! (`zstd(msgpack(...))`).
//!
//! # Examples
//!
//! ```bash
//! # Build
//! cargo build -p s3worker
//!
//! # Local TIKM file (auto-detected by TIKM magic bytes):
//! ./target/debug/manifest_viewer /path/to/base_manifest.bin
//!
//! # Raw S3 wire format blob saved to disk (auto-detected as non-TIKM):
//! ./target/debug/manifest_viewer /path/to/manifest.bin
//!
//! # Summary only — skip the per-entry table (useful for large manifests):
//! ./target/debug/manifest_viewer --no-entries /path/to/base_manifest.bin
//!
//! # From SimStore standard bucket (holds base/delta manifests and WAL):
//! PGDATA=/path/to/pgdata ./target/debug/manifest_viewer \
//!     --s3 "0/pitr/0/bases/0000000002800000/manifest.bin"
//!
//! # From SimStore express bucket:
//! PGDATA=/path/to/pgdata ./target/debug/manifest_viewer \
//!     --s3-express "0/0/chunks/1663/5/16384.0/latest"
//! ```

use std::path::{Path, PathBuf};

use s3worker::manifest::Manifest;
use s3worker::sim_store::SimStore;

// ── Argument parsing ─────────────────────────────────────────────────────────

enum Source {
    /// Path to a local file — auto-detected or forced to one format.
    File(PathBuf),
    /// Key in the SimStore standard bucket; needs $PGDATA.
    S3Standard(String),
    /// Key in the SimStore express bucket; needs $PGDATA.
    S3Express(String),
}

struct Args {
    source: Source,
    no_entries: bool,
}

fn usage() -> ! {
    eprintln!(
        "Usage:
  manifest_viewer <file>
  manifest_viewer --s3 <key>          (requires $PGDATA)
  manifest_viewer --s3-express <key>  (requires $PGDATA)

Flags:
  --no-entries   Print header summary only; skip rel_nblocks and entries table."
    );
    std::process::exit(1);
}

fn parse_args() -> Args {
    let raw: Vec<String> = std::env::args().skip(1).collect();
    let mut no_entries = false;
    let mut positional: Vec<String> = Vec::new();
    let mut s3_standard: Option<String> = None;
    let mut s3_express: Option<String> = None;

    let mut i = 0;
    while i < raw.len() {
        match raw[i].as_str() {
            "--no-entries" => no_entries = true,
            "--s3" => {
                i += 1;
                s3_standard = Some(raw.get(i).cloned().unwrap_or_else(|| {
                    eprintln!("--s3 requires a key argument");
                    std::process::exit(1);
                }));
            }
            "--s3-express" => {
                i += 1;
                s3_express = Some(raw.get(i).cloned().unwrap_or_else(|| {
                    eprintln!("--s3-express requires a key argument");
                    std::process::exit(1);
                }));
            }
            other if other.starts_with('-') => {
                eprintln!("Unknown flag: {other}");
                usage();
            }
            other => positional.push(other.to_string()),
        }
        i += 1;
    }

    let source = match (s3_standard, s3_express, positional.len()) {
        (Some(k), None, 0) => Source::S3Standard(k),
        (None, Some(k), 0) => Source::S3Express(k),
        (None, None, 1) => Source::File(PathBuf::from(&positional[0])),
        _ => usage(),
    };

    Args { source, no_entries }
}

// ── SimStore helper ───────────────────────────────────────────────────────────

fn sim_from_env() -> Result<SimStore, String> {
    let pgdata = std::env::var("PGDATA").map_err(|_| "PGDATA is not set".to_string())?;
    Ok(SimStore::new(Path::new(&pgdata)))
}

// ── TIKM magic detection ──────────────────────────────────────────────────────

const TIKM_MAGIC: &[u8; 4] = b"TIKM";

fn is_tikm(data: &[u8]) -> bool {
    data.len() >= 4 && &data[0..4] == TIKM_MAGIC
}

// ── Fork name ────────────────────────────────────────────────────────────────

fn fork_name(fork_number: i32) -> String {
    match fork_number {
        0 => "main".to_string(),
        1 => "fsm".to_string(),
        2 => "vm".to_string(),
        3 => "init".to_string(),
        n => n.to_string(),
    }
}

// ── Timestamp formatting ──────────────────────────────────────────────────────

/// Naive UTC formatting: YYYY-MM-DD HH:MM:SS UTC
fn format_timestamp(secs: i64) -> String {
    // Days since Unix epoch (1970-01-01)
    let (neg, secs) = if secs < 0 {
        (true, (-secs) as u64)
    } else {
        (false, secs as u64)
    };
    if neg {
        return format!("{secs}s before epoch");
    }

    let s = secs % 60;
    let m = (secs / 60) % 60;
    let h = (secs / 3600) % 24;
    let total_days = secs / 86400;

    // Gregorian calendar computation
    let (year, month, day) = days_to_ymd(total_days);
    format!("{year:04}-{month:02}-{day:02} {h:02}:{m:02}:{s:02} UTC")
}

fn days_to_ymd(mut days: u64) -> (u64, u64, u64) {
    // Algorithm: civil calendar from Howard Hinnant
    days += 719468;
    let era = days / 146097;
    let doe = days % 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

// ── Display ───────────────────────────────────────────────────────────────────

fn print_manifest(manifest: &Manifest, format_label: &str, no_entries: bool) {
    let lsn = manifest.checkpoint_lsn();
    let ts = manifest.timestamp();
    let entry_count = manifest.entries().map(|e| e.len()).unwrap_or(0);

    println!("Manifest");
    println!("  format:          {format_label}");
    println!("  checkpoint_lsn:  {lsn}");
    println!("  timestamp:       {}  ({})", format_timestamp(ts), ts);
    println!("  entry_count:     {entry_count}");

    if no_entries {
        return;
    }

    let rel_nblocks = manifest.rel_nblocks();
    if !rel_nblocks.is_empty() {
        println!();
        println!("rel_nblocks:");
        let mut pairs: Vec<_> = rel_nblocks.iter().collect();
        pairs.sort_by_key(|(rf, _)| *rf);
        for (rf, nblocks) in pairs {
            let fork = fork_name(rf.fork_number);
            println!(
                "  {}/{}/{}.{}  →  {} blocks",
                rf.spc_oid, rf.db_oid, rf.rel_number, fork, nblocks
            );
        }
    } else if format_label.contains("S3") {
        // S3 format can legitimately have an empty rel_nblocks map.
        println!();
        println!("rel_nblocks:     (empty)");
    }

    let entries = match manifest.entries() {
        Ok(e) => e,
        Err(err) => {
            eprintln!("error reading entries: {err}");
            return;
        }
    };

    if entries.is_empty() {
        println!();
        println!("Entries:         (none)");
        return;
    }

    println!();
    println!(
        "  {:<6}  {:<10}  {:<10}  {:<12}  {:<6}  {:<10}  │  {:<20}  {}",
        "#", "spc_oid", "db_oid", "rel_number", "fork", "chunk_id", "branch_id", "lsn"
    );
    println!("  {}", "─".repeat(92));
    for (i, (tag, cref)) in entries.iter().enumerate() {
        println!(
            "  {:<6}  {:<10}  {:<10}  {:<12}  {:<6}  {:<10}  │  {:<20}  {}",
            i,
            tag.spc_oid,
            tag.db_oid,
            tag.rel_number,
            fork_name(tag.fork_number),
            tag.chunk_id,
            cref.branch_id,
            cref.lsn,
        );
    }
}

// ── Main ──────────────────────────────────────────────────────────────────────

fn run(args: &Args) -> Result<(), String> {
    match &args.source {
        Source::File(path) => {
            let data =
                std::fs::read(path).map_err(|e| format!("cannot read {}: {e}", path.display()))?;

            if is_tikm(&data) {
                let manifest =
                    Manifest::open(path).map_err(|e| format!("failed to open TIKM: {e}"))?;
                print_manifest(&manifest, "local TIKM", args.no_entries);
            } else {
                let tmp = tempfile::TempDir::new().map_err(|e| format!("tempdir: {e}"))?;
                let tmp_path = tmp.path().join("manifest.bin");
                let manifest = Manifest::from_bytes(&data, &tmp_path)
                    .map_err(|e| format!("failed to decode S3 wire format: {e}"))?;
                print_manifest(&manifest, "S3 wire format", args.no_entries);
            }
        }

        Source::S3Standard(key) => {
            let sim = sim_from_env()?;
            let data = sim
                .get_standard(key)
                .map_err(|e| format!("SimStore error: {e}"))?
                .ok_or_else(|| format!("key not found in standard bucket: {key}"))?;
            let tmp = tempfile::TempDir::new().map_err(|e| format!("tempdir: {e}"))?;
            let tmp_path = tmp.path().join("manifest.bin");
            let manifest = Manifest::from_bytes(&data, &tmp_path)
                .map_err(|e| format!("failed to decode S3 wire format: {e}"))?;
            print_manifest(&manifest, "S3 standard bucket", args.no_entries);
        }

        Source::S3Express(key) => {
            let sim = sim_from_env()?;
            let data = sim
                .get_express(key)
                .map_err(|e| format!("SimStore error: {e}"))?
                .ok_or_else(|| format!("key not found in express bucket: {key}"))?;
            let tmp = tempfile::TempDir::new().map_err(|e| format!("tempdir: {e}"))?;
            let tmp_path = tmp.path().join("manifest.bin");
            let manifest = Manifest::from_bytes(&data, &tmp_path)
                .map_err(|e| format!("failed to decode S3 wire format: {e}"))?;
            print_manifest(&manifest, "S3 express bucket", args.no_entries);
        }
    }

    Ok(())
}

fn main() {
    let args = parse_args();
    if let Err(e) = run(&args) {
        eprintln!("manifest_viewer: {e}");
        std::process::exit(1);
    }
}
