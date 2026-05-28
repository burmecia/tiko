use std::fs;
use std::path::PathBuf;
use std::process::exit;

use chrono::{DateTime, Utc};
use clap::Parser;
use core::io::timeline::TimelineSegment;
use zstd;

#[derive(Parser)]
#[command(
    name = "tiko_tlseg_viewer",
    about = "Display timeline segment file content"
)]
struct Args {
    /// Path to the .segment file
    path: PathBuf,
}

// ── Main ──────────────────────────────────────────────────────────────────────

fn run(args: &Args) -> Result<(), String> {
    let raw = fs::read(&args.path).map_err(|e| format!("failed to read {:?}: {e}", args.path))?;
    let compressed_size = raw.len();
    let bytes =
        zstd::decode_all(raw.as_slice()).map_err(|e| format!("decompression error: {e}"))?;
    let uncompressed_size = bytes.len();
    let seg = TimelineSegment::from_bytes(&bytes).map_err(|e| format!("parse error: {e}"))?;

    println!("segment_id:        {}", seg.segment_id);
    println!("compressed_size:   {} bytes", compressed_size);
    println!("uncompressed_size: {} bytes", uncompressed_size);
    println!("checkpoints: {}", seg.checkpoints.len());

    for (i, ckpt) in seg.checkpoints.iter().enumerate() {
        println!();
        println!(
            "[{:03}] ckpt: {},\tprev_ckpt: {},\tredo_ckpt: {}",
            i, ckpt.ckpt, ckpt.prev_ckpt, ckpt.redo_ckpt
        );
        println!(
            "      chunks: {},\t\trelforks: {},\t\tpg_state: {} bytes",
            ckpt.chunks.len(),
            ckpt.relforks.len(),
            ckpt.pg_state.len()
        );
        let created_at = DateTime::<Utc>::from_timestamp(ckpt.created_at, 0)
            .map(|t| t.to_rfc3339())
            .unwrap_or_else(|| ckpt.created_at.to_string());
        println!("      created_at: {}", created_at);
    }

    Ok(())
}

fn main() {
    let args = Args::parse();
    if let Err(e) = run(&args) {
        eprintln!("tiko_tlseg_viewer: {e}");
        exit(1);
    }
}
