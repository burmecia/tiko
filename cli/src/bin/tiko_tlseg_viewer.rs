//! `tiko_tlseg_viewer` — inspect a timeline segment object.
//!
//! Reads an `S3Sim`-encoded `.segment` file (the storage representation of a
//! `TimelineSegment`: zstd-compressed msgpack, see `core::timeline::segment`)
//! and prints its per-checkpoint summaries. `--verbose` additionally dumps the
//! chunk tags and relation-fork metadata; `--json` emits a machine-readable
//! object instead of text.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::exit;

use clap::Parser;
use serde::Serialize;

use cli::util::fmt_unix_ts;
use core::chunk::ChunkTag;
use core::relfork::{RelFork, RelForkMeta};
use core::timeline::TimelineSegment;

// Standalone process (not loaded into the postmaster); `cli::pg_stubs` supplies
// the PG symbols that `core` transitively references. See `tiko_restore`.
extern crate cli;

#[derive(Parser)]
#[command(
    name = "tiko_tlseg_viewer",
    about = "Display timeline segment file content"
)]
struct Args {
    /// Path to the `S3Sim`-encoded `.segment` file.
    path: PathBuf,
    /// Also dump each checkpoint's chunk tags and relation-fork metadata.
    #[arg(short, long)]
    verbose: bool,
    /// Emit a single JSON object instead of human-readable text.
    #[arg(long)]
    json: bool,
}

// ── Output DTOs ───────────────────────────────────────────────────────────────

#[derive(Serialize)]
struct SegmentDto {
    segment_id: String,
    compressed_size: usize,
    uncompressed_size: usize,
    checkpoints: Vec<CheckpointDto>,
}

#[derive(Serialize)]
struct CheckpointDto {
    index: usize,
    ckpt: String,
    prev_ckpt: String,
    redo_ckpt: String,
    chunks: usize,
    relforks: usize,
    created_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    chunk_refs: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    relfork_metas: Option<Vec<RelForkDto>>,
}

#[derive(Serialize)]
struct RelForkDto {
    fork: String,
    nblocks: u32,
    deleted: bool,
}

// ── Main ──────────────────────────────────────────────────────────────────────

/// Read and parse a segment file, returning `(compressed_size,
/// uncompressed_size, segment)`. Uses the tolerant decoder so a decompressed
/// (or foreign-backend) object still parses.
fn load(path: &Path) -> Result<(usize, usize, TimelineSegment), String> {
    let raw = fs::read(path).map_err(|e| format!("failed to read {path:?}: {e}"))?;
    let compressed_size = raw.len();
    let bytes = core::storage::s3_sim::decode_object_autodetect(path, raw)
        .map_err(|e| format!("decompression error: {e}"))?;
    let uncompressed_size = bytes.len();
    let seg = TimelineSegment::from_bytes(&bytes).map_err(|e| format!("parse error: {e}"))?;
    Ok((compressed_size, uncompressed_size, seg))
}

/// Flatten a segment into its output DTO. `verbose` controls whether the
/// chunk/relfork contents are included; both are sorted for stable output.
fn build_dto(
    seg: &TimelineSegment,
    compressed_size: usize,
    uncompressed_size: usize,
    verbose: bool,
) -> SegmentDto {
    let checkpoints = seg
        .checkpoints
        .iter()
        .enumerate()
        .map(|(index, ckpt)| {
            let chunk_refs = verbose.then(|| {
                let mut tags: Vec<&ChunkTag> = ckpt.chunks.iter().collect();
                tags.sort();
                tags.iter().map(|t| t.to_path()).collect()
            });
            let relfork_metas = verbose.then(|| {
                let mut rels: Vec<(&RelFork, &RelForkMeta)> = ckpt.relforks.iter().collect();
                rels.sort_by_key(|(rf, _)| **rf);
                rels.iter()
                    .map(|(rf, m)| RelForkDto {
                        fork: rf.to_string(),
                        nblocks: m.nblocks,
                        deleted: m.deleted,
                    })
                    .collect()
            });
            CheckpointDto {
                index,
                ckpt: ckpt.ckpt.to_string(),
                prev_ckpt: ckpt.prev_ckpt.to_string(),
                redo_ckpt: ckpt.redo_ckpt.to_string(),
                chunks: ckpt.chunks.len(),
                relforks: ckpt.relforks.len(),
                created_at: fmt_unix_ts(ckpt.created_at),
                chunk_refs,
                relfork_metas,
            }
        })
        .collect();
    SegmentDto {
        segment_id: seg.segment_id.to_string(),
        compressed_size,
        uncompressed_size,
        checkpoints,
    }
}

fn print_human(dto: &SegmentDto) {
    println!("segment_id:        {}", dto.segment_id);
    println!("compressed_size:   {} bytes", dto.compressed_size);
    println!("uncompressed_size: {} bytes", dto.uncompressed_size);
    println!("checkpoints: {}", dto.checkpoints.len());

    for c in &dto.checkpoints {
        println!();
        println!(
            "[{:03}] ckpt: {},\tprev_ckpt: {},\tredo_ckpt: {}",
            c.index, c.ckpt, c.prev_ckpt, c.redo_ckpt
        );
        println!("      chunks: {},\t\trelforks: {}", c.chunks, c.relforks);
        println!("      created_at: {}", c.created_at);
        if let Some(refs) = &c.chunk_refs {
            for r in refs {
                println!("        chunk {r}");
            }
        }
        if let Some(rels) = &c.relfork_metas {
            for m in rels {
                println!(
                    "        relfork {} nblocks={} deleted={}",
                    m.fork, m.nblocks, m.deleted
                );
            }
        }
    }
}

fn run(args: &Args) -> Result<(), String> {
    let (compressed_size, uncompressed_size, seg) = load(&args.path)?;
    let dto = build_dto(&seg, compressed_size, uncompressed_size, args.verbose);
    if args.json {
        let s =
            serde_json::to_string_pretty(&dto).map_err(|e| format!("json encode error: {e}"))?;
        println!("{s}");
    } else {
        print_human(&dto);
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

#[cfg(test)]
mod tests {
    use super::*;
    use core::timeline::{Checkpoint, CheckpointSummary, SegmentId};
    use pgsys::lsn::Lsn;
    use pgsys::timeline_id::TimelineId;
    use std::collections::{HashMap, HashSet};

    #[test]
    fn loads_and_dumps_an_autodetected_segment() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("00000000.segment");
        let tl = TimelineId::new(1);
        let mut seg = TimelineSegment::new(SegmentId {
            timeline_id: tl,
            index: 0,
        });
        let mut summary = CheckpointSummary::new(
            Checkpoint::new(tl, Lsn::new(100)),
            Checkpoint::default(),
            Checkpoint::default(),
            HashSet::new(),
            HashMap::new(),
        );
        summary.chunks.insert(ChunkTag {
            spc_oid: 1663,
            db_oid: 5,
            rel_number: 2619,
            fork_number: 0,
            chunk_id: 1,
        });
        seg.push(summary);

        let encoded = zstd::encode_all(seg.to_bytes().unwrap().as_slice(), 1).unwrap();
        fs::write(&path, &encoded).unwrap();

        let (compressed_size, uncompressed_size, loaded) = load(&path).unwrap();
        assert_eq!(compressed_size, encoded.len());
        assert_eq!(uncompressed_size, seg.to_bytes().unwrap().len());

        let dto = build_dto(&loaded, compressed_size, uncompressed_size, true);
        assert_eq!(dto.checkpoints.len(), 1);
        assert_eq!(dto.checkpoints[0].chunks, 1);
        assert_eq!(dto.checkpoints[0].chunk_refs.as_ref().unwrap().len(), 1);
        assert!(dto.checkpoints[0].relfork_metas.is_some());
    }

    #[test]
    fn load_accepts_an_uncompressed_segment() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("00000000.segment");
        let tl = TimelineId::new(1);
        let seg = TimelineSegment::new(SegmentId {
            timeline_id: tl,
            index: 0,
        });
        fs::write(&path, seg.to_bytes().unwrap()).unwrap();

        let (_, _, loaded) = load(&path).unwrap();
        assert!(loaded.checkpoints.is_empty());
    }
}
