use core::s3_sim::S3Sim;
use pgsys::common::PG_VERSION_NUM;
use std::fs;
use std::path::Path;
use std::process::Command;

pub fn run(pg_bindir: &Path, tiko_root: Option<&Path>) {
    let temp_dir = tempfile::tempdir().unwrap_or_else(|e| {
        eprintln!("error: failed to create temp dir: {e}");
        std::process::exit(1);
    });
    let output = temp_dir
        .path()
        .join(format!("template-{}.tar.gz", PG_VERSION_NUM));
    let work = temp_dir.path().join("work");
    let store_root = work.join("store");
    let pgdata = work.join("pgdata");

    // ── 1. initdb ─────────────────────────────────────────────────────────────
    let status = Command::new(pg_bindir.join("initdb"))
        .args(["-D", pgdata.to_str().unwrap(), "--data-checksums"])
        .env("TIKO_ROOT_PATH", store_root.to_str().unwrap())
        .env("TIKO_ORG_ID", "0")
        .env("TIKO_PROJECT_ID", "0")
        .env("TIKO_BRANCH_ID", "0")
        .status()
        .unwrap_or_else(|e| {
            eprintln!("error: failed to run initdb: {e}");
            std::process::exit(1);
        });
    if !status.success() {
        eprintln!("error: initdb exited with {status}");
        std::process::exit(1);
    }

    // ── 2. Remove transient store artefacts left by initdb ────────────────────
    // dirty_chunks/ holds sidecar files written during initdb. The checkpoint
    // that runs at the end of initdb deletes the referenced sidecars, but
    // intermediate sidecars for the same chunk (one per block write) are
    // cleaned up during log parse. Any remaining files are safe to drop here
    // because the checkpoint has already uploaded the data to the S3Sim.
    let dirty_chunks_dir = store_root.join("dirty_chunks");
    if dirty_chunks_dir.exists() {
        fs::remove_dir_all(&dirty_chunks_dir).unwrap_or_else(|e| {
            eprintln!("error: failed to remove dirty_chunks: {e}");
            std::process::exit(1);
        });
    }

    // ── 3. Create postgresql.tiko.conf and include it from postgresql.conf ──────
    let tiko_conf = r#"# Tiko storage manager settings
shared_preload_libraries = 'libtikoworker'
log_min_messages = debug1

# WAL streaming settings
wal_level = replica
max_wal_senders = 2
max_replication_slots = 2
max_slot_wal_keep_size = 1GB
"#;
    fs::write(pgdata.join("postgresql.tiko.conf"), tiko_conf).unwrap_or_else(|e| {
        eprintln!("error: failed to write postgresql.tiko.conf: {e}");
        std::process::exit(1);
    });

    let mut pg_conf = fs::read_to_string(pgdata.join("postgresql.conf")).unwrap_or_else(|e| {
        eprintln!("error: failed to read postgresql.conf: {e}");
        std::process::exit(1);
    });
    pg_conf.push_str("\ninclude 'postgresql.tiko.conf'\n");
    fs::write(pgdata.join("postgresql.conf"), &pg_conf).unwrap_or_else(|e| {
        eprintln!("error: failed to write postgresql.conf: {e}");
        std::process::exit(1);
    });

    // ── 4. Strip pg_control (restored from pg_state.tar.zst at recovery time) ─
    fs::remove_file(pgdata.join("global/pg_control")).unwrap_or_else(|e| {
        eprintln!("error: {e}");
        std::process::exit(1);
    });

    // ── 5. Strip transactional state contents (keep directories) ─────────────
    // pg_wal is intentionally kept: the initial WAL segment written by initdb
    // must be present so that branches created from this template can start PG
    // without needing a restore_command to fetch the very first segment.
    for dir in [
        "pg_xact",
        "pg_commit_ts",
        "pg_multixact/members",
        "pg_multixact/offsets",
    ] {
        remove_dir_contents(&pgdata.join(dir));
    }

    // ── 6. Strip relation files; keep pg_filenode.map and pg_internal.init ────
    strip_relation_files(&pgdata.join("global"), 1);
    strip_relation_files(&pgdata.join("base"), 2);

    // ── 7. Create tarball ─────────────────────────────────────────────────────
    let status = Command::new("tar")
        .args([
            "-czf",
            output.to_str().unwrap(),
            "-C",
            work.to_str().unwrap(),
            "./",
        ])
        .status()
        .unwrap_or_else(|e| {
            eprintln!("error: failed to run tar: {e}");
            std::process::exit(1);
        });
    if !status.success() {
        eprintln!("error: tar exited with {status}");
        std::process::exit(1);
    }

    let size = fs::metadata(&output)
        .map(|m| format_size(m.len()))
        .unwrap_or_else(|_| "?".into());
    println!(
        "Created {} ({})",
        output.file_name().unwrap().to_str().unwrap(),
        size
    );

    // ── 8. Upload to S3Sim ─────────────────────────────────────────────────
    if let Some(root) = tiko_root {
        let filename = output
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_else(|| {
                eprintln!("error: could not determine filename from output path");
                std::process::exit(1);
            });
        let data = fs::read(&output).unwrap_or_else(|e| {
            eprintln!("error: failed to read {}: {e}", output.display());
            std::process::exit(1);
        });
        let sim = S3Sim::init(root);
        sim.put_template(filename, &data).unwrap_or_else(|e| {
            eprintln!("error: S3Sim upload failed: {e}");
            std::process::exit(1);
        });
        println!("Stored  {}", filename);
    }
}

/// Delete all files (not subdirectories) directly inside `dir`.
fn remove_dir_contents(dir: &Path) {
    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_file() {
            let _ = fs::remove_file(&path);
        }
    }
}

/// Remove files not named `pg_filenode.map` or `pg_internal.init`.
/// `depth` controls how many directory levels to descend before filtering files:
///   1 = files directly in `dir` (for `global/`)
///   2 = files one level down (for `base/*/`)
fn strip_relation_files(dir: &Path, depth: usize) {
    if depth == 1 {
        let entries = match fs::read_dir(dir) {
            Ok(e) => e,
            Err(_) => return,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() {
                let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                if name != "pg_filenode.map" && name != "pg_internal.init" && name != "PG_VERSION" {
                    let _ = fs::remove_file(&path);
                }
            }
        }
    } else {
        let entries = match fs::read_dir(dir) {
            Ok(e) => e,
            Err(_) => return,
        };
        for entry in entries.flatten() {
            if entry.path().is_dir() {
                strip_relation_files(&entry.path(), depth - 1);
            }
        }
    }
}

fn format_size(bytes: u64) -> String {
    if bytes >= 1024 * 1024 {
        format!("{:.1}M", bytes as f64 / (1024.0 * 1024.0))
    } else if bytes >= 1024 {
        format!("{:.1}K", bytes as f64 / 1024.0)
    } else {
        format!("{bytes}B")
    }
}
