use std::fs;
use std::path::Path;
use std::process::Command;
use store::sim_store::SimStore;

pub fn run(pg_bindir: &Path, output: &Path, sim_store: Option<&Path>) {
    let work = tempfile::tempdir().unwrap_or_else(|e| {
        eprintln!("error: failed to create temp dir: {e}");
        std::process::exit(1);
    });
    let tiko_root = work.path().join("tiko");
    let pgdata = work.path().join("pgdata");

    // ── 1. initdb ─────────────────────────────────────────────────────────────
    let status = Command::new(pg_bindir.join("initdb"))
        .args(["-D", pgdata.to_str().unwrap(), "--data-checksums"])
        .env("TIKO_ROOT_PATH", tiko_root.to_str().unwrap())
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

    // ── 2. Strip pg_control (restored from pg_state.tar.zst at recover time) ─
    fs::remove_file(pgdata.join("global/pg_control")).unwrap_or_else(|e| {
        eprintln!("error: {e}");
        std::process::exit(1);
    });

    // ── 3. Strip WAL and transactional state contents (keep directories) ──────
    for dir in [
        "pg_wal",
        "pg_xact",
        "pg_commit_ts",
        "pg_multixact/members",
        "pg_multixact/offsets",
    ] {
        remove_dir_contents(&pgdata.join(dir));
    }

    // ── 4. Strip relation files; keep pg_filenode.map and pg_internal.init ────
    strip_relation_files(&pgdata.join("global"), 1);
    strip_relation_files(&pgdata.join("base"), 2);

    // ── 5. Create tarball ─────────────────────────────────────────────────────
    if let Some(parent) = output.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).unwrap_or_else(|e| {
                eprintln!("error: {e}");
                std::process::exit(1);
            });
        }
    }
    let status = Command::new("tar")
        .args([
            "-czf",
            output.to_str().unwrap(),
            "-C",
            work.path().to_str().unwrap(),
            ".",
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

    let size = fs::metadata(output)
        .map(|m| format_size(m.len()))
        .unwrap_or_else(|_| "?".into());
    println!("Created {} ({})", output.display(), size);

    // ── 6. Upload to SimStore ─────────────────────────────────────────────────
    if let Some(sim_path) = sim_store {
        let filename = output
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_else(|| {
                eprintln!("error: could not determine filename from output path");
                std::process::exit(1);
            });
        let data = fs::read(output).unwrap_or_else(|e| {
            eprintln!("error: failed to read {}: {e}", output.display());
            std::process::exit(1);
        });
        let sim = SimStore::init(sim_path);
        sim.put_template(filename, &data).unwrap_or_else(|e| {
            eprintln!("error: SimStore upload failed: {e}");
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
                if name != "pg_filenode.map" && name != "pg_internal.init" {
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
