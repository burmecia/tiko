use std::fs;
use std::path::Path;
use std::process::Command;

use pgsys::Lsn;
use store::{
    project::{ProjectMeta, ProjectNamespace, create_branch},
    sim_store::SimStore,
};

pub fn run(
    sim: &SimStore,
    org: u64,
    project: u64,
    branch: u64,
    parent_project: u64,
    parent_branch: u64,
    lsn: &str,
    template: &str,
    pg_data: &Path,
) {
    let ns = ProjectNamespace::new(org, project, branch);
    let parent_ns = ProjectNamespace::new(org, parent_project, parent_branch);
    let branch_lsn = Lsn::parse_either(lsn).unwrap_or_else(|e| {
        eprintln!("error: {e}");
        std::process::exit(1);
    });

    let parent_meta = ProjectMeta::load(sim, &parent_ns).unwrap_or_else(|e| {
        eprintln!("error: failed to load parent project meta: {e}");
        std::process::exit(1);
    });

    // Create the new branch in store
    create_branch(
        sim,
        &parent_ns,
        parent_meta.current_timeline_id,
        &ns,
        branch_lsn,
    )
    .unwrap_or_else(|e| {
        eprintln!("error: failed to create branch: {e}");
        std::process::exit(1);
    });

    // ── Phase 1: extract `pgdata/` from template tarball → pg_data ───────────
    // Extract the tarball into a temp dir, then rename temp/pgdata → pg_data so
    // that the contents of `pgdata/` land directly at `pg_data` (not nested).
    let tarball = sim
        .get_template(template)
        .unwrap_or_else(|e| {
            eprintln!("error: failed to read template from SimStore: {e}");
            std::process::exit(1);
        })
        .unwrap_or_else(|| {
            eprintln!("error: template '{template}' not found in SimStore");
            std::process::exit(1);
        });

    let work = tempfile::tempdir().unwrap_or_else(|e| {
        eprintln!("error: failed to create temp dir: {e}");
        std::process::exit(1);
    });
    let tarball_path = work.path().join(template);
    fs::write(&tarball_path, &tarball).unwrap_or_else(|e| {
        eprintln!("error: failed to write template archive: {e}");
        std::process::exit(1);
    });
    extract_tar(&tarball_path, work.path());

    fs::rename(work.path().join("pgdata"), pg_data).unwrap_or_else(|e| {
        eprintln!(
            "error: failed to move pgdata to '{}': {e}",
            pg_data.display()
        );
        std::process::exit(1);
    });

    // ── Phase 2: overwrite with pg_state from parent checkpoint → pg_data ─────
    let pg_state_key = parent_ns.pg_state_key(parent_meta.current_timeline_id, branch_lsn);
    let pg_state_bytes = sim
        .get_standard(&pg_state_key)
        .unwrap_or_else(|e| {
            eprintln!("error: failed to read pg_state from store: {e}");
            std::process::exit(1);
        })
        .unwrap_or_else(|| {
            eprintln!("error: pg_state not found at '{pg_state_key}'");
            std::process::exit(1);
        });

    let pg_state_tmp = pg_data.join("pg_state.tar.zst");
    fs::write(&pg_state_tmp, &pg_state_bytes).unwrap_or_else(|e| {
        eprintln!("error: failed to write pg_state archive: {e}");
        std::process::exit(1);
    });
    extract_tar(&pg_state_tmp, pg_data);
    let _ = fs::remove_file(&pg_state_tmp);

    // ── Write recovery_manifest.bin for the child branch ─────────────────────
    let tiko_root = pg_data.join("tiko");
    fs::create_dir_all(&tiko_root).unwrap_or_else(|e| {
        eprintln!("error: failed to create tiko dir: {e}");
        std::process::exit(1);
    });

    let child_meta = ProjectMeta::load(sim, &ns).unwrap_or_else(|e| {
        eprintln!("error: failed to load child project meta: {e}");
        std::process::exit(1);
    });
    let base_manifest_key = ns.base_manifest_key(child_meta.current_timeline_id, branch_lsn);
    let manifest_bytes = sim
        .get_standard(&base_manifest_key)
        .unwrap_or_else(|e| {
            eprintln!("error: failed to read base manifest: {e}");
            std::process::exit(1);
        })
        .unwrap_or_else(|| {
            eprintln!("error: base manifest not found at '{base_manifest_key}'");
            std::process::exit(1);
        });

    let recovery_manifest_path = tiko_root.join("recovery_manifest.bin");
    fs::write(&recovery_manifest_path, &manifest_bytes).unwrap_or_else(|e| {
        eprintln!("error: failed to write recovery_manifest.bin: {e}");
        std::process::exit(1);
    });

    println!("{}", serde_json::to_string_pretty(&child_meta).unwrap());
}

fn extract_tar(archive: &Path, dest: &Path) {
    let status = Command::new("tar")
        .args([
            "-xf",
            &archive.to_string_lossy(),
            "-C",
            &dest.to_string_lossy(),
        ])
        .status()
        .unwrap_or_else(|e| {
            eprintln!("error: failed to run tar: {e}");
            std::process::exit(1);
        });
    if !status.success() {
        eprintln!(
            "error: tar exited with {status} for '{}'",
            archive.display()
        );
        std::process::exit(1);
    }
}
