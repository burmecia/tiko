use std::fs;
use std::path::Path;
use std::process::Command;

use pgsys::Lsn;
use store::{
    project::{ProjectMeta, ProjectNamespace, create_branch},
    recovery::prepare_recovery,
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
    let target_lsn = Lsn::parse_either(lsn).unwrap_or_else(|e| {
        eprintln!("error: {e}");
        std::process::exit(1);
    });

    let parent_meta = ProjectMeta::load(sim, &parent_ns).unwrap_or_else(|e| {
        eprintln!("error: failed to load parent project meta: {e}");
        std::process::exit(1);
    });

    // Create the new branch in store
    let child_meta = create_branch(
        sim,
        &parent_ns,
        parent_meta.current_timeline_id,
        &ns,
        target_lsn,
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

    // ── Phase 2: prepare for recovery ───────
    prepare_recovery(
        sim,
        &parent_ns,
        pg_data,
        parent_meta.current_timeline_id,
        target_lsn,
    )
    .unwrap_or_else(|e| {
        eprintln!("error: failed to prepare recovery in store: {e}");
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
