use std::fs;
use std::path::Path;
use std::process::Command;

use core::{
    project::{ProjectMeta, ProjectNamespace, create_branch},
    recovery::prepare_recovery,
    store::Store,
};
use pgsys::Lsn;

pub fn run(
    sim: &Store,
    org: u64,
    project: u64,
    branch: u64,
    parent_project: u64,
    parent_branch: u64,
    parent_pgdata: Option<&Path>,
    template: &str,
    pg_data: &Path,
    tiko_root: &Path,
) {
    let ns = ProjectNamespace::new(org, project, branch);
    let parent_ns = ProjectNamespace::new(org, parent_project, parent_branch);
    let tl = {
        let meta = ProjectMeta::load(sim, &parent_ns).unwrap_or_else(|e| {
            eprintln!("error: failed to load parent project meta: {e}");
            std::process::exit(1);
        });
        meta.current_timeline_id
    };

    // ── Determine branch LSN (latest checkpoint on parent's timeline) ─────────
    let branch_lsn = if parent_project == 0 && parent_branch == 0 {
        // Root project: latest base manifest LSN (shutdown checkpoint).
        let base_prefix = parent_ns.base_prefix_for_timeline(tl);
        let base_keys = sim.storage_list_prefix(&base_prefix).unwrap_or_else(|e| {
            eprintln!("error: failed to list parent base manifests: {e}");
            std::process::exit(1);
        });
        base_keys
            .iter()
            .filter_map(|k| {
                let rel = k.strip_prefix(&base_prefix)?;
                let lsn_hex = rel.split('/').next()?;
                Lsn::from_hex(lsn_hex).ok()
            })
            .max()
            .unwrap_or_else(|| {
                eprintln!("error: no base manifests found for parent");
                std::process::exit(1);
            })
    } else {
        // Non-root project: latest delta manifest LSN (online checkpoint).
        let delta_prefix = parent_ns.delta_prefix_for_timeline(tl);
        let delta_keys = sim.storage_list_prefix(&delta_prefix).unwrap_or_else(|e| {
            eprintln!("error: failed to list parent delta manifests: {e}");
            std::process::exit(1);
        });
        delta_keys
            .iter()
            .filter_map(|k| {
                let rest = k.strip_prefix(&delta_prefix)?;
                let lsn_hex = rest.split('/').next()?;
                Lsn::from_hex(lsn_hex).ok()
            })
            .max()
            .unwrap_or_else(|| {
                eprintln!("error: no checkpoints found for parent");
                std::process::exit(1);
            })
    };

    // ── Verify branch point checkpoint is complete ────────────────────────────
    // Refuse to create a branch from an incomplete checkpoint. An incomplete
    // checkpoint (flush_failures > 0) means some dirty chunks never reached
    // the standard bucket; for non-root branches WAL replay would reconstruct
    // them, but for root-to-child branches no WAL replay runs at all, which
    // would produce silent data corruption in the new branch.
    {
        let manifest_key = if parent_project == 0 && parent_branch == 0 {
            parent_ns.base_manifest_key(tl, branch_lsn)
        } else {
            parent_ns.delta_manifest_key(tl, branch_lsn)
        };
        if let Ok(Some(bytes)) = sim.storage_get(&manifest_key) {
            let tmp = tempfile::NamedTempFile::new().unwrap_or_else(|e| {
                eprintln!("error: failed to create temp file: {e}");
                std::process::exit(1);
            });
            if let Ok(m) = core::manifest::Manifest::from_bytes(&bytes, tmp.path()) {
                let failures = m.flush_failures();
                if failures > 0 {
                    eprintln!(
                        "error: branch point checkpoint at lsn={} has {failures} incomplete \
                         chunk flush(es); branching from an incomplete checkpoint risks \
                         silent data loss. Run another checkpoint on the parent and retry.",
                        branch_lsn.to_hex()
                    );
                    std::process::exit(1);
                }
            }
        }
    }

    // ── Register the branch in the store ─────────────────────────────────────
    let child_meta = create_branch(sim, &parent_ns, tl, &ns, branch_lsn).unwrap_or_else(|e| {
        eprintln!("error: failed to create branch: {e}");
        std::process::exit(1);
    });

    // ── Phase 1: extract `pgdata/` from template tarball → pg_data ───────────
    // Extract the tarball into a temp dir, then rename temp/pgdata → pg_data so
    // that the contents of `pgdata/` land directly at `pg_data` (not nested).
    let tarball = sim
        .get_template(template)
        .unwrap_or_else(|e| {
            eprintln!("error: failed to read template from Store: {e}");
            std::process::exit(1);
        })
        .unwrap_or_else(|| {
            eprintln!("error: template '{template}' not found in Store");
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

    if parent_project == 0 && parent_branch == 0 {
        // ── Phase 2 (root project): copy latest base manifest ────────────────
        // The root project uses a shutdown checkpoint — PostgreSQL archive
        // recovery cannot target it. Copy the base manifest and pg_state into
        // the child's namespace, then start PostgreSQL normally (no recovery.signal).
        let manifest_key = parent_ns.base_manifest_key(tl, branch_lsn);
        let manifest_bytes = sim
            .storage_get(&manifest_key)
            .unwrap_or_else(|e| {
                eprintln!("error: failed to read parent base manifest: {e}");
                std::process::exit(1);
            })
            .unwrap_or_else(|| {
                eprintln!("error: parent base manifest not found: {manifest_key}");
                std::process::exit(1);
            });

        sim.storage_put(&ns.base_manifest_key(1, branch_lsn), &manifest_bytes)
            .unwrap_or_else(|e| {
                eprintln!("error: failed to upload initial base manifest: {e}");
                std::process::exit(1);
            });

        // Extract the checkpoint's pg_state.tar.zst (pg_control, pg_xact, …)
        // into pgdata so PostgreSQL starts with a consistent control file.
        let pg_state_key = parent_ns.pg_state_key(tl, branch_lsn);
        let pg_state_bytes = sim
            .storage_get(&pg_state_key)
            .unwrap_or_else(|e| {
                eprintln!("error: failed to read pg_state: {e}");
                std::process::exit(1);
            })
            .unwrap_or_else(|| {
                eprintln!("error: pg_state not found: {pg_state_key}");
                std::process::exit(1);
            });

        let pg_state_tmp = work.path().join("pg_state.tar.zst");
        fs::write(&pg_state_tmp, &pg_state_bytes).unwrap_or_else(|e| {
            eprintln!("error: failed to write pg_state archive: {e}");
            std::process::exit(1);
        });
        extract_tar(&pg_state_tmp, pg_data);
    } else {
        // ── Phase 2 (non-root): prepare for archive recovery ─────────────────
        prepare_recovery(
            sim,
            &parent_ns,
            pg_data,
            tiko_root,
            tl,
            branch_lsn,
            parent_pgdata,
        )
        .unwrap_or_else(|e| {
            eprintln!("error: failed to prepare recovery in store: {e}");
            std::process::exit(1);
        });
    }

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
