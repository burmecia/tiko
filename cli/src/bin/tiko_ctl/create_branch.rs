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
    tiko_root: &Path,
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

    if parent_project == 0 && parent_branch == 0 {
        // ── Phase 2 (root project): copy latest parent base manifest ─────────
        // The root project uses a shutdown checkpoint — PostgreSQL archive
        // recovery cannot target it. Instead, find the latest base manifest
        // with base_lsn ≤ target_lsn, copy it into the child's namespace as
        // the initial base, and start PostgreSQL normally (no recovery.signal).
        let base_prefix = parent_ns.base_prefix_for_timeline(parent_meta.current_timeline_id);
        let base_keys = sim.list_prefix_standard(&base_prefix).unwrap_or_else(|e| {
            eprintln!("error: failed to list parent base manifests: {e}");
            std::process::exit(1);
        });

        let (chosen_lsn, best_key) = base_keys
            .iter()
            .filter_map(|k| {
                let rel = k.strip_prefix(&base_prefix)?;
                let lsn_hex = rel.split('/').next()?;
                let lsn = Lsn::from_hex(lsn_hex).ok()?;
                (lsn <= target_lsn).then_some((lsn, k))
            })
            .max_by_key(|(lsn, _)| *lsn)
            .unwrap_or_else(|| {
                eprintln!("error: no base manifest with lsn ≤ {}", target_lsn.to_hex());
                std::process::exit(1);
            });

        let manifest_bytes = sim
            .get_standard(best_key)
            .unwrap_or_else(|e| {
                eprintln!("error: failed to read parent base manifest: {e}");
                std::process::exit(1);
            })
            .unwrap_or_else(|| {
                eprintln!("error: parent base manifest not found: {best_key}");
                std::process::exit(1);
            });

        sim.put_standard(&ns.base_manifest_key(1, target_lsn), &manifest_bytes)
            .unwrap_or_else(|e| {
                eprintln!("error: failed to upload initial base manifest: {e}");
                std::process::exit(1);
            });

        // Copy nblocks express keys from parent to child.
        // `cached_file_nblocks` reads from `{org}/{project_id}/chunks/{tag}/nblocks`
        // in the express bucket. A new branch has no express data, so without
        // this copy every relation appears to have 0 blocks and pg_authid is empty.
        let parent_chunks_prefix = format!("{}/{}/chunks/", parent_ns.org_id, parent_ns.project_id);
        let child_chunks_prefix = format!("{}/{}/chunks/", ns.org_id, ns.project_id);
        let nblocks_keys = sim
            .list_prefix_express(&parent_chunks_prefix)
            .unwrap_or_else(|e| {
                eprintln!("error: failed to list parent nblocks keys: {e}");
                std::process::exit(1);
            });
        for key in &nblocks_keys {
            if key.ends_with("/nblocks") {
                if let Ok(Some(bytes)) = sim.get_express(key) {
                    let child_key = format!(
                        "{}{}",
                        child_chunks_prefix,
                        &key[parent_chunks_prefix.len()..]
                    );
                    sim.put_express(&child_key, &bytes).unwrap_or_else(|e| {
                        eprintln!("error: failed to copy nblocks key {key}: {e}");
                        std::process::exit(1);
                    });
                }
            }
        }

        // Extract the checkpoint's pg_state.tar.zst (pg_control, pg_xact, …)
        // into pgdata so PostgreSQL starts with a consistent control file.
        let pg_state_key = parent_ns.pg_state_key(parent_meta.current_timeline_id, chosen_lsn);
        let pg_state_bytes = sim
            .get_standard(&pg_state_key)
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
            parent_meta.current_timeline_id,
            target_lsn,
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
