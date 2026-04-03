use core::{
    project::{ProjectMeta, ProjectNamespace, build_initial_manifest},
    store::Store,
};
use pgsys::Lsn;

pub fn run(sim: &Store, org: u64, project: u64, branch: u64, lsn: &str) {
    let ns = ProjectNamespace::new(org, project, branch);
    let restore_lsn = Lsn::parse_either(lsn).unwrap_or_else(|e| {
        eprintln!("error: {e}");
        std::process::exit(1);
    });

    let mut meta = ProjectMeta::load(sim, &ns).unwrap_or_else(|e| {
        eprintln!("error: failed to load project meta: {e}");
        std::process::exit(1);
    });

    let old_timeline = meta.current_timeline_id;
    let new_timeline = old_timeline + 1;

    // Build the restored base manifest from the project's own PITR history.
    let local_path = std::env::temp_dir().join(format!(
        "tiko_restore_{project}_{branch}_{}.tikm",
        restore_lsn.to_hex()
    ));
    let manifest = build_initial_manifest(sim, &ns, old_timeline, restore_lsn, &local_path)
        .unwrap_or_else(|e| {
            eprintln!(
                "error: failed to build manifest at lsn {}: {e}",
                restore_lsn.to_hex()
            );
            std::process::exit(1);
        });

    // Write the new base manifest under the new timeline.
    let manifest_bytes = manifest.to_bytes().unwrap_or_else(|e| {
        eprintln!("error: failed to serialize manifest: {e}");
        std::process::exit(1);
    });
    sim.put_standard(
        &ns.base_manifest_key(new_timeline, restore_lsn),
        &manifest_bytes,
    )
    .unwrap_or_else(|e| {
        eprintln!("error: failed to write manifest: {e}");
        std::process::exit(1);
    });

    // Clear express-bucket hot data — stale after restore.
    let express_prefix = format!("{}/{}/", ns.org_id, ns.project_id);
    let express_keys = sim
        .list_prefix_express(&express_prefix)
        .unwrap_or_else(|e| {
            eprintln!("error: failed to list express keys: {e}");
            std::process::exit(1);
        });
    for key in express_keys {
        sim.delete_express(&key).unwrap_or_else(|e| {
            eprintln!("error: failed to delete express key {key}: {e}");
            std::process::exit(1);
        });
    }

    // Update project.json with the new timeline ID.
    meta.current_timeline_id = new_timeline;
    sim.put_standard(
        &ns.project_meta_key(),
        &serde_json::to_vec(&meta).unwrap_or_else(|e| {
            eprintln!("error: failed to serialize project meta: {e}");
            std::process::exit(1);
        }),
    )
    .unwrap_or_else(|e| {
        eprintln!("error: failed to write project meta: {e}");
        std::process::exit(1);
    });

    println!(
        "{}",
        serde_json::to_string_pretty(&ProjectMeta::load(sim, &ns).unwrap()).unwrap()
    );
}
