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
) {
    let ns = ProjectNamespace::new(org, project, branch);
    let parent_ns = ProjectNamespace::new(org, parent_project, parent_branch);
    let branch_lsn = Lsn::parse_either(lsn).unwrap_or_else(|e| {
        eprintln!("error: {e}");
        std::process::exit(1);
    });

    let parent_meta = ProjectMeta::load(&sim, &parent_ns).unwrap_or_else(|e| {
        eprintln!("error: failed to load parent project meta: {e}");
        std::process::exit(1);
    });

    create_branch(
        &sim,
        &parent_ns,
        parent_meta.current_timeline_id,
        &ns,
        branch_lsn,
    )
    .unwrap_or_else(|e| {
        eprintln!("error: failed to create branch: {e}");
        std::process::exit(1);
    });

    println!(
        "{}",
        serde_json::to_string_pretty(&ProjectMeta::load(&sim, &ns).unwrap()).unwrap()
    );
}
