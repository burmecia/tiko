use std::fs;
use std::process::Command;
use store::org::OrgMeta;
use store::sim_store::SimStore;

/// Source org_id baked into the template by `make_template` (initdb runs with TIKO_ORG_ID=0).
const TEMPLATE_ORG_ID: u64 = 0;

pub fn run(sim: &SimStore, org_id: u64, template: &str) {
    // ── 1. Retrieve template tarball from SimStore ─────────────────────────────
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

    // ── 2. Extract tarball to a temp directory ────────────────────────────────
    let work = tempfile::tempdir().unwrap_or_else(|e| {
        eprintln!("error: failed to create temp dir: {e}");
        std::process::exit(1);
    });
    let tarball_path = work.path().join(template);
    fs::write(&tarball_path, &tarball).unwrap_or_else(|e| {
        eprintln!("error: {e}");
        std::process::exit(1);
    });
    let status = Command::new("tar")
        .args([
            "-xf",
            tarball_path.to_str().unwrap(),
            "-C",
            work.path().to_str().unwrap(),
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

    // ── 3. Copy SimStore data from template (org=0) → new org ─────────────────
    // The template tarball includes `tiko/sim/{standard,express}/0/...` written
    // by initdb running with TIKO_ORG_ID=0. Re-key all those objects to `{org}/`.
    let tiko_sim = work.path().join("tiko").join("sim");
    sim.copy_org_data(
        &tiko_sim.join("standard"),
        &tiko_sim.join("express"),
        TEMPLATE_ORG_ID,
        org_id,
    )
    .unwrap_or_else(|e| {
        eprintln!("error: failed to copy template data: {e}");
        std::process::exit(1);
    });

    // ── 4. Create org metadata (org.json + root project.json) ─────────────────
    match OrgMeta::create(sim, org_id) {
        Ok(meta) => println!("{}", serde_json::to_string_pretty(&meta).unwrap()),
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(1);
        }
    }
}
