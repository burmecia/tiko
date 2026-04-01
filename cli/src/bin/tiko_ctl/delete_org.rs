use core::org::OrgMeta;
use core::sim_store::SimStore;

pub fn run(sim: &SimStore, org: u64, force: bool) {
    match OrgMeta::delete(sim, org, force) {
        Ok(meta) => println!("{}", serde_json::to_string_pretty(&meta).unwrap()),
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(1);
        }
    }
}
