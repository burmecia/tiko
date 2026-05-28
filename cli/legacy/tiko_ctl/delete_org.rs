use core::org::OrgMeta;
use core::store::Store;

pub fn run(sim: &Store, org: u64, force: bool) {
    match OrgMeta::delete(sim, org, force) {
        Ok(meta) => println!("{}", serde_json::to_string_pretty(&meta).unwrap()),
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(1);
        }
    }
}
