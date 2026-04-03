use core::org::OrgMeta;
use core::s3_sim::S3Sim;

pub fn run(sim: &S3Sim, org: u64, force: bool) {
    match OrgMeta::delete(sim, org, force) {
        Ok(meta) => println!("{}", serde_json::to_string_pretty(&meta).unwrap()),
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(1);
        }
    }
}
