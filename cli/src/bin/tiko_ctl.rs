use clap::{Parser, Subcommand};
use store::sim_store::SimStore;

#[derive(Parser)]
#[command(name = "tiko_ctl", about = "Tiko control CLI tools")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Create an org (also creates the root project atomically)
    CreateOrg {
        #[arg(long)]
        org: u64,
    },
    /// Soft-delete an org
    DeleteOrg {
        #[arg(long)]
        org: u64,
        #[arg(long)]
        force: bool,
    },
}

fn main() {
    let _cli = Cli::parse();
    let _sim = SimStore::new(std::path::Path::new("/usr/local/tiko/sim_store")); // TODO: configurable path

    // match cli.command {
    //     Command::CreateOrg { org } => match org::create_org(&sim, org) {
    //         Ok(meta) => println!("{}", serde_json::to_string_pretty(&meta).unwrap()),
    //         Err(e) => {
    //             eprintln!("error: {e}");
    //             std::process::exit(1);
    //         }
    //     },
    //     Command::DeleteOrg { org, force } => match org::delete_org(&sim, org, force) {
    //         Ok(meta) => println!("{}", serde_json::to_string_pretty(&meta).unwrap()),
    //         Err(e) => {
    //             eprintln!("error: {e}");
    //             std::process::exit(1);
    //         }
    //     },
    // }
}
