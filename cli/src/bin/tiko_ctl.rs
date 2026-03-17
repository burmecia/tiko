use clap::{Parser, Subcommand};
use std::path::PathBuf;
use store::org::OrgMeta;
use store::sim_store::SimStore;

#[derive(Parser)]
#[command(name = "tiko_ctl", about = "Tiko control CLI tools")]
struct Cli {
    /// Path to SimStore root directory
    #[arg(
        long,
        global = true,
        value_name = "PATH",
        value_hint = clap::ValueHint::DirPath,
        env = "TIKO_SIM_STORE"
    )]
    sim_store: Option<PathBuf>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Create an org (also creates the root project)
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
    let cli = Cli::parse();
    let Some(sim_store_path) = cli.sim_store.as_deref() else {
        eprintln!("error: missing required '--sim-store <PATH>' (or set TIKO_SIM_STORE)");
        std::process::exit(2);
    };
    let sim = SimStore::init(sim_store_path);

    match cli.command {
        Command::CreateOrg { org } => match OrgMeta::create(&sim, org) {
            Ok(meta) => println!("{}", serde_json::to_string_pretty(&meta).unwrap()),
            Err(e) => {
                eprintln!("error: {e}");
                std::process::exit(1);
            }
        },
        Command::DeleteOrg { org, force } => match OrgMeta::delete(&sim, org, force) {
            Ok(meta) => println!("{}", serde_json::to_string_pretty(&meta).unwrap()),
            Err(e) => {
                eprintln!("error: {e}");
                std::process::exit(1);
            }
        },
    }
}
