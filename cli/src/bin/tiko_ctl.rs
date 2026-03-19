#[path = "tiko_ctl/create_org.rs"]
mod create_org;
#[path = "tiko_ctl/delete_org.rs"]
mod delete_org;
#[path = "tiko_ctl/make_template.rs"]
mod make_template;

use clap::{Parser, Subcommand};
use std::path::{Path, PathBuf};
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
    command: Command_,
}

#[derive(Subcommand)]
enum Command_ {
    /// Create an org (also creates the root project).
    ///
    /// Finds the named template in the SimStore, extracts it, copies the
    /// embedded SimStore data into the org's namespace, then writes org.json
    /// and project.json.
    CreateOrg {
        #[arg(long)]
        org: u64,
        /// Template filename to seed the org from (e.g. template-18.tar.gz)
        #[arg(long, value_name = "FILE")]
        template: String,
    },
    /// Soft-delete an org
    DeleteOrg {
        #[arg(long)]
        org: u64,
        #[arg(long)]
        force: bool,
    },
    /// Create a PGDATA template tarball and store it in the SimStore standard bucket.
    ///
    /// Runs `initdb`, strips relation files and transactional state, tarballs the result,
    /// and uploads it to `standard/template/<output-filename>`.
    MakeTemplate {
        /// Directory containing the PostgreSQL binaries (initdb, postgres)
        #[arg(long, value_name = "DIR", value_hint = clap::ValueHint::DirPath)]
        pg_bindir: PathBuf,
        /// Output tarball path (e.g. template-18.tar.gz); basename is used as the SimStore key
        #[arg(long, value_name = "FILE", value_hint = clap::ValueHint::FilePath)]
        output: PathBuf,
    },
}

fn require_sim(sim_store: Option<&Path>, op: &str) -> &'static SimStore {
    let path = sim_store.unwrap_or_else(|| {
        eprintln!("error: {op} requires '--sim-store <PATH>' (or TIKO_SIM_STORE)");
        std::process::exit(2);
    });
    SimStore::init(path)
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Command_::CreateOrg { org, template } => {
            create_org::run(
                require_sim(cli.sim_store.as_deref(), "create-org"),
                org,
                &template,
            );
        }
        Command_::DeleteOrg { org, force } => {
            delete_org::run(
                require_sim(cli.sim_store.as_deref(), "delete-org"),
                org,
                force,
            );
        }
        Command_::MakeTemplate { pg_bindir, output } => {
            make_template::run(&pg_bindir, &output, cli.sim_store.as_deref());
        }
    }
}
