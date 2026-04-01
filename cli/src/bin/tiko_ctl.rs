#[path = "tiko_ctl/create_branch.rs"]
mod create_branch;
#[path = "tiko_ctl/create_org.rs"]
mod create_org;
#[path = "tiko_ctl/delete_org.rs"]
mod delete_org;
#[path = "tiko_ctl/make_template.rs"]
mod make_template;
#[path = "tiko_ctl/restore.rs"]
mod restore;

use clap::{Parser, Subcommand};
use core::sim_store::SimStore;
use std::path::{Path, PathBuf};

#[derive(Parser)]
#[command(name = "tiko_ctl", about = "Tiko control CLI tools")]
struct Cli {
    /// Path to the Tiko root directory (sets TIKO_ROOT_PATH)
    #[arg(
        long,
        global = true,
        value_name = "PATH",
        value_hint = clap::ValueHint::DirPath,
        env = "TIKO_ROOT_PATH"
    )]
    tiko_root: Option<PathBuf>,

    #[command(subcommand)]
    command: Command_,
}

#[derive(Subcommand)]
enum Command_ {
    /// Create a PGDATA template tarball and store it in the SimStore standard bucket.
    ///
    /// Runs `initdb`, strips relation files and transactional state, tarballs the result,
    /// and uploads it to `standard/template/<output-filename>`.
    MakeTemplate {
        /// Directory containing the PostgreSQL binaries (initdb, postgres)
        #[arg(long, value_name = "DIR", value_hint = clap::ValueHint::DirPath)]
        pg_bindir: PathBuf,
    },
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
    /// Create a branch forked from a parent project at its latest checkpoint.
    CreateBranch {
        #[arg(long)]
        org: u64,
        #[arg(long)]
        project: u64,
        #[arg(long)]
        branch: u64,
        #[arg(long)]
        parent_project: u64,
        #[arg(long)]
        parent_branch: u64,
        /// Parent's PGDATA directory (e.g. /var/lib/postgresql/data).
        /// WAL segments from <parent-pgdata>/pg_wal/ are copied into the child's
        /// pg_wal/ so the child can recover without a restore_command.
        #[arg(long, value_name = "DIR", value_hint = clap::ValueHint::DirPath)]
        parent_pgdata: Option<PathBuf>,
        /// Template filename to seed the base PGDATA from (e.g. template-18.tar.gz)
        #[arg(long, value_name = "FILE")]
        template: String,
        /// Local directory to extract the branch PGDATA into
        #[arg(long, value_name = "DIR", value_hint = clap::ValueHint::DirPath)]
        pg_data: PathBuf,
    },
    /// Create a branch forked from a parent project at a given LSN.
    Restore {
        #[arg(long)]
        org: u64,
        #[arg(long)]
        project: u64,
        #[arg(long)]
        branch: u64,
        #[arg(long)]
        lsn: String,
    },
}

fn require_sim(tiko_root: Option<&Path>, op: &str) -> &'static SimStore {
    let path = tiko_root.unwrap_or_else(|| {
        eprintln!("error: {op} requires '--tiko-root <PATH>' (or TIKO_ROOT_PATH)");
        std::process::exit(2);
    });
    SimStore::init(path)
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Command_::MakeTemplate { pg_bindir } => {
            make_template::run(&pg_bindir, cli.tiko_root.as_deref());
        }
        Command_::CreateOrg { org, template } => {
            create_org::run(
                require_sim(cli.tiko_root.as_deref(), "create-org"),
                org,
                &template,
            );
        }
        Command_::DeleteOrg { org, force } => {
            delete_org::run(
                require_sim(cli.tiko_root.as_deref(), "delete-org"),
                org,
                force,
            );
        }
        Command_::CreateBranch {
            org,
            project,
            branch,
            parent_project,
            parent_branch,
            parent_pgdata,
            template,
            pg_data,
        } => {
            let tiko_root = cli.tiko_root.as_deref().unwrap_or_else(|| {
                eprintln!("error: create-branch requires '--tiko-root <PATH>' (or TIKO_ROOT_PATH)");
                std::process::exit(2);
            });
            create_branch::run(
                require_sim(Some(tiko_root), "create-branch"),
                org,
                project,
                branch,
                parent_project,
                parent_branch,
                parent_pgdata.as_deref(),
                &template,
                &pg_data,
                tiko_root,
            );
        }
        Command_::Restore {
            org,
            project,
            branch,
            lsn,
        } => {
            restore::run(
                require_sim(cli.tiko_root.as_deref(), "restore"),
                org,
                project,
                branch,
                &lsn,
            );
        }
    }
}
