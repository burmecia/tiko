pub mod compute;
pub mod gc;
pub mod lease;
pub mod lifecycle;
pub mod orchestrate;
pub mod pitr;

use clap::{Parser, Subcommand};
use store::org::OrgMeta;
use store::sim_store::SimStore;

#[derive(Parser)]
#[command(name = "tikod", about = "Tiko control plane")]
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
    /// Create a branch forked from a parent project at a given LSN.
    ///
    /// Root projects are created automatically when creating an org.
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
        /// Branch point LSN, e.g. "0/3000000"
        #[arg(long)]
        lsn: String,
    },
    /// Soft-delete a branch
    DeleteBranch {
        #[arg(long)]
        org: u64,
        #[arg(long)]
        project: u64,
        #[arg(long)]
        branch: u64,
    },
    /// List projects / branches for an org
    ListProjects {
        #[arg(long)]
        org: u64,
    },
    /// Cold-start a project: picks latest checkpoint automatically, runs full §12 sequence
    Start {
        #[arg(long)]
        org: u64,
        #[arg(long)]
        project: u64,
        #[arg(long)]
        branch: u64,
        #[arg(long)]
        pgdata: String,
    },
    /// Freeze: CHECKPOINT, Firecracker pause + snapshot, daemon keeps lease alive
    Freeze {
        #[arg(long)]
        org: u64,
        #[arg(long)]
        project: u64,
        #[arg(long)]
        branch: u64,
        #[arg(long)]
        pgdata: String,
        #[arg(long)]
        snapshot_path: Option<String>,
    },
    /// Thaw: resume from local snapshot (~50ms) or fall back to cold-start
    Thaw {
        #[arg(long)]
        org: u64,
        #[arg(long)]
        project: u64,
        #[arg(long)]
        branch: u64,
        #[arg(long)]
        pgdata: String,
        #[arg(long)]
        snapshot_path: Option<String>,
    },
    /// Graceful stop: CHECKPOINT, pg_ctl stop, release lease, mark stopped
    Stop {
        #[arg(long)]
        org: u64,
        #[arg(long)]
        project: u64,
        #[arg(long)]
        branch: u64,
        #[arg(long)]
        pgdata: String,
    },
    /// Prepare PGDATA for point-in-time recovery (does not start postgres)
    PrepareRecovery {
        #[arg(long)]
        org: u64,
        #[arg(long)]
        project: u64,
        #[arg(long)]
        branch: u64,
        /// Target (timeline_id, LSN) as "TL/LSN", e.g. "1/3000000"
        #[arg(long)]
        target_lsn: String,
        #[arg(long)]
        pgdata: String,
    },
    /// Full PITR cycle: prepare + start postgres + wait for shutdown
    Restore {
        #[arg(long)]
        org: u64,
        #[arg(long)]
        project: u64,
        #[arg(long)]
        branch: u64,
        #[arg(long)]
        target_lsn: String,
        #[arg(long)]
        pgdata: String,
    },
    /// List available PITR restore points for a branch
    ListRestorePoints {
        #[arg(long)]
        org: u64,
        #[arg(long)]
        project: u64,
        #[arg(long)]
        branch: u64,
    },
    /// Run GC / retention enforcement for an org
    Gc {
        #[arg(long)]
        org: u64,
        #[arg(long, default_value_t = 500)]
        max_checkpoints: u64,
    },
    /// Materialize a base manifest for a branch
    Materialize {
        #[arg(long)]
        org: u64,
        #[arg(long)]
        project: u64,
        #[arg(long)]
        branch: u64,
    },
}

fn main() {
    let cli = Cli::parse();
    let sim = SimStore::new(std::path::Path::new("/usr/local/tiko/sim_store")); // TODO: configurable path

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
        Command::CreateBranch {
            org,
            project,
            branch,
            parent_project,
            parent_branch,
            lsn,
        } => {
            use pgsys::Lsn;
            use store::project::ProjectNamespace;
            let ns = ProjectNamespace::new(org, project, branch);
            let parent_ns = ProjectNamespace::new(org, parent_project, parent_branch);
            let result = lsn
                .split_once('/')
                .and_then(|(hi, lo)| {
                    let hi = u64::from_str_radix(hi, 16).ok()?;
                    let lo = u64::from_str_radix(lo, 16).ok()?;
                    Some(Lsn::new((hi << 32) | lo))
                })
                .or_else(|| Lsn::from_hex(&lsn).ok())
                .ok_or_else(|| format!("invalid LSN: {lsn}"))
                .and_then(|branch_lsn| {
                    lifecycle::get_project(&sim, &parent_ns)
                        .map_err(|e| e.to_string())
                        .and_then(|parent_meta| {
                            lifecycle::create_branch(
                                &sim,
                                &parent_ns,
                                parent_meta.current_timeline_id,
                                &ns,
                                branch_lsn,
                            )
                            .map_err(|e| e.to_string())
                        })
                        .and_then(|_| lifecycle::get_project(&sim, &ns).map_err(|e| e.to_string()))
                });
            match result {
                Ok(meta) => println!("{}", serde_json::to_string_pretty(&meta).unwrap()),
                Err(e) => {
                    eprintln!("error: {e}");
                    std::process::exit(1);
                }
            }
        }
        Command::DeleteBranch {
            org,
            project,
            branch,
        } => {
            eprintln!("TODO: delete_branch(org={org}, project={project}, branch={branch})");
        }
        Command::ListProjects { org } => {
            eprintln!("TODO: list_projects(org={org})");
        }
        Command::Start {
            org,
            project,
            branch,
            pgdata,
        } => {
            eprintln!(
                "TODO: start(org={org}, project={project}, branch={branch}, pgdata={pgdata})"
            );
        }
        Command::Freeze {
            org,
            project,
            branch,
            pgdata,
            snapshot_path,
        } => {
            eprintln!(
                "TODO: freeze(org={org}, project={project}, branch={branch}, pgdata={pgdata}, snapshot_path={snapshot_path:?})"
            );
        }
        Command::Thaw {
            org,
            project,
            branch,
            pgdata,
            snapshot_path,
        } => {
            eprintln!(
                "TODO: thaw(org={org}, project={project}, branch={branch}, pgdata={pgdata}, snapshot_path={snapshot_path:?})"
            );
        }
        Command::Stop {
            org,
            project,
            branch,
            pgdata,
        } => {
            eprintln!("TODO: stop(org={org}, project={project}, branch={branch}, pgdata={pgdata})");
        }
        Command::PrepareRecovery {
            org,
            project,
            branch,
            target_lsn,
            pgdata,
        } => {
            eprintln!(
                "TODO: prepare_recovery(org={org}, project={project}, branch={branch}, \
                 target_lsn={target_lsn}, pgdata={pgdata})"
            );
        }
        Command::Restore {
            org,
            project,
            branch,
            target_lsn,
            pgdata,
        } => {
            eprintln!(
                "TODO: restore(org={org}, project={project}, branch={branch}, \
                 target_lsn={target_lsn}, pgdata={pgdata})"
            );
        }
        Command::ListRestorePoints {
            org,
            project,
            branch,
        } => {
            eprintln!("TODO: list_restore_points(org={org}, project={project}, branch={branch})");
        }
        Command::Gc {
            org,
            max_checkpoints,
        } => {
            eprintln!("TODO: gc(org={org}, max_checkpoints={max_checkpoints})");
        }
        Command::Materialize {
            org,
            project,
            branch,
        } => {
            eprintln!("TODO: materialize(org={org}, project={project}, branch={branch})");
        }
    }
}
