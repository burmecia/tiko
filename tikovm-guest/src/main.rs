//! `tikovm-guestd` — the in-VM guest daemon binary.
//!
//! Minimal entry point while the implementation is built out.

use std::path::PathBuf;

use clap::Parser;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "tikovm-guestd", version, about = "tikovm in-VM guest agent")]
struct Args {
    /// Path to the workload manifest.
    #[arg(long, default_value = "/etc/tikovm/workload.toml")]
    manifest: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();

    let args = Args::parse();
    let manifest = tikovm_guest::manifest::load(&args.manifest);
    match manifest {
        Ok(m) => tracing::info!(workload = %m.workload, "tikovm-guestd starting (skeleton)"),
        Err(e) => tracing::warn!(error = %e, "could not load manifest; continuing"),
    }
    Ok(())
}
