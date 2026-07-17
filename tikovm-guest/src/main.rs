//! `tikovm-guestd` — the in-VM guest agent.
//!
//! Loads the `WorkloadManifest` and supervises its `[process]` per the restart
//! policy. Health reporting, idle evaluation, and the vsock host channel are
//! layered in as the corresponding modules land; this entry point wires the
//! supervisor now so a rootfs-baked workload actually runs.

use std::path::PathBuf;

use clap::Parser;
use tokio::signal::unix::{signal, SignalKind};
use tracing_subscriber::EnvFilter;

use tikovm_guest::manifest;
use tikovm_guest::supervisor::Supervisor;

#[derive(Debug, Parser)]
#[command(name = "tikovm-guestd", version, about = "tikovm in-VM guest agent")]
struct Args {
    /// Path to the workload manifest.
    #[arg(long, default_value = "/etc/tikovm/workload.toml", env = "TIKOVM_MANIFEST")]
    manifest: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();

    let args = Args::parse();
    let manifest = manifest::load(&args.manifest);
    let manifest = match manifest {
        Ok(m) => {
            tracing::info!(workload = %m.workload, "tikovm-guestd started; supervising workload");
            m
        }
        Err(e) => {
            tracing::error!(error = %e, "could not load manifest; exiting");
            std::process::exit(1);
        }
    };

    let Some(proc) = manifest.process.clone() else {
        tracing::warn!("manifest has no [process]; nothing to supervise");
        return Ok(());
    };

    let sup = Supervisor::new(proc, manifest.restart.clone());
    let stop = sup.stop_handle();

    // Graceful shutdown on SIGTERM / SIGINT (e.g. systemd stopping the unit).
    let mut term = signal(SignalKind::terminate())?;
    let mut int = signal(SignalKind::interrupt())?;
    tokio::spawn(async move {
        tokio::select! {
            _ = term.recv() => tracing::info!("received SIGTERM"),
            _ = int.recv() => tracing::info!("received SIGINT"),
        }
        stop.stop();
    });

    sup.run().await;
    tracing::info!("supervisor exited; guestd shutting down");
    Ok(())
}
