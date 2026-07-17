//! `tikovm-guestd` — the in-VM guest agent.
//!
//! Loads the `WorkloadManifest`, supervises its `[process]`, and (when an
//! `[idle]` policy is declared) runs the idle evaluator that signals the host
//! to suspend when the workload is sustained-idle (scale-to-zero).

use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::Notify;
use tracing_subscriber::EnvFilter;

use tikovm_guest::hostlink::VsockHostLink;
use tikovm_guest::idle::IdleEvaluator;
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
    let manifest = match manifest::load(&args.manifest) {
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

    // --- supervisor: run the workload process under the restart policy ---
    let supervisor = Supervisor::new(proc, manifest.restart.clone());
    let stop = supervisor.stop_handle();
    let mut sup_task = tokio::spawn(async move { supervisor.run().await });

    // --- host->guest command server (lifecycle hooks: PreSuspend/PostRestore) ---
    tikovm_guest::controlsrv::spawn(std::sync::Arc::new(manifest.clone()));

    // --- idle evaluator: scale-to-zero over the vsock control channel ---
    let mut idle_cancel: Option<Arc<Notify>> = None;
    if let Some(idle_policy) = manifest.idle.clone() {
        tracing::info!("idle evaluator enabled (scale-to-zero over vsock)");
        let cancel = Arc::new(Notify::new());
        idle_cancel = Some(cancel.clone());
        let host = VsockHostLink::new().into_host_comm();
        let ev = Arc::new(IdleEvaluator::new(idle_policy, host));
        tokio::spawn(async move { ev.run(cancel).await });
    }

    // --- graceful shutdown on SIGTERM / SIGINT, or supervisor self-exit ---
    let mut term = signal(SignalKind::terminate())?;
    let mut int = signal(SignalKind::interrupt())?;
    tokio::select! {
        biased;
        _ = &mut sup_task => {
            tracing::info!("supervisor exited");
            if let Some(c) = &idle_cancel { c.notify_one(); }
            return Ok(());
        }
        _ = term.recv() => tracing::info!("received SIGTERM"),
        _ = int.recv() => tracing::info!("received SIGINT"),
    }
    stop.stop();
    if let Some(c) = &idle_cancel { c.notify_one(); }
    let _ = sup_task.await;
    tracing::info!("guestd shutting down");
    Ok(())
}
