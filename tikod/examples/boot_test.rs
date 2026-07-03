//! Boot test: verify the Firecracker VM lifecycle works end-to-end.
//!
//! Prerequisites:
//!   tikod/scripts/download_kernel.sh — downloads kernel
//!   tikod/scripts/create_rootfs.sh   — creates Ubuntu rootfs with PG
//!
//! This example boots a VM via Firecracker, tests pause/resume/snapshot/restore,
//! SSHes into the VM to start PostgreSQL, and verifies connectivity from the host.

use std::path::PathBuf;
use std::time::Duration;

use tikod::vmm::firecracker::FirecrackerVmm;
use tikod::vmm::{VmConfig, Vmm};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("info".parse()?),
        )
        .init();

    let assets_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("assets");
    let kernel_path = assets_dir.join("vmlinux-6.1");
    let rootfs_path = assets_dir.join("ubuntu-24.04-rootfs.ext4");

    for (name, path) in [("kernel", &kernel_path), ("rootfs", &rootfs_path)] {
        if !path.exists() {
            eprintln!("{name} not found at {}", path.display());
            eprintln!("Run: tikod/scripts/download_kernel.sh && tikod/scripts/create_rootfs.sh");
            std::process::exit(1);
        }
    }

    let data_dir = PathBuf::from("/tmp/tikod-boot-test");
    std::fs::create_dir_all(&data_dir)?;

    let vmm = FirecrackerVmm::new(data_dir.clone());

    let vm_id = "test-fc-001".to_string();
    let config = VmConfig {
        vm_id: vm_id.clone(),
        kernel_path,
        kernel_cmdline: "console=ttyS0 root=/dev/vda rw init=/sbin/init".into(),
        rootfs_path,
        memory_mb: 512,
        vcpus: 2,
        drives: vec![],
        initrd_path: None,
    };

    // --- Create ---
    tracing::info!("=== Creating Firecracker VM (512MB, 2 vCPU) ===");
    vmm.create_vm(config).await?;
    print_state(&vmm, &vm_id).await;

    // --- Start ---
    tracing::info!("=== Starting VM ===");
    vmm.start_vm(&vm_id).await?;
    print_state(&vmm, &vm_id).await;

    // Wait for boot.
    tracing::info!("Waiting 15s for guest to boot...");
    tokio::time::sleep(Duration::from_secs(15)).await;
    print_state(&vmm, &vm_id).await;

    check_vm(&vmm, &vm_id).await?;

    // --- Pause ---
    tracing::info!("=== Pausing VM ===");
    match vmm.pause_vm(&vm_id).await {
        Ok(()) => {
            tracing::info!("Pause OK");
            print_state(&vmm, &vm_id).await;
        }
        Err(e) => tracing::error!("Pause failed: {e}"),
    }

    tokio::time::sleep(Duration::from_secs(2)).await;

    // --- Snapshot ---
    tracing::info!("=== Take snapshot of VM ===");
    let snapshot = match vmm.snapshot_vm(&vm_id).await {
        Ok(snapshot) => {
            tracing::info!("Snapshot OK");
            tracing::info!("snapshot: {:?}", snapshot);
            print_state(&vmm, &vm_id).await;
            snapshot
        }
        Err(e) => { tracing::error!("Snapshot failed: {e}"); return Ok(()); },
    };

    resume_pause_destroy(&vmm, &vm_id).await?;

    // --- Restore ---
    tracing::info!("=== Restore snapshot of VM ===");
    let vm_id = match vmm.restore_vm(&snapshot).await {
        Ok(vm_id) => {
            tracing::info!("Snapshot restore OK");
            print_state(&vmm, &vm_id).await;
            vm_id
        }
        Err(e) => { tracing::error!("Snapshot restore failed: {e}"); return Ok(()); },
    };

    resume_pause_destroy(&vmm, &vm_id).await?;

    tracing::info!("=== Boot test complete ===");
    Ok(())
}

async fn check_vm(vmm: &FirecrackerVmm, vm_id: &String) -> Result<(), Box<dyn std::error::Error>> {
    let guest_ip = vmm.vm_guest_ip(&vm_id).await?;
    if let Some(ip) = guest_ip {
        tracing::info!("Guest IP: {ip}");

        // Test SSH.
        tracing::info!("=== Testing SSH ===");
        match test_tcp_connect(ip, 22).await {
            Ok(()) => tracing::info!("SSH port open"),
            Err(e) => {
                tracing::warn!("SSH not available: {e}");
                return Ok(());
            }
        }

        // Start PostgreSQL inside the VM via SSH.
        tracing::info!("=== Starting PostgreSQL via SSH ===");
        match ssh_start_pg(ip).await {
            Ok(()) => tracing::info!("PostgreSQL started in VM"),
            Err(e) => {
                tracing::warn!("Failed to start PG via SSH: {e}");
                return Ok(());
            }
        }

        // Wait for PG to bind.
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Test PG connectivity from host.
        tracing::info!("=== Testing PostgreSQL connectivity ===");
        match test_psql_connect(ip, 5432).await {
            Ok(()) => tracing::info!("PostgreSQL connection OK!"),
            Err(e) => tracing::warn!("PostgreSQL connection failed: {e}"),
        }
    }

    Ok(())
}

async fn resume_pause_destroy(vmm: &FirecrackerVmm, vm_id: &String) -> Result<(), Box<dyn std::error::Error>> {
    // --- Resume ---
    tracing::info!("=== Resuming VM from snapshot ===");
    match vmm.resume_vm(vm_id).await {
        Ok(()) => {
            tracing::info!("Resume OK");
            print_state(vmm, vm_id).await;
        }
        Err(e) => tracing::error!("Resume failed: {e}"),
    }

    check_vm(&vmm, &vm_id).await?;

    // --- Pause ---
    tracing::info!("=== Pausing VM ===");
    match vmm.pause_vm(vm_id).await {
        Ok(()) => {
            tracing::info!("Pause OK");
            print_state(vmm, vm_id).await;
        }
        Err(e) => tracing::error!("Pause failed: {e}"),
    }

    tokio::time::sleep(Duration::from_secs(2)).await;

    // --- Destroy ---
    tracing::info!("=== Destroying VM ===");
    vmm.destroy_vm(vm_id).await?;

    Ok(())
}

async fn print_state(vmm: &FirecrackerVmm, vm_id: &str) {
    match vmm.vm_state(&vm_id.to_string()).await {
        Ok(state) => tracing::info!("VM state: {state}"),
        Err(e) => tracing::error!("Failed to query VM state: {e}"),
    }
}

async fn test_tcp_connect(ip: std::net::IpAddr, port: u16) -> Result<(), String> {
    let addr = std::net::SocketAddr::new(ip, port);
    match tokio::time::timeout(
        Duration::from_secs(5),
        tokio::net::TcpStream::connect(addr),
    )
    .await
    {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(e)) => Err(format!("connect: {e}")),
        Err(_) => Err("timeout".into()),
    }
}

/// SSH into the VM (root:root) and initialize + start PostgreSQL as the
/// postgres user.
async fn ssh_start_pg(ip: std::net::IpAddr) -> Result<(), String> {
    let ssh_target = format!("root@{ip}");
    let ssh_opts: Vec<&str> = vec![
        "-p", "root",
        "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        "-o", "ConnectTimeout=5",
    ];

    // Step 1: Initialize the data dir, then start PostgreSQL.
    tracing::debug!("Running init_pg.sh + start_pg.sh via SSH...");
    let output = tokio::process::Command::new("sshpass")
        .args(&ssh_opts)
        .arg(&ssh_target)
        .arg("su - postgres -c '/var/lib/postgresql/init_pg.sh && /var/lib/postgresql/start_pg.sh' 2>&1")
        .output()
        .await
        .map_err(|e| format!("ssh spawn: {e}"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        return Err(format!(
            "init/start_pg.sh exit {:?}\nstdout: {stdout}\nstderr: {stderr}",
            output.status.code()
        ));
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    tracing::debug!("init/start_pg.sh: {stdout}");

    // Step 2: Configure PG for remote access (listen_addresses + pg_hba trust).
    tracing::debug!("Configuring remote access...");
    let output = tokio::process::Command::new("sshpass")
        .args(&ssh_opts)
        .arg(&ssh_target)
        .arg(
            "su - postgres -c \"\
             echo \\\"listen_addresses = '*'\\\" >> /var/lib/postgresql/tt/postgresql.conf && \
             echo \\\"host all all 0.0.0.0/0 trust\\\" >> /var/lib/postgresql/tt/pg_hba.conf && \
             pg_ctl -D /var/lib/postgresql/tt -l /var/lib/postgresql/log.log reload 2>&1\""
        )
        .output()
        .await
        .map_err(|e| format!("ssh spawn (config): {e}"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        tracing::warn!("PG remote config may have failed: {stdout}{stderr}");
    } else {
        tracing::debug!("PG remote access configured");
    }

    Ok(())
}

/// Test PostgreSQL connectivity from the host using psql.
async fn test_psql_connect(ip: std::net::IpAddr, port: u16) -> Result<(), String> {
    let output = tokio::process::Command::new("psql")
        .arg("-h")
        .arg(ip.to_string())
        .arg("-p")
        .arg(port.to_string())
        .arg("-U")
        .arg("postgres")
        .arg("-t")
        .arg("-c")
        .arg("SELECT version();")
        .env("PGPASSWORD", "")
        .env("PGCONNECT_TIMEOUT", "5")
        .output()
        .await
        .map_err(|e| format!("psql spawn: {e}"))?;

    if output.status.success() {
        let version = String::from_utf8_lossy(&output.stdout);
        tracing::info!("PG version: {}", version.trim());
        Ok(())
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        Err(format!("psql exit {:?}: {stderr}", output.status.code()))
    }
}