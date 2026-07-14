//! PostgreSQL wire-protocol proxy with wake-on-connect and VM routing.
//!
//! Accepts client connections on a single port and routes each to the
//! PostgreSQL backend of the target VM. The VM is selected by the
//! `tiko.endpoint=<vm_id>` token in the startup packet's `options` field. If
//! the VM is paused or frozen, the proxy wakes it (resuming or
//! restoring from snapshot) while holding the client socket — exploiting
//! libpq's default `connect_timeout=0` (wait indefinitely).
//!
//! The proxy is **protocol-aware only during the handshake**: it reads just
//! enough of the client's opening bytes to extract the routing key and decline
//! SSL, then replays those bytes to the chosen backend and switches to a blind
//! byte splice for the rest of the connection. While splicing the backend's
//! handshake replies it also intercepts `BackendKeyData` so that
//! `CancelRequest`s (psql Ctrl-C) route to the correct VM.
//!
//! ```text
//! Client ──TCP──→ Proxy (:5432)
//!                  │
//!                  │  read startup: SSL?→'N'; Cancel?→cancel path; else parse
//!                  │  extract tiko.endpoint=<vm_id>  (else dev `default_target`)
//!                  │  lookup VM; Node::wake (Running/Paused/Stopped→restore)
//!                  │  connect backend, replay startup, splice bytes
//!                  │  (frame-scan until ReadyForQuery, then pure copy)
//!                  ▼
//!               VM PG backend
//! ```

mod cancel;
mod error;
mod startup;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info, warn};

use cancel::{CancelTable, copy_until_ready, forward_cancel};
use error::{
    error_packet, fatal_missing_endpoint, fatal_unknown_vm, fatal_wake_timeout, wake_error_packet,
};
use startup::{FirstMessage, StartupMessage, read_first_message};

use crate::control::Control;
use crate::node::Node;
use crate::vmm::VmId;

/// Default PostgreSQL port.
const PG_PORT: u16 = 5432;

/// Severity used for connection-rejection error packets.
const FATAL: &str = "FATAL";

/// Connection forward target.
#[derive(Debug, Clone)]
pub enum ForwardTarget {
    /// Forward to a specific VM by ID (looked up in the registry).
    Vm(VmId),
    /// Forward to a fixed address (dev/test escape hatch when no
    /// `tiko.endpoint` is supplied by the client).
    Direct(SocketAddr),
}

/// Configuration for the proxy.
#[derive(Debug, Clone)]
pub struct ProxyConfig {
    /// Address to listen on for client connections.
    pub listen_addr: SocketAddr,
    /// Fallback target for connections that omit `tiko.endpoint`. Only used
    /// when [`dev_allow_missing_endpoint`] is true.
    ///
    /// [`dev_allow_missing_endpoint`]: ProxyConfig::dev_allow_missing_endpoint
    pub default_target: ForwardTarget,
    /// Maximum time to wait for a VM to wake (resume / restore) before
    /// rejecting the client with a PG `FATAL` error.
    pub resume_timeout_secs: u64,
    /// Dev/test escape hatch. When `false` (the default, for production), a
    /// connection without a `tiko.endpoint` routing option is rejected with a
    /// PG `FATAL` error. When `true`, such connections fall back to
    /// [`default_target`] instead — useful for local development without a VM.
    ///
    /// [`default_target`]: ProxyConfig::default_target
    pub dev_allow_missing_endpoint: bool,
}

impl Default for ProxyConfig {
    fn default() -> Self {
        Self {
            listen_addr: "127.0.0.1:5432".parse().unwrap(),
            default_target: ForwardTarget::Direct("127.0.0.1:15432".parse().unwrap()),
            resume_timeout_secs: 30,
            dev_allow_missing_endpoint: false,
        }
    }
}

/// The PostgreSQL proxy server.
pub struct Proxy {
    node: Arc<Node>,
    control: Arc<Control>,
    config: ProxyConfig,
    cancel_table: Arc<CancelTable>,
}

impl Proxy {
    pub fn new(node: Arc<Node>, control: Arc<Control>, config: ProxyConfig) -> Self {
        Self {
            node,
            control,
            config,
            cancel_table: CancelTable::shared(),
        }
    }

    /// Run the proxy server. Accepts connections in a loop.
    pub async fn run(&self) -> io::Result<()> {
        let listener = TcpListener::bind(&self.config.listen_addr).await?;
        info!(addr = %self.config.listen_addr, "proxy listening for PG connections");

        loop {
            match listener.accept().await {
                Ok((client_stream, client_addr)) => {
                    let node = self.node.clone();
                    let control = self.control.clone();
                    let config = self.config.clone();
                    let cancel_table = self.cancel_table.clone();

                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(
                            client_stream,
                            client_addr,
                            &node,
                            &control,
                            &config,
                            &cancel_table,
                        )
                        .await
                        {
                            error!(client = %client_addr, error = %e, "connection handling failed");
                        }
                    });
                }
                Err(e) => {
                    error!(error = %e, "accept failed");
                }
            }
        }
    }
}

/// Outcome of resolving a VM routing target.
enum RouteOutcome {
    /// Forward to this backend address.
    Addr(SocketAddr),
    /// Reject the client: a PG error packet to write before closing.
    Reject(Vec<u8>),
}

/// Handle a single client connection: read startup, route, wake the VM if
/// needed, then splice bytes bidirectionally.
async fn handle_connection(
    client_stream: TcpStream,
    client_addr: SocketAddr,
    node: &Node,
    control: &Control,
    config: &ProxyConfig,
    cancel_table: &CancelTable,
) -> io::Result<()> {
    debug!(client = %client_addr, "new connection");
    let (mut client_read, mut client_write) = client_stream.into_split();

    // 1. Read the client's startup (handling the SSL/GSS prelude and cancels).
    let startup: StartupMessage = loop {
        match read_first_message(&mut client_read).await {
            Ok(Some(FirstMessage::SslRequest)) | Ok(Some(FirstMessage::GssEncRequest)) => {
                // v1 is plaintext-only: decline TLS/GSS and expect a StartupMessage.
                client_write.write_all(b"N").await?;
                continue;
            }
            Ok(Some(FirstMessage::Cancel { pid, secret })) => {
                if let Err(e) = forward_cancel(pid, secret, cancel_table).await {
                    debug!(client = %client_addr, error = %e, "cancel forward failed");
                }
                return Ok(()); // cancel connections close immediately
            }
            Ok(Some(FirstMessage::Startup(msg))) => break msg,
            Ok(None) => {
                debug!(client = %client_addr, "client closed before sending startup");
                return Ok(());
            }
            Err(e) => {
                debug!(client = %client_addr, error = %e, "startup parse failed");
                return Ok(());
            }
        }
    };

    // 2. Resolve the backend: VM routing key, else reject hard (or, in dev
    //    mode, the configured `default_target`).
    let mut connected_vm_id: Option<VmId> = None;
    let backend_addr: SocketAddr = match startup.vm_id() {
        Some(id) => {
            let id: VmId = id.to_string();
            match resolve_vm(&id, node, control, config).await {
                RouteOutcome::Addr(addr) => {
                    connected_vm_id = Some(id);
                    addr
                }
                RouteOutcome::Reject(pkt) => {
                    let _ = client_write.write_all(&pkt).await;
                    return Ok(());
                }
            }
        }
        None => {
            if config.dev_allow_missing_endpoint {
                match &config.default_target {
                    ForwardTarget::Direct(addr) => *addr,
                    ForwardTarget::Vm(id) => match resolve_vm(id, node, control, config).await {
                        RouteOutcome::Addr(addr) => {
                            connected_vm_id = Some(id.clone());
                            addr
                        }
                        RouteOutcome::Reject(pkt) => {
                            let _ = client_write.write_all(&pkt).await;
                            return Ok(());
                        }
                    },
                }
            } else {
                let _ = client_write.write_all(&fatal_missing_endpoint()).await;
                return Ok(());
            }
        }
    };

    // 3. Connect to the backend.
    debug!(client = %client_addr, backend = %backend_addr, "connecting to backend");
    let backend_stream = match TcpStream::connect(backend_addr).await {
        Ok(s) => s,
        Err(e) => {
            let pkt = error_packet(
                FATAL,
                "08006",
                &format!("cannot connect to backend {backend_addr}: {e}"),
            );
            let _ = client_write.write_all(&pkt).await;
            return Ok(());
        }
    };
    backend_stream.set_nodelay(true)?;
    // Part A: aggressive TCP keepalive so a dead backend (crash, network loss)
    // is detected within ~20s rather than hanging for minutes. The proxy
    // toggles this off when the VM is warm-paused (see wake-on-stale loop
    // below) so the frozen VM isn't declared dead.
    #[cfg(unix)]
    set_backend_keepalive(&backend_stream);
    // Capture the fd before into_split() for keepalive toggling.
    #[cfg(unix)]
    let backend_fd = {
        use std::os::fd::AsRawFd;
        backend_stream.as_raw_fd()
    };
    let (mut backend_read, mut backend_write) = backend_stream.into_split();

    // 4. Replay the buffered startup packet verbatim.
    if let Err(e) = backend_write.write_all(&startup.raw).await {
        let pkt = error_packet(
            FATAL,
            "08006",
            &format!("failed to forward startup to backend: {e}"),
        );
        let _ = client_write.write_all(&pkt).await;
        return Ok(());
    }

    // 5. Splice with wake-on-stale, keepalive toggle, and cancellation.
    //
    // For VM-routed connections, the client→backend direction uses a
    // wake-on-stale loop: it polls the per-VM thermal state (warm-paused?)
    // before each read, toggles TCP keepalive off when paused (so a frozen VM
    // isn't declared dead), resumes the VM before forwarding client data, and
    // re-enables keepalive once running. The thermal watch's `changed()` is
    // also raced against `client_read.read()` so the proxy reacts within
    // milliseconds of warm-pause — well before the kernel's keepalive probe.
    //
    // The whole splice races against the per-VM cancel signal (cold
    // freeze): when fired, the splice is dropped and both sockets
    // close, prompting the client to reconnect through wake.
    let thermal_rx = connected_vm_id
        .as_ref()
        .map(|id| control.subscribe_warm(id));

    let splice = async {
        let backend_to_client = async {
            let key = copy_until_ready(
                &mut backend_read,
                &mut client_write,
                backend_addr,
                cancel_table,
            )
            .await?;
            io::copy(&mut backend_read, &mut client_write).await?;
            client_write.shutdown().await?;
            Ok::<_, io::Error>(key)
        };

        let client_to_backend = async {
            if let (Some(mut thermal), Some(vm_id)) = (thermal_rx, connected_vm_id.as_ref()) {
                // Wake-on-stale loop for VM-routed connections.
                let mut buf = [0u8; 8192];
                let mut keepalive_on = true;
                loop {
                    let paused = *thermal.borrow();
                    // Toggle keepalive on thermal transitions: enabled when
                    // Running, disabled when WarmPaused (so a frozen VM isn't
                    // declared dead by the kernel's keepalive probe).
                    #[cfg(unix)]
                    if keepalive_on == paused {
                        keepalive_on = !paused;
                        set_keepalive_enabled(backend_fd, keepalive_on);
                    }
                    // Read from client, racing against thermal changes so we
                    // toggle keepalive promptly on warm-pause even while idle.
                    // The wake is deliberately data-triggered (below): an idle
                    // connection must NOT resume the VM, or the warm-pause →
                    // cold-freeze transition is defeated and idle VMs never
                    // scale down.
                    tokio::select! {
                        biased;
                        _ = thermal.changed() => continue,
                        r = client_read.read(&mut buf) => {
                            let n = r?;
                            if n == 0 { break; }
                            // Wake-on-stale: resume the VM only now that the
                            // client has actual data to forward.
                            if *thermal.borrow() {
                                debug!(client = %client_addr, vm_id = %vm_id, "backend warm-paused — resuming on data");
                                if let Err(e) = node.wake(vm_id, control).await {
                                    return Err(io::Error::other(format!(
                                        "wake-on-stale failed for {vm_id}: {e}"
                                    )));
                                }
                            }
                            backend_write.write_all(&buf[..n]).await?;
                        }
                    }
                }
            } else {
                // Direct routing (dev): plain copy, no thermal awareness.
                io::copy(&mut client_read, &mut backend_write).await?;
            }
            backend_write.shutdown().await?;
            Ok::<_, io::Error>(())
        };

        tokio::try_join!(client_to_backend, backend_to_client)
    };

    debug!(client = %client_addr, "proxying established");

    if let Some(cancel_signal) = connected_vm_id
        .as_ref()
        .map(|id| control.subscribe_cancel(id))
    {
        // VM-routed: race the splice against the cancel signal.
        tokio::select! {
            biased;
            _ = cancel_signal.notified() => {
                debug!(client = %client_addr, "connection cancelled (VM freezing)");
            }
            result = splice => {
                if let Ok((_, Some((pid, secret)))) = result {
                    cancel_table.remove(pid, secret);
                }
            }
        }
    } else {
        // Direct routing (dev): plain splice, no cancel.
        if let Ok((_, Some((pid, secret)))) = splice.await {
            cancel_table.remove(pid, secret);
        }
    }

    // 6. Tear-down: notify the control plane that the client has gone away,
    //    so the connection counter (used by the idle/auto-pause policy) stays
    //    accurate. For a normal close the cancel-table entry was already
    //    evicted above; for a cancelled close the backend it pointed to is
    //    being destroyed anyway, so the stale entry is harmless.
    if let Some(id) = connected_vm_id.as_ref() {
        control.on_disconnect(id);
    }
    debug!(client = %client_addr, "connection closed");
    Ok(())
}

/// Resolve a VM target to a backend socket address. Performs wake-on-connect
/// (resume / thaw, single-flighted via `Control`) bounded by
/// `resume_timeout_secs`. Records `on_connect` on success.
async fn resolve_vm(
    vm_id: &VmId,
    node: &Node,
    control: &Control,
    config: &ProxyConfig,
) -> RouteOutcome {
    // Unknown VM — reject hard.
    if control.get(vm_id).is_none() {
        return RouteOutcome::Reject(fatal_unknown_vm(vm_id));
    }

    // Wake (Running → noop; Paused → resume; Stopped → restore), bounded.
    let wake = node.wake(vm_id, control);
    match tokio::time::timeout(Duration::from_secs(config.resume_timeout_secs), wake).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => return RouteOutcome::Reject(wake_error_packet(vm_id, &e)),
        Err(_) => {
            return RouteOutcome::Reject(fatal_wake_timeout(vm_id, config.resume_timeout_secs));
        }
    }

    let guest_ip = match node.guest_ip(vm_id).await {
        Ok(ip) => ip,
        Err(e) => {
            return RouteOutcome::Reject(error_packet(
                FATAL,
                "08006",
                &format!("cannot get guest IP for VM {vm_id}: {e}"),
            ));
        }
    };

    let ip = guest_ip.unwrap_or_else(|| {
        warn!(vm_id = %vm_id, "no guest IP discovered, using localhost");
        IpAddr::V4(Ipv4Addr::LOCALHOST)
    });

    control.on_connect(vm_id);
    RouteOutcome::Addr(SocketAddr::new(ip, PG_PORT))
}

// ── TCP keepalive (Part A) ──────────────────────────────────────────────────
//
// Aggressive keepalive on the backend socket so that a dead backend (VM crash,
// network loss — anything NOT covered by the cancel signal from freeze)
// is detected within ~20s instead of hanging for TCP's default retransmission
// timeout (~15 min). This is a safety net; the primary mechanism is
// [`Control::cancel_vm_connections`] fired at the start of freeze.

#[cfg(unix)]
fn set_backend_keepalive(stream: &TcpStream) {
    use std::os::fd::AsRawFd;
    let fd = stream.as_raw_fd();

    // Helper: set an integer socket option.
    let setopt = |level: libc::c_int, opt: libc::c_int, val: libc::c_int| unsafe {
        libc::setsockopt(
            fd,
            level,
            opt,
            &val as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
    };

    // Enable keepalive.
    setopt(libc::SOL_SOCKET, libc::SO_KEEPALIVE, 1);

    // Platform-specific tuning: 10s idle, then 3 probes × 3s = ~19s to detect.
    #[cfg(target_os = "linux")]
    {
        setopt(libc::IPPROTO_TCP, libc::TCP_KEEPIDLE, 10);
        setopt(libc::IPPROTO_TCP, libc::TCP_KEEPINTVL, 3);
        setopt(libc::IPPROTO_TCP, libc::TCP_KEEPCNT, 3);
    }
    #[cfg(target_os = "macos")]
    {
        // macOS uses TCP_KEEPALIVE for the idle time (equivalent of Linux TCP_KEEPIDLE).
        setopt(libc::IPPROTO_TCP, libc::TCP_KEEPALIVE, 10);
        setopt(libc::IPPROTO_TCP, libc::TCP_KEEPINTVL, 3);
        setopt(libc::IPPROTO_TCP, libc::TCP_KEEPCNT, 3);
    }
}

/// Toggle `SO_KEEPALIVE` on/off for a backend socket. The tuning values (idle,
/// interval, count) persist on the socket — only the enable flag flips. Used by
/// the wake-on-stale loop to disable keepalive while the VM is warm-paused (so
/// the frozen VM isn't declared dead) and re-enable it when the VM resumes.
#[cfg(unix)]
fn set_keepalive_enabled(fd: std::os::fd::RawFd, enabled: bool) {
    unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_KEEPALIVE,
            &(if enabled { 1 } else { 0 } as libc::c_int) as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
    }
}
