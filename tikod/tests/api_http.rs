//! HTTP control API round-trip tests.
//!
//! Verifies routing, JSON encoding/decoding, status-code mapping, and — most
//! importantly — that error responses round-trip back into the correct
//! [`VmmError`] variants (the invariant `boot_test` relies on for its
//! `matches!` assertions). Uses a real [`FirecrackerVmm`] but never starts a
//! VM, so it runs anywhere without KVM / the firecracker binary.

use std::sync::Arc;

use tikod::api::{ApiClient, ApiServer};
use tikod::control::Control;
use tikod::node::Node;
use tikod::vmm::firecracker::FirecrackerVmm;
use tikod::vmm::{Snapshot, VmConfig, VmmError};

/// Bring up an in-process API server on an ephemeral port and return a client
/// pointed at it. The listener is bound before the serve task is spawned, so the
/// kernel accept-queue handles connects immediately — no startup race.
async fn spawn_server() -> ApiClient {
    let dir = std::env::temp_dir().join(format!(
        "tikod-api-test-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
    ));
    let vmm = Arc::new(FirecrackerVmm::new(dir.clone()));
    let node = Arc::new(Node::new(vmm, dir.join("snapshots")));
    let control = Arc::new(Control::new());
    let server = Arc::new(ApiServer::new(node, control));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = server.serve(listener).await;
    });

    ApiClient::new(addr)
}

/// Minimal config (paths need not exist — these tests never drive create_vm to
/// the point where assets matter).
fn cfg(vm_id: &str) -> VmConfig {
    VmConfig {
        vm_id: vm_id.to_string(),
        kernel_path: "/nonexistent/vmlinux".into(),
        kernel_cmdline: "console=ttyS0".into(),
        rootfs_path: "/nonexistent/rootfs.ext4".into(),
        memory_mb: 128,
        vcpus: 1,
        drives: vec![],
        initrd_path: None,
    }
}

#[tokio::test]
async fn unknown_vm_returns_vm_not_found() {
    let client = spawn_server().await;
    let ghost = "ghost-99".to_string();

    // Every per-VM op on an unknown id must reconstruct as VmNotFound with the
    // structured vm_id preserved across the HTTP boundary.
    assert!(matches!(
        client.vm_state(&ghost).await,
        Err(VmmError::VmNotFound(id)) if id == ghost
    ));
    assert!(matches!(
        client.pause_vm(&ghost).await,
        Err(VmmError::VmNotFound(_))
    ));
    assert!(matches!(
        client.resume_vm(&ghost).await,
        Err(VmmError::VmNotFound(_))
    ));
    assert!(matches!(
        client.start_vm(&ghost).await,
        Err(VmmError::VmNotFound(_))
    ));
    assert!(matches!(
        client.destroy_vm(&ghost).await,
        Err(VmmError::VmNotFound(_))
    ));
    // Guest IP lookup on an unknown VM → VmNotFound (not a None IP).
    assert!(matches!(
        client.vm_guest_ip(&ghost).await,
        Err(VmmError::VmNotFound(_))
    ));
    assert!(matches!(
        client.snapshot_vm(&ghost).await,
        Err(VmmError::VmNotFound(id)) if id == ghost
    ));
}

#[tokio::test]
async fn restore_missing_snapshot_returns_snapshot_not_found() {
    let client = spawn_server().await;

    let bogus = Snapshot {
        vm_id: "ghost-99".into(),
        state_path: "/tmp/tikod-api-test-nope.snap".into(),
        mem_path: "/tmp/tikod-api-test-nope.mem".into(),
        config: cfg("ghost-99"),
    };
    assert!(
        matches!(client.restore_vm(&bogus).await, Err(VmmError::SnapshotNotFound(id)) if id == "ghost-99")
    );
}

/// Send a raw HTTP request to the server and return the full response text.
/// Wrapped in a timeout so a server bug fails the test instead of hanging it.
async fn raw_round_trip(addr: std::net::SocketAddr, request: &[u8]) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let fut = async {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(request).await.unwrap();
        let mut resp = Vec::new();
        stream.read_to_end(&mut resp).await.unwrap();
        String::from_utf8_lossy(&resp).into_owned()
    };
    tokio::time::timeout(std::time::Duration::from_secs(5), fut)
        .await
        .expect("raw HTTP round-trip timed out")
}

#[tokio::test]
async fn bad_json_body_is_bad_request() {
    let client = spawn_server().await;
    let body = b"{not valid json";
    let mut req = format!(
        "PUT /vms HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len(),
    )
    .into_bytes();
    req.extend_from_slice(body);
    let resp = raw_round_trip(client.addr(), &req).await;
    assert!(
        resp.starts_with("HTTP/1.1 400"),
        "expected 400, got: {}",
        &resp[..resp.len().min(80)]
    );
    assert!(resp.contains("bad_request"), "missing bad_request kind: {resp}");
}

#[tokio::test]
async fn unknown_route_is_404() {
    let client = spawn_server().await;
    let resp = raw_round_trip(
        client.addr(),
        b"GET /totally/bogus HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert!(
        resp.starts_with("HTTP/1.1 404"),
        "expected 404, got: {}",
        &resp[..resp.len().min(80)]
    );
}

#[tokio::test]
async fn empty_body_provision_is_bad_request() {
    let client = spawn_server().await;
    let resp = raw_round_trip(
        client.addr(),
        b"POST /vms/provision HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert!(
        resp.starts_with("HTTP/1.1 400"),
        "expected 400, got: {}",
        &resp[..resp.len().min(80)]
    );
}

#[tokio::test]
async fn health_endpoint_reports_ok() {
    let client = spawn_server().await;
    let resp = raw_round_trip(
        client.addr(),
        b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert!(
        resp.starts_with("HTTP/1.1 200"),
        "expected 200, got: {}",
        &resp[..resp.len().min(80)]
    );
    assert!(resp.contains(r#""status":"ok""#), "missing status ok: {resp}");
}

#[tokio::test]
async fn list_vms_is_initially_empty() {
    let client = spawn_server().await;
    let resp = raw_round_trip(
        client.addr(),
        b"GET /vms HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert!(resp.starts_with("HTTP/1.1 200"));
    assert!(resp.contains(r#""vms":[]"#), "expected empty vms list: {resp}");
}
