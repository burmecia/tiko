//! tikoguest HTTP API round-trip tests.
//!
//! Drives the full HTTP → server → `pg_ctl` path with a *fake* `pg_ctl` shell
//! script, so it runs anywhere (no Postgres / KVM / VM required). The fake
//! script records every invocation and simulates a postmaster via a marker file
//! so start/stop/status behave like the real thing.

use std::net::SocketAddr;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tikoguest::pgops::PgCtl;
use tikoguest::server::PgServer;

/// A temp data dir + fake pg_ctl wiring. The fake script's state (a marker
/// file) lives under `state_dir` so the test can assert server-side effects.
struct Fixture {
    data_dir: PathBuf,
    config_file: PathBuf,
    #[allow(dead_code)]
    pg_ctl: PathBuf,
    #[allow(dead_code)]
    initdb: PathBuf,
    state_dir: PathBuf,
    server_addr: SocketAddr,
}

impl Fixture {
    /// Bring up the agent on an ephemeral port with a fake pg_ctl.
    async fn start() -> Self {
        let root = std::env::temp_dir().join(format!(
            "tikoguest-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();

        let data_dir = root.join("data");
        std::fs::create_dir_all(&data_dir).unwrap();
        // Pretend the cluster is initialized.
        std::fs::write(data_dir.join("PG_VERSION"), "17.0\n").unwrap();

        let state_dir = root.join("state");
        std::fs::create_dir_all(&state_dir).unwrap();

        let pg_ctl = root.join("fake-pg_ctl");
        // Bake the state dir path into the script so it doesn't depend on a
        // (racy, parallel-test-unfriendly) env var.
        std::fs::write(
            &pg_ctl,
            FAKE_PG_CTL_TEMPLATE.replace("@STATE_DIR@", state_dir.to_str().unwrap()),
        )
        .unwrap();
        let mut perms = std::fs::metadata(&pg_ctl).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&pg_ctl, perms).unwrap();

        // Fake initdb: writes a minimal cluster layout into -D <dir>.
        let initdb = root.join("fake-initdb");
        std::fs::write(
            &initdb,
            FAKE_INITDB_TEMPLATE.replace("@STATE_DIR@", state_dir.to_str().unwrap()),
        )
        .unwrap();
        let mut perms = std::fs::metadata(&initdb).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&initdb, perms).unwrap();

        // Override-config template in PGHOME (data dir's parent), matching
        // create_rootfs.sh so init() copies it into the data dir.
        std::fs::write(
            data_dir.parent().unwrap().join("postgresql.tiko.conf"),
            "log_min_messages=info\nlisten_addresses='*'\n",
        )
        .unwrap();

        // Per-VM identity file (written by start_vm.sh on the host). Distinct
        // values so pass-through tests can assert the agent forwards them.
        std::fs::write(
            data_dir.parent().unwrap().join("tiko.env"),
            "TIKO_ORG_ID=77\nTIKO_DB_ID=88\nTIKO_PROJECT_ID=99\n\
             TIKO_STORAGE_ROOT=/mnt/s3files/tiko_root\n\
             TIKO_LOCAL_PATH=/var/lib/postgresql/tiko_local\n",
        )
        .unwrap();

        let config_file = data_dir.join("postgresql.tiko.conf");
        let log_path = root.join("log.log");

        let ctl = PgCtl::new(
            pg_ctl.clone(),
            data_dir.clone(),
            log_path,
            config_file.clone(),
        )
        .with_initdb(initdb.clone());
        let server = Arc::new(PgServer::new(
            ctl,
            PathBuf::from("tiko_pitr"),
            PathBuf::from("tiko_branch"),
        ));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server_addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = server.serve(listener).await;
        });

        Self {
            data_dir,
            config_file,
            #[allow(dead_code)]
            pg_ctl,
            initdb,
            state_dir,
            server_addr,
        }
    }

    /// Mark the fake postmaster as "running" by writing the marker the script
    /// checks, plus a postmaster.pid the agent reads.
    fn set_running(&self, pid: i32) {
        std::fs::write(self.state_dir.join("running"), pid.to_string()).unwrap();
        std::fs::write(self.data_dir.join("postmaster.pid"), format!("{pid}\n")).unwrap();
    }

    fn initdb_calls_log(&self) -> Vec<String> {
        std::fs::read_to_string(self.state_dir.join("initdb_calls.log"))
            .unwrap_or_default()
            .lines()
            .map(String::from)
            .collect()
    }

    /// The captured `TIKO_*` env the spawned `pg_ctl` received (one block per
    /// invocation).
    fn pg_ctl_env_log(&self) -> String {
        std::fs::read_to_string(self.state_dir.join("pg_ctl_env.log")).unwrap_or_default()
    }

    /// The captured `TIKO_*` env the spawned `initdb` received.
    fn initdb_env_log(&self) -> String {
        std::fs::read_to_string(self.state_dir.join("initdb_env.log")).unwrap_or_default()
    }

    fn calls_log(&self) -> Vec<String> {
        std::fs::read_to_string(self.state_dir.join("calls.log"))
            .unwrap_or_default()
            .lines()
            .map(String::from)
            .collect()
    }

    async fn request(&self, method: &str, path: &str, body: Option<&str>) -> (u16, String) {
        let body_bytes = body.unwrap_or("").as_bytes();
        let req = format!(
            "{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\n\
             Content-Length: {}\r\nConnection: close\r\n\r\n",
            body_bytes.len(),
        )
        .into_bytes();
        let payload = [req.as_slice(), body_bytes].concat();

        let fut = async {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut stream = tokio::net::TcpStream::connect(self.server_addr)
                .await
                .unwrap();
            stream.write_all(&payload).await.unwrap();
            let mut resp = Vec::new();
            stream.read_to_end(&mut resp).await.unwrap();
            String::from_utf8_lossy(&resp).into_owned()
        };
        let text = tokio::time::timeout(Duration::from_secs(5), fut)
            .await
            .expect("tikoguest request timed out");

        let (status, body) = split_response(&text);
        (status, body.to_string())
    }
}

fn split_response(text: &str) -> (u16, &str) {
    let end = text.find("\r\n\r\n").unwrap();
    let status = text[..end]
        .lines()
        .next()
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse::<u16>()
        .unwrap();
    (status, &text[end + 4..])
}

#[tokio::test]
async fn health_reports_uninitialized_then_initialized_state() {
    let f = Fixture::start().await;

    // Initially: initialized (we wrote PG_VERSION) but not running.
    let (status, body) = f.request("GET", "/health", None).await;
    assert_eq!(status, 200);
    assert!(body.contains(r#""initialized":true"#), "{body}");
    assert!(body.contains(r#""running":false"#), "{body}");

    f.set_running(4242);
    let (_status, body) = f.request("GET", "/health", None).await;
    assert!(body.contains(r#""running":true"#), "{body}");
}

#[tokio::test]
async fn status_includes_version_and_pid() {
    let f = Fixture::start().await;
    f.set_running(4242);

    let (status, body) = f.request("GET", "/pg/status", None).await;
    assert_eq!(status, 200);
    assert!(body.contains(r#""version":"17.0""#), "{body}");
    assert!(body.contains(r#""pid":4242"#), "{body}");
    assert!(body.contains(r#""running":true"#), "{body}");
}

#[tokio::test]
async fn start_then_stop_drives_pg_ctl() {
    let f = Fixture::start().await;

    // start → 204, and the fake pg_ctl recorded a "start" call.
    let (status, _body) = f.request("POST", "/pg/start", None).await;
    assert_eq!(status, 204);
    let calls = f.calls_log();
    assert!(calls.iter().any(|c| c.ends_with(" start")), "{calls:?}");

    // stop (default fast) → 204, recorded a "stop -m fast" call.
    let (status, _body) = f.request("POST", "/pg/stop", None).await;
    assert_eq!(status, 204);
    let calls = f.calls_log();
    assert!(
        calls
            .iter()
            .any(|c| c.contains(" -m fast ") && c.ends_with(" stop")),
        "{calls:?}"
    );
}

#[tokio::test]
async fn stop_with_explicit_mode_is_validated() {
    let f = Fixture::start().await;
    // stop() short-circuits to a no-op when postgres isn't running, so mark it
    // running first to actually exercise the pg_ctl stop invocation.
    f.set_running(1234);

    let (status, _body) = f
        .request("POST", "/pg/stop", Some(r#"{"mode":"immediate"}"#))
        .await;
    assert_eq!(status, 204);
    let calls = f.calls_log();
    assert!(
        calls
            .iter()
            .any(|c| c.contains(" -m immediate ") && c.ends_with(" stop")),
        "{calls:?}"
    );

    // An invalid mode must be rejected as 400, not passed to pg_ctl.
    let (status, body) = f
        .request("POST", "/pg/stop", Some(r#"{"mode":"explode"}"#))
        .await;
    assert_eq!(status, 400, "{body}");
    assert!(body.contains("bad_request"), "{body}");
}

#[tokio::test]
async fn restart_and_reload_hit_pg_ctl() {
    let f = Fixture::start().await;

    let (status, _) = f.request("POST", "/pg/restart", None).await;
    assert_eq!(status, 204);
    let (status, _) = f.request("POST", "/pg/reload", None).await;
    assert_eq!(status, 204);

    let calls = f.calls_log();
    assert!(calls.iter().any(|c| c.ends_with(" restart")), "{calls:?}");
    assert!(calls.iter().any(|c| c.ends_with(" reload")), "{calls:?}");
}

#[tokio::test]
async fn config_get_returns_empty_when_no_override_file() {
    let f = Fixture::start().await;
    let (status, body) = f.request("GET", "/pg/config", None).await;
    assert_eq!(status, 200);
    assert!(body.contains(r#""settings":{}"#), "{body}");
}

#[tokio::test]
async fn config_put_writes_file_and_reloads() {
    let f = Fixture::start().await;

    let (status, body) = f
        .request(
            "PUT",
            "/pg/config",
            Some(r#"{"settings":{"max_connections":"200","log_min_messages":"info"}}"#),
        )
        .await;
    assert_eq!(status, 204, "{body}");

    // The override file now contains the settings.
    let written = std::fs::read_to_string(&f.config_file).unwrap();
    assert!(written.contains("max_connections = 200"), "{written}");
    assert!(written.contains("log_min_messages = 'info'"), "{written}");

    // And reload was invoked.
    let calls = f.calls_log();
    assert!(calls.iter().any(|c| c.ends_with(" reload")), "{calls:?}");

    // GET reflects the written settings.
    let (status, body) = f.request("GET", "/pg/config", None).await;
    assert_eq!(status, 200);
    assert!(body.contains(r#""max_connections":"200""#), "{body}");
}

#[tokio::test]
async fn unknown_route_is_404() {
    let f = Fixture::start().await;
    let (status, body) = f.request("GET", "/bogus", None).await;
    assert_eq!(status, 404);
    assert!(body.contains("not_found"), "{body}");
}

#[tokio::test]
async fn start_on_uninitialized_cluster_is_conflict() {
    let f = Fixture::start().await;
    // Remove PG_VERSION so is_initialized() is false.
    std::fs::remove_file(f.data_dir.join("PG_VERSION")).unwrap();

    let (status, body) = f.request("POST", "/pg/start", None).await;
    assert_eq!(status, 409, "{body}");
    assert!(body.contains("not_initialized"), "{body}");
}

#[tokio::test]
async fn init_creates_cluster_and_wires_config() {
    let f = Fixture::start().await;
    // Start from an empty data dir (uninitialized).
    std::fs::remove_file(f.data_dir.join("PG_VERSION")).unwrap();

    let (status, body) = f.request("POST", "/pg/init", None).await;
    assert_eq!(status, 204, "{body}");

    // initdb was invoked.
    let calls = f.initdb_calls_log();
    assert!(
        calls
            .iter()
            .any(|c| c.contains("-D") && c.contains("--auth=trust")),
        "{calls:?}"
    );

    // PG_VERSION is now present (written by the fake initdb).
    assert!(
        f.data_dir.join("PG_VERSION").exists(),
        "PG_VERSION missing after init"
    );

    // The override config was copied from the PGHOME template into the data dir.
    let override_conf = std::fs::read_to_string(&f.config_file).unwrap();
    assert!(
        override_conf.contains("log_min_messages=info"),
        "{override_conf}"
    );

    // postgresql.conf has the include_if_exists hook.
    let pg_conf = std::fs::read_to_string(f.data_dir.join("postgresql.conf")).unwrap();
    assert!(
        pg_conf.contains("include_if_exists='postgresql.tiko.conf'"),
        "{pg_conf}"
    );

    // pg_hba.conf has the trust line for the per-VM subnet.
    let hba = std::fs::read_to_string(f.data_dir.join("pg_hba.conf")).unwrap();
    assert!(hba.contains("host all all 172.16.0.0/16 trust"), "{hba}");
}

/// Regression: on a fresh VM the data directory does not exist at all, so
/// `pg_ctl status` exits 4 with "directory ... does not exist". `init`'s
/// running-guard must treat that as "not running" (not a 500) and proceed.
#[tokio::test]
async fn init_when_data_dir_does_not_exist_is_not_500() {
    let f = Fixture::start().await;
    // Reproduce the reported state: the entire data dir is absent.
    std::fs::remove_dir_all(&f.data_dir).unwrap();
    assert!(!f.data_dir.exists());

    let (status, body) = f.request("POST", "/pg/init", None).await;
    assert_eq!(
        status, 204,
        "init on a missing data dir returned {status}: {body}"
    );
    // initdb ran and re-created the data dir.
    assert!(f.data_dir.join("PG_VERSION").exists());
    assert!(!f.initdb_calls_log().is_empty());
}

#[tokio::test]
async fn init_on_initialized_without_force_is_conflict() {
    let f = Fixture::start().await;
    // Fixture pre-creates PG_VERSION → already initialized.

    let (status, body) = f.request("POST", "/pg/init", None).await;
    assert_eq!(status, 409, "{body}");
    assert!(body.contains("already_initialized"), "{body}");

    // initdb must NOT have run.
    assert!(f.initdb_calls_log().is_empty());
}

#[tokio::test]
async fn init_with_force_wipes_and_re_inits() {
    let f = Fixture::start().await;
    // Plant a stale file inside the data dir to prove force wipes it.
    let stale = f.data_dir.join("stale.marker");
    std::fs::write(&stale, "old").unwrap();
    assert!(stale.exists());

    let (status, body) = f
        .request("POST", "/pg/init", Some(r#"{"force":true}"#))
        .await;
    assert_eq!(status, 204, "{body}");

    // Stale file is gone (the data dir was wiped before initdb).
    assert!(!stale.exists(), "force did not wipe the data dir");
    // initdb ran and re-initialized.
    assert!(f.data_dir.join("PG_VERSION").exists());
    assert!(!f.initdb_calls_log().is_empty());
}

#[tokio::test]
async fn init_refuses_while_running() {
    let f = Fixture::start().await;
    // An initialized, running cluster: PG_VERSION present (from the fixture)
    // + a live postmaster marker. force=true bypasses AlreadyInitialized so the
    // StillRunning guard is what triggers.
    f.set_running(999);

    let (status, body) = f
        .request("POST", "/pg/init", Some(r#"{"force":true}"#))
        .await;
    assert_eq!(status, 409, "{body}");
    assert!(body.contains("still_running"), "{body}");
}

#[tokio::test]
async fn init_invalid_body_is_bad_request() {
    let f = Fixture::start().await;
    let (status, body) = f
        .request("POST", "/pg/init", Some(r#"{"force":"not-a-bool"}"#))
        .await;
    assert_eq!(status, 400, "{body}");
    assert!(body.contains("bad_request"), "{body}");
}

#[tokio::test]
async fn start_passes_tiko_identity_to_pg_ctl() {
    let f = Fixture::start().await;
    // A start invocation drives `pg_ctl start`, which must inherit the per-VM
    // identity so the in-guest tikoworker reads the right org/db/project.
    let (status, body) = f.request("POST", "/pg/start", None).await;
    assert_eq!(status, 204, "{body}");

    let env = f.pg_ctl_env_log();
    assert!(
        env.contains("TIKO_ORG_ID=77"),
        "missing TIKO_ORG_ID:\n{env}"
    );
    assert!(env.contains("TIKO_DB_ID=88"), "missing TIKO_DB_ID:\n{env}");
    assert!(
        env.contains("TIKO_PROJECT_ID=99"),
        "missing TIKO_PROJECT_ID:\n{env}"
    );
    assert!(
        env.contains("TIKO_STORAGE_ROOT=/mnt/s3files/tiko_root"),
        "missing TIKO_STORAGE_ROOT:\n{env}"
    );
}

#[tokio::test]
async fn init_passes_tiko_identity_to_initdb() {
    let f = Fixture::start().await;
    std::fs::remove_file(f.data_dir.join("PG_VERSION")).unwrap();
    let (status, body) = f.request("POST", "/pg/init", None).await;
    assert_eq!(status, 204, "{body}");

    let env = f.initdb_env_log();
    assert!(env.contains("TIKO_DB_ID=88"), "missing TIKO_DB_ID:\n{env}");
    assert!(
        env.contains("TIKO_PROJECT_ID=99"),
        "missing TIKO_PROJECT_ID:\n{env}"
    );
}

#[tokio::test]
async fn tiko_env_defaults_when_file_absent() {
    // No tiko.env → the agent must still provide the tiko_env.sh defaults so
    // postgres doesn't panic on a missing identity var.
    let root = std::env::temp_dir().join(format!(
        "tikoguest-nodefault-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(root.join("data")).unwrap();
    let pg_ctl = root.join("fake-pg_ctl");
    std::fs::write(
        &pg_ctl,
        FAKE_PG_CTL_TEMPLATE.replace("@STATE_DIR@", root.to_str().unwrap()),
    )
    .unwrap();
    use std::os::unix::fs::PermissionsExt;
    let mut perms = std::fs::metadata(&pg_ctl).unwrap().permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&pg_ctl, perms).unwrap();

    let ctl = PgCtl::new(
        pg_ctl,
        root.join("data"),
        root.join("log.log"),
        root.join("data").join("postgresql.tiko.conf"),
    );
    // Defaults: org 12, db 34, project 56.
    assert_eq!(ctl.tiko_env().get("TIKO_ORG_ID").unwrap(), "12");
    assert_eq!(ctl.tiko_env().get("TIKO_DB_ID").unwrap(), "34");
    assert_eq!(ctl.tiko_env().get("TIKO_PROJECT_ID").unwrap(), "56");
}

/// The fake `pg_ctl` template. Emulates start/stop/status/restart/reload using
/// a marker file under the baked-in state dir. Appends every argv line to
/// `calls.log` for assertions. `@STATE_DIR@` is substituted per fixture.
const FAKE_PG_CTL_TEMPLATE: &str = include_str!("fake_pg_ctl.sh");

/// The fake `initdb` template. Writes a minimal cluster layout (PG_VERSION +
/// empty postgresql.conf + empty pg_hba.conf) into the `-D <dir>` argument so
/// the agent's post-init wiring (append to those files) succeeds.
const FAKE_INITDB_TEMPLATE: &str = include_str!("fake_initdb.sh");
