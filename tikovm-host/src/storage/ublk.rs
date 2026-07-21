//! `remote_slow` backing: tikoblkd chunk volumes served as `/dev/ublkbN`
//! ublk devices (immutable chunks on the S3 Files store + NVMe journal).
//!
//! Lifecycle per remote_slow drive:
//! - provision: `vol_id = "<vm_id>-<drive_id>"`; create the tikoblk volume
//!   if missing (`POST /volumes {backend:"chunk"}`), attach it
//!   (`POST .../attach` -> `/dev/ublkbN`), mkfs ONLY when tikoblk reports
//!   `formatted=false` (fresh volume). Idempotent — re-provision after a
//!   destroy, or re-attach at snapshot restore, returns the same device
//!   node (tikoblk's registry reserves dev ids) and never reformats.
//! - on_destroy (TERMINAL destroy only, via `cleanup_vm`; suspend keeps the
//!   device attached): `POST .../detach`. The volume + its chunks persist
//!   on the store.
//!
//! The control API client is a minimal synchronous HTTP/1.1-over-UDS in
//! the style of `vmm/firecracker.rs`'s FcApiClient. Tests drive the same
//! logic through the [`UblkApi`] seam (no socket needed).

use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde_json::json;
use tracing::info;

use super::run_sudo;
use super::volume::RemoteBacking;
use crate::vmm::{DriveConfig, VmmError, VmmResult};

/// tikoblkd control API seam (real: HTTP over its UDS; tests: mock).
pub trait UblkApi: Send + Sync {
    /// (status_code, parsed json body; `{}` when empty).
    fn call(&self, method: &str, path: &str, body: Option<&serde_json::Value>)
    -> VmmResult<(u16, serde_json::Value)>;
}

/// mkfs seam (real: `sudo -n mkfs.ext4 -q -L <label> <device>`).
type MkfsFn = Arc<dyn Fn(&str, &str) -> VmmResult<()> + Send + Sync>;

/// chmod seam (real: `sudo -n chmod 666 <device>`).
type ChmodFn = Arc<dyn Fn(&str) -> VmmResult<()> + Send + Sync>;

/// The ublk chunk-store backing.
pub struct UblkBacking {
    api: Arc<dyn UblkApi>,
    mkfs: MkfsFn,
    chmod: ChmodFn,
}

impl UblkBacking {
    /// Real backing: tikoblkd at `sock`; mkfs/chmod via `sudo -n` (the ublk
    /// block device is root-owned).
    pub fn new(sock: &Path) -> Self {
        Self {
            api: Arc::new(HttpUblkApi {
                sock: sock.to_path_buf(),
            }),
            mkfs: Arc::new(|label, device| {
                run_sudo("mkfs.ext4", &["-q", "-L", label, device])
            }),
            chmod: Arc::new(|device| run_sudo("chmod", &["666", device])),
        }
    }

    /// Test seam: inject API + mkfs/chmod fakes.
    #[cfg(test)]
    pub fn with_seams(api: Arc<dyn UblkApi>, mkfs: MkfsFn, chmod: ChmodFn) -> Self {
        Self { api, mkfs, chmod }
    }

    /// tikoblk volume id for a drive: `<vm_id>-<drive_id>`.
    pub fn vol_id(vm_id: &str, drive_id: &str) -> String {
        format!("{vm_id}-{drive_id}")
    }
}

impl RemoteBacking for UblkBacking {
    fn provision(&self, vm_id: &str, drive: &DriveConfig) -> VmmResult<PathBuf> {
        let vol_id = Self::vol_id(vm_id, &drive.drive_id);

        // Create if missing (idempotent re-provision: a 200 means the
        // volume and its data already exist on the store).
        let (code, _) = self.api.call("GET", &format!("/volumes/{vol_id}"), None)?;
        match code {
            200 => {}
            404 => {
                let size_mb = drive.size_mb.ok_or_else(|| {
                    VmmError::InvalidConfig(format!(
                        "ublk volume {} requires size_mb",
                        drive.drive_id
                    ))
                })?;
                info!(%vol_id, size_mb, "creating tikoblk chunk volume");
                let (code, body) = self.api.call(
                    "POST",
                    "/volumes",
                    Some(&json!({"vol_id": vol_id, "size_mb": size_mb, "backend": "chunk"})),
                )?;
                if code != 201 {
                    return Err(VmmError::Backend(format!(
                        "tikoblk create {vol_id}: HTTP {code}: {body}"
                    )));
                }
            }
            other => {
                return Err(VmmError::Backend(format!(
                    "tikoblk get {vol_id}: HTTP {other}"
                )));
            }
        }

        // Attach (idempotent) -> "/dev/ublkbN" + formatted flag.
        let (code, body) = self
            .api
            .call("POST", &format!("/volumes/{vol_id}/attach"), None)?;
        if code != 200 {
            return Err(VmmError::Backend(format!(
                "tikoblk attach {vol_id}: HTTP {code}: {body}"
            )));
        }
        let device = body
            .get("device")
            .and_then(|d| d.as_str())
            .ok_or_else(|| VmmError::Backend(format!("tikoblk attach {vol_id}: no device: {body}")))?
            .to_string();
        let formatted = body
            .get("formatted")
            .and_then(|f| f.as_bool())
            .unwrap_or(false);

        // mkfs ONLY on a fresh volume — a persisted volume keeps its fs.
        if !formatted {
            info!(%vol_id, %device, drive = %drive.drive_id, "fresh ublk volume: mkfs.ext4");
            (self.mkfs)(&drive.drive_id, &device)?;
        }
        // Firecracker runs unprivileged, but udev settles fresh ublk nodes
        // at 0660 root:disk (and does so AFTER tikoblkd's own attach-time
        // chmod — udev wins that race). Relax it here, immediately before
        // the drive is handed to Firecracker.
        (self.chmod)(&device)?;
        Ok(PathBuf::from(device))
    }

    fn on_destroy(&self, vm_id: &str, drive: &DriveConfig) -> VmmResult<()> {
        let vol_id = Self::vol_id(vm_id, &drive.drive_id);
        let (code, body) = self
            .api
            .call("POST", &format!("/volumes/{vol_id}/detach"), None)?;
        // 200 = detached (or was never attached); 404 = unknown volume —
        // both fine at teardown.
        if code == 200 || code == 404 {
            Ok(())
        } else {
            Err(VmmError::Backend(format!(
                "tikoblk detach {vol_id}: HTTP {code}: {body}"
            )))
        }
    }
}

// ---------------------------------------------------------------------------
// HTTP/1.1 over the tikoblkd Unix socket (style: firecracker.rs FcApiClient)
// ---------------------------------------------------------------------------

struct HttpUblkApi {
    sock: PathBuf,
}

impl UblkApi for HttpUblkApi {
    fn call(
        &self,
        method: &str,
        path: &str,
        body: Option<&serde_json::Value>,
    ) -> VmmResult<(u16, serde_json::Value)> {
        let body_str = body.map(|b| b.to_string()).unwrap_or_default();
        let request = format!(
            "{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\n\
             Content-Length: {len}\r\nConnection: close\r\n\r\n{body}",
            len = body_str.len(),
            body = body_str,
        );
        let mut stream = std::os::unix::net::UnixStream::connect(&self.sock)
            .map_err(|e| VmmError::Backend(format!("connect tikoblk socket: {e}")))?;
        stream
            .write_all(request.as_bytes())
            .map_err(|e| VmmError::Backend(format!("write tikoblk socket: {e}")))?;

        let mut raw = Vec::new();
        stream
            .read_to_end(&mut raw)
            .map_err(|e| VmmError::Backend(format!("read tikoblk socket: {e}")))?;
        let text = String::from_utf8_lossy(&raw);
        let status = text
            .lines()
            .next()
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|s| s.parse::<u16>().ok())
            .unwrap_or(0);
        let body_start = text.find("\r\n\r\n").map(|i| i + 4).unwrap_or(text.len());
        let json_body: serde_json::Value =
            serde_json::from_str(&text[body_start..]).unwrap_or_else(|_| json!({}));
        Ok((status, json_body))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use tikovm_protocol::volume::VolumeTier;

    #[derive(Default)]
    struct MockApiState {
        calls: Vec<(String, String)>,
        volume_exists: bool,
        formatted: bool,
    }

    struct MockApi {
        state: Mutex<MockApiState>,
    }

    impl UblkApi for MockApi {
        fn call(
            &self,
            method: &str,
            path: &str,
            _body: Option<&serde_json::Value>,
        ) -> VmmResult<(u16, serde_json::Value)> {
            let mut st = self.state.lock().unwrap();
            st.calls.push((method.to_string(), path.to_string()));
            match (method, path) {
                ("GET", p) if p.starts_with("/volumes/") => {
                    if st.volume_exists {
                        Ok((200, json!({"vol_id": "vm-1-archive"})))
                    } else {
                        Ok((404, json!({})))
                    }
                }
                ("POST", "/volumes") => {
                    st.volume_exists = true;
                    Ok((201, json!({"vol_id": "vm-1-archive"})))
                }
                ("POST", p) if p.ends_with("/attach") => {
                    Ok((200, json!({"device": "/dev/ublkb7", "formatted": st.formatted})))
                }
                ("POST", p) if p.ends_with("/detach") => Ok((200, json!({"ok": true}))),
                _ => Ok((404, json!({}))),
            }
        }
    }

    fn drive() -> DriveConfig {
        DriveConfig {
            drive_id: "archive".into(),
            path: PathBuf::new(),
            read_only: false,
            size_mb: Some(64),
            tier: VolumeTier::RemoteSlow,
            source: None,
            persist_key: None,
        }
    }

    type Seams = (UblkBacking, Arc<MockApi>, Arc<Mutex<Vec<(String, String)>>>);

    fn seam() -> Seams {
        let api = Arc::new(MockApi {
            state: Mutex::new(MockApiState::default()),
        });
        let mkfs_calls = Arc::new(Mutex::new(Vec::new()));
        let mc = mkfs_calls.clone();
        let backing = UblkBacking::with_seams(
            api.clone(),
            Arc::new(move |label, dev| {
                mc.lock()
                    .unwrap()
                    .push((label.to_string(), dev.to_string()));
                Ok(())
            }),
            Arc::new(|_dev| Ok(())),
        );
        (backing, api, mkfs_calls)
    }

    #[test]
    fn vol_id_mapping() {
        assert_eq!(UblkBacking::vol_id("vm-1", "archive"), "vm-1-archive");
    }

    #[test]
    fn provision_creates_attaches_formats_once() {
        let (backing, api, mkfs) = seam();
        let p = backing.provision("vm-1", &drive()).unwrap();
        assert_eq!(p, PathBuf::from("/dev/ublkb7"));
        let calls = api.state.lock().unwrap().calls.clone();
        assert_eq!(
            calls,
            vec![
                ("GET".to_string(), "/volumes/vm-1-archive".to_string()),
                ("POST".to_string(), "/volumes".to_string()),
                ("POST".to_string(), "/volumes/vm-1-archive/attach".to_string()),
            ]
        );
        assert_eq!(
            mkfs.lock().unwrap().as_slice(),
            &[("archive".to_string(), "/dev/ublkb7".to_string())],
            "unformatted volume is mkfs'd"
        );
    }

    #[test]
    fn reprovision_is_idempotent_no_second_create_no_mkfs() {
        let (backing, api, mkfs) = seam();
        {
            let mut st = api.state.lock().unwrap();
            st.volume_exists = true;
            st.formatted = true; // persisted volume from an earlier boot
        }
        let p = backing.provision("vm-1", &drive()).unwrap();
        assert_eq!(p, PathBuf::from("/dev/ublkb7"));
        let calls = api.state.lock().unwrap().calls.clone();
        assert_eq!(
            calls,
            vec![
                ("GET".to_string(), "/volumes/vm-1-archive".to_string()),
                ("POST".to_string(), "/volumes/vm-1-archive/attach".to_string()),
            ],
            "no second create for an existing volume"
        );
        assert!(mkfs.lock().unwrap().is_empty(), "formatted volume is never reformatted");
    }

    #[test]
    fn on_destroy_detaches() {
        let (backing, api, _mkfs) = seam();
        backing.on_destroy("vm-1", &drive()).unwrap();
        let calls = api.state.lock().unwrap().calls.clone();
        assert_eq!(
            calls,
            vec![("POST".to_string(), "/volumes/vm-1-archive/detach".to_string())]
        );
    }

    #[test]
    fn ublk_requires_size_mb() {
        let (backing, _api, _mkfs) = seam();
        let mut d = drive();
        d.size_mb = None;
        assert!(matches!(
            backing.provision("vm-1", &d),
            Err(VmmError::InvalidConfig(_))
        ));
    }
}
