//! Apple Virtualization Framework backend (macOS only).
//!
//! Uses Apple's [`Virtualization`] framework via the [`arcbox_vz`] Rust crate
//! to run Linux VMs on macOS **without** KVM. This is the development backend.
//!
//! ## Architecture
//!
//! VZ operations must run on a single persistent thread with a stable
//! current-thread tokio runtime + `LocalSet`. This is because:
//! 1. VZ futures contain raw pointers (`*mut AnyObject`) that aren't `Send`
//! 2. VZ completion handlers may be tied to the runtime context
//!
//! The [`AppleVzVmm`] struct sends commands to a dedicated VZ thread via
//! a channel, and receives results via oneshot channels.
//!
//! [`Virtualization`]: https://developer.apple.com/documentation/virtualization

use std::collections::HashMap;
use std::ffi::c_void;
use std::net::IpAddr;
use std::os::unix::io::RawFd;
use std::path::PathBuf;
use std::time::Duration;

use arcbox_vz::{
    EntropyDeviceConfiguration, GenericPlatform, LinuxBootLoader, MemoryBalloonDeviceConfiguration,
    NetworkDeviceConfiguration, SerialPortConfiguration, StorageDeviceConfiguration,
    VirtualMachine, VirtualMachineConfiguration,
};
use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot};
use tracing::{info, warn};

use super::{Snapshot, VmConfig, VmId, VmInfo, VmState, Vmm, VmmError, VmmResult};

// ============================================================================
// CoreFoundation Run Loop FFI
// ============================================================================

/// Direct ObjC FFI for VZ lifecycle operations.
///
/// VZ's pause/resume methods need to be called on the VM's dispatch queue
/// (via `dispatch_async_f`) and require a non-nil completion handler block
/// to finalize the state transition.
mod objc_ffi {
    use super::c_void;
    use std::sync::atomic::{AtomicBool, Ordering};

    unsafe extern "C" {
        fn objc_msgSend(obj: *mut c_void, sel: *mut c_void, ...) -> *mut c_void;
        fn sel_registerName(name: *const u8) -> *mut c_void;
        fn dispatch_async_f(
            queue: *mut c_void,
            context: *mut c_void,
            work: extern "C" fn(*mut c_void),
        );
        static _NSConcreteStackBlock: *const c_void;
        fn _Block_copy(block: *const c_void) -> *mut c_void;
        fn _Block_release(block: *const c_void);
    }

    /// Completion flag set by the pause/resume completion handler block.
    static COMPLETED: AtomicBool = AtomicBool::new(false);

    /// Block descriptor (no copy/dispose helpers needed).
    #[repr(C)]
    struct BlockDescriptor {
        reserved: u64,
        size: u64,
    }
    static DESCRIPTOR: BlockDescriptor = BlockDescriptor {
        reserved: 0,
        size: std::mem::size_of::<Block>() as u64,
    };

    /// ObjC block layout for a `^(NSError *)` completion handler.
    #[repr(C)]
    struct Block {
        isa: *const c_void,
        flags: i32,
        reserved: i32,
        invoke: unsafe extern "C" fn(*const Block, *mut c_void),
        descriptor: *const BlockDescriptor,
    }

    /// The completion handler invoke function — called by VZ when the
    /// pause/resume operation finishes.
    unsafe extern "C" fn completion_invoke(_block: *const Block, _error: *mut c_void) {
        COMPLETED.store(true, Ordering::SeqCst);
    }

    /// Create a heap-copied completion handler block.
    fn create_completion_block() -> *mut c_void {
        let block = Block {
            isa: unsafe { _NSConcreteStackBlock },
            flags: 0,
            reserved: 0,
            invoke: completion_invoke,
            descriptor: &DESCRIPTOR,
        };
        unsafe { _Block_copy(&block as *const _ as *const c_void) }
    }

    /// Context passed to the `dispatch_async_f` work function.
    struct PauseContext {
        inner: *mut c_void,
        completion: *mut c_void,
    }

    extern "C" fn pause_work(ctx: *mut c_void) {
        let pc = unsafe { &*(ctx as *const PauseContext) };
        let sel = reg_sel("pauseWithCompletionHandler:");
        let func: unsafe extern "C" fn(*mut c_void, *mut c_void, *mut c_void) -> *mut c_void =
            unsafe { std::mem::transmute(objc_msgSend as *const ()) };
        unsafe { func(pc.inner, sel, pc.completion) };
    }

    extern "C" fn resume_work(ctx: *mut c_void) {
        let pc = unsafe { &*(ctx as *const PauseContext) };
        let sel = reg_sel("resumeWithCompletionHandler:");
        let func: unsafe extern "C" fn(*mut c_void, *mut c_void, *mut c_void) -> *mut c_void =
            unsafe { std::mem::transmute(objc_msgSend as *const ()) };
        unsafe { func(pc.inner, sel, pc.completion) };
    }

    fn reg_sel(name: &str) -> *mut c_void {
        let c_name = std::ffi::CString::new(name).unwrap();
        unsafe { sel_registerName(c_name.as_ptr() as *const u8) }
    }

    fn vm_inner_ptr(vm: &arcbox_vz::VirtualMachine) -> *mut c_void {
        unsafe {
            let base = vm as *const _ as *const c_void;
            *(base as *const *mut c_void)
        }
    }

    fn vm_queue_ptr(vm: &arcbox_vz::VirtualMachine) -> *mut c_void {
        unsafe {
            let base = vm as *const _ as *const u8;
            *(base.add(8) as *const *mut c_void)
        }
    }

    /// Dispatch `pauseWithCompletionHandler:` on the VM's serial queue.
    /// Returns immediately; poll `is_completed()` to detect completion.
    pub fn pause(vm: &arcbox_vz::VirtualMachine) {
        COMPLETED.store(false, Ordering::SeqCst);
        let ctx = Box::new(PauseContext {
            inner: vm_inner_ptr(vm),
            completion: create_completion_block(),
        });
        let ctx_ptr = Box::into_raw(ctx) as *mut c_void;
        // Leak the context — the work function references it, and we can't
        // safely free it until the block is released. For a dev tool this
        // small leak per pause/resume is acceptable.
        unsafe { dispatch_async_f(vm_queue_ptr(vm), ctx_ptr, pause_work) };
    }

    /// Dispatch `resumeWithCompletionHandler:` on the VM's serial queue.
    pub fn resume(vm: &arcbox_vz::VirtualMachine) {
        COMPLETED.store(false, Ordering::SeqCst);
        let ctx = Box::new(PauseContext {
            inner: vm_inner_ptr(vm),
            completion: create_completion_block(),
        });
        let ctx_ptr = Box::into_raw(ctx) as *mut c_void;
        unsafe { dispatch_async_f(vm_queue_ptr(vm), ctx_ptr, resume_work) };
    }

    /// Check if the pause/resume completion handler has fired.
    pub fn is_completed() -> bool {
        COMPLETED.load(Ordering::SeqCst)
    }

    // --- PL011 UART support for serial console capture ---

    unsafe extern "C" {
        fn objc_getClass(name: *const u8) -> *mut c_void;
        fn dlopen(path: *const u8, mode: i32) -> *mut c_void;
    }

    /// Ensure the Virtualization framework is loaded.
    fn ensure_vz_framework() {
        unsafe {
            let path = b"/System/Library/Frameworks/Virtualization.framework/Virtualization\0";
            dlopen(path.as_ptr(), 2); // RTLD_NOW
        }
    }

    /// Get an ObjC class by name. Returns nil if not found.
    fn cls(name: &str) -> *mut c_void {
        ensure_vz_framework();
        let c_name = std::ffi::CString::new(name).unwrap();
        unsafe { objc_getClass(c_name.as_ptr() as *const u8) }
    }

    /// Get an ObjC selector.
    fn sl(name: &str) -> *mut c_void {
        let c_name = std::ffi::CString::new(name).unwrap();
        unsafe { sel_registerName(c_name.as_ptr() as *const u8) }
    }

    /// Add a PL011 UART serial port to the VZ config's serialPorts array.
    ///
    /// IMPORTANT: Must be called BEFORE `config.build()`, which calls
    /// `apply_devices()`. We set serialPorts directly on the ObjC object.
    /// Since arcbox-vz's Rust Vec is empty (we skip `add_serial_port`),
    /// `apply_devices()` won't overwrite our setting.
    ///
    /// Returns the read fd for the UART pipe.
    pub fn add_pl011_uart(
        vz_config: &arcbox_vz::VirtualMachineConfiguration,
    ) -> Option<std::os::unix::io::RawFd> {
        unsafe {
            // Create a pipe for UART I/O
            let mut fds = [0i32; 2];
            if libc::pipe(fds.as_mut_ptr()) != 0 {
                return None;
            }
            let read_fd = fds[0];
            let write_fd = fds[1];

            // NSFileHandle for read and write
            let ns_fh_cls = cls("NSFileHandle");
            if ns_fh_cls.is_null() { libc::close(write_fd); libc::close(read_fd); return None; }

            let f_alloc: unsafe extern "C" fn(*mut c_void, *mut c_void) -> *mut c_void =
                std::mem::transmute(objc_msgSend as *const ());
            let f_init_fd: unsafe extern "C" fn(*mut c_void, *mut c_void, i32) -> *mut c_void =
                std::mem::transmute(objc_msgSend as *const ());

            let read_handle = f_init_fd(f_alloc(ns_fh_cls, sl("alloc")), sl("initWithFileDescriptor:"), read_fd);
            let write_handle = f_init_fd(f_alloc(ns_fh_cls, sl("alloc")), sl("initWithFileDescriptor:"), write_fd);

            if read_handle.is_null() || write_handle.is_null() {
                libc::close(write_fd); libc::close(read_fd);
                return None;
            }

            // VZFileHandleSerialPortAttachment
            let attach_cls = cls("VZFileHandleSerialPortAttachment");
            if attach_cls.is_null() { libc::close(write_fd); libc::close(read_fd); return None; }

            let f_init_attach: unsafe extern "C" fn(
                *mut c_void, *mut c_void, *mut c_void, *mut c_void,
            ) -> *mut c_void = std::mem::transmute(objc_msgSend as *const ());

            let attachment = f_init_attach(
                f_alloc(attach_cls, sl("alloc")),
                sl("initWithFileHandleForReading:fileHandleForWriting:"),
                read_handle, write_handle,
            );
            if attachment.is_null() { libc::close(write_fd); libc::close(read_fd); return None; }

            // VZUARTSerialPortConfiguration
            let uart_cls = cls("VZUARTSerialPortConfiguration");
            if uart_cls.is_null() {
                tracing::warn!("VZUARTSerialPortConfiguration class not found — no UART console");
                libc::close(write_fd); libc::close(read_fd);
                return None;
            }

            let f_init_simple: unsafe extern "C" fn(*mut c_void, *mut c_void) -> *mut c_void =
                std::mem::transmute(objc_msgSend as *const ());
            let uart_config = f_init_simple(f_alloc(uart_cls, sl("alloc")), sl("init"));
            if uart_config.is_null() { libc::close(write_fd); libc::close(read_fd); return None; }

            let f_set: unsafe extern "C" fn(*mut c_void, *mut c_void, *mut c_void) =
                std::mem::transmute(objc_msgSend as *const ());
            f_set(uart_config, sl("setAttachment:"), attachment);

            // Create NSArray containing the UART config
            let ns_array_cls = cls("NSArray");
            let f_array_with_obj: unsafe extern "C" fn(
                *mut c_void, *mut c_void, *mut c_void,
            ) -> *mut c_void = std::mem::transmute(objc_msgSend as *const ());
            let serial_ports_array = f_array_with_obj(
                ns_array_cls,
                sl("arrayWithObject:"),
                uart_config,
            );

            if serial_ports_array.is_null() {
                libc::close(write_fd); libc::close(read_fd);
                return None;
            }

            // Set serialPorts on the VZ config's inner ObjC object.
            // Access inner pointer at offset 0 of VirtualMachineConfiguration.
            let inner = {
                let struct_ptr: *const arcbox_vz::VirtualMachineConfiguration = vz_config;
                *(struct_ptr as *const *mut c_void)
            };

            f_set(inner, sl("setSerialPorts:"), serial_ports_array);

            Some(read_fd)
        }
    }
}

/// Commands sent to the VZ worker thread.
enum VzCommand {
    Create {
        config: VmConfig,
        reply: oneshot::Sender<VmmResult<VmId>>,
    },
    Start {
        vm_id: VmId,
        reply: oneshot::Sender<VmmResult<()>>,
    },
    Pause {
        vm_id: VmId,
        reply: oneshot::Sender<VmmResult<()>>,
    },
    Resume {
        vm_id: VmId,
        reply: oneshot::Sender<VmmResult<()>>,
    },
    Snapshot {
        vm_id: VmId,
        reply: oneshot::Sender<VmmResult<Snapshot>>,
    },
    Restore {
        snapshot: Snapshot,
        reply: oneshot::Sender<VmmResult<VmId>>,
    },
    Destroy {
        vm_id: VmId,
        reply: oneshot::Sender<VmmResult<()>>,
    },
    State {
        vm_id: VmId,
        reply: oneshot::Sender<VmmResult<VmState>>,
    },
    GuestIp {
        vm_id: VmId,
        reply: oneshot::Sender<VmmResult<Option<IpAddr>>>,
    },
    List {
        reply: oneshot::Sender<VmmResult<Vec<VmInfo>>>,
    },
}

/// Internal VM state on the VZ thread.
struct VzVmEntry {
    vm: VirtualMachine,
    config: VmConfig,
    guest_ip: Option<IpAddr>,
    serial_read_fd: Option<RawFd>,
    uart_read_fd: Option<RawFd>,
}

/// Apple Virtualization Framework VMM backend.
///
/// Sends all VZ operations to a dedicated worker thread via a channel.
/// Not for production use — development/testing only.
pub struct AppleVzVmm {
    cmd_tx: mpsc::Sender<VzCommand>,
    snapshot_dir: PathBuf,
    runtime_dir: PathBuf,
}

impl AppleVzVmm {
    pub fn new(snapshot_dir: PathBuf) -> Self {
        let runtime_dir = snapshot_dir.join("runtime");
        std::fs::create_dir_all(&runtime_dir).ok();
        std::fs::create_dir_all(&snapshot_dir).ok();

        let (cmd_tx, cmd_rx) = mpsc::channel(64);

        // Spawn the persistent VZ worker thread.
        let snap_dir = snapshot_dir.clone();
        let rt_dir = runtime_dir.clone();
        std::thread::Builder::new()
            .name("vz-worker".into())
            .spawn(move || {
                vz_worker_thread(cmd_rx, snap_dir, rt_dir);
            })
            .expect("failed to spawn VZ worker thread");

        Self {
            cmd_tx,
            snapshot_dir,
            runtime_dir,
        }
    }

    /// Send a command to the VZ worker and await the result.
    async fn send<R>(
        &self,
        make_cmd: impl FnOnce(oneshot::Sender<VmmResult<R>>) -> VzCommand,
    ) -> VmmResult<R> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(make_cmd(tx))
            .await
            .map_err(|_| VmmError::Backend("VZ worker thread died".into()))?;
        rx.await
            .map_err(|_| VmmError::Backend("VZ worker dropped reply".into()))?
    }
}

#[async_trait]
impl Vmm for AppleVzVmm {
    async fn create_vm(&self, config: VmConfig) -> VmmResult<VmId> {
        self.send(|tx| VzCommand::Create { config, reply: tx }).await
    }

    async fn start_vm(&self, vm_id: &VmId) -> VmmResult<()> {
        self.send(|tx| VzCommand::Start {
            vm_id: vm_id.clone(),
            reply: tx,
        })
        .await
    }

    async fn pause_vm(&self, vm_id: &VmId) -> VmmResult<()> {
        self.send(|tx| VzCommand::Pause {
            vm_id: vm_id.clone(),
            reply: tx,
        })
        .await
    }

    async fn resume_vm(&self, vm_id: &VmId) -> VmmResult<()> {
        self.send(|tx| VzCommand::Resume {
            vm_id: vm_id.clone(),
            reply: tx,
        })
        .await
    }

    async fn snapshot_vm(&self, vm_id: &VmId) -> VmmResult<Snapshot> {
        self.send(|tx| VzCommand::Snapshot {
            vm_id: vm_id.clone(),
            reply: tx,
        })
        .await
    }

    async fn restore_vm(&self, snapshot: &Snapshot) -> VmmResult<VmId> {
        self.send(|tx| VzCommand::Restore {
            snapshot: snapshot.clone(),
            reply: tx,
        })
        .await
    }

    async fn destroy_vm(&self, vm_id: &VmId) -> VmmResult<()> {
        self.send(|tx| VzCommand::Destroy {
            vm_id: vm_id.clone(),
            reply: tx,
        })
        .await
    }

    async fn vm_state(&self, vm_id: &VmId) -> VmmResult<VmState> {
        self.send(|tx| VzCommand::State {
            vm_id: vm_id.clone(),
            reply: tx,
        })
        .await
    }

    async fn vm_guest_ip(&self, vm_id: &VmId) -> VmmResult<Option<IpAddr>> {
        self.send(|tx| VzCommand::GuestIp {
            vm_id: vm_id.clone(),
            reply: tx,
        })
        .await
    }

    async fn list_vms(&self) -> VmmResult<Vec<VmInfo>> {
        self.send(|tx| VzCommand::List { reply: tx }).await
    }
}

// ============================================================================
// VZ Worker Thread
// ============================================================================

/// The persistent VZ worker thread. Runs a current-thread tokio runtime
/// with a `LocalSet` so that non-Send VZ futures can be spawned.
fn vz_worker_thread(
    mut cmd_rx: mpsc::Receiver<VzCommand>,
    snapshot_dir: PathBuf,
    runtime_dir: PathBuf,
) {
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            tracing::error!("VZ worker runtime creation failed: {e}");
            return;
        }
    };

    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async move {
        let mut vms: HashMap<VmId, VzVmEntry> = HashMap::new();

        // NOTE: The CFRunLoop pump task was here but caused the LocalSet
        // executor to hang. VZ's GCD queue processes pause/resume blocks
        // independently of CFRunLoop, so the pump is not needed for those
        // operations when using dispatch_async_f.

        tracing::info!("VZ worker thread ready");

        while let Some(cmd) = cmd_rx.recv().await {
            match cmd {
                VzCommand::Create { config, reply } => {
                    let result = handle_create(
                        &mut vms,
                        &config,
                        &snapshot_dir,
                        &runtime_dir,
                    );
                    let _ = reply.send(result);
                }

                VzCommand::Start { vm_id, reply } => {
                    let result = match vms.get(&vm_id) {
                        Some(entry) => {
                            let state = entry.vm.state();
                            match state {
                                arcbox_vz::VirtualMachineState::Stopped => {
                                    info!(vm_id = %vm_id, "starting Apple VZ VM");
                                    entry.vm.start().await.map_err(|e| {
                                        VmmError::Backend(format!("VZ start: {e}"))
                                    })
                                }
                                arcbox_vz::VirtualMachineState::Paused => {
                                    info!(vm_id = %vm_id, "resuming paused VM");
                                    entry.vm.resume().await.map_err(|e| {
                                        VmmError::Backend(format!("VZ resume: {e}"))
                                    })
                                }
                                arcbox_vz::VirtualMachineState::Running => Ok(()),
                                other => Err(VmmError::Backend(format!(
                                    "unexpected state: {other:?}"
                                ))),
                            }
                        }
                        None => Err(VmmError::VmNotFound(vm_id)),
                    };
                    let _ = reply.send(result);
                }

                VzCommand::Pause { vm_id, reply } => {
                    let result = match vms.get(&vm_id) {
                        Some(entry) => {
                            info!(vm_id = %vm_id, "pausing Apple VZ VM");
                            // Dispatch pauseWithCompletionHandler: onto the
                            // VM's GCD queue. VZ suspends all process threads
                            // during the pause, so we use a busy-wait (not
                            // sleep) and check the completion flag + state.
                            objc_ffi::pause(&entry.vm);
                            let deadline =
                                std::time::Instant::now() + Duration::from_secs(15);
                            loop {
                                if objc_ffi::is_completed() {
                                    break Ok(());
                                }
                                let st = entry.vm.state();
                                if st == arcbox_vz::VirtualMachineState::Paused {
                                    break Ok(());
                                }
                                if std::time::Instant::now() > deadline {
                                    break Err(VmmError::Backend(
                                        format!("VZ pause timed out (state={st:?})"),
                                    ));
                                }
                                // Busy-wait — VZ may suspend this thread
                                // during pause, so sleep might not return.
                                std::hint::spin_loop();
                            }
                        }
                        None => Err(VmmError::VmNotFound(vm_id)),
                    };
                    let _ = reply.send(result);
                }

                VzCommand::Resume { vm_id, reply } => {
                    let result = match vms.get(&vm_id) {
                        Some(entry) => {
                            info!(vm_id = %vm_id, "resuming Apple VZ VM");
                            objc_ffi::resume(&entry.vm);
                            let deadline =
                                std::time::Instant::now() + Duration::from_secs(15);
                            loop {
                                if objc_ffi::is_completed() {
                                    break Ok(());
                                }
                                let st = entry.vm.state();
                                if st == arcbox_vz::VirtualMachineState::Running {
                                    break Ok(());
                                }
                                if std::time::Instant::now() > deadline {
                                    break Err(VmmError::Backend(
                                        format!("VZ resume timed out (state={st:?})"),
                                    ));
                                }
                                std::hint::spin_loop();
                            }
                        }
                        None => Err(VmmError::VmNotFound(vm_id)),
                    };
                    let _ = reply.send(result);
                }

                VzCommand::Snapshot { vm_id, reply } => {
                    let snap_path = snapshot_dir.join(format!("{vm_id}.vzrestore"));
                    let result = match vms.get(&vm_id) {
                        Some(entry) => {
                            let state = entry.vm.state();
                            if state != arcbox_vz::VirtualMachineState::Paused {
                                Err(VmmError::InvalidState {
                                    vm_id: vm_id.clone(),
                                    current: map_vz_state(state),
                                    expected: VmState::Paused,
                                })
                            } else {
                                warn!(vm_id = %vm_id, "VZ save/restore stub");
                                std::fs::write(&snap_path, b"VZ_SNAPSHOT_STUB").ok();
                                Ok(Snapshot {
                                    vm_id: vm_id.clone(),
                                    state_path: snap_path.clone(),
                                    mem_path: snap_path.with_extension("mem"),
                                    config: entry.config.clone(),
                                })
                            }
                        }
                        None => Err(VmmError::VmNotFound(vm_id)),
                    };
                    let _ = reply.send(result);
                }

                VzCommand::Restore { snapshot, reply } => {
                    let result = handle_restore(&mut vms, &snapshot, &snapshot_dir, &runtime_dir);
                    let _ = reply.send(result);
                }

                VzCommand::Destroy { vm_id, reply } => {
                    let result = match vms.remove(&vm_id) {
                        Some(entry) => {
                            info!(vm_id = %vm_id, "destroying Apple VZ VM");
                            // Close serial fds to unblock reader threads.
                            if let Some(fd) = entry.serial_read_fd {
                                unsafe { libc::close(fd) };
                            }
                            if let Some(fd) = entry.uart_read_fd {
                                unsafe { libc::close(fd) };
                            }
                            drop(entry);
                            Ok(())
                        }
                        None => Err(VmmError::VmNotFound(vm_id)),
                    };
                    let _ = reply.send(result);
                }

                VzCommand::State { vm_id, reply } => {
                    let result = match vms.get(&vm_id) {
                        Some(entry) => Ok(map_vz_state(entry.vm.state())),
                        None => Err(VmmError::VmNotFound(vm_id)),
                    };
                    let _ = reply.send(result);
                }

                VzCommand::GuestIp { vm_id, reply } => {
                    let result = match vms.get(&vm_id) {
                        Some(entry) => Ok(entry.guest_ip),
                        None => Err(VmmError::VmNotFound(vm_id)),
                    };
                    let _ = reply.send(result);
                }

                VzCommand::List { reply } => {
                    let result: VmmResult<Vec<VmInfo>> = Ok(vms
                        .iter()
                        .map(|(vm_id, entry)| VmInfo {
                            vm_id: vm_id.clone(),
                            state: map_vz_state(entry.vm.state()),
                            guest_ip: entry.guest_ip,
                        })
                        .collect());
                    let _ = reply.send(result);
                }
            }
        }

        tracing::info!("VZ worker thread shutting down");
    });
}

/// Handle VM creation: build VZ config, validate, create VM.
fn handle_create(
    vms: &mut HashMap<VmId, VzVmEntry>,
    config: &VmConfig,
    snapshot_dir: &PathBuf,
    runtime_dir: &PathBuf,
) -> VmmResult<VmId> {
    let vm_id = config.vm_id.clone();

    if config.memory_mb < 128 {
        return Err(VmmError::InvalidConfig("memory_mb must be >= 128".into()));
    }
    if config.vcpus == 0 {
        return Err(VmmError::InvalidConfig("vcpus must be > 0".into()));
    }
    if !config.kernel_path.exists() {
        return Err(VmmError::InvalidConfig(format!(
            "kernel not found: {}",
            config.kernel_path.display()
        )));
    }
    if !config.rootfs_path.exists() {
        return Err(VmmError::InvalidConfig(format!(
            "rootfs not found: {}",
            config.rootfs_path.display()
        )));
    }

    if vms.contains_key(&vm_id) {
        return Err(VmmError::InvalidConfig(format!("VM already exists: {vm_id}")));
    }

    info!(vm_id = %vm_id, "creating Apple VZ VM");

    let (vz_config, serial_fds) = build_vz_config(config)?;
    let vm = vz_config
        .build()
        .map_err(|e| VmmError::Backend(format!("VZ build: {e}")))?;

    // Spawn serial console readers (virtio-console + PL011 UART)
    if let Some(read_fd) = serial_fds.read_fd {
        let log_path = runtime_dir.join(format!("{vm_id}.serial.log"));
        let vm_id_clone = vm_id.clone();
        std::thread::spawn(move || {
            serial_log_reader(read_fd, log_path, vm_id_clone);
        });
    }
    if let Some(uart_fd) = serial_fds.uart_read_fd {
        let log_path = runtime_dir.join(format!("{vm_id}.uart.log"));
        let vm_id_clone = vm_id.clone();
        std::thread::spawn(move || {
            serial_log_reader(uart_fd, log_path, vm_id_clone);
        });
    }

    vms.insert(
        vm_id.clone(),
        VzVmEntry {
            vm,
            config: config.clone(),
            guest_ip: None,
            serial_read_fd: serial_fds.read_fd,
            uart_read_fd: serial_fds.uart_read_fd,
        },
    );

    Ok(vm_id)
}

/// Handle VM restore from snapshot (currently rebuilds from config — cold boot).
fn handle_restore(
    vms: &mut HashMap<VmId, VzVmEntry>,
    snapshot: &Snapshot,
    _snapshot_dir: &PathBuf,
    runtime_dir: &PathBuf,
) -> VmmResult<VmId> {
    let vm_id = snapshot.vm_id.clone();

    if !snapshot.state_path.exists() {
        return Err(VmmError::SnapshotNotFound(vm_id));
    }

    warn!(vm_id = %vm_id, "VZ restore stub — rebuilding from config");

    let (vz_config, serial_fds) = build_vz_config(&snapshot.config)?;
    let vm = vz_config
        .build()
        .map_err(|e| VmmError::Backend(format!("VZ rebuild: {e}")))?;

    if let Some(read_fd) = serial_fds.read_fd {
        let log_path = runtime_dir.join(format!("{vm_id}.serial.log"));
        let vm_id_clone = vm_id.clone();
        std::thread::spawn(move || {
            serial_log_reader(read_fd, log_path, vm_id_clone);
        });
    }
    if let Some(uart_fd) = serial_fds.uart_read_fd {
        let log_path = runtime_dir.join(format!("{vm_id}.uart.log"));
        let vm_id_clone = vm_id.clone();
        std::thread::spawn(move || {
            serial_log_reader(uart_fd, log_path, vm_id_clone);
        });
    }

    vms.insert(
        vm_id.clone(),
        VzVmEntry {
            vm,
            config: snapshot.config.clone(),
            guest_ip: None,
            serial_read_fd: serial_fds.read_fd,
            uart_read_fd: serial_fds.uart_read_fd,
        },
    );

    Ok(vm_id)
}

/// Build a VZ configuration from our [`VmConfig`].
fn build_vz_config(
    config: &VmConfig,
) -> Result<(VirtualMachineConfiguration, SerialFds), VmmError> {
    let mut vz_config = VirtualMachineConfiguration::new()
        .map_err(|e| VmmError::Backend(format!("VZ config: {e}")))?;

    vz_config
        .set_cpu_count(config.vcpus as usize)
        .set_memory_size(config.memory_mb * 1024 * 1024);

    let platform = GenericPlatform::new()
        .map_err(|e| VmmError::Backend(format!("platform: {e}")))?;
    vz_config.set_platform(platform);

    let mut boot_loader = LinuxBootLoader::new(&config.kernel_path)
        .map_err(|e| VmmError::Backend(format!("boot loader: {e}")))?;
    boot_loader.set_command_line(&config.kernel_cmdline);
    if let Some(ref initrd) = config.initrd_path {
        eprintln!("[VZ] initrd path: {} (exists={})", initrd.display(), initrd.exists());
        boot_loader.set_initial_ramdisk(initrd);
    } else {
        eprintln!("[VZ] no initrd configured");
    }
    vz_config.set_boot_loader(boot_loader);

    let rootfs = StorageDeviceConfiguration::disk_image(&config.rootfs_path, false)
        .map_err(|e| VmmError::Backend(format!("rootfs: {e}")))?;
    vz_config.add_storage_device(rootfs);

    for drive in &config.drives {
        let dev = StorageDeviceConfiguration::disk_image(&drive.path, drive.read_only)
            .map_err(|e| VmmError::Backend(format!("drive: {e}")))?;
        vz_config.add_storage_device(dev);
    }

    let net = NetworkDeviceConfiguration::nat()
        .map_err(|e| VmmError::Backend(format!("network: {e}")))?;
    vz_config.add_network_device(net);

    // Serial console via virtio-console (appears as hvc0 in guest).
    // We DON'T use arcbox-vz's add_serial_port here — instead we set
    // serialPorts directly via ObjC to include both virtio-console AND
    // PL011 UART. Since arcbox-vz's Rust Vec stays empty, apply_devices()
    // during build() won't overwrite our setting.
    let serial = SerialPortConfiguration::virtio_console()
        .map_err(|e| VmmError::Backend(format!("serial: {e}")))?;
    let virtio_read_fd = serial.read_fd();
    let virtio_write_fd = serial.write_fd();
    // Consume the serial config (frees the Rust wrapper but keeps the ObjC
    // object alive — it was heap-copied by into_ptr()). We DON'T push it
    // to the Vec; instead, set it directly on the ObjC object below.
    let _ = serial;

    // PL011 UART serial port — captures kernel boot messages on ttyAMA0
    // (always built-in, unlike virtio-console which is a module).
    let uart_read_fd = objc_ffi::add_pl011_uart(&vz_config);

    let serial_fds = SerialFds {
        read_fd: virtio_read_fd,
        write_fd: virtio_write_fd,
        uart_read_fd,
    };

    let entropy = EntropyDeviceConfiguration::new()
        .map_err(|e| VmmError::Backend(format!("entropy: {e}")))?;
    vz_config.add_entropy_device(entropy);

    let balloon = MemoryBalloonDeviceConfiguration::new()
        .map_err(|e| VmmError::Backend(format!("balloon: {e}")))?;
    vz_config.add_memory_balloon_device(balloon);

    Ok((vz_config, serial_fds))
}

/// Serial console file descriptors.
struct SerialFds {
    read_fd: Option<RawFd>,
    #[allow(dead_code)]
    write_fd: Option<RawFd>,
    /// PL011 UART read fd (captures ttyAMA0 output — kernel boot messages).
    uart_read_fd: Option<RawFd>,
}

/// Background thread that reads serial console output to a log file.
fn serial_log_reader(read_fd: RawFd, log_path: PathBuf, vm_id: String) {
    use std::io::Write;

    let mut file = match std::fs::File::create(&log_path) {
        Ok(f) => f,
        Err(e) => {
            warn!(vm_id = %vm_id, error = %e, "failed to create serial log");
            return;
        }
    };

    let mut buf = [0u8; 4096];
    loop {
        let n = unsafe { libc::read(read_fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len()) };
        if n <= 0 {
            break;
        }
        let n = n as usize;
        let _ = file.write_all(&buf[..n]);
        let _ = file.flush();

        let text = String::from_utf8_lossy(&buf[..n]);
        if let Some(ip) = extract_guest_ip(&text) {
            info!(vm_id = %vm_id, guest_ip = %ip, "discovered guest IP");
        }
    }
    info!(vm_id = %vm_id, "serial reader stopped");
}

fn extract_guest_ip(text: &str) -> Option<IpAddr> {
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("TIKO_GUEST_IP=") {
            if let Ok(ip) = rest.trim().parse() {
                return Some(ip);
            }
        }
    }
    None
}

fn map_vz_state(state: arcbox_vz::VirtualMachineState) -> VmState {
    match state {
        arcbox_vz::VirtualMachineState::Stopped => VmState::Stopped,
        arcbox_vz::VirtualMachineState::Running => VmState::Running,
        arcbox_vz::VirtualMachineState::Paused => VmState::Paused,
        arcbox_vz::VirtualMachineState::Starting => VmState::Starting,
        arcbox_vz::VirtualMachineState::Pausing => VmState::Paused,
        arcbox_vz::VirtualMachineState::Resuming => VmState::Restoring,
        arcbox_vz::VirtualMachineState::Stopping => VmState::Stopped,
        arcbox_vz::VirtualMachineState::Saving => VmState::Snapshotting,
        arcbox_vz::VirtualMachineState::Restoring => VmState::Restoring,
        arcbox_vz::VirtualMachineState::Error => VmState::Stopped,
    }
}
