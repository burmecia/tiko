use core::io_control::IoControl;
use core::{org::OrgMeta, project::ProjectCtx, store::Store, tiko_root_path};
use pgsys::{
    common::{MaxBackends, NUM_AUXILIARY_PROCS, get_my_proc_number, is_under_postmaster},
    logging::*,
    wait_events::new_wait_event,
};

#[unsafe(no_mangle)]
pub extern "C-unwind" fn tiko_init() {
    unsafe {
        // Initialize wait event identifiers for S3 I/O operations
        crate::WAIT_EVENT_S3_IO_READ = new_wait_event(c"S3IORead".as_ptr());
        crate::WAIT_EVENT_S3_IO_WRITE = new_wait_event(c"S3IOWrite".as_ptr());
    }

    let root_dir = tiko_root_path();

    // Initialize S3Sim unconditionally — needed for both initdb and normal run.
    Store::init(&root_dir);

    // Try to initialize ProjectCtx from env vars (TIKO_ORG_ID/TIKO_PROJECT_ID/TIKO_BRANCH_ID).
    // This enables the initdb write path to reach S3Sim express.
    ProjectCtx::init_from_env(&root_dir);

    // If a recovery manifest exists, load it into memory and set RECOVERY_MODE=true.
    let recovery_manifest = core::manifest::Manifest::recovery_manifest_path(&root_dir);
    if recovery_manifest.exists() {
        if let Err(e) = core::recovery::load_recovery_manifest(&recovery_manifest) {
            pg_log_error(&format!("tiko_init: failed to load recovery manifest: {e}"));
        }
    }

    // Skip shared memory attachment in initdb (--boot) and single-user mode.
    // In those modes MyProcNumber is invalid and the IoControl shmem block
    // was never sized/initialised via shmem_request_hook.
    if !is_under_postmaster() {
        let org_id = ProjectCtx::get().meta.ns.org_id;
        OrgMeta::ensure_org_meta(Store::get(), org_id)
            .unwrap_or_else(|e| pg_log_error(&format!("tiko_init: ensure_org_meta failed: {e}")));
        return;
    }

    unsafe {
        // Explicitly attach to shared memory in this backend process.
        // ShmemInitStruct looks up the existing block (found=true), no reinitialization.
        let total_procs = (MaxBackends + NUM_AUXILIARY_PROCS) as usize;
        let control = IoControl::init_or_attach(total_procs);

        // Attach this backend to its slot pool
        let proc_num = get_my_proc_number();
        let pool = control.backend_pool(proc_num);
        pool.attach();

        pg_log_debug1(&format!(
            "tiko.smgr.tiko_init: backend {} pool attached",
            proc_num
        ));
    }
}
