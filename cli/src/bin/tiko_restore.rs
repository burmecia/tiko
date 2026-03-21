//! WAL restore command for PostgreSQL's `restore_command`.
//!
//! Usage (postgresql.conf):
//! ```ini
//! restore_command = '/path/to/tiko_restore %f %p'
//! ```
//!
//! Arguments:
//! - `argv[1]` — WAL segment filename to fetch (`%f`)
//! - `argv[2]` — destination path to write it to (`%p`)
//!
//! Exits 0 if the segment was found and written; exits 1 if not found or on error.
//!
//! Required environment variables:
//! - `TIKO_ROOT_PATH`    — root path for the sim store (replaces `PGDATA`)
//! - `TIKO_ORG_ID`      — organisation identifier (u64)
//! - `TIKO_PROJECT_ID`  — project identifier (u64)
//! - `TIKO_BRANCH_ID`   — branch identifier (u64)
//!
//! Optional environment variables (for branch WAL fallback):
//! - `TIKO_PARENT_PROJECT_ID`     — parent project identifier (u64)
//! - `TIKO_PARENT_BRANCH_ID`      — parent branch identifier (u64)
//! - `TIKO_BRANCH_CHECKPOINT_LSN` — 16-char hex LSN at which the branch was forked
//!
//! Fallback logic:
//! 1. Try own namespace.  Found → exit 0.
//! 2. If parent context is present AND `segment_lsn ≤ branch_checkpoint_lsn`:
//!    try parent namespace.  Found → exit 0.
//! 3. All misses → exit 1.

use std::path::{Path, PathBuf};

use pgsys::Lsn;
use store::{
    ENV_BRANCH_ID, ENV_ORG_ID, ENV_PROJECT_ID, project::ProjectNamespace, sim_store::SimStore,
};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 3 {
        eprintln!("Usage: tiko_restore <wal_filename> <dest_path>");
        std::process::exit(1);
    }

    let wal_filename = &args[1];
    let dest_path = PathBuf::from(&args[2]);

    eprintln!(
        "tiko_restore: segment={} dest={}",
        wal_filename,
        dest_path.display()
    );

    let sim = match sim_from_env() {
        Ok(s) => s,
        Err(e) => {
            eprintln!("tiko_restore: {e}");
            std::process::exit(1);
        }
    };

    let own_ns = match namespace_from_env() {
        Ok(n) => n,
        Err(e) => {
            eprintln!("tiko_restore: {e}");
            std::process::exit(1);
        }
    };

    let parent_ns = parent_namespace_from_env();
    let branch_lsn = branch_checkpoint_lsn_from_env();

    match restore_segment(
        &sim,
        &own_ns,
        parent_ns.as_ref(),
        branch_lsn,
        wal_filename,
        &dest_path,
    ) {
        Ok(true) => {
            eprintln!("tiko_restore: ok segment={wal_filename}");
            std::process::exit(0);
        }
        Ok(false) => {
            eprintln!("tiko_restore: not found segment={wal_filename}");
            std::process::exit(1);
        }
        Err(e) => {
            eprintln!("tiko_restore: error segment={wal_filename}: {e}");
            std::process::exit(1);
        }
    }
}

// ── Core helpers (also used by tests) ────────────────────────────────────────

/// Build a `SimStore` from `$TIKO_ROOT_PATH`.
fn sim_from_env() -> Result<&'static SimStore, String> {
    let root = std::env::var("TIKO_ROOT_PATH").map_err(|_| "TIKO_ROOT_PATH not set".to_string())?;
    Ok(SimStore::init(Path::new(&root)))
}

/// Build own `ProjectNamespace` from `TIKO_ORG_ID`, `TIKO_PROJECT_ID`, `TIKO_BRANCH_ID`.
fn namespace_from_env() -> Result<ProjectNamespace, String> {
    let org_id = read_u64(ENV_ORG_ID)?;
    let project_id = read_u64(ENV_PROJECT_ID)?;
    let branch_id = read_u64(ENV_BRANCH_ID)?;
    Ok(ProjectNamespace::new(org_id, project_id, branch_id))
}

/// Build optional parent `ProjectNamespace` from `TIKO_PARENT_PROJECT_ID` and
/// `TIKO_PARENT_BRANCH_ID`.  Returns `None` if either variable is absent.
fn parent_namespace_from_env() -> Option<ProjectNamespace> {
    let org_id = read_u64(ENV_ORG_ID).ok()?;
    let parent_project_id = read_u64("TIKO_PARENT_PROJECT_ID").ok()?;
    let parent_branch_id = read_u64("TIKO_PARENT_BRANCH_ID").ok()?;
    Some(ProjectNamespace::new(
        org_id,
        parent_project_id,
        parent_branch_id,
    ))
}

/// Read `TIKO_BRANCH_CHECKPOINT_LSN` (16-char hex) into a `Lsn`.
/// Returns `None` if the variable is absent or cannot be parsed.
fn branch_checkpoint_lsn_from_env() -> Option<Lsn> {
    let s = std::env::var("TIKO_BRANCH_CHECKPOINT_LSN").ok()?;
    Lsn::from_hex(s.trim()).ok()
}

/// Download a WAL segment from the sim standard store and write it to `dest`.
///
/// Returns `Ok(true)` on success, `Ok(false)` if the segment is not found in
/// either namespace.
///
/// Fallback order:
/// 1. Try own namespace.
/// 2. If a parent namespace is provided AND the segment's LSN is within the
///    branch point (`segment_lsn ≤ branch_lsn`), try the parent namespace.
fn restore_segment(
    sim: &SimStore,
    own_ns: &ProjectNamespace,
    parent_ns: Option<&ProjectNamespace>,
    branch_lsn: Option<Lsn>,
    filename: &str,
    dest: &Path,
) -> std::io::Result<bool> {
    let timeline = match parse_timeline(filename) {
        Some(t) => t,
        None => return Ok(false),
    };

    // Step 1: try own namespace.
    if download_wal_segment(sim, own_ns, timeline, filename, dest)? {
        return Ok(true);
    }

    // Step 2: branch fallback — only if parent context is present and the
    // segment predates the branch checkpoint.
    if let (Some(p_ns), Some(branch_lsn)) = (parent_ns, branch_lsn) {
        let seg_lsn = segment_lsn_from_name(filename).unwrap_or(Lsn::new(u64::MAX));
        if seg_lsn <= branch_lsn && download_wal_segment(sim, p_ns, timeline, filename, dest)? {
            return Ok(true);
        }
    }

    Ok(false)
}

/// Download a WAL segment from `{org}/pitr/{proj}/wal/{timeline:08X}/{segment}`
/// in the standard store and write it to `dest`.
///
/// Returns `Ok(true)` on success, `Ok(false)` if the key does not exist.
fn download_wal_segment(
    sim: &SimStore,
    ns: &ProjectNamespace,
    timeline: u32,
    segment: &str,
    dest: &Path,
) -> std::io::Result<bool> {
    match sim.get_standard(&ns.wal_key(timeline, segment))? {
        Some(bytes) => {
            if let Some(parent) = dest.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(dest, &bytes)?;
            Ok(true)
        }
        None => Ok(false),
    }
}

/// Extract the 4-byte timeline from the first 8 hex characters of a WAL filename.
fn parse_timeline(filename: &str) -> Option<u32> {
    if filename.len() < 8 {
        return None;
    }
    u32::from_str_radix(&filename[..8], 16).ok()
}

/// Compute the starting LSN of the WAL segment from its 24-character filename.
///
/// WAL names: `{timeline:08X}{log:08X}{seg:08X}`
/// LSN = `(log << 32) | (seg * segment_size)` where segment_size = 16 MB (1 << 24).
fn segment_lsn_from_name(filename: &str) -> Option<Lsn> {
    if filename.len() < 24 {
        return None;
    }
    let log = u32::from_str_radix(&filename[8..16], 16).ok()?;
    let seg = u32::from_str_radix(&filename[16..24], 16).ok()?;
    // 16 MB segments (PostgreSQL default).
    let lsn = ((log as u64) << 32) | ((seg as u64) << 24);
    Some(Lsn::new(lsn))
}

fn read_u64(var: &str) -> Result<u64, String> {
    std::env::var(var)
        .map_err(|_| format!("{var} not set"))?
        .parse::<u64>()
        .map_err(|_| format!("{var} is not a valid u64"))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn setup() -> (TempDir, SimStore) {
        let dir = TempDir::new().unwrap();
        let sim = SimStore::new(dir.path());
        (dir, sim)
    }

    fn own_ns() -> ProjectNamespace {
        ProjectNamespace::new(1001, 2001, 1)
    }

    fn parent_ns() -> ProjectNamespace {
        ProjectNamespace::new(1001, 2000, 7)
    }

    /// Store a WAL segment in the sim standard bucket.
    fn seed_wal(sim: &SimStore, ns: &ProjectNamespace, segment: &str, data: &[u8]) {
        let timeline = parse_timeline(segment).unwrap();
        let key = ns.wal_key(timeline, segment);
        sim.put_standard(&key, data).unwrap();
    }

    const SEG1: &str = "000000010000000000000001"; // lsn = 0x1000000  (16 MB)
    const SEG_BEYOND: &str = "000000010000000000000040"; // lsn = 0x40000000 (1 GB) — beyond 0x3A000028

    // branch_checkpoint_lsn = 0x3A000028 from the spec example.
    fn branch_lsn() -> Lsn {
        Lsn::from_hex("000000003A000028").unwrap()
    }

    // ── Archive → restore round-trip ──────────────────────────────────────────

    #[test]
    fn archive_and_restore_round_trip() {
        let (dir, sim) = setup();
        let ns = own_ns();

        // Seed the WAL segment directly (simulating tiko_archive having run).
        seed_wal(&sim, &ns, SEG1, b"wal_segment_contents");

        let dest = dir.path().join("restored");
        let found = restore_segment(&sim, &ns, None, None, SEG1, &dest).unwrap();

        assert!(
            found,
            "restore must succeed when segment exists in own namespace"
        );
        assert_eq!(std::fs::read(&dest).unwrap(), b"wal_segment_contents");
    }

    // ── Branch fallback: segment only in parent namespace ─────────────────────

    #[test]
    fn branch_fallback_finds_segment_in_parent() {
        let (dir, sim) = setup();
        let own = own_ns();
        let parent = parent_ns();

        // Segment is ONLY in the parent namespace.
        seed_wal(&sim, &parent, SEG1, b"parent_wal_data");

        let dest = dir.path().join("restored");
        let found =
            restore_segment(&sim, &own, Some(&parent), Some(branch_lsn()), SEG1, &dest).unwrap();

        assert!(
            found,
            "fallback to parent must succeed for segment within branch point"
        );
        assert_eq!(std::fs::read(&dest).unwrap(), b"parent_wal_data");
    }

    // ── Segment beyond branch_checkpoint_lsn → not fetched from parent ────────

    #[test]
    fn segment_beyond_branch_lsn_not_fetched_from_parent() {
        let (dir, sim) = setup();
        let own = own_ns();
        let parent = parent_ns();

        // Segment is in the parent namespace but its LSN > branch_checkpoint_lsn.
        seed_wal(&sim, &parent, SEG_BEYOND, b"future_wal_data");

        let dest = dir.path().join("restored");
        let found = restore_segment(
            &sim,
            &own,
            Some(&parent),
            Some(branch_lsn()),
            SEG_BEYOND,
            &dest,
        )
        .unwrap();

        assert!(
            !found,
            "segment beyond branch_lsn must not be fetched from parent"
        );
        assert!(!dest.exists(), "dest must not be created on miss");
    }

    // ── Segment absent from both namespaces → not found ───────────────────────

    #[test]
    fn segment_absent_from_both_returns_false() {
        let (dir, sim) = setup();
        let own = own_ns();
        let parent = parent_ns();

        let dest = dir.path().join("restored");
        let found =
            restore_segment(&sim, &own, Some(&parent), Some(branch_lsn()), SEG1, &dest).unwrap();

        assert!(!found, "absent segment must return false");
        assert!(!dest.exists());
    }

    // ── segment_lsn_from_name ─────────────────────────────────────────────────

    #[test]
    fn segment_lsn_within_branch_point() {
        // SEG1: log=0, seg=1 → lsn = 1 << 24 = 0x1000000
        let lsn = segment_lsn_from_name(SEG1).unwrap();
        assert!(lsn <= branch_lsn(), "SEG1 lsn {lsn:?} must be ≤ branch_lsn");
    }

    #[test]
    fn segment_lsn_beyond_branch_point() {
        // SEG_BEYOND: log=0, seg=0x40 → lsn = 0x40 << 24 = 0x40000000
        let lsn = segment_lsn_from_name(SEG_BEYOND).unwrap();
        assert!(
            lsn > branch_lsn(),
            "SEG_BEYOND lsn {lsn:?} must be > branch_lsn"
        );
    }
}
