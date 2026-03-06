//! WAL archive command for PostgreSQL's `archive_command`.
//!
//! Usage (postgresql.conf):
//! ```ini
//! archive_command = '/path/to/tiko_archive %p %f'
//! ```
//!
//! Arguments:
//! - `argv[1]` — absolute path to the completed WAL segment on disk (`%p`)
//! - `argv[2]` — WAL segment filename (`%f`)
//!
//! Exits 0 on success; exits 1 on any error (PostgreSQL retries on non-zero exit).
//!
//! Required environment variables:
//! - `PGDATA`            — PostgreSQL data directory (used to locate the sim store)
//! - `TIKO_ORG_ID`      — organisation identifier (u64)
//! - `TIKO_PROJECT_ID`  — project identifier (u64)
//! - `TIKO_BRANCH_ID`   — branch identifier (u64)

use std::path::{Path, PathBuf};

use s3worker::project::{ENV_BRANCH_ID, ENV_ORG_ID, ENV_PROJECT_ID, ProjectNamespace};
use s3worker::sim_store::SimStore;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 3 {
        eprintln!("Usage: tiko_archive <wal_path> <wal_filename>");
        std::process::exit(1);
    }

    let wal_path = PathBuf::from(&args[1]);
    let wal_filename = &args[2];

    let sim = match sim_from_env() {
        Ok(s) => s,
        Err(e) => {
            eprintln!("tiko_archive: {e}");
            std::process::exit(1);
        }
    };

    let ns = match namespace_from_env() {
        Ok(n) => n,
        Err(e) => {
            eprintln!("tiko_archive: {e}");
            std::process::exit(1);
        }
    };

    let timeline = match parse_timeline(wal_filename) {
        Some(t) => t,
        None => {
            eprintln!("tiko_archive: cannot parse timeline from filename: {wal_filename}");
            std::process::exit(1);
        }
    };

    if let Err(e) = upload_wal_segment(&sim, &ns, timeline, wal_filename, &wal_path) {
        eprintln!("tiko_archive: upload failed: {e}");
        std::process::exit(1);
    }

    std::process::exit(0);
}

// ── Core helpers (also used by tests) ────────────────────────────────────────

/// Build a `SimStore` from `$PGDATA`.
fn sim_from_env() -> Result<SimStore, String> {
    let pgdata = std::env::var("PGDATA").map_err(|_| "PGDATA not set".to_string())?;
    Ok(SimStore::new(Path::new(&pgdata)))
}

/// Build a `ProjectNamespace` from `TIKO_ORG_ID`, `TIKO_PROJECT_ID`, `TIKO_BRANCH_ID`.
fn namespace_from_env() -> Result<ProjectNamespace, String> {
    let org_id = read_u64(ENV_ORG_ID)?;
    let project_id = read_u64(ENV_PROJECT_ID)?;
    let branch_id = read_u64(ENV_BRANCH_ID)?;
    Ok(ProjectNamespace::new(org_id, project_id, branch_id))
}

/// Extract the timeline number from a 24-character WAL segment name.
///
/// WAL names have the form `{timeline:08X}{log:08X}{seg:08X}`.
/// The first 8 hex characters encode the timeline ID.
fn parse_timeline(filename: &str) -> Option<u32> {
    if filename.len() < 8 {
        return None;
    }
    u32::from_str_radix(&filename[..8], 16).ok()
}

/// Read a WAL segment from disk and PUT it into the sim standard store at
/// `{org}/pitr/{proj}/wal/{timeline:08X}/{segment}`.
fn upload_wal_segment(
    sim: &SimStore,
    ns: &ProjectNamespace,
    timeline: u32,
    segment: &str,
    path: &Path,
) -> std::io::Result<()> {
    let bytes = std::fs::read(path)?;
    sim.put_standard(&ns.wal_key(timeline, segment), &bytes)
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

    fn ns() -> ProjectNamespace {
        ProjectNamespace::new(1001, 2001, 7)
    }

    // ── Archive a synthetic WAL file ──────────────────────────────────────────

    #[test]
    fn archive_stores_wal_at_correct_key() {
        let (dir, sim) = setup();
        let ns = ns();
        let segment = "000000010000000000000001";
        let timeline = 1u32;

        // Write a fake WAL segment to disk.
        let wal_path = dir.path().join(segment);
        std::fs::write(&wal_path, b"fake_wal_data_archive").unwrap();

        upload_wal_segment(&sim, &ns, timeline, segment, &wal_path).unwrap();

        // Verify it lives at the expected standard-bucket key.
        let key = ns.wal_key(timeline, segment);
        let stored = sim.get_standard(&key).unwrap();
        assert_eq!(stored, Some(b"fake_wal_data_archive".to_vec()));
    }

    // ── parse_timeline ────────────────────────────────────────────────────────

    #[test]
    fn parse_timeline_extracts_first_8_hex_chars() {
        assert_eq!(parse_timeline("000000010000000000000001"), Some(1));
        assert_eq!(parse_timeline("000000030000000000000005"), Some(3));
        assert_eq!(parse_timeline("0000000A0000000000000001"), Some(10));
    }

    #[test]
    fn parse_timeline_short_filename_returns_none() {
        assert_eq!(parse_timeline("0000001"), None);
        assert_eq!(parse_timeline(""), None);
    }
}
