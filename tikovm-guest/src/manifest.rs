//! Load and validate the [`WorkloadManifest`] from `/etc/tikovm/workload.toml`
//! (design §5.1). The manifest is guest-only; the guest reads it to run the
//! workload. The host injects an override copy at provision time if provided.

use std::path::Path;

use tikovm_protocol::manifest::WorkloadManifest;

#[derive(Debug, thiserror::Error)]
pub enum ManifestError {
    #[error("failed to read manifest {0}: {1}")]
    Read(String, String),
    #[error("failed to parse manifest: {0}")]
    Parse(String),
}

/// Load the manifest from a TOML file path.
pub fn load(path: &Path) -> Result<WorkloadManifest, ManifestError> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| ManifestError::Read(path.display().to_string(), e.to_string()))?;
    load_str(&text)
}

/// Parse a manifest from a TOML string.
pub fn load_str(text: &str) -> Result<WorkloadManifest, ManifestError> {
    let m: WorkloadManifest =
        toml::from_str(text).map_err(|e| ManifestError::Parse(e.to_string()))?;
    if let Some(s) = &m.schedule {
        s.validate().map_err(ManifestError::Parse)?;
    }
    Ok(m)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_minimal() {
        let t = r#"
workload = "echo"
[health]
kind = "none"
"#;
        let m = load_str(t).unwrap();
        assert_eq!(m.workload, "echo");
    }
}
