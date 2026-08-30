//! Remote S3 store backend (stub).
//!
//! This module will hold the real networked S3 implementation.
//! The current codebase still uses `s3_sim` as the active backend.

use super::ObjectStorage;
use crate::error::Result;

/// Placeholder for the real remote S3 store implementation.
#[derive(Debug, Default)]
pub struct S3;

impl S3 {
    /// Build a remote store from process configuration/environment.
    #[allow(dead_code)]
    pub(super) fn new_from_env() -> Result<Self> {
        todo!("remote S3 backend is not implemented yet")
    }
}

impl ObjectStorage for S3 {
    fn put(&self, _key: &str, _data: &[u8]) -> Result<()> {
        todo!("remote S3 backend is not implemented yet")
    }

    fn get(&self, _key: &str) -> Result<Vec<u8>> {
        todo!("remote S3 backend is not implemented yet")
    }

    fn delete(&self, _key: &str) -> Result<()> {
        todo!("remote S3 backend is not implemented yet")
    }

    fn list_prefix(&self, _prefix: &str) -> Result<Vec<String>> {
        todo!("remote S3 backend is not implemented yet")
    }
}
