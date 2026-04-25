//! Remote S3 store backend (stub).
//!
//! This module will hold the real networked S3 implementation.
//! The current codebase still uses `s3_sim` as the active backend.

use super::backend::ObjectStore;
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

impl ObjectStore for S3 {
    fn put_express(&self, _key: &str, _data: &[u8]) -> Result<()> {
        todo!("remote S3 backend is not implemented yet")
    }
    fn get_express(&self, _key: &str) -> Result<Vec<u8>> {
        todo!("remote S3 backend is not implemented yet")
    }
    fn rename_express(&self, _src_key: &str, _dst_key: &str) -> Result<()> {
        todo!("remote S3 backend is not implemented yet")
    }
    fn delete_express(&self, _key: &str) -> Result<()> {
        todo!("remote S3 backend is not implemented yet")
    }
    fn list_prefix_express(&self, _prefix: &str) -> Result<Vec<String>> {
        todo!("remote S3 backend is not implemented yet")
    }
    fn put_standard(&self, _key: &str, _data: &[u8]) -> Result<()> {
        todo!("remote S3 backend is not implemented yet")
    }
    fn get_standard(&self, _key: &str) -> Result<Vec<u8>> {
        todo!("remote S3 backend is not implemented yet")
    }
    fn delete_standard(&self, _key: &str) -> Result<()> {
        todo!("remote S3 backend is not implemented yet")
    }
    fn remove_dir_standard(&self, _prefix: &str) -> Result<()> {
        todo!("remote S3 backend is not implemented yet")
    }
    fn list_prefix_standard(&self, _prefix: &str) -> Result<Vec<String>> {
        todo!("remote S3 backend is not implemented yet")
    }
    fn copy_express_to_standard(&self, _src_key: &str, _dst_key: &str) -> Result<()> {
        todo!("remote S3 backend is not implemented yet")
    }
}
