//! Remote S3 store backend (stub).
//!
//! This module will hold the real networked S3 implementation.
//! The current codebase still uses `s3_sim` as the active backend.

use std::io;

/// Placeholder for the real remote S3 store implementation.
#[derive(Debug, Default)]
pub struct S3;

impl S3 {
    /// Build a remote store from process configuration/environment.
    pub fn new_from_env() -> io::Result<Self> {
        todo!("remote S3 backend is not implemented yet")
    }

    /// Store an object in the remote S3 backend.
    pub fn put_object(&self, _key: &str, _data: &[u8]) -> io::Result<()> {
        todo!("remote S3 backend is not implemented yet")
    }

    /// Read an object from the remote S3 backend.
    /// Returns `Ok(None)` when the key does not exist.
    pub fn get_object(&self, _key: &str) -> io::Result<Option<Vec<u8>>> {
        todo!("remote S3 backend is not implemented yet")
    }

    /// Delete an object from the remote S3 backend.
    pub fn delete_object(&self, _key: &str) -> io::Result<()> {
        todo!("remote S3 backend is not implemented yet")
    }
}
