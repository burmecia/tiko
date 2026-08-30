use super::{ObjectStorage, s3_sim::S3Sim};
use crate::error::Result;
use std::path::Path;

pub(crate) struct Storage {
    backend: Box<dyn ObjectStorage + Send + Sync>,
}

impl Storage {
    pub(crate) fn new(root_path: &Path) -> Self {
        let backend = Box::new(S3Sim::new(root_path));
        Self { backend }
    }

    pub(crate) fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        self.backend.put(key, data)
    }

    pub(crate) fn get(&self, key: &str) -> Result<Vec<u8>> {
        self.backend.get(key)
    }

    pub(crate) fn delete(&self, key: &str) -> Result<()> {
        self.backend.delete(key)
    }

    pub(crate) fn list_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        self.backend.list_prefix(prefix)
    }
}
