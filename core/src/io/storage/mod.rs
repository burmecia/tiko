pub mod s3;
pub mod s3_sim;
pub mod storage;

use crate::error::Result;
pub(crate) use storage::Storage;

pub(crate) trait ObjectStorage {
    fn put(&self, key: &str, data: &[u8]) -> Result<()>;
    fn get(&self, key: &str) -> Result<Vec<u8>>;
    fn delete(&self, key: &str) -> Result<()>;
    fn list_prefix(&self, prefix: &str) -> Result<Vec<String>>;
}
