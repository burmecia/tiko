use serde::{Deserialize, Serialize};
use std::{fmt, sync::Mutex};

use crate::env;
use crate::io::timeline::Checkpoint;
use pgsys::Lsn;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub(crate) struct DbNamespace {
    pub org_id: u64,
    pub db_id: u64,
    pub project_id: u64,
}

impl DbNamespace {
    pub(crate) fn new(org_id: u64, db_id: u64, project_id: u64) -> Self {
        Self {
            org_id,
            db_id,
            project_id,
        }
    }

    pub(crate) fn new_from_env() -> Self {
        let org_id = env::read_u64(env::ENV_ORG_ID);
        let db_id = env::read_u64(env::ENV_DB_ID);
        let project_id = env::read_u64(env::ENV_PROJECT_ID);
        DbNamespace::new(org_id, db_id, project_id)
    }
}

impl fmt::Display for DbNamespace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.org_id, self.db_id)
    }
}

#[derive(Serialize, Deserialize, Clone)]
struct DbMetaInner {
    #[serde(flatten)]
    ns: DbNamespace,
    parent_db_id: Option<u64>,
    parent_checkpoint_lsn: Option<Lsn>,
    parent_timeline_id: Option<u32>,
    checkpoint: Checkpoint,
    created_at: i64,
    status: String,
    deleted_at: Option<i64>,
}

pub(crate) struct DbMeta {
    inner: Mutex<DbMetaInner>,
}

impl DbMeta {
    pub(crate) fn new(ns: DbNamespace) -> Self {
        let inner = DbMetaInner {
            ns,
            parent_db_id: None,
            parent_checkpoint_lsn: None,
            parent_timeline_id: None,
            checkpoint: Checkpoint::default(),
            // created_at: chrono::Utc::now().timestamp(),
            created_at: 0,
            status: "active".to_string(),
            deleted_at: None,
        };
        Self {
            inner: Mutex::new(inner),
        }
    }

    pub(crate) fn set_checkpoint_lsn(&self, ckpt: &Checkpoint) {
        let mut inner = self.inner.lock().unwrap();
        inner.checkpoint = *ckpt;
    }

    pub(crate) fn load_from_json_bytes(&self, bytes: &[u8]) {
        let inner: DbMetaInner = serde_json::from_slice(bytes).expect("failed to load DbMetaInner");
        let mut guard = self.inner.lock().unwrap();
        *guard = inner;
    }

    pub(crate) fn to_json_bytes(&self) -> Vec<u8> {
        let inner = self.inner.lock().unwrap();
        serde_json::to_vec(&*inner).expect("failed to serialize DbMetaInner")
    }
}
