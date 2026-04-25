use serde::{Deserialize, Serialize};
use std::{fmt, sync::Mutex};

use crate::env;
use crate::manifest::ChunkRef;
use crate::{ChunkTag, chunk::RelFork};
use pgsys::Lsn;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub(crate) struct DbNamespace {
    org_id: u64,
    db_id: u64,
    project_id: u64,
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
    timeline_id: u32,
    checkpoint_lsn: Lsn,
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
            timeline_id: 1,
            checkpoint_lsn: Lsn::default(),
            created_at: 0,
            status: "active".to_string(),
            deleted_at: None,
        };
        Self {
            inner: Mutex::new(inner),
        }
    }

    pub(crate) fn set_checkpoint_lsn(&self, lsn: Lsn) {
        self.inner.lock().unwrap().checkpoint_lsn = lsn;
    }

    pub(crate) fn relfork_meta_key(&self, rf: &RelFork) -> String {
        let inner = self.inner.lock().unwrap();
        format!(
            "{}/chunks/{}/{rf}/meta.json",
            inner.ns,
            inner.checkpoint_lsn.to_hex()
        )
    }

    pub(crate) fn relfork_chunk_key(&self, tag: &ChunkTag) -> String {
        let inner = self.inner.lock().unwrap();
        let rf = tag.relfork();
        format!(
            "{}/chunks/{}/{rf}/{}",
            inner.ns,
            inner.checkpoint_lsn.to_hex(),
            tag.chunk_id
        )
    }

    pub(crate) fn chunk_key_standard(&self, tag: &ChunkTag, _chunk_ref: &ChunkRef) -> String {
        // TODO: placeholder for future
        let inner = self.inner.lock().unwrap();
        let rf = tag.relfork();
        format!("{}/chunks/{rf}/{}", inner.ns, tag.chunk_id)
    }
}
