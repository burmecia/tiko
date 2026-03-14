//! HTTP API — axum routes wrapping modules 2–7.
//!
//! All handlers are thin: they parse path/body, call the appropriate tikod
//! module function, and map errors to HTTP status codes.
//!
//! `/recover` returns 202 Accepted immediately; actual pg_ctl orchestration is
//! the caller's responsibility (spawned in the background by `serve`).

use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::{delete, get, post},
};
use pgsys::Lsn;
use serde::{Deserialize, Serialize};
use store::project::ProjectNamespace;
use store::sim_store::SimStore;

use crate::{lifecycle, org, pitr};

// ── AppState ──────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct AppState {
    pub sim: Arc<SimStore>,
    pub server_id: String,
    pub max_checkpoints: u64,
}

// ── Request / response types ──────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct CreateOrgRequest {
    pub org_id: u64,
}

#[derive(Deserialize)]
pub struct CreateBranchRequest {
    pub project_id: u64,
    pub branch_id: u64,
    pub parent_project_id: u64,
    pub parent_branch_id: u64,
    /// Parent timeline (usually 1 for a fresh project).
    #[serde(default = "default_timeline")]
    pub parent_timeline_id: u32,
    /// Branch-point LSN as hex string, e.g. `"0/3000000"`.
    pub lsn: String,
}

#[derive(Deserialize)]
pub struct RecoverRequest {
    pub target_timeline_id: u32,
    /// Target LSN as hex string, e.g. `"0/3000000"`.
    pub target_lsn: String,
}

#[derive(Serialize, Deserialize)]
pub struct RecoverAccepted {
    pub status: String,
    pub target_lsn: String,
}

fn default_timeline() -> u32 {
    1
}

/// Parse an LSN string in either PostgreSQL `X/Y` format or plain hex.
fn parse_lsn(s: &str) -> Option<Lsn> {
    if let Some((hi, lo)) = s.split_once('/') {
        let hi = u64::from_str_radix(hi, 16).ok()?;
        let lo = u64::from_str_radix(lo, 16).ok()?;
        Some(Lsn::new((hi << 32) | lo))
    } else {
        Lsn::from_hex(s).ok()
    }
}

// ── Error helper ─────────────────────────────────────────────────────────────

type ApiResult<T> = Result<(StatusCode, Json<T>), (StatusCode, String)>;

fn bad_request(msg: impl ToString) -> (StatusCode, String) {
    (StatusCode::BAD_REQUEST, msg.to_string())
}

fn not_found(msg: impl ToString) -> (StatusCode, String) {
    (StatusCode::NOT_FOUND, msg.to_string())
}

fn conflict(msg: impl ToString) -> (StatusCode, String) {
    (StatusCode::CONFLICT, msg.to_string())
}

fn internal(msg: impl ToString) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, msg.to_string())
}

// ── Handlers ──────────────────────────────────────────────────────────────────

/// `POST /orgs` — create org + root project.
async fn create_org(
    State(st): State<AppState>,
    Json(req): Json<CreateOrgRequest>,
) -> ApiResult<org::OrgMeta> {
    org::create_org(&st.sim, req.org_id)
        .map(|meta| (StatusCode::CREATED, Json(meta)))
        .map_err(|e| match e {
            org::Error::AlreadyExists => conflict(e),
            org::Error::NotFound => not_found(e),
            _ => internal(e),
        })
}

/// `POST /orgs/:org_id/projects` — create a branch forked from a parent.
async fn create_branch(
    State(st): State<AppState>,
    Path(org_id): Path<u64>,
    Json(req): Json<CreateBranchRequest>,
) -> ApiResult<store::project::ProjectMeta> {
    let lsn =
        parse_lsn(&req.lsn).ok_or_else(|| bad_request(format!("invalid lsn: {}", req.lsn)))?;

    let parent_ns = ProjectNamespace::new(org_id, req.parent_project_id, req.parent_branch_id);
    let child_ns = ProjectNamespace::new(org_id, req.project_id, req.branch_id);

    lifecycle::create_branch(&st.sim, &parent_ns, req.parent_timeline_id, &child_ns, lsn).map_err(
        |e| match e {
            lifecycle::Error::NotFound => not_found(e),
            _ => internal(e),
        },
    )?;

    lifecycle::get_project(&st.sim, &child_ns)
        .map(|meta| (StatusCode::CREATED, Json(meta)))
        .map_err(|e| internal(e))
}

/// `DELETE /orgs/:org_id/projects/:proj_id/:branch_id` — soft-delete a branch.
async fn delete_branch_handler(
    State(st): State<AppState>,
    Path((org_id, proj_id, branch_id)): Path<(u64, u64, u64)>,
) -> ApiResult<store::project::ProjectMeta> {
    let ns = ProjectNamespace::new(org_id, proj_id, branch_id);
    lifecycle::delete_branch(&st.sim, &ns).map_err(|e| match e {
        lifecycle::Error::NotFound => not_found(e),
        _ => internal(e),
    })?;
    lifecycle::get_project(&st.sim, &ns)
        .map(|meta| (StatusCode::OK, Json(meta)))
        .map_err(|e| internal(e))
}

/// `GET /orgs/:org_id/projects/:proj_id/:branch_id/restore-points`
async fn list_restore_points_handler(
    State(st): State<AppState>,
    Path((org_id, proj_id, branch_id)): Path<(u64, u64, u64)>,
) -> ApiResult<Vec<pitr::RestorePoint>> {
    let ns = ProjectNamespace::new(org_id, proj_id, branch_id);
    pitr::list_restore_points(&st.sim, &ns)
        .map(|pts| (StatusCode::OK, Json(pts)))
        .map_err(|e| internal(e.to_string()))
}

/// `POST /orgs/:org_id/projects/:proj_id/:branch_id/recover`
///
/// Validates the target delta exists, then returns 202 Accepted.
/// The caller is responsible for subsequently calling `orchestrate::run_recovery`.
async fn recover_handler(
    State(st): State<AppState>,
    Path((org_id, proj_id, branch_id)): Path<(u64, u64, u64)>,
    Json(req): Json<RecoverRequest>,
) -> ApiResult<RecoverAccepted> {
    let lsn = parse_lsn(&req.target_lsn)
        .ok_or_else(|| bad_request(format!("invalid lsn: {}", req.target_lsn)))?;
    let ns = ProjectNamespace::new(org_id, proj_id, branch_id);

    // Validate delta exists before accepting.
    let delta_key = ns.delta_manifest_key(req.target_timeline_id, lsn);
    let manifest_key = format!("{delta_key}/manifest.bin");
    let exists = st
        .sim
        .get_standard(&manifest_key)
        .map_err(|e| internal(e))?
        .is_some();
    if !exists {
        return Err(not_found(format!(
            "no delta manifest at tl={} lsn={}",
            req.target_timeline_id, req.target_lsn
        )));
    }

    Ok((
        StatusCode::ACCEPTED,
        Json(RecoverAccepted {
            status: "accepted".to_owned(),
            target_lsn: req.target_lsn,
        }),
    ))
}

// ── Router ────────────────────────────────────────────────────────────────────

/// Build the axum router.  Exported so `main.rs` can call it.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/orgs", post(create_org))
        .route("/orgs/{org_id}/projects", post(create_branch))
        .route(
            "/orgs/{org_id}/projects/{proj_id}/{branch_id}",
            delete(delete_branch_handler),
        )
        .route(
            "/orgs/{org_id}/projects/{proj_id}/{branch_id}/restore-points",
            get(list_restore_points_handler),
        )
        .route(
            "/orgs/{org_id}/projects/{proj_id}/{branch_id}/recover",
            post(recover_handler),
        )
        .with_state(state)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Method, Request};
    use std::collections::HashMap;
    use store::project::ensure_root_project_meta;
    use tempfile::TempDir;
    use tower::ServiceExt as _;

    fn temp_state() -> (AppState, TempDir) {
        let dir = TempDir::new().unwrap();
        let sim = SimStore::new(dir.path());
        let state = AppState {
            sim: Arc::new(sim),
            server_id: "test-server".to_owned(),
            max_checkpoints: 500,
        };
        (state, dir)
    }

    fn json_body(v: impl Serialize) -> Body {
        Body::from(serde_json::to_vec(&v).unwrap())
    }

    async fn call(app: Router, method: Method, uri: &str, body: Body) -> axum::response::Response {
        let req = Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json")
            .body(body)
            .unwrap();
        app.oneshot(req).await.unwrap()
    }

    /// `POST /orgs` creates an org and returns 201 with org metadata.
    #[tokio::test]
    async fn post_orgs_creates_org_and_returns_201() {
        let (state, _dir) = temp_state();
        let app = router(state.clone());

        let body = json_body(serde_json::json!({ "org_id": 1 }));
        let resp = call(app, Method::POST, "/orgs", body).await;

        assert_eq!(resp.status(), StatusCode::CREATED);

        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let meta: org::OrgMeta = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(meta.org_id, 1);
        assert!(meta.deleted_at.is_none());

        // Verify root project.json was written at (org=1, proj=0, branch=0).
        let ns = ProjectNamespace::new(1, 0, 0);
        assert!(
            state
                .sim
                .get_standard(&ns.project_meta_key())
                .unwrap()
                .is_some()
        );
    }

    /// `POST /orgs/:org/projects` creates a branch and returns 201 with metadata.
    #[tokio::test]
    async fn post_projects_creates_branch_and_returns_201() {
        let (state, _dir) = temp_state();

        // Set up parent: org + root project + a base manifest at branch_lsn.
        org::create_org(&state.sim, 2).unwrap();
        let parent_ns = ProjectNamespace::new(2, 20, 200);
        let branch_lsn = Lsn::new(0x3000);

        // Write a minimal base manifest for the parent.
        let tmp = TempDir::new().unwrap();
        let m = store::manifest::Manifest::new(
            branch_lsn,
            0,
            vec![],
            HashMap::new(),
            &tmp.path().join("base.tikm"),
        )
        .unwrap();
        state
            .sim
            .put_standard(
                &parent_ns.base_manifest_key(1, branch_lsn),
                &m.to_bytes().unwrap(),
            )
            .unwrap();

        let app = router(state.clone());
        let body = json_body(serde_json::json!({
            "project_id": 21,
            "branch_id": 201,
            "parent_project_id": 20,
            "parent_branch_id": 200,
            "parent_timeline_id": 1,
            "lsn": "0/3000"
        }));
        let resp = call(app, Method::POST, "/orgs/2/projects", body).await;

        assert_eq!(resp.status(), StatusCode::CREATED);
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let meta: store::project::ProjectMeta = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(meta.ns.project_id, 21);
        assert_eq!(meta.parent_project_id, Some(20));
    }

    /// `DELETE /orgs/:org/projects/:proj/:branch` soft-deletes and returns 200.
    #[tokio::test]
    async fn delete_project_sets_deleted_at() {
        let (state, _dir) = temp_state();

        let ns = ProjectNamespace::new(3, 30, 300);
        ensure_root_project_meta(&state.sim, &ns).unwrap();

        let app = router(state.clone());
        let resp = call(
            app,
            Method::DELETE,
            "/orgs/3/projects/30/300",
            Body::empty(),
        )
        .await;

        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let meta: store::project::ProjectMeta = serde_json::from_slice(&bytes).unwrap();
        assert!(meta.deleted_at.is_some(), "deleted_at must be set");
        assert_eq!(meta.status, "deleted");
    }

    /// `GET /orgs/:org/projects/:proj/:branch/restore-points` returns correct list.
    #[tokio::test]
    async fn get_restore_points_returns_correct_list() {
        let (state, _dir) = temp_state();

        let ns = ProjectNamespace::new(4, 40, 400);
        ensure_root_project_meta(&state.sim, &ns).unwrap();

        // Write two delta manifests at distinct LSNs.
        let lsn_a = Lsn::new(0x1000);
        let lsn_b = Lsn::new(0x2000);
        for lsn in [lsn_a, lsn_b] {
            let tmp = TempDir::new().unwrap();
            let m = store::manifest::Manifest::new(
                lsn,
                lsn.as_u64() as i64, // use lsn as timestamp for easy verification
                vec![],
                HashMap::new(),
                &tmp.path().join("delta.tikm"),
            )
            .unwrap();
            let delta_key = ns.delta_manifest_key(1, lsn);
            state
                .sim
                .put_standard(&format!("{delta_key}/manifest.bin"), &m.to_bytes().unwrap())
                .unwrap();
        }

        let app = router(state);
        let resp = call(
            app,
            Method::GET,
            "/orgs/4/projects/40/400/restore-points",
            Body::empty(),
        )
        .await;

        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let pts: Vec<pitr::RestorePoint> = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(pts.len(), 2);
        assert!(pts[0].lsn <= pts[1].lsn, "must be sorted ascending");
    }

    /// `POST /orgs/:org/projects/:proj/:branch/recover` validates delta exists,
    /// returns 202 Accepted; returns 404 if no delta manifest found.
    #[tokio::test]
    async fn post_recover_returns_202_when_delta_exists_and_404_when_not() {
        let (state, _dir) = temp_state();

        let ns = ProjectNamespace::new(5, 50, 500);
        ensure_root_project_meta(&state.sim, &ns).unwrap();

        let lsn = Lsn::new(0x5000);

        // 404 before delta is written.
        let app = router(state.clone());
        let body = json_body(serde_json::json!({
            "target_timeline_id": 1,
            "target_lsn": "0/5000"
        }));
        let resp = call(app, Method::POST, "/orgs/5/projects/50/500/recover", body).await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        // Write a minimal delta manifest.
        let tmp = TempDir::new().unwrap();
        let m = store::manifest::Manifest::new(
            lsn,
            0,
            vec![],
            HashMap::new(),
            &tmp.path().join("delta.tikm"),
        )
        .unwrap();
        let delta_key = ns.delta_manifest_key(1, lsn);
        state
            .sim
            .put_standard(&format!("{delta_key}/manifest.bin"), &m.to_bytes().unwrap())
            .unwrap();

        // 202 after delta is written.
        let app2 = router(state);
        let body2 = json_body(serde_json::json!({
            "target_timeline_id": 1,
            "target_lsn": "0/5000"
        }));
        let resp2 = call(app2, Method::POST, "/orgs/5/projects/50/500/recover", body2).await;
        assert_eq!(resp2.status(), StatusCode::ACCEPTED);
        let bytes = axum::body::to_bytes(resp2.into_body(), usize::MAX)
            .await
            .unwrap();
        let accepted: RecoverAccepted =
            serde_json::from_str(std::str::from_utf8(&bytes).unwrap()).unwrap();
        assert_eq!(accepted.status, "accepted");
    }
}
