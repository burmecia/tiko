//! Service trait + registry for future in-VM services.
//!
//! The tikoguest agent is not just a Postgres controller — it will manage
//! other in-guest services (WAL archiver, backup agent, sidecars). This module
//! provides the extensibility scaffold:
//!
//! - [`Service`] trait: a service exposes a name, an HTTP request handler
//!   (under `/services/{name}/*`), and a status. The actual work runs as
//!   background tasks; the trait is the control surface.
//! - [`ServiceRegistry`]: holds registered services; the HTTP server dispatches
//!   `/services/{name}/*` requests to the matching service.
//!
//! PG control (`/pg/*`) stays first-class — it's the core responsibility with
//! fixed routes, not a `Service`.

use std::collections::HashMap;
use std::sync::Arc;

use crate::http::Response;

/// Status of a registered service.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ServiceStatus {
    Running,
    Stopped,
    Error,
}

/// A managed in-VM service. Implementations register with
/// [`ServiceRegistry`] and receive HTTP requests under `/services/{name}/*`.
///
/// `handle_request` is synchronous — for async work (e.g. starting a
/// background task), use `tokio::spawn` inside the handler and return
/// immediately. The HTTP control surface should be fast and non-blocking.
pub trait Service: Send + Sync + 'static {
    /// Service name (used in the URL path: `/services/{name}/...`).
    fn name(&self) -> &str;

    /// Handle an HTTP request. `rest` is the path segments after
    /// `/services/{name}/`. An empty `rest` means the request hit
    /// `/services/{name}` directly.
    fn handle_request(&self, method: &str, rest: &[&str], body: &[u8]) -> Response;

    /// Current status of the service.
    fn status(&self) -> ServiceStatus {
        ServiceStatus::Running
    }
}

/// Registry of services by name. Used by [`PgServer`](crate::server::PgServer)
/// to dispatch `/services/{name}/*` requests.
pub struct ServiceRegistry {
    services: HashMap<String, Arc<dyn Service>>,
}

impl ServiceRegistry {
    pub fn new() -> Self {
        Self {
            services: HashMap::new(),
        }
    }

    /// Register a service. If a service with the same name already exists, it
    /// is replaced.
    pub fn register(&mut self, service: Arc<dyn Service>) {
        let name = service.name().to_string();
        self.services.insert(name, service);
    }

    /// Look up a service by name.
    pub fn get(&self, name: &str) -> Option<&Arc<dyn Service>> {
        self.services.get(name)
    }

    /// List all registered services with their current status.
    pub fn list(&self) -> Vec<(&str, ServiceStatus)> {
        self.services
            .iter()
            .map(|(name, svc)| (name.as_str(), svc.status()))
            .collect()
    }

    /// Dispatch a request to the named service. Returns `None` if the service
    /// is not registered (caller should return 404).
    pub fn route(
        &self,
        name: &str,
        method: &str,
        rest: &[&str],
        body: &[u8],
    ) -> Option<Response> {
        self.services
            .get(name)
            .map(|svc| svc.handle_request(method, rest, body))
    }
}

impl Default for ServiceRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::{ok_json, no_content, not_found};

    /// A minimal service for testing the registry + dispatch.
    struct FakeService {
        running: std::sync::atomic::AtomicBool,
    }

    impl FakeService {
        fn new() -> Self {
            Self {
                running: std::sync::atomic::AtomicBool::new(true),
            }
        }
    }

    impl Service for FakeService {
        fn name(&self) -> &str {
            "fake"
        }

        fn handle_request(&self, method: &str, rest: &[&str], _body: &[u8]) -> Response {
            match (method, rest) {
                ("GET", []) => ok_json(serde_json::json!({"status": "ok"})),
                ("POST", ["start"]) => {
                    self.running.store(true, std::sync::atomic::Ordering::Relaxed);
                    no_content()
                }
                ("POST", ["stop"]) => {
                    self.running.store(false, std::sync::atomic::Ordering::Relaxed);
                    no_content()
                }
                _ => not_found(method, &format!("/services/fake/{}", rest.join("/"))),
            }
        }

        fn status(&self) -> ServiceStatus {
            if self.running.load(std::sync::atomic::Ordering::Relaxed) {
                ServiceStatus::Running
            } else {
                ServiceStatus::Stopped
            }
        }
    }

    #[test]
    fn registry_dispatch() {
        let mut registry = ServiceRegistry::new();
        registry.register(Arc::new(FakeService::new()));

        // GET /services/fake → 200 {"status":"ok"}
        let resp = registry.route("fake", "GET", &[], &[]).unwrap();
        assert_eq!(resp.status, 200);
        assert!(String::from_utf8_lossy(&resp.body).contains("ok"));

        // POST /services/fake/start → 204
        let resp = registry.route("fake", "POST", &["start"], &[]).unwrap();
        assert_eq!(resp.status, 204);

        // Unknown route → 404
        let resp = registry.route("fake", "GET", &["nonsense"], &[]).unwrap();
        assert_eq!(resp.status, 404);

        // Unknown service → None
        assert!(registry.route("ghost", "GET", &[], &[]).is_none());
    }

    #[test]
    fn registry_list() {
        let mut registry = ServiceRegistry::new();
        registry.register(Arc::new(FakeService::new()));
        let list = registry.list();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].0, "fake");
        assert!(matches!(list[0].1, ServiceStatus::Running));
    }
}
