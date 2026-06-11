//! API-key authentication middleware.
//!
//! Clients authenticate by presenting their virtual key as a Bearer token in
//! the `Authorization` header. The gateway resolves the token to a
//! [`KeyIdentity`] — which carries the key name and any per-key routing
//! overrides — or returns a 401 if the key is unknown.
//!
//! The resolved identity is stored in axum's [`Extensions`] and consumed by
//! the budget, limits, and routing layers.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use axum::extract::FromRequestParts;
use axum::http::{request::Parts, StatusCode};
use axum::response::{IntoResponse, Response};

use crate::config::VirtualKey;

/// Information attached to an authenticated request.
#[derive(Debug, Clone)]
pub struct KeyIdentity {
    /// Human-readable key name from config.
    pub name: String,
    /// When true every request from this key is treated as private,
    /// regardless of per-request markers.
    pub enforce_private: bool,
}

/// In-memory lookup table, built once from config at startup.
#[derive(Debug, Clone)]
pub struct KeyStore {
    /// bearer token -> identity
    inner: Arc<HashMap<String, KeyIdentity>>,
}

impl KeyStore {
    /// Build from the virtual-key table resolved by [`crate::config::GatewayConfig`].
    pub fn from_virtual_keys(keys: &[VirtualKey]) -> Self {
        let inner = keys
            .iter()
            .map(|vk| {
                let identity = KeyIdentity {
                    name: vk.name.clone(),
                    enforce_private: vk.enforce_private,
                };
                (vk.key.expose().to_string(), identity)
            })
            .collect();
        Self {
            inner: Arc::new(inner),
        }
    }

    /// `true` if the store has no configured virtual keys. In this mode every
    /// request is admitted as a public (non-private) caller — useful for
    /// unkeyed development deployments.
    pub fn is_open(&self) -> bool {
        self.inner.is_empty()
    }

    /// Look up a bearer token. Returns `None` if the token is unknown.
    pub fn authenticate(&self, token: &str) -> Option<&KeyIdentity> {
        self.inner.get(token)
    }
}

// ---------------------------------------------------------------------------
// Axum extractor
// ---------------------------------------------------------------------------

/// Extractor that resolves the caller's virtual key from the `Authorization`
/// header. On success, injects the [`KeyIdentity`] into request extensions so
/// downstream handlers can access it without re-parsing the header.
///
/// If no virtual keys are configured ([`KeyStore::is_open`]) every request is
/// admitted with a synthetic "anonymous" identity (non-private).
pub struct AuthedKey(pub KeyIdentity);

/// Rejection returned when authentication fails.
#[derive(Debug)]
pub struct AuthError(pub &'static str);

impl IntoResponse for AuthError {
    fn into_response(self) -> Response {
        (
            StatusCode::UNAUTHORIZED,
            axum::Json(serde_json::json!({
                "error": {
                    "message": self.0,
                    "type": "invalid_request_error",
                    "code": "invalid_api_key"
                }
            })),
        )
            .into_response()
    }
}

#[async_trait]
impl<S> FromRequestParts<S> for AuthedKey
where
    S: Send + Sync,
{
    type Rejection = AuthError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, AuthError> {
        let store = parts
            .extensions
            .get::<KeyStore>()
            .expect("KeyStore must be inserted into router extensions via Extension layer");

        if store.is_open() {
            return Ok(AuthedKey(KeyIdentity {
                name: "anonymous".to_string(),
                enforce_private: false,
            }));
        }

        let header = parts
            .headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .ok_or(AuthError("missing Authorization header"))?;

        let token = header
            .strip_prefix("Bearer ")
            .ok_or(AuthError("Authorization header must use the Bearer scheme"))?;

        store
            .authenticate(token)
            .cloned()
            .map(AuthedKey)
            .ok_or(AuthError("invalid API key"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Secret;

    fn make_key(name: &str, raw: &str, enforce_private: bool) -> VirtualKey {
        VirtualKey {
            name: name.to_string(),
            key: Secret::new(raw),
            enforce_private,
        }
    }

    #[test]
    fn known_key_authenticates() {
        let store = KeyStore::from_virtual_keys(&[make_key("dev", "sk-dev-abc", false)]);
        let id = store.authenticate("sk-dev-abc").unwrap();
        assert_eq!(id.name, "dev");
        assert!(!id.enforce_private);
    }

    #[test]
    fn unknown_key_returns_none() {
        let store = KeyStore::from_virtual_keys(&[make_key("dev", "sk-dev-abc", false)]);
        assert!(store.authenticate("sk-wrong").is_none());
    }

    #[test]
    fn enforce_private_is_propagated() {
        let store = KeyStore::from_virtual_keys(&[make_key("priv", "sk-priv-xyz", true)]);
        assert!(store.authenticate("sk-priv-xyz").unwrap().enforce_private);
    }

    #[test]
    fn empty_store_is_open() {
        let store = KeyStore::from_virtual_keys(&[]);
        assert!(store.is_open());
        // Open store admits anything.
        assert!(store.authenticate("whatever").is_none());
    }

    #[test]
    fn multiple_keys_are_independent() {
        let keys = [
            make_key("a", "sk-aaa", false),
            make_key("b", "sk-bbb", true),
        ];
        let store = KeyStore::from_virtual_keys(&keys);
        assert_eq!(store.authenticate("sk-aaa").unwrap().name, "a");
        assert_eq!(store.authenticate("sk-bbb").unwrap().name, "b");
        assert!(store.authenticate("sk-ccc").is_none());
    }
}
