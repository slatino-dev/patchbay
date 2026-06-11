//! HTTP server assembly: axum router, middleware stack, and request handlers.
//!
//! # Routes
//!
//! | Method | Path                     | Description                              |
//! |--------|--------------------------|------------------------------------------|
//! | POST   | /v1/chat/completions     | OpenAI-compatible proxy (stream + non-stream) |
//! | GET    | /v1/models               | List of all configured model identifiers |
//! | GET    | /healthz                 | Liveness probe — always 200 OK           |
//! | GET    | /metrics                 | Prometheus text-format metrics           |
//!
//! # Fallback + retry
//!
//! Before the first upstream byte is written to the client, transient errors
//! (upstream non-2xx, connection failure) trigger jittered exponential
//! backoff retries against the same backend, then promotion to the next
//! eligible backend. Once the relay has started streaming (first byte sent),
//! no retry is possible — the error propagates to the client.
//!
//! # Non-stream requests
//!
//! If `"stream": false` (or absent) patchbay reads the full upstream response
//! into memory and returns it as a single JSON body. Token usage from the
//! response is accounted identically to streaming.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::{Extension, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use bytes::Bytes;
use futures::StreamExt;
use serde::Deserialize;
use tokio::time::sleep;
use tracing::{info, warn};

use crate::auth::{AuthedKey, KeyStore};
use crate::budget::BudgetLedger;
use crate::config::GatewayConfig;
use crate::limits::RateLimitStore;
use crate::metrics::MetricsHandle;
use crate::router::{RouteQuery, Router as LlmRouter};
use crate::upstream::{RelayOutcome, SseRelay, UpstreamClient, UpstreamError};

// ---------------------------------------------------------------------------
// Shared application state
// ---------------------------------------------------------------------------

/// All shared objects injected into every handler.
#[derive(Clone)]
pub struct AppState {
    pub router: Arc<LlmRouter>,
    pub upstream: Arc<UpstreamClient>,
    pub keys: KeyStore,
    pub budget: BudgetLedger,
    pub limits: RateLimitStore,
    pub metrics: MetricsHandle,
}

impl AppState {
    pub fn from_config(cfg: &GatewayConfig) -> anyhow::Result<Self> {
        Ok(Self {
            router: Arc::new(LlmRouter::from_config(cfg)),
            upstream: Arc::new(UpstreamClient::new()?),
            keys: KeyStore::from_virtual_keys(&cfg.virtual_keys),
            budget: BudgetLedger::new(Default::default(), Duration::from_secs(3600), None),
            limits: RateLimitStore::new(Default::default()),
            metrics: MetricsHandle::new(),
        })
    }
}

// ---------------------------------------------------------------------------
// Retry policy
// ---------------------------------------------------------------------------

/// Jitter + exponential backoff for pre-first-byte retries only.
/// Base 100ms, up to 5 retries, capped at 2s per wait.
const MAX_RETRIES: usize = 5;
const BASE_BACKOFF_MS: u64 = 100;
const MAX_BACKOFF_MS: u64 = 2_000;
const STALL_TIMEOUT: Duration = Duration::from_secs(30);

fn backoff_ms(attempt: usize) -> u64 {
    // Truncated exponential: 100, 200, 400, 800, 1600 -> capped at 2000
    let ms = BASE_BACKOFF_MS * (1u64 << attempt.min(10));
    ms.min(MAX_BACKOFF_MS)
}

/// Jitter: add up to 25% random noise to avoid thundering herds.
fn jitter(ms: u64) -> u64 {
    use std::hash::{BuildHasher, Hasher};
    // A quick pseudo-random jitter from the current time — avoids pulling in
    // the `rand` crate while still spreading retries across backends.
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u64(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64,
    );
    let noise = h.finish() % (ms / 4 + 1);
    ms + noise
}

// ---------------------------------------------------------------------------
// Route: POST /v1/chat/completions
// ---------------------------------------------------------------------------

/// Minimal deserialization of the client's payload so we know whether
/// streaming was requested. We never re-serialize; the raw bytes go upstream.
#[derive(Debug, Deserialize, Default)]
struct CompletionRequest {
    #[serde(default)]
    model: String,
    #[serde(default)]
    stream: bool,
}

pub async fn chat_completions(
    State(state): State<AppState>,
    AuthedKey(identity): AuthedKey,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    // --- Rate limiting ---
    if let Err(e) = state.limits.check_rpm(&identity.name) {
        state.metrics.record_rate_limit_rejection(&identity.name);
        warn!(key = %identity.name, "rate limit exceeded: {e}");
        return error_response(
            StatusCode::TOO_MANY_REQUESTS,
            "rate_limit_exceeded",
            &e.to_string(),
        );
    }

    // --- Parse model + stream flag ---
    let req: CompletionRequest = serde_json::from_slice(&body).unwrap_or_default();
    let model = if req.model.is_empty() {
        // Fall back to the model listed in the path (none in this simple
        // gateway), or fail gracefully.
        return error_response(
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            "field `model` is required",
        );
    } else {
        req.model.clone()
    };

    // --- Route ---
    let mark_private = identity.enforce_private;
    let query = RouteQuery::new(model.clone());
    let backend = match state.router.route(query, mark_private) {
        Ok(b) => b.clone(),
        Err(e) => {
            warn!(key = %identity.name, "no eligible backend: {e}");
            return error_response(
                StatusCode::BAD_GATEWAY,
                "no_eligible_backend",
                &e.to_string(),
            );
        }
    };

    // --- Forward with retries (pre-first-byte only) ---
    let mut last_err: Option<UpstreamError> = None;
    for attempt in 0..=MAX_RETRIES {
        if attempt > 0 {
            let ms = jitter(backoff_ms(attempt - 1));
            info!(
                attempt,
                backend = %backend.name,
                wait_ms = ms,
                "retrying upstream request"
            );
            sleep(Duration::from_millis(ms)).await;
        }

        let resp = match state
            .upstream
            .open_sse(&backend, "/v1/chat/completions", body.clone())
            .await
        {
            Ok(r) => r,
            Err(e) => {
                warn!(backend = %backend.name, attempt, "upstream error: {e}");
                state.metrics.record_upstream_error(&backend.name);
                last_err = Some(e);
                continue;
            }
        };

        // --- Got a response — relay it ---
        let status_class = "2xx".to_string();
        state
            .metrics
            .record_request(&backend.name, &model, &status_class);

        // Forward the Content-Type from the upstream so clients receive
        // text/event-stream for streaming responses.
        let content_type = resp
            .headers()
            .get(header::CONTENT_TYPE)
            .cloned()
            .unwrap_or_else(|| {
                if req.stream {
                    header::HeaderValue::from_static("text/event-stream")
                } else {
                    header::HeaderValue::from_static("application/json")
                }
            });

        if req.stream {
            return stream_response(
                state,
                identity.name,
                backend.name.clone(),
                model.clone(),
                resp,
                content_type,
                headers,
            )
            .await;
        } else {
            return buffered_response(state, identity.name, backend.name.clone(), resp).await;
        }
    }

    // Exhausted all retries.
    let msg = last_err
        .map(|e| e.to_string())
        .unwrap_or_else(|| "upstream unreachable".to_string());
    state.metrics.record_request(&backend.name, &model, "5xx");
    error_response(StatusCode::BAD_GATEWAY, "upstream_error", &msg)
}

/// Stream the upstream response through the SSE relay, accounting usage at end.
async fn stream_response(
    state: AppState,
    key_name: String,
    backend_name: String,
    model: String,
    resp: reqwest::Response,
    content_type: header::HeaderValue,
    _client_headers: HeaderMap,
) -> Response {
    let (relay, summary_rx) = SseRelay::from_response(resp, STALL_TIMEOUT);

    // Spawn background task to account usage when stream ends.
    let metrics = state.metrics.clone();
    let budget = state.budget.clone();
    let limits = state.limits.clone();
    let key = key_name.clone();
    let bk = backend_name.clone();
    let mdl = model.clone();
    tokio::spawn(async move {
        if let Ok(summary) = summary_rx.await {
            if let Some(usage) = &summary.usage {
                budget.record(&key, usage);
                metrics.record_tokens(&key, usage);
                let _ = limits.record_tpm(&key, usage.total_tokens);
            }
            match summary.outcome {
                RelayOutcome::Completed => {}
                RelayOutcome::Stalled => {
                    warn!(backend = %bk, model = %mdl, "stream stalled");
                    metrics.record_upstream_error(&bk);
                }
                RelayOutcome::UpstreamError(ref e) => {
                    warn!(backend = %bk, model = %mdl, "stream upstream error: {e}");
                    metrics.record_upstream_error(&bk);
                }
                RelayOutcome::ClientDisconnected => {
                    info!(key = %key, "client disconnected mid-stream");
                }
            }
        }
    });

    // Build the streaming response body from the relay.
    let stream =
        relay.map(|r| r.map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>));

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type)
        .header("X-Accel-Buffering", "no")
        .header("Cache-Control", "no-cache")
        .body(Body::from_stream(stream))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

/// Read the full upstream response into memory and return it as-is.
async fn buffered_response(
    state: AppState,
    key_name: String,
    backend_name: String,
    resp: reqwest::Response,
) -> Response {
    let status = resp.status();
    let ct = resp
        .headers()
        .get(header::CONTENT_TYPE)
        .cloned()
        .unwrap_or_else(|| header::HeaderValue::from_static("application/json"));

    let body = match resp.bytes().await {
        Ok(b) => b,
        Err(e) => {
            warn!(backend = %backend_name, "failed to read upstream body: {e}");
            state.metrics.record_upstream_error(&backend_name);
            return error_response(StatusCode::BAD_GATEWAY, "upstream_error", &e.to_string());
        }
    };

    // Try to extract token usage from the non-streaming response body.
    if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&body) {
        if let Some(usage_obj) = v.get("usage") {
            if let Ok(usage) = serde_json::from_value::<crate::upstream::Usage>(usage_obj.clone()) {
                state.budget.record(&key_name, &usage);
                state.metrics.record_tokens(&key_name, &usage);
                let _ = state.limits.record_tpm(&key_name, usage.total_tokens);
            }
        }
    }

    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, ct)
        .body(Body::from(body))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

// ---------------------------------------------------------------------------
// Route: GET /v1/models
// ---------------------------------------------------------------------------

pub async fn list_models(State(state): State<AppState>, AuthedKey(_): AuthedKey) -> Response {
    let models: Vec<serde_json::Value> = state
        .router
        .backends()
        .iter()
        .flat_map(|b| b.models.iter().cloned())
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .map(|id| {
            serde_json::json!({
                "id": id,
                "object": "model",
                "owned_by": "patchbay"
            })
        })
        .collect();

    (
        StatusCode::OK,
        axum::Json(serde_json::json!({
            "object": "list",
            "data": models
        })),
    )
        .into_response()
}

// ---------------------------------------------------------------------------
// Route: GET /healthz
// ---------------------------------------------------------------------------

pub async fn healthz() -> StatusCode {
    StatusCode::OK
}

// ---------------------------------------------------------------------------
// Route: GET /metrics
// ---------------------------------------------------------------------------

pub async fn metrics(State(state): State<AppState>) -> Response {
    let body = state.metrics.render();
    if body.is_empty() {
        // No data yet; return empty 200 with the correct content type.
        Response::builder()
            .status(StatusCode::OK)
            .header(
                header::CONTENT_TYPE,
                "text/plain; version=0.0.4; charset=utf-8",
            )
            .body(Body::empty())
            .unwrap()
    } else {
        Response::builder()
            .status(StatusCode::OK)
            .header(
                header::CONTENT_TYPE,
                "text/plain; version=0.0.4; charset=utf-8",
            )
            .body(Body::from(body))
            .unwrap()
    }
}

// ---------------------------------------------------------------------------
// Router assembly
// ---------------------------------------------------------------------------

/// Build the full axum [`Router`] with all routes and shared state.
///
/// The `KeyStore` is injected via [`Extension`] so the [`AuthedKey`]
/// extractor can access it without exposing the raw keys in `AppState`.
pub fn build_router(state: AppState) -> Router {
    let keys = state.keys.clone();
    Router::new()
        .route("/v1/chat/completions", post(chat_completions))
        .route("/v1/models", get(list_models))
        .route("/healthz", get(healthz))
        .route("/metrics", get(metrics))
        .layer(Extension(keys))
        .with_state(state)
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

fn error_response(status: StatusCode, err_type: &str, message: &str) -> Response {
    (
        status,
        axum::Json(serde_json::json!({
            "error": {
                "message": message,
                "type": err_type,
                "code": status.as_u16()
            }
        })),
    )
        .into_response()
}
