//! End-to-end integration tests for the full HTTP server.
//!
//! Each test spins up one or more in-process mock upstream servers, binds
//! the patchbay gateway on an ephemeral port, and exercises it with
//! plain `reqwest` calls through a real TCP connection. This exercises the
//! full path: auth → rate-limit → router → upstream client → relay → client.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::State;
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use bytes::Bytes;
use futures::StreamExt;

use patchbay::auth::KeyStore;
use patchbay::budget::BudgetLedger;
use patchbay::config::{Backend, GatewayConfig, Privacy, Secret};
use patchbay::limits::RateLimitStore;
use patchbay::metrics::MetricsHandle;
use patchbay::router::Router as LlmRouter;
use patchbay::server::{build_router, AppState};
use patchbay::upstream::UpstreamClient;

// ---------------------------------------------------------------------------
// Mock upstream helpers
// ---------------------------------------------------------------------------

/// What the mock should serve for a given request.
#[derive(Clone)]
enum MockBehavior {
    /// Return a streaming SSE response with these chunks.
    Stream(Vec<&'static [u8]>),
    /// Return a non-streaming JSON body.
    Json(&'static str),
    /// Return this HTTP status code (non-2xx — simulates an upstream error).
    Error(u16),
}

#[derive(Clone)]
struct MockUpstreamState {
    behavior: MockBehavior,
    /// Counts how many requests hit this mock.
    hits: Arc<AtomicU32>,
}

async fn mock_upstream_handler(
    State(state): State<MockUpstreamState>,
    _headers: HeaderMap,
) -> Response {
    state.hits.fetch_add(1, Ordering::SeqCst);
    match &state.behavior {
        MockBehavior::Stream(chunks) => {
            let chunks = chunks.clone();
            let stream = futures::stream::iter(
                chunks
                    .into_iter()
                    .map(|c| Ok::<_, std::convert::Infallible>(Bytes::from_static(c))),
            );
            (
                [(header::CONTENT_TYPE, "text/event-stream")],
                Body::from_stream(stream),
            )
                .into_response()
        }
        MockBehavior::Json(body) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/json")],
            *body,
        )
            .into_response(),
        MockBehavior::Error(code) => Response::builder()
            .status(*code)
            .body(Body::from(format!("upstream error {code}")))
            .unwrap(),
    }
}

/// Bind a mock upstream and return its address + hit counter.
async fn start_mock_upstream(behavior: MockBehavior) -> (SocketAddr, Arc<AtomicU32>) {
    let hits = Arc::new(AtomicU32::new(0));
    let state = MockUpstreamState {
        behavior,
        hits: hits.clone(),
    };
    let app = axum::Router::new()
        .route("/v1/chat/completions", post(mock_upstream_handler))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (addr, hits)
}

// ---------------------------------------------------------------------------
// Gateway helpers
// ---------------------------------------------------------------------------

fn build_backend(name: &str, addr: SocketAddr) -> Backend {
    Backend {
        name: name.to_string(),
        base_url: format!("http://{addr}"),
        api_key: Some(Secret::new(format!("key-{name}"))),
        models: vec!["test-model".to_string()],
        capability_tags: vec![],
        privacy: Privacy::Local,
    }
}

fn make_config_for_backends(backends: Vec<Backend>) -> GatewayConfig {
    GatewayConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        backends,
        virtual_keys: vec![],
        policy: patchbay::config::PolicySelection::StaticPriority,
    }
}

async fn start_gateway(cfg: &GatewayConfig) -> (SocketAddr, reqwest::Client) {
    let state = AppState {
        router: Arc::new(LlmRouter::from_config(cfg)),
        upstream: Arc::new(UpstreamClient::new().unwrap()),
        keys: KeyStore::from_virtual_keys(&cfg.virtual_keys),
        budget: BudgetLedger::new(Default::default(), Duration::from_secs(3600), None),
        limits: RateLimitStore::new(Default::default()),
        metrics: MetricsHandle::new(),
    };
    let app = build_router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    (addr, client)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Non-streaming request: the gateway forwards the upstream JSON body
/// verbatim and returns 200.
#[tokio::test]
async fn non_stream_request_proxied() {
    let response_body = r#"{"id":"chatcmpl-1","object":"chat.completion","model":"test-model","choices":[{"message":{"role":"assistant","content":"hello"},"finish_reason":"stop"}],"usage":{"prompt_tokens":5,"completion_tokens":3,"total_tokens":8}}"#;
    let (upstream_addr, hits) = start_mock_upstream(MockBehavior::Json(response_body)).await;

    let cfg = make_config_for_backends(vec![build_backend("primary", upstream_addr)]);
    let (gw_addr, client) = start_gateway(&cfg).await;

    let resp = client
        .post(format!("http://{gw_addr}/v1/chat/completions"))
        .header("Content-Type", "application/json")
        .body(
            r#"{"model":"test-model","stream":false,"messages":[{"role":"user","content":"hi"}]}"#,
        )
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    // The upstream body must be returned verbatim.
    assert!(body.contains("chatcmpl-1"), "got: {body}");
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}

/// Streaming request: the gateway relays every chunk from the mock upstream.
#[tokio::test]
async fn stream_request_relayed() {
    let chunks: Vec<&'static [u8]> = vec![
        b"data: {\"id\":\"c1\",\"choices\":[{\"delta\":{\"content\":\"Hello\"}}]}\n\n",
        b"data: {\"id\":\"c1\",\"choices\":[],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":1,\"total_tokens\":4}}\n\n",
        b"data: [DONE]\n\n",
    ];
    let (upstream_addr, hits) = start_mock_upstream(MockBehavior::Stream(chunks.clone())).await;

    let cfg = make_config_for_backends(vec![build_backend("primary", upstream_addr)]);
    let (gw_addr, client) = start_gateway(&cfg).await;

    let resp = client
        .post(format!("http://{gw_addr}/v1/chat/completions"))
        .header("Content-Type", "application/json")
        .body(r#"{"model":"test-model","stream":true,"messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);

    // Collect the full streamed body.
    let mut stream = resp.bytes_stream();
    let mut received: Vec<u8> = Vec::new();
    while let Some(chunk) = stream.next().await {
        received.extend_from_slice(&chunk.unwrap());
    }

    let expected: Vec<u8> = chunks.concat();
    assert_eq!(
        received, expected,
        "streamed bytes differ from upstream script"
    );
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}

/// Retry behaviour: primary mock returns 500, gateway retries up to MAX_RETRIES
/// times against the same backend (StaticPriority always picks the same one),
/// then returns a 502.
///
/// Note: per-request backend *promotion* (trying the next eligible backend after
/// exhausting retries on the first) is not yet wired — the router is stateless
/// per-request. This test verifies that the gateway retries transient errors and
/// fails closed with a 502 rather than hanging or panicking.
#[tokio::test]
async fn upstream_5xx_is_retried_then_502() {
    let (primary_addr, primary_hits) = start_mock_upstream(MockBehavior::Error(500)).await;

    let cfg = make_config_for_backends(vec![build_backend("primary", primary_addr)]);
    let (gw_addr, client) = start_gateway(&cfg).await;

    let resp = client
        .post(format!("http://{gw_addr}/v1/chat/completions"))
        .header("Content-Type", "application/json")
        .body(
            r#"{"model":"test-model","stream":false,"messages":[{"role":"user","content":"hi"}]}"#,
        )
        .send()
        .await
        .unwrap();

    // After exhausting all retries the gateway returns 502.
    assert_eq!(resp.status().as_u16(), 502, "expected 502 after retry exhaustion");

    // The primary must have been hit more than once (retries fired).
    let hits = primary_hits.load(Ordering::SeqCst);
    assert!(
        hits > 1,
        "expected retries (hits > 1), got {hits}"
    );
}

/// GET /healthz always returns 200.
#[tokio::test]
async fn healthz_returns_200() {
    let (upstream_addr, _) = start_mock_upstream(MockBehavior::Json("{}")).await;
    let cfg = make_config_for_backends(vec![build_backend("b", upstream_addr)]);
    let (gw_addr, client) = start_gateway(&cfg).await;

    let resp = client
        .get(format!("http://{gw_addr}/healthz"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

/// GET /v1/models returns a list containing all configured model IDs.
#[tokio::test]
async fn models_endpoint_lists_models() {
    let (upstream_addr, _) = start_mock_upstream(MockBehavior::Json("{}")).await;
    // Build two backends with different models.
    let mut b1 = build_backend("b1", upstream_addr);
    b1.models = vec!["model-a".to_string(), "model-b".to_string()];
    let mut b2 = build_backend("b2", upstream_addr);
    b2.models = vec!["model-c".to_string()];

    let cfg = make_config_for_backends(vec![b1, b2]);
    let (gw_addr, client) = start_gateway(&cfg).await;

    let resp = client
        .get(format!("http://{gw_addr}/v1/models"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let ids: Vec<String> = body["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v["id"].as_str().unwrap().to_string())
        .collect();
    assert!(ids.contains(&"model-a".to_string()));
    assert!(ids.contains(&"model-b".to_string()));
    assert!(ids.contains(&"model-c".to_string()));
}

/// GET /metrics starts empty and accumulates after requests.
#[tokio::test]
async fn metrics_endpoint_accumulates() {
    let response_body = r#"{"id":"c","choices":[],"usage":{"prompt_tokens":2,"completion_tokens":2,"total_tokens":4}}"#;
    let (upstream_addr, _) = start_mock_upstream(MockBehavior::Json(response_body)).await;
    let cfg = make_config_for_backends(vec![build_backend("b", upstream_addr)]);
    let (gw_addr, client) = start_gateway(&cfg).await;

    // Before any requests, /metrics is empty (or has no data lines).
    let resp = client
        .get(format!("http://{gw_addr}/metrics"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    // No counter lines yet (families are omitted when empty).
    let body = resp.text().await.unwrap();
    assert!(!body.contains("patchbay_requests_total{"));

    // Make one request.
    client
        .post(format!("http://{gw_addr}/v1/chat/completions"))
        .header("Content-Type", "application/json")
        .body(r#"{"model":"test-model","stream":false,"messages":[]}"#)
        .send()
        .await
        .unwrap();

    // Now /metrics should have the counter.
    let resp2 = client
        .get(format!("http://{gw_addr}/metrics"))
        .send()
        .await
        .unwrap();
    let body2 = resp2.text().await.unwrap();
    assert!(
        body2.contains("patchbay_requests_total"),
        "metrics body: {body2}"
    );
}

/// Missing `model` field returns a 400 Bad Request.
#[tokio::test]
async fn missing_model_returns_400() {
    let (upstream_addr, _) = start_mock_upstream(MockBehavior::Json("{}")).await;
    let cfg = make_config_for_backends(vec![build_backend("b", upstream_addr)]);
    let (gw_addr, client) = start_gateway(&cfg).await;

    let resp = client
        .post(format!("http://{gw_addr}/v1/chat/completions"))
        .header("Content-Type", "application/json")
        .body(r#"{"messages":[]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
}

/// A 4xx from the upstream is passed through immediately with exactly one hit —
/// it is not retried (retrying client errors wastes upstream capacity and
/// delays the error response).
#[tokio::test]
async fn upstream_4xx_is_not_retried() {
    let (upstream_addr, hits) = start_mock_upstream(MockBehavior::Error(400)).await;
    let cfg = make_config_for_backends(vec![build_backend("b", upstream_addr)]);
    let (gw_addr, client) = start_gateway(&cfg).await;

    let resp = client
        .post(format!("http://{gw_addr}/v1/chat/completions"))
        .header("Content-Type", "application/json")
        .body(r#"{"model":"test-model","messages":[]}"#)
        .send()
        .await
        .unwrap();

    // The upstream 400 should be surfaced as a 4xx (not a 502).
    let status = resp.status().as_u16();
    assert!(
        (400..500).contains(&status),
        "expected 4xx, got {status}"
    );
    // Exactly one hit — no retry.
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "4xx response was retried — expected exactly 1 upstream hit"
    );
}

/// Requesting a model not served by any backend returns a 502.
#[tokio::test]
async fn unknown_model_returns_502() {
    let (upstream_addr, _) = start_mock_upstream(MockBehavior::Json("{}")).await;
    let cfg = make_config_for_backends(vec![build_backend("b", upstream_addr)]);
    let (gw_addr, client) = start_gateway(&cfg).await;

    let resp = client
        .post(format!("http://{gw_addr}/v1/chat/completions"))
        .header("Content-Type", "application/json")
        .body(r#"{"model":"nonexistent-model","messages":[]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 502);
}
