//! End-to-end SSE relay tests against an in-test mock upstream.
//!
//! The mock is a real axum HTTP server on an ephemeral port that streams
//! scripted chunks (with scripted pauses) through a real TCP connection, so
//! these tests exercise the full path the gateway uses in production:
//! `reqwest` streaming -> `SseRelay` -> client.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::Body;
use axum::extract::State;
use axum::http::{header, HeaderMap};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use bytes::Bytes;
use futures::StreamExt;
use tokio::sync::Notify;

use patchbay::config::{Backend, Privacy, Secret};
use patchbay::upstream::{RelayOutcome, SseRelay, UpstreamClient, Usage};

#[derive(Clone)]
enum Step {
    Chunk(&'static [u8]),
    Wait(Duration),
}

struct MockState {
    script: Vec<Step>,
    /// Authorization header observed on the last request.
    auth: Mutex<Option<String>>,
    /// Set when the mock fails to write because the peer went away.
    peer_disconnected: AtomicBool,
    notify: Notify,
}

async fn mock_handler(State(state): State<Arc<MockState>>, headers: HeaderMap) -> Response {
    *state.auth.lock().unwrap() = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .map(String::from);

    let (tx, rx) = tokio::sync::mpsc::channel::<Bytes>(1);
    let script = state.script.clone();
    let state2 = state.clone();
    tokio::spawn(async move {
        for step in script {
            match step {
                Step::Wait(d) => tokio::time::sleep(d).await,
                Step::Chunk(c) => {
                    if tx.send(Bytes::from_static(c)).await.is_err() {
                        state2.peer_disconnected.store(true, Ordering::SeqCst);
                        state2.notify.notify_waiters();
                        return;
                    }
                }
            }
        }
        // Dropping tx ends the body stream (normal upstream completion).
    });

    let stream = futures::stream::unfold(rx, |mut rx| async move {
        rx.recv()
            .await
            .map(|b| (Ok::<_, std::convert::Infallible>(b), rx))
    });
    (
        [(header::CONTENT_TYPE, "text/event-stream")],
        Body::from_stream(stream),
    )
        .into_response()
}

async fn start_mock(
    script: Vec<Step>,
) -> (SocketAddr, Arc<MockState>, tokio::task::JoinHandle<()>) {
    let state = Arc::new(MockState {
        script,
        auth: Mutex::new(None),
        peer_disconnected: AtomicBool::new(false),
        notify: Notify::new(),
    });
    let app = axum::Router::new()
        .route("/v1/chat/completions", post(mock_handler))
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (addr, state, server)
}

fn mock_backend(addr: SocketAddr) -> Backend {
    Backend {
        name: "mock".to_string(),
        base_url: format!("http://{addr}"),
        api_key: Some(Secret::new("test-virtual-key-123")),
        models: vec!["m".to_string()],
        capability_tags: vec![],
        privacy: Privacy::Local,
    }
}

/// Scripted chunks with deliberately hostile framing: events split mid-line,
/// mid-JSON-key, multi-byte UTF-8 in the payload, a `"usage":null` chunk, a
/// real usage chunk, and a [DONE] terminator.
const SCRIPT_CHUNKS: &[&[u8]] = &[
    b"data: {\"id\":\"c1\",\"choices\":[{\"delta\":{\"content\":\"Hel",
    b"lo\"}}],\"usage\":null}\n\nda",
    "ta: {\"id\":\"c1\",\"choices\":[{\"delta\":{\"content\":\" w\u{00f6}rld \u{2728}\"}}]}\n\n"
        .as_bytes(),
    b"data: {\"id\":\"c1\",\"choices\":[],\"usage\":{\"prompt_tokens\":7,\"completion_tokens\":42,\"total_tokens\":49}}\n\n",
    b"data: [DONE]\n\n",
];

#[tokio::test]
async fn relay_is_byte_faithful_and_intercepts_usage() {
    let script: Vec<Step> = SCRIPT_CHUNKS.iter().map(|c| Step::Chunk(c)).collect();
    let (addr, state, server) = start_mock(script).await;

    let client = UpstreamClient::new().unwrap();
    let backend = mock_backend(addr);
    let request_body = Bytes::from_static(b"{\"model\":\"m\",\"stream\":true}");
    let response = client
        .open_sse(&backend, "/v1/chat/completions", request_body)
        .await
        .unwrap();

    let (relay, summary_rx) = SseRelay::from_response(response, Duration::from_secs(5));
    let relayed: Vec<Result<Bytes, _>> = relay.collect().await;

    // Byte-faithful: the concatenation of everything the client received is
    // exactly the concatenation of what the upstream sent. (Per-chunk
    // boundaries are owned by TCP, not by the relay contract.)
    let mut got: Vec<u8> = Vec::new();
    for chunk in relayed {
        got.extend_from_slice(&chunk.expect("no relay error expected"));
    }
    let want: Vec<u8> = SCRIPT_CHUNKS.concat();
    assert_eq!(got, want, "relayed bytes differ from scripted bytes");

    // Usage interception for accounting.
    let summary = summary_rx.await.unwrap();
    assert_eq!(summary.outcome, RelayOutcome::Completed);
    assert_eq!(
        summary.usage,
        Some(Usage {
            prompt_tokens: 7,
            completion_tokens: 42,
            total_tokens: 49
        })
    );
    assert!(summary.saw_done);
    assert_eq!(summary.bytes_relayed, want.len() as u64);

    // Credentials were injected from the backend definition.
    assert_eq!(
        state.auth.lock().unwrap().as_deref(),
        Some("Bearer test-virtual-key-123")
    );

    server.abort();
}

#[tokio::test]
async fn relay_errors_when_upstream_stalls() {
    let script = vec![
        Step::Chunk(b"data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n"),
        Step::Wait(Duration::from_secs(30)),
        Step::Chunk(b"data: [DONE]\n\n"),
    ];
    let (addr, _state, server) = start_mock(script).await;

    let client = UpstreamClient::new().unwrap();
    let response = client
        .open_sse(&mock_backend(addr), "/v1/chat/completions", Bytes::new())
        .await
        .unwrap();

    let (mut relay, summary_rx) = SseRelay::from_response(response, Duration::from_millis(200));

    // First chunk arrives fine.
    let first = relay.next().await.unwrap().unwrap();
    assert!(first.starts_with(b"data: "));

    // Then the upstream goes quiet: the relay must error out at ~200ms, not
    // hang for the scripted 30s.
    let next = tokio::time::timeout(Duration::from_secs(5), relay.next())
        .await
        .expect("relay must not hang on a stalled upstream");
    let err = next.unwrap().unwrap_err();
    assert!(
        err.to_string().contains("stalled"),
        "expected stall error, got: {err}"
    );
    // Stream is fused after the stall error.
    assert!(relay.next().await.is_none());

    let summary = summary_rx.await.unwrap();
    assert_eq!(summary.outcome, RelayOutcome::Stalled);
    assert_eq!(summary.usage, None);
    assert!(!summary.saw_done);

    server.abort();
}

#[tokio::test]
async fn client_disconnect_propagates_to_upstream() {
    let script = vec![
        Step::Chunk(b"data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n"),
        Step::Wait(Duration::from_millis(50)),
        Step::Chunk(b"data: {\"choices\":[{\"delta\":{\"content\":\"y\"}}]}\n\n"),
        Step::Wait(Duration::from_millis(50)),
        Step::Chunk(b"data: {\"choices\":[{\"delta\":{\"content\":\"z\"}}]}\n\n"),
        Step::Wait(Duration::from_secs(30)),
        Step::Chunk(b"data: [DONE]\n\n"),
    ];
    let (addr, state, server) = start_mock(script).await;

    let client = UpstreamClient::new().unwrap();
    let response = client
        .open_sse(&mock_backend(addr), "/v1/chat/completions", Bytes::new())
        .await
        .unwrap();

    let (mut relay, summary_rx) = SseRelay::from_response(response, Duration::from_secs(5));

    // Client consumes one chunk, then walks away mid-stream.
    relay.next().await.unwrap().unwrap();
    drop(relay);

    // The accounting summary still fires, marked as a disconnect.
    let summary = summary_rx.await.unwrap();
    assert_eq!(summary.outcome, RelayOutcome::ClientDisconnected);
    assert!(!summary.saw_done);

    // And the upstream sees the cancellation: its next write fails because
    // dropping the relay dropped the reqwest response and closed the
    // connection.
    tokio::time::timeout(Duration::from_secs(5), state.notify.notified())
        .await
        .expect("upstream never observed the client disconnect");
    assert!(state.peer_disconnected.load(Ordering::SeqCst));

    server.abort();
}

#[tokio::test]
async fn non_2xx_upstream_is_an_error_not_a_stream() {
    // Point at a route that doesn't exist on the mock -> 405/404.
    let (addr, _state, server) = start_mock(vec![]).await;
    let client = UpstreamClient::new().unwrap();
    let err = client
        .open_sse(&mock_backend(addr), "/nope", Bytes::new())
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("returned 404") || err.to_string().contains("returned 405"),
        "got: {err}"
    );
    server.abort();
}
