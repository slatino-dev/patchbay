//! Upstream HTTP client + byte-faithful SSE relay.
//!
//! The relay's contract:
//!
//! - **Byte-faithful.** Every chunk received from the upstream is forwarded
//!   to the client *verbatim* — the same `Bytes` value, no re-serialization,
//!   no re-framing, no normalization. A side-channel scanner inspects a copy
//!   of the byte stream to extract the final `usage` accounting object, but
//!   it can never alter what the client sees.
//! - **Usage interception.** OpenAI-compatible streams (with
//!   `stream_options.include_usage`) carry token usage in the last data
//!   event before `data: [DONE]`. The scanner parses only events whose
//!   payload mentions `"usage"` and remembers the last non-null value; it is
//!   reported in the [`RelaySummary`] when the stream ends, for budget and
//!   metrics accounting.
//! - **Client disconnects propagate.** The relay is pull-based: when the
//!   client goes away, the server drops the [`SseRelay`] stream, which drops
//!   the underlying `reqwest` response, which cancels the upstream request.
//!   The summary channel still fires (with `ClientDisconnected`) so partial
//!   usage can be accounted.
//! - **Stalls are bounded.** If the upstream sends nothing for
//!   `stall_timeout`, the relay errors out instead of holding the client
//!   connection open forever.

use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use futures::{Future, Stream, StreamExt};
use serde::Deserialize;
use tokio::sync::oneshot;
use tokio::time::{sleep, Instant, Sleep};

use crate::config::Backend;

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;
type ByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, BoxError>> + Send + 'static>>;

/// Token usage as reported by OpenAI-compatible streaming responses.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Usage {
    #[serde(default)]
    pub prompt_tokens: u64,
    #[serde(default)]
    pub completion_tokens: u64,
    #[serde(default)]
    pub total_tokens: u64,
}

#[derive(Debug, Deserialize)]
struct UsageEnvelope {
    usage: Option<Usage>,
}

/// How a relayed stream ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelayOutcome {
    /// Upstream closed the stream normally.
    Completed,
    /// Upstream errored mid-stream (connection reset, decode error, ...).
    UpstreamError(String),
    /// Upstream sent nothing for the configured stall timeout.
    Stalled,
    /// The client stopped consuming before the upstream finished.
    ClientDisconnected,
}

/// End-of-stream accounting record, delivered on the summary channel
/// returned by [`SseRelay::new`]. Exactly one summary is sent per relay,
/// however the stream ends.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelaySummary {
    pub outcome: RelayOutcome,
    /// Last usage object seen in the stream, if any.
    pub usage: Option<Usage>,
    /// Whether a `data: [DONE]` terminator was observed.
    pub saw_done: bool,
    /// Total bytes forwarded to the client.
    pub bytes_relayed: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum RelayError {
    #[error("upstream stream error: {0}")]
    Upstream(String),
    #[error("upstream stalled: no bytes received for {0:?}")]
    StallTimeout(Duration),
}

// ---------------------------------------------------------------------------
// Usage scanner — incremental SSE event-boundary parser over a byte copy.
// ---------------------------------------------------------------------------

/// Upper bound on bytes buffered while waiting for an event boundary.
/// OpenAI-style chunks are a few KB; a single event larger than this is
/// pathological. On overflow the scanner stops (accounting degrades to
/// "no usage observed") but the relay itself keeps streaming verbatim.
const MAX_EVENT_BUFFER: usize = 1024 * 1024;

#[derive(Debug, Default)]
struct UsageScanner {
    buf: Vec<u8>,
    last_usage: Option<Usage>,
    saw_done: bool,
    overflowed: bool,
}

/// What sits at `buf[i]` in terms of SSE line terminators.
enum Term {
    /// Not a line terminator.
    Not,
    /// A complete terminator of this byte length (`\n`, `\r\n`, or lone `\r`).
    Len(usize),
    /// A `\r` at the end of the buffer — might be half of `\r\n`.
    Incomplete,
}

fn term_at(buf: &[u8], i: usize) -> Term {
    match buf.get(i) {
        Some(b'\n') => Term::Len(1),
        Some(b'\r') => match buf.get(i + 1) {
            Some(b'\n') => Term::Len(2),
            Some(_) => Term::Len(1),
            None => Term::Incomplete,
        },
        _ => Term::Not,
    }
}

/// Find the first complete SSE event: returns `(event_end, consume_to)`
/// where `event_end` is the exclusive end of the event body and
/// `consume_to` is the index just past the blank-line separator.
fn find_event_boundary(buf: &[u8]) -> Option<(usize, usize)> {
    let mut i = 0;
    while i < buf.len() {
        match term_at(buf, i) {
            Term::Not => i += 1,
            Term::Incomplete => return None,
            Term::Len(l1) => {
                let j = i + l1;
                match term_at(buf, j) {
                    Term::Not => i = j,
                    Term::Incomplete => return None,
                    Term::Len(l2) => return Some((i, j + l2)),
                }
            }
        }
    }
    None
}

impl UsageScanner {
    fn feed(&mut self, chunk: &[u8]) {
        if self.overflowed {
            return;
        }
        self.buf.extend_from_slice(chunk);
        while let Some((event_end, consume_to)) = find_event_boundary(&self.buf) {
            // Copy the event out, then drop it (and its separator) from the
            // front of the buffer before parsing.
            let event: Vec<u8> = self.buf[..event_end].to_vec();
            self.buf.drain(..consume_to);
            self.process_event(&event);
        }
        if self.buf.len() > MAX_EVENT_BUFFER {
            tracing::warn!(
                buffered = self.buf.len(),
                "SSE usage scanner: event exceeds buffer cap; accounting disabled for this stream"
            );
            self.overflowed = true;
            self.buf = Vec::new();
        }
    }

    fn process_event(&mut self, event: &[u8]) {
        // Join the payloads of all `data:` lines with '\n', per the SSE spec.
        let mut data: Vec<u8> = Vec::new();
        let mut saw_data_line = false;
        let mut i = 0;
        let mut line_start = 0;
        loop {
            match term_at(event, i) {
                Term::Len(l) => {
                    self.collect_data_line(&event[line_start..i], &mut data, &mut saw_data_line);
                    i += l;
                    line_start = i;
                }
                Term::Incomplete | Term::Not => {
                    if i >= event.len() {
                        self.collect_data_line(&event[line_start..], &mut data, &mut saw_data_line);
                        break;
                    }
                    i += 1;
                }
            }
        }
        if !saw_data_line {
            return; // comment or non-data event
        }
        if data == b"[DONE]" {
            self.saw_done = true;
            return;
        }
        // Cheap pre-filter: only events that mention "usage" are parsed, so
        // ordinary token-delta chunks don't pay JSON-parsing costs (chunks
        // with `"usage":null` parse to None and are ignored).
        if data.windows(7).any(|w| w == b"\"usage\"") {
            if let Ok(UsageEnvelope { usage: Some(u) }) = serde_json::from_slice(&data) {
                self.last_usage = Some(u);
            }
        }
    }

    fn collect_data_line(&self, line: &[u8], data: &mut Vec<u8>, saw_data_line: &mut bool) {
        let payload = if let Some(rest) = line.strip_prefix(b"data:") {
            rest.strip_prefix(b" ").unwrap_or(rest)
        } else if line == b"data" {
            b"".as_slice()
        } else {
            return;
        };
        if *saw_data_line {
            data.push(b'\n');
        }
        data.extend_from_slice(payload);
        *saw_data_line = true;
    }
}

// ---------------------------------------------------------------------------
// The relay stream
// ---------------------------------------------------------------------------

/// A byte-faithful relay over an upstream byte stream. Yields exactly the
/// chunks the upstream produced; reports a [`RelaySummary`] on the channel
/// returned by [`SseRelay::new`] when the stream ends for any reason.
pub struct SseRelay {
    upstream: ByteStream,
    scanner: UsageScanner,
    stall: Pin<Box<Sleep>>,
    stall_timeout: Duration,
    reporter: Option<oneshot::Sender<RelaySummary>>,
    bytes_relayed: u64,
    finished: bool,
}

impl SseRelay {
    /// Wrap an upstream byte stream. `stall_timeout` bounds the gap between
    /// upstream chunks (including time to the first chunk).
    pub fn new<S, E>(
        upstream: S,
        stall_timeout: Duration,
    ) -> (Self, oneshot::Receiver<RelaySummary>)
    where
        S: Stream<Item = Result<Bytes, E>> + Send + 'static,
        E: std::error::Error + Send + Sync + 'static,
    {
        let (tx, rx) = oneshot::channel();
        let relay = Self {
            upstream: Box::pin(upstream.map(|r| r.map_err(|e| Box::new(e) as BoxError))),
            scanner: UsageScanner::default(),
            stall: Box::pin(sleep(stall_timeout)),
            stall_timeout,
            reporter: Some(tx),
            bytes_relayed: 0,
            finished: false,
        };
        (relay, rx)
    }

    /// Convenience constructor over a `reqwest` streaming response.
    pub fn from_response(
        response: reqwest::Response,
        stall_timeout: Duration,
    ) -> (Self, oneshot::Receiver<RelaySummary>) {
        Self::new(response.bytes_stream(), stall_timeout)
    }

    fn report(&mut self, outcome: RelayOutcome) {
        if let Some(tx) = self.reporter.take() {
            let _ = tx.send(RelaySummary {
                outcome,
                usage: self.scanner.last_usage.clone(),
                saw_done: self.scanner.saw_done,
                bytes_relayed: self.bytes_relayed,
            });
        }
    }
}

impl Stream for SseRelay {
    type Item = Result<Bytes, RelayError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.finished {
            return Poll::Ready(None);
        }
        match this.upstream.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(chunk))) => {
                this.scanner.feed(&chunk);
                this.bytes_relayed += chunk.len() as u64;
                let deadline = Instant::now() + this.stall_timeout;
                this.stall.as_mut().reset(deadline);
                // The exact chunk the upstream produced — never re-serialized.
                Poll::Ready(Some(Ok(chunk)))
            }
            Poll::Ready(Some(Err(e))) => {
                this.finished = true;
                let msg = e.to_string();
                this.report(RelayOutcome::UpstreamError(msg.clone()));
                Poll::Ready(Some(Err(RelayError::Upstream(msg))))
            }
            Poll::Ready(None) => {
                this.finished = true;
                this.report(RelayOutcome::Completed);
                Poll::Ready(None)
            }
            Poll::Pending => match this.stall.as_mut().poll(cx) {
                Poll::Ready(()) => {
                    this.finished = true;
                    this.report(RelayOutcome::Stalled);
                    Poll::Ready(Some(Err(RelayError::StallTimeout(this.stall_timeout))))
                }
                Poll::Pending => Poll::Pending,
            },
        }
    }
}

impl Drop for SseRelay {
    fn drop(&mut self) {
        // If no terminal outcome was reported, the stream was dropped while
        // still live — the client went away. Dropping `upstream` here is
        // what cancels the in-flight reqwest request.
        self.report(RelayOutcome::ClientDisconnected);
    }
}

// ---------------------------------------------------------------------------
// Upstream HTTP client
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum UpstreamError {
    #[error("http client error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("upstream `{backend}` returned {status}: {body}")]
    BadStatus {
        backend: String,
        status: u16,
        body: String,
    },
}

/// Thin client for OpenAI-compatible upstreams. One instance is shared
/// across all backends (reqwest pools per-host internally).
#[derive(Debug, Clone)]
pub struct UpstreamClient {
    http: reqwest::Client,
}

impl UpstreamClient {
    pub fn new() -> Result<Self, UpstreamError> {
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            // No total-request timeout: streams legitimately run for minutes.
            // Stall detection is the relay's job.
            .build()?;
        Ok(Self { http })
    }

    /// POST `body` (the client's payload, forwarded verbatim) to
    /// `{base_url}{path}` with the backend's credentials, returning the
    /// streaming response. Non-2xx responses are read (truncated) and
    /// surfaced as [`UpstreamError::BadStatus`].
    pub async fn open_sse(
        &self,
        backend: &Backend,
        path: &str,
        body: Bytes,
    ) -> Result<reqwest::Response, UpstreamError> {
        let url = format!("{}{}", backend.base_url, path);
        let mut req = self
            .http
            .post(url)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .header(reqwest::header::ACCEPT, "text/event-stream")
            .body(body);
        if let Some(key) = &backend.api_key {
            req = req.bearer_auth(key.expose());
        }
        let resp = req.send().await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            let body = body.chars().take(2048).collect();
            return Err(UpstreamError::BadStatus {
                backend: backend.name.clone(),
                status: status.as_u16(),
                body,
            });
        }
        Ok(resp)
    }
}

#[cfg(test)]
mod scanner_tests {
    use super::*;

    fn feed_chunks(chunks: &[&[u8]]) -> UsageScanner {
        let mut s = UsageScanner::default();
        for c in chunks {
            s.feed(c);
        }
        s
    }

    #[test]
    fn extracts_usage_from_final_chunk() {
        let s = feed_chunks(&[
            b"data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}],\"usage\":null}\n\n",
            b"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":7,\"completion_tokens\":42,\"total_tokens\":49}}\n\n",
            b"data: [DONE]\n\n",
        ]);
        assert_eq!(
            s.last_usage,
            Some(Usage {
                prompt_tokens: 7,
                completion_tokens: 42,
                total_tokens: 49
            })
        );
        assert!(s.saw_done);
    }

    #[test]
    fn events_split_at_arbitrary_chunk_boundaries() {
        let s = feed_chunks(&[
            b"data: {\"usage\":{\"prompt_to",
            b"kens\":1,\"completion_tokens\":2,\"tot",
            b"al_tokens\":3}}\n",
            b"\ndata: [D",
            b"ONE]\n\n",
        ]);
        assert_eq!(
            s.last_usage,
            Some(Usage {
                prompt_tokens: 1,
                completion_tokens: 2,
                total_tokens: 3
            })
        );
        assert!(s.saw_done);
    }

    #[test]
    fn crlf_separators_are_understood() {
        let s = feed_chunks(&[
            b"data: {\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":6,\"total_tokens\":11}}\r\n\r\n",
            b"data: [DONE]\r\n\r\n",
        ]);
        assert_eq!(s.last_usage.as_ref().map(|u| u.total_tokens), Some(11));
        assert!(s.saw_done);
    }

    #[test]
    fn cr_split_across_chunks_is_not_a_false_boundary() {
        // "\r\n\r\n" arriving as "...\r" + "\n\r\n..." must still be one separator.
        let s = feed_chunks(&[
            b"data: {\"usage\":{\"total_tokens\":9}}\r",
            b"\n\r\ndata: [DONE]\r\n\r\n",
        ]);
        assert_eq!(s.last_usage.as_ref().map(|u| u.total_tokens), Some(9));
        assert!(s.saw_done);
    }

    #[test]
    fn multiple_data_lines_join_with_newline() {
        // JSON split over two data lines in one event is still one payload.
        let s = feed_chunks(&[b"data: {\"usage\":{\"total_tokens\":\ndata: 4}}\n\n"]);
        // Joined payload contains a '\n' inside the JSON, which is valid
        // whitespace — usage still parses.
        assert_eq!(s.last_usage.as_ref().map(|u| u.total_tokens), Some(4));
    }

    #[test]
    fn non_json_and_comment_events_are_ignored() {
        let s = feed_chunks(&[
            b": keep-alive\n\n",
            b"event: ping\n\n",
            b"data: not json at all\n\n",
            b"data: {\"usage\":{\"total_tokens\":2}}\n\n",
        ]);
        assert_eq!(s.last_usage.as_ref().map(|u| u.total_tokens), Some(2));
    }

    #[test]
    fn usage_null_is_not_usage() {
        let s = feed_chunks(&[b"data: {\"choices\":[],\"usage\":null}\n\n"]);
        assert_eq!(s.last_usage, None);
    }

    #[test]
    fn last_usage_wins() {
        let s = feed_chunks(&[
            b"data: {\"usage\":{\"total_tokens\":1}}\n\n",
            b"data: {\"usage\":{\"total_tokens\":2}}\n\n",
        ]);
        assert_eq!(s.last_usage.as_ref().map(|u| u.total_tokens), Some(2));
    }

    #[test]
    fn oversized_event_disables_scanning_without_panic() {
        let mut s = UsageScanner::default();
        // A "data:" line that never terminates.
        s.feed(b"data: ");
        let big = vec![b'x'; MAX_EVENT_BUFFER + 1];
        s.feed(&big);
        assert!(s.overflowed);
        // Further feeds are no-ops, not panics.
        s.feed(b"data: {\"usage\":{\"total_tokens\":3}}\n\n");
        assert_eq!(s.last_usage, None);
    }
}
