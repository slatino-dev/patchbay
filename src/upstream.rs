//! Upstream backend pool + HTTP client management.
//!
//! TODO(phase-B):
//!   - `UpstreamPool` holding one `reqwest::Client` per backend
//!   - proxy OpenAI-compatible `/v1/chat/completions` requests (non-streaming + SSE streaming)
//!   - SSE passthrough: stream backend response chunks directly to the caller
//!   - track per-backend latency (EWMA) for the router
//!   - credential injection from env vars (`OPENAI_API_KEY`, `OPENAI_BASE_URL`)
//!   - health probes (optional, lightweight `/models` ping)

// Placeholder — no logic yet.
