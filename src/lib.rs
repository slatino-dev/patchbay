//! patchbay — an OpenAI-compatible LLM gateway/router.
//!
//! Core pieces (this crate is consumed by the `patchbay` binary and by the
//! integration tests / benches):
//!
//! - [`config`] — TOML config with env-resolved secrets, validated on load.
//! - [`router`] — policy-driven backend selection with **type-enforced
//!   privacy routing**: a request classified as private cannot reach an
//!   `External` backend, by construction.
//! - [`upstream`] — byte-faithful SSE relay over a streaming HTTP client,
//!   with usage interception for accounting, stall detection, and client
//!   disconnect propagation.
//! - [`auth`] — virtual API-key lookup and axum extractor.
//! - [`budget`] — per-key token-budget accounting with periodic JSON
//!   snapshots for restart survival.
//! - [`limits`] — per-key RPM/TPM enforcement via `governor` (GCRA).
//! - [`metrics`] — Prometheus text-format counters exposed on `GET /metrics`.
//! - [`server`] — axum routes: `POST /v1/chat/completions` (stream + non-stream
//!   proxy with jittered backoff + fallback), `GET /v1/models`, `GET /healthz`,
//!   `GET /metrics`.

pub mod auth;
pub mod budget;
pub mod config;
pub mod limits;
pub mod metrics;
pub mod router;
pub mod server;
pub mod upstream;
