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
//!
//! The remaining modules ([`auth`], [`budget`], [`limits`], [`metrics`],
//! [`server`]) are scaffolding for the next phase (HTTP endpoint assembly).

pub mod auth;
pub mod budget;
pub mod config;
pub mod limits;
pub mod metrics;
pub mod router;
pub mod server;
pub mod upstream;
