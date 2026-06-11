//! HTTP server assembly — axum router, middleware stack, graceful shutdown.
//!
//! TODO(phase-B):
//!   - assemble full axum Router with all routes:
//!     POST /v1/chat/completions (OpenAI-compatible),
//!     POST /v1/completions, GET /v1/models, GET /metrics
//!   - layer auth middleware
//!   - layer rate-limit middleware
//!   - layer request-id tracing span
//!   - graceful shutdown on SIGINT/SIGTERM (tokio signal)

// Placeholder — no logic yet.
