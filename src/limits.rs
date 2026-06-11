//! Rate limiting via `governor` (token-bucket / GCRA).
//!
//! TODO(phase-B):
//!   - global rate limiter (requests/second across all keys)
//!   - per-key rate limiter keyed by API key identity
//!   - axum middleware that checks limits before routing
//!   - return `429 Too Many Requests` with `Retry-After` header on exceed
//!   - tier-based limits (free, standard, unlimited) from config

// Placeholder — no logic yet.
