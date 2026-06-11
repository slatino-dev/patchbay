//! Tag-based privacy routing and latency-aware backend selection.
//!
//! TODO(phase-B):
//!   - parse `x-patchbay-tags` request header (e.g. "private,pii")
//!   - enforce private → local-only routing (reject if no local backend available)
//!   - latency-aware backend selection: EWMA latency per upstream, pick lowest
//!   - build fallback chains from config (ordered list of upstream ids)
//!   - expose `select_backend(tags, model) -> Vec<UpstreamId>` (ordered by priority)

// Placeholder — no logic yet.
