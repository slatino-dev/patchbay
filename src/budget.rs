//! Per-key token and cost budget enforcement.
//!
//! TODO(phase-B):
//!   - budget state: AtomicU64 token counters per key, reset on configurable cadence
//!   - pre-request check: reject with 429 if key would exceed budget
//!   - post-response accounting: subtract actual prompt+completion tokens from budget
//!   - persist budget snapshots to disk (SQLite or flat file) for restart survival
//!   - budget config: `[budgets.<key_id>]` section in patchbay.toml

// Placeholder — no logic yet.
