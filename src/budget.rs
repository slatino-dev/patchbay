//! Per-key token-budget accounting.
//!
//! Each virtual key can be assigned a maximum number of prompt tokens,
//! completion tokens, and/or total tokens per accounting period (currently
//! fixed at one hour, reset on startup at the top of the next hour). All
//! counters live in memory as [`std::sync::atomic::AtomicU64`] values so
//! post-request accounting from any async context is contention-free.
//!
//! # Persistence
//!
//! The ledger is snapshotted to a JSON file at a configurable path whenever
//! [`BudgetLedger::flush`] is called. The server calls this periodically (the
//! snapshot interval is tunable) and on graceful shutdown. On startup the
//! snapshot is re-loaded so budgets survive restarts within the same
//! accounting period.
//!
//! # No database
//!
//! Storage is flat JSON (`budget_snapshot.json` next to the config by
//! default). The file is written atomically (write to a `.tmp` file, then
//! rename) to avoid partial-write corruption.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::upstream::Usage;

/// Budget limit for a single key, in tokens. `None` means unlimited.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct BudgetLimit {
    /// Maximum prompt tokens per accounting period.
    pub max_prompt_tokens: Option<u64>,
    /// Maximum completion tokens per accounting period.
    pub max_completion_tokens: Option<u64>,
    /// Maximum total tokens per accounting period.
    pub max_total_tokens: Option<u64>,
}

impl BudgetLimit {
    /// A limit that allows everything — the default for unconfigured keys.
    pub const fn unlimited() -> Self {
        Self {
            max_prompt_tokens: None,
            max_completion_tokens: None,
            max_total_tokens: None,
        }
    }
}

/// Live counters for one key. All fields are atomics so they can be updated
/// from the response path without acquiring a lock.
#[derive(Debug, Default)]
struct KeyCounters {
    prompt_tokens: AtomicU64,
    completion_tokens: AtomicU64,
    total_tokens: AtomicU64,
}

impl KeyCounters {
    fn add_usage(&self, usage: &Usage) {
        self.prompt_tokens
            .fetch_add(usage.prompt_tokens, Ordering::Relaxed);
        self.completion_tokens
            .fetch_add(usage.completion_tokens, Ordering::Relaxed);
        self.total_tokens
            .fetch_add(usage.total_tokens, Ordering::Relaxed);
    }

    fn snapshot(&self) -> UsageSnapshot {
        UsageSnapshot {
            prompt_tokens: self.prompt_tokens.load(Ordering::Relaxed),
            completion_tokens: self.completion_tokens.load(Ordering::Relaxed),
            total_tokens: self.total_tokens.load(Ordering::Relaxed),
        }
    }

    fn restore(&self, snap: &UsageSnapshot) {
        self.prompt_tokens
            .store(snap.prompt_tokens, Ordering::Relaxed);
        self.completion_tokens
            .store(snap.completion_tokens, Ordering::Relaxed);
        self.total_tokens
            .store(snap.total_tokens, Ordering::Relaxed);
    }

    /// Check whether the given counters plus `proposed_usage` would exceed
    /// `limit`. Returns the first exceeded dimension, for error messages.
    fn would_exceed(&self, limit: &BudgetLimit, proposed: &Usage) -> Option<BudgetExceeded> {
        if let Some(max) = limit.max_prompt_tokens {
            let current = self.prompt_tokens.load(Ordering::Relaxed);
            if current + proposed.prompt_tokens > max {
                return Some(BudgetExceeded::PromptTokens {
                    used: current,
                    limit: max,
                });
            }
        }
        if let Some(max) = limit.max_completion_tokens {
            let current = self.completion_tokens.load(Ordering::Relaxed);
            if current + proposed.completion_tokens > max {
                return Some(BudgetExceeded::CompletionTokens {
                    used: current,
                    limit: max,
                });
            }
        }
        if let Some(max) = limit.max_total_tokens {
            let current = self.total_tokens.load(Ordering::Relaxed);
            if current + proposed.total_tokens > max {
                return Some(BudgetExceeded::TotalTokens {
                    used: current,
                    limit: max,
                });
            }
        }
        None
    }
}

/// Which dimension was exceeded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BudgetExceeded {
    PromptTokens { used: u64, limit: u64 },
    CompletionTokens { used: u64, limit: u64 },
    TotalTokens { used: u64, limit: u64 },
}

impl std::fmt::Display for BudgetExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PromptTokens { used, limit } => {
                write!(f, "prompt token budget exceeded ({used}/{limit})")
            }
            Self::CompletionTokens { used, limit } => {
                write!(f, "completion token budget exceeded ({used}/{limit})")
            }
            Self::TotalTokens { used, limit } => {
                write!(f, "total token budget exceeded ({used}/{limit})")
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Persistence types
// ---------------------------------------------------------------------------

/// Token totals for one key, stored in the snapshot file.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct UsageSnapshot {
    prompt_tokens: u64,
    completion_tokens: u64,
    total_tokens: u64,
}

/// Full on-disk snapshot: one entry per key + the period start timestamp so
/// stale periods are discarded on load.
#[derive(Debug, Serialize, Deserialize)]
struct LedgerSnapshot {
    /// Unix epoch seconds of the start of the current accounting period.
    period_start: u64,
    /// key name -> usage in this period
    entries: HashMap<String, UsageSnapshot>,
}

fn period_start_secs(period: Duration) -> u64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    // Truncate to the nearest period boundary.
    let secs = period.as_secs().max(1);
    (now / secs) * secs
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Holds live per-key counters, limits, and the snapshot file path.
///
/// Clone-cheap: the counters live behind an `Arc`.
#[derive(Debug, Clone)]
pub struct BudgetLedger {
    inner: Arc<LedgerInner>,
}

#[derive(Debug)]
struct LedgerInner {
    /// When this period started (epoch seconds).
    period_start: u64,
    /// key name -> counters
    counters: RwLock<HashMap<String, Arc<KeyCounters>>>,
    /// key name -> configured limit
    limits: HashMap<String, BudgetLimit>,
    /// Where to write snapshots; `None` disables persistence.
    snapshot_path: Option<PathBuf>,
}

impl BudgetLedger {
    /// Construct with the given accounting period and per-key limits. If
    /// `snapshot_path` is provided, a previous snapshot will be loaded (if
    /// the period matches) and future flushes will persist to that path.
    pub fn new(
        limits: HashMap<String, BudgetLimit>,
        period: Duration,
        snapshot_path: Option<PathBuf>,
    ) -> Self {
        let period_start = period_start_secs(period);
        let mut counters: HashMap<String, Arc<KeyCounters>> = HashMap::new();

        // Try to restore from the snapshot file.
        if let Some(ref path) = snapshot_path {
            if let Ok(data) = std::fs::read_to_string(path) {
                if let Ok(snap) = serde_json::from_str::<LedgerSnapshot>(&data) {
                    // Only restore if the snapshot is from the current period.
                    if snap.period_start == period_start {
                        for (key, usage) in &snap.entries {
                            let c = Arc::new(KeyCounters::default());
                            c.restore(usage);
                            counters.insert(key.clone(), c);
                        }
                        tracing::info!(
                            keys = counters.len(),
                            period_start,
                            "budget ledger: restored from snapshot"
                        );
                    } else {
                        tracing::info!(
                            "budget ledger: snapshot is from a previous period; starting fresh"
                        );
                    }
                }
            }
        }

        Self {
            inner: Arc::new(LedgerInner {
                period_start,
                counters: RwLock::new(counters),
                limits,
                snapshot_path,
            }),
        }
    }

    fn counters_for(&self, key: &str) -> Arc<KeyCounters> {
        // Fast path: already exists.
        {
            let r = self
                .inner
                .counters
                .read()
                .unwrap_or_else(|e| e.into_inner());
            if let Some(c) = r.get(key) {
                return c.clone();
            }
        }
        // Slow path: first request for this key.
        let mut w = self
            .inner
            .counters
            .write()
            .unwrap_or_else(|e| e.into_inner());
        w.entry(key.to_string())
            .or_insert_with(|| Arc::new(KeyCounters::default()))
            .clone()
    }

    /// Look up the limit configured for `key`, falling back to unlimited.
    pub fn limit_for(&self, key: &str) -> BudgetLimit {
        self.inner
            .limits
            .get(key)
            .copied()
            .unwrap_or(BudgetLimit::unlimited())
    }

    /// Check whether a request from `key` with the given *expected* usage
    /// would exceed its budget. Returns `Ok(())` if it would not, or
    /// `Err(BudgetExceeded)` describing the first dimension that would be
    /// exceeded.
    ///
    /// Called *before* forwarding the request so that prompt-token-heavy
    /// requests can be rejected early. Because the check is not atomic with
    /// the accounting step, concurrent requests can temporarily exceed the
    /// limit by a small margin — this is acceptable for a best-effort budget.
    pub fn check(&self, key: &str, proposed: &Usage) -> Result<(), BudgetExceeded> {
        let limit = self.limit_for(key);
        let counters = self.counters_for(key);
        match counters.would_exceed(&limit, proposed) {
            Some(exceeded) => Err(exceeded),
            None => Ok(()),
        }
    }

    /// Record the actual usage of a completed (or interrupted) request from
    /// `key`. This is called after the upstream response has been consumed.
    pub fn record(&self, key: &str, usage: &Usage) {
        self.counters_for(key).add_usage(usage);
    }

    /// Current token counters for `key` (for metrics/reporting).
    pub fn usage_for(&self, key: &str) -> Option<(u64, u64, u64)> {
        let r = self
            .inner
            .counters
            .read()
            .unwrap_or_else(|e| e.into_inner());
        r.get(key).map(|c| {
            (
                c.prompt_tokens.load(Ordering::Relaxed),
                c.completion_tokens.load(Ordering::Relaxed),
                c.total_tokens.load(Ordering::Relaxed),
            )
        })
    }

    /// Write current counters to the snapshot file atomically.
    /// A `.tmp` file is written first, then renamed, to avoid corruption.
    pub fn flush(&self) -> std::io::Result<()> {
        let path = match &self.inner.snapshot_path {
            Some(p) => p,
            None => return Ok(()),
        };
        let entries = {
            let r = self
                .inner
                .counters
                .read()
                .unwrap_or_else(|e| e.into_inner());
            r.iter()
                .map(|(k, c)| (k.clone(), c.snapshot()))
                .collect::<HashMap<_, _>>()
        };
        let snap = LedgerSnapshot {
            period_start: self.inner.period_start,
            entries,
        };
        let json = serde_json::to_string_pretty(&snap).map_err(std::io::Error::other)?;

        let tmp_path = path.with_extension("tmp");
        std::fs::write(&tmp_path, &json)?;
        std::fs::rename(&tmp_path, path)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn usage(prompt: u64, completion: u64, total: u64) -> Usage {
        Usage {
            prompt_tokens: prompt,
            completion_tokens: completion,
            total_tokens: total,
        }
    }

    fn ledger_with_limit(key: &str, limit: BudgetLimit) -> BudgetLedger {
        let mut limits = HashMap::new();
        limits.insert(key.to_string(), limit);
        BudgetLedger::new(limits, Duration::from_secs(3600), None)
    }

    #[test]
    fn unconfigured_key_is_unlimited() {
        let ledger = BudgetLedger::new(HashMap::new(), Duration::from_secs(3600), None);
        let u = usage(1_000_000, 1_000_000, 2_000_000);
        assert!(ledger.check("any-key", &u).is_ok());
    }

    #[test]
    fn check_fails_when_total_exceeded() {
        let limit = BudgetLimit {
            max_total_tokens: Some(100),
            ..BudgetLimit::unlimited()
        };
        let ledger = ledger_with_limit("k", limit);
        ledger.record("k", &usage(50, 50, 100));
        // Now at 100/100; adding 1 more total should fail.
        let err = ledger.check("k", &usage(0, 0, 1)).unwrap_err();
        assert!(matches!(err, BudgetExceeded::TotalTokens { .. }));
    }

    #[test]
    fn check_passes_when_under_limit() {
        let limit = BudgetLimit {
            max_total_tokens: Some(200),
            ..BudgetLimit::unlimited()
        };
        let ledger = ledger_with_limit("k", limit);
        ledger.record("k", &usage(50, 50, 100));
        assert!(ledger.check("k", &usage(10, 10, 20)).is_ok());
    }

    #[test]
    fn prompt_token_limit_is_checked() {
        let limit = BudgetLimit {
            max_prompt_tokens: Some(50),
            ..BudgetLimit::unlimited()
        };
        let ledger = ledger_with_limit("k", limit);
        ledger.record("k", &usage(40, 0, 40));
        let err = ledger.check("k", &usage(20, 0, 20)).unwrap_err();
        assert!(matches!(err, BudgetExceeded::PromptTokens { .. }));
    }

    #[test]
    fn completion_token_limit_is_checked() {
        let limit = BudgetLimit {
            max_completion_tokens: Some(30),
            ..BudgetLimit::unlimited()
        };
        let ledger = ledger_with_limit("k", limit);
        ledger.record("k", &usage(0, 25, 25));
        let err = ledger.check("k", &usage(0, 10, 10)).unwrap_err();
        assert!(matches!(err, BudgetExceeded::CompletionTokens { .. }));
    }

    #[test]
    fn record_accumulates_usage() {
        let ledger = BudgetLedger::new(HashMap::new(), Duration::from_secs(3600), None);
        ledger.record("k", &usage(10, 20, 30));
        ledger.record("k", &usage(5, 5, 10));
        let (p, c, t) = ledger.usage_for("k").unwrap();
        assert_eq!(p, 15);
        assert_eq!(c, 25);
        assert_eq!(t, 40);
    }

    #[test]
    fn flush_and_restore() {
        let dir = tempdir();
        let snap_path = dir.join("budget.json");

        let mut limits = HashMap::new();
        limits.insert(
            "alice".to_string(),
            BudgetLimit {
                max_total_tokens: Some(1000),
                ..BudgetLimit::unlimited()
            },
        );

        // Write some usage.
        let ledger = BudgetLedger::new(
            limits.clone(),
            Duration::from_secs(3600),
            Some(snap_path.clone()),
        );
        ledger.record("alice", &usage(100, 200, 300));
        ledger.flush().expect("flush failed");

        // Restore from snapshot.
        let ledger2 = BudgetLedger::new(limits, Duration::from_secs(3600), Some(snap_path));
        let (p, c, t) = ledger2.usage_for("alice").unwrap();
        assert_eq!((p, c, t), (100, 200, 300));
    }

    /// Minimal temp dir for tests (no external crates).
    fn tempdir() -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "patchbay_budget_test_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .subsec_nanos()
        ));
        std::fs::create_dir_all(&path).unwrap();
        path
    }
}
