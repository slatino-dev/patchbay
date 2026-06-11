//! Per-key rate limiting via `governor` (GCRA / token-bucket).
//!
//! Each virtual key is independently limited by requests per minute (RPM) and
//! tokens per minute (TPM). Limits are enforced at the point of request
//! admission: the handler calls [`RateLimiter::check_rpm`] before forwarding,
//! and [`RateLimiter::record_tpm`] after accounting usage.
//!
//! # Implementation notes
//!
//! - RPM is a pure rate limit: each request costs one token; the bucket
//!   refills at `rpm/60` tokens per second (continuous GCRA).
//! - TPM is a count tracked over a rolling 60-second window using a simple
//!   [`AtomicU64`] sliding counter. This is approximate (it measures tokens
//!   in the last reset interval, not a strict 60-second window) but avoids
//!   per-request allocations and is sufficient for gateway-level metering.
//! - Both limits are optional. Unconfigured keys are unlimited.

use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use governor::clock::DefaultClock;
use governor::middleware::NoOpMiddleware;
use governor::state::keyed::DefaultKeyedStateStore;
use governor::{Quota, RateLimiter as GovernorLimiter};

/// Configuration for one key's rate limits.
#[derive(Debug, Clone, Copy, Default)]
pub struct RateLimit {
    /// Maximum requests per minute. `None` = unlimited.
    pub max_rpm: Option<u32>,
    /// Maximum tokens per minute. `None` = unlimited.
    pub max_tpm: Option<u64>,
}

impl RateLimit {
    pub const fn unlimited() -> Self {
        Self {
            max_rpm: None,
            max_tpm: None,
        }
    }
}

/// Reason a rate-limit check was rejected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LimitExceeded {
    /// Requests-per-minute budget exhausted.
    Rpm { limit: u32 },
    /// Tokens-per-minute budget exhausted.
    Tpm { used: u64, limit: u64 },
}

impl std::fmt::Display for LimitExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rpm { limit } => write!(f, "rate limit exceeded ({limit} rpm)"),
            Self::Tpm { used, limit } => {
                write!(f, "token rate limit exceeded ({used}/{limit} tpm)")
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Internal per-key state
// ---------------------------------------------------------------------------

type GovernorKey = String;
type RpmLimiter =
    GovernorLimiter<GovernorKey, DefaultKeyedStateStore<GovernorKey>, DefaultClock, NoOpMiddleware>;

/// Rolling token-per-minute counter: accumulated tokens plus the wall-clock
/// second at which the current window started.
#[derive(Debug, Default)]
struct TpmCounter {
    window_start: AtomicU64,
    tokens: AtomicU64,
}

impl TpmCounter {
    /// Add `tokens` to the current window, resetting if the window has
    /// elapsed. Returns the new total for this window.
    fn add(&self, tokens: u64, now_secs: u64) -> u64 {
        let ws = self.window_start.load(Ordering::Relaxed);
        if now_secs.saturating_sub(ws) >= 60 {
            // New window — reset.
            self.window_start.store(now_secs, Ordering::Relaxed);
            self.tokens.store(tokens, Ordering::Relaxed);
            tokens
        } else {
            self.tokens.fetch_add(tokens, Ordering::Relaxed) + tokens
        }
    }

    fn current(&self, now_secs: u64) -> u64 {
        let ws = self.window_start.load(Ordering::Relaxed);
        if now_secs.saturating_sub(ws) >= 60 {
            0
        } else {
            self.tokens.load(Ordering::Relaxed)
        }
    }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Holds all per-key rate limiters. Clone-cheap (inner `Arc`).
#[derive(Clone)]
pub struct RateLimitStore {
    inner: Arc<RateLimitInner>,
}

struct RateLimitInner {
    /// key name -> configured limits
    limits: HashMap<String, RateLimit>,
    /// key name -> dedicated RPM governor (one per key that has an RPM limit,
    /// sized exactly to that key's configured quota so there is no cross-key
    /// bleed from a shared-maximum governor).
    rpm_limiters: HashMap<String, Arc<RpmLimiter>>,
    /// key name -> TPM rolling counter
    tpm_counters: RwLock<HashMap<String, Arc<TpmCounter>>>,
}

impl std::fmt::Debug for RateLimitStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RateLimitStore")
            .field("keys", &self.inner.limits.len())
            .finish()
    }
}

impl RateLimitStore {
    /// Build from a per-key limits table.
    ///
    /// Each key that has an RPM limit gets its own governor instance sized
    /// exactly to that key's quota. This ensures a key configured at 10 RPM
    /// cannot consume more than 10 requests per minute, regardless of other
    /// keys' limits.
    pub fn new(limits: HashMap<String, RateLimit>) -> Self {
        let rpm_limiters: HashMap<String, Arc<RpmLimiter>> = limits
            .iter()
            .filter_map(|(name, limit)| {
                let rpm = NonZeroU32::new(limit.max_rpm?)?;
                let quota = Quota::per_minute(rpm);
                Some((name.clone(), Arc::new(GovernorLimiter::keyed(quota))))
            })
            .collect();

        Self {
            inner: Arc::new(RateLimitInner {
                limits,
                rpm_limiters,
                tpm_counters: RwLock::new(HashMap::new()),
            }),
        }
    }

    fn limit_for(&self, key: &str) -> RateLimit {
        self.inner
            .limits
            .get(key)
            .copied()
            .unwrap_or(RateLimit::unlimited())
    }

    fn tpm_counter(&self, key: &str) -> Arc<TpmCounter> {
        {
            let r = self
                .inner
                .tpm_counters
                .read()
                .unwrap_or_else(|e| e.into_inner());
            if let Some(c) = r.get(key) {
                return c.clone();
            }
        }
        let mut w = self
            .inner
            .tpm_counters
            .write()
            .unwrap_or_else(|e| e.into_inner());
        w.entry(key.to_string())
            .or_insert_with(|| Arc::new(TpmCounter::default()))
            .clone()
    }

    /// Check whether `key` is allowed to make one more request.
    /// Returns `Ok(())` if the key is under its RPM limit, or
    /// `Err(LimitExceeded::Rpm)` if it has been exhausted.
    pub fn check_rpm(&self, key: &str) -> Result<(), LimitExceeded> {
        let limit = self.limit_for(key);
        let rpm = match limit.max_rpm {
            None => return Ok(()),
            Some(r) => r,
        };
        // Each key has its own governor sized to its exact quota.
        if let Some(rl) = self.inner.rpm_limiters.get(key) {
            if rl.check_key(&key.to_string()).is_err() {
                return Err(LimitExceeded::Rpm { limit: rpm });
            }
        }
        Ok(())
    }

    /// Record that `key` consumed `tokens` and check whether the TPM limit is
    /// now exceeded. Returns the new token-per-minute total for the key.
    pub fn record_tpm(&self, key: &str, tokens: u64) -> Result<u64, LimitExceeded> {
        let limit = self.limit_for(key);
        let max_tpm = match limit.max_tpm {
            None => {
                // Still track even when unlimited, so metrics can read it.
                let now = now_secs();
                let c = self.tpm_counter(key);
                return Ok(c.add(tokens, now));
            }
            Some(m) => m,
        };
        let now = now_secs();
        let c = self.tpm_counter(key);
        let new_total = c.add(tokens, now);
        if new_total > max_tpm {
            Err(LimitExceeded::Tpm {
                used: new_total,
                limit: max_tpm,
            })
        } else {
            Ok(new_total)
        }
    }

    /// Current TPM usage for `key` in the active window (for metrics).
    pub fn current_tpm(&self, key: &str) -> u64 {
        self.tpm_counter(key).current(now_secs())
    }
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs()
}

// ---------------------------------------------------------------------------
// Retry-After helper
// ---------------------------------------------------------------------------

/// Suggest a `Retry-After` value in seconds given a refill rate. For RPM
/// limits, refill is once per minute divided by the limit.
pub fn retry_after_secs(rpm: u32) -> u64 {
    if rpm == 0 {
        60
    } else {
        (60u64).div_ceil(rpm as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn store_with(key: &str, limit: RateLimit) -> RateLimitStore {
        let mut m = HashMap::new();
        m.insert(key.to_string(), limit);
        RateLimitStore::new(m)
    }

    #[test]
    fn unconfigured_key_has_no_limits() {
        let store = RateLimitStore::new(HashMap::new());
        assert!(store.check_rpm("any").is_ok());
        let total = store.record_tpm("any", 999_999).unwrap();
        assert_eq!(total, 999_999);
    }

    #[test]
    fn rpm_allows_up_to_limit() {
        // 60 RPM = one token per second; governor refills instantly in tests
        // so we can fire a burst up to the per-cell capacity.
        let store = store_with(
            "k",
            RateLimit {
                max_rpm: Some(120),
                max_tpm: None,
            },
        );
        // First request must pass.
        assert!(store.check_rpm("k").is_ok());
    }

    #[test]
    fn tpm_allows_under_limit() {
        let store = store_with(
            "k",
            RateLimit {
                max_rpm: None,
                max_tpm: Some(1000),
            },
        );
        let total = store.record_tpm("k", 500).unwrap();
        assert_eq!(total, 500);
        let total = store.record_tpm("k", 499).unwrap();
        assert_eq!(total, 999);
    }

    #[test]
    fn tpm_rejects_when_exceeded() {
        let store = store_with(
            "k",
            RateLimit {
                max_rpm: None,
                max_tpm: Some(100),
            },
        );
        store.record_tpm("k", 100).unwrap();
        let err = store.record_tpm("k", 1).unwrap_err();
        assert!(matches!(err, LimitExceeded::Tpm { .. }));
    }

    #[test]
    fn retry_after_is_positive() {
        assert!(retry_after_secs(60) > 0);
        assert!(retry_after_secs(1) > 0);
        assert_eq!(retry_after_secs(0), 60);
    }

    #[test]
    fn current_tpm_tracks_within_window() {
        let store = RateLimitStore::new(HashMap::new());
        store.record_tpm("k", 50).unwrap();
        store.record_tpm("k", 30).unwrap();
        assert_eq!(store.current_tpm("k"), 80);
    }

    /// A key with a 1-RPM limit must be rejected after the burst is consumed.
    /// This tests the rejection path which was previously untested.
    #[test]
    fn rpm_rejects_after_burst_exhausted() {
        // 1 RPM = the governor allows exactly 1 request per minute.
        // The GCRA burst capacity for N/min is N (one token); the first
        // request consumes it and the second is rejected immediately.
        let store = store_with(
            "k",
            RateLimit {
                max_rpm: Some(1),
                max_tpm: None,
            },
        );
        // First request must pass.
        assert!(store.check_rpm("k").is_ok(), "first request should pass");
        // Subsequent request in the same window must be rejected.
        let err = store.check_rpm("k");
        assert!(
            matches!(err, Err(LimitExceeded::Rpm { limit: 1 })),
            "second request should be rejected, got: {err:?}"
        );
    }

    /// A lower-RPM key must not borrow capacity from a higher-RPM key on the
    /// same store.
    #[test]
    fn rpm_keys_are_independent() {
        let mut m = HashMap::new();
        m.insert(
            "low".to_string(),
            RateLimit {
                max_rpm: Some(1),
                max_tpm: None,
            },
        );
        m.insert(
            "high".to_string(),
            RateLimit {
                max_rpm: Some(120),
                max_tpm: None,
            },
        );
        let store = RateLimitStore::new(m);

        // Exhaust the low-RPM key.
        assert!(store.check_rpm("low").is_ok());
        assert!(matches!(
            store.check_rpm("low"),
            Err(LimitExceeded::Rpm { .. })
        ));

        // The high-RPM key must still be available.
        assert!(store.check_rpm("high").is_ok());
    }
}
