//! Prometheus-compatible metrics exposition.
//!
//! All metrics live as `Arc<AtomicU64>` counters — no external crate
//! required. The [`MetricsHandle`] is cheaply cloneable; handlers hold a
//! clone and update counters inline.
//!
//! # Exposed metrics (GET /metrics)
//!
//! ```text
//! # HELP patchbay_requests_total Total requests received.
//! # TYPE patchbay_requests_total counter
//! patchbay_requests_total{backend="local-llm",model="qwen-coder",status="2xx"} 42
//!
//! # HELP patchbay_tokens_total Total tokens consumed.
//! # TYPE patchbay_tokens_total counter
//! patchbay_tokens_total{key="dev",kind="prompt"} 1024
//! patchbay_tokens_total{key="dev",kind="completion"} 512
//! patchbay_tokens_total{key="dev",kind="total"} 1536
//!
//! # HELP patchbay_upstream_errors_total Upstream non-2xx or connection errors.
//! # TYPE patchbay_upstream_errors_total counter
//! patchbay_upstream_errors_total{backend="openai"} 3
//!
//! # HELP patchbay_rate_limit_rejections_total Requests rejected by rate limiter.
//! # TYPE patchbay_rate_limit_rejections_total counter
//! patchbay_rate_limit_rejections_total{key="dev"} 1
//!
//! # HELP patchbay_budget_rejections_total Requests rejected by token budget.
//! # TYPE patchbay_budget_rejections_total counter
//! patchbay_budget_rejections_total{key="dev"} 0
//! ```

use std::collections::HashMap;
use std::fmt::Write as FmtWrite;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use crate::upstream::Usage;

// ---------------------------------------------------------------------------
// Internal counter store
// ---------------------------------------------------------------------------

/// A set of labeled counters. Labels are `(name, label_set_str)` where the
/// label set is pre-formatted as `{k="v",...}` for direct text-format output.
#[derive(Debug, Default)]
struct LabeledCounters {
    inner: RwLock<HashMap<String, Arc<AtomicU64>>>,
}

impl LabeledCounters {
    fn get_or_create(&self, key: String) -> Arc<AtomicU64> {
        {
            let r = self.inner.read().unwrap_or_else(|e| e.into_inner());
            if let Some(c) = r.get(&key) {
                return c.clone();
            }
        }
        let mut w = self.inner.write().unwrap_or_else(|e| e.into_inner());
        w.entry(key)
            .or_insert_with(|| Arc::new(AtomicU64::new(0)))
            .clone()
    }

    fn inc(&self, key: String) {
        self.get_or_create(key).fetch_add(1, Ordering::Relaxed);
    }

    fn add(&self, key: String, n: u64) {
        self.get_or_create(key).fetch_add(n, Ordering::Relaxed);
    }

    /// Iterate in sorted order (deterministic output for tests).
    fn snapshot(&self) -> Vec<(String, u64)> {
        let r = self.inner.read().unwrap_or_else(|e| e.into_inner());
        let mut pairs: Vec<(String, u64)> = r
            .iter()
            .map(|(k, v)| (k.clone(), v.load(Ordering::Relaxed)))
            .collect();
        pairs.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        pairs
    }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Shared metrics handle. Clone-cheap; all clones update the same counters.
#[derive(Debug, Clone, Default)]
pub struct MetricsHandle {
    inner: Arc<MetricsInner>,
}

#[derive(Debug, Default)]
struct MetricsInner {
    requests: LabeledCounters,
    tokens: LabeledCounters,
    upstream_errors: LabeledCounters,
    rate_limit_rejections: LabeledCounters,
    budget_rejections: LabeledCounters,
}

impl MetricsHandle {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record one completed (or failed) request. `status_class` is "2xx",
    /// "4xx", "5xx", etc.
    pub fn record_request(&self, backend: &str, model: &str, status_class: &str) {
        let key = format!(
            r#"{{backend="{}",model="{}",status="{}"}}"#,
            escape(backend),
            escape(model),
            escape(status_class)
        );
        self.inner.requests.inc(key);
    }

    /// Record token usage for a virtual `key` after a completed response.
    pub fn record_tokens(&self, key: &str, usage: &Usage) {
        for (kind, val) in [
            ("prompt", usage.prompt_tokens),
            ("completion", usage.completion_tokens),
            ("total", usage.total_tokens),
        ] {
            let label = format!(r#"{{key="{}",kind="{}"}}"#, escape(key), kind);
            self.inner.tokens.add(label, val);
        }
    }

    /// Record a failed upstream request (non-2xx or connection error).
    pub fn record_upstream_error(&self, backend: &str) {
        let key = format!(r#"{{backend="{}"}}"#, escape(backend));
        self.inner.upstream_errors.inc(key);
    }

    /// Record a request rejected by the rate limiter.
    pub fn record_rate_limit_rejection(&self, key: &str) {
        let label = format!(r#"{{key="{}"}}"#, escape(key));
        self.inner.rate_limit_rejections.inc(label);
    }

    /// Record a request rejected by the token budget.
    pub fn record_budget_rejection(&self, key: &str) {
        let label = format!(r#"{{key="{}"}}"#, escape(key));
        self.inner.budget_rejections.inc(label);
    }

    /// Render all metrics in Prometheus text format (text/plain; version=0.0.4).
    pub fn render(&self) -> String {
        let mut out = String::with_capacity(2048);

        write_family(
            &mut out,
            "patchbay_requests_total",
            "counter",
            "Total requests received.",
            &self.inner.requests.snapshot(),
        );

        write_family(
            &mut out,
            "patchbay_tokens_total",
            "counter",
            "Total tokens consumed.",
            &self.inner.tokens.snapshot(),
        );

        write_family(
            &mut out,
            "patchbay_upstream_errors_total",
            "counter",
            "Upstream non-2xx or connection errors.",
            &self.inner.upstream_errors.snapshot(),
        );

        write_family(
            &mut out,
            "patchbay_rate_limit_rejections_total",
            "counter",
            "Requests rejected by rate limiter.",
            &self.inner.rate_limit_rejections.snapshot(),
        );

        write_family(
            &mut out,
            "patchbay_budget_rejections_total",
            "counter",
            "Requests rejected by token budget.",
            &self.inner.budget_rejections.snapshot(),
        );

        out
    }
}

fn write_family(out: &mut String, name: &str, ty: &str, help: &str, pairs: &[(String, u64)]) {
    if pairs.is_empty() {
        return;
    }
    writeln!(out, "# HELP {name} {help}").unwrap();
    writeln!(out, "# TYPE {name} {ty}").unwrap();
    for (labels, value) in pairs {
        writeln!(out, "{name}{labels} {value}").unwrap();
    }
}

/// Escape label values: backslash, double-quote, and newline must be escaped.
fn escape(s: &str) -> String {
    s.replace('\\', r"\\")
        .replace('"', r#"\""#)
        .replace('\n', r"\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_is_empty_before_any_records() {
        let h = MetricsHandle::new();
        assert!(h.render().is_empty());
    }

    #[test]
    fn request_counter_appears_in_output() {
        let h = MetricsHandle::new();
        h.record_request("local-llm", "qwen-coder", "2xx");
        h.record_request("local-llm", "qwen-coder", "2xx");
        h.record_request("openai", "gpt-4o", "5xx");

        let out = h.render();
        assert!(out.contains("patchbay_requests_total"));
        assert!(out.contains(r#"backend="local-llm""#));
        assert!(out.contains(r#"model="qwen-coder""#));
        assert!(out.contains(r#"status="2xx"} 2"#));
        assert!(out.contains(r#"status="5xx"} 1"#));
    }

    #[test]
    fn token_counters_accumulate() {
        let h = MetricsHandle::new();
        let u = Usage {
            prompt_tokens: 10,
            completion_tokens: 20,
            total_tokens: 30,
        };
        h.record_tokens("dev", &u);
        h.record_tokens("dev", &u);

        let out = h.render();
        assert!(out.contains(r#"kind="prompt"} 20"#));
        assert!(out.contains(r#"kind="completion"} 40"#));
        assert!(out.contains(r#"kind="total"} 60"#));
    }

    #[test]
    fn upstream_error_counter() {
        let h = MetricsHandle::new();
        h.record_upstream_error("openai");
        h.record_upstream_error("openai");

        let out = h.render();
        assert!(out.contains("patchbay_upstream_errors_total"));
        assert!(out.contains(r#"backend="openai"} 2"#));
    }

    #[test]
    fn rate_limit_and_budget_rejection_counters() {
        let h = MetricsHandle::new();
        h.record_rate_limit_rejection("alice");
        h.record_budget_rejection("bob");

        let out = h.render();
        assert!(out.contains("patchbay_rate_limit_rejections_total"));
        assert!(out.contains(r#"key="alice"} 1"#));
        assert!(out.contains("patchbay_budget_rejections_total"));
        assert!(out.contains(r#"key="bob"} 1"#));
    }

    #[test]
    fn label_values_are_escaped() {
        let h = MetricsHandle::new();
        h.record_request(r#"back"end"#, "model\nname", "2xx");
        let out = h.render();
        // Quotes and newlines must be escaped in the output.
        assert!(out.contains(r#"back\"end"#) || out.contains(r#"back\\"end"#));
    }

    #[test]
    fn help_and_type_lines_appear() {
        let h = MetricsHandle::new();
        h.record_request("b", "m", "2xx");
        let out = h.render();
        assert!(out.contains("# HELP patchbay_requests_total"));
        assert!(out.contains("# TYPE patchbay_requests_total counter"));
    }
}
