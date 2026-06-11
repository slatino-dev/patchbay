# patchbay

Single-binary OpenAI-compatible LLM gateway in Rust: type-enforced privacy routing, latency-aware backend selection, per-key rate limits, and byte-faithful SSE streaming.

## The problem

Running multiple LLM backends — a local model for sensitive work, a cloud API for heavy lifting — means writing routing logic somewhere. The usual choices are a proxy config that can't enforce privacy rules, an application-level `if/else` that a refactor can silently break, or a full Python stack (LiteLLM) that brings 200+ transitive dependencies and a process interpreter to a problem that is fundamentally an HTTP relay.

patchbay is a focused answer: a single statically linked binary that routes requests to the right backend, enforces that private traffic never leaves operator-controlled infrastructure at the type level (not with a runtime flag), relays SSE streams byte-for-byte without re-serialization, and accounts token usage per key.

## Why not just LiteLLM?

LiteLLM is the right tool for most teams. It supports 100+ providers, has mature cost tracking, a management UI, a thriving plugin ecosystem, and an active community. If you need broad provider coverage or want something production-proven today, use LiteLLM.

patchbay makes different tradeoffs:

| | patchbay | LiteLLM |
|---|---|---|
| **Deployment** | Single static binary, ~15 deps | Python process + pip tree |
| **Privacy routing** | Compile-time: forbidden states don't compile | Runtime config flag |
| **SSE relay** | Byte-faithful, no re-serialization | Re-encoded |
| **Provider support** | OpenAI-compatible endpoints | 100+ providers |
| **Maturity** | Early / experimental | Production-proven |
| **Throughput** | Benchmarks in progress (`bench/README.md`) | Established baseline |

The dependency count is real: 16 direct crates vs a Python package tree. The privacy guarantee is real. The throughput numbers are not fabricated — the methodology is documented in `bench/README.md` and results will be published once collected.

## Architecture

```
Client request
    │
    ▼
┌───────────────────────────────────────────────────────┐
│  axum HTTP server  (src/server.rs)                    │
│                                                       │
│  AuthedKey extractor  ──►  KeyStore (src/auth.rs)     │
│  RPM / TPM check      ──►  RateLimitStore             │
│  Route                ──►  Router  (src/router.rs)    │
│    │                                                  │
│    ▼  TypedQuery<Private>  or  TypedQuery<Shareable>  │
│  Candidates::gather  (privacy filter, type-witnessed) │
│  Policy::choose      (StaticPriority | EwmaLatency)   │
│    │                                                  │
│    ▼  Backend (guaranteed to satisfy privacy class)   │
│  UpstreamClient::open_sse  (src/upstream.rs)          │
│    │                                                  │
│    ▼                                                  │
│  SseRelay  ──── byte-faithful chunks ────►  Client    │
│     │                                                 │
│     └── usage scanner (side-channel, never mutates    │
│         the forwarded bytes)                          │
│     └── BudgetLedger::record + MetricsHandle          │
└───────────────────────────────────────────────────────┘
```

**Config layer** (`src/config.rs`): TOML file where every secret and URL is an env-var name; 19 distinct load-time validation errors; `deny_unknown_fields`; injectable env lookup so tests never mutate process state.

**Router** (`src/router.rs`): privacy routing via sealed, uninhabited marker types. `Candidates<Private>` can only be constructed through a filter that rejects `External` backends. A `Selection<Private>` referencing an external backend is literally unrepresentable. 1024-case proptest suite — including a deliberately malicious `Policy` returning arbitrary indices — verifies the invariant holds for any backend table or latency history.

**SSE relay** (`src/upstream.rs`): hand-rolled incremental SSE event-boundary parser that handles `\r\n` split across TCP chunks. Stall timeout, 1 MiB overflow cap with graceful degradation, client-disconnect propagation via `Drop`. Usage scanner intercepts the final usage event for accounting without touching the forwarded byte sequence.

**Rate limiting** (`src/limits.rs`): per-key governor (GCRA) — each key gets its own quota-sized instance, so a 10-RPM key cannot borrow from a 120-RPM key on the same store.

**Budget** (`src/budget.rs`): per-key token counters with atomic updates, periodic JSON snapshot, and restart recovery within the same accounting period.

## Stack

Rust 2021 · axum 0.7 · tokio · reqwest · governor (GCRA) · serde / toml · tracing · proptest (1024-case privacy suite) · criterion (routing micro-benchmarks)

## Quickstart

```bash
# Set the env vars named in patchbay.toml (the loader tells you which are missing):
export LLM_BASE_URL=http://127.0.0.1:8000   # any OpenAI-compatible local server
export OPENAI_BASE_URL=https://api.openai.com
export OPENAI_API_KEY=sk-...
export PATCHBAY_KEY_DEV=my-gateway-key      # clients authenticate with this

cargo run --release

# Health check
curl http://localhost:8080/healthz

# Chat completion (streaming)
curl -s http://localhost:8080/v1/chat/completions \
  -H "Authorization: Bearer $PATCHBAY_KEY_DEV" \
  -H "Content-Type: application/json" \
  -d '{"model":"qwen-coder","stream":true,"messages":[{"role":"user","content":"hello"}]}'
```

## Running tests

```bash
cargo test                                  # unit + property + integration (75 tests)
cargo test --doc                            # doctests including compile_fail proof
cargo clippy --all-targets -- -D warnings
cargo bench --bench routing                 # criterion routing micro-benchmarks
```

The privacy guarantee has a dedicated 1024-case property suite (`src/router.rs`, module `privacy_properties`). A compile-time proof lives in the module doc of `src/router.rs` — `cargo test --doc` verifies it does not compile.

## Configuration

`patchbay.toml` is the example config and is safe to commit — no secrets live in it:

```toml
listen = "0.0.0.0:8080"

[policy]
kind = "ewma_latency"
alpha = 0.3               # blending factor for latency EWMA

[[backends]]
name = "local-llm"
base_url_env = "LLM_BASE_URL"
models = ["qwen-coder", "qwen-chat"]
capability_tags = ["code", "fast"]
privacy = "local"         # may serve private traffic

[[backends]]
name = "openai"
base_url_env = "OPENAI_BASE_URL"
api_key_env = "OPENAI_API_KEY"
models = ["gpt-4o", "gpt-4o-mini"]
capability_tags = ["general"]
privacy = "external"      # structurally excluded from private requests

[[virtual_keys]]
name = "dev"
key_env = "PATCHBAY_KEY_DEV"
enforce_private = true    # all traffic on this key stays on local backends
```

## Limitations and what I'd do differently

**Not yet wired:**
- Per-request backend promotion after retry exhaustion. The retry loop retries the same backend up to five times with jittered backoff. True failover (routing to the next eligible backend after the first is exhausted) requires propagating upstream failure back into the router — the machinery exists but the per-request feedback loop is not closed.
- `enforce_private` and `require_tags` are not (yet) settable per-request via a header. Both exist as config-level controls.
- EWMA latency observations are not fed from the live proxy path. The policy runs on cold-start zeros until observations are plumbed from `SseRelay` timing back into `EwmaLatency::observe`.
- Budget `flush` is not called periodically or on graceful shutdown yet — budget accounting lives in-memory between restarts.

**Design choices I'd revisit:**
- The `BudgetLedger::check()` pre-flight is not atomic with `BudgetLedger::record()`. Concurrent requests can temporarily exceed budget limits by one request's worth of tokens. For a gateway this is acceptable, but the code should say so more clearly at the call site.
- `/metrics` is unauthenticated and exposes virtual-key names as label values. Acceptable for a homelab deployment; a production deployment should gate it.
- The 1 MiB SSE scanner overflow cap disables usage accounting for that stream only. A stream pathology that consistently triggers it would silently zero out accounting.
- Usage counting happens at response-open time for the `2xx` status label, so a stream that later stalls is still counted as `2xx` in the counter (a separate `upstream_error` counter does fire). These metrics should be reconciled in a single event.

**Throughput vs LiteLLM:** The bench methodology is in `bench/README.md`. Numbers will be published once collected — none are claimed here.
