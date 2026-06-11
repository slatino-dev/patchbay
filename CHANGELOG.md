# Changelog

All notable changes to patchbay are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
patchbay uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [0.1.0] — 2026-06-11

Initial public release.

### Added

**Core routing**
- Type-enforced privacy routing via sealed, uninhabited marker types (`Private` / `Shareable`). A `Candidates<Private>` set can only be constructed through a filter that excludes `External` backends; a `Selection<Private>` referencing an external backend is unrepresentable at the type level.
- `StaticPriority` policy: picks the first eligible backend in config order.
- `EwmaLatency` policy: picks the lowest exponentially-weighted moving-average latency backend; cold backends score 0 (optimistic probing rather than starvation).
- Tag-based capability filtering: backends declare `capability_tags`; route queries can require a subset.
- 1024-case proptest suite for the privacy invariant, including a deliberately malicious `Policy` implementation returning arbitrary indices.
- `compile_fail` doctest proving the forbidden `Candidates<Private>` direct-construction does not compile.

**HTTP server**
- `POST /v1/chat/completions`: streaming (SSE relay) and non-streaming (buffered) proxy with jittered exponential backoff on transient errors (5xx / 429 / connection failures). 4xx errors are passed through immediately without retry.
- `GET /v1/models`: lists all model identifiers across configured backends.
- `GET /healthz`: liveness probe.
- `GET /metrics`: Prometheus text-format counters (`patchbay_requests_total`, `patchbay_upstream_errors_total`, `patchbay_tokens_total`, `patchbay_rate_limit_rejections_total`).

**SSE relay**
- Byte-faithful relay: every upstream chunk forwarded verbatim, no re-serialization.
- Incremental SSE event-boundary parser handling `\r\n` split across TCP chunks and multi-`data:` line events per spec.
- Side-channel usage scanner: intercepts the final usage event for accounting without altering forwarded bytes.
- 1 MiB overflow cap: disables usage accounting for pathologically large events; relay continues unaffected.
- Stall timeout (default 30 s): terminates hung upstream connections.
- Client-disconnect propagation via `Drop` on the relay stream.

**Authentication**
- Virtual API key lookup from `Authorization: Bearer` header.
- Open mode (no configured keys): every request admitted as anonymous, useful for local development.
- `enforce_private = true` per virtual key: forces all traffic onto `privacy = "local"` backends.

**Rate limiting**
- Per-key RPM enforcement via `governor` (GCRA). Each key has its own quota-sized governor instance — a 10-RPM key cannot consume another key's capacity.
- Per-key TPM tracking via a rolling 60-second atomic counter.

**Budget accounting**
- Per-key prompt / completion / total token counters with atomic updates.
- `BudgetLedger::check()` pre-flight (non-atomic with record; best-effort).
- JSON snapshot with atomic write (write to `.tmp`, then rename) for restart recovery within the same accounting period.

**Config**
- TOML config with `deny_unknown_fields`; 19 distinct load-time validation errors.
- All secrets and URLs resolved from environment variables at load time; injectable env lookup for deterministic tests.
- `Secret<T>` with redacted `Debug` output.
- Duplicate virtual-key-secret detection.

**Observability**
- `tracing` / `tracing-subscriber` with `RUST_LOG` env filter.
- Prometheus text-format metrics on `GET /metrics`.

**Testing & CI**
- Unit tests, property tests (proptest), and integration tests covering the full TCP path with in-process mock upstreams.
- GitHub Actions CI: `cargo fmt --check`, `cargo clippy -D warnings`, `cargo test --all-targets`, `cargo test --doc` (verifies `compile_fail` proofs), on `x86_64` and `aarch64` Linux runners.
- `scripts/scrub_check.sh` gate: blocks accidental commit of internal infrastructure patterns.

**Benchmarks**
- `criterion` micro-benchmarks for backend routing (`benches/routing.rs`).
- System bench methodology documented in `bench/README.md` (LiteLLM head-to-head methodology defined; no fabricated numbers).

[0.1.0]: https://github.com/SamLatino/patchbay/releases/tag/v0.1.0
