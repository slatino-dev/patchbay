# patchbay

OpenAI-compatible LLM gateway with tag-based privacy routing, latency-aware backend selection, and byte-faithful SSE streaming relay.

## What's implemented

- **Config** (`src/config.rs`) — TOML config where backends declare `{name, base_url_env, api_key_env, models, capability_tags, privacy}`. Secrets and URLs are resolved from environment variables at load time and never live in the file. Fully validated on load (unknown fields, duplicates, malformed URLs, missing env vars, bad policy parameters are all startup errors).
- **Router** (`src/router.rs`) — policy-driven backend selection with **type-enforced privacy routing**: a request classified as private *cannot* select a `privacy = "external"` backend. The constraint is carried by sealed witness types (`Candidates<Private>` is only constructible through the privacy filter, and policies pick by bounds-checked index), not by a runtime `if` that a refactor could drop. Policies: `static_priority` (config order) and `ewma_latency` (lowest EWMA latency, optimistic cold-start probing).
- **SSE relay** (`src/upstream.rs`) — relays upstream stream chunks to the client byte-for-byte (no re-serialization), while a side-channel scanner intercepts the final `usage` event for accounting. Bounded stall detection, and client disconnects propagate upstream by cancelling the in-flight request.

The HTTP endpoint layer (`/v1/chat/completions` proxy wiring auth, rate limits, budgets, and metrics together) is the next phase; those modules are documented stubs.

## Try it

```bash
cargo test                                      # unit + property + integration tests
cargo clippy --all-targets -- -D warnings
cargo bench --bench routing                     # criterion routing benches
```

The privacy guarantee has a dedicated property suite (`src/router.rs`, `privacy_properties`): across randomized backend tables, queries, latency histories, and policies — including a deliberately malicious `Policy` implementation returning arbitrary indexes — no private request ever resolves to an external backend.

To run the binary, set the env vars named in `patchbay.toml` (the loader tells you exactly which are missing):

```bash
export LLM_BASE_URL=http://127.0.0.1:8000     # any OpenAI-compatible local server
export OPENAI_BASE_URL=https://api.openai.com
export OPENAI_API_KEY=...
export PATCHBAY_KEY_DEV=...                   # key clients will use against the gateway
cargo run
```

## Limitations

- The OpenAI-compatible proxy endpoints are not wired yet — the binary currently validates config and serves `/healthz`. The routing core and relay are library-complete and fully tested.
- Usage interception assumes OpenAI-style streams (`stream_options.include_usage`: usage in the final data event before `data: [DONE]`). Streams without usage still relay fine; they just account zero tokens.
- A single SSE event larger than 1 MiB disables usage accounting for that stream (the relay itself is unaffected).
- EWMA latency observations are plumbed (`EwmaLatency::observe`) but will be fed by the proxy layer once it lands.
