# patchbay — system bench scripts

End-to-end latency and throughput tests against a running gateway instance.

## Status

Scaffolding only — scripts land in Phase B.

## Planned scripts

- `bench_throughput.sh` — drive sustained requests/second at the gateway, report p50/p95/p99 via `oha` or `wrk2`
- `bench_streaming.sh` — measure time-to-first-token and streaming tail latency across backends
- `bench_fallback.sh` — simulate upstream failures and measure fallback promotion latency

## Usage (future)

```bash
# Start the gateway first
PATCHBAY_CONFIG=bench/fixtures/bench.toml cargo run --release

# Run throughput bench
bash bench/bench_throughput.sh http://127.0.0.1:8080
```

## Dependencies

- `oha` or `wrk2` for HTTP load generation
- `jq` for response parsing
- A running patchbay instance with at least one healthy upstream
