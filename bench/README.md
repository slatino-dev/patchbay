# patchbay — system bench scripts

End-to-end latency and throughput tests against a running gateway instance.
These are *system* benchmarks that require a live gateway and (optionally)
a reference upstream; they complement the in-process Criterion micro-benchmarks
in `benches/routing.rs`.

## Methodology

### Parity configuration

To produce a fair LiteLLM head-to-head comparison, both gateways must be
configured identically:

- **Same upstream(s):** point both at the same backend (e.g. a local vLLM
  instance) so backend variance is not a factor.
- **Same model:** use the same model ID on both sides.
- **Same host:** run both gateways and the load generator on the same machine
  (or the same latency tier) to eliminate network variance.
- **Same concurrency:** use the same `-c`/`--workers` setting in `oha` or `wrk2`.
- **Warm-up:** discard the first 5 seconds of each run.
- **Steady-state run:** 60 seconds per configuration.

### What to measure

| Metric              | Tool                     | Notes                              |
|---------------------|--------------------------|------------------------------------|
| Requests/s (RPS)    | `oha -n 1000 -c 10 URL`  | Non-streaming, fixed concurrency   |
| p50 / p95 / p99     | `oha`, `wrk2`            | Focus on p99 for tail-latency SLO  |
| Time-to-first-byte  | `curl -w "%{time_starttransfer}"` | Streaming path only    |
| Streaming tail      | custom client timing     | Time from first byte to last byte  |

## Planned scripts

- `bench_throughput.sh` — drive sustained requests/second at the gateway,
  report p50/p95/p99 via `oha` or `wrk2`.
- `bench_streaming.sh` — measure time-to-first-token and streaming tail
  latency across backends.
- `bench_fallback.sh` — simulate upstream failures and measure fallback
  promotion latency (time added by retry + jitter).

## Usage

```bash
# 1. Start your upstream (e.g. vLLM)
#    vllm serve <model> --host 127.0.0.1 --port 8000

# 2. Start patchbay
PATCHBAY_CONFIG=bench/fixtures/bench.toml cargo run --release

# 3. (Optionally) start LiteLLM on another port for comparison
#    litellm --config bench/fixtures/litellm.yaml --port 8081

# 4. Run throughput bench against patchbay
oha -n 2000 -c 20 \
  -m POST -T "application/json" \
  -d '{"model":"test-model","messages":[{"role":"user","content":"ping"}],"stream":false}' \
  http://127.0.0.1:8080/v1/chat/completions

# 5. Repeat against LiteLLM on port 8081 with the same flags
```

## Dependencies

- `oha` or `wrk2` for HTTP load generation (`cargo install oha` or
  `brew install wrk2`)
- `jq` for response parsing
- A running patchbay instance (and optionally a LiteLLM instance) with at
  least one healthy upstream
