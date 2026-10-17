# distributed-rate-limiter

[![ci](https://github.com/HOFFMAN-ALTERNATIVE/distributed-rate-limiter/actions/workflows/ci.yml/badge.svg)](https://github.com/HOFFMAN-ALTERNATIVE/distributed-rate-limiter/actions/workflows/ci.yml)
![Go](https://img.shields.io/badge/Go-1.22%2B-00ADD8?logo=go&logoColor=white)
![Redis](https://img.shields.io/badge/Redis-Lua%20scripts-DC382D?logo=redis&logoColor=white)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

Rate limits that hold across replicas. Every decision against Redis is one Lua script, so two replicas can never both spend the last token. The integration suite runs four clients against one Redis and checks that the global limit is hit exactly, with not one request over.

Use it as a Go library (`ratelimit` + `net/http` middleware) or run `ratelimiterd` and call `GET /check?key=...` from any language. Token bucket and sliding-window log, in-memory or Redis backends, Prometheus metrics, a distroless non-root image.

Measured on an 8 vCPU Xeon VM, Go 1.27, in-memory backend: 57 ns per decision across 10k keys, 226 ns when every goroutine hits the same key, zero allocations. The Redis backend costs one round trip per decision; CI benchmarks it against a real Redis 7 on every push.

## Why

Per-process counters multiply the real limit by the replica count. A `GET`, compute, `SET` sequence against Redis races under load and lets bursts through. This project closes both gaps:

- **One round trip, no race window.** Refill, consume and write happen inside Redis in a single `EVALSHA`.
- **One clock.** Scripts read Redis `TIME`, so replicas with skewed clocks agree on one timeline.
- **Exactness is a test,** not a claim (`TestIntegration_DistributedExactness` in `ratelimit/redis_integration_test.go`).

## Features

| | |
|---|---|
| Algorithms | Token bucket (rate + burst), sliding window log (exact N per window) |
| Backends | In-memory (64-way lock striping, zero allocations) · Redis / Redis Cluster (hash-tagged keys) |
| Weighted requests | `AllowN(ctx, key, cost)`, for example to charge expensive endpoints more |
| Integration | `net/http` middleware with `RateLimit-Limit`, `RateLimit-Remaining`, `Retry-After` headers; pluggable key funcs |
| Service | `ratelimiterd`: `/check`, `/demo`, `/healthz`, `/readyz`, `/metrics` |
| Failure policy | Fail-closed by default (503); opt-in fail-open wrapper |
| Observability | `ratelimit_decisions_total`, `ratelimit_errors_total`, `ratelimit_decision_duration_seconds` |
| Ops | Distroless non-root image, graceful shutdown, server timeouts, bounded memory via periodic sweep |
| Quality | `go test -race`, real-Redis integration tests in CI, benchmarks, golangci-lint (15 linters), Docker smoke test |

## Architecture

```mermaid
flowchart LR
    subgraph Clients
        A[Service A<br/>Go middleware]
        B[Service B<br/>any language]
    end
    subgraph Replicas["ratelimiterd replicas"]
        R1[ratelimiterd #1]
        R2[ratelimiterd #2]
    end
    B -- "GET /check?key=user:42" --> R1
    B -- "GET /check?key=user:42" --> R2
    A -- "Limiter.AllowN()" --> LIB
    R1 --> LIB
    R2 --> LIB
    subgraph LIB["ratelimit package"]
        direction TB
        M[metrics.Instrument] --> FO[FailOpen<br/>optional]
        FO --> IF{{Limiter interface}}
        IF --> MEM[Memory backend<br/>sharded maps]
        IF --> RED[Redis backend]
    end
    RED -- "EVALSHA token_bucket.lua /<br/>sliding_window.lua (atomic)" --> REDIS[(Redis)]
    M -. scrape .-> P[Prometheus]
```

Decision flow inside Redis (token bucket):

```mermaid
sequenceDiagram
    participant S as ratelimiterd
    participant R as Redis (Lua)
    S->>R: EVALSHA tb {key} rate burst cost
    R->>R: now = TIME
    R->>R: HMGET tokens, ts → refill by (now-ts)·rate, cap at burst
    alt tokens ≥ cost
        R->>R: tokens -= cost
    else
        R->>R: retry = (cost-tokens)/rate
    end
    R->>R: HSET + PEXPIRE (key vanishes when bucket would be full)
    R-->>S: {allowed, remaining, retry_after_us}
```

## Quickstart

### As a service (docker compose: 2 replicas + Redis)

```bash
docker compose up --build
# Both replicas share one bucket per key (burst 10, 5 req/s):
for i in $(seq 1 12); do curl -s "localhost:808$((i%2+1))/check?key=alice"; done
```

```json
{"allowed":true,"key":"alice","limit":10,"remaining":9,"retry_after_ms":0}
...
{"allowed":false,"key":"alice","limit":10,"remaining":0,"retry_after_ms":187}
```

Allowed → `200`, limited → `429` with `Retry-After`. Weighted calls:
`/check?key=alice&cost=5`.

### As a library

```go
import (
    "github.com/HOFFMAN-ALTERNATIVE/distributed-rate-limiter/middleware"
    "github.com/HOFFMAN-ALTERNATIVE/distributed-rate-limiter/ratelimit"
    "github.com/redis/go-redis/v9"
)

rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
limiter, _ := ratelimit.NewRedisTokenBucket(rdb, ratelimit.TokenBucket{Rate: 10, Burst: 20})

// Key by API key header, fail open if Redis is down.
mw := middleware.RateLimit(ratelimit.FailOpen(limiter, nil), middleware.Options{
    Key: middleware.KeyByHeader("X-API-Key"),
})
http.ListenAndServe(":8080", mw(appHandler))
```

Swap `NewRedisTokenBucket` for `NewMemoryTokenBucket`, `NewRedisSlidingWindow` or
`NewMemorySlidingWindow`; they all implement `ratelimit.Limiter`.

### Configuration (environment)

| Variable | Default | Meaning |
|---|---|---|
| `RL_ADDR` | `:8080` | Listen address |
| `RL_BACKEND` | `memory` | `memory` or `redis` |
| `RL_ALGORITHM` | `token_bucket` | `token_bucket` or `sliding_window` |
| `RL_RATE` / `RL_BURST` | `10` / `20` | Token bucket refill (tokens/s) and capacity |
| `RL_LIMIT` / `RL_WINDOW` | `100` / `1m` | Sliding window: max events per window |
| `RL_REDIS_ADDR` / `RL_REDIS_DB` | `localhost:6379` / `0` | Redis connection |
| `RL_KEY_PREFIX` | `rl:` | Namespace for Redis keys |
| `RL_FAIL_OPEN` | `false` | Allow requests when the backend errors |
| `RL_BACKEND_TIMEOUT` | `50ms` | Per-decision deadline |

## Design decisions and trade-offs

Recorded as ADRs: [0001 Lua scripts instead of MULTI/WATCH](docs/adr/0001-lua-scripts-over-optimistic-transactions.md), [0002 fail closed by default](docs/adr/0002-fail-closed-by-default.md), [0003 Redis TIME as the only clock](docs/adr/0003-redis-time-as-the-clock.md).


- **Token bucket vs sliding log.** Token bucket is O(1) memory per key and allows
  controlled bursts, which is the right default for APIs. Sliding log is *exact* (no 2×
  burst at window boundaries like fixed windows), at O(limit) memory per key;
  use it for low limits where precision matters (logins, verification-code sends, payments).
  The in-memory sliding log uses a fixed ring buffer so it never allocates on the
  hot path.
- **Lua over `MULTI`/`WATCH`.** Optimistic transactions retry under contention,
  which is exactly when a limiter is busiest. A script runs atomically with no retries and
  a single round-trip.
- **Redis time, not client time.** Avoids clock-skew bugs across replicas at the
  cost of one extra `TIME` call inside the script (sub-microsecond).
- **Self-expiring keys.** Bucket keys expire when they would be full again, log
  keys after one window, so Redis memory tracks *active* keys only and needs no
  cleanup job. An absent key is semantically a full bucket.
- **Cluster-ready keys.** The user key is wrapped in a hash tag
  (`rl:tb:{user:42}`) so each script touches exactly one slot.
- **Fail-closed by default.** A limiter that silently disappears when Redis does
  is a security hole for login/abuse use cases. Availability-first services opt
  into `FailOpen` explicitly; swallowed errors are still counted in metrics.
- **No per-key metric labels.** Keys are unbounded (IPs, users); labelling by key
  would explode Prometheus cardinality. Metrics are per limiter and decision.
- **Middleware does not trust `X-Forwarded-For`** by default. Spoofable headers
  must be an explicit choice (`KeyByHeader`) behind a trusted proxy.

## Benchmarks

`go test -run '^$' -bench Memory -benchmem ./ratelimit` on an 8 vCPU Xeon VM, Go 1.27.2:

```
BenchmarkMemoryTokenBucket_HotKey-8       226.0 ns/op   0 B/op   0 allocs/op
BenchmarkMemoryTokenBucket_10kKeys-8       56.8 ns/op   0 B/op   0 allocs/op
BenchmarkMemorySlidingWindow_10kKeys-8     59.6 ns/op   1 B/op   0 allocs/op
```

The hot-key case is bound by a single shard mutex (worst case: every goroutine
on one key); spread keys scale across the 64 shards. The Redis backend is
dominated by network round-trip time; CI runs `BenchmarkRedisTokenBucket`
against a real Redis service container on every push.

## Development

```bash
make test         # unit tests with -race (Lua scripts run on miniredis)
make integration  # real Redis: REDIS_ADDR=localhost:6379
make bench
make lint         # golangci-lint
make docker && make up
```

CI (`.github/workflows/ci.yml`): golangci-lint · unit tests on Go 1.22 and 1.23 ·
integration tests + benchmarks against a Redis 7 service container · Docker
build and HTTP smoke test of the image.

## Limits

- No Redis Sentinel or Cluster failover logic of its own. Keys are hash-tagged so Cluster works, but failover behaviour is whatever go-redis and your Redis setup give you.
- During a Redis outage the service fails closed (503). Fail-open is one env var away, and then limits are not enforced at all; there is no local fallback bucket yet.
- The sliding-window log keeps one entry per request, so memory per key grows with the limit. Use the token bucket for limits in the thousands.
- `/check` has no authentication. Run it on a private network or behind your gateway; it is a policy oracle, not a public endpoint.
- The `/demo` endpoint keys by `RemoteAddr`. Behind a proxy every client shares one key unless you configure `KeyByHeader` for your trusted proxy header.

## Operations

- `/healthz` liveness, `/readyz` pings Redis, `/metrics` Prometheus. The image runs `ratelimiterd healthcheck` as its Docker `HEALTHCHECK`.
- SIGTERM stops accepting connections and drains in-flight requests for up to 10 s.
- Server timeouts: 5 s read header, 10 s read and write, 60 s idle. Redis: 2 s dial, 1 s read/write/pool, plus the per-decision deadline (`RL_BACKEND_TIMEOUT`, default 50 ms).
- `/check` rejects keys over 256 bytes and request bodies over 4 KiB.

See [SECURITY.md](SECURITY.md) for the threat model and how to report a vulnerability.

## Project layout

```
ratelimit/          core: Limiter interface, memory + Redis backends, Lua scripts
middleware/         net/http middleware and key functions
metrics/            Prometheus instrumentation decorator
internal/config/    env configuration
internal/server/    HTTP API of ratelimiterd
cmd/ratelimiterd/   service entrypoint
```

## Container image

Multi-arch (amd64/arm64) images are published to GitHub Container Registry on every release:

```bash
docker pull ghcr.io/hoffman-alternative/distributed-rate-limiter:latest
```

## License

MIT
