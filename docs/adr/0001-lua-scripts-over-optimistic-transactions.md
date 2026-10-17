# 0001. Lua scripts instead of MULTI/WATCH
Status: accepted
Date: 2026-10-17

## Context
A distributed limiter reads a bucket, computes the refill, decides, and writes back. Done as separate commands from several replicas, two replicas can read the same state and both spend the last token. The limiter is busiest exactly when contention is highest, so the concurrency strategy matters most under load.

## Options
1. Client-side `GET` / compute / `SET`. Simple. Races; over-admits under load.
2. `WATCH` + `MULTI`/`EXEC` (optimistic). Correct. Every conflict is a retry and another round trip, and conflicts rise with load.
3. One Lua script per decision via `EVALSHA`. Atomic on the Redis side, one round trip, no retries. Logic lives in two short `.lua` files that need their own tests.

## Decision
Option 3. It is the only option that is both correct and gets cheaper, not more expensive, as contention grows.

## Consequences
- Scripts are tested twice: on miniredis in unit tests and on real Redis 7 in CI integration tests.
- Each script touches exactly one key, wrapped in a hash tag, so Redis Cluster works.
- A long-running script would block Redis. Both scripts are O(1) (token bucket) or O(expired entries) (sliding log); revisit if a new algorithm needs loops over unbounded data.
