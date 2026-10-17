# 0002. Fail closed by default
Status: accepted
Date: 2026-10-17

## Context
When Redis is unreachable the limiter cannot decide. Some callers use it for abuse protection (login attempts, one-time code sends), where silently admitting everything is a security hole. Others use it for fairness on a public API, where rejecting all traffic during a Redis blip is an outage.

## Options
1. Fail open by default. Best availability. A Redis outage silently disables brute-force protection.
2. Fail closed by default, fail open as an explicit opt-in (`RL_FAIL_OPEN=true` or `ratelimit.FailOpen`). Safe default; availability-first users make a visible choice.
3. Local in-memory fallback bucket during outages. Keeps some limiting. Limits become per-replica, the effective limit multiplies by replica count, and the code path is rarely exercised.

## Decision
Option 2. A default that can only be wrong in the safe direction, plus a one-line opt-out.

## Consequences
- During an outage `ratelimiterd` returns 503 and `/readyz` fails, so orchestrators stop routing to it.
- Fail-open still counts backend errors in `ratelimit_errors_total`, so the outage is visible.
- Option 3 is the natural next step if a user needs degraded limiting instead of none.
