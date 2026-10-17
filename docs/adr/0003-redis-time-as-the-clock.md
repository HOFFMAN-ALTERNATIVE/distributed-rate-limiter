# 0003. Redis TIME as the only clock
Status: accepted
Date: 2026-10-17

## Context
Token refill and window expiry depend on "now". Replicas run on different hosts whose clocks drift. If each replica passes its own timestamp, a replica running ahead refills buckets early and one running behind can push a bucket's timestamp backwards.

## Options
1. Client timestamps passed as script arguments. Easy to test with a fake clock. Correctness depends on NTP across every replica.
2. Redis `TIME` inside the script. One timeline for all replicas. Costs a `TIME` call per decision and needs Redis 5+ script effects replication (default since Redis 5).

## Decision
Option 2. Correctness should not depend on the health of NTP on every replica.

## Consequences
- Replica clock skew has no effect on decisions.
- Tests that need time control use miniredis's settable clock instead of injecting timestamps.
- The in-memory backend uses the process clock, which is fine because it is single-process by definition.
