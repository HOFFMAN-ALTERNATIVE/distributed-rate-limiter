# Security

## Reporting

Report vulnerabilities privately through GitHub's "Report a vulnerability" button on this repository (Security tab). I aim to reply within 72 hours. Please do not open public issues for exploitable bugs.

## Threat model

- **Assets:** availability of the protected services; Redis memory and CPU; the integrity of limit decisions.
- **Actors:** abusive clients trying to exceed limits; anyone who can reach `ratelimiterd` directly.
- **Entry points:** `/check` (key, cost), `/demo`, `/readyz`, `/metrics`, environment configuration.
- **Top risks and mitigations:**
  - Limit bypass through races between replicas: one atomic Lua script per decision, Redis `TIME` as the clock.
  - Memory exhaustion through unbounded keys: keys over 256 bytes and bodies over 4 KiB are rejected; Redis keys self-expire; the in-memory backend sweeps idle keys every minute.
  - Bypass when Redis is down: fail closed by default (ADR 0002).
  - Key spoofing through `X-Forwarded-For`: not trusted unless explicitly configured.
  - Information leaks: errors to clients are generic; `/readyz` does not echo backend errors.

## Deployment expectations

- `/check` and `/metrics` are unauthenticated. Expose them only on a private network or behind an authenticating proxy.
- The image runs as the distroless `nonroot` user; the compose demo also sets `read_only` and `no-new-privileges`.
- Use Redis AUTH/TLS in production. Credentials go in the environment or a secret store, never in the repository.
