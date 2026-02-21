# Security Model

## Current State

Floe currently provides an **auth scaffold**, not full identity verification.

What exists now:

- request context classification (public vs authenticated-context)
- tiered request limits
- owner propagation field into upload session/finalization flow

What does **not** exist yet:

- JWT signature/issuer/audience verification
- API key database validation/revocation
- wallet challenge-signature verification
- strict authorization on file reads by owner

## Risk Profile

At scaffold stage, auth-like headers can elevate request tier unless protected by upstream controls.

Use this stage for:

- internal testing
- trusted environments
- integration scaffolding for real auth providers

## Recommended Hardening Path

1. Add verified principal layer:
- JWT verification middleware
- API key lookup with hashed-at-rest keys
- wallet challenge flow

2. Enforce authorization:
- bind `owner` to verified principal
- add read-access policies for `/v1/files/*`

3. Add abuse controls:
- per-principal quotas/credits
- stricter edge rate limits and WAF rules

4. Add auditability:
- principal-aware structured logs
- security event metrics and alerts

## Secrets and Key Handling

- never commit `.env` with live secrets
- rotate Sui and Redis credentials regularly
- restrict environment access in deployment platform
