# Access

Runbooks for request scope denials and unusual access patterns. The worker
trusts Temporal to authenticate callers and checks each request's tenant and
project against `authorization.allowed_scopes` before any storage read; see
[caller authorization](../reference/configuration.md#caller-authorization) and
[access audit events](../reference/durable-v1-runtime.md#access-audit-events).
Start from [on-call escalation and severity](on-call.md) for the rules that
apply to every incident.

Access events carry only truncated hashes: each `*_hash` field is the first 16
hexadecimal characters of the SHA-256 of the raw identifier. To find the hash
of a tenant or project you already know, without sending it anywhere:

```sh
printf '%s' 'TENANT_ID' | shasum -a 256 | cut -c1-16
```

The events are best-effort logs, not a durable audit store: a deployment whose
log level is `warn` or above keeps denials but drops the allowed events.

## Request scope denials

This covers denials of a caller outside `authorization.allowed_scopes`, or a
misused execution reference.

- **Signal:** `request scope access decision` warnings with `outcome=denied`,
  grouped by `tenant_hash`, `project_hash` and `activity_type`. Each denied
  Activity attempt also counts in `llmtw_activity_failure_total{origin="caller"}`
  (code `permission_denied` raised before dispatch). Denied control queries log
  `control query access decision` with `outcome=denied` and `query_kind`.
- **Readiness reason:** none.
- **Default severity:** SEV3 for a known caller whose scope was not yet
  added; SEV2 when production callers are denied after a configuration
  change; involve Security (SEV2 or higher) when the tenant or project hash
  matches no known caller.
- **Safe action:**
  1. Hash the expected tenant and project identifiers and compare them with
     the denied hashes.
  2. A known caller missing from the allowlist: add the exact tenant and
     project pair to `authorization.allowed_scopes` through the normal
     configuration review, reload, and confirm
     `llmtw_config_reload_total{outcome="success"}`.
  3. Denials that started right after a reload: compare the previous and
     current `allowed_scopes`; revert if a pair was dropped by mistake.
  4. An unknown hash, or many hashes from one source: treat it as a possible
     misuse of the Temporal namespace. Involve Security and the Temporal
     platform owner to identify which client started the workflows.
- **Forbidden:** widening `allowed_scopes` to a wildcard or to pairs no one has
  asked for in order to silence denials, or turning off
  `authorization.mode: trusted_temporal`. The scope check is the worker's only
  tenant boundary.

## Unusual access to a tenant or checkpoint

- **Signal:** `request scope access decision` (`outcome=allowed`) and
  `request access completed` info events, grouped by `tenant_hash`,
  `actor_hash` or `parent_checkpoint_hash`. Examples: many distinct
  `actor_hash` values continuing one `parent_checkpoint_hash`; one
  `actor_hash` touching many tenants; a sudden volume change for one tenant.
  Completed events also carry `operation_id_hash`, `checkpoint_hash`,
  `status`, `error_code` and the `route_id` that served the request.
- **Readiness reason:** none.
- **Default severity:** SEV3 to investigate; SEV2 with Security when the
  pattern suggests a leaked checkpoint handle or caller credential.
- **Safe action:**
  1. Confirm the pattern over a longer window and check whether it matches a
     known batch job or a new integration with the owning team.
  2. For a suspected leaked checkpoint handle, identify the callers from
     Temporal (the workflows that carried that `parent_checkpoint_hash`) and
     involve Security.
  3. For a suspected leaked Temporal client credential, the Temporal platform
     owner revokes it; the worker's scope check limits the damage to the
     allowed tenant and project pairs.
- **Forbidden:** rotating `continuation.handle_keys` to cut off one handle.
  That invalidates every outstanding handle for every tenant; escalate to the
  service owner instead. Do not copy raw identifiers recovered during the
  investigation into tickets or chat; use the hashes.
