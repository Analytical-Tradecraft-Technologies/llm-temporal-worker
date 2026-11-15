# On-call escalation and severity

This page is the entry point for anyone paged on the worker. It defines
severities, who to escalate to, the rules that apply to every incident, and
where each runbook lives. Every alert in the
[alerts catalog](../architecture/deployment-and-operations.md#alerts) links to
the runbook section that handles it.

## Severity definitions

Severity is about money, data and callers, not about how noisy an alert is.
Pick the highest level that matches, and raise it as soon as a higher one
applies.

| Severity | Meaning | Examples | Response |
| --- | --- | --- | --- |
| SEV1 | Paid work may be duplicated or charged wrongly, durable state may be lost, or every worker has stopped serving | Redis budget authority lost or fenced; a durable identity field changed under running traffic; continuation or request state corrupt across many requests; all replicas unready | Page the primary and secondary on-call and the service owner now. Open an incident channel. Work continuously until mitigated |
| SEV2 | Callers are failing or waiting, or one dependency or provider is degraded, with no evidence of wrong charges or lost state | One provider failing or held; budget denials blocking a tenant; DynamoDB throttling; task queue backlog growing; a failed configuration reload of a needed change | Primary on-call acknowledges within 15 minutes and works in business and out-of-hours time. Escalate to SEV1 if it spreads or touches money |
| SEV3 | A warning that needs action before it becomes an outage | Catalog price expiry approaching; Redis growth trending toward its sized capacity; isolated priority downgrades; a single unknown-cost settlement | Handle in business hours. Open a tracking issue |
| SEV4 | Information only | A route circuit opening and closing again; an expected access pattern | Note it; no page |

Each runbook section gives a default severity. Use judgment: a SEV3 signal on
every replica at once can be a SEV1.

## Escalation

Escalate by role. Deployments fill in the people and contact routes for each
role in their own on-call tool; this repository names no individuals.

1. **Primary on-call** acknowledges the page, sets the severity, and follows
   the runbook.
2. **Secondary on-call** joins for SEV1, or when the primary has not
   mitigated a SEV2 within one hour.
3. **Service owner** (the team that owns this worker) joins every SEV1, any
   incident whose runbook says "escalate", and any decision to change a
   process-lifetime or durable identity setting.
4. **Provider account owner** handles provider credentials, credit, billing
   and quota with the provider.
5. **Platform owners** handle Redis, DynamoDB, S3, Temporal and Kubernetes
   when the fault is in that service.
6. **Security** joins for unexpected access patterns, scope denials from an
   unknown caller, or any suspected credential leak.

Hand over explicitly: say what you changed, what you are watching, and what
the next check is.

## Rules for every incident

These hold in every runbook. A runbook may add more.

- **Never hand-edit Redis budget state.** Do not delete, rename, `EXPIRE` or
  rewrite `durable-budget`, `budget:` or authority-marker keys, and do not
  flush the database. Redis is the budget authority; recovery goes through
  [`budget-initialize`](../reference/cli.md#budget-initialize) only. See the
  [Redis recovery boundary](redis-budget-generation-recovery.md).
- **Never reset receipts or records.** Do not delete the cloud budget
  initialization receipt, request or attempt records, pending index rows,
  checkpoints or blobs to get past an error. They are the evidence that paid
  work happened.
- **Never treat an unknown cost as zero**, and never lower a conservative
  charge by hand. Exact-cost correction is not implemented
  ([#819](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/819)).
- **Never resubmit paid work by hand** outside the worker. A retry through the
  same operation key is the only safe way to run a request again.
- **Never change a durable identity setting** (`environment`,
  `temporal.namespace`, `state.redis.key_prefix`, `state.redis.key_secret`,
  `state.requests` namespace, secret, table or payload store, or the result
  blob store location) to escape an incident. It does not move existing state,
  and work already in flight would read as absent. See
  [configuration rollback](configuration-and-credentials.md#configuration-rollback-for-process-lifetime-fields).
- **Never terminate or reset request workflows** to clear an alert. A
  terminated workflow leaves its pending attempt and charge with nothing
  watching them until an operator runs
  [pending-request recovery](terminated-workflow-pending-recovery.md).
- **Keep content out of the incident.** Do not copy prompts, outputs,
  credentials, provider request bodies or raw provider identifiers into
  tickets or chat. Logs and metrics already carry only bounded fields.

## Readiness reason codes

`/health/ready` returns `200 ok` or `503 not ready` and nothing else. Behind it,
the runtime checks three required dependencies: `redis`, `cloud_requests` and
`blob_store`. Each check reports one of four reasons:

| Reason | Meaning |
| --- | --- |
| `ready` | The check passed |
| `unavailable` | The dependency could not be reached or refused the read |
| `policy_mismatch` | Redis answered but its persistence, eviction or admission code does not match the configuration |
| `timeout` | The check did not finish within `server.readiness_probe_timeout` |

The dependency and reason are internal: they are not in the HTTP response,
metrics or logs. Readiness turns `503` and polling stops
(`llmtw_worker_polling` at `0`) only after three consecutive failed check
rounds; one success resets the count. `/health/live` stays `200`. To find
which check fails, reproduce it with the read-only tools named in each
runbook, for example
[`budget-initialize`](../reference/cli.md#budget-initialize) without `--apply`
for Redis.

`llmtw_worker_polling` at `0` with readiness `503` can also mean the Temporal
SDK stopped the worker; see
[Temporal namespace outage](temporal.md#temporal-namespace-outage).

Alerts that do not involve a required dependency leave readiness at `200`;
their runbooks say "Readiness reason: none".

## Evidence to capture

Before changing anything, record:

- the worker version (`llm-temporal-worker version`) and the running
  `config_version` from the latest `configuration reloaded` log or from
  `llm-temporal-worker validate-config` on the deployed file;
- incident start time, affected replicas and the alert's metric values;
- the bounded log fields the runbook names (`cause`, `config_field`,
  `error_code`, `phase`, `endpoint`, `route_id`, `tenant_hash` and so on);
- the Temporal workflow and run IDs of affected requests, from Temporal itself.

## After the incident

Every SEV1 and SEV2 gets a written review within five business days: timeline,
impact (including any money charged conservatively), root cause, what the
runbook got wrong, and follow-up issues. Fix the runbook in the same change
as any follow-up that alters its procedure.

## Runbooks

| Area | Runbook |
| --- | --- |
| Uncertain provider outcomes | [Stuck ambiguous attempt](stuck-ambiguous-attempt.md) |
| Budgets, cost and Redis growth | [Budget and cost](budget-and-cost.md) |
| Redis budget data loss and failover | [Redis budget authority and recovery](redis-budget-generation-recovery.md) |
| Terminated or timed-out request workflows | [Terminated workflows and pending-request recovery](terminated-workflow-pending-recovery.md) |
| Providers, routes and catalogs | [Providers and routing](provider-and-routing.md) |
| Redis readiness, DynamoDB, S3 and corrupt state | [State stores](state-stores.md) |
| Temporal, stuck Activities and backlog | [Temporal](temporal.md) |
| Configuration reload, rollback and credential rotation | [Configuration and credentials](configuration-and-credentials.md) |
| Request scope and access audit | [Access](access.md) |
