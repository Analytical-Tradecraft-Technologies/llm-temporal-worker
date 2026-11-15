# Fleet upgrade and rollback

This runbook upgrades or rolls back one worker fleet: every worker that polls
the same `temporal.task_queue` and shares one Redis key prefix and one cloud
request namespace. It covers the binary, its configuration, the Redis code it
calls, and the durable record formats it writes. Steps are written against the
[Kubernetes base](../../golang/deploy/kubernetes/base), but the order applies
to any deployment.

The detail behind each step lives in the reference documents this page links;
where they disagree with this summary, they win. Work through the
[pre-rollout checklist](#pre-rollout-checklist) before every upgrade, and read
the [no-rollback gates](#no-rollback-gates) before you start, not when you
need to roll back.

## Pre-rollout checklist

Compare the current and target configurations field by field (step 1 of the
[upgrade procedure](#upgrade-procedure) shows how) and answer each item.

1. **Redis admission code.** Does the target release's
   `state.redis.function_library` differ from the running one? If so, load it
   first ([step 2](#2-load-the-redis-admission-code)). The binary and the three
   fields `function_library`, `admission_version` and `admission_digest` must
   change together: a worker refuses code that does not match its own build.
2. **`server.inline_payload_bytes`.** Raising it needs no drain. Lowering it
   needs a [drain](#4-drain-when-the-change-requires-it), even after #1806.
   Since #1806 each workflow run records the limit it started with (the
   `pinned-inline-payload-limit-v1` version marker) and replays under it, so a
   restart no longer breaks replay. But every new Activity execution still
   applies the worker's current limit, so an open request whose next Activity
   argument is above the new limit fails with `llm_invalid_argument`, and a
   provider attempt it had paid for is then recoverable only through the
   pending-request index or `generation-recover`. Runs started by a release
   without the marker still replay under the current limit and fail with a
   non-determinism error. A reload cannot change this field. See
   [the configuration reference](../reference/configuration.md#complete-shape)
   (the paragraphs after the complete shape) and
   [the release runbook](../release/runbook.md#configuration-changes-in-a-rollout).
3. **`limits.provider_response_bytes`.** Releases that include #1851 bound it
   at load time so that a full response fits in the 8 MiB durable record that
   saves it. An explicit value above the bound fails to load, and the error
   names the largest allowed value. This includes `16777216`, the 16 MiB
   default before #1851, which the Kubernetes base and example configurations
   set explicitly until then.
   Running the target binary's `validate-config` on the target configuration
   catches this before the rollout. `limits.max_output_tokens` must also stay
   below both `server.inline_payload_bytes` and
   `limits.provider_response_bytes` ([output reservations](../reference/configuration.md#output-reservations)).
4. **Any change to `config_version`.** Each request records the configuration
   digest it was prepared under. Work with no unfinished paid attempt is never
   re-planned under another configuration: a worker on the new configuration
   answers it with a retryable `state_unavailable` for its first 15 minutes,
   which covers a rolling replacement, and then fails it with a non-retryable
   `configuration` error. The caller submits it again under a new operation
   key. Requests still waiting for budget or a retry when the old workers stop
   can therefore fail after the rollout. If that is not acceptable,
   [drain](#4-drain-when-the-change-requires-it) first. To see whether an
   upgrade changes it, compare the `valid config version` that the running and
   target binaries print for their configurations.
5. **Endpoint recovery identity.** An attempt that already reached its provider
   is polled or recovered under a new configuration only while its route and
   its endpoint's recovery identity are unchanged. `timeout`, `langfuse`,
   `optional` and `outbound_hosts` are not part of that identity. Every other
   endpoint field (family, base URL, region, account region, AWS workspace,
   auth reference, service classes and tiers, capability profile, price
   catalog, provider storage and extensions) is. Changing one makes in-flight
   paid attempts on that endpoint wait with a retryable `state_unavailable`
   until compatible settings return; they are never submitted again. Change
   these fields only after a [drain](#4-drain-when-the-change-requires-it), or
   accept the wait; restoring the old settings resumes the waiting work. See
   [storage and callers](../reference/mvp-v1-status.md#storage-and-callers).

   The four excluded fields are safe to change with paid work in flight only
   for attempts planned by a build that has #1640. A plan saved by an earlier
   build carries only the digest of the whole endpoint configuration, and
   recovery accepts it only while that whole configuration is unchanged. A
   change to any endpoint field, these four included, stalls such an attempt
   the same way. Until no attempt planned before #1640 can still be in flight,
   keep the whole endpoint configuration unchanged or drain first.
6. **Durable identity: never change in an upgrade.** Requests, reservations,
   checkpoints and results exist only under the identity they were written
   with, and nothing copies them:
   - `environment` and `temporal.namespace` are part of every durable scope.
     A restart with a new value invalidates every checkpoint handle and turns
     every cached response into a miss
     ([durable scope identity](../reference/configuration.md#durable-scope-identity));
   - the material behind `state.redis.key_secret` and `state.requests.secret`
     ([Redis namespace identity](../reference/configuration.md#redis-namespace-identity)).
     Replacing either is a separate migration, not an upgrade
     ([rotating runtime credentials](rotating-runtime-credentials.md));
   - `state.kind`, `state.redis.key_prefix` (and `LLMTW_REDIS_KEY_PREFIX`),
     `state.redis.admission_hash_tag`, the `state.requests` provider block,
     `request_table`, `payload_store` and `namespace`, and the result store's
     `blob_store.kind`, `blob_store.file.root` and `blob_store.s3` bucket,
     region and prefix ([state namespace selection](../reference/configuration.md#state-namespace-selection)).
7. **Budget window identities.** A window's spend is keyed by
   `<policy id>/<window id>`. Adding, changing or removing an `id`, or changing
   `duration` or `bucket` on a window without one, starts that window from
   zero. A running worker rejects a reload that reuses an `id` with another
   geometry, but a restart does not, and a rolling replacement restarts every
   worker: never reuse an explicit `id` with another geometry
   ([budget window identity](../reference/configuration.md#budget-window-identity)).
8. **Key sets: add, then promote, never drop early.** For
   `temporal.payload_codec` keys and `continuation.handle_keys`, any listed
   key decrypts or verifies and only the `primary: true` key writes. Add a new
   key as non-primary on every worker (and, for the codec, every caller) in one
   rollout, make it primary in a later one, and remove the old key only after
   nothing it wrote is still in use: Temporal history within its retention
   period for the codec, outstanding checkpoint handles for handle keys. The
   codec is process-lifetime, so each step is a rollout
   ([Temporal payload encryption](../reference/configuration.md#temporal-payload-encryption)).
9. **`state.requests.parent_snapshot_storage: blob`.** Set it only after every
   worker in the fleet runs a build that has the field, and only if you will
   never roll back to a build without it. See
   [no-rollback gates](#no-rollback-gates).
10. **Worker versioning.** Changing `temporal.worker.versioning` (or the
    controller-supplied `TEMPORAL_DEPLOYMENT_NAME` and
    `TEMPORAL_WORKER_BUILD_ID`) needs a restart. Enabling it does not move
    existing unversioned workflows: keep an unversioned worker until they
    drain ([worker versioning](../reference/configuration.md#optional-temporal-worker-versioning)).
11. **Release notes.** Look for a new durable record format, a new Redis
    library, or a workflow change, and apply the matching
    [no-rollback gate](#no-rollback-gates). Also check Redis memory headroom:
    an upgrade from a release that wrote unbounded coordination Streams needs
    headroom if Redis is already over `maxmemory`
    ([readiness and Redis budget policy](../reference/configuration.md#readiness-and-redis-budget-policy)).

## Upgrade procedure

### 1. Pre-checks

- The fleet is healthy: every pod's `/health/ready` returns `200`,
  `llmtw_worker_polling` is `1` on every pod,
  `llmtw_config_reload_total{outcome="failure"}` is not increasing, and the
  task-queue backlog (Temporal's task-queue backlog metrics for
  `temporal.task_queue`) is at its normal level.
- Record the running image digest and configuration. They are the rollback
  target.
- With the target binary, check the target configuration:

  ```sh
  llm-temporal-worker version
  llm-temporal-worker validate-config --config ./target-config.yaml
  llm-temporal-worker print-effective-config --config ./current-config.yaml > current.json
  llm-temporal-worker print-effective-config --config ./target-config.yaml > target.json
  ```

  `validate-config` prints `valid config version <digest>`. It does not read
  secrets, catalogs or dependencies; worker startup is the full check. Diff
  the two canonical JSON documents and classify every changed field with the
  [table in step 3](#3-classify-the-configuration-changes) and the
  [checklist](#pre-rollout-checklist).

### 2. Load the Redis admission code

Skip this step when `state.redis.function_library` is unchanged.

The worker never loads, replaces or rewrites Redis code; it only verifies and
calls it. The library and function names carry the first 64 bits of the Lua
source's SHA-256 (`llmtw_admission_<tag>` and `admission_<tag>`), so each
release that changes the Lua has its own library, and the old one stays loaded
beside it. See
[readiness and Redis budget policy](../reference/configuration.md#readiness-and-redis-budget-policy).

1. Build the library from the target release's
   `golang/storage/redis/functions/admission.lua`, exactly as the
   `redis-function-provisioner` service in `golang/compose.yaml` does: the
   line `#!lua name=<function_library>`, the line
   `redis.register_function('<admission_version>', function(KEYS, ARGV)`, the
   source, then a newline and `end)` and a newline. Its SHA-256 must equal the
   target `admission_digest`.
2. `FUNCTION LOAD` it without `REPLACE` (the name is new). With Redis Cluster,
   load it on every primary: readiness verifies the library on every primary.
   In `admission_mode: lua`, load the raw source with `SCRIPT LOAD` instead;
   its `admission_digest` is the SHA-256 of that source.
3. Verify with the target binary and target configuration. Without `--apply`
   the command only reads:

   ```sh
   llm-temporal-worker budget-initialize --config ./target-config.yaml
   ```

   It reports `{"status":"ready","applied":false}` when the code, persistence
   and eviction policy and budget authority are all in place. It fails with
   `Redis initialization preflight failed` when the library is missing, its
   code does not match `admission_digest`, or Redis does not meet the
   configured persistence and eviction policy.
4. Do not `FUNCTION DELETE` the old library. Old workers keep calling it
   through the rollout, and a rollback needs it
   ([step 7](#7-post-checks)).

The worker's other Lua (provider state, model catalog, continuation) is sent
with `EVALSHA`/`EVAL` and needs no provisioning. The `budget_status_v3`
Function is used only by embeddings that wire a Redis budget status reader;
the production CLI does not. Such an embedding preloads both the v2 and v3
libraries during the rollout ([persisted queries](../reference/persisted-query-service.md)).

### 3. Classify the configuration changes

The production `worker` reloads on `SIGHUP` and when its `--config` file
changes; see [configuration reload](../reference/cli.md#configuration-reload).
The Kubernetes base generates an immutable, content-hashed ConfigMap, so every
configuration or catalog change there rolls the pods anyway. The distinction
still matters for two reasons: during a rollout, old and new workers run
different configurations side by side; and a deployment that edits the file in
place applies reload-safe changes without a restart.

| Change | How to apply |
| --- | --- |
| Routes, models, capability and price catalogs, budgets (subject to [window identity](#pre-rollout-checklist)), `authorization`, `limits` other than the inline limit, endpoint fields other than `outbound_hosts` (subject to [recovery identity](#pre-rollout-checklist)), `continuation.handle_keys` (add, then promote), `state.requests.parent_snapshot_storage` (gated), `state.redis.addresses`, Redis credentials, TLS, connection limits and timeouts, `blob_store.s3.auth` | Reload-safe. A rejected reload keeps the old snapshot serving, records `llmtw_config_reload_total{outcome="failure"}` and logs `configuration reload failed` with a `cause` and, for validation, a `config_field`. A successful one logs `configuration reloaded` with its `config_version` |
| `state.redis.function_library`, `admission_version`, `admission_digest`, `admission_mode` | Only together with the binary that pins that code, in a rollout. A worker with code that does not match its build fails startup, or rejects the reload with `cause=dependency` |
| `environment` (see the checklist), `server` listener addresses, `server.shutdown_timeout`, readiness probe settings, `server.inline_payload_bytes`; the `temporal` connection, task queue, `temporal.payload_codec` and `temporal.worker` settings including `versioning`; `telemetry`; the set of `endpoints` and each `endpoints.*.outbound_hosts`; `state.redis.service_mesh`; `state.requests.provider.aws` profile, `temp_directory`, `failover` and `allow_mrsc`; `blob_store.s3.failover`; environment variables | Rollout. Reload rejects them with `cause=process_lifetime` and the field in `config_field` |
| Durable identity (checklist item 6) | Never in an upgrade. Reload rejects it with `cause=process_lifetime`, but a restart accepts it and strands everything written under the old identity |

### 4. Drain when the change requires it

Drain before the rollout when it lowers `server.inline_payload_bytes`, changes
an endpoint's recovery identity while paid work is in flight, retires the last
unversioned worker after enabling worker versioning, or when the release notes
say so. Drain too before any `config_version` change if unpaid requests must
not fail with `configuration` (checklist item 4). Otherwise skip to step 5.

1. Stop callers from starting new `llm.generate.workflow.v1` and
   `llm.compact.workflow.v1` workflows.
2. Let the old workers finish every open workflow. Count what is left with the
   Temporal CLI:

   ```sh
   temporal workflow count --namespace <temporal.namespace> \
     --query 'TaskQueue="<temporal.task_queue>" AND ExecutionStatus="Running"'
   ```

3. The drain is complete when that count is `0` and the task-queue backlog is
   empty. Do not terminate workflows to get there: a terminated request with a
   paid attempt is then recoverable only through the pending-request index
   ([terminated workflow recovery](terminated-workflow-pending-recovery.md))
   or `generation-recover` ([CLI reference](../reference/cli.md#generation-recover)).

### 5. Roll the workers

Change the image digest and the configuration in one rollout, so that every
new worker starts with the configuration its binary expects.

- The Kubernetes base uses `maxUnavailable: 0`, `maxSurge: 1` and
  `minReadySeconds: 10`, so each new pod must be ready before an old one stops.
  Readiness requires the worker to be polling and Redis, cloud request storage
  and the result blob store to pass their checks, including the admission
  code ([probes and shutdown](../architecture/deployment-and-operations.md#probes-and-shutdown)).
- Each old pod receives `SIGTERM`: readiness fails, polling stops, in-flight
  Activities get `temporal.worker.graceful_stop_timeout`, and the process exits
  within `server.shutdown_timeout`, which `terminationGracePeriodSeconds` must
  exceed. An Activity cut off by the grace period is retried by Temporal on
  another worker. A saved provider attempt is polled or recovered, never
  submitted again, and an uncertain submission keeps its original charge.
- Stop the rollout if a new pod does not become ready, if
  `llmtw_config_reload_total{outcome="failure"}` rises, or if
  `llmtw_activity_failure_total{origin="worker"}` rises above its baseline.
  Then follow the [rollback](#rollback).
- With worker versioning, start the new build ID beside the old one and move
  Temporal's routing to it with the worker deployment APIs or the Temporal
  Worker Controller. Existing runs stay pinned to the old version; keep its
  workers until step 6.

### 6. Verify the old workers drained

- Without versioning: no pod runs the old image digest, and the sum of
  `llmtw_worker_polling` across the fleet equals the replica count. Each
  pod's image can also be checked with `llm-temporal-worker version` run in
  the container.
- With versioning: keep the old version's workers until Temporal reports that
  version drained, then stop them within their shutdown deadline.
- In both cases the task-queue backlog returns to its normal level and
  `llmtw_heartbeat_age_seconds` shows no stuck Activities.

### 7. Post-checks

- `/health/ready` returns `200` and `llmtw_worker_polling` is `1` on every
  pod.
- No new `llmtw_config_reload_total{outcome="failure"}`, and no rise in
  `llmtw_activity_failure_total{origin="worker"}`,
  `llmtw_provider_attempt_total{outcome="failure"}`,
  `llmtw_ambiguous_total` or `llmtw_cost_status_total{status="unknown"}`. A
  rollout that cut off provider calls can add ambiguous attempts; investigate
  each one ([alerts](../architecture/deployment-and-operations.md#alerts)).
- `llmtw_provider_poll_total` and `llmtw_operation_state_total` keep moving,
  so work that was in flight is finishing.
- Only once the rollback window has closed: `FUNCTION DELETE` the old admission
  library on every primary, after no worker of the old release remains.
- Writer-side changes that close off a rollback (`parent_snapshot_storage:
  blob`, promoting a new primary key) go in a later, separate change, after
  this release has settled.

## Rollback

### No-rollback gates

Check these before deciding to roll back. A gate that has been passed means
rolling back below that release is unsafe, or stalls work, until you roll
forward again.

- **Workflow changes.** Workflow code changes are guarded with Temporal version
  markers, such as `pinned-inline-payload-limit-v1` from #1806, so open runs
  replay on newer code. The reverse does not hold: older code does not know
  the newer branches, so a run started or advanced on the new release can fail
  with a non-determinism error on the old one. Drain before rolling back
  across a workflow change, or use worker versioning, which keeps those runs
  pinned to the new version while new runs are routed back to the old one.
- **New-format rows.** A release that writes a durable record form older
  builds cannot read is rolled out readers first: every worker runs a build
  that reads the new form before any worker writes it. Once one such row
  exists, rolling back below the reader release is unsafe, because older
  builds cannot read the row.
- **`blob` parent snapshots.** Once any worker with
  `state.requests.parent_snapshot_storage: blob` has prepared a request, do not
  roll back to a build without the field: it cannot read a referenced
  preparation. Setting the field back to `inline` affects only new
  preparations ([cloud request repository](../reference/cloud-request-repository.md)).
- **Keyring versions.** Never roll back to a configuration that lacks a key
  that may have written live data: a `temporal.payload_codec` key that
  encrypted history still within retention, or a `continuation.handle_keys`
  key that signed outstanding handles. Moving `primary: true` back is safe
  while both keys stay listed. The managed storage keyring (#861) is a
  proposed design and is not implemented; when it ships, rolling back below
  the release that reads its new blob format is forbidden once any worker has
  written that format, and a promotion is undone by publishing a new keyring
  version, never by moving `AWSCURRENT` back
  ([durable-storage key management](../architecture/durable-storage-key-management.md#rollback)).
- **Endpoint recovery digest.** Since #1640, plans record a digest of only the
  endpoint's recovery identity. A build without #1640 cannot match it: after a
  rollback below #1640 it answers those in-flight attempts with a retryable
  `state_unavailable` until a newer worker returns. Nothing fails and nothing
  is submitted again, but that paid work stalls until you roll forward. The
  older build compares only the digest of the whole endpoint configuration, so
  rolling back with any endpoint field changed, including `timeout`,
  `langfuse`, `optional` or `outbound_hosts`, also stalls its own attempts.
- **Redis admission code.** A rollback needs the old admission library still
  loaded. If it was deleted, load it again ([step 2](#2-load-the-redis-admission-code))
  before rolling back.

### Rollback procedure

1. Check the gates above. If one has been passed, roll forward with a fix
   instead, or drain first where the gate allows it.
2. Confirm the old admission library is still loaded: run the old binary's
   `budget-initialize --config` (without `--apply`) against the old
   configuration and expect `{"status":"ready","applied":false}`.
3. Restore the recorded image digest together with its recorded
   configuration, including its `function_library`, `admission_version` and
   `admission_digest`. Revert any change that needed a drain, such as a lowered
   inline limit or an endpoint recovery-identity change, only after the same
   drain. Restoring the old configuration is itself a `config_version` change,
   so checklist item 4 applies to requests the new release prepared.
4. Roll the workers as in [step 5](#5-roll-the-workers), or, with worker
   versioning, route new runs back to the old build ID and keep the new one
   until Temporal reports it drained.
5. Run the [post-checks](#7-post-checks). Keep the new release's admission
   library loaded until you decide not to roll forward again.

Lost Redis accounting is a separate procedure; see the
[Redis budget authority runbook](redis-budget-generation-recovery.md).
