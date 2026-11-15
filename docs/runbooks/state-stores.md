# State stores

Runbooks for the worker's required state: Redis (budget authority), DynamoDB
(cloud request records) and S3 (payloads and results), plus corrupt durable
state. Each store is a required readiness dependency; see
[readiness reason codes](on-call.md#readiness-reason-codes) for what the
`redis`, `cloud_requests` and `blob_store` checks report and why the reason is
not exported. Start from [on-call escalation and severity](on-call.md) for the
rules that apply to every incident.

When a store is unavailable the worker stays unavailable rather than dispatch
paid work without its coordination guarantees. Storage failover, where
configured, covers DynamoDB and S3 only; it never provides Redis or Temporal
failover.

## Redis readiness failures

This covers the "Redis persistence, eviction, memory or Function digest
failures" alert.

- **Signal:** `/health/ready` returns `503` and `llmtw_worker_polling` is `0`
  while `/health/live` stays `200`. Readiness turns only after three
  consecutive failed check rounds.
- **Readiness reason:** the `redis` check reports `policy_mismatch` when Redis
  answers but its persistence (`state.redis.required_persistence`), eviction
  policy (`state.redis.maxmemory_policy`, default `noeviction`) or admission
  code (`function_library`, `admission_version` and `admission_digest`, or the
  Lua script digest) does not match the configuration on the slot owner, one
  of its replicas or any primary, or when the coordination Stream is
  malformed. It reports `unavailable` when Redis, the slot owner or a listed
  replica cannot be reached or the budget authority marker cannot be
  verified, and `timeout` when the check exceeds
  `server.readiness_probe_timeout`. Memory exhaustion does not fail this
  read-only check: under `noeviction` a full Redis rejects budget writes, which
  surface as `state_unavailable` failures in
  `llmtw_activity_failure_total{origin="worker"}`; see
  [Redis growth](budget-and-cost.md#redis-growth-toward-the-sized-capacity).
- **Default severity:** SEV1 when every replica is unready (no paid work
  runs); SEV2 when only some are.
- **Safe action:**
  1. Run the read-only check from a pod with the worker's configuration. It
     runs the same Redis preflight (pinned admission code and persistence
     policy) and reads the authority marker against the cloud receipt:

     ```sh
     llm-temporal-worker budget-initialize --config /etc/llmtw/config.yaml
     ```

     - `Redis initialization preflight failed`: the Redis checks themselves
       fail (reachability, persistence, eviction or admission code).
     - `{"status":"recovery_required",...}`: Redis lost the authority marker;
       follow [Redis budget authority and recovery](redis-budget-generation-recovery.md).
     - `initialization_required` or `incomplete` on a deployment that was
       running: the namespace or receipt does not match; escalate.
     - `Redis budget initialization completed but worker readiness checks failed`:
       the marker is fine but the coordination Stream check fails.
     - `ready`: every Redis check passes from this pod. Look at the
       `cloud_requests` and `blob_store` checks, or at the network path from
       the unready pods.
  2. For a policy mismatch, compare `CONFIG GET appendonly`, `save` and
     `maxmemory-policy` on the slot owner and its replicas with the
     configuration, and `FUNCTION LIST` on every primary with the configured
     library. Restore the configured settings through the platform's Redis
     management, not from the worker.
  3. For a replica that cannot be reached, let the cluster fail it or bring it
     back; readiness recovers once Redis drops it from `CLUSTER SLOTS` or it
     rejoins.
  4. Polling resumes on its own after every check passes again. Confirm
     `llmtw_worker_polling` returns to `1`.
- **Forbidden:** loading or replacing the admission Function from an ad hoc
  script (`FUNCTION LOAD REPLACE`), switching to an evicting
  `maxmemory_policy`, turning persistence off to recover memory, or deleting,
  recreating or editing the authority marker or any budget key. Never run
  `budget-initialize --apply` to "fix" a mismatch: on a ready receipt it
  recovers, and on an unused namespace it initializes, and neither repairs a
  policy setting.

## Continuation corruption or digest mismatch

- **Signal:** `llmtw_activity_failure_total{origin="worker"}` rises with the
  `llm_state_corrupt` Temporal failure type (code `state_corrupt`,
  non-retryable). Causes include a committed request, checkpoint or cache
  record whose blob is missing, a payload that fails decryption, digest or
  tenant checks, and a saved plan or result that does not match its record.
- **Readiness reason:** none. Corruption is found per request, not by the
  readiness checks.
- **Default severity:** SEV2 for an isolated request; SEV1 when many requests
  fail at once, because that usually means a store restore lost objects or a
  durable identity or key changed.
- **Safe action:**
  1. Establish the scope: one request, one tenant, or everything since a
     deployment, configuration change or restore.
  2. Everything since a configuration change: compare the deployed
     configuration with the previous one for durable identity and key
     material (`state.requests.secret`, `namespace`, `request_table`,
     `payload_store`, `continuation.handle_keys`, the result blob store) and
     follow
     [configuration rollback](configuration-and-credentials.md#configuration-rollback-for-process-lifetime-fields).
  3. Everything since a store restore: follow
     [S3 restored with missing objects](#s3-restored-with-missing-objects).
  4. One request: preserve its Temporal workflow and run IDs and escalate to
     the service owner. Do not retry it under a new operation key until the
     owner has confirmed whether its earlier attempt was paid.
- **Forbidden:** deleting the request, attempt or checkpoint record so the
  operation key reads as new work, writing substitute blobs, or rotating
  `state.requests.secret` or `continuation.handle_keys` to get past a decrypt
  or verify failure. Deleting a record can let the same paid work run twice.

## DynamoDB throttling

- **Signal:** AWS reports throttled requests on the request table
  (`ProvisionedThroughputExceededException`, `ThrottlingException`,
  `RequestLimitExceeded` or HTTP 429; in CloudWatch, the table's throttle
  events). The worker keeps raw SDK errors out of logs. Requests whose storage
  call was throttled fail that step with `state_unavailable` (Temporal type
  `llm_provider_transient`, retryable), counted in
  `llmtw_activity_failure_total{origin="worker"}`; Temporal retries the step
  with exponential backoff within the Activity's 24-hour retry horizon.
- **Readiness reason:** the `cloud_requests` check reports `unavailable` if
  its bounded table query is throttled, and readiness turns `503` after three
  consecutive throttled rounds.
- **Default severity:** SEV2.
- **Why failover does not help:** the regional router fails over only on
  unavailable, timeout, 5xx and network errors. Throttling is a definitive
  400-class answer, so it neither fails over to another region nor is retried
  inside the call; it goes straight back to the step.
- **Safe action:**
  1. Raise the table's provisioned capacity or switch it to on-demand
     capacity through the platform's infrastructure tooling. A single-region
     table stays usable while it is `UPDATING`; a global (MRSC) table does
     not open while `UPDATING` (see the next section).
  2. Check for a hot partition: throttling on one partition with spare table
     capacity needs AWS support, not more capacity.
  3. Watch `llmtw_activity_failure_total{origin="worker"}` fall and readiness
     return. Retried steps replay durable work and do not dispatch providers
     again.
- **Forbidden:** editing the `state.requests` table or provider mappings to
  point at a different table, deleting items to reduce load, or terminating
  waiting request workflows. The table binding is durable identity: a new
  table does not contain the in-flight records.

## DynamoDB table UPDATING

- **Signal:** the request table reports status `UPDATING`, during a capacity,
  billing-mode, encryption, stream or table-class change, or global-table
  maintenance. For a global (MRSC) table, new workers fail at startup with an
  open cloud request storage error and Kubernetes restarts them with
  back-off; configuration reloads that must open storage fail with
  `cause=dependency`.
- **Readiness reason:** none for running workers: readiness keeps the handle it
  opened earlier and lets its bounded table query decide, so a worker that was
  ready stays ready while DynamoDB serves that query. A single-region table in
  `UPDATING` opens normally.
- **Default severity:** SEV3; SEV2 if a global-table update blocks a needed
  rollout or scale-up.
- **Safe action:**
  1. Wait for the table to return to `ACTIVE`.
  2. Avoid rollouts, scale-ups and node drains while a global table is
     `UPDATING`; new pods cannot start until it is `ACTIVE`, unless
     `failover` is configured and at least one replica is `ACTIVE`.
  3. If a reload failed with `cause=dependency` during the update, repeat it
     afterwards with `SIGHUP`.
  See [global tables in `UPDATING`](../reference/cloud-request-repository.md#global-tables-in-updating).
- **Forbidden:** writing to a global table while it converts to MRSC, or
  forcing startup by removing `allow_mrsc` or the failover settings.

## S3 unavailable

- **Signal:** `/health/ready` returns `503` and `llmtw_worker_polling` is `0`
  after three failed rounds; requests that read or write payloads or results
  fail with `state_unavailable` (retryable) in
  `llmtw_activity_failure_total{origin="worker"}`.
- **Readiness reason:** the `blob_store` check (`HeadBucket` on the result
  bucket) or the `cloud_requests` check (a read from the payload bucket)
  reports `unavailable` or `timeout`.
- **Default severity:** SEV1 when every replica is unready; SEV2 otherwise.
- **Safe action:**
  1. Check S3 health and the worker's IAM access to the configured buckets
     for its region.
  2. With `failover` configured, reads move to replica buckets and writes need
     one available bucket; readiness needs one reachable, authorized bucket.
     Without it, wait for the bucket to recover. Retried steps resume from
     durable state.
  3. After recovery, confirm readiness and that `state_unavailable` failures
     stop.
- **Forbidden:** pointing `blob_store.s3` or the payload store alias at a
  different bucket on a running deployment. Bucket location is durable
  identity, and existing references would read as missing.

## S3 restored with missing objects

- **Signal:** after a bucket restore, lifecycle rule or manual deletion,
  requests fail with `llm_state_corrupt` (code `state_corrupt`,
  non-retryable). A committed reference whose object is gone is never read as
  an absent record. With asynchronous cross-region replication, a payload
  acknowledged in one region can be missing in the other after a regional
  failure.
- **Readiness reason:** none. The readiness read uses a probe key whose
  absence is normal, so readiness stays `200`.
- **Default severity:** SEV1 when many objects are missing; SEV2 for a few.
- **Safe action:**
  1. Stop the cause: disable the lifecycle rule or the deletion job.
  2. Restore the missing objects to their original keys from bucket versioning
     or backup. Objects are immutable and checked against their digests, so
     only the original bytes are accepted.
  3. Once objects are back, a retry of the same operation key reads them.
     Requests whose payloads cannot be restored stay failed; record their
     workflow and run IDs and escalate to the service owner.
- **Forbidden:** deleting the records that reference missing objects,
  uploading placeholder objects, or retrying the work under a new operation
  key without confirming whether the original attempt was paid. Missing
  committed payloads fail closed precisely so that they never authorize
  duplicate provider work. See the
  [blob garbage-collection boundary](../reference/blob-garbage-collection.md).
