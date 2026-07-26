# Redis Budget Generation Recovery

> **Design-time runbook:** the recovery commands, metric names, and deployment
> controls do not exist yet. The implementation must replace every named
> placeholder below with tested, environment-specific commands before enabling
> production paid work. This runbook never authorizes direct Redis key edits,
> `FLUSHDB`, or an unfenced PostgreSQL rebuild.

The Go `storage/redis` package provides the bounded, storage-neutral
`budget-manifest/v1` record and deterministic validator used by this runbook's
manifest checks. It verifies generation/incarnation provenance, config/price
and policy/window hashes, concrete journal/Stream high-water marks, complete
coverage, every expected member, catalog identity, and the conservative
rounding version. `RedisBudgetGenerationPort` atomically stores an immutable
manifest and switches the active pointer, while `RedisBudgetEventPort` tails
the coordination Stream without consumer groups. `BudgetStreamTailer` now
provides the worker-side bounded poll contract: each worker owns an independent
cursor, disabled consumption and retained-stream gaps perform a Redis-only
manifest reload, and a generation-switch hint is never applied to the old
local plan. Recovery captures the live Stream last entry immediately before
the reload instead of reusing the manifest's historical high-water mark; this
prevents a retained-prefix gap from looping forever and preserves hints
appended while the reload runs. A reload with no concrete current cursor fails
closed. The tailer returns its latest cursor for the runtime owner to persist
in the worker lease; hints remain non-authoritative and every paid decision
still goes through the atomic Redis Function.

These adapters do not materialize window keys or execute the Redis admission
Function; runtime composition and the fenced recovery coordinator remain
subject to the procedures below. The event adapter and tailer intentionally
never trim the Stream: a separate retention coordinator must use the minimum
non-expired worker cursor plus a configured safety margin, and remains a
pending Task 6 slice.

## Purpose and safety invariant

PostgreSQL is the durable financial system of record. Redis is the required
production optimization that holds a conservative nano-USD materialization for
fast atomic admission and coordinates worker replicas. A Redis failure is
therefore recoverable, but recovery must never expose budget capacity twice.

The invariant is: paid provider dispatch remains stopped until every active
budget window and open reservation is represented in one verified Redis
generation, bound to the current Redis dataset incarnation and caught up
through a recorded PostgreSQL journal sequence.

Read-only diagnostics may continue. Generate, Compact, cache fills, and any
provider-management refresh that could spend money fail closed while the
budget generation is untrusted. The optional Redis Stream may speed worker
wake-up, but its availability or contents are never recovery authority.

## Evidence to capture first

Before changing deployment state, record a content-free incident bundle:

- incident start time in UTC, deployment/config epoch, worker build, and
  namespace prefixes;
- Redis endpoint identity, server run ID, dataset-incarnation ID, persistence
  status, active-generation ID, manifest digest, journal high-water mark, and
  worker lease/session-roster counts;
- which manifest/member/digest/horizon validation failed;
- PostgreSQL budget-journal maximum sequence and the count of open reservations
  in the configured active horizon, without prompt or provider-response data;
- Temporal task-queue backlog and whether paid-work polling is paused;
- Redis/KMS/PostgreSQL health and the last persistence/restore event.

Do not log credentials, raw Redis values, tenant identifiers, prompts, outputs,
provider request IDs, or unkeyed cache/budget identifiers. Preserve the failed
generation and evidence until the incident owner closes the investigation.

## Classify the condition

1. **Intact generation, including after scale-to-zero or a persistent Redis
   restart.** Manifest, incarnation binding, member catalog, horizon sentinels,
   digests, and high-water mark all validate. Adopt it with Redis-only reads;
   do not query PostgreSQL budget tables or manufacture a new generation.
2. **Verified new empty/incomplete dataset incarnation.** Redis proves that its
   prior persisted dataset was not retained. A fenced cold rebuild is allowed.
3. **Same-incarnation partial loss or unexplained mismatch.** Do not infer an
   empty dataset and do not read PostgreSQL as an online fallback. Restore the
   Redis dataset from its persistence/backup first. If that cannot succeed,
   perform the deliberate replacement procedure below, which requires a full
   fleet quiescence and creates a new dataset incarnation.
4. **Live or reconnecting worker sessions remain.** Do not rebuild. Restore the
   intact generation or quiesce the entire fleet and prove all old sessions can
   no longer write before considering replacement.
5. **PostgreSQL, Redis, blob encryption/KMS, or the journal fence is unhealthy.**
   Keep paid work stopped and escalate; there is no degraded paid-write mode.

## Adopt an intact generation

1. Keep paid polling paused while one worker performs the full manifest check.
2. Compare the configured namespace/config epoch, Redis incarnation, member
   catalog, active-horizon sentinels, safe-integer bounds, manifest digest, and
   stored journal high-water mark.
3. Register the new in-memory worker session and lease without changing the
   active generation.
4. Have every replica independently verify the same manifest. A Stream event
   may wake replicas, but replicas reload the active-generation pointer and
   manifest directly from Redis.
   If a worker's cursor predates the retained Stream prefix, the adapter
   reports a gap and the worker discards its hints before reloading the
   manifest; it never treats a partial Stream read as authorization state.
5. Resume paid polling only after readiness reports the adopted generation.
   Confirm that this path executed zero PostgreSQL budget-table SELECTs.

## Fenced rebuild for a verified new incarnation

1. Prove either a new empty/incomplete Redis dataset incarnation or a cold fleet
   with zero live/reconnecting worker sessions after intact-generation adoption
   failed. Elect exactly one coordinator with the namespaced bootstrap fence;
   all other workers wait with paid polling disabled.
2. Create candidate generation keys under a new immutable generation ID. Never
   overwrite or delete the previous generation in place.
3. Under the exceptional recovery repository path, read from PostgreSQL only
   the active budget horizon, open reservations, and ordered journal tail. Apply
   the versioned conservative conversion: positive amounts round up to nano-USD
   and limits round down. Preserve each operation's applied integer so release
   and reconciliation never recompute it.
4. Verify candidate counts, digests, non-negative balances, safe-integer bounds,
   policy/config epoch, incarnation binding, and journal sequence.
5. Acquire the PostgreSQL advisory fence also taken in shared mode by journal
   writers. Capture the final sequence, idempotently apply every missing journal
   event, re-run the verification, and atomically switch the Redis
   `budget:active-generation` pointer. Append a generation-switch Stream hint.
6. Release the PostgreSQL and Redis fences. Each worker reloads and independently
   verifies the active pointer/manifest before readiness and paid polling resume.
7. Retain the old generation through the incident rollback window. Garbage
   collection may remove it only after no lease, cursor, reservation, or audit
   reference remains.

## Deliberate replacement after same-incarnation corruption

This is an operator-authorized outage procedure, not automatic fallback:

1. Pause paid-work polling and quiesce every Go worker. Prove the Redis lease set
   and persistent session roster contain no process that can reconnect and
   mutate the old generation. If that proof is unavailable, stop and escalate.
2. Preserve/export the corrupted dataset and evidence. Do not repair individual
   members or clear the shared database.
3. Provision an empty Redis dataset with a new verified incarnation under the
   configured persistence, `noeviction`, TLS/auth, ACL, key-prefix, and Function
   digest requirements.
4. Start the fleet in recovery mode and follow the fenced-new-incarnation rebuild
   above. Normal readiness must reject recovery mode after the flip succeeds.

## Verification before closure

- exactly one generation is active and every replica reports its ID/digest;
- the generation is caught up through the PostgreSQL journal maximum observed
  under the final advisory fence;
- active reservations and accounted charges remain conservative and no window
  exceeds its limit;
- replayed journal IDs changed Redis state at most once;
- no provider dispatch occurred while paid polling was paused;
- PostgreSQL budget SELECT instrumentation shows reads only in the approved
  fenced rebuild, not during adoption or normal restart;
- a budget-status query uses Redis and labels values
  `nano_usd_conservative`; and
- the Stream-disabled verification path reaches the same decision.

If verification fails before the active pointer flips, abandon the candidate
and keep the old generation/outage state. If it fails after the flip, pause paid
polling immediately; flip back only when the preserved old generation is still
fully valid for the same incarnation and doing so is protected by the same
fences. Otherwise perform another new-generation rebuild. Escalate any ambiguous
dispatch, missing journal range, unsafe integer, or inability to prove fleet
quiescence; never guess a balance or mark an unknown cost as zero.
