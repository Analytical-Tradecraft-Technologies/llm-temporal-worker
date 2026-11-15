# Budget and cost

Runbooks for budget denials, conservatively charged unknown costs and Redis
growth. Redis is the budget authority and cloud request records hold the
evidence of every paid attempt; see
[pricing and budgets](../architecture/pricing-and-budgets.md) and
[Redis budget leases](../reference/redis-budget-leases.md). Start from
[on-call escalation and severity](on-call.md) for the rules that apply to
every incident.

## Budget denials

- **Signal:** `llmtw_budget_admission_total{policy,outcome="denied"}` rises
  for a policy, and `llmtw_activity_failure_total{origin="budget"}` counts
  Activity attempts that failed with `budget_denied` (Temporal type
  `llm_budget_wait`). A request waiting for capacity adds one `denied` per
  acquisition retry, so the counter measures retries, not requests.
- **Readiness reason:** none. Denials are normal admission decisions and leave
  readiness at `200`.
- **Default severity:** SEV3 when a tenant reaches a limit it is meant to
  reach; SEV2 when production traffic is blocked unexpectedly.
- **Safe action:**
  1. Identify the `policy` label and the matching `budgets.policies[].id` in
     the running configuration.
  2. Decide with the budget owner whether the limit is right. Denied requests
     wait with a Temporal timer and retry on their own; they do not occupy a
     worker slot, and they succeed once the window has capacity.
  3. To raise a limit, change only that window's `limit_usd` and reload. The
     window keeps its identity and its recorded spend. Confirm
     `llmtw_config_reload_total{outcome="success"}` and the
     `configuration reloaded` log, then watch denials fall for new requests.
  4. A raised limit does **not** release requests that are already waiting.
     Each one keeps the budget plan saved on its first acquisition, and that
     plan's reservation carries the limit in force when it was planned, so
     its retries are still judged against the old limit. The reload also
     changes the configuration digest, so a waiting request, which has nothing
     paid yet, is now work prepared under a configuration no worker serves:
     it returns a retryable wait for 15 minutes after the reload and then
     fails with the non-retryable `configuration` error (counted in
     `llmtw_activity_failure_total{origin="worker"}`). See
     [reload behaviour for unfinished work](../reference/mvp-v1-status.md#storage-and-callers).
     Before raising a limit to unblock waiting traffic:
     - tell the owning callers that requests waiting at the time of the reload
       will fail within about 15 minutes and must be submitted again under a
       new operation key; the waiting attempt holds no claimed reservation,
       so resubmitting does not pay twice for it;
     - if many requests are waiting, or callers cannot resubmit, escalate to
       the service owner before reloading instead of reloading repeatedly;
     - do not revert the reload to "rescue" waiting requests: restoring the
       old configuration resumes them only on the old limit.
  5. If denials appear with no matching spend (for example right after a Redis
     restore), check readiness and follow
     [Redis readiness failures](state-stores.md#redis-readiness-failures).
- **Forbidden:** changing a window's `id`, `duration` or `bucket` to clear
  denials. A new identity starts from zero and forgets the recorded spend;
  the worker rejects a geometry change under an existing `id` on reload, and
  a restart would accept it silently. Never delete or edit Redis budget keys
  to free capacity. See
  [budget window identity](../reference/configuration.md#budget-window-identity).

## Paid work charged at its reservation because cost is unknown

- **Signal:** `llmtw_cost_status_total{endpoint,model,class,status="unknown"}`
  rises. It counts completed responses whose exact cost could not be
  established (the provider omitted usage, or the route was unpriced); the
  worker settles each at its full reservation. The series is recorded with
  `method="other"`. Attempts whose outcome itself is unknown are counted by
  `llmtw_ambiguous_total` instead; see
  [stuck ambiguous attempt](stuck-ambiguous-attempt.md).
- **Readiness reason:** none.
- **Default severity:** SEV3. SEV2 when one endpoint reports unknown cost for
  most of its traffic, because spend is then being over-reserved at scale.
- **Safe action:**
  1. Group by `endpoint` and `model`. A sudden rise on one endpoint usually
     means the provider stopped returning usage, or a catalog price no longer
     covers the route; check
     [catalog price expiry](provider-and-routing.md#catalog-price-approaching-or-past-expiry).
  2. Compare with the provider's own usage reporting for the same period and
     record the difference in the incident.
  3. If the provider changed its response format, open an issue against the
     adapter with the endpoint, model and time window, without any request
     content.
- **Forbidden:** treating unknown cost as zero in dashboards, lowering the
  charge by hand, or editing settled Redis records. Exact-cost correction
  needs the unimplemented
  [#819](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/819)
  receipts.

## Redis growth toward the sized capacity

- **Signal:** `llmtw_budget_operation_records{retention="expiring"|"persistent"}`
  approaches the capacity from the
  [sizing formula](../reference/redis-budget-leases.md#redis-growth-and-sizing),
  or `llmtw_budget_stream_length` stays above 12,000 for 10 minutes, which
  means the Stream's `MAXLEN` bound is not being applied. Both gauges update
  every readiness probe interval.
- **Readiness reason:** none until Redis runs out of memory. Redis runs
  `noeviction`, so a full Redis rejects budget writes: acquisition and
  settlement then fail with `state_unavailable` (counted in
  `llmtw_activity_failure_total{origin="worker"}`) while the read-only
  readiness probe may still pass.
- **Default severity:** SEV3 while trending; SEV1 once Redis rejects writes.
- **Safe action:**
  1. Compare the `persistent` series with the expected number of claimed or
     ambiguous operations awaiting settlement. A steady rise in `persistent`
     means attempts are not settling; see
     [stuck ambiguous attempt](stuck-ambiguous-attempt.md).
  2. If `expiring` grows without bound, check
     `state.redis.operation_key_reuse_horizon`. With `0` every record is kept
     indefinitely. Set a horizon that callers can honour (at least `24h` and at
     least `state.ambiguous_retention`), through a reviewed rollout.
  3. Add Redis memory before it is exhausted. Resizing Redis is safe as long
     as persistence, eviction policy and the admission Function stay the same.
  4. For a Stream above 12,000, confirm the running worker version applies
     `XADD MAXLEN`, and open an issue with the gauge history.
- **Forbidden:** deleting `durable-budget-operation` records or the
  `budget:operations` index to free memory, switching `maxmemory_policy` to an
  evicting policy on a running deployment, or running `FLUSHDB`. Each of
  these destroys budget authority. If Redis has already lost data, follow
  [Redis budget authority and recovery](redis-budget-generation-recovery.md).
