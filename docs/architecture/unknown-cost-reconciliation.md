# Authorized unknown-cost reconciliation

Status: design for [#819](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/819),
tracked by [#812](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/812).
Nothing in this document is implemented yet. It is a separate recovery
feature that runs under its own credentials. The initial deployment keeps
every conservative charge, and this design does not block that deployment.

Cleanup coordination is in
[cloud retention and deletion](cloud-retention-and-deletion.md) (#818).

## Problem

A provider submission whose response was lost may represent paid work. Today
the worker charges such an attempt at its full reservation and keeps the
attempt discoverable. A retry is a new, independently budgeted attempt. The
conservative charge may later be lowered, or raised, to the exact cost. That
must happen only when authenticated, attempt-specific, exact evidence is
committed to durable cloud storage *before* Redis changes. The change must be
replayable, idempotent, auditable, and refused when evidence changes. Missing
or unsupported evidence leaves the cost unknown. Unknown is never treated as
zero.

## What exists today

All claims in this section describe current code.

### How an unknown cost is charged

- `CloudProviderExecution.Resume` in
  [`cloud_provider_execution.go`](../../golang/internal/runtime/cloud_provider_execution.go)
  moves an attempt whose dispatch may have happened, and that cannot be
  polled or recovered by idempotency key, to `cloudstate.ExecutionUnknown`
  with `provider.CodeAmbiguousDispatch`.
- `settleUnknown` waits until `RecoverAfter` (`StartedAt +
  durable.BudgetStartLease`, 15 minutes). It requires a recorded `Claim` and
  `Plan.RequiresReservation()`. It then saves `Settlement =
  executionSettlement(next)` and calls `settle`.
- `executionSettlement` builds one `budget.CompletionEvent` per reserved
  window. When there is no exact cost, the event is
  `budget.JournalFinalizeUnknown` with `CostUnknown`, reason
  `provider_cost_unknown`, `AccountedIncreaseUSD = ReservedDecreaseUSD =` the
  window's reserved amount, `ReservationRevision = reserved + 1`, and
  `EventID = sha256("provider-settlement-v1/" + reserved.EventID)`. The same
  function also produces `finalize_unknown` for an `ExecutionSucceeded`
  response whose `Cost.ActualCostUSD` is nil. Unknown cost is therefore not
  limited to lost submissions.
- `settle` persists the response and the exact event batch before calling
  `BudgetMaterializer.Reconcile`, then records `Settled = true`. A lost Redis
  reply retries the identical batch.
- `validExecutionTransition` in
  [`provider_execution.go`](../../golang/storage/cloudstate/provider_execution.go)
  closes a settled unknown execution: "an exact outcome learned later is an
  authorized correction, never a second settlement". Reconciliation must
  therefore not mutate the attempt's execution record.
- An unknown attempt is replaced only through `BeginRequestAttempt`, which
  allocates a new attempt ID and a new reservation. The old attempt keeps its
  pending row and its charge (`cloud_execution_runtime.go`).

### What Redis already supports

- `budget.JournalResolveUnknownExact` (`resolve_unknown_exact`) exists in
  [`budget/journal.go`](../../golang/budget/journal.go). Validation requires
  `CostExact`, a non-nil `ActualCostUSD`, `AccountedIncreaseUSD ==
  ActualCostUSD`, and exactly one of `ReservedDecreaseUSD` and
  `AccountedDecreaseUSD` non-zero.
- The `durable_reconcile` action in
  [`admission.lua`](../../golang/storage/redis/functions/admission.lua)
  accepts `resolve_unknown_exact` only when the reservation status is
  `ambiguous` (set by `finalize_unknown` or `retain_ambiguous`). It requires a
  higher `reservation_revision`, and requires the decrease to equal the stored
  reserved/accounted amounts. It stages the whole batch before mutating
  anything. It records `events[event_id] = fingerprint`: an identical replay
  is a no-op, and a different fingerprint under the same ID returns
  `conflict`. A reservation already `finalized` returns `finalized`. When the
  window's expiry entry has already been cleaned up, it returns `not_found`
  rather than subtract another operation's contribution.
- `RedisBudgetMaterializer.Reconcile` in
  [`budget_materializer.go`](../../golang/storage/redis/budget_materializer.go)
  maps these statuses to `ErrRedisBudgetConflict`,
  `ErrRedisBudgetReservationFinalized`, `ErrRedisBudgetReservationNotFound`,
  `ErrRedisBudgetGenerationMismatch`, `ErrRedisBudgetIncarnationMismatch` and
  `ErrBudgetAuthorityUnavailable`. It rejects a request whose
  generation/incarnation differs from the materializer's own. The event
  fingerprint is the SHA-256 of the JSON event, so `OccurredAt` and every
  amount are part of the identity.
- `durable.UnknownCostBoundary.Resolve` and `UnknownCostResolution.Validate`
  in [`unknown_cost.go`](../../golang/storage/durable/unknown_cost.go) check
  that a batch has only `resolve_unknown_exact` events, with one exact amount
  and unique IDs, and forward it to `Reconcile`. On failure, `Resolve`
  returns `fmt.Errorf("%w: %v", ErrReconcilePending, err)`. The `%v` discards
  the typed Redis cause, so a caller cannot tell `not_found`, `conflict`,
  `finalized`, a generation/incarnation mismatch and a transport error apart
  without matching strings. **There is no production caller and no durable
  receipt.**

The missing pieces are evidence, authorization, the durable receipt written
before Redis, discovery, operator recovery and audit. This design supplies
them.

## Evidence model

```
UnknownCostEvidence (v1, canonical JSON, no content):
  source_kind        provider_billing_record | provider_usage_record | operator_attestation
  provider_family    from the saved BudgetPlan.Family
  endpoint_digest    must equal the plan's saved endpoint digest
  account_digest     HMAC of the provider account/project the source authenticated as
  subject            one of:
                       provider_operation_digest  HMAC of ProviderExecution.ProviderOperationID
                       operation_key_digest       ProviderRecoveryOperationKeyDigest(original key)
  exact_cost_usd     decimal string parsed by pricing.USD (never float)
  pricing            for provider_usage_record only: price_version and usage counters,
                     priced with the plan's saved quote/price version
  source_record_digest HMAC of the provider's billing/usage record identifier
  observed_at, billing_period
  authenticator      {kind: provider_api | signature, key_id}
```

Each evidence field meets one requirement:

- **Authenticated.** `provider_api` evidence is fetched by the reconciler over
  TLS, with a read-only billing credential resolved from a secret reference
  and separate from inference credentials. `operator_attestation` evidence
  carries a detached Ed25519 signature over the canonical evidence, from a key
  whose public half is in worker settings and whose private half is held
  outside the worker. Unsigned or unverifiable evidence is rejected.
- **Scope-bound.** The reconciler loads the attempt with
  `Repository.Read(scope, id)`, which reports a cross-scope ID as not found.
  It then requires the evidence's `endpoint_digest`, `account_digest` and
  `subject` to match the attempt's saved `BudgetPlan` and execution. Evidence
  that names another attempt, endpoint or account is rejected before any
  write. The scope must also be in the trusted tenant/project allowlist from
  the worker's Temporal caller policy (#908).
- **Exact USD.** Only a per-request exact amount is accepted. Usage-derived
  evidence counts as exact only when priced with the plan's own saved price
  version. Aggregated, estimated, rounded-to-period or currency-converted
  figures are `evidence_unsupported`. A missing provider record is not
  evidence of zero. A zero amount is accepted only from an evidence kind that
  positively attests this specific request was not billed. The amount may
  exceed the reservation. The correction is then an increase. It is applied,
  because under-charging is also wrong, and audited as `exceeds_reservation`.
- **Idempotent resolution identity.** `resolution_id = HMAC(secret,
  "unknown-cost-resolution/v1", namespace, generation_id, operation_id)`.
  Redis can resolve a reservation once, so there is exactly one resolution
  per paid operation, whatever the evidence source. `evidence_digest =
  HMAC(secret, "unknown-cost-evidence/v1", canonical evidence)`.
- **Provenance.** The receipt stores `source_kind`, `authenticator`,
  `source_record_digest`, `observed_at`, the reconciler build and
  configuration digest, and the acting maintenance identity. It never stores
  raw provider IDs, prompts, outputs or credentials. Those are kept only as
  keyed digests.

## Durable resolution receipt

The receipt is written with the existing `cloudstate` primitives (encrypted
blob, then a `Create`-only KV pointer), in the style of
`budget_initialization.go`. It is new code:

```
blob stream: unknown-cost/<resolution tag>            (Repository.writeBlob, AES-GCM, key-bound AAD)
receipt:     <ns>/unknown-cost/receipt/<resolution tag>  Create only, never replaced
applied:     <ns>/unknown-cost/applied/<resolution tag>  Create only, written after Redis
open index:  <ns>/unknown-cost/open/<shard 0..7>, sort key = attempt request ID
```

The receipt payload holds:

- the resolution identity and the evidence (above);
- the attempt reference: request ID, scope tag and binding digest;
- the reservation identity (`OperationID`, `GenerationID`, `IncarnationID`)
  and the original `finalize_unknown` event IDs and revisions copied from the
  saved `Settlement`;
- the **exact `durable.ReconcileRequest` to apply**, fixed once. One event per
  window: `Kind = resolve_unknown_exact`, `CostStatus = exact`,
  `ActualCostUSD = AccountedIncreaseUSD = exact`, `ReservedDecreaseUSD = 0`,
  `AccountedDecreaseUSD =` the window's conservative amount, `ReservationRevision
  =` finalize revision `+ 1`, `EventID = sha256("unknown-cost-resolution-v1/" +
  resolution tag + "/" + reserved.EventID)`, and `OccurredAt =` the receipt's
  `created_at`. Storing the batch, rather than recomputing it, keeps the Redis
  fingerprint identical across every replay;
- `created_at`, the conservative total and the delta.

The receipt is immutable. Lowering the charge requires a committed receipt.
The applied marker records `{outcome, applied_at}` and is only an
acknowledgement: losing it causes a harmless replay.

The open index row is written by the request path (phase 1) before `settle`
calls Redis, whenever the settlement contains `finalize_unknown`.

**Authoritative window expiry.** Redis sets each reservation's
`window_expires_millis` from Redis `TIME` at acceptance (`admission.lua`). The
worker's clock, including `ProviderExecution.StartedAt`, is not a valid bound
on that value, because the two clocks can straddle a bucket boundary. On a
successful `durable_reconcile`, the Function already returns the encoded
operation record, but `RedisBudgetMaterializer.Reconcile` discards it. Phase 1
changes that. The materializer returns each reservation's
`window_expires_millis` from the `finalize_unknown` settlement result, and the
request path stores those values in the open index row. They are later
copied into the receipt. A `window_expired` classification requires every
stored value to be earlier than Redis `TIME`, read through the same
authority-checked connection. If the values were never captured (settled
before phase 1, or the reply was lost and the retry returned only a
deduplicated `ok`), they are read from the Redis operation record while it
still exists. If neither is available, `not_found` is treated as authority
loss: the receipt stays RECEIPTED and an operator investigates. It makes
both lost-submission and succeeded-without-cost attempts discoverable. Lost
submissions are also in `ListPending` as `outcome_unknown`. Succeeded attempts
are terminal and filtered from that list. The row is retired once the applied
marker exists.

## State machine

```
            +-----------+  evidence verified,   +-------------+  Reconcile ok /     +---------+
 settled -> | UNRESOLVED| ---------------------> |  RECEIPTED  | -------------------> | APPLIED |
 unknown    +-----------+  receipt Create ok     +-------------+  window_expired     +---------+
                 |   ^        or identical            |   ^
                 |   |                                |   | transport error, ambiguous reply,
   unsupported / |   | definite cloud failure         |   | authority loss, conflict
   mismatch /    |   | (no Redis call)                +---+ (retry identical batch; never
   changed       v   |                                      regenerate; alert on conflict)
   evidence   (stays UNRESOLVED; conservative charge unchanged)
```

Preconditions for leaving UNRESOLVED: the attempt's execution has
`Settlement` containing `finalize_unknown` events **and** `Settled == true`.
If it is not settled, the normal `settle` path is still responsible, and the
reconciler waits. An `ExecutionUnknown` attempt without `Claim`, or still
before `RecoverAfter`, is ineligible. `settleUnknown` deliberately leaves such
an attempt alone.

Steps, all replayable:

1. **Load and verify.** Read the attempt by scope. Load the saved plan and
   execution. Compute `resolution_id`. Verify the evidence (authentication,
   scope binding, exactness). Failures are terminal for this evidence and
   cause no writes.
2. **Check for an existing receipt.** `Get` the receipt. If it exists with
   the same `evidence_digest`, go to step 4. If it exists with a different
   digest, return **`ErrResolutionConflict`**. Changed evidence under the same
   identity is rejected and audited, and nothing else happens. A wrong receipt
   cannot be overwritten in-band (see operator recovery).
3. **Commit the receipt.** Write the blob, then `Create` the pointer.
   `ErrAlreadyExists` triggers a re-read and a compare: identical counts as
   committed, and different is `ErrResolutionConflict`. `ErrOutcomeUnknown`
   leads to a retry of the identical write. Redis is never called until a
   read confirms the receipt.
4. **Apply in Redis.** Call `UnknownCostBoundary.Resolve` with the stored
   batch, under the same bounded detached timeout as `settle`
   (`finalizationTimeout`). Phase 2 first changes `Resolve` to keep the
   typed cause. It wraps both `ErrReconcilePending` and the materializer
   error with `%w` (or returns the cause in a typed result field). Callers
   then classify with `errors.Is` against `ErrRedisBudgetConflict`,
   `ErrRedisBudgetReservationFinalized`, `ErrRedisBudgetReservationNotFound`,
   `ErrRedisBudgetGenerationMismatch`, `ErrRedisBudgetIncarnationMismatch`
   and `ErrBudgetAuthorityUnavailable`. The reconciler never matches error
   text. An error that matches none of these counts as a transport or unknown
   result.
5. **Acknowledge.** `Create` the applied marker, then retire the open index
   row.

Redis results in step 4:

| Result | Meaning | Action |
| --- | --- | --- |
| `ok` (including a deduplicated replay) | applied | step 5, outcome `applied` |
| `ErrRedisBudgetReservationNotFound`, and the **Redis-authoritative** expiry captured at settlement has passed for every window, measured by Redis `TIME` (see below) | the conservative charge already aged out of every window; nothing to lower | step 5, outcome `window_expired`. The receipt still records the exact cost for audit. |
| `ErrRedisBudgetReservationNotFound` otherwise | the operation record is missing: possible Redis data loss | stay RECEIPTED, fail closed, alert. See authority loss. |
| `ErrRedisBudgetConflict` | status is not `ambiguous` (for example, a restored snapshot predates `finalize_unknown`) or the event fingerprint differs | stay RECEIPTED, alert, no hot retry |
| `ErrRedisBudgetReservationFinalized` | a different batch already resolved it | stay RECEIPTED, alert. Possible double-resolution attempt. |
| `ErrRedisBudgetGenerationMismatch` / `ErrRedisBudgetIncarnationMismatch` / `ErrBudgetAuthorityUnavailable` | authority change or loss | stay RECEIPTED, fail closed |
| transport error / lost reply | unknown | retry the identical batch later |

## Failure cases

- **Cloud write fails.** The failure may be definite (`ErrConflict`,
  permission denied) or the outcome may be unknown. Either way no Redis call
  happens, and the conservative charge stays. An uncertain write is retried
  only with identical bytes. "No refund without a receipt" holds because step
  4 runs only after step 3 is confirmed by a read.
- **Redis fails after a durable receipt.** The receipt is in RECEIPTED and the
  open row remains. The next pass replays the stored batch. Redis
  deduplicates by `event_id` and fingerprint, so a lost reply that actually
  committed is a no-op on replay. Until success, the conservative charge
  stays in Redis. That is the safe direction.
- **Lost response to the reconciler.** Every step is idempotent: the receipt
  compares on read, the Redis events are deduplicated, and the marker is a
  `Create` compared on read. A rerun reaches the same terminal state.
- **Duplicate evidence.** The same evidence submitted again (by two operators
  or two reconciler replicas) maps to the same `resolution_id` and
  `evidence_digest`. That replays the same steps.
- **Changed evidence.** It gets the same `resolution_id` with a different
  `evidence_digest`, which is `ErrResolutionConflict`. It is never applied,
  never merged, and always audited.
- **Authority loss.** This covers a missing or mismatched Redis marker, a new
  incarnation, or a restore. The receipt waits. The conservative bound stays
  in whichever Redis state is authoritative. `RedisBudgetMaterializer`
  refuses other incarnations, so a receipt for an old incarnation cannot be
  applied automatically. The #856 data-loss recovery procedure must take the
  set of RECEIPTED and APPLIED receipts as input. They are durable, authorized
  facts that may be re-applied to a restored authority after explicit
  operator review. Re-applying an APPLIED receipt to a snapshot that predates
  it is correct and idempotent. Nothing rebuilds balances from receipts
  alone.
- **Unsupported evidence.** The attempt stays UNRESOLVED with its
  conservative charge, and the outcome is counted as `evidence_unsupported`.
  It is never resolved to zero.

## Authorization

- The reconciler is a separate CLI subcommand, `unknown-cost-reconcile`. It is
  modeled on `budget-initialize` in [CLI](../reference/cli.md#budget-initialize):
  read-only by default, `--apply` to write, an overall timeout of at most five
  minutes, and bounded pages. It is never an Activity and never reachable
  from tenant workflows.
- It requires an explicit tenant/project for every attempt. The pair must be
  in the trusted allowlist, and the attempt must be readable under that scope.
- A distinct IAM role. KV `GetItem`/`Query` on request, plan and execution
  items. Conditional `PutItem` only on `unknown-cost/*` items. Blob
  `PutObject` and `GetObject` only under the `unknown-cost` stream. No
  deletes.
- A distinct Redis ACL user that may `FCALL` only the admission Function, so
  every resolution can be attributed. Redis ACLs cannot restrict a single
  action inside a Function. The guarantee that this user only issues
  `durable_reconcile` with `resolve_unknown_exact` events comes from the
  subcommand's code path and the receipt precondition, not from Redis.
- Billing-API credentials are read-only and separate from inference
  credentials. The attestation-signing key is never present in the worker.
- `operator_attestation` additionally requires two different attesting
  identities recorded in the receipt's provenance. This is a two-person rule
  for manually supplied amounts.

## Audit logging

Every terminal or alerting transition emits one structured info-level event,
`unknown cost resolution`. Like `observability.Logger.QueryAudit` in the
[query audit log](../reference/query-audit-ledger.md), tenant, project and
operation identifiers are hashed. The fields are:

- `resolution_tag`, `scope_hash` and `operation_digest`;
- `source_kind`, `authenticator.kind`, `authenticator.key_id` and
  `evidence_digest`;
- `conservative_usd`, `exact_usd` and `delta_usd` (amounts are not secrets);
- `stage` and `outcome`. `outcome` is one of `applied`, `window_expired`,
  `conflict`, `authority_unavailable`, `evidence_rejected`,
  `evidence_unsupported` or `exceeds_reservation`;
- the actor identity, build and configuration digest.

Prompts, outputs, raw provider IDs, credentials and evidence payloads are
never logged. Logs are best-effort, as for query audit. **The receipt is the
durable audit record**. Logs are not the authority.

## Bounded execution

- A pass reads at most `--limit` open-index rows (1 to 1000) per shard, across
  the 8 shards, with page tokens. It performs at most one evidence fetch per
  attempt, and the fetch runs under its own timeout.
- Redis calls use a detached bounded timeout. No step sleeps or waits on
  budget.
- Results report examined, receipted, applied, window_expired, conflicts,
  unsupported and errors. A protected or failing attempt never blocks the
  rest of the page.

## Operator recovery

- **Stuck in RECEIPTED.** The `--apply` replay is safe to repeat. Conflicts
  need investigation. Read the Redis operation record with the existing
  status reader, and compare the stored `finalize_unknown` events against the
  receipt. Do not edit Redis by hand.
- **Wrong evidence already receipted.** There is no in-band overwrite. If the
  receipt was not applied, keep the reconciler disabled for that attempt by
  recording a `blocked` marker (`Create`-only, with provenance). If it was
  applied, Redis has `finalized` the reservation, and correcting it would
  need a new journal event kind. That is out of scope and must be designed
  separately. This is why evidence admission is strict and manual amounts
  need two people.
- **Authority loss.** Follow the
  [Redis recovery boundary](../runbooks/redis-budget-generation-recovery.md):
  keep paid work stopped and preserve receipts. Do not apply receipts to a new
  authority until #856's procedure explicitly accepts them.

## Retention obligations and coordination with #818

- An attempt with `finalize_unknown` is protected from retention until its
  applied marker exists, or until an operator-approved unknown-cost horizon
  passes with the attempt left UNRESOLVED. In that case the attempt's
  identity, plan digest and settlement events stay in a compact tombstone, so
  the charge remains explainable.
- Receipts, applied markers and blocked markers are retained for the
  configured financial-audit horizon. With no horizon they are retained
  indefinitely, like the budget initialization receipt. They are referenced
  through `blob` edges so blob deletion protects their payloads.
- The Redis operation record and its `events` map must outlive every possible
  resolution. Any future Redis retention (#814) must keep `ambiguous`
  reservations until an applied marker exists or the horizon passes.
- None of this blocks the initial retain-all deployment: with retention
  disabled, every obligation is trivially met.

## Metrics

All metrics use the `llmtw_` prefix and carry no tenant labels.

- `llmtw_unknown_cost_open`: a sampled gauge of open-index rows;
- `llmtw_unknown_cost_resolution_total{outcome}`;
- `llmtw_unknown_cost_receipted_unapplied`: a gauge;
- `llmtw_unknown_cost_oldest_open_age_seconds`.

## Phased implementation plan

| Phase | Exit | Default |
| --- | --- | --- |
| 0 | This design | n/a |
| 1 | Request path writes the open index when a settlement contains `finalize_unknown`, including the Redis-authoritative `window_expires_millis` returned by the materializer; read-only `unknown-cost-reconcile` report; metrics | report only |
| 2 | Receipt store in `cloudstate`; state machine; `UnknownCostBoundary.Resolve` changed to keep typed Redis causes, then wired; `operator_attestation` evidence with signatures and the two-person rule; IAM/ACL identities | off |
| 3 | Provider evidence adapters, one family at a time. Each is enabled only after [source contracts](../reference/source-contracts.md) verify that the provider exposes authoritative per-request cost or usage bound to our idempotency key or job ID. Otherwise that family stays `evidence_unsupported`. | off |
| 4 | Scheduled passes; integration with #856 recovery and #818 retention protections | off |

## Test plan

All rows except those marked otherwise run with real Redis (the existing
integration-test Function) and the generic KV/blob fixtures. Rows marked
**AWS** also run in the opt-in DynamoDB/S3 gate.

| # | Scenario | Expectation |
| --- | --- | --- |
| 1 | Receipt `Create` fails definitively | no Redis call; conservative charge unchanged (AWS) |
| 2 | Receipt `Create` outcome unknown, then identical retry | single receipt; Redis applied once (AWS) |
| 3 | Redis fails after durable receipt | RECEIPTED; next pass applies; charge lowered exactly once |
| 4 | Redis reply lost after commit | replay deduplicated by event ID and fingerprint; marker written |
| 5 | Duplicate identical evidence from two replicas | one receipt, one Redis change |
| 6 | Changed evidence under the same identity | `ErrResolutionConflict`; no Redis call; audited |
| 7 | Cross-scope attempt ID or mismatched endpoint/account/subject | denied before any write |
| 8 | Unsupported or aggregated evidence; provider "not found" | stays unknown; never zero |
| 9 | Unsettled unknown, before `RecoverAfter`, or no `Claim` | ineligible |
| 10 | Window already expired per stored Redis `window_expires_millis` and Redis `TIME` | `window_expired`; no Redis mutation |
| 10a | `not_found` with the worker clock past the bucket but Redis expiry not proven | stays RECEIPTED; treated as authority loss |
| 10b | Each typed Redis error through `UnknownCostBoundary.Resolve` | classified by `errors.Is`, never by message text |
| 11 | Incarnation mismatch or missing authority marker | fail closed; RECEIPTED retained |
| 12 | Redis snapshot restored to before `finalize_unknown` | `conflict`; no change; alert |
| 13 | Exact cost above the reservation | applied as an increase; `exceeds_reservation` audited |
| 14 | Succeeded attempt without cost (terminal) | discovered via the open index; resolvable |
| 15 | Logs | contain no prompts, provider IDs, credentials or raw scope |
| 16 | #818 sweeper sees an unapplied receipt | attempt retained |
