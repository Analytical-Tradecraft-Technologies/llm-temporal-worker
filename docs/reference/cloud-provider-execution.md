# Cloud provider execution

`CloudProviderExecution` connects the saved budget plan to one provider
attempt. It builds on [budget admission](cloud-budget-admission.md) and
[provider reconstruction](cloud-provider-recovery.md). It is a runtime building
block; the production composition and Temporal workflow wiring are separate.

1. Reserve budget using the original saved quote when its mode is `reserved`.
2. Atomically record the attempt before consuming the Redis start claim. Only a
   confirmed new write may proceed. Competing proposals have distinct tokens.
3. Consume the single-use Redis claim. Immediately before possible HTTP writes,
   persist its receipt and the submitting state. A persisted receipt is recovery
   evidence, never authorization for another submission.
4. For synchronous providers, invoke once. For asynchronous providers, submit
   once and persist the provider job ID before returning pending. Resume polls
   once and honors the saved poll time; workflow timers own waiting.
5. Save the normalized result, its immutable completion timestamp and exact
   settlement batch before reconciling Redis. A lost settlement reply replays
   the same batch without provider calls or changing cache freshness.

The attempt is part of the encrypted request progress, discovered through the
existing eight cloud KV pending partitions. Provider IDs and normalized output
are encrypted in blob storage; no SDK parameters or credentials are serialized.
Redis remains authoritative for budgets and provider availability/inventory.

An interrupted submission waits until its bounded recovery time before trying
documented provider idempotency recovery. Without that capability, it becomes
`outcome_unknown`; its consumed reservation stays charged. A later paid retry
must use a fresh request/operation identity and fresh budget. It must not reuse
this attempt's claim. The orchestration of those replacement attempts belongs
to the internal request workflow.

Pending job transport/protocol failures preserve the saved job. They cannot
change its provider ID or turn into another submission. Poll responses are
checked against the original operation key. Original configuration, route and
compiler bindings are required for recovery; historical snapshot lookup is not
provided by this helper.

Completed, truncated, refused and tool-call responses retain their actual
provider response status. Cache eligibility remains the cache layer's decision.

Tool-call IDs stay unique for a checkpoint lineage. Some providers number calls
from zero in every response, so a Generate response can repeat the ID of an
earlier, already resolved call. Before the response is saved, each such ID is
replaced by `call_` followed by 32 hexadecimal characters derived from the
attempt's operation ID and the provider's ID; a result for that call in the same
output follows it. The saved, cached and published output all carry the
replacement, and the caller answers with it. IDs that do not collide with the
request transcript are never changed. Two calls sharing an ID within one
response are not separated.
Known exact costs settle every reserved window together. Missing cost evidence
keeps the conservative charge. Only an explicitly classified rejection or
pre-dispatch failure has known zero cost. Provider errors persist safe enums,
not diagnostic causes or arbitrary provider error text.

A response that arrived in full but could not be lifted is an accepted
`provider_invalid_response`. Tool-call arguments with a duplicate JSON key are
one example; every adapter rejects them while lifting. The attempt is a terminal
failure settled at the conservative charge. It is not `outcome_unknown`, so it
does not wait for the recovery time or lead to another submission.

A saved terminal result is immutable; only its settlement acknowledgement may
follow. Results are compared in canonical JSON form, so the key order a provider
used in tool-call arguments is not a different result. Callers receive the
canonical, key-sorted arguments held in the saved response.

Provider calls have a five-minute bound. Saving an execution revision and
settlement each have a separate bound even if the activity context has ended.
One save attempt has ten seconds. A transient storage failure is retried in
process, with the identical revision, after 200 ms, 1 s and 3 s while a
30-second total remains, because a provider result exists only in memory until
it is saved. A retry is a compare-and-set of the same content and never calls
the provider. If a retry finds a conflict, the attempt is reloaded; when the
stored revision is this same result, an earlier write was applied without an
acknowledgement and the save has succeeded. Any other conflict, and a failure
that outlasts the retries, is returned to the Activity as before: the saved
state stays `submitting`, and recovery follows the interrupted-submission rules
above without another submission.

If the pre-HTTP save of the submitting state still fails after those retries,
the adapter is refused before it can write, so nothing was sent. The attempt is
then saved as a retryable `not_dispatched` failure and its consumed claim is
settled at zero cost, instead of waiting for the recovery time. This applies
only when the saved state is the untouched claiming record or this caller's own
unacknowledged marker; otherwise the error is returned unchanged. A lost reply
to the Redis claim or to the initial attempt record remains ambiguous and still
waits for the recovery time.

No cancellation API or background cleanup process is introduced here.

## Calls without reservations

Saved plans explicitly distinguish `reserved`, `free` and `unmatched` admission.
`free` requires a known zero estimate backed by the captured quote. `unmatched`
is permitted only by the captured budget policy; its provider call may still
cost money. An absent price is recorded as `unpriced` and never implies zero
actual cost. Provider-reported charges are preserved even if a catalog expected
free work.

Free and unmatched attempts use the same durable submission fence and poll-once
recovery. They have no Redis reservation, claim or settlement batch. `Reserve`
returns the zero result for these modes; it does not fabricate an accepted lease.
`Claim` rejects these calls. Callers must inspect the saved plan's
`RequiresReservation` rather than treating an empty vector as permission.

An unreserved finalization handoff must match a successful saved execution,
including operation, generation, route, cost and original completion timestamp.
The finalizer verifies this before saving or replaying publication, then completes
the cache fill without touching Redis. The handoff flag alone cannot bypass a
reserved attempt's settlement.
