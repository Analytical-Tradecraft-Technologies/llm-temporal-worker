# Cloud provider execution

`CloudProviderExecution` connects the saved budget plan to one paid provider
attempt. It builds on [budget admission](cloud-budget-admission.md) and
[provider reconstruction](cloud-provider-recovery.md). It is a runtime building
block; the production composition and Temporal workflow wiring are separate.

1. Reserve budget using the original saved quote.
2. Atomically record the attempt before consuming the Redis start claim. Only a
   confirmed new write may proceed. Competing proposals have distinct tokens.
3. Consume the single-use Redis claim. Immediately before possible HTTP writes,
   persist its receipt and the submitting state. A persisted receipt is recovery
   evidence, never authorization for another submission.
4. For synchronous providers, invoke once. For asynchronous providers, submit
   once and persist the provider job ID before returning pending. Resume polls
   once and honors the saved poll time; workflow timers own waiting.
5. Save the normalized result and exact settlement batch before reconciling
   Redis. A lost settlement reply replays the same batch without provider calls.

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
Known exact costs settle every reserved window together. Missing cost evidence
keeps the conservative charge. Only an explicitly classified rejection or
pre-dispatch failure has known zero cost. Provider errors persist safe enums,
not diagnostic causes or arbitrary provider error text.

Provider calls have a five-minute bound; saving paid outcomes and settlement
each have a separate ten-second bound even if the activity context has ended.
No cancellation API or background cleanup process is introduced here.
