# Background submission and polling

`llm.generate.v1` and `llm.compact.v1` can return a pending operation handle.
`llm.poll.v1` accepts that handle with the caller's tenant, project, and actor,
checks the original provider once, and returns `pending`, `completed`, or
`failed`. Activity names remain v1. There is no new workflow, internal polling
loop, timer, or automatic resubmission.

The built-in direct OpenAI Responses and Azure Responses adapters support
background submission, using the documented [OpenAI](https://developers.openai.com/api/docs/guides/background)
and [Azure](https://learn.microsoft.com/en-us/azure/foundry/openai/how-to/responses) create/GET APIs. Generic compatible Responses, Chat Completions,
Anthropic, and Bedrock adapters retain synchronous invocation. This selects
native background Responses, not batch APIs. Provider/model/account support
still determines whether an individual request is accepted; a rejected
background request must not be silently retried synchronously after a possible
write. Compaction is eligible when its selected implementation uses that same
background Responses path; this does not make a synchronous native compaction
endpoint asynchronous.

A pending Generate response is:

```json
{
  "api_version": "llm.temporal/v1",
  "operation_key": "request-42",
  "operation_id": "operation-42",
  "status": "pending",
  "pending": {
    "operation_id": "operation-42",
    "kind": "generate",
    "provider": "openai",
    "endpoint_id": "openai-prod",
    "provider_operation_id": "resp_42"
  }
}
```

Pass `pending` unchanged to `llm.poll.v1`:

```json
{
  "api_version": "llm.temporal/poll/v1",
  "context": {"tenant": "tenant", "project": "project", "actor": "actor"},
  "pending": {
    "operation_id": "operation-42",
    "kind": "generate",
    "provider": "openai",
    "endpoint_id": "openai-prod",
    "provider_operation_id": "resp_42"
  }
}
```

A poll response repeats that handle. A completed response includes exactly one
of `generate` or `compact`, containing the normal completed v1 response. A
failed response includes a sanitized `failure.code` (`provider_failed`,
`provider_cancelled`, or `result_unavailable`) and `cost_unknown: true`.
Transient HTTP errors remain Activity errors; they do not release reservations.
A missing or expired upstream result is an unknown-cost terminal failure.
The handle contains no credentials, provider URL, budget amounts, or transcript
and is not authorization. Runtime loading must authorize the caller and verify
all handle fields against persisted operation state before accessing a provider.

## Budget settlement

Admission reserves budget once. A pending response leaves that reservation in
place; polling never acquires another reservation. After a terminal response:

1. Persist the immutable terminal result and its finalization timestamp. A
   concurrent finalizer must return the already-stored winner.
2. Derive completion event IDs from the original reservation events. Every
   retry uses the same IDs, timestamp, amount, generation, and incarnation.
3. Append those events to the existing PostgreSQL recovery journal.
4. Run the existing Redis atomic reconciliation function. It checks event
   fingerprints and reservation revisions, applies all window deltas atomically,
   and treats identical events as already applied. Conflicting events fail.

Redis is the authority for whether counters have already changed. There is no
PostgreSQL read-before-release check, Redis check-then-delete race, or separate
expiring “released” marker. A lost Redis acknowledgement repeats the same event
without changing counters again. The journal supports the existing fenced Redis
recovery process; it is not consulted to decide whether to release a lease.

For exact cost, settlement removes the original reservation and accounts the
actual provider charge in each reserved window. For unknown cost, including
failed/cancelled operations without trustworthy usage, settlement marks the
reservation ambiguous and retains its amount. The existing unknown-cost
reconciliation path must resolve it later. Completion alone does not prove a
zero charge. Abandoned workflows likewise need a later reconciliation policy;
this change does not automatically cancel provider work or forgive its cost.

## Runtime composition

Master currently exposes deployment-supplied Generate/Compact phase factories;
this change extends that boundary rather than supplying the outstanding
production migration wiring. The concrete factory must:

- Select a resumable adapter and mark its route `Background` before admission.
- Dispatch through `durable.DispatchProvider`, which chooses `Submit` for a
  resumable adapter and `Invoke` otherwise.
- Convert a pending provider outcome into the bound `PendingOperationV1`, then
  implement `Suspend` to durably save the handle, request identity, original
  route/pricing snapshot, reservation, and any state needed for finalization.
- Replay that saved handle without a second reservation or submission.
- Supply `PollPortsFactory.Load` with the original provider call, reservation,
  Redis materializer, journal, and idempotent terminal finalizer. Loading must
  reauthorize every caller; handles must never choose arbitrary credentials.

A background route without `Suspend` fails before reservation. The complete
runtime builder rejects submission-capable ports without polling ports. A
crash between upstream acceptance and saving its ID remains ambiguous: the
journal's possible-write boundary must prevent resubmission. Polling must use
the original route and pricing even after configuration reload. A completed
poll replay performs no provider GET and retries journal/Redis settlement.

This is the activity/adapter contract for the migration tracked by [#812](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/812) / [#813](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/813);
production activation still depends on those concrete runtime factories. It
must not be presented as deployed behavior until that wiring is completed.

## OCaml

`Llm_temporal.Poll.generate_activity` and `compact_activity` return
`Ready response` or `Pending { operation_key; handle }`. `Poll.activity` makes
one status check and returns `Still_pending`, `Generated`, `Compacted`, or
`Failed`. These low-level descriptors leave retry and timer policy to the future
workflow. Existing terminal-only conversation helpers require a `Ready` result;
use the new descriptors when choosing a background-capable route.
