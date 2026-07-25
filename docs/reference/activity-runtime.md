# v1 Activity runtime boundary

The worker registers three exact names on its configured Temporal task queue:

| Name | Input | Output |
| --- | --- | --- |
| `llm.generate.v1` | `llm.GenerateRequestV1` | `llm.GenerateResponseV1` |
| `llm.compact.v1` | `llm.CompactRequestV1` | `llm.CompactResponseV1` |
| `llm.query.v1` | `llm.QueryRequestV1` | `llm.QueryResponseV1` |

The Activity adapter validates the closed JSON record and the configured
application payload limit before calling the injected `activity.V1Runtime`.
Responses are validated against the same limit before Temporal serialization;
errors are converted to bounded `SafeErrorDetails` and never include prompts,
outputs, provider bodies, or identifiers from a runtime error message.
The Activity boundary tests exercise that redaction contract independently for
Generate, Compact, and Query so a new entry point cannot accidentally expose a
provider or storage detail through its Temporal error.

The v1 wire schemas are closed tagged records. Generate responses may carry
only `generation` or `cache_replay` checkpoints; Compact policies contain only
the documented `target_tokens` and `summary_style` controls; and each Query
`kind` is bound to its corresponding filter and result page. Cache variants
remain signed 32-bit values; once inherited settings are materialized, a
non-zero variant requires a positive temperature, while Compact accepts variant
zero only. These checks are performed by the JSON schema gate where
representable and by the Go contract validators before an Activity is
dispatched.

`Activities.QueryService` is an independent seam for `llm.query.v1`. It may be
provided before the Generate/Compact runtime is composed; the Activity still
fails closed when neither seam is configured. The boundary authorizes the
tenant/project/actor scope and admits all five closed query kinds: provider
status, model inventory, credit status, budget status, and spend summary. It
verifies HMAC cursors for the three keyset-paginated kinds, binding each token
to the query kind, scope/tags, canonical filter, and snapshot horizon. Budget
status and spend summary intentionally have no public cursor because each is a
single bounded snapshot. Typed handlers receive authenticated cursor claims,
including the opaque storage position and horizon, so a repeatable-read
adapter can enforce the same snapshot before reading its next page. Cursors
must be issued with the worker's typed `CursorCodec` key; the raw `Handler`
seam remains available for adapters migrating from the legacy cursor envelope.
The production client set forwards a query service only when it is supplied by
the same snapshot-scoped PostgreSQL closer as its repositories; the default
composition does not invent authorization, cursor keys, or handlers. Query
families without a configured service therefore fail closed. The reusable
PostgreSQL composition for provider status, model inventory, credit status,
and spend summary is documented in
[persisted-query-service.md](persisted-query-service.md) and is installed only
through an explicit `ProductionFactoryOptions.QueryServiceBuilder`. Spend
summary additionally requires `PersistedQueryOptions.ResolveScope`, an
authenticated deployment resolver for the opaque PostgreSQL scope UUID;
missing, failed, or nil scope resolution fails closed. Refresh requests and
budget status remain fail-closed until their management and Redis-generation
adapters are supplied. `PersistedQueryOptions.BudgetStatus` is the typed
snapshot-scoped reader seam for that Redis adapter; it must bind the requested
instant to active generation/manifest/Stream provenance and cannot fall back
to PostgreSQL. The current storage package does not yet publish a versioned
window-hash field reader, so a manifest-only adapter is intentionally rejected.
Remaining
complete Activity composition work is tracked in
[Task 14, typed Query service and Temporal Activity, of the forkable
conversation-state plan](../superpowers/plans/2026-07-18-forkable-conversation-state.md#task-14-implement-typed-query-service-and-temporal-activity).
`QueryService.Audit` is the storage-neutral seam for the audit requirement: it
receives canonical redacted request/response envelopes, SHA-256 request and
response digests, and exact-or-unknown cost metadata after all response and
cursor checks. A configured sink must persist the record before `Execute`
returns; a sink error becomes retryable state-unavailable/finalize failure.
The production Activity factory still has to connect this hook to the
repository-only [query execution audit ledger](query-audit-ledger.md).

### Query failure classification

The query service treats its handler as a durable state boundary. A raw
handler failure, including a child storage deadline while the Activity context
is still live, is wrapped as `state_unavailable` at `state_load` with
`not_dispatched` and `same_operation`; the original cause remains available to
local diagnostics but is not exposed through the Temporal error envelope.
Provider-classified errors are preserved so their authorization, provider, or
other retry semantics remain intact. If the caller's Activity context is
canceled or reaches its deadline, that terminal context result is preserved
instead of being retried as a state outage. Response/encoding and cursor
contract violations remain validation failures. An audit sink failure is the
separate retryable `state_unavailable`/`finalize` case described above, and the
response is not returned until the audit callback succeeds.

## Query service snapshot composition

During each immutable snapshot build, runtime first checks whether the client
set exposes a query service through `runtime.QueryServiceSource`. If the
`EngineFactory` also implements `runtime.QueryServiceFactory`, its
`BuildQueryService` result is authoritative and replaces the client-provided
value. A factory error aborts snapshot construction and closes the clients that
were already built, so a query repository cannot leak across a failed reload.

A factory that returns no query service without an error leaves the snapshot
valid, but `llm.query.v1` remains fail-closed with a non-retryable configuration
error and performs no provider or storage read. Each query execution acquires
the application snapshot lease before dispatch and releases it afterward;
configuration reload therefore waits for in-flight query work before closing
the old snapshot's query clients.

`V1Runtime` is the seam for the durable checkpoint, cache, provider, and
control-plane implementation. Production composition currently installs an
explicit fail-closed runtime until that implementation is wired. In every
environment other than the checked-in `development` fixture, runtime startup
refuses to start listeners or Temporal polling when this seam is still
unconfigured (`ErrV1RuntimeUnavailable`). This prevents a production process
from reporting readiness while every advertised v1 Activity would return a
non-retryable configuration error before provider dispatch. The local Compose
worker uses `environment: development` only for parser/configuration/readiness
checks; when the durable seam is absent it omits the v1 Activity registrations
and cannot dispatch inference. It is not a development fallback for any other
environment, and never silently falls back to the pre-release envelope.

The boundary is one-shot by design. It does not register or dispatch
`llm.StreamingEngine`, token events, or provider stream decoders. Provider
fragment decoders remain parser-regression code only.

## Checkpoint-aware composition seam

`activity.MaterializingV1Runtime` is the bounded composition seam for durable
checkpoint replay. A Generate request with a parent, and every Compact request,
must resolve its opaque handle through an explicitly supplied scope resolver and
handle-capable materializer before dispatch. The materialized state is passed to
the runtime's `MaterializedGenerateRuntime` or `MaterializedCompactRuntime`
extension; a runtime that does not implement the extension fails closed without
provider dispatch. Root Generate requests have no parent to replay and continue
through the ordinary `V1Runtime` method.

The seam does not perform provider, cache, budget, or checkpoint publication
work. Production composition must still provide those operations behind the
state-aware runtime and must map authenticated request context to the durable
scope identifier; raw tenant/project concatenation is intentionally not used.

After a successful materializer read, the Activity seam performs a second,
storage-neutral contract check before dispatch. The returned handle and scope
must match the requested values; lineage must be non-empty, acyclic, and end
at that handle; depth and the materialized root model must be valid; and the
tool-call frontier must exactly match `ValidateTranscript` over the returned
items. A mismatch is classified as non-retryable `state_corrupt` with no
provider dispatch. This is defense in depth for adapter composition, not a
replacement for the durable materializer's graph, blob, expiry, and size
validation.

The Generate and Compact request/response records carry only bounded deltas,
settings, result metadata, and opaque checkpoint handles. A regression test
serializes synthetic one-, 100-, and 10,000-turn ancestor lineages, including
their realistic serialized checkpoint depths. Request sizes remain identical;
response sizes can grow only by the decimal digit count of that depth, never by
the ancestor transcript length. The materialized transcript therefore stays in
the worker's state store rather than growing Temporal history or Activity
arguments with conversation depth.
