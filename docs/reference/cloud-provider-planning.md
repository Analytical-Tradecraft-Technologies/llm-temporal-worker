# Shared cloud provider planning

`V1RuntimeCapabilities.NewProviderPlanning(ctx)` captures the route catalog,
health view, configuration digest and epoch once from the snapshot source. It
rejects a digest from a different runtime snapshot. Route slices, eligibility
lists, tier maps, capability maps and health maps are copied; planning one call
cannot change the captured catalog or another invocation. The supplied planner
and adapter registry must themselves belong to that immutable snapshot.

Inside a phase factory, construct one helper and pass it the authorized input
from [request preparation](cloud-request-preparation.md):

```go
planning, err := capabilities.NewProviderPlanning(ctx)
// Handle constructor errors before installing the runtime.
prepared, err := PrepareGenerateInput(ctx, request, replay)
// Handle input errors before planning.
planned, err := planning.Generate(ctx, prepared)
// Compact instead uses PrepareCompactInput and planning.Compact.
// Handle planning errors before reserving budget.
route, err := planned.Route(operationID, generationID)
```

The existing routing planner applies model configuration, tenant eligibility,
health, capabilities and class fallback order. Each candidate must match the
captured route before adapter lookup. Adapter capability versions must match
the verified route version. Compilation receives a detached request with the
resolved provider model, attempted class and no fallback classes. Application
settings retain the logical model alias and original class for checkpoints and
future planning. Strict/best-effort portability is preserved. Checkpoint handles
never become provider continuation identifiers.

Local adapter-resolution, capability and compilation failures may yield to the
next candidate. The first valid compiled call wins in this compilation-only
helper. [Cloud budget planning](cloud-budget-planning.md) additionally checks
the selected candidate's configured policies and price before it can win,
allowing authorized fallback when pricing is unusable. Its endpoint, family, model,
operation key, class, capability version, provider tier and request digest must
match the selection. Missing SDK parameters or mismatched calls fail closed.
A compiler reporting possible dispatch stops with an ambiguous, non-retryable
error; it cannot become fallback. Context cancellation also stops planning.
Errors omit raw compiler/configuration messages, provider payloads and causes.
If no candidate compiles, the result is `unsupported_capability` when a route
declined the request itself (the routing planner found a capability or
extension it lacks, or its adapter's compiler rejected the request, for
example in strict portability) and no route was passed over for a context
limit or a failed adapter lookup; otherwise it is `no_route`. A blocked route
keeps `provider_unavailable`/`same_operation` and an unpriced or unbudgeted
one keeps `no_route` at the `price` phase. The error's safe details carry
`rejected_routes` and, for the first four rejections with the ones that
explain the code first, `route_<n>` (the configured route ID) and `reason_<n>`:
one of the routing planner's rejection codes, `route_compile_rejected`,
`route_adapter_unavailable` or `route_quote_unavailable`, suffixed with
`:<feature>` when the feature is known. Each rejection is also logged as
`route rejected during provider planning` with `route_id`, `cause`,
`error_code` and `phase`. These details never leave the worker: the Temporal
error carries only the code, phase and dispatch certainty.

A Generate whose parent recorded provider-state provenance is pinned as
described in [routing and continuation](../architecture/routing-and-continuation.md#portable-and-pinned-continuation).
Each candidate on another lineage is rejected with reason
`continuation_pinned` in strict portability; in best-effort portability it
compiles without the provider state recorded for other lineages. When any
route was rejected that way, the error keeps its code (normally `no_route`)
and adds the safe detail `continuation=continuation_pinned`. Compaction
summarizer requests are not pinned. The same stripped request is what budget
quoting estimates and what exact-route recovery recompiles, so its digest is
reproducible.

`PlannedProviderCall` carries the candidate, process-local adapter/SDK call,
config digest/epoch, capability version and complete cache route identity. The
account dimension uses the existing endpoint account HMAC. The identity also
includes provider, endpoint, region, model revision and `<family>/cloud-v2`
compiler version. Bump that compiler version when the projection/lowering
contract changes; public activity names stay v1. `Route` supplies the same
identity to the durable runner. Cache fingerprint construction must additionally
use config, epoch, capability version, prepared content and sample index (and
compaction source/policy versions). This helper does not acquire a cache fill.

Treat a plan as immutable invocation-local data. Do not serialize its adapter
or SDK parameters, expose them through Temporal, log the plan, or reuse it across
operations. A plan is not a dispatch grant. Composition must recover prior paid
work, persist stable route/attempt and cache identities, quote budget for that
selection, acquire the fill when enabled, and consume a fresh Redis claim before
submission. Retry must verify/reconstruct a persisted selection rather than
replan unknown paid work. Replanning after automatic compaction also requires
a key appropriate to the new execution input.

[Cloud provider recovery](cloud-provider-recovery.md) reconstructs a call from
the saved route and input without selecting a replacement. It shares the
compilation checks with this planner and requires the original configuration
identity. Its reconstructed SDK objects remain local to that invocation.

Compact with a nil prepared request fails before routing; the phase's no-work
path must handle it without budget or provider work. The helper is available to
phase factories and does not install production factories, authorize scope,
quote/claim budget, persist attempts, invoke providers or implement polling.
The bounded cloud runtime supplies those composition steps, and SQL
persistence has been removed. Production CLI authorization and deployment
verification remain separate gates.

Offline tests cover actual preparation, deterministic routing and the real
OpenAI Responses compiler for both phases with a transport counting unexpected
HTTP calls. They also cover fallback isolation, route/call bindings, snapshot
copies, context cancellation, ambiguous compiler errors, malformed configuration
and concurrent use. They do not establish live provider, Redis, AWS or Temporal
behavior.
