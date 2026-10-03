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
If no candidate compiles, the result is `no_route` before admission.

`PlannedProviderCall` carries the candidate, process-local adapter/SDK call,
config digest/epoch, capability version and complete cache route identity. The
account dimension uses the existing endpoint account HMAC. The identity also
includes provider, endpoint, region, model revision and `<family>/cloud-v1`
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
Those composition steps and remaining SQL removal are subsequent work.

Offline tests cover actual preparation, deterministic routing and the real
OpenAI Responses compiler for both phases with a transport counting unexpected
HTTP calls. They also cover fallback isolation, route/call bindings, snapshot
copies, context cancellation, ambiguous compiler errors, malformed configuration
and concurrent use. They do not establish live provider, Redis, AWS or Temporal
behavior.
