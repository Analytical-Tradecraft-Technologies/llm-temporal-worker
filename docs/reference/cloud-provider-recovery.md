# Reconstructing cloud provider calls

`V1RuntimeCapabilities.NewProviderRecovery(ctx)` captures the verified route
catalog, health view, configuration digest/epoch and adapter registry once.
It needs no selection planner, price resolver, budget leaser or request store.
`Generate` and `Compact` reconstruct SDK parameters from authorized prepared
input and a `ProviderRecoveryBinding` supplied by durable composition.

An initial compiled call can project that binding with
`planned.RecoveryBinding(route)`. It contains the original configuration and
compiled request digests, candidate ID, route/cache identity, family, capability
version, provider tier, requested/attempted classes and an operation-key digest.
Adapter clients, SDK parameters, credentials and raw operation keys are absent.
The route also keeps the paid operation/generation IDs. A price version resolved
by budget quoting is allowed when the configured route did not pin a version;
the original quote/reservation must still be retained by composition.

On restart, construct the binding from the saved selection and the original
input read from durable request storage. `ProviderRecoveryOperationKeyDigest`
supplies the separate operation-key binding: the semantic/cache request digest
intentionally excludes that key. Do not derive this binding from a replacement
incoming request, rerun budget quoting, or replace the saved attempt IDs. The
saved budget plan supplies the route, configuration identity, compiled request
digest, candidate ID (`Estimate.CandidateID`), classes, family, tier and capability
version; its immutable original input supplies the operation key. This helper
does not load or write those records itself.

Recovery rechecks configured tenant/region permissions, required features,
extension support, context limits and authorized classes with the deterministic
routing eligibility rules. Only the original candidate ID and full route/cache
identity may pass. It does not call the injected selection planner, take the
first eligible route or fall back. Candidate IDs must use the deterministic
routing identity used by the original selection.

The prepared provider input must have the original resolved request digest and
operation key before adapter lookup. Capabilities and the compiled call then
pass the same binding checks used for new selection. Both paths share the
compilation implementation. Compilation receives detached input with the
resolved provider model, attempted class, no fallback list and the original
strict/best-effort portability.

A disabled, health-open or auth-open original route blocks recovery with
`no_route` and `same_operation`. Ordinary adapter-resolution/capability/compile
failures return a sanitized `state_unavailable`/`same_operation` error.
Incompatible configuration, identity or compiled bindings fail with
`configuration`/`never`. No error authorizes a different route. A compiler
reporting possible paid dispatch returns an ambiguous, non-retryable error;
context cancellation returns no usable call.

Only a snapshot with the original configuration digest and epoch can reconstruct
the call. A changed configuration is rejected rather than silently substituted.
Historical snapshot loading is outside this helper. Snapshot catalogs and
health are detached from later mutations; the injected registry must also belong
to the captured snapshot. The recovered candidate retains its configured price
version: use the saved route and exact quote/reservation for admission, including
any price version supplied by the original quote. A route that now compiles
without a price version, because its catalog schedules a version change, also
recovers a selection recorded under the candidate ID it carried while the bound
price version was still pinned, so plans admitted before an upgrade dispatch.

A reconstructed `PlannedProviderCall` is invocation-local compilation data.
It does not establish whether the old attempt was submitted or consumed a claim.
Composition must inspect durable provider/claim state before any submission;
only a fresh Redis claim grants dispatch permission. This helper does not invoke
or poll providers, authorize caller scope, mutate budgets, renew leases, refund
paid work, implement outcome-unknown retries or install production phase
factories. It can be integrated with cloud budget-plan persistence separately.

Offline tests compare reconstructed requests from the real OpenAI Responses
compiler for both activities with zero HTTP calls. They cover serialization,
second-route recovery, no fallback, changed input/key/scope/identity, configured
eligibility, health blocks, snapshot reloads and mutations, quote-version
retention, compiler failures, cancellation, possible dispatch and concurrent
reconstruction. They do not establish live provider, Redis, cloud or Temporal
behavior.
