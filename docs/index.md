# LLM Temporal Worker

This repository contains a reusable Go library and Temporal Activity Worker
that performs large-language-model inference. The worker presents one versioned
request contract, selects a configured endpoint, enforces cost budgets, invokes
the provider through an official SDK, and returns a normalized response plus a
durable continuation handle.

The implementation foundation and production process composition are checked
in alongside the architecture and active v1 completion plans. The plans
identify remaining hardening and release evidence; the current code and its
tests are the source of truth for behavior that has already been implemented.

The staged target design is documentation-only and not yet implemented. It
replaces the unreleased v1 contract in place through independently releasable
durable-conversation, compaction/budget, optional route-isolated cache, and
typed-query phases. Cross-provider cache equivalence and FX are future ADRs,
not current schema or release gates. [Scope](scope.md#staged-delivery-and-document-authority)
is the single status/authority index:

- [Conversation checkpoints, cache affinity, and compaction](architecture/conversation-checkpoints-and-compaction.md)
- [PostgreSQL state, cache, accounting, and control plane](architecture/postgresql-state-cache-and-control-plane.md)
- [PostgreSQL repository foundation](reference/postgresql-repository-foundation.md)
- [OCaml conversation and typed query client](architecture/ocaml-conversation-and-query-client.md)
- [Production implementation plan](superpowers/plans/2026-07-18-forkable-conversation-state.md)
- [Maintenance retention and outbox contract](reference/maintenance.md)

## Non-negotiable v1 decisions

| Area | Decision |
| --- | --- |
| Public processing class | Exactly `economy`, `standard`, or `priority`; omission normalizes to `standard` |
| Provider abstraction | Semantic item IR compiled to a provider request; never a lowest-common-denominator text blob |
| Provider clients | Official OpenAI, Anthropic, AWS, and Temporal Go SDKs; raw HTTP is isolated to a documented SDK gap |
| Retry ownership | Provider SDK retries are disabled; the worker classifies outcomes and Temporal owns durable retries |
| Ambiguous dispatch | Never resend automatically when a provider may have accepted a billable request |
| Continuation | Immutable opaque handles backed by a state store and pinned to an endpoint when provider state requires it |
| Budget accounting | Conservative preflight reservation across every matching sliding window, followed by refund/finalization |
| Shared state | Redis is the v1 shared-state backend for conservative active-budget admission, throttles, and replica coordination; the configured blob store holds oversized payloads and results; memory is single-process development/test only |
| Activity scope | Generate, Compact, and typed Query only; tool execution and agent-loop orchestration stay in caller workflows |
| Response delivery | The v1 public contract exposes only one-shot `Generate` and a final normalized response; live streaming and token-event APIs are not supported. Compact and Query are separate final-response Activities |
| Deployment | One stateless worker image, horizontally scalable when replicas share the configured Redis and production blob-store dependencies |
| Go baseline | Go 1.26, using the latest security patch in that release line |

## Read in this order

1. [Scope and success criteria](scope.md)
2. [System overview](architecture/system-overview.md)
3. [Unified API](architecture/unified-api.md)
4. [Provider adapters](architecture/provider-adapters.md)
5. [Routing and continuation](architecture/routing-and-continuation.md)
6. [Pricing and budgets](architecture/pricing-and-budgets.md)
7. [Temporal worker](architecture/temporal-worker.md)
8. [State and storage](architecture/state-and-storage.md)
9. [Deployment and operations](architecture/deployment-and-operations.md)
10. [Security and privacy](architecture/security-and-privacy.md)
11. [Testing strategy](testing/strategy.md)
12. [Master implementation sequence](superpowers/plans/2026-07-13-master-sequence.md)
13. [V1 completion execution plan](superpowers/plans/2026-07-14-v1-completion.md)
14. [Target conversation/cache/control design](architecture/conversation-checkpoints-and-compaction.md)
15. [Target Redis-budget/PostgreSQL design and exact indexes](architecture/postgresql-state-cache-and-control-plane.md)
16. [Target OCaml client design](architecture/ocaml-conversation-and-query-client.md)
17. [Staged target implementation sequence](superpowers/plans/2026-07-18-forkable-conversation-state.md)

## Reference material

- [Package layout](architecture/package-layout.md)
- [Configuration reference](reference/configuration.md)
- [Command-line reference](reference/cli.md)
- [Catalog loader contract](reference/catalog-loaders.md)
- [Error model](reference/error-model.md)
- [Verified upstream contracts](reference/source-contracts.md)
- [AWS Bedrock Converse provider](reference/provider-bedrock-converse.md)
- [Generic compaction safeguards](reference/generic-compaction.md)
- [v1 Activity runtime boundary](reference/activity-runtime.md)
- [Query execution audit ledger](reference/query-audit-ledger.md)
- [Guarded live-provider contracts](reference/live-provider-contracts.md)
- [Adapter fixture matrix](testing/fixture-matrix.md)
- [Architecture decisions](decisions/)
- [Redis budget generation recovery runbook](runbooks/redis-budget-generation-recovery.md)

## v1 completion gate

The first release is complete only when all of these statements are true:

- The same semantic request passes contract tests against OpenAI Responses,
  OpenAI-compatible Chat Completions, Anthropic Messages, Claude Platform on
  AWS, and Amazon Bedrock Anthropic profiles supported by the capability
  matrix.
- Every adapter has request, response, error, and usage golden tests;
  strict-mode lossy conversions fail before dispatch.
- The router is deterministic for a fixed configuration snapshot and never
  changes service class without an explicit request fallback.
- The operation ledger returns a cached completed result on an Activity retry
  and refuses to replay an ambiguous provider dispatch.
- The in-memory exact-budget reference model and production Redis Function pass
  the same atomic-window transition suite, including concurrent
  overlapping-window admission tests.
- The worker passes Temporal Activity tests for retry, heartbeat, cancellation,
  graceful shutdown, and non-retryable error typing.
- The container runs as a non-root user, readiness probes exercise real Redis
  and configured blob-store dependencies, and two-replica Redis-backed smoke
  tests pass.
- Pull-request and master GitHub Actions are green; master also builds daily at
  05:00 in `Australia/Sydney`.

## v1 traceability status

The [partial v1 traceability catalog](release/v1-requirements.json) records
source-backed implementation and verification references. Its recorded states
are not release evidence: they do not claim a candidate, provider invocation,
signing, publication, tag, or release result.
