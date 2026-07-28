# Forkable Conversation State and Control Plane Implementation Plan

> **For the implementing agent:** Execute tasks in order on a clean HTTPS-backed
> branch. This document is a production contract, not an instruction to
> implement a known defect. Do not silently redesign it; when implementation
> evidence requires a deviation, first add a short superseding ADR amendment
> describing the changed invariant, alternatives, schema/API/test impact, and
> rationale, then update affected fixtures and this plan.

**Goal:** Replace repeated full-history inference calls with immutable forkable
checkpoints, exact-response caching, provider-cache affinity, isolated
compaction, resumable provider polling, and typed status/model/credit/budget/
spend queries in Go and the existing OCaml package.

**Architecture:** PostgreSQL is the durable system of record. Redis remains the
atomic, low-latency, provably-current materialization and coordination
optimization for the complete active budget working set and operational throttles. PostgreSQL stores
the durable operation/budget journal, exact-decimal USD cost facts, checkpoints,
cache entries, provider state, health/inventory, and maintenance state. Normal
budget decisions perform no PostgreSQL reads; Redis can be rebuilt from the
journal only on a full-service cold start or detected non-persistent Redis loss.
Both Redis and the worker-owned PostgreSQL namespace are configurable. The
unreleased **llm.generate.v1** contract is replaced
in place and appends a delta to an
immutable parent. **llm.compact.v1** creates a lossily summarized child with
application tools and structured output disabled. **llm.query.v1** is a closed
tagged union mirrored by an OCaml GADT and uses a dedicated bounded inline audit
ledger rather than the paid LLM operation/blob state machine.

**Primary references:**

- [Conversation checkpoints and compaction](../../architecture/conversation-checkpoints-and-compaction.md)
- [PostgreSQL schema and query protocols](../../architecture/postgresql-state-cache-and-control-plane.md)
- [OCaml conversation and query client](../../architecture/ocaml-conversation-and-query-client.md)
- [ADR 0006](../../decisions/0006-forkable-conversation-checkpoints.md)
- [ADR 0007](../../decisions/0007-postgresql-authoritative-state-and-response-cache.md)
- [ADR 0008](../../decisions/0008-resumable-provider-operations-and-typed-queries.md)

## Delivery phases

Execute and release phases independently; later phases may not hold an earlier
one hostage:

- **Phase A — durable conversation core:** Tasks 0-5, 7, 12, 15-17, and the
  matching slices of 19-21. Exit with PostgreSQL operations/attempts, encrypted
  payloads, forkable checkpoints, exact USD costs, restart-safe polling, and the
  Generate OCaml facade. Preserve existing Redis throttling until Phase B.
- **Phase B — compaction and Redis materialization:** Tasks 6, 10, 11 and their
  integration/operations slices. Exit with the exact PostgreSQL budget journal,
  conservative nano-USD Redis decision/coordination layer, adopt/rebuild proof,
  runbook, prompt-cache affinity, and compaction.
- **Phase C — opt-in exact-response cache:** Tasks 8-9 only after naming a
  staging workflow or incident-reproduction caller and recording baseline spend
  and expected reuse. The initial cache is route-isolated.
- **Phase D — typed control queries:** Tasks 13-14 and 18 using the dedicated
  bounded `query_executions` ledger, followed by the query slices of 19-21.
- **Future ADRs:** cross-provider response-cache equivalence and FX. Do not add
  their schema, configuration, packages, fixtures, or release gates in these
  phases.

At each exit, update release traceability only for that phase and leave later
requirements explicitly pending. The phase owner may split commits further;
task numbering is dependency guidance, not a mandate to publish one giant PR.

## Current implementation and evidence status (2026-07-29)

This plan remains the implementation contract; its task checkboxes are the
original work breakdown and are not a release-evidence ledger. The following
summary records what is present on `master` and what still requires a protected
deployment run.

Implemented repository slices include:

- **Task 0, Redis namespace isolation.** The validated prefix and direct
  environment override are wired through the runtime and Redis stores in
  [`config/types.go`](../../../golang/config/types.go),
  [`config/load.go`](../../../golang/config/load.go), and
  [`internal/runtime/factory.go`](../../../golang/internal/runtime/factory.go).
  Prefix validation and two-prefix isolation are covered by
  [`config/redis_prefix_test.go`](../../../golang/config/redis_prefix_test.go),
  [`storage/redis/prefix_isolation_test.go`](../../../golang/storage/redis/prefix_isolation_test.go),
  and the opt-in shared-state conformance fixture
  [`storage/redis/shared_state_conformance_integration_test.go`](../../../golang/storage/redis/shared_state_conformance_integration_test.go).
- **Tasks 17-18, OCaml protocol and typed facade.** The nominal identifiers,
  exact decimal models, Activity descriptors, immutable conversation facade,
  and typed Query GADT are split across the
  [`ocaml/llm_temporal_worker/lib`](../../../ocaml/llm_temporal_worker/lib)
  modules. Checked-in offline coverage is provided by
  [`test_activity.ml`](../../../ocaml/llm_temporal_worker/test/test_activity.ml),
  [`test_conversation.ml`](../../../ocaml/llm_temporal_worker/test/test_conversation.ml),
  [`test_query.ml`](../../../ocaml/llm_temporal_worker/test/test_query.ml),
  and the downstream
  [`consumer_workflow_smoke`](../../../ocaml/consumer_workflow_smoke) fixture.
  These checks do not stand in for protected production-provider or release
  evidence.
- **Task 20, PostgreSQL maintenance primitives.** Bounded retention, blob GC,
  outbox fencing, and conservative operation/checkpoint/cache safety are
  implemented in [`storage/postgres/maintenance.go`](../../../golang/storage/postgres/maintenance.go),
  [`storage/postgres/blob_gc.go`](../../../golang/storage/postgres/blob_gc.go),
  and [`storage/postgres/retention_batch.go`](../../../golang/storage/postgres/retention_batch.go),
  with integration coverage in
  [`maintenance_integration_test.go`](../../../golang/storage/postgres/maintenance_integration_test.go)
  and [`blob_gc_integration_test.go`](../../../golang/storage/postgres/blob_gc_integration_test.go).
  Query-plan and budget-read classifier fixtures are present in
  [`query_plan_integration_test.go`](../../../golang/storage/postgres/query_plan_integration_test.go)
  and [`sql_classifier_integration_test.go`](../../../golang/storage/postgres/sql_classifier_integration_test.go).

The following blockers are intentionally still open:

1. **Task 19 production composition.** The production factory exposes optional
   `V1RuntimeBuilder`, `GeneratePortsFactory`, and `CompactPortsFactory` seams,
   but does not construct the Redis/PostgreSQL/provider/checkpoint/materializer
   composition or its cross-store recovery state machine. The fail-closed
   boundary is documented in
   [`postgresql-repository-foundation.md`](../../reference/postgresql-repository-foundation.md)
   and [`internal/runtime/factory.go`](../../../golang/internal/runtime/factory.go).
   Enabling it requires deployment-owned blob locators, key bindings,
   authorization, and protected Redis/PostgreSQL failure tests.
2. **Task 20 operations.** There is no worker maintenance command: the
   documented `llmtw-maintenance retention-once` adapter must be owned by the
   deployment so runtime credentials cannot acquire the maintenance role.
   Full operation/journal/reservation retention and autovacuum/fillfactor load
   evidence remain deliberately disabled until their foreign-key and rebuild
   obligations are proven; see [`maintenance.md`](../../reference/maintenance.md).
3. **Task 21 protected recovery evidence.** The repository still needs the
   authorized 10,000-turn/100-way crash and concurrency runs, SQL zero-budget-
   read assertions across the composed runtime, and PostgreSQL/blob/Redis
   backup-restore and fenced-generation-rebuild evidence. Offline unit tests,
   opt-in integration fixtures, and CI compilation do not prove those gates.

Do not mark these deployment or protected-evidence items complete merely
because the corresponding ports, repositories, or deterministic tests exist.

## Global constraints

All Go source/test paths below are relative to the repository's current
**golang/** module after rebasing onto `master`; run Go/Make commands from that
directory unless a command explicitly says otherwise.

- Do not modify or overlap any Temporal-owned PostgreSQL relation. Support a
  configurable worker database, schema, and relation-name prefix. Recommend a
  dedicated database and schema, but test shared-server, shared-database, and
  explicitly prefixed shared-schema layouts. Keep worker roles, backup policy,
  and readiness checks explicit.
- PostgreSQL and public v1/OCaml money are exact USD decimals with 18
  fractional digits. No float, generic currency field, or externally injected
  FX value is accepted.
- Redis and PostgreSQL remain required in durable mode and own different budget
  roles. PostgreSQL is the record; Redis atomically decides against its
  conservative active-window materialization. PostgreSQL receives idempotent
  journal/projection writes before dispatch and is the exceptional rebuild
  source. Never claim a cross-store transaction.
- Budget-read exceptions, adopt-if-intact behavior, Stream optionality, outage
  trade, and workload/test envelope are normative only in the
  [control-plane design](../../architecture/postgresql-state-cache-and-control-plane.md#the-only-postgresql-budget-read-conditions).
  Tests link to those rules rather than restating variants.
- Operation replay, worker exact-response cache, and provider prompt cache
  remain three separate mechanisms.
- Cache opt-in requires **max_age_seconds**. Omission means neither read nor
  populate.
- Variant is non-negative int32. A positive variant requires materialized
  temperature explicitly greater than zero and is never sent as provider seed.
- Phase C exact-cache keys include a keyed route identity. Cross-provider cache
  equivalence is deferred until a concrete verifiable pair receives a
  superseding ADR and new cache epoch.
- The same checkpoint may have arbitrarily many immutable children.
- Provider poll IDs are persisted before polling. Restart resumes the ID and
  never submits again.
- A compaction request has no application tools, tool choice is none, and no
  application structured-output configuration. The subsequent Generate
  restores those settings.
- Every completed Generate/Compact/cache hit and dedicated query execution has an explicit
  exact-or-unknown cost state. Exact uses **actual_cost_usd NUMERIC(38,18)**;
  unknown uses NULL plus a safe reason. Only confirmed free work is exact zero.
- Provider SDK automatic retries remain zero. Never convert the external
  acceptance/persistence gap into an exactly-once claim.
- Prompt, output, tool data, provider IDs, raw errors, cache fingerprints, and
  tenant identifiers stay out of logs/metrics/traces.
- Durable operations store content-free JSONB manifests plus authenticated
  envelope-encrypted inline/blob payloads; raw prompt/tool/output text is never
  ordinary JSONB or plaintext bytea.
- Keep **state.kind=memory** as an explicitly non-durable single-process
  development mode. Reject it in production and multi-replica configurations;
  it requires neither Redis, PostgreSQL, nor an external blob store.
- Begin each behavior task with a failing test and finish with a focused commit.
- This is a pre-release contract replacement. Do not add a `v2`, compatibility
  adapter, data import, backfill, table rename, dual-read, dual-write, legacy
  fallback, or migration plan for this change. Initialize clean PostgreSQL and
  Redis namespaces and replace the unreleased v1 fixtures/contracts atomically.
  After the first release, any incompatible change must receive a separate,
  explicit migration and compatibility design.

## Branch and dependency preparation

- [ ] Run **git status --short**, **git remote get-url origin**, and
  **git branch --show-current**. Stop for unrelated changes.
- [ ] Fetch/rebase from
  **https://github.com/mfow/llm-temporal-worker.git master**.
- [ ] Confirm merged PR 109's OCaml continuation/expiry validation remains in
  the implementation base, and inspect any later OCaml validation PRs. Do not
  overwrite stronger codecs or nominal-ID invariants.
- [ ] Confirm the rebased tree still has the Go worker under **golang/**. Treat
  every Go path in Tasks 0-21 as relative to that module and run Go/Make commands
  there; repository-level docs/deploy paths remain relative to the repository.
- [ ] Record current Go, OCaml, PostgreSQL, Temporal, Redis, and provider SDK
  baselines in the implementation PR.
- [ ] Add only reviewed current security-patched PostgreSQL client and
  exact-decimal dependencies. Record checksums and licenses.
- [ ] Run **make verify** and the existing OCaml Dune test from a clean base.
  Record unrelated baseline failures before changing code.

---

### Task 0: Make the existing Redis key namespace configurable

**Files:**

- Modify: **config/types.go**, **config/validate.go**, **config/load.go**
- Modify: **api/schema/v1/config.schema.json**
- Modify: **internal/runtime/factory.go**, **storage/redis/options.go**
- Modify: **deploy/docker-compose.yml**, **deploy/kubernetes/**
- Test: **config/redis_prefix_test.go**
- Test: **storage/redis/prefix_isolation_test.go**
- Test: **internal/runtime/factory_redis_prefix_test.go**

- [ ] Add **state.redis.key_prefix**, default **llmtw**, and the direct
  environment override **LLMTW_REDIS_KEY_PREFIX**. Apply direct non-secret
  environment overrides before validation and effective-config rendering.
- [ ] Accept only **[A-Za-z0-9][A-Za-z0-9._-]{0,63}**. Reject empty, colon,
  braces, whitespace, control characters, overlength values, and any value
  that would make a Redis Cluster hash tag or ACL pattern ambiguous.
- [ ] Construct one validated **KeyOptions** in the runtime factory and pass it
  to admission, continuation, throttling, replay, and every other worker-owned
  Redis store. Remove literal `llmtw` fallbacks from individual factories.
- [ ] Preserve the existing Redis Cluster hash-tag placement inside the
  prefix. Do not treat the prefix as a tenant boundary, authentication control,
  or replacement for tenant/project hashing.
- [ ] Render the effective prefix in **print-effective-config**, readiness
  diagnostics, Compose, and Kubernetes examples. Document the required ACL as
  `~<key-prefix>:*` while keeping Redis function-library naming and
  lifecycle a separately reviewed global concern.
- [ ] Run the complete Redis conformance suite twice against one Redis server,
  using two prefixes, and prove replay, budgets/throttling, continuation,
  cleanup, and expiry cannot observe or delete the other namespace.
- [ ] Treat the prefix as immutable for the worker process lifetime. A changed
  prefix selects a clean namespace; do not copy, rename, scan, dual-read,
  dual-write, or fall back to the old prefix.
- [ ] Commit: **feat(config): make redis key namespace explicit**.

**Implementation status (2026-07-27):** The Redis namespace contract is already
implemented on `master` by PR #127 (`7c4b224c`). The implementation uses the
validated `state.redis.key_prefix` value throughout the worker-owned Redis
stores, applies `LLMTW_REDIS_KEY_PREFIX` before validation, and rejects reload
changes for the lifetime of a process. Follow-up coverage includes two-prefix
integration conformance and hash-tag isolation in
`storage/redis/shared_state_conformance_integration_test.go` and
`storage/redis/prefix_isolation_test.go`. The unchecked boxes above are kept as
the original implementation checklist; release evidence for live conformance
remains tracked separately in `docs/release/v1-requirements.json`.

### Task 1: Freeze the initial v1 JSON contracts and canonical fixtures

**Files:**

- Modify: **api/schema/v1/generate-request.schema.json**
- Modify: **api/schema/v1/generate-response.schema.json**
- Create: **api/schema/v1/compact-request.schema.json**
- Create: **api/schema/v1/compact-response.schema.json**
- Create: **api/schema/v1/query-request.schema.json**
- Create: **api/schema/v1/query-response.schema.json**
- Modify: **llm/testdata/v1/**
- Modify: **llm/schema/**, **activity/types.go**, **activity/payload.go**
- Test: **llm/schema/v1_contract_test.go**
- Test: **activity/v1_payload_test.go**

- [ ] Write fixtures for root/delta/fork, every Set/Clear/omitted patch,
  cache omitted/present, int32 boundaries, positive variant with zero/positive/
  unknown temperature, each cache disposition, Compact cache omitted/present,
  Compact positive-variant rejection, and every Query tag.
- [ ] Add negative fixtures for unknown fields/tags, mismatched query result,
  currency fields, numeric rather than string USD, tool/structured-output fields
  on Compact, oversized pages/cursors, and transcript fields in Generate responses.
- [ ] Define canonical decimal strings with 0 to 18 fractional digits on input
  and one normalized representation on output.
- [ ] Define **llm.generate.v1**, **llm.compact.v1**, and **llm.query.v1**
  constants. Keep request and result schemas independently versioned.
- [ ] Replace the existing unreleased Generate v1 schema and fixtures in place.
  Do not create an `api/schema/v2` tree, a v2 Activity name, or compatibility
  codecs for the superseded pre-release shape.
- [ ] Run focused schema/payload tests. Expected: fail before types/codecs exist.
- [ ] Implement only schema parsing and closed wire records; do not dispatch.
- [ ] Run **make schema-verify** and canonical JSON tests. Expected: pass.
- [ ] Commit: **feat(contract): define checkpoint cache compact and query APIs**.

**Implementation status (2026-07-28):** The Task 1 contract slice is already
implemented on `master` across commits `48fd9031`, `50652b5f`, `f34a5888`, and
`49e6be23`. It includes the closed v1 JSON schemas, canonical valid and negative
fixtures, schema verification, and focused payload-codec/round-trip tests for
Generate, Compact, and Query. As described in the Global constraints, the paths
listed above are relative to `golang/` in the current worker layout. The
unchecked boxes above remain the original implementation checklist; protected
release evidence for the contract and its production conformance is tracked
separately in `docs/release/v1-requirements.json`.

### Task 2: Replace generic currency/microUSD authority with exact USD decimal

**Files:**

- Modify: **pricing/decimal.go**, **pricing/money.go**, **pricing/cost.go**
- Modify: **pricing/entry.go**, **pricing/catalog.go**
- Modify: **budget/**, **admission/**, **llm/response.go**
- Modify: **api/schema/v1/generate-response.schema.json**
- Test: **pricing/usd_decimal_test.go**
- Test: **pricing/usd_decimal_property_test.go**

- [ ] Define a non-negative fixed-scale USD decimal backed by checked big
  integer/rational arithmetic and capped to **NUMERIC(38,18)**.
- [ ] Write parse/add/subtract/multiply/divide/round-trip/overflow tests,
  including values smaller than one microUSD, 18 fractional digits, $1, $10,
  the maximum 20-whole-digit value, and one-digit overflow.
- [ ] Prove no float conversion in price, estimate, reconciliation, JSON, SQL,
  metrics input, or query aggregation. Add a source-policy test for forbidden
  money float/currency fields.
- [ ] Rename public/database concepts to **reserved_cost_usd**,
  **incurred_cost_usd**, **actual_cost_usd**, and **limit_usd**.
- [ ] Define exact/unknown cost variants. NULL is unknown; zero is known free.
  Keep estimates, reservations, retained budget bounds, and actual cost as
  distinct types/columns. Never coalesce unknown actual cost to zero.
- [ ] Make catalog component prices nullable with exact/partial/unknown status,
  closed unknown-component codes, and a safe reason. Only an exact catalog can
  drive catalog costing or monetary admission.
- [ ] Remove generic **currency** from price compilation and downstream Cost.
  Remove **pricing.currency** configuration and v1/OCaml response currency in
  the same contract replacement.
- [ ] Rewrite the existing pre-release fixtures at the same time and delete the
  integer-microUSD and generic-currency codecs. Do not retain a conversion
  adapter or legacy read path.
- [ ] Run **go test -race ./pricing ./budget ./admission ./llm/...**.
- [ ] Commit: **refactor(money): make exact usd decimal authoritative**.

### Task 3: Add the initial PostgreSQL schema and physical namespace contract

**Files:**

- Create: **storage/postgres/schema/000001_worker_state.sql**
- Create: **storage/postgres/install_schema.go**
- Create: **storage/postgres/namespace.go**
- Create: **storage/postgres/schema_contract_test.go**
- Create: **storage/postgres/index_contract_test.go**
- Create: **storage/postgres/testdb_test.go**
- Modify: **go.mod**, **go.sum**, **Makefile**, CI service configuration

- [ ] Add configuration and direct environment overrides for
  **state.postgres.database/LLMTW_POSTGRES_DATABASE**,
  **state.postgres.schema/LLMTW_POSTGRES_SCHEMA**, and
  **state.postgres.table_prefix/LLMTW_POSTGRES_TABLE_PREFIX**. Apply them before
  validation and print the effective non-secret selection.
- [ ] Validate database/schema as **[a-z][a-z0-9_]{0,62}** and the table prefix
  as empty or **[a-z][a-z0-9_]{0,22}_**, with a 24-byte maximum. Reject quoted,
  mixed-case, overlength, control-character, and unqualified values.
- [ ] Build every identifier through one namespace object and safe driver
  identifier quoting. Never use `search_path`, string concatenation, or an
  Activity-supplied identifier. Check **current_database()** against the
  configured database before schema installation and at readiness.
- [ ] Transcribe the final DDL from the PostgreSQL architecture document,
  including every check, unique key, FK, partial/covering/BRIN index, fillfactor,
  and deferred cycle FK. Apply the configured prefix to every worker-owned
  table, index, sequence, constraint, view, function, and schema-version object.
- [ ] Make schema-owner, runtime, and maintenance role grants explicit. Runtime cannot
  update/delete checkpoint tables; maintenance is separate.
- [ ] Add a prefixed schema-contract/version marker for clean installation and
  readiness verification. Startup verifies the exact expected contract but
  never mutates schema.
- [ ] Test a clean install, idempotent contract check, least-privilege runtime
  operations, and destructive test-only teardown in an ephemeral database.
- [ ] Query **pg_catalog** to assert exact numeric precision/scale, FK support
  indexes, partial predicates, uniqueness, fillfactor, and no money/float type.
- [ ] Assert the budget schema exactly includes the journal identity/unique
  event keys, window/bucket/journal rebuild covering index, operation/journal
  index, journal-time BRIN, one-active-generation partial unique index,
  generation-history covering index, bucket projection primary/retention
  indexes, reservation window covering index, and open-reservation partial
  index, plus the generation and last-journal foreign-key support indexes,
  specified in the architecture DDL. Do not substitute a broad JSONB or
  standalone low-cardinality index.
- [ ] Assert operations have bounded canonical request JSONB and cache entries
  have canonical manifest JSONB, with no broad JSONB index; fixed-size
  HMAC-SHA-256 columns own lookup.
- [ ] Test dedicated database/schema, a shared server, a shared database with a
  dedicated schema, and an explicitly prefixed shared schema. Reject any
  computed worker object name that collides with a configured Temporal-owned
  relation; do not require the database itself to differ.
- [ ] Assert all generated identifiers fit PostgreSQL's 63-byte limit without
  truncation and are collision-free for the complete schema object catalog.
- [ ] Do not implement a data importer, backfill, Redis scan, relation rename,
  dual storage, or upgrade runner. This task installs the first schema into a
  clean selected namespace. A future post-release schema change will need a
  separately designed migration mechanism.
- [ ] Add **make postgres-integration** and a CI PostgreSQL service.
- [ ] Run schema/index tests against the pinned PostgreSQL image.
- [ ] Commit: **feat(postgres): add authoritative worker schema**.

### Task 4: Implement PostgreSQL repository foundation

**Files:**

- Create: **storage/postgres/pool.go**
- Create: **storage/postgres/transaction.go**
- Create: **storage/postgres/scope.go**
- Create: **storage/postgres/configuration.go**
- Create: **storage/postgres/blob.go**
- Create: **storage/postgres/codec.go**
- Create: **storage/postgres/crypto.go**
- Test: **storage/postgres/foundation_integration_test.go**
- Test: **storage/postgres/crypto_test.go**

- [ ] Add bounded pgx pool configuration, TLS verification, UTC session setup,
  statement/lock/idle transaction timeouts, health checks, and redacted errors.
- [ ] Implement UUIDv7 and keyed tenant/project HMAC derivation with rotation
  version in application configuration.
- [ ] Envelope-encrypt locators/provider references with authenticated context;
  test wrong scope/key/tamper and rotation.
- [ ] Add exact NUMERIC encode/decode that rejects rounding, exponent ambiguity,
  negative values, NaN, infinity, excess scale, and overflow.
- [ ] Implement scope/config/blob repositories using statements compiled from
  the validated namespace, fully qualified identifiers, positional values, and
  context deadlines. Test that values can never alter identifiers.
- [ ] Add fault injection at Begin/Commit/network loss and ensure no transaction
  remains open across blob/provider I/O.
- [ ] Run **go test -race ./storage/postgres -run Foundation**.
- [ ] Commit: **feat(postgres): add scoped repository foundation**.

### Task 5: Move operation replay, attempts, results, and cost to PostgreSQL

**Files:**

- Create: **storage/postgres/operation.go**
- Create: **storage/postgres/attempt.go**
- Create: **storage/postgres/result.go**
- Modify: **admission/store.go**, **admission/types.go**
- Modify: **storage/conformance/conformance.go**
- Test: **storage/postgres/operation_integration_test.go**
- Test: **storage/postgres/operation_concurrency_test.go**

- [ ] Extend conformance for operation kinds, exact decimal costs, provider
  pending, multiple attempts, scoped conflict, result digest, and expiry.
- [ ] Implement unique-key Begin with digest conflict and completed replay.
- [ ] Store the complete normalized Generate v1 Activity request JSONB and verify it with
  the request-fingerprint HMAC on operation-key conflict.
- [ ] Implement compare-and-set state/lease transitions and documented lock
  order. Only provably pre-write expired leases can be reclaimed.
- [ ] Persist each route attempt rather than overwriting only final route facts.
- [ ] Enforce completed implies result and a valid exact-or-unknown USD cost
  state. Exact requires amount/method; unknown requires NULL amount/method plus
  a safe reason. Cache/query zero is explicit exact zero.
- [ ] Test 100 concurrent same-key Begins, different-digest conflict, crash
  before/after commit, terminal expiry lookup, and no sensitive SQL/log output.
- [ ] Replace the old interchangeable-store conformance assumption: operation
  replay/result/cost conformance targets memory and PostgreSQL; Redis receives
  the separate active-budget/throttle conformance in Task 6.
- [ ] Commit: **feat(postgres): persist operations attempts results and cost**.

### Task 6: Split atomic Redis budgets from the PostgreSQL durable journal

**Files:**

- Create: **storage/postgres/budget_journal.go**
- Create: **storage/redis/budget_manifest.go**
- Create: **storage/redis/budget_stream.go**
- Create: **storage/redis/budget_bootstrap.go**
- Modify: **storage/redis/function/**
- Modify: **budget/policy.go**, **budget/window.go**
- Modify: **admission/transition.go**
- Test: **storage/redis/budget_nano_usd_test.go**
- Test: **storage/redis/budget_generation_integration_test.go**
- Test: **storage/postgres/budget_journal_integration_test.go**
- Test: **integration/budget_cross_store_recovery_test.go**
- Test: **integration/budget_postgres_read_policy_test.go**

- [ ] Persist compiled policies/windows from the immutable configuration
  snapshot with exact **limit_usd** in PostgreSQL and materialize the complete
  active horizon in Redis, including explicit zero bucket fields.
- [ ] Keep exact **NUMERIC(38,18)** only in PostgreSQL/Go. Convert in Go without
  float to safe-integer nano-USD: ceil every positive charge/reservation, floor
  every limit, store each applied integer by operation/window, and subtract that
  stored value during finalize/release. Reject limits above
  **9007199.254740991 USD**. Prove the materialization can over-throttle but
  never under-throttle, and rebuild derives identical integers.
- [ ] Add the active-generation pointer, manifest, policy/window hashes,
  operation reservation index, **budget:events** Stream, and **budget:workers**
  leases under the configured prefix/hash tag. Validate generation, manifest,
  complete coverage, every expected member, config/price version, and operation
  identity inside each atomic Function.
- [ ] Make one Function atomically check all matching monetary/request/token/
  concurrency windows, acquire the idempotent reservation, and XADD the change
  event. Reconcile/release through equally idempotent Functions. Use Redis
  `TIME`; do not hold a PostgreSQL transaction during a Redis call.
- [ ] Let every worker tail the Stream independently with `XREAD`, storing its
  cursor in its lease. Do not use one consumer group. Make this an explicit
  coordination optimization for hint invalidation, generation-switch wake-up,
  and reduced batch-drain polling: local state may reject early but never
  authorize. A disabled tailer or gap discards hints and reloads Redis only;
  prove correctness is unchanged. Trim behind the minimum non-expired worker
  cursor plus a safety margin.
- [ ] Give each Go process a random in-memory session ID and persist a bounded
  roster entry in the Redis generation. A still-running process reconnecting
  after a persistent Redis outage presents the same session and never triggers
  a PostgreSQL budget read even if its lease expired. New processes get new
  sessions; do not treat a pod/host name as stable identity.
- [ ] After Redis acceptance, use one PostgreSQL write transaction to insert the
  idempotent journal event and conditionally update bucket/reservation
  projections via `INSERT ... ON CONFLICT DO NOTHING RETURNING`. Dispatch only
  after commit. A write failure triggers best-effort Redis release and never a
  provider call. Finalization commits journal/cost first, then reconciles Redis.
- [ ] Instrument SQL at the repository boundary and fail tests if normal
  admission, completion, budget status, a joining worker, a single-worker
  restart, or a Stream gap issues a SELECT against any PostgreSQL budget table.
- [ ] Implement the normative adopt-if-intact, session/incarnation, exceptional
  PostgreSQL-read, same-incarnation failure, and fenced-generation-rebuild state
  machine exactly as specified in the
  [control-plane design](../../architecture/postgresql-state-cache-and-control-plane.md#the-only-postgresql-budget-read-conditions).
  Do not create a second description of the eligibility rules in Go comments or
  operator configuration.
- [ ] Test crash at Redis acquire, PostgreSQL journal commit, dispatch,
  PostgreSQL completion, and Redis reconcile; duplicate events; partial Redis
  key loss; FLUSHDB/non-persistent restart; persistent Redis restart; full fleet
  restart; rolling restart; worker lease expiry; Stream trimming/gaps; and
  racing completion during rebuild. Failures may over-reserve, never overspend.
- [ ] Assert accepted accounted charges plus active reservations never exceed
  any window under concurrency and run the bounded sizing plus queued-batch
  tests defined by the normative workload envelope. Do not turn that envelope
  into configuration or a runtime ceiling.
- [ ] Finalize
  **docs/runbooks/redis-budget-generation-recovery.md** by replacing every
  design-time placeholder with tested deployment commands and metric names.
  Preserve its dependency posture, evidence capture, adopt-if-intact,
  persistent/new-incarnation/same-incarnation cases, fenced replacement,
  verification, rollback, and escalation. Never recommend `FLUSHDB` or
  unfenced key edits.
- [ ] Capture indexed **EXPLAIN (ANALYZE, BUFFERS)** only for the exceptional
  active-horizon/journal rebuild queries; no SQL active-window-sum query exists.
- [ ] Commit: **feat(budget): keep atomic redis budgets with durable postgres journal**.

### Task 7: Implement immutable checkpoint graph and materializer

**Files:**

- Create: **state/checkpoint.go**
- Create: **state/settings_patch.go**
- Create: **state/materialize.go**
- Create: **state/model_state.go**
- Create: **storage/postgres/checkpoint.go**
- Modify: **state/handle.go**, **state/continuation.go**
- Modify: **engine/generate.go**, **engine/finalize.go**
- Test: **state/materialize_property_test.go**
- Test: **storage/postgres/checkpoint_integration_test.go**
- Test: **engine/fork_test.go**

- [ ] Model omitted/Set/Clear without pointers that collapse states. Collection
  Set replaces; Clear resets/removes.
- [ ] Write graph tests for root, linear history, N siblings, repeated identical
  deltas with distinct operation keys, depth/size limits, and cross-tenant
  handles.
- [ ] Fix the current continuation gap: materialization includes every ancestor
  delta/response or verified snapshot, not only current request plus output.
- [ ] Persist blobs first, then atomically publish child/result/operation.
  Existing checkpoints never update.
- [ ] Validate tool-call/result frontier across delta boundaries and never allow
  a child to begin inside an unmatched exchange.
- [ ] Add periodic self-contained snapshots and property-test that snapshot
  materialization equals full replay byte-for-byte.
- [ ] Return only new output plus child handle through Activity payloads.
- [ ] Run **go test -race ./state ./storage/postgres ./engine -run
  'Checkpoint|Materialize|Fork'**.
- [ ] Commit: **feat(state): add immutable forkable checkpoint graph**.

### Task 8: Implement route-isolated cache fingerprints

**Files:**

- Create: **cache/fingerprint.go**
- Create: **cache/canonical.go**
- Create: **cache/route_identity.go**
- Modify: **config/types.go**, **config/validate.go**, route catalog loaders
- Test: **cache/fingerprint_test.go**
- Test: **cache/fingerprint_property_test.go**
- Test: **cache/route_identity_test.go**

- [ ] Derive one HMAC route-cache identity from configuration digest, provider,
  endpoint/account, region, resolved model/revision, and compiler profile. A
  display model name never removes route isolation.
- [ ] Add negative fixtures proving OpenAI, Azure OpenAI, OpenRouter, regions,
  accounts, revisions, quantization uncertainty, hidden transforms, and
  compiler differences cannot share an entry in this phase.
- [ ] Define the fingerprint include/exclude matrix exactly as the architecture
  document. Provider/route/account/region/model revision enter through the route
  identity; service class remains non-semantic scheduling consent.
- [ ] Domain-separate Generate and Compact fingerprints. Compact uses the same
  opt-in maximum-age contract but accepts only variant zero.
- [ ] HMAC canonical semantic bytes; never store/log raw prompt hashes. Version
  canonicalizer, semantic profile, compiler, compaction prompt, and cache epoch.
- [ ] Persist a bounded canonical cache-request manifest JSONB plus digest for
  audit/match verification. Represent ancestor/large content with immutable
  digests/BlobRefs; never query JSONB on the cache hot path.
- [ ] Property-test map order, JSON spelling, operation key, cache age, route,
  and actor tags do not change the key; every output-affecting setting does.
- [ ] Do not add equivalence tables/configuration. Record cross-provider sharing
  as a future ADR requiring one concrete verifiable pair and a new cache epoch.
- [ ] Commit: **feat(cache): add route-isolated semantic keys**.

### Task 9: Implement exact-response cache and concurrent fill collapse

**Files:**

- Create: **cache/policy.go**
- Create: **cache/service.go**
- Create: **storage/postgres/cache.go**
- Modify: **engine/generate.go**, **engine/finalize.go**
- Test: **storage/postgres/cache_integration_test.go**
- Test: **engine/cache_test.go**
- Test: **integration/cache_concurrency_test.go**

- [ ] Validate cache omission/presence, bounded positive age, int32 variant, and
  effective temperature rules after inheritance.
- [ ] Keep the cache service operation-kind neutral and require a domain-
  separated result template; Task 11 wires Compact through the same service.
- [ ] Lookup route-isolated semantic keys using completion time for
  freshness. PostgreSQL failure on opt-in fails before a paid call.
- [ ] Implement fill owner/lease without a long transaction. Resolve expired
  owners through operation state before takeover; never assume dispatching safe.
- [ ] On origin completion insert entry/use with count one. On a hit atomically
  insert distinct use, saturate int32 count, update last-used, create a
  cache-replay child/result, complete operation with exact zero.
- [ ] Rehydrate current operation/checkpoint/provenance; never return origin
  operation identity. Drop only provider state certified fork-safe.
- [ ] Test old-but-recently-used retention, stale completion despite fresh last
  use, Activity retry count idempotency, max-int saturation without arithmetic
  overflow, tombstone-then-refill uniqueness, concurrent miss collapse, owner
  crash at every boundary, variant samples, and cross-route isolation.
- [ ] Capture cache lookup/fill/GC query plans at representative volume.
- [ ] Commit: **feat(cache): add durable exact response reuse**.

### Task 10: Add provider prompt-cache affinity without weakening routing

**Files:**

- Modify: **state/checkpoint.go**, **routing/types.go**, **routing/planner.go**
- Modify: provider usage lifting in **llm/provider/** adapters
- Create: **routing/affinity.go**
- Test: **routing/affinity_property_test.go**
- Test: provider affinity fixtures under **llm/provider/testdata/**

- [ ] Persist hard pin and ordered soft affinity with provider/endpoint/account/
  region/family/model lineage, HMAC cache key, epoch, usage observations, and
  expiry.
- [ ] Filter authorization/residency/class/capability/health/price/budget before
  moving exact soft-affinity route to the front of its eligible class.
- [ ] Derive stable provider prompt-cache keys from tenant scope, parent
  canonical digest, provider epoch, and model lineage; never raw content IDs.
- [ ] Add route properties proving affinity cannot authorize a route/class,
  bypass an open credit incident, or drop required opaque state.
- [ ] Prove three forks reuse the parent prefix/cache identity.
- [ ] Record cache-read/write tokens separately from worker cache disposition.
- [ ] Commit: **feat(routing): prefer safe provider cache affinity**.

### Task 11: Implement isolated explicit and automatic compaction

**Files:**

- Create: **compaction/policy.go**
- Create: **compaction/generic.go**
- Create: **compaction/trigger.go**
- Create: **compaction/prompt/v1.txt**
- Create: **engine/compact.go**
- Modify: **engine/generate.go**, provider capability catalogs
- Test: **compaction/generic_test.go**
- Test: **compaction/trigger_property_test.go**
- Test: **engine/compaction_recovery_test.go**
- Test: provider-native fixtures

- [ ] Add Compact Activity/engine path that creates an immutable child and
  never returns a normal answer.
- [ ] Strip application tools/tool policy and output format from the internal
  compaction call; force tool choice none and accept bounded plain text only.
- [ ] Preserve original settings unchanged on the checkpoint and restore them
  for the subsequent Generate. Test exact before/after equality.
- [ ] Version generic prompt, policy, summarizer route, retained recent turns,
  output reserve, hysteresis, and materialization thresholds.
- [ ] Never cut unmatched tool call/result, instructions, tools/schemas, open
  tasks, durable facts, citations, or recent-turn window.
- [ ] Run generic compaction as a deterministic durable child operation.
  Crash between compact/generate reuses the completed child/cost.
- [ ] Apply the same opt-in exact cache to explicit Compact. For automatic
  compaction, propagate the parent Generate maximum age but force compaction
  variant zero; omission propagates as omission. A hit creates a new
  compaction child at exact zero cost.
- [ ] Implement provider-native compaction only for a capability fixture proving
  tool/output isolation, artifact reuse/fork/expiry, and complete usage
  aggregation. Otherwise select generic.
- [ ] Pin beta headers and transport restrictions in capability data, not
  adapter conditionals. Initial fixtures must cover Anthropic's
  `compact-2026-01-12` header and Bedrock's `InvokeModel`-only boundary; a
  Bedrock `Converse` route must fail capability selection rather than silently
  omitting compaction.
- [ ] Exact cache lookup precedes automatic compaction; include policy/prompt/
  artifact versions in fingerprint.
- [ ] Commit: **feat(compaction): add isolated durable context reduction**.

### Task 12: Add resumable provider submit/poll adapters

**Files:**

- Modify: **llm/provider/adapter.go**, **llm/provider/error.go**
- Create: **llm/provider/resumable.go**
- Create: **engine/provider_pending.go**
- Modify: **engine/attempt.go**, **activity/heartbeat.go**
- Modify: supported provider adapters and capability profiles
- Test: **llm/provider/contracttest/resumable.go**
- Test: **engine/provider_pending_test.go**
- Test: **integration/temporal/provider_poll_recovery_test.go**

- [ ] Define optional Submit/Poll/RecoverByIdempotencyKey capability with typed
  pending/completed/failed status and safe next-poll guidance.
- [ ] Persist deterministic idempotency key and dispatching boundary before
  submit. Persist encrypted poll ID and **provider_pending** before first poll.
- [ ] Start every retry by reading operation state. Pending means Poll, never
  Submit.
- [ ] Handle crash after acceptance/before ID: recover only through documented
  idempotency lookup; otherwise ambiguous/non-retryable.
- [ ] Bound polling by Activity deadline, provider guidance, poll count, and
  cancellation. Heartbeats contain no poll ID.
- [ ] Crash the worker before submit, after submit before ID persistence, after
  ID commit, during poll, and after provider completion before result commit.
  Assert submission count at most one except the explicitly unrecoverable case,
  which remains ambiguous and still does not resubmit.
- [ ] Commit: **feat(provider): resume durable asynchronous operations**.

### Task 13: Persist provider health, credit incidents, and model inventory

Status: the provider-control normalization and persistence slices are
implemented: closed status and credit observations, sticky incident and
replay rules, bounded inventory snapshots, refresh collapse, and PostgreSQL
status/inventory projections have offline and opt-in database coverage. This
does not complete Task 13. No production provider profile currently
implements `provider.ModelLister`; management refresh therefore remains an
explicit deployment-owned adapter, and `configured_only` or `unsupported`
inventory is the honest state until one is supplied. Runtime status recording,
protected provider credentials, and durable composition that binds those
adapters to a live query Activity remain follow-up work. Keep the original
checklist below unchecked until those deployment and protected-evidence gates
are proven.

**Files:**

- Create: **control/status.go**
- Create: **control/credit.go**
- Create: **control/inventory.go**
- Create: **storage/postgres/provider_status.go**
- Create: **storage/postgres/inventory.go**
- Modify: **routing/health.go**, provider error classifiers
- Test: **control/status_test.go**
- Test: **storage/postgres/provider_control_integration_test.go**
- Test: provider management API fixtures

- [ ] Convert inference/startup/operator/management observations to a closed
  safe event. Update append-only event and current projection transactionally.
- [ ] Require documented provider code/field or operator evidence for exhausted
  credit/billing issue. Generic 429 remains rate/capacity.
- [ ] Define sticky-clear rules for credit/billing incidents and config epoch.
- [ ] Implement supported provider model listing with pagination; store bounded
  normalized snapshots and safe metadata. Unsupported is explicit.
- [ ] Keep configured logical routes authoritative; discovered models never
  become routable automatically.
- [ ] Add refresh collapse per endpoint and stale/current/complete provenance.
- [ ] Commit: **feat(control): persist provider status credit and inventory**.

### Task 14: Implement typed Query service and Temporal Activity

Status: the typed Activity boundary, five closed query models, scope/filter/
horizon-bound cursors, persisted PostgreSQL read adapters, Redis budget reader
seam, and query-execution audit adapter are implemented with offline and
opt-in PostgreSQL plan coverage. The boundary now also rejects unordered or
duplicate typed result keys before a continuation is signed or audit evidence
is committed. Provider refresh management, live Redis window materialization,
and deployment-owned authorization/key/audit wiring remain explicit
composition and protected-evidence work; they are intentionally fail-closed
and are not represented as complete by the checked-in tests.

**Files:**

- Create: **control/query.go**
- Create: **control/provider_status_query.go**
- Create: **control/model_inventory_query.go**
- Create: **control/credit_status_query.go**
- Create: **control/budget_status_query.go**
- Create: **control/spend_summary_query.go**
- Create: **activity/query.go**
- Modify: **activity/activities.go**, **activity/errors.go**
- Test: **control/query_test.go**
- Test: **activity/query_test.go**
- Test: **storage/postgres/query_plan_integration_test.go**

- [ ] Implement the five closed request/result pairs, common provenance, stable
  ordering, bounded page size, and HMAC cursor bound to scope/tag/filter/horizon.
- [ ] Default provider/model/credit/spend queries to persisted reads. Optional
  freshness may call only supported provider management/list APIs, never
  inference.
- [ ] Budget status reads exact current reservations/accounted charges/available
  conservative nano-USD reservations/accounted charges/available USD from
  Redis only and returns generation/manifest/Stream provenance. Reject
  an instant outside Redis coverage; never fall back to PostgreSQL budget
  tables. Spend summary uses completed operation/cost rows, a half-open UTC
  interval, allow-listed dimensions, known exact sum, unknown count, and
  partial/complete status; SQL never coalesces NULL cost to zero.
- [ ] Insert every completed query in the dedicated bounded
  **query_executions** audit ledger with its exact-or-unknown actual USD cost.
  Do not create an inference operation or blob for a Query. Use exact zero only
  for a confirmed-free stored/control call. Commit the audit row before the
  Activity completes; a PostgreSQL outage therefore causes a Temporal retry,
  even when **budget_status** obtained its answer entirely from Redis.
- [ ] Add authorization tests, tag/result mismatch, cursor tamper/cross-kind/
  expiry, unknown enum, pagination stability, timeout, and refresh collapse.
- [ ] Seed representative data and require intended indexes for the four
  PostgreSQL query shapes; prove budget status uses only Redis. `spend_summary`
  must not read PostgreSQL budget policy/window/bucket/reservation/journal rows.
- [ ] Commit: **feat(activity): add typed control plane queries**.

### Task 15: Wire Generate v1 and Compact Activities end to end

Status: snapshot-scoped composition and reload-aware readiness seams landed; the reusable legacy
`llm.Engine` is intentionally not adapted to the v1 Activity boundary. The
factory now accepts an optional per-snapshot `V1RuntimeBuilder`, and the
Activity registration resolves that source while holding the snapshot lease;
readiness re-evaluates the source after every reload;
the snapshot client set also copies a preparatory typed
`V1RuntimeCapabilities` bundle with private adapter-map ownership, while
leaving legacy admission/continuation/result stores out until Task 19's
durable ports exist. The bundle now carries only an optional write-only
PostgreSQL `durable.Journal` capability (nil in memory mode); it never
activates V1 composition or falls back to the legacy engine;
see [ADR 0010](../../decisions/0010-durable-v1-runtime-composition.md) and
[ADR 0012](../../decisions/0012-snapshot-scoped-v1-runtime-source.md). The
Storage-neutral Generate and Compact phase runners are recorded in [ADR
0011](../../decisions/0011-durable-generate-phase-runner.md) and [ADR
0013](../../decisions/0013-durable-compact-phase-runner.md). The concrete
durable implementation and protected integration evidence remain pending.

**Files:**

- Modify: **activity/activities.go**, **activity/types.go**
- Modify: **engine/dependencies.go**, **engine/generate.go**
- Modify: **internal/runtime/factory.go**, **internal/runtime/temporal.go**
- Test: **activity/v1_integration_test.go**
- Test: **integration/temporal/conversation_lifecycle_test.go**

- [x] Register exact three Activity names on the existing task queue. Worker
  construction rejects unsafe queues before creating a Temporal worker, then
  binds the registration through `RegisterForTaskQueue`; descriptor tests and
  the app-level startup guard cover the one-shot v1 boundary.
- [ ] Keep heartbeats/errors/payloads small and redacted. Set limits well below
  current Temporal limits and reverify official limits at implementation time.
- [ ] Order Generate as replay, materialize/validate, route-isolated
  cache lookup, compaction decision, route/affinity, atomic Redis budget
  reservation, PostgreSQL journal write, provider state machine, PostgreSQL
  checkpoint/cache/cost finalization, then Redis budget reconciliation.
- [ ] Make cache and query database failures fail closed before inference.
- [ ] Prove Activity input/output size is independent of ancestor transcript
  length for 1, 100, and 10,000-turn synthetic lineages.
- [ ] Test tool-call response followed by tool-result delta and structured-output
  restoration after compaction.
- [ ] Commit: **feat(activity): serve delta conversations and compaction**.

### Task 16: Enforce the USD-only catalog boundary and defer FX

Status: complete for the implementation boundary and checked-in offline
fixture coverage. The strict loader, exact USD persistence adapter,
unknown-component handling, atomic snapshot reload, and focused
unit/integration coverage are present in
`golang/internal/catalog`, `golang/internal/runtime`, and
`golang/storage/postgres`. Protected deployment/release evidence remains
tracked in the v1 catalog and is not implied by these offline tests.
In-process resolver reloads also recheck the compiled digest and retain the
last valid snapshot when a replacement fails validation.

**Files:**

- Create: **storage/postgres/pricing.go**
- Modify: **internal/runtime/catalog_snapshot.go**
- Modify: **config/types.go**, **config/validate.go**, config schema/examples
- Test: **pricing/catalog_currency_test.go**
- Test: **internal/runtime/catalog_reload_test.go**

- [x] Remove externally supplied currency/rate values and all downstream
  currency fields. Public Go/JSON/OCaml money values are exact decimal USD by
  contract, so they do not redundantly carry **currency = USD**.
- [x] Validate that every initially configured provider price source is USD.
  Reject a non-USD catalog entry rather than accepting a caller-supplied rate,
  silently treating it as USD, or adding an incomplete FX subsystem.
- [x] Persist only exact USD price entries. Do not add an FX adapter, rate table,
  refresh job, configuration surface, or foreign-currency operation field in
  this release.
- [x] Preserve unresolved catalog components as NULL with partial/unknown
  status. Do not make a route look free when source pricing is uncertain.
- [x] Make price/config reload atomic and preserve the last still-valid USD
  catalog snapshot if a replacement fails validation.
- [x] Add fixtures for valid USD, rejected non-USD input, missing/unknown price,
  and excessive precision.
- [x] Add source-outage, rotation, and audit-linkage fixtures/evidence in the
  checked-in catalog tests. Protected deployment/release evidence remains
  separate release work and is not inferred from these fixtures.
- [x] Record that the first concrete non-USD provider requires a superseding ADR
  defining worker-owned rate retrieval, exact conversion, staleness, and audit
  behavior. The worker will still persist and report only USD after that ADR.
- [x] Landed across the USD boundary, catalog persistence, and catalog evidence
  commits; the implementation remains USD-only until a superseding ADR.

### Task 17: Extend the OCaml protocol layer

Status: complete for the protocol layer and its checked-in offline evidence.
The separate natural-facade compatibility decision is tracked under Task 18;
protected release evidence is not inferred from these checks.

**Files:**

- Modify: **ocaml/llm_temporal_worker/lib/llm_temporal_identifier.ml/.mli**
- Modify: **ocaml/llm_temporal_worker/lib/llm_temporal_models.ml/.mli**
- Modify: **ocaml/llm_temporal_worker/lib/llm_temporal_codec.ml**
- Modify: **ocaml/llm_temporal_worker/lib/llm_temporal_invocation.ml/.mli**
- Modify: **ocaml/llm_temporal_worker/lib/dune**
- Test: **ocaml/llm_temporal_worker/test/test_wrapper.ml**

- [x] Retain landed PR 109 validation and its nominal ID/error conventions.
- [x] Implement exact **Usd_decimal.t**, exact/unknown settled-cost variants,
  checkpoint, cursor, and query-execution IDs,
  patch/cache/Generate/Compact records, and every Query wire variant/result.
- [x] Remove public currency/microUSD fields from v1 models. Prohibit float in
  the v1 money/temperature API; the pre-v1 compatibility record remains
  explicitly outside the v1 boundary.
- [x] Encode Keep by omission, Set/Clear distinctly, decimal as string, variant
  as int32, and all query tags closed.
- [x] Add three exact Activity descriptors and low-level invoke functions. The
  descriptors and one-attempt helpers are implemented in
  [`llm_temporal_invocation.ml`](../../../ocaml/llm_temporal_worker/lib/llm_temporal_invocation.ml),
  with compile-time signature and remote-descriptor coverage in
  [`test_activity.ml`](../../../ocaml/llm_temporal_worker/test/test_activity.ml).
- [x] Consume Go golden fixtures for the bounded Generate/Compact/Query
  positive/negative shapes and assert
  canonical round trips.
- [x] Run Dune build/test with the pinned Temporal SDK. The pull-request and
  master workflows pin the SDK commit before running `dune build` and
  `dune runtest` (see
  [master.yml](../../../.github/workflows/master.yml#L96-L108)); the same job
  installs the package through its Git subpath and compiles both downstream
  consumer fixtures.
- [x] Landed across the OCaml protocol, fixture, and dependency-update commits.

### Task 18: Add natural OCaml Conversation and Query GADT APIs

Status: complete for the immutable facade, typed query surface, and
compatibility boundary. The package-level `execute` and `workflow` helpers,
as well as the deprecated pre-v1 `Request`/`invoke_once` names, now dispatch
only the canonical Generate v1 envelope. The compatibility path rejects
legacy controls which have no v1 representation rather than dropping them.
Protected release evidence is not inferred from these offline checks.

**Files:**

- Create: **ocaml/llm_temporal_worker/lib/llm_temporal_conversation.ml/.mli**
- Create: **ocaml/llm_temporal_worker/lib/llm_temporal_query.ml/.mli**
- Modify: **ocaml/llm_temporal_worker/lib/llm_temporal.ml/.mli**
- Modify: **ocaml/llm_temporal_worker/README.md**
- Create: **ocaml/llm_temporal_worker/test/test_conversation.ml**
- Create: **ocaml/llm_temporal_worker/test/test_query.ml**
- Add downstream compile fixture

- [x] Implement immutable root/checkpoint/fork/respond/compact values. A
  success returns response plus child conversation; no mutable implicit head.
- [x] Add natural persistent builders for Settings.Patch and Cache_policy.
- [x] Implement the five-constructor GADT and safe tag/result matcher without
  **Obj.magic** or unchecked JSON.
- [x] Add validated `Query.Filter` builders for bounded pagination, refresh
  ages, cursor-kind association, spend intervals, and unique dimensions.
- [x] Keep cursor/result type associated across pagination.
- [x] Add the additive typed `Llm_temporal.Generate` facade for direct,
  non-streaming one-shot v1 calls while preserving legacy `Request` names.
- [x] Rebuild the remaining pre-v1 `Request`/`invoke_once` helpers on a
  Generate v1 root in the same package/facade, replacing their unreleased
  wire shape in place. The deprecated compatibility path validates and
  converts the old constructor shape before dispatching the canonical
  `llm.generate.v1` descriptor; unsupported legacy controls fail closed.
- [x] Test three siblings from one parent, no inherited fields on wire, decimal
  exactness, compaction tool/output isolation, query type inference/mismatch,
  and unchanged Temporal errors. The focused Conversation and Query suites are
  [`test_conversation.ml`](../../../ocaml/llm_temporal_worker/test/test_conversation.ml)
  and [`test_query.ml`](../../../ocaml/llm_temporal_worker/test/test_query.ml);
  the external workflow fixture also type-checks the immutable fan-out and
  all five query constructors ([`workflow_sample.ml`](../../../ocaml/consumer_workflow_smoke/workflow_sample.ml)).
- [x] Compile the exact end-to-end sample in the OCaml architecture document
  as the external `ocaml/consumer_workflow_smoke` package importing only
  **Llm_temporal**. The compile-only fixture type-checks all five typed Query
  constructors, a cache-enabled root call, three sibling forks from one
  immutable parent using variants 0/1/2, explicit compaction, and a
  post-compaction Generate that restores tools and structured output.
- [x] Type-check both the natural facade and the low-level Activity descriptor
  examples. The external `ocaml/consumer_workflow_smoke` package now compiles
  the exact v1 `Temporal.Activity.execute` calls for
  `generate_v1_activity`, `compact_v1_activity`, and `query_v1_activity` with
  the exported one-attempt retry policy, while also asserting the facade's
  `Temporal.Future.all` fan-out has no hidden mutable conversation head.
- [x] Commit: **feat(ocaml): expose immutable conversations and typed queries**
  (including the deprecated compatibility-helper migration).

### Task 19: Compose Redis budgets with PostgreSQL durable state

**Files:**

- Modify: **internal/runtime/factory.go**, dependency probes/reload
- Modify: **config/** and **api/schema/v1/config.schema.json**
- Modify: **compose.yaml**, **deploy/local/**, **deploy/kubernetes/**
- Modify: **Dockerfile**, CI workflows, **config.example.yaml**
- Update: all affected docs/reference/deployment files
- Test: runtime, Compose, Kubernetes, configuration policy tests

- [ ] Add worker PostgreSQL addresses/TLS/pool/database/schema/table-prefix/key
  references and the exact direct environment overrides defined in Task 3.
  Reject only namespace selections whose computed relations overlap
  Temporal-owned relations; do not reject a safely isolated shared server or
  shared database.
- [ ] Start only after PostgreSQL transaction/schema/index/blob/key checks and
  Redis PING/TIME/noeviction/persistence/Function/prefix/manifest checks pass.
  Pause new-work polling on loss of either dependency; completed operation
  replay may use a separately bounded path only if its dependencies are healthy.
- [ ] Keep provider availability out of process readiness; it remains routing/
  query state.
- [ ] Make Compose demonstrate the recommended separate Temporal and worker
  databases/roles. Add integration profiles for a shared database with a
  dedicated schema and an explicitly prefixed shared schema. Two worker
  replicas share the selected worker namespace.
- [ ] Keep Redis as a required production service in Compose/Kubernetes with
  AOF+RDB, `noeviction`, TLS/auth, monitored persistence, the configured key
  prefix/ACL, preprovisioned Function digest, and persistent storage. Two worker
  replicas share the same Redis budget generation and Stream.
- [ ] Update Kubernetes secrets/config, network policy, probes, resource
  examples, backup/restore runbook, and graceful shutdown.
- [ ] Switch composition in one commit so Redis owns only active budgets and
  operational throttles while PostgreSQL owns durable operation/journal/
  conversation/cache/control state. Remove only superseded Redis durable
  continuation/result/operation-replay representations; retain Redis Functions,
  configuration, deployment, client, readiness, and budget tests.
- [ ] Do not import Redis data or implement a migration, backfill, relation
  rename, dual-read/write interval, or fallback. This is a clean pre-release
  responsibility split. Redis/PostgreSQL writes are not compatibility
  dual-writes: they record different hot and durable facts.
- [ ] Exercise rolling worker restart (no PostgreSQL budget reads), full-fleet
  cold restart (one fenced PostgreSQL budget rebuild), persistent Redis restart
  (no lost-generation rebuild), non-persistent Redis restart (fenced rebuild),
  and independent PostgreSQL outage.
- [ ] Commit: **refactor(runtime): compose redis budgets with postgres state**.

### Task 20: Add retention, GC, outbox, and database operations

Status: partially complete. Bounded maintenance, retention fences, outbox
leases, checkpoint/cache safety, observability, and a conservative terminal
operation orphan pass are implemented. Full budget/operation retention,
authoritative unknown-cost resolution, and production autovacuum/fillfactor
load evidence remain open and must not be inferred from the implemented
maintenance primitives.

**Files:**

- Create: **maintenance/cache_gc.go**
- Create: **maintenance/retention.go**
- Create: **maintenance/outbox.go**
- Create: **storage/postgres/maintenance.go**
- Modify: metrics/telemetry and deployment operations docs
- Test: **maintenance/retention_integration_test.go**
- Test: **maintenance/outbox_integration_test.go**

- [x] Implement bounded **SKIP LOCKED** batches and separate maintenance role.
- [x] Cache eligibility uses last use; old-but-used-yesterday is retained.
- [x] Recheck child/fill/use/blob references inside deletion transaction.
- [x] Publish external deletion in outbox and make missing object success.
- [x] Add bounded status/inventory/query/checkpoint retention without deleting
  active retry/poll/audit state.
- [x] Add a bounded terminal-operation orphan pass that deletes only inline,
  unreferenced, non-unknown-cost rows while preserving active, audit,
  budget, cache, checkpoint, lineage, and blob-backed operations.
- [ ] Add budget/operation retention without deleting active retry/poll/audit
  state; the current adapter leaves those projections disabled pending their
  restrictive foreign-key, journal, and cold-rebuild obligations.
- [ ] Budget retention uses bounded indexed DELETE/UPDATE statements but never
  loads the active budget working set into a running worker. It must preserve
  every row required by the maximum window, open reservation, and allowed cold
  rebuild.
- [ ] Execute an authorized billing caller that resolves an unknown operation
  with **resolve_unknown_exact**, binds protected evidence, and demonstrates
  the production Redis retry path. The PostgreSQL transaction and the
  storage-neutral Redis handoff are implemented; the conservative bound must
  remain in force until reconciliation succeeds.
- [x] Define the storage-neutral unknown-cost handoff contract: PostgreSQL
  commits first, Redis reconciliation is fenced and retryable, and a Redis
  failure returns the authoritative PostgreSQL receipt without weakening the
  conservative bound.
- [x] Add metrics for eligible/deleted/skipped/failure, dead tuples, pool and
  maintenance/query latency, cache hit/use/fill, pending polls, and
  exact/unknown cost.
- [x] Instrument PostgreSQL lock latency at the provider-route transaction
  advisory-lock boundary. The observer records the bounded `lock` histogram
  from the Activity metrics context (or an explicit composition observer),
  without exposing route, tenant, or database identifiers.
- [ ] Load test autovacuum/fillfactor and record table-specific production
  settings instead of guessing.
- [ ] Commit: **feat(maintenance): add safe state and cache retention**.

### Task 21: Prove crash, fork, cache, query, and restore behavior

**Files:**

- Create: **integration/temporal/v1_recovery_test.go**
- Create: **integration/postgres/concurrency_test.go**
- Create: **integration/postgres/query_plan_test.go**
- Create: **integration/postgres/restore_test.go**
- Create: **integration/redis/budget_recovery_test.go**
- Create: **integration/redis/budget_stream_test.go**
- Modify: **docs/testing/strategy.md**, CI workflows, Makefile

- [ ] Run a 10,000-turn synthetic lineage with bounded Activity payloads,
  periodic snapshots, compaction, and three-way forks.
- [ ] Kill workers/database connections at every operation/cache/compaction/
  submit/poll/finalization commit boundary. Count provider submissions.
- [ ] Run 100-way identical cache miss, same-parent fork, same-key replay, and
  overlapping-budget admission under race/deadlock detection.
- [ ] Prove steady-state budget admission/finalization, a joining worker,
  rolling restart, Stream gap, and budget-status queries execute zero
  PostgreSQL budget SELECTs. Use a SQL statement classifier/allowlist in the
  integration fixture, not timing or log inference.
- [ ] Prove a zero-live-worker cold start elects one bootstrap reader and a
  Redis non-persistent loss performs one fenced rebuild; all other workers wait.
  Persistent Redis restart preserves generation, partial loss fails closed, and
  a racing journal writer is applied exactly once across the generation flip.
- [ ] Verify route-isolated cache reuse and prove that two provider routes never
  share an entry, even when their public model names match.
- [ ] Verify cache zero cost, count once, freshness versus last use, and 180-day
  unused cleanup.
- [ ] Verify all five Go/OCaml queries, pagination, authorization, fresh/stale/
  unsupported states, exact spend decimals, NULL unknown prices/costs, and
  partial spend summaries.
- [ ] Back up PostgreSQL plus blobs and Redis persistence, restore to an
  isolated environment, and replay a completion/resume a pending poll without
  resubmission. Separately prove PostgreSQL can rebuild a deliberately empty
  Redis budget generation during an allowed cold bootstrap.
- [ ] Run security tests for cross-tenant handles/HMACs, SQL injection, cursor
  tamper, encrypted provider IDs, log redaction, KMS rotation, and database role
  denial.
- [ ] Run query-plan tests at representative cardinality and remove only
  empirically unused indexes after updating this design through a superseding
  ADR.
- [ ] Commit: **test(v1): prove durable conversation and control plane recovery**.

## Phase exits

These gates are cumulative only within the phase being released. A later
phase's unimplemented gate does not block an earlier release.

### Every phase

- [ ] **make verify**
- [ ] **make compose-smoke**
- [ ] **make kustomize-verify**
- [ ] Focused race tests for packages changed in the phase
- [ ] Focused fuzz/property tests for canonical JSON, patches, decimals,
  checkpoints, cache fingerprints, cursors, and provider state
- [ ] Relevant database schema/index contract and representative EXPLAIN gates
- [ ] **git diff --exit-code** after generated fixtures/checks

### Phase A — durable conversation core

- [ ] **make postgres-integration** and Temporal recovery integration
- [ ] Encrypted inline/blob payload, one-submit/resumed-poll, immutable fork,
  exact-or-unknown cost, and delta-size-independence proofs
- [ ] OCaml Generate facade Dune build/test plus external package compile
- [ ] Physical-namespace matrix: dedicated database/schema, shared database,
  and prefixed shared schema, with exact object names and no truncation/collision
- [ ] Backup/restore and pending-provider-poll continuation proof

### Phase B — compaction and Redis materialization

- [ ] **make redis-integration** and compaction integration
- [ ] Redis prefix isolation, conservative nano-USD Function, optional
  coordination Stream,
  generation-loss detection, and cold-rebuild gates
- [ ] SQL instrumentation proves zero PostgreSQL budget reads outside the two
  explicitly allowed cold-recovery conditions
- [ ] Run the bounded sizing workload and separate queued-batch drain defined by
  the normative workload envelope; verify it remains a test assumption rather
  than a configuration field or runtime limit
- [ ] Runbook commands/metrics are deployment-specific and the persistent,
  new-incarnation, and same-incarnation recovery drills pass
- [ ] Compact disables tools/structured output and the next Generate restores
  both; provider prompt-cache affinity remains a preference, not eligibility

### Phase C — opt-in exact-response cache

- [ ] Named staging/incident-reproduction caller and baseline cost/reuse record
- [ ] Concurrent fill collapse, route isolation, saturated use count, tombstone
  refill, max-age/last-use behavior, and 180-day unused cleanup proof
- [ ] Cache hit records exact zero provider cost and no provider dispatch

### Phase D — typed control queries

- [ ] All five Go/OCaml query pairs, GADT tag matching, pagination, authorization,
  and external OCaml package compile
- [ ] Dedicated bounded query-ledger replay/audit tests with exact-or-unknown
  cost; Query never requires an inference operation or blob
- [ ] Budget status executes zero PostgreSQL budget SELECTs; spend summary unions
  operation and query-ledger costs without treating NULL as zero
- [ ] Provider management refresh collapse and content-free live fixtures

## Final acceptance traceability

| Requirement | Owning tasks | Required proof |
| --- | --- | --- |
| Delta-only Temporal history | 1, 7, 15 | payload size independent of ancestry |
| Immutable forks | 7, 18, 21 | three concurrent children from one parent |
| Sparse settings | 1, 7, 17 | omitted/Set/Clear cross-language fixtures |
| Exact response cache | 8, 9 | freshness, variants, one fill, count/last use |
| Route-isolated cache identity | 8, 9 | matching route hit and cross-route miss |
| Provider cache affinity | 10 | preference only after eligibility |
| Explicit/automatic compaction | 11, 15 | durable reuse and isolation/restoration |
| Restart-safe provider polling | 5, 12, 21 | one submit and persisted ID polling |
| Provider/model/credit queries | 13, 14, 18 | closed Go/OCaml tag/result pairs |
| Budget/spend queries | 6, 14, 18 | Redis current budget plus PostgreSQL operation-cost summary |
| All completed costs | 2, 5, 14 | valid exact-or-unknown state; zero only if known |
| Price precision and uncertainty | 2, 3, 14, 17 | sub-micro through $10+, NULL preserved end to end |
| USD-only pricing boundary | 2, 16 | no downstream currency; non-USD input rejected pending a future ADR |
| Redis budget optimization | 0, 6, 14, 19, 21 | conservative atomic windows, optional coordination Stream, loss detection, adoption, rebuild fencing |
| Near-zero PostgreSQL budget reads | 6, 14, 19, 21 | SQL classifier; cold fleet or verified non-persistent new Redis incarnation are the only exceptions |
| Production PostgreSQL | 3-6, 19-21 | durable journal/state, constraints/indexes, roles, restore |
| Unified OCaml package | 17, 18 | same facade/package and downstream compile |
| Configurable storage namespaces | 0, 3, 4, 19 | Redis prefix isolation and PostgreSQL namespace matrix |
| Pre-release in-place contract | 1, 2, 15, 17-19 | Generate v1 only; no compatibility or data movement |
