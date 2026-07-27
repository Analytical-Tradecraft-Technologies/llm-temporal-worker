# Pricing and Budgets Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement exact pricing, conservative multi-route estimation,
overlapping sliding-window policies, a durable idempotent operation ledger, and
conformant memory/Redis shared state.

**Architecture:** Pure pricing/budget packages compute immutable quotes,
matches, estimates, and bucket operations. `AdmissionStore` is the single atomic
boundary joining operation deduplication with every matching budget mutation.
Memory and Redis execute one shared black-box conformance suite.

**Tech Stack:** Go 1.26 `math/big` and checked integers, go-redis v9, Redis
Functions/Lua with Redis `TIME`, Testcontainers Redis integration, injected
clocks, Go race/property/fuzz tests.

**Global Constraints:**

- Monetary decisions use integer microUSD; no float enters pricing/admission.
- Admission reserves the maximum authorized candidate estimate before dispatch.
- Every matching policy/window applies atomically.
- A post-write ambiguous call retains the full reservation.
- Redis failures fail closed; retrying a mutation requires read-after-error.
- All Redis numeric operands and sums remain below `2^53`.

---

### Task 1: Implement exact decimal and microUSD arithmetic

**Files:**

- Create: `pricing/money.go`
- Create: `pricing/decimal.go`
- Create: `pricing/arithmetic.go`
- Test: `pricing/decimal_test.go`
- Test: `pricing/arithmetic_test.go`
- Test: `pricing/arithmetic_fuzz_test.go`

- [ ] Write table tests for zero, fractional microUSD, exact integer microUSD,
  many decimal places, per-million multiplication, component ceil, addition,
  negative/NaN/exponent rejection, `int64` overflow, and Redis `2^53` bound.
- [ ] Use a `math/big.Rat` oracle in tests and assert:

```go
got, err := pricing.CeilMicroUSD(decimalUSD, units, unitsPerPrice)
// got must be the least integer microUSD greater than or equal to exact cost.
```

- [ ] Run `go test ./pricing`. Expected: FAIL because package is absent.
- [ ] Implement strict ASCII decimal parsing into signless numerator and scale;
  configuration prices must be nonnegative and use a bounded scale. Multiply
  with checked big integers, divide, and ceil once at the documented component
  boundary before narrowing to `int64`.
- [ ] Implement checked `MicroUSD.Add/Sub`; subtraction cannot produce a
  negative bucket.
- [ ] Fuzz decimal strings and operands against the rational oracle for 60
  seconds. Expected: PASS without panic or disagreement.
- [ ] Commit: `feat(pricing): add exact microUSD arithmetic`.

### Task 2: Compile and resolve immutable price catalogs

**Files:**

- Create: `pricing/catalog.go`
- Create: `pricing/entry.go`
- Create: `pricing/resolver.go`
- Create: `pricing/cost.go`
- Test: `pricing/catalog_test.go`
- Test: `pricing/resolver_test.go`
- Create: `pricing/testdata/catalog/valid.yaml`
- Create: `pricing/testdata/catalog/invalid/*.yaml`

- [ ] Write tests for exact endpoint/family/region/model/provider-tier/effective
  lookup, endpoint override precedence, overlapping/expired entries, unknown
  price, currency mismatch, provenance/version digest, and immutable reload.
- [ ] Run `go test ./pricing -run 'TestCatalog|TestResolve'`. Expected: FAIL.
- [ ] Implement strict catalog compile and deterministic resolver. Provider
  actual cost is not in the catalog; `CostFromUsage` pins the candidate's exact
  entry/version.
- [ ] Implement usage components for input, output, cache read/write, reasoning,
  per-request, and declared media charges. Missing required usage yields
  conservative reservation or unknown status, never zero by assumption.
- [ ] Add response cost method values
  `provider_reported | catalog_usage | reconstructed_usage | retained_reservation`.
- [ ] Run `go test -race ./pricing`. Expected: PASS.
- [ ] Commit: `feat(pricing): resolve versioned endpoint price catalogs`.

### Task 3: Estimate the worst authorized request cost

**Files:**

- Create: `budget/estimate.go`
- Create: `budget/token_estimator.go`
- Create: `budget/media_estimator.go`
- Test: `budget/estimate_test.go`
- Test: `budget/token_estimator_test.go`

- [ ] Write tests for exact provider tokenizer hooks, conservative UTF-8 byte
  fallback, structural/tool/schema overhead, images/documents, cache-write
  assumption, maximum output/reasoning, fixed charge, candidate price
  differences, and overflow.
- [ ] Add a plan with standard plus priority fallback and assert the reservation
  equals the larger eligible estimate, independent of route attempt order.
- [ ] Run `go test ./budget -run TestEstimate`. Expected: FAIL.
- [ ] Implement `Estimator.EstimateCandidate` returning component facts and
  `EstimatePlan` returning the maximum with the candidate ID that determined it.
  The first canonical candidate wins equal-cost ties, including an all-free
  plan, so the reservation always retains an auditable candidate identity.
  The safety ratio is an exact decimal/rational, not float.
- [ ] Reject candidates whose maximum context/output or media size cannot fit.
  Do not estimate from average completion length.
- [ ] Run `go test -race ./budget`. Expected: PASS.
- [ ] Commit: `feat(budget): conservatively estimate authorized plan cost`.

### Task 4: Match policies and compute conservative sliding windows

**Files:**

- Create: `budget/policy.go`
- Create: `budget/match.go`
- Create: `budget/window.go`
- Create: `budget/retry_after.go`
- Test: `budget/match_test.go`
- Test: `budget/window_test.go`
- Test: `budget/window_property_test.go`

- [ ] Write matcher tests for tenant/project/actor/environment/logical model/
  endpoint/service class, wildcard rules, missing context, all-match behavior,
  and required-policy failure.
- [ ] Write boundary tests around `t-W` and bucket edges using:

```go
first := floorDiv(nowNanos-windowNanos, bucketNanos)
last := floorDiv(nowNanos, bucketNanos)
```

Assert the full intersecting first bucket is included and the active sum never
undercounts an exact timestamp event.
- [ ] Write retry-after tests with multiple denying windows; aggregate time must
  be at least every individual safe admission time.
- [ ] Run `go test ./budget -run 'TestMatch|TestWindow|TestRetryAfter'`.
  Expected: FAIL.
- [ ] Implement compiled immutable matchers, bounded bucket counts, negative-time
  floor division, active index range, current bucket, and conservative expiry.
- [ ] Add property tests comparing bucket totals against an exact event-list
  oracle across random times/windows; bucket total must be greater than or equal
  to exact total.
- [ ] Run `go test -race ./budget`. Expected: PASS.
- [ ] Commit: `feat(budget): compile overlapping conservative windows`.

### Task 5: Define admission state and implement the memory store

**Files:**

- Create: `admission/types.go`
- Create: `admission/store.go`
- Create: `admission/state.go`
- Create: `admission/transition.go`
- Create: `admission/contract/suite.go`
- Create: `storage/memory/admission.go`
- Test: `admission/state_test.go`
- Test: `storage/memory/admission_test.go`

- [ ] Encode every legal/illegal transition and write failing tests for Begin,
  replay, digest conflict, dispatch token/lease, Complete, definite Fail,
  Ambiguous, Canceled, reclaim of proven pre-write expiry, and terminal replay.
- [ ] Build a store conformance suite parameterized by factory and injected
  clock. Include all matching windows, exact limit, concurrent barrier calls,
  route-window union reservation, charged/uncharged Continue, original-bucket
  refund, underestimation excess, ambiguity, overflow, clock rollback, and
  expiry.
- [ ] Run `go test ./admission/... ./storage/memory -run Admission`. Expected:
  FAIL.
- [ ] Implement `AdmissionStore` and memory transaction under one mutex. `Begin`
  takes precompiled matched window operations so storage does not implement
  policy matching. `Continue` atomically finalizes a definite prior attempt and
  reserves the remaining plan before returning its next attempt token.
- [ ] Operation lookup precedes budget mutation. Same digest returns existing
  state; different digest returns conflict. `Complete` writes exact incurred
  cost even when it breaches a limit and returns a policy-violation fact.
- [ ] Deep-copy all values and use the injected clock; wall-clock rollback
  returns `state_unavailable` until caught up.
- [ ] Run `go test -race -count=100 ./admission/... ./storage/memory`.
  Expected: PASS and no accepted sum exceeds a limit.
- [ ] Commit: `feat(admission): add ledger and conformant memory admission`.

### Task 6: Implement atomic Redis admission Functions

**Files:**

- Create: `storage/redis/client.go`
- Create: `storage/redis/key.go`
- Create: `storage/redis/codec.go`
- Create: `storage/redis/admission.go`
- Create: `storage/redis/function.go`
- Create: `storage/redis/functions/admission.lua`
- Create: `storage/redis/functions/embed.go`
- Test: `storage/redis/key_test.go`
- Test: `storage/redis/codec_test.go`
- Test: `storage/redis/function_test.go`
- Test: `storage/redis/admission_integration_test.go`

- [ ] Add pinned go-redis and Testcontainers Redis modules after confirming their
  current official paths; record versions in the dependency baseline.
- [ ] Write tests asserting every Begin key hashes to the configured literal
  admission tag, operation keys use HMAC digest, numeric strings parse within
  limits, and function source digest/version matches readiness metadata.
- [ ] Run the memory conformance suite against a Redis factory. Expected: FAIL.
- [ ] Implement versioned Redis Function entry points `begin`,
  `mark_dispatching`, `continue`, `complete`, and `fail`. Use `redis.call('TIME')`, validate
  argument count/length/range, compare state/token/lease, check all windows
  before any increment, and return a typed array/status.
- [ ] Embed source by `go:embed`. Load with Function Load Replace only during
  controlled startup, then verify library/source digest. Provide Lua `EVALSHA`
  fallback only when configuration explicitly permits the tested Redis version.
- [ ] Implement client calls with no blind retry after transport error. Read the
  operation record and resolve whether the Function committed.
- [ ] Run `go test -race ./storage/redis -run Integration` with real Redis.
  Expected: PASS for every memory conformance case.
- [ ] Commit: `feat(redis): atomically enforce admission and ledger`.

### Task 7: Implement Redis continuation and result references

**Files:**

- Create: `storage/redis/continuation.go`
- Create: `storage/redis/result.go`
- Create: `storage/redis/expiry.go`
- Test: `storage/redis/continuation_integration_test.go`
- Test: `storage/redis/result_integration_test.go`

- [ ] Run the shared state continuation suite against Redis. Expected: FAIL.
- [ ] Implement create-if-absent immutable continuation records, tenant-hashed
  keys, versioned codecs, TTL, digest/MAC verification, child-by-operation
  idempotency, and bounded reads.
- [ ] Implement immutable result objects in Redis only below configured size;
  larger results require the BlobStore port. Write result first, then pass its
  reference to atomic `Complete`.
- [ ] Simulate lost response after SET/Function calls and prove read resolves
  committed state without duplicate records or charges.
- [ ] Run `go test -race ./state/contract ./storage/redis`. Expected: PASS.
- [ ] Commit: `feat(redis): persist continuation and result references`.

### Task 8: Prove concurrency, crash, and persistence behavior

**Files:**

- Create: `integration/redis/admission_test.go`
- Create: `integration/redis/network_fault_test.go`
- Create: `integration/redis/persistence_test.go`
- Create: `integration/redis/compose.yaml`
- Modify: `Makefile`
- Modify: `docs/architecture/deployment-and-operations.md` only if measured
  operational limits refine existing guidance

- [ ] Start a pinned Redis container with `noeviction`, AOF, and RDB. Assert
  configuration through `CONFIG GET`/`INFO persistence` before tests.
- [ ] Race hundreds of operations at every limit boundary across multiple store
  clients. Expected before any test-specific fix: new tests expose missing
  assumptions or pass; never relax the invariant.
- [ ] Inject connection close immediately before request, during Function
  response, and after server commit. Assert definite pre-send or read-resolved
  result; unresolved post-send remains safe/ambiguous.
- [ ] Restart Redis after acknowledged reservations under the selected AOF mode.
  Assert behavior matches documented durability. Run a backup/restore fixture
  and verify operation/bucket consistency.
- [ ] Add `make redis-integration` and include it in master/integration gates,
  with a clear Docker-unavailable skip only outside CI.
- [ ] Run `make redis-integration` three times. Expected: PASS without leaked
  containers or data.
- [ ] Commit: `test(redis): verify concurrent and crash-safe admission`.

## Phase exit

- [ ] Run `go test -race -count=100 ./pricing ./budget ./admission/... ./storage/...`.
- [ ] Run pricing/window fuzz targets for 60 seconds each.
- [ ] Run `make redis-integration` against the pinned image.
- [ ] Inspect Redis Function source for dynamic source/key interpolation,
  floating-point money, unbounded loops, and cross-slot keys. Expected: none.
- [ ] Run `make verify`. Expected: PASS with no rewritten files.
