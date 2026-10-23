# Redis durable budget materializer

The v1 durable phase runners consume `storage/durable.BudgetMaterializer`.
`golang/storage/redis.RedisBudgetMaterializer` is the first production storage
implementation of that port.

It executes two actions through the existing immutable admission Function/Lua
deployment:

- `durable_reserve` atomically checks every requested window, writes a
  generation- and incarnation-fenced operation record, and increments
  nano-USD bucket totals.
- `durable_reconcile` applies journal completion events atomically, records
  event fingerprints for idempotent retries, and updates the bucket expiry
  index.

The materializer uses a separate `durable-budget` key family. This is
intentional: legacy admission records use micro-USD values and must never be
interpreted as the exact v1 materialization. Limits are rounded down and
charges/reservation releases are rounded up at the Redis boundary according
to `pricing.NanoUSDMaterializationVersion`.

Each instance is bound to one immutable generation/incarnation. A snapshot
swap, generation mismatch, event fingerprint conflict, missing operation, or
uncertain Redis mutation fails closed. The production runtime factory
(`golang/internal/runtime/factory.go`) builds this adapter as the durable v1
path's budget authority.

`conformance.RunDurableRollingBudget` (`golang/storage/conformance/durable_budget.go`)
is the suite this adapter and `durable.ReferenceBudgetMaterializer` share, run
against live Redis in both Function and Lua modes. It covers reservation,
accounting, multi-bucket batches, concurrent admission across buckets and
across overlapping windows, policy and window isolation, reservation and claim
expiry, unknown-cost finalization, and duplicate or replayed settlement. The
legacy `admission/v1` suite in `conformance.go` covers only the legacy ledger.

## Verification

Unit tests use the FunctionInvoker seam for denial, timeout-after-mutation,
duplicate reconciliation, and snapshot-fence cases:

```sh
cd golang
go test ./storage/redis -run RedisBudgetMaterializer
```

The live Redis conformance path remains protected by the existing isolated
Redis integration setup:

```sh
make redis-integration
```

The conformance denial case requests more than the remaining window capacity;
requests exactly equal to the remaining limit are valid and must be accepted.

That command is required before enabling this port in a production runtime.
