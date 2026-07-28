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
uncertain Redis mutation fails closed. The adapter is not selected by the
production runtime factory yet; full Generate/Compact composition still needs
durable checkpoint, journal, result, provider, and operation ports.

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
