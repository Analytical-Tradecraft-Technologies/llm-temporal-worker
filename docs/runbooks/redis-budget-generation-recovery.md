# Redis budget authority and recovery boundary

Redis is the authoritative budget store for admission. Cloud request records
durably hold every paid attempt's reservation, claim evidence and settlement,
and are the source for rebuilding lost Redis accounting. The Redis Stream is a
coordination feed and cannot be replayed as permission to spend. Losing
accounting data must not turn paid work into fresh capacity.

## Supported initialization and checks

The worker's `budget-initialize` command defaults to a read-only check.
`--apply` is only for the first initialization of an unused namespace, with all
writers stopped. It conditionally creates a permanent cloud receipt, writes a
matching persistent Redis marker and initial stream event, then marks the cloud
receipt ready. See [CLI](../reference/cli.md#budget-initialize) and
[Redis budget leases](../reference/redis-budget-leases.md#initialization-and-readiness)
for the exact contract.

Only the initializer that receives an acknowledged creation of the cloud receipt
can send the first Redis marker creation. A retry may finish initialization only
while the matching marker survives. A receipt with an absent marker is not a
fresh installation, even if Redis appears empty.

Production startup checks the receipt. Readiness and every reserve, claim and
settlement check the matching persistent marker. Missing, preparing,
recovering, expiring, wrongly typed or mismatched markers keep paid work
unavailable. Readiness never repairs or initializes accounting state; only
`budget-initialize --apply` recovers it.

## Data loss or failover

When Redis loses its data, every worker's `budget-initialize --apply` init step
detects the ready receipt without a matching marker and recovers automatically.
Paid work stays unavailable until recovery commits; there is nothing to delete
or reset. Follow the JSON lines on the init container's standard error:

- `Redis budget authority is fenced for recovery` — recovery started.
- `budget recovery is running in another process` — this pod is waiting for
  the elected recoverer; it continues when the marker is ready.
- `budget recovery merged a shard` — progress, with scanned and restored counts.
  Shards run concurrently and finish in any order.
- `budget recovery is still in progress` — this run's time ran out or too
  little was left to start; the next init container start resumes from the
  saved cursor. Restarts are expected for a large history.
- `budget recovery completed` — the namespace is ready again.

After a Redis failover or snapshot restore whose marker survived, run once:

```sh
llm-temporal-worker budget-initialize --config /etc/llmtw/config.yaml --apply --recover --timeout 5m
```

If the timeout expires, rerun the same command; it resumes from its cursor.

Failure reasons and actions:

- `durable_record_invalid` or `durable_record_unavailable`: restore cloud
  request storage access or investigate the corrupt record, then rerun. The
  fence stays in place, which keeps spending stopped.
- `conflict`: a surviving Redis operation disagrees with its durable record
  (different generation, incarnation or request fingerprint). Preserve both and
  investigate; never delete either to reopen spending.
- `authority_unavailable`: the marker is preparing or belongs to another
  receipt. Investigate the namespace and receipt; recovery does not replace a
  marker it does not own.
- `timeout`: rerun; progress is kept.

See [recovery after Redis data loss](../reference/redis-budget-leases.md#recovery-after-redis-data-loss)
for what is restored, the conservative choices, and the recovery boundary.

## Uncertain first initialization

Do not delete a receipt, reset a marker, change the namespace, or rerun first
initialization to reopen spending. An ambiguous first receipt or marker write
(before the receipt is ready) needs investigation if the matching marker cannot
be read; recovery only applies to a ready receipt.

Capture content-free evidence: worker/configuration versions, incident times,
readiness reason codes, Redis persistence/restore history and the
initialization stage. Keep credentials, encryption material, prompts, outputs
and provider IDs out of logs.

## Remaining implementation

Broader budget work is tracked in [#814](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/814).
Generation manifests, stream tailers and worker-lease contracts remain available
for future coordination. Publishing a manifest alone does not prove a complete
working set. Tailers use Redis-only reloads after stream gaps; their cursors and
hints never authorize paid work.
