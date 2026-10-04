# Redis budget authority and recovery boundary

Redis is the authoritative budget store. Cloud requests retain execution and
provider recovery context, but they are not a complete accounting ledger. The
Redis Stream is a coordination feed and cannot be replayed as permission to
spend. Losing accounting data must not turn paid work into fresh capacity.

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
settlement check the matching persistent marker. Missing, preparing, expiring,
wrongly typed or mismatched markers keep paid work unavailable. Readiness never
repairs or initializes accounting state.

## Data loss or uncertain initialization

Keep paid work stopped when accounting integrity is uncertain. Preserve the
receipt, marker and affected Redis data for investigation. Do not delete a
receipt, reset a marker, change the namespace, or rerun first initialization to
reopen spending. Do not guess missing balances from provider result records.

The receipt detects loss of the authority marker. It does not prove every
budget key survived, detect an older snapshot restored together with its marker,
or provide a complete recovery journal. Partial key loss and rollback require
separate reconciliation before spending can resume. An ambiguous first receipt
or marker write also needs investigation if the matching marker cannot be read.

Capture content-free evidence: worker/configuration versions, incident times,
readiness reason codes, Redis persistence/restore history and the initialization
stage. Keep credentials, encryption material, prompts, outputs and provider IDs
out of logs. Recovery must account for claimed paid work that could have happened
after a restored snapshot before establishing any new budget authority.

## Remaining implementation

Full data-loss recovery is tracked in [#856](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/856)
and the broader budget work in [#814](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/814).
There is no automatic rebuild coordinator or supported data-loss repair command.

Generation manifests, stream tailers and worker-lease contracts remain available
for future coordination. Publishing a manifest alone does not prove a complete
working set. Tailers use Redis-only reloads after stream gaps; their cursors and
hints never authorize paid work. Automatic fleet consumer lifecycle and bounded
stream retention require their own integration and evidence.
