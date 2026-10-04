# ADR 0007: Cloud durable state and Redis budget authority

- Status: Accepted; replaces the earlier storage design

## Decision

Use the generic `cloud-storage` table, blob, and event-sourcing interfaces for
requests, attempts, pending discovery, checkpoints and cache artifacts. The
initial implementation is DynamoDB and S3 with IAM authentication. Conditional
writes enforce identity and publication; immutable encrypted blobs hold payloads.
There is no existing service dataset to import.

Redis owns budget reservations, single-use claims and idempotent settlement.
Budget policy is compiled from worker settings. Provider status, inventory and
operational throttles also use Redis. Audit observations go to structured logs.
Temporal owns its own persistence independently of worker application state.

A new paid attempt must be recorded durably and claim its Redis authorization
before provider dispatch. Unused authorizations have a 15-minute start deadline.
A claimed or uncertain paid attempt must not be refunded just because time
passed. Retrying an uncertain submission requires a new attempt and reservation;
the original remains accounted and discoverable.

Cache identity includes the request index, which defaults to zero and is not
sent as a provider seed. Successful completion determines cache age. An enabled
cache without a maximum age has no age restriction. A newer failure does not
invalidate an older eligible success. Incomplete responses are not normal cache
successes; valid application tool calls are successful model responses.

Money remains exact decimal USD in Go, JSON and OCaml, with at most 20 integer
and 18 fractional digits. Redis uses conservative nano-USD: positive charges
round up and limits round down. Unknown actual cost remains explicitly unknown.

## Consequences

The cloud store and Redis do not share a transaction. Durable execution phases,
conditional writes and idempotent settlement make retries recoverable without
silently buying another provider call. Losing Redis accounting cannot be
repaired by replaying cloud requests or coordination events. Initialization
receipts fence an absent authority marker; complete data-loss recovery remains
separate work.

See [state and storage](../architecture/state-and-storage.md),
[cloud requests](../reference/cloud-request-repository.md),
[Redis budget leases](../reference/redis-budget-leases.md), and the
[recovery boundary](../runbooks/redis-budget-generation-recovery.md).
