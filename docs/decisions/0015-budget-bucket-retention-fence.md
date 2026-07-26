# ADR 0015: Bounded budget bucket retention fence

- Status: Accepted implementation slice
- Date: 2026-07-26
- Complements: ADR 0007, ADR 0010, and Task 20 of the forkable conversation plan

## Context

Task 20 deliberately leaves operation and budget retention disabled until
their foreign-key, journal, and cold-rebuild obligations are handled. Empty
historical budget buckets are a smaller safe slice: they carry only a
projection, while journal history remains the rebuild authority.

## Decision

`MaintenanceRepository.PruneExpiredBudgetBuckets` performs a bounded,
indexed PostgreSQL pass. The caller supplies a cutoff already constrained by
the maximum configured window horizon. The query locks candidates with
`FOR UPDATE SKIP LOCKED` and deletes only buckets whose reserved and accounted
projections are exactly zero and which have no operation reservation row.
Journal events are never deleted, and any reservation row—including finalized
or released history—remains a fence until a future operation-retention design
can account for its audit and foreign-key obligations.

The method does not read Redis, infer the window horizon, or load the active
budget working set into a worker. It is therefore safe to run as a bounded
maintenance pass, but it does not claim that operation or journal retention is
complete.

## Evidence

- `golang/storage/postgres/maintenance.go` implements the indexed, locked
  deletion contract and reports bounded maintenance results.
- `golang/storage/postgres/budget_bucket_retention_integration_test.go` proves
  a remaining reservation row fences an empty bucket and that the bucket is
  deleted only after the fence is removed.
