# Implementer report: fail-closed memory SLO evidence

## Scope

This change tightens the offline release-evidence collector for the in-memory
admission-and-compilation benchmark. It rejects a retained benchmark summary
when the sampled p99 is at or above the documented 25 ms memory target and
records an explicit `target_status: "pass"` when the target is met.

The artifact remains `objective_status: "measurement_only"`: the same-region
Redis measurement is a separately authorized operator run, so this change does
not update the v1 catalog or claim the complete admission objective.

## Requirements covered

- Release collection fails closed when the offline memory proxy misses its
  strict target.
- The artifact schema requires the target status and preserves the existing
  redacted measurement contract.
- Boundary behavior at exactly 25 ms is covered by an architecture test.
- The runbook and testing strategy distinguish the passing memory half from
  the unmeasured same-region Redis half.

## Validation

- `go test ./internal/architecturetest -run 'TestReleaseEvidence|TestSLOEvidence|TestV1Traceability' -count=1`
- `make fmt-check schema-verify docs-verify slo-evidence-verify`
- `go test ./...`
- `git diff --check`

All commands passed in the isolated worktree. No provider credentials, live
provider calls, publication controls, or release-catalog evidence were changed.
