# Architecture Decision Records

Each decision's status line records where it stands, grouped here:

- **Current and implemented:** 0001, 0002, 0003, 0004 (its storage ownership
  refined by 0007), 0006, 0007, 0008, 0010, 0012 and 0016 (behind the optional
  `model_sync` configuration).
- **Implemented contract, different production path:** 0011 and 0013 define
  direct-phase Generate and Compact contracts; production runs the bounded
  cloud workflows instead.
- **Reference only:** 0014, a test and reference budget materializer.
- **Superseded:** 0005, by the Generate-only Temporal Activity boundary, and
  0015, whose bucket-pruning implementation was removed while its conservative
  retention principle still applies.
- **Deferred:** 0009, non-USD pricing; the v1 pricing contract is USD-only.

[Scope](../scope.md#staged-delivery-and-document-authority) owns phase and
document authority. A specification defect or measured evidence may change an
accepted design through a short superseding ADR amendment rather than silent
divergence or knowingly faithful implementation of a defect.

1. [Semantic IR and official SDKs](0001-semantic-ir-and-official-sdks.md)
2. [Public service classes](0002-service-classes.md)
3. [Durable ledger and retry boundary](0003-durable-ledger-and-retry-boundary.md)
4. [Redis shared state](0004-redis-shared-state.md)
5. [Streaming boundary](0005-streaming-boundary.md)
6. [Forkable conversation checkpoints](0006-forkable-conversation-checkpoints.md)
7. [Cloud durable state and Redis budget authority](0007-cloud-storage-and-redis.md)
8. [Resumable provider operations and typed queries](0008-resumable-provider-operations-and-typed-queries.md)
9. [Future non-USD pricing normalization](0009-future-non-usd-pricing.md)
10. [Durable v1 runtime composition](0010-durable-v1-runtime-composition.md)
11. [Durable Generate phase runner](0011-durable-generate-phase-runner.md)
12. [Snapshot-scoped v1 runtime source](0012-snapshot-scoped-v1-runtime-source.md)
13. [Durable Compact phase runner](0013-durable-compact-phase-runner.md)
14. [Reference budget materializer](0014-reference-budget-materializer.md)
15. [Bounded budget bucket retention fence](0015-budget-bucket-retention-fence.md)
16. [OpenRouter model sync](0016-openrouter-model-sync.md)
