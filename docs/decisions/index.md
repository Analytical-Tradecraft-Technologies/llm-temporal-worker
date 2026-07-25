# Architecture Decision Records

The first five decisions describe current implementation. Decisions 0006
through 0008 define staged target designs; acceptance does not claim that any
phase has shipped. [Scope](../scope.md#staged-delivery-and-document-authority)
owns phase and document authority. A specification defect or measured evidence
may change an accepted design through a short superseding ADR amendment rather
than silent divergence or knowingly faithful implementation of a defect.

1. [Semantic IR and official SDKs](0001-semantic-ir-and-official-sdks.md)
2. [Public service classes](0002-service-classes.md)
3. [Durable ledger and retry boundary](0003-durable-ledger-and-retry-boundary.md)
4. [Redis shared state](0004-redis-shared-state.md)
5. [Streaming boundary](0005-streaming-boundary.md)
6. [Forkable conversation checkpoints](0006-forkable-conversation-checkpoints.md)
7. [PostgreSQL durable state, Redis budgets, and exact-response cache](0007-postgresql-authoritative-state-and-response-cache.md)
8. [Resumable provider operations and typed queries](0008-resumable-provider-operations-and-typed-queries.md)
9. [Future non-USD pricing normalization](0009-future-non-usd-pricing.md)
10. [Durable v1 runtime composition](0010-durable-v1-runtime-composition.md)
11. [Durable Generate phase runner](0011-durable-generate-phase-runner.md)
12. [Snapshot-scoped v1 runtime source](0012-snapshot-scoped-v1-runtime-source.md)
13. [Durable Compact phase runner](0013-durable-compact-phase-runner.md)
