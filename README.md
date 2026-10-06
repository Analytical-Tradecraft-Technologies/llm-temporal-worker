# LLM Temporal Worker

This repository contains the contract, implementation foundation, and staged
plans for a reusable Go inference library and Temporal Activity Worker. The
implemented foundation covers the semantic request/response API, strict
configuration snapshots, official-SDK provider adapters, deterministic
one-shot-response normalization, deterministic routing, tenant-bound
continuation handles, exact pricing, and in-memory admission. The process
runtime composition now includes
reloadable snapshots, probe servers, graceful worker shutdown, TLS-safe
Temporal client wiring, verified catalog loading, provider adapters, Redis
state, and blob-backed result replay. `EngineFactory` remains an injectable
seam for tests and custom deployments; the CLI uses the production composition
by default and fails closed when its configured dependencies are unsupported.

Start with the [documentation index](docs/index.md), then follow the
[master implementation sequence](docs/superpowers/plans/2026-07-13-master-sequence.md).

For Redis behind an externally enforced mTLS service mesh, set
`state.redis.service_mesh: true`, `state.redis.tls.enabled: false`, and omit
`state.redis.username` and `state.redis.password`. This explicit mode delegates
transport encryption and client authentication to the mesh; the application
uses plain Redis connections without AUTH. Deployments must enforce strict mesh
mTLS and restrict callers by workload identity. Without this opt-in, production
continues to require application TLS and credential references. The stable
`state.redis.key_secret` remains required and is unrelated to Redis authentication.
