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

The worker is one part of the ATT forecasting system. Before treating a worker
release as evidence that forecasting is operational, consult the dated
[cross-repository working-status matrix](https://github.com/victoria-hft/victoria-hft/blob/master/docs/research/ai_ach/forecast-competition-status.md).
