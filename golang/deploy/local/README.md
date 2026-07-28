# Local Compose fixture

The default Compose profile starts pinned Postgres-backed Temporal, Redis, and
a deterministic OpenAI-compatible provider fixture. No provider credential is
used. Postgres and Temporal share the development-only
`${LLMTW_POSTGRES_PASSWORD:-local-only}` fixture variable; override it for any
non-local use. Redis disables its default user and gives the worker and
provisioner the development-only `${LLMTW_REDIS_USERNAME:-local}` /
`${LLMTW_REDIS_PASSWORD:-local-only}` ACL; override both for any non-local use.
The Redis command explicitly enables AOF and RDB snapshots and sets
`maxmemory-policy noeviction`; its healthcheck reads those settings before the
worker profile can start. This is a local contract check, not evidence of a
production backup or restore exercise.
The worker profile is deliberately opt-in because it needs a continuation
HMAC key. The supported lifecycle gate creates that key with restrictive
permissions, uses a development-only durable file-blob volume, and removes its
own containers, volumes, images, temporary Go cache, and key when it finishes:

```sh
LLMTW_COMPOSE_LIVE=1 make compose-live-integration
```

The live gate waits up to five minutes for the Compose healthchecks. This is a
bounded startup allowance for the pinned Temporal, Redis, and worker healthcheck
budgets; it does not weaken any readiness or recovery assertion.

The target is intentionally distinct from `make compose-smoke`: the latter
only validates the Compose model and never starts Docker services. The live
gate starts a uniquely named local project, checks the worker's exact
`/health/live` and `/health/ready` endpoints through its distroless binary,
then stops and restores Redis to verify the same liveness/readiness recovery
contract. It also runs a real Temporal SDK recovery test against that Temporal
and Redis pair.

The fixture is for development only. It uses a local-only bearer value and a
zero-cost catalog; production deployments must use external secret delivery,
real catalog digests, and the release image digest. The worker exposes the
same probes as Kubernetes: `/health/live`, `/health/ready`, and `/metrics`.

For a storage-composition check, the opt-in `durable` profile adds a separate
digest-pinned `worker-postgres` service with its own named volume, database,
and role. `durable-worker` validates `durable-config.yaml` and waits for both
that database and Redis (Redis remains the active budget/throttle dependency):

```sh
docker compose --profile durable run --rm durable-worker
```

This profile deliberately does not install the worker schema or start Temporal
polling. Schema installation is an explicit deployment operation, and the
existing `worker` profile remains the offline Redis-only parser/readiness
fixture.

The checked-in worker profile remains a
parser/configuration/readiness fixture, not an end-to-end provider invocation
path. Provider egress is deliberately fail-closed in every environment, so
provider egress is not available to the private Docker-network address resolved
for `provider-mock`. The recovery gate injects a content-free adapter in the Go
test process instead of requesting that address; it records an accepted write,
stops one Temporal worker, starts a replacement, and requires conservative
ambiguity with no second provider dispatch or shared-budget reservation. To
make real provider requests from a local worker, use a private local
configuration with a reviewed public HTTPS provider hostname and its matching
`outbound_hosts` entry; do not weaken the policy for a Docker-private
destination.
