# PostgreSQL operation persistence

Task 5 adds the durable one-shot operation repository. The repository is
implemented and integration-tested, but it is not yet wired into the
production Temporal factory. `storage/postgres.OperationRepository`
implements the existing `admission.AdmissionStore` lifecycle while storing the
normalized request manifest, request-fingerprint HMAC, result digest,
exact-or-unknown USD cost, and every route attempt in the worker schema.

The current production composition does not yet bind this repository to the
engine's operation and continuation ports; that path still uses the Redis
operation and continuation stores. PostgreSQL readiness and its other durable
repositories are separate capabilities. Generate, Compact, and Query
Activities therefore retain the explicit fail-closed boundary described in the
[v1 Activity runtime reference](activity-runtime.md): they do not dispatch a
provider until snapshot-scoped PostgreSQL checkpoint/cache/control
dependencies and a concrete `activity.V1Runtime` have been composed.

## Replay and transitions

`Begin` derives a keyed operation key from the caller's id and scope. A replay
with the same request fingerprint returns the existing operation; a different
fingerprint returns `admission.ErrOperationConflict`. Dispatch, continuation,
completion, failure, and provider-pending updates are compare-and-set
transitions performed inside a read-committed transaction. Dispatch tokens are
deterministic HMACs of the operation UUID, so a worker restart can safely
reconstruct the token without persisting a bearer secret.

Replay results are hydrated from the durable row after the fingerprint check,
including the request digest, lease, expiry, and reserved-cost metadata. This
keeps an idempotent `Begin` replay equivalent to a restart recovery read.

Concurrent `Begin` calls use a conflict-safe insert covering both the
idempotency key and the deterministic operation UUID. Exactly one caller
creates the row; the others reread it and follow the normal replay or conflict
path instead of surfacing a database duplicate-key error. Reusing an operation
ID with a different scope or operation kind is classified as an operation
conflict even though its idempotency key differs.

Each retry derives its attempt number from the durable attempt rows while the
operation row is locked. Provider-accepted or ambiguous failures persist an
unknown cost with a null exact amount; only rejected/not-dispatched failures
use the exact zero-cost cache method.

The engine's `tenant\x00operation-key` scope format is split only for keyed
scope lookup; the original scope key is retained in an authenticated encrypted
column. Reads hydrate the scope, lease/operation expiry, request digest, and
result reference metadata needed by the result store after a worker restart.

Provider identifiers and request/result payloads are never logged or embedded
in SQL text. The operation row stores HMACs and opaque encrypted markers; the
blob/result repository remains responsible for payload bytes.

## Cost contract

Terminal operations require either an exact `pricing.USD` amount and an
allowed method, or an unknown amount with a safe reason code. Decimal values
are bound as canonical `NUMERIC(38,18)` text. Explicit cache replays use the
`worker_cache_zero` method and the exact zero amount.

The repository is intentionally bounded to one-shot Temporal activities. It
does not add a live streaming path.

## Validation

The PostgreSQL integration tests are opt-in through the existing
`LLMTW_POSTGRES_ADDR` test configuration. Unit tests cover manifest shape,
stable HMAC/UUID derivation, safe reason normalization, and exact money
encoding. `OperationRepository.Attempts` and `AttemptRepository.List` expose
all persisted attempts for release conformance and operational inspection.
