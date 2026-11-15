# Key compromise and runtime credential rotation

Status: **design, partly implemented**. Tracking issue:
[#1559](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/1559).
Statements about current behaviour describe the code at the time of writing
and name where it lives. The production warning for `kind: env` credentials
and the reload test are implemented. Every section headed or marked
**Proposal** is not implemented.

The [durable-storage key management design](durable-storage-key-management.md)
(#861) covers the cloud request storage keyring and nothing else. Its
[out-of-scope list](durable-storage-key-management.md#out-of-scope) leaves out
the Redis `key_secret`, continuation handle keys, provider API credentials and
Temporal payload codec keys. This document covers those, plus the other
runtime credentials an operator rotates: the Redis ACL password, the Langfuse
key pair and Temporal API key or mTLS material. It does not change #861. Where
both documents touch the same secret (`state.requests.secret`), #861 decides.

The step-by-step procedures are in the
[rotating runtime credentials runbook](../runbooks/rotating-runtime-credentials.md).
The [threat model](threat-model.md) names the actors that could obtain each
key and lists this design's remaining gaps among its
[residual risks](threat-model.md#residual-risks). Rolling restarts and drains
follow the [fleet upgrade and rollback runbook](../runbooks/fleet-upgrade-and-rollback.md).

## How secrets reach the worker today

Configuration holds references, never values (`config.SecretRef`: `env`,
`file` or `workload_identity`; provider endpoints use `auth.kind`
`bearer_env` or `header_env`). The worker binary rejects `workload_identity`.

Every configuration reload, whether from `SIGHUP` or a change to the
`--config` file, builds a complete replacement snapshot. `app.App.Reload`
(`internal/app/app.go`) compiles the file, resolves every reference again
(`secrets.ConfigResolver`), builds new clients through the production factory
(`internal/runtime/factory.go`), verifies their dependencies and then
publishes. It has no "configuration unchanged" short-circuit, so a reload of
byte-identical configuration still re-reads every `file` secret and rebuilds
every snapshot client. `TestReloadOfUnchangedConfigRebuildsClientsFromRotatedSecretFiles`
pins this.

Two limits follow:

- **Environment variables cannot change.** A process cannot see a new value
  for its own environment, so an `env` reference keeps its startup value until
  the process restarts.
- **The file watcher watches only the configuration file.** Replacing a
  secret file does not trigger a reload. Send `SIGHUP`, change the
  configuration file's content, or restart.

Which credentials a reload picks up depends on where their client lives:

| Credential | Reference | Client lifetime | Rotation without restart |
| --- | --- | --- | --- |
| Provider API key | `endpoints.*.auth` `bearer_env`/`header_env` | Snapshot | No: environment only |
| Redis ACL password | `state.redis.password` | Snapshot | Yes, with `kind: file` |
| Redis TLS CA | `state.redis.tls.ca_file` | Snapshot | Yes |
| Langfuse key pair | `langfuse.public_key`, `langfuse.secret_key` | Snapshot | Yes, with `kind: file` |
| Continuation handle keys | `continuation.handle_keys[*].secret` | Snapshot | Yes, with `kind: file` (staged, below) |
| Temporal API key | `temporal.api_key_file` | Process | No |
| Temporal mTLS | `temporal.tls.cert_file`, `key_file`, `ca_file` | Process | No |
| Payload codec keys | `temporal.payload_codec.keys[*].secret` | Process | No |
| AWS and Azure workload credentials | default credential chain | SDK-managed | Refreshed by the SDK |
| Redis namespace identity | `state.redis.key_secret` | Never rotated in place | No (namespace migration) |
| Cloud request storage identity | `state.requests.secret` | Never rotated in place | No (#861) |

The process-lifetime rows come from `validateRuntimeReplacement`
(`internal/runtime/reload_validation.go`), which rejects a reload that changes
those fields. The Temporal client, including its TLS configuration, static
API-key credentials (`client.NewAPIKeyStaticCredentials`) and payload codec,
is built once in `runtime.New` (`internal/runtime/temporal.go`). Changing the
bytes behind an unchanged process-lifetime path has no effect until restart.

### Production warning for `kind: env`

`config.Config.RotatableEnvSecretFields` names every snapshot-lifetime
credential configured as `kind: env` in a production environment (any
`environment` other than `development`): `state.redis.password` (unless
`state.kind: memory` or `state.redis.service_mesh`), `langfuse.public_key`,
`langfuse.secret_key` and each `continuation.handle_keys[i].secret`.

- `validate-config` prints one `warning:` line per field on standard error
  and still exits `0`.
- The worker logs one `WARN` record per field at startup and after every
  published reload, with the field in `config_field`.

Both name the schema path only, never the variable name or value. It is a
warning, not an error, because an `env` reference works; it only makes
rotation a restart. Not listed: provider auth (it has no file form, so the
warning could not be acted on), the payload codec keys (process lifetime
whatever their kind), and the two identity secrets (never rotated in place).

## Redis namespace identity (`state.redis.key_secret`)

### What it protects

`state.redis.key_secret` is the root of two keyed functions in
`storage/redis`:

- **Key names.** `keySpace.digest` (`storage/redis/key.go`) HMACs tenant
  scopes, operation IDs and policy identifiers into Redis key names, so a key
  scan does not reveal tenant names.
- **Responses API result copies.** When `server.responses_api_listen_address`
  is set, terminal Generate results are copied to Redis sealed with
  AES-256-GCM under a key derived from it (`ResponseResultStore`,
  `storage/redis/response_result.go`). Copies expire after an hour and results
  over 1 MiB are not copied.

It is not a Redis credential: it does not authenticate to Redis.

**Compromise exposure.** Someone with the key *and* read access to Redis can
test guesses (a tenant name, an operation key) against key names, linking keys
to tenants. Where the Responses API listener runs, they can also decrypt the
result copies present while they have access: model outputs from roughly the
last hour. Without Redis access the key reveals nothing, and it cannot forge
budget authority (the [Redis writer](threat-model.md#redis-writer) actor
covers what Redis write access alone allows). First rotate the Redis ACL
password (which is what grants access) and audit Redis access. Replace the
identity as well when the listener is in use and Redis read access cannot be
ruled out, or when linkability of existing key names matters.

### Why it cannot be rotated in place

- Every key name changes with the secret, so a worker with a new secret sees
  an empty namespace: no spend, no reservations, no throttle state.
- `budget-initialize` binds the cloud receipt to the namespace *and* a
  fingerprint of the secret (`BudgetKeySpace.InitializationIdentity`,
  `storage/redis/budget_initialization.go`). A production durable worker
  reads that receipt on every snapshot build (`ProductionEngineFactory.Build`).
  A changed secret under the same `key_prefix` and `admission_hash_tag` fails
  that check, so startup fails and a reload is rejected with
  `ErrBudgetInitializationRequired`. This guard is deliberate: it stops a
  worker from silently spending against an empty namespace.
- The reference itself is process-lifetime, and Temporal workflows in flight
  hold budget reservations that exist only under the old key names.

### Decision: fresh namespace and full drain

Rotating `key_secret` means moving to a **new Redis namespace** with a full
drain. There is no in-place rotation and no dual-key overlap. The procedure is
in the runbook; in outline:

1. Provision a new random secret of at least 32 bytes, and choose a new
   `state.redis.key_prefix` (or `admission_hash_tag`) so the new identity gets
   its own receipt and marker.
2. With the new configuration, run `budget-initialize --apply` to create the
   new namespace's receipt and marker. The old namespace is untouched.
3. Drain: stop starting new workflows on the task queue and wait until no
   workflow is open, so no reservation, claim or settlement spans the cutover
   ([drain procedure](../runbooks/fleet-upgrade-and-rollback.md#4-drain-when-the-change-requires-it)).
4. Stop every old worker, then start workers with the new configuration.
5. Keep the old namespace read-only until its longest budget window has
   passed, then delete its keys by prefix.

**Accounting consequence.** The new namespace starts with no spend. Budget
windows open after the cutover do not count spend before it, so for one
window length a tenant can spend up to its limit again. Lower the affected
limits for that period if that is not acceptable. Cloud request records do not
move: they are keyed by `state.requests.namespace`, not by the Redis
namespace, so completed operations still replay.

**Proposal: carry accounting into the new namespace.** `budget-initialize
--apply` already rebuilds reservations, claims and settlements from durable
cloud execution records when a ready receipt has lost its Redis marker
([recovery](../reference/redis-budget-leases.md#recovery-after-redis-data-loss)).
A migration mode could run the same rebuild into a freshly initialized
namespace before cutover, removing the window reset and the need for a full
drain. It is not implemented, and the recovery path must not be forced (for
example by deleting a marker) to imitate it.

## Continuation handle keys (`continuation.handle_keys`)

### What they protect

`state.Keyring` (`state/handle.go`) signs two tokens with HMAC-SHA-256 under
the primary key and verifies them under any listed key:

- **Checkpoint handles** (`ctn_v1.<key-id>.<id>.<mac>`), returned to callers.
  The MAC binds the checkpoint's random UUID to the caller's durable scope.
- **Reference capabilities** (`ref_v1.<key-id>.<mac>`), internal. Preparation
  issues one per request; the next step of the same workflow presents it to
  load that request (`CloudRequestPreparation.Load`). They travel in Temporal
  history and are never public output.

**Compromise exposure.** A party that can already start workflows or
Activities on the worker's task queue (the
[Temporal namespace caller](threat-model.md#temporal-namespace-caller)) could
mint handles and capabilities for
any checkpoint UUID or request ID it learns, within the scopes
`authorization.allowed_scopes` lets it select, and so read stored transcripts
it was never given. The key decrypts nothing and gives nothing to a party
without Temporal access. Temporal namespace access control remains the outer
boundary.

### Current behaviour

- The keyring is rebuilt for every snapshot (`continuationKeyring`), so the key
  list is reloadable; only the bytes behind an existing ID must never change.
- An unknown key ID, including a removed one, fails exactly like a forged or
  foreign handle (`state.ErrInvalidHandle`, deliberately indistinguishable).
  Caller-visible results:
  - a checkpoint handle signed by a removed key: `invalid_argument`,
    non-retryable (`internal/runtime/checkpoint_replay.go`);
  - a reference capability signed by a removed key, held by an open workflow:
    `state_unavailable`, retryable (`cloudRuntimeError(contracts.ErrNotFound)`).
    The request step retries under its 24-hour schedule-to-close bound and
    succeeds if the key is restored in time; otherwise the workflow fails;
  - the Langfuse export child workflow presents the same capability, so its
    export fails after its bounded retries.
- Changing the bytes behind an existing ID has the same effect as removing it:
  every outstanding token under that ID stops verifying.
- In `state.kind: memory`, a change to the key material rebuilds the
  process-local continuation store, dropping its continuations.

### Revocation procedure

Rotation and revocation are the same staged edit; only the speed differs.

1. **Add** the new key `K2` as non-primary on every worker. Confirm every
   worker loaded it (a successful reload log after the change, or a restart).
   A worker without `K2` would reject tokens other workers sign with it.
2. **Promote** `K2` and demote `K1` on every worker. New handles and
   capabilities use `K2`; `K1` still verifies.
3. **Retire** `K1` by removing it. Before that, wait for both:
   - `state.continuation_retention` since step 2 finished: every
     `K1`-signed checkpoint handle points at a checkpoint created before
     promotion, and those checkpoints have expired;
   - the longest workflow still open from before step 2 to close, so no
     open workflow holds a `K1` capability.

**Under compromise**, do steps 1 and 2 back to back, then choose:

- *Retire `K1` immediately.* Forged tokens stop working at once. Callers'
  existing `K1` handles fail as `invalid_argument` and those conversations
  restart from a new root. Open workflows past preparation retry
  `state_unavailable` and then fail, so drain the task queue first if possible.
- *Keep `K1` until it ages out (step 3).* Nothing breaks, but forged tokens
  keep working until then.

**Re-issue path.** Every successful Generate or Compact returns a new
checkpoint handle signed with the current primary, so active conversations
move to `K2` on their next turn. There is no API to re-sign an idle handle.

**Proposal: handle re-issue.** An authorized query that verifies a handle
under any listed key and returns a handle for the same checkpoint under the
primary would let callers migrate idle conversations before `K1` is retired.
It is not implemented.

## Temporal payload codec keys (`temporal.payload_codec`)

### What they protect

`temporalcodec` encrypts every payload the worker's Temporal client writes
(workflow and Activity inputs and results, heartbeat details, memos) with
AES-256-GCM under a key derived from the configured secret. The key ID is
stored in plaintext metadata and authenticated. See
[Temporal payload encryption](../reference/configuration.md#temporal-payload-encryption).

**Compromise exposure.** Anyone with the key and read access to Temporal
history (the namespace, its archival, exported histories or backups) can
decrypt every payload written under that key: prompts and outputs below
`server.inline_payload_bytes`. With write access they could also forge
payloads that decode. Temporal history is immutable, so data already written
under a compromised key stays readable with that key for its retention period.
No rotation re-encrypts it. Without the codec, the same history is plaintext
to every namespace reader (a residual risk in the
[threat model](threat-model.md#residual-risks)).

### Current behaviour

- The codec is built once with the Temporal client. A reload that changes
  `temporal.payload_codec` is rejected, so every codec change is a restart.
- `Decode` names the missing key ID and fails for a payload whose key is not
  configured (`temporalcodec/codec.go`). A worker without the key cannot
  replay a workflow that used it, and Temporal retries the failed workflow
  task without progress until the key is configured again.
- The repository ships no codec server. Any codec server operators run for the
  Temporal UI or CLI must hold the same key set.

### Revocation procedure

1. **Add** `K2` as non-primary to every worker, every caller's codec and every
   codec server, and restart the workers. Callers encrypt workflow inputs
   themselves, so they must be able to read `K2` results before any worker
   writes them.
2. **Promote** `K2` on every worker and caller, and restart. New payloads stop
   using `K1`.
3. **Retire** `K1` only when no open workflow used it and every closed
   workflow that used it has passed the namespace retention period. Until then
   remove it neither from workers (open workflows would stop replaying) nor
   from the codec server (closed histories would show opaque bytes).

**Under compromise**, the only way to make `K1` data unreadable sooner is to
remove the data: terminate or finish the affected workflows and delete their
histories or let retention expire them, including archival and exports.
Retiring `K1` from the worker does not protect history an attacker already
holds.

## Cloud request storage identity (`state.requests.secret`)

Today this single 32-byte secret derives both the payload encryption key and
every identifier (`storage/cloudstate`). It is resolved on every snapshot
build, and its reference is process-lifetime. The keyring that separates
encryption from identity is #861's design and is not implemented yet.

**Compromise exposure.** With read access to the payload bucket (the
[DynamoDB or S3 writer](threat-model.md#dynamodb-or-s3-writer) actor), the secret
decrypts every stored blob: prompts, outputs and checkpoints. It also lets an
attacker compute identifiers and forge checkpoint blob references. Response:
restrict and audit bucket access first. Replacing the secret is a namespace
replacement (a new `state.requests.namespace` and secret, with the old
namespace read-only until retired), which #861 treats as a deliberate
migration. Nothing guards against the bytes changing behind an unchanged
reference today; #861 adds a namespace canary for that.

## Runtime credentials

These authenticate the worker to a dependency and protect no stored data.
Compromise exposure is whatever the credential grants at the provider. Each
dependency accepts two credentials during the overlap, so every rotation is:
create the new credential, deliver it to every worker, confirm, revoke the old
one.

- **Provider API keys** are `bearer_env`/`header_env` only. Rotation is a
  rolling restart.
- **Redis ACL password, Langfuse key pair, Redis TLS CA**: replace the file
  and reload. The Redis procedure keeps the ACL user and adds a second
  password to it, which is why `state.redis.username` is not in the warning
  list.
- **Temporal API key and mTLS**: process lifetime. Rotation is a rolling
  restart.

The runbook gives the commands and the verification for each.

**Proposal: file-based provider auth.** `bearer_file` and `header_file` auth
kinds that read the key from an absolute path, resolved per snapshot like
`state.redis.password`, would make provider key rotation a reload. With a
file form available, `RotatableEnvSecretFields` could then also warn about
`bearer_env` and `header_env` in production.

**Proposal: Temporal credentials callback.** Replace
`client.NewAPIKeyStaticCredentials` with `client.NewAPIKeyDynamicCredentials`
over a cached reader of `temporal.api_key_file` (re-read at most every 30
seconds and on `SIGHUP`, keeping the last valid key if a read fails), and load
the mTLS client certificate through `tls.Config.GetClientCertificate` the same
way. The paths stay process-lifetime; their contents become rotatable without
a restart. Neither is implemented.

## Verification today

There is no per-key fingerprint metric for these secrets. Rotation is
verified by outcome:

- a reload's `configuration reloaded` log record and
  `llmtw_config_reload_total{outcome="success"}` on each worker, after the
  secret file changed;
- readiness staying ready: a reload whose new credential fails its dependency
  check is rejected with `cause="dependency"` and the old snapshot stays
  active;
- after revoking the old credential, no rise in
  `llmtw_provider_attempt_total{outcome="failure"}`,
  `llmtw_langfuse_export_failures_total{reason="rejected"}` or Redis readiness
  failures.

**Proposal: key fingerprint status.** Report, per worker, the key IDs and a
non-reversible fingerprint (as #861 specifies for the storage keyring) for
handle and codec keys, so a staged rotation can gate each step on positive
evidence from every worker instead of on reload logs.
