# Durable-storage key management

Status: **proposed design**. Nothing in this document is implemented yet.
Tracking issue: [#861](https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/issues/861).
It extends the cloud request and checkpoint persistence described in the
[cloud request repository](../reference/cloud-request-repository.md) (#859, #860).

This document picks a design for managed delivery and rotation of the secret
that protects durable cloud storage. It also records the operational
tradeoffs. Code changes follow in the phases listed at the end.

## Three separate protections

Operators must keep three mechanisms apart. This design changes only the
second.

| Layer | What it protects | Who owns the key | Changed here |
| --- | --- | --- | --- |
| Managed secret retrieval | Delivery of key material to the worker process (AWS Secrets Manager, file or environment) | Secrets Manager, with its own KMS key | Adds a source |
| Application encryption | Confidentiality and integrity of every payload blob (AES-256-GCM in `storage/cloudstate`). Also the keyed identifiers (HMAC-SHA-256) | The worker's storage keyring | Yes |
| Cloud storage encryption at rest | S3/DynamoDB media and backups (SSE-S3, SSE-KMS, DynamoDB table encryption) | AWS, as configured in infrastructure | No |

At-rest encryption does not hide payloads from anyone who can read the bucket
or table. Application encryption does. Secrets Manager only delivers keys: it
does not decide which key encrypted a given blob.

## How the secret is used today

`state.requests.secret` is a `config.SecretRef` (`env` or `file`;
`workload_identity` is rejected because it is not stable). It must resolve to
standard base64 of exactly 32 bytes. `internal/runtime/cloud_requests.go`
resolves and decodes it once per process and passes it to `cloudstate.Open`.
Changing the reference is a process-lifetime change, so
`internal/runtime/reload_validation.go` rejects it on reload.

Inside `storage/cloudstate` the single 32-byte secret is the root of every
keyed function, through `derive(secret, domain, data) =
HMAC-SHA-256(secret, "llmtw/cloudstate/v1/" + domain + "\x00" + data)`:

| Use | Domain(s) | Where it ends up |
| --- | --- | --- |
| AES-256-GCM encryption key | `encryption` | Every payload blob |
| Content-addressed blob key | `payload/<stream>` | S3 object key `<ns>/payload/<hex>`, KV pointers, checkpoint blob IDs |
| Scope tag, request binding, fingerprint | `scope`, `request-binding`, `request-fingerprint` | KV rows; deduplication and conflict detection |
| Operation ID, attempt ID | `operation-id`, `request-attempt-id` | UUIDv8 request IDs, so deduplication and pending-request recovery depend on them |
| Checkpoint identities and streams | `checkpoint/<kind>`, `checkpoint-blob-scope` | Checkpoint KV rows |
| Checkpoint blob reference proof | `checkpoint-blob-reference` | `llmtw_cpb_<tag>.<proof>` IDs carried in checkpoints and Temporal payloads |
| Append token, readiness probe key | `append-token`, `readiness` | KV rows, probe |
| Response-cache fingerprint key | `response-cache-key` | Cache keys |

The blob ciphertext is `nonce(12) || GCM(ciphertext || tag)`, with the blob key
as additional authenticated data. Blobs are immutable: `writeBlob` uses
`Create` and, on `ErrAlreadyExists`, reads and compares the existing object.
`readBlob` also recomputes the keyed blob key from the plaintext. The format
carries no version or key identifier.

So replacing the secret today breaks three things:

1. Existing blobs no longer decrypt.
2. Every identifier changes. Deduplication misses, operations restart under new
   IDs, pending-request recovery cannot find its rows, and checkpoint blob
   references fail their proof.
3. Content addressing splits: the same content gets a new blob key.

Rotation therefore has to change only the encryption key and leave every
identifier as it is.

Continuation handles use a separate `state.Keyring`
(`continuation.handle_keys`). It holds key IDs matching
`^[A-Za-z0-9_-]{1,32}$` and one primary. New handles are signed with the
primary, and any listed key verifies. This design reuses that *pattern*. It
does not share keys or lifecycle with the handle keyring (#861 scope).

## Decision

**Fetch a versioned storage keyring directly from AWS Secrets Manager, and
split the material into a never-rotated identity key and a rotatable set of
encryption keys.** Do not adopt per-object KMS envelope encryption now.

### Option A: a versioned keyring from Secrets Manager (selected)

The worker reads a JSON keyring document from Secrets Manager at startup, holds
it in memory, and refreshes it on a bounded interval. Each blob records which
key ID encrypted it. Readers accept any key still in the keyring. Writers use
the primary.

- Simple at runtime: one `GetSecretValue` per refresh interval, and no network
  call on the blob read/write path. Read latency and availability do not
  change.
- Builds on the existing `SecretRef` resolver and the key-ID/primary model
  that the continuation keyring already uses.
- Testable with a fake client and in-memory stores. All rotation logic is
  local and deterministic.
- Works the same in every failover region. Replicate the secret to the
  fallback regions; no multi-Region KMS keys are needed on the data path.
- Tradeoffs:
  - Key bytes live in worker memory, as they do today.
  - Rotation needs a disciplined, staged procedure (below).
  - Revocation is by retirement, not instant crypto-shredding.
  - Audit covers secret retrieval (CloudTrail `GetSecretValue`), not
    individual decryptions.

### Option B: KMS envelope encryption (not selected)

Each blob, or each batch of blobs, gets a data key that AWS KMS generates and
wraps (`GenerateDataKey`/`Decrypt`). The wrapped key is stored with the blob.

- Strengths:
  - The key-encryption key never leaves KMS.
  - KMS rotates backing keys automatically and keeps old ones.
  - Every unwrap is audited.
  - Disabling the KMS key shreds all data at once.
- Costs and risks:
  - **A KMS call on the read path.** Each unwrap is a network call, which
    adds latency, a per-account/region request quota, and a new availability
    dependency for every replay, checkpoint materialisation and cache hit.
    Caching unwrapped data keys avoids that, but it brings key material back
    into memory and adds cache expiry and invalidation logic.
  - **It does not solve identity.** The identifiers above are HMACs computed
    on hot paths, often several per request. KMS `GenerateMac` per identifier
    is not viable, so a delivered symmetric identity key is still required,
    and Option A's resolver has to be built anyway.
  - **Regional failover.** Data replicated to fallback buckets (#1230) needs a
    multi-Region KMS key or per-region rewrap. A KMS outage in a region
    becomes a storage outage there.
  - **A larger format and harder tests.** The wrapped key adds roughly 200
    bytes per blob, and tests need KMS emulation.

### Why A

Option A gives safe rotation with fewer moving parts, no new data-path
dependency, and no behaviour change for reads and writes beyond a header. The
envelope below is versioned, so a later format version can carry a
KMS-wrapped data key if a deployment needs per-object KMS audit or
crypto-shredding. That change would not touch identity, because identity uses
its own key.

To get KMS audit and control over the delivery step, encrypt the keyring
secret with a customer-managed KMS key and grant decryption only through
Secrets Manager (see [IAM](#least-privilege-iam)).

## Key model

| Key | Purpose | Lifetime |
| --- | --- | --- |
| **Identity key** (32 bytes) | Every `derive(...)` domain except `encryption`: blob keys, request/operation/attempt IDs, scope tags, bindings, fingerprints, checkpoint identities and reference proofs, cache fingerprint key | **Never rotated** for a namespace. For existing deployments it is the current `state.requests.secret`, byte for byte |
| **Legacy encryption key** | Decrypts unversioned (format v0) blobs. It is `derive(identity, "encryption")`, exactly as today | Read-only. Retired only after an inventory shows no v0 blob remains |
| **Encryption keys** (32 bytes each, with key IDs) | AES-256-GCM for new blobs (format v1). Each is used directly as the AES key after `derive(key, "encryption-v1")`, keeping domain separation | Rotated. Exactly one is primary. Others are decrypt-only until retired |

### Why identity stays stable

The identity key is a separate secret from the encryption keyring, and it is
never rotated. Every identifier, blob key and reference proof is computed from
it alone. Changing the primary encryption key therefore changes no request ID,
operation ID, attempt ID, scope tag, binding, fingerprint, checkpoint identity,
blob key, `llmtw_cpb_` reference, append token or cache key. As a result:

- deduplication still finds the original operation;
- pending-request recovery still lists the same shards and rows;
- checkpoint handles and blob references issued before rotation still verify;
- content addressing still converges. The same plaintext under the same stream
  still maps to the same blob key, whichever encryption key wrote the first
  copy.

This is the "separate identity key" strategy from the issue. We rejected an
explicit lookup or migration strategy (dual-computing identifiers under old and
new identity keys). It would double every lookup, make deduplication ambiguous
during the overlap, and need a migration of immutable rows for no security
benefit. HMAC identifiers reveal nothing about plaintext beyond equality.

**Identity-key compromise.** An attacker with only the identity key cannot
decrypt v1 data. They can forge blob reference proofs, which grant no
authorization by themselves, and they can test guesses of known content
against blob keys. They can also decrypt v0 data, because the legacy
encryption key is derived from the identity key. That is one reason to
re-encrypt nothing in place but to let v0 data age out, and to bring new
writes onto v1 promptly. Recovering from an identity compromise means
replacing the namespace: configure a new `namespace`, provision a new identity
key, and treat the old namespace as read-only until it is retired. That is a
deliberate migration, not a rotation, and it is out of scope here.

**Wrong identity key guard.** A mistyped identity key would make a worker
silently create a parallel, empty universe and break deduplication. Phase 2
adds a namespace canary to prevent that. A KV row at a fixed,
non-derived key `<namespace>/identity-check` holds
`derive(identity, "identity-check")`. `Open` creates it with a conditional
write if absent and fails closed on mismatch. The row holds a PRF output, not
key material.

## Ciphertext envelope

### Format v1

```
offset  size  field
0       4     magic  "LTWE" (0x4C 0x54 0x57 0x45)
4       1     format version = 0x01
5       1     key-ID length L, 1..32
6       L     key ID, ASCII [A-Za-z0-9_-]
6+L     12    GCM nonce (random)
18+L    n+16  AES-256-GCM ciphertext and tag
```

- AAD = `blobKey || 0x00 || header[0 : 6+L]`. The magic, version and key ID
  are authenticated, so changing the key ID or downgrading the version fails
  authentication.
- The maximum overhead grows from 28 to 66 bytes. `readBlob` size limits must
  use the maximum header length.
- The blob key, the `readBlob` plaintext-to-key check, immutability and the
  `AlreadyExists` compare path do not change.

### Reading v0 (existing unversioned) data

v0 blobs start with a random nonce, so in about one blob in 2^40 the bytes
look like a v1 header. AEAD authentication resolves the ambiguity. The reader
does this:

1. If the bytes parse as a v1 header and the key ID is in the keyring, open
   with that key. On success, return the plaintext.
2. Otherwise, if v0 decryption is enabled (`legacy_unversioned` is `decrypt`),
   open the whole object as v0 with the legacy key. On success, return the
   plaintext.
3. Otherwise, classify the failure:
   - The v1 header names a key ID listed as **retired** → `ErrKeyRetired`.
     This is non-retryable and distinct from corruption, so operators can see
     stranded data.
   - The v1 header names an **unknown** key ID → `ErrKeyUnavailable`. This is
     *retryable*, classified like a dependency outage. The common cause is a
     worker with an older keyring snapshot. Reporting `ErrCorrupt` here would
     let the runtime mark the request `state_corrupt`, which must not happen
     during a rotation.
   - Anything else → `ErrCorrupt`, as today.

A GCM forgery under the wrong interpretation has probability about 2^-128, so
trying both interpretations cannot return wrong plaintext.

Writers emit v1 only when a keyring is configured. Without one, the worker
behaves exactly as it does today: it writes v0 with the legacy key. Existing
deployments need no migration. v0 data stays readable as long as
`legacy_unversioned` is `decrypt`. There is no in-place re-encryption, because
blobs are immutable and the contract has no atomic replace. Retiring the
legacy key relies on retention and the inventory tool below.

### Deployment order: readers first

A release that can *read* v1 must be fully deployed before any worker is given
a keyring and starts *writing* v1. Phase 1 ships the reader with writes
unchanged. A rollback below that release is unsafe once v1 data exists. The
runbook states this as an explicit gate.

## Keyring document

The encryption keyring is one Secrets Manager secret (`SecretString`, JSON,
at most 64 KiB; the resolver's existing limit):

```json
{
  "format": "llmtw.storage-keyring.v1",
  "legacy_unversioned": "decrypt",
  "keys": [
    {"id": "k2026-10", "key": "<base64 of 32 bytes>", "state": "primary"},
    {"id": "k2026-04", "key": "<base64 of 32 bytes>", "state": "decrypt"}
  ],
  "retired": ["k2025-10"]
}
```

These rules are validated on every load and every refresh:

- Unknown fields are rejected. Base64 is strict and keys are exactly 32 bytes.
  IDs match `^[A-Za-z0-9_-]{1,32}$` and are unique.
- There is exactly one `primary`. Other keys are `decrypt`. A key may also be
  `pending`: loaded and readable, but announced as not yet safe to promote.
  The rotation tooling uses this, and readers treat it like `decrypt`.
- `retired` lists IDs that must not appear in `keys`.
- `legacy_unversioned` is `decrypt` or `retired`.

These transition rules apply against the worker's last-good keyring. A refresh
that breaks one is rejected: the last-good keyring stays in use and an alarm
fires.

- **Key IDs are immutable.** An ID present in both versions must have
  identical bytes.
- **No silent removal.** A key may leave `keys` only if the same document
  lists it in `retired`. A retired ID may never come back.
- `legacy_unversioned` may go from `decrypt` to `retired`, never back.

The identity key stays in its own secret, `state.requests.secret`. It is
resolved once per process and never refreshed. Keeping it out of the keyring
document means routine rotation edits can never touch it.

## Resolver boundary and configuration

### Boundary

The `internal/secrets` interface stays `Resolve(ctx, SecretRef) ([]byte,
error)`. It returns opaque bytes and is unaware of keyring semantics.

- A new kind, `aws_secrets_manager`, is added to `config.SecretRef`. It is
  implemented by a `SecretsManager` source inside `internal/secrets`, behind a
  small client interface (`GetSecretValue`) so tests can inject a fake. Later
  providers (GCP Secret Manager, Vault) add kinds without changing callers.
- The keyring parser, validation, transition rules and refresh loop belong in
  a storage-key package used by `storage/cloudstate`, for example
  `storage/cloudstate/keys`. They receive bytes from the resolver.
  `cloudstate` never imports an AWS SDK for secrets.
- `cloudstate.Open` takes a key provider, not a raw secret. The provider
  returns the identity key, which is immutable, and an atomically swappable,
  immutable keyring snapshot.
- `file` and `env` stay available for both secrets, for local development and
  tests. A `file` keyring is re-read on the same refresh interval.
- Workload credentials come from the default AWS credential chain (IRSA, EKS
  Pod Identity, ECS task role or instance profile). The configuration accepts
  no access keys, and the strict loader rejects unknown fields as it does now.

### Configuration

```yaml
state:
  requests:
    # ...provider, tables, namespace unchanged...
    secret:                       # identity key: base64 of 32 bytes, process lifetime
      kind: aws_secrets_manager
      aws:
        region: ap-southeast-2
        secret_id: arn:aws:secretsmanager:ap-southeast-2:123456789012:secret:llmtw/prod/storage-identity-AbCdEf
        version_stage: AWSCURRENT # or version_id (mutually exclusive)
    encryption_keyring:           # optional; absent = today's single-key v0 behaviour
      source:
        kind: aws_secrets_manager
        aws:
          region: ap-southeast-2
          secret_id: arn:aws:secretsmanager:ap-southeast-2:123456789012:secret:llmtw/prod/storage-keyring-GhIjKl
          version_stage: AWSCURRENT
      refresh_interval: 5m        # default 5m, allowed 30s..1h, ±10% jitter
      fetch_timeout: 5s           # per attempt, default 5s, max 30s
      stale_after: 1h             # report degraded status after this long without a successful refresh
```

- `secret_id` must be a full ARN, which names the account and avoids
  name-suffix ambiguity. `region` is required and must match the ARN.
- `version_stage` defaults to `AWSCURRENT`. `version_id` pins one version, for
  incident response or reproducible tests.
- All fields are references, so they are non-secret and appear in the
  configuration digest and output. The resolved value, the Secrets Manager
  `VersionId` and the key IDs are runtime status, not configuration.
- `state.requests.secret` and `state.requests.encryption_keyring.source` are
  process-lifetime fields and are added to `reload_validation.go`. The
  keyring's *contents* change through refresh, not through configuration
  reload. A successful configuration reload, or `SIGHUP`, also triggers an
  immediate keyring refresh, rate-limited to one every 30 seconds.
- For multi-region deployments, replicate both secrets to every fallback
  region. Each regional deployment points at its local replica's ARN.

### Failure behaviour

| Situation | Behaviour |
| --- | --- |
| Startup: access denied, not found, decryption failure, malformed or invalid document | Fail closed immediately, with no retries. The snapshot is rejected with a bounded classification (`secret_access_denied`, `secret_not_found`, `secret_malformed`, ...). The worker never polls for paid work |
| Startup: throttling, 5xx, timeout | Up to 3 attempts with jittered backoff inside a 30 s budget, then fail closed as above |
| Refresh fails (any cause) | Keep the last-good keyring, increment a failure counter, and retry at the next interval with backoff. Readiness is unchanged, because the last-good keyring is still correct for everything it lists. After `stale_after`, report the keyring status as `stale` |
| Refresh returns an invalid document or breaks a transition rule | Same as a failed refresh, plus a distinct `keyring_transition_rejected` alarm |
| Read names an unknown key ID | Retryable `ErrKeyUnavailable`, which triggers an immediate rate-limited refresh. Temporal retries the Activity |
| Read names a retired key ID | Non-retryable `ErrKeyRetired`, reported with the key ID only |
| **Any** failure | **Never generate, derive or substitute a key.** There is no fallback to an empty keyring, the legacy key or a random key |

Secrets are never fetched on a blob operation. The only network fetches are at
startup, on the refresh interval, on configuration reload, and on the
rate-limited unknown-key trigger.

## Rotation procedure

Rotation is a staged change to the keyring document. Each stage is published
as a new Secrets Manager version (`PutSecretValue`, which moves `AWSCURRENT`).
Workers pick it up within one `refresh_interval` (plus jitter and backoff).
The *convergence window* `W` is `refresh_interval × 2 + rollout time for any
worker that has the keyring configured but has not restarted`. Every gate
below is checked with the `keyring_info{primary, key_ids, version}` status
metric, not only by waiting.

Workers can hold different snapshots at the same time, because refresh
intervals drift and new workers start during a rollout. The stages make every
mix of adjacent snapshots safe.

1. **Add** the new key `K2` as `pending`. Primary stays `K1`. Wait until every
   worker reports `K2` loaded, or wait `W` if metrics are missing.
   - *Mixed state:* all workers write with `K1`. Workers that already have
     `K2` cannot see any `K2` data, because none exists yet.
2. **Promote** `K2` to `primary` and demote `K1` to `decrypt`. Wait `W`.
   - *Mixed state:* some workers write `K1`, others `K2`. Every worker can read
     both: all have `K2` after stage 1, and `K1` is still listed.
     Content-addressed collisions are safe: whichever key wrote the first copy
     of a blob, the `AlreadyExists` compare path decrypts it with the key named
     in its header.
3. **Keep** `K1` as `decrypt` for as long as any retained data, or any backup
   that may be restored, contains `K1` blobs. See [retirement](#retirement).

Rotation does not change the identity key, so no identifier, handle,
reference or recovery shard changes at any stage.

### Rollback

- **Rolling back a promotion** (after stage 2): publish a *new* version with
  `K1` primary and `K2` `decrypt`. Do not just move `AWSCURRENT` back to the
  stage-1 version. Both documents list both keys, so data written under either
  key stays readable during the mixed period.
- **Rolling back an addition** (after stage 1, before promotion): `K2` may be
  removed only by listing it in `retired`, under the no-silent-removal rule.
  This is safe because no data was written with it. Prefer leaving it as
  `decrypt`.
- **Never** move `AWSCURRENT` to a version that lacks a key that may have
  written data. Workers reject that transition (no silent removal) and keep
  their last-good keyring. A *newly started* worker has no last-good keyring
  and would accept it, then return `ErrKeyUnavailable` for affected blobs. The
  rotation tooling therefore refuses to publish such a document, and the
  runbook forbids `UpdateSecretVersionStage` rollbacks.
- **Binary rollback** below the v1-reader release is forbidden once any v1 data
  exists (see [deployment order](#deployment-order-readers-first)).

### Secrets Manager rotation Lambdas

Do not configure automatic Secrets Manager rotation for the keyring. Its
single-step `AWSPENDING`→`AWSCURRENT` flow cannot express the add, wait,
promote sequence or the waits for convergence. Rotation runs from operator
tooling (Phase 5) on a schedule, for example every 90 days, or after an
incident.

### Retirement

A decrypt-only key, or `legacy_unversioned`, may be retired only when all of
these hold:

1. It is not primary, and it has not been primary for at least `W` plus the
   longest in-flight Activity time, so no straggler is still writing with it.
2. A key inventory scan of the payload bucket(s), including regional replicas,
   reports **zero** live objects under that key ID (or zero v0 objects for the
   legacy key). The scanner reads each object's header bytes. Objects are
   attributed by their header, not by write time, because a blob written
   early under `K1` stays `K1` forever. Retiring a key early would strand
   retained data unless that data has passed its documented retention and been
   deleted.
3. The key is kept in [escrow](#backup-and-restore) until every data backup
   that might contain its blobs has expired.

Retirement is an explicit edit: move the key from `keys` to `retired`.
Afterwards, a stray object under that key yields `ErrKeyRetired` rather than a
silent `ErrCorrupt`. Stranded data is visible, and it can be recovered by
restoring the key from escrow. A retired ID cannot return to the keyring, so
recovery means a new keyring entry under a *new* ID with the same bytes. The
runbook names this the break-glass path, because the reader matches by ID.

## Backup and restore

Data backups (DynamoDB PITR/on-demand backups, S3 versioning or replication)
cannot be read without the keys. Back up keys separately from data and keep
them at least as long as the data.

- **Escrow.** After every keyring change, and once for the identity key, copy
  the secret value to an escrow store in a separate AWS account. One option is
  a Secrets Manager secret encrypted with a break-glass KMS key, readable only
  by a restore role that needs MFA or an approval workflow. Keep every key ID
  that ever existed for the backup retention period, plus margin.
- **Restore order.**
  1. Restore the identity secret byte-identically. The namespace canary
     verifies this.
  2. Restore a keyring that contains every key ID present in the restored
     data. A retired ID needed again comes back under a new ID with the same
     bytes, per the retirement section.
  3. Restore data.
  4. Run the inventory scan and a sample read of each key ID before
     re-enabling workers.
- **Restore drill.** The release AWS gate restores a synthetic namespace into
  an isolated account. The data spans v0, `K1` and `K2`, and the drill checks
  that the scanner and reads succeed.

## Least-privilege IAM

**Worker role** (workload identity; no static credentials):

```json
{
  "Effect": "Allow",
  "Action": "secretsmanager:GetSecretValue",
  "Resource": ["<identity secret ARN>", "<keyring secret ARN>"],
  "Condition": {"StringEquals": {"secretsmanager:VersionStage": "AWSCURRENT"}}
}
```

```json
{
  "Effect": "Allow",
  "Action": "kms:Decrypt",
  "Resource": "<CMK ARN encrypting the secrets>",
  "Condition": {
    "StringEquals": {"kms:ViaService": "secretsmanager.<region>.amazonaws.com"},
    "StringLike": {"kms:EncryptionContext:SecretARN": ["<identity secret ARN>", "<keyring secret ARN>"]}
  }
}
```

- When a deployment pins `version_id`, drop the `VersionStage` condition for
  that role.
- Workers get no `DescribeSecret`, `ListSecrets`, `PutSecretValue`,
  `UpdateSecretVersionStage`, `DeleteSecret`, `kms:Encrypt` or
  `kms:GenerateDataKey`.
- **Rotation operator role:** `GetSecretValue`, `PutSecretValue` and
  `DescribeSecret` on the keyring secret only, plus `kms:Encrypt`,
  `kms:GenerateDataKey` and `kms:Decrypt` via Secrets Manager. It cannot touch
  the identity secret. Assuming it needs MFA or CI approval.
- **Secret resource policies** allow only the worker role, the rotation
  operator role and the escrow/backup role. They deny `DeleteSecret` to
  everyone except a break-glass role, and set a 30-day recovery window.
- **Inventory scanner:** `s3:GetObject` (ranged) and `s3:ListBucket` on the
  payload bucket(s). It does not need the keys and never decrypts.

Provisioning (the secrets, CMK, replicas, policies and escrow account) is
infrastructure work in att-deps and is not embedded in the storage adapter.

## Redaction guarantees

Resolved identity and encryption key bytes:

- never enter configuration snapshots, the configuration digest, effective
  configuration output, logs, errors, metrics, traces, Temporal payloads,
  Activity results or histories, KV rows or blob plaintext;
- are held only in the resolver's transient buffer, which is cleared after
  parsing (`clear`), and in the immutable keyring snapshot. A replaced snapshot
  is cleared once no reader holds it. Go's GC means this is best-effort, not a
  guarantee against memory disclosure;
- are never formatted. Keyring types implement `String`, `GoString`,
  `Format` and `MarshalJSON`/`LogValue` to print `[redacted]`, as
  `internal/diagnostic` does for other sensitive values.

Errors carry only:

- the configuration path, for example `state.requests.encryption_keyring`;
- a bounded cause class: `access_denied`, `not_found`, `throttled`,
  `timeout`, `decryption_failed`, `malformed`, `transition_rejected`;
- where relevant, a key ID.

AWS SDK error messages and response bodies are never wrapped verbatim. This
matches how `buildCloudRequests` treats provider errors today.

Status exposed to operators contains no key material:

- primary key ID, all loaded key IDs and states, and retired IDs;
- the Secrets Manager `VersionId` of the active document;
- last refresh success time and age, consecutive failures, and `stale` /
  `ok`;
- a per-key **fingerprint**: the first 8 bytes of
  `HMAC-SHA-256(key, "llmtw/keyring-fingerprint/v1")`, hex encoded. It lets
  operators confirm that every worker holds the same bytes for an ID without
  revealing the key.

## Implementation phases

Each phase is a separate PR, kept small enough to review, with the listed tests
as its acceptance gate. Phases 1 and 2 have no AWS dependency.

### Phase 1: versioned envelope reader (no write change)

- Internal key-set type in `cloudstate` (identity, legacy AEAD, keyed AEADs,
  primary). Parse v1 envelopes with the read algorithm above. Writes stay v0.
- Add `ErrKeyUnavailable` (retryable, mapped to dependency unavailable) and
  `ErrKeyRetired` (non-retryable, distinct from `state_corrupt`), and update
  `readBlob` size limits.
- Tests:
  - golden v0 blobs produced by the current code still decrypt;
  - v1 round trip;
  - tampering with the magic, version, key ID or nonce fails;
  - a v0 blob whose nonce begins with the magic bytes still decrypts;
  - unknown and retired key IDs classify correctly;
  - the oversize/undersize limits hold;
  - fuzz the header parser.

### Phase 2: keyring model and v1 writes

- Keyring document parser and validator, with transition rules against the
  last-good keyring. `file`/`env` keyring sources. `cloudstate.Open` accepts a
  key provider, and v1 writes start when a keyring is configured. Namespace
  identity canary. Configuration fields and reload validation.
- Tests:
  - document validation table;
  - each transition rule (immutable IDs, no silent removal, legacy one-way);
  - **mixed workers:** two repositories on shared in-memory stores, with
    snapshots {v0-only, K1 pending, K1 primary, K2 primary} in every adjacent
    pair, write and read each other's data;
  - restart with a new snapshot;
  - rollback by re-promotion;
  - an `AlreadyExists` collision across keys;
  - request deduplication, operation/attempt IDs, pending-request recovery,
    checkpoint handles and `llmtw_cpb_` references are byte-identical before
    and after rotation;
  - the canary rejects a wrong identity key.

### Phase 3: AWS Secrets Manager source

- `aws_secrets_manager` `SecretRef` kind, configuration validation (ARN and
  region agreement, stage/ID exclusivity), and a client behind an interface
  using the default credential chain. Bounded startup retries and the
  cause-classified errors.
- Tests with a fake client:
  - initial load;
  - access denied, not found, KMS decryption failure, malformed and oversized
    values (no retries);
  - throttling and timeout then success, and exhaustion;
  - version stage and version ID selection;
  - context cancellation;
  - a sentinel-key redaction test that scans errors, logs and configuration
    output.
- Add an opt-in real-AWS case to the existing AWS gate.

### Phase 4: refresh, status and runtime wiring

- Refresh loop with jitter, last-good retention, refresh on reload and on
  unknown key, an atomic snapshot swap, status metrics, and readiness and
  Activity error classification.
- Tests:
  - refresh picks up a promotion;
  - a failed or invalid refresh keeps last-good and reports `stale` after
    `stale_after`;
  - concurrent reads during a swap (`-race`);
  - the rate limit on the unknown-key trigger;
  - metrics, logs and Temporal test histories contain no sentinel key bytes
    or their base64.

### Phase 5: operator tooling and runbook

- Offline keyring tooling to generate a key (written to a 0600 file, never to
  stdout), validate a document, and check a proposed transition against the
  current one. The tooling refuses unsafe documents.
- Key inventory scanner, and a runbook covering rotation, rollback,
  retirement, escrow and restore.
- Operator documentation in `reference/configuration.md` and the cloud request
  repository reference.
- Link the att-deps infrastructure work.
- Tests: tooling transition checks match the worker's rules (shared code);
  the scanner counts v0/v1 per key ID on fixtures; the restore-drill script
  runs in the AWS gate.

## Out of scope

- Rotating the identity key. That requires a namespace migration.
- Rotating continuation handle keys and provider API credentials. They may
  reuse the `aws_secrets_manager` resolver kind later, with independent
  lifecycles.
- Redis `key_secret`.
- In-place re-encryption of immutable blobs.
- Per-object KMS envelope encryption. A future envelope format version could
  add it.
