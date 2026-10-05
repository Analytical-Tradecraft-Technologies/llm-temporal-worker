# Security and Privacy

> Target phase status and authority are centralized in
> [scope](../scope.md#staged-delivery-and-document-authority). Request metadata
> and indexes contain content-free references; prompt/tool/output payloads use
> authenticated encrypted blobs. Cache
> lookup uses tenant-scoped HMAC-SHA-256. See
> [State and storage](state-and-storage.md).

## Trust boundaries

Untrusted inputs include Activity payloads, continuation handles, configuration
files, provider responses/streams, Redis values, blob objects, compatible API
endpoints, and all namespaced extensions. Authentication does not make content
structurally trusted.

The worker validates before:

- allocating from claimed lengths;
- following a URL or blob locator;
- compiling JSON Schema;
- sending data to a configured endpoint;
- decoding provider-state bytes;
- using provider cost/usage in arithmetic;
- emitting error or observability fields.

## Tenant isolation

Every request has a validated tenant scope. Operation keys, continuation records,
result blobs, budgets, and audit identifiers bind to that scope. Store lookup
requires the scope and handle MAC; knowing a continuation ID alone is
insufficient.

Logical model routes declare allowed tenants/projects, regions, and data
classifications. A continuation cannot cross those restrictions. Shared Redis
keys use keyed hashes so tenant identifiers are not exposed in key scans.

## Secrets

Configuration contains references, not secret values:

```yaml
auth:
  kind: bearer_env
  name: OPENAI_API_KEY
```

`auth.kind` names the authentication mode (`bearer_env`, `header_env`, or a
provider workload-identity/default chain). Standalone secret references use
`kind: env`, `file`, or `workload_identity` instead.

V1 resolvers support environment, mounted file, and platform workload identity
where the official SDK supports it. Secret values:

- are resolved only at process/client construction;
- never enter snapshots, digests, Temporal payloads, logs, metrics, traces,
  errors, continuation records, or test fixtures;
- are held for the minimum useful lifetime;
- are redacted by allow-list logging, not regex alone;
- rotate by building a new client snapshot.

Arbitrary command execution as a secret resolver is not supported.

## Egress

Endpoints are operator-configured and validated:

- every endpoint has a non-empty `outbound_hosts` list of normalized DNS
  hostnames; IP literals, user info, URLs, and duplicate spellings are
  rejected, and a configured base URL's hostname must appear in that list;
- HTTPS is required, and base URLs cannot contain user info, query, or
  fragment;
- the provider transport checks each request host and HTTPS port against that
  endpoint's policy rather than trusting a request-time URL: the base URL host
  is limited to its effective port, and any additional configured host is
  limited to 443;
- automatic redirects and environment proxies are disabled; no v1 endpoint is
  documented as redirecting, so a future exception must revalidate every hop;
- every success, error, and streaming response body is capped by
  `limits.provider_response_bytes` (16 MiB by default, with a 64 MiB hard
  ceiling); oversized declared lengths are rejected before parsing and
  unknown or misleading lengths are stopped by a one-byte overrun probe;
- DNS is resolved at dial time, every returned address is rejected if it is
  loopback, private, link-local, multicast, unspecified, carrier-grade NAT,
  benchmarking, deprecated IPv6 site-local (`fec0::/10`), or a known cloud
  metadata address, and the connected address is checked again before use;
- only an explicit policy/configuration rejection is an egress denial. Before
  a writable connection is acquired, guarded DNS/TCP/TLS and client-local
  timeout failures are certified `not_dispatched` availability failures, while
  caller cancellation/deadline remains non-retryable and `not_dispatched`;
  after that boundary the result is ambiguous rather than re-routed;
- provider transport errors are classified with a configured endpoint ID and
  never include URLs, credentials, authorization headers, request content,
  continuation handles, or raw provider bodies;
- custom CA roots are file references;
- compatible endpoints require an allow-listed hostname and profile ID.

User-provided image/reference URLs are not fetched by the worker in v1. They are
passed only to endpoint profiles that accept remote references and only after
scheme/host policy validation: image and document URLs must use `https` or
`http`, must not carry user information, and must not name a local, private or
metadata destination, because the provider fetches them inside its own network.
The host policy rejects:

- an IP literal that the provider egress policy above would refuse to dial
  (the two share one address table): loopback, private, link-local,
  unspecified, multicast, carrier-grade NAT, reserved and benchmarking ranges,
  cloud metadata addresses, IPv6 site-local, and IPv4-mapped, IPv4-compatible,
  NAT64, 6to4 and Teredo forms that embed such an IPv4 address; a zoned IPv6
  literal is always rejected;
- a host that is not a canonical IP literal but that resolvers and URL parsers
  read as IPv4 because its last label is a decimal, octal or hexadecimal number
  (`127.1`, `2130706433`, `0x7f000001`, `0177.0.0.1`); names that merely contain
  digits, such as `cdn1.example.com` or `1password.com`, are unaffected;
- a host containing anything other than ASCII letters, digits, `-`, `_` and
  `.`, which a fetcher could normalise into one of the forms above;
  internationalised names must be given in punycode;
- `localhost` and `.localhost` names, the private-use `.internal` zone (which
  holds `metadata.google.internal` and the EC2 internal names), and the
  metadata aliases `metadata`, `metadata.goog` and `instance-data`.

The policy inspects the URL only. A public DNS name that resolves to a private
address cannot be detected by the worker, which never resolves or fetches these
URLs; providers remain responsible for their own fetch-time controls.

The policy is enforced where a request enters: the v1 Generate request codec
(`append` items and `settings_patch.instructions`), the legacy Activity payload
boundary and the engine entry points. It is deliberately not part of the stored
transcript codec, so a checkpoint written under an earlier policy still decodes
and materializes; only the structural check (a scheme other than `javascript`
or `data`) applies when stored content is decoded. Compact requests carry no
content of their own.

Stored content is checked again before it can reach a provider. Request
preparation applies the policy to the whole provider-facing request, replayed
parent transcript and inherited instructions included, for both Generate and
Compact. Continuing from a checkpoint that holds a now-blocked URL therefore
fails as non-retryable `invalid_argument` before the operation is recorded,
rather than replaying the URL or being retried as a transient storage failure.
Blob locators address configured stores, not arbitrary URLs.

## Content and history

Prompts, outputs, tool arguments/results, images, documents, reasoning, and
provider state are sensitive by default. The worker:

- avoids content in logs, metrics, trace attributes, heartbeat details, and
  current v1 ledger records;
- uses BlobRefs to limit Temporal history content/size;
- supports an external Temporal Payload Codec for encryption;
- encrypts production blob/Redis traffic in transit and relies on configured
  at-rest encryption;
- records retention/expiry metadata; automatic production cleanup is deferred;
- records provider storage/retention choices in endpoint profiles;
- is intended to provide content-free audit events for access and deletion;
  these are not yet emitted for Generate/Compact access or scope denials (only
  query audit and configuration reloads are logged today).

The cloud Generate contract stores bounded content-free metadata and references
for recovery and cache verification. Prompt, tool, output and provider-state
payloads use immutable encrypted blobs, with digest and key provenance in cloud
metadata. The runtime can decrypt only with the configured keys; access to stored
ciphertext alone does not reveal plaintext. Payloads are
retention-governed and excluded from observability and indexes. Large or
ancestral content remains in encrypted blobs referenced by digest rather than
being duplicated into every cache row.

Provider-hosted continuation is enabled only when the operator accepts that
provider's retention behavior. A local canonical transcript can be disabled for
data-minimization, at the cost of route portability.

## Schema and tool safety

The worker validates and transports tool definitions and calls; it never runs
tools. Tool names, descriptions, schemas, call IDs, JSON depth, byte size, and
argument shape are bounded. JSON parsers reject duplicate keys and excessive
nesting.

Structured model output is untrusted even after provider strict-schema claims.
It is parsed with limits and validated locally against the canonical schema.
Callers remain responsible for authorization and side effects when executing a
tool.

## Provider-state safety

Opaque state is tagged with provenance, media type, digest, and size. It is
never interpreted by a different adapter and never copied to another provider.
Logs show only digest prefix and safe provenance. Unknown blocks fail strict
conversion.

Thinking/reasoning content may have provider-specific integrity signatures.
Adapters preserve exact bytes and ordering, and tests prove round-trip behavior.

## Supply chain

- Go modules, Actions, and the Go toolchain patch are version-pinned and
  updated through reviewed PRs.
- Official provider SDKs are preferred; raw HTTP requires an ADR, scoped package,
  wire fixtures, and a migration/removal condition.
- The race job captures test output and scans it before printing failures. This
  preserves the project-specific provider-payload leakage gate without running
  the Go suite a second time. GitHub secret scanning covers generic checked-in
  secrets for this public repository.
- Pull-request CI uses GitHub dependency review plus `make
  security-pr-verify`. Direct modules must fall under reviewed ATT-owned or
  well-known module roots. `govulncheck` scans both base and head with the same
  pinned v1.7.0 tool on the reviewed 1.26.7 toolchain and blocks only new
  findings or new reachable traces.
- A dedicated scheduled workflow runs `make security-verify` against current
  `master`, including transitive dependencies, and enforces the complete
  vulnerability-exception inventory. Routine master verification does not
  repeat this scan; guarded release preflight retains it.
- License inventory and vulnerability exceptions are explicit, reviewed files;
  an exception requires an owner, future expiry, remediation reference, and
  trace scope. A module-only exception cannot suppress a later reachable
  package/function trace without explicit re-review, nor can any exception
  suppress an unlisted or newly reported finding.
- Release artifacts are reproducible where practical, signed, and accompanied
  by provenance.
- The runtime image is non-root, read-only, capability-dropped, and contains no
  compiler or package manager.

## Abuse and resource limits

Configuration bounds:

- request, item, part, schema, and inline byte counts;
- context and maximum output tokens;
- tools, parallel calls, extension size, and JSON depth;
- provider/Activity deadlines and route attempts;
- concurrent Activities per task queue and per endpoint;
- Redis/client pool sizes;
- continuation depth and retained bytes.

Admission runs after structural validation but before expensive provider work.
Limits fail closed with safe errors and do not echo attacker-controlled content.

## Security verification

Required tests include cross-tenant handle access, MAC/key rotation, key-name
privacy, malicious Redis/blob data, SSRF-shaped URLs, redirect behavior,
duplicate/deep JSON, oversized streams, log/trace/heartbeat leak assertions,
secret rotation, corrupt provider state, and untrusted provider error bodies.

Live credentials are never required for the core security suite.
