# Conversation Checkpoints, Forking, Caching, and Compaction

## Status and boundary

The cloud workflow runtime implements immutable checkpoints, response caching
and compaction under the existing v1 API names. Public Generate and Compact
workflows return final typed responses; their bounded activities may return
pending while a resumable provider finishes. See
[workflow behavior](../reference/internal-workflows.md) and
[cloud storage](../reference/cloud-request-repository.md) for the implemented
execution boundary.

A response may contain application tool calls. The caller executes those tools
and supplies their results in a later request; the worker does not own the agent
loop. Schema, fixture, Go and OCaml changes remain coordinated before the first
release.

## Why a checkpoint graph

Temporal serializes Activity inputs and outputs into Workflow history. Repeating
an entire transcript for every turn produces quadratic history growth and risks
service payload/event limits. The Generate boundary sends only:

- one opaque parent checkpoint handle, omitted for a root;
- new semantic items being appended;
- a sparse settings patch;
- per-operation policy such as cache acceptance; and
- a stable operation key.

A successful operation creates an immutable child. The parent is never updated,
so the graph naturally supports branches:

~~~text
root A
  |
  +-- response X1
        |
        +-- request B --> response X2
        |
        +-- request C --> response Y2
        |
        +-- request D --> response Z2
~~~

All three child requests name X1 and use distinct operation keys. Scheduling
order does not affect their meaning. Retrying any one request returns its own
stored result without mutating the other branches.

## Activity contracts

### Generate v1 request

~~~json
{
  "api_version": "llm.temporal/v1",
  "operation_key": "claim-481-turn-7-branch-2",
  "context": {
    "tenant": "acme",
    "project": "claims",
    "actor": "workflow:claim-481"
  },
  "parent": "ckp_v1.k1.opaque-id.opaque-mac",
  "append": [
    {
      "kind": "tool_result",
      "call_id": "call_17",
      "content": [{"kind": "json", "value": {"approved": true}}],
      "is_error": false
    },
    {
      "kind": "message",
      "actor": "human",
      "content": [{"kind": "text", "text": "Continue from that result."}]
    }
  ],
  "settings_patch": {
    "reasoning": {
      "effort": {"set": "high"}
    },
    "tools": {"clear": true}
  },
  "cache": {
    "max_age_seconds": 15552000,
    "variant": 0
  }
}
~~~

The request is valid only after resolving the parent and materializing the
effective settings. The **context** is mandatory operation scope and is never
inherited from stored model content. The checkpoint handle is MAC-bound to the
canonical tenant scope; cross-tenant lookup has the same safe error as an
unknown handle.

**operation_key** is stable for every Temporal retry of the same logical
request. Reusing it with a different fully normalized request digest is a
non-retryable conflict. A fork uses a new operation key, even if its delta is
byte-identical to another child.

**append** preserves the existing semantic item types. It may be empty only
when a meaningful settings change requests a new model turn and the selected
provider contract permits an input-less turn. The worker rejects unpaired tool
results, reused tool-call IDs, invalid ordering, and a delta that begins inside
an incomplete tool exchange.

### Sparse settings patch

Every patchable leaf has three wire states:

| Wire state | Meaning |
| --- | --- |
| Field omitted | Inherit the parent value; on a root use the documented default |
| **{"set": value}** | Replace the leaf or collection with value |
| **{"clear": true}** | Remove an optional value or reset it to its documented root default |

**set** and **clear** are mutually exclusive. An empty patch object is valid and
identical to omission. Unknown leaves fail validation. Collection patches
replace or clear the complete collection; implicit append/remove operations are
not supported because they are difficult to hash and reason about across
versions.

Patchable groups include:

- logical model and service class/fallbacks;
- portability;
- ordered instructions;
- tools and tool policy;
- output limit/format;
- temperature and other sampling controls;
- reasoning effort/summary controls;
- provider-neutral compaction policy; and
- allow-listed provider extension values.

Nested leaves are independent. For example, changing reasoning effort does not
resend or reset the summary preference. Tools remain exactly unchanged when
the **tools** field is absent, including their schemas and order.

Root materialization applies the same public defaults as v1, including omitted
service class becoming **standard**. A root request must materialize a model and
all other fields required by the semantic validator. Provider defaults that
would make cache validation ambiguous are represented as unknown rather than
invented values.

### Cache policy and request index

The **cache** object is opt-in:

| Request form | Exact-response cache behavior |
| --- | --- |
| **cache** omitted | Do not read or populate the worker cache |
| **cache: {}** | Enable reuse without an age restriction |
| **cache.max_age_seconds** present | Reuse a matching success within that positive age bound |

Age is measured from successful completion against the lookup's single supplied
clock instant. Reading or using an entry never refreshes it; newer failures do
not invalidate an older eligible success.

The public **cache.variant** is the non-negative int32 request index, normally
zero. It participates in internal request/cache identity and separates samples;
it is not provider input or a seed and does not guarantee different output.
It is independent of temperature, including omitted or zero temperature.
Compaction uses its compatible content/policy identity and variant zero.

Unavailable or corrupt opted-in cache state is an error, not a miss that silently
creates another paid request. Truncated or incomplete output is not cached as a
normal success; valid application tool-call output can be successful.

### Generate v1 response

~~~json
{
  "api_version": "llm.temporal/v1",
  "operation_key": "claim-481-turn-7-branch-2",
  "operation_id": "op_01J...",
  "status": "completed",
  "output": [
    {
      "kind": "message",
      "actor": "model",
      "content": [{"kind": "text", "text": "The claim is approved."}]
    }
  ],
  "checkpoint": {
    "handle": "ckp_v1.k1.opaque-id.opaque-mac",
    "parent": "ckp_v1.k1.opaque-id.opaque-mac",
    "kind": "generation",
    "depth": 8
  },
  "cache": {
    "disposition": "miss_populated",
    "variant": 0,
    "entry_age_seconds": 0
  },
  "cost": {
    "status": "exact",
    "actual_cost_usd": "0.000014725000000000",
    "method": "provider_reported"
  }
}
~~~

The response contains only the new turn, safe route/usage/diagnostic data, and
the child handle. It never rematerializes the transcript into Temporal history.
Decimal money is encoded as a base-10 string so JSON and OCaml do not pass it
through binary floating point. There is no downstream currency field or
currency enum: names such as **actual_cost_usd** make the denomination part of
the type contract.

If the real charge cannot be established, the top-level Generate
**status** remains **completed** while **cost.status=unknown**,
**cost.actual_cost_usd=null**, and a safe cost reason is present with no method.
The worker never substitutes a catalog guess, reservation, or zero. This shape
maps to a closed exact/unknown OCaml cost variant without inventing an invalid
Generate lifecycle state.

A Generate cache hit still creates a distinct completed operation and immutable
**cache_replay** child. A Compact cache hit creates a distinct compaction child
with cache provenance. In both cases the result template is copied while
operation identity, timestamps, zero cost, cache diagnostics, and child handle
are regenerated. A cached result never returns the origin operation key or
checkpoint as if it were the current call.

## Checkpoint contents and materialization

A checkpoint row contains immutable metadata and content-addressed references:

- tenant/project ownership, parent, kind, depth, creation, and retention facts;
- the current delta and normalized response artifact references;
- canonical lineage digest and materialized-settings digest;
- model, routing, service, capability, price, and compaction versions;
- tool-call frontier and transcript validation summary;
- provider affinity and provider-state child records;
- originating operation and optional cache entry; and
- optional compaction provenance.

Large canonical item arrays, output, binary parts, and provider artifacts stay
in the configured encrypted blob store. The cloud KV store holds bounded
metadata, digests and immutable references. Cache response templates also use
encrypted blobs; the KV metadata does not contain plaintext model output.

Materialization walks parent links only until the newest compaction base or
materialized snapshot. It then:

1. verifies scope, handle MAC, row schema version, digests, and blob lengths;
2. reconstructs the inherited settings from versioned snapshots and patches;
3. reconstructs canonical semantic items in order;
4. validates tool-call/result pairing and provider-state provenance;
5. applies the new patch and append;
6. calculates the effective state digest and projected context size; and
7. returns an immutable in-memory view used by validation, caching, routing,
   admission, and provider compilation.

Materialization has hard limits for depth, rows, bytes, item count, and blob
reads. A periodic snapshot is a performance optimization and contains the same
digest as replaying the lineage. Snapshot creation never changes a public
handle or the logical graph.

Parent and child writes use foreign keys and immutable-column guards. There is
no mutable **latest checkpoint** pointer in the correctness path. Applications
may store their chosen branch head in Workflow state, where branch selection is
already durable.

## Exact-response fingerprint

The cache fingerprint is a versioned HMAC over canonical semantic input. The
matrix is normative:

| Field class | Cache-key treatment | Reason |
| --- | --- | --- |
| Tenant/project cache namespace | Include | Prevent cross-scope disclosure |
| Operation kind (`generate` or `compact`) | Include as a domain separator | A summary artifact and a normal model answer are different result contracts even when they share input lineage |
| Materialized canonical conversation, instructions, tool definitions/policy, structured output, sampling/temperature, reasoning controls, and output-affecting extensions | Include | May change provider-visible input or output |
| Route-cache identity, semantic compiler/profile version, capability lowering version, and cache epoch | Include | Initial cache reuse is isolated to one configured provider/endpoint/account/region/model lowering |
| Required opaque provider-state/pinning digest | Include; otherwise caching is ineligible | Required state may change the result and cannot be fabricated on replay |
| Compaction policy, summarizer equivalence ID, prompt version, and compacted artifact digest | Include when the lineage contains compaction | A lossy context representation is semantic input |
| Non-negative int32 variant | Separate indexed key component, included exactly once | Selects an explicitly retained stochastic sample |
| Provider, route, endpoint, account, region, and resolved provider model revision | Include through the keyed route-cache identity; origin provider request ID is excluded | Public metadata is insufficient to certify cross-provider identity, while a per-call request ID is provenance only |
| Requested/attempted service class and fallbacks | Exclude | Scheduling/cost consent, not model semantics; eligibility is still checked before reuse |
| Price/catalog versions, budgets, health/circuit state, deadlines, retry policy, and Temporal identifiers | Exclude | Admission/operations facts that do not change the cached answer |
| Operation key/ID, maximum cache age, timestamps, tracing IDs, pagination, and actor-only observability tags | Exclude | Per-call control or telemetry |
| Credentials and configuration secrets | Never hash into this key or persist in the manifest | Secret rotation must not reveal or silently redefine semantic identity |

Every extension leaf is included unless its versioned capability/compiler
profile explicitly certifies it as transport-only. Adapters cannot dynamically
declare a request field ignorable. Authorization, residency, route identity,
required provider-state availability, and current cache policy are checked
before lookup; excluding a per-call control field does not allow a cache hit to
authorize work. Cross-provider equivalence is deferred until a concrete pair
can meet a superseding ADR's evidence and negative-test requirements.

An HMAC, rather than a raw content hash, prevents offline confirmation of
sensitive prompt content from leaked database keys. The canonical encoder is
shared with request-conflict hashing and has golden fixtures in Go and OCaml.
Changing semantic normalization, route identity, or a provider compiler
requires a cache epoch bump. Old entries may coexist until retention removes
them.

Cloud cache lookup uses a scoped keyed identity that includes operation kind,
resolved route, semantic fingerprint and request index. Metadata and digests bind
published successes to their origin checkpoint and response artifact. Encrypted
artifacts contain the canonical content; lookup does not scan or index prompt
JSON. See [cloud response caching](../reference/cloud-response-cache.md).

The entry stores a normalized response template, not provider SDK bytes. It
includes output, usage provenance, route facts, portable checkpoint delta,
compaction artifact, and only provider state declared immutable and fork-safe
by the adapter. Required provider state that cannot safely be replayed makes the
operation ineligible for worker caching. Optional unsafe state is omitted with a
recorded diagnostic.

### Three different caches

Do not merge these concepts:

| Mechanism | Purpose | Identity |
| --- | --- | --- |
| Operation replay | Make one logical operation idempotent across Temporal retries | Tenant scope plus operation key and request digest |
| Worker exact-response cache | Reuse output across distinct opted-in operations | Semantic fingerprint plus variant |
| Provider prompt/context cache | Reduce provider input cost/latency while still producing a new output | Provider-specific prefix identity and affinity |

An operation replay is always allowed because the caller already requested that
logical call. Exact-response cache reuse requires **max_age_seconds**. Provider
prompt caching does not imply identical output and does not count as a worker
cache hit.

## Provider cache affinity

Each successful checkpoint stores a safe **ProviderCacheAffinity**:

- provider, endpoint, account, region, API family, and compatible model lineage;
- provider cache key/epoch or provider conversation reference digest;
- whether provider usage observed cache writes or reads;
- last successful use and known expiry; and
- hard-pinned versus soft-preferred semantics.

Routing applies constraints in this order:

1. tenant authorization, residency, endpoint enablement, and credential scope;
2. requested model, service class, capabilities, context, and extensions;
3. health, deadline, price, and budget eligibility;
4. required provider-state hard pin;
5. exact soft-affinity route first within the remaining requested-class
   candidates; and
6. configured deterministic order for the rest.

Affinity never bypasses authorization, service-class consent, health, budget,
residency, or required capability. If the preferred route is unavailable and
the canonical state is portable, routing may fail over and records the cache
affinity loss. If opaque state is required, it returns a pin error instead.

Forks from the same parent share the same provider prefix identity. A
provider-specific stable cache key is an HMAC of tenant scope, parent canonical
digest, provider cache epoch, and compatible model lineage. Raw tenant IDs,
prompt hashes, and provider credentials are not sent as cache keys.

Provider usage fields such as cache-read/write tokens refresh affinity and are
retained separately from the worker exact-response cache. An adapter capability
matrix states whether provider state and cache keys are fork-safe, portable,
expiring, or required.

## Compaction

Compaction reduces the context compiled for future provider calls; it does not
delete audit lineage or make a lossy summary equal to the original transcript.

### Dedicated Activity

**llm.compact.v1** accepts:

- operation key and context;
- one parent checkpoint;
- optional compaction-policy patch;
- optional summarizer logical model/service class; and
- optional exact-cache acceptance age using the same cache policy, with variant
  fixed to zero; and
- output reserve and retention controls.

It returns a child checkpoint of kind **compaction**, a compacted context
summary, provenance, usage, and an exact-or-unknown cost state. It does not
request a normal model answer and does not execute a tool. Reusing the original
parent remains valid, so callers can branch before or after compaction.

Compaction is an LLM call and therefore participates in opt-in exact-response
caching. Its fingerprint is domain-separated from Generate and includes parent
semantic state, retained-turn boundary, prompt/policy/summarizer equivalence,
compiler/capability versions, and every other summary-affecting control.
Compaction sampling is fixed to the compaction contract, so a positive cache
variant is invalid; zero remains a named cache slot even if the underlying
provider is not perfectly deterministic. On a hit the worker creates a new
compaction child with cache provenance and exact zero cost. It never returns the
origin checkpoint as the current operation's child.

### Automatic trigger

Generate evaluates compaction after operation replay and exact-cache lookup.
A cache hit needs no compaction or provider call. On a miss, the worker
materializes projected input and triggers before dispatch when any configured
limit is crossed:

- provider context tokens minus reserved output/reasoning capacity;
- canonical item/token count;
- un-compacted lineage depth;
- stored materialized bytes; or
- provider-native continuation expiry/limit.

Temporal payload size is not the compaction trigger because the delta payload is
already a delta. Token estimates are conservative and model/version specific.
The policy has hysteresis: compact to a lower target than the trigger so each
new turn does not compact again.

The generation workflow's automatic compaction uses unrestricted-age summary
caching with sample index zero. Explicit Compact requests can choose another
non-negative sample index. Final-answer freshness and sample indexes do not
change automatic summary identity; source content and policy versions do.

### Generic worker compaction

The generic path is a durable sub-operation with a deterministic key derived
from the Generate operation and compaction policy. It:

1. selects a complete prefix ending before the configured recent-turn window;
2. never splits an unmatched tool call/result pair or a provider-state unit;
3. preserves instructions, tool definitions, settings, schemas, durable facts,
   open tasks, citations, and recent turns outside the lossy summary;
4. constructs an internal compaction request with the application's tools
   absent, tool choice forced to none, and the application's structured-output
   format absent;
5. invokes the configured summarizer through normal routing, budget, status,
   resumable-operation, and cost accounting;
6. accepts only bounded plain-text compaction output and records
   prompt/model/policy versions;
7. writes a compaction checkpoint without changing the stored application tool
   or output settings; and
8. compiles the requested Generate turn from that child with those application
   settings restored.

If the worker crashes after compaction but before generation, retry reuses the
completed sub-operation and child checkpoint. It never pays for the same
compaction twice. Compaction and generation have distinct operation/cost rows
and budget reservations, while the parent Generate operation records their
relationship.

The default generic prompt is versioned repository data with contract fixtures.
Applications may select an allow-listed policy version but may not inject an
unbounded prompt through a cache policy field.

Compaction isolation is a security and correctness invariant. The summarizer
cannot call an application tool, emit a tool call, or be constrained by the
application's final-answer JSON schema. A provider response that contains a
tool call or structured-output artifact during compaction is invalid and never
becomes a checkpoint.

### Provider-native compaction

Adapters may use a native provider feature only when the capability catalog
defines its request, returned artifact, reuse, fork, expiry, usage, and cost
semantics. The adapter must also prove that the compaction engine cannot invoke
application tools and is not governed by the application's structured-output
format. If a provider cannot separate those settings from compaction, the route
uses generic worker compaction. The worker always stores enough canonical state
and provenance to detect a lost native artifact and either replay portably or
return a hard pin.

Current provider documentation shows materially different mechanisms:

- OpenAI exposes a separate Responses
  [compact endpoint](https://developers.openai.com/api/reference/resources/responses/methods/compact).
- Anthropic documents server-side
  [context compaction](https://platform.claude.com/docs/en/build-with-claude/compaction)
  as a beta capability requiring the `compact-2026-01-12` header, and requires
  returned compaction blocks to be supplied on later requests.
- Amazon Bedrock documents Claude
  [server-side compaction](https://docs.aws.amazon.com/bedrock/latest/userguide/claude-messages-compaction.html)
  with the same beta header through `InvokeModel`, not `Converse`, and notes
  that usage can span multiple iterations.

Provider contracts are versioned and reverified during implementation. Usage
from every native compaction iteration must be aggregated; relying only on a
top-level final usage value can understate spend.

## Retention and deletion

Retention treats graph integrity and cache reuse separately:

- checkpoint/blob retention must exceed the longest Workflow retry and
  business audit horizon;
- a parent cannot be deleted while a retained child needs it, unless the child
  has a verified self-contained snapshot;
- provider opaque state may have a shorter expiry than canonical state;
- exact-cache garbage collection considers **last_used_at**, not creation age;
- an entry a year old but used yesterday remains retained;
- default future cache cleanup may target 180 days without use, but the value is
  configuration, not hard-coded protocol;
- legal or tenant deletion traverses all descendants, cache entries, blobs,
  provider state, status references, and audit metadata under an authorized
  job.

Deletion is batched with **FOR UPDATE SKIP LOCKED**, rechecks eligibility inside
the deleting transaction, and removes the blob only after no retained reference
exists. Metrics expose eligible, deleted, skipped-in-use, and failed counts.

## Failure semantics

- Unknown/tampered/wrong-tenant parent: non-retryable safe not-found.
- Corrupt lineage/digest/blob: non-retryable state-corrupt plus alert.
- Settings patch or effective validation failure: non-retryable invalid
  argument before cache/budget/provider work.
- Cache opted in but cloud storage unavailable: retryable state unavailable;
  never bypass to a paid call.
- Concurrent identical misses: one fill owner; waiters resolve the completed
  entry or use their own deadline without dispatching duplicates.
- Compaction failure: original parent remains usable; automatic Generate fails
  with the sub-operation's typed error.
- Preferred provider cache route unhealthy: portable failover or hard-pin
  failure, never unauthorized routing.

## Acceptance properties

Implementation is incomplete until tests prove:

- N concurrent children from one parent all retain the same parent and distinct
  outputs;
- materializing a snapshot equals replaying every ancestor byte-for-byte;
- omitted settings encode no inherited values in the Activity payload;
- set, clear, and omitted remain distinct through Go and OCaml round trips;
- cache freshness uses completion time while cleanup uses last use;
- temperature zero plus variant greater than zero is rejected after inheritance;
- cache hits create new child checkpoints, charge zero, and increment usage once
  despite Activity retries;
- prompt-cache affinity wins only among otherwise eligible routes;
- a crash between compaction and generation does not repeat compaction;
- compaction requests contain no application tools or structured-output
  setting, while the following Generate restores both exactly;
- compaction never separates a tool call from its result; and
- no test Activity input/output grows with total ancestor transcript size.

Temporal Cloud currently documents payload and Workflow-history limits and
recommends external storage for large payloads; implementation must reverify
the current limits rather than encode them as protocol constants:
[Temporal Cloud limits](https://docs.temporal.io/cloud/limits).
