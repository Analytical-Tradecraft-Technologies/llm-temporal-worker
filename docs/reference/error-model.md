# Error Model

## Principles

Errors must answer four independent questions:

1. What failed?
2. At which phase?
3. Could the provider have accepted a billable request?
4. Is an automatic retry safe and useful within the current deadline?

HTTP status alone cannot answer these questions.

## Common type

```go
type Error struct {
	Code          Code
	Phase         Phase
	Dispatch      DispatchCertainty
	Retry         RetryDisposition
	RetryAfter    time.Duration
	OperationID   string
	Provider      ProviderFacts
	SafeMessage   string
	SafeDetails   map[string]string
	Cause         error
}

type DispatchCertainty string

const (
	NotDispatched DispatchCertainty = "not_dispatched"
	Rejected      DispatchCertainty = "rejected"
	Accepted      DispatchCertainty = "accepted"
	Ambiguous     DispatchCertainty = "ambiguous"
)
```

`Error()` returns the safe message. `Unwrap()` permits local diagnosis, but the
wrapped provider body is never serialized into Temporal details or logs.

Phases are `decode`, `normalize`, `state_load`, `plan`, `price`,
`admission`, `compile`, `dispatch`, `stream`, `lift`, `finalize`, and
`continuation_write`.

## Stable codes

| Code | Meaning | Default retry |
| --- | --- | --- |
| `invalid_argument` | malformed or internally inconsistent request | never |
| `unsupported_capability` | no strict semantic conversion: an otherwise eligible route declined the request | never without request/config change |
| `no_route` | no configured eligible candidate, or every candidate was passed over for context, adapter, price or budget-policy reasons | never without config/health change |
| `authentication` | invalid provider/store credentials | never within same snapshot |
| `permission_denied` | account/model/region denied | never within same snapshot |
| `budget_denied` | matching limit would be exceeded | at calculated time when horizon permits |
| `operation_conflict` | operation key reused with different digest | never |
| `ambiguous_dispatch` | provider may have accepted request | never automatically |
| `provider_rate_limited` | definite provider rate/resource rejection | after provider hint |
| `provider_unavailable` | explicit egress-policy denial or guarded endpoint failure before a writable connection | bounded retry/fallback |
| `provider_invalid_response` | malformed or semantically invalid output | route policy dependent, never if acceptance/cost ambiguity remains |
| `deadline_exceeded` | caller or bounded operation deadline | only if guarded proof establishes not dispatched |
| `canceled` | caller cancellation | caller controlled; never rerouted once classified |
| `state_unavailable` | Redis/blob dependency unavailable | retryable, fail closed |
| `state_corrupt` | MAC/digest/schema/invariant failure | never; alert |
| `configuration` | invalid or expired compiled snapshot | never within same snapshot |
| `internal` | invariant or unexpected implementation failure | only if ledger proves safe |

Adding a code is a versioned compatibility change. Provider-specific codes are
retained in `ProviderFacts.Code`, not added to the public enum for every API.

## Dispatch observation

Transport instrumentation marks:

- `not_dispatched` only when the transport/adapter proves no writable provider
  connection was acquired (or another affirmative no-dispatch fact exists);
- `rejected` only from affirmative provider/transport evidence that no billable
  request was accepted;
- `accepted` when response headers/body/job ID prove acceptance;
- `ambiguous` when bytes may have been written and no proof resolves outcome.

Connection establishment alone does not mean dispatched. A timeout after a write
is ambiguous. A provider 429 is rejected/uncharged only when that provider
contract/profile verifies it; otherwise cost handling remains conservative.

## Retry disposition

`RetryDisposition` is one of `never`, `same_operation`,
`next_route`, or `after`. It is derived after the ledger mutation:

- `same_operation` means a Temporal retry can re-enter idempotently;
- `next_route` means this Activity attempt may use the next planned candidate;
- `after` includes a conservative retry time;
- `never` covers ambiguity and caller/config errors.

The caller deadline, maximum attempt count, route policy, and service-class
authorization may make a normally transient error terminal.

## Provider facts

Safe provider facts are endpoint/profile ID, HTTP/RPC status, provider error
code, request/generation ID, retry-after, reported usage/cost presence, and
actual tier value. Provider response bodies and headers are deny-listed by
default; only profile-reviewed headers enter facts.

## Aggregation

When multiple candidates fail safely, the returned error contains:

- one primary code selected by deterministic precedence;
- a bounded list of per-attempt safe summaries;
- the plan digest and last operation ID;
- whether all requested-class routes were exhausted;
- whether an explicit fallback class was attempted.

Authentication, invalid request, ambiguity, and state corruption stop
aggregation immediately. Error aggregation never concatenates raw provider
messages.

## Testing

Every adapter fixture asserts code, phase, dispatch certainty, retry
disposition, cost treatment, Temporal mapping, safe serialization, and absence
of prompt/secret/provider-body leakage.
