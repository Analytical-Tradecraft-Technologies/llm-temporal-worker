# Redis operational throttles

`storage/redis` exposes a small, atomic throttle lease alongside the monetary
admission store. It is intended for request, token, and in-flight concurrency
limits that must be shared by worker replicas; it is not a second accounting
ledger.

```go
limits := []redisstore.ThrottleLimit{
    {Kind: redisstore.ThrottleRequests, Scope: tenant, Amount: 1, Limit: 100, Window: time.Minute},
    {Kind: redisstore.ThrottleTokens, Scope: tenant, Amount: estimate, Limit: 100_000, Window: time.Minute},
}
lease, err := throttles.Acquire(ctx, reservationID, limits)
if errors.Is(err, redisstore.ErrThrottleDenied) {
    // Queue/retry with the caller's policy; no provider request was sent.
}
// Release only after the operation is known not to need the lease.
_ = throttles.Release(ctx, lease.Reservation)
```

All limits in one acquire are checked and incremented in one Redis Function
invocation. A retry with the same reservation ID and the same canonical limit
digest returns the existing lease; a different digest returns a conflict. The
caller must resolve an ambiguous transport result with `Lookup` before trying
again. Release is idempotent and never blindly retries a failed mutation.

Reservation and counter keys use the configured `state.redis.key_prefix` and
`admission_hash_tag`; scopes and reservation IDs are HMAC-derived and are not
written as Redis key components. The throttle Function is versioned as
`llmtw_throttle_v1/throttle_v1`, with an explicit SHA-256 source digest. Deploy
the immutable Function (or its explicitly configured preloaded Lua fallback)
before enabling workers. Function loading/replacement is deliberately outside
the request path.

Counters are fixed windows. A counter's TTL, equal to the largest configured
window in the acquire, is set once, when the first acquire creates it; later
acquires do not extend it, so the counter resets at the end of each window. A
lease records the end of the window each counter was charged in. Release
returns capacity only while that window is still current: once it ends, the
counter has already reset, so the capacity is free and a newer window is not
decremented. A lease whose record expired returns its capacity at the end of
its window the same way. Leases have the same TTL. Redis `noeviction` and the configured persistence policy remain
startup/readiness requirements. Missing, malformed, over-limit, or ambiguous
state fails closed; Redis does not fall back to PostgreSQL for a normal throttle
decision. These operational leases do not imply a financial journal entry.
