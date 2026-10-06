package cloudstate

import "github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/cache"

// CacheFingerprint uses a namespace-separated key without exposing storage
// secrets to the activity runtime. It is pure and performs no storage access.
func (r *Repository) CacheFingerprint(input cache.Input) (cache.Fingerprint, error) {
	if r == nil || len(r.secret) != 32 {
		return cache.Fingerprint{}, ErrInvalid
	}
	key := derive(r.secret, "response-cache-key", []byte(r.namespace))
	defer clear(key)
	return cache.Compute(key, input)
}
