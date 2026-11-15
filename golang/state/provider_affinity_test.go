package state

import (
	"testing"
	"time"
)

// testAffinity is a complete rank-0 provider cache affinity last used at
// lastSuccess.
func testAffinity(lastSuccess time.Time) ProviderCacheAffinity {
	return ProviderCacheAffinity{
		Rank: 0, Provider: "openai", RouteID: "route", EndpointID: "endpoint",
		EndpointAccountHMAC: [32]byte{1}, Region: "us-east-1", EndpointFamily: "responses",
		ModelLineage: "lineage", RouteModelRevision: "revision", CacheEpoch: "epoch",
		LastSuccessAt: lastSuccess,
	}
}

func TestProviderCacheAffinityValidationKeepsExpiredHistoryAuditable(t *testing.T) {
	now := time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC)
	expired := now.Add(-time.Minute)
	affinity := testAffinity(now.Add(-2 * time.Minute))
	affinity.ExpiresAt = &expired
	if err := affinity.Validate(now); err != nil {
		t.Fatal(err)
	}
	if affinity.Active(now) {
		t.Fatal("expired affinity reported active")
	}
}

func TestProviderCacheAffinityValidationRequiresHMACAndSeparateUsage(t *testing.T) {
	now := time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC)
	base := testAffinity(now)
	for accepted, mutate := range map[string]func(*ProviderCacheAffinity){
		"affinity without endpoint-account HMAC accepted":     func(value *ProviderCacheAffinity) { value.EndpointAccountHMAC = [32]byte{} },
		"affinity with an empty provider cache HMAC accepted": func(value *ProviderCacheAffinity) { value.HasProviderCacheKey = true },
		"negative cache-write tokens accepted":                func(value *ProviderCacheAffinity) { value.ObservedCacheWriteTokens = -1 },
	} {
		invalid := base
		mutate(&invalid)
		if err := invalid.Validate(now); err == nil {
			t.Fatal(accepted)
		}
	}
}
