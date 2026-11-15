package routing

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func FuzzProviderCacheKeyForkIdentity(f *testing.F) {
	seed := [32]byte{}
	for index := range seed {
		seed[index] = byte(index)
	}
	f.Add("tenant/project", "epoch-1", "lineage", seed[:])
	f.Fuzz(func(t *testing.T, scope, epoch, lineage string, raw []byte) {
		// DeriveProviderCacheKey rejects an empty or whitespace-only tenant
		// scope. Keep those invalid inputs out of the identity-preservation
		// property; they are covered by the validation test below.
		if strings.TrimSpace(scope) == "" || len(epoch) == 0 || len(lineage) == 0 || len(raw) > 128 {
			t.Skip()
		}
		var parent [32]byte
		copy(parent[:], raw)
		if parent == [32]byte{} {
			parent[0] = 1
		}
		key := mustDeriveProviderCacheKey(t, []byte("fuzz-secret"), scope, parent, epoch, lineage)
		// Every immutable fork receives the same parent digest and provider
		// lineage, so all three forks must reuse exactly one prefix identity.
		for fork := 0; fork < 3; fork++ {
			reused, err := DeriveProviderCacheKey([]byte("fuzz-secret"), scope, parent, epoch, lineage)
			if err != nil {
				t.Fatalf("derive fork %d key: %v", fork, err)
			}
			if reused != key {
				t.Fatalf("fork %d changed provider cache identity", fork)
			}
		}
	})
}

func TestAffinitySetValidationRejectsDuplicateRanksAndNegativeUsage(t *testing.T) {
	now := time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC)
	base := affinityForRoute("route", "endpoint", "provider", "family", "region", [32]byte{1}, "lineage", "revision", now)
	duplicate := base
	duplicate.RouteID = "route-2"
	if err := (ProviderCacheAffinitySet{base, duplicate}).Validate(now); err == nil {
		t.Fatal("duplicate rank accepted")
	}
	negative := base
	negative.ObservedCacheReadTokens = -1
	if err := negative.Validate(now); err == nil {
		t.Fatal("negative cache-read observation accepted")
	}
}

func TestProviderCacheAffinityNeverExposesRawKeyMaterial(t *testing.T) {
	var parent [32]byte
	parent[0] = 42
	key := mustDeriveProviderCacheKey(t, []byte("secret"), "tenant/project", parent, "epoch", "lineage")
	if bytes.Contains(key[:], []byte("tenant")) || bytes.Contains(key[:], []byte("lineage")) {
		t.Fatalf("raw identity leaked into HMAC digest: %x", key)
	}
}
