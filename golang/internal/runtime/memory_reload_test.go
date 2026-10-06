package runtime

import (
	"context"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/engine"
	"github.com/mfow/llm-temporal-worker/golang/internal/secrets"
)

// Memory mode's state belongs to the process, not to one snapshot: a reload
// keeps completed-operation deduplication, results and continuations, and only
// a continuation key rotation rebuilds the continuation store (#802).
func TestBuildMemoryKeepsProcessStateAcrossReloads(t *testing.T) {
	now := time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC)
	secret := "01234567890123456789012345678901"
	factory := &ProductionEngineFactory{options: ProductionFactoryOptions{
		Clock: nowFunc(now),
		Resolver: secrets.ResolverFunc(func(context.Context, config.SecretRef) ([]byte, error) {
			return []byte(secret), nil
		}),
	}}
	value := config.Config{
		State:        config.StateConfig{Kind: config.StateKindMemory, ContinuationRetention: config.Duration(time.Hour), ReservationLease: config.Duration(time.Minute)},
		BlobStore:    config.BlobStoreConfig{Kind: "memory", InlineBytes: 256},
		Limits:       config.LimitsConfig{RequestBytes: 1024, ContinuationDepth: 4, RouteAttempts: 1, TokenEstimateSafetyRatio: "1", MaxOutputTokens: 16, MaxBudgetBucketsPerWindow: 100},
		Continuation: config.ContinuationConfig{HandleKeys: []config.HandleKey{{ID: "key-2026-07", Primary: true, Secret: config.SecretRef{Kind: config.SecretEnv, Name: "CONTINUATION_KEY"}}}},
	}
	build := func() {
		t.Helper()
		if _, _, err := factory.buildMemory(context.Background(), value, engine.StaticSnapshot{}, nil, nil, [32]byte{}); err != nil {
			t.Fatal(err)
		}
	}
	build()
	admissionStore, blobs, continuations := factory.memory.admission, factory.memory.blobs, factory.memory.continuations
	begin := admission.BeginRequest{ID: "op", ScopeKey: "tenant/op", RequestDigest: admission.Digest([]byte("request")), LeaseUntil: now.Add(time.Minute), ExpiresAt: now.Add(time.Hour)}
	if _, err := admissionStore.Begin(context.Background(), begin); err != nil {
		t.Fatal(err)
	}
	build()
	if factory.memory.admission != admissionStore || factory.memory.blobs != blobs || factory.memory.continuations != continuations {
		t.Fatal("an identical reload replaced memory-mode state")
	}
	if existing, err := factory.memory.admission.Begin(context.Background(), begin); err != nil || !existing.Existing {
		t.Fatalf("operation lost across reload: %+v %v", existing, err)
	}
	secret = "abcdefghijabcdefghijabcdefghijab"
	build()
	if factory.memory.admission != admissionStore || factory.memory.blobs != blobs {
		t.Fatal("a key rotation replaced admission or result state")
	}
	if factory.memory.continuations == continuations {
		t.Fatal("a continuation key rotation kept the store built for the old key")
	}
}
