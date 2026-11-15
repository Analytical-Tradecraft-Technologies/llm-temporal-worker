package config

import (
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"testing"
)

// retainedState returns a kind state with valid retention and lease settings.
func retainedState(kind string) StateConfig {
	return StateConfig{Kind: kind, OperationTerminalRetention: 24, AmbiguousRetention: 48, ContinuationRetention: 24, ReservationLease: 1}
}

func TestDurableStateRequiresCloudRequests(t *testing.T) {
	state := retainedState(StateKindDurable)
	state.Redis = RedisConfig{KeyPrefix: "llmtw", Addresses: []string{"redis:6379"}, Username: SecretRef{Kind: SecretEnv, Name: "REDIS_USER"}, Password: SecretRef{Kind: SecretEnv, Name: "REDIS_PASSWORD"}}
	if err := state.validate("development"); err == nil {
		t.Fatal("durable state without cloud request stores was accepted")
	}
}

func TestMemoryStateUsesNoExternalStateConfiguration(t *testing.T) {
	state := retainedState(StateKindMemory)
	if err := state.validate("development"); err != nil {
		t.Fatalf("memory state with no external dependencies rejected: %v", err)
	}
}

func TestMemoryStateIsDevelopmentOnly(t *testing.T) {
	state := retainedState(StateKindMemory)
	if err := state.validate("production"); err == nil {
		t.Fatal("memory state was accepted in production")
	}
}

func TestMemoryStateRequiresMemoryBlobStore(t *testing.T) {
	testassert.ErrorContains(t, validateStateBlobComposition(StateKindMemory, "file"), "blob_store.kind must be memory")
}

func TestMemoryBlobStoreRequiresMemoryState(t *testing.T) {
	testassert.ErrorContains(t, validateStateBlobComposition(StateKindRedis, "memory"), "blob_store.kind memory requires state.kind memory")
}

func TestRedisOnlyStateIsDevelopmentOnly(t *testing.T) {
	state := retainedState(StateKindRedis)
	state.Redis = RedisConfig{KeyPrefix: "llmtw", Addresses: []string{"redis:6379"}, Username: SecretRef{Kind: SecretEnv, Name: "REDIS_USER"}, Password: SecretRef{Kind: SecretEnv, Name: "REDIS_PASSWORD"}, AdmissionHashTag: "admission", AdmissionMode: "function", FunctionLibrary: "llmtw_admission_628f464157f0b550", AdmissionVersion: "admission_628f464157f0b550", AdmissionDigest: "0000000000000000000000000000000000000000000000000000000000000000", MaxConnections: 1, DialTimeout: 1, OperationTimeout: 1, RequiredPersistence: "aof_and_rdb"}
	if err := state.validate("production"); err == nil {
		t.Fatal("Redis-only state was accepted for production")
	}
}
