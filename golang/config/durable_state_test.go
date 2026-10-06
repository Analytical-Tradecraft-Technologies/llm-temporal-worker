package config

import (
	"strings"
	"testing"
)

func TestDurableStateRequiresCloudRequests(t *testing.T) {
	state := StateConfig{
		Kind:                       StateKindDurable,
		OperationTerminalRetention: 24,
		AmbiguousRetention:         48,
		ContinuationRetention:      24,
		ReservationLease:           1,
		Redis:                      RedisConfig{KeyPrefix: "llmtw", Addresses: []string{"redis:6379"}, Username: SecretRef{Kind: SecretEnv, Name: "REDIS_USER"}, Password: SecretRef{Kind: SecretEnv, Name: "REDIS_PASSWORD"}},
	}
	if err := state.validate("development"); err == nil {
		t.Fatal("durable state without cloud request stores was accepted")
	}
}

func TestMemoryStateUsesNoExternalStateConfiguration(t *testing.T) {
	state := StateConfig{
		Kind:                       StateKindMemory,
		OperationTerminalRetention: 24,
		AmbiguousRetention:         48,
		ContinuationRetention:      24,
		ReservationLease:           1,
	}
	if err := state.validate("development"); err != nil {
		t.Fatalf("memory state with no external dependencies rejected: %v", err)
	}
}

func TestMemoryStateIsDevelopmentOnly(t *testing.T) {
	state := StateConfig{
		Kind:                       StateKindMemory,
		OperationTerminalRetention: 24,
		AmbiguousRetention:         48,
		ContinuationRetention:      24,
		ReservationLease:           1,
	}
	if err := state.validate("production"); err == nil {
		t.Fatal("memory state was accepted in production")
	}
}

func TestMemoryStateRequiresMemoryBlobStore(t *testing.T) {
	if err := validateStateBlobComposition(StateKindMemory, "file"); err == nil || !strings.Contains(err.Error(), "blob_store.kind must be memory") {
		t.Fatalf("memory state/blob mismatch error = %v", err)
	}
}

func TestMemoryBlobStoreRequiresMemoryState(t *testing.T) {
	if err := validateStateBlobComposition(StateKindRedis, "memory"); err == nil || !strings.Contains(err.Error(), "blob_store.kind memory requires state.kind memory") {
		t.Fatalf("memory blob/state mismatch error = %v", err)
	}
}

func TestRedisOnlyStateIsDevelopmentOnly(t *testing.T) {
	state := StateConfig{
		Kind:                       StateKindRedis,
		OperationTerminalRetention: 24,
		AmbiguousRetention:         48,
		ContinuationRetention:      24,
		ReservationLease:           1,
		Redis:                      RedisConfig{KeyPrefix: "llmtw", Addresses: []string{"redis:6379"}, Username: SecretRef{Kind: SecretEnv, Name: "REDIS_USER"}, Password: SecretRef{Kind: SecretEnv, Name: "REDIS_PASSWORD"}, AdmissionHashTag: "admission", AdmissionMode: "function", FunctionLibrary: "llmtw_admission_bba60419c1306b1f", AdmissionVersion: "admission_bba60419c1306b1f", AdmissionDigest: "0000000000000000000000000000000000000000000000000000000000000000", MaxConnections: 1, DialTimeout: 1, OperationTimeout: 1, RequiredPersistence: "aof_and_rdb"},
	}
	if err := state.validate("production"); err == nil {
		t.Fatal("Redis-only state was accepted for production")
	}
}
