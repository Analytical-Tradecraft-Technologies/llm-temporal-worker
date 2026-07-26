package config

import (
	"strings"
	"testing"
)

func TestDurableStateRequiresPostgresNamespaceAndCredentials(t *testing.T) {
	state := StateConfig{
		Kind:                       StateKindDurable,
		OperationTerminalRetention: 24,
		AmbiguousRetention:         48,
		ContinuationRetention:      24,
		ReservationLease:           1,
		Redis:                      RedisConfig{KeyPrefix: "llmtw", Addresses: []string{"redis:6379"}, Username: SecretRef{Kind: SecretEnv, Name: "REDIS_USER"}, Password: SecretRef{Kind: SecretEnv, Name: "REDIS_PASSWORD"}},
		Postgres:                   PostgresConfig{Database: "worker_db", Schema: "worker_state", MaxConnections: 1, DialTimeout: 1, StatementTimeout: 1, LockTimeout: 1, IdleTransactionTimeout: 1},
	}
	if err := state.validate("development"); err == nil {
		t.Fatal("durable state without PostgreSQL addresses/credentials was accepted")
	}
}

func TestMemoryStateUsesNoExternalStateConfiguration(t *testing.T) {
	state := StateConfig{
		Kind:                       StateKindMemory,
		OperationTerminalRetention: 24,
		AmbiguousRetention:         48,
		ContinuationRetention:      24,
		ReservationLease:           1,
		Postgres:                   PostgresConfig{Database: "worker_db", Schema: "worker_state", MaxConnections: 1, DialTimeout: 1, StatementTimeout: 1, LockTimeout: 1, IdleTransactionTimeout: 1},
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
		Redis:                      RedisConfig{KeyPrefix: "llmtw", Addresses: []string{"redis:6379"}, Username: SecretRef{Kind: SecretEnv, Name: "REDIS_USER"}, Password: SecretRef{Kind: SecretEnv, Name: "REDIS_PASSWORD"}, AdmissionHashTag: "admission", AdmissionMode: "function", FunctionLibrary: "llmtw_admission_v1", AdmissionVersion: "admission_v1", AdmissionDigest: "0000000000000000000000000000000000000000000000000000000000000000", MaxConnections: 1, DialTimeout: 1, OperationTimeout: 1, RequiredPersistence: "aof_and_rdb"},
	}
	if err := state.validate("production"); err == nil {
		t.Fatal("Redis-only state was accepted for production")
	}
}

func TestPostgresPoolBoundsAreValidated(t *testing.T) {
	base := StateConfig{
		Kind:                       StateKindDurable,
		OperationTerminalRetention: 24,
		AmbiguousRetention:         48,
		ContinuationRetention:      24,
		ReservationLease:           1,
		Redis:                      RedisConfig{KeyPrefix: "llmtw", Addresses: []string{"redis:6379"}, Username: SecretRef{Kind: SecretEnv, Name: "REDIS_USER"}, Password: SecretRef{Kind: SecretEnv, Name: "REDIS_PASSWORD"}, AdmissionHashTag: "admission", AdmissionMode: "function", FunctionLibrary: "llmtw_admission_v1", AdmissionVersion: "admission_v1", AdmissionDigest: "0000000000000000000000000000000000000000000000000000000000000000", MaxConnections: 1, DialTimeout: 1, OperationTimeout: 1, RequiredPersistence: "aof_and_rdb"},
		Postgres:                   PostgresConfig{Database: "worker_db", Schema: "worker_state", MaxConnections: 4, MinConnections: 2, DialTimeout: 1, StatementTimeout: 1, LockTimeout: 1, IdleTransactionTimeout: 1, Addresses: []string{"postgres:5432"}, Username: SecretRef{Kind: SecretEnv, Name: "PG_USER"}, Password: SecretRef{Kind: SecretEnv, Name: "PG_PASSWORD"}},
	}
	base.Postgres.MinConnections = 5
	if err := base.validate("development"); err == nil || !strings.Contains(err.Error(), "min_connections") {
		t.Fatalf("invalid PostgreSQL pool bounds error = %v", err)
	}
	base.Postgres.MinConnections = 2
	base.Postgres.IdleTransactionTimeout = 0
	if err := base.validate("development"); err == nil || !strings.Contains(err.Error(), "idle_transaction_timeout") {
		t.Fatalf("invalid PostgreSQL idle transaction timeout error = %v", err)
	}
}
