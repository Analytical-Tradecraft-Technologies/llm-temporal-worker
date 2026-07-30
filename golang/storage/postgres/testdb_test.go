package postgres

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Integration tests are enabled by the PostgreSQL service in CI. Local and
// offline test runs remain deterministic and skip when no address is set.
func TestPostgresIntegrationConfiguration(t *testing.T) {
	addr := os.Getenv("LLMTW_POSTGRES_ADDR")
	if addr == "" {
		t.Skip("LLMTW_POSTGRES_ADDR is not configured; set it for PostgreSQL integration tests")
	}
	ns, err := NewNamespace(
		valueOr("LLMTW_POSTGRES_DATABASE", "llm_worker"),
		valueOr("LLMTW_POSTGRES_SCHEMA", "llm_worker"),
		os.Getenv("LLMTW_POSTGRES_TABLE_PREFIX"),
	)
	if err != nil {
		t.Fatal(err)
	}
	password := valueOr("LLMTW_POSTGRES_PASSWORD", "llmtw")
	user := valueOr("LLMTW_POSTGRES_USER", "llmtw")
	dsn := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", user, password, addr, ns.Database)
	poolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	poolConfig.MaxConns = 4
	poolConfig.MinConns = 1
	poolConfig.MaxConnLifetime = 2 * time.Minute
	pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := Install(ctx, pool, ns); err != nil {
		t.Fatalf("clean install: %v", err)
	}
	if err := Install(ctx, pool, ns); err != nil {
		t.Fatalf("idempotent install: %v", err)
	}
	if err := Verify(ctx, pool, ns); err != nil {
		t.Fatalf("contract verification: %v", err)
	}
	var tableCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = $1 AND c.relkind = 'r'`, ns.Schema).Scan(&tableCount); err != nil {
		t.Fatal(err)
	}
	if tableCount < 20 {
		t.Fatalf("schema contains %d tables, want at least 20", tableCount)
	}
}

func TestPostgresSharedSchemaLeavesSchemaAclAndForeignConstraintsUntouched(t *testing.T) {
	addr := os.Getenv("LLMTW_POSTGRES_ADDR")
	if addr == "" {
		t.Skip("LLMTW_POSTGRES_ADDR is not configured; set it for PostgreSQL integration tests")
	}
	ns, err := NewNamespace(
		valueOr("LLMTW_POSTGRES_DATABASE", "llm_worker"),
		fmt.Sprintf("shared_worker_%d", time.Now().UnixNano()),
		"worker_",
	)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := NewPool(context.Background(), PoolOptions{
		Namespace: ns, Addresses: []string{addr},
		Username:       valueOr("LLMTW_POSTGRES_USER", "llmtw"),
		Password:       valueOr("LLMTW_POSTGRES_PASSWORD", "llmtw"),
		MaxConnections: 4, MinConnections: 1,
		DialTimeout: 5 * time.Second, StatementTimeout: 5 * time.Second,
		LockTimeout: time.Second, IdleTxTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	schema := ns.SchemaIdentifier().Sanitize()
	if _, err := pool.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer pool.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
	var before string
	if err := pool.QueryRow(ctx, "SELECT COALESCE(nspacl::text, '') FROM pg_namespace WHERE nspname = $1", ns.Schema).Scan(&before); err != nil {
		t.Fatal(err)
	}
	// A relation outside the worker prefix models a Temporal-owned object in
	// an explicitly shared schema. Install must not rename or grant/revoke it.
	if _, err := pool.Exec(ctx, "CREATE TABLE "+schema+".temporal_sentinel (id integer PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	var sentinelConstraint string
	if err := pool.QueryRow(ctx, "SELECT conname FROM pg_constraint c JOIN pg_class r ON r.oid = c.conrelid JOIN pg_namespace n ON n.oid = r.relnamespace WHERE n.nspname = $1 AND r.relname = 'temporal_sentinel'", ns.Schema).Scan(&sentinelConstraint); err != nil {
		t.Fatal(err)
	}
	if err := Install(ctx, pool, ns); err != nil {
		t.Fatalf("shared-schema install: %v", err)
	}
	var after, gotSentinelConstraint string
	if err := pool.QueryRow(ctx, "SELECT COALESCE(nspacl::text, '') FROM pg_namespace WHERE nspname = $1", ns.Schema).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Fatalf("shared schema ACL changed from %q to %q", before, after)
	}
	if err := pool.QueryRow(ctx, "SELECT conname FROM pg_constraint c JOIN pg_class r ON r.oid = c.conrelid JOIN pg_namespace n ON n.oid = r.relnamespace WHERE n.nspname = $1 AND r.relname = 'temporal_sentinel'", ns.Schema).Scan(&gotSentinelConstraint); err != nil {
		t.Fatal(err)
	}
	if gotSentinelConstraint != sentinelConstraint {
		t.Fatalf("foreign schema constraint was renamed from %q to %q", sentinelConstraint, gotSentinelConstraint)
	}
}

func TestPostgresInstallRejectsWorkerRelationCollision(t *testing.T) {
	addr := os.Getenv("LLMTW_POSTGRES_ADDR")
	if addr == "" {
		t.Skip("LLMTW_POSTGRES_ADDR is not configured; set it for PostgreSQL integration tests")
	}
	ns, err := NewNamespace(
		valueOr("LLMTW_POSTGRES_DATABASE", "llm_worker"),
		fmt.Sprintf("collision_worker_%d", time.Now().UnixNano()),
		"worker_",
	)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := NewPool(context.Background(), PoolOptions{
		Namespace: ns, Addresses: []string{addr},
		Username: valueOr("LLMTW_POSTGRES_USER", "llmtw"), Password: valueOr("LLMTW_POSTGRES_PASSWORD", "llmtw"),
		MaxConnections: 4, MinConnections: 1, DialTimeout: 5 * time.Second,
		StatementTimeout: 5 * time.Second, LockTimeout: time.Second, IdleTxTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	schema := ns.SchemaIdentifier().Sanitize()
	if _, err := pool.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer pool.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
	if _, err := pool.Exec(ctx, "CREATE TABLE "+schema+".worker_operations (id integer)"); err != nil {
		t.Fatal(err)
	}
	if err := Install(ctx, pool, ns); err == nil || !strings.Contains(err.Error(), "worker_operations") {
		t.Fatalf("Install collision error = %v, want worker_operations collision", err)
	}
}

func valueOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
