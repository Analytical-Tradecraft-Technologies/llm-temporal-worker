package postgres

import (
	"strings"
	"testing"
	"time"
)

func TestBuildPoolConfigSetsDurableSessionDefaults(t *testing.T) {
	options := PoolOptions{
		Namespace:        Namespace{Database: "llm_worker", Schema: "llm_worker"},
		Addresses:        []string{"db.internal:5432", "db-replica.internal:5432"},
		Username:         "worker",
		Password:         "test-secret-value",
		MaxConnections:   10,
		MinConnections:   2,
		DialTimeout:      2 * time.Second,
		StatementTimeout: 5 * time.Second,
		LockTimeout:      time.Second,
		IdleTxTimeout:    10 * time.Second,
		ApplicationName:  "llmtw-test",
	}
	config, err := BuildPoolConfig(options)
	if err != nil {
		t.Fatal(err)
	}
	if config.ConnConfig.Database != "llm_worker" || config.ConnConfig.User != "worker" || config.ConnConfig.Password != "test-secret-value" {
		t.Fatalf("unexpected pool identity: %#v", config.ConnConfig)
	}
	if config.ConnConfig.Host != "db.internal" || config.ConnConfig.Port != 5432 || len(config.ConnConfig.Fallbacks) != 1 || config.ConnConfig.Fallbacks[0].Host != "db-replica.internal" {
		t.Fatalf("unexpected pool hosts: %q:%d fallbacks=%#v", config.ConnConfig.Host, config.ConnConfig.Port, config.ConnConfig.Fallbacks)
	}
	if config.MaxConns != 10 || config.MinConns != 2 || config.AfterConnect == nil {
		t.Fatalf("unexpected pool limits/hooks: max=%d min=%d", config.MaxConns, config.MinConns)
	}
}

func TestBuildPoolConfigRequiresVerifiedTLSWhenEnabled(t *testing.T) {
	options := PoolOptions{
		Namespace:        Namespace{Database: "llm_worker", Schema: "llm_worker"},
		Addresses:        []string{"db.internal:5432"},
		Username:         "worker",
		MaxConnections:   1,
		MinConnections:   0,
		DialTimeout:      time.Second,
		StatementTimeout: time.Second,
		LockTimeout:      time.Second,
		IdleTxTimeout:    time.Second,
		TLS:              TLSOptions{Enabled: true, ServerName: "db.internal", CAFile: "/does/not/exist"},
	}
	_, err := BuildPoolConfig(options)
	if err == nil || !strings.Contains(err.Error(), "CA file") {
		t.Fatalf("invalid TLS config error = %v", err)
	}
	if strings.Contains(strings.ToLower(err.Error()), "secret-value") {
		t.Fatal("pool error leaked password")
	}
}
