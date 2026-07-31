package postgres

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// BuildPoolConfig validates and compiles one immutable namespace connection
// configuration. Identifiers are used only as database metadata; SQL always
// comes from Namespace and never from Activity input.
func BuildPoolConfig(options PoolOptions) (*pgxpool.Config, error) {
	if err := options.validate(); err != nil {
		return nil, err
	}
	tlsConfig, err := options.TLS.tlsConfig()
	if err != nil {
		return nil, err
	}
	firstHost, firstPort, err := net.SplitHostPort(options.Addresses[0])
	if err != nil {
		return nil, fmt.Errorf("parse PostgreSQL address: %w", err)
	}
	port, err := strconv.ParseUint(firstPort, 10, 16)
	if err != nil || port == 0 {
		return nil, fmt.Errorf("PostgreSQL port is invalid")
	}
	for _, address := range options.Addresses[1:] {
		host, addressPort, splitErr := net.SplitHostPort(address)
		if splitErr != nil {
			return nil, fmt.Errorf("parse PostgreSQL address: %w", splitErr)
		}
		if addressPort != firstPort {
			return nil, fmt.Errorf("PostgreSQL pool addresses must use one port")
		}
		if host == "" {
			return nil, fmt.Errorf("PostgreSQL host is empty")
		}
	}
	if firstHost == "" {
		return nil, fmt.Errorf("PostgreSQL host is empty")
	}

	// Parse a minimal URI to obtain pgx defaults, then replace the connection
	// fields. Password is assigned directly, avoiding URI escaping and keeping
	// it out of any parse/validation error.
	config, err := pgxpool.ParseConfig("postgres://localhost/" + options.Namespace.Database + "?sslmode=disable")
	if err != nil {
		return nil, fmt.Errorf("construct PostgreSQL pool config: %w", err)
	}
	config.ConnConfig.Host = firstHost
	config.ConnConfig.Port = uint16(port)
	config.ConnConfig.Database = options.Namespace.Database
	config.ConnConfig.User = options.Username
	config.ConnConfig.Password = options.Password
	config.ConnConfig.TLSConfig = tlsConfig
	config.ConnConfig.ConnectTimeout = options.DialTimeout
	config.MaxConns = options.MaxConnections
	config.MinConns = options.MinConnections
	config.MaxConnLifetime = 30 * time.Minute
	config.MaxConnIdleTime = 5 * time.Minute
	config.HealthCheckPeriod = 30 * time.Second
	for _, address := range options.Addresses[1:] {
		host, _, _ := net.SplitHostPort(address)
		config.ConnConfig.Fallbacks = append(config.ConnConfig.Fallbacks, &pgconn.FallbackConfig{
			Host: host, Port: uint16(port), TLSConfig: tlsConfig,
		})
	}
	if options.ApplicationName != "" {
		config.ConnConfig.RuntimeParams["application_name"] = options.ApplicationName
	}
	config.AfterConnect = func(ctx context.Context, connection *pgx.Conn) error {
		if _, err := connection.Exec(ctx, "SET TIME ZONE 'UTC'"); err != nil {
			return redactPostgresError(fmt.Errorf("set UTC session: %w", err))
		}
		for name, value := range map[string]time.Duration{
			"statement_timeout":                   options.StatementTimeout,
			"lock_timeout":                        options.LockTimeout,
			"idle_in_transaction_session_timeout": options.IdleTxTimeout,
		} {
			if _, err := connection.Exec(ctx, "SELECT set_config($1, $2, false)", name, durationMilliseconds(value)); err != nil {
				return redactPostgresError(fmt.Errorf("set %s: %w", name, err))
			}
		}
		if _, err := connection.Exec(ctx, "SET synchronous_commit = 'on'"); err != nil {
			return redactPostgresError(fmt.Errorf("set synchronous commit: %w", err))
		}
		return nil
	}
	return config, nil
}

func NewPool(ctx context.Context, options PoolOptions) (*pgxpool.Pool, error) {
	if ctx == nil {
		return nil, fmt.Errorf("PostgreSQL pool context is nil")
	}
	config, err := BuildPoolConfig(options)
	if err != nil {
		return nil, err
	}
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, redactPostgresError(err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, redactPostgresError(fmt.Errorf("ping PostgreSQL: %w", err))
	}
	return pool, nil
}

// Health verifies the configured physical database and UTC session. It is a
// read-only readiness check; schema creation belongs exclusively to Install.
func Health(ctx context.Context, pool *pgxpool.Pool, namespace Namespace) error {
	if ctx == nil {
		return fmt.Errorf("PostgreSQL health context is nil")
	}
	if pool == nil {
		return fmt.Errorf("PostgreSQL pool is nil")
	}
	if err := namespace.Validate(); err != nil {
		return err
	}
	if err := verifyDatabase(ctx, pool, namespace); err != nil {
		return err
	}
	var timezone string
	if err := pool.QueryRow(ctx, "SELECT current_setting('TimeZone')").Scan(&timezone); err != nil {
		return redactPostgresError(fmt.Errorf("check PostgreSQL timezone: %w", err))
	}
	if !strings.EqualFold(timezone, "UTC") {
		return fmt.Errorf("PostgreSQL session timezone is %q, expected UTC", timezone)
	}
	return nil
}
