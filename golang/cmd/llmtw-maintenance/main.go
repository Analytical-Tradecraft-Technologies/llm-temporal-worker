// Command llmtw-maintenance runs one explicitly bounded maintenance pass.
//
// It is intentionally a separate binary from llm-temporal-worker. The worker
// process must never hold the maintenance PostgreSQL role, and this command
// therefore requires dedicated credentials through environment variables.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/storage/postgres"
)

const defaultConfigPath = "/etc/llmtw/config.yaml"

type commandOptions struct {
	ConfigPath  string
	Now         time.Time
	Limit       int
	Cache       time.Time
	Provider    time.Time
	Inventory   time.Time
	Queries     time.Time
	Operations  time.Time
	Budgets     time.Time
	Checkpoints time.Time
	MaxWindow   time.Duration
}

func main() {
	ctx, cancel := signalContext()
	defer cancel()
	if err := execute(ctx, os.Args[1:], os.Stdout, os.Stderr, os.LookupEnv); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// signalContext has no worker/Temporal lifecycle dependency. A maintenance
// invocation is one bounded operation and exits when it is cancelled.
func signalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
}

type lookupEnv func(string) (string, bool)

func execute(ctx context.Context, args []string, out, errOut io.Writer, lookup lookupEnv) error {
	if len(args) == 0 || args[0] != "retention-once" {
		return errors.New("usage: llmtw-maintenance retention-once [flags]")
	}
	options, err := parseOptions(args[1:], errOut)
	if err != nil {
		return err
	}
	data, err := os.ReadFile(options.ConfigPath)
	if err != nil {
		return fmt.Errorf("read configuration: %w", err)
	}
	value, err := config.Load(data)
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}
	if value.State.Kind != config.StateKindDurable {
		return errors.New("maintenance requires durable state")
	}
	username, err := requiredSecret(lookup, "LLMTW_MAINTENANCE_POSTGRES_USERNAME")
	if err != nil {
		return err
	}
	password, err := requiredSecret(lookup, "LLMTW_MAINTENANCE_POSTGRES_PASSWORD")
	if err != nil {
		return err
	}
	namespace, err := postgres.NewNamespace(value.State.Postgres.Database, value.State.Postgres.Schema, value.State.Postgres.TablePrefix)
	if err != nil {
		return fmt.Errorf("construct PostgreSQL namespace: %w", err)
	}
	poolOptions := postgres.PoolOptions{
		Namespace: namespace, Addresses: value.State.Postgres.Addresses,
		Username: username, Password: password,
		TLS:            postgres.TLSOptions{Enabled: value.State.Postgres.TLS.Enabled, ServerName: value.State.Postgres.TLS.ServerName, CAFile: value.State.Postgres.TLS.CAFile},
		MaxConnections: int32(value.State.Postgres.MaxConnections), MinConnections: int32(value.State.Postgres.MinConnections),
		DialTimeout: time.Duration(value.State.Postgres.DialTimeout), StatementTimeout: time.Duration(value.State.Postgres.StatementTimeout),
		LockTimeout: time.Duration(value.State.Postgres.LockTimeout), IdleTxTimeout: time.Duration(value.State.Postgres.IdleTransactionTimeout),
		ApplicationName: "llmtw-maintenance",
	}
	pool, err := postgres.NewPool(ctx, poolOptions)
	if err != nil {
		return fmt.Errorf("construct maintenance PostgreSQL pool: %w", err)
	}
	defer pool.Close()
	if err := postgres.Health(ctx, pool, namespace); err != nil {
		return fmt.Errorf("check maintenance PostgreSQL: %w", err)
	}
	result, err := (postgres.MaintenanceRepository{Pool: pool, Namespace: namespace}).RunRetentionBatch(ctx, postgres.RetentionBatchOptions{
		Now: options.Now, Limit: options.Limit, CacheUnusedBefore: options.Cache,
		ProviderStatusExpiresBefore: options.Provider, InventoryExpiresBefore: options.Inventory,
		QueryExecutionsExpiresBefore: options.Queries, OperationsExpiresBefore: options.Operations,
		BudgetBucketsBefore: options.Budgets, CheckpointsExpiresBefore: options.Checkpoints,
		MaxBudgetWindow: options.MaxWindow,
	})
	if encodeErr := encodeResult(out, result); encodeErr != nil {
		return encodeErr
	}
	if err != nil {
		return err
	}
	return nil
}

func requiredSecret(lookup lookupEnv, name string) (string, error) {
	if lookup == nil {
		return "", errors.New("environment lookup is unavailable")
	}
	value, ok := lookup(name)
	if !ok || strings.TrimSpace(value) == "" {
		return "", fmt.Errorf("%s must be set for the dedicated maintenance role", name)
	}
	return value, nil
}

func parseOptions(args []string, errOut io.Writer) (commandOptions, error) {
	flags := flag.NewFlagSet("retention-once", flag.ContinueOnError)
	flags.SetOutput(errOut)
	configPath := flags.String("config", defaultConfigPath, "worker configuration YAML path")
	now := flags.String("now", "", "UTC maintenance timestamp (RFC3339 with Z)")
	limit := flags.Int("limit", 0, "maximum rows per retention pass (1-10000)")
	cache := flags.String("cache-before", "", "UTC cache last-use cutoff (RFC3339 with Z)")
	provider := flags.String("provider-status-before", "", "UTC provider-status cutoff (RFC3339 with Z)")
	inventory := flags.String("inventory-before", "", "UTC inventory cutoff (RFC3339 with Z)")
	queries := flags.String("query-executions-before", "", "UTC query-execution cutoff (RFC3339 with Z)")
	operations := flags.String("operations-before", "", "UTC operation cutoff (RFC3339 with Z)")
	budgets := flags.String("budget-buckets-before", "", "UTC budget-bucket cutoff (RFC3339 with Z)")
	checkpoints := flags.String("checkpoints-before", "", "UTC checkpoint cutoff (RFC3339 with Z)")
	maxWindow := flags.Duration("max-budget-window", 0, "largest configured budget window")
	if err := flags.Parse(args); err != nil {
		return commandOptions{}, err
	}
	if flags.NArg() != 0 {
		return commandOptions{}, errors.New("retention-once does not accept positional arguments")
	}
	parse := func(name, value string) (time.Time, error) {
		if value == "" {
			return time.Time{}, fmt.Errorf("--%s is required", name)
		}
		if !strings.HasSuffix(value, "Z") {
			return time.Time{}, fmt.Errorf("--%s must use UTC RFC3339 with Z", name)
		}
		parsed, err := time.Parse(time.RFC3339Nano, value)
		if err != nil || parsed.Location() != time.UTC {
			return time.Time{}, fmt.Errorf("--%s must use UTC RFC3339 with Z", name)
		}
		return parsed, nil
	}
	parsedNow, err := parse("now", *now)
	if err != nil {
		return commandOptions{}, err
	}
	parsed := commandOptions{ConfigPath: *configPath, Now: parsedNow, Limit: *limit, MaxWindow: *maxWindow}
	cutoffs := []struct {
		name  string
		value string
		set   func(time.Time)
	}{
		{name: "cache-before", value: *cache, set: func(value time.Time) { parsed.Cache = value }},
		{name: "provider-status-before", value: *provider, set: func(value time.Time) { parsed.Provider = value }},
		{name: "inventory-before", value: *inventory, set: func(value time.Time) { parsed.Inventory = value }},
		{name: "query-executions-before", value: *queries, set: func(value time.Time) { parsed.Queries = value }},
		{name: "operations-before", value: *operations, set: func(value time.Time) { parsed.Operations = value }},
		{name: "budget-buckets-before", value: *budgets, set: func(value time.Time) { parsed.Budgets = value }},
		{name: "checkpoints-before", value: *checkpoints, set: func(value time.Time) { parsed.Checkpoints = value }},
	}
	for _, target := range cutoffs {
		cutoff, parseErr := parse(target.name, target.value)
		if parseErr != nil {
			return commandOptions{}, parseErr
		}
		target.set(cutoff)
	}
	if parsed.ConfigPath == "" || parsed.Limit <= 0 || parsed.Limit > 10000 || parsed.MaxWindow <= 0 {
		return commandOptions{}, errors.New("--config, --limit, and --max-budget-window must be valid")
	}
	return parsed, nil
}

type resultJSON struct {
	Passes []passJSON `json:"passes"`
}

type passJSON struct {
	Name       string `json:"name"`
	Examined   int    `json:"examined"`
	Tombstoned int    `json:"tombstoned"`
	Deleted    int    `json:"deleted"`
	Skipped    int    `json:"skipped"`
	Error      string `json:"error,omitempty"`
}

func encodeResult(out io.Writer, result postgres.RetentionBatchResult) error {
	encoded := resultJSON{Passes: make([]passJSON, 0, len(result.Passes))}
	for _, pass := range result.Passes {
		item := passJSON{Name: pass.Name, Examined: pass.Result.Examined, Tombstoned: pass.Result.Tombstoned, Deleted: pass.Result.Deleted, Skipped: pass.Result.Skipped}
		if pass.Err != nil {
			item.Error = pass.Err.Error()
		}
		encoded.Passes = append(encoded.Passes, item)
	}
	if err := json.NewEncoder(out).Encode(encoded); err != nil {
		return fmt.Errorf("write retention result: %w", err)
	}
	return nil
}
