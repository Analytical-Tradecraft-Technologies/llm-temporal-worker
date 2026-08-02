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
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/storage/postgres"
)

const defaultConfigPath = "/etc/llmtw/config.yaml"

// Keep the command's file read bounded independently of config.Load so an
// operator-controlled path cannot cause an unbounded allocation before the
// configuration validator sees the input.
const maxConfigBytes = 4 << 20

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

type inspectOptions struct {
	ConfigPath string
}

type blobGCOptions struct {
	ConfigPath string
	Now        time.Time
	Limit      int
}

type unknownCostOptions struct {
	ConfigPath string
	ScopeID    uuid.UUID
	Limit      int
	After      *postgres.UnknownCostCursor
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
	if len(args) == 0 {
		return errors.New("usage: llmtw-maintenance <retention-once|blob-gc-once|inspect-settings|unknown-cost-list> [flags]")
	}
	switch args[0] {
	case "retention-once":
		options, err := parseOptions(args[1:], errOut)
		if err != nil {
			return err
		}
		repository, closeRepository, err := openMaintenanceRepository(ctx, options.ConfigPath, lookup)
		if err != nil {
			return err
		}
		defer closeRepository()
		result, err := repository.RunRetentionBatch(ctx, postgres.RetentionBatchOptions{
			Now: options.Now, Limit: options.Limit, CacheUnusedBefore: options.Cache,
			ProviderStatusExpiresBefore: options.Provider, InventoryExpiresBefore: options.Inventory,
			QueryExecutionsExpiresBefore: options.Queries, OperationsExpiresBefore: options.Operations,
			BudgetBucketsBefore: options.Budgets, CheckpointsExpiresBefore: options.Checkpoints,
			MaxBudgetWindow: options.MaxWindow,
		})
		if encodeErr := encodeResult(out, result); encodeErr != nil {
			return encodeErr
		}
		return err
	case "inspect-settings":
		options, err := parseInspectOptions(args[1:], errOut)
		if err != nil {
			return err
		}
		repository, closeRepository, err := openMaintenanceRepository(ctx, options.ConfigPath, lookup)
		if err != nil {
			return err
		}
		defer closeRepository()
		settings, err := repository.InspectTableSettings(ctx)
		if err != nil {
			return err
		}
		return encodeSettings(out, settings)
	case "blob-gc-once":
		options, err := parseBlobGCOptions(args[1:], errOut)
		if err != nil {
			return err
		}
		repository, closeRepository, err := openMaintenanceRepository(ctx, options.ConfigPath, lookup)
		if err != nil {
			return err
		}
		defer closeRepository()
		result, err := repository.MarkExpiredBlobsEligible(ctx, options.Now, options.Limit)
		if encodeErr := encodeBlobGCResult(out, result); encodeErr != nil {
			return encodeErr
		}
		return err
	case "unknown-cost-list":
		options, err := parseUnknownCostOptions(args[1:], errOut)
		if err != nil {
			return err
		}
		maintenanceRepository, closeRepository, err := openMaintenanceRepository(ctx, options.ConfigPath, lookup)
		if err != nil {
			return err
		}
		defer closeRepository()
		repository := postgres.UnknownCostRepository{Pool: maintenanceRepository.Pool, Namespace: maintenanceRepository.Namespace}
		candidates, err := repository.List(ctx, postgres.UnknownCostListOptions{ScopeID: options.ScopeID, After: options.After, Limit: options.Limit})
		if encodeErr := encodeUnknownCostResult(out, candidates, options.Limit); encodeErr != nil {
			return encodeErr
		}
		return err
	default:
		return errors.New("usage: llmtw-maintenance <retention-once|blob-gc-once|inspect-settings|unknown-cost-list> [flags]")
	}
}

func openMaintenanceRepository(ctx context.Context, configPath string, lookup lookupEnv) (postgres.MaintenanceRepository, func(), error) {
	data, err := readConfig(configPath)
	if err != nil {
		return postgres.MaintenanceRepository{}, func() {}, fmt.Errorf("read configuration: %w", err)
	}
	value, err := config.Load(data)
	if err != nil {
		return postgres.MaintenanceRepository{}, func() {}, fmt.Errorf("load configuration: %w", err)
	}
	if value.State.Kind != config.StateKindDurable {
		return postgres.MaintenanceRepository{}, func() {}, errors.New("maintenance requires durable state")
	}
	// Runtime/provider credential references are intentionally never resolved by
	// this command. Only the dedicated maintenance role may be supplied here.
	username, err := requiredSecret(lookup, "LLMTW_MAINTENANCE_POSTGRES_USERNAME")
	if err != nil {
		return postgres.MaintenanceRepository{}, func() {}, err
	}
	password, err := requiredSecret(lookup, "LLMTW_MAINTENANCE_POSTGRES_PASSWORD")
	if err != nil {
		return postgres.MaintenanceRepository{}, func() {}, err
	}
	namespace, err := postgres.NewNamespace(value.State.Postgres.Database, value.State.Postgres.Schema, value.State.Postgres.TablePrefix)
	if err != nil {
		return postgres.MaintenanceRepository{}, func() {}, fmt.Errorf("construct PostgreSQL namespace: %w", err)
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
		return postgres.MaintenanceRepository{}, func() {}, fmt.Errorf("construct maintenance PostgreSQL pool: %w", err)
	}
	closeRepository := func() { pool.Close() }
	if err := postgres.Health(ctx, pool, namespace); err != nil {
		closeRepository()
		return postgres.MaintenanceRepository{}, func() {}, fmt.Errorf("check maintenance PostgreSQL: %w", err)
	}
	return postgres.MaintenanceRepository{Pool: pool, Namespace: namespace}, closeRepository, nil
}

func readConfig(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxConfigBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxConfigBytes {
		return nil, fmt.Errorf("configuration exceeds %d bytes", maxConfigBytes)
	}
	return data, nil
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

func parseInspectOptions(args []string, errOut io.Writer) (inspectOptions, error) {
	flags := flag.NewFlagSet("inspect-settings", flag.ContinueOnError)
	flags.SetOutput(errOut)
	configPath := flags.String("config", defaultConfigPath, "worker configuration YAML path")
	if err := flags.Parse(args); err != nil {
		return inspectOptions{}, err
	}
	if flags.NArg() != 0 {
		return inspectOptions{}, errors.New("inspect-settings does not accept positional arguments")
	}
	if *configPath == "" {
		return inspectOptions{}, errors.New("--config must not be empty")
	}
	return inspectOptions{ConfigPath: *configPath}, nil
}

func parseBlobGCOptions(args []string, errOut io.Writer) (blobGCOptions, error) {
	flags := flag.NewFlagSet("blob-gc-once", flag.ContinueOnError)
	flags.SetOutput(errOut)
	configPath := flags.String("config", defaultConfigPath, "worker configuration YAML path")
	now := flags.String("now", "", "UTC maintenance timestamp (RFC3339 with Z)")
	limit := flags.Int("limit", 0, "maximum rows per eligibility pass (1-10000)")
	if err := flags.Parse(args); err != nil {
		return blobGCOptions{}, err
	}
	if flags.NArg() != 0 {
		return blobGCOptions{}, errors.New("blob-gc-once does not accept positional arguments")
	}
	if *configPath == "" {
		return blobGCOptions{}, errors.New("--config must not be empty")
	}
	if *now == "" || !strings.HasSuffix(*now, "Z") {
		return blobGCOptions{}, errors.New("--now must use UTC RFC3339 with Z")
	}
	parsedNow, err := time.Parse(time.RFC3339Nano, *now)
	if err != nil || parsedNow.Location() != time.UTC {
		return blobGCOptions{}, errors.New("--now must use UTC RFC3339 with Z")
	}
	if *limit <= 0 || *limit > 10000 {
		return blobGCOptions{}, errors.New("--limit must be between 1 and 10000")
	}
	return blobGCOptions{ConfigPath: *configPath, Now: parsedNow, Limit: *limit}, nil
}

func parseUnknownCostOptions(args []string, errOut io.Writer) (unknownCostOptions, error) {
	flags := flag.NewFlagSet("unknown-cost-list", flag.ContinueOnError)
	flags.SetOutput(errOut)
	configPath := flags.String("config", defaultConfigPath, "worker configuration YAML path")
	scopeID := flags.String("scope-id", "", "opaque scope UUID authorized for billing reconciliation")
	limit := flags.Int("limit", 0, "maximum candidates to return (1-10000)")
	afterCompletedAt := flags.String("after-completed-at", "", "UTC cursor completion timestamp (RFC3339 with Z)")
	afterOperationID := flags.String("after-operation-id", "", "cursor operation UUID paired with --after-completed-at")
	if err := flags.Parse(args); err != nil {
		return unknownCostOptions{}, err
	}
	if flags.NArg() != 0 {
		return unknownCostOptions{}, errors.New("unknown-cost-list does not accept positional arguments")
	}
	if *configPath == "" {
		return unknownCostOptions{}, errors.New("--config must not be empty")
	}
	if *limit <= 0 || *limit > 10000 {
		return unknownCostOptions{}, errors.New("--limit must be between 1 and 10000")
	}
	parsedScope, err := uuid.Parse(*scopeID)
	if err != nil || parsedScope == uuid.Nil {
		return unknownCostOptions{}, errors.New("--scope-id must be a non-zero UUID")
	}
	if (*afterCompletedAt == "") != (*afterOperationID == "") {
		return unknownCostOptions{}, errors.New("--after-completed-at and --after-operation-id must be supplied together")
	}
	var cursor *postgres.UnknownCostCursor
	if *afterCompletedAt != "" {
		if !strings.HasSuffix(*afterCompletedAt, "Z") {
			return unknownCostOptions{}, errors.New("--after-completed-at must use UTC RFC3339 with Z")
		}
		completedAt, parseErr := time.Parse(time.RFC3339Nano, *afterCompletedAt)
		if parseErr != nil || completedAt.Location() != time.UTC {
			return unknownCostOptions{}, errors.New("--after-completed-at must use UTC RFC3339 with Z")
		}
		operationID, parseErr := uuid.Parse(*afterOperationID)
		if parseErr != nil || operationID == uuid.Nil {
			return unknownCostOptions{}, errors.New("--after-operation-id must be a non-zero UUID")
		}
		cursor = &postgres.UnknownCostCursor{CompletedAt: completedAt, OperationID: operationID}
	}
	return unknownCostOptions{ConfigPath: *configPath, ScopeID: parsedScope, Limit: *limit, After: cursor}, nil
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

type blobGCResultJSON struct {
	Examined int `json:"examined"`
	Eligible int `json:"eligible"`
	Skipped  int `json:"skipped"`
}

func encodeBlobGCResult(out io.Writer, result postgres.BlobGCResult) error {
	if err := json.NewEncoder(out).Encode(blobGCResultJSON{Examined: result.Examined, Eligible: result.Eligible, Skipped: result.Skipped}); err != nil {
		return fmt.Errorf("write blob GC result: %w", err)
	}
	return nil
}

type unknownCostResultJSON struct {
	Candidates []unknownCostCandidateJSON `json:"candidates"`
	NextCursor *unknownCostCursorJSON     `json:"next_cursor,omitempty"`
}

type unknownCostCandidateJSON struct {
	OperationID       string `json:"operation_id"`
	CompletedAt       string `json:"completed_at"`
	UnknownReasonCode string `json:"unknown_reason_code"`
}

type unknownCostCursorJSON struct {
	CompletedAt string `json:"completed_at"`
	OperationID string `json:"operation_id"`
}

func encodeUnknownCostResult(out io.Writer, candidates []postgres.UnknownCostCandidate, limit int) error {
	if limit <= 0 || limit > 10000 {
		return errors.New("unknown-cost result limit must be between 1 and 10000")
	}
	encoded := unknownCostResultJSON{Candidates: make([]unknownCostCandidateJSON, 0, len(candidates))}
	for _, candidate := range candidates {
		encoded.Candidates = append(encoded.Candidates, unknownCostCandidateJSON{
			OperationID: candidate.OperationID.String(), CompletedAt: candidate.CompletedAt.UTC().Format(time.RFC3339Nano),
			UnknownReasonCode: candidate.UnknownReasonCode,
		})
	}
	if len(candidates) == limit {
		last := candidates[len(candidates)-1]
		encoded.NextCursor = &unknownCostCursorJSON{CompletedAt: last.CompletedAt.UTC().Format(time.RFC3339Nano), OperationID: last.OperationID.String()}
	}
	if err := json.NewEncoder(out).Encode(encoded); err != nil {
		return fmt.Errorf("write unknown-cost result: %w", err)
	}
	return nil
}

type settingsJSON struct {
	Tables []tableSettingsJSON `json:"tables"`
}

type tableSettingsJSON struct {
	Resource                     string     `json:"resource"`
	Fillfactor                   *int       `json:"fillfactor,omitempty"`
	AutovacuumVacuumThreshold    *int64     `json:"autovacuum_vacuum_threshold,omitempty"`
	AutovacuumVacuumScaleFactor  *float64   `json:"autovacuum_vacuum_scale_factor,omitempty"`
	AutovacuumAnalyzeThreshold   *int64     `json:"autovacuum_analyze_threshold,omitempty"`
	AutovacuumAnalyzeScaleFactor *float64   `json:"autovacuum_analyze_scale_factor,omitempty"`
	AutovacuumEnabled            *bool      `json:"autovacuum_enabled,omitempty"`
	ToastAutovacuumEnabled       *bool      `json:"toast_autovacuum_enabled,omitempty"`
	LiveTuples                   int64      `json:"live_tuples"`
	DeadTuples                   int64      `json:"dead_tuples"`
	LastAutovacuum               *time.Time `json:"last_autovacuum,omitempty"`
	LastAutoanalyze              *time.Time `json:"last_autoanalyze,omitempty"`
}

func encodeSettings(out io.Writer, settings []postgres.TableMaintenanceSettings) error {
	encoded := settingsJSON{Tables: make([]tableSettingsJSON, 0, len(settings))}
	for _, item := range settings {
		encoded.Tables = append(encoded.Tables, tableSettingsJSON{
			Resource: item.Resource, Fillfactor: item.Fillfactor,
			AutovacuumVacuumThreshold:    item.AutovacuumVacuumThreshold,
			AutovacuumVacuumScaleFactor:  item.AutovacuumVacuumScaleFactor,
			AutovacuumAnalyzeThreshold:   item.AutovacuumAnalyzeThreshold,
			AutovacuumAnalyzeScaleFactor: item.AutovacuumAnalyzeScaleFactor,
			AutovacuumEnabled:            item.AutovacuumEnabled, ToastAutovacuumEnabled: item.ToastAutovacuumEnabled,
			LiveTuples: item.LiveTuples, DeadTuples: item.DeadTuples,
			LastAutovacuum: item.LastAutovacuum, LastAutoanalyze: item.LastAutoanalyze,
		})
	}
	sort.Slice(encoded.Tables, func(i, j int) bool { return encoded.Tables[i].Resource < encoded.Tables[j].Resource })
	if err := json.NewEncoder(out).Encode(encoded); err != nil {
		return fmt.Errorf("write maintenance settings: %w", err)
	}
	return nil
}
