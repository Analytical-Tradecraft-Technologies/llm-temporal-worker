package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/internal/catalog"
	workerruntime "github.com/mfow/llm-temporal-worker/golang/internal/runtime"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	postgresstore "github.com/mfow/llm-temporal-worker/golang/storage/postgres"
	redisstore "github.com/mfow/llm-temporal-worker/golang/storage/redis"
	redisclient "github.com/redis/go-redis/v9"
)

const (
	defaultConfigPath = "/etc/llmtw/config.yaml"
	priceVersion      = "local-v1"
)

var (
	coverageStart = time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	coverageEnd   = time.Date(2027, time.January, 1, 0, 0, 0, 0, time.UTC)
)

type windowSeed struct {
	ID              uuid.UUID
	Key             string
	DurationSeconds int64
	BucketSeconds   int64
	LimitUSD        pricing.USD
}

type policySeed struct {
	ID             uuid.UUID
	Key            string
	Tenant         string
	Project        string
	Selector       []byte
	SelectorDigest [32]byte
	Priority       int
	Windows        []windowSeed
}

type bootstrapState struct {
	Snapshot     *config.Snapshot
	SourceDigest [32]byte
	Manifest     redisstore.BudgetManifest
	Policies     []policySeed
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	state, err := buildBootstrap(ctx, os.Getenv("BUDGET_CONFIG_FILE"))
	if err != nil {
		log.Fatal(err)
	}
	if identityPath := os.Getenv("PRICING_IDENTITY_FILE"); identityPath != "" {
		if err = writePricingIdentity(state.Snapshot, identityPath); err != nil {
			log.Fatal(err)
		}
	}
	if err = seedPostgres(ctx, os.Getenv("DATABASE_URL"), os.Getenv("WORKER_POSTGRES_SCOPE_KEY"), state); err != nil {
		log.Fatal(err)
	}

	username := os.Getenv("REDIS_USERNAME")
	password := os.Getenv("REDIS_PASSWORD")
	if username == "" || password == "" {
		log.Fatal("joined budget bootstrap Redis credentials are required")
	}
	address := os.Getenv("REDIS_ADDRESS")
	if address == "" {
		address = "redis:6379"
	}
	client := redisclient.NewClient(&redisclient.Options{
		Addr: address, Username: username, Password: password,
		DialTimeout: 3 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second,
		MaxRetries: -1,
	})
	defer client.Close()
	if err = client.Ping(ctx).Err(); err != nil {
		log.Fatal("joined budget bootstrap Redis is unavailable")
	}
	for _, source := range []string{
		redisstore.AdmissionFunctionSource(),
		redisstore.ThrottleFunctionSource(),
		redisstore.BudgetStatusFunctionLibrarySource(),
	} {
		if err = client.FunctionLoad(ctx, source).Err(); err != nil && !strings.Contains(strings.ToLower(err.Error()), "already exists") {
			log.Fatalf("load joined Redis function: %v", err)
		}
	}
	secret := sha256.Sum256(append([]byte("llmtw:redis-key-v1:"), []byte(password)...))
	keys, err := redisstore.NewBudgetKeySpace(redisstore.KeyOptions{Prefix: "joined_smoke", HashTag: "admission", KeySecret: secret[:]})
	if err != nil {
		log.Fatal(err)
	}
	if _, err = redisstore.BootstrapBudgetColdStart(ctx, redisstore.BudgetColdStartOptions{Client: client, Keys: keys, Manifest: state.Manifest}); err != nil {
		log.Fatal(err)
	}
	log.Print("JOINED_SMOKE_BUDGET_READY")
}

type pricingIdentity struct {
	PricingGenerationID   string `json:"pricing_generation_id"`
	PricingManifestSHA256 string `json:"pricing_manifest_sha256"`
}

func writePricingIdentity(snapshot *config.Snapshot, path string) error {
	if snapshot == nil {
		return errors.New("joined smoke pricing identity requires a configuration snapshot")
	}
	bundle, err := catalog.Load(snapshot.Config())
	if err != nil {
		return fmt.Errorf("load joined pricing catalogs: %w", err)
	}
	compiled, err := workerruntime.MergePricingCatalogs(bundle, snapshot.ConfigVersion())
	if err != nil {
		return fmt.Errorf("compile joined runtime pricing identity: %w", err)
	}
	identity := pricingIdentity{
		PricingGenerationID: compiled.Version, PricingManifestSHA256: compiled.DigestHex(),
	}
	encoded, err := json.Marshal(identity)
	if err != nil {
		return fmt.Errorf("encode joined pricing identity: %w", err)
	}
	if strings.TrimSpace(path) == "" || !filepath.IsAbs(path) {
		return errors.New("joined pricing identity path must be absolute")
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".pricing-identity-*")
	if err != nil {
		return fmt.Errorf("create joined pricing identity: %w", err)
	}
	name := temporary.Name()
	defer os.Remove(name)
	defer func() { _ = temporary.Close() }()
	if err = temporary.Chmod(0o644); err != nil {
		return fmt.Errorf("protect joined pricing identity: %w", err)
	}
	if _, err = temporary.Write(encoded); err != nil {
		return fmt.Errorf("write joined pricing identity: %w", err)
	}
	if err = temporary.Sync(); err != nil {
		return fmt.Errorf("sync joined pricing identity: %w", err)
	}
	if err = temporary.Close(); err != nil {
		return fmt.Errorf("close joined pricing identity: %w", err)
	}
	if err = os.Rename(name, path); err != nil {
		return fmt.Errorf("publish joined pricing identity: %w", err)
	}
	return nil
}

func buildManifest(ctx context.Context, path string) (redisstore.BudgetManifest, error) {
	state, err := buildBootstrap(ctx, path)
	return state.Manifest, err
}

func buildBootstrap(ctx context.Context, path string) (bootstrapState, error) {
	if path == "" {
		path = defaultConfigPath
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return bootstrapState{}, fmt.Errorf("read joined worker config: %w", err)
	}
	snapshot, err := config.Compile(ctx, data, nil)
	if err != nil {
		return bootstrapState{}, fmt.Errorf("compile joined worker config: %w", err)
	}
	value := snapshot.Config()
	if len(value.Budgets.Policies) == 0 {
		return bootstrapState{}, errors.New("joined worker config has no budget policies")
	}
	policyHash, err := digestJSON(value.Budgets.Policies)
	if err != nil {
		return bootstrapState{}, err
	}
	allWindows := make([]config.BudgetWindow, 0)
	for _, policy := range value.Budgets.Policies {
		allWindows = append(allWindows, policy.Windows...)
	}
	windowHash, err := digestJSON(allWindows)
	if err != nil {
		return bootstrapState{}, err
	}

	members := make([]redisstore.BudgetManifestMember, 0, len(allWindows))
	policies := make([]policySeed, 0, len(value.Budgets.Policies))
	for policyIndex, policyValue := range value.Budgets.Policies {
		if policyValue.Match.Tenant == "" || policyValue.Match.Tenant == "*" || policyValue.Match.Project == "" || policyValue.Match.Project == "*" {
			return bootstrapState{}, fmt.Errorf("joined budget policy %q must bind one tenant and project scope", policyValue.ID)
		}
		policyIdentity, err := budget.PolicyIdentity(snapshot.Digest(), policyValue.ID)
		if err != nil {
			return bootstrapState{}, err
		}
		policyUUID, err := uuid.Parse(policyIdentity)
		if err != nil {
			return bootstrapState{}, fmt.Errorf("parse joined policy identity: %w", err)
		}
		selector, err := json.Marshal(policyValue.Match)
		if err != nil {
			return bootstrapState{}, fmt.Errorf("marshal joined budget selector: %w", err)
		}
		seed := policySeed{
			ID: policyUUID, Key: policyValue.ID,
			Tenant: policyValue.Match.Tenant, Project: policyValue.Match.Project,
			Selector: selector, SelectorDigest: sha256.Sum256(selector), Priority: policyIndex,
			Windows: make([]windowSeed, 0, len(policyValue.Windows)),
		}
		for index, window := range policyValue.Windows {
			width := time.Duration(window.Bucket)
			duration := time.Duration(window.Duration)
			if width <= 0 || duration <= 0 || coverageEnd.Sub(coverageStart)%width != 0 {
				return bootstrapState{}, errors.New("joined budget coverage is not aligned to a configured bucket")
			}
			limit := window.LimitUSD
			if limit.IsZero() {
				limit, err = pricing.USDFromMicro(pricing.MicroUSD(window.LimitMicroUSD))
				if err != nil {
					return bootstrapState{}, err
				}
			}
			limitNano, err := pricing.FloorNanoUSD(limit)
			if err != nil || limitNano <= 0 {
				return bootstrapState{}, errors.New("joined budget limit cannot be materialized")
			}
			windowIdentity, err := budget.WindowIdentity(policyIdentity, index)
			if err != nil {
				return bootstrapState{}, err
			}
			windowUUID, err := uuid.Parse(windowIdentity)
			if err != nil {
				return bootstrapState{}, fmt.Errorf("parse joined window identity: %w", err)
			}
			bucketDigest, err := digestJSON(struct {
				Start time.Time     `json:"start"`
				End   time.Time     `json:"end"`
				Width time.Duration `json:"width_ns"`
			}{Start: coverageStart, End: coverageEnd, Width: width})
			if err != nil {
				return bootstrapState{}, err
			}
			members = append(members, redisstore.BudgetManifestMember{
				PolicyID: policyIdentity, WindowID: windowIdentity,
				PolicyHash: policyHash, WindowHash: windowHash,
				ConfigVersion: snapshot.ConfigVersion(), PriceVersion: priceVersion,
				CoverageStart: coverageStart, CoverageEnd: coverageEnd,
				BucketCount: int(coverageEnd.Sub(coverageStart) / width), BucketWidth: width,
				BucketCatalogDigest: bucketDigest, LimitNanoUSD: limitNano.String(),
			})
			seed.Windows = append(seed.Windows, windowSeed{
				ID: windowUUID, Key: fmt.Sprintf("%d", index),
				DurationSeconds: int64(duration / time.Second), BucketSeconds: int64(width / time.Second), LimitUSD: limit,
			})
		}
		policies = append(policies, seed)
	}
	catalogDigest, err := redisstore.MemberCatalogDigest(members)
	if err != nil {
		return bootstrapState{}, err
	}
	generationID := deterministicBootstrapID("generation", snapshot.Digest())
	incarnationID := deterministicBootstrapID("incarnation", snapshot.Digest())
	manifest := redisstore.BudgetManifest{
		Schema: redisstore.BudgetManifestSchema, GenerationID: redisstore.BudgetGenerationID(generationID), IncarnationID: redisstore.BudgetIncarnationID(incarnationID),
		ConfigVersion: snapshot.ConfigVersion(), PriceVersion: priceVersion,
		PolicyHash: policyHash, WindowHash: windowHash, RebuildComplete: true,
		CoverageStart: coverageStart, CoverageEnd: coverageEnd,
		PolicyCount: len(value.Budgets.Policies), WindowCount: len(members),
		StreamHighWaterMark: redisstore.BudgetColdStartStreamID,
		RoundingVersion:     redisstore.BudgetRoundingVersion, MemberCatalogDigest: catalogDigest, Members: members,
	}
	for _, member := range members {
		manifest.BucketCount += member.BucketCount
	}
	if err := redisstore.ValidateBudgetColdStartManifest(manifest); err != nil {
		return bootstrapState{}, fmt.Errorf("validate joined budget manifest: %w", err)
	}
	return bootstrapState{Snapshot: snapshot, SourceDigest: sha256.Sum256(data), Manifest: manifest, Policies: policies}, nil
}

func deterministicBootstrapID(kind string, digest [32]byte) string {
	material := append([]byte("llmtw/joined-budget-bootstrap/v1\x00"+kind+"\x00"), digest[:]...)
	return uuid.NewSHA1(uuid.NameSpaceOID, material).String()
}

func seedPostgres(ctx context.Context, databaseURL, scopeKey string, state bootstrapState) error {
	if databaseURL == "" {
		return errors.New("joined budget bootstrap PostgreSQL URL is required")
	}
	if len(scopeKey) != 32 {
		return errors.New("joined budget bootstrap PostgreSQL scope key must be exactly 32 bytes")
	}
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return fmt.Errorf("open joined budget PostgreSQL: %w", err)
	}
	defer pool.Close()
	if err = pool.Ping(ctx); err != nil {
		return errors.New("joined budget bootstrap PostgreSQL is unavailable")
	}
	namespace := postgresstore.Namespace{Database: "llmtw_worker", Schema: "llmtw_state", TablePrefix: "llmtw_"}
	scopeKeys := postgresstore.ScopeKeyring{ActiveVersion: "scope-v1", Keys: map[string][]byte{"scope-v1": []byte(scopeKey)}}
	scopes := make(map[string]postgresstore.Scope, len(state.Policies))
	for _, policy := range state.Policies {
		key := policy.Tenant + "\x00" + policy.Project
		if _, ok := scopes[key]; ok {
			continue
		}
		tenantDigest, projectDigest, deriveErr := scopeKeys.Derive(policy.Tenant, policy.Project)
		if deriveErr != nil {
			return fmt.Errorf("derive joined budget scope: %w", deriveErr)
		}
		scopeIdentity := append([]byte("llmtw/joined-budget-scope/v1\x00"), tenantDigest[:]...)
		scopeIdentity = append(scopeIdentity, projectDigest[:]...)
		repository := postgresstore.DefaultScopeRepository(pool, namespace, scopeKeys)
		repository.NewID = func() (uuid.UUID, error) {
			return uuid.NewSHA1(uuid.NameSpaceOID, scopeIdentity), nil
		}
		scope, ensureErr := repository.Ensure(ctx, policy.Tenant, policy.Project)
		if ensureErr != nil {
			return fmt.Errorf("seed joined budget scope: %w", ensureErr)
		}
		scopes[key] = scope
	}

	configs, err := namespace.Render("configuration_snapshots")
	if err != nil {
		return err
	}
	policies, err := namespace.Render("budget_policies")
	if err != nil {
		return err
	}
	windows, err := namespace.Render("budget_windows")
	if err != nil {
		return err
	}
	generations, err := namespace.Render("budget_redis_generations")
	if err != nil {
		return err
	}
	manifestDigest, err := state.Manifest.ManifestDigest()
	if err != nil {
		return fmt.Errorf("digest joined budget manifest: %w", err)
	}
	generationUUID, err := uuid.Parse(string(state.Manifest.GenerationID))
	if err != nil {
		return fmt.Errorf("parse joined generation identity: %w", err)
	}
	configDigest := state.Snapshot.Digest()

	return postgresstore.WithTransaction(ctx, pool, func(ctx context.Context, tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "INSERT INTO "+configs+" (config_digest, config_version, source_digest, sanitized_config) VALUES ($1,$2,$3,$4::jsonb) ON CONFLICT (config_digest) DO NOTHING", configDigest[:], state.Snapshot.ConfigVersion(), state.SourceDigest[:], state.Snapshot.Canonical()); err != nil {
			return fmt.Errorf("seed joined configuration snapshot: %w", err)
		}
		for _, policy := range state.Policies {
			scope := scopes[policy.Tenant+"\x00"+policy.Project]
			if _, err := tx.Exec(ctx, "INSERT INTO "+policies+" (policy_id, scope_id, policy_key, config_digest, selector_digest, sanitized_selector, priority, enabled, effective_from) VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7,true,$8) ON CONFLICT (policy_id) DO NOTHING", policy.ID, scope.ID, policy.Key, configDigest[:], policy.SelectorDigest[:], policy.Selector, policy.Priority, coverageStart); err != nil {
				return fmt.Errorf("seed joined budget policy %q: %w", policy.Key, err)
			}
			if err := verifyPolicy(ctx, tx, policies, policy, scope.ID, configDigest); err != nil {
				return err
			}
			for _, window := range policy.Windows {
				if _, err := tx.Exec(ctx, "INSERT INTO "+windows+" (window_id, policy_id, window_key, duration_seconds, bucket_seconds, limit_usd) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (window_id) DO NOTHING", window.ID, policy.ID, window.Key, window.DurationSeconds, window.BucketSeconds, window.LimitUSD.String()); err != nil {
					return fmt.Errorf("seed joined budget window %q: %w", window.Key, err)
				}
				if err := verifyWindow(ctx, tx, windows, policy.ID, window); err != nil {
					return err
				}
			}
		}
		if _, err := tx.Exec(ctx, "INSERT INTO "+generations+" (generation_id, reason, state, source_journal_id, coverage_start, coverage_end, manifest_digest, completed_at) VALUES ($1,'initial_cold_start','active',0,$2,$3,$4,clock_timestamp()) ON CONFLICT (generation_id) DO NOTHING", generationUUID, coverageStart, coverageEnd, manifestDigest[:]); err != nil {
			return fmt.Errorf("seed joined budget generation: %w", err)
		}
		return verifyGeneration(ctx, tx, generations, generationUUID, manifestDigest)
	})
}

func verifyPolicy(ctx context.Context, tx pgx.Tx, table string, expected policySeed, scopeID uuid.UUID, configDigest [32]byte) error {
	var storedScope uuid.UUID
	var key string
	var storedConfig, selector []byte
	var priority int
	var enabled bool
	if err := tx.QueryRow(ctx, "SELECT scope_id, policy_key, config_digest, selector_digest, priority, enabled FROM "+table+" WHERE policy_id=$1", expected.ID).Scan(&storedScope, &key, &storedConfig, &selector, &priority, &enabled); err != nil {
		return fmt.Errorf("verify joined budget policy %q: %w", expected.Key, err)
	}
	if storedScope != scopeID || key != expected.Key || !bytes.Equal(storedConfig, configDigest[:]) || !bytes.Equal(selector, expected.SelectorDigest[:]) || priority != expected.Priority || !enabled {
		return fmt.Errorf("joined budget policy %q conflicts with its deterministic identity", expected.Key)
	}
	return nil
}

func verifyWindow(ctx context.Context, tx pgx.Tx, table string, policyID uuid.UUID, expected windowSeed) error {
	var storedPolicy uuid.UUID
	var key, limit string
	var duration, bucket int64
	if err := tx.QueryRow(ctx, "SELECT policy_id, window_key, duration_seconds, bucket_seconds, limit_usd::text FROM "+table+" WHERE window_id=$1", expected.ID).Scan(&storedPolicy, &key, &duration, &bucket, &limit); err != nil {
		return fmt.Errorf("verify joined budget window %q: %w", expected.Key, err)
	}
	if storedPolicy != policyID || key != expected.Key || duration != expected.DurationSeconds || bucket != expected.BucketSeconds || limit != expected.LimitUSD.String() {
		return fmt.Errorf("joined budget window %q conflicts with its deterministic identity", expected.Key)
	}
	return nil
}

func verifyGeneration(ctx context.Context, tx pgx.Tx, table string, generationID uuid.UUID, manifestDigest [32]byte) error {
	var reason, state string
	var sourceJournalID int64
	var start, end time.Time
	var storedDigest []byte
	if err := tx.QueryRow(ctx, "SELECT reason, state, source_journal_id, coverage_start, coverage_end, manifest_digest FROM "+table+" WHERE generation_id=$1", generationID).Scan(&reason, &state, &sourceJournalID, &start, &end, &storedDigest); err != nil {
		return fmt.Errorf("verify joined budget generation: %w", err)
	}
	if reason != "initial_cold_start" || state != "active" || sourceJournalID != 0 || !start.Equal(coverageStart) || !end.Equal(coverageEnd) || !bytes.Equal(storedDigest, manifestDigest[:]) {
		return errors.New("joined budget generation conflicts with its deterministic identity")
	}
	return nil
}

func digestJSON(value any) (string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("marshal joined budget identity: %w", err)
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:]), nil
}
