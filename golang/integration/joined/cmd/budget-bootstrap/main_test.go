package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mfow/llm-temporal-worker/golang/budget"
	workerruntime "github.com/mfow/llm-temporal-worker/golang/internal/runtime"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	redisstore "github.com/mfow/llm-temporal-worker/golang/storage/redis"
)

func TestBuildManifestMatchesJoinedWorkerBudget(t *testing.T) {
	state, err := buildBootstrap(context.Background(), "../../llm-config.yaml")
	manifest := state.Manifest
	if err != nil {
		t.Fatal(err)
	}
	if err = redisstore.ValidateBudgetColdStartManifest(manifest); err != nil {
		t.Fatalf("manifest is invalid: %v", err)
	}
	if _, parseErr := uuid.Parse(string(manifest.GenerationID)); parseErr != nil {
		t.Fatalf("generation ID is not a UUID: %v", parseErr)
	}
	if _, parseErr := uuid.Parse(string(manifest.IncarnationID)); parseErr != nil || manifest.ConfigVersion == "" {
		t.Fatalf("manifest identity = %#v", manifest)
	}
	if len(manifest.Members) != 1 {
		t.Fatalf("manifest member count = %d, want 1", len(manifest.Members))
	}
	member := manifest.Members[0]
	policyID, _ := budget.PolicyIdentity(state.Snapshot.Digest(), "joined-smoke")
	windowID, _ := budget.WindowIdentity(policyID, 0)
	if member.PolicyID != policyID || member.WindowID != windowID || member.LimitNanoUSD != "5000000000" {
		t.Fatalf("manifest member = %#v", member)
	}
	if member.CoverageStart != coverageStart || member.CoverageEnd != coverageEnd || member.BucketCount != 525600 {
		t.Fatalf("manifest coverage = %s..%s (%d buckets)", member.CoverageStart, member.CoverageEnd, member.BucketCount)
	}
	if len(state.Policies) != 1 || state.Policies[0].ID.String() != policyID || len(state.Policies[0].Windows) != 1 || state.Policies[0].Windows[0].ID.String() != windowID {
		t.Fatalf("PostgreSQL seeds do not match Redis member: %#v", state.Policies)
	}
}

func TestWritePricingIdentityUsesRuntimeCatalogIdentity(t *testing.T) {
	configBytes, err := os.ReadFile("../../llm-config.yaml")
	if err != nil {
		t.Fatal(err)
	}
	capabilitiesPath, err := filepath.Abs("../../capabilities.yaml")
	if err != nil {
		t.Fatal(err)
	}
	pricesPath, err := filepath.Abs("../../prices.yaml")
	if err != nil {
		t.Fatal(err)
	}
	configBytes = []byte(strings.ReplaceAll(strings.ReplaceAll(string(configBytes),
		"/etc/llmtw/capabilities.yaml", capabilitiesPath), "/etc/llmtw/prices.yaml", pricesPath))
	root := t.TempDir()
	configPath := filepath.Join(root, "llm-config.yaml")
	if err = os.WriteFile(configPath, configBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	state, err := buildBootstrap(context.Background(), configPath)
	if err != nil {
		t.Fatal(err)
	}
	identityPath := filepath.Join(root, "pricing-identity.json")
	if err = writePricingIdentity(state.Snapshot, identityPath); err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(identityPath)
	if err != nil {
		t.Fatal(err)
	}
	var identity pricingIdentity
	if err := json.Unmarshal(content, &identity); err != nil {
		t.Fatal(err)
	}
	runtimeSnapshot, err := (workerruntime.CatalogSnapshotLoader{Clock: func() time.Time { return coverageStart }}).Load(context.Background(), state.Snapshot)
	if err != nil {
		t.Fatal(err)
	}
	quote, err := runtimeSnapshot.Prices.Resolve(pricing.Query{Provider: "joined-smoke", Family: "openai_chat", EndpointID: "joined-smoke", Region: "local", Model: "joined-smoke-2026-08-10", ProviderTier: "standard", At: coverageStart})
	if err != nil {
		t.Fatal(err)
	}
	if identity.PricingGenerationID != quote.CatalogVersion || identity.PricingManifestSHA256 != quote.CatalogDigest {
		t.Fatalf("pricing identity = %#v, runtime quote = %#v", identity, quote)
	}
}
