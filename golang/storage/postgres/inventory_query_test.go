package postgres

import (
	"crypto/sha256"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mfow/llm-temporal-worker/golang/control"
)

func TestInventoryModelListOptionsNormalizeDefaultsAndBounds(t *testing.T) {
	options := InventoryModelListOptions{ConfigDigest: sha256.Sum256([]byte("config"))}
	if err := options.Normalize(); err != nil {
		t.Fatal(err)
	}
	if options.Limit != DefaultInventoryPageSize {
		t.Fatalf("default page size = %d, want %d", options.Limit, DefaultInventoryPageSize)
	}

	valid := InventoryModelListOptions{
		ConfigDigest:    options.ConfigDigest,
		Provider:        "openai",
		EndpointID:      "primary",
		ModelPrefix:     "gpt-",
		Lifecycle:       control.LifecycleAvailable,
		SnapshotHorizon: time.Date(2026, 7, 20, 0, 0, 0, 0, time.UTC),
		After:           InventoryModelPosition{Provider: "openai", EndpointID: "primary", SnapshotID: uuid.MustParse("018f7b5d-9ad8-7f5e-8d8c-4cf8e5c4a4d1"), ProviderModelID: "gpt-4o"},
		Limit:           10,
	}
	if err := valid.Normalize(); err != nil {
		t.Fatal(err)
	}

	for _, test := range []struct {
		name    string
		options InventoryModelListOptions
	}{
		{name: "missing digest", options: InventoryModelListOptions{}},
		{name: "oversized page", options: InventoryModelListOptions{ConfigDigest: options.ConfigDigest, Limit: MaxInventoryPageSize + 1}},
		{name: "invalid lifecycle", options: InventoryModelListOptions{ConfigDigest: options.ConfigDigest, Lifecycle: control.Lifecycle("future")}},
		{name: "unsafe provider", options: InventoryModelListOptions{ConfigDigest: options.ConfigDigest, Provider: " provider"}},
		{name: "unsafe prefix", options: InventoryModelListOptions{ConfigDigest: options.ConfigDigest, ModelPrefix: strings.Repeat("m", 257)}},
		{name: "incomplete position", options: InventoryModelListOptions{ConfigDigest: options.ConfigDigest, SnapshotHorizon: time.Now(), After: InventoryModelPosition{EndpointID: "endpoint"}}},
		{name: "position without horizon", options: InventoryModelListOptions{ConfigDigest: options.ConfigDigest, After: InventoryModelPosition{Provider: "provider", EndpointID: "endpoint", SnapshotID: uuid.New(), ProviderModelID: "model"}}},
		{name: "filter mismatch", options: InventoryModelListOptions{ConfigDigest: options.ConfigDigest, Provider: "provider", SnapshotHorizon: time.Now(), After: InventoryModelPosition{Provider: "other", EndpointID: "endpoint", SnapshotID: uuid.New(), ProviderModelID: "model"}}},
		{name: "position without snapshot", options: InventoryModelListOptions{ConfigDigest: options.ConfigDigest, SnapshotHorizon: time.Now(), After: InventoryModelPosition{Provider: "provider", EndpointID: "endpoint", ProviderModelID: "model"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := test.options.Normalize(); err == nil {
				t.Fatal("invalid options were accepted")
			}
		})
	}
}

func TestInventorySnapshotInfoProvenance(t *testing.T) {
	now := time.Date(2026, 7, 20, 0, 0, 0, 0, time.UTC)
	current := InventorySnapshotInfo{Source: control.InventoryProviderAPI, ExpiresAt: now.Add(time.Minute)}
	if got := current.ProvenanceAt(now); got != control.ProvenanceCurrent {
		t.Fatalf("current provenance = %q", got)
	}
	if got := current.ProvenanceAt(now.Add(time.Minute)); got != control.ProvenanceStale {
		t.Fatalf("expired provenance = %q", got)
	}
	unsupported := current
	unsupported.Source = control.InventoryUnsupported
	if got := unsupported.ProvenanceAt(now); got != control.ProvenanceUnsupported {
		t.Fatalf("unsupported provenance = %q", got)
	}
}

func TestLatestInventoryQueriesPinSnapshotHorizonAndUseKeyset(t *testing.T) {
	snapshots := `"private"."provider_inventory_snapshots"`
	models := `"private"."provider_inventory_models"`
	horizon := latestInventoryHorizonQuery(snapshots)
	if !strings.Contains(horizon, "DISTINCT ON (provider, endpoint_id, endpoint_account_hmac)") || !strings.Contains(horizon, "endpoint_account_hmac, observed_at DESC") {
		t.Fatalf("horizon query is not latest-per-endpoint: %s", horizon)
	}
	query := latestInventoryModelsQuery(snapshots, models)
	for _, expected := range []string{
		"observed_at <= $4",
		"LEFT(model.provider_model_id, char_length($5))=$5",
		"(latest.provider, latest.endpoint_id, latest.inventory_snapshot_id, model.provider_model_id) > ($7,$8,$9,$10)",
		"ORDER BY latest.provider, latest.endpoint_id, latest.inventory_snapshot_id, model.provider_model_id",
	} {
		if !strings.Contains(query, expected) {
			t.Errorf("inventory query missing %q", expected)
		}
	}
}
