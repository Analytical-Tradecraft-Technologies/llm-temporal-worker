package config_test

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/schema"
)

// withParentSnapshotStorage sets state.requests.parent_snapshot_storage in the
// complete example, or leaves it unset when value is empty.
func withParentSnapshotStorage(t *testing.T, value string) []byte {
	t.Helper()
	data := string(exampleYAML(t))
	const anchor = "    namespace: worker-v1\n"
	if !strings.Contains(data, anchor) {
		t.Fatal("example has no state.requests.namespace")
	}
	if value == "" {
		return []byte(data)
	}
	return []byte(strings.Replace(data, anchor, anchor+"    parent_snapshot_storage: "+value+"\n", 1))
}

func TestParentSnapshotStorageDefaultsInlineWithoutChangingDigest(t *testing.T) {
	compile := func(value string) *config.Snapshot {
		snapshot, err := config.Compile(context.Background(), withParentSnapshotStorage(t, value), nil)
		if err != nil {
			t.Fatalf("%q: %v", value, err)
		}
		return snapshot
	}
	unset, inline, blob := compile(""), compile("inline"), compile("blob")
	if unset.ConfigVersion() != inline.ConfigVersion() || strings.Contains(string(unset.Canonical()), "parent_snapshot_storage") {
		t.Fatal("the default parent-snapshot storage changed the configuration digest")
	}
	if got := inline.Config().State.Requests.ParentSnapshotStorage; got != "" {
		t.Fatalf("explicit inline normalized to %q", got)
	}
	if blob.ConfigVersion() == unset.ConfigVersion() || blob.Config().State.Requests.ParentSnapshotStorage != config.ParentSnapshotStorageBlob {
		t.Fatal("blob storage was not part of the effective configuration")
	}
	if _, err := config.Load(withParentSnapshotStorage(t, "sideways")); err == nil || !strings.Contains(err.Error(), "state.requests.parent_snapshot_storage") {
		t.Fatalf("Load() accepted an unknown storage: %v", err)
	}
}

func TestConfigSchemaParentSnapshotStorage(t *testing.T) {
	source, err := os.ReadFile("../api/schema/v1/config.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := schema.Parse(source)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := config.Load(exampleYAML(t))
	if err != nil {
		t.Fatal(err)
	}
	for value, valid := range map[string]bool{"": true, "inline": true, "blob": true, "sideways": false} {
		changed := loaded.Clone()
		changed.State.Requests.ParentSnapshotStorage = value
		encoded, err := json.Marshal(changed)
		if err != nil {
			t.Fatal(err)
		}
		if err := compiled.Validate(encoded); (err == nil) != valid {
			t.Errorf("%q: schema validation = %v, want valid=%t", value, err, valid)
		}
		if err := changed.Validate(); (err == nil) != valid {
			t.Errorf("%q: Validate() = %v, want valid=%t", value, err, valid)
		}
	}
}
