package config_test

import (
	"context"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
)

// withParentSnapshotStorage sets state.requests.parent_snapshot_storage in the
// complete example, or leaves it unset when value is empty.
func withParentSnapshotStorage(t *testing.T, value string) []byte {
	t.Helper()
	const anchor = "    namespace: worker-v1\n"
	if value == "" {
		return []byte(exampleContaining(t, anchor))
	}
	return replaceExample(t, anchor, anchor+"    parent_snapshot_storage: "+value+"\n")
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
	testassert.ErrorContains(t, testassert.Err(config.Load(withParentSnapshotStorage(t, "sideways"))), "state.requests.parent_snapshot_storage")
}

func TestConfigSchemaParentSnapshotStorage(t *testing.T) {
	compiled := configSchema(t)
	loaded := mustLoadConfig(t, exampleYAML(t))
	for value, valid := range map[string]bool{"": true, "inline": true, "blob": true, "sideways": false} {
		changed := loaded.Clone()
		changed.State.Requests.ParentSnapshotStorage = value
		encoded := testassert.MarshalJSON(t, changed)
		if err := compiled.Validate(encoded); (err == nil) != valid {
			t.Errorf("%q: schema validation = %v, want valid=%t", value, err, valid)
		}
		if err := changed.Validate(); (err == nil) != valid {
			t.Errorf("%q: Validate() = %v, want valid=%t", value, err, valid)
		}
	}
}
