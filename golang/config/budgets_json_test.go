package config_test

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/config"
)

func TestBudgetsJSONMatchesYAMLAndSnapshotIdentity(t *testing.T) {
	original := exampleYAML(t)
	yamlSnapshot, err := config.Compile(context.Background(), original, nil)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(yamlSnapshot.Config().Budgets)
	if err != nil {
		t.Fatal(err)
	}
	start := strings.Index(string(original), "\nbudgets:\n")
	end := strings.Index(string(original), "\ncontinuation:\n")
	if start < 0 || end < start {
		t.Fatal("example budget block missing")
	}
	asJSON := string(original[:start]) + "\nbudgets_json: |\n  " + string(encoded) + "\n" + string(original[end:])
	jsonSnapshot, err := config.Compile(context.Background(), []byte(asJSON), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(yamlSnapshot.Config().Budgets, jsonSnapshot.Config().Budgets) || yamlSnapshot.Digest() != jsonSnapshot.Digest() {
		t.Fatal("JSON and YAML must yield the same effective policy and snapshot identity")
	}
	changed := strings.Replace(asJSON, "25.000000000000000000", "26.000000000000000000", 1)
	if changed == asJSON {
		t.Fatal("expected limit missing")
	}
	changedSnapshot, err := config.Compile(context.Background(), []byte(changed), nil)
	if err != nil {
		t.Fatal(err)
	}
	if changedSnapshot.Digest() == jsonSnapshot.Digest() {
		t.Fatal("budget change omitted from configuration digest")
	}
	if _, err := config.Load(append(original, []byte("\nbudgets_json: '{}'")...)); err == nil {
		t.Fatal("accepted two budget sources")
	}
	withoutLease := strings.Replace(string(original), "  reservation_lease: 15m\n", "", 1)
	loaded, err := config.Load([]byte(withoutLease))
	if err != nil {
		t.Fatal(err)
	}
	if time.Duration(loaded.State.ReservationLease) != 15*time.Minute {
		t.Fatalf("lease default = %v", loaded.State.ReservationLease)
	}
}

func TestBudgetsJSONRejectsInvalidPolicies(t *testing.T) {
	for _, input := range []string{
		``, `null`, `[]`, `{`, `{} {}`, `{"unknown":true}`, `{"policies":[],"policies":[]}`,
		`{"require_match":true,"policies":[{"id":"test","match":{"tenant":"acme"},"windows":[{"duration":"1h","bucket":"1m","limit_usd":"-1"}]}]}`,
		`{"policies":[{"id":"test","typo":true}]}`,
	} {
		t.Run(input, func(t *testing.T) {
			if _, err := config.ParseBudgetsJSON([]byte(input)); err == nil {
				t.Fatal("invalid budget JSON accepted")
			}
		})
	}
}
