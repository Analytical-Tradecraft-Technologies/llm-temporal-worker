package config_test

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
)

func TestBudgetsJSONMatchesYAMLAndSnapshotIdentity(t *testing.T) {
	original := exampleYAML(t)
	yamlSnapshot := mustCompile(t, original)
	encoded := mustMarshalJSON(t, yamlSnapshot.Config().Budgets)
	start := strings.Index(string(original), "\nbudgets:\n")
	end := strings.Index(string(original), "\ncontinuation:\n")
	if start < 0 || end < start {
		t.Fatal("example budget block missing")
	}
	asJSON := string(original[:start]) + "\nbudgets_json: |\n  " + string(encoded) + "\n" + string(original[end:])
	jsonSnapshot := mustCompile(t, []byte(asJSON))
	if !reflect.DeepEqual(yamlSnapshot.Config().Budgets, jsonSnapshot.Config().Budgets) || yamlSnapshot.Digest() != jsonSnapshot.Digest() {
		t.Fatal("JSON and YAML must yield the same effective policy and snapshot identity")
	}
	changed := strings.Replace(asJSON, "25.000000000000000000", "26.000000000000000000", 1)
	if changed == asJSON {
		t.Fatal("expected limit missing")
	}
	changedSnapshot := mustCompile(t, []byte(changed))
	if changedSnapshot.Digest() == jsonSnapshot.Digest() {
		t.Fatal("budget change omitted from configuration digest")
	}
	requireLoadRejected(t, append(original, []byte("\nbudgets_json: '{}'")...), "accepted two budget sources")
	loaded := loadExampleWith(t, "  reservation_lease: 15m\n", "")
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
