package contracttest

import (
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"testing"

	yaml "go.yaml.in/yaml/v4"
)

type requiredCaseFile struct {
	Version         int                  `yaml:"version"`
	CapabilityFacts []string             `yaml:"capability_facts"`
	Cases           []requiredCaseRecord `yaml:"cases"`
}

type requiredCaseRecord struct {
	ID         string         `yaml:"id"`
	Capability string         `yaml:"capability"`
	Artifacts  []ArtifactKind `yaml:"artifacts"`
}

// TestFixtureManifestComplete is the repository-wide release gate for the
// code-owned fixture matrix. Individual adapter packages retain focused
// conversion assertions; this test ensures every checked-in fixture profile is
// enforced and that the reviewed YAML inventory cannot drift from the case
// registry. Runtime route composition binds those profiles separately.
func TestFixtureManifestComplete(t *testing.T) {
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("could not locate contracttest package")
	}
	providerRoot := filepath.Dir(filepath.Dir(source))
	fixturePath := filepath.Join(providerRoot, "testdata", "contracts", "required-cases.yaml")
	data, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatalf("read required-case inventory: %v", err)
	}
	var inventory requiredCaseFile
	if err := yaml.Load(data, &inventory, yaml.WithKnownFields(), yaml.WithUniqueKeys(), yaml.WithSingleDocument()); err != nil {
		t.Fatalf("parse required-case inventory: %v", err)
	}
	if inventory.Version != 1 {
		t.Fatalf("required-case inventory version = %d, want 1", inventory.Version)
	}
	assertCapabilityFacts(t, inventory.CapabilityFacts)
	assertCaseInventory(t, inventory.Cases)

	report, err := ValidateRepository(filepath.Join(providerRoot, "..", ".."))
	if err != nil {
		t.Fatalf("validate adapter fixture repository: %v", err)
	}
	if err := report.RequireAllEnforced(); err != nil {
		t.Fatal(err)
	}
	if len(report.Enforced) == 0 {
		t.Fatal("fixture repository has no enforced profiles")
	}
}

func assertCapabilityFacts(t *testing.T, got []string) {
	t.Helper()
	want := GovernedCapabilityFacts()
	actual := append([]string(nil), got...)
	sort.Strings(actual)
	if len(actual) != len(want) {
		t.Fatalf("governed capability facts = %#v, want %#v", actual, want)
	}
	for index := range want {
		if actual[index] != want[index] {
			t.Fatalf("governed capability facts = %#v, want %#v", actual, want)
		}
	}
}

func assertCaseInventory(t *testing.T, records []requiredCaseRecord) {
	t.Helper()
	want := CaseRequirements()
	if len(records) != len(want) {
		t.Fatalf("required case count = %d, want %d", len(records), len(want))
	}
	actual := make(map[string]requiredCaseRecord, len(records))
	for _, record := range records {
		if _, duplicate := actual[record.ID]; duplicate {
			t.Fatalf("required case %q appears more than once", record.ID)
		}
		actual[record.ID] = record
	}
	for _, requirement := range want {
		record, ok := actual[requirement.ID]
		if !ok {
			t.Fatalf("required case inventory omits %q", requirement.ID)
		}
		if record.Capability != requirement.Capability {
			t.Fatalf("case %q capability = %q, want %q", requirement.ID, record.Capability, requirement.Capability)
		}
		gotArtifacts := append([]ArtifactKind(nil), record.Artifacts...)
		sort.Slice(gotArtifacts, func(left, right int) bool { return gotArtifacts[left] < gotArtifacts[right] })
		wantArtifacts := append([]ArtifactKind(nil), requirement.Artifacts...)
		sort.Slice(wantArtifacts, func(left, right int) bool { return wantArtifacts[left] < wantArtifacts[right] })
		if len(gotArtifacts) != len(wantArtifacts) {
			t.Fatalf("case %q artifacts = %#v, want %#v", requirement.ID, gotArtifacts, wantArtifacts)
		}
		for index := range wantArtifacts {
			if gotArtifacts[index] != wantArtifacts[index] {
				t.Fatalf("case %q artifacts = %#v, want %#v", requirement.ID, gotArtifacts, wantArtifacts)
			}
		}
	}
}
