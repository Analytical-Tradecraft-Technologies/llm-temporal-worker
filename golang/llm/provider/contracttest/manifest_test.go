package contracttest

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
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

// TestFixtureMetadataUsesDirectSDKVersions keeps the checked-in fixture
// provenance tied to the versions that the worker actually builds against.
// A fixture recorded against an older SDK can otherwise look reviewed while
// silently exercising a different request/response surface than production.
func TestFixtureMetadataUsesDirectSDKVersions(t *testing.T) {
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("could not locate contracttest package")
	}
	providerRoot := filepath.Dir(filepath.Dir(source))
	repositoryRoot := filepath.Join(providerRoot, "..", "..")
	report, err := ValidateRepository(repositoryRoot)
	if err != nil {
		t.Fatalf("validate adapter fixture repository: %v", err)
	}
	versions, err := directModuleVersions(filepath.Join(repositoryRoot, "go.mod"))
	if err != nil {
		t.Fatalf("read direct Go module versions: %v", err)
	}
	profiles := append(append([]Profile(nil), report.Bootstrap...), report.Enforced...)
	for _, profile := range profiles {
		metadataPath := filepath.Join(repositoryRoot, filepath.FromSlash(profile.Path), "metadata.yaml")
		metadata, err := loadMetadata(metadataPath)
		if err != nil {
			t.Fatalf("load metadata for profile %q: %v", profile.ID, err)
		}
		module, version, ok := strings.Cut(metadata.SDKVersion, "@")
		if !ok || strings.TrimSpace(module) == "" || strings.TrimSpace(version) == "" {
			t.Fatalf("profile %q has invalid sdk_version %q", profile.ID, metadata.SDKVersion)
		}
		want, ok := versions[module]
		if !ok {
			t.Fatalf("profile %q records SDK module %q, which is not a direct go.mod requirement", profile.ID, module)
		}
		if version != want {
			t.Fatalf("profile %q records SDK %s@%s, want direct go.mod version %s", profile.ID, module, version, want)
		}
	}
}

func directModuleVersions(path string) (map[string]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	versions := make(map[string]string)
	inBlock := false
	seenBlock := false
	for _, raw := range strings.Split(string(data), "\n") {
		line := strings.TrimSpace(raw)
		if line == "require (" {
			if seenBlock {
				break
			}
			seenBlock = true
			inBlock = true
			continue
		}
		if !inBlock {
			continue
		}
		if line == ")" {
			break
		}
		line = strings.TrimSpace(strings.SplitN(line, "//", 2)[0])
		fields := strings.Fields(line)
		if len(fields) >= 2 {
			versions[fields[0]] = fields[1]
		}
	}
	if len(versions) == 0 {
		return nil, fmt.Errorf("go.mod has no direct require block")
	}
	return versions, nil
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
