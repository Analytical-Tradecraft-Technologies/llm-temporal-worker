package contracttest

import (
	"fmt"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidateRepositoryReportsBootstrapProfiles(t *testing.T) {
	root := t.TempDir()
	profileDir := filepath.Join(root, "llm", "provider", "example", "testdata", "contracts", "example")
	mustWriteManifestWithValidServiceClasses(t, filepath.Join(profileDir, "manifest.yaml"), `version: 1
id: example
provider: example
family: chat
coverage: bootstrap
metadata: metadata.yaml
cases:
  - id: semantic-request
    artifacts:
      semantic: request.semantic.json
      wire: request.wire.json
      events: events.wire
`)
	mustWriteFile(t, filepath.Join(profileDir, "metadata.yaml"), `profile: example
upstream_url: https://example.test/contracts
upstream_date: "2026-07-14"
sdk_version: example-sdk/v1
provenance: synthetic
redactions:
  - credentials
capability_facts:
  usage: unsupported
  streaming: unsupported
  strict_loss: unsupported
  best_effort: unsupported
  service_class: unsupported
  continuation: unsupported
generated_field_exemptions: []
`)
	mustWriteFile(t, filepath.Join(profileDir, "request.semantic.json"), `{}`)
	mustWriteFile(t, filepath.Join(profileDir, "request.wire.json"), `{}`)
	mustWriteFile(t, filepath.Join(profileDir, "events.wire"), "event: completed\n")

	report, err := ValidateRepository(root)
	testassert.NoError(t, err)
	if len(report.Bootstrap) != 1 || report.Bootstrap[0].ID != "example" {
		t.Fatalf("bootstrap profiles = %#v", report.Bootstrap)
	}
	if len(report.Enforced) != 0 {
		t.Fatalf("enforced profiles = %#v", report.Enforced)
	}
}

func TestValidateRepositoryRejectsInvalidServiceClassesWithoutLeakingValues(t *testing.T) {
	secret := "AKIA" + "1234567890ABCDEF"
	tests := []struct {
		name           string
		serviceClasses string
		want           string
	}{
		{
			name: "missing public class",
			serviceClasses: `service_classes:
  economy:
    supported: false
  standard:
    requested_tier: default
`,
			want: "manifest service_classes must declare non-empty priority facts",
		},
		{
			name: "empty public class",
			serviceClasses: `service_classes:
  economy:
    supported: false
  standard: {}
  priority:
    supported: false
`,
			want: "manifest service_classes must declare non-empty standard facts",
		},
		{
			name: "provider default public class",
			serviceClasses: `service_classes:
  economy:
    supported: false
  standard:
    requested_tier: default
  priority:
    supported: false
  provider_default:
    captured_value: ` + secret + `
`,
			want: "manifest service_classes must not declare provider_default",
		},
		{
			name: "malformed public class value",
			serviceClasses: `service_classes:
  economy:
    supported: false
  standard:
    requested_tier: default
  priority: ` + secret + `
`,
			want: "manifest is not valid YAML",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			profileDir := testProfileDir(root)
			mustWriteFile(t, filepath.Join(profileDir, "manifest.yaml"), `version: 1
id: example
provider: example
family: chat
coverage: bootstrap
metadata: metadata.yaml
`+test.serviceClasses+`cases:
  - id: semantic-request
    artifacts:
      semantic: request.semantic.json
`)
			mustWriteFile(t, filepath.Join(profileDir, "metadata.yaml"), validTestMetadata)
			mustWriteFile(t, filepath.Join(profileDir, "request.semantic.json"), `{}`)

			_, err := ValidateRepository(root)
			testassert.ErrorContains(t, err, test.want)
			if strings.Contains(err.Error(), secret) {
				t.Fatalf("service class value leaked through error: %v", err)
			}
		})
	}
}

func TestValidateRepositoryAcceptsSupplementalServiceClassScenarios(t *testing.T) {
	root := t.TempDir()
	profileDir := testProfileDir(root)
	mustWriteFile(t, filepath.Join(profileDir, "manifest.yaml"), `version: 1
id: example
provider: example
family: chat
coverage: bootstrap
metadata: metadata.yaml
`+validServiceClasses+`  priority_downgrade:
    requested_tier: priority
    actual_tier: standard
  unknown_tier:
    actual_tier: scale
  reported_cost:
    currency: USD
cases:
  - id: semantic-request
    artifacts:
      semantic: request.semantic.json
`)
	mustWriteFile(t, filepath.Join(profileDir, "metadata.yaml"), validTestMetadata)
	mustWriteFile(t, filepath.Join(profileDir, "request.semantic.json"), `{}`)

	testassert.NoError(t, testassert.Err(ValidateRepository(root)), "supplemental service class scenarios rejected")
}

func TestValidateRepositoryRejectsUnsafeFixtureBytesWithoutLeakingThem(t *testing.T) {
	root := t.TempDir()
	profileDir := filepath.Join(root, "llm", "provider", "example", "testdata", "contracts", "example")
	mustWriteManifestWithValidServiceClasses(t, filepath.Join(profileDir, "manifest.yaml"), `version: 1
id: example
provider: example
family: chat
coverage: bootstrap
metadata: metadata.yaml
cases:
  - id: captured-wire-request
    artifacts:
      wire: request.wire.json
`)
	mustWriteFile(t, filepath.Join(profileDir, "metadata.yaml"), `profile: example
upstream_url: https://example.test/contracts
upstream_date: "2026-07-14"
sdk_version: example-sdk/v1
provenance: synthetic
redactions:
  - credentials
capability_facts:
  streaming: unsupported
generated_field_exemptions: []
`)
	secret := "AKIA" + "1234567890ABCDEF"
	mustWriteFile(t, filepath.Join(profileDir, "request.wire.json"), `{"authorization":"`+secret+`"}`)

	_, err := ValidateRepository(root)
	if err == nil {
		t.Fatal("ValidateRepository succeeded with credential-like fixture bytes")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("unsafe fixture bytes leaked through error: %v", err)
	}
	if !strings.Contains(err.Error(), "request.wire.json") {
		t.Fatalf("error did not identify the sanitized fixture path: %v", err)
	}
}

func TestContainsUnsafeFixtureBytesDetectsCredentialFieldsAcrossSyntaxes(t *testing.T) {
	tests := []struct {
		name    string
		fixture string
	}{
		{
			name:    "quoted JSON authorization",
			fixture: `{"authorization":"Bearer s` + `k-live-0123456789abcdef"}`,
		},
		{
			name:    "escaped JSON authorization",
			fixture: `{\"authorization\":\"Bearer s` + `k-live-0123456789abcdef\"}`,
		},
		{
			name:    "quoted JSON API key",
			fixture: `{"api_key":"s` + `k-live-0123456789abcdef"}`,
		},
		{
			name:    "YAML access token",
			fixture: "access_token: token-value-0123456789abcdef",
		},
		{
			name:    "equals secret key",
			fixture: "secret-key=token-value-0123456789abcdef",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if !containsUnsafeFixtureBytes([]byte(test.fixture)) {
				t.Fatal("credential-like field was not detected")
			}
		})
	}
}

func TestValidateRepositoryRejectsIncompleteEnforcedProfile(t *testing.T) {
	root := t.TempDir()
	profileDir := filepath.Join(root, "llm", "provider", "example", "testdata", "contracts", "example")
	mustWriteManifestWithValidServiceClasses(t, filepath.Join(profileDir, "manifest.yaml"), `version: 1
id: example
provider: example
family: chat
coverage: enforced
metadata: metadata.yaml
cases:
  - id: captured-wire-request
    artifacts:
      wire: request.wire.json
`)
	mustWriteFile(t, filepath.Join(profileDir, "metadata.yaml"), `profile: example
upstream_url: https://example.test/contracts
upstream_date: "2026-07-14"
sdk_version: example-sdk/v1
provenance: synthetic
redactions:
  - credentials
capability_facts:
  streaming: unsupported
generated_field_exemptions: []
`)
	mustWriteFile(t, filepath.Join(profileDir, "request.wire.json"), `{}`)

	_, err := ValidateRepository(root)
	testassert.ErrorContains(t, err, "missing required case")
}

func TestValidateRepositoryRequiresDecoderFixturesWhenDeclared(t *testing.T) {
	for _, missingCase := range []string{"full-stream", "fragmented-stream"} {
		t.Run(missingCase, func(t *testing.T) {
			root := t.TempDir()
			profileDir := testProfileDir(root)
			remainingCase := "full-stream"
			if missingCase == remainingCase {
				remainingCase = "fragmented-stream"
			}
			mustWriteManifestWithValidServiceClasses(t, filepath.Join(profileDir, "manifest.yaml"), `version: 1
id: example
provider: example
family: chat
coverage: enforced
metadata: metadata.yaml
cases:
  - id: semantic-request
    artifacts:
      semantic: semantic.json
  - id: captured-wire-request
    artifacts:
      wire: wire.json
  - id: response
    artifacts:
      semantic: semantic.json
      wire: wire.json
  - id: classified-error
    artifacts:
      wire: wire.json
  - id: security-redaction
    artifacts:
      wire: wire.json
  - id: `+remainingCase+`
    artifacts:
      events: events.stream
`)
			mustWriteFile(t, filepath.Join(profileDir, "metadata.yaml"), `profile: example
upstream_url: https://example.test/contracts
upstream_date: "2026-07-14"
sdk_version: example-sdk/v1
provenance: synthetic
redactions:
  - credentials
capability_facts:
  usage: unsupported
  streaming: unsupported
  stream_decoder: supported
  strict_loss: unsupported
  best_effort: unsupported
  service_class: unsupported
  continuation: unsupported
generated_field_exemptions: []
`)
			mustWriteFile(t, filepath.Join(profileDir, "semantic.json"), `{}`)
			mustWriteFile(t, filepath.Join(profileDir, "wire.json"), `{}`)
			mustWriteFile(t, filepath.Join(profileDir, "events.stream"), "event: completed\n")

			_, err := ValidateRepository(root)
			if err == nil || !strings.Contains(err.Error(), `missing required case "`+missingCase+`"`) {
				t.Fatalf("decoder fixture error = %v, want missing %q", err, missingCase)
			}
		})
	}
}

func TestValidateRepositoryRejectsEnforcedProfileWithMissingCapabilityFacts(t *testing.T) {
	for _, test := range []struct {
		name  string
		facts string
	}{
		{name: "missing stream decoder fact", facts: `  usage: unsupported
  streaming: unsupported
  strict_loss: unsupported
  best_effort: unsupported
  service_class: unsupported
  continuation: unsupported
`},
		{name: "undeclared enforced capability", facts: `  streaming: unsupported
`},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			profileDir := testProfileDir(root)
			mustWriteManifestWithValidServiceClasses(t, filepath.Join(profileDir, "manifest.yaml"), `version: 1
id: example
provider: example
family: chat
coverage: enforced
metadata: metadata.yaml
cases:
  - id: semantic-request
    artifacts:
      semantic: semantic.json
  - id: captured-wire-request
    artifacts:
      wire: wire.json
  - id: response
    artifacts:
      semantic: semantic.json
      wire: wire.json
  - id: classified-error
    artifacts:
      wire: wire.json
  - id: security-redaction
    artifacts:
      wire: wire.json
`)
			mustWriteFile(t, filepath.Join(profileDir, "metadata.yaml"), `profile: example
upstream_url: https://example.test/contracts
upstream_date: "2026-07-14"
sdk_version: example-sdk/v1
provenance: synthetic
redactions:
  - credentials
capability_facts:
`+test.facts+`generated_field_exemptions: []
`)
			mustWriteFile(t, filepath.Join(profileDir, "semantic.json"), `{}`)
			mustWriteFile(t, filepath.Join(profileDir, "wire.json"), `{}`)
			requireRepositoryError(t, root, "missing enforced capability fact", test.name)
		})
	}
}

func TestReportRequireAllEnforcedRejectsBootstrapProfiles(t *testing.T) {
	err := (Report{Bootstrap: []Profile{{ID: "example"}}}).RequireAllEnforced()
	testassert.ErrorContains(t, err, "example")
}

func TestValidateRepositoryRejectsMalformedManifest(t *testing.T) {
	root := t.TempDir()
	profileDir := testProfileDir(root)
	mustWriteFile(t, filepath.Join(profileDir, "manifest.yaml"), "version: [\n")

	_, err := ValidateRepository(root)
	testassert.ErrorContains(t, err, "manifest is not valid YAML")
}

func TestValidateRepositoryRejectsUnknownManifestFields(t *testing.T) {
	root := t.TempDir()
	profileDir := testProfileDir(root)
	mustWriteFile(t, filepath.Join(profileDir, "manifest.yaml"), `version: 1
id: example
provider: example
family: chat
coverage: bootstrap
metadata: metadata.yaml
unexpected: true
cases:
  - id: semantic-request
    artifacts:
      semantic: request.semantic.json
`)
	mustWriteFile(t, filepath.Join(profileDir, "metadata.yaml"), validTestMetadata)
	mustWriteFile(t, filepath.Join(profileDir, "request.semantic.json"), `{}`)

	_, err := ValidateRepository(root)
	testassert.ErrorContains(t, err, "manifest is not valid YAML")
}

func TestValidateRepositoryRejectsMissingDeclaredFixture(t *testing.T) {
	root := t.TempDir()
	writeBootstrapProfile(t, root, "captured-wire-request", "wire", "missing.wire.json", validTestMetadata)
	requireRepositoryError(t, root, "references a missing fixture", "missing fixture")
}

func TestValidateRepositoryRejectsCaseWithoutItsDocumentedArtifact(t *testing.T) {
	root := t.TempDir()
	writeBootstrapProfile(t, root, "semantic-request", "wire", "request.wire.json", validTestMetadata, "request.wire.json")
	requireRepositoryError(t, root, "documented semantic artifact", "wrong case artifact")
}

func TestValidateRepositoryRejectsMissingSourceDate(t *testing.T) {
	root := t.TempDir()
	writeBootstrapProfile(t, root, "semantic-request", "semantic", "request.semantic.json", strings.Replace(validTestMetadata, "upstream_date: \"2026-07-14\"\n", "", 1), "request.semantic.json")
	requireRepositoryError(t, root, "metadata upstream_date must be YYYY-MM-DD", "missing source date")
}

func TestValidateRepositoryRejectsTraversalPathWithoutLeakingIt(t *testing.T) {
	root := t.TempDir()
	traversal := "../../secret-fixture-name"
	writeBootstrapProfile(t, root, "captured-wire-request", "wire", traversal, validTestMetadata)
	err := requireRepositoryError(t, root, "case has an invalid artifact path", "traversal path")
	if strings.Contains(err.Error(), traversal) || strings.Contains(err.Error(), "secret-fixture-name") {
		t.Fatalf("traversal input leaked through error: %v", err)
	}
}

func TestValidateRepositoryScansSharedContractFixturesWithoutLeakingBytes(t *testing.T) {
	root := t.TempDir()
	profileDir := writeBootstrapProfile(t, root, "semantic-request", "semantic", "request.semantic.json", validTestMetadata, "request.semantic.json")
	secret := "AKIA" + "1234567890ABCDEF"
	mustWriteFile(t, filepath.Join(filepath.Dir(profileDir), "events.wire"), "token: "+secret)
	err := requireRepositoryError(t, root, "events.wire", "shared fixture scan")
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("shared fixture bytes leaked through error: %v", err)
	}
}

func TestVerifySemanticRoundTripIgnoresGeneratedFields(t *testing.T) {
	err := VerifySemanticRoundTrip(
		[]byte(`{"provider":{"response_id":"before"},"text":"hello"}`),
		func([]byte) ([]byte, error) {
			return []byte(`{"text":"hello","provider":{"response_id":"after"}}`), nil
		},
		[]string{"provider.response_id"},
	)
	testassert.NoError(t, err)
}

func TestVerifyStreamAssemblyEquivalentRejectsDifferentResponse(t *testing.T) {
	err := VerifyStreamAssemblyEquivalent(
		[]byte("event: text\\ndata: hello\\n"),
		[]byte(`{"text":"hello"}`),
		func([]byte) ([]byte, error) {
			return []byte(`{"text":"different"}`), nil
		},
		nil,
	)
	testassert.ErrorContains(t, err, "stream assembly differs")
}

func TestRepositoryManifestReportRequiresAllProfilesEnforced(t *testing.T) {
	tests := []struct {
		name    string
		report  Report
		wantErr string
	}{
		{
			name:    "no profiles",
			wantErr: "repository has no adapter contract profiles",
		},
		{
			name: "bootstrap profiles",
			report: Report{
				Bootstrap: []Profile{{ID: "bootstrap"}},
			},
			wantErr: "adapter contract profiles remain bootstrap: bootstrap",
		},
		{
			name: "enforced profiles",
			report: Report{
				Enforced: []Profile{{ID: "enforced"}},
			},
		},
		{
			name: "mixed profiles",
			report: Report{
				Bootstrap: []Profile{{ID: "bootstrap"}},
				Enforced:  []Profile{{ID: "enforced"}},
			},
			wantErr: "adapter contract profiles remain bootstrap: bootstrap",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateRepositoryManifestReport(test.report)
			if test.wantErr == "" {
				testassert.NoError(t, err, "validate report")
				return
			}
			testassert.ErrorContains(t, err, test.wantErr)
		})
	}
}

func TestRepositoryManifests(t *testing.T) {
	root := filepath.Clean(filepath.Join("..", "..", ".."))
	report, err := ValidateRepository(root)
	testassert.NoError(t, err)
	testassert.NoError(t, validateRepositoryManifestReport(report))
	t.Logf("adapter contract bootstrap profiles: %s", profileIDs(report.Bootstrap))
	t.Logf("adapter contract enforced profiles: %s", profileIDs(report.Enforced))
}

// validateRepositoryManifestReport verifies the checked-in repository is ready
// for the release gate: it has profiles and each one is enforced.
func validateRepositoryManifestReport(report Report) error {
	if len(report.Bootstrap)+len(report.Enforced) == 0 {
		return fmt.Errorf("repository has no adapter contract profiles")
	}
	return report.RequireAllEnforced()
}

func profileIDs(profiles []Profile) string {
	ids := make([]string, 0, len(profiles))
	for _, profile := range profiles {
		ids = append(ids, profile.ID)
	}
	return strings.Join(ids, ", ")
}

func mustWriteFile(t *testing.T, path, contents string) {
	t.Helper()
	testassert.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	testassert.NoError(t, os.WriteFile(path, []byte(contents), 0o600))
}

func mustWriteManifestWithValidServiceClasses(t *testing.T, path, contents string) {
	t.Helper()
	const metadataMarker = "metadata: metadata.yaml\n"
	if !strings.Contains(contents, metadataMarker) {
		t.Fatal("test manifest is missing its metadata marker")
	}
	mustWriteFile(t, path, strings.Replace(contents, metadataMarker, metadataMarker+validServiceClasses, 1))
}

const validTestMetadata = `profile: example
upstream_url: https://example.test/contracts
upstream_date: "2026-07-14"
sdk_version: example-sdk/v1
provenance: synthetic
redactions:
  - credentials
capability_facts:
  streaming: unsupported
generated_field_exemptions: []
`

const validServiceClasses = `service_classes:
  economy:
    supported: false
  standard:
    requested_tier: default
  priority:
    supported: false
`

func testProfileDir(root string) string {
	return filepath.Join(root, "llm", "provider", "example", "testdata", "contracts", "example")
}

// writeBootstrapProfile writes a bootstrap profile under root whose one case
// caseID declares its kind artifact at path, with metadata and an empty JSON
// fixture for each of files, and returns the profile directory.
func writeBootstrapProfile(t *testing.T, root, caseID, kind, path, metadata string, files ...string) string {
	t.Helper()
	profileDir := testProfileDir(root)
	mustWriteManifestWithValidServiceClasses(t, filepath.Join(profileDir, "manifest.yaml"), `version: 1
id: example
provider: example
family: chat
coverage: bootstrap
metadata: metadata.yaml
cases:
  - id: `+caseID+`
    artifacts:
      `+kind+`: `+path+`
`)
	mustWriteFile(t, filepath.Join(profileDir, "metadata.yaml"), metadata)
	for _, file := range files {
		mustWriteFile(t, filepath.Join(profileDir, file), `{}`)
	}
	return profileDir
}

// requireRepositoryError checks that validating root fails with an error
// containing want, naming the failure what, and returns the error.
func requireRepositoryError(t *testing.T, root, want, what string) error {
	t.Helper()
	_, err := ValidateRepository(root)
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("%s error = %v, want %q", what, err, want)
	}
	return err
}
