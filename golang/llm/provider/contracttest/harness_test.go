package contracttest

import (
	"fmt"
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
	if err != nil {
		t.Fatal(err)
	}
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
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("service class error = %v, want %q", err, test.want)
			}
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

	if _, err := ValidateRepository(root); err != nil {
		t.Fatalf("supplemental service class scenarios rejected: %v", err)
	}
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
	if err == nil || !strings.Contains(err.Error(), "missing required case") {
		t.Fatalf("enforced profile error = %v, want missing required case", err)
	}
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

func TestValidateRepositoryRejectsEnforcedProfileWithoutStreamDecoderFact(t *testing.T) {
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
  usage: unsupported
  streaming: unsupported
  strict_loss: unsupported
  best_effort: unsupported
  service_class: unsupported
  continuation: unsupported
generated_field_exemptions: []
`)
	mustWriteFile(t, filepath.Join(profileDir, "semantic.json"), `{}`)
	mustWriteFile(t, filepath.Join(profileDir, "wire.json"), `{}`)

	_, err := ValidateRepository(root)
	if err == nil || !strings.Contains(err.Error(), "missing enforced capability fact") {
		t.Fatalf("missing stream decoder fact error = %v", err)
	}
}

func TestReportRequireAllEnforcedRejectsBootstrapProfiles(t *testing.T) {
	err := (Report{Bootstrap: []Profile{{ID: "example"}}}).RequireAllEnforced()
	if err == nil || !strings.Contains(err.Error(), "example") {
		t.Fatalf("RequireAllEnforced error = %v, want bootstrap profile name", err)
	}
}

func TestValidateRepositoryRejectsEnforcedProfileWithUndeclaredCapability(t *testing.T) {
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
  streaming: unsupported
generated_field_exemptions: []
`)
	mustWriteFile(t, filepath.Join(profileDir, "semantic.json"), `{}`)
	mustWriteFile(t, filepath.Join(profileDir, "wire.json"), `{}`)

	_, err := ValidateRepository(root)
	if err == nil || !strings.Contains(err.Error(), "missing enforced capability fact") {
		t.Fatalf("undeclared enforced capability error = %v", err)
	}
}

func TestValidateRepositoryRejectsMalformedManifest(t *testing.T) {
	root := t.TempDir()
	profileDir := testProfileDir(root)
	mustWriteFile(t, filepath.Join(profileDir, "manifest.yaml"), "version: [\n")

	_, err := ValidateRepository(root)
	if err == nil || !strings.Contains(err.Error(), "manifest is not valid YAML") {
		t.Fatalf("malformed manifest error = %v", err)
	}
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
	if err == nil || !strings.Contains(err.Error(), "manifest is not valid YAML") {
		t.Fatalf("unknown manifest field error = %v", err)
	}
}

func TestValidateRepositoryRejectsMissingDeclaredFixture(t *testing.T) {
	root := t.TempDir()
	profileDir := testProfileDir(root)
	mustWriteManifestWithValidServiceClasses(t, filepath.Join(profileDir, "manifest.yaml"), `version: 1
id: example
provider: example
family: chat
coverage: bootstrap
metadata: metadata.yaml
cases:
  - id: captured-wire-request
    artifacts:
      wire: missing.wire.json
`)
	mustWriteFile(t, filepath.Join(profileDir, "metadata.yaml"), validTestMetadata)

	_, err := ValidateRepository(root)
	if err == nil || !strings.Contains(err.Error(), "references a missing fixture") {
		t.Fatalf("missing fixture error = %v", err)
	}
}

func TestValidateRepositoryRejectsCaseWithoutItsDocumentedArtifact(t *testing.T) {
	root := t.TempDir()
	profileDir := testProfileDir(root)
	mustWriteManifestWithValidServiceClasses(t, filepath.Join(profileDir, "manifest.yaml"), `version: 1
id: example
provider: example
family: chat
coverage: bootstrap
metadata: metadata.yaml
cases:
  - id: semantic-request
    artifacts:
      wire: request.wire.json
`)
	mustWriteFile(t, filepath.Join(profileDir, "metadata.yaml"), validTestMetadata)
	mustWriteFile(t, filepath.Join(profileDir, "request.wire.json"), `{}`)

	_, err := ValidateRepository(root)
	if err == nil || !strings.Contains(err.Error(), "documented semantic artifact") {
		t.Fatalf("wrong case artifact error = %v", err)
	}
}

func TestValidateRepositoryRejectsMissingSourceDate(t *testing.T) {
	root := t.TempDir()
	profileDir := testProfileDir(root)
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
`)
	mustWriteFile(t, filepath.Join(profileDir, "metadata.yaml"), `profile: example
upstream_url: https://example.test/contracts
sdk_version: example-sdk/v1
provenance: synthetic
redactions:
  - credentials
capability_facts:
  streaming: unsupported
generated_field_exemptions: []
`)
	mustWriteFile(t, filepath.Join(profileDir, "request.semantic.json"), `{}`)

	_, err := ValidateRepository(root)
	if err == nil || !strings.Contains(err.Error(), "metadata upstream_date must be YYYY-MM-DD") {
		t.Fatalf("missing source date error = %v", err)
	}
}

func TestValidateRepositoryRejectsTraversalPathWithoutLeakingIt(t *testing.T) {
	root := t.TempDir()
	profileDir := testProfileDir(root)
	traversal := "../../secret-fixture-name"
	mustWriteManifestWithValidServiceClasses(t, filepath.Join(profileDir, "manifest.yaml"), `version: 1
id: example
provider: example
family: chat
coverage: bootstrap
metadata: metadata.yaml
cases:
  - id: captured-wire-request
    artifacts:
      wire: `+traversal+`
`)
	mustWriteFile(t, filepath.Join(profileDir, "metadata.yaml"), validTestMetadata)

	_, err := ValidateRepository(root)
	if err == nil || !strings.Contains(err.Error(), "case has an invalid artifact path") {
		t.Fatalf("traversal path error = %v", err)
	}
	if strings.Contains(err.Error(), traversal) || strings.Contains(err.Error(), "secret-fixture-name") {
		t.Fatalf("traversal input leaked through error: %v", err)
	}
}

func TestValidateRepositoryScansSharedContractFixturesWithoutLeakingBytes(t *testing.T) {
	root := t.TempDir()
	profileDir := testProfileDir(root)
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
`)
	mustWriteFile(t, filepath.Join(profileDir, "metadata.yaml"), validTestMetadata)
	mustWriteFile(t, filepath.Join(profileDir, "request.semantic.json"), `{}`)
	secret := "AKIA" + "1234567890ABCDEF"
	mustWriteFile(t, filepath.Join(filepath.Dir(profileDir), "events.wire"), "token: "+secret)

	_, err := ValidateRepository(root)
	if err == nil || !strings.Contains(err.Error(), "events.wire") {
		t.Fatalf("shared fixture scan error = %v", err)
	}
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
	if err != nil {
		t.Fatal(err)
	}
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
	if err == nil || !strings.Contains(err.Error(), "stream assembly differs") {
		t.Fatalf("stream equivalence error = %v", err)
	}
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
				if err != nil {
					t.Fatalf("validate report: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("validate report error = %v, want %q", err, test.wantErr)
			}
		})
	}
}

func TestRepositoryManifests(t *testing.T) {
	root := filepath.Clean(filepath.Join("..", "..", ".."))
	report, err := ValidateRepository(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateRepositoryManifestReport(report); err != nil {
		t.Fatal(err)
	}
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
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
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
