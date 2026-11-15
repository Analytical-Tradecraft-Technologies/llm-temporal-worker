package main

import (
	"encoding/base64"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/repotest"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
)

const (
	sourceVerifierTestSourceLimit     = 1 << 20
	sourceVerifierTestOutputLimit     = 16 << 20
	sourceVerifierPreviousOutputLimit = 8 << 20
)

func TestScanContentDetectsRawAndDecodedCredentialFields(t *testing.T) {
	t.Parallel()

	secret := "Bearer " + strings.Repeat("x", 24)
	raw := `{"authorization":"` + secret + `"}`
	tests := []struct {
		name    string
		content string
	}{
		{name: "raw", content: raw},
		{name: "escaped", content: strings.ReplaceAll(raw, `"`, `\\"`)},
		{name: "url encoded", content: url.QueryEscape(raw)},
		{name: "base64 encoded", content: base64.StdEncoding.EncodeToString([]byte(raw))},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if mustScan(t, scanContent, test.content) == nil {
				t.Fatal("scanContent accepted an unredacted credential field")
			}
		})
	}
}

func TestScanContentAllowsExplicitRedactions(t *testing.T) {
	t.Parallel()

	for _, content := range []string{
		`{"authorization":"Bearer redacted"}`,
		`{"api_key":"local-only"}`,
		`password=local-redis-password-0123456789`,
		`mock_api_key=mock-api-key-0123456789`,
		`api_key=test-provider-key-0123456789`,
		`password=not-configured`,
	} {
		if finding := mustScan(t, scanContent, content); finding != nil {
			t.Fatalf("scanContent rejected explicit redaction: %#v", finding)
		}
	}
}

func TestScanContentRejectsRedactionMarkerSubstringCollisions(t *testing.T) {
	t.Parallel()

	for _, content := range []string{
		`api_key=latest-production-credential-0123456789`,
		`password=contest-secret-value-0123456789`,
		`access_token=smock-provider-token-0123456789`,
		`secret_key=dislocal-production-key-0123456789`,
	} {
		if mustScan(t, scanContent, content) == nil {
			t.Fatal("scanContent accepted a credential containing a marker substring")
		}
	}
}

func TestScanCapturedStreamDetectsRawAndDecodedDenyFieldFindings(t *testing.T) {
	t.Parallel()

	raw := "raw provider body leaked: " + strings.Repeat("opaque-", 4)
	for _, output := range []string{
		raw,
		url.QueryEscape(raw),
		base64.StdEncoding.EncodeToString([]byte(raw)),
	} {
		if mustScan(t, scanTestOutput, output) == nil {
			t.Fatal("scanTestOutput accepted a denied-field leak")
		}
	}
}

func TestScanContentFailsClosedWhenDecodedCandidateLimitIsExceeded(t *testing.T) {
	t.Parallel()

	tokens := make([]string, 0, maxCandidates)
	for index := 0; index < maxCandidates; index++ {
		tokens = append(tokens, base64.StdEncoding.EncodeToString([]byte("safe-fixture-value-"+strconv.Itoa(index))))
	}
	content := strings.Join(tokens, " ") + " trailing-safe-value"

	finding, err := scanContent([]byte(content))
	if err == nil {
		t.Fatal("scanContent accepted input whose decoded candidates exceeded the safety bound")
	}
	if finding != nil {
		t.Fatalf("scanContent returned a finding with the candidate-limit error: %#v", finding)
	}
	if !strings.Contains(err.Error(), "decoded candidate limit") {
		t.Fatalf("candidate-limit error = %q", err)
	}
}

func TestScanContentDeduplicatesRepeatedDecodedCandidates(t *testing.T) {
	t.Parallel()

	safeToken := base64.StdEncoding.EncodeToString([]byte("safe-fixture-value-0123456789"))
	content := strings.TrimSpace(strings.Repeat(safeToken+" ", maxCandidates*2))
	if finding, err := scanContent([]byte(content)); err != nil || finding != nil {
		t.Fatalf("scanContent rejected repeated safe candidates: finding=%#v err=%v", finding, err)
	}
}

func TestVerifyScansSourceFixturesAndTestOutputWithoutLeakingPayload(t *testing.T) {
	t.Parallel()

	secret := "Bearer " + strings.Repeat("y", 24)
	raw := `{"authorization":"` + secret + `"}`
	tests := []struct {
		name         string
		fixturePath  string
		fixtureBytes []byte
		testOutput   []byte
		wantLocation string
	}{
		{
			name:         "source",
			fixturePath:  "internal/config.go",
			fixtureBytes: []byte(`const authorization = "` + secret + `"`),
			wantLocation: "internal/config.go",
		},
		{
			name:         "Go test source",
			fixturePath:  "internal/config_test.go",
			fixtureBytes: []byte(`const authorization = "` + secret + `"`),
			wantLocation: "internal/config_test.go",
		},
		{
			name:         "Markdown",
			fixturePath:  "README.md",
			fixtureBytes: []byte(`authorization = "` + secret + `"`),
			wantLocation: "README.md",
		},
		{
			name:         "plain text",
			fixturePath:  "notes.txt",
			fixtureBytes: []byte(`authorization = "` + secret + `"`),
			wantLocation: "notes.txt",
		},
		{
			name:         "Dockerfile",
			fixturePath:  "deploy/Dockerfile.worker",
			fixtureBytes: []byte(`ENV authorization="` + secret + `"`),
			wantLocation: "deploy/Dockerfile.worker",
		},
		{
			name:         "repository root workflow",
			fixturePath:  ".github/workflows/release.yml",
			fixtureBytes: []byte(`api_key: "` + secret + `"`),
			wantLocation: ".github/workflows/release.yml",
		},
		{
			name:         "OCaml implementation source",
			fixturePath:  "ocaml/llm_temporal_worker/lib/provider.ml",
			fixtureBytes: []byte(`let api_key = "` + secret + `"`),
			wantLocation: "ocaml/llm_temporal_worker/lib/provider.ml",
		},
		{
			name:         "OCaml interface source",
			fixturePath:  "ocaml/llm_temporal_worker/lib/provider.mli",
			fixtureBytes: []byte(`val api_key : string` + "\n" + `let api_key = "` + secret + `"`),
			wantLocation: "ocaml/llm_temporal_worker/lib/provider.mli",
		},
		{
			name:         "fixture",
			fixturePath:  "llm/testdata/request.fixture",
			fixtureBytes: []byte(base64.StdEncoding.EncodeToString([]byte(raw))),
			wantLocation: "llm/testdata/request.fixture",
		},
		{
			name:         "test output",
			fixturePath:  "internal/safe.go",
			fixtureBytes: []byte("package internal\n"),
			testOutput:   []byte(url.QueryEscape(raw)),
			wantLocation: "test output",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			writeTestFile(t, root, test.fixturePath, test.fixtureBytes)
			outputPath := ""
			if len(test.testOutput) > 0 {
				outputPath = filepath.Join(root, "test-output.json")
				writeTestFile(t, root, "test-output.json", test.testOutput)
			}

			err := verify(root, outputPath)
			requireRedactedRejection(t, err, "verify accepted an unredacted credential field", secret)
			if !strings.Contains(err.Error(), test.wantLocation) {
				t.Fatalf("verify error %q does not identify %q", err, test.wantLocation)
			}
		})
	}
}

func TestVerifyDetectsRecognizedCredentialPatternsInCheckedInText(t *testing.T) {
	t.Parallel()

	patterns := []struct {
		name  string
		value string
	}{
		{name: "private key", value: "-----BEGIN " + "PRIVATE KEY-----"},
		{name: "AWS access key", value: "AKIA" + strings.Repeat("A", 16)},
		{name: "GitHub token", value: "gh" + "p_" + strings.Repeat("a", 24)},
		{name: "Slack token", value: "xo" + "xb-" + strings.Repeat("a", 12)},
		{name: "OpenAI token", value: "s" + "k-" + strings.Repeat("a", 24)},
		{name: "Anthropic token", value: "s" + "k-ant-" + strings.Repeat("a", 24)},
	}

	for _, pattern := range patterns {
		t.Run(pattern.name, func(t *testing.T) {
			t.Parallel()
			err := verifyFile(t, "README.md", "credential: "+pattern.value+"\n")
			requireRedactedRejection(t, err, "verify accepted recognized credential-like material", pattern.value)
			if !strings.Contains(err.Error(), "README.md") || !strings.Contains(err.Error(), "credential-like material") {
				t.Fatalf("verify error %q does not identify the unsafe text file", err)
			}
		})
	}
}

func TestVerifyScansBoundedTextRegardlessOfExtension(t *testing.T) {
	t.Parallel()

	privateKey := "-----BEGIN " + "PRIVATE KEY-----"
	secret := "Bearer " + strings.Repeat("u", 24)
	configSecret := strings.Repeat("n", 24)
	tests := []struct {
		name    string
		path    string
		content string
	}{
		{name: "PEM", path: "certs/client.pem", content: privateKey},
		{name: "key file", path: "certs/client.key", content: privateKey},
		{name: "netrc", path: ".netrc", content: `machine registry.example login build password ` + configSecret},
		{name: "npmrc", path: ".npmrc", content: `//registry.example/:_authToken=` + configSecret},
		{name: "environment production variant", path: ".env.production", content: `api_key="` + secret + `"`},
		{name: "unquoted environment staging variant", path: ".env.staging", content: `api_key=` + secret},
		{name: "extensionless config", path: "credentials", content: `authorization="` + secret + `"`},
		{name: "unknown text extension", path: "config.runtime", content: `secret_key="` + secret + `"`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			err := verifyFile(t, test.path, test.content)
			requireRedactedRejection(t, err, "verify accepted credential-like material in a text file", secret, configSecret, privateKey)
			if !strings.Contains(err.Error(), test.path) {
				t.Fatalf("verify error %q does not identify %q", err, test.path)
			}
		})
	}
}

func TestVerifyFailsClosedForOversizeTextRegardlessOfExtension(t *testing.T) {
	t.Parallel()

	err := verifyFile(t, "config.runtime", strings.Repeat("x", sourceVerifierTestSourceLimit+1))
	if err == nil {
		t.Fatal("verify accepted oversized text with an unrecognized extension")
	}
	if !strings.Contains(err.Error(), "config.runtime") {
		t.Fatalf("verify error %q does not identify the oversized text file", err)
	}
}

func TestVerifyDetectsUnquotedCredentialLiteralsInExecutableSource(t *testing.T) {
	t.Parallel()

	secret := "productioncredential" + strings.Repeat("9", 16)
	for _, test := range []struct {
		name    string
		path    string
		content string
	}{
		{name: "shell", path: "scripts/deploy.sh", content: "API_KEY=" + secret},
		{name: "Dockerfile", path: "Dockerfile", content: "ENV API_KEY=" + secret},
		{name: "Dockerfile legacy ENV", path: "Dockerfile.legacy", content: "ENV API_KEY " + secret},
		{name: "Makefile", path: "Makefile", content: "API_KEY=" + secret},
		{name: "Makefile immediate assignment", path: "Makefile.immediate", content: "API_KEY := " + secret},
		{name: "Makefile POSIX immediate assignment", path: "Makefile.posix", content: "API_KEY ::= " + secret},
		{name: "Makefile escaped immediate assignment", path: "Makefile.escaped", content: "API_KEY :::= " + secret},
		{name: "Makefile conditional assignment", path: "Makefile.conditional", content: "API_KEY ?= " + secret},
		{name: "Makefile append assignment", path: "Makefile.append", content: "API_KEY += " + secret},
		{name: "Makefile shell assignment", path: "Makefile.shell", content: "API_KEY != " + secret},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			requireRedactedRejection(t, verifyFile(t, test.path, test.content), "verify accepted an unquoted credential literal", secret)
		})
	}
}

func TestVerifyAllowsCredentialVariableWiringInExecutableSource(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		path    string
		content string
	}{
		{path: "scripts/deploy.sh", content: `API_KEY="$API_KEY"`},
		{path: "Dockerfile", content: "ARG API_KEY\nENV API_KEY $API_KEY"},
		{path: "Dockerfile.fixture", content: "ENV API_KEY fixture-docker-key"},
		{path: "Makefile", content: `API_KEY ?= $${API_KEY}`},
		{path: "Makefile.variable", content: `API_KEY ::= $${API_KEY}`},
		{path: "Makefile.fixture", content: `API_KEY += fixture-make-key`},
	} {
		t.Run(test.path, func(t *testing.T) {
			t.Parallel()
			testassert.NoError(t, verifyFile(t, test.path, test.content), "verify rejected credential variable wiring")
		})
	}
}

func TestVerifyAllowsDockerInstructionProse(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name    string
		content string
	}{
		{name: "LABEL", content: `LABEL security="password authentication is disabled"`},
		{name: "RUN", content: `RUN echo "password authentication is disabled"`},
		{name: "comment", content: `# password authentication is disabled`},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			testassert.NoError(t, verifyFile(t, "Dockerfile", test.content), "verify rejected non-ENV Dockerfile prose")
		})
	}
}

func TestVerifySkipsGeneratedBinaryAndVendorArtifacts(t *testing.T) {
	t.Parallel()

	secret := []byte(`authorization = "Bearer ` + strings.Repeat("g", 24) + `"`)
	tests := []struct {
		name    string
		path    string
		content []byte
	}{
		{name: "build output", path: "build/README.md", content: secret},
		{name: "distribution output", path: "dist/release.txt", content: secret},
		{name: "OCaml build output", path: "_build/generated.ml", content: secret},
		{name: "coverage output", path: "coverage/report.md", content: secret},
		{name: "release artifacts", path: "release-artifacts/evidence.json", content: secret},
		{name: "vendored source", path: "vendor/module/README.md", content: secret},
		{name: "node modules", path: "node_modules/module/README.md", content: secret},
		{name: "binary content", path: "assets/image.unknown", content: append([]byte{0x00, 0xff, 0x00}, secret...)},
		{name: "oversized binary content", path: "assets/archive.unknown", content: append([]byte{0x00}, []byte(strings.Repeat("binary", sourceVerifierTestSourceLimit))...)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			writeTestFile(t, root, "safe.go", []byte("package safe\n"))
			writeTestFile(t, root, test.path, test.content)
			if err := verify(root, ""); err != nil {
				t.Fatalf("verify scanned excluded artifact %q: %v", test.path, err)
			}
		})
	}
}

func TestVerifyAllowsTestOutputAboveTheRepositoryFileLimit(t *testing.T) {
	testassert.NoError(t, verifyTestOutput(t, safeTestOutputAboveRepositoryFileLimit(t, "")), "verify rejected benign bounded test output")
}

func TestVerifyDetectsFindingsNearTheTailOfLargeTestStream(t *testing.T) {
	secret := "Bearer " + strings.Repeat("z", 24)
	rawCredential := `{"authorization":"` + secret + `"}`
	tests := []struct {
		name     string
		tail     string
		wantPart string
	}{
		{name: "raw credential field", tail: rawCredential, wantPart: "credential-like denied field"},
		{name: "URL encoded credential field", tail: url.QueryEscape(rawCredential), wantPart: "credential-like denied field"},
		{name: "base64 encoded credential field", tail: base64.StdEncoding.EncodeToString([]byte(rawCredential)), wantPart: "credential-like denied field"},
		{name: "denied field finding", tail: "raw provider body leaked: " + strings.Repeat("opaque-", 4), wantPart: "denied-field leak"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := verifyTestOutput(t, safeTestOutputAboveRepositoryFileLimit(t, "\n"+test.tail))
			requireRedactedRejection(t, err, "verify accepted a denied value at the tail of large test output", secret)
			if !strings.Contains(err.Error(), "test output") || !strings.Contains(err.Error(), test.wantPart) {
				t.Fatalf("verify error %q does not identify the tail leak %q", err, test.wantPart)
			}
		})
	}
}

func TestVerifyDetectsBase64FindingAtTheTailOfGoTestJSONStream(t *testing.T) {
	secret := "Bearer " + strings.Repeat("q", 24)
	rawCredential := `{"authorization":"` + secret + `"}`
	err := verifyTestOutput(t, goTestJSONOutputAboveRepositoryFileLimit(t, base64.StdEncoding.EncodeToString([]byte(rawCredential))))
	requireRedactedRejection(t, err, "verify accepted a base64 credential at the tail of Go JSON test output", secret)
	if !strings.Contains(err.Error(), "test output") || !strings.Contains(err.Error(), "credential-like denied field") {
		t.Fatalf("verify error %q does not identify the base64 tail leak", err)
	}
}

func TestVerifyFailsClosedWhenTestOutputExceedsItsBound(t *testing.T) {
	err := verifyTestOutput(t, []byte(strings.Repeat("safe test output\n", sourceVerifierTestOutputLimit/len("safe test output\n")+1)))
	if err == nil {
		t.Fatal("verify accepted test output above its explicit bound")
	}
	if !strings.Contains(err.Error(), "test output") || !strings.Contains(err.Error(), "file exceeds the verification size limit") {
		t.Fatalf("test output cap failure = %q", err)
	}
}

func safeTestOutputAboveRepositoryFileLimit(t *testing.T, tail string) []byte {
	t.Helper()
	output := []byte(strings.Repeat("safe test output\n", sourceVerifierPreviousOutputLimit/len("safe test output\n")+1) + tail)
	if len(output) <= sourceVerifierPreviousOutputLimit || len(output) > sourceVerifierTestOutputLimit {
		t.Fatalf("test output length = %d, want (%d, %d]", len(output), sourceVerifierPreviousOutputLimit, sourceVerifierTestOutputLimit)
	}
	return output
}

func goTestJSONOutputAboveRepositoryFileLimit(t *testing.T, tail string) []byte {
	t.Helper()
	var output strings.Builder
	for record := 0; output.Len() <= sourceVerifierPreviousOutputLimit; record++ {
		output.WriteString(`{"Time":"2026-07-15T00:00:00Z","Action":"output","Package":"example.test","Test":"TestSafe`)
		output.WriteString(strconv.Itoa(record))
		output.WriteString(`","Output":"safe test output\\n"}` + "\n")
	}
	output.WriteString(`{"Time":"2026-07-15T00:00:01Z","Action":"output","Package":"example.test","Output":`)
	output.WriteString(strconv.Quote(tail))
	output.WriteString("}\n")
	bytes := []byte(output.String())
	if len(bytes) <= sourceVerifierPreviousOutputLimit || len(bytes) > sourceVerifierTestOutputLimit {
		t.Fatalf("Go JSON test output length = %d, want (%d, %d]", len(bytes), sourceVerifierPreviousOutputLimit, sourceVerifierTestOutputLimit)
	}
	return bytes
}

func TestVerifyAllowsCredentialVariableWiringInSource(t *testing.T) {
	t.Parallel()

	testassert.NoError(t, verifyFile(t, "internal/factory.go", `package internal

func configure(password string) {
	_ = struct{ Password string }{Password: password}
}
`), "verify rejected a credential variable reference")
}

func TestVerifyCanInspectOnlyCapturedTestOutput(t *testing.T) {
	t.Parallel()

	output := filepath.Join(t.TempDir(), "go-test.json")
	testassert.NoError(t, os.WriteFile(output, []byte(`{"Action":"pass","Package":"example.test/package"}`), 0o600))
	testassert.NoError(t, verify("", output), "test-output-only verification failed")
	if err := verify("", ""); err == nil {
		t.Fatal("verification accepted no source root and no test output")
	}
}

func TestMakefileComposesBoundedSecurityVerify(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile(filepath.Join(moduleRoot(t), "Makefile"))
	testassert.NoError(t, err)
	makefile := string(data)
	_, target, found := strings.Cut(makefile, "security-verify:")
	if !found {
		t.Fatal("Makefile does not define security-verify")
	}
	for _, expected := range []string{
		"$(GO) test -json ./...",
		"$(GO) run ./tools/sourceverify",
		"$(GO) run ./tools/supplychainverify",
		"$(GO) tool govulncheck",
		"GOTOOLCHAIN=$(SECURITY_GO_TOOLCHAIN)",
		"mktemp",
		"./tools/sourceverify -test-output",
	} {
		if !strings.Contains(target, expected) {
			t.Fatalf("security-verify target does not contain %q", expected)
		}
	}
	for _, externalTool := range []string{"gitleaks", "trivy", "curl", "go install"} {
		if strings.Contains(target, externalTool) {
			t.Fatalf("security-verify target invokes external tool %q", externalTool)
		}
	}
}

func TestSourceScannerIncludesOCamlSourceFiles(t *testing.T) {
	t.Parallel()

	requireSourceCode(t, "ocaml/llm_temporal_worker/lib/client.ml", "ocaml/llm_temporal_worker/lib/client.mli")
}

func TestSourceScannerClassifiesExecutableTextAsSource(t *testing.T) {
	t.Parallel()

	requireSourceCode(t, "Dockerfile", "deploy/Dockerfile.worker", "Makefile", "scripts/check.sh", "scripts/check.py")
	if isSourceCode("README.md") {
		t.Fatal("isSourceCode(README.md) = true, want false")
	}
}

// mustScan runs scan over content and returns its finding, failing the test
// on a scan error.
func mustScan(t *testing.T, scan func([]byte) (*finding, error), content string) *finding {
	t.Helper()
	finding, err := scan([]byte(content))
	testassert.NoError(t, err)
	return finding
}

// verifyFile verifies a new source root holding only content at path.
func verifyFile(t *testing.T, path, content string) error {
	t.Helper()
	root := t.TempDir()
	writeTestFile(t, root, path, []byte(content))
	return verify(root, "")
}

// verifyTestOutput verifies output as captured test output beside a safe
// source root.
func verifyTestOutput(t *testing.T, output []byte) error {
	t.Helper()
	root := t.TempDir()
	writeTestFile(t, root, "internal/safe.go", []byte("package internal\n"))
	writeTestFile(t, root, "test-output.json", output)
	return verify(root, filepath.Join(root, "test-output.json"))
}

// requireRedactedRejection requires err to be a rejection that discloses none
// of the secrets; accepted is the failure when verification passed.
func requireRedactedRejection(t *testing.T, err error, accepted string, secrets ...string) {
	t.Helper()
	if err == nil {
		t.Fatal(accepted)
	}
	for _, secret := range secrets {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("verify leaked credential bytes: %v", err)
		}
	}
}

// requireSourceCode requires every path to be classified as source code.
func requireSourceCode(t *testing.T, paths ...string) {
	t.Helper()
	for _, path := range paths {
		if !isSourceCode(path) {
			t.Fatalf("isSourceCode(%q) = false, want true", path)
		}
	}
}

func writeTestFile(t *testing.T, root, relative string, data []byte) {
	t.Helper()
	path := filepath.Join(root, relative)
	testassert.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	testassert.NoError(t, os.WriteFile(path, data, 0o600))
}

// repositoryRoot and moduleRoot locate the checkout these tests verify.
var (
	repositoryRoot = repotest.RepositoryRoot
	moduleRoot     = repotest.ModuleRoot
)
