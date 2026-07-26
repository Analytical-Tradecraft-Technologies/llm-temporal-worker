package main

import (
	"encoding/base64"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const (
	sourceVerifierTestSourceLimit = 1 << 20
	sourceVerifierTestOutputLimit = 8 << 20
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
			finding, err := scanContent([]byte(test.content))
			if err != nil {
				t.Fatal(err)
			}
			if finding == nil {
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
	} {
		finding, err := scanContent([]byte(content))
		if err != nil {
			t.Fatal(err)
		}
		if finding != nil {
			t.Fatalf("scanContent rejected explicit redaction: %#v", finding)
		}
	}
}

func TestScanTestOutputDetectsRawAndDecodedDenyFieldLeaks(t *testing.T) {
	t.Parallel()

	raw := "raw provider body leaked: " + strings.Repeat("opaque-", 4)
	for _, output := range []string{
		raw,
		url.QueryEscape(raw),
		base64.StdEncoding.EncodeToString([]byte(raw)),
	} {
		finding, err := scanTestOutput([]byte(output))
		if err != nil {
			t.Fatal(err)
		}
		if finding == nil {
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
			if err == nil {
				t.Fatal("verify accepted an unredacted credential field")
			}
			if strings.Contains(err.Error(), secret) {
				t.Fatalf("verify leaked credential bytes: %v", err)
			}
			if !strings.Contains(err.Error(), test.wantLocation) {
				t.Fatalf("verify error %q does not identify %q", err, test.wantLocation)
			}
		})
	}
}

func TestVerifyAllowsTestOutputAboveTheRepositoryFileLimit(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, root, "internal/safe.go", []byte("package internal\n"))
	outputPath := filepath.Join(root, "test-output.json")
	writeTestFile(t, root, "test-output.json", safeTestOutputAboveRepositoryFileLimit(t, ""))

	if err := verify(root, outputPath); err != nil {
		t.Fatalf("verify rejected benign bounded test output: %v", err)
	}
}

func TestVerifyDetectsLeaksNearTheTailOfLargeTestOutput(t *testing.T) {
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
		{name: "denied field leak", tail: "raw provider body leaked: " + strings.Repeat("opaque-", 4), wantPart: "denied-field leak"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			writeTestFile(t, root, "internal/safe.go", []byte("package internal\n"))
			outputPath := filepath.Join(root, "test-output.json")
			writeTestFile(t, root, "test-output.json", safeTestOutputAboveRepositoryFileLimit(t, "\n"+test.tail))

			err := verify(root, outputPath)
			if err == nil {
				t.Fatal("verify accepted a denied value at the tail of large test output")
			}
			if strings.Contains(err.Error(), secret) {
				t.Fatalf("verify leaked credential bytes: %v", err)
			}
			if !strings.Contains(err.Error(), "test output") || !strings.Contains(err.Error(), test.wantPart) {
				t.Fatalf("verify error %q does not identify the tail leak %q", err, test.wantPart)
			}
		})
	}
}

func TestVerifyDetectsBase64LeakAtTheTailOfGoTestJSONOutput(t *testing.T) {
	secret := "Bearer " + strings.Repeat("q", 24)
	rawCredential := `{"authorization":"` + secret + `"}`
	root := t.TempDir()
	writeTestFile(t, root, "internal/safe.go", []byte("package internal\n"))
	outputPath := filepath.Join(root, "test-output.json")
	writeTestFile(t, root, "test-output.json", goTestJSONOutputAboveRepositoryFileLimit(t, base64.StdEncoding.EncodeToString([]byte(rawCredential))))

	err := verify(root, outputPath)
	if err == nil {
		t.Fatal("verify accepted a base64 credential at the tail of Go JSON test output")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("verify leaked credential bytes: %v", err)
	}
	if !strings.Contains(err.Error(), "test output") || !strings.Contains(err.Error(), "credential-like denied field") {
		t.Fatalf("verify error %q does not identify the base64 tail leak", err)
	}
}

func TestVerifyFailsClosedWhenTestOutputExceedsItsBound(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, root, "internal/safe.go", []byte("package internal\n"))
	outputPath := filepath.Join(root, "test-output.json")
	writeTestFile(t, root, "test-output.json", []byte(strings.Repeat("safe test output\n", sourceVerifierTestOutputLimit/len("safe test output\n")+1)))

	err := verify(root, outputPath)
	if err == nil {
		t.Fatal("verify accepted test output above its explicit bound")
	}
	if !strings.Contains(err.Error(), "test output") || !strings.Contains(err.Error(), "file exceeds the verification size limit") {
		t.Fatalf("test output cap failure = %q", err)
	}
}

func safeTestOutputAboveRepositoryFileLimit(t *testing.T, tail string) []byte {
	t.Helper()
	output := []byte(strings.Repeat("safe test output\n", sourceVerifierTestSourceLimit/len("safe test output\n")+1) + tail)
	if len(output) <= sourceVerifierTestSourceLimit || len(output) > sourceVerifierTestOutputLimit {
		t.Fatalf("test output length = %d, want (%d, %d]", len(output), sourceVerifierTestSourceLimit, sourceVerifierTestOutputLimit)
	}
	return output
}

func goTestJSONOutputAboveRepositoryFileLimit(t *testing.T, tail string) []byte {
	t.Helper()
	var output strings.Builder
	for record := 0; output.Len() <= sourceVerifierTestSourceLimit; record++ {
		output.WriteString(`{"Time":"2026-07-15T00:00:00Z","Action":"output","Package":"example.test","Test":"TestSafe`)
		output.WriteString(strconv.Itoa(record))
		output.WriteString(`","Output":"safe test output\\n"}` + "\n")
	}
	output.WriteString(`{"Time":"2026-07-15T00:00:01Z","Action":"output","Package":"example.test","Output":`)
	output.WriteString(strconv.Quote(tail))
	output.WriteString("}\n")
	bytes := []byte(output.String())
	if len(bytes) <= sourceVerifierTestSourceLimit || len(bytes) > sourceVerifierTestOutputLimit {
		t.Fatalf("Go JSON test output length = %d, want (%d, %d]", len(bytes), sourceVerifierTestSourceLimit, sourceVerifierTestOutputLimit)
	}
	return bytes
}

func TestVerifyAllowsCredentialVariableWiringInSource(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeTestFile(t, root, "internal/factory.go", []byte(`package internal

func configure(password string) {
	_ = struct{ Password string }{Password: password}
}
`))
	if err := verify(root, ""); err != nil {
		t.Fatalf("verify rejected a credential variable reference: %v", err)
	}
}

func TestMakefileComposesBoundedSecurityVerify(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile(filepath.Join(moduleRoot(t), "Makefile"))
	if err != nil {
		t.Fatal(err)
	}
	makefile := string(data)
	_, target, found := strings.Cut(makefile, "security-verify:")
	if !found {
		t.Fatal("Makefile does not define security-verify")
	}
	for _, expected := range []string{
		"$(GO) test -json ./...",
		"$(GO) run ./tools/sourceverify",
		"$(GO) run ./tools/supplychainverify",
		"golang.org/x/vuln/cmd/govulncheck@v1.6.0",
		"GOTOOLCHAIN=$(SECURITY_GO_TOOLCHAIN)",
		"mktemp",
		"./tools/sourceverify -root ..",
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

	for _, path := range []string{
		"ocaml/llm_temporal_worker/lib/client.ml",
		"ocaml/llm_temporal_worker/lib/client.mli",
	} {
		if !shouldScanSource(path) {
			t.Fatalf("shouldScanSource(%q) = false, want true", path)
		}
		if !isSourceCode(path) {
			t.Fatalf("isSourceCode(%q) = false, want true", path)
		}
	}
}

func writeTestFile(t *testing.T, root, relative string, data []byte) {
	t.Helper()
	path := filepath.Join(root, relative)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	directory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(directory, ".git")); err == nil {
			return directory
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			t.Fatal("repository root with .git not found")
		}
		directory = parent
	}
}

func moduleRoot(t *testing.T) string {
	t.Helper()
	return filepath.Join(repositoryRoot(t), "golang")
}
