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
		`password=local-redis-password-0123456789`,
		`mock_api_key=mock-api-key-0123456789`,
		`api_key=test-provider-key-0123456789`,
		`password=not-configured`,
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

func TestScanContentRejectsRedactionMarkerSubstringCollisions(t *testing.T) {
	t.Parallel()

	for _, content := range []string{
		`api_key=latest-production-credential-0123456789`,
		`password=contest-secret-value-0123456789`,
		`access_token=smock-provider-token-0123456789`,
		`secret_key=dislocal-production-key-0123456789`,
	} {
		finding, err := scanContent([]byte(content))
		if err != nil {
			t.Fatal(err)
		}
		if finding == nil {
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
			root := t.TempDir()
			writeTestFile(t, root, "README.md", []byte("credential: "+pattern.value+"\n"))

			err := verify(root, "")
			if err == nil {
				t.Fatal("verify accepted recognized credential-like material")
			}
			if strings.Contains(err.Error(), pattern.value) {
				t.Fatalf("verify leaked credential bytes: %v", err)
			}
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
			root := t.TempDir()
			writeTestFile(t, root, test.path, []byte(test.content))
			err := verify(root, "")
			if err == nil {
				t.Fatal("verify accepted credential-like material in a text file")
			}
			if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), configSecret) || strings.Contains(err.Error(), privateKey) {
				t.Fatalf("verify leaked credential bytes: %v", err)
			}
			if !strings.Contains(err.Error(), test.path) {
				t.Fatalf("verify error %q does not identify %q", err, test.path)
			}
		})
	}
}

func TestVerifyFailsClosedForOversizeTextRegardlessOfExtension(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeTestFile(t, root, "config.runtime", []byte(strings.Repeat("x", sourceVerifierTestSourceLimit+1)))
	err := verify(root, "")
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
			root := t.TempDir()
			writeTestFile(t, root, test.path, []byte(test.content))
			err := verify(root, "")
			if err == nil {
				t.Fatal("verify accepted an unquoted credential literal")
			}
			if strings.Contains(err.Error(), secret) {
				t.Fatalf("verify leaked credential bytes: %v", err)
			}
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
			root := t.TempDir()
			writeTestFile(t, root, test.path, []byte(test.content))
			if err := verify(root, ""); err != nil {
				t.Fatalf("verify rejected credential variable wiring: %v", err)
			}
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
			root := t.TempDir()
			writeTestFile(t, root, "Dockerfile", []byte(test.content))
			if err := verify(root, ""); err != nil {
				t.Fatalf("verify rejected non-ENV Dockerfile prose: %v", err)
			}
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
	root := t.TempDir()
	writeTestFile(t, root, "internal/safe.go", []byte("package internal\n"))
	outputPath := filepath.Join(root, "test-output.json")
	writeTestFile(t, root, "test-output.json", safeTestOutputAboveRepositoryFileLimit(t, ""))

	if err := verify(root, outputPath); err != nil {
		t.Fatalf("verify rejected benign bounded test output: %v", err)
	}
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

func TestVerifyDetectsBase64FindingAtTheTailOfGoTestJSONStream(t *testing.T) {
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

func TestVerifyCanInspectOnlyCapturedTestOutput(t *testing.T) {
	t.Parallel()

	output := filepath.Join(t.TempDir(), "go-test.json")
	if err := os.WriteFile(output, []byte(`{"Action":"pass","Package":"example.test/package"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := verify("", output); err != nil {
		t.Fatalf("test-output-only verification failed: %v", err)
	}
	if err := verify("", ""); err == nil {
		t.Fatal("verification accepted no source root and no test output")
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

	for _, path := range []string{
		"ocaml/llm_temporal_worker/lib/client.ml",
		"ocaml/llm_temporal_worker/lib/client.mli",
	} {
		if !isSourceCode(path) {
			t.Fatalf("isSourceCode(%q) = false, want true", path)
		}
	}
}

func TestSourceScannerClassifiesExecutableTextAsSource(t *testing.T) {
	t.Parallel()

	for _, path := range []string{
		"Dockerfile",
		"deploy/Dockerfile.worker",
		"Makefile",
		"scripts/check.sh",
		"scripts/check.py",
	} {
		if !isSourceCode(path) {
			t.Fatalf("isSourceCode(%q) = false, want true", path)
		}
	}
	if isSourceCode("README.md") {
		t.Fatal("isSourceCode(README.md) = true, want false")
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
