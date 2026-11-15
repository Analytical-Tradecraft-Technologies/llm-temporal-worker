package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/contractkit"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

const repositoryRoot = "../../../.."

// The test binary doubles as an out-of-process consumer codec: with
// CONTRACTKIT_TEST_CODEC set it serves one roundtrip request and exits.
func TestMain(m *testing.M) {
	switch os.Getenv("CONTRACTKIT_TEST_CODEC") {
	case "go":
		os.Exit(serveGoCodec())
	case "echo":
		if _, err := io.Copy(os.Stdout, os.Stdin); err != nil {
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func serveGoCodec() int {
	targets := map[string]func() any{
		"generate-request":  func() any { return new(llm.GenerateRequestV1) },
		"generate-response": func() any { return new(llm.GenerateResponseV1) },
		"compact-request":   func() any { return new(llm.CompactRequestV1) },
		"compact-response":  func() any { return new(llm.CompactResponseV1) },
		"query-request":     func() any { return new(llm.QueryRequestV1) },
		"query-response":    func() any { return new(llm.QueryResponseV1) },
	}
	target, ok := targets[os.Getenv("LLMTW_CONTRACT")]
	if !ok || os.Getenv("LLMTW_FIXTURE") == "" {
		os.Stderr.WriteString("missing contract or fixture environment\n")
		return 2
	}
	value := target()
	if err := json.NewDecoder(os.Stdin).Decode(value); err != nil {
		os.Stderr.WriteString(err.Error() + "\n")
		return 1
	}
	if err := json.NewEncoder(os.Stdout).Encode(value); err != nil {
		return 1
	}
	return 0
}

func runCommand(t *testing.T, stdin string, args ...string) (string, error) {
	t.Helper()
	var stdout bytes.Buffer
	err := run(args, strings.NewReader(stdin), &stdout, io.Discard)
	return stdout.String(), err
}

func TestRoundTripRunsAnExternalCodec(t *testing.T) {
	t.Setenv("CONTRACTKIT_TEST_CODEC", "go")
	output, err := runCommand(t, "", "roundtrip", "--", os.Args[0])
	if err != nil {
		t.Fatalf("worker codec failed the kit: %v\n%s", err, output)
	}
	if !strings.Contains(output, "fixtures passed against schema digest sha256:") || strings.Contains(output, "FAIL") {
		t.Fatalf("unexpected report:\n%s", output)
	}

	t.Setenv("CONTRACTKIT_TEST_CODEC", "echo")
	output, err = runCommand(t, "", "roundtrip", "--", os.Args[0])
	if !errors.Is(err, errFailed) || !strings.Contains(output, "FAIL negative-") {
		t.Fatalf("a codec accepting every fixture passed: %v\n%s", err, output)
	}
	if _, err := runCommand(t, "", "roundtrip"); err == nil || errors.Is(err, errFailed) {
		t.Fatalf("roundtrip without a command: %v", err)
	}
}

func TestValidateChecksFilesAndStandardInput(t *testing.T) {
	fixture := filepath.Join(repositoryRoot, "golang", "llm", "testdata", "v1", "generate-root.json")
	negative := filepath.Join(repositoryRoot, "golang", "llm", "testdata", "v1", "negative-unknown-field.json")
	if output, err := runCommand(t, "", "validate", "-contract", "generate-request", fixture); err != nil {
		t.Fatalf("valid request rejected: %v\n%s", err, output)
	}
	output, err := runCommand(t, "", "validate", "-contract", "generate-request", fixture, negative)
	if !errors.Is(err, errFailed) || !strings.Contains(output, "FAIL "+negative) || !strings.Contains(output, "ok   "+fixture) {
		t.Fatalf("mixed input: %v\n%s", err, output)
	}
	if _, err := runCommand(t, "{}", "validate", "-contract", "query-request"); !errors.Is(err, errFailed) {
		t.Fatalf("empty query request on standard input: %v", err)
	}
	if _, err := runCommand(t, "{}", "validate"); err == nil || errors.Is(err, errFailed) {
		t.Fatalf("validate without -contract: %v", err)
	}
}

func TestListExportAndKitFlag(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "kit")
	if _, err := runCommand(t, "", "export", "-dir", directory); err != nil {
		t.Fatal(err)
	}
	embedded, err := runCommand(t, "", "list")
	if err != nil {
		t.Fatal(err)
	}
	exported, err := runCommand(t, "", "list", "-kit", directory)
	if err != nil {
		t.Fatal(err)
	}
	if embedded != exported || !strings.Contains(embedded, "generate-root.json\tgenerate-request\tpositive\n") || !strings.Contains(embedded, "\tnegative\n") {
		t.Fatalf("list output differs or is incomplete:\n%s", exported)
	}
	if err := os.WriteFile(filepath.Join(directory, "fixtures", "v1", "generate-root.json"), []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := runCommand(t, "", "list", "-kit", directory); err == nil {
		t.Fatal("altered kit directory loaded")
	}
}

func TestManifestOfTheRepositoryIsTheEmbeddedManifest(t *testing.T) {
	directory := t.TempDir()
	embedded, repository := filepath.Join(directory, "embedded.json"), filepath.Join(directory, "repository.json")
	if _, err := runCommand(t, "", "manifest", "-output", embedded); err != nil {
		t.Fatal(err)
	}
	if _, err := runCommand(t, "", "manifest", "-source-root", repositoryRoot, "-output", repository); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(readFile(t, embedded), readFile(t, repository)) {
		t.Fatal("repository manifest differs from the embedded kit's")
	}
	if _, err := runCommand(t, "", "manifest", "-output", embedded); err == nil {
		t.Fatal("manifest overwrote an existing file")
	}
}

func TestPackageWritesTheVersionedTarballAndDigest(t *testing.T) {
	kit, err := contractkit.Embedded()
	if err != nil {
		t.Fatal(err)
	}
	expect := filepath.Join(t.TempDir(), "contract-kit.json")
	if err := os.WriteFile(expect, kit.ManifestJSON(), 0o644); err != nil {
		t.Fatal(err)
	}
	output := t.TempDir()
	if _, err := runCommand(t, "", "package", "-source-root", repositoryRoot, "-version", "v1.2.3", "-output-dir", output, "-expect", expect); err != nil {
		t.Fatal(err)
	}
	tarball := readFile(t, filepath.Join(output, "llmtw-contract-kit-v1.2.3.tar.gz"))
	sum := sha256.Sum256(tarball)
	if got, want := string(readFile(t, filepath.Join(output, "llmtw-contract-kit-v1.2.3.tar.gz.sha256"))), hex.EncodeToString(sum[:])+"  llmtw-contract-kit-v1.2.3.tar.gz\n"; got != want {
		t.Fatalf("sha256 file %q, want %q", got, want)
	}
	var embeddedTarball bytes.Buffer
	if err := kit.WriteTarball(&embeddedTarball, "v1.2.3"); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(tarball, embeddedTarball.Bytes()) {
		t.Fatal("tarball packaged from the repository differs from the embedded kit's")
	}
	if !bytes.Equal(readFile(t, filepath.Join(output, "contract-kit.json")), kit.ManifestJSON()) {
		t.Fatal("packaged manifest differs")
	}

	if _, err := runCommand(t, "", "package", "-source-root", repositoryRoot, "-version", "v1.2.3", "-output-dir", output); err == nil {
		t.Fatal("package wrote into a non-empty directory")
	}
	var manifest map[string]any
	if err := json.Unmarshal(kit.ManifestJSON(), &manifest); err != nil {
		t.Fatal(err)
	}
	manifest["schema_digest"] = "sha256:" + strings.Repeat("0", 64)
	stale, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(expect, stale, 0o644); err != nil {
		t.Fatal(err)
	}
	_, err = runCommand(t, "", "package", "-source-root", repositoryRoot, "-version", "v1.2.3", "-output-dir", t.TempDir(), "-expect", expect)
	if err == nil || !strings.Contains(err.Error(), "digests do not match") {
		t.Fatalf("package accepted an inconsistent expected manifest: %v", err)
	}
	if _, err := runCommand(t, "", "package", "-source-root", repositoryRoot, "-version", "1.2.3", "-output-dir", t.TempDir()); err == nil {
		t.Fatal("package accepted a version without the v prefix")
	}
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
