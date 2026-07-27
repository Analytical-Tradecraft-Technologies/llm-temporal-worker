// Command releaseverify records and validates a redacted, local release
// evidence bundle. It never contacts a provider, signs, publishes, or pushes
// an image; image construction and scanner execution are deliberately owned by
// the caller that supplies the already-generated artifacts.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	pathpkg "path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	jsonschema "github.com/santhosh-tekuri/jsonschema/v6"
)

const (
	evidenceSchemaVersion = 1
	schemaResourceURL     = "urn:llmtw:release-evidence:v1"
	artifactSchemaURL     = "urn:llmtw:release-evidence-artifact:v1"
	maxArtifactBytes      = 32 * 1024 * 1024
	maxOCIExpandedBytes   = 1024 * 1024 * 1024
	maxOCIMetadataBytes   = 1024 * 1024
)

var (
	digestPattern     = regexp.MustCompile(`^[a-f0-9]{64}$`)
	revisionPattern   = regexp.MustCompile(`^[a-f0-9]{40,64}$`)
	safeDNSLabel      = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$`)
	numericDottedHost = regexp.MustCompile(`^[0-9]+(?:\.[0-9]+)+$`)
	safeHTTPSPath     = regexp.MustCompile(`^[A-Za-z0-9._~!$&'()*+,;=:@/-]*$`)

	privateKeyPattern      = regexp.MustCompile(`-----BEGIN(?: [A-Z]+)? PRIVATE KEY-----`)
	awsAccessKeyPattern    = regexp.MustCompile(`\bAKIA[0-9A-Z]{16}\b`)
	githubTokenPattern     = regexp.MustCompile(`\b(?:gh[pousr]_[A-Za-z0-9_]{20,}|github_pat_[A-Za-z0-9_]{20,})\b`)
	slackTokenPattern      = regexp.MustCompile(`\bxox[baprs]-[A-Za-z0-9-]{10,}\b`)
	openAITokenPattern     = regexp.MustCompile(`\bsk-(?:proj-)?[A-Za-z0-9_-]{20,}\b`)
	anthropicTokenPattern  = regexp.MustCompile(`\bsk-ant-[A-Za-z0-9_-]{20,}\b`)
	credentialFieldPattern = regexp.MustCompile(
		`(?i)(?:authorization|api[_-]?key|access[_-]?token|secret[_-]?key|password)\s*(?:\\?["']\s*)?[:=]\s*(?:\\?["']\s*)?(?:bearer\s+)?[A-Za-z0-9_./=+-]{8,}`,
	)
)

var requiredArtifacts = []string{
	"test_summary",
	"race_summary",
	"fuzz_summary",
	"benchmark_summary",
	"fixture_manifest",
	"redis_summary",
	"temporal_summary",
	"compose_summary",
	"redis_log",
	"temporal_log",
	"compose_log",
	"rendered_manifests",
	"dependency_license",
	"vulnerability_results",
	"sbom",
	"image_scan",
}

// Legacy v1 bundles were recorded before benchmark_summary was introduced.
// Keep them verifiable while requiring the benchmark for every newly recorded
// bundle through requiredArtifacts and artifactArguments.mapForRequired.
var legacyRequiredArtifacts = []string{
	"test_summary",
	"race_summary",
	"fuzz_summary",
	"fixture_manifest",
	"redis_summary",
	"temporal_summary",
	"compose_summary",
	"redis_log",
	"temporal_log",
	"compose_log",
	"rendered_manifests",
	"dependency_license",
	"vulnerability_results",
	"sbom",
	"image_scan",
}

var canonicalArtifactPaths = map[string]string{
	"test_summary":          "test-summary.json",
	"race_summary":          "race-summary.json",
	"fuzz_summary":          "fuzz-summary.json",
	"benchmark_summary":     "benchmark-summary.json",
	"fixture_manifest":      "fixture-manifest.json",
	"redis_summary":         "redis-summary.json",
	"temporal_summary":      "temporal-summary.json",
	"compose_summary":       "compose-summary.json",
	"redis_log":             "redis-log.json",
	"temporal_log":          "temporal-log.json",
	"compose_log":           "compose-log.json",
	"rendered_manifests":    "rendered-manifests.json",
	"dependency_license":    "dependencies.json",
	"vulnerability_results": "vulnerabilities.json",
	"sbom":                  "sbom.cdx.json",
	"image_scan":            "image-scan.json",
}

var summaryArtifacts = map[string]struct{}{
	"test_summary":          {},
	"race_summary":          {},
	"fuzz_summary":          {},
	"benchmark_summary":     {},
	"fixture_manifest":      {},
	"redis_summary":         {},
	"temporal_summary":      {},
	"compose_summary":       {},
	"redis_log":             {},
	"temporal_log":          {},
	"compose_log":           {},
	"rendered_manifests":    {},
	"dependency_license":    {},
	"vulnerability_results": {},
}

var logArtifactServices = map[string]string{
	"redis_log":    "redis",
	"temporal_log": "temporal",
	"compose_log":  "compose",
}

var logEventsByArtifact = map[string]map[string]struct{}{
	"redis_log": {
		"redacted_line":     {},
		"redis_ready":       {},
		"redis_initialized": {},
		"redis_loading":     {},
	},
	"temporal_log": {
		"redacted_line":           {},
		"temporal_started":        {},
		"temporal_ready":          {},
		"temporal_serving":        {},
		"temporal_database_ready": {},
	},
	"compose_log": {
		"redacted_line":           {},
		"redis_ready":             {},
		"redis_initialized":       {},
		"redis_loading":           {},
		"temporal_started":        {},
		"temporal_ready":          {},
		"temporal_serving":        {},
		"temporal_database_ready": {},
	},
}

var redisRuntimeEvents = map[string]struct{}{
	"redis_ready":       {},
	"redis_initialized": {},
	"redis_loading":     {},
}

var temporalRuntimeEvents = map[string]struct{}{
	"temporal_started":        {},
	"temporal_ready":          {},
	"temporal_serving":        {},
	"temporal_database_ready": {},
}

type evidence struct {
	SchemaVersion int                         `json:"schema_version"`
	GeneratedAt   string                      `json:"generated_at"`
	Source        source                      `json:"source"`
	Image         image                       `json:"image"`
	Artifacts     map[string]evidenceArtifact `json:"artifacts"`
}

type source struct {
	Repository string `json:"repository"`
	Revision   string `json:"revision"`
}

type image struct {
	Reference string `json:"reference"`
	Digest    string `json:"digest"`
}

type evidenceArtifact struct {
	Path     string `json:"path"`
	Bytes    int64  `json:"bytes"`
	SHA256   string `json:"sha256"`
	Redacted bool   `json:"redacted"`
}

type fixtureManifest struct {
	Version  int             `json:"version"`
	Fixtures []fixtureRecord `json:"fixtures"`
}

type fixtureRecord struct {
	Profile        string `json:"profile"`
	UpstreamURL    string `json:"upstream_url"`
	UpstreamDate   string `json:"upstream_date"`
	ManifestSHA256 string `json:"manifest_sha256"`
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "release evidence verification failed:", err)
		os.Exit(1)
	}
}

func run(args []string, stdout io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: releaseverify <record|verify> [flags]")
	}
	switch args[0] {
	case "record":
		return runRecord(args[1:], stdout)
	case "verify":
		return runVerify(args[1:], stdout)
	case "layout-digest":
		return runLayoutDigest(args[1:], stdout)
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func runLayoutDigest(args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("layout-digest", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	layoutPath := flags.String("layout", "", "temporary OCI image layout directory")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *layoutPath == "" {
		return errors.New("layout-digest requires -layout")
	}
	digest, err := inspectOCILayout(*layoutPath)
	if err != nil {
		return err
	}
	fmt.Fprintln(stdout, digest)
	return nil
}

func runVerify(args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("verify", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	schemaPath := flags.String("schema", "", "evidence JSON Schema path")
	artifactDir := flags.String("artifact-dir", "", "directory containing evidence artifacts")
	evidencePath := flags.String("evidence", "", "machine-readable evidence record")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *schemaPath == "" || *artifactDir == "" || *evidencePath == "" {
		return errors.New("verify requires -schema, -artifact-dir, and -evidence")
	}
	if err := verify(*schemaPath, *artifactDir, *evidencePath); err != nil {
		return err
	}
	fmt.Fprintln(stdout, "release evidence verified")
	return nil
}

func runRecord(args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("record", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	schemaPath := flags.String("schema", "", "evidence JSON Schema path")
	artifactDir := flags.String("artifact-dir", "", "directory containing evidence artifacts")
	outputPath := flags.String("output", "", "evidence record output path")
	repository := flags.String("repository", "", "HTTPS source repository URL")
	revision := flags.String("revision", "", "source revision SHA")
	imageReference := flags.String("image-reference", "", "local image reference")
	imageDigest := flags.String("image-digest", "", "final local image digest")
	var artifacts artifactArguments
	flags.Var(&artifacts, "artifact", "artifact reference in the form name=relative-path; repeat for every required artifact")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *schemaPath == "" || *artifactDir == "" || *outputPath == "" || *repository == "" || *revision == "" || *imageReference == "" || *imageDigest == "" {
		return errors.New("record requires -schema, -artifact-dir, -output, -repository, -revision, -image-reference, -image-digest, and every -artifact")
	}

	artifactDirectory, err := absoluteDirectory(*artifactDir)
	if err != nil {
		return err
	}
	artifactMap, err := artifacts.mapForRequired()
	if err != nil {
		return err
	}
	if err := validateCanonicalArtifactPaths(artifactMap); err != nil {
		return err
	}
	evidenceArtifacts := make(map[string]evidenceArtifact, len(requiredArtifacts))
	paths := make(map[string]string, len(requiredArtifacts))
	for _, name := range requiredArtifacts {
		relativePath := artifactMap[name]
		artifactPath, err := secureArtifactPath(artifactDirectory, relativePath)
		if err != nil {
			return fmt.Errorf("artifact %q: %w", name, err)
		}
		info, err := os.Lstat(artifactPath)
		if err != nil || info.Size() <= 0 || info.Size() > maxArtifactBytes {
			return fmt.Errorf("artifact %q has an invalid byte length", name)
		}
		data, err := os.ReadFile(artifactPath)
		if err != nil {
			return fmt.Errorf("artifact %q cannot be read", name)
		}
		if err := rejectSecretLikeContent(filepath.Base(artifactPath), data); err != nil {
			return err
		}
		paths[name] = artifactPath
		evidenceArtifacts[name] = evidenceArtifact{
			Path:     filepath.ToSlash(filepath.Clean(relativePath)),
			Bytes:    int64(len(data)),
			SHA256:   sha256Hex(data),
			Redacted: true,
		}
	}
	safeOutputPath, err := secureOutputPath(artifactDirectory, *outputPath)
	if err != nil {
		return err
	}
	for name, path := range paths {
		if safeOutputPath == path {
			return fmt.Errorf("evidence output path must not overwrite required artifact %q", name)
		}
	}
	if safeOutputPath != filepath.Join(artifactDirectory, "evidence.json") {
		return errors.New("evidence output path must be artifact-dir/evidence.json")
	}
	expectedPaths := make([]string, 0, len(paths)+1)
	expectedPaths = append(expectedPaths, safeOutputPath)
	for _, path := range paths {
		expectedPaths = append(expectedPaths, path)
	}
	if err := validateArtifactDirectoryContents(artifactDirectory, expectedPaths...); err != nil {
		return err
	}
	record := evidence{
		SchemaVersion: evidenceSchemaVersion,
		GeneratedAt:   time.Now().UTC().Format(time.RFC3339),
		Source:        source{Repository: *repository, Revision: *revision},
		Image:         image{Reference: *imageReference, Digest: *imageDigest},
		Artifacts:     evidenceArtifacts,
	}
	if err := validateEvidenceMetadata(record); err != nil {
		return err
	}
	if err := validateSummaryArtifacts(*schemaPath, paths, requiredArtifacts); err != nil {
		return err
	}
	if err := validateFixtureManifest(paths["fixture_manifest"]); err != nil {
		return err
	}
	if err := validateCycloneDXSBOM(paths["sbom"], record.Image); err != nil {
		return err
	}
	if err := validateTrivyScan(paths["image_scan"], record.Image); err != nil {
		return err
	}
	data, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal evidence record: %w", err)
	}
	temporary, err := os.CreateTemp(filepath.Dir(safeOutputPath), "."+filepath.Base(safeOutputPath)+".tmp-*")
	if err != nil {
		return errors.New("cannot create private evidence record")
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return errors.New("cannot protect private evidence record")
	}
	if _, err := temporary.Write(append(data, '\n')); err != nil {
		temporary.Close()
		return errors.New("cannot write private evidence record")
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return errors.New("cannot sync private evidence record")
	}
	if err := temporary.Close(); err != nil {
		return errors.New("cannot close private evidence record")
	}
	if err := verifyTemporaryRecord(*schemaPath, artifactDirectory, temporaryPath); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, safeOutputPath); err != nil {
		return errors.New("cannot atomically publish evidence record")
	}
	fmt.Fprintln(stdout, "release evidence recorded")
	return nil
}

func verify(schemaPath, artifactDir, evidencePath string) error {
	return verifyRecord(schemaPath, artifactDir, evidencePath, true)
}

func verifyTemporaryRecord(schemaPath, artifactDir, evidencePath string) error {
	return verifyRecord(schemaPath, artifactDir, evidencePath, false)
}

func verifyRecord(schemaPath, artifactDir, evidencePath string, requireCanonicalPath bool) error {
	artifactDirectory, err := absoluteDirectory(artifactDir)
	if err != nil {
		return err
	}
	safeEvidencePath, err := secureExistingEvidencePath(artifactDirectory, evidencePath)
	if err != nil {
		return err
	}
	if requireCanonicalPath && safeEvidencePath != filepath.Join(artifactDirectory, "evidence.json") {
		return errors.New("evidence record path must be artifact-dir/evidence.json")
	}
	schemaData, err := os.ReadFile(schemaPath)
	if err != nil {
		return errors.New("cannot read evidence schema")
	}
	evidenceData, err := os.ReadFile(safeEvidencePath)
	if err != nil {
		return errors.New("cannot read evidence record")
	}
	if err := rejectSecretLikeContent(filepath.Base(safeEvidencePath), evidenceData); err != nil {
		return err
	}
	if err := validateEvidenceProvenanceURL(evidenceData); err != nil {
		return err
	}
	if err := validateSchema(schemaData, evidenceData); err != nil {
		return err
	}
	var record evidence
	if err := json.Unmarshal(evidenceData, &record); err != nil {
		return errors.New("evidence record is not valid JSON")
	}
	if err := validateEvidenceMetadata(record); err != nil {
		return err
	}
	artifactNames := requiredArtifactsForRecord(record)

	paths := make(map[string]string, len(artifactNames))
	seenPaths := make(map[string]string, len(artifactNames))
	for _, name := range artifactNames {
		artifact, ok := record.Artifacts[name]
		if !ok {
			return fmt.Errorf("evidence record is missing required artifact %q", name)
		}
		if !artifact.Redacted {
			return fmt.Errorf("artifact %q is not marked redacted", name)
		}
		if !digestPattern.MatchString(artifact.SHA256) {
			return fmt.Errorf("artifact %q has invalid sha256", name)
		}
		if artifact.Bytes <= 0 || artifact.Bytes > maxArtifactBytes {
			return fmt.Errorf("artifact %q has an invalid byte length", name)
		}
		if artifact.Path != canonicalArtifactPaths[name] {
			return fmt.Errorf("artifact %q must use canonical path %q", name, canonicalArtifactPaths[name])
		}
		path, err := secureArtifactPath(artifactDirectory, artifact.Path)
		if err != nil {
			return fmt.Errorf("artifact %q: %w", name, err)
		}
		if previous, duplicate := seenPaths[path]; duplicate {
			return fmt.Errorf("artifacts %q and %q reference the same file", previous, name)
		}
		seenPaths[path] = name
		data, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("artifact %q cannot be read", name)
		}
		if actual := sha256Hex(data); actual != artifact.SHA256 {
			return fmt.Errorf("artifact %q sha256 does not match evidence", name)
		}
		if int64(len(data)) != artifact.Bytes {
			return fmt.Errorf("artifact %q byte length does not match evidence", name)
		}
		if err := rejectSecretLikeContent(filepath.Base(path), data); err != nil {
			return err
		}
		paths[name] = path
	}
	if len(record.Artifacts) != len(artifactNames) {
		return errors.New("evidence record has an unexpected artifact entry")
	}
	expectedPaths := make([]string, 0, len(paths)+1)
	expectedPaths = append(expectedPaths, safeEvidencePath)
	for _, path := range paths {
		expectedPaths = append(expectedPaths, path)
	}
	if err := validateArtifactDirectoryContents(artifactDirectory, expectedPaths...); err != nil {
		return err
	}
	if err := validateSummaryArtifacts(schemaPath, paths, artifactNames); err != nil {
		return err
	}
	if err := validateFixtureManifest(paths["fixture_manifest"]); err != nil {
		return err
	}
	if err := validateCycloneDXSBOM(paths["sbom"], record.Image); err != nil {
		return err
	}
	if err := validateTrivyScan(paths["image_scan"], record.Image); err != nil {
		return err
	}
	return nil
}

func validateSchema(schemaData, evidenceData []byte) error {
	schemaDocument, err := jsonschema.UnmarshalJSON(bytes.NewReader(schemaData))
	if err != nil {
		return errors.New("evidence schema is not valid JSON")
	}
	compiler := jsonschema.NewCompiler()
	compiler.UseLoader(noRemoteLoader{})
	if err := compiler.AddResource(schemaResourceURL, schemaDocument); err != nil {
		return fmt.Errorf("cannot load evidence schema: %w", err)
	}
	compiled, err := compiler.Compile(schemaResourceURL)
	if err != nil {
		return fmt.Errorf("cannot compile evidence schema: %w", err)
	}
	instance, err := jsonschema.UnmarshalJSON(bytes.NewReader(evidenceData))
	if err != nil {
		return errors.New("evidence record is not valid JSON")
	}
	if err := compiled.Validate(instance); err != nil {
		return fmt.Errorf("evidence record does not satisfy schema: %w", err)
	}
	return nil
}

func compileArtifactSchema(evidenceSchemaPath string) (*jsonschema.Schema, error) {
	artifactSchemaPath := filepath.Join(filepath.Dir(evidenceSchemaPath), "artifact.schema.json")
	data, err := os.ReadFile(artifactSchemaPath)
	if err != nil {
		return nil, errors.New("cannot read release evidence artifact schema")
	}
	document, err := jsonschema.UnmarshalJSON(bytes.NewReader(data))
	if err != nil {
		return nil, errors.New("release evidence artifact schema is not valid JSON")
	}
	compiler := jsonschema.NewCompiler()
	compiler.UseLoader(noRemoteLoader{})
	if err := compiler.AddResource(artifactSchemaURL, document); err != nil {
		return nil, fmt.Errorf("cannot load release evidence artifact schema: %w", err)
	}
	compiled, err := compiler.Compile(artifactSchemaURL)
	if err != nil {
		return nil, fmt.Errorf("cannot compile release evidence artifact schema: %w", err)
	}
	return compiled, nil
}

func validateSummaryArtifact(schema *jsonschema.Schema, name string, data []byte) error {
	instance, err := jsonschema.UnmarshalJSON(bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("artifact %q summary is not valid JSON", name)
	}
	if err := schema.Validate(instance); err != nil {
		return fmt.Errorf("artifact %q summary does not satisfy the allowlisted schema: %w", name, err)
	}
	document, ok := instance.(map[string]any)
	if !ok {
		return fmt.Errorf("artifact %q summary must be an object", name)
	}
	kind, _ := document["kind"].(string)
	if kind != name {
		return fmt.Errorf("artifact %q summary kind is %q", name, kind)
	}
	return nil
}

func validateSummaryArtifacts(schemaPath string, paths map[string]string, artifactNames []string) error {
	schema, err := compileArtifactSchema(schemaPath)
	if err != nil {
		return err
	}
	for _, name := range artifactNames {
		if _, ok := summaryArtifacts[name]; !ok {
			continue
		}
		path, ok := paths[name]
		if !ok {
			return fmt.Errorf("missing summary artifact %q", name)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("cannot read summary artifact %q", name)
		}
		if err := validateSummaryProvenanceURLs(name, data); err != nil {
			return err
		}
		if err := validateSummaryArtifact(schema, name, data); err != nil {
			return err
		}
		if _, isLogEvidence := logArtifactServices[name]; isLogEvidence {
			if err := validateRedactedLogArtifact(name, data); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateEvidenceProvenanceURL(data []byte) error {
	var document struct {
		Source struct {
			Repository string `json:"repository"`
		} `json:"source"`
	}
	if err := json.Unmarshal(data, &document); err != nil {
		// The schema validator owns malformed evidence diagnostics; this preflight
		// only prevents unsafe values from reaching schema errors that may echo
		// them.
		return nil
	}
	if !isSafeHTTPSURL(document.Source.Repository) {
		return errors.New("evidence source repository must be an HTTPS URL")
	}
	return nil
}

func validateSummaryProvenanceURLs(name string, data []byte) error {
	switch name {
	case "fixture_manifest":
		var document struct {
			Fixtures []struct {
				UpstreamURL string `json:"upstream_url"`
			} `json:"fixtures"`
		}
		if err := json.Unmarshal(data, &document); err != nil {
			return nil
		}
		for _, fixture := range document.Fixtures {
			if !isSafeHTTPSURL(fixture.UpstreamURL) {
				return errors.New("fixture manifest evidence has an unsafe upstream URL")
			}
		}
	case "dependency_license":
		if err := validateDependencyLicenseArtifact(data); err != nil {
			return err
		}
	}
	return nil
}

func validateDependencyLicenseArtifact(data []byte) error {
	var document struct {
		DirectModules []struct {
			Source string `json:"source"`
		} `json:"direct_modules"`
	}
	if err := json.Unmarshal(data, &document); err != nil {
		return errors.New("dependency license evidence is not valid JSON")
	}
	for _, module := range document.DirectModules {
		if !isSafeHTTPSURL(module.Source) {
			return errors.New("dependency license evidence has an unsafe source URL")
		}
	}
	return nil
}

func validateRedactedLogArtifact(name string, data []byte) error {
	expectedService, ok := logArtifactServices[name]
	if !ok {
		return fmt.Errorf("unknown redacted log evidence artifact %q", name)
	}
	var document map[string]any
	if err := json.Unmarshal(data, &document); err != nil {
		return fmt.Errorf("artifact %q redacted log is not valid JSON", name)
	}
	kind, _ := document["kind"].(string)
	service, _ := document["service"].(string)
	source, _ := document["source"].(string)
	policy, _ := document["redaction_policy"].(string)
	if kind != name || service != expectedService || source != "docker_compose_logs" || policy != "allowlist-v1" {
		return fmt.Errorf("artifact %q has an invalid redacted-log identity", name)
	}
	lineCount, ok := jsonNumber(document["line_count"])
	if !ok || lineCount < 1 {
		return fmt.Errorf("artifact %q has an invalid redacted-log line count", name)
	}
	inputBytes, ok := jsonNumber(document["input_bytes"])
	if !ok || inputBytes < 1 {
		return fmt.Errorf("artifact %q has an invalid redacted-log input length", name)
	}
	rawCounts, ok := document["event_counts"].(map[string]any)
	if !ok || len(rawCounts) == 0 {
		return fmt.Errorf("artifact %q has no redacted-log events", name)
	}
	allowedEvents := logEventsByArtifact[name]
	total := 0
	redisEvents := 0
	temporalEvents := 0
	for event, rawCount := range rawCounts {
		if _, allowed := allowedEvents[event]; !allowed {
			return fmt.Errorf("artifact %q contains an unallowlisted redacted-log event", name)
		}
		count, ok := jsonNumber(rawCount)
		if !ok || count < 1 || count > lineCount || total > lineCount-count {
			return fmt.Errorf("artifact %q has an invalid redacted-log event count", name)
		}
		total += count
		if _, isRedisBoundary := redisRuntimeEvents[event]; isRedisBoundary {
			redisEvents += count
		}
		if _, isTemporalBoundary := temporalRuntimeEvents[event]; isTemporalBoundary {
			temporalEvents += count
		}
	}
	if total != lineCount {
		return fmt.Errorf("artifact %q redacted-log events do not account for every input line", name)
	}
	switch name {
	case "redis_log":
		if redisEvents == 0 {
			return fmt.Errorf("artifact %q has no Redis runtime-boundary event", name)
		}
	case "temporal_log":
		if temporalEvents == 0 {
			return fmt.Errorf("artifact %q has no Temporal runtime-boundary event", name)
		}
	case "compose_log":
		if redisEvents == 0 || temporalEvents == 0 {
			return fmt.Errorf("artifact %q must contain Redis and Temporal runtime-boundary events", name)
		}
	}
	return nil
}

func validateEvidenceMetadata(record evidence) error {
	if record.SchemaVersion != evidenceSchemaVersion {
		return fmt.Errorf("evidence schema_version must be %d", evidenceSchemaVersion)
	}
	if _, err := time.Parse(time.RFC3339, record.GeneratedAt); err != nil {
		return errors.New("evidence generated_at is not RFC 3339")
	}
	if !isSafeHTTPSURL(record.Source.Repository) {
		return errors.New("evidence source repository must be an HTTPS URL")
	}
	if !revisionPattern.MatchString(record.Source.Revision) {
		return errors.New("evidence source revision must be a full lowercase Git SHA")
	}
	if strings.TrimSpace(record.Image.Reference) == "" {
		return errors.New("evidence image reference is empty")
	}
	if !strings.HasPrefix(record.Image.Digest, "sha256:") || !digestPattern.MatchString(strings.TrimPrefix(record.Image.Digest, "sha256:")) {
		return errors.New("evidence image digest must be a sha256 digest")
	}
	referenceParts := strings.Split(record.Image.Reference, "@")
	if len(referenceParts) != 2 || strings.TrimSpace(referenceParts[0]) == "" || referenceParts[1] != record.Image.Digest {
		return errors.New("evidence image reference must be an immutable digest reference matching image.digest")
	}
	lastSlash := strings.LastIndex(referenceParts[0], "/")
	if strings.Contains(referenceParts[0][lastSlash+1:], ":") {
		return errors.New("evidence image reference must not include a mutable tag")
	}
	return nil
}

func requiredArtifactsForRecord(record evidence) []string {
	if _, present := record.Artifacts["benchmark_summary"]; present {
		return requiredArtifacts
	}
	return legacyRequiredArtifacts
}

func validateFixtureManifest(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return errors.New("cannot read fixture manifest evidence")
	}
	var manifest fixtureManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return errors.New("fixture manifest evidence is not valid JSON")
	}
	if manifest.Version != 1 || len(manifest.Fixtures) == 0 {
		return errors.New("fixture manifest evidence must contain version 1 fixtures")
	}
	seenProfiles := make(map[string]struct{}, len(manifest.Fixtures))
	for _, fixture := range manifest.Fixtures {
		if strings.TrimSpace(fixture.Profile) == "" {
			return errors.New("fixture manifest evidence has an empty profile")
		}
		if _, duplicate := seenProfiles[fixture.Profile]; duplicate {
			return errors.New("fixture manifest evidence has a duplicate profile")
		}
		seenProfiles[fixture.Profile] = struct{}{}
		if !isSafeHTTPSURL(fixture.UpstreamURL) {
			return fmt.Errorf("fixture %q has an invalid upstream URL", fixture.Profile)
		}
		if _, err := time.Parse("2006-01-02", fixture.UpstreamDate); err != nil {
			return fmt.Errorf("fixture %q has an invalid upstream date", fixture.Profile)
		}
		if !digestPattern.MatchString(fixture.ManifestSHA256) {
			return fmt.Errorf("fixture %q has an invalid manifest sha256", fixture.Profile)
		}
	}
	return nil
}

func isSafeHTTPSURL(value string) bool {
	parsed, err := url.Parse(value)
	return err == nil &&
		parsed.IsAbs() &&
		parsed.Scheme == "https" &&
		parsed.Host != "" &&
		parsed.Hostname() != "" &&
		isSafeDNSHostname(parsed.Hostname()) &&
		strings.EqualFold(parsed.Host, parsed.Hostname()) &&
		parsed.Port() == "" &&
		parsed.User == nil &&
		parsed.RawQuery == "" &&
		!parsed.ForceQuery &&
		parsed.Fragment == "" &&
		!strings.Contains(value, "?") &&
		!strings.Contains(value, "#") &&
		!strings.Contains(value, "\\") &&
		!strings.Contains(value, "%") &&
		safeHTTPSPath.MatchString(parsed.Path)
}

func isSafeDNSHostname(hostname string) bool {
	normalized := strings.ToLower(hostname)
	if len(normalized) > 253 || numericDottedHost.MatchString(normalized) {
		return false
	}
	for _, label := range strings.Split(normalized, ".") {
		if !safeDNSLabel.MatchString(label) {
			return false
		}
	}
	return true
}

type ociDescriptor struct {
	MediaType string `json:"mediaType"`
	Digest    string `json:"digest"`
	Size      int64  `json:"size"`
}

const (
	ociImageIndexMediaType    = "application/vnd.oci.image.index.v1+json"
	ociImageManifestMediaType = "application/vnd.oci.image.manifest.v1+json"
	maxOCIImageIndexDepth     = 2
)

type ociLayoutDocument struct {
	ImageLayoutVersion string `json:"imageLayoutVersion"`
}

type ociIndexDocument struct {
	SchemaVersion int             `json:"schemaVersion"`
	Manifests     []ociDescriptor `json:"manifests"`
}

type ociManifestDocument struct {
	SchemaVersion int             `json:"schemaVersion"`
	Config        ociDescriptor   `json:"config"`
	Layers        []ociDescriptor `json:"layers"`
}

type ociLayoutEntry struct {
	size   int64
	digest string
	data   []byte
}

func inspectOCILayout(path string) (string, error) {
	if filepath.Clean(path) != path {
		return "", errors.New("OCI layout must use a canonical path")
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("OCI layout must be a real directory")
	}

	entries, err := readOCILayoutDirectory(path)
	if err != nil {
		return "", err
	}
	layoutEntry, ok := entries["oci-layout"]
	if !ok || layoutEntry.data == nil {
		return "", errors.New("OCI layout is missing oci-layout metadata")
	}
	var layout ociLayoutDocument
	if err := json.Unmarshal(layoutEntry.data, &layout); err != nil || layout.ImageLayoutVersion != "1.0.0" {
		return "", errors.New("OCI layout has invalid oci-layout metadata")
	}
	indexEntry, ok := entries["index.json"]
	if !ok || indexEntry.data == nil {
		return "", errors.New("OCI layout is missing index.json")
	}
	var index ociIndexDocument
	if err := json.Unmarshal(indexEntry.data, &index); err != nil || index.SchemaVersion != 2 || len(index.Manifests) != 1 {
		return "", errors.New("OCI layout index must contain exactly one manifest descriptor")
	}
	requiredEntries := map[string]struct{}{
		"oci-layout": {},
		"index.json": {},
	}
	manifestDescriptor, err := resolveOCIManifestDescriptor(entries, index.Manifests[0], requiredEntries)
	if err != nil {
		return "", err
	}
	manifestEntry, manifestPath, err := ociDescriptorEntry(entries, manifestDescriptor)
	if err != nil {
		return "", fmt.Errorf("OCI manifest descriptor: %w", err)
	}
	if manifestEntry.data == nil {
		return "", errors.New("OCI manifest payload is too large")
	}
	var manifest ociManifestDocument
	if err := json.Unmarshal(manifestEntry.data, &manifest); err != nil || manifest.SchemaVersion != 2 || len(manifest.Layers) == 0 {
		return "", errors.New("OCI manifest payload is invalid")
	}
	if !strings.HasPrefix(manifest.Config.MediaType, "application/vnd.oci.image.config.v1+") {
		return "", errors.New("OCI manifest has an invalid image config descriptor")
	}
	configEntry, configPath, err := ociDescriptorEntry(entries, manifest.Config)
	if err != nil {
		return "", fmt.Errorf("OCI image config descriptor: %w", err)
	}
	if configEntry.data == nil {
		return "", errors.New("OCI image config payload is too large")
	}
	if err := rejectSecretLikeContent("OCI image config", configEntry.data); err != nil {
		return "", err
	}
	requiredEntries[manifestPath] = struct{}{}
	requiredEntries[configPath] = struct{}{}
	for _, layer := range manifest.Layers {
		if !strings.HasPrefix(layer.MediaType, "application/vnd.oci.image.layer.v1.") {
			return "", errors.New("OCI manifest has an invalid layer descriptor")
		}
		_, layerPath, err := ociDescriptorEntry(entries, layer)
		if err != nil {
			return "", fmt.Errorf("OCI layer descriptor: %w", err)
		}
		requiredEntries[layerPath] = struct{}{}
	}
	if err := validateOptionalOCILayoutMetadata(entries, requiredEntries); err != nil {
		return "", err
	}
	for name := range entries {
		if _, ok := requiredEntries[name]; !ok {
			return "", fmt.Errorf("OCI layout contains an unreferenced payload %q", name)
		}
	}
	return manifestDescriptor.Digest, nil
}

// resolveOCIManifestDescriptor accepts the exact descriptor shapes emitted by
// Buildx's OCI exporter: the top-level index may reference an OCI manifest
// directly or a chain of up to two single-child OCI indexes. The bound avoids
// accepting arbitrary descriptor graphs while retaining the one extra index
// layer Buildx adds to an OCI layout.
func resolveOCIManifestDescriptor(entries map[string]ociLayoutEntry, descriptor ociDescriptor, requiredEntries map[string]struct{}) (ociDescriptor, error) {
	seenIndexPayloads := make(map[string]struct{})
	for indexDepth := 0; ; indexDepth++ {
		switch descriptor.MediaType {
		case ociImageManifestMediaType:
			return descriptor, nil
		case ociImageIndexMediaType:
			nestedIndexEntry, nestedIndexPath, err := ociDescriptorEntry(entries, descriptor)
			if err != nil {
				return ociDescriptor{}, fmt.Errorf("OCI image index descriptor: %w", err)
			}
			if _, seen := seenIndexPayloads[nestedIndexPath]; seen {
				return ociDescriptor{}, errors.New("OCI image index descriptor chain contains a cycle")
			}
			if indexDepth >= maxOCIImageIndexDepth {
				return ociDescriptor{}, errors.New("OCI image index descriptor chain exceeds the supported Buildx depth")
			}
			if nestedIndexEntry.data == nil {
				return ociDescriptor{}, errors.New("OCI image index payload is too large")
			}
			var nestedIndex ociIndexDocument
			if err := json.Unmarshal(nestedIndexEntry.data, &nestedIndex); err != nil || nestedIndex.SchemaVersion != 2 || len(nestedIndex.Manifests) != 1 {
				return ociDescriptor{}, errors.New("OCI image index must contain exactly one manifest descriptor")
			}
			seenIndexPayloads[nestedIndexPath] = struct{}{}
			requiredEntries[nestedIndexPath] = struct{}{}
			descriptor = nestedIndex.Manifests[0]
		default:
			if indexDepth == 0 {
				return ociDescriptor{}, errors.New("OCI layout index does not reference an OCI image manifest")
			}
			return ociDescriptor{}, errors.New("OCI image index does not reference an OCI image manifest")
		}
	}
}

// Docker's OCI exporter may retain a compatibility manifest.json beside the
// OCI-standard index and blobs. It is not part of the descriptor graph, so
// accept only that exact, bounded JSON metadata filename; every image payload
// remains required to be referenced by the OCI manifest descriptor above.
func validateOptionalOCILayoutMetadata(entries map[string]ociLayoutEntry, requiredEntries map[string]struct{}) error {
	entry, ok := entries["manifest.json"]
	if !ok {
		return nil
	}
	if entry.data == nil {
		return errors.New("OCI layout compatibility metadata is too large")
	}
	if err := rejectSecretLikeContent("OCI layout compatibility metadata", entry.data); err != nil {
		return err
	}
	var document any
	if err := json.Unmarshal(entry.data, &document); err != nil {
		return errors.New("OCI layout compatibility metadata is not valid JSON")
	}
	switch document.(type) {
	case map[string]any, []any:
	default:
		return errors.New("OCI layout compatibility metadata must be a JSON object or array")
	}
	requiredEntries["manifest.json"] = struct{}{}
	return nil
}

func readOCILayoutDirectory(root string) (map[string]ociLayoutEntry, error) {
	entries := make(map[string]ociLayoutEntry)
	var expanded int64
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return errors.New("OCI layout directory cannot be read")
		}
		if path == root {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return errors.New("OCI layout contains an unsafe directory path")
		}
		name, err := safeOCIDirectoryPath(relative)
		if err != nil {
			return err
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("OCI layout contains a symlink %q", name)
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return errors.New("OCI layout directory entry cannot be inspected")
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("OCI layout contains an unsupported directory entry %q", name)
		}
		if _, duplicate := entries[name]; duplicate {
			return fmt.Errorf("OCI layout repeats directory entry %q", name)
		}
		if info.Size() < 0 || info.Size() > maxOCIExpandedBytes-expanded {
			return errors.New("OCI layout payload is too large")
		}
		expanded += info.Size()
		layoutEntry, err := readOCILayoutFile(path, info.Size())
		if err != nil {
			return err
		}
		entries[name] = layoutEntry
		return nil
	})
	if err != nil {
		return nil, err
	}
	return entries, nil
}

func safeOCIDirectoryPath(value string) (string, error) {
	canonical := filepath.ToSlash(value)
	cleaned := pathpkg.Clean(canonical)
	if canonical == "" || filepath.IsAbs(value) || strings.Contains(canonical, "\\") || cleaned == "." || cleaned == ".." || strings.HasPrefix(cleaned, "../") || cleaned != canonical {
		return "", errors.New("OCI layout contains an unsafe directory path")
	}
	return cleaned, nil
}

func readOCILayoutFile(path string, size int64) (ociLayoutEntry, error) {
	file, err := os.Open(path)
	if err != nil {
		return ociLayoutEntry{}, errors.New("OCI layout directory entry cannot be read")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() != size {
		return ociLayoutEntry{}, errors.New("OCI layout directory entry changed while reading")
	}
	hash := sha256.New()
	var data bytes.Buffer
	writer := io.Writer(hash)
	if size <= maxOCIMetadataBytes {
		writer = io.MultiWriter(hash, &data)
	}
	written, err := io.CopyN(writer, file, size)
	if err != nil || written != size {
		return ociLayoutEntry{}, errors.New("OCI layout directory entry is truncated")
	}
	return ociLayoutEntry{size: size, digest: hex.EncodeToString(hash.Sum(nil)), data: data.Bytes()}, nil
}

func ociDescriptorEntry(entries map[string]ociLayoutEntry, descriptor ociDescriptor) (ociLayoutEntry, string, error) {
	if !strings.HasPrefix(descriptor.Digest, "sha256:") || !digestPattern.MatchString(strings.TrimPrefix(descriptor.Digest, "sha256:")) || descriptor.Size < 0 {
		return ociLayoutEntry{}, "", errors.New("has an invalid digest or size")
	}
	path := "blobs/sha256/" + strings.TrimPrefix(descriptor.Digest, "sha256:")
	entry, ok := entries[path]
	if !ok || entry.size != descriptor.Size || entry.digest != strings.TrimPrefix(descriptor.Digest, "sha256:") {
		return ociLayoutEntry{}, "", errors.New("does not match a retained payload")
	}
	return entry, path, nil
}

func validateCycloneDXSBOM(path string, image image) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return errors.New("cannot read SBOM")
	}
	var document struct {
		BOMFormat   string `json:"bomFormat"`
		SpecVersion string `json:"specVersion"`
		Metadata    struct {
			Component struct {
				Type       string `json:"type"`
				Properties []struct {
					Name  string `json:"name"`
					Value string `json:"value"`
				} `json:"properties"`
			} `json:"component"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(data, &document); err != nil {
		return errors.New("SBOM is not valid JSON")
	}
	if document.BOMFormat != "CycloneDX" || strings.TrimSpace(document.SpecVersion) == "" {
		return errors.New("SBOM is not CycloneDX JSON")
	}
	if document.Metadata.Component.Type != "container" {
		return errors.New("SBOM does not describe a container image")
	}
	properties := make(map[string]string, len(document.Metadata.Component.Properties))
	for _, property := range document.Metadata.Component.Properties {
		if property.Name == "org.opencontainers.image.ref.name" || property.Name == "org.opencontainers.image.manifest.digest" {
			if _, duplicate := properties[property.Name]; duplicate {
				return fmt.Errorf("SBOM repeats immutable image subject property %q", property.Name)
			}
			properties[property.Name] = property.Value
		}
	}
	if properties["org.opencontainers.image.ref.name"] != image.Reference || properties["org.opencontainers.image.manifest.digest"] != image.Digest {
		return errors.New("SBOM immutable image subject does not exactly match the OCI manifest descriptor")
	}
	return nil
}

func validateTrivyScan(path string, image image) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return errors.New("cannot read final-image scan")
	}
	var document map[string]any
	if err := json.Unmarshal(data, &document); err != nil {
		return errors.New("final-image scan is not valid JSON")
	}
	if schemaVersion, ok := jsonNumber(document["SchemaVersion"]); !ok || schemaVersion != 2 {
		return errors.New("final-image scan is not a Trivy schema version 2 report")
	}
	if artifactType, _ := document["ArtifactType"].(string); artifactType != "container_image" {
		return errors.New("final-image scan is not a Trivy container-image report")
	}
	artifactName, _ := document["ArtifactName"].(string)
	if artifactName != "image.oci" {
		return errors.New("final-image scan was not normalized to the temporary OCI directory basename")
	}
	subject, ok := document["release_subject"].(map[string]any)
	if !ok {
		return errors.New("final-image scan is missing its immutable image subject")
	}
	reference, _ := subject["reference"].(string)
	digest, _ := subject["digest"].(string)
	if reference != image.Reference || digest != image.Digest {
		return errors.New("final-image scan immutable image subject does not exactly match the OCI manifest descriptor")
	}
	results, ok := document["Results"].([]any)
	if !ok {
		return errors.New("final-image scan has no results")
	}
	for _, result := range results {
		resultObject, ok := result.(map[string]any)
		if !ok {
			return errors.New("final-image scan result is malformed")
		}
		vulnerabilities, exists := resultObject["Vulnerabilities"]
		if !exists || vulnerabilities == nil {
			continue
		}
		findings, ok := vulnerabilities.([]any)
		if !ok {
			return errors.New("final-image scan vulnerabilities are malformed")
		}
		for _, finding := range findings {
			findingObject, ok := finding.(map[string]any)
			if !ok {
				return errors.New("final-image scan vulnerability is malformed")
			}
			severity, _ := findingObject["Severity"].(string)
			if severity == "HIGH" || severity == "CRITICAL" {
				return errors.New("final-image scan contains HIGH or CRITICAL findings")
			}
		}
	}
	return nil
}

func absoluteDirectory(path string) (string, error) {
	directory, err := filepath.Abs(path)
	if err != nil {
		return "", errors.New("cannot resolve artifact directory")
	}
	info, err := os.Stat(directory)
	if err != nil || !info.IsDir() {
		return "", errors.New("artifact directory does not exist")
	}
	resolved, err := filepath.EvalSymlinks(directory)
	if err != nil {
		return "", errors.New("cannot resolve artifact directory")
	}
	return filepath.Clean(resolved), nil
}

func secureArtifactPath(directory, value string) (string, error) {
	if value == "" || filepath.IsAbs(value) {
		return "", errors.New("path must be a non-empty relative path")
	}
	cleaned := filepath.Clean(filepath.FromSlash(value))
	if cleaned == "." || cleaned == ".." || strings.HasPrefix(cleaned, ".."+string(filepath.Separator)) {
		return "", errors.New("path escapes artifact directory")
	}
	path := filepath.Join(directory, cleaned)
	relative, err := filepath.Rel(directory, path)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", errors.New("path escapes artifact directory")
	}
	current := directory
	for _, component := range strings.Split(cleaned, string(filepath.Separator)) {
		current = filepath.Join(current, component)
		info, err := os.Lstat(current)
		if err != nil {
			return "", errors.New("path does not exist")
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return "", errors.New("path must not traverse a symlink")
		}
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() {
		return "", errors.New("path must be a regular file")
	}
	return path, nil
}

func secureOutputPath(directory, value string) (string, error) {
	if value == "" {
		return "", errors.New("evidence output path is empty")
	}
	path, err := filepath.Abs(value)
	if err != nil {
		return "", errors.New("cannot resolve evidence output path")
	}
	relative, err := filepath.Rel(directory, path)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", errors.New("evidence output path escapes artifact directory")
	}
	if relative == "." {
		return "", errors.New("evidence output path must name a file")
	}
	current := directory
	components := strings.Split(relative, string(filepath.Separator))
	for index, component := range components {
		current = filepath.Join(current, component)
		info, err := os.Lstat(current)
		if err == nil {
			if info.Mode()&os.ModeSymlink != 0 {
				return "", errors.New("evidence output path must not traverse a symlink")
			}
			if index < len(components)-1 && !info.IsDir() {
				return "", errors.New("evidence output parent must be a directory")
			}
			if index == len(components)-1 && !info.Mode().IsRegular() {
				return "", errors.New("evidence output path must be a regular file")
			}
			continue
		}
		if !errors.Is(err, os.ErrNotExist) {
			return "", errors.New("cannot inspect evidence output path")
		}
		if index < len(components)-1 {
			return "", errors.New("evidence output parent does not exist")
		}
	}
	return path, nil
}

func secureExistingEvidencePath(directory, value string) (string, error) {
	path, err := secureOutputPath(directory, value)
	if err != nil {
		return "", err
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() {
		return "", errors.New("evidence record must be a regular file")
	}
	return path, nil
}

func validateArtifactDirectoryContents(directory string, expectedPaths ...string) error {
	allowedFiles := make(map[string]struct{}, len(expectedPaths))
	allowedDirectories := map[string]struct{}{directory: {}}
	for _, path := range expectedPaths {
		cleaned := filepath.Clean(path)
		relative, err := filepath.Rel(directory, cleaned)
		if err != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			return errors.New("artifact directory has an invalid expected path")
		}
		allowedFiles[cleaned] = struct{}{}
		for parent := filepath.Dir(cleaned); ; parent = filepath.Dir(parent) {
			allowedDirectories[parent] = struct{}{}
			if parent == directory {
				break
			}
		}
	}
	return filepath.WalkDir(directory, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return errors.New("cannot inspect artifact directory contents")
		}
		if path == directory {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return errors.New("artifact directory contains a symlink")
		}
		if entry.IsDir() {
			if _, allowed := allowedDirectories[path]; !allowed {
				return errors.New("artifact directory contains an unreferenced directory")
			}
			return nil
		}
		if !entry.Type().IsRegular() {
			return errors.New("artifact directory contains an unsupported entry")
		}
		if _, allowed := allowedFiles[path]; !allowed {
			return errors.New("artifact directory contains an unreferenced file")
		}
		return nil
	})
}

func validateCanonicalArtifactPaths(paths map[string]string) error {
	for _, name := range requiredArtifacts {
		expected, ok := canonicalArtifactPaths[name]
		if !ok {
			return fmt.Errorf("artifact %q has no canonical path", name)
		}
		if paths[name] != expected {
			return fmt.Errorf("artifact %q must use canonical path %q", name, expected)
		}
	}
	return nil
}

func rejectSecretLikeContent(name string, data []byte) error {
	if containsSecretLikeBytes(data) || (json.Valid(data) && decodedJSONContainsSecretLikeValue(data)) {
		return fmt.Errorf("release evidence artifact %q contains an unredacted secret-like value", name)
	}
	return nil
}

func containsSecretLikeBytes(data []byte) bool {
	for _, pattern := range []*regexp.Regexp{
		privateKeyPattern,
		awsAccessKeyPattern,
		githubTokenPattern,
		slackTokenPattern,
		openAITokenPattern,
		anthropicTokenPattern,
		credentialFieldPattern,
	} {
		if pattern.Match(data) {
			return true
		}
	}
	return false
}

// decodedJSONContainsSecretLikeValue scans JSON tokens after unescaping them.
// A raw byte scan alone cannot recognize an escaped credential key such as
// "api\\u005fkey". Token traversal also visits duplicate object keys instead of
// relying on map unmarshalling, which would silently retain only the last one.
func decodedJSONContainsSecretLikeValue(data []byte) bool {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	secret, _, err := scanDecodedJSONValue(decoder, 0)
	return err == nil && secret
}

func scanDecodedJSONValue(decoder *json.Decoder, depth int) (bool, any, error) {
	// JSON evidence has shallow, fixed schemas. Treat excessive nesting as
	// suspicious rather than allowing a crafted artifact to exhaust recursion.
	if depth > 128 {
		return true, nil, nil
	}
	token, err := decoder.Token()
	if err != nil {
		return false, nil, err
	}
	switch value := token.(type) {
	case json.Delim:
		switch value {
		case '{':
			object := make(map[string]any)
			for decoder.More() {
				rawKey, err := decoder.Token()
				if err != nil {
					return false, nil, err
				}
				key, ok := rawKey.(string)
				if !ok {
					return false, nil, errors.New("JSON object key is not a string")
				}
				secret, child, err := scanDecodedJSONValue(decoder, depth+1)
				if err != nil || secret {
					return secret, nil, err
				}
				if jsonPairContainsSecretLikeValue(key, child) {
					return true, nil, nil
				}
				object[key] = child
			}
			if _, err := decoder.Token(); err != nil {
				return false, nil, err
			}
			return false, object, nil
		case '[':
			values := make([]any, 0)
			for decoder.More() {
				secret, child, err := scanDecodedJSONValue(decoder, depth+1)
				if err != nil || secret {
					return secret, nil, err
				}
				values = append(values, child)
			}
			if _, err := decoder.Token(); err != nil {
				return false, nil, err
			}
			return false, values, nil
		default:
			return false, nil, errors.New("unexpected JSON delimiter")
		}
	case string:
		return containsSecretLikeBytes([]byte(value)), value, nil
	default:
		return false, value, nil
	}
}

func jsonPairContainsSecretLikeValue(key string, value any) bool {
	keyJSON, err := json.Marshal(key)
	if err != nil {
		return true
	}
	valueJSON, err := json.Marshal(value)
	if err != nil {
		return true
	}
	return containsSecretLikeBytes(append(append(keyJSON, ':'), valueJSON...))
}

func sha256Hex(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func jsonNumber(value any) (int, bool) {
	number, ok := value.(float64)
	if !ok || number != float64(int(number)) {
		return 0, false
	}
	return int(number), true
}

type noRemoteLoader struct{}

func (noRemoteLoader) Load(rawURL string) (any, error) {
	return nil, fmt.Errorf("external schema reference %q is disabled", rawURL)
}

type artifactArguments []string

func (arguments *artifactArguments) String() string {
	return strings.Join(*arguments, ",")
}

func (arguments *artifactArguments) Set(value string) error {
	*arguments = append(*arguments, value)
	return nil
}

func (arguments artifactArguments) mapForRequired() (map[string]string, error) {
	values := make(map[string]string, len(arguments))
	allowed := make(map[string]struct{}, len(requiredArtifacts))
	for _, name := range requiredArtifacts {
		allowed[name] = struct{}{}
	}
	for _, argument := range arguments {
		name, path, found := strings.Cut(argument, "=")
		if !found || name == "" || path == "" {
			return nil, fmt.Errorf("invalid artifact argument %q", argument)
		}
		if _, ok := allowed[name]; !ok {
			return nil, fmt.Errorf("unknown artifact %q", name)
		}
		if _, duplicate := values[name]; duplicate {
			return nil, fmt.Errorf("artifact %q was specified more than once", name)
		}
		values[name] = path
	}
	missing := make([]string, 0, len(requiredArtifacts))
	for _, name := range requiredArtifacts {
		if _, ok := values[name]; !ok {
			missing = append(missing, name)
		}
	}
	if len(missing) != 0 {
		sort.Strings(missing)
		return nil, fmt.Errorf("missing required artifacts: %s", strings.Join(missing, ", "))
	}
	return values, nil
}
