package architecturetest

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/observability"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
)

const sloEvidenceSourceRevision = "0123456789abcdef0123456789abcdef01234567"
const sloBindingReleaseRevision = sloEvidenceSourceRevision
const sloBindingImageReference = "registry.example:5000/llm-temporal-worker"
const sloBindingImageDigest = "sha256:" + "d" + "ddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
const sloBindingArtifactName = "release-evidence"
const sloBindingArtifactDigest = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"

func TestSLOEvidenceMakeTargetIsOfflineAndFocused(t *testing.T) {
	makefile := readRepositoryFile(t, moduleRoot(t), "Makefile")
	target := makeTarget(t, makefile, "slo-evidence-verify:\n", "\n\nintegration:")
	expectContainsEach(t, target, "slo-evidence-verify is missing %q",
		"$(GO) test ./internal/architecturetest -run '^TestSLOEvidence' -count=1",
	)
	expectOmitsEach(t, target, "slo-evidence-verify must not require protected or external state via %q",
		"LLMTW_LIVE_",
		"LLMTW_REDIS_ADDR",
		"docker",
		"release-verify",
	)
}

func TestSLOEvidenceRecordsAndVerifiesCanonicalRedactedPassMeasurement(t *testing.T) {
	root := repositoryRoot(t)
	inputPath, evidencePath, digest := recordValidSLOEvidence(t, root, t.TempDir(), "slo-measurement.json")

	raw := []byte(readText(t, evidencePath))
	var record map[string]any
	decodeJSON(t, raw, "SLO evidence", &record)
	assertExactKeys(t, "record", record,
		"schema_version", "kind", "status", "measured_at",
		"source_revision", "deployment_id_sha256", "region",
		"admission_compilation", "worker_error_rate", "redacted", "content_sha256",
	)
	for _, forbidden := range redactedEvidenceForbiddenFields {
		if _, found := record[forbidden]; found {
			t.Fatalf("record retains forbidden %q field: %#v", forbidden, record)
		}
	}
	if got := record["content_sha256"]; got != digest {
		t.Fatalf("record content_sha256 = %#v, want %q", got, digest)
	}
	assertCanonicalJSON(t, raw, "record")

	testassert.NoError(t, testassert.Err(runSLOEvidence(root, "verify", "--evidence", evidencePath, "--source-revision", sloEvidenceSourceRevision, "--content-sha256", digest)), "verify recorded SLO evidence")
	if _, err := runSLOEvidence(root, "record", "--input", inputPath, "--evidence", evidencePath); err == nil {
		t.Fatal("record unexpectedly overwrote immutable evidence path")
	}
	assertPrivateFileMode(t, evidencePath, "evidence")
}

func TestSLOEvidenceBindsAndVerifiesReleaseMetadataWithoutOverwrite(t *testing.T) {
	root := repositoryRoot(t)
	directory := t.TempDir()
	_, evidencePath, digest := recordValidSLOEvidence(t, root, directory, "slo-measurement.json")
	bindingPath := filepath.Join(directory, "slo-binding.json")
	args := sloBindingArguments(evidencePath, bindingPath, digest)
	output, err := runSLOEvidence(root, append([]string{"bind"}, args...)...)
	if err != nil {
		t.Fatalf("bind SLO evidence: %v\n%s", err, output)
	}
	const prefix = "slo evidence binding recorded sha256="
	bindingDigest := strings.TrimPrefix(strings.TrimSpace(string(output)), prefix)
	if bindingDigest == strings.TrimSpace(string(output)) || len(bindingDigest) != 64 {
		t.Fatalf("bind output = %q, want binding digest", output)
	}

	raw := []byte(readText(t, bindingPath))
	var binding map[string]any
	decodeJSON(t, raw, "SLO binding", &binding)
	assertExactKeys(t, "binding", binding, "schema_version", "kind", "status", "evidence", "release", "redacted", "content_sha256")
	if got, want := binding["kind"], "slo_release_binding"; got != want {
		t.Fatalf("binding kind = %#v, want %q", got, want)
	}
	if got, want := binding["status"], "bound"; got != want {
		t.Fatalf("binding status = %#v, want %q", got, want)
	}
	evidence, ok := binding["evidence"].(map[string]any)
	if !ok || evidence["source_revision"] != sloEvidenceSourceRevision || evidence["content_sha256"] != digest {
		t.Fatalf("binding evidence = %#v, want source and digest", binding["evidence"])
	}
	release := mappingField(t, "binding release", binding, "release")
	for key, want := range map[string]any{
		"source_revision": sloBindingReleaseRevision, "image_reference": sloBindingImageReference,
		"image_digest": sloBindingImageDigest, "workflow_run_id": float64(30535408998),
		"artifact_name": sloBindingArtifactName, "artifact_digest": sloBindingArtifactDigest,
	} {
		if got := release[key]; got != want {
			t.Fatalf("binding release[%q] = %#v, want %#v", key, got, want)
		}
	}
	if got := binding["content_sha256"]; got != bindingDigest {
		t.Fatalf("binding content_sha256 = %#v, want %q", got, bindingDigest)
	}
	bindingSchema := compileSLOEvidenceJSONSchemaFile(t, root, "slo-evidence-binding.schema.json", "urn:llmtw:slo-evidence-binding:v1")
	assertSLOSchemaRejectsUnknownField(t, bindingSchema, binding, "binding schema", "binding")
	assertCanonicalJSON(t, raw, "binding")

	testassert.NoError(t, testassert.Err(runSLOEvidence(root, append([]string{"verify-binding"}, args...)...)), "verify SLO binding")
	before := append([]byte(nil), raw...)
	assertSLOEvidenceRejected(t, root, "bind unexpectedly overwrote immutable binding", append([]string{"bind"}, args...)...)
	after, err := os.ReadFile(bindingPath)
	testassert.NoError(t, err)
	if !bytes.Equal(after, before) {
		t.Fatal("failed bind changed immutable binding")
	}
	assertPrivateFileMode(t, bindingPath, "binding")
}

func TestSLOEvidenceBindingRejectsCrossRevisionAndUnsafeReleaseMetadata(t *testing.T) {
	root := repositoryRoot(t)
	directory := t.TempDir()
	_, evidencePath, digest := recordValidSLOEvidence(t, root, directory, "slo-measurement.json")
	testCases := []struct {
		name   string
		mutate func([]string)
	}{
		{name: "source revision mismatch", mutate: func(args []string) { setSLOBindingFlag(args, "--source-revision", strings.Repeat("f", 40)) }},
		{name: "content digest mismatch", mutate: func(args []string) { setSLOBindingFlag(args, "--content-sha256", strings.Repeat("f", 64)) }},
		{name: "release revision mismatch", mutate: func(args []string) { setSLOBindingFlag(args, "--release-revision", strings.Repeat("f", 40)) }},
		{name: "image reference contains credentials", mutate: func(args []string) {
			setSLOBindingFlag(args, "--image-reference", "ghcr.io/user:password@registry.example/worker")
		}},
		{name: "image digest malformed", mutate: func(args []string) { setSLOBindingFlag(args, "--image-digest", "sha256:bad") }},
		{name: "workflow run id not positive", mutate: func(args []string) { setSLOBindingFlag(args, "--workflow-run-id", "0") }},
		{name: "artifact name is not release evidence", mutate: func(args []string) { setSLOBindingFlag(args, "--artifact-name", "other-artifact") }},
		{name: "artifact digest malformed", mutate: func(args []string) { setSLOBindingFlag(args, "--artifact-digest", "bad") }},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			bindingPath := filepath.Join(directory, testCase.name+".json")
			args := sloBindingArguments(evidencePath, bindingPath, digest)
			testCase.mutate(args)
			assertSLOEvidenceRejected(t, root, "bind result", append([]string{"bind"}, args...)...)
			assertPathAbsent(t, bindingPath, "rejected bind left output path")
		})
	}
}

func sloBindingArguments(evidencePath, bindingPath, digest string) []string {
	return []string{
		"--evidence", evidencePath, "--binding", bindingPath,
		"--source-revision", sloEvidenceSourceRevision, "--content-sha256", digest,
		"--release-revision", sloBindingReleaseRevision, "--image-reference", sloBindingImageReference,
		"--image-digest", sloBindingImageDigest, "--workflow-run-id", "30535408998",
		"--artifact-name", sloBindingArtifactName, "--artifact-digest", sloBindingArtifactDigest,
	}
}

func setSLOBindingFlag(arguments []string, flag, value string) {
	for index := range arguments {
		if arguments[index] == flag {
			arguments[index+1] = value
			return
		}
	}
	panic("missing SLO binding flag " + flag)
}

func TestSLOEvidenceRejectsUnsafeOrFailingCandidates(t *testing.T) {
	root := repositoryRoot(t)
	testCases := []struct {
		name   string
		mutate func(map[string]any)
	}{
		{
			name: "unknown top level field",
			mutate: func(candidate map[string]any) {
				candidate["endpoint"] = "https://redis.internal.example"
			},
		},
		{
			name: "memory threshold equality fails",
			mutate: func(candidate map[string]any) {
				candidate["admission_compilation"].(map[string]any)["memory"].(map[string]any)["p99_microseconds"] = 25_000
			},
		},
		{
			name: "redis threshold equality fails",
			mutate: func(candidate map[string]any) {
				candidate["admission_compilation"].(map[string]any)["same_region_redis"].(map[string]any)["p99_microseconds"] = 75_000
			},
		},
		{
			name: "one in one thousand worker errors fails",
			mutate: func(candidate map[string]any) {
				errorRate := candidate["worker_error_rate"].(map[string]any)
				errorRate["completed_attempts"] = 999
				errorRate["worker_failed_attempts"] = 1
			},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			directory := t.TempDir()
			candidate := validSLOEvidenceCandidate()
			testCase.mutate(candidate)
			inputPath := filepath.Join(directory, "candidate.json")
			writeSLOEvidenceCandidate(t, inputPath, candidate)
			assertSLOEvidenceRejected(t, root, "record result", "record", "--input", inputPath, "--evidence", filepath.Join(directory, "evidence.json"))
		})
	}
}

func TestSLOEvidenceRejectsDuplicateJSONKeys(t *testing.T) {
	root := repositoryRoot(t)
	for _, testCase := range []struct {
		name string
		raw  string
	}{
		{
			name: "duplicate top-level measured timestamp",
			raw:  `{"measured_at":"2026-07-24T00:00:00Z","measured_at":"2026-07-24T00:01:00Z"}`,
		},
		{
			name: "duplicate nested sample count",
			raw:  `{"measured_at":"2026-07-24T00:00:00Z","source_revision":"0123456789abcdef0123456789abcdef01234567","deployment_id_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","region":"ap-southeast-2","admission_compilation":{"memory":{"sample_count":10000,"sample_count":10000,"p99_microseconds":24999},"same_region_redis":{"sample_count":10000,"p99_microseconds":74999,"redis":{"major_version":7,"persistence":"aof+rdb","function_digest":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}}},"worker_error_rate":{"window_started_at":"2026-07-24T00:00:00Z","window_ended_at":"2026-07-24T00:05:00Z","completed_attempts":9999,"worker_failed_attempts":0}}`,
		},
		{
			name: "non-finite number",
			raw:  `NaN`,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			directory := t.TempDir()
			inputPath := filepath.Join(directory, "candidate.json")
			writeReleaseArtifact(t, inputPath, []byte(testCase.raw))
			assertSLOEvidenceRejected(t, root, "record duplicate-key candidate", "record", "--input", inputPath, "--evidence", filepath.Join(directory, "evidence.json"))
		})
	}

	evidencePath := filepath.Join(t.TempDir(), "evidence.json")
	writeReleaseArtifact(t, evidencePath, []byte(`{"schema_version":1,"schema_version":1}`))
	assertSLOEvidenceRejected(t, root, "verify duplicate-key evidence", "verify", "--evidence", evidencePath, "--source-revision", sloEvidenceSourceRevision, "--content-sha256", strings.Repeat("a", 64))
}

func TestSLOEvidenceVerifierRejectsTamperingAndSchemaForbidsUnsafeFields(t *testing.T) {
	root := repositoryRoot(t)
	_, evidencePath, digest := recordValidSLOEvidence(t, root, t.TempDir(), "evidence.json")

	record := readJSONObject(t, evidencePath)
	record["endpoint"] = "https://redis.internal.example"
	tampered := testassert.MarshalJSON(t, record)
	writeReleaseArtifact(t, evidencePath, tampered)
	assertSLOEvidenceRejected(t, root, "verify tampered evidence", "verify", "--evidence", evidencePath, "--source-revision", sloEvidenceSourceRevision, "--content-sha256", digest)

	schema := compileSLOEvidenceJSONSchema(t, root)
	delete(record, "endpoint")
	assertSLOSchemaRejectsUnknownField(t, schema, record, "schema", "evidence")
}

func TestSLOEvidenceCandidateSchemaMatchesRecorderInput(t *testing.T) {
	root := repositoryRoot(t)
	schema := compileSLOEvidenceJSONSchemaFile(t, root, "slo-evidence-candidate.schema.json", "urn:llmtw:slo-evidence-candidate:v1")
	candidate := validSLOEvidenceCandidate()
	testassert.NoError(t, validateJSONSchema(schema, candidate), "candidate schema rejects recorder input")
	candidate["content_sha256"] = strings.Repeat("c", 64)
	if err := validateJSONSchema(schema, candidate); err == nil {
		t.Fatal("candidate schema accepts recorder-owned persisted fields")
	}
}

func TestSLOEvidenceWorkerErrorSummaryIsRedactedMeasurementOnly(t *testing.T) {
	root := repositoryRoot(t)
	inputPath, outputPath := tempFilePaths(t, "metrics.prom", "worker-error-summary.json")
	metrics := strings.Join([]string{
		"# TYPE llmtw_activity_total counter",
		`llmtw_activity_total{status="completed",error_class="none"} 5000`,
		`llmtw_activity_total{status="completed",error_class="worker"} 4999`,
		`llmtw_activity_failure_total{origin="worker"} 1`,
		`llmtw_activity_failure_total{origin="provider"} 7`,
		"process_cpu_seconds_total 12",
	}, "\n") + "\n"
	writeReleaseArtifact(t, inputPath, []byte(metrics))
	output, err := runSLOEvidence(root, "worker-error-summary", "--input", inputPath, "--output", outputPath)
	if err != nil {
		t.Fatalf("worker error summary: %v\n%s", err, output)
	}
	if !strings.HasPrefix(strings.TrimSpace(string(output)), "worker error summary recorded sha256=") {
		t.Fatalf("summary output = %q", output)
	}
	raw := []byte(readText(t, outputPath))
	var summary map[string]any
	decodeJSON(t, raw, "summary", &summary)
	want := map[string]any{
		"schema_version":         float64(1),
		"kind":                   "worker_error_summary",
		"status":                 "pass",
		"scope":                  "prometheus_snapshot_measurement_only",
		"completed_attempts":     float64(9999),
		"worker_failed_attempts": float64(1),
		"objective_status":       "measurement_only",
		"redacted":               true,
	}
	if len(summary) != len(want) {
		t.Fatalf("summary keys = %#v, want only %#v", summary, want)
	}
	for key, expected := range want {
		if got := summary[key]; got != expected {
			t.Fatalf("summary[%q] = %#v, want %#v", key, got, expected)
		}
	}
	schema := compileSLOEvidenceJSONSchemaFile(t, root, "slo-worker-error-summary.schema.json", "urn:llmtw:slo-worker-error-summary:v1")
	assertSLOSchemaRejectsUnknownField(t, schema, summary, "summary schema", "output")
	delete(summary, "api_key")
	if bytes.Contains(raw, []byte("llmtw_activity")) || bytes.Contains(raw, []byte("process_cpu")) {
		t.Fatalf("summary retained raw metric names: %q", raw)
	}
	assertCanonicalJSON(t, raw, "summary")
	assertPrivateFileMode(t, outputPath, "summary")
	if _, err := runSLOEvidence(root, "worker-error-summary", "--input", inputPath, "--output", outputPath); err == nil {
		t.Fatal("summary unexpectedly overwrote immutable output")
	}
}

func TestSLOEvidenceWorkerErrorSummaryRejectsUnsafeSnapshots(t *testing.T) {
	root := repositoryRoot(t)
	testCases := []struct {
		name    string
		metrics string
	}{
		{
			name: "missing worker failure series",
			metrics: strings.Join([]string{
				`llmtw_activity_total{status="completed",error_class="none"} 1000`,
			}, "\n"),
		},
		{
			name: "duplicate completed series",
			metrics: strings.Join([]string{
				`llmtw_activity_total{status="completed",error_class="none"} 1000`,
				`llmtw_activity_total{status="completed",error_class="none"} 1000`,
				`llmtw_activity_failure_total{origin="worker"} 0`,
			}, "\n"),
		},
		{
			name: "fractional counter",
			metrics: strings.Join([]string{
				`llmtw_activity_total{status="completed",error_class="none"} 1000.5`,
				`llmtw_activity_failure_total{origin="worker"} 0`,
			}, "\n"),
		},
		{
			name: "non-finite counter",
			metrics: strings.Join([]string{
				`llmtw_activity_total{status="completed",error_class="none"} NaN`,
				`llmtw_activity_failure_total{origin="worker"} 0`,
			}, "\n"),
		},
		{
			name: "worker rate threshold",
			metrics: strings.Join([]string{
				`llmtw_activity_total{status="completed",error_class="none"} 999`,
				`llmtw_activity_failure_total{origin="worker"} 1`,
			}, "\n"),
		},
		{
			name: "unknown label escape",
			metrics: strings.Join([]string{
				`llmtw_activity_total{status="completed",error_class="bad\q"} 1000`,
				`llmtw_activity_failure_total{origin="worker"} 0`,
			}, "\n"),
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			inputPath, outputPath := tempFilePaths(t, "metrics.prom", "summary.json")
			writeReleaseArtifact(t, inputPath, []byte(testCase.metrics+"\n"))
			assertSLOEvidenceRejected(t, root, "summary result", "worker-error-summary", "--input", inputPath, "--output", outputPath)
			assertPathAbsent(t, outputPath, "rejected summary left output path")
		})
	}
}

func validSLOEvidenceCandidate() map[string]any {
	return map[string]any{
		"measured_at":          "2026-07-24T00:00:00Z",
		"source_revision":      sloEvidenceSourceRevision,
		"deployment_id_sha256": strings.Repeat("a", 64),
		"region":               "ap-southeast-2",
		"admission_compilation": map[string]any{
			"memory": map[string]any{"sample_count": 10_000, "p99_microseconds": 24_999},
			"same_region_redis": map[string]any{
				"sample_count": 10_000, "p99_microseconds": 74_999,
				"redis": map[string]any{"major_version": 7, "persistence": "aof+rdb", "function_digest": strings.Repeat("b", 64)},
			},
		},
		"worker_error_rate": map[string]any{
			"window_started_at": "2026-07-24T00:00:00Z", "window_ended_at": "2026-07-24T00:05:00Z",
			"completed_attempts": 9_999, "worker_failed_attempts": 0,
		},
	}
}

// recordValidSLOEvidence records the valid candidate as evidenceName under
// directory and returns the candidate path, evidence path and content digest.
func recordValidSLOEvidence(t *testing.T, root, directory, evidenceName string) (string, string, string) {
	t.Helper()
	inputPath := filepath.Join(directory, "candidate.json")
	evidencePath := filepath.Join(directory, evidenceName)
	writeSLOEvidenceCandidate(t, inputPath, validSLOEvidenceCandidate())
	output, err := runSLOEvidence(root, "record", "--input", inputPath, "--evidence", evidencePath)
	if err != nil {
		t.Fatalf("record SLO evidence: %v\n%s", err, output)
	}
	const prefix = "slo evidence recorded sha256="
	digest := strings.TrimPrefix(strings.TrimSpace(string(output)), prefix)
	if digest == strings.TrimSpace(string(output)) || len(digest) != 64 {
		t.Fatalf("record output = %q, want digest", output)
	}
	return inputPath, evidencePath, digest
}

func writeSLOEvidenceCandidate(t *testing.T, path string, candidate map[string]any) {
	t.Helper()
	writeJSONArtifact(t, path, candidate)
}

// assertCanonicalJSON requires raw to be compact JSON ending in a newline;
// label names the document in failures.
func assertCanonicalJSON(t *testing.T, raw []byte, label string) {
	t.Helper()
	if !bytes.HasSuffix(raw, []byte("\n")) || bytes.Contains(raw, []byte(": ")) {
		t.Fatalf("%s is not canonical JSON: %q", label, raw)
	}
}

// assertPrivateFileMode requires the file at path to have mode 0600; label
// names it in failures.
func assertPrivateFileMode(t *testing.T, path, label string) {
	t.Helper()
	metadata, err := os.Stat(path)
	testassert.NoError(t, err)
	if got := metadata.Mode().Perm(); got != 0o600 {
		t.Fatalf("%s mode = %o, want 600", label, got)
	}
}

// assertSLOEvidenceRejected runs slo-evidence.py with arguments and fails with
// failure unless it exits non-zero with only the safe rejection message.
func assertSLOEvidenceRejected(t *testing.T, root, failure string, arguments ...string) {
	t.Helper()
	output, err := runSLOEvidence(root, arguments...)
	if err == nil || strings.TrimSpace(string(output)) != "SLO evidence rejected" {
		t.Fatalf("%s = %v, %q; want safe rejection", failure, err, output)
	}
}

func runSLOEvidence(root string, arguments ...string) ([]byte, error) {
	return runReleaseScript(root, "slo-evidence.py", arguments...)
}

// assertSLOSchemaRejectsUnknownField requires schema to accept the recorded
// document and to reject it once an unsafe unknown api_key field is added.
// The field is left in document.
func assertSLOSchemaRejectsUnknownField(t *testing.T, schema *jsonschema.Schema, document map[string]any, label, recorded string) {
	t.Helper()
	if err := validateJSONSchema(schema, document); err != nil {
		t.Fatalf("%s rejects recorded %s: %v", label, recorded, err)
	}
	document["api_key"] = "must-not-be-accepted"
	if err := validateJSONSchema(schema, document); err == nil {
		t.Fatalf("%s accepts unsafe unknown field", label)
	}
}

func compileSLOEvidenceJSONSchema(t *testing.T, root string) *jsonschema.Schema {
	return compileSLOEvidenceJSONSchemaFile(t, root, "slo-evidence.schema.json", "urn:llmtw:slo-evidence:v1")
}

func compileSLOEvidenceJSONSchemaFile(t *testing.T, root, filename, resourceURL string) *jsonschema.Schema {
	t.Helper()
	return compileRepositoryJSONSchema(t, root, filepath.Join("docs", "release", filename), resourceURL)
}

// TestSLOEvidenceAcceptsAHealthyWorkerExport feeds the real exporter's scrape
// of a worker that has completed Activities but never failed one into the
// evidence command.
func TestSLOEvidenceAcceptsAHealthyWorkerExport(t *testing.T) {
	root := repositoryRoot(t)
	metrics, err := observability.NewMetrics(observability.AllowedValues{})
	testassert.NoError(t, err)
	for index := 0; index < 1000; index++ {
		metrics.RecordActivity("completed", "none", time.Millisecond, "finalize")
	}
	recorder := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	inputPath, outputPath := tempFilePaths(t, "metrics.prom", "worker-error-summary.json")
	writeReleaseArtifact(t, inputPath, recorder.Body.Bytes())
	output, err := runSLOEvidence(root, "worker-error-summary", "--input", inputPath, "--output", outputPath)
	if err != nil {
		t.Fatalf("worker error summary of a healthy export: %v\n%s", err, output)
	}
	summary := readJSONObject(t, outputPath)
	if summary["completed_attempts"] != float64(1000) || summary["worker_failed_attempts"] != float64(0) {
		t.Fatalf("summary = %#v, want 1000 completed and 0 worker failures", summary)
	}
}
