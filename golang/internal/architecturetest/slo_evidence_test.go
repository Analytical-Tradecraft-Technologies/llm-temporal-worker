package architecturetest

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

const sloEvidenceSourceRevision = "0123456789abcdef0123456789abcdef01234567"
const sloBindingReleaseRevision = sloEvidenceSourceRevision
const sloBindingImageReference = "ghcr.io/mfow/llm-temporal-worker"
const sloBindingImageDigest = "sha256:" + "d" + "ddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
const sloBindingArtifactName = "release-evidence"
const sloBindingArtifactDigest = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"

func TestSLOEvidenceRecordsAndVerifiesCanonicalRedactedPassMeasurement(t *testing.T) {
	root := repositoryRoot(t)
	directory := t.TempDir()
	inputPath := filepath.Join(directory, "candidate.json")
	evidencePath := filepath.Join(directory, "slo-measurement.json")
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

	raw, err := os.ReadFile(evidencePath)
	if err != nil {
		t.Fatal(err)
	}
	var record map[string]any
	if err := json.Unmarshal(raw, &record); err != nil {
		t.Fatalf("decode SLO evidence: %v", err)
	}
	wantKeys := map[string]bool{
		"schema_version": true, "kind": true, "status": true, "measured_at": true,
		"source_revision": true, "deployment_id_sha256": true, "region": true,
		"admission_compilation": true, "worker_error_rate": true, "redacted": true, "content_sha256": true,
	}
	if len(record) != len(wantKeys) {
		t.Fatalf("record keys = %#v, want only %#v", record, wantKeys)
	}
	for key := range wantKeys {
		if _, ok := record[key]; !ok {
			t.Fatalf("record does not contain %q: %#v", key, record)
		}
	}
	for _, forbidden := range []string{"prompt", "output", "endpoint", "credential", "api_key", "raw"} {
		if _, found := record[forbidden]; found {
			t.Fatalf("record retains forbidden %q field: %#v", forbidden, record)
		}
	}
	if got := record["content_sha256"]; got != digest {
		t.Fatalf("record content_sha256 = %#v, want %q", got, digest)
	}
	if !bytes.HasSuffix(raw, []byte("\n")) || bytes.Contains(raw, []byte(": ")) {
		t.Fatalf("record is not canonical JSON: %q", raw)
	}

	if _, err := runSLOEvidence(root, "verify", "--evidence", evidencePath, "--source-revision", sloEvidenceSourceRevision, "--content-sha256", digest); err != nil {
		t.Fatalf("verify recorded SLO evidence: %v", err)
	}
	if _, err := runSLOEvidence(root, "record", "--input", inputPath, "--evidence", evidencePath); err == nil {
		t.Fatal("record unexpectedly overwrote immutable evidence path")
	}
	metadata, err := os.Stat(evidencePath)
	if err != nil {
		t.Fatal(err)
	}
	if got := metadata.Mode().Perm(); got != 0o600 {
		t.Fatalf("evidence mode = %o, want 600", got)
	}
}

func TestSLOEvidenceBindsAndVerifiesReleaseMetadataWithoutOverwrite(t *testing.T) {
	root := repositoryRoot(t)
	directory := t.TempDir()
	inputPath := filepath.Join(directory, "candidate.json")
	evidencePath := filepath.Join(directory, "slo-measurement.json")
	bindingPath := filepath.Join(directory, "slo-binding.json")
	writeSLOEvidenceCandidate(t, inputPath, validSLOEvidenceCandidate())

	output, err := runSLOEvidence(root, "record", "--input", inputPath, "--evidence", evidencePath)
	if err != nil {
		t.Fatalf("record SLO evidence: %v\n%s", err, output)
	}
	digest := strings.TrimPrefix(strings.TrimSpace(string(output)), "slo evidence recorded sha256=")
	args := sloBindingArguments(evidencePath, bindingPath, digest)
	output, err = runSLOEvidence(root, append([]string{"bind"}, args...)...)
	if err != nil {
		t.Fatalf("bind SLO evidence: %v\n%s", err, output)
	}
	const prefix = "slo evidence binding recorded sha256="
	bindingDigest := strings.TrimPrefix(strings.TrimSpace(string(output)), prefix)
	if bindingDigest == strings.TrimSpace(string(output)) || len(bindingDigest) != 64 {
		t.Fatalf("bind output = %q, want binding digest", output)
	}

	raw, err := os.ReadFile(bindingPath)
	if err != nil {
		t.Fatal(err)
	}
	var binding map[string]any
	if err := json.Unmarshal(raw, &binding); err != nil {
		t.Fatalf("decode SLO binding: %v", err)
	}
	wantKeys := map[string]bool{"schema_version": true, "kind": true, "status": true, "evidence": true, "release": true, "redacted": true, "content_sha256": true}
	if len(binding) != len(wantKeys) {
		t.Fatalf("binding keys = %#v, want only %#v", binding, wantKeys)
	}
	for key := range wantKeys {
		if _, ok := binding[key]; !ok {
			t.Fatalf("binding does not contain %q: %#v", key, binding)
		}
	}
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
	release, ok := binding["release"].(map[string]any)
	if !ok {
		t.Fatalf("binding release = %#v, want object", binding["release"])
	}
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
	if !bytes.HasSuffix(raw, []byte("\n")) || bytes.Contains(raw, []byte(": ")) {
		t.Fatalf("binding is not canonical JSON: %q", raw)
	}

	if _, err := runSLOEvidence(root, append([]string{"verify-binding"}, args...)...); err != nil {
		t.Fatalf("verify SLO binding: %v", err)
	}
	before := append([]byte(nil), raw...)
	if output, err := runSLOEvidence(root, append([]string{"bind"}, args...)...); err == nil || strings.TrimSpace(string(output)) != "SLO evidence rejected" {
		t.Fatalf("bind unexpectedly overwrote immutable binding: %v, %q", err, output)
	}
	after, err := os.ReadFile(bindingPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, before) {
		t.Fatal("failed bind changed immutable binding")
	}
	metadata, err := os.Stat(bindingPath)
	if err != nil {
		t.Fatal(err)
	}
	if got := metadata.Mode().Perm(); got != 0o600 {
		t.Fatalf("binding mode = %o, want 600", got)
	}
}

func TestSLOEvidenceBindingRejectsCrossRevisionAndUnsafeReleaseMetadata(t *testing.T) {
	root := repositoryRoot(t)
	directory := t.TempDir()
	inputPath := filepath.Join(directory, "candidate.json")
	evidencePath := filepath.Join(directory, "slo-measurement.json")
	writeSLOEvidenceCandidate(t, inputPath, validSLOEvidenceCandidate())
	output, err := runSLOEvidence(root, "record", "--input", inputPath, "--evidence", evidencePath)
	if err != nil {
		t.Fatalf("record SLO evidence: %v\n%s", err, output)
	}
	digest := strings.TrimPrefix(strings.TrimSpace(string(output)), "slo evidence recorded sha256=")
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
			output, err := runSLOEvidence(root, append([]string{"bind"}, args...)...)
			if err == nil || strings.TrimSpace(string(output)) != "SLO evidence rejected" {
				t.Fatalf("bind result = %v, %q; want safe rejection", err, output)
			}
			if _, statErr := os.Stat(bindingPath); !os.IsNotExist(statErr) {
				t.Fatalf("rejected bind left output path: %v", statErr)
			}
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
			output, err := runSLOEvidence(root, "record", "--input", inputPath, "--evidence", filepath.Join(directory, "evidence.json"))
			if err == nil || strings.TrimSpace(string(output)) != "SLO evidence rejected" {
				t.Fatalf("record result = %v, %q; want safe rejection", err, output)
			}
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
			if err := os.WriteFile(inputPath, []byte(testCase.raw), 0o600); err != nil {
				t.Fatal(err)
			}
			output, err := runSLOEvidence(root, "record", "--input", inputPath, "--evidence", filepath.Join(directory, "evidence.json"))
			if err == nil || strings.TrimSpace(string(output)) != "SLO evidence rejected" {
				t.Fatalf("record duplicate-key candidate = %v, %q; want safe rejection", err, output)
			}
		})
	}

	evidencePath := filepath.Join(t.TempDir(), "evidence.json")
	if err := os.WriteFile(evidencePath, []byte(`{"schema_version":1,"schema_version":1}`), 0o600); err != nil {
		t.Fatal(err)
	}
	output, err := runSLOEvidence(root, "verify", "--evidence", evidencePath, "--source-revision", sloEvidenceSourceRevision, "--content-sha256", strings.Repeat("a", 64))
	if err == nil || strings.TrimSpace(string(output)) != "SLO evidence rejected" {
		t.Fatalf("verify duplicate-key evidence = %v, %q; want safe rejection", err, output)
	}
}

func TestSLOEvidenceVerifierRejectsTamperingAndSchemaForbidsUnsafeFields(t *testing.T) {
	root := repositoryRoot(t)
	directory := t.TempDir()
	inputPath := filepath.Join(directory, "candidate.json")
	evidencePath := filepath.Join(directory, "evidence.json")
	writeSLOEvidenceCandidate(t, inputPath, validSLOEvidenceCandidate())
	output, err := runSLOEvidence(root, "record", "--input", inputPath, "--evidence", evidencePath)
	if err != nil {
		t.Fatalf("record SLO evidence: %v\n%s", err, output)
	}
	digest := strings.TrimPrefix(strings.TrimSpace(string(output)), "slo evidence recorded sha256=")

	raw, err := os.ReadFile(evidencePath)
	if err != nil {
		t.Fatal(err)
	}
	var record map[string]any
	if err := json.Unmarshal(raw, &record); err != nil {
		t.Fatal(err)
	}
	record["endpoint"] = "https://redis.internal.example"
	tampered, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(evidencePath, tampered, 0o600); err != nil {
		t.Fatal(err)
	}
	if output, err := runSLOEvidence(root, "verify", "--evidence", evidencePath, "--source-revision", sloEvidenceSourceRevision, "--content-sha256", digest); err == nil || strings.TrimSpace(string(output)) != "SLO evidence rejected" {
		t.Fatalf("verify tampered evidence = %v, %q; want safe rejection", err, output)
	}

	schema := compileSLOEvidenceJSONSchema(t, root)
	delete(record, "endpoint")
	if err := validateSLOEvidenceJSONSchema(schema, record); err != nil {
		t.Fatalf("schema rejects recorded evidence: %v", err)
	}
	record["api_key"] = "must-not-be-accepted"
	if err := validateSLOEvidenceJSONSchema(schema, record); err == nil {
		t.Fatal("schema accepts unsafe unknown field")
	}
}

func TestSLOEvidenceCandidateSchemaMatchesRecorderInput(t *testing.T) {
	root := repositoryRoot(t)
	schema := compileSLOEvidenceJSONSchemaFile(t, root, "slo-evidence-candidate.schema.json", "urn:llmtw:slo-evidence-candidate:v1")
	candidate := validSLOEvidenceCandidate()
	if err := validateSLOEvidenceJSONSchema(schema, candidate); err != nil {
		t.Fatalf("candidate schema rejects recorder input: %v", err)
	}
	candidate["content_sha256"] = strings.Repeat("c", 64)
	if err := validateSLOEvidenceJSONSchema(schema, candidate); err == nil {
		t.Fatal("candidate schema accepts recorder-owned persisted fields")
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

func writeSLOEvidenceCandidate(t *testing.T, path string, candidate map[string]any) {
	t.Helper()
	data, err := json.Marshal(candidate)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func runSLOEvidence(root string, arguments ...string) ([]byte, error) {
	command := exec.Command("python3", append([]string{filepath.Join(root, "scripts", "release", "slo-evidence.py")}, arguments...)...)
	command.Dir = root
	return command.CombinedOutput()
}

func compileSLOEvidenceJSONSchema(t *testing.T, root string) *jsonschema.Schema {
	return compileSLOEvidenceJSONSchemaFile(t, root, "slo-evidence.schema.json", "urn:llmtw:slo-evidence:v1")
}

func compileSLOEvidenceJSONSchemaFile(t *testing.T, root, filename, resourceURL string) *jsonschema.Schema {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(root, "docs", "release", filename))
	if err != nil {
		t.Fatal(err)
	}
	document, err := jsonschema.UnmarshalJSON(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("decode SLO evidence schema: %v", err)
	}
	compiler := jsonschema.NewCompiler()
	if err := compiler.AddResource(resourceURL, document); err != nil {
		t.Fatalf("add SLO evidence schema: %v", err)
	}
	schema, err := compiler.Compile(resourceURL)
	if err != nil {
		t.Fatalf("compile SLO evidence schema: %v", err)
	}
	return schema
}

func validateSLOEvidenceJSONSchema(schema *jsonschema.Schema, record map[string]any) error {
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	instance, err := jsonschema.UnmarshalJSON(bytes.NewReader(data))
	if err != nil {
		return err
	}
	return schema.Validate(instance)
}
