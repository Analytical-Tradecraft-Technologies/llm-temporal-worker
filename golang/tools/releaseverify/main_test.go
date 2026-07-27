package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestResolveOCIManifestDescriptorRejectsCyclicOCIIndexChain(t *testing.T) {
	const (
		digestA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		digestB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	)
	indexA := []byte(fmt.Sprintf(`{"schemaVersion":2,"manifests":[{"mediaType":"%s","digest":"sha256:%s","size":999}]}`,
		ociImageIndexMediaType, digestB))
	indexB := []byte(fmt.Sprintf(`{"schemaVersion":2,"manifests":[{"mediaType":"%s","digest":"sha256:%s","size":999}]}`,
		ociImageIndexMediaType, digestA))
	if len(indexA) > 999 || len(indexB) > 999 {
		t.Fatal("test index payload no longer fits the stable three-digit size placeholder")
	}
	indexA = []byte(fmt.Sprintf(`{"schemaVersion":2,"manifests":[{"mediaType":"%s","digest":"sha256:%s","size":%d}]}`,
		ociImageIndexMediaType, digestB, len(indexB)))
	indexB = []byte(fmt.Sprintf(`{"schemaVersion":2,"manifests":[{"mediaType":"%s","digest":"sha256:%s","size":%d}]}`,
		ociImageIndexMediaType, digestA, len(indexA)))
	entries := map[string]ociLayoutEntry{
		"blobs/sha256/" + digestA: {size: int64(len(indexA)), digest: digestA, data: indexA},
		"blobs/sha256/" + digestB: {size: int64(len(indexB)), digest: digestB, data: indexB},
	}

	_, err := resolveOCIManifestDescriptor(entries, ociDescriptor{
		MediaType: ociImageIndexMediaType,
		Digest:    "sha256:" + digestA,
		Size:      int64(len(indexA)),
	}, map[string]struct{}{})
	if err == nil || !strings.Contains(err.Error(), "cycle") {
		t.Fatalf("cyclic OCI index chain error = %v, want cycle rejection", err)
	}
}

func TestRunRejectsUnknownOrIncompleteCommands(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "empty", want: "usage"},
		{name: "unknown", args: []string{"publish"}, want: `unknown command "publish"`},
		{name: "verify missing flags", args: []string{"verify"}, want: "verify requires"},
		{name: "layout missing path", args: []string{"layout-digest"}, want: "layout-digest requires"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var stdout bytes.Buffer
			err := run(test.args, &stdout)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("run(%v) error = %v, want substring %q", test.args, err, test.want)
			}
			if stdout.Len() != 0 {
				t.Fatalf("run(%v) wrote unexpected stdout %q", test.args, stdout.String())
			}
		})
	}
}

func TestRejectAmbiguousJSONRequiresUniqueMembersAtEveryObjectDepth(t *testing.T) {
	if err := rejectAmbiguousJSON("test document", []byte(`{"left":{"status":"ok"},"right":{"status":"ok"}}`)); err != nil {
		t.Fatalf("same member name in separate objects rejected: %v", err)
	}

	tests := []struct {
		name string
		data string
	}{
		{name: "top level duplicate", data: `{"status":"pass","status":"fail"}`},
		{name: "nested duplicate", data: `{"finding":{"severity":"CRITICAL","severity":"LOW"}}`},
		{name: "escaped equivalent duplicate", data: `{"severity":"CRITICAL","se\u0076erity":"LOW"}`},
		{name: "trailing value", data: `{"status":"pass"}{"status":"pass"}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := rejectAmbiguousJSON("test document", []byte(test.data))
			if err == nil || !strings.Contains(err.Error(), "invalid or ambiguous JSON") {
				t.Fatalf("rejectAmbiguousJSON() error = %v, want generic ambiguity rejection", err)
			}
			if strings.Contains(err.Error(), "severity") || strings.Contains(err.Error(), "status") {
				t.Fatalf("ambiguity rejection disclosed an input member name: %v", err)
			}
		})
	}

	deep := strings.Repeat("[", maxJSONDepth+2) + "null" + strings.Repeat("]", maxJSONDepth+2)
	if err := rejectAmbiguousJSON("test document", []byte(deep)); err == nil || !strings.Contains(err.Error(), "invalid or ambiguous JSON") {
		t.Fatalf("rejectAmbiguousJSON() excessive-depth error = %v, want generic rejection", err)
	}
}

func TestArtifactArgumentsMapForRequiredRejectsMalformedInput(t *testing.T) {
	complete := make([]string, 0, len(requiredArtifacts))
	for _, name := range requiredArtifacts {
		complete = append(complete, name+"="+canonicalArtifactPaths[name])
	}
	if values, err := (artifactArguments(complete)).mapForRequired(); err != nil {
		t.Fatalf("complete artifact arguments rejected: %v", err)
	} else if len(values) != len(requiredArtifacts) {
		t.Fatalf("complete artifact arguments returned %d values, want %d", len(values), len(requiredArtifacts))
	}

	tests := []struct {
		name string
		args artifactArguments
		want string
	}{
		{name: "missing equals", args: artifactArguments{"test_summary=test-summary.json", "race_summary"}, want: "invalid artifact argument"},
		{name: "unknown artifact", args: artifactArguments{"unexpected=artifact.json"}, want: `unknown artifact "unexpected"`},
		{name: "duplicate artifact", args: artifactArguments{"test_summary=a.json", "test_summary=b.json"}, want: `artifact "test_summary" was specified more than once`},
		{name: "missing required artifact", args: artifactArguments{"test_summary=test-summary.json"}, want: "missing required artifacts"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := test.args.mapForRequired(); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("mapForRequired(%v) error = %v, want substring %q", test.args, err, test.want)
			}
		})
	}
}

func TestValidateEvidenceMetadataEnforcesImmutableReleaseSubjects(t *testing.T) {
	digest := strings.Repeat("a", 64)
	valid := evidence{
		SchemaVersion: evidenceSchemaVersion,
		GeneratedAt:   "2026-07-19T00:00:00Z",
		Source:        source{Repository: "https://github.com/example/project", Revision: strings.Repeat("b", 40)},
		Image:         image{Reference: "registry.example/project@sha256:" + digest, Digest: "sha256:" + digest},
	}
	if err := validateEvidenceMetadata(valid); err != nil {
		t.Fatalf("valid evidence metadata rejected: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*evidence)
		want   string
	}{
		{name: "non HTTPS repository", mutate: func(record *evidence) { record.Source.Repository = "http://github.com/example/project" }, want: "HTTPS URL"},
		{name: "short revision", mutate: func(record *evidence) { record.Source.Revision = strings.Repeat("b", 39) }, want: "full lowercase Git SHA"},
		{name: "digest mismatch", mutate: func(record *evidence) { record.Image.Digest = "sha256:" + strings.Repeat("c", 64) }, want: "immutable digest reference"},
		{name: "mutable tag", mutate: func(record *evidence) { record.Image.Reference = "registry.example/project:latest@sha256:" + digest }, want: "must not include a mutable tag"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			record := valid
			test.mutate(&record)
			if err := validateEvidenceMetadata(record); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validateEvidenceMetadata() error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestProvenanceAndSecretGuardsRejectUnsafeEvidence(t *testing.T) {
	for _, test := range []struct {
		value string
		ok    bool
	}{
		{value: "https://github.com/example/project", ok: true},
		{value: "https://example.invalid/path/to/artifact", ok: true},
		{value: "http://github.com/example/project"},
		{value: "https://user:password@example.com/project"},
		{value: "https://example.com:443/project"},
		{value: "https://127.0.0.1/project"},
		{value: "https://example.com/project?token=secret"},
		{value: "https://example.com/project#fragment"},
	} {
		if got := isSafeHTTPSURL(test.value); got != test.ok {
			t.Errorf("isSafeHTTPSURL(%q) = %v, want %v", test.value, got, test.ok)
		}
	}

	for _, test := range []struct {
		name string
		data string
	}{
		{name: "private key", data: "-----BEGIN PRIVATE KEY-----"},
		{name: "credential field", data: `{"api_key":"release-secret-0123456789"}`},
		{name: "escaped credential field", data: `{"api\u005fkey":"release-secret-0123456789"}`},
		{name: "provider token", data: "ghp_012345678901234567890123456789"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := rejectSecretLikeContent(test.name, []byte(test.data)); err == nil {
				t.Fatalf("rejectSecretLikeContent accepted %s", test.name)
			}
		})
	}
	if err := rejectSecretLikeContent("redacted.json", []byte(`{"status":"ok"}`)); err != nil {
		t.Fatalf("ordinary redacted evidence rejected: %v", err)
	}
}

func TestValidateRedactedLogArtifactRequiresRuntimeBoundaryEvents(t *testing.T) {
	base := func(name string) map[string]any {
		service := logArtifactServices[name]
		return map[string]any{
			"kind":             name,
			"service":          service,
			"source":           "docker_compose_logs",
			"redaction_policy": "allowlist-v1",
			"line_count":       2,
			"input_bytes":      20,
			"event_counts":     map[string]any{"redacted_line": 1, "redis_ready": 1},
		}
	}
	valid := base("redis_log")
	if err := validateRedactedLogArtifact("redis_log", mustJSON(t, valid)); err != nil {
		t.Fatalf("valid Redis log rejected: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(map[string]any)
		want   string
	}{
		{name: "wrong identity", mutate: func(document map[string]any) { document["service"] = "temporal" }, want: "invalid redacted-log identity"},
		{name: "unknown event", mutate: func(document map[string]any) { document["event_counts"].(map[string]any)["secret_dump"] = 1 }, want: "unallowlisted"},
		{name: "line count mismatch", mutate: func(document map[string]any) { document["event_counts"].(map[string]any)["redis_ready"] = 2 }, want: "invalid redacted-log event count"},
		{name: "missing Redis boundary", mutate: func(document map[string]any) { document["event_counts"] = map[string]any{"redacted_line": 2} }, want: "no Redis runtime-boundary"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			document := base("redis_log")
			test.mutate(document)
			if err := validateRedactedLogArtifact("redis_log", mustJSON(t, document)); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validateRedactedLogArtifact() error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestRunLayoutDigestSmoke(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "oci-layout"), []byte(`{"imageLayoutVersion":"1.0.0"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	config := []byte(`{"architecture":"arm64","os":"linux"}`)
	layer := []byte("application layer")
	configDigest := writeOCIBlob(t, root, config)
	layerDigest := writeOCIBlob(t, root, layer)
	manifestData, err := json.Marshal(ociManifestDocument{
		SchemaVersion: 2,
		Config:        ociDescriptor{MediaType: "application/vnd.oci.image.config.v1+json", Digest: "sha256:" + configDigest, Size: int64(len(config))},
		Layers:        []ociDescriptor{{MediaType: "application/vnd.oci.image.layer.v1.tar+gzip", Digest: "sha256:" + layerDigest, Size: int64(len(layer))}},
	})
	if err != nil {
		t.Fatal(err)
	}
	manifestDigest := writeOCIBlob(t, root, manifestData)
	indexData, err := json.Marshal(ociIndexDocument{
		SchemaVersion: 2,
		Manifests:     []ociDescriptor{{MediaType: ociImageManifestMediaType, Digest: "sha256:" + manifestDigest, Size: int64(len(manifestData))}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "index.json"), indexData, 0o600); err != nil {
		t.Fatal(err)
	}

	var stdout bytes.Buffer
	if err := run([]string{"layout-digest", "-layout", root}, &stdout); err != nil {
		t.Fatalf("layout-digest smoke test failed: %v", err)
	}
	if got := strings.TrimSpace(stdout.String()); got != "sha256:"+manifestDigest {
		t.Fatalf("layout-digest = %q, want %q", got, "sha256:"+manifestDigest)
	}
}

func TestRunLayoutDigestRejectsDuplicateDescriptorMembers(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "oci-layout"), []byte(`{"imageLayoutVersion":"1.0.0"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	config := []byte(`{"architecture":"arm64","os":"linux"}`)
	layer := []byte("application layer")
	configDigest := writeOCIBlob(t, root, config)
	layerDigest := writeOCIBlob(t, root, layer)
	manifestData, err := json.Marshal(ociManifestDocument{
		SchemaVersion: 2,
		Config:        ociDescriptor{MediaType: "application/vnd.oci.image.config.v1+json", Digest: "sha256:" + configDigest, Size: int64(len(config))},
		Layers:        []ociDescriptor{{MediaType: "application/vnd.oci.image.layer.v1.tar+gzip", Digest: "sha256:" + layerDigest, Size: int64(len(layer))}},
	})
	if err != nil {
		t.Fatal(err)
	}
	manifestDigest := writeOCIBlob(t, root, manifestData)
	indexData := []byte(fmt.Sprintf(
		`{"schemaVersion":2,"manifests":[{"mediaType":"%s","digest":"sha256:%s","digest":"sha256:%s","size":%d}]}`,
		ociImageManifestMediaType,
		strings.Repeat("b", 64),
		manifestDigest,
		len(manifestData),
	))
	if err := os.WriteFile(filepath.Join(root, "index.json"), indexData, 0o600); err != nil {
		t.Fatal(err)
	}

	var stdout bytes.Buffer
	err = run([]string{"layout-digest", "-layout", root}, &stdout)
	if err == nil || !strings.Contains(err.Error(), "invalid or ambiguous JSON") {
		t.Fatalf("layout-digest duplicate descriptor error = %v, want ambiguity rejection", err)
	}
	if stdout.Len() != 0 {
		t.Fatalf("layout-digest wrote output after rejecting duplicate descriptor: %q", stdout.String())
	}
}

func TestReleaseEvidenceRecordAndVerifySmoke(t *testing.T) {
	root := t.TempDir()
	artifactDir := filepath.Join(root, "artifacts")
	if err := os.Mkdir(artifactDir, 0o700); err != nil {
		t.Fatal(err)
	}
	resolvedArtifactDir, err := filepath.EvalSymlinks(artifactDir)
	if err != nil {
		t.Fatal(err)
	}
	artifactDir = resolvedArtifactDir
	digest := strings.Repeat("a", 64)
	image := "registry.example/project@sha256:" + digest
	writeReleaseEvidenceArtifacts(t, artifactDir, image, "sha256:"+digest)
	schemaPath := releaseEvidenceSchemaPath(t)

	recordArgs := []string{
		"record",
		"-schema", schemaPath,
		"-artifact-dir", artifactDir,
		"-output", filepath.Join(artifactDir, "evidence.json"),
		"-repository", "https://github.com/example/project",
		"-revision", strings.Repeat("b", 40),
		"-image-reference", image,
		"-image-digest", "sha256:" + digest,
	}
	for _, name := range requiredArtifacts {
		recordArgs = append(recordArgs, "-artifact", name+"="+canonicalArtifactPaths[name])
	}
	var stdout bytes.Buffer
	if err := run(recordArgs, &stdout); err != nil {
		t.Fatalf("record smoke test failed: %v", err)
	}
	if got := strings.TrimSpace(stdout.String()); got != "release evidence recorded" {
		t.Fatalf("record output = %q", got)
	}
	if info, err := os.Stat(filepath.Join(artifactDir, "evidence.json")); err != nil {
		t.Fatal(err)
	} else if info.Mode().Perm() != 0o600 {
		t.Fatalf("evidence.json mode = %o, want 600", info.Mode().Perm())
	}

	stdout.Reset()
	if err := run([]string{
		"verify",
		"-schema", schemaPath,
		"-artifact-dir", artifactDir,
		"-evidence", filepath.Join(artifactDir, "evidence.json"),
	}, &stdout); err != nil {
		t.Fatalf("verify smoke test failed: %v", err)
	}
	if got := strings.TrimSpace(stdout.String()); got != "release evidence verified" {
		t.Fatalf("verify output = %q", got)
	}

	testSummary := filepath.Join(artifactDir, canonicalArtifactPaths["test_summary"])
	data, err := os.ReadFile(testSummary)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(testSummary, append(data, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{
		"verify",
		"-schema", schemaPath,
		"-artifact-dir", artifactDir,
		"-evidence", filepath.Join(artifactDir, "evidence.json"),
	}, &stdout); err == nil || !strings.Contains(err.Error(), "sha256 does not match") {
		t.Fatalf("tampered evidence verification error = %v, want digest mismatch", err)
	}
}

func TestReleaseEvidenceVerifyAcceptsPreBenchmarkV1Bundle(t *testing.T) {
	root := t.TempDir()
	artifactDir := filepath.Join(root, "artifacts")
	if err := os.Mkdir(artifactDir, 0o700); err != nil {
		t.Fatal(err)
	}
	resolvedArtifactDir, err := filepath.EvalSymlinks(artifactDir)
	if err != nil {
		t.Fatal(err)
	}
	artifactDir = resolvedArtifactDir
	digest := strings.Repeat("a", 64)
	image := "registry.example/project@sha256:" + digest
	writeReleaseEvidenceArtifacts(t, artifactDir, image, "sha256:"+digest)
	schemaPath := releaseEvidenceSchemaPath(t)

	recordArgs := []string{
		"record",
		"-schema", schemaPath,
		"-artifact-dir", artifactDir,
		"-output", filepath.Join(artifactDir, "evidence.json"),
		"-repository", "https://github.com/example/project",
		"-revision", strings.Repeat("b", 40),
		"-image-reference", image,
		"-image-digest", "sha256:" + digest,
	}
	for _, name := range requiredArtifacts {
		recordArgs = append(recordArgs, "-artifact", name+"="+canonicalArtifactPaths[name])
	}
	if err := run(recordArgs, io.Discard); err != nil {
		t.Fatalf("record smoke test failed: %v", err)
	}

	// Simulate a retained v1 bundle from before benchmark_summary existed.
	evidencePath := filepath.Join(artifactDir, "evidence.json")
	evidenceData, err := os.ReadFile(evidencePath)
	if err != nil {
		t.Fatal(err)
	}
	var record map[string]any
	if err := json.Unmarshal(evidenceData, &record); err != nil {
		t.Fatal(err)
	}
	artifacts, ok := record["artifacts"].(map[string]any)
	if !ok {
		t.Fatal("evidence artifacts are not an object")
	}
	delete(artifacts, "benchmark_summary")
	if err := os.Remove(filepath.Join(artifactDir, canonicalArtifactPaths["benchmark_summary"])); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(evidencePath, append(mustJSON(t, record), '\n'), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := run([]string{
		"verify",
		"-schema", schemaPath,
		"-artifact-dir", artifactDir,
		"-evidence", evidencePath,
	}, io.Discard); err != nil {
		t.Fatalf("pre-benchmark v1 bundle was rejected: %v", err)
	}
}

func releaseEvidenceSchemaPath(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Join(filepath.Dir(file), "..", "..", "..", "docs", "release", "evidence.schema.json")
}

func writeReleaseEvidenceArtifacts(t *testing.T, directory, imageReference, imageDigest string) {
	t.Helper()
	digest := strings.Repeat("c", 64)
	artifacts := map[string]any{
		"test_summary": gateSummary("test_summary"),
		"race_summary": gateSummary("race_summary"),
		"fuzz_summary": gateSummary("fuzz_summary"),
		"benchmark_summary": map[string]any{
			"schema_version": 1, "kind": "benchmark_summary", "status": "pass",
			"benchmark": "BenchmarkGenerateMemoryAdmissionAndCompile", "scope": "memory",
			"samples": 4267, "ns_per_op": 255245, "p99_ms_per_op": 0.7286,
			"target_ms": 25, "objective_status": "measurement_only",
			"output_sha256": digest, "output_bytes": 256, "redacted": true,
		},
		"fixture_manifest": map[string]any{
			"schema_version": 1, "kind": "fixture_manifest", "status": "pass", "version": 1,
			"fixtures": []any{map[string]any{
				"profile": "example", "upstream_url": "https://example.invalid/contracts", "upstream_date": "2026-07-19", "manifest_sha256": digest,
			}}, "redacted": true,
		},
		"redis_summary": map[string]any{
			"schema_version": 1, "kind": "redis_summary", "status": "pass", "service": "redis", "state": "running", "health": "healthy", "redacted": true,
		},
		"temporal_summary": map[string]any{
			"schema_version": 1, "kind": "temporal_summary", "status": "pass", "service": "temporal", "state": "running", "health": "healthy", "redacted": true,
		},
		"compose_summary": map[string]any{
			"schema_version": 1, "kind": "compose_summary", "status": "pass", "services": []string{"redis", "temporal"}, "redacted": true,
		},
		"redis_log":    redactedLog("redis_log", "redis", map[string]int{"redis_ready": 1}),
		"temporal_log": redactedLog("temporal_log", "temporal", map[string]int{"temporal_ready": 1}),
		"compose_log":  redactedLog("compose_log", "compose", map[string]int{"redis_ready": 1, "temporal_ready": 1}),
		"rendered_manifests": map[string]any{
			"schema_version": 1, "kind": "rendered_manifests", "status": "pass",
			"manifests": []any{map[string]any{"source": "compose.yaml", "sha256": digest, "bytes": 1, "objects": 1}}, "redacted": true,
		},
		"dependency_license": map[string]any{
			"schema_version": 1, "kind": "dependency_license", "status": "pass", "baseline_sha256": digest,
			"direct_modules": []any{map[string]any{"path": "github.com/example/module", "version": "v1.0.0", "license": "MIT", "source": "https://github.com/example/module"}}, "redacted": true,
		},
		"vulnerability_results": map[string]any{
			"schema_version": 1, "kind": "vulnerability_results", "status": "pass",
			"components":          map[string]string{"test": "pass", "source": "pass", "go_mod": "pass", "vulnerability": "pass"},
			"direct_module_count": 1, "findings": []string{}, "approved_findings": []string{}, "redacted": true,
		},
		"sbom": map[string]any{
			"bomFormat": "CycloneDX", "specVersion": "1.5",
			"metadata": map[string]any{"component": map[string]any{
				"type": "container", "properties": []any{
					map[string]any{"name": "org.opencontainers.image.ref.name", "value": imageReference},
					map[string]any{"name": "org.opencontainers.image.manifest.digest", "value": imageDigest},
				},
			}},
		},
		"image_scan": map[string]any{
			"SchemaVersion": 2, "ArtifactType": "container_image", "ArtifactName": "image.oci",
			"release_subject": map[string]any{"reference": imageReference, "digest": imageDigest},
			"Results":         []any{map[string]any{"Vulnerabilities": []any{}}},
		},
	}
	for name, value := range artifacts {
		path := filepath.Join(directory, canonicalArtifactPaths[name])
		if err := os.WriteFile(path, mustJSON(t, value), 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
}

func gateSummary(kind string) map[string]any {
	return map[string]any{
		"schema_version": 1, "kind": kind, "status": "pass", "output_sha256": strings.Repeat("d", 64), "output_bytes": 1, "redacted": true,
	}
}

func redactedLog(kind, service string, events map[string]int) map[string]any {
	lineCount := 0
	for _, count := range events {
		lineCount += count
	}
	return map[string]any{
		"schema_version": 1, "kind": kind, "status": "pass", "service": service, "source": "docker_compose_logs", "redaction_policy": "allowlist-v1",
		"line_count": lineCount, "input_bytes": lineCount, "event_counts": events, "redacted": true,
	}
}

func TestValidateCycloneDXSBOMBindsTheImmutableImageSubject(t *testing.T) {
	image := image{Reference: "registry.example/project@sha256:" + strings.Repeat("a", 64), Digest: "sha256:" + strings.Repeat("a", 64)}
	valid := map[string]any{
		"bomFormat":   "CycloneDX",
		"specVersion": "1.5",
		"metadata": map[string]any{
			"component": map[string]any{
				"type": "container",
				"properties": []any{
					map[string]any{"name": "org.opencontainers.image.ref.name", "value": image.Reference},
					map[string]any{"name": "org.opencontainers.image.manifest.digest", "value": image.Digest},
				},
			},
		},
	}
	path := filepath.Join(t.TempDir(), "sbom.json")
	if err := os.WriteFile(path, mustJSON(t, valid), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := validateCycloneDXSBOM(path, image); err != nil {
		t.Fatalf("valid SBOM rejected: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(map[string]any)
		want   string
	}{
		{name: "wrong component type", mutate: func(document map[string]any) {
			document["metadata"].(map[string]any)["component"].(map[string]any)["type"] = "library"
		}, want: "does not describe a container image"},
		{name: "stale digest", mutate: func(document map[string]any) {
			properties := document["metadata"].(map[string]any)["component"].(map[string]any)["properties"].([]any)
			properties[1].(map[string]any)["value"] = "sha256:" + strings.Repeat("b", 64)
		}, want: "does not exactly match"},
		{name: "duplicate subject property", mutate: func(document map[string]any) {
			properties := document["metadata"].(map[string]any)["component"].(map[string]any)["properties"].([]any)
			properties = append(properties, map[string]any{"name": "org.opencontainers.image.ref.name", "value": image.Reference})
			document["metadata"].(map[string]any)["component"].(map[string]any)["properties"] = properties
		}, want: "repeats immutable image subject property"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			document := cloneJSONMap(t, valid)
			test.mutate(document)
			if err := os.WriteFile(path, mustJSON(t, document), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := validateCycloneDXSBOM(path, image); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validateCycloneDXSBOM() error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestValidateTrivyScanRejectsUnsafeImageReports(t *testing.T) {
	image := image{Reference: "registry.example/project@sha256:" + strings.Repeat("a", 64), Digest: "sha256:" + strings.Repeat("a", 64)}
	valid := map[string]any{
		"SchemaVersion": 2,
		"ArtifactType":  "container_image",
		"ArtifactName":  "image.oci",
		"release_subject": map[string]any{
			"reference": image.Reference,
			"digest":    image.Digest,
		},
		"Results": []any{map[string]any{"Vulnerabilities": []any{}}},
	}
	path := filepath.Join(t.TempDir(), "scan.json")
	if err := os.WriteFile(path, mustJSON(t, valid), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := validateTrivyScan(path, image); err != nil {
		t.Fatalf("valid Trivy report rejected: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(map[string]any)
		want   string
	}{
		{name: "wrong report type", mutate: func(document map[string]any) { document["ArtifactType"] = "filesystem" }, want: "not a Trivy container-image report"},
		{name: "wrong temporary basename", mutate: func(document map[string]any) { document["ArtifactName"] = "image.tar" }, want: "not normalized"},
		{name: "stale subject", mutate: func(document map[string]any) {
			document["release_subject"].(map[string]any)["digest"] = "sha256:" + strings.Repeat("b", 64)
		}, want: "does not exactly match"},
		{name: "critical finding", mutate: func(document map[string]any) {
			document["Results"] = []any{map[string]any{"Vulnerabilities": []any{map[string]any{"Severity": "CRITICAL"}}}}
		}, want: "HIGH or CRITICAL"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			document := cloneJSONMap(t, valid)
			test.mutate(document)
			if err := os.WriteFile(path, mustJSON(t, document), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := validateTrivyScan(path, image); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validateTrivyScan() error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestSecureArtifactAndOutputPathsRejectTraversalAndSymlinks(t *testing.T) {
	directory := t.TempDir()
	artifactPath := filepath.Join(directory, "artifact.json")
	if err := os.WriteFile(artifactPath, []byte(`{"ok":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if got, err := secureArtifactPath(directory, "artifact.json"); err != nil || got != artifactPath {
		t.Fatalf("secureArtifactPath valid path = %q, %v", got, err)
	}
	for _, value := range []string{"", "../artifact.json", "/tmp/artifact.json", "missing.json"} {
		if _, err := secureArtifactPath(directory, value); err == nil {
			t.Errorf("secureArtifactPath(%q) accepted an unsafe path", value)
		}
	}
	outside := filepath.Join(t.TempDir(), "outside.json")
	if err := os.WriteFile(outside, []byte(`{"outside":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(directory, "link.json")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}
	if _, err := secureArtifactPath(directory, "link.json"); err == nil {
		t.Fatal("secureArtifactPath accepted a symlink")
	}
	if got, err := secureOutputPath(directory, filepath.Join(directory, "evidence.json")); err != nil || got != filepath.Join(directory, "evidence.json") {
		t.Fatalf("secureOutputPath valid path = %q, %v", got, err)
	}
	if _, err := secureOutputPath(directory, filepath.Join(directory, "..", "evidence.json")); err == nil {
		t.Fatal("secureOutputPath accepted a path outside the artifact directory")
	}
}

func TestValidateFixtureManifestRejectsDuplicateProfilesAndUnsafeProvenance(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "fixture-manifest.json")
	valid := fixtureManifest{
		Version: 1,
		Fixtures: []fixtureRecord{{
			Profile:        "example",
			UpstreamURL:    "https://example.invalid/contracts",
			UpstreamDate:   "2026-07-19",
			ManifestSHA256: strings.Repeat("a", 64),
		}},
	}
	if err := os.WriteFile(path, mustJSON(t, valid), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := validateFixtureManifest(path); err != nil {
		t.Fatalf("valid fixture manifest rejected: %v", err)
	}
	invalid := valid
	invalid.Fixtures = append(invalid.Fixtures, valid.Fixtures[0])
	if err := os.WriteFile(path, mustJSON(t, invalid), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := validateFixtureManifest(path); err == nil || !strings.Contains(err.Error(), "duplicate profile") {
		t.Fatalf("duplicate fixture profile error = %v", err)
	}
	invalid = valid
	invalid.Fixtures[0].UpstreamURL = "https://user:secret@example.invalid/contracts"
	if err := os.WriteFile(path, mustJSON(t, invalid), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := validateFixtureManifest(path); err == nil || !strings.Contains(err.Error(), "invalid upstream URL") {
		t.Fatalf("unsafe fixture URL error = %v", err)
	}
}

func cloneJSONMap(t *testing.T, value map[string]any) map[string]any {
	t.Helper()
	return mustUnmarshalMap(t, mustJSON(t, value))
}

func mustUnmarshalMap(t *testing.T, data []byte) map[string]any {
	t.Helper()
	var value map[string]any
	if err := json.Unmarshal(data, &value); err != nil {
		t.Fatal(err)
	}
	return value
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func writeOCIBlob(t *testing.T, root string, data []byte) string {
	t.Helper()
	digest := sha256Hex(data)
	directory := filepath.Join(root, "blobs", "sha256")
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, digest), data, 0o600); err != nil {
		t.Fatal(err)
	}
	return digest
}
