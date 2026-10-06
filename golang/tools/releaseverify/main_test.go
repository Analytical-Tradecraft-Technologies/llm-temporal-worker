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
	amd64 := "sha256:" + strings.Repeat("d", 64)
	arm64 := "sha256:" + strings.Repeat("e", 64)
	newValid := func() evidence {
		return evidence{
			SchemaVersion: evidenceSchemaVersion,
			GeneratedAt:   "2026-07-19T00:00:00Z",
			Source:        source{Repository: "https://github.com/example/project", Revision: strings.Repeat("b", 40)},
			Image: image{
				Reference: "registry.example/project@sha256:" + digest,
				Digest:    "sha256:" + digest,
				MediaType: ociImageIndexMediaType,
				Platforms: &imagePlatforms{
					LinuxAMD64: imageSubject{Reference: "registry.example/project@" + amd64, Digest: amd64},
					LinuxARM64: imageSubject{Reference: "registry.example/project@" + arm64, Digest: arm64},
				},
			},
		}
	}
	if err := validateEvidenceMetadata(newValid()); err != nil {
		t.Fatalf("valid evidence metadata rejected: %v", err)
	}
	legacy := newValid()
	legacy.SchemaVersion = legacyEvidenceSchemaVersion
	legacy.Image.MediaType = ""
	legacy.Image.Platforms = nil
	if err := validateEvidenceMetadata(legacy); err != nil {
		t.Fatalf("valid single-manifest v1 evidence metadata rejected: %v", err)
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
		{name: "unknown schema version", mutate: func(record *evidence) { record.SchemaVersion = 3 }, want: "schema_version must be"},
		{name: "subject is not an index", mutate: func(record *evidence) { record.Image.MediaType = ociImageManifestMediaType }, want: "must be an OCI image index"},
		{name: "platforms missing", mutate: func(record *evidence) { record.Image.Platforms = nil }, want: "must record linux/amd64 and linux/arm64"},
		{name: "invalid arm64 digest", mutate: func(record *evidence) {
			record.Image.Platforms.LinuxARM64 = imageSubject{Reference: "registry.example/project@sha256:ABC", Digest: "sha256:ABC"}
		}, want: "linux/arm64 digest must be a sha256 digest"},
		{name: "arm64 reference in another repository", mutate: func(record *evidence) {
			record.Image.Platforms.LinuxARM64.Reference = "registry.example/other@" + arm64
		}, want: "linux/arm64 reference must be the image index repository"},
		{name: "arm64 repeats amd64", mutate: func(record *evidence) {
			record.Image.Platforms.LinuxARM64 = record.Image.Platforms.LinuxAMD64
		}, want: "linux/arm64 digest repeats the linux/amd64 digest"},
		{name: "v1 with platforms", mutate: func(record *evidence) {
			record.SchemaVersion = legacyEvidenceSchemaVersion
			record.Image.MediaType = ""
		}, want: "schema-v1 evidence binds a single manifest"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			record := newValid()
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
		{name: "private key", data: "-----BEGIN " + "PRIVATE KEY-----"},
		{name: "credential field", data: `{"api_` + `key":"release-secret-0123456789"}`},
		{name: "escaped credential field", data: `{"api\u005fkey":"release-secret-0123456789"}`},
		{name: "provider token", data: "gh" + "p_012345678901234567890123456789"},
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
	artifactDir := newReleaseEvidenceArtifactDirectory(t)
	subjects := writeReleaseEvidenceArtifacts(t, artifactDir)
	schemaPath := releaseEvidenceSchemaPath(t)

	var stdout bytes.Buffer
	if err := run(releaseEvidenceRecordArguments(schemaPath, artifactDir, subjects), &stdout); err != nil {
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

func TestReleaseEvidenceRecordsTheImageIndexAsTheSubject(t *testing.T) {
	artifactDir := newReleaseEvidenceArtifactDirectory(t)
	subjects := writeReleaseEvidenceArtifacts(t, artifactDir)
	schemaPath := releaseEvidenceSchemaPath(t)
	if err := run(releaseEvidenceRecordArguments(schemaPath, artifactDir, subjects), io.Discard); err != nil {
		t.Fatalf("record rejected a complete multi-platform bundle: %v", err)
	}
	record := readEvidenceRecord(t, artifactDir)
	if got := record["schema_version"]; got != float64(evidenceSchemaVersion) {
		t.Fatalf("schema_version = %v, want %d", got, evidenceSchemaVersion)
	}
	image := record["image"].(map[string]any)
	if image["digest"] != subjects.indexDigest || image["reference"] != subjects.indexReference {
		t.Fatalf("evidence subject = %#v, want the image index %s", image, subjects.indexReference)
	}
	if image["media_type"] != ociImageIndexMediaType {
		t.Fatalf("evidence subject media type = %#v", image["media_type"])
	}
	platforms := image["platforms"].(map[string]any)
	for platform, digest := range map[string]string{platformLinuxAMD64: subjects.amd64Digest, platformLinuxARM64: subjects.arm64Digest} {
		subject := platforms[platform].(map[string]any)
		if subject["digest"] != digest || subject["reference"] != subjects.repository+"@"+digest {
			t.Fatalf("evidence %s subject = %#v, want %s", platform, subject, digest)
		}
	}
	artifacts := record["artifacts"].(map[string]any)
	for _, name := range []string{"image_index", "sbom", "image_scan", "sbom_arm64", "image_scan_arm64"} {
		if _, ok := artifacts[name]; !ok {
			t.Fatalf("evidence omitted %s: %#v", name, artifacts)
		}
	}
}

func TestReleaseEvidenceRejectsMissingOrMismatchedPlatformEvidence(t *testing.T) {
	schemaPath := releaseEvidenceSchemaPath(t)
	otherDigest := "sha256:" + strings.Repeat("9", 64)
	tests := []struct {
		name   string
		mutate func(t *testing.T, directory string, subjects *releaseEvidenceSubjects, arguments *[]string)
		want   string
	}{
		{
			name: "arm64 SBOM bound to the amd64 manifest",
			mutate: func(t *testing.T, directory string, subjects *releaseEvidenceSubjects, _ *[]string) {
				writeArtifactJSON(t, directory, "sbom_arm64", releaseEvidenceSBOM(subjects.repository+"@"+subjects.amd64Digest, subjects.amd64Digest))
			},
			want: "linux/arm64 SBOM immutable image subject",
		},
		{
			name: "amd64 scan bound to the image index",
			mutate: func(t *testing.T, directory string, subjects *releaseEvidenceSubjects, _ *[]string) {
				writeArtifactJSON(t, directory, "image_scan", releaseEvidenceScan(subjects.indexReference, subjects.indexDigest))
			},
			want: "linux/amd64 final-image scan immutable image subject",
		},
		{
			name: "missing arm64 scan",
			mutate: func(t *testing.T, directory string, _ *releaseEvidenceSubjects, arguments *[]string) {
				removeArtifact(t, directory, "image_scan_arm64", arguments)
			},
			want: "missing required artifacts: image_scan_arm64",
		},
		{
			name: "missing image index",
			mutate: func(t *testing.T, directory string, _ *releaseEvidenceSubjects, arguments *[]string) {
				removeArtifact(t, directory, "image_index", arguments)
			},
			want: "missing required artifacts: image_index",
		},
		{
			name: "missing arm64 digest",
			mutate: func(_ *testing.T, _ string, _ *releaseEvidenceSubjects, arguments *[]string) {
				replaceArgument(arguments, "-arm64-digest", "")
			},
			want: "-arm64-digest",
		},
		{
			name: "subject is a platform manifest rather than the index",
			mutate: func(_ *testing.T, _ string, subjects *releaseEvidenceSubjects, arguments *[]string) {
				replaceArgument(arguments, "-image-reference", subjects.repository+"@"+subjects.amd64Digest)
				replaceArgument(arguments, "-image-digest", subjects.amd64Digest)
			},
			want: "evidence linux/amd64 digest repeats the image index digest",
		},
		{
			name: "recorded arm64 digest is not in the index",
			mutate: func(t *testing.T, directory string, subjects *releaseEvidenceSubjects, arguments *[]string) {
				replaceArgument(arguments, "-arm64-digest", otherDigest)
				writeArtifactJSON(t, directory, "sbom_arm64", releaseEvidenceSBOM(subjects.repository+"@"+otherDigest, otherDigest))
				writeArtifactJSON(t, directory, "image_scan_arm64", releaseEvidenceScan(subjects.repository+"@"+otherDigest, otherDigest))
			},
			want: "image index linux/arm64 digest does not match the recorded platform digest",
		},
		{
			name: "index bytes differ from the published digest",
			mutate: func(t *testing.T, directory string, _ *releaseEvidenceSubjects, _ *[]string) {
				path := filepath.Join(directory, canonicalArtifactPaths["image_index"])
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, append(data, '\n'), 0o600); err != nil {
					t.Fatal(err)
				}
			},
			want: "image index bytes do not match the evidence image digest",
		},
		{
			name: "index without arm64",
			mutate: func(t *testing.T, directory string, subjects *releaseEvidenceSubjects, arguments *[]string) {
				rewriteImageIndex(t, directory, subjects, arguments, func(manifests []any) []any {
					return []any{manifests[0], manifests[2]}
				})
			},
			want: "image index does not reference linux/arm64",
		},
		{
			name: "index with an extra platform",
			mutate: func(t *testing.T, directory string, subjects *releaseEvidenceSubjects, arguments *[]string) {
				rewriteImageIndex(t, directory, subjects, arguments, func(manifests []any) []any {
					extra := indexDescriptor(otherDigest, "linux", "amd64", nil)
					return append(manifests, extra)
				})
			},
			want: "image index references linux/amd64 more than once",
		},
		{
			name: "index with an unallowlisted platform",
			mutate: func(t *testing.T, directory string, subjects *releaseEvidenceSubjects, arguments *[]string) {
				rewriteImageIndex(t, directory, subjects, arguments, func(manifests []any) []any {
					return append(manifests, indexDescriptor(otherDigest, "windows", "amd64", nil))
				})
			},
			want: "does not satisfy the allowlisted schema",
		},
		{
			name: "attestation for a foreign manifest",
			mutate: func(t *testing.T, directory string, subjects *releaseEvidenceSubjects, arguments *[]string) {
				rewriteImageIndex(t, directory, subjects, arguments, func(manifests []any) []any {
					return append(manifests, indexDescriptor("sha256:"+strings.Repeat("8", 64), "unknown", "unknown", map[string]any{
						attestationReferenceTypeKey:   attestationReferenceType,
						attestationReferenceDigestKey: otherDigest,
					}))
				})
			},
			want: "attestation does not reference a release platform manifest",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			artifactDir := newReleaseEvidenceArtifactDirectory(t)
			subjects := writeReleaseEvidenceArtifacts(t, artifactDir)
			arguments := releaseEvidenceRecordArguments(schemaPath, artifactDir, subjects)
			test.mutate(t, artifactDir, &subjects, &arguments)
			if err := run(arguments, io.Discard); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("record error = %v, want substring %q", err, test.want)
			}
			if _, err := os.Stat(filepath.Join(artifactDir, "evidence.json")); !os.IsNotExist(err) {
				t.Fatalf("record left evidence after rejecting %s: %v", test.name, err)
			}
		})
	}
}

func TestReleaseEvidenceVerifyRejectsDowngradedOrIncompleteMultiPlatformRecords(t *testing.T) {
	schemaPath := releaseEvidenceSchemaPath(t)
	tests := []struct {
		name   string
		mutate func(record map[string]any)
		want   string
	}{
		{
			name: "platforms removed",
			mutate: func(record map[string]any) {
				delete(record["image"].(map[string]any), "platforms")
			},
			want: "does not satisfy schema",
		},
		{
			name: "arm64 SBOM entry removed",
			mutate: func(record map[string]any) {
				delete(record["artifacts"].(map[string]any), "sbom_arm64")
			},
			want: "does not satisfy schema",
		},
		{
			name: "relabelled as schema v1",
			mutate: func(record map[string]any) {
				record["schema_version"] = 1
			},
			want: "does not satisfy schema",
		},
		{
			name: "platform reference from another repository",
			mutate: func(record map[string]any) {
				arm64 := record["image"].(map[string]any)["platforms"].(map[string]any)[platformLinuxARM64].(map[string]any)
				arm64["reference"] = "registry.example/other@" + arm64["digest"].(string)
			},
			want: "evidence linux/arm64 reference must be the image index repository",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			artifactDir := newReleaseEvidenceArtifactDirectory(t)
			subjects := writeReleaseEvidenceArtifacts(t, artifactDir)
			if err := run(releaseEvidenceRecordArguments(schemaPath, artifactDir, subjects), io.Discard); err != nil {
				t.Fatal(err)
			}
			record := readEvidenceRecord(t, artifactDir)
			test.mutate(record)
			writeEvidenceRecord(t, artifactDir, record)
			err := run([]string{"verify", "-schema", schemaPath, "-artifact-dir", artifactDir, "-evidence", filepath.Join(artifactDir, "evidence.json")}, io.Discard)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("verify error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestReleaseEvidenceVerifyAcceptsSingleManifestV1Bundle(t *testing.T) {
	artifactDir := newReleaseEvidenceArtifactDirectory(t)
	subjects := writeReleaseEvidenceArtifacts(t, artifactDir)
	schemaPath := releaseEvidenceSchemaPath(t)
	if err := run(releaseEvidenceRecordArguments(schemaPath, artifactDir, subjects), io.Discard); err != nil {
		t.Fatal(err)
	}
	record := downgradeToSingleManifestV1(t, artifactDir, subjects)
	writeEvidenceRecord(t, artifactDir, record)
	if err := run([]string{"verify", "-schema", schemaPath, "-artifact-dir", artifactDir, "-evidence", filepath.Join(artifactDir, "evidence.json")}, io.Discard); err != nil {
		t.Fatalf("retained single-manifest v1 bundle was rejected: %v", err)
	}

	// A v1 record must not smuggle multi-platform fields past the v1 rules.
	record["image"].(map[string]any)["media_type"] = ociImageIndexMediaType
	writeEvidenceRecord(t, artifactDir, record)
	if err := run([]string{"verify", "-schema", schemaPath, "-artifact-dir", artifactDir, "-evidence", filepath.Join(artifactDir, "evidence.json")}, io.Discard); err == nil {
		t.Fatal("v1 record with an image index media type was accepted")
	}
}

func TestReleaseEvidenceAcceptsOptionalWorkerErrorSummary(t *testing.T) {
	artifactDir := newReleaseEvidenceArtifactDirectory(t)
	subjects := writeReleaseEvidenceArtifacts(t, artifactDir)
	workerSummary := map[string]any{
		"schema_version":         1,
		"kind":                   "worker_error_summary",
		"status":                 "pass",
		"scope":                  "prometheus_snapshot_measurement_only",
		"completed_attempts":     9999,
		"worker_failed_attempts": 1,
		"objective_status":       "measurement_only",
		"redacted":               true,
	}
	workerPath := filepath.Join(artifactDir, canonicalArtifactPaths["worker_error_summary"])
	if err := os.WriteFile(workerPath, mustJSON(t, workerSummary), 0o600); err != nil {
		t.Fatal(err)
	}
	schemaPath := releaseEvidenceSchemaPath(t)
	recordArgs := releaseEvidenceRecordArguments(schemaPath, artifactDir, subjects)
	recordArgs = append(recordArgs, "-artifact", "worker_error_summary="+canonicalArtifactPaths["worker_error_summary"])
	if err := run(recordArgs, io.Discard); err != nil {
		t.Fatalf("record rejected optional worker error summary: %v", err)
	}
	evidencePath := filepath.Join(artifactDir, "evidence.json")
	record := readEvidenceRecord(t, artifactDir)
	artifacts, ok := record["artifacts"].(map[string]any)
	if !ok {
		t.Fatal("evidence artifacts are not an object")
	}
	if _, ok := artifacts["worker_error_summary"]; !ok {
		t.Fatalf("evidence omitted optional worker summary: %#v", artifacts)
	}
	if err := run([]string{"verify", "-schema", schemaPath, "-artifact-dir", artifactDir, "-evidence", evidencePath}, io.Discard); err != nil {
		t.Fatalf("verify rejected optional worker error summary: %v", err)
	}
}

func TestReleaseEvidenceAcceptsPreTargetStatusBenchmarkV1Bundle(t *testing.T) {
	artifactDir := newReleaseEvidenceArtifactDirectory(t)
	subjects := writeReleaseEvidenceArtifacts(t, artifactDir)

	// Simulate a retained schema-v1 benchmark summary from before target_status
	// was introduced. The legacy schema branch must remain verify-compatible.
	benchmarkPath := filepath.Join(artifactDir, canonicalArtifactPaths["benchmark_summary"])
	benchmarkData, err := os.ReadFile(benchmarkPath)
	if err != nil {
		t.Fatal(err)
	}
	var benchmark map[string]any
	if err := json.Unmarshal(benchmarkData, &benchmark); err != nil {
		t.Fatal(err)
	}
	delete(benchmark, "target_status")
	if err := os.WriteFile(benchmarkPath, append(mustJSON(t, benchmark), '\n'), 0o600); err != nil {
		t.Fatal(err)
	}

	schemaPath := releaseEvidenceSchemaPath(t)
	if err := run(releaseEvidenceRecordArguments(schemaPath, artifactDir, subjects), io.Discard); err != nil {
		t.Fatalf("record rejected pre-target_status benchmark summary: %v", err)
	}
	if err := run([]string{
		"verify",
		"-schema", schemaPath,
		"-artifact-dir", artifactDir,
		"-evidence", filepath.Join(artifactDir, "evidence.json"),
	}, io.Discard); err != nil {
		t.Fatalf("verify rejected pre-target_status benchmark summary: %v", err)
	}
}

func TestReleaseEvidenceRejectsCurrentBenchmarkAtTargetBoundary(t *testing.T) {
	artifactDir := newReleaseEvidenceArtifactDirectory(t)
	subjects := writeReleaseEvidenceArtifacts(t, artifactDir)

	benchmarkPath := filepath.Join(artifactDir, canonicalArtifactPaths["benchmark_summary"])
	benchmarkData, err := os.ReadFile(benchmarkPath)
	if err != nil {
		t.Fatal(err)
	}
	var benchmark map[string]any
	if err := json.Unmarshal(benchmarkData, &benchmark); err != nil {
		t.Fatal(err)
	}
	benchmark["p99_ms_per_op"] = 25.0
	if err := os.WriteFile(benchmarkPath, append(mustJSON(t, benchmark), '\n'), 0o600); err != nil {
		t.Fatal(err)
	}

	schemaPath := releaseEvidenceSchemaPath(t)
	if err := run(releaseEvidenceRecordArguments(schemaPath, artifactDir, subjects), io.Discard); err == nil || !strings.Contains(err.Error(), "p99_ms_per_op") {
		t.Fatalf("record accepted current target-boundary benchmark summary: %v", err)
	}
}

func TestReleaseEvidenceVerifyAcceptsPreBenchmarkV1Bundle(t *testing.T) {
	artifactDir := newReleaseEvidenceArtifactDirectory(t)
	subjects := writeReleaseEvidenceArtifacts(t, artifactDir)
	schemaPath := releaseEvidenceSchemaPath(t)
	if err := run(releaseEvidenceRecordArguments(schemaPath, artifactDir, subjects), io.Discard); err != nil {
		t.Fatalf("record smoke test failed: %v", err)
	}

	// Simulate a retained v1 bundle from before benchmark_summary existed.
	record := downgradeToSingleManifestV1(t, artifactDir, subjects)
	artifacts, ok := record["artifacts"].(map[string]any)
	if !ok {
		t.Fatal("evidence artifacts are not an object")
	}
	delete(artifacts, "benchmark_summary")
	if err := os.Remove(filepath.Join(artifactDir, canonicalArtifactPaths["benchmark_summary"])); err != nil {
		t.Fatal(err)
	}
	legacyVulnerabilities := []byte(`{"schema_version":1,"kind":"vulnerability_results","status":"pass","components":{"test":"pass","source":"pass","go_mod":"pass","vulnerability":"pass"},"direct_module_count":1,"findings":[],"approved_findings":[],"redacted":true}` + "\n")
	legacyVulnerabilityPath := filepath.Join(artifactDir, canonicalArtifactPaths["vulnerability_results"])
	if err := os.WriteFile(legacyVulnerabilityPath, legacyVulnerabilities, 0o600); err != nil {
		t.Fatal(err)
	}
	artifacts["vulnerability_results"] = map[string]any{
		"path": canonicalArtifactPaths["vulnerability_results"], "sha256": sha256Hex(legacyVulnerabilities), "bytes": len(legacyVulnerabilities), "redacted": true,
	}
	writeEvidenceRecord(t, artifactDir, record)

	if err := run([]string{
		"verify",
		"-schema", schemaPath,
		"-artifact-dir", artifactDir,
		"-evidence", filepath.Join(artifactDir, "evidence.json"),
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

// releaseEvidenceSubjects names the published image index and the two
// platform manifests it references, as the master container job reports them.
type releaseEvidenceSubjects struct {
	repository     string
	indexReference string
	indexDigest    string
	amd64Digest    string
	arm64Digest    string
}

func newReleaseEvidenceArtifactDirectory(t *testing.T) string {
	t.Helper()
	artifactDir := filepath.Join(t.TempDir(), "artifacts")
	if err := os.Mkdir(artifactDir, 0o700); err != nil {
		t.Fatal(err)
	}
	resolved, err := filepath.EvalSymlinks(artifactDir)
	if err != nil {
		t.Fatal(err)
	}
	return resolved
}

func releaseEvidenceRecordArguments(schemaPath, artifactDir string, subjects releaseEvidenceSubjects) []string {
	arguments := []string{
		"record",
		"-schema", schemaPath,
		"-artifact-dir", artifactDir,
		"-output", filepath.Join(artifactDir, "evidence.json"),
		"-repository", "https://github.com/example/project",
		"-revision", strings.Repeat("b", 40),
		"-image-reference", subjects.indexReference,
		"-image-digest", subjects.indexDigest,
		"-amd64-digest", subjects.amd64Digest,
		"-arm64-digest", subjects.arm64Digest,
	}
	for _, name := range requiredArtifacts {
		arguments = append(arguments, "-artifact", name+"="+canonicalArtifactPaths[name])
	}
	return arguments
}

func replaceArgument(arguments *[]string, flagName, value string) {
	for index := 0; index < len(*arguments)-1; index++ {
		if (*arguments)[index] == flagName {
			(*arguments)[index+1] = value
			return
		}
	}
	panic("missing argument " + flagName)
}

func removeArtifact(t *testing.T, directory, name string, arguments *[]string) {
	t.Helper()
	if err := os.Remove(filepath.Join(directory, canonicalArtifactPaths[name])); err != nil {
		t.Fatal(err)
	}
	kept := (*arguments)[:0]
	for index := 0; index < len(*arguments); index++ {
		if (*arguments)[index] == "-artifact" && index+1 < len(*arguments) && strings.HasPrefix((*arguments)[index+1], name+"=") {
			index++
			continue
		}
		kept = append(kept, (*arguments)[index])
	}
	*arguments = kept
}

func indexDescriptor(digest, operatingSystem, architecture string, annotations map[string]any) map[string]any {
	descriptor := map[string]any{
		"mediaType": ociImageManifestMediaType,
		"digest":    digest,
		"size":      1024,
		"platform":  map[string]any{"os": operatingSystem, "architecture": architecture},
	}
	if annotations != nil {
		descriptor["annotations"] = annotations
	}
	return descriptor
}

func imageIndexDocument(manifests []any) []byte {
	data, err := json.Marshal(map[string]any{
		"schemaVersion": 2,
		"mediaType":     ociImageIndexMediaType,
		"manifests":     manifests,
	})
	if err != nil {
		panic(err)
	}
	return data
}

// rewriteImageIndex replaces the retained index and moves the evidence subject
// to the new index digest, so only the index content is under test.
func rewriteImageIndex(t *testing.T, directory string, subjects *releaseEvidenceSubjects, arguments *[]string, mutate func([]any) []any) {
	t.Helper()
	path := filepath.Join(directory, canonicalArtifactPaths["image_index"])
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	manifests := mustUnmarshalMap(t, data)["manifests"].([]any)
	updated := imageIndexDocument(mutate(manifests))
	if err := os.WriteFile(path, updated, 0o600); err != nil {
		t.Fatal(err)
	}
	subjects.indexDigest = "sha256:" + sha256Hex(updated)
	subjects.indexReference = subjects.repository + "@" + subjects.indexDigest
	replaceArgument(arguments, "-image-reference", subjects.indexReference)
	replaceArgument(arguments, "-image-digest", subjects.indexDigest)
}

func readEvidenceRecord(t *testing.T, directory string) map[string]any {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(directory, "evidence.json"))
	if err != nil {
		t.Fatal(err)
	}
	return mustUnmarshalMap(t, data)
}

func writeEvidenceRecord(t *testing.T, directory string, record map[string]any) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(directory, "evidence.json"), append(mustJSON(t, record), '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
}

// downgradeToSingleManifestV1 rewrites a freshly recorded bundle into the shape
// retained before #1235: schema version 1, the linux/amd64 manifest as the
// subject, and no image index or arm64 artifacts.
func downgradeToSingleManifestV1(t *testing.T, directory string, subjects releaseEvidenceSubjects) map[string]any {
	t.Helper()
	record := readEvidenceRecord(t, directory)
	record["schema_version"] = legacyEvidenceSchemaVersion
	record["image"] = map[string]any{
		"reference": subjects.repository + "@" + subjects.amd64Digest,
		"digest":    subjects.amd64Digest,
	}
	artifacts := record["artifacts"].(map[string]any)
	for _, name := range []string{"image_index", "sbom_arm64", "image_scan_arm64"} {
		delete(artifacts, name)
		if err := os.Remove(filepath.Join(directory, canonicalArtifactPaths[name])); err != nil {
			t.Fatal(err)
		}
	}
	return record
}

func writeArtifactJSON(t *testing.T, directory, name string, value any) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(directory, canonicalArtifactPaths[name]), mustJSON(t, value), 0o600); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
}

func releaseEvidenceSBOM(reference, digest string) map[string]any {
	return map[string]any{
		"bomFormat": "CycloneDX", "specVersion": "1.5",
		"metadata": map[string]any{"component": map[string]any{
			"type": "container", "properties": []any{
				map[string]any{"name": "org.opencontainers.image.ref.name", "value": reference},
				map[string]any{"name": "org.opencontainers.image.manifest.digest", "value": digest},
			},
		}},
	}
}

func releaseEvidenceScan(reference, digest string) map[string]any {
	return map[string]any{
		"SchemaVersion": 2, "ArtifactType": "container_image", "ArtifactName": "image.oci",
		"release_subject": map[string]any{"reference": reference, "digest": digest},
		"Results":         []any{map[string]any{"Vulnerabilities": []any{}}},
	}
}

func writeReleaseEvidenceArtifacts(t *testing.T, directory string) releaseEvidenceSubjects {
	t.Helper()
	digest := strings.Repeat("c", 64)
	repository := "registry.example/project"
	amd64Digest := "sha256:" + strings.Repeat("a", 64)
	arm64Digest := "sha256:" + strings.Repeat("e", 64)
	index := imageIndexDocument([]any{
		indexDescriptor(amd64Digest, "linux", "amd64", nil),
		indexDescriptor(arm64Digest, "linux", "arm64", nil),
		indexDescriptor("sha256:"+strings.Repeat("f", 64), "unknown", "unknown", map[string]any{
			attestationReferenceTypeKey:   attestationReferenceType,
			attestationReferenceDigestKey: amd64Digest,
		}),
	})
	indexDigest := "sha256:" + sha256Hex(index)
	subjects := releaseEvidenceSubjects{
		repository:     repository,
		indexReference: repository + "@" + indexDigest,
		indexDigest:    indexDigest,
		amd64Digest:    amd64Digest,
		arm64Digest:    arm64Digest,
	}
	if err := os.WriteFile(filepath.Join(directory, canonicalArtifactPaths["image_index"]), index, 0o600); err != nil {
		t.Fatal(err)
	}
	artifacts := map[string]any{
		"test_summary": gateSummary("test_summary"),
		"race_summary": gateSummary("race_summary"),
		"fuzz_summary": gateSummary("fuzz_summary"),
		"benchmark_summary": map[string]any{
			"schema_version": 1, "kind": "benchmark_summary", "status": "pass",
			"benchmark": "BenchmarkGenerateMemoryAdmissionAndCompile", "scope": "memory",
			"samples": 4267, "ns_per_op": 255245, "p99_ms_per_op": 0.7286,
			"target_ms": 25, "target_status": "pass", "objective_status": "measurement_only",
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
		"sbom":             releaseEvidenceSBOM(repository+"@"+amd64Digest, amd64Digest),
		"image_scan":       releaseEvidenceScan(repository+"@"+amd64Digest, amd64Digest),
		"sbom_arm64":       releaseEvidenceSBOM(repository+"@"+arm64Digest, arm64Digest),
		"image_scan_arm64": releaseEvidenceScan(repository+"@"+arm64Digest, arm64Digest),
	}
	for name, value := range artifacts {
		writeArtifactJSON(t, directory, name, value)
	}
	return subjects
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

// publishedBuildxImageIndex is the exact image index the registry served for
// docker.io/analyticaltradecraft/llm-temporal-worker:20261006.999. It pins
// the real Buildx shape: two platform manifests and one provenance
// attestation for each of them.
const publishedBuildxImageIndex = `{
  "schemaVersion": 2,
  "mediaType": "application/vnd.oci.image.index.v1+json",
  "manifests": [
    {
      "mediaType": "application/vnd.oci.image.manifest.v1+json",
      "digest": "sha256:2adc39fc186ecf407191f28be11630076903e462d2e1bb3363f8ba7ecdfbe631",
      "size": 2935,
      "platform": {
        "architecture": "amd64",
        "os": "linux"
      }
    },
    {
      "mediaType": "application/vnd.oci.image.manifest.v1+json",
      "digest": "sha256:e6f7efed13893c947601f21f00b1ef1dca1b9110a546f619f7027c69bacf0f56",
      "size": 1111,
      "annotations": {
        "vnd.docker.reference.digest": "sha256:2adc39fc186ecf407191f28be11630076903e462d2e1bb3363f8ba7ecdfbe631",
        "vnd.docker.reference.type": "attestation-manifest"
      },
      "platform": {
        "architecture": "unknown",
        "os": "unknown"
      }
    },
    {
      "mediaType": "application/vnd.oci.image.manifest.v1+json",
      "digest": "sha256:245c33dbdc7b4504c0b12483f39b58a864b0fa315f7f073c32529cb475dd6844",
      "size": 2935,
      "platform": {
        "architecture": "arm64",
        "os": "linux"
      }
    },
    {
      "mediaType": "application/vnd.oci.image.manifest.v1+json",
      "digest": "sha256:ad35672d5a540148585b58e155c2241ce7216713b931eb7efa55f476f2fd56ab",
      "size": 1111,
      "annotations": {
        "vnd.docker.reference.digest": "sha256:245c33dbdc7b4504c0b12483f39b58a864b0fa315f7f073c32529cb475dd6844",
        "vnd.docker.reference.type": "attestation-manifest"
      },
      "platform": {
        "architecture": "unknown",
        "os": "unknown"
      }
    }
  ]
}`

func TestValidateImageIndexAcceptsThePublishedBuildxIndex(t *testing.T) {
	const (
		repository  = "docker.io/analyticaltradecraft/llm-temporal-worker"
		indexDigest = "sha256:97fe0a95a4e9e0d5ff7d866a6c682e9cb26c3377ed264c3806a557f3066b72b7"
		amd64Digest = "sha256:2adc39fc186ecf407191f28be11630076903e462d2e1bb3363f8ba7ecdfbe631"
		arm64Digest = "sha256:245c33dbdc7b4504c0b12483f39b58a864b0fa315f7f073c32529cb475dd6844"
	)
	path := filepath.Join(t.TempDir(), "image-index.json")
	if err := os.WriteFile(path, []byte(publishedBuildxImageIndex), 0o600); err != nil {
		t.Fatal(err)
	}
	subject := image{
		Reference: repository + "@" + indexDigest,
		Digest:    indexDigest,
		MediaType: ociImageIndexMediaType,
		Platforms: &imagePlatforms{
			LinuxAMD64: imageSubject{Reference: repository + "@" + amd64Digest, Digest: amd64Digest},
			LinuxARM64: imageSubject{Reference: repository + "@" + arm64Digest, Digest: arm64Digest},
		},
	}
	if err := validateImageIndex(releaseEvidenceSchemaPath(t), path, subject); err != nil {
		t.Fatalf("published Buildx image index rejected: %v", err)
	}

	swapped := subject
	swapped.Platforms = &imagePlatforms{LinuxAMD64: subject.Platforms.LinuxARM64, LinuxARM64: subject.Platforms.LinuxAMD64}
	if err := validateImageIndex(releaseEvidenceSchemaPath(t), path, swapped); err == nil || !strings.Contains(err.Error(), "linux/amd64 digest does not match") {
		t.Fatalf("validateImageIndex() with swapped platforms error = %v", err)
	}
}

// ocimergeImageIndex is the exact image index that golang/tools/ocimerge wrote
// when merging two single-platform Buildx OCI layouts (linux/amd64 and
// linux/arm64, each with provenance and SBOM attestations), as
// scripts/ci/build-scanned-image.sh does on master before skopeo publishes it
// unchanged. The master container job reports this index digest and the two
// platform digests to release-evidence.
const ocimergeImageIndex = `{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[{"mediaType":"application/vnd.oci.image.manifest.v1+json","digest":"sha256:103a17a20256719b77e2225687396cbed19e8ecd7157b9c93ecc2333dc83fadd","size":476,"platform":{"architecture":"amd64","os":"linux"}},{"mediaType":"application/vnd.oci.image.manifest.v1+json","digest":"sha256:737aff019753c41d2c088330f1c6ce29fc96bb3bd0fbba4781f2c05b75dd89f1","size":1106,"annotations":{"vnd.docker.reference.digest":"sha256:103a17a20256719b77e2225687396cbed19e8ecd7157b9c93ecc2333dc83fadd","vnd.docker.reference.type":"attestation-manifest"},"platform":{"architecture":"unknown","os":"unknown"}},{"mediaType":"application/vnd.oci.image.manifest.v1+json","digest":"sha256:62d2a8140f08c175599d9db1a09c7210f4d84e1a9742e0eb86eedf1b947ca164","size":476,"platform":{"architecture":"arm64","os":"linux"}},{"mediaType":"application/vnd.oci.image.manifest.v1+json","digest":"sha256:017ee1203a707a727b3594cd11108ce00e0ce0782429cc04480be0015b2cd4a7","size":1106,"annotations":{"vnd.docker.reference.digest":"sha256:62d2a8140f08c175599d9db1a09c7210f4d84e1a9742e0eb86eedf1b947ca164","vnd.docker.reference.type":"attestation-manifest"},"platform":{"architecture":"unknown","os":"unknown"}}]}`

func TestReleaseEvidenceRecordsAndVerifiesTheOCIMergeIndex(t *testing.T) {
	const (
		repository  = "docker.io/analyticaltradecraft/llm-temporal-worker"
		amd64Digest = "sha256:103a17a20256719b77e2225687396cbed19e8ecd7157b9c93ecc2333dc83fadd"
		arm64Digest = "sha256:62d2a8140f08c175599d9db1a09c7210f4d84e1a9742e0eb86eedf1b947ca164"
	)
	indexDigest := "sha256:" + sha256Hex([]byte(ocimergeImageIndex))
	if indexDigest != "sha256:e5795212f7ae719c4008afc269e0c799435ed1ae9a3fb43d417dd5dae4c05fc1" {
		t.Fatalf("ocimerge fixture digest changed: %s", indexDigest)
	}
	artifactDir := newReleaseEvidenceArtifactDirectory(t)
	writeReleaseEvidenceArtifacts(t, artifactDir)
	if err := os.WriteFile(filepath.Join(artifactDir, canonicalArtifactPaths["image_index"]), []byte(ocimergeImageIndex), 0o600); err != nil {
		t.Fatal(err)
	}
	writeArtifactJSON(t, artifactDir, "sbom", releaseEvidenceSBOM(repository+"@"+amd64Digest, amd64Digest))
	writeArtifactJSON(t, artifactDir, "image_scan", releaseEvidenceScan(repository+"@"+amd64Digest, amd64Digest))
	writeArtifactJSON(t, artifactDir, "sbom_arm64", releaseEvidenceSBOM(repository+"@"+arm64Digest, arm64Digest))
	writeArtifactJSON(t, artifactDir, "image_scan_arm64", releaseEvidenceScan(repository+"@"+arm64Digest, arm64Digest))
	subjects := releaseEvidenceSubjects{
		repository:     repository,
		indexReference: repository + "@" + indexDigest,
		indexDigest:    indexDigest,
		amd64Digest:    amd64Digest,
		arm64Digest:    arm64Digest,
	}
	schemaPath := releaseEvidenceSchemaPath(t)
	if err := run(releaseEvidenceRecordArguments(schemaPath, artifactDir, subjects), io.Discard); err != nil {
		t.Fatalf("record rejected the ocimerge index: %v", err)
	}
	verify := []string{"verify", "-schema", schemaPath, "-artifact-dir", artifactDir, "-evidence", filepath.Join(artifactDir, "evidence.json")}
	if err := run(verify, io.Discard); err != nil {
		t.Fatalf("verify rejected the ocimerge index: %v", err)
	}

	// The container job reads the platform digests from this same index, so a
	// platform digest that is not in it, or swapped platforms, fail closed.
	for _, test := range []struct {
		name         string
		amd64, arm64 string
	}{
		{name: "swapped platforms", amd64: arm64Digest, arm64: amd64Digest},
		{name: "arm64 not in the index", amd64: amd64Digest, arm64: "sha256:" + strings.Repeat("9", 64)},
	} {
		t.Run(test.name, func(t *testing.T) {
			subject := image{
				Reference: subjects.indexReference,
				Digest:    indexDigest,
				MediaType: ociImageIndexMediaType,
				Platforms: &imagePlatforms{
					LinuxAMD64: imageSubject{Reference: repository + "@" + test.amd64, Digest: test.amd64},
					LinuxARM64: imageSubject{Reference: repository + "@" + test.arm64, Digest: test.arm64},
				},
			}
			if err := validateImageIndex(schemaPath, filepath.Join(artifactDir, canonicalArtifactPaths["image_index"]), subject); err == nil {
				t.Fatal("validateImageIndex accepted platform digests that do not match the index")
			}
		})
	}
}

func TestValidateCycloneDXSBOMBindsTheImmutableImageSubject(t *testing.T) {
	image := imageSubject{Reference: "registry.example/project@sha256:" + strings.Repeat("a", 64), Digest: "sha256:" + strings.Repeat("a", 64)}
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
	image := imageSubject{Reference: "registry.example/project@sha256:" + strings.Repeat("a", 64), Digest: "sha256:" + strings.Repeat("a", 64)}
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
