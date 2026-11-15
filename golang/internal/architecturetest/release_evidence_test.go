package architecturetest

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	jsonschema "github.com/santhosh-tekuri/jsonschema/v6"
)

var requiredReleaseEvidenceArtifacts = []string{
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
	"sbom",
	"image_scan",
	"image_index",
	"sbom_arm64",
	"image_scan_arm64",
}

var releaseEvidenceArtifactPaths = map[string]string{
	"test_summary":       "test-summary.json",
	"race_summary":       "race-summary.json",
	"fuzz_summary":       "fuzz-summary.json",
	"benchmark_summary":  "benchmark-summary.json",
	"fixture_manifest":   "fixture-manifest.json",
	"redis_summary":      "redis-summary.json",
	"temporal_summary":   "temporal-summary.json",
	"compose_summary":    "compose-summary.json",
	"redis_log":          "redis-log.json",
	"temporal_log":       "temporal-log.json",
	"compose_log":        "compose-log.json",
	"rendered_manifests": "rendered-manifests.json",
	"dependency_license": "dependencies.json",
	"sbom":               "sbom.cdx.json",
	"image_scan":         "image-scan.json",
	"image_index":        "image-index.json",
	"sbom_arm64":         "sbom-arm64.cdx.json",
	"image_scan_arm64":   "image-scan-arm64.json",
}

var releaseEvidenceOverlongDNSHostnameURL = "https://" + strings.Repeat("a", 63) + "." + strings.Repeat("b", 63) + "." + strings.Repeat("c", 63) + "." + strings.Repeat("d", 63) + "/path"

func TestReleaseEvidenceVerifierAcceptsCompleteRedactedBundle(t *testing.T) {
	root := repositoryRoot(t)
	bundle := writeReleaseEvidenceBundle(t, false)

	output, err := runReleaseEvidenceVerifier(t, root, bundle.directory)
	if err != nil {
		t.Fatalf("release evidence verifier rejected complete bundle: %v\n%s", err, output)
	}
	if !strings.Contains(string(output), "release evidence verified") {
		t.Fatalf("release evidence verifier success output = %q", output)
	}
	assertPathAbsent(t, filepath.Join(bundle.directory, "image.oci.tar"), "retained evidence bundle contains an OCI archive")
}

func TestReleaseEvidenceLayoutDigestUsesTheOCIManifestDescriptor(t *testing.T) {
	root := repositoryRoot(t)
	path := filepath.Join(t.TempDir(), "image.oci")
	want := writeReleaseEvidenceOCILayout(t, path)

	assertReleaseEvidenceLayoutDigest(t, root, path, want, "a complete OCI layout", "OCI manifest digest")
}

func TestReleaseEvidenceLayoutDigestAcceptsDockerOCICompatibilityMetadata(t *testing.T) {
	root := repositoryRoot(t)
	path := filepath.Join(t.TempDir(), "image.oci")
	want := writeReleaseEvidenceOCILayoutWithCompatibilityMetadata(t, path)

	assertReleaseEvidenceLayoutDigest(t, root, path, want, "Docker OCI compatibility metadata", "OCI manifest digest")
}

func TestReleaseEvidenceLayoutDigestRejectsDockerTopLevelDescriptorWithCompatibilityMetadata(t *testing.T) {
	root := repositoryRoot(t)
	path := filepath.Join(t.TempDir(), "image.oci")
	writeReleaseEvidenceOCILayoutWithCompatibilityMetadata(t, path)

	rewriteReleaseEvidenceOCIIndex(t, path, func(index map[string]any, manifests []any) {
		releaseEvidenceOCIIndexDescriptor(t, manifests)["mediaType"] = "application/vnd.docker.distribution.manifest.v2+json"
	})

	output, err := runReleaseEvidenceLayoutDigest(root, path)
	assertCommandRejected(t, output, err, "layout digest accepted a Docker top-level descriptor with compatibility metadata", "OCI layout index does not reference an OCI image manifest", "Docker top-level descriptor")
}

func TestReleaseEvidenceLayoutDigestUsesSingleChildOCIIndexManifestDescriptor(t *testing.T) {
	root := repositoryRoot(t)
	path := filepath.Join(t.TempDir(), "image.oci")
	want := writeReleaseEvidenceNestedOCIIndexLayout(t, path, func(manifestDigest string, manifestSize int) []byte {
		return []byte(fmt.Sprintf(`{"schemaVersion":2,"manifests":[{"mediaType":"application/vnd.oci.image.manifest.v1+json","digest":"sha256:%s","size":%d}]}`,
			manifestDigest, manifestSize))
	})

	assertReleaseEvidenceLayoutDigest(t, root, path, want, "a single-child OCI image index", "nested OCI image manifest digest")
}

func TestReleaseEvidenceLayoutDigestAcceptsBuildxDoubleNestedOCIIndexManifestDescriptor(t *testing.T) {
	root := repositoryRoot(t)
	path := filepath.Join(t.TempDir(), "image.oci")
	want := writeReleaseEvidenceOCIIndexChainLayout(t, path, 2, 1, "application/vnd.oci.image.manifest.v1+json")

	assertReleaseEvidenceLayoutDigest(t, root, path, want, "the bounded Buildx OCI index chain", "Buildx OCI image manifest digest")
}

func TestReleaseEvidenceLayoutDigestRejectsUnsafeBuildxOCIIndexChains(t *testing.T) {
	root := repositoryRoot(t)
	for _, test := range []struct {
		name                    string
		nestedIndexCount        int
		terminalDescriptorCount int
		terminalDescriptorType  string
		mutate                  func(t *testing.T, path string)
		failure                 string
	}{
		{
			name:                    "branching inner index",
			nestedIndexCount:        2,
			terminalDescriptorCount: 2,
			terminalDescriptorType:  "application/vnd.oci.image.manifest.v1+json",
			failure:                 "OCI image index must contain exactly one manifest descriptor",
		},
		{
			name:                    "unknown terminal descriptor type",
			nestedIndexCount:        2,
			terminalDescriptorCount: 1,
			terminalDescriptorType:  "application/vnd.docker.distribution.manifest.v2+json",
			failure:                 "OCI image index does not reference an OCI image manifest",
		},
		{
			name:                    "third nested OCI index",
			nestedIndexCount:        3,
			terminalDescriptorCount: 1,
			terminalDescriptorType:  "application/vnd.oci.image.manifest.v1+json",
			failure:                 "OCI image index descriptor chain exceeds the supported Buildx depth",
		},
		{
			name:                    "mismatched nested index size",
			nestedIndexCount:        2,
			terminalDescriptorCount: 1,
			terminalDescriptorType:  "application/vnd.oci.image.manifest.v1+json",
			mutate: func(t *testing.T, path string) {
				t.Helper()
				rewriteReleaseEvidenceOCIIndex(t, path, func(index map[string]any, manifests []any) {
					descriptor := releaseEvidenceOCIIndexDescriptor(t, manifests)
					size, ok := descriptor["size"].(float64)
					if !ok {
						t.Fatalf("test OCI index descriptor size = %#v", descriptor["size"])
					}
					descriptor["size"] = size + 1
				})
			},
			failure: "does not match a retained payload",
		},
		{
			name:                    "unreferenced nested payload",
			nestedIndexCount:        2,
			terminalDescriptorCount: 1,
			terminalDescriptorType:  "application/vnd.oci.image.manifest.v1+json",
			mutate: func(t *testing.T, path string) {
				t.Helper()
				data := []byte("unreferenced nested OCI payload")
				writeReleaseArtifact(t, filepath.Join(path, "blobs", "sha256", sha256Hex(data)), data)
			},
			failure: "unreferenced payload",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "image.oci")
			writeReleaseEvidenceOCIIndexChainLayout(t, path, test.nestedIndexCount, test.terminalDescriptorCount, test.terminalDescriptorType)
			if test.mutate != nil {
				test.mutate(t, path)
			}

			output, err := runReleaseEvidenceLayoutDigest(root, path)
			assertCommandRejected(t, output, err, fmt.Sprintf("layout digest accepted Buildx OCI index chain with %s", test.name), test.failure, fmt.Sprintf("Buildx OCI index chain %s", test.name))
		})
	}
}

func TestReleaseEvidenceLayoutDigestRejectsUnsafeNestedOCIIndexes(t *testing.T) {
	root := repositoryRoot(t)
	for _, test := range []struct {
		name        string
		nestedIndex func(manifestDigest string, manifestSize int) []byte
		failure     string
	}{
		{
			name: "multiple child descriptors",
			nestedIndex: func(manifestDigest string, manifestSize int) []byte {
				descriptor := fmt.Sprintf(`{"mediaType":"application/vnd.oci.image.manifest.v1+json","digest":"sha256:%s","size":%d}`, manifestDigest, manifestSize)
				return []byte(`{"schemaVersion":2,"manifests":[` + descriptor + `,` + descriptor + `]}`)
			},
			failure: "OCI image index must contain exactly one manifest descriptor",
		},
		{
			name: "non-image child descriptor",
			nestedIndex: func(manifestDigest string, manifestSize int) []byte {
				return []byte(fmt.Sprintf(`{"schemaVersion":2,"manifests":[{"mediaType":"application/vnd.docker.distribution.manifest.v2+json","digest":"sha256:%s","size":%d}]}`,
					manifestDigest, manifestSize))
			},
			failure: "OCI image index does not reference an OCI image manifest",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "image.oci")
			writeReleaseEvidenceNestedOCIIndexLayout(t, path, test.nestedIndex)

			output, err := runReleaseEvidenceLayoutDigest(root, path)
			assertCommandRejected(t, output, err, fmt.Sprintf("layout digest accepted nested OCI index with %s", test.name), test.failure, fmt.Sprintf("nested OCI index %s", test.name))
		})
	}
}

func TestReleaseEvidenceLayoutDigestRejectsFilesAndUnsafeDirectoryPaths(t *testing.T) {
	root := repositoryRoot(t)
	t.Run("archive file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "image.oci.tar")
		writeReleaseArtifact(t, path, []byte("not an OCI directory"))

		output, err := runReleaseEvidenceLayoutDigest(root, path)
		assertCommandRejected(t, output, err, "layout digest accepted an OCI archive file", "real directory", "OCI archive input")
	})

	t.Run("symlinked layout", func(t *testing.T) {
		target := filepath.Join(t.TempDir(), "image.oci")
		writeReleaseEvidenceOCILayout(t, target)
		path := filepath.Join(t.TempDir(), "image.oci")
		if err := os.Symlink(target, path); err != nil {
			t.Fatal(err)
		}

		output, err := runReleaseEvidenceLayoutDigest(root, path)
		assertCommandRejected(t, output, err, "layout digest accepted a symlinked OCI directory", "real directory", "symlinked OCI directory")
	})

	t.Run("noncanonical path", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "image.oci")
		writeReleaseEvidenceOCILayout(t, path)
		noncanonical := filepath.Dir(path) + string(filepath.Separator) + "." + string(filepath.Separator) + filepath.Base(path)

		output, err := runReleaseEvidenceLayoutDigest(root, noncanonical)
		assertCommandRejected(t, output, err, "layout digest accepted a noncanonical OCI directory path", "canonical path", "noncanonical OCI directory")
	})
}

func TestReleaseEvidenceLayoutDigestRejectsUnsafeOrUnboundDirectoryEntries(t *testing.T) {
	root := repositoryRoot(t)
	for _, test := range []struct {
		name    string
		mutate  func(t *testing.T, path string)
		failure string
	}{
		{
			name: "nested blob symlink",
			mutate: func(t *testing.T, path string) {
				t.Helper()
				if err := os.Symlink("missing-blob", filepath.Join(path, "blobs", "sha256", "nested-link")); err != nil {
					t.Fatal(err)
				}
			},
			failure: "contains a symlink",
		},
		{
			name: "unreferenced blob",
			mutate: func(t *testing.T, path string) {
				t.Helper()
				data := []byte("unreferenced OCI blob")
				writeReleaseArtifact(t, filepath.Join(path, "blobs", "sha256", sha256Hex(data)), data)
			},
			failure: "unreferenced payload",
		},
		{
			name: "mismatched config blob",
			mutate: func(t *testing.T, path string) {
				t.Helper()
				var index struct {
					Manifests []struct {
						Digest string `json:"digest"`
					} `json:"manifests"`
				}
				readJSONFile(t, filepath.Join(path, "index.json"), "test OCI index", &index)
				if len(index.Manifests) != 1 {
					t.Fatalf("cannot read test OCI index: %d manifests", len(index.Manifests))
				}
				var manifest struct {
					Config struct {
						Digest string `json:"digest"`
					} `json:"config"`
				}
				readJSONFile(t, filepath.Join(path, "blobs", "sha256", strings.TrimPrefix(index.Manifests[0].Digest, "sha256:")), "test OCI manifest", &manifest)
				writeReleaseArtifact(t, filepath.Join(path, "blobs", "sha256", strings.TrimPrefix(manifest.Config.Digest, "sha256:")), []byte("mismatched OCI config"))
			},
			failure: "does not match a retained payload",
		},
		{
			name: "multiple manifest descriptors",
			mutate: func(t *testing.T, path string) {
				t.Helper()
				rewriteReleaseEvidenceOCIIndex(t, path, func(index map[string]any, manifests []any) {
					index["manifests"] = append(manifests, manifests[0])
				})
			},
			failure: "exactly one manifest descriptor",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "image.oci")
			writeReleaseEvidenceOCILayout(t, path)
			test.mutate(t, path)

			output, err := runReleaseEvidenceLayoutDigest(root, path)
			assertCommandRejected(t, output, err, fmt.Sprintf("layout digest accepted %s", test.name), test.failure, test.name)
		})
	}
}

func TestReleaseEvidenceCollectorBuildsFixtureManifestFromWhitespaceMetadata(t *testing.T) {
	root := repositoryRoot(t)
	fixtureRoot := t.TempDir()
	metadata := "profile:   example-profile  \nupstream_url:  https://example.invalid/contracts  \nupstream_date:  '2026-07-15'  \n"
	fixtureDirectory := writeReleaseEvidenceFixtureMetadata(t, fixtureRoot, metadata)
	writeReleaseArtifact(t, filepath.Join(fixtureDirectory, "fixture.json"), []byte(`{"example":true}`))
	outputPath := filepath.Join(t.TempDir(), "fixture-manifest.json")

	output, err := runReleaseCollector(root, "fixture-manifest", "--root", fixtureRoot, "--output", outputPath)
	if err != nil {
		t.Fatalf("fixture collector rejected ordinary whitespace-delimited metadata: %v\n%s", err, output)
	}
	var manifest struct {
		Kind     string `json:"kind"`
		Status   string `json:"status"`
		Fixtures []struct {
			Profile      string `json:"profile"`
			UpstreamURL  string `json:"upstream_url"`
			UpstreamDate string `json:"upstream_date"`
		} `json:"fixtures"`
	}
	readJSONFile(t, outputPath, "fixture manifest", &manifest)
	if manifest.Kind != "fixture_manifest" || manifest.Status != "pass" || len(manifest.Fixtures) != 1 {
		t.Fatalf("fixture collector manifest = %#v", manifest)
	}
	fixture := manifest.Fixtures[0]
	if fixture.Profile != "example-profile" || fixture.UpstreamURL != "https://example.invalid/contracts" || fixture.UpstreamDate != "2026-07-15" {
		t.Fatalf("fixture collector did not normalize metadata: %#v", fixture)
	}
}

func TestReleaseEvidenceCollectorRejectsUnsafeProvenanceURLs(t *testing.T) {
	root := repositoryRoot(t)
	sentinel := "opaque-release-evidence-sentinel-0123456789"

	t.Run("fixture basic auth", func(t *testing.T) {
		fixtureRoot := t.TempDir()
		writeReleaseEvidenceFixtureMetadata(t, fixtureRoot, "profile: example-profile\nupstream_url: https://operator:"+sentinel+"@example.invalid/contracts\nupstream_date: 2026-07-15\n")
		outputPath := filepath.Join(t.TempDir(), "fixture-manifest.json")
		output, err := runReleaseCollector(root, "fixture-manifest", "--root", fixtureRoot, "--output", outputPath)
		assertCommandRejected(t, output, err, "fixture collector accepted basic-auth upstream URL", "userinfo", "fixture basic-auth")
		assertOutputOmitsSentinel(t, output, sentinel, "fixture basic-auth")
		assertPathAbsent(t, outputPath, "fixture collector wrote evidence after rejecting unsafe URL")
	})

	t.Run("dependency basic auth", func(t *testing.T) {
		directory := t.TempDir()
		baselinePath := filepath.Join(directory, "baseline.json")
		goModPath := filepath.Join(directory, "go-mod.json")
		writeReleaseArtifact(t, baselinePath, []byte(`{"trusted_module_roots":[{"path":"example.com/module","license":"Apache-2.0","source":"https://operator:`+sentinel+`@example.invalid/module"}]}`))
		writeReleaseArtifact(t, goModPath, []byte(`{"Require":[{"Path":"example.com/module","Version":"v1.2.3"}]}`))
		outputPath := filepath.Join(directory, "dependencies.json")
		output, err := runReleaseCollector(root, "dependency-license", "--baseline", baselinePath, "--go-mod", goModPath, "--output", outputPath)
		assertCommandRejected(t, output, err, "dependency collector accepted basic-auth source URL", "userinfo", "dependency basic-auth")
		assertOutputOmitsSentinel(t, output, sentinel, "dependency basic-auth")
		assertPathAbsent(t, outputPath, "dependency collector wrote evidence after rejecting unsafe URL")
	})
}

type releaseEvidenceProvenanceURLCase struct {
	name   string
	url    string
	accept bool
}

// releaseEvidenceProvenanceURLCases is the canonical provenance URL policy the
// collector and the verifier both enforce; only the first case is canonical.
// The basic-auth case embeds sentinel, which a rejection must not disclose.
func releaseEvidenceProvenanceURLCases(sentinel string) []releaseEvidenceProvenanceURLCase {
	return []releaseEvidenceProvenanceURLCase{
		{name: "accepted normal path", url: "https://example.invalid/provenance/path", accept: true},
		{name: "non-numeric port", url: "https://example.invalid:bad/path"},
		{name: "out-of-range port", url: "https://example.invalid:65536/path"},
		{name: "explicit HTTPS port", url: "https://example.invalid:443/path"},
		{name: "backslash authority", url: `https://example.invalid\path`},
		{name: "basic auth", url: "https://operator:" + sentinel + "@example.invalid/path"},
		{name: "query", url: "https://example.invalid/path?ref=main"},
		{name: "fragment", url: "https://example.invalid/path#section"},
		{name: "IPv4 literal", url: "https://127.0.0.1/path"},
		{name: "empty DNS label", url: "https://a..b/path"},
		{name: "leading hyphen DNS label", url: "https://a.-b/path"},
		{name: "trailing hyphen DNS label", url: "https://a-.b/path"},
		{name: "overlong DNS hostname", url: releaseEvidenceOverlongDNSHostnameURL},
	}
}

func TestReleaseEvidenceCollectorEnforcesCanonicalProvenanceURLPolicy(t *testing.T) {
	root := repositoryRoot(t)
	sentinel := "opaque-release-evidence-sentinel-0123456789"
	for _, test := range releaseEvidenceProvenanceURLCases(sentinel) {
		for _, kind := range []string{"fixture", "dependency"} {
			t.Run(kind+" "+test.name, func(t *testing.T) {
				directory := t.TempDir()
				var output []byte
				var err error
				var outputPath string
				switch kind {
				case "fixture":
					writeReleaseEvidenceFixtureMetadata(t, directory, "profile: example-profile\nupstream_url: "+test.url+"\nupstream_date: 2026-07-15\n")
					outputPath = filepath.Join(directory, "fixture-manifest.json")
					output, err = runReleaseCollector(root, "fixture-manifest", "--root", directory, "--output", outputPath)
				case "dependency":
					baselinePath := filepath.Join(directory, "baseline.json")
					goModPath := filepath.Join(directory, "go-mod.json")
					baseline, marshalErr := json.Marshal(map[string]any{
						"trusted_module_roots": []map[string]string{{
							"path":    "example.com/module",
							"license": "Apache-2.0",
							"source":  test.url,
						}},
					})
					if marshalErr != nil {
						t.Fatal(marshalErr)
					}
					writeReleaseArtifact(t, baselinePath, baseline)
					writeReleaseArtifact(t, goModPath, []byte(`{"Require":[{"Path":"example.com/module","Version":"v1.2.3"}]}`))
					outputPath = filepath.Join(directory, "dependencies.json")
					output, err = runReleaseCollector(root, "dependency-license", "--baseline", baselinePath, "--go-mod", goModPath, "--output", outputPath)
				default:
					t.Fatalf("unknown provenance kind %q", kind)
				}
				if test.accept {
					if err != nil {
						t.Fatalf("collector rejected canonical %s URL: %v\n%s", kind, err, output)
					}
					if _, err := os.Stat(outputPath); err != nil {
						t.Fatalf("collector did not write canonical %s evidence: %v", kind, err)
					}
					return
				}
				assertCommandRejected(t, output, err, fmt.Sprintf("collector accepted noncanonical %s URL %q", kind, test.url), "DNS hostname", fmt.Sprintf("noncanonical %s URL", kind))
				assertOutputOmitsSentinel(t, output, sentinel, fmt.Sprintf("noncanonical %s URL", kind))
				assertPathAbsent(t, outputPath, fmt.Sprintf("collector wrote %s evidence after rejecting URL", kind))
			})
		}
	}
}

func TestDependencyEvidenceUsesCurrentVersionsUnderTrustedRoots(t *testing.T) {
	t.Parallel()

	root := repositoryRoot(t)
	directory := t.TempDir()
	baselinePath := filepath.Join(directory, "baseline.json")
	goModPath := filepath.Join(directory, "go-mod.json")
	outputPath := filepath.Join(directory, "dependencies.json")
	writeReleaseArtifact(t, baselinePath, []byte(`{"trusted_module_roots":[{"path":"example.com/publisher","license":"Apache-2.0","source":"https://example.com/publisher"}]}`))
	writeReleaseArtifact(t, goModPath, []byte(`{"Require":[{"Path":"example.com/publisher/module","Version":"v9.8.7"},{"Path":"example.com/indirect","Version":"v1.0.0","Indirect":true}]}`))

	if output, err := runReleaseCollector(root, "dependency-license", "--baseline", baselinePath, "--go-mod", goModPath, "--output", outputPath); err != nil {
		t.Fatalf("dependency collector failed: %v\n%s", err, output)
	}
	var evidence struct {
		DirectModules []map[string]string `json:"direct_modules"`
	}
	readJSONFile(t, outputPath, "dependency evidence", &evidence)
	want := []map[string]string{{
		"path": "example.com/publisher/module", "version": "v9.8.7", "license": "Apache-2.0", "source": "https://example.com/publisher",
	}}
	if !reflect.DeepEqual(evidence.DirectModules, want) {
		t.Fatalf("direct modules = %#v, want %#v", evidence.DirectModules, want)
	}
}

func TestReleaseEvidenceCollectorRejectsNonContainerTrivyReport(t *testing.T) {
	root := repositoryRoot(t)
	inputPath, outputPath := tempFilePaths(t, "scan.json", "image-scan.json")
	digest := "sha256:" + strings.Repeat("b", 64)
	writeReleaseArtifact(t, inputPath, []byte(`{"SchemaVersion":2,"ArtifactType":"filesystem","ArtifactName":"image.oci","Results":[]}`))

	output, err := runReleaseCollector(root, "annotate-scan", "--input", inputPath, "--output", outputPath, "--reference", "llm-temporal-worker@"+digest, "--digest", digest)
	assertCommandRejected(t, output, err, "scan collector accepted a non-container Trivy report", "container-image", "non-container scan")
	assertPathAbsent(t, outputPath, "scan collector wrote evidence after rejecting non-container report")
}

func TestReleaseEvidenceCollectorNormalizesTemporaryTrivyOCIDirectoryName(t *testing.T) {
	root := repositoryRoot(t)
	digest := "sha256:" + strings.Repeat("b", 64)
	for _, test := range []struct {
		name         string
		artifactName string
		accept       bool
	}{
		{name: "temporary absolute OCI directory path", artifactName: "/private/runner-temp/image.oci", accept: true},
		{name: "unexpected OCI directory basename", artifactName: "/private/runner-temp/other-image.oci", accept: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			directory := t.TempDir()
			inputPath := filepath.Join(directory, "scan.json")
			outputPath := filepath.Join(directory, "image-scan.json")
			writeReleaseArtifact(t, inputPath, []byte(`{"SchemaVersion":2,"ArtifactType":"container_image","ArtifactName":"`+test.artifactName+`","Results":[]}`))

			output, err := runReleaseCollector(root, "annotate-scan", "--input", inputPath, "--output", outputPath, "--reference", "llm-temporal-worker@"+digest, "--digest", digest)
			if test.accept {
				if err != nil {
					t.Fatalf("scan collector rejected temporary OCI directory path: %v\n%s", err, output)
				}
				document := readReleaseEvidenceJSONArtifact(t, directory, "image_scan")
				if artifactName, _ := document["ArtifactName"].(string); artifactName != "image.oci" {
					t.Fatalf("annotated scan artifact name = %q, want stable basename", artifactName)
				}
				return
			}
			assertCommandRejected(t, output, err, "scan collector accepted unexpected temporary OCI directory basename", "temporary OCI directory", "unexpected OCI directory basename")
			assertPathAbsent(t, outputPath, "scan collector wrote evidence after rejecting OCI directory basename")
		})
	}
}

func TestReleaseEvidenceCollectorRejectsRetainedOrSymlinkedOCIDirectoryDestination(t *testing.T) {
	root := repositoryRoot(t)
	for _, test := range []struct {
		name    string
		prepare func(t *testing.T, artifactDirectory string) string
		want    string
	}{
		{
			name: "artifact directory",
			prepare: func(_ *testing.T, artifactDirectory string) string {
				return filepath.Join(artifactDirectory, "image.oci")
			},
			want: "outside the artifact directory",
		},
		{
			name: "dangling symlink",
			prepare: func(t *testing.T, _ string) string {
				t.Helper()
				layoutPath := filepath.Join(t.TempDir(), "image.oci")
				if err := os.Symlink(filepath.Join(t.TempDir(), "missing-image.oci"), layoutPath); err != nil {
					t.Fatal(err)
				}
				return layoutPath
			},
			want: "path must not already exist",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			artifactDirectory := t.TempDir()
			layoutPath := test.prepare(t, artifactDirectory)
			command := exec.Command(
				"bash", filepath.Join(root, "scripts", "release", "collect.sh"),
				"--artifact-dir", artifactDirectory,
				"--image-oci-layout", layoutPath,
			)
			command.Dir = root
			output, err := command.CombinedOutput()
			assertCommandRejected(t, output, err, "collector accepted unsafe temporary OCI directory destination", test.want, "unsafe temporary OCI directory destination")
		})
	}
}

func TestReleaseEvidenceCollectorRedactsComposeLogsAndRequiresServiceBoundaries(t *testing.T) {
	root := repositoryRoot(t)
	sentinel := "api_key=release-evidence-sentinel-0123456789"
	for _, test := range []struct {
		name  string
		kind  string
		input string
		event string
	}{
		{
			name:  "Redis",
			kind:  "redis_log",
			input: "2026-07-15T00:00:00Z redis-1 | Ready to accept connections\n2026-07-15T00:00:01Z redis-1 | " + sentinel + "\n",
			event: "redis_ready",
		},
		{
			name:  "Temporal",
			kind:  "temporal_log",
			input: "2026-07-15T00:00:00Z temporal-1 | Temporal server started\n2026-07-15T00:00:01Z temporal-1 | " + sentinel + "\n",
			event: "temporal_started",
		},
		{
			name:  "combined Compose",
			kind:  "compose_log",
			input: "2026-07-15T00:00:00Z redis-1 | Ready to accept connections\n2026-07-15T00:00:01Z temporal-1 | Temporal server started\n2026-07-15T00:00:02Z temporal-1 | " + sentinel + "\n",
			event: "temporal_started",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			inputPath, outputPath := tempFilePaths(t, "compose.log", "redacted-log.json")
			writeReleaseArtifact(t, inputPath, []byte(test.input))
			output, err := runReleaseCollector(root, "redacted-log", "--kind", test.kind, "--input", inputPath, "--output", outputPath)
			if err != nil {
				t.Fatalf("redacted-log collector rejected %s input: %v\n%s", test.name, err, output)
			}
			data := readText(t, outputPath)
			if strings.Contains(data, sentinel) {
				t.Fatalf("redacted-log artifact retained sentinel secret: %s", data)
			}
			var artifact struct {
				EventCounts map[string]int `json:"event_counts"`
			}
			decodeJSON(t, []byte(data), "redacted-log artifact", &artifact)
			if artifact.EventCounts[test.event] != 1 || artifact.EventCounts["redacted_line"] != 1 {
				t.Fatalf("redacted-log event counts = %#v", artifact.EventCounts)
			}
			if test.kind == "compose_log" && artifact.EventCounts["redis_ready"] != 1 {
				t.Fatalf("combined Compose log did not retain Redis boundary count: %#v", artifact.EventCounts)
			}
		})
	}

	for _, test := range []struct {
		name  string
		kind  string
		input string
	}{
		{name: "Redis has only redacted lines", kind: "redis_log", input: "2026-07-15T00:00:00Z redis-1 | " + sentinel + "\n"},
		{name: "Temporal has only redacted lines", kind: "temporal_log", input: "2026-07-15T00:00:00Z temporal-1 | " + sentinel + "\n"},
		{name: "combined Compose lacks Temporal", kind: "compose_log", input: "2026-07-15T00:00:00Z redis-1 | Ready to accept connections\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			inputPath, outputPath := tempFilePaths(t, "compose.log", "redacted-log.json")
			writeReleaseArtifact(t, inputPath, []byte(test.input))
			output, err := runReleaseCollector(root, "redacted-log", "--kind", test.kind, "--input", inputPath, "--output", outputPath)
			assertCommandRejected(t, output, err, "redacted-log collector accepted missing service boundary", "runtime-boundary", "missing boundary")
			assertPathAbsent(t, outputPath, "redacted-log collector wrote an artifact after rejecting input")
		})
	}
}

func TestReleaseEvidenceVerifierRejectsChangedArtifact(t *testing.T) {
	root := repositoryRoot(t)
	bundle := writeReleaseEvidenceBundle(t, false)
	writeFile(t, filepath.Join(bundle.directory, "test-summary.json"), []byte(`{"status":"changed"}`), 0o600)

	assertReleaseEvidenceVerifierRejects(t, root, bundle.directory, "release evidence verifier accepted changed artifact", "sha256", "changed artifact")
}

func TestReleaseEvidenceVerifierRejectsIncorrectByteLength(t *testing.T) {
	root := repositoryRoot(t)
	bundle := writeReleaseEvidenceBundle(t, false)
	evidence := readReleaseEvidence(t, bundle.directory)
	artifacts := evidence["artifacts"].(map[string]any)
	testSummary := artifacts["test_summary"].(map[string]any)
	testSummary["bytes"] = testSummary["bytes"].(float64) + 1
	writeReleaseEvidence(t, bundle.directory, evidence)

	assertReleaseEvidenceVerifierRejects(t, root, bundle.directory, "release evidence verifier accepted an incorrect byte length", "byte length", "incorrect byte length")
}

func TestReleaseEvidenceVerifierRejectsUnsafeEvidenceRepository(t *testing.T) {
	root := repositoryRoot(t)
	sentinel := "release-evidence-sentinel-0123456789"
	for _, test := range []struct {
		name       string
		repository string
		want       string
	}{
		{
			name:       "sentinel query value",
			repository: "https://github.com/mfow/llm-temporal-worker?api_key=" + sentinel,
			want:       "secret-like",
		},
		{
			name:       "basic auth userinfo",
			repository: "https://operator:" + sentinel + "@example.invalid/llm-temporal-worker",
			want:       "source repository",
		},
		{
			name:       "ordinary query",
			repository: "https://github.com/mfow/llm-temporal-worker?ref=master",
			want:       "source repository",
		},
		{
			name:       "fragment",
			repository: "https://github.com/mfow/llm-temporal-worker#release-evidence",
			want:       "source repository",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			bundle := writeReleaseEvidenceBundle(t, false)
			evidence := readReleaseEvidence(t, bundle.directory)
			evidence["source"].(map[string]any)["repository"] = test.repository
			writeReleaseEvidence(t, bundle.directory, evidence)

			output, err := runReleaseEvidenceVerifier(t, root, bundle.directory)
			assertCommandRejected(t, output, err, "release evidence verifier accepted unsafe repository URL", test.want, "unsafe repository")
			assertOutputOmitsSentinel(t, output, sentinel, "unsafe repository")
		})
	}
}

func TestReleaseEvidenceVerifierRejectsUnsafeArtifactProvenanceURLs(t *testing.T) {
	root := repositoryRoot(t)
	sentinel := "opaque-release-evidence-sentinel-0123456789"
	for _, test := range []struct {
		name     string
		artifact string
		apply    func(t *testing.T, document map[string]any, value string)
		want     string
	}{
		{
			name:     "fixture basic auth",
			artifact: "fixture_manifest",
			apply: func(t *testing.T, document map[string]any, value string) {
				t.Helper()
				fixtures := document["fixtures"].([]any)
				fixtures[0].(map[string]any)["upstream_url"] = value
			},
			want: "unsafe upstream URL",
		},
		{
			name:     "dependency basic auth",
			artifact: "dependency_license",
			apply: func(t *testing.T, document map[string]any, value string) {
				t.Helper()
				modules := document["direct_modules"].([]any)
				modules[0].(map[string]any)["source"] = value
			},
			want: "unsafe source URL",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			bundle := writeReleaseEvidenceBundle(t, false)
			evidence := readReleaseEvidence(t, bundle.directory)
			artifactPath := filepath.Join(bundle.directory, releaseEvidenceArtifactPaths[test.artifact])
			document := readJSONObject(t, artifactPath)
			test.apply(t, document, "https://operator:"+sentinel+"@example.invalid/provenance")
			data := marshalJSON(t, document)
			replaceReleaseEvidenceArtifact(t, bundle, evidence, test.artifact, data)
			writeReleaseEvidence(t, bundle.directory, evidence)

			output, err := runReleaseEvidenceVerifier(t, root, bundle.directory)
			assertCommandRejected(t, output, err, fmt.Sprintf("release evidence verifier accepted unsafe %s provenance URL", test.name), test.want, fmt.Sprintf("unsafe %s provenance", test.name))
			assertOutputOmitsSentinel(t, output, sentinel, fmt.Sprintf("unsafe %s provenance", test.name))
		})
	}
}

func TestReleaseEvidenceVerifierEnforcesCanonicalProvenanceURLPolicy(t *testing.T) {
	root := repositoryRoot(t)
	sentinel := "opaque-release-evidence-sentinel-0123456789"
	for _, test := range releaseEvidenceProvenanceURLCases(sentinel) {
		for _, target := range []string{"repository", "fixture", "dependency"} {
			t.Run(target+" "+test.name, func(t *testing.T) {
				bundle := writeReleaseEvidenceBundle(t, false)
				evidence := readReleaseEvidence(t, bundle.directory)
				switch target {
				case "repository":
					evidence["source"].(map[string]any)["repository"] = test.url
				case "fixture", "dependency":
					artifact := "fixture_manifest"
					if target == "dependency" {
						artifact = "dependency_license"
					}
					path := filepath.Join(bundle.directory, releaseEvidenceArtifactPaths[artifact])
					document := readJSONObject(t, path)
					if target == "fixture" {
						document["fixtures"].([]any)[0].(map[string]any)["upstream_url"] = test.url
					} else {
						document["direct_modules"].([]any)[0].(map[string]any)["source"] = test.url
					}
					data := marshalJSON(t, document)
					replaceReleaseEvidenceArtifact(t, bundle, evidence, artifact, data)
				default:
					t.Fatalf("unknown provenance target %q", target)
				}
				writeReleaseEvidence(t, bundle.directory, evidence)

				output, err := runReleaseEvidenceVerifier(t, root, bundle.directory)
				if test.accept {
					if err != nil {
						t.Fatalf("verifier rejected canonical %s URL: %v\n%s", target, err, output)
					}
					return
				}
				if err == nil {
					t.Fatalf("verifier accepted noncanonical %s URL %q:\n%s", target, test.url, output)
				}
				assertOutputOmitsSentinel(t, output, sentinel, fmt.Sprintf("noncanonical %s URL", target))
			})
		}
	}
}

func TestReleaseEvidenceSchemasEnforceDNSProvenancePolicy(t *testing.T) {
	root := repositoryRoot(t)
	bundle := writeReleaseEvidenceBundle(t, false)
	evidenceSchema := compileRepositoryJSONSchema(t, root, filepath.Join("docs", "release", "evidence.schema.json"), "urn:llmtw:release-evidence:v1")
	artifactSchema := compileRepositoryJSONSchema(t, root, filepath.Join("docs", "release", "artifact.schema.json"), "urn:llmtw:release-evidence-artifact:v1")
	baseEvidence := readReleaseEvidence(t, bundle.directory)
	baseFixture := readReleaseEvidenceJSONArtifact(t, bundle.directory, "fixture_manifest")
	baseDependency := readReleaseEvidenceJSONArtifact(t, bundle.directory, "dependency_license")

	for _, test := range []struct {
		name   string
		url    string
		accept bool
	}{
		{name: "accepted DNS labels", url: "https://example.invalid/provenance/path", accept: true},
		{name: "IPv4 literal", url: "https://127.0.0.1/path"},
		{name: "empty DNS label", url: "https://a..b/path"},
		{name: "leading hyphen DNS label", url: "https://a.-b/path"},
		{name: "trailing hyphen DNS label", url: "https://a-.b/path"},
		{name: "overlong DNS hostname", url: releaseEvidenceOverlongDNSHostnameURL},
	} {
		for _, target := range []string{"repository", "fixture", "dependency"} {
			t.Run(target+" "+test.name, func(t *testing.T) {
				var schema *jsonschema.Schema
				var document map[string]any
				switch target {
				case "repository":
					schema = evidenceSchema
					document = cloneReleaseEvidenceJSON(t, baseEvidence)
					document["source"].(map[string]any)["repository"] = test.url
				case "fixture":
					schema = artifactSchema
					document = cloneReleaseEvidenceJSON(t, baseFixture)
					document["fixtures"].([]any)[0].(map[string]any)["upstream_url"] = test.url
				case "dependency":
					schema = artifactSchema
					document = cloneReleaseEvidenceJSON(t, baseDependency)
					document["direct_modules"].([]any)[0].(map[string]any)["source"] = test.url
				default:
					t.Fatalf("unknown provenance target %q", target)
				}

				err := validateJSONSchema(schema, document)
				if test.accept && err != nil {
					t.Fatalf("schema rejected canonical %s URL: %v", target, err)
				}
				if !test.accept && err == nil {
					t.Fatalf("schema accepted invalid %s URL %q", target, test.url)
				}
			})
		}
	}
}

func TestReleaseEvidenceVerifierRejectsJSONEscapedSecretKeys(t *testing.T) {
	root := repositoryRoot(t)
	sentinel := "escaped-release-evidence-sentinel-0123456789"
	for _, test := range []struct {
		name     string
		artifact string
		contents func(bundle releaseEvidenceBundle) string
	}{
		{
			name:     "SBOM",
			artifact: "sbom",
			contents: func(bundle releaseEvidenceBundle) string {
				return strings.TrimSuffix(releaseEvidenceSBOM(bundle.imageReference, bundle.imageDigest), "}") + `,"api\u005fkey":"` + sentinel + `"}`
			},
		},
		{
			name:     "Trivy scan",
			artifact: "image_scan",
			contents: func(bundle releaseEvidenceBundle) string {
				return strings.TrimSuffix(releaseEvidenceScan(bundle.imageReference, bundle.imageDigest, false), "}") + `,"api\u005fkey":"` + sentinel + `"}`
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			bundle := writeReleaseEvidenceBundle(t, false)
			evidence := readReleaseEvidence(t, bundle.directory)
			replaceReleaseEvidenceArtifact(t, bundle, evidence, test.artifact, []byte(test.contents(bundle)))
			writeReleaseEvidence(t, bundle.directory, evidence)

			output, err := runReleaseEvidenceVerifier(t, root, bundle.directory)
			assertCommandRejected(t, output, err, fmt.Sprintf("release evidence verifier accepted escaped secret key in %s", test.name), "secret-like", "escaped secret-key")
			assertOutputOmitsSentinel(t, output, sentinel, "escaped secret-key")
		})
	}
}

func TestReleaseEvidenceVerifierRejectsDuplicateJSONMembers(t *testing.T) {
	root := repositoryRoot(t)

	t.Run("evidence record", func(t *testing.T) {
		bundle := writeReleaseEvidenceBundle(t, false)
		path := filepath.Join(bundle.directory, "evidence.json")
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		ambiguous := append([]byte(`{"schema_version":1,`), bytes.TrimPrefix(data, []byte{'{'})...)
		writeReleaseArtifact(t, path, ambiguous)

		assertReleaseEvidenceVerifierRejects(t, root, bundle.directory, "release evidence verifier accepted a duplicate evidence member", "invalid or ambiguous JSON", "duplicate evidence-member")
	})

	for _, test := range []struct {
		name     string
		artifact string
		contents func(bundle releaseEvidenceBundle) string
	}{
		{
			name:     "Trivy severity",
			artifact: "image_scan",
			contents: func(bundle releaseEvidenceBundle) string {
				return strings.Replace(
					releaseEvidenceScan(bundle.imageReference, bundle.imageDigest, true),
					`"Severity":"CRITICAL"`,
					`"Severity":"CRITICAL","Severity":"LOW"`,
					1,
				)
			},
		},
		{
			name:     "SBOM component type",
			artifact: "sbom",
			contents: func(bundle releaseEvidenceBundle) string {
				return strings.Replace(
					releaseEvidenceSBOM(bundle.imageReference, bundle.imageDigest),
					`"type":"container"`,
					`"type":"library","type":"container"`,
					1,
				)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			bundle := writeReleaseEvidenceBundle(t, false)
			evidence := readReleaseEvidence(t, bundle.directory)
			replaceReleaseEvidenceArtifact(t, bundle, evidence, test.artifact, []byte(test.contents(bundle)))
			writeReleaseEvidence(t, bundle.directory, evidence)

			output, err := runReleaseEvidenceVerifier(t, root, bundle.directory)
			assertCommandRejected(t, output, err, fmt.Sprintf("release evidence verifier accepted duplicate JSON members in %s", test.name), "invalid or ambiguous JSON", fmt.Sprintf("duplicate %s-member", test.name))
			if strings.Contains(string(output), "Severity") || strings.Contains(string(output), "component type") {
				t.Fatalf("duplicate-member failure disclosed an input member name: %q", output)
			}
		})
	}
}

func TestReleaseEvidenceVerifierRejectsEscapingAndSymlinkedArtifacts(t *testing.T) {
	root := repositoryRoot(t)
	for _, mutate := range []struct {
		name  string
		apply func(t *testing.T, directory string, evidence map[string]any)
	}{
		{
			name: "path escape",
			apply: func(t *testing.T, directory string, evidence map[string]any) {
				t.Helper()
				outside := filepath.Join(filepath.Dir(directory), "outside.json")
				data := []byte(`{"status":"outside"}`)
				writeReleaseArtifact(t, outside, data)
				artifact := evidence["artifacts"].(map[string]any)["test_summary"].(map[string]any)
				artifact["path"] = "../outside.json"
				artifact["bytes"] = len(data)
				artifact["sha256"] = sha256Hex(data)
			},
		},
		{
			name: "renamed canonical artifact",
			apply: func(t *testing.T, _ string, evidence map[string]any) {
				t.Helper()
				evidence["artifacts"].(map[string]any)["test_summary"].(map[string]any)["path"] = "renamed-test-summary.json"
			},
		},
		{
			name: "nested canonical artifact",
			apply: func(t *testing.T, _ string, evidence map[string]any) {
				t.Helper()
				evidence["artifacts"].(map[string]any)["test_summary"].(map[string]any)["path"] = "nested/test-summary.json"
			},
		},
		{
			name: "symlink",
			apply: func(t *testing.T, directory string, evidence map[string]any) {
				t.Helper()
				outside := filepath.Join(filepath.Dir(directory), "outside.json")
				data := []byte(`{"status":"outside"}`)
				writeReleaseArtifact(t, outside, data)
				path := filepath.Join(directory, "test-summary.json")
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(outside, path); err != nil {
					t.Fatal(err)
				}
				artifact := evidence["artifacts"].(map[string]any)["test_summary"].(map[string]any)
				artifact["bytes"] = len(data)
				artifact["sha256"] = sha256Hex(data)
			},
		},
	} {
		t.Run(mutate.name, func(t *testing.T) {
			bundle := writeReleaseEvidenceBundle(t, false)
			evidence := readReleaseEvidence(t, bundle.directory)
			mutate.apply(t, bundle.directory, evidence)
			writeReleaseEvidence(t, bundle.directory, evidence)

			assertReleaseEvidenceVerifierRejects(t, root, bundle.directory, fmt.Sprintf("release evidence verifier accepted %s", mutate.name), "path", mutate.name)
		})
	}
}

func TestReleaseEvidenceVerifierRejectsUnknownArtifact(t *testing.T) {
	root := repositoryRoot(t)
	bundle := writeReleaseEvidenceBundle(t, false)
	evidence := readReleaseEvidence(t, bundle.directory)
	evidence["artifacts"].(map[string]any)["unexpected"] = map[string]any{
		"path":     "unexpected.json",
		"bytes":    1,
		"sha256":   strings.Repeat("a", 64),
		"redacted": true,
	}
	writeReleaseEvidence(t, bundle.directory, evidence)

	assertReleaseEvidenceVerifierRejects(t, root, bundle.directory, "release evidence verifier accepted an unknown artifact", "does not satisfy schema", "unknown artifact")
}

func TestReleaseEvidenceVerifierRejectsCriticalImageFinding(t *testing.T) {
	root := repositoryRoot(t)
	bundle := writeReleaseEvidenceBundle(t, true)

	assertReleaseEvidenceVerifierRejects(t, root, bundle.directory, "release evidence verifier accepted critical image finding", "HIGH or CRITICAL", "critical image finding")
}

func TestReleaseEvidenceVerifierRejectsNonContainerTrivyReport(t *testing.T) {
	root := repositoryRoot(t)
	bundle := writeReleaseEvidenceBundle(t, false)
	evidence := readReleaseEvidence(t, bundle.directory)
	scan := strings.Replace(releaseEvidenceScan(bundle.imageReference, bundle.imageDigest, false), `"ArtifactType":"container_image"`, `"ArtifactType":"filesystem"`, 1)
	replaceReleaseEvidenceArtifact(t, bundle, evidence, "image_scan", []byte(scan))
	writeReleaseEvidence(t, bundle.directory, evidence)

	assertReleaseEvidenceVerifierRejects(t, root, bundle.directory, "release evidence verifier accepted a non-container Trivy report", "container-image", "non-container Trivy report")
}

func TestReleaseEvidenceVerifierRejectsUnnormalizedTemporaryTrivyOCIDirectoryName(t *testing.T) {
	root := repositoryRoot(t)
	bundle := writeReleaseEvidenceBundle(t, false)
	evidence := readReleaseEvidence(t, bundle.directory)
	scan := strings.Replace(releaseEvidenceScan(bundle.imageReference, bundle.imageDigest, false), `"ArtifactName":"image.oci"`, `"ArtifactName":"/private/runner-temp/image.oci"`, 1)
	replaceReleaseEvidenceArtifact(t, bundle, evidence, "image_scan", []byte(scan))
	writeReleaseEvidence(t, bundle.directory, evidence)

	assertReleaseEvidenceVerifierRejects(t, root, bundle.directory, "release evidence verifier accepted a runner-temporary Trivy path", "normalized to the temporary OCI directory basename", "unnormalized temporary Trivy path")
}

func TestReleaseEvidenceVerifierRejectsMutableAndStaleImageSubjects(t *testing.T) {
	root := repositoryRoot(t)
	staleDigest := "sha256:" + strings.Repeat("c", 64)
	staleReference := "llm-temporal-worker@" + staleDigest
	for _, mutate := range []struct {
		name  string
		apply func(t *testing.T, bundle releaseEvidenceBundle, evidence map[string]any)
	}{
		{
			name: "mutable image tag",
			apply: func(_ *testing.T, _ releaseEvidenceBundle, evidence map[string]any) {
				evidence["image"].(map[string]any)["reference"] = "llm-temporal-worker:image-verify"
			},
		},
		{
			name: "stale SBOM subject",
			apply: func(t *testing.T, bundle releaseEvidenceBundle, evidence map[string]any) {
				replaceReleaseEvidenceArtifact(t, bundle, evidence, "sbom", []byte(releaseEvidenceSBOM(staleReference, staleDigest)))
			},
		},
		{
			name: "stale scan subject",
			apply: func(t *testing.T, bundle releaseEvidenceBundle, evidence map[string]any) {
				replaceReleaseEvidenceArtifact(t, bundle, evidence, "image_scan", []byte(releaseEvidenceScan(staleReference, staleDigest, false)))
			},
		},
	} {
		t.Run(mutate.name, func(t *testing.T) {
			bundle := writeReleaseEvidenceBundle(t, false)
			evidence := readReleaseEvidence(t, bundle.directory)
			mutate.apply(t, bundle, evidence)
			writeReleaseEvidence(t, bundle.directory, evidence)

			output, err := runReleaseEvidenceVerifier(t, root, bundle.directory)
			if err == nil {
				t.Fatalf("release evidence verifier accepted %s:\n%s", mutate.name, output)
			}
			if !strings.Contains(strings.ToLower(string(output)), "image") && !strings.Contains(strings.ToLower(string(output)), "oci") {
				t.Fatalf("%s failure = %q, want immutable-image evidence", mutate.name, output)
			}
		})
	}
}

func TestReleaseEvidenceVerifierRejectsUnstructuredAndFailingSummaries(t *testing.T) {
	root := repositoryRoot(t)
	for _, mutate := range []struct {
		name     string
		artifact string
		contents string
	}{
		{name: "raw test output", artifact: "test_summary", contents: "ok\n"},
		{name: "failing race summary", artifact: "race_summary", contents: `{"status":"fail"}`},
	} {
		t.Run(mutate.name, func(t *testing.T) {
			bundle := writeReleaseEvidenceBundle(t, false)
			data := []byte(mutate.contents)
			writeReleaseArtifact(t, filepath.Join(bundle.directory, releaseEvidenceArtifactPaths[mutate.artifact]), data)
			evidence := readReleaseEvidence(t, bundle.directory)
			artifact := evidence["artifacts"].(map[string]any)[mutate.artifact].(map[string]any)
			artifact["bytes"] = len(data)
			artifact["sha256"] = sha256Hex(data)
			writeReleaseEvidence(t, bundle.directory, evidence)

			assertReleaseEvidenceVerifierRejects(t, root, bundle.directory, fmt.Sprintf("release evidence verifier accepted %s", mutate.name), "summary", mutate.name)
		})
	}
}

func TestReleaseEvidenceVerifierRejectsMissingServiceLogBoundaries(t *testing.T) {
	root := repositoryRoot(t)
	for _, test := range []struct {
		name     string
		artifact string
		contents string
		want     string
	}{
		{
			name:     "Redis",
			artifact: "redis_log",
			contents: `{"schema_version":1,"kind":"redis_log","status":"pass","service":"redis","source":"docker_compose_logs","redaction_policy":"allowlist-v1","line_count":1,"input_bytes":1,"event_counts":{"redacted_line":1},"redacted":true}`,
			want:     "Redis runtime-boundary",
		},
		{
			name:     "Temporal",
			artifact: "temporal_log",
			contents: `{"schema_version":1,"kind":"temporal_log","status":"pass","service":"temporal","source":"docker_compose_logs","redaction_policy":"allowlist-v1","line_count":1,"input_bytes":1,"event_counts":{"redacted_line":1},"redacted":true}`,
			want:     "Temporal runtime-boundary",
		},
		{
			name:     "combined Compose missing Temporal",
			artifact: "compose_log",
			contents: `{"schema_version":1,"kind":"compose_log","status":"pass","service":"compose","source":"docker_compose_logs","redaction_policy":"allowlist-v1","line_count":1,"input_bytes":1,"event_counts":{"redis_ready":1},"redacted":true}`,
			want:     "Redis and Temporal",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			bundle := writeReleaseEvidenceBundle(t, false)
			evidence := readReleaseEvidence(t, bundle.directory)
			replaceReleaseEvidenceArtifact(t, bundle, evidence, test.artifact, []byte(test.contents))
			writeReleaseEvidence(t, bundle.directory, evidence)

			assertReleaseEvidenceVerifierRejects(t, root, bundle.directory, fmt.Sprintf("release evidence verifier accepted a missing %s boundary", test.name), test.want, fmt.Sprintf("missing %s boundary", test.name))
		})
	}
}

func TestReleaseEvidenceVerifierRejectsUnreferencedArtifactFile(t *testing.T) {
	assertVerifierRejectsUnreferencedFile(t, "orphan-secret.json", "api_key=orphaned-release-evidence-secret-0123456789", "an unreferenced artifact file", "unreferenced artifact")
}

func TestReleaseEvidenceVerifierRejectsResidualOCIArchive(t *testing.T) {
	assertVerifierRejectsUnreferencedFile(t, "image.oci.tar", "api_key=residual-oci-archive-secret-0123456789", "a residual OCI archive", "residual OCI archive")
}

// assertVerifierRejectsUnreferencedFile adds name, holding sentinel, to a
// valid bundle and requires the verifier to reject it as an unreferenced file
// without disclosing sentinel; accepted and failure describe the file.
func assertVerifierRejectsUnreferencedFile(t *testing.T, name, sentinel, accepted, failure string) {
	t.Helper()
	root := repositoryRoot(t)
	bundle := writeReleaseEvidenceBundle(t, false)
	writeReleaseArtifact(t, filepath.Join(bundle.directory, name), []byte(sentinel))

	output, err := runReleaseEvidenceVerifier(t, root, bundle.directory)
	assertCommandRejected(t, output, err, "release evidence verifier accepted "+accepted, "unreferenced file", failure)
	assertOutputOmitsSentinel(t, output, sentinel, failure)
}

func TestReleaseEvidenceVerifierRejectsResidualOCILayoutDirectory(t *testing.T) {
	root := repositoryRoot(t)
	bundle := writeReleaseEvidenceBundle(t, false)
	layout := filepath.Join(bundle.directory, "image.oci")
	makeDirectory(t, 0o700, layout, "blobs", "sha256")
	writeReleaseArtifact(t, filepath.Join(layout, "oci-layout"), []byte(`{"imageLayoutVersion":"1.0.0"}`))

	assertReleaseEvidenceVerifierRejects(t, root, bundle.directory, "release evidence verifier accepted a residual OCI layout directory", "unreferenced directory", "residual OCI layout directory")
}

func TestReleaseEvidenceVerifierScansEveryRetainedArtifact(t *testing.T) {
	root := repositoryRoot(t)
	for _, name := range requiredReleaseEvidenceArtifacts {
		t.Run(name, func(t *testing.T) {
			bundle := writeReleaseEvidenceBundle(t, false)
			evidence := readReleaseEvidence(t, bundle.directory)
			sentinel := "api_key=retained-" + strings.ReplaceAll(name, "_", "-") + "-secret-0123456789"
			path := filepath.Join(bundle.directory, releaseEvidenceArtifactPaths[name])
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			replaceReleaseEvidenceArtifact(t, bundle, evidence, name, append(data, []byte("\n"+sentinel)...))
			writeReleaseEvidence(t, bundle.directory, evidence)

			output, err := runReleaseEvidenceVerifier(t, root, bundle.directory)
			assertCommandRejected(t, output, err, fmt.Sprintf("release evidence verifier accepted a secret-like value in retained artifact %q", name), "secret-like", fmt.Sprintf("retained artifact %q secret", name))
			assertOutputOmitsSentinel(t, output, sentinel, fmt.Sprintf("retained artifact %q secret", name))
		})
	}
}

func TestReleaseEvidenceVerifierRejectsNoncanonicalEvidencePath(t *testing.T) {
	root := repositoryRoot(t)
	bundle := writeReleaseEvidenceBundle(t, false)
	directory := resolvedReleaseEvidenceDirectory(t, bundle.directory)
	renamedPath := filepath.Join(directory, "nested", "evidence.json")
	makeDirectory(t, 0o700, filepath.Dir(renamedPath))
	if err := os.Rename(filepath.Join(directory, "evidence.json"), renamedPath); err != nil {
		t.Fatal(err)
	}

	command := exec.Command(
		"bash", filepath.Join(root, "scripts", "release", "verify.sh"),
		"--artifact-dir", directory,
		"--evidence", renamedPath,
	)
	command.Dir = root
	output, err := command.CombinedOutput()
	assertCommandRejected(t, output, err, "release evidence verifier accepted a nested evidence record", "record path must be artifact-dir/evidence.json", "nested evidence record")
}

func TestReleaseEvidenceRecorderRejectsUnreferencedArtifactFile(t *testing.T) {
	root := repositoryRoot(t)
	bundle, directory := writeUnrecordedReleaseEvidenceBundle(t, false)
	writeReleaseArtifact(t, filepath.Join(directory, "orphan.json"), []byte(`{"api_`+`key":"orphaned-release-evidence-secret-0123456789"}`))
	outputPath := filepath.Join(directory, "evidence.json")

	output, err := runReleaseEvidenceRecorder(root, directory, outputPath, bundle)
	assertCommandRejected(t, output, err, "release evidence recorder accepted an unreferenced artifact file", "unreferenced file", "unreferenced recorder")
	assertPathAbsent(t, outputPath, "recorder wrote evidence after unreferenced artifact rejection")
}

func TestReleaseEvidenceRecorderRejectsImageLayoutArtifact(t *testing.T) {
	root := repositoryRoot(t)
	bundle, directory := writeUnrecordedReleaseEvidenceBundle(t, false)
	writeReleaseArtifact(t, filepath.Join(directory, "image.oci.tar"), []byte("not a retained OCI archive"))
	outputPath := filepath.Join(directory, "evidence.json")

	output, err := runReleaseEvidenceRecorderWithAdditionalArtifact(
		root,
		directory,
		outputPath,
		bundle,
		"image_layout",
		"image.oci.tar",
	)
	assertCommandRejected(t, output, err, "release evidence recorder accepted a raw OCI archive artifact", "unknown artifact", "raw OCI archive recorder")
	assertPathAbsent(t, outputPath, "recorder wrote evidence after raw OCI archive rejection")
}

func TestReleaseEvidenceRecorderRejectsResidualOCILayoutDirectory(t *testing.T) {
	root := repositoryRoot(t)
	bundle, directory := writeUnrecordedReleaseEvidenceBundle(t, false)
	layout := filepath.Join(directory, "image.oci")
	makeDirectory(t, 0o700, layout, "blobs", "sha256")
	writeReleaseArtifact(t, filepath.Join(layout, "oci-layout"), []byte(`{"imageLayoutVersion":"1.0.0"}`))
	outputPath := filepath.Join(directory, "evidence.json")

	output, err := runReleaseEvidenceRecorder(root, directory, outputPath, bundle)
	assertCommandRejected(t, output, err, "release evidence recorder accepted a residual OCI layout directory", "unreferenced directory", "residual OCI layout directory recorder")
	assertPathAbsent(t, outputPath, "recorder wrote evidence after residual OCI layout rejection")
}

func TestReleaseEvidenceRecorderRejectsOutputArtifactCollision(t *testing.T) {
	root := repositoryRoot(t)
	bundle, directory := writeUnrecordedReleaseEvidenceBundle(t, false)
	outputPath := filepath.Join(directory, releaseEvidenceArtifactPaths["test_summary"])
	before := readText(t, outputPath)

	output, err := runReleaseEvidenceRecorder(root, directory, outputPath, bundle)
	assertCommandRejected(t, output, err, "release evidence recorder accepted an output artifact collision", "must not overwrite required artifact", "output artifact collision")
	after := readText(t, outputPath)
	if after != before {
		t.Fatal("recorder changed an input artifact after rejecting output collision")
	}
}

func TestReleaseEvidenceRecorderRejectsNoncanonicalArtifactPath(t *testing.T) {
	root := repositoryRoot(t)
	bundle, directory := writeUnrecordedReleaseEvidenceBundle(t, false)
	outputPath := filepath.Join(directory, "evidence.json")

	output, err := runReleaseEvidenceRecorderWithArtifactOverride(
		root,
		directory,
		outputPath,
		bundle,
		"test_summary",
		"nested/test-summary.json",
	)
	assertCommandRejected(t, output, err, "release evidence recorder accepted a noncanonical artifact path", "canonical path", "noncanonical artifact path")
}

func TestReleaseEvidenceRecorderRejectsSymlinkedOutputParent(t *testing.T) {
	root := repositoryRoot(t)
	bundle := writeReleaseEvidenceBundle(t, false)
	directory := resolvedReleaseEvidenceDirectory(t, bundle.directory)
	outside := makeDirectory(t, 0o700, filepath.Dir(directory), "outside-output")
	link := filepath.Join(directory, "escaped")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}

	output, err := runReleaseEvidenceRecorder(root, directory, filepath.Join(link, "evidence.json"), bundle)
	assertCommandRejected(t, output, err, "release evidence recorder accepted a symlinked output parent", "symlink", "symlinked output")
	assertPathAbsent(t, filepath.Join(outside, "evidence.json"), "recorder wrote evidence outside artifact directory")
}

func TestReleaseEvidenceRecorderLeavesNoEvidenceOnValidationFailure(t *testing.T) {
	root := repositoryRoot(t)
	bundle, directory := writeUnrecordedReleaseEvidenceBundle(t, true)
	outputPath := filepath.Join(directory, "evidence.json")

	output, err := runReleaseEvidenceRecorder(root, directory, outputPath, bundle)
	if err == nil {
		t.Fatalf("release evidence recorder accepted a failing image scan:\n%s", output)
	}
	assertPathAbsent(t, outputPath, "recorder left final evidence after validation failure")
	assertNoTemporaryEvidence(t, outputPath)
}

func TestReleaseEvidenceRecorderAtomicallyWritesVerifiedByteBoundRecord(t *testing.T) {
	root := repositoryRoot(t)
	bundle, directory := writeUnrecordedReleaseEvidenceBundle(t, false)
	outputPath := filepath.Join(directory, "evidence.json")

	output, err := runReleaseEvidenceRecorder(root, directory, outputPath, bundle)
	if err != nil {
		t.Fatalf("release evidence recorder rejected complete bundle: %v\n%s", err, output)
	}
	if !strings.Contains(string(output), "release evidence recorded") {
		t.Fatalf("recorder success output = %q", output)
	}
	info, err := os.Lstat(outputPath)
	if err != nil || !info.Mode().IsRegular() {
		t.Fatalf("recorder output = %#v, %v; want regular evidence file", info, err)
	}
	evidence := readJSONObject(t, outputPath)
	for _, name := range requiredReleaseEvidenceArtifacts {
		artifact := evidence["artifacts"].(map[string]any)[name].(map[string]any)
		if _, ok := artifact["bytes"].(float64); !ok || artifact["bytes"].(float64) <= 0 {
			t.Fatalf("recorded artifact %q has no positive byte count: %#v", name, artifact)
		}
		if digest, _ := artifact["sha256"].(string); len(digest) != 64 {
			t.Fatalf("recorded artifact %q sha256 = %q", name, digest)
		}
	}
	assertNoTemporaryEvidence(t, outputPath)
}

func TestReleaseEvidenceEntrypointIsLocalAndNonPublishing(t *testing.T) {
	root := repositoryRoot(t)
	makefile := readRepositoryFile(t, moduleRoot(t), "Makefile")
	assertContains(t, makefile, "release-verify:", "Makefile does not expose release-verify")
	assertOmits(t, makefile, "RELEASE_EVIDENCE_FILE", "Makefile permits a noncanonical release-evidence record path")
	assertOmits(t, makefile, "RELEASE_EVIDENCE_DIR", "Makefile permits a workflow-controlled release-evidence directory")
	assertContains(t, makefile, `--artifact-dir "release-artifacts" --evidence "release-artifacts/evidence.json"`, "Makefile does not seal release verification to the canonical evidence bundle")
	for _, path := range []string{
		filepath.Join("scripts", "release", "collect.sh"),
		filepath.Join("scripts", "release", "collect.py"),
		filepath.Join("scripts", "release", "verify.sh"),
		filepath.Join("scripts", "release", "record.sh"),
		filepath.Join("docs", "release", "evidence.schema.json"),
		filepath.Join("docs", "release", "artifact.schema.json"),
		filepath.Join("docs", "release", "runbook.md"),
		filepath.Join("scripts", "release", "trivy.yaml"),
	} {
		if _, err := os.Stat(filepath.Join(root, path)); err != nil {
			t.Fatalf("release evidence asset %s: %v", path, err)
		}
	}

	gitignore := readRepositoryFile(t, root, ".gitignore")
	assertContains(t, gitignore, "release-artifacts/", ".gitignore does not exclude local release-artifacts/")

	for _, path := range []string{
		filepath.Join("scripts", "release", "collect.sh"),
		filepath.Join("scripts", "release", "collect.py"),
		filepath.Join("scripts", "release", "verify.sh"),
		filepath.Join("scripts", "release", "record.sh"),
	} {
		data := readRepositoryFile(t, root, path)
		lower := strings.ToLower(data)
		assertOmitsEach(t, lower, path+" contains publication or Task 24 control %q",
			"docker push",
			"cosign sign",
			"gh release",
			"workflow_dispatch",
			"id-token: write",
		)
	}
	// Task 24 is separately documented below this heading. Keep the existing
	// Task 23 runbook boundary nonpublishing while allowing that later,
	// fail-closed control to document its manual trigger and OIDC boundary.
	runbook := readRepositoryFile(t, root, "docs", "release", "runbook.md")
	task23Runbook, _, found := strings.Cut(runbook, "\n## Guarded manual publication boundary")
	if !found {
		t.Fatal("release runbook does not separate the Task 23 and Task 24 boundaries")
	}
	assertOmitsEach(t, strings.ToLower(task23Runbook), "Task 23 runbook boundary contains publication or Task 24 control %q",
		"docker push",
		"cosign sign",
		"gh release",
		"workflow_dispatch",
		"id-token: write",
	)
	artifactSchema := readRepositoryFile(t, root, "docs", "release", "artifact.schema.json")
	evidenceSchema := readRepositoryFile(t, root, "docs", "release", "evidence.schema.json")
	canonicalProvenanceReference := `"$ref": "#/$defs/provenance_url"`
	if strings.Count(evidenceSchema, canonicalProvenanceReference) != 1 || strings.Count(artifactSchema, canonicalProvenanceReference) != 2 {
		t.Fatal("release schemas do not apply the same canonical provenance URL policy to repository, fixture, and dependency URLs")
	}
	canonicalDNSLabelPattern := `"pattern": "^https://[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?(?:\\.[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?)*(?:/[A-Za-z0-9._~!$&'()*+,;=:@/-]*)?$"`
	numericDottedHostExclusion := `"pattern": "^https://[0-9]+(?:\\.[0-9]+)+(?:/|$)"`
	dnsHostnameLengthExclusion := `"pattern": "^https://[^/]{254,}(?:/|$)"`
	for _, schema := range []string{evidenceSchema, artifactSchema} {
		if strings.Count(schema, canonicalDNSLabelPattern) != 1 || strings.Count(schema, numericDottedHostExclusion) != 1 || strings.Count(schema, dnsHostnameLengthExclusion) != 1 {
			t.Fatal("release schemas do not enforce the canonical DNS-label provenance URL policy")
		}
	}
	assertContainsEach(t, artifactSchema, "release log schema is missing allowlisted summary field %q", "\"event_counts\"", "\"line_count\"", "\"input_bytes\"", "\"redaction_policy\"")
	assertOmitsEach(t, artifactSchema, "release log schema permits raw log content field %q",
		"\"content\"",
		"\"message\"",
		"\"text\"",
		"\"raw_log\"",
		"\"log_lines\"",
	)
}

func TestReleaseEvidenceCollectorRunsFuzzGateFromModuleRoot(t *testing.T) {
	root := repositoryRoot(t)
	collector := readRepositoryFile(t, root, "scripts", "release", "collect.sh")
	assertContains(t, collector, `run_gate fuzz_summary bash -c "cd \"$module_root\" && bash \"$module_root/scripts/run-fuzz.sh\" smoke"`, "release evidence collector does not run the fuzz gate from the Go module root")
}

func TestReleaseEvidenceCollectorRetainsMemoryBenchmarkMeasurement(t *testing.T) {
	root := repositoryRoot(t)
	collector := readRepositoryFile(t, root, "scripts", "release", "collect.sh")
	// The gate must time the production v1 Activities path, never the
	// legacy in-process engine that no production worker runs (#1497).
	assertContains(t, collector, "go test ./internal/runtime -run '^$' -bench '^BenchmarkV1ActivitiesAdmissionMemory$'", "release evidence collector does not run the v1 Activities memory admission benchmark")
	assertOmits(t, collector, "BenchmarkGenerateMemoryAdmissionAndCompile", "release evidence collector still runs the legacy engine benchmark")
	pythonCollector := readRepositoryFile(t, root, "scripts", "release", "collect.py")
	assertContainsEach(t, pythonCollector, "benchmark collector is missing %q", "benchmark-summary", "p99_ms_per_op", "target_status", "measurement_only")
	assertContainsEach(t, collector, "release evidence collector is missing optional worker SLO input %q", "--worker-error-metrics", "slo-evidence.py", "worker-error-summary.json")
}

func TestReleaseEvidenceCollectorSummarizesMemoryBenchmark(t *testing.T) {
	for _, suffix := range []string{"", "-2", "-14"} {
		t.Run("processors"+suffix, func(t *testing.T) {
			root := repositoryRoot(t)
			inputPath := filepath.Join(t.TempDir(), "benchmark.out")
			outputPath := filepath.Join(t.TempDir(), "benchmark-summary.json")
			input := "goos: linux\n" +
				"BenchmarkV1ActivitiesAdmissionMemory" + suffix + "    4267    255245 ns/op         0.7286 p99_ms/op  350632 B/op    4851 allocs/op\n" +
				"PASS\n"
			writeReleaseArtifact(t, inputPath, []byte(input))
			collect := func() ([]byte, error) {
				return runReleaseCollector(root, "benchmark-summary", "--kind", "benchmark_summary", "--input", inputPath, "--output", outputPath)
			}
			if output, err := collect(); err != nil {
				t.Fatalf("benchmark collector rejected valid output: %v\n%s", err, output)
			}
			document := readReleaseEvidenceJSONArtifact(t, filepath.Dir(outputPath), "benchmark_summary")
			if document["scope"] != "memory" || document["target_status"] != "pass" || document["objective_status"] != "measurement_only" {
				t.Fatalf("benchmark summary scope/status = %#v/%#v", document["scope"], document["objective_status"])
			}
			if document["p99_ms_per_op"] != 0.7286 {
				t.Fatalf("benchmark summary p99 = %#v", document["p99_ms_per_op"])
			}
			assertRejected := func(input, accepted, retained string) {
				t.Helper()
				writeReleaseArtifact(t, inputPath, []byte(input))
				if output, err := collect(); err == nil {
					t.Fatalf("benchmark collector accepted %s:\n%s", accepted, output)
				}
				assertPathAbsent(t, outputPath, "benchmark collector retained output "+retained)
			}

			if err := os.Remove(outputPath); err != nil {
				t.Fatal(err)
			}
			assertRejected(input+"BenchmarkV1ActivitiesAdmissionMemory"+suffix+" 1 1 ns/op 1 p99_ms/op\n",
				"multiple measurements", "after rejecting multiple measurements")
			// Output of the legacy engine benchmark is not the v1
			// Activities measurement and must not be retained (#1497).
			assertRejected("goos: linux\nBenchmarkGenerateMemoryAdmissionAndCompile"+suffix+"    4267    255245 ns/op         0.7286 p99_ms/op\nPASS\n",
				"legacy engine benchmark output", "for the legacy engine benchmark")
			assertRejected("goos: linux\nBenchmarkV1ActivitiesAdmissionMemory"+suffix+" 1 1 ns/op 25 p99_ms/op\nPASS\n",
				"a p99 at the strict target boundary", "after rejecting target miss")
		})
	}
}

func TestReleaseEvidenceMakeTargetIgnoresEvidencePathEnvironment(t *testing.T) {
	root := repositoryRoot(t)
	command := exec.Command("make", "--no-print-directory", "-n", "release-verify")
	command.Dir = root
	command.Env = append(os.Environ(),
		"RELEASE_EVIDENCE_DIR=alternate-artifacts",
		"RELEASE_EVIDENCE_FILE=alternate-evidence.json",
	)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("dry-run release-verify failed: %v\n%s", err, output)
	}
	if !strings.Contains(string(output), "--artifact-dir \"release-artifacts\" --evidence \"release-artifacts/evidence.json\"") {
		t.Fatalf("release-verify did not retain canonical paths under override environment: %q", output)
	}
	if strings.Contains(string(output), "alternate-artifacts") || strings.Contains(string(output), "alternate-evidence.json") {
		t.Fatalf("release-verify was redirected by an evidence-path environment override: %q", output)
	}
}

// releaseEvidenceBundle describes a schema-v2 bundle. imageReference and
// imageDigest name the linux/amd64 manifest that sbom and image_scan bind;
// the evidence subject is the image index that references it and arm64.
type releaseEvidenceBundle struct {
	directory      string
	imageReference string
	imageDigest    string
	arm64Reference string
	arm64Digest    string
	indexReference string
	indexDigest    string
}

func writeReleaseEvidenceBundle(t *testing.T, criticalFinding bool) releaseEvidenceBundle {
	t.Helper()
	directory := t.TempDir()
	imageDigest := writeReleaseEvidenceOCILayout(t, filepath.Join(t.TempDir(), "image.oci"))
	imageReference := "llm-temporal-worker@" + imageDigest
	arm64Digest := "sha256:" + sha256Hex([]byte("release-evidence-test-arm64-manifest"))
	arm64Reference := "llm-temporal-worker@" + arm64Digest
	index := releaseEvidenceImageIndex(imageDigest, arm64Digest)
	indexDigest := "sha256:" + sha256Hex([]byte(index))
	indexReference := "llm-temporal-worker@" + indexDigest
	contents := map[string]string{
		"test_summary":          releaseEvidenceGateSummary("test_summary"),
		"race_summary":          releaseEvidenceGateSummary("race_summary"),
		"fuzz_summary":          releaseEvidenceGateSummary("fuzz_summary"),
		"benchmark_summary":     `{"schema_version":1,"kind":"benchmark_summary","status":"pass","benchmark":"BenchmarkV1ActivitiesAdmissionMemory","scope":"memory","samples":4267,"ns_per_op":255245,"p99_ms_per_op":0.7286,"target_ms":25,"target_status":"pass","objective_status":"measurement_only","output_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","output_bytes":256,"redacted":true}`,
		"fixture_manifest":      `{"schema_version":1,"kind":"fixture_manifest","status":"pass","version":1,"fixtures":[{"profile":"openai-responses","upstream_url":"https://platform.openai.com/docs/api-reference/responses","upstream_date":"2026-07-14","manifest_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}],"redacted":true}`,
		"redis_summary":         `{"schema_version":1,"kind":"redis_summary","status":"pass","service":"redis","state":"running","health":"healthy","redacted":true}`,
		"temporal_summary":      `{"schema_version":1,"kind":"temporal_summary","status":"pass","service":"temporal","state":"running","health":"healthy","redacted":true}`,
		"compose_summary":       `{"schema_version":1,"kind":"compose_summary","status":"pass","services":["redis","temporal"],"redacted":true}`,
		"redis_log":             `{"schema_version":1,"kind":"redis_log","status":"pass","service":"redis","source":"docker_compose_logs","redaction_policy":"allowlist-v1","line_count":2,"input_bytes":42,"event_counts":{"redis_ready":1,"redacted_line":1},"redacted":true}`,
		"temporal_log":          `{"schema_version":1,"kind":"temporal_log","status":"pass","service":"temporal","source":"docker_compose_logs","redaction_policy":"allowlist-v1","line_count":2,"input_bytes":42,"event_counts":{"temporal_started":1,"redacted_line":1},"redacted":true}`,
		"compose_log":           `{"schema_version":1,"kind":"compose_log","status":"pass","service":"compose","source":"docker_compose_logs","redaction_policy":"allowlist-v1","line_count":4,"input_bytes":84,"event_counts":{"redis_ready":1,"temporal_started":1,"redacted_line":2},"redacted":true}`,
		"rendered_manifests":    `{"schema_version":1,"kind":"rendered_manifests","status":"pass","manifests":[{"source":"deploy/kubernetes/base","sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","bytes":1,"objects":1}],"redacted":true}`,
		"dependency_license":    `{"schema_version":1,"kind":"dependency_license","status":"pass","baseline_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","direct_modules":[{"path":"github.com/openai/openai-go/v3","version":"v3.0.0","license":"Apache-2.0","source":"https://github.com/openai/openai-go"}],"redacted":true}`,
		"vulnerability_results": `{"schema_version":1,"kind":"vulnerability_results","status":"pass","components":{"test":"pass","source":"pass","go_mod":"pass","vulnerability":"pass"},"direct_module_count":1,"findings":[],"approved_findings":[],"redacted":true}`,
		"sbom":                  releaseEvidenceSBOM(imageReference, imageDigest),
		"image_scan":            releaseEvidenceScan(imageReference, imageDigest, criticalFinding),
		"image_index":           index,
		"sbom_arm64":            releaseEvidenceSBOM(arm64Reference, arm64Digest),
		"image_scan_arm64":      releaseEvidenceScan(arm64Reference, arm64Digest, false),
	}

	evidenceArtifacts := make(map[string]map[string]any, len(requiredReleaseEvidenceArtifacts))
	for _, name := range requiredReleaseEvidenceArtifacts {
		path := releaseEvidenceArtifactPaths[name]
		writeReleaseArtifact(t, filepath.Join(directory, path), []byte(contents[name]))
		data, err := os.ReadFile(filepath.Join(directory, path))
		if err != nil {
			t.Fatal(err)
		}
		evidenceArtifacts[name] = map[string]any{
			"path":     path,
			"bytes":    len(data),
			"sha256":   sha256Hex(data),
			"redacted": true,
		}
	}
	evidence := map[string]any{
		"schema_version": 2,
		"generated_at":   "2026-07-15T00:00:00Z",
		"source": map[string]any{
			"repository": "https://github.com/mfow/llm-temporal-worker",
			"revision":   strings.Repeat("b", 40),
		},
		"image": map[string]any{
			"reference":  indexReference,
			"digest":     indexDigest,
			"media_type": "application/vnd.oci.image.index.v1+json",
			"platforms": map[string]any{
				"linux/amd64": map[string]any{"reference": imageReference, "digest": imageDigest},
				"linux/arm64": map[string]any{"reference": arm64Reference, "digest": arm64Digest},
			},
		},
		"artifacts": evidenceArtifacts,
	}
	writeReleaseEvidence(t, directory, evidence)
	return releaseEvidenceBundle{
		directory:      directory,
		imageReference: imageReference,
		imageDigest:    imageDigest,
		arm64Reference: arm64Reference,
		arm64Digest:    arm64Digest,
		indexReference: indexReference,
		indexDigest:    indexDigest,
	}
}

// releaseEvidenceImageIndex is a Buildx-shaped OCI image index: one manifest
// per release platform plus a provenance attestation for each of them.
func releaseEvidenceImageIndex(amd64Digest, arm64Digest string) string {
	return releaseEvidenceImageIndexWithManifests(
		releaseEvidenceIndexDescriptor(amd64Digest, "linux", "amd64", ""),
		releaseEvidenceIndexDescriptor(arm64Digest, "linux", "arm64", ""),
		releaseEvidenceIndexDescriptor("sha256:"+sha256Hex([]byte("amd64-attestation")), "unknown", "unknown", amd64Digest),
		releaseEvidenceIndexDescriptor("sha256:"+sha256Hex([]byte("arm64-attestation")), "unknown", "unknown", arm64Digest),
	)
}

func releaseEvidenceImageIndexWithManifests(manifests ...string) string {
	return `{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[` + strings.Join(manifests, ",") + `]}`
}

func releaseEvidenceIndexDescriptor(digest, operatingSystem, architecture, attests string) string {
	annotations := ""
	if attests != "" {
		annotations = `,"annotations":{"vnd.docker.reference.digest":"` + attests + `","vnd.docker.reference.type":"attestation-manifest"}`
	}
	return `{"mediaType":"application/vnd.oci.image.manifest.v1+json","digest":"` + digest + `","size":1024` + annotations + `,"platform":{"architecture":"` + architecture + `","os":"` + operatingSystem + `"}}`
}

func releaseEvidenceSBOM(reference, digest string) string {
	return `{"bomFormat":"CycloneDX","specVersion":"1.5","serialNumber":"urn:uuid:11111111-1111-1111-1111-111111111111","version":1,"metadata":{"component":{"type":"container","name":"llm-temporal-worker","properties":[{"name":"org.opencontainers.image.ref.name","value":"` + reference + `"},{"name":"org.opencontainers.image.manifest.digest","value":"` + digest + `"}]}},"components":[{"type":"library","name":"llm-temporal-worker","version":"test"}]}`
}

func releaseEvidenceGateSummary(kind string) string {
	return `{"schema_version":1,"kind":"` + kind + `","status":"pass","output_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","output_bytes":1,"redacted":true}`
}

func releaseEvidenceScan(reference, digest string, criticalFinding bool) string {
	vulnerabilities := "null"
	if criticalFinding {
		vulnerabilities = `[{"VulnerabilityID":"CVE-2026-0001","Severity":"CRITICAL"}]`
	}
	return `{"SchemaVersion":2,"ArtifactName":"image.oci","ArtifactType":"container_image","release_subject":{"reference":"` + reference + `","digest":"` + digest + `"},"Results":[{"Target":"image","Vulnerabilities":` + vulnerabilities + `}]}`
}

func writeReleaseEvidenceOCILayout(t *testing.T, path string) string {
	t.Helper()
	return writeReleaseEvidenceOCILayoutWithCompatibility(t, path, false)
}

func writeReleaseEvidenceOCILayoutWithCompatibilityMetadata(t *testing.T, path string) string {
	t.Helper()
	return writeReleaseEvidenceOCILayoutWithCompatibility(t, path, true)
}

func writeReleaseEvidenceOCILayoutWithCompatibility(t *testing.T, path string, includeCompatibilityMetadata bool) string {
	t.Helper()
	entries, manifestDigest, manifestSize := releaseEvidenceImageLayoutEntries()
	index := []byte(fmt.Sprintf(`{"schemaVersion":2,"manifests":[{"mediaType":"application/vnd.oci.image.manifest.v1+json","digest":"sha256:%s","size":%d}]}`,
		manifestDigest, manifestSize))
	entries = append(entries, releaseEvidenceLayoutEntry{name: "index.json", data: index})
	if includeCompatibilityMetadata {
		entries = append(entries, releaseEvidenceLayoutEntry{name: "manifest.json", data: []byte(`[]`)})
	}
	writeReleaseEvidenceLayoutEntries(t, path, entries)
	return "sha256:" + manifestDigest
}

func writeReleaseEvidenceNestedOCIIndexLayout(t *testing.T, path string, nestedIndex func(manifestDigest string, manifestSize int) []byte) string {
	t.Helper()
	entries, manifestDigest, manifestSize := releaseEvidenceImageLayoutEntries()
	innerIndex := nestedIndex(manifestDigest, manifestSize)
	innerIndexDigest := sha256Hex(innerIndex)
	outerIndex := []byte(fmt.Sprintf(`{"schemaVersion":2,"manifests":[{"mediaType":"application/vnd.oci.image.index.v1+json","digest":"sha256:%s","size":%d}]}`,
		innerIndexDigest, len(innerIndex)))
	entries = append(entries,
		releaseEvidenceLayoutEntry{name: "index.json", data: outerIndex},
		releaseEvidenceLayoutEntry{name: "blobs/sha256/" + innerIndexDigest, data: innerIndex},
	)
	writeReleaseEvidenceLayoutEntries(t, path, entries)
	return "sha256:" + manifestDigest
}

func writeReleaseEvidenceOCIIndexChainLayout(t *testing.T, path string, nestedIndexCount, terminalDescriptorCount int, terminalDescriptorType string) string {
	t.Helper()
	if nestedIndexCount < 1 || terminalDescriptorCount < 1 {
		t.Fatal("test OCI index chain requires positive descriptor counts")
	}
	entries, manifestDigest, manifestSize := releaseEvidenceImageLayoutEntries()
	terminalDescriptor := fmt.Sprintf(`{"mediaType":"%s","digest":"sha256:%s","size":%d}`,
		terminalDescriptorType, manifestDigest, manifestSize)
	children := make([]string, terminalDescriptorCount)
	for index := range children {
		children[index] = terminalDescriptor
	}
	for index := 0; index < nestedIndexCount; index++ {
		nestedIndex := []byte(`{"schemaVersion":2,"manifests":[` + strings.Join(children, ",") + `]}`)
		nestedIndexDigest := sha256Hex(nestedIndex)
		entries = append(entries, releaseEvidenceLayoutEntry{name: "blobs/sha256/" + nestedIndexDigest, data: nestedIndex})
		children = []string{fmt.Sprintf(`{"mediaType":"application/vnd.oci.image.index.v1+json","digest":"sha256:%s","size":%d}`,
			nestedIndexDigest, len(nestedIndex))}
	}
	entries = append(entries, releaseEvidenceLayoutEntry{name: "index.json", data: []byte(`{"schemaVersion":2,"manifests":[` + strings.Join(children, ",") + `]}`)})
	writeReleaseEvidenceLayoutEntries(t, path, entries)
	return "sha256:" + manifestDigest
}

// releaseEvidenceLayoutEntry is one file of a test OCI layout, named relative
// to the layout root with forward slashes.
type releaseEvidenceLayoutEntry struct {
	name string
	data []byte
}

// releaseEvidenceImageLayoutEntries returns the oci-layout marker and the
// config, layer and manifest blobs of the single-platform test image, with
// the manifest's hex digest and size for the caller's index.
func releaseEvidenceImageLayoutEntries() ([]releaseEvidenceLayoutEntry, string, int) {
	config := []byte(`{"architecture":"amd64","os":"linux","config":{"Env":[]}}`)
	layer := []byte("release-evidence-test-layer")
	configDigest := sha256Hex(config)
	layerDigest := sha256Hex(layer)
	manifest := []byte(fmt.Sprintf(`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{"mediaType":"application/vnd.oci.image.config.v1+json","digest":"sha256:%s","size":%d},"layers":[{"mediaType":"application/vnd.oci.image.layer.v1.tar","digest":"sha256:%s","size":%d}]}`,
		configDigest, len(config), layerDigest, len(layer)))
	manifestDigest := sha256Hex(manifest)
	return []releaseEvidenceLayoutEntry{
		{name: "oci-layout", data: []byte(`{"imageLayoutVersion":"1.0.0"}`)},
		{name: "blobs/sha256/" + configDigest, data: config},
		{name: "blobs/sha256/" + layerDigest, data: layer},
		{name: "blobs/sha256/" + manifestDigest, data: manifest},
	}, manifestDigest, len(manifest)
}

// writeReleaseEvidenceLayoutEntries writes entries under the layout at path.
func writeReleaseEvidenceLayoutEntries(t *testing.T, path string, entries []releaseEvidenceLayoutEntry) {
	t.Helper()
	makeDirectory(t, 0o700, path, "blobs", "sha256")
	for _, entry := range entries {
		writeReleaseArtifact(t, filepath.Join(path, filepath.FromSlash(entry.name)), entry.data)
	}
}

// runReleaseCollector runs one scripts/release/collect.py subcommand from the
// repository root and returns its combined output.
func runReleaseCollector(root, subcommand string, arguments ...string) ([]byte, error) {
	return runReleaseScript(root, "collect.py", append([]string{subcommand}, arguments...)...)
}

// runReleaseScript runs the named Python script under scripts/release from
// the repository root and returns its combined output.
func runReleaseScript(root, script string, arguments ...string) ([]byte, error) {
	return combinedOutputIn(root, "python3", append([]string{filepath.Join(root, "scripts", "release", script)}, arguments...)...)
}

// writeReleaseEvidenceFixtureMetadata writes metadata as the metadata.yaml of
// an example contract fixture profile under fixtureRoot and returns the
// profile directory.
func writeReleaseEvidenceFixtureMetadata(t *testing.T, fixtureRoot, metadata string) string {
	t.Helper()
	fixtureDirectory := makeDirectory(t, 0o700, fixtureRoot, "llm", "provider", "example", "testdata", "contracts", "example-profile")
	writeReleaseArtifact(t, filepath.Join(fixtureDirectory, "metadata.yaml"), []byte(metadata))
	return fixtureDirectory
}

// assertNoTemporaryEvidence fails if the recorder left temporary files beside
// outputPath.
func assertNoTemporaryEvidence(t *testing.T, outputPath string) {
	t.Helper()
	temporary, err := filepath.Glob(outputPath + ".tmp-*")
	if err != nil {
		t.Fatal(err)
	}
	if len(temporary) != 0 {
		t.Fatalf("recorder left temporary evidence files: %v", temporary)
	}
}

// assertReleaseEvidenceVerifierRejects runs the release evidence verifier on
// directory and requires the rejection described by assertCommandRejected.
func assertReleaseEvidenceVerifierRejects(t *testing.T, root, directory, accepted, want, failure string) {
	t.Helper()
	output, err := runReleaseEvidenceVerifier(t, root, directory)
	assertCommandRejected(t, output, err, accepted, want, failure)
}

func runReleaseEvidenceVerifier(t *testing.T, root, directory string) ([]byte, error) {
	t.Helper()
	directory = resolvedReleaseEvidenceDirectory(t, directory)
	return combinedOutputIn(root, "bash", filepath.Join(root, "scripts", "release", "verify.sh"), "--artifact-dir", directory, "--evidence", filepath.Join(directory, "evidence.json"))
}

// assertReleaseEvidenceLayoutDigest requires the layout digest of the OCI
// layout at path to be want; layout and digest describe them in failures.
func assertReleaseEvidenceLayoutDigest(t *testing.T, root, path, want, layout, digest string) {
	t.Helper()
	output, err := runReleaseEvidenceLayoutDigest(root, path)
	if err != nil {
		t.Fatalf("layout digest rejected %s: %v\n%s", layout, err, output)
	}
	if got := strings.TrimSpace(string(output)); got != want {
		t.Fatalf("layout digest = %q, want %s %q", got, digest, want)
	}
}

func runReleaseEvidenceLayoutDigest(root, path string) ([]byte, error) {
	return combinedOutputIn(filepath.Join(root, "golang"), "go", "run", "./tools/releaseverify", "layout-digest", "-layout", path)
}

func runReleaseEvidenceRecorder(root, directory, outputPath string, bundle releaseEvidenceBundle) ([]byte, error) {
	return runReleaseEvidenceRecorderWithArtifactOverrides(root, directory, outputPath, bundle, nil)
}

func runReleaseEvidenceRecorderWithArtifactOverride(root, directory, outputPath string, bundle releaseEvidenceBundle, name, path string) ([]byte, error) {
	return runReleaseEvidenceRecorderWithArtifactOverrides(root, directory, outputPath, bundle, map[string]string{name: path})
}

func runReleaseEvidenceRecorderWithAdditionalArtifact(root, directory, outputPath string, bundle releaseEvidenceBundle, name, path string) ([]byte, error) {
	arguments := releaseEvidenceRecorderArguments(root, directory, outputPath, bundle, nil)
	arguments = append(arguments, "-artifact", name+"="+path)
	return combinedOutputIn(root, "bash", arguments...)
}

func runReleaseEvidenceRecorderWithArtifactOverrides(root, directory, outputPath string, bundle releaseEvidenceBundle, overrides map[string]string) ([]byte, error) {
	arguments := releaseEvidenceRecorderArguments(root, directory, outputPath, bundle, overrides)
	return combinedOutputIn(root, "bash", arguments...)
}

func releaseEvidenceRecorderArguments(root, directory, outputPath string, bundle releaseEvidenceBundle, overrides map[string]string) []string {
	arguments := []string{
		filepath.Join(root, "scripts", "release", "record.sh"),
		"-artifact-dir", directory,
		"-output", outputPath,
		"-repository", "https://github.com/mfow/llm-temporal-worker",
		"-revision", strings.Repeat("b", 40),
		"-image-reference", bundle.indexReference,
		"-image-digest", bundle.indexDigest,
		"-amd64-digest", bundle.imageDigest,
		"-arm64-digest", bundle.arm64Digest,
	}
	for _, name := range requiredReleaseEvidenceArtifacts {
		path := releaseEvidenceArtifactPaths[name]
		if override, ok := overrides[name]; ok {
			path = override
		}
		arguments = append(arguments, "-artifact", name+"="+path)
	}
	return arguments
}

// rewriteReleaseEvidenceOCIIndex lets mutate edit the single-descriptor
// index.json of the OCI layout at path and writes the result back.
func rewriteReleaseEvidenceOCIIndex(t *testing.T, path string, mutate func(index map[string]any, manifests []any)) {
	t.Helper()
	indexPath := filepath.Join(path, "index.json")
	index := readJSONObject(t, indexPath)
	manifests, ok := index["manifests"].([]any)
	if !ok || len(manifests) != 1 {
		t.Fatalf("test OCI index manifest list = %#v", index["manifests"])
	}
	mutate(index, manifests)
	writeJSONArtifact(t, indexPath, index)
}

func releaseEvidenceOCIIndexDescriptor(t *testing.T, manifests []any) map[string]any {
	t.Helper()
	descriptor, ok := manifests[0].(map[string]any)
	if !ok {
		t.Fatalf("test OCI index descriptor = %#v", manifests[0])
	}
	return descriptor
}

func writeReleaseArtifact(t *testing.T, path string, data []byte) {
	t.Helper()
	writeFile(t, path, data, 0o600)
}

// marshalJSON encodes value as JSON, failing the test on error.
func marshalJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// writeJSONArtifact writes value as JSON to path with writeReleaseArtifact.
func writeJSONArtifact(t *testing.T, path string, value any) {
	t.Helper()
	writeReleaseArtifact(t, path, marshalJSON(t, value))
}

func readReleaseEvidence(t *testing.T, directory string) map[string]any {
	t.Helper()
	return readJSONObject(t, filepath.Join(directory, "evidence.json"))
}

func readReleaseEvidenceJSONArtifact(t *testing.T, directory, name string) map[string]any {
	t.Helper()
	return readJSONObject(t, filepath.Join(directory, releaseEvidenceArtifactPaths[name]))
}

func cloneReleaseEvidenceJSON(t *testing.T, document map[string]any) map[string]any {
	t.Helper()
	var clone map[string]any
	decodeJSON(t, marshalJSON(t, document), "release evidence clone", &clone)
	return clone
}

// compileRepositoryJSONSchema compiles the repository JSON schema at
// relativePath, registered as resourceURL.
func compileRepositoryJSONSchema(t *testing.T, root, relativePath, resourceURL string) *jsonschema.Schema {
	t.Helper()
	document, err := jsonschema.UnmarshalJSON(bytes.NewReader([]byte(readRepositoryFile(t, root, relativePath))))
	if err != nil {
		t.Fatalf("JSON schema %s is not valid JSON: %v", relativePath, err)
	}
	compiler := jsonschema.NewCompiler()
	if err := compiler.AddResource(resourceURL, document); err != nil {
		t.Fatalf("cannot add JSON schema %s: %v", relativePath, err)
	}
	compiled, err := compiler.Compile(resourceURL)
	if err != nil {
		t.Fatalf("cannot compile JSON schema %s: %v", relativePath, err)
	}
	return compiled
}

func validateJSONSchema(schema *jsonschema.Schema, document map[string]any) error {
	data, err := json.Marshal(document)
	if err != nil {
		return err
	}
	instance, err := jsonschema.UnmarshalJSON(bytes.NewReader(data))
	if err != nil {
		return err
	}
	return schema.Validate(instance)
}

func writeReleaseEvidence(t *testing.T, directory string, evidence map[string]any) {
	t.Helper()
	writeJSONArtifact(t, filepath.Join(directory, "evidence.json"), evidence)
}

func replaceReleaseEvidenceArtifact(t *testing.T, bundle releaseEvidenceBundle, evidence map[string]any, name string, data []byte) {
	t.Helper()
	writeReleaseArtifact(t, filepath.Join(bundle.directory, releaseEvidenceArtifactPaths[name]), data)
	artifact := evidence["artifacts"].(map[string]any)[name].(map[string]any)
	artifact["bytes"] = len(data)
	artifact["sha256"] = sha256Hex(data)
}

func sha256Hex(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

// writeUnrecordedReleaseEvidenceBundle writes a bundle without its evidence
// record and returns it with its resolved directory, ready for the recorder.
func writeUnrecordedReleaseEvidenceBundle(t *testing.T, criticalFinding bool) (releaseEvidenceBundle, string) {
	t.Helper()
	bundle := writeReleaseEvidenceBundle(t, criticalFinding)
	directory := resolvedReleaseEvidenceDirectory(t, bundle.directory)
	if err := os.Remove(filepath.Join(directory, "evidence.json")); err != nil {
		t.Fatal(err)
	}
	return bundle, directory
}

func resolvedReleaseEvidenceDirectory(t *testing.T, directory string) string {
	t.Helper()
	resolved, err := filepath.EvalSymlinks(directory)
	if err != nil {
		t.Fatal(err)
	}
	return resolved
}
