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
	"vulnerability_results",
	"sbom",
	"image_scan",
}

var releaseEvidenceArtifactPaths = map[string]string{
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
	if _, err := os.Stat(filepath.Join(bundle.directory, "image.oci.tar")); !os.IsNotExist(err) {
		t.Fatalf("retained evidence bundle contains an OCI archive: %v", err)
	}
}

func TestReleaseEvidenceLayoutDigestUsesTheOCIManifestDescriptor(t *testing.T) {
	root := repositoryRoot(t)
	path := filepath.Join(t.TempDir(), "image.oci")
	want := writeReleaseEvidenceOCILayout(t, path)

	output, err := runReleaseEvidenceLayoutDigest(root, path)
	if err != nil {
		t.Fatalf("layout digest rejected a complete OCI layout: %v\n%s", err, output)
	}
	if got := strings.TrimSpace(string(output)); got != want {
		t.Fatalf("layout digest = %q, want OCI manifest digest %q", got, want)
	}
}

func TestReleaseEvidenceLayoutDigestAcceptsDockerOCICompatibilityMetadata(t *testing.T) {
	root := repositoryRoot(t)
	path := filepath.Join(t.TempDir(), "image.oci")
	want := writeReleaseEvidenceOCILayoutWithCompatibilityMetadata(t, path)

	output, err := runReleaseEvidenceLayoutDigest(root, path)
	if err != nil {
		t.Fatalf("layout digest rejected Docker OCI compatibility metadata: %v\n%s", err, output)
	}
	if got := strings.TrimSpace(string(output)); got != want {
		t.Fatalf("layout digest = %q, want OCI manifest digest %q", got, want)
	}
}

func TestReleaseEvidenceLayoutDigestRejectsDockerTopLevelDescriptorWithCompatibilityMetadata(t *testing.T) {
	root := repositoryRoot(t)
	path := filepath.Join(t.TempDir(), "image.oci")
	writeReleaseEvidenceOCILayoutWithCompatibilityMetadata(t, path)

	indexPath := filepath.Join(path, "index.json")
	data, err := os.ReadFile(indexPath)
	if err != nil {
		t.Fatal(err)
	}
	var index map[string]any
	if err := json.Unmarshal(data, &index); err != nil {
		t.Fatal(err)
	}
	manifests, ok := index["manifests"].([]any)
	if !ok || len(manifests) != 1 {
		t.Fatalf("test OCI index manifest list = %#v", index["manifests"])
	}
	descriptor, ok := manifests[0].(map[string]any)
	if !ok {
		t.Fatalf("test OCI index descriptor = %#v", manifests[0])
	}
	descriptor["mediaType"] = "application/vnd.docker.distribution.manifest.v2+json"
	updated, err := json.Marshal(index)
	if err != nil {
		t.Fatal(err)
	}
	writeReleaseArtifact(t, indexPath, updated)

	output, err := runReleaseEvidenceLayoutDigest(root, path)
	if err == nil {
		t.Fatalf("layout digest accepted a Docker top-level descriptor with compatibility metadata:\n%s", output)
	}
	if !strings.Contains(string(output), "OCI layout index does not reference an OCI image manifest") {
		t.Fatalf("Docker top-level descriptor failure = %q", output)
	}
}

func TestReleaseEvidenceLayoutDigestUsesSingleChildOCIIndexManifestDescriptor(t *testing.T) {
	root := repositoryRoot(t)
	path := filepath.Join(t.TempDir(), "image.oci")
	want := writeReleaseEvidenceNestedOCIIndexLayout(t, path, func(manifestDigest string, manifestSize int) []byte {
		return []byte(fmt.Sprintf(`{"schemaVersion":2,"manifests":[{"mediaType":"application/vnd.oci.image.manifest.v1+json","digest":"sha256:%s","size":%d}]}`,
			manifestDigest, manifestSize))
	})

	output, err := runReleaseEvidenceLayoutDigest(root, path)
	if err != nil {
		t.Fatalf("layout digest rejected a single-child OCI image index: %v\n%s", err, output)
	}
	if got := strings.TrimSpace(string(output)); got != want {
		t.Fatalf("layout digest = %q, want nested OCI image manifest digest %q", got, want)
	}
}

func TestReleaseEvidenceLayoutDigestAcceptsBuildxDoubleNestedOCIIndexManifestDescriptor(t *testing.T) {
	root := repositoryRoot(t)
	path := filepath.Join(t.TempDir(), "image.oci")
	want := writeReleaseEvidenceOCIIndexChainLayout(t, path, 2, 1, "application/vnd.oci.image.manifest.v1+json")

	output, err := runReleaseEvidenceLayoutDigest(root, path)
	if err != nil {
		t.Fatalf("layout digest rejected the bounded Buildx OCI index chain: %v\n%s", err, output)
	}
	if got := strings.TrimSpace(string(output)); got != want {
		t.Fatalf("layout digest = %q, want Buildx OCI image manifest digest %q", got, want)
	}
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
				indexPath := filepath.Join(path, "index.json")
				data, err := os.ReadFile(indexPath)
				if err != nil {
					t.Fatal(err)
				}
				var index map[string]any
				if err := json.Unmarshal(data, &index); err != nil {
					t.Fatal(err)
				}
				manifests, ok := index["manifests"].([]any)
				if !ok || len(manifests) != 1 {
					t.Fatalf("test OCI index manifest list = %#v", index["manifests"])
				}
				descriptor, ok := manifests[0].(map[string]any)
				if !ok {
					t.Fatalf("test OCI index descriptor = %#v", manifests[0])
				}
				size, ok := descriptor["size"].(float64)
				if !ok {
					t.Fatalf("test OCI index descriptor size = %#v", descriptor["size"])
				}
				descriptor["size"] = size + 1
				updated, err := json.Marshal(index)
				if err != nil {
					t.Fatal(err)
				}
				writeReleaseArtifact(t, indexPath, updated)
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
			if err == nil {
				t.Fatalf("layout digest accepted Buildx OCI index chain with %s:\n%s", test.name, output)
			}
			if !strings.Contains(string(output), test.failure) {
				t.Fatalf("Buildx OCI index chain %s failure = %q, want %q", test.name, output, test.failure)
			}
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
			if err == nil {
				t.Fatalf("layout digest accepted nested OCI index with %s:\n%s", test.name, output)
			}
			if !strings.Contains(string(output), test.failure) {
				t.Fatalf("nested OCI index %s failure = %q, want %q", test.name, output, test.failure)
			}
		})
	}
}

func TestReleaseEvidenceLayoutDigestRejectsFilesAndUnsafeDirectoryPaths(t *testing.T) {
	root := repositoryRoot(t)
	t.Run("archive file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "image.oci.tar")
		writeReleaseArtifact(t, path, []byte("not an OCI directory"))

		output, err := runReleaseEvidenceLayoutDigest(root, path)
		if err == nil {
			t.Fatalf("layout digest accepted an OCI archive file:\n%s", output)
		}
		if !strings.Contains(string(output), "real directory") {
			t.Fatalf("OCI archive input failure = %q", output)
		}
	})

	t.Run("symlinked layout", func(t *testing.T) {
		target := filepath.Join(t.TempDir(), "image.oci")
		writeReleaseEvidenceOCILayout(t, target)
		path := filepath.Join(t.TempDir(), "image.oci")
		if err := os.Symlink(target, path); err != nil {
			t.Fatal(err)
		}

		output, err := runReleaseEvidenceLayoutDigest(root, path)
		if err == nil {
			t.Fatalf("layout digest accepted a symlinked OCI directory:\n%s", output)
		}
		if !strings.Contains(string(output), "real directory") {
			t.Fatalf("symlinked OCI directory failure = %q", output)
		}
	})

	t.Run("noncanonical path", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "image.oci")
		writeReleaseEvidenceOCILayout(t, path)
		noncanonical := filepath.Dir(path) + string(filepath.Separator) + "." + string(filepath.Separator) + filepath.Base(path)

		output, err := runReleaseEvidenceLayoutDigest(root, noncanonical)
		if err == nil {
			t.Fatalf("layout digest accepted a noncanonical OCI directory path:\n%s", output)
		}
		if !strings.Contains(string(output), "canonical path") {
			t.Fatalf("noncanonical OCI directory failure = %q", output)
		}
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
				indexData, err := os.ReadFile(filepath.Join(path, "index.json"))
				if err != nil {
					t.Fatal(err)
				}
				var index struct {
					Manifests []struct {
						Digest string `json:"digest"`
					} `json:"manifests"`
				}
				if err := json.Unmarshal(indexData, &index); err != nil || len(index.Manifests) != 1 {
					t.Fatalf("cannot read test OCI index: %v", err)
				}
				manifestData, err := os.ReadFile(filepath.Join(path, "blobs", "sha256", strings.TrimPrefix(index.Manifests[0].Digest, "sha256:")))
				if err != nil {
					t.Fatal(err)
				}
				var manifest struct {
					Config struct {
						Digest string `json:"digest"`
					} `json:"config"`
				}
				if err := json.Unmarshal(manifestData, &manifest); err != nil {
					t.Fatal(err)
				}
				writeReleaseArtifact(t, filepath.Join(path, "blobs", "sha256", strings.TrimPrefix(manifest.Config.Digest, "sha256:")), []byte("mismatched OCI config"))
			},
			failure: "does not match a retained payload",
		},
		{
			name: "multiple manifest descriptors",
			mutate: func(t *testing.T, path string) {
				t.Helper()
				indexPath := filepath.Join(path, "index.json")
				data, err := os.ReadFile(indexPath)
				if err != nil {
					t.Fatal(err)
				}
				var index map[string]any
				if err := json.Unmarshal(data, &index); err != nil {
					t.Fatal(err)
				}
				manifests, ok := index["manifests"].([]any)
				if !ok || len(manifests) != 1 {
					t.Fatalf("test OCI index manifest list = %#v", index["manifests"])
				}
				index["manifests"] = append(manifests, manifests[0])
				updated, err := json.Marshal(index)
				if err != nil {
					t.Fatal(err)
				}
				writeReleaseArtifact(t, indexPath, updated)
			},
			failure: "exactly one manifest descriptor",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "image.oci")
			writeReleaseEvidenceOCILayout(t, path)
			test.mutate(t, path)

			output, err := runReleaseEvidenceLayoutDigest(root, path)
			if err == nil {
				t.Fatalf("layout digest accepted %s:\n%s", test.name, output)
			}
			if !strings.Contains(string(output), test.failure) {
				t.Fatalf("%s failure = %q, want %q", test.name, output, test.failure)
			}
		})
	}
}

func TestReleaseEvidenceCollectorBuildsFixtureManifestFromWhitespaceMetadata(t *testing.T) {
	root := repositoryRoot(t)
	fixtureRoot := t.TempDir()
	fixtureDirectory := filepath.Join(fixtureRoot, "llm", "provider", "example", "testdata", "contracts", "example-profile")
	if err := os.MkdirAll(fixtureDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	metadata := "profile:   example-profile  \nupstream_url:  https://example.invalid/contracts  \nupstream_date:  '2026-07-15'  \n"
	writeReleaseArtifact(t, filepath.Join(fixtureDirectory, "metadata.yaml"), []byte(metadata))
	writeReleaseArtifact(t, filepath.Join(fixtureDirectory, "fixture.json"), []byte(`{"example":true}`))
	outputPath := filepath.Join(t.TempDir(), "fixture-manifest.json")

	command := exec.Command(
		"python3", filepath.Join(root, "scripts", "release", "collect.py"), "fixture-manifest",
		"--root", fixtureRoot,
		"--output", outputPath,
	)
	command.Dir = root
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("fixture collector rejected ordinary whitespace-delimited metadata: %v\n%s", err, output)
	}
	data, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatal(err)
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
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatal(err)
	}
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
		fixtureDirectory := filepath.Join(fixtureRoot, "llm", "provider", "example", "testdata", "contracts", "example-profile")
		if err := os.MkdirAll(fixtureDirectory, 0o700); err != nil {
			t.Fatal(err)
		}
		writeReleaseArtifact(t, filepath.Join(fixtureDirectory, "metadata.yaml"), []byte("profile: example-profile\nupstream_url: https://operator:"+sentinel+"@example.invalid/contracts\nupstream_date: 2026-07-15\n"))
		outputPath := filepath.Join(t.TempDir(), "fixture-manifest.json")
		command := exec.Command(
			"python3", filepath.Join(root, "scripts", "release", "collect.py"), "fixture-manifest",
			"--root", fixtureRoot,
			"--output", outputPath,
		)
		command.Dir = root
		output, err := command.CombinedOutput()
		if err == nil {
			t.Fatalf("fixture collector accepted basic-auth upstream URL:\n%s", output)
		}
		if !strings.Contains(string(output), "userinfo") {
			t.Fatalf("fixture basic-auth failure = %q", output)
		}
		if strings.Contains(string(output), sentinel) {
			t.Fatalf("fixture basic-auth failure disclosed sentinel: %q", output)
		}
		if _, err := os.Stat(outputPath); !os.IsNotExist(err) {
			t.Fatalf("fixture collector wrote evidence after rejecting unsafe URL: %v", err)
		}
	})

	t.Run("dependency basic auth", func(t *testing.T) {
		directory := t.TempDir()
		baselinePath := filepath.Join(directory, "baseline.json")
		writeReleaseArtifact(t, baselinePath, []byte(`{"direct_modules":[{"path":"example.com/module","version":"v1.2.3","license":"Apache-2.0","source":"https://operator:`+sentinel+`@example.invalid/module"}]}`))
		outputPath := filepath.Join(directory, "dependencies.json")
		command := exec.Command(
			"python3", filepath.Join(root, "scripts", "release", "collect.py"), "dependency-license",
			"--baseline", baselinePath,
			"--output", outputPath,
		)
		command.Dir = root
		output, err := command.CombinedOutput()
		if err == nil {
			t.Fatalf("dependency collector accepted basic-auth source URL:\n%s", output)
		}
		if !strings.Contains(string(output), "userinfo") {
			t.Fatalf("dependency basic-auth failure = %q", output)
		}
		if strings.Contains(string(output), sentinel) {
			t.Fatalf("dependency basic-auth failure disclosed sentinel: %q", output)
		}
		if _, err := os.Stat(outputPath); !os.IsNotExist(err) {
			t.Fatalf("dependency collector wrote evidence after rejecting unsafe URL: %v", err)
		}
	})
}

func TestReleaseEvidenceCollectorEnforcesCanonicalProvenanceURLPolicy(t *testing.T) {
	root := repositoryRoot(t)
	sentinel := "opaque-release-evidence-sentinel-0123456789"
	for _, test := range []struct {
		name   string
		url    string
		accept bool
	}{
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
	} {
		for _, kind := range []string{"fixture", "dependency"} {
			t.Run(kind+" "+test.name, func(t *testing.T) {
				directory := t.TempDir()
				var command *exec.Cmd
				var outputPath string
				switch kind {
				case "fixture":
					fixtureDirectory := filepath.Join(directory, "llm", "provider", "example", "testdata", "contracts", "example-profile")
					if err := os.MkdirAll(fixtureDirectory, 0o700); err != nil {
						t.Fatal(err)
					}
					writeReleaseArtifact(t, filepath.Join(fixtureDirectory, "metadata.yaml"), []byte("profile: example-profile\nupstream_url: "+test.url+"\nupstream_date: 2026-07-15\n"))
					outputPath = filepath.Join(directory, "fixture-manifest.json")
					command = exec.Command("python3", filepath.Join(root, "scripts", "release", "collect.py"), "fixture-manifest", "--root", directory, "--output", outputPath)
				case "dependency":
					baselinePath := filepath.Join(directory, "baseline.json")
					baseline, err := json.Marshal(map[string]any{
						"direct_modules": []map[string]string{{
							"path":    "example.com/module",
							"version": "v1.2.3",
							"license": "Apache-2.0",
							"source":  test.url,
						}},
					})
					if err != nil {
						t.Fatal(err)
					}
					writeReleaseArtifact(t, baselinePath, baseline)
					outputPath = filepath.Join(directory, "dependencies.json")
					command = exec.Command("python3", filepath.Join(root, "scripts", "release", "collect.py"), "dependency-license", "--baseline", baselinePath, "--output", outputPath)
				default:
					t.Fatalf("unknown provenance kind %q", kind)
				}
				command.Dir = root
				output, err := command.CombinedOutput()
				if test.accept {
					if err != nil {
						t.Fatalf("collector rejected canonical %s URL: %v\n%s", kind, err, output)
					}
					if _, err := os.Stat(outputPath); err != nil {
						t.Fatalf("collector did not write canonical %s evidence: %v", kind, err)
					}
					return
				}
				if err == nil {
					t.Fatalf("collector accepted noncanonical %s URL %q:\n%s", kind, test.url, output)
				}
				if !strings.Contains(string(output), "DNS hostname") {
					t.Fatalf("noncanonical %s URL failure = %q", kind, output)
				}
				if strings.Contains(string(output), sentinel) {
					t.Fatalf("noncanonical %s URL failure disclosed sentinel: %q", kind, output)
				}
				if _, err := os.Stat(outputPath); !os.IsNotExist(err) {
					t.Fatalf("collector wrote %s evidence after rejecting URL: %v", kind, err)
				}
			})
		}
	}
}

func TestReleaseEvidenceCollectorRejectsNonContainerTrivyReport(t *testing.T) {
	root := repositoryRoot(t)
	directory := t.TempDir()
	inputPath := filepath.Join(directory, "scan.json")
	outputPath := filepath.Join(directory, "image-scan.json")
	digest := "sha256:" + strings.Repeat("b", 64)
	writeReleaseArtifact(t, inputPath, []byte(`{"SchemaVersion":2,"ArtifactType":"filesystem","ArtifactName":"image.oci","Results":[]}`))

	command := exec.Command(
		"python3", filepath.Join(root, "scripts", "release", "collect.py"), "annotate-scan",
		"--input", inputPath,
		"--output", outputPath,
		"--reference", "llm-temporal-worker@"+digest,
		"--digest", digest,
	)
	command.Dir = root
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatalf("scan collector accepted a non-container Trivy report:\n%s", output)
	}
	if !strings.Contains(string(output), "container-image") {
		t.Fatalf("non-container scan failure = %q", output)
	}
	if _, err := os.Stat(outputPath); !os.IsNotExist(err) {
		t.Fatalf("scan collector wrote evidence after rejecting non-container report: %v", err)
	}
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

			command := exec.Command(
				"python3", filepath.Join(root, "scripts", "release", "collect.py"), "annotate-scan",
				"--input", inputPath,
				"--output", outputPath,
				"--reference", "llm-temporal-worker@"+digest,
				"--digest", digest,
			)
			command.Dir = root
			output, err := command.CombinedOutput()
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
			if err == nil {
				t.Fatalf("scan collector accepted unexpected temporary OCI directory basename:\n%s", output)
			}
			if !strings.Contains(string(output), "temporary OCI directory") {
				t.Fatalf("unexpected OCI directory basename failure = %q", output)
			}
			if _, err := os.Stat(outputPath); !os.IsNotExist(err) {
				t.Fatalf("scan collector wrote evidence after rejecting OCI directory basename: %v", err)
			}
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
			if err == nil {
				t.Fatalf("collector accepted unsafe temporary OCI directory destination:\n%s", output)
			}
			if !strings.Contains(string(output), test.want) {
				t.Fatalf("unsafe temporary OCI directory destination failure = %q, want %q", output, test.want)
			}
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
			directory := t.TempDir()
			inputPath := filepath.Join(directory, "compose.log")
			outputPath := filepath.Join(directory, "redacted-log.json")
			writeReleaseArtifact(t, inputPath, []byte(test.input))
			command := exec.Command(
				"python3", filepath.Join(root, "scripts", "release", "collect.py"), "redacted-log",
				"--kind", test.kind,
				"--input", inputPath,
				"--output", outputPath,
			)
			command.Dir = root
			output, err := command.CombinedOutput()
			if err != nil {
				t.Fatalf("redacted-log collector rejected %s input: %v\n%s", test.name, err, output)
			}
			data, err := os.ReadFile(outputPath)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(data), sentinel) {
				t.Fatalf("redacted-log artifact retained sentinel secret: %s", data)
			}
			var artifact struct {
				EventCounts map[string]int `json:"event_counts"`
			}
			if err := json.Unmarshal(data, &artifact); err != nil {
				t.Fatal(err)
			}
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
			directory := t.TempDir()
			inputPath := filepath.Join(directory, "compose.log")
			outputPath := filepath.Join(directory, "redacted-log.json")
			writeReleaseArtifact(t, inputPath, []byte(test.input))
			command := exec.Command(
				"python3", filepath.Join(root, "scripts", "release", "collect.py"), "redacted-log",
				"--kind", test.kind,
				"--input", inputPath,
				"--output", outputPath,
			)
			command.Dir = root
			output, err := command.CombinedOutput()
			if err == nil {
				t.Fatalf("redacted-log collector accepted missing service boundary:\n%s", output)
			}
			if !strings.Contains(string(output), "runtime-boundary") {
				t.Fatalf("missing boundary failure = %q", output)
			}
			if _, err := os.Stat(outputPath); !os.IsNotExist(err) {
				t.Fatalf("redacted-log collector wrote an artifact after rejecting input: %v", err)
			}
		})
	}
}

func TestReleaseEvidenceVerifierRejectsChangedArtifact(t *testing.T) {
	root := repositoryRoot(t)
	bundle := writeReleaseEvidenceBundle(t, false)
	if err := os.WriteFile(filepath.Join(bundle.directory, "test-summary.json"), []byte(`{"status":"changed"}`), 0o600); err != nil {
		t.Fatal(err)
	}

	output, err := runReleaseEvidenceVerifier(t, root, bundle.directory)
	if err == nil {
		t.Fatalf("release evidence verifier accepted changed artifact:\n%s", output)
	}
	if !strings.Contains(string(output), "sha256") {
		t.Fatalf("changed artifact failure = %q, want digest evidence", output)
	}
}

func TestReleaseEvidenceVerifierRejectsIncorrectByteLength(t *testing.T) {
	root := repositoryRoot(t)
	bundle := writeReleaseEvidenceBundle(t, false)
	evidence := readReleaseEvidence(t, bundle.directory)
	artifacts := evidence["artifacts"].(map[string]any)
	testSummary := artifacts["test_summary"].(map[string]any)
	testSummary["bytes"] = testSummary["bytes"].(float64) + 1
	writeReleaseEvidence(t, bundle.directory, evidence)

	output, err := runReleaseEvidenceVerifier(t, root, bundle.directory)
	if err == nil {
		t.Fatalf("release evidence verifier accepted an incorrect byte length:\n%s", output)
	}
	if !strings.Contains(string(output), "byte length") {
		t.Fatalf("incorrect byte length failure = %q", output)
	}
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
			if err == nil {
				t.Fatalf("release evidence verifier accepted unsafe repository URL:\n%s", output)
			}
			if !strings.Contains(string(output), test.want) {
				t.Fatalf("unsafe repository failure = %q, want %q", output, test.want)
			}
			if strings.Contains(string(output), sentinel) {
				t.Fatalf("unsafe repository failure disclosed sentinel secret: %q", output)
			}
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
			data, err := os.ReadFile(artifactPath)
			if err != nil {
				t.Fatal(err)
			}
			var document map[string]any
			if err := json.Unmarshal(data, &document); err != nil {
				t.Fatal(err)
			}
			test.apply(t, document, "https://operator:"+sentinel+"@example.invalid/provenance")
			data, err = json.Marshal(document)
			if err != nil {
				t.Fatal(err)
			}
			replaceReleaseEvidenceArtifact(t, bundle, evidence, test.artifact, data)
			writeReleaseEvidence(t, bundle.directory, evidence)

			output, err := runReleaseEvidenceVerifier(t, root, bundle.directory)
			if err == nil {
				t.Fatalf("release evidence verifier accepted unsafe %s provenance URL:\n%s", test.name, output)
			}
			if !strings.Contains(string(output), test.want) {
				t.Fatalf("unsafe %s provenance failure = %q, want %q", test.name, output, test.want)
			}
			if strings.Contains(string(output), sentinel) {
				t.Fatalf("unsafe %s provenance failure disclosed sentinel: %q", test.name, output)
			}
		})
	}
}

func TestReleaseEvidenceVerifierEnforcesCanonicalProvenanceURLPolicy(t *testing.T) {
	root := repositoryRoot(t)
	sentinel := "opaque-release-evidence-sentinel-0123456789"
	for _, test := range []struct {
		name   string
		url    string
		accept bool
	}{
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
	} {
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
					data, err := os.ReadFile(path)
					if err != nil {
						t.Fatal(err)
					}
					var document map[string]any
					if err := json.Unmarshal(data, &document); err != nil {
						t.Fatal(err)
					}
					if target == "fixture" {
						document["fixtures"].([]any)[0].(map[string]any)["upstream_url"] = test.url
					} else {
						document["direct_modules"].([]any)[0].(map[string]any)["source"] = test.url
					}
					data, err = json.Marshal(document)
					if err != nil {
						t.Fatal(err)
					}
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
				if strings.Contains(string(output), sentinel) {
					t.Fatalf("noncanonical %s URL failure disclosed sentinel: %q", target, output)
				}
			})
		}
	}
}

func TestReleaseEvidenceSchemasEnforceDNSProvenancePolicy(t *testing.T) {
	root := repositoryRoot(t)
	bundle := writeReleaseEvidenceBundle(t, false)
	evidenceSchema := compileReleaseEvidenceJSONSchema(t, root, filepath.Join("docs", "release", "evidence.schema.json"), "urn:llmtw:release-evidence:v1")
	artifactSchema := compileReleaseEvidenceJSONSchema(t, root, filepath.Join("docs", "release", "artifact.schema.json"), "urn:llmtw:release-evidence-artifact:v1")
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

				err := validateReleaseEvidenceJSONSchema(schema, document)
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
			if err == nil {
				t.Fatalf("release evidence verifier accepted escaped secret key in %s:\n%s", test.name, output)
			}
			if !strings.Contains(string(output), "secret-like") {
				t.Fatalf("escaped secret-key failure = %q", output)
			}
			if strings.Contains(string(output), sentinel) {
				t.Fatalf("escaped secret-key failure disclosed sentinel: %q", output)
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

			output, err := runReleaseEvidenceVerifier(t, root, bundle.directory)
			if err == nil {
				t.Fatalf("release evidence verifier accepted %s:\n%s", mutate.name, output)
			}
			if !strings.Contains(string(output), "path") {
				t.Fatalf("%s failure = %q", mutate.name, output)
			}
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

	output, err := runReleaseEvidenceVerifier(t, root, bundle.directory)
	if err == nil {
		t.Fatalf("release evidence verifier accepted an unknown artifact:\n%s", output)
	}
	if !strings.Contains(string(output), "does not satisfy schema") {
		t.Fatalf("unknown artifact failure = %q", output)
	}
}

func TestReleaseEvidenceVerifierRejectsCriticalImageFinding(t *testing.T) {
	root := repositoryRoot(t)
	bundle := writeReleaseEvidenceBundle(t, true)

	output, err := runReleaseEvidenceVerifier(t, root, bundle.directory)
	if err == nil {
		t.Fatalf("release evidence verifier accepted critical image finding:\n%s", output)
	}
	if !strings.Contains(string(output), "HIGH or CRITICAL") {
		t.Fatalf("critical image finding failure = %q", output)
	}
}

func TestReleaseEvidenceVerifierRejectsNonContainerTrivyReport(t *testing.T) {
	root := repositoryRoot(t)
	bundle := writeReleaseEvidenceBundle(t, false)
	evidence := readReleaseEvidence(t, bundle.directory)
	scan := strings.Replace(releaseEvidenceScan(bundle.imageReference, bundle.imageDigest, false), `"ArtifactType":"container_image"`, `"ArtifactType":"filesystem"`, 1)
	replaceReleaseEvidenceArtifact(t, bundle, evidence, "image_scan", []byte(scan))
	writeReleaseEvidence(t, bundle.directory, evidence)

	output, err := runReleaseEvidenceVerifier(t, root, bundle.directory)
	if err == nil {
		t.Fatalf("release evidence verifier accepted a non-container Trivy report:\n%s", output)
	}
	if !strings.Contains(string(output), "container-image") {
		t.Fatalf("non-container Trivy report failure = %q", output)
	}
}

func TestReleaseEvidenceVerifierRejectsUnnormalizedTemporaryTrivyOCIDirectoryName(t *testing.T) {
	root := repositoryRoot(t)
	bundle := writeReleaseEvidenceBundle(t, false)
	evidence := readReleaseEvidence(t, bundle.directory)
	scan := strings.Replace(releaseEvidenceScan(bundle.imageReference, bundle.imageDigest, false), `"ArtifactName":"image.oci"`, `"ArtifactName":"/private/runner-temp/image.oci"`, 1)
	replaceReleaseEvidenceArtifact(t, bundle, evidence, "image_scan", []byte(scan))
	writeReleaseEvidence(t, bundle.directory, evidence)

	output, err := runReleaseEvidenceVerifier(t, root, bundle.directory)
	if err == nil {
		t.Fatalf("release evidence verifier accepted a runner-temporary Trivy path:\n%s", output)
	}
	if !strings.Contains(string(output), "normalized to the temporary OCI directory basename") {
		t.Fatalf("unnormalized temporary Trivy path failure = %q", output)
	}
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

			output, err := runReleaseEvidenceVerifier(t, root, bundle.directory)
			if err == nil {
				t.Fatalf("release evidence verifier accepted %s:\n%s", mutate.name, output)
			}
			if !strings.Contains(string(output), "summary") {
				t.Fatalf("%s failure = %q, want summary-schema evidence", mutate.name, output)
			}
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

			output, err := runReleaseEvidenceVerifier(t, root, bundle.directory)
			if err == nil {
				t.Fatalf("release evidence verifier accepted a missing %s boundary:\n%s", test.name, output)
			}
			if !strings.Contains(string(output), test.want) {
				t.Fatalf("missing %s boundary failure = %q", test.name, output)
			}
		})
	}
}

func TestReleaseEvidenceVerifierRejectsUnreferencedArtifactFile(t *testing.T) {
	root := repositoryRoot(t)
	bundle := writeReleaseEvidenceBundle(t, false)
	sentinel := "api_key=orphaned-release-evidence-secret-0123456789"
	writeReleaseArtifact(t, filepath.Join(bundle.directory, "orphan-secret.json"), []byte(sentinel))

	output, err := runReleaseEvidenceVerifier(t, root, bundle.directory)
	if err == nil {
		t.Fatalf("release evidence verifier accepted an unreferenced artifact file:\n%s", output)
	}
	if !strings.Contains(string(output), "unreferenced file") {
		t.Fatalf("unreferenced artifact failure = %q", output)
	}
	if strings.Contains(string(output), sentinel) {
		t.Fatalf("unreferenced artifact failure disclosed sentinel secret: %q", output)
	}
}

func TestReleaseEvidenceVerifierRejectsResidualOCIArchive(t *testing.T) {
	root := repositoryRoot(t)
	bundle := writeReleaseEvidenceBundle(t, false)
	sentinel := "api_key=residual-oci-archive-secret-0123456789"
	writeReleaseArtifact(t, filepath.Join(bundle.directory, "image.oci.tar"), []byte(sentinel))

	output, err := runReleaseEvidenceVerifier(t, root, bundle.directory)
	if err == nil {
		t.Fatalf("release evidence verifier accepted a residual OCI archive:\n%s", output)
	}
	if !strings.Contains(string(output), "unreferenced file") {
		t.Fatalf("residual OCI archive failure = %q", output)
	}
	if strings.Contains(string(output), sentinel) {
		t.Fatalf("residual OCI archive failure disclosed sentinel secret: %q", output)
	}
}

func TestReleaseEvidenceVerifierRejectsResidualOCILayoutDirectory(t *testing.T) {
	root := repositoryRoot(t)
	bundle := writeReleaseEvidenceBundle(t, false)
	layout := filepath.Join(bundle.directory, "image.oci")
	if err := os.MkdirAll(filepath.Join(layout, "blobs", "sha256"), 0o700); err != nil {
		t.Fatal(err)
	}
	writeReleaseArtifact(t, filepath.Join(layout, "oci-layout"), []byte(`{"imageLayoutVersion":"1.0.0"}`))

	output, err := runReleaseEvidenceVerifier(t, root, bundle.directory)
	if err == nil {
		t.Fatalf("release evidence verifier accepted a residual OCI layout directory:\n%s", output)
	}
	if !strings.Contains(string(output), "unreferenced directory") {
		t.Fatalf("residual OCI layout directory failure = %q", output)
	}
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
			if err == nil {
				t.Fatalf("release evidence verifier accepted a secret-like value in retained artifact %q:\n%s", name, output)
			}
			if !strings.Contains(string(output), "secret-like") {
				t.Fatalf("retained artifact %q secret failure = %q", name, output)
			}
			if strings.Contains(string(output), sentinel) {
				t.Fatalf("retained artifact %q secret failure disclosed sentinel: %q", name, output)
			}
		})
	}
}

func TestReleaseEvidenceVerifierRejectsNoncanonicalEvidencePath(t *testing.T) {
	root := repositoryRoot(t)
	bundle := writeReleaseEvidenceBundle(t, false)
	directory := resolvedReleaseEvidenceDirectory(t, bundle.directory)
	renamedPath := filepath.Join(directory, "nested", "evidence.json")
	if err := os.Mkdir(filepath.Dir(renamedPath), 0o700); err != nil {
		t.Fatal(err)
	}
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
	if err == nil {
		t.Fatalf("release evidence verifier accepted a nested evidence record:\n%s", output)
	}
	if !strings.Contains(string(output), "record path must be artifact-dir/evidence.json") {
		t.Fatalf("nested evidence record failure = %q", output)
	}
}

func TestReleaseEvidenceRecorderRejectsUnreferencedArtifactFile(t *testing.T) {
	root := repositoryRoot(t)
	bundle := writeReleaseEvidenceBundle(t, false)
	directory := resolvedReleaseEvidenceDirectory(t, bundle.directory)
	if err := os.Remove(filepath.Join(directory, "evidence.json")); err != nil {
		t.Fatal(err)
	}
	writeReleaseArtifact(t, filepath.Join(directory, "orphan.json"), []byte(`{"api_key":"orphaned-release-evidence-secret-0123456789"}`))
	outputPath := filepath.Join(directory, "evidence.json")

	output, err := runReleaseEvidenceRecorder(root, directory, outputPath, bundle.imageReference, bundle.imageDigest)
	if err == nil {
		t.Fatalf("release evidence recorder accepted an unreferenced artifact file:\n%s", output)
	}
	if !strings.Contains(string(output), "unreferenced file") {
		t.Fatalf("unreferenced recorder failure = %q", output)
	}
	if _, err := os.Stat(outputPath); !os.IsNotExist(err) {
		t.Fatalf("recorder wrote evidence after unreferenced artifact rejection: %v", err)
	}
}

func TestReleaseEvidenceRecorderRejectsImageLayoutArtifact(t *testing.T) {
	root := repositoryRoot(t)
	bundle := writeReleaseEvidenceBundle(t, false)
	directory := resolvedReleaseEvidenceDirectory(t, bundle.directory)
	if err := os.Remove(filepath.Join(directory, "evidence.json")); err != nil {
		t.Fatal(err)
	}
	writeReleaseArtifact(t, filepath.Join(directory, "image.oci.tar"), []byte("not a retained OCI archive"))
	outputPath := filepath.Join(directory, "evidence.json")

	output, err := runReleaseEvidenceRecorderWithAdditionalArtifact(
		root,
		directory,
		outputPath,
		bundle.imageReference,
		bundle.imageDigest,
		"image_layout",
		"image.oci.tar",
	)
	if err == nil {
		t.Fatalf("release evidence recorder accepted a raw OCI archive artifact:\n%s", output)
	}
	if !strings.Contains(string(output), "unknown artifact") {
		t.Fatalf("raw OCI archive recorder failure = %q", output)
	}
	if _, err := os.Stat(outputPath); !os.IsNotExist(err) {
		t.Fatalf("recorder wrote evidence after raw OCI archive rejection: %v", err)
	}
}

func TestReleaseEvidenceRecorderRejectsResidualOCILayoutDirectory(t *testing.T) {
	root := repositoryRoot(t)
	bundle := writeReleaseEvidenceBundle(t, false)
	directory := resolvedReleaseEvidenceDirectory(t, bundle.directory)
	if err := os.Remove(filepath.Join(directory, "evidence.json")); err != nil {
		t.Fatal(err)
	}
	layout := filepath.Join(directory, "image.oci")
	if err := os.MkdirAll(filepath.Join(layout, "blobs", "sha256"), 0o700); err != nil {
		t.Fatal(err)
	}
	writeReleaseArtifact(t, filepath.Join(layout, "oci-layout"), []byte(`{"imageLayoutVersion":"1.0.0"}`))
	outputPath := filepath.Join(directory, "evidence.json")

	output, err := runReleaseEvidenceRecorder(root, directory, outputPath, bundle.imageReference, bundle.imageDigest)
	if err == nil {
		t.Fatalf("release evidence recorder accepted a residual OCI layout directory:\n%s", output)
	}
	if !strings.Contains(string(output), "unreferenced directory") {
		t.Fatalf("residual OCI layout directory recorder failure = %q", output)
	}
	if _, err := os.Stat(outputPath); !os.IsNotExist(err) {
		t.Fatalf("recorder wrote evidence after residual OCI layout rejection: %v", err)
	}
}

func TestReleaseEvidenceRecorderRejectsOutputArtifactCollision(t *testing.T) {
	root := repositoryRoot(t)
	bundle := writeReleaseEvidenceBundle(t, false)
	directory := resolvedReleaseEvidenceDirectory(t, bundle.directory)
	if err := os.Remove(filepath.Join(directory, "evidence.json")); err != nil {
		t.Fatal(err)
	}
	outputPath := filepath.Join(directory, releaseEvidenceArtifactPaths["test_summary"])
	before, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatal(err)
	}

	output, err := runReleaseEvidenceRecorder(root, directory, outputPath, bundle.imageReference, bundle.imageDigest)
	if err == nil {
		t.Fatalf("release evidence recorder accepted an output artifact collision:\n%s", output)
	}
	if !strings.Contains(string(output), "must not overwrite required artifact") {
		t.Fatalf("output artifact collision failure = %q", output)
	}
	after, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(before) {
		t.Fatal("recorder changed an input artifact after rejecting output collision")
	}
}

func TestReleaseEvidenceRecorderRejectsNoncanonicalArtifactPath(t *testing.T) {
	root := repositoryRoot(t)
	bundle := writeReleaseEvidenceBundle(t, false)
	directory := resolvedReleaseEvidenceDirectory(t, bundle.directory)
	if err := os.Remove(filepath.Join(directory, "evidence.json")); err != nil {
		t.Fatal(err)
	}
	outputPath := filepath.Join(directory, "evidence.json")

	output, err := runReleaseEvidenceRecorderWithArtifactOverride(
		root,
		directory,
		outputPath,
		bundle.imageReference,
		bundle.imageDigest,
		"test_summary",
		"nested/test-summary.json",
	)
	if err == nil {
		t.Fatalf("release evidence recorder accepted a noncanonical artifact path:\n%s", output)
	}
	if !strings.Contains(string(output), "canonical path") {
		t.Fatalf("noncanonical artifact path failure = %q", output)
	}
}

func TestReleaseEvidenceRecorderRejectsSymlinkedOutputParent(t *testing.T) {
	root := repositoryRoot(t)
	bundle := writeReleaseEvidenceBundle(t, false)
	directory := resolvedReleaseEvidenceDirectory(t, bundle.directory)
	outside := filepath.Join(filepath.Dir(directory), "outside-output")
	if err := os.Mkdir(outside, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(directory, "escaped")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}

	output, err := runReleaseEvidenceRecorder(root, directory, filepath.Join(link, "evidence.json"), bundle.imageReference, bundle.imageDigest)
	if err == nil {
		t.Fatalf("release evidence recorder accepted a symlinked output parent:\n%s", output)
	}
	if !strings.Contains(string(output), "symlink") {
		t.Fatalf("symlinked output failure = %q", output)
	}
	if _, err := os.Stat(filepath.Join(outside, "evidence.json")); !os.IsNotExist(err) {
		t.Fatalf("recorder wrote evidence outside artifact directory: %v", err)
	}
}

func TestReleaseEvidenceRecorderLeavesNoEvidenceOnValidationFailure(t *testing.T) {
	root := repositoryRoot(t)
	bundle := writeReleaseEvidenceBundle(t, true)
	directory := resolvedReleaseEvidenceDirectory(t, bundle.directory)
	outputPath := filepath.Join(directory, "evidence.json")
	if err := os.Remove(outputPath); err != nil {
		t.Fatal(err)
	}

	output, err := runReleaseEvidenceRecorder(root, directory, outputPath, bundle.imageReference, bundle.imageDigest)
	if err == nil {
		t.Fatalf("release evidence recorder accepted a failing image scan:\n%s", output)
	}
	if _, err := os.Stat(outputPath); !os.IsNotExist(err) {
		t.Fatalf("recorder left final evidence after validation failure: %v", err)
	}
	temporary, err := filepath.Glob(outputPath + ".tmp-*")
	if err != nil {
		t.Fatal(err)
	}
	if len(temporary) != 0 {
		t.Fatalf("recorder left temporary evidence files: %v", temporary)
	}
}

func TestReleaseEvidenceRecorderAtomicallyWritesVerifiedByteBoundRecord(t *testing.T) {
	root := repositoryRoot(t)
	bundle := writeReleaseEvidenceBundle(t, false)
	directory := resolvedReleaseEvidenceDirectory(t, bundle.directory)
	outputPath := filepath.Join(directory, "evidence.json")
	if err := os.Remove(outputPath); err != nil {
		t.Fatal(err)
	}

	output, err := runReleaseEvidenceRecorder(root, directory, outputPath, bundle.imageReference, bundle.imageDigest)
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
	data, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatal(err)
	}
	var evidence map[string]any
	if err := json.Unmarshal(data, &evidence); err != nil {
		t.Fatal(err)
	}
	for _, name := range requiredReleaseEvidenceArtifacts {
		artifact := evidence["artifacts"].(map[string]any)[name].(map[string]any)
		if _, ok := artifact["bytes"].(float64); !ok || artifact["bytes"].(float64) <= 0 {
			t.Fatalf("recorded artifact %q has no positive byte count: %#v", name, artifact)
		}
		if digest, _ := artifact["sha256"].(string); len(digest) != 64 {
			t.Fatalf("recorded artifact %q sha256 = %q", name, digest)
		}
	}
	temporary, err := filepath.Glob(outputPath + ".tmp-*")
	if err != nil {
		t.Fatal(err)
	}
	if len(temporary) != 0 {
		t.Fatalf("recorder left temporary evidence files: %v", temporary)
	}
}

func TestReleaseEvidenceEntrypointIsLocalAndNonPublishing(t *testing.T) {
	root := repositoryRoot(t)
	makefile := readRepositoryFile(t, moduleRoot(t), "Makefile")
	if !strings.Contains(makefile, "release-verify:") {
		t.Fatal("Makefile does not expose release-verify")
	}
	if strings.Contains(makefile, "RELEASE_EVIDENCE_FILE") {
		t.Fatal("Makefile permits a noncanonical release-evidence record path")
	}
	if strings.Contains(makefile, "RELEASE_EVIDENCE_DIR") {
		t.Fatal("Makefile permits a workflow-controlled release-evidence directory")
	}
	if !strings.Contains(makefile, `--artifact-dir "release-artifacts" --evidence "release-artifacts/evidence.json"`) {
		t.Fatal("Makefile does not seal release verification to the canonical evidence bundle")
	}
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
	if !strings.Contains(gitignore, "release-artifacts/") {
		t.Fatal(".gitignore does not exclude local release-artifacts/")
	}

	for _, path := range []string{
		filepath.Join("scripts", "release", "collect.sh"),
		filepath.Join("scripts", "release", "collect.py"),
		filepath.Join("scripts", "release", "verify.sh"),
		filepath.Join("scripts", "release", "record.sh"),
	} {
		data := readRepositoryFile(t, root, path)
		lower := strings.ToLower(data)
		for _, forbidden := range []string{"docker push", "cosign sign", "gh release", "workflow_dispatch", "id-token: write"} {
			if strings.Contains(lower, forbidden) {
				t.Fatalf("%s contains publication or Task 24 control %q", path, forbidden)
			}
		}
	}
	// Task 24 is separately documented below this heading. Keep the existing
	// Task 23 runbook boundary nonpublishing while allowing that later,
	// fail-closed control to document its manual trigger and OIDC boundary.
	runbook := readRepositoryFile(t, root, "docs", "release", "runbook.md")
	task23Runbook, _, found := strings.Cut(runbook, "\n## Guarded manual publication boundary")
	if !found {
		t.Fatal("release runbook does not separate the Task 23 and Task 24 boundaries")
	}
	for _, forbidden := range []string{"docker push", "cosign sign", "gh release", "workflow_dispatch", "id-token: write"} {
		if strings.Contains(strings.ToLower(task23Runbook), forbidden) {
			t.Fatalf("Task 23 runbook boundary contains publication or Task 24 control %q", forbidden)
		}
	}
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
	for _, want := range []string{"\"event_counts\"", "\"line_count\"", "\"input_bytes\"", "\"redaction_policy\""} {
		if !strings.Contains(artifactSchema, want) {
			t.Fatalf("release log schema is missing allowlisted summary field %q", want)
		}
	}
	for _, forbidden := range []string{"\"content\"", "\"message\"", "\"text\"", "\"raw_log\"", "\"log_lines\""} {
		if strings.Contains(artifactSchema, forbidden) {
			t.Fatalf("release log schema permits raw log content field %q", forbidden)
		}
	}
}

func TestReleaseEvidenceCollectorRunsFuzzGateFromModuleRoot(t *testing.T) {
	root := repositoryRoot(t)
	collector := readRepositoryFile(t, root, "scripts", "release", "collect.sh")
	if !strings.Contains(collector, `run_gate fuzz_summary bash -c "cd \"$module_root\" && bash \"$module_root/scripts/run-fuzz.sh\" smoke"`) {
		t.Fatal("release evidence collector does not run the fuzz gate from the Go module root")
	}
}

func TestReleaseEvidenceCollectorRetainsMemoryBenchmarkMeasurement(t *testing.T) {
	root := repositoryRoot(t)
	collector := readRepositoryFile(t, root, "scripts", "release", "collect.sh")
	if !strings.Contains(collector, "BenchmarkGenerateMemoryAdmissionAndCompile") {
		t.Fatal("release evidence collector does not run the memory admission benchmark")
	}
	pythonCollector := readRepositoryFile(t, root, "scripts", "release", "collect.py")
	for _, required := range []string{"benchmark-summary", "p99_ms_per_op", "measurement_only"} {
		if !strings.Contains(pythonCollector, required) {
			t.Fatalf("benchmark collector is missing %q", required)
		}
	}
}

func TestReleaseEvidenceCollectorSummarizesMemoryBenchmark(t *testing.T) {
	root := repositoryRoot(t)
	inputPath := filepath.Join(t.TempDir(), "benchmark.out")
	outputPath := filepath.Join(t.TempDir(), "benchmark-summary.json")
	input := "goos: linux\n" +
		"BenchmarkGenerateMemoryAdmissionAndCompile-14    4267    255245 ns/op         0.7286 p99_ms/op  350632 B/op    4851 allocs/op\n" +
		"PASS\n"
	writeReleaseArtifact(t, inputPath, []byte(input))
	command := exec.Command(
		"python3", filepath.Join(root, "scripts", "release", "collect.py"), "benchmark-summary",
		"--kind", "benchmark_summary", "--input", inputPath, "--output", outputPath,
	)
	command.Dir = root
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("benchmark collector rejected valid output: %v\n%s", err, output)
	}
	document := readReleaseEvidenceJSONArtifact(t, filepath.Dir(outputPath), "benchmark_summary")
	if document["scope"] != "memory" || document["objective_status"] != "measurement_only" {
		t.Fatalf("benchmark summary scope/status = %#v/%#v", document["scope"], document["objective_status"])
	}
	if document["p99_ms_per_op"] != 0.7286 {
		t.Fatalf("benchmark summary p99 = %#v", document["p99_ms_per_op"])
	}

	writeReleaseArtifact(t, inputPath, []byte(input+"BenchmarkGenerateMemoryAdmissionAndCompile-14 1 1 ns/op 1 p99_ms/op\n"))
	if err := os.Remove(outputPath); err != nil {
		t.Fatal(err)
	}
	if output, err := command.CombinedOutput(); err == nil {
		t.Fatalf("benchmark collector accepted multiple measurements:\n%s", output)
	}
	if _, err := os.Stat(outputPath); !os.IsNotExist(err) {
		t.Fatalf("benchmark collector retained output after rejecting multiple measurements: %v", err)
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

type releaseEvidenceBundle struct {
	directory      string
	imageReference string
	imageDigest    string
}

func writeReleaseEvidenceBundle(t *testing.T, criticalFinding bool) releaseEvidenceBundle {
	t.Helper()
	directory := t.TempDir()
	imageDigest := writeReleaseEvidenceOCILayout(t, filepath.Join(t.TempDir(), "image.oci"))
	imageReference := "llm-temporal-worker@" + imageDigest
	contents := map[string]string{
		"test_summary":          releaseEvidenceGateSummary("test_summary"),
		"race_summary":          releaseEvidenceGateSummary("race_summary"),
		"fuzz_summary":          releaseEvidenceGateSummary("fuzz_summary"),
		"benchmark_summary":     `{"schema_version":1,"kind":"benchmark_summary","status":"pass","benchmark":"BenchmarkGenerateMemoryAdmissionAndCompile","scope":"memory","samples":4267,"ns_per_op":255245,"p99_ms_per_op":0.7286,"target_ms":25,"objective_status":"measurement_only","output_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","output_bytes":256,"redacted":true}`,
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
		"schema_version": 1,
		"generated_at":   "2026-07-15T00:00:00Z",
		"source": map[string]any{
			"repository": "https://github.com/mfow/llm-temporal-worker",
			"revision":   strings.Repeat("b", 40),
		},
		"image": map[string]any{
			"reference": imageReference,
			"digest":    imageDigest,
		},
		"artifacts": evidenceArtifacts,
	}
	data, err := json.Marshal(evidence)
	if err != nil {
		t.Fatal(err)
	}
	writeReleaseArtifact(t, filepath.Join(directory, "evidence.json"), data)
	return releaseEvidenceBundle{directory: directory, imageReference: imageReference, imageDigest: imageDigest}
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
	config := []byte(`{"architecture":"amd64","os":"linux","config":{"Env":[]}}`)
	layer := []byte("release-evidence-test-layer")
	configDigest := sha256Hex(config)
	layerDigest := sha256Hex(layer)
	manifest := []byte(fmt.Sprintf(`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{"mediaType":"application/vnd.oci.image.config.v1+json","digest":"sha256:%s","size":%d},"layers":[{"mediaType":"application/vnd.oci.image.layer.v1.tar","digest":"sha256:%s","size":%d}]}`,
		configDigest, len(config), layerDigest, len(layer)))
	manifestDigest := sha256Hex(manifest)
	index := []byte(fmt.Sprintf(`{"schemaVersion":2,"manifests":[{"mediaType":"application/vnd.oci.image.manifest.v1+json","digest":"sha256:%s","size":%d}]}`,
		manifestDigest, len(manifest)))
	if err := os.MkdirAll(filepath.Join(path, "blobs", "sha256"), 0o700); err != nil {
		t.Fatal(err)
	}
	entries := []struct {
		name string
		data []byte
	}{
		{name: "oci-layout", data: []byte(`{"imageLayoutVersion":"1.0.0"}`)},
		{name: "index.json", data: index},
		{name: "blobs/sha256/" + configDigest, data: config},
		{name: "blobs/sha256/" + layerDigest, data: layer},
		{name: "blobs/sha256/" + manifestDigest, data: manifest},
	}
	if includeCompatibilityMetadata {
		entries = append(entries, struct {
			name string
			data []byte
		}{name: "manifest.json", data: []byte(`[]`)})
	}
	for _, entry := range entries {
		writeReleaseArtifact(t, filepath.Join(path, filepath.FromSlash(entry.name)), entry.data)
	}
	return "sha256:" + manifestDigest
}

func writeReleaseEvidenceNestedOCIIndexLayout(t *testing.T, path string, nestedIndex func(manifestDigest string, manifestSize int) []byte) string {
	t.Helper()
	config := []byte(`{"architecture":"amd64","os":"linux","config":{"Env":[]}}`)
	layer := []byte("release-evidence-test-layer")
	configDigest := sha256Hex(config)
	layerDigest := sha256Hex(layer)
	manifest := []byte(fmt.Sprintf(`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{"mediaType":"application/vnd.oci.image.config.v1+json","digest":"sha256:%s","size":%d},"layers":[{"mediaType":"application/vnd.oci.image.layer.v1.tar","digest":"sha256:%s","size":%d}]}`,
		configDigest, len(config), layerDigest, len(layer)))
	manifestDigest := sha256Hex(manifest)
	innerIndex := nestedIndex(manifestDigest, len(manifest))
	innerIndexDigest := sha256Hex(innerIndex)
	outerIndex := []byte(fmt.Sprintf(`{"schemaVersion":2,"manifests":[{"mediaType":"application/vnd.oci.image.index.v1+json","digest":"sha256:%s","size":%d}]}`,
		innerIndexDigest, len(innerIndex)))
	if err := os.MkdirAll(filepath.Join(path, "blobs", "sha256"), 0o700); err != nil {
		t.Fatal(err)
	}
	for _, entry := range []struct {
		name string
		data []byte
	}{
		{name: "oci-layout", data: []byte(`{"imageLayoutVersion":"1.0.0"}`)},
		{name: "index.json", data: outerIndex},
		{name: "blobs/sha256/" + configDigest, data: config},
		{name: "blobs/sha256/" + layerDigest, data: layer},
		{name: "blobs/sha256/" + manifestDigest, data: manifest},
		{name: "blobs/sha256/" + innerIndexDigest, data: innerIndex},
	} {
		writeReleaseArtifact(t, filepath.Join(path, filepath.FromSlash(entry.name)), entry.data)
	}
	return "sha256:" + manifestDigest
}

func writeReleaseEvidenceOCIIndexChainLayout(t *testing.T, path string, nestedIndexCount, terminalDescriptorCount int, terminalDescriptorType string) string {
	t.Helper()
	if nestedIndexCount < 1 || terminalDescriptorCount < 1 {
		t.Fatal("test OCI index chain requires positive descriptor counts")
	}
	config := []byte(`{"architecture":"amd64","os":"linux","config":{"Env":[]}}`)
	layer := []byte("release-evidence-test-layer")
	configDigest := sha256Hex(config)
	layerDigest := sha256Hex(layer)
	manifest := []byte(fmt.Sprintf(`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{"mediaType":"application/vnd.oci.image.config.v1+json","digest":"sha256:%s","size":%d},"layers":[{"mediaType":"application/vnd.oci.image.layer.v1.tar","digest":"sha256:%s","size":%d}]}`,
		configDigest, len(config), layerDigest, len(layer)))
	manifestDigest := sha256Hex(manifest)
	terminalDescriptor := fmt.Sprintf(`{"mediaType":"%s","digest":"sha256:%s","size":%d}`,
		terminalDescriptorType, manifestDigest, len(manifest))
	children := make([]string, terminalDescriptorCount)
	for index := range children {
		children[index] = terminalDescriptor
	}
	entries := []struct {
		name string
		data []byte
	}{
		{name: "oci-layout", data: []byte(`{"imageLayoutVersion":"1.0.0"}`)},
		{name: "blobs/sha256/" + configDigest, data: config},
		{name: "blobs/sha256/" + layerDigest, data: layer},
		{name: "blobs/sha256/" + manifestDigest, data: manifest},
	}
	for index := 0; index < nestedIndexCount; index++ {
		nestedIndex := []byte(`{"schemaVersion":2,"manifests":[` + strings.Join(children, ",") + `]}`)
		nestedIndexDigest := sha256Hex(nestedIndex)
		entries = append(entries, struct {
			name string
			data []byte
		}{name: "blobs/sha256/" + nestedIndexDigest, data: nestedIndex})
		children = []string{fmt.Sprintf(`{"mediaType":"application/vnd.oci.image.index.v1+json","digest":"sha256:%s","size":%d}`,
			nestedIndexDigest, len(nestedIndex))}
	}
	entries = append(entries, struct {
		name string
		data []byte
	}{name: "index.json", data: []byte(`{"schemaVersion":2,"manifests":[` + strings.Join(children, ",") + `]}`)})
	if err := os.MkdirAll(filepath.Join(path, "blobs", "sha256"), 0o700); err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		writeReleaseArtifact(t, filepath.Join(path, filepath.FromSlash(entry.name)), entry.data)
	}
	return "sha256:" + manifestDigest
}

func runReleaseEvidenceVerifier(t *testing.T, root, directory string) ([]byte, error) {
	t.Helper()
	directory = resolvedReleaseEvidenceDirectory(t, directory)
	command := exec.Command("bash", filepath.Join(root, "scripts", "release", "verify.sh"), "--artifact-dir", directory, "--evidence", filepath.Join(directory, "evidence.json"))
	command.Dir = root
	return command.CombinedOutput()
}

func runReleaseEvidenceLayoutDigest(root, path string) ([]byte, error) {
	command := exec.Command("go", "run", "./tools/releaseverify", "layout-digest", "-layout", path)
	command.Dir = filepath.Join(root, "golang")
	return command.CombinedOutput()
}

func runReleaseEvidenceRecorder(root, directory, outputPath, imageReference, imageDigest string) ([]byte, error) {
	return runReleaseEvidenceRecorderWithArtifactOverrides(root, directory, outputPath, imageReference, imageDigest, nil)
}

func runReleaseEvidenceRecorderWithArtifactOverride(root, directory, outputPath, imageReference, imageDigest, name, path string) ([]byte, error) {
	return runReleaseEvidenceRecorderWithArtifactOverrides(root, directory, outputPath, imageReference, imageDigest, map[string]string{name: path})
}

func runReleaseEvidenceRecorderWithAdditionalArtifact(root, directory, outputPath, imageReference, imageDigest, name, path string) ([]byte, error) {
	arguments := releaseEvidenceRecorderArguments(root, directory, outputPath, imageReference, imageDigest, nil)
	arguments = append(arguments, "-artifact", name+"="+path)
	command := exec.Command("bash", arguments...)
	command.Dir = root
	return command.CombinedOutput()
}

func runReleaseEvidenceRecorderWithArtifactOverrides(root, directory, outputPath, imageReference, imageDigest string, overrides map[string]string) ([]byte, error) {
	arguments := releaseEvidenceRecorderArguments(root, directory, outputPath, imageReference, imageDigest, overrides)
	command := exec.Command("bash", arguments...)
	command.Dir = root
	return command.CombinedOutput()
}

func releaseEvidenceRecorderArguments(root, directory, outputPath, imageReference, imageDigest string, overrides map[string]string) []string {
	arguments := []string{
		filepath.Join(root, "scripts", "release", "record.sh"),
		"-artifact-dir", directory,
		"-output", outputPath,
		"-repository", "https://github.com/mfow/llm-temporal-worker",
		"-revision", strings.Repeat("b", 40),
		"-image-reference", imageReference,
		"-image-digest", imageDigest,
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

func writeReleaseArtifact(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func readReleaseEvidence(t *testing.T, directory string) map[string]any {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(directory, "evidence.json"))
	if err != nil {
		t.Fatal(err)
	}
	var evidence map[string]any
	if err := json.Unmarshal(data, &evidence); err != nil {
		t.Fatal(err)
	}
	return evidence
}

func readReleaseEvidenceJSONArtifact(t *testing.T, directory, name string) map[string]any {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(directory, releaseEvidenceArtifactPaths[name]))
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := json.Unmarshal(data, &document); err != nil {
		t.Fatal(err)
	}
	return document
}

func cloneReleaseEvidenceJSON(t *testing.T, document map[string]any) map[string]any {
	t.Helper()
	data, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	var clone map[string]any
	if err := json.Unmarshal(data, &clone); err != nil {
		t.Fatal(err)
	}
	return clone
}

func compileReleaseEvidenceJSONSchema(t *testing.T, root, relativePath, resourceURL string) *jsonschema.Schema {
	t.Helper()
	document, err := jsonschema.UnmarshalJSON(bytes.NewReader([]byte(readRepositoryFile(t, root, relativePath))))
	if err != nil {
		t.Fatalf("release evidence schema %s is not valid JSON: %v", relativePath, err)
	}
	compiler := jsonschema.NewCompiler()
	if err := compiler.AddResource(resourceURL, document); err != nil {
		t.Fatalf("cannot add release evidence schema %s: %v", relativePath, err)
	}
	compiled, err := compiler.Compile(resourceURL)
	if err != nil {
		t.Fatalf("cannot compile release evidence schema %s: %v", relativePath, err)
	}
	return compiled
}

func validateReleaseEvidenceJSONSchema(schema *jsonschema.Schema, document map[string]any) error {
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
	data, err := json.Marshal(evidence)
	if err != nil {
		t.Fatal(err)
	}
	writeReleaseArtifact(t, filepath.Join(directory, "evidence.json"), data)
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

func resolvedReleaseEvidenceDirectory(t *testing.T, directory string) string {
	t.Helper()
	resolved, err := filepath.EvalSymlinks(directory)
	if err != nil {
		t.Fatal(err)
	}
	return resolved
}
