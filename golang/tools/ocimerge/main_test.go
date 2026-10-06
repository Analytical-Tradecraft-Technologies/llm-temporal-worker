package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// fixture writes OCI layouts shaped like Buildx single-platform exports with
// provenance and SBOM attestations.
type fixture struct {
	t   *testing.T
	dir string
}

type builtImage struct {
	manifest    descriptor
	attestation descriptor
	entries     []json.RawMessage
}

func newFixture(t *testing.T, name string) *fixture {
	t.Helper()
	dir := filepath.Join(t.TempDir(), name)
	if err := os.MkdirAll(filepath.Join(dir, "blobs", "sha256"), 0o755); err != nil {
		t.Fatal(err)
	}
	f := &fixture{t: t, dir: dir}
	f.writeFile("oci-layout", []byte(`{"imageLayoutVersion":"1.0.0"}`))
	return f
}

func (f *fixture) writeFile(name string, data []byte) {
	f.t.Helper()
	if err := os.WriteFile(filepath.Join(f.dir, name), data, 0o644); err != nil {
		f.t.Fatal(err)
	}
}

func (f *fixture) blob(mediaType string, data []byte) descriptor {
	f.t.Helper()
	d := descriptor{MediaType: mediaType, Digest: digestOf(data), Size: int64(len(data))}
	if err := os.WriteFile(blobPath(f.dir, d.Digest), data, 0o644); err != nil {
		f.t.Fatal(err)
	}
	return d
}

func (f *fixture) jsonBlob(mediaType string, value any) descriptor {
	f.t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		f.t.Fatal(err)
	}
	return f.blob(mediaType, data)
}

func (f *fixture) manifest(config, layer descriptor) descriptor {
	return f.jsonBlob(mediaTypeImageManifest, imageManifest{SchemaVersion: 2, MediaType: mediaTypeImageManifest, Config: config, Layers: []descriptor{layer}})
}

// image writes a platform manifest whose config declares configPlatform, a
// shared base layer, a platform-specific layer, and one attestation manifest.
func (f *fixture) image(configPlatform platform) builtImage {
	f.t.Helper()
	config := f.jsonBlob("application/vnd.oci.image.config.v1+json", imageConfig{OS: configPlatform.OS, Architecture: configPlatform.Architecture, Variant: configPlatform.Variant})
	layer := f.blob("application/vnd.oci.image.layer.v1.tar+gzip", []byte("layer for "+configPlatform.String()))
	manifest := f.manifest(config, layer)
	manifest.Platform = &platform{OS: configPlatform.OS, Architecture: configPlatform.Architecture, Variant: configPlatform.Variant}

	attestationConfig := f.blob("application/vnd.oci.image.config.v1+json", []byte(`{"architecture":"unknown","os":"unknown","config":{}}`))
	statement := f.blob("application/vnd.in-toto+json", []byte(`{"_type":"https://in-toto.io/Statement/v0.1","subject":"`+manifest.Digest+`"}`))
	attestation := f.manifest(attestationConfig, statement)
	attestation.Platform = &platform{OS: "unknown", Architecture: "unknown"}
	attestation.Annotations = map[string]string{
		referenceDigestAnnotation: manifest.Digest,
		referenceTypeAnnotation:   attestationManifestType,
	}
	return builtImage{manifest: manifest, attestation: attestation, entries: []json.RawMessage{f.raw(manifest), f.raw(attestation)}}
}

func (f *fixture) raw(d descriptor) json.RawMessage {
	f.t.Helper()
	data, err := json.Marshal(d)
	if err != nil {
		f.t.Fatal(err)
	}
	return data
}

// root writes the inner image index and an index.json that references it.
func (f *fixture) root(entries ...json.RawMessage) descriptor {
	f.t.Helper()
	index := f.jsonBlob(mediaTypeImageIndex, imageIndex{SchemaVersion: 2, MediaType: mediaTypeImageIndex, Manifests: entries})
	f.writeIndexJSON(index)
	return index
}

func (f *fixture) writeIndexJSON(top descriptor) {
	f.t.Helper()
	top.Annotations = map[string]string{"org.opencontainers.image.created": "2026-10-07T00:00:00Z"}
	data, err := json.Marshal(imageIndex{SchemaVersion: 2, MediaType: mediaTypeImageIndex, Manifests: []json.RawMessage{f.raw(top)}})
	if err != nil {
		f.t.Fatal(err)
	}
	f.writeFile("index.json", data)
}

var (
	linuxAMD64 = platform{OS: "linux", Architecture: "amd64"}
	linuxARM64 = platform{OS: "linux", Architecture: "arm64"}
)

func buildxLayout(t *testing.T, p platform) (*fixture, builtImage) {
	t.Helper()
	f := newFixture(t, p.Architecture)
	image := f.image(p)
	f.root(image.entries...)
	return f, image
}

func mergeOutput(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "merged.oci")
}

func TestMergeBuildsTwoPlatformIndexWithAttestations(t *testing.T) {
	amd64, amd64Image := buildxLayout(t, linuxAMD64)
	arm64, arm64Image := buildxLayout(t, linuxARM64)
	output := mergeOutput(t)

	var stdout bytes.Buffer
	err := run([]string{"merge", "-output", output, "-platform", "linux/amd64=" + amd64.dir, "-platform", "linux/arm64=" + arm64.dir}, &stdout)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	digest := strings.TrimSpace(stdout.String())

	top, err := readLayoutRoot(output)
	if err != nil {
		t.Fatal(err)
	}
	if top.Digest != digest || top.MediaType != mediaTypeImageIndex {
		t.Fatalf("index.json references %s %s, merge printed %s", top.MediaType, top.Digest, digest)
	}
	indexBytes, err := os.ReadFile(blobPath(output, digest))
	if err != nil || digestOf(indexBytes) != digest {
		t.Fatalf("merged index blob missing or does not hash to %s", digest)
	}
	var merged imageIndex
	if err := json.Unmarshal(indexBytes, &merged); err != nil {
		t.Fatal(err)
	}
	if merged.SchemaVersion != 2 || merged.MediaType != mediaTypeImageIndex {
		t.Fatalf("merged index header = %d %q", merged.SchemaVersion, merged.MediaType)
	}
	want := append(append([]json.RawMessage{}, amd64Image.entries...), arm64Image.entries...)
	if len(merged.Manifests) != len(want) {
		t.Fatalf("merged index has %d entries, want %d", len(merged.Manifests), len(want))
	}
	for i := range want {
		if !bytes.Equal(merged.Manifests[i], want[i]) {
			t.Fatalf("entry %d = %s, want the input descriptor %s unchanged", i, merged.Manifests[i], want[i])
		}
	}
	var attestation descriptor
	if err := json.Unmarshal(merged.Manifests[3], &attestation); err != nil {
		t.Fatal(err)
	}
	if attestation.Annotations[referenceDigestAnnotation] != arm64Image.manifest.Digest || attestation.Annotations[referenceTypeAnnotation] != attestationManifestType {
		t.Fatalf("arm64 attestation lost its reference annotations: %v", attestation.Annotations)
	}

	// Every input blob reachable from either platform is present, and the
	// per-platform inner indexes (not reachable from the merged index) are not.
	for _, image := range []builtImage{amd64Image, arm64Image} {
		for _, d := range []descriptor{image.manifest, image.attestation} {
			if err := verifyBlob(output, d); err != nil {
				t.Fatalf("merged layout: %v", err)
			}
		}
	}
	entries, err := os.ReadDir(filepath.Join(output, "blobs", "sha256"))
	if err != nil {
		t.Fatal(err)
	}
	// 2 platforms x (manifest, config, layer, attestation manifest,
	// attestation config, statement) + merged index, less the attestation
	// config the platforms share, which is copied once.
	if len(entries) != 12 {
		t.Fatalf("merged layout holds %d blobs, want 12", len(entries))
	}

	stdout.Reset()
	if err := run([]string{"verify", "-layout", output, "-platform", "linux/arm64", "-platform", "linux/amd64"}, &stdout); err != nil {
		t.Fatalf("verify merged layout: %v", err)
	}
	if strings.TrimSpace(stdout.String()) != digest {
		t.Fatalf("verify printed %q, want %s", stdout.String(), digest)
	}
}

func TestMergeIsDeterministic(t *testing.T) {
	amd64, _ := buildxLayout(t, linuxAMD64)
	arm64, _ := buildxLayout(t, linuxARM64)
	inputs := []platformInput{{platform: linuxAMD64, layout: amd64.dir}, {platform: linuxARM64, layout: arm64.dir}}
	first, err := merge(mergeOutput(t), inputs)
	if err != nil {
		t.Fatal(err)
	}
	second, err := merge(mergeOutput(t), inputs)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("merging the same layouts twice gave %s and %s", first, second)
	}
}

func TestMergeAcceptsDirectManifestLayout(t *testing.T) {
	amd64, _ := buildxLayout(t, linuxAMD64)
	arm64 := newFixture(t, "arm64-direct")
	image := arm64.image(linuxARM64)
	manifest := image.manifest
	manifest.Platform = nil
	arm64.writeIndexJSON(manifest)

	output := mergeOutput(t)
	if _, err := merge(output, []platformInput{{platform: linuxAMD64, layout: amd64.dir}, {platform: linuxARM64, layout: arm64.dir}}); err != nil {
		t.Fatalf("merge direct manifest layout: %v", err)
	}
	if _, err := verifyMultiPlatformLayout(output, []platform{linuxAMD64, linuxARM64}); err != nil {
		t.Fatal(err)
	}
}

func TestMergeRejectsUnsafeInputs(t *testing.T) {
	tests := []struct {
		name  string
		setup func(t *testing.T) []platformInput
		want  string
	}{
		{
			name: "layout built for another platform",
			setup: func(t *testing.T) []platformInput {
				amd64, _ := buildxLayout(t, linuxAMD64)
				return []platformInput{{platform: linuxARM64, layout: amd64.dir}}
			},
			want: "layout contains linux/amd64, not linux/arm64",
		},
		{
			name: "descriptor platform disagrees with image config",
			setup: func(t *testing.T) []platformInput {
				f := newFixture(t, "mislabelled")
				image := f.image(linuxAMD64)
				image.manifest.Platform = &linuxARM64
				f.root(f.raw(image.manifest), f.raw(image.attestation))
				return []platformInput{{platform: linuxARM64, layout: f.dir}}
			},
			want: "image config declares linux/amd64",
		},
		{
			name: "duplicate platform flag",
			setup: func(t *testing.T) []platformInput {
				amd64, _ := buildxLayout(t, linuxAMD64)
				return []platformInput{{platform: linuxAMD64, layout: amd64.dir}, {platform: linuxAMD64, layout: amd64.dir}}
			},
			want: "listed more than once",
		},
		{
			name: "corrupted layer",
			setup: func(t *testing.T) []platformInput {
				f, image := buildxLayout(t, linuxAMD64)
				var manifest imageManifest
				data, _ := os.ReadFile(blobPath(f.dir, image.manifest.Digest))
				if err := json.Unmarshal(data, &manifest); err != nil {
					t.Fatal(err)
				}
				layer := manifest.Layers[0]
				corrupt := bytes.Repeat([]byte{'x'}, int(layer.Size))
				if err := os.WriteFile(blobPath(f.dir, layer.Digest), corrupt, 0o644); err != nil {
					t.Fatal(err)
				}
				return []platformInput{{platform: linuxAMD64, layout: f.dir}}
			},
			want: "does not match its descriptor",
		},
		{
			name: "missing attestation blob",
			setup: func(t *testing.T) []platformInput {
				f, image := buildxLayout(t, linuxAMD64)
				if err := os.Remove(blobPath(f.dir, image.attestation.Digest)); err != nil {
					t.Fatal(err)
				}
				return []platformInput{{platform: linuxAMD64, layout: f.dir}}
			},
			want: "is missing",
		},
		{
			name: "attestation for an absent manifest",
			setup: func(t *testing.T) []platformInput {
				f := newFixture(t, "orphan")
				image := f.image(linuxAMD64)
				image.attestation.Annotations[referenceDigestAnnotation] = "sha256:" + strings.Repeat("0", 64)
				f.root(f.raw(image.manifest), f.raw(image.attestation))
				return []platformInput{{platform: linuxAMD64, layout: f.dir}}
			},
			want: "not a platform manifest in this index",
		},
		{
			name: "two platform manifests in one layout",
			setup: func(t *testing.T) []platformInput {
				f := newFixture(t, "two")
				first := f.image(linuxAMD64)
				second := f.image(linuxARM64)
				f.root(append(first.entries, second.entries...)...)
				return []platformInput{{platform: linuxAMD64, layout: f.dir}}
			},
			want: "exactly one platform image, found 2",
		},
		{
			name: "nested index entry",
			setup: func(t *testing.T) []platformInput {
				f := newFixture(t, "nested")
				image := f.image(linuxAMD64)
				nested := f.jsonBlob(mediaTypeImageIndex, imageIndex{SchemaVersion: 2, Manifests: image.entries})
				f.root(f.raw(nested))
				return []platformInput{{platform: linuxAMD64, layout: f.dir}}
			},
			want: "unsupported media type",
		},
		{
			name: "index.json with two descriptors",
			setup: func(t *testing.T) []platformInput {
				f, image := buildxLayout(t, linuxAMD64)
				data, _ := json.Marshal(imageIndex{SchemaVersion: 2, Manifests: []json.RawMessage{f.raw(image.manifest), f.raw(image.attestation)}})
				f.writeFile("index.json", data)
				return []platformInput{{platform: linuxAMD64, layout: f.dir}}
			},
			want: "exactly one descriptor",
		},
		{
			name: "wrong layout version",
			setup: func(t *testing.T) []platformInput {
				f, _ := buildxLayout(t, linuxAMD64)
				f.writeFile("oci-layout", []byte(`{"imageLayoutVersion":"2.0.0"}`))
				return []platformInput{{platform: linuxAMD64, layout: f.dir}}
			},
			want: "imageLayoutVersion 1.0.0",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			inputs := test.setup(t)
			output := mergeOutput(t)
			_, err := merge(output, inputs)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("merge error = %v, want %q", err, test.want)
			}
			if _, statErr := os.Lstat(output); !os.IsNotExist(statErr) {
				t.Fatal("failed merge left an output layout behind")
			}
		})
	}
}

func TestMergeRefusesExistingOutput(t *testing.T) {
	amd64, _ := buildxLayout(t, linuxAMD64)
	output := mergeOutput(t)
	if err := os.Mkdir(output, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := merge(output, []platformInput{{platform: linuxAMD64, layout: amd64.dir}}); err == nil {
		t.Fatal("merge overwrote an existing directory")
	}
	if _, err := os.Lstat(output); err != nil {
		t.Fatal("merge removed a directory it did not create")
	}
}

func TestVerifyRejectsWrongPlatformSet(t *testing.T) {
	amd64, _ := buildxLayout(t, linuxAMD64)
	arm64, _ := buildxLayout(t, linuxARM64)
	output := mergeOutput(t)
	if _, err := merge(output, []platformInput{{platform: linuxAMD64, layout: amd64.dir}, {platform: linuxARM64, layout: arm64.dir}}); err != nil {
		t.Fatal(err)
	}
	for _, expected := range [][]platform{
		{linuxAMD64},
		{linuxAMD64, linuxARM64, {OS: "linux", Architecture: "s390x"}},
		{linuxAMD64, {OS: "linux", Architecture: "arm64", Variant: "v7"}},
	} {
		if _, err := verifyMultiPlatformLayout(output, expected); err == nil {
			t.Fatalf("verify accepted platforms %v", expected)
		}
	}
	// arm64/v8 is the same platform as arm64.
	if _, err := verifyMultiPlatformLayout(output, []platform{linuxAMD64, {OS: "linux", Architecture: "arm64", Variant: "v8"}}); err != nil {
		t.Fatalf("verify rejected linux/arm64/v8: %v", err)
	}
	// A single-platform Buildx layout is not the merged multi-platform index.
	if _, err := verifyMultiPlatformLayout(amd64.dir, []platform{linuxAMD64, linuxARM64}); err == nil {
		t.Fatal("verify accepted a single-platform layout")
	}
}

func TestConfigDigestBindsTheScannedPlatformImage(t *testing.T) {
	f, image := buildxLayout(t, linuxARM64)
	data, err := os.ReadFile(blobPath(f.dir, image.manifest.Digest))
	if err != nil {
		t.Fatal(err)
	}
	var manifest imageManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatal(err)
	}
	var stdout bytes.Buffer
	if err := run([]string{"config-digest", "-layout", f.dir, "-platform", "linux/arm64"}, &stdout); err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(stdout.String()); got != manifest.Config.Digest {
		t.Fatalf("config-digest = %s, want the platform manifest config %s", got, manifest.Config.Digest)
	}
	if err := run([]string{"config-digest", "-layout", f.dir, "-platform", "linux/amd64"}, &bytes.Buffer{}); err == nil {
		t.Fatal("config-digest accepted a layout for another platform")
	}
}

func TestMergeAcceptsARM64V8ImageConfig(t *testing.T) {
	amd64, _ := buildxLayout(t, linuxAMD64)
	f, _ := buildxLayout(t, platform{OS: "linux", Architecture: "arm64", Variant: "v8"})
	if _, err := merge(mergeOutput(t), []platformInput{{platform: linuxAMD64, layout: amd64.dir}, {platform: linuxARM64, layout: f.dir}}); err != nil {
		t.Fatalf("merge rejected an arm64/v8 image: %v", err)
	}
}

func TestRunRejectsMalformedArguments(t *testing.T) {
	for _, args := range [][]string{
		nil,
		{"push"},
		{"merge", "-output", "x"},
		{"merge", "-platform", "linux/amd64=x"},
		{"merge", "-output", "x", "-platform", "linux/amd64"},
		{"merge", "-output", "x", "-platform", "linux=x"},
		{"verify", "-layout", "x"},
		{"verify", "-platform", "linux/amd64"},
		{"verify", "-layout", "x", "-platform", "linux/amd64", "extra"},
		{"config-digest", "-layout", "x"},
		{"config-digest", "-layout", "x", "-platform", "linux/amd64", "-platform", "linux/arm64"},
	} {
		if err := run(args, &bytes.Buffer{}); err == nil {
			t.Fatalf("run(%q) succeeded", args)
		}
	}
}
