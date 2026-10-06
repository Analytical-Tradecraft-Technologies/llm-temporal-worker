// Command ocimerge assembles single-platform OCI image layouts into one
// multi-platform OCI image layout, and verifies such a layout, without
// contacting a registry.
//
// Docker Build Cloud is a multi-node builder (one node per platform), and
// Buildx cannot export a multi-platform OCI archive from a multi-node builder.
// CI therefore builds each platform separately to its own OCI layout, scans
// each layout, and uses this command to write the image index that a single
// multi-platform build would have produced: every platform image manifest
// (with its platform) followed by its Buildx attestation manifests, which
// keep their vnd.docker.reference.* annotations. The merged layout holds only
// blobs reachable from that index, each copied after its digest and size are
// checked.
//
// Usage:
//
//	ocimerge merge -output DIR -platform linux/amd64=DIR -platform linux/arm64=DIR
//	ocimerge verify -layout DIR -platform linux/amd64 -platform linux/arm64
//	ocimerge config-digest -layout DIR -platform linux/amd64
//
// merge and verify print the merged image index digest. config-digest
// validates a single-platform layout and prints its image config digest, which
// CI compares with the image ID in that platform's vulnerability scan so the
// scan is bound to the exact bytes that are later published.
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
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

const (
	mediaTypeImageIndex    = "application/vnd.oci.image.index.v1+json"
	mediaTypeImageManifest = "application/vnd.oci.image.manifest.v1+json"
	ociLayoutVersion       = "1.0.0"

	// Buildx marks attestation manifests in an image index with these
	// annotations; registries and clients use them to link an attestation
	// to the platform manifest it describes.
	referenceTypeAnnotation   = "vnd.docker.reference.type"
	referenceDigestAnnotation = "vnd.docker.reference.digest"
	attestationManifestType   = "attestation-manifest"

	// Index, manifest, and config documents are small JSON payloads. Layers
	// are streamed and never held in memory.
	maxJSONDocumentSize = 4 << 20
)

var sha256Digest = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

type descriptor struct {
	MediaType   string            `json:"mediaType"`
	Digest      string            `json:"digest"`
	Size        int64             `json:"size"`
	Platform    *platform         `json:"platform,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
}

type platform struct {
	Architecture string `json:"architecture"`
	OS           string `json:"os"`
	Variant      string `json:"variant,omitempty"`
}

// normalized treats linux/arm64/v8 and linux/arm64 as the same platform, as
// containerd and BuildKit do; v8 is the only arm64 variant.
func (p platform) normalized() platform {
	if p.Architecture == "arm64" && p.Variant == "v8" {
		p.Variant = ""
	}
	return p
}

func (p platform) String() string {
	if p.Variant != "" {
		return p.OS + "/" + p.Architecture + "/" + p.Variant
	}
	return p.OS + "/" + p.Architecture
}

type imageIndex struct {
	SchemaVersion int               `json:"schemaVersion"`
	MediaType     string            `json:"mediaType,omitempty"`
	Manifests     []json.RawMessage `json:"manifests"`
}

type imageManifest struct {
	SchemaVersion int          `json:"schemaVersion"`
	MediaType     string       `json:"mediaType,omitempty"`
	Config        descriptor   `json:"config"`
	Layers        []descriptor `json:"layers"`
}

type imageConfig struct {
	Architecture string `json:"architecture"`
	OS           string `json:"os"`
	Variant      string `json:"variant,omitempty"`
}

type layoutMetadata struct {
	ImageLayoutVersion string `json:"imageLayoutVersion"`
}

// platformImage is one platform manifest plus the attestation manifests that
// reference it, together with every blob reachable from them.
type platformImage struct {
	platform platform
	// descriptors holds the exact index entries: the platform manifest
	// first, then its attestation manifests in their original order.
	descriptors  []json.RawMessage
	blobs        []descriptor
	configDigest string
}

type platformInput struct {
	platform platform
	layout   string
}

type platformInputs []platformInput

func (inputs *platformInputs) String() string { return "" }

func (inputs *platformInputs) Set(value string) error {
	name, layout, ok := strings.Cut(value, "=")
	if !ok || layout == "" {
		return fmt.Errorf("platform layout %q must be os/arch=DIR", value)
	}
	parsed, err := parsePlatform(name)
	if err != nil {
		return err
	}
	*inputs = append(*inputs, platformInput{platform: parsed, layout: layout})
	return nil
}

type platformNames []platform

func (names *platformNames) String() string { return "" }

func (names *platformNames) Set(value string) error {
	parsed, err := parsePlatform(value)
	if err != nil {
		return err
	}
	*names = append(*names, parsed)
	return nil
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "ocimerge:", err)
		os.Exit(1)
	}
}

func run(args []string, stdout io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: ocimerge merge|verify|config-digest [flags]")
	}
	switch args[0] {
	case "merge":
		return runMerge(args[1:], stdout)
	case "verify":
		return runVerify(args[1:], stdout)
	case "config-digest":
		return runConfigDigest(args[1:], stdout)
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func runMerge(args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("merge", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	output := flags.String("output", "", "new OCI layout directory to create")
	var inputs platformInputs
	flags.Var(&inputs, "platform", "os/arch=DIR single-platform OCI layout; repeat for each platform")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *output == "" || len(inputs) == 0 {
		return errors.New("merge requires -output and at least one -platform os/arch=DIR")
	}
	digest, err := merge(*output, inputs)
	if err != nil {
		return err
	}
	fmt.Fprintln(stdout, digest)
	return nil
}

func runVerify(args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("verify", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	layout := flags.String("layout", "", "multi-platform OCI layout directory")
	var platforms platformNames
	flags.Var(&platforms, "platform", "os/arch every platform the index must contain; repeat for each platform")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *layout == "" || len(platforms) == 0 {
		return errors.New("verify requires -layout and at least one -platform os/arch")
	}
	digest, err := verifyMultiPlatformLayout(*layout, platforms)
	if err != nil {
		return err
	}
	fmt.Fprintln(stdout, digest)
	return nil
}

func runConfigDigest(args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("config-digest", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	layout := flags.String("layout", "", "single-platform OCI layout directory")
	var platforms platformNames
	flags.Var(&platforms, "platform", "os/arch the layout must contain")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *layout == "" || len(platforms) != 1 {
		return errors.New("config-digest requires -layout and exactly one -platform os/arch")
	}
	image, err := loadSinglePlatformLayout(*layout, platforms[0])
	if err != nil {
		return err
	}
	fmt.Fprintln(stdout, image.configDigest)
	return nil
}

func parsePlatform(value string) (platform, error) {
	parts := strings.Split(value, "/")
	if len(parts) < 2 || len(parts) > 3 {
		return platform{}, fmt.Errorf("platform %q must be os/arch or os/arch/variant", value)
	}
	for _, part := range parts {
		if part == "" || strings.ContainsAny(part, " \t=") {
			return platform{}, fmt.Errorf("platform %q must be os/arch or os/arch/variant", value)
		}
	}
	parsed := platform{OS: parts[0], Architecture: parts[1]}
	if len(parts) == 3 {
		parsed.Variant = parts[2]
	}
	return parsed.normalized(), nil
}

// merge writes a new OCI layout at output whose single index.json entry is an
// image index over every input platform, and returns that index's digest. The
// output directory must not already exist; it is removed again on failure.
func merge(output string, inputs []platformInput) (digest string, err error) {
	seen := make(map[string]struct{}, len(inputs))
	images := make([]platformImage, 0, len(inputs))
	for _, input := range inputs {
		key := input.platform.String()
		if _, duplicate := seen[key]; duplicate {
			return "", fmt.Errorf("platform %s is listed more than once", key)
		}
		seen[key] = struct{}{}
		image, err := loadSinglePlatformLayout(input.layout, input.platform)
		if err != nil {
			return "", fmt.Errorf("%s layout %s: %w", key, input.layout, err)
		}
		images = append(images, image)
	}

	if err := os.Mkdir(output, 0o755); err != nil {
		return "", fmt.Errorf("create output layout: %w", err)
	}
	defer func() {
		if err != nil {
			_ = os.RemoveAll(output)
		}
	}()
	if err := os.MkdirAll(filepath.Join(output, "blobs", "sha256"), 0o755); err != nil {
		return "", err
	}

	merged := imageIndex{SchemaVersion: 2, MediaType: mediaTypeImageIndex, Manifests: []json.RawMessage{}}
	for i, image := range images {
		for _, blob := range image.blobs {
			if err := copyBlob(inputs[i].layout, output, blob); err != nil {
				return "", fmt.Errorf("%s: %w", image.platform, err)
			}
		}
		merged.Manifests = append(merged.Manifests, image.descriptors...)
	}
	indexBytes, err := json.Marshal(merged)
	if err != nil {
		return "", err
	}
	indexDescriptor := descriptor{MediaType: mediaTypeImageIndex, Digest: digestOf(indexBytes), Size: int64(len(indexBytes))}
	if err := writeNewFile(blobPath(output, indexDescriptor.Digest), indexBytes); err != nil {
		return "", err
	}
	top, err := json.Marshal(struct {
		SchemaVersion int          `json:"schemaVersion"`
		MediaType     string       `json:"mediaType"`
		Manifests     []descriptor `json:"manifests"`
	}{SchemaVersion: 2, MediaType: mediaTypeImageIndex, Manifests: []descriptor{indexDescriptor}})
	if err != nil {
		return "", err
	}
	if err := writeNewFile(filepath.Join(output, "index.json"), top); err != nil {
		return "", err
	}
	layout, err := json.Marshal(layoutMetadata{ImageLayoutVersion: ociLayoutVersion})
	if err != nil {
		return "", err
	}
	if err := writeNewFile(filepath.Join(output, "oci-layout"), layout); err != nil {
		return "", err
	}

	expected := make([]platform, 0, len(inputs))
	for _, input := range inputs {
		expected = append(expected, input.platform)
	}
	verified, err := verifyMultiPlatformLayout(output, expected)
	if err != nil {
		return "", fmt.Errorf("merged layout failed verification: %w", err)
	}
	if verified != indexDescriptor.Digest {
		return "", errors.New("merged layout index digest changed during verification")
	}
	return verified, nil
}

// loadSinglePlatformLayout reads a Buildx single-platform OCI layout: an
// index.json with one descriptor that is either an image index holding one
// platform manifest and its attestations, or the platform manifest itself.
func loadSinglePlatformLayout(dir string, want platform) (platformImage, error) {
	top, err := readLayoutRoot(dir)
	if err != nil {
		return platformImage{}, err
	}
	var entries []json.RawMessage
	switch top.MediaType {
	case mediaTypeImageIndex:
		index, err := readIndexBlob(dir, top)
		if err != nil {
			return platformImage{}, err
		}
		entries = index.Manifests
	case mediaTypeImageManifest:
		// Without attestations Buildx may reference the manifest directly.
		// Record the platform from the image config, which is checked
		// against the requested platform below.
		manifest, err := readManifestBlob(dir, top)
		if err != nil {
			return platformImage{}, err
		}
		config, err := readConfigBlob(dir, manifest.Config)
		if err != nil {
			return platformImage{}, err
		}
		entry := descriptor{MediaType: top.MediaType, Digest: top.Digest, Size: top.Size, Platform: &platform{OS: config.OS, Architecture: config.Architecture, Variant: config.Variant}}
		raw, err := json.Marshal(entry)
		if err != nil {
			return platformImage{}, err
		}
		entries = []json.RawMessage{raw}
	default:
		return platformImage{}, fmt.Errorf("index.json references unsupported media type %q", top.MediaType)
	}
	images, err := readPlatformImages(dir, entries)
	if err != nil {
		return platformImage{}, err
	}
	if len(images) != 1 {
		return platformImage{}, fmt.Errorf("layout must contain exactly one platform image, found %d", len(images))
	}
	if images[0].platform != want.normalized() {
		return platformImage{}, fmt.Errorf("layout contains %s, not %s", images[0].platform, want)
	}
	return images[0], nil
}

// verifyMultiPlatformLayout checks that a layout's single index.json entry is
// an image index containing exactly the expected platforms, each with only
// attestations that reference it, and that every reachable blob is present
// with the recorded digest and size. It returns the image index digest.
func verifyMultiPlatformLayout(dir string, expected []platform) (string, error) {
	top, err := readLayoutRoot(dir)
	if err != nil {
		return "", err
	}
	if top.MediaType != mediaTypeImageIndex {
		return "", fmt.Errorf("index.json must reference an image index, not %q", top.MediaType)
	}
	index, err := readIndexBlob(dir, top)
	if err != nil {
		return "", err
	}
	images, err := readPlatformImages(dir, index.Manifests)
	if err != nil {
		return "", err
	}
	got := make(map[platform]struct{}, len(images))
	for _, image := range images {
		got[image.platform] = struct{}{}
	}
	want := make(map[platform]struct{}, len(expected))
	for _, p := range expected {
		want[p.normalized()] = struct{}{}
	}
	if len(got) != len(want) {
		return "", fmt.Errorf("image index has %d platforms, want %d", len(got), len(want))
	}
	for p := range want {
		if _, ok := got[p]; !ok {
			return "", fmt.Errorf("image index lacks platform %s", p)
		}
	}
	return top.Digest, nil
}

func readLayoutRoot(dir string) (descriptor, error) {
	info, err := os.Lstat(dir)
	if err != nil || !info.IsDir() {
		return descriptor{}, errors.New("OCI layout must be an existing directory")
	}
	layoutBytes, err := readSmallFile(filepath.Join(dir, "oci-layout"))
	if err != nil {
		return descriptor{}, fmt.Errorf("read oci-layout: %w", err)
	}
	var layout layoutMetadata
	if err := json.Unmarshal(layoutBytes, &layout); err != nil || layout.ImageLayoutVersion != ociLayoutVersion {
		return descriptor{}, errors.New("oci-layout must declare imageLayoutVersion 1.0.0")
	}
	indexBytes, err := readSmallFile(filepath.Join(dir, "index.json"))
	if err != nil {
		return descriptor{}, fmt.Errorf("read index.json: %w", err)
	}
	var index imageIndex
	if err := json.Unmarshal(indexBytes, &index); err != nil || index.SchemaVersion != 2 {
		return descriptor{}, errors.New("index.json is not a schemaVersion 2 image index")
	}
	if len(index.Manifests) != 1 {
		return descriptor{}, fmt.Errorf("index.json must contain exactly one descriptor, found %d", len(index.Manifests))
	}
	var top descriptor
	if err := json.Unmarshal(index.Manifests[0], &top); err != nil {
		return descriptor{}, errors.New("index.json descriptor is invalid")
	}
	if err := checkDescriptor(top); err != nil {
		return descriptor{}, fmt.Errorf("index.json descriptor: %w", err)
	}
	return top, nil
}

// readPlatformImages groups index entries into platform images. Every entry
// must be an image manifest: either a platform manifest (with a platform that
// matches its image config) or a Buildx attestation manifest that references
// a platform manifest in the same index. Anything else fails closed.
func readPlatformImages(dir string, entries []json.RawMessage) ([]platformImage, error) {
	var images []platformImage
	byDigest := make(map[string]int)
	type attestation struct {
		raw        json.RawMessage
		descriptor descriptor
	}
	var attestations []attestation
	for _, raw := range entries {
		var entry descriptor
		if err := json.Unmarshal(raw, &entry); err != nil {
			return nil, errors.New("image index entry is invalid")
		}
		if err := checkDescriptor(entry); err != nil {
			return nil, fmt.Errorf("image index entry: %w", err)
		}
		if entry.MediaType != mediaTypeImageManifest {
			return nil, fmt.Errorf("image index entry %s has unsupported media type %q", entry.Digest, entry.MediaType)
		}
		if entry.Annotations[referenceTypeAnnotation] == attestationManifestType {
			attestations = append(attestations, attestation{raw: raw, descriptor: entry})
			continue
		}
		if entry.Platform == nil || entry.Platform.OS == "" || entry.Platform.Architecture == "" {
			return nil, fmt.Errorf("image manifest %s has no platform", entry.Digest)
		}
		if _, duplicate := byDigest[entry.Digest]; duplicate {
			return nil, fmt.Errorf("image manifest %s is listed more than once", entry.Digest)
		}
		entryPlatform := entry.Platform.normalized()
		for _, image := range images {
			if image.platform == entryPlatform {
				return nil, fmt.Errorf("platform %s is listed more than once", image.platform)
			}
		}
		blobs, err := manifestBlobs(dir, entry, &entryPlatform)
		if err != nil {
			return nil, err
		}
		byDigest[entry.Digest] = len(images)
		images = append(images, platformImage{platform: entryPlatform, descriptors: []json.RawMessage{raw}, blobs: blobs, configDigest: blobs[1].Digest})
	}
	for _, item := range attestations {
		subject := item.descriptor.Annotations[referenceDigestAnnotation]
		position, ok := byDigest[subject]
		if !ok {
			return nil, fmt.Errorf("attestation manifest %s references %q, which is not a platform manifest in this index", item.descriptor.Digest, subject)
		}
		blobs, err := manifestBlobs(dir, item.descriptor, nil)
		if err != nil {
			return nil, err
		}
		images[position].descriptors = append(images[position].descriptors, item.raw)
		images[position].blobs = append(images[position].blobs, blobs...)
	}
	if len(images) == 0 {
		return nil, errors.New("image index contains no platform manifest")
	}
	return images, nil
}

// manifestBlobs verifies a manifest, its config, and its layers, and returns
// their descriptors. When want is set, the image config must declare that
// platform.
func manifestBlobs(dir string, manifestDescriptor descriptor, want *platform) ([]descriptor, error) {
	manifest, err := readManifestBlob(dir, manifestDescriptor)
	if err != nil {
		return nil, err
	}
	if err := checkDescriptor(manifest.Config); err != nil {
		return nil, fmt.Errorf("manifest %s config: %w", manifestDescriptor.Digest, err)
	}
	if want != nil {
		config, err := readConfigBlob(dir, manifest.Config)
		if err != nil {
			return nil, err
		}
		declared := platform{OS: config.OS, Architecture: config.Architecture, Variant: config.Variant}.normalized()
		if declared != *want {
			return nil, fmt.Errorf("manifest %s is labelled %s but its image config declares %s", manifestDescriptor.Digest, want, declared)
		}
	} else if err := verifyBlob(dir, manifest.Config); err != nil {
		return nil, err
	}
	blobs := []descriptor{manifestDescriptor, manifest.Config}
	for _, layer := range manifest.Layers {
		if err := checkDescriptor(layer); err != nil {
			return nil, fmt.Errorf("manifest %s layer: %w", manifestDescriptor.Digest, err)
		}
		if err := verifyBlob(dir, layer); err != nil {
			return nil, err
		}
		blobs = append(blobs, layer)
	}
	return blobs, nil
}

func readIndexBlob(dir string, d descriptor) (imageIndex, error) {
	data, err := readJSONBlob(dir, d)
	if err != nil {
		return imageIndex{}, err
	}
	var index imageIndex
	if err := json.Unmarshal(data, &index); err != nil || index.SchemaVersion != 2 {
		return imageIndex{}, fmt.Errorf("image index %s is invalid", d.Digest)
	}
	if index.MediaType != "" && index.MediaType != mediaTypeImageIndex {
		return imageIndex{}, fmt.Errorf("image index %s declares media type %q", d.Digest, index.MediaType)
	}
	return index, nil
}

func readManifestBlob(dir string, d descriptor) (imageManifest, error) {
	data, err := readJSONBlob(dir, d)
	if err != nil {
		return imageManifest{}, err
	}
	var manifest imageManifest
	if err := json.Unmarshal(data, &manifest); err != nil || manifest.SchemaVersion != 2 {
		return imageManifest{}, fmt.Errorf("image manifest %s is invalid", d.Digest)
	}
	if manifest.MediaType != "" && manifest.MediaType != mediaTypeImageManifest {
		return imageManifest{}, fmt.Errorf("image manifest %s declares media type %q", d.Digest, manifest.MediaType)
	}
	return manifest, nil
}

func readConfigBlob(dir string, d descriptor) (imageConfig, error) {
	data, err := readJSONBlob(dir, d)
	if err != nil {
		return imageConfig{}, err
	}
	var config imageConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return imageConfig{}, fmt.Errorf("image config %s is invalid", d.Digest)
	}
	return config, nil
}

func checkDescriptor(d descriptor) error {
	if !sha256Digest.MatchString(d.Digest) {
		return fmt.Errorf("digest %q is not a sha256 digest", d.Digest)
	}
	if d.Size < 0 {
		return fmt.Errorf("blob %s has a negative size", d.Digest)
	}
	if d.MediaType == "" {
		return fmt.Errorf("blob %s has no media type", d.Digest)
	}
	return nil
}

func blobPath(dir, digest string) string {
	return filepath.Join(dir, "blobs", "sha256", strings.TrimPrefix(digest, "sha256:"))
}

func readJSONBlob(dir string, d descriptor) ([]byte, error) {
	if d.Size > maxJSONDocumentSize {
		return nil, fmt.Errorf("blob %s is too large for a JSON document", d.Digest)
	}
	file, err := openBlob(dir, d)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, d.Size+1))
	if err != nil {
		return nil, fmt.Errorf("read blob %s: %w", d.Digest, err)
	}
	if int64(len(data)) != d.Size || digestOf(data) != d.Digest {
		return nil, fmt.Errorf("blob %s does not match its descriptor", d.Digest)
	}
	return data, nil
}

func verifyBlob(dir string, d descriptor) error {
	file, err := openBlob(dir, d)
	if err != nil {
		return err
	}
	defer file.Close()
	return hashMatches(file, d)
}

func openBlob(dir string, d descriptor) (*os.File, error) {
	path := blobPath(dir, d.Digest)
	info, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("blob %s is missing", d.Digest)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("blob %s is not a regular file", d.Digest)
	}
	if info.Size() != d.Size {
		return nil, fmt.Errorf("blob %s has size %d, descriptor says %d", d.Digest, info.Size(), d.Size)
	}
	return os.Open(path)
}

func hashMatches(reader io.Reader, d descriptor) error {
	hash := sha256.New()
	written, err := io.Copy(hash, reader)
	if err != nil {
		return fmt.Errorf("read blob %s: %w", d.Digest, err)
	}
	if written != d.Size || "sha256:"+hex.EncodeToString(hash.Sum(nil)) != d.Digest {
		return fmt.Errorf("blob %s does not match its descriptor", d.Digest)
	}
	return nil
}

// copyBlob copies a verified blob into the output layout. Blobs shared by
// several platforms (for example a common base layer) are written once.
func copyBlob(sourceDir, outputDir string, d descriptor) error {
	destination := blobPath(outputDir, d.Digest)
	if _, err := os.Lstat(destination); err == nil {
		return verifyBlob(outputDir, d)
	}
	source, err := openBlob(sourceDir, d)
	if err != nil {
		return err
	}
	defer source.Close()
	target, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	hash := sha256.New()
	written, copyErr := io.Copy(io.MultiWriter(target, hash), source)
	closeErr := target.Close()
	if copyErr != nil || closeErr != nil {
		return fmt.Errorf("copy blob %s: %w", d.Digest, errors.Join(copyErr, closeErr))
	}
	if written != d.Size || "sha256:"+hex.EncodeToString(hash.Sum(nil)) != d.Digest {
		return fmt.Errorf("blob %s changed while it was copied", d.Digest)
	}
	return nil
}

func readSmallFile(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxJSONDocumentSize+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxJSONDocumentSize {
		return nil, errors.New("file is too large")
	}
	return data, nil
}

func writeNewFile(path string, data []byte) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	_, writeErr := io.Copy(file, bytes.NewReader(data))
	return errors.Join(writeErr, file.Close())
}

func digestOf(data []byte) string {
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}
