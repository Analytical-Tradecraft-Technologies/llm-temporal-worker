// Package contractkit is the consumer-side test kit for the v1 wire
// contract (#1575). It carries the published v1 JSON Schemas and the shared
// v1 fixture corpus, embedded from golang/api/schema/v1 and
// golang/llm/testdata/v1, and checks bytes a caller's own codec emits:
// Validate and Check judge one encoding, and RoundTrip runs a codec over
// every fixture. The same kit is released as a versioned tarball whose
// contract-kit.json manifest, with the schema digest, is recorded in the
// release evidence. docs/testing/contract-kit.md is the consumer guide.
package contractkit

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"sort"
	"strings"

	schemav1 "github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/api/schema/v1"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

const (
	// APIVersion is the wire contract version the kit describes.
	APIVersion = "v1"
	// SchemaDir and FixtureDir are the kit-relative directories of the
	// schemas and fixtures, in the tarball and in contract-kit.json.
	SchemaDir  = "schema/v1"
	FixtureDir = "fixtures/v1"
	// ManifestName is the manifest file at the kit root.
	ManifestName = "contract-kit.json"

	// NegativeManifestName classifies every negative fixture, and
	// MediaURLPolicyName lists the shared remote-media URL cases.
	NegativeManifestName = "negative-fixtures.tsv"
	MediaURLPolicyName   = "media-url-policy.tsv"

	// The repository directories the kit is published from.
	repositorySchemaDir  = "golang/api/schema/v1"
	repositoryFixtureDir = "golang/llm/testdata/v1"

	maxKitFileBytes = 4 << 20
	maxKitFiles     = 512
)

// Kit is one immutable revision of the v1 contract kit.
type Kit struct {
	files     map[string][]byte // kit-relative path -> bytes
	manifest  Manifest
	fixtures  []Fixture
	negatives map[string]NegativeFixture
	schemas   *schemaSet
}

// Manifest describes a kit revision. The same document is the kit's
// contract-kit.json and the contract_kit artifact of the release evidence,
// so its digests are what a release records and a consumer verifies.
type Manifest struct {
	SchemaVersion  int            `json:"schema_version"`
	Kind           string         `json:"kind"`
	Status         string         `json:"status"`
	APIVersion     string         `json:"api_version"`
	SchemaDigest   string         `json:"schema_digest"`
	FixturesDigest string         `json:"fixtures_digest"`
	Files          []ManifestFile `json:"files"`
	Redacted       bool           `json:"redacted"`
}

// ManifestFile is one kit file, by kit-relative path.
type ManifestFile struct {
	Path   string `json:"path"`
	Bytes  int    `json:"bytes"`
	SHA256 string `json:"sha256"`
}

// Embedded returns the kit compiled into this module version.
func Embedded() (*Kit, error) {
	files := map[string][]byte{}
	if err := addDirectory(files, schemav1.FS(), ".", SchemaDir, isSchemaFile); err != nil {
		return nil, err
	}
	if err := addDirectory(files, llm.V1ContractFixtures(), ".", FixtureDir, isFixtureFile); err != nil {
		return nil, err
	}
	return newKit(files)
}

// Load reads a kit laid out as the release tarball is: schema/v1,
// fixtures/v1 and contract-kit.json at the root of fsys. The manifest is
// required and must describe exactly the files present, so a downloaded
// kit that was altered or truncated is rejected.
func Load(fsys fs.FS) (*Kit, error) {
	files := map[string][]byte{}
	if err := addDirectory(files, fsys, SchemaDir, SchemaDir, isSchemaFile); err != nil {
		return nil, err
	}
	if err := addDirectory(files, fsys, FixtureDir, FixtureDir, isFixtureFile); err != nil {
		return nil, err
	}
	kit, err := newKit(files)
	if err != nil {
		return nil, err
	}
	data, err := readRegularFile(fsys, ManifestName)
	if err != nil {
		return nil, fmt.Errorf("contract kit has no readable %s: %w", ManifestName, err)
	}
	recorded, err := ParseManifest(data)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", ManifestName, err)
	}
	if err := kit.manifest.Compare(recorded); err != nil {
		return nil, fmt.Errorf("%s does not describe the kit files: %w", ManifestName, err)
	}
	return kit, nil
}

// FromRepository reads the kit from a repository checkout or from the
// golang/api/schema/v1 and golang/llm/testdata/v1 trees of one, such as the
// tree a release extracts from its tag commit.
func FromRepository(fsys fs.FS) (*Kit, error) {
	files := map[string][]byte{}
	if err := addDirectory(files, fsys, repositorySchemaDir, SchemaDir, isSchemaFile); err != nil {
		return nil, err
	}
	if err := addDirectory(files, fsys, repositoryFixtureDir, FixtureDir, isFixtureFile); err != nil {
		return nil, err
	}
	return newKit(files)
}

// ParseManifest decodes a contract-kit.json document and checks its own
// consistency: fixed fields, ordered file entries and matching digests.
func ParseManifest(data []byte) (Manifest, error) {
	var manifest Manifest
	if err := decodeStrict(data, &manifest); err != nil {
		return Manifest{}, fmt.Errorf("not a contract kit manifest: %w", err)
	}
	if err := manifest.validate(); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

// Manifest returns the kit manifest.
func (kit *Kit) Manifest() Manifest {
	manifest := kit.manifest
	manifest.Files = append([]ManifestFile(nil), kit.manifest.Files...)
	return manifest
}

// ManifestJSON returns contract-kit.json exactly as the kit writes it.
func (kit *Kit) ManifestJSON() []byte {
	data, err := json.MarshalIndent(kit.manifest, "", "  ")
	if err != nil {
		panic(err) // unreachable: the manifest has only plain fields
	}
	return append(data, '\n')
}

// SchemaDigest identifies the schema revision: the sha256 of the
// sha256sum-format listing of every schema/v1 file in path order.
func (kit *Kit) SchemaDigest() string { return kit.manifest.SchemaDigest }

// Paths returns every kit-relative file path in order.
func (kit *Kit) Paths() []string {
	paths := make([]string, 0, len(kit.files))
	for _, file := range kit.manifest.Files {
		paths = append(paths, file.Path)
	}
	return paths
}

// File returns a copy of the kit file at the kit-relative path.
func (kit *Kit) File(name string) ([]byte, bool) {
	data, ok := kit.files[name]
	return bytes.Clone(data), ok
}

// Compare reports how other differs from manifest. Only the content
// identity matters: the API version, both digests and every file.
func (manifest Manifest) Compare(other Manifest) error {
	if err := other.validate(); err != nil {
		return err
	}
	if manifest.APIVersion != other.APIVersion {
		return fmt.Errorf("api_version %q, want %q", other.APIVersion, manifest.APIVersion)
	}
	if manifest.SchemaDigest != other.SchemaDigest {
		return fmt.Errorf("schema_digest %s, want %s", other.SchemaDigest, manifest.SchemaDigest)
	}
	if manifest.FixturesDigest != other.FixturesDigest {
		return fmt.Errorf("fixtures_digest %s, want %s", other.FixturesDigest, manifest.FixturesDigest)
	}
	if len(manifest.Files) != len(other.Files) {
		return fmt.Errorf("lists %d files, want %d", len(other.Files), len(manifest.Files))
	}
	for index, file := range manifest.Files {
		if other.Files[index] != file {
			return fmt.Errorf("file entry %q differs from %q", other.Files[index].Path, file.Path)
		}
	}
	return nil
}

// validate checks a decoded manifest's own consistency: its fixed fields,
// ordered unique paths, and digests recomputed from its file entries.
func (manifest Manifest) validate() error {
	if manifest.SchemaVersion != 1 || manifest.Kind != "contract_kit" || manifest.Status != "pass" || !manifest.Redacted {
		return errors.New("manifest must be a schema_version 1 contract_kit summary with status pass")
	}
	if manifest.APIVersion != APIVersion {
		return fmt.Errorf("manifest api_version %q is not %q", manifest.APIVersion, APIVersion)
	}
	if len(manifest.Files) == 0 || len(manifest.Files) > maxKitFiles {
		return errors.New("manifest must list between one and 512 files")
	}
	for index, file := range manifest.Files {
		if index > 0 && manifest.Files[index-1].Path >= file.Path {
			return errors.New("manifest files must be unique and in path order")
		}
		directory, name := path.Split(file.Path)
		valid := (directory == SchemaDir+"/" && isSchemaFile(name)) || (directory == FixtureDir+"/" && isFixtureFile(name))
		if !valid || file.Bytes <= 0 || file.Bytes > maxKitFileBytes || !isHexDigest(file.SHA256) {
			return fmt.Errorf("manifest file entry %q is invalid", file.Path)
		}
	}
	schemaDigest, fixturesDigest := manifestDigests(manifest.Files)
	if manifest.SchemaDigest != schemaDigest || manifest.FixturesDigest != fixturesDigest {
		return errors.New("manifest digests do not match its file entries")
	}
	return nil
}

func newKit(files map[string][]byte) (*Kit, error) {
	if len(files) > maxKitFiles {
		return nil, errors.New("contract kit has too many files")
	}
	kit := &Kit{files: files}
	paths := make([]string, 0, len(files))
	for name := range files {
		paths = append(paths, name)
	}
	sort.Strings(paths)
	entries := make([]ManifestFile, 0, len(paths))
	for _, name := range paths {
		sum := sha256.Sum256(files[name])
		entries = append(entries, ManifestFile{Path: name, Bytes: len(files[name]), SHA256: hex.EncodeToString(sum[:])})
	}
	schemaDigest, fixturesDigest := manifestDigests(entries)
	kit.manifest = Manifest{
		SchemaVersion:  1,
		Kind:           "contract_kit",
		Status:         "pass",
		APIVersion:     APIVersion,
		SchemaDigest:   schemaDigest,
		FixturesDigest: fixturesDigest,
		Files:          entries,
		Redacted:       true,
	}
	if err := kit.manifest.validate(); err != nil {
		return nil, err
	}
	schemas, err := compileSchemas(files)
	if err != nil {
		return nil, err
	}
	kit.schemas = schemas
	if err := kit.classifyFixtures(); err != nil {
		return nil, err
	}
	return kit, nil
}

// manifestDigests hashes the sha256sum-format listing ("<hex>  <path>\n")
// of each directory's files in path order, so a consumer can recompute a
// digest with sha256sum alone; docs/testing/contract-kit.md shows how.
func manifestDigests(files []ManifestFile) (schemaDigest, fixturesDigest string) {
	schemaListing, fixtureListing := sha256.New(), sha256.New()
	for _, file := range files {
		line := file.SHA256 + "  " + file.Path + "\n"
		switch {
		case strings.HasPrefix(file.Path, SchemaDir+"/"):
			schemaListing.Write([]byte(line))
		case strings.HasPrefix(file.Path, FixtureDir+"/"):
			fixtureListing.Write([]byte(line))
		}
	}
	return "sha256:" + hex.EncodeToString(schemaListing.Sum(nil)), "sha256:" + hex.EncodeToString(fixtureListing.Sum(nil))
}

// addDirectory copies the regular files of one flat source directory into
// files under target. Anything else, including a nested directory, a
// symbolic link or a file the filter does not accept, fails the kit.
func addDirectory(files map[string][]byte, fsys fs.FS, source, target string, accept func(string) bool) error {
	entries, err := fs.ReadDir(fsys, source)
	if err != nil {
		return fmt.Errorf("contract kit directory %s: %w", target, err)
	}
	found := false
	for _, entry := range entries {
		name := entry.Name()
		if source == repositorySchemaDir && entry.Type().IsRegular() && strings.HasSuffix(name, ".go") {
			continue // the Go package that embeds the schemas
		}
		if !entry.Type().IsRegular() || !accept(name) {
			return fmt.Errorf("contract kit directory %s holds unexpected entry %q", target, name)
		}
		data, err := readRegularFile(fsys, path.Join(source, name))
		if err != nil {
			return err
		}
		files[path.Join(target, name)] = data
		found = true
	}
	if !found {
		return fmt.Errorf("contract kit directory %s is empty", target)
	}
	return nil
}

func readRegularFile(fsys fs.FS, name string) ([]byte, error) {
	info, err := fs.Stat(fsys, name)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > maxKitFileBytes {
		return nil, fmt.Errorf("contract kit file %s is not a regular file of 1 byte to 4 MiB", name)
	}
	return fs.ReadFile(fsys, name)
}

func isSchemaFile(name string) bool {
	return strings.HasSuffix(name, ".schema.json") && safeName(name)
}

func isFixtureFile(name string) bool {
	return (strings.HasSuffix(name, ".json") || strings.HasSuffix(name, ".tsv")) && safeName(name)
}

func safeName(name string) bool {
	if name == "" || len(name) > 128 || strings.HasPrefix(name, ".") {
		return false
	}
	for _, character := range name {
		if !(character >= 'a' && character <= 'z' || character >= '0' && character <= '9' || character == '-' || character == '.') {
			return false
		}
	}
	return true
}

func isHexDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil && strings.ToLower(value) == value
}

func decodeStrict(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if decoder.More() {
		return errors.New("trailing data")
	}
	return nil
}
