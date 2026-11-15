package contractkit_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/contractkit"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

// repositoryRoot is the checkout root, relative to this package.
const repositoryRoot = "../.."

func embeddedKit(t *testing.T) *contractkit.Kit {
	t.Helper()
	kit, err := contractkit.Embedded()
	if err != nil {
		t.Fatal(err)
	}
	return kit
}

// goCodec is the worker's own codec as a consumer codec: the kit must
// accept it without exception.
func goCodec(contract contractkit.Contract, _ string, data []byte) ([]byte, error) {
	targets := map[contractkit.Contract]func() any{
		contractkit.GenerateRequest:  func() any { return new(llm.GenerateRequestV1) },
		contractkit.GenerateResponse: func() any { return new(llm.GenerateResponseV1) },
		contractkit.CompactRequest:   func() any { return new(llm.CompactRequestV1) },
		contractkit.CompactResponse:  func() any { return new(llm.CompactResponseV1) },
		contractkit.QueryRequest:     func() any { return new(llm.QueryRequestV1) },
		contractkit.QueryResponse:    func() any { return new(llm.QueryResponseV1) },
	}
	value := targets[contract]()
	if err := json.Unmarshal(data, value); err != nil {
		return nil, err
	}
	return json.Marshal(value)
}

// TestEmbeddedKitIsTheRepositoryContract pins the embedded kit to the
// schema and fixture trees it is published from: the same files, bytes
// and manifest.
func TestEmbeddedKitIsTheRepositoryContract(t *testing.T) {
	embedded := embeddedKit(t)
	repository, err := contractkit.FromRepository(os.DirFS(repositoryRoot))
	if err != nil {
		t.Fatal(err)
	}
	if err := embedded.Manifest().Compare(repository.Manifest()); err != nil {
		t.Fatalf("embedded kit differs from the repository: %v", err)
	}
	schemas, _ := filepath.Glob(filepath.Join(repositoryRoot, "golang", "api", "schema", "v1", "*.schema.json"))
	fixtures, _ := filepath.Glob(filepath.Join(repositoryRoot, "golang", "llm", "testdata", "v1", "*"))
	if len(embedded.Paths()) != len(schemas)+len(fixtures) || len(schemas) == 0 || len(fixtures) == 0 {
		t.Fatalf("kit has %d files, repository has %d schemas and %d fixture files", len(embedded.Paths()), len(schemas), len(fixtures))
	}
	for _, name := range []string{contractkit.NegativeManifestName, contractkit.MediaURLPolicyName} {
		if _, ok := embedded.File(contractkit.FixtureDir + "/" + name); !ok {
			t.Errorf("kit omits the shared manifest %s", name)
		}
	}
	if len(embedded.Contracts()) != len(schemas) {
		t.Fatalf("kit compiled %d schemas, want %d", len(embedded.Contracts()), len(schemas))
	}
}

func TestRoundTripAcceptsTheWorkerCodec(t *testing.T) {
	kit := embeddedKit(t)
	results := kit.RoundTrip(goCodec)
	negatives := 0
	for _, result := range results {
		if result.Negative {
			negatives++
		}
	}
	if len(results) < 50 || negatives == 0 || negatives == len(results) {
		t.Fatalf("round trip covered %d fixtures, %d negative", len(results), negatives)
	}
	for _, failed := range contractkit.Failed(results) {
		t.Errorf("%s: %v", failed.Fixture, failed.Err)
	}
	if len(kit.NegativeManifest()) != negatives {
		t.Fatalf("negative manifest lists %d fixtures, round trip ran %d", len(kit.NegativeManifest()), negatives)
	}
	cases, err := kit.MediaURLPolicy()
	if err != nil || len(cases) == 0 {
		t.Fatalf("media URL policy: %d cases, %v", len(cases), err)
	}
}

// TestRoundTripRejectsDefectiveCodecs shows each way a consumer codec can
// fail the kit: rejecting a valid fixture, accepting a negative one,
// writing schema-invalid bytes, and dropping a field the schema allows to
// be absent.
func TestRoundTripRejectsDefectiveCodecs(t *testing.T) {
	kit := embeddedKit(t)
	for name, test := range map[string]struct {
		codec contractkit.Codec
		want  string
	}{
		"rejects everything": {
			codec: func(contractkit.Contract, string, []byte) ([]byte, error) { return nil, errors.New("no") },
			want:  "codec rejected a valid fixture",
		},
		"accepts everything": {
			codec: func(_ contractkit.Contract, _ string, data []byte) ([]byte, error) { return data, nil },
			want:  "codec accepted a negative fixture",
		},
		"writes invalid bytes": {
			codec: func(contract contractkit.Contract, fixture string, data []byte) ([]byte, error) {
				if _, err := goCodec(contract, fixture, data); err != nil {
					return nil, err
				}
				return []byte(`{"api_version":"v0"}`), nil
			},
			want: "schema",
		},
		"drops a member": {
			codec: func(contract contractkit.Contract, fixture string, data []byte) ([]byte, error) {
				encoded, err := goCodec(contract, fixture, data)
				if err != nil || fixture != "generate-root.json" {
					return encoded, err
				}
				var document map[string]any
				if err := json.Unmarshal(encoded, &document); err != nil {
					return nil, err
				}
				if _, ok := document["settings_patch"]; !ok {
					return nil, errors.New("generate-root.json has no settings_patch to drop")
				}
				delete(document, "settings_patch")
				return json.Marshal(document)
			},
			want: "does not carry the fixture's value",
		},
	} {
		t.Run(name, func(t *testing.T) {
			failed := contractkit.Failed(kit.RoundTrip(test.codec))
			if len(failed) == 0 {
				t.Fatal("defective codec passed the kit")
			}
			for _, result := range failed {
				if strings.Contains(result.Err.Error(), test.want) {
					return
				}
			}
			t.Fatalf("no failure mentions %q; first: %s: %v", test.want, failed[0].Fixture, failed[0].Err)
		})
	}
}

func TestCheckValidatesCallerBytes(t *testing.T) {
	kit := embeddedKit(t)
	root, ok := kit.File(contractkit.FixtureDir + "/generate-root.json")
	if !ok {
		t.Fatal("no generate-root.json")
	}
	if err := kit.Check(contractkit.GenerateRequest, root); err != nil {
		t.Fatalf("valid request rejected: %v", err)
	}
	for name, data := range map[string][]byte{
		"wrong contract": root,
		"not JSON":       []byte("{"),
		"unknown field":  bytes.Replace(root, []byte(`{`), []byte(`{"bogus":1,`), 1),
	} {
		contract := contractkit.GenerateRequest
		if name == "wrong contract" {
			contract = contractkit.QueryRequest
		}
		if err := kit.Check(contract, data); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if err := kit.Check("no-such-contract", root); err == nil {
		t.Error("unknown contract accepted")
	}
	// Contracts without fixtures are schema-checked only.
	if err := kit.Validate("config", []byte(`[]`)); err == nil {
		t.Error("config schema accepted an array")
	}
}

func TestTarballIsDeterministicAndLoadsBack(t *testing.T) {
	kit := embeddedKit(t)
	var first, second bytes.Buffer
	if err := kit.WriteTarball(&first, "v1.2.3"); err != nil {
		t.Fatal(err)
	}
	if err := kit.WriteTarball(&second, "v1.2.3"); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first.Bytes(), second.Bytes()) {
		t.Fatal("tarball bytes differ between runs")
	}
	if err := kit.WriteTarball(io.Discard, "latest"); err == nil {
		t.Fatal("non-release version accepted")
	}
	extracted := untar(t, first.Bytes(), "llmtw-contract-kit-v1.2.3/")
	loaded, err := contractkit.Load(extracted)
	if err != nil {
		t.Fatal(err)
	}
	if err := kit.Manifest().Compare(loaded.Manifest()); err != nil {
		t.Fatal(err)
	}

	tampered := cloneMap(extracted)
	tampered["schema/v1/generate-request.schema.json"].Data = append(bytes.Clone(tampered["schema/v1/generate-request.schema.json"].Data), ' ')
	if _, err := contractkit.Load(tampered); err == nil || !strings.Contains(err.Error(), "does not describe") {
		t.Fatalf("tampered schema loaded: %v", err)
	}
	missing := cloneMap(extracted)
	delete(missing, "fixtures/v1/generate-root.json")
	if _, err := contractkit.Load(missing); err == nil {
		t.Fatal("kit missing a fixture loaded")
	}
	extra := cloneMap(extracted)
	extra["fixtures/v1/evil.sh"] = &fstest.MapFile{Data: []byte("x")}
	if _, err := contractkit.Load(extra); err == nil {
		t.Fatal("kit with an unexpected file loaded")
	}
	noManifest := cloneMap(extracted)
	delete(noManifest, contractkit.ManifestName)
	if _, err := contractkit.Load(noManifest); err == nil {
		t.Fatal("kit without a manifest loaded")
	}
}

func TestExportLoadsBack(t *testing.T) {
	kit := embeddedKit(t)
	directory := filepath.Join(t.TempDir(), "kit")
	if err := kit.Export(directory); err != nil {
		t.Fatal(err)
	}
	loaded, err := contractkit.Load(os.DirFS(directory))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(loaded.ManifestJSON(), kit.ManifestJSON()) {
		t.Fatal("exported manifest differs")
	}
	if err := kit.Export(directory); err == nil {
		t.Fatal("export overwrote an existing directory")
	}
}

// TestSchemaDigestIsReproducibleWithSha256sum pins the documented digest
// recipe: sha256 over the sha256sum listing of schema/v1 in path order.
func TestSchemaDigestIsReproducibleWithSha256sum(t *testing.T) {
	kit := embeddedKit(t)
	var listing strings.Builder
	for _, file := range kit.Manifest().Files {
		if strings.HasPrefix(file.Path, contractkit.SchemaDir+"/") {
			listing.WriteString(file.SHA256 + "  " + file.Path + "\n")
		}
	}
	if want := "sha256:" + sha256Hex([]byte(listing.String())); kit.SchemaDigest() != want {
		t.Fatalf("schema digest %s, want %s", kit.SchemaDigest(), want)
	}
}

func TestKitRejectsUnclassifiedFixtures(t *testing.T) {
	base := untar(t, tarball(t, embeddedKit(t)), "llmtw-contract-kit-v0.0.1/")
	unlisted := repositoryLayout(base)
	unlisted["golang/llm/testdata/v1/negative-new.json"] = &fstest.MapFile{Data: []byte(`{}`)}
	if _, err := contractkit.FromRepository(unlisted); err == nil || !strings.Contains(err.Error(), "not listed") {
		t.Fatalf("unlisted negative fixture accepted: %v", err)
	}
	unnamed := repositoryLayout(base)
	unnamed["golang/llm/testdata/v1/other.json"] = &fstest.MapFile{Data: []byte(`{}`)}
	if _, err := contractkit.FromRepository(unnamed); err == nil || !strings.Contains(err.Error(), "does not name") {
		t.Fatalf("fixture without a contract accepted: %v", err)
	}
}

func tarball(t *testing.T, kit *contractkit.Kit) []byte {
	t.Helper()
	var buffer bytes.Buffer
	if err := kit.WriteTarball(&buffer, "v0.0.1"); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

// untar reads a kit tarball into a file system rooted at prefix, requiring
// every entry to sit under it.
func untar(t *testing.T, data []byte, prefix string) fstest.MapFS {
	t.Helper()
	compressed, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	archive := tar.NewReader(compressed)
	files := fstest.MapFS{}
	for {
		header, err := archive.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(header.Name, prefix) {
			t.Fatalf("tarball entry %s is outside %s", header.Name, prefix)
		}
		if header.Typeflag != tar.TypeReg {
			continue
		}
		contents, err := io.ReadAll(archive)
		if err != nil {
			t.Fatal(err)
		}
		files[strings.TrimPrefix(header.Name, prefix)] = &fstest.MapFile{Data: contents, Mode: fs.FileMode(header.Mode)}
	}
	return files
}

func cloneMap(files fstest.MapFS) fstest.MapFS {
	clone := fstest.MapFS{}
	for name, file := range files {
		copied := *file
		clone[name] = &copied
	}
	return clone
}

// repositoryLayout moves a kit file system to the repository paths
// FromRepository reads.
func repositoryLayout(files fstest.MapFS) fstest.MapFS {
	layout := fstest.MapFS{}
	for name, file := range files {
		switch {
		case strings.HasPrefix(name, contractkit.SchemaDir+"/"):
			layout["golang/api/schema/v1/"+strings.TrimPrefix(name, contractkit.SchemaDir+"/")] = file
		case strings.HasPrefix(name, contractkit.FixtureDir+"/"):
			layout["golang/llm/testdata/v1/"+strings.TrimPrefix(name, contractkit.FixtureDir+"/")] = file
		}
	}
	return layout
}
