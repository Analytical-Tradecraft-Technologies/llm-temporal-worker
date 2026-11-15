package contractkit_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/contractkit"
)

// The in-repository OCaml client is the kit's first consumer. Its package
// is installed by opam from the ocaml/llm_temporal_worker subdirectory
// alone (CI proves the remote --subpath install), so its tests cannot read
// the kit from golang/ and carry a copy instead. These tests make that
// copy, and what the OCaml codec writes, answer to the kit (#1575, #1517).
var (
	ocamlFixtures = filepath.Join(repositoryRoot, "ocaml", "llm_temporal_worker", "test", "fixtures", "v1")
	ocamlEncoded  = filepath.Join(repositoryRoot, "ocaml", "llm_temporal_worker", "test", "fixtures", "v1-encoded")
)

// TestOCamlFixturesMirrorTheKit requires ocaml/.../test/fixtures/v1 to be
// a byte-identical copy of the kit's fixtures/v1, the negative-fixture and
// media URL manifests included, with no file missing or extra.
func TestOCamlFixturesMirrorTheKit(t *testing.T) {
	kit := embeddedKit(t)
	entries, err := os.ReadDir(ocamlFixtures)
	if err != nil {
		t.Fatal(err)
	}
	mirrored := map[string]bool{}
	for _, entry := range entries {
		name := entry.Name()
		mirrored[name] = true
		want, ok := kit.File(contractkit.FixtureDir + "/" + name)
		if !ok {
			t.Errorf("ocaml/llm_temporal_worker/test/fixtures/v1/%s is not a kit fixture", name)
			continue
		}
		got, err := os.ReadFile(filepath.Join(ocamlFixtures, name))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("ocaml/llm_temporal_worker/test/fixtures/v1/%s differs from the kit's %s/%s", name, contractkit.FixtureDir, name)
		}
	}
	for _, name := range kit.Paths() {
		if base, ok := strings.CutPrefix(name, contractkit.FixtureDir+"/"); ok && !mirrored[base] {
			t.Errorf("kit fixture %s has no copy in ocaml/llm_temporal_worker/test/fixtures/v1", base)
		}
	}
}

// TestOCamlEncodingsPassTheKit checks what the OCaml codec writes, not
// only what it reads: the OCaml tests pin their encoding of each positive
// fixture under fixtures/v1-encoded, and each must pass the same check the
// consumer runner applies, CheckEncoding.
func TestOCamlEncodingsPassTheKit(t *testing.T) {
	kit := embeddedKit(t)
	positive := map[string]bool{}
	for _, fixture := range kit.Fixtures() {
		if !fixture.Negative {
			positive[fixture.Name] = true
		}
	}
	encodings, err := filepath.Glob(filepath.Join(ocamlEncoded, "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range encodings {
		if !positive[filepath.Base(path)] {
			t.Errorf("%s has no positive kit fixture", path)
		}
	}
	for name := range positive {
		t.Run(name, func(t *testing.T) {
			encoded, err := os.ReadFile(filepath.Join(ocamlEncoded, name))
			if err != nil {
				t.Fatalf("no pinned OCaml encoding: %v", err)
			}
			if err := kit.CheckEncoding(name, encoded); err != nil {
				t.Fatalf("OCaml encoding fails the kit: %v", err)
			}
		})
	}
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
