package architecturetest

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestPackageLicenseMetadataMatchesRepositoryLicense keeps published package
// and image metadata aligned with the repository's Apache-2.0 LICENSE.
func TestPackageLicenseMetadataMatchesRepositoryLicense(t *testing.T) {
	root := repositoryRoot(t)
	license, err := os.ReadFile(filepath.Join(root, "LICENSE"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(license), "Apache License") || !strings.Contains(string(license), "Version 2.0") {
		t.Fatal("repository LICENSE is no longer Apache-2.0; update this test and package metadata together")
	}
	for path, want := range map[string]string{
		filepath.Join("ocaml", "llm_temporal_worker", "llm-temporal-ocaml.opam"): `license: "Apache-2.0"`,
		filepath.Join("golang", "Dockerfile"):                                    `org.opencontainers.image.licenses="Apache-2.0"`,
	} {
		data, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(data), want) {
			t.Errorf("%s does not declare %s", path, want)
		}
	}
}
