package llm

import (
	"embed"
	"io/fs"
)

// The shared v1 wire fixtures and the manifests that classify them
// (negative-fixtures.tsv, media-url-policy.tsv) are part of the published
// contract kit (#1575), so they are embedded here rather than read from the
// repository tree. The linker drops them from binaries that never call
// V1ContractFixtures.
//
//go:embed testdata/v1/*.json testdata/v1/*.tsv
var v1ContractFixtures embed.FS

// V1ContractFixtures returns the shared v1 fixture corpus with every file at
// the root of the returned file system. The contractkit package classifies
// the fixtures and runs them against a consumer codec.
func V1ContractFixtures() fs.FS {
	fixtures, err := fs.Sub(v1ContractFixtures, "testdata/v1")
	if err != nil {
		panic(err) // unreachable: the directory is embedded above
	}
	return fixtures
}
