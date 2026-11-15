package contractkit

import (
	"archive/tar"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

var releaseVersion = regexp.MustCompile(`^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$`)

// tarballEpoch is the fixed modification time of every tarball entry, so
// one kit and version always produce the same tarball bytes.
var tarballEpoch = time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)

// TarballName is the release tarball file name for version.
func TarballName(version string) string {
	return "llmtw-contract-kit-" + version + ".tar.gz"
}

// WriteTarball writes the kit as a deterministic gzip-compressed tarball:
// one llmtw-contract-kit-<version>/ directory holding contract-kit.json,
// schema/v1 and fixtures/v1. version must be a release tag such as v1.2.3.
func (kit *Kit) WriteTarball(output io.Writer, version string) error {
	if !releaseVersion.MatchString(version) {
		return fmt.Errorf("version %q is not a vMAJOR.MINOR.PATCH release tag", version)
	}
	compressed := gzip.NewWriter(output)
	archive := tar.NewWriter(compressed)
	root := strings.TrimSuffix(TarballName(version), ".tar.gz")
	directories := []string{root, path.Join(root, "schema"), path.Join(root, SchemaDir), path.Join(root, "fixtures"), path.Join(root, FixtureDir)}
	for _, directory := range directories {
		if err := archive.WriteHeader(&tar.Header{Typeflag: tar.TypeDir, Name: directory + "/", Mode: 0o755, ModTime: tarballEpoch, Format: tar.FormatUSTAR}); err != nil {
			return err
		}
	}
	write := func(name string, data []byte) error {
		if err := archive.WriteHeader(&tar.Header{Typeflag: tar.TypeReg, Name: path.Join(root, name), Mode: 0o644, Size: int64(len(data)), ModTime: tarballEpoch, Format: tar.FormatUSTAR}); err != nil {
			return err
		}
		_, err := archive.Write(data)
		return err
	}
	if err := write(ManifestName, kit.ManifestJSON()); err != nil {
		return err
	}
	for _, name := range kit.Paths() {
		if err := write(name, kit.files[name]); err != nil {
			return err
		}
	}
	if err := archive.Close(); err != nil {
		return err
	}
	return compressed.Close()
}

// Export writes the kit's files and contract-kit.json under directory,
// which must not exist yet, so Load(os.DirFS(directory)) reads it back.
func (kit *Kit) Export(directory string) error {
	if _, err := os.Lstat(directory); err == nil {
		return fmt.Errorf("%s already exists", directory)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	for _, name := range append([]string{ManifestName}, kit.Paths()...) {
		data := kit.files[name]
		if name == ManifestName {
			data = kit.ManifestJSON()
		}
		target := filepath.Join(directory, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(target, data, 0o644); err != nil {
			return err
		}
	}
	return nil
}
