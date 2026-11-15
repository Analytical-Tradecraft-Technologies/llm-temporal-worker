package main

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func writeTestBlob(t *testing.T, dir string, data []byte) descriptor {
	t.Helper()
	d := descriptor{Digest: digestOf(data), Size: int64(len(data))}
	path := blobPath(dir, d.Digest)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	return d
}

func TestReadVersionedBlobChecksSchemaAndMediaType(t *testing.T) {
	dir := t.TempDir()
	for name, test := range map[string]struct {
		body string
		want string
	}{
		"valid":            {`{"schemaVersion":2,"manifests":[]}`, ""},
		"declared type":    {`{"schemaVersion":2,"mediaType":"` + mediaTypeImageIndex + `","manifests":[]}`, ""},
		"wrong version":    {`{"schemaVersion":1,"manifests":[]}`, "image index %s is invalid"},
		"not json":         {`{`, "image index %s is invalid"},
		"wrong media type": {`{"schemaVersion":2,"mediaType":"other","manifests":[]}`, `image index %s declares media type "other"`},
	} {
		d := writeTestBlob(t, dir, []byte(test.body))
		_, err := readIndexBlob(dir, d)
		if test.want == "" {
			if err != nil {
				t.Errorf("%s: %v", name, err)
			}
			continue
		}
		if want := fmt.Sprintf(test.want, d.Digest); err == nil || err.Error() != want {
			t.Errorf("%s = %v, want %s", name, err, want)
		}
	}
}

func TestParseLayoutFlagsRejectsPositionalArguments(t *testing.T) {
	layout, platforms, err := parseLayoutFlags("verify", []string{"-layout", "dir", "-platform", "linux/amd64"}, "", "")
	if err != nil || layout != "dir" || len(platforms) != 1 {
		t.Fatalf("parsed = %q %v, %v", layout, platforms, err)
	}
	if layout, _, err := parseLayoutFlags("verify", []string{"-layout", "dir", "extra"}, "", ""); err != nil || layout != "" {
		t.Fatalf("positional = %q, %v", layout, err)
	}
	if _, _, err := parseLayoutFlags("verify", []string{"-unknown"}, "", ""); err == nil {
		t.Fatal("unknown flag accepted")
	}
}
