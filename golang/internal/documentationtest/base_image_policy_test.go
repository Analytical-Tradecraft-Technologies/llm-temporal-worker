package documentationtest

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestDocumentationDescribesMutableBaseImageTags keeps the image docs in step
// with deploy/verify.sh, which rejects base-image digests in the Dockerfile.
func TestDocumentationDescribesMutableBaseImageTags(t *testing.T) {
	root := repositoryRoot(t)
	dockerfile, err := os.ReadFile(filepath.Join(root, "golang", "Dockerfile"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(dockerfile), "@sha256:") {
		t.Skip("Dockerfile now pins base digests; update the documentation policy")
	}
	for _, path := range []string{
		filepath.Join("golang", "deploy", "README.md"),
		filepath.Join("docs", "architecture", "deployment-and-operations.md"),
	} {
		data, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		text := strings.ToLower(string(data))
		for _, claim := range []string{"digest-pinned go", "digest-pinned distroless", "image by digest;", "digest-pinned go builder"} {
			if strings.Contains(text, claim) {
				t.Errorf("%s claims %q, but the Dockerfile uses mutable base tags", path, claim)
			}
		}
	}
}
