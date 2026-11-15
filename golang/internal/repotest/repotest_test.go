package repotest

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestFindRepositoryRootReturnsNearestAncestorWithGit(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, ".git"), 0o700); err != nil {
		t.Fatal(err)
	}
	nested := filepath.Join(root, "golang", "internal", "repotest")
	if err := os.MkdirAll(nested, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, start := range []string{root, nested} {
		got, ok := findRepositoryRoot(start)
		if !ok || got != root {
			t.Fatalf("findRepositoryRoot(%q) = %q, %v; want %q, true", start, got, ok, root)
		}
	}
}

func TestReadJSONObjectDecodesAnObject(t *testing.T) {
	path := filepath.Join(t.TempDir(), "object.json")
	WriteFile(t, path, []byte(`{"name":"value","count":2}`), 0o600)
	got := ReadJSONObject(t, path)
	if len(got) != 2 || got["name"] != "value" || got["count"] != float64(2) {
		t.Fatalf("ReadJSONObject = %#v", got)
	}
}

func TestWriteFileAndReadTextRoundTrip(t *testing.T) {
	directory := t.TempDir()
	WriteFile(t, filepath.Join(directory, "note.txt"), []byte("contents\n"), 0o640)
	if info, err := os.Stat(filepath.Join(directory, "note.txt")); err != nil || info.Mode().Perm() != 0o640 {
		t.Fatalf("WriteFile mode = %v, %v; want 0640", info, err)
	}
	if got := ReadText(t, directory, "note.txt"); got != "contents\n" {
		t.Fatalf("ReadText = %q, want %q", got, "contents\n")
	}
}

func TestModuleRootContainsThisModule(t *testing.T) {
	if _, err := os.Stat(filepath.Join(ModuleRoot(t), "go.mod")); err != nil {
		t.Fatalf("module root has no go.mod: %v", err)
	}
	if _, err := os.Stat(filepath.Join(RepositoryRoot(t), ".git")); err != nil {
		t.Fatalf("repository root has no .git: %v", err)
	}
}

// recordingTB records fatal failures instead of stopping the test.
type recordingTB struct {
	testing.TB
	failures []string
}

func (r *recordingTB) Helper() {}

func (r *recordingTB) Fatal(args ...any) { r.failures = append(r.failures, fmt.Sprint(args...)) }

func (r *recordingTB) Fatalf(format string, args ...any) {
	r.failures = append(r.failures, fmt.Sprintf(format, args...))
}

func TestTextAssertionsReportOnlyViolations(t *testing.T) {
	recorder := &recordingTB{TB: t}
	AssertContains(recorder, "alpha beta", "beta", "present")
	AssertContains(recorder, "alpha beta", "gamma", "missing gamma")
	AssertOmits(recorder, "alpha beta", "gamma", "absent")
	AssertOmits(recorder, "alpha beta", "alpha", "contains alpha")
	AssertContainsEach(recorder, "alpha beta", "missing %q", "alpha", "delta")
	AssertOmitsEach(recorder, "alpha beta", "contains %q", "gamma", "beta")
	want := []string{"missing gamma", "contains alpha", `missing "delta"`, `contains "beta"`}
	if fmt.Sprint(recorder.failures) != fmt.Sprint(want) {
		t.Fatalf("failures = %q, want %q", recorder.failures, want)
	}
}
