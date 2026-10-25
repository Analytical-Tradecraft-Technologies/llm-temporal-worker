package documentationtest

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// The package-layout tree calls itself the current layout, so every Go
// package directory under golang/ must appear in it, and every directory it
// names under golang/ must exist (#1686).
func TestPackageLayoutTreeMatchesTheModule(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(repositoryRoot(t), "docs", "architecture", "package-layout.md"))
	if err != nil {
		t.Fatal(err)
	}
	listed := packageLayoutTreePaths(t, string(data))
	module := moduleRoot(t)
	var missing []string
	err = filepath.WalkDir(module, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			return nil
		}
		if name := entry.Name(); name == "testdata" || (strings.HasPrefix(name, ".") && path != module) {
			return filepath.SkipDir
		}
		goFiles, err := filepath.Glob(filepath.Join(path, "*.go"))
		if err != nil || len(goFiles) == 0 || path == module {
			return err
		}
		relative, err := filepath.Rel(module, path)
		if err != nil {
			return err
		}
		if !listed[filepath.ToSlash(relative)] {
			missing = append(missing, filepath.ToSlash(relative))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Fatalf("docs/architecture/package-layout.md omits Go packages: %s", strings.Join(missing, ", "))
	}
	for path := range listed {
		if strings.Contains(path, "*") {
			continue
		}
		if _, err := os.Stat(filepath.Join(module, filepath.FromSlash(path))); err != nil {
			t.Errorf("docs/architecture/package-layout.md lists golang/%s, which does not exist", path)
		}
	}
}

// packageLayoutTreePaths reconstructs the module-relative paths of the
// directories the layout tree lists under golang/.
func packageLayoutTreePaths(t *testing.T, document string) map[string]bool {
	t.Helper()
	start := strings.Index(document, "```text\n.\n")
	if start < 0 {
		t.Fatal("package-layout.md has no layout tree")
	}
	block := document[start+len("```text\n.\n"):]
	block = block[:strings.Index(block, "```")]
	paths := map[string]bool{}
	var stack []string
	for _, line := range strings.Split(block, "\n") {
		branch := strings.Index(line, "── ")
		if branch < 0 {
			continue
		}
		// Each level of nesting is four display columns ("│   " or "    ").
		depth := len([]rune(line[:branch])) / 4
		fields := strings.Fields(line[branch+len("── "):])
		if len(fields) == 0 {
			continue
		}
		name := fields[0]
		stack = append(stack[:min(depth, len(stack))], strings.TrimSuffix(name, "/"))
		if !strings.HasSuffix(name, "/") || stack[0] != "golang" || len(stack) < 2 {
			continue
		}
		// A segment such as api/schema/v1/ names every directory on its way.
		path := strings.Join(stack[1:], "/")
		parts := strings.Split(path, "/")
		for index := range parts {
			paths[strings.Join(parts[:index+1], "/")] = true
		}
	}
	if !paths["llm"] || !paths["workflows"] {
		t.Fatalf("layout tree parsed to %d paths without llm and workflows; the parser is out of date", len(paths))
	}
	return paths
}
