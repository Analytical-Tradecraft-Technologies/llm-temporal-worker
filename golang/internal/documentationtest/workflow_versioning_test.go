package documentationtest

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

const workflowVersioningPage = "docs/reference/workflow-versioning.md"

var (
	versionInventoryRow = regexp.MustCompile("(?m)^\\| `([^`]+)` \\| `([^`]+)` \\|(.*)\\|[ \t]*$")
	retiredChangeRow    = regexp.MustCompile("(?m)^\\| `([^`]+)` \\|")
	testName            = regexp.MustCompile("`(Test[A-Za-z0-9_]+)`")
)

// TestWorkflowVersioningPageListsEveryChangeID keeps the change-ID inventory
// in docs/reference/workflow-versioning.md equal to the GetVersion calls in
// the module's non-test Go code (#1564). A new change ID, a changed supported
// range or a removed call fails here until the page records it, so every
// marker has a documented retirement path. A retired change ID must not come
// back, and every inventory row must name tests that exist.
func TestWorkflowVersioningPageListsEveryChangeID(t *testing.T) {
	inCode := getVersionChangeIDs(t, moduleRoot(t))
	if len(inCode) == 0 {
		t.Fatal("found no GetVersion calls; the scanner no longer matches the workflow code")
	}
	page := readText(t, repositoryRoot(t), workflowVersioningPage)
	current := section(t, page, "## Current change IDs")
	retired := section(t, page, "## Retired change IDs")

	documented := map[string]string{}
	for _, row := range versionInventoryRow.FindAllStringSubmatch(current, -1) {
		id, versions, rest := row[1], row[2], row[3]
		if _, duplicate := documented[id]; duplicate {
			t.Errorf("%s lists change ID %s twice", workflowVersioningPage, id)
		}
		documented[id] = versions
		tests := testName.FindAllStringSubmatch(rest, -1)
		if len(tests) == 0 {
			t.Errorf("%s: change ID %s names no test that covers it", workflowVersioningPage, id)
		}
		for _, test := range tests {
			if !workflowTestExists(t, test[1]) {
				t.Errorf("%s: change ID %s names %s, which golang/workflows does not define", workflowVersioningPage, id, test[1])
			}
		}
	}
	for id, versions := range inCode {
		switch got, ok := documented[id]; {
		case !ok:
			t.Errorf("GetVersion change ID %s is not listed under Current change IDs in %s", id, workflowVersioningPage)
		case got != versions:
			t.Errorf("%s lists change ID %s with versions %s; the code supports %s", workflowVersioningPage, id, got, versions)
		}
	}
	for id := range documented {
		if _, ok := inCode[id]; !ok {
			t.Errorf("%s lists change ID %s, which no GetVersion call uses; move it to Retired change IDs", workflowVersioningPage, id)
		}
	}
	for _, row := range retiredChangeRow.FindAllStringSubmatch(retired, -1) {
		if _, ok := inCode[row[1]]; ok {
			t.Errorf("retired change ID %s is used again; a retired ID must never be reused", row[1])
		}
		if _, ok := documented[row[1]]; ok {
			t.Errorf("change ID %s is listed as both current and retired", row[1])
		}
	}
}

// section returns the text under heading up to the next level-two heading.
func section(t *testing.T, page, heading string) string {
	t.Helper()
	start := strings.Index(page, "\n"+heading+"\n")
	if start < 0 {
		t.Fatalf("%s lacks the %q section", workflowVersioningPage, heading)
	}
	body := page[start+len(heading)+2:]
	if end := strings.Index(body, "\n## "); end >= 0 {
		body = body[:end]
	}
	return body
}

func workflowTestExists(t *testing.T, name string) bool {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(moduleRoot(t), "workflows", "*_test.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		if strings.Contains(readText(t, file), "func "+name+"(") {
			return true
		}
	}
	return false
}

// getVersionChangeIDs maps the change ID of every workflow.GetVersion and
// GetVersionWithOptions call in non-test code to its supported range, written
// "min..max". A change ID built as a constant prefix plus a runtime value, such
// as a per-check sequence number, is reported as the prefix followed by "<n>".
// A change ID or version the scanner cannot resolve to constants fails the
// test, so the inventory cannot silently miss a call.
func getVersionChangeIDs(t *testing.T, root string) map[string]string {
	t.Helper()
	found := map[string]string{}
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if name := entry.Name(); path != root && (name == "testdata" || name == "vendor" || strings.HasPrefix(name, ".")) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil || !strings.Contains(string(data), "GetVersion") {
			return err
		}
		scanGetVersionCalls(t, path, found)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return found
}

func scanGetVersionCalls(t *testing.T, path string, found map[string]string) {
	t.Helper()
	files := token.NewFileSet()
	file, err := parser.ParseFile(files, path, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	aliases := map[string]bool{}
	for _, spec := range file.Imports {
		if spec.Path.Value != strconv.Quote("go.temporal.io/sdk/workflow") {
			continue
		}
		alias := "workflow"
		if spec.Name != nil {
			alias = spec.Name.Name
		}
		aliases[alias] = true
	}
	if len(aliases) == 0 {
		return
	}
	constants := packageStringConstants(t, filepath.Dir(path))
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || (selector.Sel.Name != "GetVersion" && selector.Sel.Name != "GetVersionWithOptions") {
			return true
		}
		if pkg, ok := selector.X.(*ast.Ident); !ok || !aliases[pkg.Name] {
			return true
		}
		at := files.Position(call.Pos())
		if len(call.Args) < 4 {
			t.Fatalf("%s: GetVersion call has %d arguments", at, len(call.Args))
		}
		id, constant, ok := changeID(call.Args[1], constants)
		if !ok {
			t.Fatalf("%s: GetVersion change ID must be a string constant, optionally plus a runtime suffix", at)
		}
		if !constant {
			id += "<n>"
		}
		versions := version(t, at, call.Args[2], aliases) + ".." + version(t, at, call.Args[3], aliases)
		if previous, ok := found[id]; ok && previous != versions {
			t.Fatalf("%s: change ID %s is used with versions %s and %s", at, id, previous, versions)
		}
		found[id] = versions
		return true
	})
}

// changeID evaluates a change-ID expression. constant is false when the
// expression is a constant prefix followed by a runtime value.
func changeID(expr ast.Expr, constants map[string]string) (id string, constant, ok bool) {
	switch expr := expr.(type) {
	case *ast.BasicLit:
		if expr.Kind != token.STRING {
			return "", false, false
		}
		value, err := strconv.Unquote(expr.Value)
		return value, true, err == nil
	case *ast.Ident:
		value, ok := constants[expr.Name]
		return value, true, ok
	case *ast.ParenExpr:
		return changeID(expr.X, constants)
	case *ast.BinaryExpr:
		if expr.Op != token.ADD {
			return "", false, false
		}
		prefix, prefixConstant, ok := changeID(expr.X, constants)
		if !ok || !prefixConstant {
			return "", false, false
		}
		if suffix, suffixConstant, ok := changeID(expr.Y, constants); ok && suffixConstant {
			return prefix + suffix, true, true
		}
		return prefix, false, true
	}
	return "", false, false
}

func version(t *testing.T, at token.Position, expr ast.Expr, aliases map[string]bool) string {
	t.Helper()
	switch expr := expr.(type) {
	case *ast.SelectorExpr:
		if pkg, ok := expr.X.(*ast.Ident); ok && aliases[pkg.Name] && expr.Sel.Name == "DefaultVersion" {
			return "DefaultVersion"
		}
	case *ast.BasicLit:
		if expr.Kind == token.INT {
			return expr.Value
		}
	}
	t.Fatalf("%s: GetVersion versions must be workflow.DefaultVersion or integer literals", at)
	return ""
}

// packageStringConstants collects the string constants declared in the
// non-test files of dir.
func packageStringConstants(t *testing.T, dir string) map[string]string {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	constants := map[string]string{}
	files := token.NewFileSet()
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(files, path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			general, ok := decl.(*ast.GenDecl)
			if !ok || general.Tok != token.CONST {
				continue
			}
			for _, spec := range general.Specs {
				value := spec.(*ast.ValueSpec)
				for i, name := range value.Names {
					if i >= len(value.Values) {
						break
					}
					if literal, ok := value.Values[i].(*ast.BasicLit); ok && literal.Kind == token.STRING {
						if text, err := strconv.Unquote(literal.Value); err == nil {
							constants[name.Name] = text
						}
					}
				}
			}
		}
	}
	return constants
}
