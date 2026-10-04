package architecturetest

import (
	"encoding/json"
	"go/parser"
	"go/token"
	"io/fs"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// Check every package and its tests, including code outside the worker binary.
// A transitive SQL dependency must not silently restore the removed backend.
func TestWorkerHasNoSQLDependencies(t *testing.T) {
	command := exec.Command("go", "list", "-deps", "-test", "./...")
	command.Dir = moduleRoot(t)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("list worker dependencies: %v\n%s", err, output)
	}
	for _, dependency := range strings.Fields(string(output)) {
		// UUID serialization transitively uses this standard-library interface
		// package. It does not connect to a database or include a database driver.
		if dependency != "database/sql/driver" && sqlDependency(dependency) {
			t.Errorf("worker depends on SQL package %s", dependency)
		}
	}
}

// Parse imports independently of build tags so integration-only or platform-
// specific code cannot reintroduce SQL. Our own code may not import even the
// standard-library serialization interfaces permitted transitively above.
func TestWorkerSourceHasNoSQLImports(t *testing.T) {
	root := moduleRoot(t)
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if entry.Name() == "vendor" || strings.HasPrefix(entry.Name(), ".") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		source, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, declaration := range source.Imports {
			imported, err := strconv.Unquote(declaration.Path.Value)
			if err != nil {
				return err
			}
			if sqlDependency(imported) {
				t.Errorf("%s imports SQL package %s", path, imported)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// Also reject unused explicit requirements and replacements; checking only
// compiled imports would let a dormant driver remain in the module manifest.
func TestWorkerModuleHasNoSQLRequirements(t *testing.T) {
	command := exec.Command("go", "mod", "edit", "-json")
	command.Dir = moduleRoot(t)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("read module requirements: %v\n%s", err, output)
	}
	type module struct{ Path string }
	var manifest struct {
		Require []module
		Replace []struct{ Old, New module }
	}
	if err := json.Unmarshal(output, &manifest); err != nil {
		t.Fatal(err)
	}
	modules := append([]module(nil), manifest.Require...)
	for _, replacement := range manifest.Replace {
		modules = append(modules, replacement.Old, replacement.New)
	}
	for _, dependency := range modules {
		if sqlDependency(dependency.Path) {
			t.Errorf("worker module references SQL module %s", dependency.Path)
		}
	}
}

func sqlDependency(path string) bool {
	for _, prefix := range []string{
		"database/sql", "github.com/jackc", "github.com/lib/pq",
		"github.com/go-sql-driver", "github.com/mattn/go-sqlite3",
		"modernc.org/sqlite", "github.com/jmoiron/sqlx",
		"gorm.io", "github.com/jinzhu/gorm", "entgo.io/ent",
		"github.com/uptrace/bun", "github.com/volatiletech/sqlboiler",
		"github.com/stephenafamo/bob", "github.com/DATA-DOG/go-sqlmock",
		modulePath + "/storage/postgres",
	} {
		if path == prefix || strings.HasPrefix(path, prefix+"/") {
			return true
		}
	}
	return false
}
