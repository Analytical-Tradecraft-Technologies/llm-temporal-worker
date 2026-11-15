package qualitytest

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/repotest"
)

var fuzzTargetName = regexp.MustCompile(`^Fuzz[[:alnum:]_]+$`)

func TestFuzzGateCoversEveryRepositoryFuzzTarget(t *testing.T) {
	root := moduleRoot(t)
	script := readText(t, moduleRoot(t), "scripts", "run-fuzz.sh")

	for _, target := range repositoryFuzzTargets(t, root) {
		entry := target.packagePath + " " + target.name
		if !strings.Contains(script, entry) {
			t.Fatalf("fuzz gate does not include %s", entry)
		}
	}
	assertContains(t, script, "reproduce a saved corpus input", "fuzz gate does not document a saved-corpus reproduction command")
}

func TestFuzzGateCoversNanoUSDMaterialization(t *testing.T) {
	root := moduleRoot(t)
	script := readText(t, root, "scripts", "run-fuzz.sh")
	assertContains(t, script, "./pricing FuzzNanoUSDMaterialization", "fuzz gate does not include the conservative nanoUSD rounding target")
}

func TestRepositoryFuzzTargetsIncludesOrdinaryTestFiles(t *testing.T) {
	root := moduleRoot(t)
	for _, target := range repositoryFuzzTargets(t, root) {
		if target.packagePath == "./state" && target.name == "FuzzVerifyHandleNeverPanics" {
			return
		}
	}
	t.Fatal("repository fuzz target scan omitted FuzzVerifyHandleNeverPanics in state/handle_test.go")
}

func TestRepositoryFuzzTargetsIncludesOutsideLegacyAllowlist(t *testing.T) {
	root := t.TempDir()
	for _, directory := range []string{"llm", "pricing", "budget", "state", "storage/redis", "routing", "admission"} {
		if err := os.MkdirAll(filepath.Join(root, directory), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	writeFile(t, filepath.Join(root, "admission", "engine_fuzz_test.go"), []byte(`package admission

import "testing"

func FuzzAdmissionEngine(f *testing.F) {}
`), 0o600)

	for _, target := range repositoryFuzzTargets(t, root) {
		if target.packagePath == "./admission" && target.name == "FuzzAdmissionEngine" {
			return
		}
	}
	t.Fatal("repository fuzz target scan omitted FuzzAdmissionEngine outside the legacy allowlist")
}

func TestRepositoryFuzzTargetsIgnoresFuzzLookalikesOutsideDeclarations(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "lookalike_fuzz_test.go"), []byte(`package fixture

import "testing"

// func FuzzComment(f *testing.F) {}
const source = `+"`"+`
func FuzzString(f *testing.F) {}
`+"`"+`

func FuzzReal(f *testing.F) {}
`), 0o600)

	targets := repositoryFuzzTargets(t, root)
	if len(targets) != 1 || targets[0].packagePath != "." || targets[0].name != "FuzzReal" {
		t.Fatalf("fuzz targets = %#v, want only FuzzReal", targets)
	}
}

func TestRepositoryFuzzTargetsRecognizesStandardTestingAliasesOnly(t *testing.T) {
	root := t.TempDir()
	files := map[string]string{
		"alias_fuzz_test.go": `package fixture

import testingalias "testing"

func FuzzAlias(f *testingalias.F) {}
`,
		"dot_fuzz_test.go": `package fixture

import . "testing"

func FuzzDot(f *F) {}
`,
		"foreign_fuzz_test.go": `package fixture

import testing "example.com/not-the-standard-library/testing"

func FuzzForeign(f *testing.F) {}
`,
	}
	for name, data := range files {
		writeFile(t, filepath.Join(root, name), []byte(data), 0o600)
	}

	targets := repositoryFuzzTargets(t, root)
	if len(targets) != 2 || targets[0] != (fuzzEntry{packagePath: ".", name: "FuzzAlias"}) || targets[1] != (fuzzEntry{packagePath: ".", name: "FuzzDot"}) {
		t.Fatalf("fuzz targets = %#v, want aliased and dot-imported testing.F targets only", targets)
	}
}

func TestQualityGatesAreWiredIntoMakeCIAndTestingStrategy(t *testing.T) {
	root := repositoryRoot(t)
	makefile := readText(t, moduleRoot(t), "Makefile")
	assertContainsEach(t, makefile, "Makefile does not include %q", "fuzz-smoke:", "mutation-verify:", "scripts/run-fuzz.sh smoke", "scripts/run-mutation.sh")

	pullRequest := readText(t, root, ".github", "workflows", "pull-request.yml")
	assertContainsEach(t, pullRequest, "pull-request workflow does not run %q", "make fuzz-smoke", "make mutation-verify")

	master := readText(t, root, ".github", "workflows", "master.yml")
	// The fuzz and race shard scripts take their shard count from the matrix
	// (strategy.job-total), and release evidence must merge exactly that many
	// shard summaries (#1570).
	fuzzShards := masterJobShardCount(t, master, "fuzz-shard")
	raceShards := masterJobShardCount(t, master, "verify-race")
	assertContainsEach(t, master, "master workflow does not configure %q",
		`bash scripts/run-fuzz.sh shard "${{ matrix.shard }}" "${{ strategy.job-total }}"`,
		`bash scripts/run-race-shard.sh "$RACE_SHARD" "${{ strategy.job-total }}"`,
		"merge-shards.py fuzz_summary fuzz "+strconv.Itoa(fuzzShards)+" fuzz-inputs ",
		"merge-shards.py race_summary race "+strconv.Itoa(raceShards)+" race-inputs ",
		"merge-shards.py test_summary test "+strconv.Itoa(raceShards)+" race-inputs ",
	)

	strategy := readText(t, root, "docs", "testing", "strategy.md")
	assertContainsEach(t, strategy, "testing strategy does not document %q", "make fuzz-smoke", "make mutation-verify")
}

var shardMatrix = regexp.MustCompile(`(?m)^        shard: \[([0-9, ]+)\]$`)

// masterJobShardCount returns how many shards a master.yml matrix job lists,
// requiring the list to be exactly 0..n-1 with at least two shards.
func masterJobShardCount(t *testing.T, master, job string) int {
	t.Helper()
	start := strings.Index(master, "\n  "+job+":\n")
	if start < 0 {
		t.Fatalf("master workflow has no %s job", job)
	}
	block := master[start+1:]
	if end := regexp.MustCompile(`\n  [a-z][a-z0-9-]*:\n`).FindStringIndex(block[1:]); end != nil {
		block = block[:end[0]+1]
	}
	match := shardMatrix.FindStringSubmatch(block)
	if match == nil {
		t.Fatalf("master workflow %s job does not list its shards", job)
	}
	shards := strings.Split(match[1], ", ")
	for index, shard := range shards {
		if shard != strconv.Itoa(index) {
			t.Fatalf("master workflow %s shards = [%s], want 0..%d", job, match[1], len(shards)-1)
		}
	}
	if len(shards) < 2 {
		t.Fatalf("master workflow %s lists %d shard, want at least two", job, len(shards))
	}
	return len(shards)
}

func TestMasterFuzzShardsUseFixedExecutionBudget(t *testing.T) {
	root := repositoryRoot(t)
	master := readText(t, root, ".github", "workflows", "master.yml")
	assertContains(t, master, `FUZZ_TIME=250000x bash ../scripts/release/run-gate.sh fuzz_summary "$RUNNER_TEMP/fuzz-evidence/fuzz-${{ matrix.shard }}.json" bash scripts/run-fuzz.sh shard "${{ matrix.shard }}"`, "master fuzz shards must use the fixed 250000x execution budget")
	assertOmits(t, master, "FUZZ_TIME=45s", "master fuzz shards must not use a deadline-based fuzz budget")

	script := readText(t, moduleRoot(t), "scripts", "run-fuzz.sh")
	assertContains(t, script, "-parallel=1", "fuzz runner must retain one worker per target")

	strategy := readText(t, root, "docs", "testing", "strategy.md")
	assertContainsEach(t, strategy, "testing strategy does not document %q", "FUZZ_TIME=250000x", "248,282")
}

func TestMutationManifestCoversCriticalSemanticInvariants(t *testing.T) {
	manifest := readText(t, moduleRoot(t), "tools", "mutationverify", "main.go")
	assertContainsEach(t, manifest, "mutation manifest does not include %q",
		"money-round-up",
		"window-comparison-boundary",
		"dispatch-certainty",
		"service-class-default",
		"state-transition",
	)
}

type fuzzEntry struct {
	packagePath string
	name        string
}

func repositoryFuzzTargets(t *testing.T, root string) []fuzzEntry {
	t.Helper()
	var targets []fuzzEntry
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if path != root && excludedFromFuzzTargetScan(entry.Name()) {
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(entry.Name(), "_test.go") {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(root, filepath.Dir(path))
		if err != nil {
			return err
		}
		packagePath := "."
		if relative != "." {
			packagePath = "./" + filepath.ToSlash(relative)
		}
		names, err := fuzzTargetsInFile(path, data)
		if err != nil {
			return err
		}
		for _, name := range names {
			targets = append(targets, fuzzEntry{packagePath: packagePath, name: name})
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Slice(targets, func(i, j int) bool {
		if targets[i].packagePath != targets[j].packagePath {
			return targets[i].packagePath < targets[j].packagePath
		}
		return targets[i].name < targets[j].name
	})
	return targets
}

func fuzzTargetsInFile(path string, data []byte) ([]string, error) {
	parsed, err := parser.ParseFile(token.NewFileSet(), path, data, 0)
	if err != nil {
		return nil, err
	}
	testingNames, dotTesting := standardTestingImports(parsed)
	var names []string
	for _, declaration := range parsed.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || !isFuzzTarget(function, testingNames, dotTesting) {
			continue
		}
		names = append(names, function.Name.Name)
	}
	return names, nil
}

// standardTestingImports resolves only direct imports of the standard-library
// testing package. Fuzz targets may use its default name, an explicit alias,
// or a dot import; imports with a lookalike local path must not become gates.
func standardTestingImports(file *ast.File) (map[string]bool, bool) {
	names := make(map[string]bool)
	dotImported := false
	for _, spec := range file.Imports {
		path, err := strconv.Unquote(spec.Path.Value)
		if err != nil || path != "testing" {
			continue
		}
		if spec.Name == nil {
			names["testing"] = true
			continue
		}
		switch spec.Name.Name {
		case ".":
			dotImported = true
		case "_":
			// A blank import cannot name testing.F.
		default:
			names[spec.Name.Name] = true
		}
	}
	return names, dotImported
}

func isFuzzTarget(function *ast.FuncDecl, testingNames map[string]bool, dotTesting bool) bool {
	if function.Recv != nil || !fuzzTargetName.MatchString(function.Name.Name) || function.Type.Params == nil || len(function.Type.Params.List) != 1 {
		return false
	}
	if function.Type.Results != nil && len(function.Type.Results.List) > 0 {
		return false
	}
	parameter := function.Type.Params.List[0]
	if len(parameter.Names) > 1 {
		return false
	}
	pointer, ok := parameter.Type.(*ast.StarExpr)
	if !ok {
		return false
	}
	switch value := pointer.X.(type) {
	case *ast.SelectorExpr:
		pkg, ok := value.X.(*ast.Ident)
		return ok && testingNames[pkg.Name] && value.Sel.Name == "F"
	case *ast.Ident:
		return dotTesting && value.Name == "F"
	default:
		return false
	}
}

// excludedFromFuzzTargetScan deliberately mirrors directories that Go does not
// treat as module packages, so every actual repository fuzz target remains
// covered without interpreting fixtures or vendored code as runnable tests.
func excludedFromFuzzTargetScan(name string) bool {
	switch name {
	case ".git", ".github", ".agents", ".codex", "testdata", "vendor", "node_modules":
		return true
	default:
		return strings.HasPrefix(name, ".")
	}
}

// repositoryRoot and moduleRoot locate the checkout these tests verify.
var (
	repositoryRoot = repotest.RepositoryRoot
	moduleRoot     = repotest.ModuleRoot
	readText       = repotest.ReadText
	writeFile      = repotest.WriteFile

	assertContains     = repotest.AssertContains
	assertOmits        = repotest.AssertOmits
	assertContainsEach = repotest.AssertContainsEach
	assertOmitsEach    = repotest.AssertOmitsEach
)
