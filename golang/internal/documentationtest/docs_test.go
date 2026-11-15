package documentationtest

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/repotest"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/contracttest"
)

var markdownLink = regexp.MustCompile(`(?m)(?:^|[^!])\[[^\]]+\]\(([^)]+)\)`)

func TestDocumentationLinksAndInvariants(t *testing.T) {
	root := repositoryRoot(t)
	makefile := readText(t, moduleRoot(t), "Makefile")
	assertOmits(t, makefile, "test ./internal/documentationtest -run", "docs-verify must not filter documentation tests")
	required := []string{
		"README.md",
		"docs/index.md",
		"docs/superpowers/plans/2026-07-13-master-sequence.md",
	}
	for _, relative := range required {
		if _, err := os.Stat(filepath.Join(root, relative)); err != nil {
			t.Fatalf("required documentation %s: %v", relative, err)
		}
	}
	var files []string
	files = append(files, filepath.Join(root, "README.md"))
	err := filepath.WalkDir(filepath.Join(root, "docs"), func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".md") {
			files = append(files, path)
		}
		return nil
	})
	testassert.NoError(t, err)
	for _, path := range files {
		text := readText(t, path)
		if strings.Contains(text, "TBD") || strings.Contains(text, "TODO") || strings.Contains(text, "FIXME") || strings.Contains(text, "PLACEHOLDER") {
			t.Fatalf("planning placeholder found in %s", path)
		}
		for _, match := range markdownLink.FindAllStringSubmatch(text, -1) {
			raw := strings.TrimSpace(strings.Fields(match[1])[0])
			raw = strings.Trim(raw, "<>")
			if raw == "" || strings.HasPrefix(raw, "#") || strings.HasPrefix(raw, "https://") || strings.HasPrefix(raw, "http://") || strings.HasPrefix(raw, "mailto:") {
				continue
			}
			target := strings.SplitN(raw, "#", 2)[0]
			resolved, err := filepath.Abs(filepath.Join(filepath.Dir(path), target))
			testassert.NoError(t, err)
			if _, err := os.Stat(resolved); err != nil {
				t.Fatalf("%s links to missing target %s: %s", path, raw, err)
			}
		}
	}
	decision := readText(t, root, "docs/decisions/0002-service-classes.md")
	if strings.Count(decision, "economy | standard | priority") != 1 {
		t.Fatal("service-class decision must state the exact public enum once")
	}
}

func TestConfigurationReferenceCompleteShapeLoads(t *testing.T) {
	withoutConfigEnvironmentOverrides(t)

	path := filepath.Join(repositoryRoot(t), "docs/reference/configuration.md")
	data := readText(t, path)

	example := fencedYAMLForSection(t, data, "## Complete shape")
	testassert.NoError(t, testassert.Err(config.Load([]byte(example))), "complete configuration example must pass the strict loader")
}

func fencedYAMLForSection(t *testing.T, document, heading string) string {
	t.Helper()
	sectionStart := strings.Index(document, heading)
	if sectionStart < 0 {
		t.Fatalf("missing documentation section %q", heading)
	}
	section := document[sectionStart+len(heading):]
	if sectionEnd := strings.Index(section, "\n## "); sectionEnd >= 0 {
		section = section[:sectionEnd]
	}
	fenceStart := strings.Index(section, "```yaml\n")
	if fenceStart < 0 {
		t.Fatalf("section %q has no YAML fence", heading)
	}
	yaml := section[fenceStart+len("```yaml\n"):]
	fenceEnd := strings.Index(yaml, "\n```")
	if fenceEnd < 0 {
		t.Fatalf("section %q has no closing YAML fence", heading)
	}
	return yaml[:fenceEnd]
}

func withoutConfigEnvironmentOverrides(t *testing.T) {
	t.Helper()
	for _, name := range []string{
		"LLMTW_REDIS_KEY_PREFIX",
	} {
		value, present := os.LookupEnv(name)
		if err := os.Unsetenv(name); err != nil {
			t.Fatalf("unset %s: %v", name, err)
		}
		t.Cleanup(func() {
			if present {
				_ = os.Setenv(name, value)
			} else {
				_ = os.Unsetenv(name)
			}
		})
	}
}

func TestActivityRuntimeReferencesCurrentQueryContract(t *testing.T) {
	root := repositoryRoot(t)
	path := filepath.Join(root, "docs/reference/activity-runtime.md")
	text := readNormalizedText(t, path)
	const queryLink = "[persisted query service](persisted-query-service.md)"
	if !strings.Contains(text, queryLink) {
		t.Fatalf("%s must link the current query service contract", path)
	}
	if strings.Contains(text, "Task 14 of the v1 plan") {
		t.Fatalf("%s contains the stale v1-plan Task 14 reference", path)
	}
	if !strings.Contains(text, "[query audit logging](query-audit-ledger.md)") {
		t.Fatalf("%s must link the query audit logging reference", path)
	}
}

func TestLiveProviderDocumentationSeparatesManualWorkflowFromRelease(t *testing.T) {
	root := repositoryRoot(t)
	text := readNormalizedText(t, filepath.Join(root, "docs/testing/strategy.md"))
	assertContains(t, text, "protected manual live-provider workflow", "live-provider documentation must name the protected manual live-provider workflow")
	assertContains(t, text, "redacted live-provider evidence", "live-provider documentation must distinguish its redacted evidence")
	assertOmits(t, text, "protected manual release workflow", "live-provider workflow must not be documented as a release workflow")
}

func TestFixtureMatrixListsEveryEnforcedProfile(t *testing.T) {
	root := repositoryRoot(t)
	matrixPath := filepath.Join(root, "docs", "testing", "fixture-matrix.md")
	matrix := readText(t, matrixPath)

	report, err := contracttest.ValidateRepository(moduleRoot(t))
	testassert.NoError(t, err, "validate provider fixture manifests")
	if len(report.Enforced) == 0 {
		t.Fatal("no enforced provider fixture manifests found")
	}
	for _, profile := range report.Enforced {
		listed := false
		for _, line := range strings.Split(matrix, "\n") {
			if strings.HasPrefix(strings.TrimSpace(line), "| `"+profile.ID+"` |") {
				listed = true
				break
			}
		}
		if !listed {
			t.Errorf("fixture matrix does not list enforced profile %q from %s", profile.ID, profile.Path)
		}
	}
}

func TestReadinessDocumentationStatesRedisPrefixBoundary(t *testing.T) {
	root := repositoryRoot(t)
	for _, relative := range []string{
		"docs/reference/configuration.md",
		"docs/architecture/deployment-and-operations.md",
	} {
		t.Run(relative, func(t *testing.T) {
			text := readNormalizedText(t, filepath.Join(root, relative))
			if !strings.Contains(text, "budget authority marker") ||
				!strings.Contains(text, "budget-initialize") {
				t.Fatalf("%s must state that production readiness verifies explicitly initialized budget authority", relative)
			}
			if strings.Contains(text, "Redis probe performs its normal connectivity, clock, policy, function, prefix, and manifest checks") ||
				strings.Contains(text, "expose the exact configured budget Function library/version/digest plus a complete active budget generation/manifest") {
				t.Fatalf("%s still overstates Redis readiness coverage", relative)
			}
			if !strings.Contains(text, "enabled coordination Stream") || !strings.Contains(text, "valid monotonic IDs") {
				t.Fatalf("%s must document the enabled coordination Stream readiness contract", relative)
			}
		})
	}
}

func TestDockerBuildInstructionsUseGoModuleContext(t *testing.T) {
	root := repositoryRoot(t)
	for _, relative := range []string{
		"docs/superpowers/plans/2026-07-13-master-sequence.md",
		"docs/superpowers/plans/2026-07-14-v1-completion.md",
	} {
		t.Run(relative, func(t *testing.T) {
			data := readText(t, root, relative)
			for _, line := range strings.Split(data, "\n") {
				if strings.HasPrefix(strings.TrimSpace(line), "docker build ") {
					t.Fatalf("%s must build from the nested golang module context: %s", relative, strings.TrimSpace(line))
				}
			}
		})
	}
}

func TestV1DocumentationStatesFinalResponseBoundary(t *testing.T) {
	root := repositoryRoot(t)
	for _, test := range []struct {
		path      string
		required  string
		forbidden []string
	}{
		{
			path:      "docs/index.md",
			required:  "Public workflows return final normalized responses; create/poll activity results may be pending. Live streaming and token-event APIs are not supported",
			forbidden: []string{"Optional typed stream APIs are for reusable-library callers"},
		},
		{
			path:      "docs/scope.md",
			required:  "No live streaming or token-event API is supported in v1.",
			forbidden: []string{"Typed streaming events for reusable Go-library callers"},
		},
		{
			path:     "docs/architecture/provider-adapters.md",
			required: "v1 has no supported streaming adapter or token-event API, and this code must not be wired into Temporal dispatch.",
		},
		{
			path:     "docs/architecture/temporal-worker.md",
			required: "No streaming or token-event API is supported in v1, including for reusable library callers.",
		},
		{
			path:      "docs/architecture/system-overview.md",
			required:  "Public workflows return final responses; internal activities can return pending. No streaming or token-event API is supported in v1.",
			forbidden: []string{"`Engine.Generate` is the only supported v1 inference entry point."},
		},
		{
			path:      "docs/architecture/unified-api.md",
			required:  "No streaming or token-event API is supported in v1.",
			forbidden: []string{"A caller that needs typed events may require `llm.StreamingEngine`"},
		},
		{
			path:     "docs/decisions/0005-streaming-boundary.md",
			required: "Residual decoder or public API code is unsupported in v1 and must not be wired into Temporal dispatch; this decision does not claim that the residual code has been physically deleted.",
		},
		{
			path:      "docs/testing/strategy.md",
			required:  "These decoder tests do not establish a streaming API, engine dispatch path, or Temporal capability.",
			forbidden: []string{"The generic stream API remains outside the Temporal Activity/runtime boundary."},
		},
		{
			path:     "docs/testing/fixture-matrix.md",
			required: "They are not a supported v1 streaming profile:",
		},
		{
			path:     "docs/reference/source-contracts.md",
			required: "V1 now supports only one-shot `Generate` and a completed normalized response; it has no supported streaming or token-event API.",
		},
		{
			path:     "docs/superpowers/plans/2026-07-14-openai-fixture-coverage.md",
			required: "Current v1 enforcement depends on each profile's declared non-streaming capability facts; it does not require a streaming adapter, SDK stream dispatch, or Temporal runtime dispatch.",
		},
		{
			path:     "docs/superpowers/plans/2026-07-14-v1-completion.md",
			required: "V1 supports only one-shot `Generate` and a completed normalized response. It does not require a streaming adapter, SDK stream dispatch, or Temporal runtime dispatch.",
		},
		{
			path:     "docs/superpowers/plans/2026-07-14-v1-completion.md",
			required: "Task 9: [Superseded] Wire Bedrock Anthropic streaming **Status:** Out of scope for the Generate-only v1 boundary.",
		},
		{
			path:     "llm/engine.go",
			required: "Deprecated: streaming is unsupported in v1. This interface remains for source compatibility only and MUST NOT be wired into the Temporal runtime.",
		},
		{
			path:     "engine/stream.go",
			required: "Deprecated: streaming is unsupported in v1. This method remains for source compatibility only and MUST NOT be wired into the Temporal runtime.",
		},
	} {
		t.Run(test.path, func(t *testing.T) {
			base := root
			if strings.HasPrefix(test.path, "llm/") || strings.HasPrefix(test.path, "engine/") {
				base = moduleRoot(t)
			}
			data := readText(t, base, test.path)
			text := strings.Join(strings.Fields(strings.ReplaceAll(data, "// ", "")), " ")
			required := strings.Join(strings.Fields(test.required), " ")
			if !strings.Contains(text, required) {
				t.Fatalf("%s must state %q", test.path, test.required)
			}
			for _, forbidden := range test.forbidden {
				if strings.Contains(text, strings.Join(strings.Fields(forbidden), " ")) {
					t.Fatalf("%s must not claim %q", test.path, forbidden)
				}
			}
		})
	}
}

func TestProviderAdapterDocumentationListsBedrockConverseProfile(t *testing.T) {
	root := repositoryRoot(t)
	text := readNormalizedText(t, filepath.Join(root, "docs/architecture/provider-adapters.md"))
	for _, required := range []string{
		"| Amazon Bedrock Converse | AWS SDK for Go v2 `bedrockruntime` | Converse |",
		"One-shot `Converse` only; live token streaming is outside the Temporal v1 boundary",
	} {
		if !strings.Contains(text, strings.Join(strings.Fields(required), " ")) {
			t.Fatalf("provider adapter documentation must list Bedrock Converse with its one-shot boundary: missing %q", required)
		}
	}
}

// readNormalizedText reads the file at path with every run of whitespace
// collapsed to one space, so checks ignore Markdown line wrapping.
func readNormalizedText(t *testing.T, path string) string {
	t.Helper()
	data := readText(t, path)
	return strings.Join(strings.Fields(data), " ")
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

func TestDependencyBaselineDocumentsEveryVulnerabilityException(t *testing.T) {
	baselineBytes, err := os.ReadFile(filepath.Join(moduleRoot(t), "tools", "supplychainverify", "baseline.json"))
	testassert.NoError(t, err)
	var baseline struct {
		VulnerabilityExceptions []struct {
			ID      string `json:"id"`
			Owner   string `json:"owner"`
			Expires string `json:"expires"`
			Scope   string `json:"scope"`
		} `json:"vulnerability_exceptions"`
	}
	testassert.NoError(t, json.Unmarshal(baselineBytes, &baseline))
	docBytes := readText(t, repositoryRoot(t), "docs", "reference", "dependency-baseline.md")
	documented := 0
	for _, line := range strings.Split(docBytes, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "| `GO-") {
			documented++
		}
	}
	if documented != len(baseline.VulnerabilityExceptions) {
		t.Errorf("dependency baseline documents %d vulnerability exceptions, baseline.json has %d", documented, len(baseline.VulnerabilityExceptions))
	}
	for _, exception := range baseline.VulnerabilityExceptions {
		prefix := "| `" + exception.ID + "` | `" + exception.Owner + "` | `" + exception.Expires + "` |"
		found := false
		for _, line := range strings.Split(docBytes, "\n") {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, prefix) && strings.HasSuffix(trimmed, "| `"+exception.Scope+"` |") {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("dependency baseline documentation does not match exception %s (owner %s, expiry %s, scope %s)", exception.ID, exception.Owner, exception.Expires, exception.Scope)
		}
	}
}

// The direct-modules table names exactly the direct requirements of go.mod and
// carries no versions, so a version-only Dependabot update never needs a
// documentation edit while an added or removed direct module does (#1571).
func TestDependencyBaselineListsDirectModulesWithoutVersions(t *testing.T) {
	goMod, err := os.ReadFile(filepath.Join(moduleRoot(t), "go.mod"))
	testassert.NoError(t, err)
	direct := directRequirements(string(goMod))
	if len(direct) == 0 {
		t.Fatal("go.mod declares no direct requirements")
	}
	docBytes, err := os.ReadFile(filepath.Join(repositoryRoot(t), "docs", "reference", "dependency-baseline.md"))
	testassert.NoError(t, err)
	doc := string(docBytes)
	start := strings.Index(doc, "\n## Direct modules\n")
	if start < 0 {
		t.Fatal("dependency baseline has no Direct modules section")
	}
	section := doc[start+1:]
	if end := strings.Index(section[len("## "):], "\n## "); end >= 0 {
		section = section[:len("## ")+end]
	}
	documented := map[string]bool{}
	for _, line := range strings.Split(section, "\n") {
		if !strings.HasPrefix(line, "| `") {
			continue
		}
		module := strings.TrimPrefix(line, "| `")
		module = module[:strings.Index(module, "`")]
		if documented[module] {
			t.Errorf("dependency baseline lists %s more than once", module)
		}
		documented[module] = true
	}
	for module := range direct {
		if !documented[module] {
			t.Errorf("dependency baseline does not list direct requirement %s", module)
		}
	}
	for module := range documented {
		if !direct[module] {
			t.Errorf("dependency baseline lists %s, which is not a direct requirement in go.mod", module)
		}
	}
	if version := regexp.MustCompile("`v[0-9]+\\.[0-9]+\\.[0-9]+[^`]*`").FindString(section); version != "" {
		t.Errorf("dependency baseline Direct modules section records version %s; go.mod is the source of truth", version)
	}
}

func directRequirements(goMod string) map[string]bool {
	direct := map[string]bool{}
	inBlock := false
	for _, line := range strings.Split(goMod, "\n") {
		line = strings.TrimSpace(line)
		switch {
		case line == "require (":
			inBlock = true
			continue
		case inBlock && line == ")":
			inBlock = false
			continue
		case strings.HasPrefix(line, "require "):
			line = strings.TrimPrefix(line, "require ")
		case !inBlock:
			continue
		}
		if line == "" || strings.HasPrefix(line, "//") || strings.Contains(line, "// indirect") {
			continue
		}
		if fields := strings.Fields(line); len(fields) >= 2 {
			direct[fields[0]] = true
		}
	}
	return direct
}
