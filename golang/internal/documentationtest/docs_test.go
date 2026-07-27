package documentationtest

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

var markdownLink = regexp.MustCompile(`(?m)(?:^|[^!])\[[^\]]+\]\(([^)]+)\)`)

func TestDocumentationLinksAndInvariants(t *testing.T) {
	root := repositoryRoot(t)
	makefile, err := os.ReadFile(filepath.Join(moduleRoot(t), "Makefile"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(makefile), "test ./internal/documentationtest -run") {
		t.Fatal("docs-verify must not filter documentation tests")
	}
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
	err = filepath.WalkDir(filepath.Join(root, "docs"), func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".md") {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range files {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		text := string(data)
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
			if err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(resolved); err != nil {
				t.Fatalf("%s links to missing target %s: %s", path, raw, err)
			}
		}
	}
	decision, err := os.ReadFile(filepath.Join(root, "docs/decisions/0002-service-classes.md"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(decision), "economy | standard | priority") != 1 {
		t.Fatal("service-class decision must state the exact public enum once")
	}
}

func TestActivityRuntimeReferencesCurrentQueryPlan(t *testing.T) {
	root := repositoryRoot(t)
	path := filepath.Join(root, "docs/reference/activity-runtime.md")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	text := strings.Join(strings.Fields(string(data)), " ")
	const taskLink = "[Task 14, typed Query service and Temporal Activity, of the forkable conversation-state plan](../superpowers/plans/2026-07-18-forkable-conversation-state.md#task-14-implement-typed-query-service-and-temporal-activity)"
	if !strings.Contains(text, taskLink) {
		t.Fatalf("%s must link Task 14 to the current forkable conversation-state plan", path)
	}
	if strings.Contains(text, "Task 14 of the v1 plan") {
		t.Fatalf("%s contains the stale v1-plan Task 14 reference", path)
	}
	if !strings.Contains(text, "[query execution audit ledger](query-audit-ledger.md)") {
		t.Fatalf("%s must link the query execution audit ledger", path)
	}
}

func TestOCamlArchitectureDocumentsModelInventoryWireStatus(t *testing.T) {
	root := repositoryRoot(t)
	path := filepath.Join(root, "docs/architecture/ocaml-conversation-and-query-client.md")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	text := strings.Join(strings.Fields(string(data)), " ")
	for _, required := range []string{
		"members are part of the target Task 14 query-result contract",
		"not yet emitted or accepted by the pre-query wire scaffold",
		"provider-control.md",
		"Unknown_inventory_source",
		"Safe_metadata.empty",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("%s must document model-inventory wire status with %q", path, required)
		}
	}
}

func TestLiveProviderDocumentationSeparatesManualWorkflowFromRelease(t *testing.T) {
	root := repositoryRoot(t)
	data, err := os.ReadFile(filepath.Join(root, "docs/testing/strategy.md"))
	if err != nil {
		t.Fatal(err)
	}
	text := strings.Join(strings.Fields(string(data)), " ")
	if !strings.Contains(text, "protected manual live-provider workflow") {
		t.Fatal("live-provider documentation must name the protected manual live-provider workflow")
	}
	if !strings.Contains(text, "redacted live-provider evidence") {
		t.Fatal("live-provider documentation must distinguish its redacted evidence")
	}
	if strings.Contains(text, "protected manual release workflow") {
		t.Fatal("live-provider workflow must not be documented as a release workflow")
	}
}

func TestReadinessDocumentationStatesRedisPrefixBoundary(t *testing.T) {
	root := repositoryRoot(t)
	for _, relative := range []string{
		"docs/reference/configuration.md",
		"docs/architecture/deployment-and-operations.md",
	} {
		t.Run(relative, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join(root, relative))
			if err != nil {
				t.Fatal(err)
			}
			text := strings.Join(strings.Fields(string(data)), " ")
			if !strings.Contains(text, "active budget-generation pointer") ||
				!strings.Contains(text, "canonical, complete manifest") {
				t.Fatalf("%s must state that durable readiness verifies the active budget generation and canonical manifest", relative)
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
			data, err := os.ReadFile(filepath.Join(root, relative))
			if err != nil {
				t.Fatal(err)
			}
			for _, line := range strings.Split(string(data), "\n") {
				if strings.HasPrefix(strings.TrimSpace(line), "docker build ") {
					t.Fatalf("%s must build from the nested golang module context: %s", relative, strings.TrimSpace(line))
				}
			}
		})
	}
}

func TestV1DocumentationStatesGenerateOnlyBoundary(t *testing.T) {
	root := repositoryRoot(t)
	for _, test := range []struct {
		path      string
		required  string
		forbidden []string
	}{
		{
			path:      "docs/index.md",
			required:  "The v1 public contract exposes only one-shot `Generate` and a final normalized response; live streaming and token-event APIs are not supported",
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
			path:     "docs/architecture/system-overview.md",
			required: "`Engine.Generate` is the only supported v1 inference entry point.",
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
			data, err := os.ReadFile(filepath.Join(base, test.path))
			if err != nil {
				t.Fatal(err)
			}
			text := strings.Join(strings.Fields(strings.ReplaceAll(string(data), "// ", "")), " ")
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

func repositoryRoot(t *testing.T) string {
	t.Helper()
	directory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(directory, ".git")); err == nil {
			return directory
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			t.Fatal("repository checkout root not found")
		}
		directory = parent
	}
}

func moduleRoot(t *testing.T) string {
	t.Helper()
	return filepath.Join(repositoryRoot(t), "golang")
}
