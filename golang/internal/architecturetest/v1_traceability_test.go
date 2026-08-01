package architecturetest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"unicode"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	yaml "go.yaml.in/yaml/v4"
)

const v1TraceabilityCatalogPath = "docs/release/v1-requirements.json"

var expectedV1TraceabilityIDs = []string{
	"v1.ci.green-scheduled",
	"v1.contract.adapter-golden-strict",
	"v1.contract.azure-openai-chat",
	"v1.contract.cross-protocol",
	"v1.contract.live-provider.anthropic-aws",
	"v1.contract.live-provider.anthropic-direct",
	"v1.contract.live-provider.azure-responses",
	"v1.contract.live-provider.bedrock-anthropic",
	"v1.contract.live-provider.bedrock-converse",
	"v1.contract.live-provider.exa-chat",
	"v1.contract.live-provider.openai-chat",
	"v1.contract.live-provider.openai-responses",
	"v1.contract.live-provider.openrouter-chat",
	"v1.decision.service-class.no-provider-default",
	"v1.decision.service-class.omission-normalizes-standard",
	"v1.decision.service-class.public-enum",
	"v1.deploy.runtime-smoke",
	"v1.ledger.retry-and-ambiguity",
	"v1.publication.guarded-preflight",
	"v1.publication.irreversible-operations",
	"v1.routing.deterministic-explicit-fallback",
	"v1.slo.admission-compilation-p99",
	"v1.slo.budget-overspend-known-usage",
	"v1.slo.worker-caused-error-rate",
	"v1.state.memory-redis-conformance",
	"v1.temporal.activity-lifecycle",
}

var evidenceStatusForMode = map[string]string{
	"protected_manual":                "awaiting_protected_run",
	"external_authorization_required": "authorization_required",
}

// The release-evidence artifact recorded below was produced by this exact
// protected master revision. Keep this candidate pin explicit so a later
// catalog refresh cannot accidentally retain evidence from an older run.
const expectedV1EvidenceRevision = "803be9368ea2198aab1a560353d6f18592e01fb6"
const expectedV1EvidenceWorkflowRunID int64 = 30708893192
const expectedV1EvidenceArtifactName = "release-evidence"

// GitHub exposes the artifact digest as sha256:<hex>; the catalog stores the
// normalized lowercase hex payload so it remains comparable across evidence
// sources that use the same digest algorithm.
const expectedV1EvidenceArtifactDigest = "95a8b2d16d6d2cc9d95e359e0ea3490b94a022d3a8e7e4815c26266af8ed043f"

type v1TraceabilityCatalog struct {
	SchemaVersion int                         `json:"schema_version"`
	CatalogState  string                      `json:"catalog_state"`
	Requirements  []v1TraceabilityRequirement `json:"requirements"`
}

type v1TraceabilityRequirement struct {
	ID                  string                     `json:"id"`
	Source              v1TraceabilitySource       `json:"source"`
	ImplementationPaths []string                   `json:"implementation_paths"`
	Verification        v1TraceabilityVerification `json:"verification"`
	Evidence            v1TraceabilityEvidence     `json:"evidence"`
}

type v1TraceabilitySource struct {
	Path   string `json:"path"`
	Anchor string `json:"anchor"`
	Quote  string `json:"quote"`
}

type v1TraceabilityVerification struct {
	MakeTargets  []string                    `json:"make_targets"`
	WorkflowJobs []v1TraceabilityWorkflowJob `json:"workflow_jobs"`
}

type v1TraceabilityWorkflowJob struct {
	Path string `json:"path"`
	Job  string `json:"job"`
}

type v1TraceabilityEvidence struct {
	Mode           string `json:"mode"`
	Status         string `json:"status"`
	Revision       string `json:"revision,omitempty"`
	WorkflowRunID  int64  `json:"workflow_run_id,omitempty"`
	ArtifactName   string `json:"artifact_name,omitempty"`
	ArtifactDigest string `json:"artifact_digest,omitempty"`
}

func recordedV1Evidence() v1TraceabilityEvidence {
	return v1TraceabilityEvidence{
		Mode:           "offline",
		Status:         "recorded",
		Revision:       expectedV1EvidenceRevision,
		WorkflowRunID:  expectedV1EvidenceWorkflowRunID,
		ArtifactName:   expectedV1EvidenceArtifactName,
		ArtifactDigest: expectedV1EvidenceArtifactDigest,
	}
}

func TestV1TraceabilityCatalog(t *testing.T) {
	root := repositoryRoot(t)
	if err := validateV1TraceabilityCatalog(root, readV1TraceabilityCatalog(t, root)); err != nil {
		t.Fatal(err)
	}
}

func TestV1TraceabilityAdapterGoldenRequirementUsesGenerateOnlySource(t *testing.T) {
	root := repositoryRoot(t)
	canonical, err := llm.CanonicalJSON(readV1TraceabilityCatalog(t, root))
	if err != nil {
		t.Fatalf("canonicalize catalog: %v", err)
	}
	var catalog v1TraceabilityCatalog
	if err := json.Unmarshal(canonical, &catalog); err != nil {
		t.Fatalf("decode catalog: %v", err)
	}
	const id = "v1.contract.adapter-golden-strict"
	var requirement *v1TraceabilityRequirement
	for index := range catalog.Requirements {
		if catalog.Requirements[index].ID == id {
			requirement = &catalog.Requirements[index]
			break
		}
	}
	if requirement == nil {
		t.Fatalf("catalog is missing %q", id)
	}
	if got, want := requirement.Source, (v1TraceabilitySource{
		Path:   "docs/index.md",
		Anchor: "#v1-completion-gate",
		Quote:  "Every adapter has request, response, error, and usage golden tests; strict-mode lossy conversions fail before dispatch.",
	}); got != want {
		t.Errorf("requirement %q source = %#v, want %#v", id, got, want)
	}
}

func TestV1TraceabilitySLORequirements(t *testing.T) {
	root := repositoryRoot(t)
	canonical, err := llm.CanonicalJSON(readV1TraceabilityCatalog(t, root))
	if err != nil {
		t.Fatalf("canonicalize catalog: %v", err)
	}
	var catalog v1TraceabilityCatalog
	if err := json.Unmarshal(canonical, &catalog); err != nil {
		t.Fatalf("decode catalog: %v", err)
	}

	requirements := make(map[string]v1TraceabilityRequirement, len(catalog.Requirements))
	for _, requirement := range catalog.Requirements {
		requirements[requirement.ID] = requirement
	}

	for _, want := range []v1TraceabilityRequirement{
		{
			ID: "v1.slo.admission-compilation-p99",
			Source: v1TraceabilitySource{
				Path:   "docs/scope.md",
				Anchor: "#quality-targets",
				Quote:  "Admission and compilation p99 | Under 25 ms with memory state; under 75 ms with same-region Redis",
			},
			ImplementationPaths: []string{
				"Makefile",
				"docs/release/artifact.schema.json",
				"docs/testing/strategy.md",
				"golang/engine/benchmark_redis_test.go",
				"golang/engine/benchmark_test.go",
				"scripts/release/collect.py",
				"scripts/release/collect.sh",
			},
			Verification: v1TraceabilityVerification{
				MakeTargets: []string{"benchmark", "redis-benchmark", "redis-benchmark-compile", "release-verify"},
				WorkflowJobs: []v1TraceabilityWorkflowJob{
					{Path: ".github/workflows/master.yml", Job: "release-evidence"},
					{Path: ".github/workflows/master.yml", Job: "verify"},
					{Path: ".github/workflows/pull-request.yml", Job: "verify"},
				},
			},
			Evidence: v1TraceabilityEvidence{Mode: "offline", Status: "unrecorded"},
		},
		{
			ID: "v1.slo.budget-overspend-known-usage",
			Source: v1TraceabilitySource{
				Path:   "docs/scope.md",
				Anchor: "#quality-targets",
				Quote:  "Budget overspend from known usage | Zero under the documented clock and durability assumptions",
			},
			ImplementationPaths: []string{
				"golang/storage/conformance/conformance.go",
				"golang/storage/memory/conformance_test.go",
				"golang/storage/redis/shared_state_conformance_integration_test.go",
			},
			Verification: v1TraceabilityVerification{
				MakeTargets: []string{"redis-integration"},
				WorkflowJobs: []v1TraceabilityWorkflowJob{
					{Path: ".github/workflows/master.yml", Job: "verify"},
				},
			},
			Evidence: recordedV1Evidence(),
		},
		{
			ID: "v1.slo.worker-caused-error-rate",
			Source: v1TraceabilitySource{
				Path:   "docs/scope.md",
				Anchor: "#quality-targets",
				Quote:  "Worker-caused successful-call error rate | Below 0.1%",
			},
			ImplementationPaths: []string{
				"docs/architecture/deployment-and-operations.md",
				"docs/release/artifact.schema.json",
				"golang/activity/activities.go",
				"golang/activity/metrics_test.go",
				"golang/internal/observability/metrics.go",
				"golang/tools/releaseverify/main.go",
				"scripts/release/collect.sh",
				"scripts/release/slo-evidence.py",
			},
			Verification: v1TraceabilityVerification{
				MakeTargets: []string{"test"},
				WorkflowJobs: []v1TraceabilityWorkflowJob{
					{Path: ".github/workflows/master.yml", Job: "verify"},
					{Path: ".github/workflows/pull-request.yml", Job: "verify"},
				},
			},
			Evidence: v1TraceabilityEvidence{Mode: "offline", Status: "unrecorded"},
		},
	} {
		got, exists := requirements[want.ID]
		if !exists {
			t.Errorf("catalog is missing SLO requirement %q", want.ID)
			continue
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("SLO requirement %q = %#v, want %#v", want.ID, got, want)
		}
	}
}

func TestV1TraceabilityLiveProviderRequirements(t *testing.T) {
	root := repositoryRoot(t)
	raw := readV1TraceabilityCatalog(t, root)
	canonical, err := llm.CanonicalJSON(raw)
	if err != nil {
		t.Fatalf("canonicalize catalog: %v", err)
	}
	var catalog v1TraceabilityCatalog
	if err := json.Unmarshal(canonical, &catalog); err != nil {
		t.Fatalf("decode catalog: %v", err)
	}

	requirements := make(map[string]v1TraceabilityRequirement, len(catalog.Requirements))
	for _, requirement := range catalog.Requirements {
		requirements[requirement.ID] = requirement
	}

	var gotIDs []string
	for _, profile := range liveProviderWorkflowProfiles {
		id := "v1.contract.live-provider." + profile.id
		gotIDs = append(gotIDs, id)
		requirement, ok := requirements[id]
		if !ok {
			t.Errorf("catalog is missing protected live-provider requirement %q", id)
			continue
		}
		if got, want := requirement.Source, (v1TraceabilitySource{
			Path:   "docs/reference/live-provider-contracts.md",
			Anchor: "#pinned-profiles",
			Quote:  profile.id,
		}); got != want {
			t.Errorf("requirement %q source = %#v, want %#v", id, got, want)
		}
		if got, want := requirement.ImplementationPaths, []string{
			".github/workflows/live-provider-contracts.yml",
			"golang/integration/live/harness.go",
			"golang/integration/live/runner.go",
			"golang/internal/architecturetest/live_provider_workflow_test.go",
		}; !reflect.DeepEqual(got, want) {
			t.Errorf("requirement %q implementation paths = %#v, want %#v", id, got, want)
		}
		if got, want := requirement.Verification, (v1TraceabilityVerification{
			MakeTargets: []string{"test", "workflow-verify"},
			WorkflowJobs: []v1TraceabilityWorkflowJob{
				{Path: ".github/workflows/live-provider-contracts.yml", Job: profile.id},
				{Path: ".github/workflows/master.yml", Job: "verify"},
				{Path: ".github/workflows/pull-request.yml", Job: "verify"},
			},
		}); !reflect.DeepEqual(got, want) {
			t.Errorf("requirement %q verification = %#v, want %#v", id, got, want)
		}
		if got, want := requirement.Evidence, (v1TraceabilityEvidence{Mode: "protected_manual", Status: "awaiting_protected_run"}); got != want {
			t.Errorf("requirement %q evidence = %#v, want %#v", id, got, want)
		}
	}
	sort.Strings(gotIDs)

	var catalogIDs []string
	for _, requirement := range catalog.Requirements {
		if strings.HasPrefix(requirement.ID, "v1.contract.live-provider.") {
			catalogIDs = append(catalogIDs, requirement.ID)
		}
	}
	if !reflect.DeepEqual(catalogIDs, gotIDs) {
		t.Errorf("catalog protected live-provider IDs = %#v, want %#v", catalogIDs, gotIDs)
	}
}

func TestV1TraceabilityCatalogRejectsDuplicateJSONKeys(t *testing.T) {
	root := repositoryRoot(t)
	raw := string(readV1TraceabilityCatalog(t, root))

	for _, test := range []struct {
		name   string
		before string
		after  string
	}{
		{
			name:   "root catalog state where final value is valid",
			before: "\"catalog_state\": \"partial\",",
			after:  "\"catalog_state\": \"complete\",\n  \"catalog_state\": \"partial\",",
		},
		{
			name:   "nested evidence status where final value is valid",
			before: "\"mode\": \"offline\",\n        \"status\": \"recorded\"",
			after:  "\"mode\": \"offline\",\n        \"status\": \"authorization_required\",\n        \"status\": \"recorded\"",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			mutated := strings.Replace(raw, test.before, test.after, 1)
			if mutated == raw {
				t.Fatalf("duplicate-key fixture %q did not match the catalog", test.name)
			}
			if err := validateV1TraceabilityCatalog(root, []byte(mutated)); err == nil {
				t.Fatalf("catalog validator accepted %s", test.name)
			}
		})
	}
}

func TestV1TraceabilityMakeTarget(t *testing.T) {
	targets, err := readMakeTargets(moduleRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := targets["traceability-verify"]; !exists {
		t.Fatal("Makefile must define traceability-verify")
	}
	makefile, err := os.ReadFile(filepath.Join(moduleRoot(t), "Makefile"))
	if err != nil {
		t.Fatalf("read Makefile: %v", err)
	}
	const want = "traceability-verify:\n\t$(GO) test ./internal/architecturetest -run '^TestV1Traceability' -count=1"
	if !strings.Contains(string(makefile), want) {
		t.Fatalf("traceability-verify must run the complete traceability suite; want %q", want)
	}
}

func TestV1TraceabilityCatalogRejectsBrokenStaticRecords(t *testing.T) {
	root := repositoryRoot(t)
	raw := readV1TraceabilityCatalog(t, root)

	for _, test := range []struct {
		name   string
		mutate func(t *testing.T, document map[string]any)
	}{
		{
			name: "unknown field",
			mutate: func(_ *testing.T, document map[string]any) {
				document["unexpected"] = true
			},
		},
		{
			name: "duplicate ID",
			mutate: func(t *testing.T, document map[string]any) {
				requirementAt(t, document, 1)["id"] = requirementAt(t, document, 0)["id"]
			},
		},
		{
			name: "unsorted IDs",
			mutate: func(t *testing.T, document map[string]any) {
				requirements := catalogRequirements(t, document)
				requirements[1], requirements[2] = requirements[2], requirements[1]
			},
		},
		{
			name: "escaping source path",
			mutate: func(t *testing.T, document map[string]any) {
				catalogSource(t, requirementAt(t, document, 0))["path"] = "../outside.md"
			},
		},
		{
			name: "missing source heading",
			mutate: func(t *testing.T, document map[string]any) {
				catalogSource(t, requirementAt(t, document, 0))["anchor"] = "#not-a-heading"
			},
		},
		{
			name: "missing source quote",
			mutate: func(t *testing.T, document map[string]any) {
				catalogSource(t, requirementAt(t, document, 0))["quote"] = "This quote is not present in the checked-in source."
			},
		},
		{
			name: "missing implementation path",
			mutate: func(t *testing.T, document map[string]any) {
				requirementAt(t, document, 0)["implementation_paths"] = []any{"missing/implementation.go"}
			},
		},
		{
			name: "non-repository implementation path",
			mutate: func(t *testing.T, document map[string]any) {
				requirementAt(t, document, 0)["implementation_paths"] = []any{"/etc/passwd"}
			},
		},
		{
			name: "missing Make target",
			mutate: func(t *testing.T, document map[string]any) {
				catalogVerification(t, requirementAt(t, document, 0))["make_targets"] = []any{"not-a-make-target"}
			},
		},
		{
			name: "missing workflow job",
			mutate: func(t *testing.T, document map[string]any) {
				catalogVerification(t, requirementAt(t, document, 0))["workflow_jobs"] = []any{map[string]any{
					"path": ".github/workflows/master.yml",
					"job":  "not-a-workflow-job",
				}}
			},
		},
		{
			name: "unsafe artifact reference",
			mutate: func(t *testing.T, document map[string]any) {
				requirementAt(t, document, 0)["artifact"] = "release-artifacts/evidence.json"
			},
		},
		{
			name: "invalid evidence mode status combination",
			mutate: func(t *testing.T, document map[string]any) {
				catalogEvidence(t, requirementAt(t, document, 0))["status"] = "authorization_required"
			},
		},
		{
			name: "candidate field",
			mutate: func(t *testing.T, document map[string]any) {
				requirementAt(t, document, 0)["candidate_sha"] = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
			},
		},
		{
			name: "provider run field",
			mutate: func(t *testing.T, document map[string]any) {
				requirementAt(t, document, 0)["provider_run_id"] = "opaque-run-id"
			},
		},
		{
			name: "publication field",
			mutate: func(t *testing.T, document map[string]any) {
				requirementAt(t, document, 0)["publication_status"] = "published"
			},
		},
		{
			name: "result field",
			mutate: func(t *testing.T, document map[string]any) {
				requirementAt(t, document, 0)["result"] = "passed"
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			mutated := mutateV1TraceabilityCatalog(t, raw, test.mutate)
			if err := validateV1TraceabilityCatalog(root, mutated); err == nil {
				t.Fatalf("catalog validator accepted %s", test.name)
			}
		})
	}
}

func TestV1TraceabilityRejectsStaleOfflineEvidenceRevision(t *testing.T) {
	root := repositoryRoot(t)
	raw := readV1TraceabilityCatalog(t, root)
	mutated := mutateV1TraceabilityCatalog(t, raw, func(t *testing.T, document map[string]any) {
		evidence := catalogEvidence(t, requirementAt(t, document, 0))
		evidence["revision"] = "2456e83a8f4e6f9f6b54b7b6e06ccf54886f9f3b"
	})
	if err := validateV1TraceabilityCatalog(root, mutated); err == nil {
		t.Fatal("catalog validator accepted offline evidence from a stale revision")
	}
}

func readV1TraceabilityCatalog(t *testing.T, root string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(v1TraceabilityCatalogPath)))
	if err != nil {
		t.Fatalf("read v1 traceability catalog: %v", err)
	}
	return data
}

func mutateV1TraceabilityCatalog(t *testing.T, raw []byte, mutate func(t *testing.T, document map[string]any)) []byte {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var document map[string]any
	if err := decoder.Decode(&document); err != nil {
		t.Fatalf("decode catalog mutation fixture: %v", err)
	}
	mutate(t, document)
	data, err := json.Marshal(document)
	if err != nil {
		t.Fatalf("encode catalog mutation fixture: %v", err)
	}
	return data
}

func catalogRequirements(t *testing.T, document map[string]any) []any {
	t.Helper()
	requirements, ok := document["requirements"].([]any)
	if !ok {
		t.Fatalf("requirements = %#v, want array", document["requirements"])
	}
	return requirements
}

func requirementAt(t *testing.T, document map[string]any, index int) map[string]any {
	t.Helper()
	requirements := catalogRequirements(t, document)
	if index >= len(requirements) {
		t.Fatalf("requirement index %d out of range", index)
	}
	requirement, ok := requirements[index].(map[string]any)
	if !ok {
		t.Fatalf("requirement %d = %#v, want object", index, requirements[index])
	}
	return requirement
}

func catalogSource(t *testing.T, requirement map[string]any) map[string]any {
	t.Helper()
	source, ok := requirement["source"].(map[string]any)
	if !ok {
		t.Fatalf("source = %#v, want object", requirement["source"])
	}
	return source
}

func catalogVerification(t *testing.T, requirement map[string]any) map[string]any {
	t.Helper()
	verification, ok := requirement["verification"].(map[string]any)
	if !ok {
		t.Fatalf("verification = %#v, want object", requirement["verification"])
	}
	return verification
}

func catalogEvidence(t *testing.T, requirement map[string]any) map[string]any {
	t.Helper()
	evidence, ok := requirement["evidence"].(map[string]any)
	if !ok {
		t.Fatalf("evidence = %#v, want object", requirement["evidence"])
	}
	return evidence
}

func validateV1TraceabilityCatalog(root string, raw []byte) error {
	// Decode raw bytes through the duplicate-key-safe canonicalizer before any
	// map or struct decoder can collapse an earlier static evidence claim.
	canonical, err := llm.CanonicalJSON(raw)
	if err != nil {
		return fmt.Errorf("canonicalize catalog: %w", err)
	}
	if err := rejectStaticEvidenceClaims(canonical); err != nil {
		return err
	}

	decoder := json.NewDecoder(bytes.NewReader(canonical))
	decoder.DisallowUnknownFields()
	var catalog v1TraceabilityCatalog
	if err := decoder.Decode(&catalog); err != nil {
		return fmt.Errorf("decode catalog: %w", err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return err
	}
	if catalog.SchemaVersion != 1 {
		return fmt.Errorf("schema_version = %d, want 1", catalog.SchemaVersion)
	}
	if catalog.CatalogState != "partial" {
		return fmt.Errorf("catalog_state = %q, want partial", catalog.CatalogState)
	}
	if err := validateRequirementIDs(catalog.Requirements); err != nil {
		return err
	}

	makeTargets, err := readMakeTargets(filepath.Join(root, "golang"))
	if err != nil {
		return err
	}
	for _, requirement := range catalog.Requirements {
		if err := validateV1TraceabilityRequirement(root, makeTargets, requirement); err != nil {
			return fmt.Errorf("requirement %q: %w", requirement.ID, err)
		}
	}
	return nil
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("catalog contains more than one JSON value")
		}
		return fmt.Errorf("read trailing catalog data: %w", err)
	}
	return nil
}

func validateRequirementIDs(requirements []v1TraceabilityRequirement) error {
	if len(requirements) != len(expectedV1TraceabilityIDs) {
		return fmt.Errorf("requirement count = %d, want %d", len(requirements), len(expectedV1TraceabilityIDs))
	}
	previous := ""
	seen := make(map[string]struct{}, len(requirements))
	for index, requirement := range requirements {
		if !validV1RequirementID(requirement.ID) {
			return fmt.Errorf("requirement %d has invalid id %q", index, requirement.ID)
		}
		if _, exists := seen[requirement.ID]; exists {
			return fmt.Errorf("duplicate requirement id %q", requirement.ID)
		}
		if previous != "" && requirement.ID < previous {
			return fmt.Errorf("requirement ids are not sorted: %q precedes %q", requirement.ID, previous)
		}
		if requirement.ID != expectedV1TraceabilityIDs[index] {
			return fmt.Errorf("requirement id at index %d = %q, want %q", index, requirement.ID, expectedV1TraceabilityIDs[index])
		}
		seen[requirement.ID] = struct{}{}
		previous = requirement.ID
	}
	return nil
}

func validV1RequirementID(id string) bool {
	if id == "" {
		return false
	}
	for _, character := range id {
		if !((character >= 'a' && character <= 'z') || (character >= '0' && character <= '9') || character == '.' || character == '-') {
			return false
		}
	}
	return true
}

func validateV1TraceabilityRequirement(root string, makeTargets map[string]struct{}, requirement v1TraceabilityRequirement) error {
	if err := validateTraceabilitySource(root, requirement.Source); err != nil {
		return err
	}
	if err := validateSortedStrings("implementation_paths", requirement.ImplementationPaths, func(reference string) error {
		_, err := resolveRepositoryPath(root, reference, false)
		return err
	}); err != nil {
		return err
	}
	if err := validateSortedStrings("verification.make_targets", requirement.Verification.MakeTargets, func(target string) error {
		if _, exists := makeTargets[target]; !exists {
			return fmt.Errorf("Make target %q does not exist", target)
		}
		return nil
	}); err != nil {
		return err
	}
	if err := validateWorkflowJobs(root, requirement.Verification.WorkflowJobs); err != nil {
		return err
	}
	if requirement.Evidence.Mode == "offline" {
		switch requirement.Evidence.Status {
		case "recorded":
			if requirement.Evidence.Revision != expectedV1EvidenceRevision {
				return fmt.Errorf("offline evidence revision = %q, want %q", requirement.Evidence.Revision, expectedV1EvidenceRevision)
			}
			if requirement.Evidence.WorkflowRunID != expectedV1EvidenceWorkflowRunID {
				return fmt.Errorf("offline evidence workflow_run_id = %d, want %d", requirement.Evidence.WorkflowRunID, expectedV1EvidenceWorkflowRunID)
			}
			if requirement.Evidence.ArtifactName != expectedV1EvidenceArtifactName {
				return fmt.Errorf("offline evidence artifact_name = %q, want %q", requirement.Evidence.ArtifactName, expectedV1EvidenceArtifactName)
			}
			if requirement.Evidence.ArtifactDigest != expectedV1EvidenceArtifactDigest {
				return fmt.Errorf("offline evidence artifact_digest = %q, want %q", requirement.Evidence.ArtifactDigest, expectedV1EvidenceArtifactDigest)
			}
		case "unrecorded":
			if requirement.Evidence.Revision != "" || requirement.Evidence.WorkflowRunID != 0 || requirement.Evidence.ArtifactName != "" || requirement.Evidence.ArtifactDigest != "" {
				return fmt.Errorf("unrecorded offline evidence must not contain release artifact fields")
			}
		default:
			return fmt.Errorf("offline evidence requires status %q or %q, got %q", "recorded", "unrecorded", requirement.Evidence.Status)
		}
		return nil
	}
	if expectedStatus, exists := evidenceStatusForMode[requirement.Evidence.Mode]; !exists {
		return fmt.Errorf("evidence mode %q is not allowed", requirement.Evidence.Mode)
	} else if requirement.Evidence.Status != expectedStatus {
		return fmt.Errorf("evidence mode %q requires status %q, got %q", requirement.Evidence.Mode, expectedStatus, requirement.Evidence.Status)
	} else if requirement.Evidence.Revision != "" || requirement.Evidence.WorkflowRunID != 0 || requirement.Evidence.ArtifactName != "" || requirement.Evidence.ArtifactDigest != "" {
		return fmt.Errorf("non-offline evidence must not contain release artifact fields")
	}
	return nil
}

func validateTraceabilitySource(root string, source v1TraceabilitySource) error {
	if path.Ext(source.Path) != ".md" {
		return fmt.Errorf("source path %q must name a Markdown file", source.Path)
	}
	sourcePath, err := resolveRepositoryPath(root, source.Path, true)
	if err != nil {
		return fmt.Errorf("source path: %w", err)
	}
	if !strings.HasPrefix(source.Anchor, "#") || source.Anchor == "#" {
		return fmt.Errorf("source anchor %q is not a Markdown anchor", source.Anchor)
	}
	if normalizeMarkdownQuote(source.Quote) != source.Quote || source.Quote == "" {
		return fmt.Errorf("source quote must be a nonempty normalized string")
	}
	data, err := os.ReadFile(sourcePath)
	if err != nil {
		return fmt.Errorf("read source %q: %w", source.Path, err)
	}
	section, err := markdownSectionForAnchor(string(data), source.Anchor)
	if err != nil {
		return fmt.Errorf("source %q: %w", source.Path, err)
	}
	if !strings.Contains(normalizeMarkdownQuote(section), source.Quote) {
		return fmt.Errorf("source quote is absent from %s%s", source.Path, source.Anchor)
	}
	return nil
}

func validateSortedStrings(label string, values []string, validate func(string) error) error {
	if len(values) == 0 {
		return fmt.Errorf("%s must not be empty", label)
	}
	previous := ""
	for _, value := range values {
		if value == "" {
			return fmt.Errorf("%s contains an empty value", label)
		}
		if previous != "" && value <= previous {
			return fmt.Errorf("%s must be sorted and unique: %q follows %q", label, value, previous)
		}
		if err := validate(value); err != nil {
			return fmt.Errorf("%s %q: %w", label, value, err)
		}
		previous = value
	}
	return nil
}

func validateWorkflowJobs(root string, jobs []v1TraceabilityWorkflowJob) error {
	if len(jobs) == 0 {
		return fmt.Errorf("verification.workflow_jobs must not be empty")
	}
	previous := ""
	for _, job := range jobs {
		if job.Job == "" {
			return fmt.Errorf("workflow job is empty")
		}
		key := job.Path + "\x00" + job.Job
		if previous != "" && key <= previous {
			return fmt.Errorf("workflow jobs must be sorted and unique: %q follows %q", key, previous)
		}
		if !strings.HasPrefix(job.Path, ".github/workflows/") || path.Ext(job.Path) != ".yml" {
			return fmt.Errorf("workflow path %q is not a checked-in workflow", job.Path)
		}
		workflowPath, err := resolveRepositoryPath(root, job.Path, true)
		if err != nil {
			return fmt.Errorf("workflow path %q: %w", job.Path, err)
		}
		data, err := os.ReadFile(workflowPath)
		if err != nil {
			return fmt.Errorf("read workflow %q: %w", job.Path, err)
		}
		var workflow struct {
			Jobs map[string]any `yaml:"jobs"`
		}
		if err := yaml.Unmarshal(data, &workflow); err != nil {
			return fmt.Errorf("parse workflow %q: %w", job.Path, err)
		}
		if _, exists := workflow.Jobs[job.Job]; !exists {
			return fmt.Errorf("workflow %q has no job %q", job.Path, job.Job)
		}
		previous = key
	}
	return nil
}

func readMakeTargets(root string) (map[string]struct{}, error) {
	makefilePath, err := resolveRepositoryPath(root, "Makefile", true)
	if err != nil {
		return nil, fmt.Errorf("Makefile: %w", err)
	}
	data, err := os.ReadFile(makefilePath)
	if err != nil {
		return nil, fmt.Errorf("read Makefile: %w", err)
	}
	targets := make(map[string]struct{})
	for _, line := range strings.Split(string(data), "\n") {
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "\t") || strings.HasPrefix(line, " ") {
			continue
		}
		name, _, found := strings.Cut(line, ":")
		if !found || name == ".PHONY" || !validMakeTargetName(name) {
			continue
		}
		targets[name] = struct{}{}
	}
	return targets, nil
}

func validMakeTargetName(name string) bool {
	if name == "" {
		return false
	}
	for _, character := range name {
		if !((character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') || (character >= '0' && character <= '9') || character == '.' || character == '_' || character == '-') {
			return false
		}
	}
	return true
}

func resolveRepositoryPath(root, reference string, requireRegularFile bool) (string, error) {
	if err := validateRepositoryReference(reference); err != nil {
		return "", err
	}
	joined := filepath.Join(root, filepath.FromSlash(reference))
	entry, err := os.Lstat(joined)
	if err != nil {
		return "", fmt.Errorf("does not exist: %w", err)
	}
	if entry.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("must not be a symlink")
	}
	canonicalRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", fmt.Errorf("resolve repository root: %w", err)
	}
	canonicalPath, err := filepath.EvalSymlinks(joined)
	if err != nil {
		return "", fmt.Errorf("resolve path: %w", err)
	}
	relative, err := filepath.Rel(canonicalRoot, canonicalPath)
	if err != nil {
		return "", fmt.Errorf("compare repository path: %w", err)
	}
	if relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) || filepath.IsAbs(relative) {
		return "", fmt.Errorf("escapes repository")
	}
	info, err := os.Stat(canonicalPath)
	if err != nil {
		return "", fmt.Errorf("stat path: %w", err)
	}
	if requireRegularFile && !info.Mode().IsRegular() {
		return "", fmt.Errorf("must be a regular file")
	}
	return canonicalPath, nil
}

func validateRepositoryReference(reference string) error {
	if reference == "" || strings.ContainsRune(reference, 0) || strings.Contains(reference, "\\") {
		return fmt.Errorf("is not a safe repository-relative path")
	}
	if path.IsAbs(reference) || filepath.IsAbs(reference) || path.Clean(reference) != reference || reference == "." || reference == ".." || strings.HasPrefix(reference, "../") {
		return fmt.Errorf("is not a canonical repository-relative path")
	}
	return nil
}

func markdownSectionForAnchor(markdown, wantAnchor string) (string, error) {
	lines := strings.Split(markdown, "\n")
	found := false
	level := 0
	section := make([]string, 0, len(lines))
	for _, line := range lines {
		headingLevel, title, isHeading := markdownHeading(line)
		if isHeading {
			if found && headingLevel <= level {
				break
			}
			if !found && markdownAnchor(title) == wantAnchor {
				found = true
				level = headingLevel
			}
		}
		if found {
			section = append(section, line)
		}
	}
	if !found {
		return "", fmt.Errorf("does not contain heading %q", wantAnchor)
	}
	return strings.Join(section, "\n"), nil
}

func markdownHeading(line string) (int, string, bool) {
	line = strings.TrimSpace(line)
	level := 0
	for level < len(line) && line[level] == '#' {
		level++
	}
	if level == 0 || level > 6 || len(line) <= level || (line[level] != ' ' && line[level] != '\t') {
		return 0, "", false
	}
	title := strings.TrimSpace(line[level:])
	title = strings.TrimSpace(strings.TrimRight(title, "#"))
	if title == "" {
		return 0, "", false
	}
	return level, title, true
}

func markdownAnchor(title string) string {
	var builder strings.Builder
	pendingDash := false
	for _, character := range strings.ToLower(title) {
		switch {
		case unicode.IsLetter(character), unicode.IsDigit(character):
			if pendingDash && builder.Len() > 0 {
				builder.WriteByte('-')
			}
			builder.WriteRune(character)
			pendingDash = false
		case character == ' ' || character == '-':
			pendingDash = true
		}
	}
	return "#" + builder.String()
}

func normalizeMarkdownQuote(value string) string {
	return strings.Join(strings.Fields(strings.ReplaceAll(value, "`", "")), " ")
}

func rejectStaticEvidenceClaims(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return fmt.Errorf("decode static catalog fields: %w", err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return err
	}
	return rejectForbiddenCatalogFields(value, "")
}

func rejectForbiddenCatalogFields(value any, location string) error {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			fieldLocation := key
			if location != "" {
				fieldLocation = location + "." + key
			}
			if forbiddenStaticCatalogField(key) && !allowedRecordedEvidenceField(location, key) {
				return fmt.Errorf("static catalog must not contain evidence claim field %q", fieldLocation)
			}
			if err := rejectForbiddenCatalogFields(child, fieldLocation); err != nil {
				return err
			}
		}
	case []any:
		for index, child := range typed {
			if err := rejectForbiddenCatalogFields(child, fmt.Sprintf("%s[%d]", location, index)); err != nil {
				return err
			}
		}
	}
	return nil
}

func allowedRecordedEvidenceField(location, field string) bool {
	if !strings.HasSuffix(location, ".evidence") {
		return false
	}
	switch field {
	case "revision", "workflow_run_id", "artifact_name", "artifact_digest":
		return true
	default:
		return false
	}
}

func forbiddenStaticCatalogField(field string) bool {
	normalized := strings.ToLower(strings.ReplaceAll(field, "-", "_"))
	for _, forbidden := range []string{
		"artifact", "candidate", "result", "run", "commit", "revision", "release", "tag", "sign", "publish",
	} {
		if strings.Contains(normalized, forbidden) {
			return true
		}
	}
	switch normalized {
	case "sha", "provider_invocation", "provider_request":
		return true
	default:
		return strings.HasSuffix(normalized, "_sha")
	}
}
