package architecturetest

import (
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"
)

// masterVerificationJobs are the concurrent jobs that together replace the
// former single sequential master verify job. Every one of them must gate the
// aggregating verify job, image publication, and release evidence.
var masterVerificationJobs = []string{
	"verify-static",
	"verify-race",
	"verify-race-repeat",
	"verify-cloud-workflow",
	"verify-redis",
	"verify-image",
}

const masterVerificationIf = "github.ref == 'refs/heads/master'"

// assertMasterJobNeedsEveryVerificationGate requires jobName to need the
// aggregating verify job, every split verification job, and then exactly the
// listed additional gates, in that order.
func assertMasterJobNeedsEveryVerificationGate(t *testing.T, master workflowDocument, jobName string, additional ...string) {
	t.Helper()
	want := append(append([]string{"verify"}, masterVerificationJobs...), additional...)
	got := stringSequence(t, master.name, workflowJob(t, master, jobName), "needs")
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s job %q needs = %v, want every verification gate %v", master.name, jobName, got, want)
	}
}

func TestWorkflowMasterSplitVerificationJobsGatePublication(t *testing.T) {
	master := readWorkflow(t, "master.yml")
	jobs := workflowMapping(t, master, "jobs")

	// No verification job may exist outside the gated set, so a new split job
	// cannot be added without also blocking publication.
	var declared []string
	for name := range jobs {
		if strings.HasPrefix(name, "verify-") {
			declared = append(declared, name)
		}
	}
	sort.Strings(declared)
	wantDeclared := append([]string(nil), masterVerificationJobs...)
	sort.Strings(wantDeclared)
	if !reflect.DeepEqual(declared, wantDeclared) {
		t.Fatalf("master verification jobs = %v, want %v", declared, wantDeclared)
	}

	for _, name := range masterVerificationJobs {
		job := workflowJob(t, master, name)
		// A job-level condition other than the master ref (for example a
		// schedule-only condition) would skip the job and every job needing it.
		if got := scalarString(t, master.name, job, "if"); got != masterVerificationIf {
			t.Fatalf("%s job %q if = %q, want %q", master.name, name, got, masterVerificationIf)
		}
		if _, ok := job["timeout-minutes"]; !ok {
			t.Fatalf("%s job %q does not declare a timeout", master.name, name)
		}
		if _, ok := job["needs"]; ok {
			t.Fatalf("%s job %q must start immediately rather than wait on other jobs", master.name, name)
		}
		assertJobUsesAction(t, master, name, checkoutAction)
		assertJobUsesAction(t, master, name, setupGoAction)
		_, credentialed := job["environment"]
		if credentialed != (name == "verify-image") {
			t.Fatalf("%s job %q environment = %#v; only verify-image may use docker_push", master.name, name, job["environment"])
		}
	}

	aggregate := workflowJob(t, master, "verify")
	if got := scalarString(t, master.name, aggregate, "name"); got != "Verify" {
		t.Fatalf("aggregating verify job name = %q, want Verify", got)
	}
	if got := stringSequence(t, master.name, aggregate, "needs"); !reflect.DeepEqual(got, masterVerificationJobs) {
		t.Fatalf("aggregating verify job needs = %v, want %v", got, masterVerificationJobs)
	}
	if got := scalarString(t, master.name, aggregate, "if"); got != "always() && "+masterVerificationIf {
		t.Fatalf("aggregating verify job if = %q, want it to report failure instead of being skipped", got)
	}
	assertJobRunContains(t, master, "verify", "jq -e 'length == 6 and all(.[]; .result == \"success\")'")

	assertMasterJobNeedsEveryVerificationGate(t, master, "container", "ocaml", "fuzz-shard")
	assertMasterJobNeedsEveryVerificationGate(t, master, "release-evidence", "fuzz-shard", "container")

	for _, gate := range []struct {
		job     string
		command string
	}{
		{job: "verify-static", command: "make workflow-verify"},
		{job: "verify-static", command: "scripts/check-go-format.sh"},
		{job: "verify-static", command: "make schema-verify"},
		{job: "verify-static", command: "make docs-verify"},
		{job: "verify-static", command: "make deployment-policy-verify"},
		{job: "verify-static", command: "go vet ./..."},
		{job: "verify-static", command: "make openai-smoke-check"},
		{job: "verify-static", command: "go test -tags=live ./integration/live -run '^$'"},
		{job: "verify-static", command: "make live-contract-verify"},
		{job: "verify-static", command: "make redis-benchmark-compile"},
		{job: "verify-static", command: "go build ./..."},
		{job: "verify-race", command: "go test -race ./... -timeout 30m"},
		{job: "verify-race-repeat", command: "go test -race -count=1 -timeout 30m "},
		{job: "verify-cloud-workflow", command: "make cloud-workflow-integration"},
		{job: "verify-redis", command: "make redis-integration"},
		{job: "verify-redis", command: "make readiness-integration"},
		{job: "verify-image", command: "make compose-live-integration"},
		{job: "verify-image", command: "make image-verify"},
	} {
		assertJobRunContains(t, master, gate.job, gate.command)
	}

	// Each evidence-producing job retains its own artifact; release evidence
	// merges both into the directory collect.sh reads.
	for _, evidence := range []struct {
		job      string
		artifact string
	}{
		{job: "verify-race", artifact: "verified-inputs-race"},
		{job: "verify-image", artifact: "verified-inputs-compose"},
	} {
		step := artifactUploadStep(t, master, evidence.job, evidence.artifact)
		if got := scalarString(t, master.name, step, "if"); got != "github.event_name == 'push'" {
			t.Fatalf("%s evidence upload if = %q, want push only", evidence.job, got)
		}
	}
	assertJobActionInput(t, master, "release-evidence", downloadArtifactAction, "pattern", "verified-inputs-*")
}

// TestWorkflowMasterRaceRepetitionRunsOneIterationPerJob pins the scheduled
// race repetition to independent single-iteration matrix jobs. One -count=5
// process gave each package five iterations within one timeout, which the
// growing suite outran (issue #1310).
func TestWorkflowMasterRaceRepetitionRunsOneIterationPerJob(t *testing.T) {
	master := readWorkflow(t, "master.yml")
	job := workflowJob(t, master, "verify-race-repeat")
	strategy := nestedMapping(t, master.name, job, "strategy")
	if failFast, ok := strategy["fail-fast"].(bool); !ok || failFast {
		t.Fatalf("verify-race-repeat fail-fast = %#v, want false so every repetition reports", strategy["fail-fast"])
	}
	// Only scheduled runs expand all five repetitions; other events get one
	// placeholder whose steps are all skipped.
	const wantRepetitions = "${{ github.event_name == 'schedule' && fromJSON('[1, 2, 3, 4, 5]') || fromJSON('[1]') }}"
	if got := scalarString(t, master.name, nestedMapping(t, master.name, strategy, "matrix"), "repetition"); got != wantRepetitions {
		t.Fatalf("verify-race-repeat matrix repetition = %q, want %q", got, wantRepetitions)
	}
	if strings.Contains(master.raw, "-count=5") {
		t.Fatal("master.yml must not repeat packages within one go test process")
	}
}

// TestWorkflowMasterReportsScheduledFailures requires a failed scheduled run
// to surface as an issue, because push and merge-queue runs skip the scheduled
// repetition and would otherwise hide a red daily build.
func TestWorkflowMasterReportsScheduledFailures(t *testing.T) {
	master := readWorkflow(t, "master.yml")
	job := workflowJob(t, master, "report-scheduled-failure")
	if got, want := scalarString(t, master.name, job, "if"), "failure() && github.event_name == 'schedule'"; got != want {
		t.Fatalf("report-scheduled-failure if = %q, want %q", got, want)
	}
	if got, want := stringSequence(t, master.name, job, "needs"), []string{"verify", "ocaml", "fuzz-shard", "container"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("report-scheduled-failure needs = %v, want %v", got, want)
	}
	permissions := nestedMapping(t, master.name, job, "permissions")
	if len(permissions) != 1 || scalarString(t, master.name, permissions, "issues") != "write" {
		t.Fatalf("report-scheduled-failure permissions = %#v, want only issues: write", permissions)
	}
	assertJobRunContains(t, master, "report-scheduled-failure", "gh issue create")
	assertJobRunContains(t, master, "report-scheduled-failure", "gh issue comment")
}

// TestWorkflowScheduledWorkflowsReportFailures requires every workflow with a
// schedule trigger to report a failed scheduled run as an issue. Nothing else
// watches a scheduled run, so without the report a red run goes unnoticed.
func TestWorkflowScheduledWorkflowsReportFailures(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join(repositoryRoot(t), ".github", "workflows", "*.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var scheduled []string
	for _, path := range paths {
		workflow := readWorkflow(t, filepath.Base(path))
		if _, ok := workflowMapping(t, workflow, "on")["schedule"]; !ok {
			continue
		}
		scheduled = append(scheduled, workflow.name)
		job := workflowJob(t, workflow, "report-scheduled-failure")
		if got, want := scalarString(t, workflow.name, job, "if"), "failure() && github.event_name == 'schedule'"; got != want {
			t.Fatalf("%s report-scheduled-failure if = %q, want %q", workflow.name, got, want)
		}
		// failure() only observes ancestor jobs, so every other job that can
		// run on a schedule must be a transitive dependency of the reporter.
		covered := map[string]bool{}
		pending := jobNeeds(t, workflow, "report-scheduled-failure")
		for len(pending) > 0 {
			need := pending[0]
			pending = pending[1:]
			if !covered[need] {
				covered[need] = true
				pending = append(pending, jobNeeds(t, workflow, need)...)
			}
		}
		for name := range workflowMapping(t, workflow, "jobs") {
			if name == "report-scheduled-failure" || covered[name] || !jobCanRunOnSchedule(t, workflow, name) {
				continue
			}
			t.Fatalf("%s job %q can fail on a schedule without report-scheduled-failure depending on it", workflow.name, name)
		}
		permissions := nestedMapping(t, workflow.name, job, "permissions")
		if len(permissions) != 1 || scalarString(t, workflow.name, permissions, "issues") != "write" {
			t.Fatalf("%s report-scheduled-failure permissions = %#v, want only issues: write", workflow.name, permissions)
		}
		assertJobRunContains(t, workflow, "report-scheduled-failure", "gh issue create")
		assertJobRunContains(t, workflow, "report-scheduled-failure", "gh issue comment")
	}
	// Guard against the scan silently matching nothing.
	for _, want := range []string{"master.yml", "security-scheduled.yml"} {
		if !slices.Contains(scheduled, want) {
			t.Fatalf("scheduled workflows = %v, want them to include %s", scheduled, want)
		}
	}
}

// jobNeeds returns a job's direct dependencies, accepting both the scalar and
// sequence forms of needs.
func jobNeeds(t *testing.T, workflow workflowDocument, name string) []string {
	t.Helper()
	job := workflowJob(t, workflow, name)
	switch needs := job["needs"].(type) {
	case nil:
		return nil
	case string:
		return []string{needs}
	default:
		return stringSequence(t, workflow.name, job, "needs")
	}
}

var eventNameEquality = regexp.MustCompile(`github\.event_name == '([a-z_]+)'`)

// jobCanRunOnSchedule reports false only for a job whose condition requires a
// specific non-schedule event, such as push-only release evidence.
func jobCanRunOnSchedule(t *testing.T, workflow workflowDocument, name string) bool {
	t.Helper()
	condition, _ := workflowJob(t, workflow, name)["if"].(string)
	matches := eventNameEquality.FindAllStringSubmatch(condition, -1)
	if len(matches) == 0 || strings.Contains(condition, "||") {
		return true
	}
	for _, match := range matches {
		if match[1] == "schedule" {
			return true
		}
	}
	return false
}
