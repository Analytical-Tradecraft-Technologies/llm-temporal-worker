package architecturetest

import (
	"reflect"
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
		{job: "verify-race", command: "go test -race ./... -timeout 20m"},
		{job: "verify-race-repeat", command: "go test -race -count=5 -timeout 30m "},
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
