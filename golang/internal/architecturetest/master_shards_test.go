package architecturetest

import (
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// masterShardCount returns the number of shards a master matrix job lists and
// requires them to be exactly 0..n-1, each reporting even when another fails.
func masterShardCount(t *testing.T, master workflowDocument, jobName string) int {
	t.Helper()
	strategy := nestedMapping(t, master.name, workflowJob(t, master, jobName), "strategy")
	if failFast, ok := strategy["fail-fast"].(bool); !ok || failFast {
		t.Fatalf("%s job %q fail-fast = %#v, want false so every shard reports", master.name, jobName, strategy["fail-fast"])
	}
	shards, ok := nestedMapping(t, master.name, strategy, "matrix")["shard"].([]any)
	if !ok || len(shards) < 2 {
		t.Fatalf("%s job %q matrix shard = %#v, want a list of at least two shards", master.name, jobName, shards)
	}
	for index, shard := range shards {
		if shard != index {
			t.Fatalf("%s job %q matrix shard = %v, want 0..%d", master.name, jobName, shards, len(shards)-1)
		}
	}
	return len(shards)
}

// commandLines runs a command and returns its non-empty output lines.
func commandLines(t *testing.T, directory, name string, args ...string) []string {
	t.Helper()
	command := exec.Command(name, args...)
	command.Dir = directory
	output, err := command.Output()
	if err != nil {
		var stderr []byte
		if exitError, ok := err.(*exec.ExitError); ok {
			stderr = exitError.Stderr
		}
		t.Fatalf("%s %s: %v: %s", name, strings.Join(args, " "), err, stderr)
	}
	var lines []string
	for _, line := range strings.Split(string(output), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

// assertShardPartition requires every shard to be non-empty and the shards
// together to hold every item exactly once.
func assertShardPartition(t *testing.T, label string, items []string, count int, shard func(index int) []string) {
	t.Helper()
	seen := make(map[string]int, len(items))
	for index := range count {
		assigned := shard(index)
		if len(assigned) == 0 {
			t.Fatalf("%s shard %d of %d is empty", label, index, count)
		}
		for _, item := range assigned {
			if previous, ok := seen[item]; ok {
				t.Fatalf("%s item %q runs in shards %d and %d", label, item, previous, index)
			}
			seen[item] = index
		}
	}
	for _, item := range items {
		if _, ok := seen[item]; !ok {
			t.Fatalf("%s item %q runs in no shard of %d", label, item, count)
		}
	}
	if len(seen) != len(items) {
		t.Fatalf("%s shards run %d items, want exactly the %d listed", label, len(seen), len(items))
	}
}

// TestWorkflowMasterShardsCoverEveryRaceAndFuzzItem keeps the sharded race
// suite and fuzz targets equivalent to the single jobs they replaced (#1570):
// every package and every fuzz target runs in exactly one shard, and release
// evidence requires a passing summary from every shard the matrix lists.
func TestWorkflowMasterShardsCoverEveryRaceAndFuzzItem(t *testing.T) {
	master := readWorkflow(t, "master.yml")
	module := filepath.Join(repositoryRoot(t), "golang")
	raceShards := masterShardCount(t, master, "verify-race")
	fuzzShards := masterShardCount(t, master, "fuzz-shard")

	assertJobRunContains(t, master, "verify-race", `bash scripts/run-race-shard.sh "$RACE_SHARD" "${{ strategy.job-total }}"`)
	assertJobRunContains(t, master, "fuzz-shard", `FUZZ_TIME=250000x bash ../scripts/release/run-gate.sh fuzz_summary`)
	assertJobRunContains(t, master, "fuzz-shard", `bash scripts/run-fuzz.sh shard "${{ matrix.shard }}" "${{ strategy.job-total }}"`)
	for _, merge := range []string{
		fmt.Sprintf("scripts/release/merge-shards.py race_summary race %d race-inputs verified-inputs/race-summary.json", raceShards),
		fmt.Sprintf("scripts/release/merge-shards.py test_summary test %d race-inputs verified-inputs/test-summary.json", raceShards),
		fmt.Sprintf("scripts/release/merge-shards.py fuzz_summary fuzz %d fuzz-inputs verified-inputs/fuzz-summary.json", fuzzShards),
	} {
		assertJobRunContains(t, master, "release-evidence", merge)
	}
	assertJobNamedStepInput(t, master, "release-evidence", "Download successful race evidence from this run", "pattern", "race-evidence-*")
	assertJobNamedStepInput(t, master, "release-evidence", "Download successful fuzz evidence from this run", "pattern", "fuzz-evidence-*")
	assertJobRunPrecedesRunContains(t, master, "release-evidence", "merge-shards.py race_summary", "bash scripts/release/collect.sh")
	for _, job := range []struct{ name, artifact, timings string }{
		{name: "verify-race", artifact: "race-evidence-${{ matrix.shard }}", timings: "race-timings-${{ matrix.shard }}.tsv"},
		{name: "fuzz-shard", artifact: "fuzz-evidence-${{ matrix.shard }}", timings: "fuzz-timings-${{ matrix.shard }}.tsv"},
	} {
		artifactUploadStep(t, master, job.name, job.artifact)
		assertJobRunContains(t, master, job.name, "bash scripts/ci/report-shard-timings.sh")
		assertJobRunContains(t, master, job.name, job.timings)
	}

	// Race work items are every package plus each test carved out of one;
	// TestWorkflowRaceShardCarvesSlowTestsIntoTheirOwnItems shows a package
	// item skips exactly the tests carved out of it.
	raceItems := commandLines(t, module, "go", "list", "./...")
	for _, line := range strings.Split(readRepositoryFile(t, module, "scripts", "race-weights.tsv"), "\n") {
		item, _, _ := strings.Cut(line, "\t")
		if !strings.HasPrefix(line, "#") && strings.Contains(item, " ") {
			raceItems = append(raceItems, item)
		}
	}
	assertShardPartition(t, "race work item", raceItems, raceShards, func(index int) []string {
		return commandLines(t, module, "bash", "scripts/run-race-shard.sh", "--list", fmt.Sprint(index), fmt.Sprint(raceShards))
	})
	targets := commandLines(t, module, "bash", "scripts/run-fuzz.sh", "list", "0", "1")
	if len(targets) < fuzzShards {
		t.Fatalf("fuzz targets = %v, want at least one per shard", targets)
	}
	assertShardPartition(t, "fuzz target", targets, fuzzShards, func(index int) []string {
		return commandLines(t, module, "bash", "scripts/run-fuzz.sh", "list", fmt.Sprint(index), fmt.Sprint(fuzzShards))
	})
}

// TestWorkflowShardBalancerAssignsByMeasuredWeight pins the balancer's
// contract: longest-processing-time-first assignment, deterministic output,
// and refusal of stale or missing weights.
func TestWorkflowShardBalancerAssignsByMeasuredWeight(t *testing.T) {
	balancer := filepath.Join(repositoryRoot(t), "scripts", "ci", "balance-shards.py")
	weights := filepath.Join(t.TempDir(), "weights.tsv")
	writeReleaseArtifact(t, weights, []byte("# measured\n./a FuzzA\t9\n./b FuzzB\t5\n./c FuzzC\t4\n./d FuzzD\t3\n./e FuzzE\t2\n"))
	run := func(stdin string, args ...string) (string, error) {
		command := exec.Command("python3", append([]string{balancer}, args...)...)
		command.Stdin = strings.NewReader(stdin)
		output, err := command.CombinedOutput()
		return string(output), err
	}
	items := "./a FuzzA\n./b FuzzB\n./c FuzzC\n./d FuzzD\n./e FuzzE\n"

	// Heaviest first onto the lightest shard: 9 -> 0, 5 -> 1, 4 -> 1, then
	// 3 -> 0 on the 9/9 tie (lowest index) and 2 -> 1.
	plan, err := run(items, "--plan", weights, "2")
	if err != nil {
		t.Fatalf("%v: %s", err, plan)
	}
	want := "0\t9\t./a FuzzA\n1\t5\t./b FuzzB\n1\t4\t./c FuzzC\n0\t3\t./d FuzzD\n1\t2\t./e FuzzE\n# shard 0 12\n# shard 1 11\n"
	if plan != want {
		t.Fatalf("plan =\n%s\nwant\n%s", plan, want)
	}
	for shard, want := range []string{"./a FuzzA\n./d FuzzD\n", "./b FuzzB\n./c FuzzC\n./e FuzzE\n"} {
		got, err := run(items, weights, fmt.Sprint(shard), "2")
		if err != nil || got != want {
			t.Fatalf("shard %d = %q, %v; want %q", shard, got, err, want)
		}
	}

	for _, refused := range []struct {
		name  string
		stdin string
		args  []string
	}{
		{name: "stale weight", stdin: "./a FuzzA\n./b FuzzB\n./c FuzzC\n./d FuzzD\n", args: []string{weights, "0", "2"}},
		{name: "unweighted item", stdin: items + "./f FuzzF\n", args: []string{weights, "0", "2"}},
		{name: "empty shard", stdin: items, args: []string{weights, "0", "6"}},
		{name: "duplicate item", stdin: items + "./a FuzzA\n", args: []string{weights, "0", "2"}},
		{name: "shard out of range", stdin: items, args: []string{weights, "2", "2"}},
	} {
		if output, err := run(refused.stdin, refused.args...); err == nil {
			t.Fatalf("%s accepted: %s", refused.name, output)
		}
	}
	// A default weight admits a new, unmeasured item such as a new package.
	if output, err := run(items+"./f FuzzF\n", "--default", "1", weights, "0", "2"); err != nil {
		t.Fatalf("default weight refused: %v: %s", err, output)
	}
}

// TestReleaseEvidenceShardMergeRequiresEverySuccessfulShard refuses any
// incomplete, extra or failed shard set and then writes no combined summary.
func TestReleaseEvidenceShardMergeRequiresEverySuccessfulShard(t *testing.T) {
	for _, scenario := range []string{"complete", "missing", "failed", "extra", "wrong kind"} {
		t.Run(scenario, func(t *testing.T) {
			dir := t.TempDir()
			names := []string{"race-0.json", "race-1.json", "race-2.json"}
			if scenario == "missing" {
				names = names[:2]
			}
			if scenario == "extra" {
				names = append(names, "race-3.json")
			}
			for i, name := range names {
				kind, status := "race_summary", "pass"
				if scenario == "failed" && i == 2 {
					status = "fail"
				}
				if scenario == "wrong kind" && i == 1 {
					kind = "fuzz_summary"
				}
				writeReleaseArtifact(t, filepath.Join(dir, name), []byte(`{"kind":"`+kind+`","status":"`+status+`","redacted":true}`))
			}
			// Timing records and the sibling test summaries are not shards.
			writeReleaseArtifact(t, filepath.Join(dir, "race-timings-0.tsv"), []byte("example.com/demo\t1\n"))
			writeReleaseArtifact(t, filepath.Join(dir, "test-0.json"), []byte(`{"kind":"test_summary","status":"pass","redacted":true}`))
			dest := filepath.Join(dir, "combined.json")
			output, err := exec.Command("python3", filepath.Join(repositoryRoot(t), "scripts/release/merge-shards.py"), "race_summary", "race", "3", dir, dest).CombinedOutput()
			if scenario == "complete" {
				if err != nil {
					t.Fatalf("%v: %s", err, output)
				}
				combined, err := os.ReadFile(dest)
				if err != nil || !strings.Contains(string(combined), `"kind": "race_summary"`) || !strings.Contains(string(combined), `"status": "pass"`) {
					t.Fatalf("combined summary = %s, %v", combined, err)
				}
				return
			}
			if err == nil {
				t.Fatal("incomplete shard evidence accepted")
			}
			if _, err := os.Stat(dest); !os.IsNotExist(err) {
				t.Fatal("invalid evidence retained")
			}
		})
	}
}

// TestWorkflowShardTimingReportPrintsOnlyTimingRecords keeps the timing report
// usable for rebalancing and silent when a shard recorded nothing.
func TestWorkflowShardTimingReportPrintsOnlyTimingRecords(t *testing.T) {
	script := filepath.Join(repositoryRoot(t), "scripts", "ci", "report-shard-timings.sh")
	dir := t.TempDir()
	timings := filepath.Join(dir, "timings.tsv")
	summary := filepath.Join(dir, "summary.md")
	writeReleaseArtifact(t, timings, []byte("./llm FuzzA\t30\n./state FuzzB\t12\n"))
	command := exec.Command("bash", script, "Fuzz target", timings)
	command.Env = append(os.Environ(), "GITHUB_STEP_SUMMARY="+summary)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("%v: %s", err, output)
	}
	if !strings.Contains(string(output), "42 in total") || !strings.Contains(string(output), "./llm FuzzA\t30") {
		t.Fatalf("timing report = %s", output)
	}
	written, err := os.ReadFile(summary)
	if err != nil || !strings.Contains(string(written), "| `./state FuzzB` | 12 |") || !strings.Contains(string(written), "| Total | 42 |") {
		t.Fatalf("step summary = %s, %v", written, err)
	}
	output, err = exec.Command("bash", script, "Fuzz target", filepath.Join(dir, "missing.tsv")).CombinedOutput()
	if err != nil || len(output) != 0 {
		t.Fatalf("missing timings report = %q, %v; want silent success", output, err)
	}
}

// raceShardStub is a stub go command for run-race-shard.sh. It lists three
// packages, lists tests TestSlow and TestFast for any -list query, records
// every other test invocation and reports each named package as passing.
const raceShardStub = `#!/usr/bin/env bash
set -euo pipefail
if [[ "$1" == list ]]; then
  printf '%s\n' example.com/m/a example.com/m/b example.com/m/heavy
  exit 0
fi
shift
if [[ " $* " == *" -list "* ]]; then
  printf '%s\n' TestSlow TestFast 'ok  	example.com/m/heavy	0.01s'
  exit 0
fi
printf '%s\n' "$*" >>"$STUB_LOG"
for argument in "$@"; do
  case "$argument" in
    example.com/*) printf 'ok  \t%s\t1.5s\n' "$argument" ;;
  esac
done
`

// runRaceShards runs every shard of run-race-shard.sh against the stub and
// returns the recorded go test invocations, the timings, and the first error.
func runRaceShards(t *testing.T, weights string, count int) ([]string, string, error) {
	t.Helper()
	dir := t.TempDir()
	stub := filepath.Join(dir, "go")
	log := filepath.Join(dir, "invocations.log")
	timings := filepath.Join(dir, "timings.tsv")
	writeReleaseArtifact(t, stub, []byte(raceShardStub))
	if err := os.Chmod(stub, 0o755); err != nil {
		t.Fatal(err)
	}
	weightsPath := filepath.Join(dir, "weights.tsv")
	writeReleaseArtifact(t, weightsPath, []byte(weights))
	script := filepath.Join(repositoryRoot(t), "golang", "scripts", "run-race-shard.sh")
	for shard := range count {
		command := exec.Command("bash", script, fmt.Sprint(shard), fmt.Sprint(count))
		command.Env = append(os.Environ(), "GO="+stub, "STUB_LOG="+log, "RACE_WEIGHTS="+weightsPath, "RACE_TIMINGS_OUTPUT="+timings)
		if output, err := command.CombinedOutput(); err != nil {
			return nil, "", fmt.Errorf("shard %d: %v: %s", shard, err, output)
		}
	}
	invocations, err := os.ReadFile(log)
	if err != nil {
		t.Fatal(err)
	}
	recorded, err := os.ReadFile(timings)
	if err != nil {
		t.Fatal(err)
	}
	return strings.Split(strings.TrimSpace(string(invocations)), "\n"), string(recorded), nil
}

// TestWorkflowRaceShardCarvesSlowTestsIntoTheirOwnItems runs the race shard
// script against a stub go command. Whole packages run once across the
// shards; a carved test runs alone and its package runs every other test;
// every work item records its duration; and a carved test or package that no
// longer exists fails the shard.
func TestWorkflowRaceShardCarvesSlowTestsIntoTheirOwnItems(t *testing.T) {
	invocations, recorded, err := runRaceShards(t, "example.com/m/heavy TestSlow\t30\nexample.com/m/heavy\t10\nexample.com/m/a\t4\n", 2)
	if err != nil {
		t.Fatal(err)
	}
	runs := map[string]int{}
	for _, invocation := range invocations {
		if !strings.HasPrefix(invocation, "-race ") || !strings.HasSuffix(invocation, " -timeout 30m") {
			t.Fatalf("race shard invocation %q is not a race run with the 30m package timeout", invocation)
		}
		fields := strings.Fields(strings.TrimSuffix(strings.TrimPrefix(invocation, "-race "), " -timeout 30m"))
		switch fields[0] {
		case "-run", "-skip":
			if len(fields) != 3 {
				t.Fatalf("selective run %q must name exactly one package", invocation)
			}
			runs[strings.Join(fields, " ")]++
		default:
			for _, field := range fields {
				runs[field]++
			}
		}
	}
	want := map[string]int{
		"example.com/m/a":                        1,
		"example.com/m/b":                        1,
		"-run ^TestSlow$ example.com/m/heavy":    1,
		"-skip ^(TestSlow)$ example.com/m/heavy": 1,
	}
	if !maps.Equal(runs, want) {
		t.Fatalf("race shard runs = %v, want %v", runs, want)
	}
	for _, line := range []string{"example.com/m/a\t1.5\n", "example.com/m/b\t1.5\n", "example.com/m/heavy TestSlow\t1.5\n", "example.com/m/heavy\t1.5\n"} {
		if !strings.Contains(recorded, line) {
			t.Fatalf("race timings = %q, want %q", recorded, line)
		}
	}

	for name, weights := range map[string]string{
		"renamed carved test":  "example.com/m/heavy TestGone\t30\n",
		"removed package":      "example.com/m/gone TestSlow\t30\n",
		"stale package weight": "example.com/m/gone\t30\n",
	} {
		if _, _, err := runRaceShards(t, weights, 2); err == nil {
			t.Fatalf("%s accepted", name)
		}
	}
}
