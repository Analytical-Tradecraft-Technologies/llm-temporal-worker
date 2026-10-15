package architecturetest

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestReleaseEvidenceGateCapturesOnlySuccessfulRedactedResults(t *testing.T) {
	for _, status := range []string{"0", "9"} {
		t.Run(status, func(t *testing.T) {
			summary := filepath.Join(t.TempDir(), "nested", "race-summary.json")
			output, err := exec.Command("bash", filepath.Join(repositoryRoot(t), "scripts/release/run-gate.sh"), "race_summary", summary, "bash", "-c", "echo secret-sentinel; exit "+status).CombinedOutput()
			if strings.Contains(string(output), "secret-sentinel") {
				t.Fatal("raw output leaked")
			}
			if status != "0" {
				if err == nil {
					t.Fatal("failed gate accepted")
				}
				if _, err := os.Stat(summary); !os.IsNotExist(err) {
					t.Fatal("failed gate retained a summary")
				}
				return
			}
			if err != nil {
				t.Fatalf("%v: %s", err, output)
			}
			data, err := os.ReadFile(summary)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(data), `"status":"pass"`) {
				t.Fatalf("invalid summary %s", data)
			}
			if !strings.Contains(string(output), "passed (") {
				t.Fatal("missing elapsed time")
			}
		})
	}
}

func TestReleaseEvidenceGateFailurePrintsAllowlistedDiagnostics(t *testing.T) {
	// Assembled at runtime so the repository source scan does not flag it.
	credential := "gh" + "p_" + strings.Repeat("A1", 15)
	for _, scenario := range []string{"scanned", "credential"} {
		t.Run(scenario, func(t *testing.T) {
			message := "length 5 exceeds 4"
			if scenario == "credential" {
				message = "unexpected token " + credential
			}
			lines := []string{
				"fuzz ./llm FuzzDemo for 250000x",
				"secret-sentinel",
				"--- FAIL: FuzzDemo (0.01s)",
				"    --- FAIL: FuzzDemo (0.00s)",
				"        demo_test.go:9: " + message,
				"    Failing input written to testdata/fuzz/FuzzDemo/f34630c44c11bb13",
				"    go test -run=FuzzDemo/f34630c44c11bb13",
				"FAIL\texample.com/demo\t0.36s",
			}
			summary := filepath.Join(t.TempDir(), "fuzz-summary.json")
			command := exec.Command("bash", filepath.Join(repositoryRoot(t), "scripts/release/run-gate.sh"), "fuzz_summary", summary, "bash", "-c", `printf '%s\n' "$GATE_LINES"; exit 1`)
			command.Env = append(os.Environ(), "GATE_LINES="+strings.Join(lines, "\n"))
			output, err := command.CombinedOutput()
			if err == nil {
				t.Fatal("failed gate accepted")
			}
			text := string(output)
			for _, forbidden := range []string{"secret-sentinel", credential} {
				if strings.Contains(text, forbidden) {
					t.Fatalf("unallowlisted output leaked: %s", text)
				}
			}
			for _, expected := range []string{
				"fuzz ./llm FuzzDemo for 250000x",
				"--- FAIL: FuzzDemo (0.01s)",
				"Failing input written to testdata/fuzz/FuzzDemo/f34630c44c11bb13",
				"go test -run=FuzzDemo/f34630c44c11bb13",
				"FAIL\texample.com/demo\t0.36s",
			} {
				if !strings.Contains(text, expected) {
					t.Fatalf("diagnostics omit %q: %s", expected, text)
				}
			}
			detail := strings.Contains(text, "demo_test.go:9: length 5 exceeds 4")
			if scenario == "scanned" && !detail {
				t.Fatalf("scanned failure message withheld: %s", text)
			}
			if scenario == "credential" && (strings.Contains(text, "demo_test.go:9:") || !strings.Contains(text, "free-text failure messages withheld")) {
				t.Fatalf("unscanned failure message printed: %s", text)
			}
		})
	}
}

func TestReleaseEvidenceGateDiagnosticsReadGoTestJSON(t *testing.T) {
	input := filepath.Join(t.TempDir(), "test-output.json")
	events := `{"Action":"output","Package":"example.com/demo","Test":"TestDemo","Output":"secret-sentinel\n"}
{"Action":"output","Package":"example.com/demo","Test":"TestDemo","Output":"--- FAIL: TestDemo (0.00s)\n"}
{"Action":"output","Package":"example.com/demo","Output":"FAIL\texample.com/demo\t0.12s\n"}
`
	if err := os.WriteFile(input, []byte(events), 0600); err != nil {
		t.Fatal(err)
	}
	output, err := exec.Command("python3", filepath.Join(repositoryRoot(t), "scripts/release/collect.py"), "gate-diagnostics", "--input", input).CombinedOutput()
	if err != nil {
		t.Fatalf("%v: %s", err, output)
	}
	text := string(output)
	if strings.Contains(text, "secret-sentinel") || !strings.Contains(text, "--- FAIL: TestDemo (0.00s)") || !strings.Contains(text, "FAIL\texample.com/demo\t0.12s") {
		t.Fatalf("unexpected diagnostics: %s", text)
	}
}

func TestWorkflowFuzzShardRetainsNewCrashersOnFailure(t *testing.T) {
	master := readWorkflow(t, "master.yml")
	assertJobRunContains(t, master, "fuzz-shard", "git ls-files -z --others --exclude-standard -- 'golang/**/testdata/fuzz/**'")
	step := artifactUploadStep(t, master, "fuzz-shard", "fuzz-crashers-${{ matrix.shard }}")
	if got := scalarString(t, master.name, step, "if"); got != "failure()" {
		t.Fatalf("fuzz crasher upload condition = %q, want failure()", got)
	}
}

func TestReleaseEvidenceFuzzReuseRequiresEverySuccessfulShard(t *testing.T) {
	for _, scenario := range []string{"complete", "missing", "failed"} {
		t.Run(scenario, func(t *testing.T) {
			dir := t.TempDir()
			for i, name := range []string{"fuzz-0.json", "fuzz-1.json", "fuzz-2.json"} {
				if scenario == "missing" && i == 2 {
					continue
				}
				status := "pass"
				if scenario == "failed" && i == 2 {
					status = "fail"
				}
				if err := os.WriteFile(filepath.Join(dir, name), []byte(`{"kind":"fuzz_summary","status":"`+status+`","redacted":true}`), 0600); err != nil {
					t.Fatal(err)
				}
			}
			dest := filepath.Join(dir, "combined.json")
			output, err := exec.Command("python3", filepath.Join(repositoryRoot(t), "scripts/release/merge-fuzz.py"), dir, dest).CombinedOutput()
			if scenario == "complete" {
				if err != nil {
					t.Fatalf("%v: %s", err, output)
				}
				return
			}
			if err == nil {
				t.Fatal("incomplete fuzz evidence accepted")
			}
			if _, err := os.Stat(dest); !os.IsNotExist(err) {
				t.Fatal("invalid evidence retained")
			}
		})
	}
}

func TestWorkflowReleaseEvidenceReusesSuccessfulRunInputs(t *testing.T) {
	master := readWorkflow(t, "master.yml")
	assertJobRunContains(t, master, "verify-race", "scripts/release/run-gate.sh race_summary")
	assertJobRunContains(t, master, "verify-image", "RELEASE_COMPOSE_EVIDENCE_DIR=")
	assertJobRunContains(t, master, "fuzz-shard", "scripts/release/run-gate.sh fuzz_summary")
	assertJobRunContains(t, master, "release-evidence", "--verified-inputs verified-inputs")
	assertJobRunContains(t, master, "release-evidence", "scripts/release/merge-fuzz.py")
	job := workflowJob(t, master, "release-evidence")
	for _, raw := range job["steps"].([]any) {
		step := raw.(map[string]any)
		run, _ := step["run"].(string)
		for _, forbidden := range []string{"go test", "make image-verify", "setup-build-cloud.sh"} {
			if strings.Contains(run, forbidden) {
				t.Fatalf("evidence job repeats %s", forbidden)
			}
		}
		if uses, _ := step["uses"].(string); strings.HasPrefix(uses, "actions/download-artifact@") {
			with := step["with"].(map[string]any)
			for _, forbidden := range []string{"run-id", "repository", "github-token"} {
				if _, exists := with[forbidden]; exists {
					t.Fatalf("evidence download escapes current run via %s", forbidden)
				}
			}
		}
	}
}

func TestReleaseEvidenceComposeCaptureRedactsAndRejectsUnhealthyServices(t *testing.T) {
	for _, health := range []string{"healthy", "unhealthy"} {
		t.Run(health, func(t *testing.T) {
			dir := t.TempDir()
			fake := `#!/usr/bin/env bash
case "$1" in
 ps) echo fake-container ;;
 inspect) echo "running|$FAKE_HEALTH" ;;
 logs) echo 'secret-sentinel'; echo 'Ready to accept connections'; echo 'temporal ready' ;;
 *) exit 99 ;;
esac
`
			if err := os.WriteFile(filepath.Join(dir, "docker"), []byte(fake), 0700); err != nil {
				t.Fatal(err)
			}
			dest := filepath.Join(dir, "evidence")
			cmd := exec.Command("bash", filepath.Join(repositoryRoot(t), "scripts/release/collect-compose.sh"), "tested-project", dest)
			cmd.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"), "FAKE_HEALTH="+health)
			output, err := cmd.CombinedOutput()
			if health == "unhealthy" {
				if err == nil {
					t.Fatal("unhealthy service accepted")
				}
				return
			}
			if err != nil {
				t.Fatalf("%v: %s", err, output)
			}
			for _, name := range []string{"redis-summary", "temporal-summary", "compose-summary", "redis-log", "temporal-log", "compose-log"} {
				data, err := os.ReadFile(filepath.Join(dest, name+".json"))
				if err != nil {
					t.Fatal(err)
				}
				if strings.Contains(string(data), "secret-sentinel") {
					t.Fatal("raw service text leaked")
				}
			}
		})
	}
}
