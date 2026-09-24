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
	assertJobRunContains(t, master, "verify", "scripts/release/run-gate.sh race_summary")
	assertJobRunContains(t, master, "verify", "RELEASE_COMPOSE_EVIDENCE_DIR=")
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
