package architecturetest

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestFailedImageScanSummaryDoesNotExposePayloads(t *testing.T) {
	input := `{"ArtifactName":"private-image-path","Results":[{"Target":"private-package-path","Secrets":[{"Match":"private-secret-value","Code":{"Lines":["private-source-line"]}}],"Vulnerabilities":[{"VulnerabilityID":"CVE-2026-12345","Severity":"HIGH","FixedVersion":"1.2.3","PkgName":"private-package-name","Description":"private-description"},{"VulnerabilityID":"private-identifier","Severity":"private-severity","FixedVersion":""}]}]}`
	path := filepath.Join(t.TempDir(), "scan.json")
	if err := os.WriteFile(path, []byte(input), 0600); err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(repositoryRoot(t), "scripts", "ci", "summarize-image-scan.py")
	output, err := exec.Command("python3", script, path).CombinedOutput()
	if err != nil {
		t.Fatalf("summary failed: %v: %s", err, output)
	}
	if strings.Contains(string(output), "private-") {
		t.Fatalf("summary exposed scan payload: %s", output)
	}
	var summary struct {
		Vulnerabilities []struct {
			ID       string `json:"id"`
			Severity string `json:"severity"`
			HasFix   bool   `json:"has_fix"`
		} `json:"vulnerabilities"`
	}
	if err := json.Unmarshal(output, &summary); err != nil {
		t.Fatal(err)
	}
	if len(summary.Vulnerabilities) != 2 || summary.Vulnerabilities[0].ID != "CVE-2026-12345" || !summary.Vulnerabilities[0].HasFix || summary.Vulnerabilities[1].ID != "unlisted" || summary.Vulnerabilities[1].Severity != "UNKNOWN" {
		t.Fatalf("unexpected summary: %s", output)
	}
	if err := os.WriteFile(path, []byte(`{"private-malformed-data"`), 0600); err != nil {
		t.Fatal(err)
	}
	output, err = exec.Command("python3", script, path).CombinedOutput()
	if err == nil || string(output) != "Image scan summary unavailable\n" {
		t.Fatalf("malformed report must fail without echoing its contents: %v: %s", err, output)
	}
}
