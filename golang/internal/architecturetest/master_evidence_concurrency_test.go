package architecturetest

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestMasterPushRunsAreNeverCancelled keeps a release-evidence run for every
// master commit: the release guard requires a successful push run whose
// head_sha is the tagged commit.
func TestMasterPushRunsAreNeverCancelled(t *testing.T) {
	master := readWorkflow(t, "master.yml").raw
	expectContainsEach(t, master, "master.yml concurrency is missing %q",
		"group: master-${{ github.event_name }}-${{ github.event_name == 'push' && github.sha || github.ref }}",
		"cancel-in-progress: ${{ github.event_name != 'push' }}",
	)
}

// TestReleaseEvidenceRendersEveryPolicyCheckedOverlay keeps release evidence
// covering the same Kustomize overlays that deploy/verify.sh policy-checks.
func TestReleaseEvidenceRendersEveryPolicyCheckedOverlay(t *testing.T) {
	root := repositoryRoot(t)
	collect := readRepositoryFile(t, root, "scripts", "release", "collect.sh")
	verify, err := os.ReadFile(filepath.Join(root, "golang", "deploy", "verify.sh"))
	if err != nil {
		t.Fatal(err)
	}
	for _, overlay := range []string{"base", "examples/aws-workload-identity", "examples/azure-workload-identity", "examples/private-state-egress", "examples/redis-tls"} {
		path := "deploy/kubernetes/" + overlay
		if !strings.Contains(string(verify), path) {
			t.Fatalf("deploy/verify.sh no longer checks %s; update this test", path)
		}
		if !strings.Contains(collect, path) {
			t.Errorf("release evidence does not render %s", path)
		}
	}
}
