package architecturetest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

const (
	downloadArtifactActionPin = "actions/download-artifact@d3f86a106a0bac45b974a628896c90dbdf5c8093"
	uploadArtifactActionPin   = "actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02"
)

var fullGitCommitID = regexp.MustCompile(`^[0-9a-f]{40}$`)

func TestWorkflowGuardedPublicationBoundary(t *testing.T) {
	release := readWorkflow(t, "release.yml")
	master := readWorkflow(t, "master.yml")

	assertManualGuardedReleaseTrigger(t, release)
	assertReadOnlyPermissions(t, release)
	if !strings.Contains(release.raw, "cancel-in-progress: false") {
		t.Fatal("release.yml must serialize guarded publication requests instead of cancelling them")
	}

	preflight := workflowJob(t, release, "preflight")
	if scalarString(t, release.name, preflight, "if") != "github.event_name == 'workflow_dispatch' && github.ref == 'refs/heads/master'" {
		t.Fatalf("release preflight must reject non-master manual dispatches, got %#v", preflight["if"])
	}
	assertJobPermissions(t, release.name, "preflight", preflight, map[string]string{
		"actions":  "read",
		"contents": "read",
	})
	if _, found := preflight["environment"]; found {
		t.Fatal("release preflight must not enter the protected publication environment")
	}
	for _, action := range []string{setupGoActionPin, downloadArtifactActionPin} {
		assertJobUsesAction(t, release, "preflight", action)
	}
	assertAnonymousFixedPublicCheckout(t, release)
	assertJobActionInput(t, release, "preflight", setupGoActionPin, "token", "")
	assertJobActionInput(t, release, "preflight", setupGoActionPin, "cache", "false")
	assertJobActionInput(t, release, "preflight", downloadArtifactActionPin, "name", "release-evidence")
	assertJobActionInput(t, release, "preflight", downloadArtifactActionPin, "github-token", "${{ github.token }}")
	assertJobActionInput(t, release, "preflight", downloadArtifactActionPin, "repository", "${{ github.repository }}")
	assertJobActionInput(t, release, "preflight", downloadArtifactActionPin, "run-id", "${{ inputs.evidence_run_id }}")
	assertJobActionInput(t, release, "preflight", downloadArtifactActionPin, "path", "release-artifacts")

	for _, command := range []string{
		"make security-verify",
		"make workflow-verify",
		"make deployment-policy-verify",
		"make image-verify",
		"make release-verify",
	} {
		if !jobHasRunCommand(preflight, command) {
			t.Fatalf("release preflight does not run %q", command)
		}
	}
	for _, want := range []string{
		"bash scripts/release/guard.sh validate-request",
		"bash scripts/release/guard.sh verify-public-run",
		"bash scripts/release/guard.sh verify-evidence",
		"--evidence-run-id \"$EVIDENCE_RUN_ID\"",
		"name: release-evidence",
	} {
		if !strings.Contains(release.raw, want) {
			t.Fatalf("release preflight does not retain trusted-evidence guard %q", want)
		}
	}
	assertTrustedMasterEvidenceArtifactSource(t, master)
	assertGitHubTokenIsExclusiveToArtifactDownload(t, release)
	assertPublicRunVerifierIsTokenless(t)

	protected := workflowJob(t, release, "protected-signing-publication")
	if scalarString(t, release.name, protected, "needs") != "preflight" {
		t.Fatalf("protected publication job must require preflight, got %#v", protected["needs"])
	}
	if scalarString(t, release.name, protected, "if") != "github.event_name == 'workflow_dispatch' && github.ref == 'refs/heads/master'" {
		t.Fatalf("protected publication job must reject non-master manual dispatches, got %#v", protected["if"])
	}
	assertJobPermissions(t, release.name, "protected-signing-publication", protected, map[string]string{
		"contents": "read",
		"id-token": "write",
	})
	environment := nestedMapping(t, release.name, protected, "environment")
	if scalarString(t, release.name, environment, "name") != "release-publication" {
		t.Fatalf("protected publication environment = %#v, want release-publication", protected["environment"])
	}
	if !strings.Contains(release.raw, "Task 24 stops before external signing, registry publication, tagging, or release creation.") {
		t.Fatal("protected job must fail closed with a clear external-boundary message")
	}

	assertReleaseWorkflowDoesNotPublish(t, release)
	assertNoWorkflowShellTokenReference(t, release)
	assertManualInputsAreNotInterpolatedInShell(t, release)
	assertReleaseGuardDoesNotReachExternalSinks(t)
}

func TestReleaseRunbookDocumentsExternalAuthorizationBoundary(t *testing.T) {
	runbook := readRepositoryFile(t, repositoryRoot(t), "docs", "release", "runbook.md")
	for _, want := range []string{
		"Guarded manual publication boundary",
		"workflow_dispatch",
		"`refs/tags/vMAJOR.MINOR.PATCH`",
		"`RELEASE_PUBLICATION_IMAGE_REPOSITORY`",
		"`release-publication`",
		"automatic, job-scoped `GITHUB_TOKEN`",
		"`actions: read`",
		"credential-free HTTPS `GET`",
		"fixed unauthenticated HTTPS Git fetch",
		"`https://github.com/mfow/llm-temporal-worker.git`",
		"`github.sha`",
		"`.github/workflows/master.yml`",
		"only as the input to GitHub's pinned",
		"does not create or modify that environment",
		"does not sign, publish, push, create a tag, or create a release",
		"fails closed",
	} {
		if !strings.Contains(runbook, want) {
			t.Fatalf("release runbook does not document guarded-publication prerequisite %q", want)
		}
	}
}

func TestReleaseRunbookListsRecentMergedValidation(t *testing.T) {
	runbook := readRepositoryFile(t, repositoryRoot(t), "docs", "release", "runbook.md")
	for _, want := range []struct {
		pr     string
		commit string
		run    string
	}{
		{pr: "#562", commit: "a48258d6e6ce7996ad4492b419a597e948c14e9b", run: "30743830181"},
		{pr: "#563", commit: "9d0ceb4a3c8157fc31fce2b9b71519aa0f4662c4", run: "30743919451"},
		{pr: "#564", commit: "47f33c033f9c3fdcfc8554e279516bdb51b0a31d", run: "30746120429"},
		{pr: "#565", commit: "832be7a08d5efc2c7214f2bc629b43839ba94710", run: "30746200992"},
		{pr: "#566", commit: "efa76d8dc722057111a5fdde82f574b5bcfcbb1b", run: "30748402963"},
	} {
		prMarker := "[" + want.pr + "](https://github.com/mfow/llm-temporal-worker/pull/" + strings.TrimPrefix(want.pr, "#") + ")"
		runMarker := "[" + want.run + "](https://github.com/mfow/llm-temporal-worker/actions/runs/" + want.run + ")"
		var rows []string
		for _, line := range strings.Split(runbook, "\n") {
			if strings.Contains(line, prMarker) {
				rows = append(rows, line)
			}
		}
		if len(rows) != 1 {
			t.Fatalf("release runbook rows for %s = %d, want exactly one", want.pr, len(rows))
		}
		if !strings.Contains(rows[0], "`"+want.commit+"`") {
			t.Errorf("release runbook row for %s does not bind merge commit %s: %s", want.pr, want.commit, rows[0])
		}
		if !strings.Contains(rows[0], runMarker) {
			t.Errorf("release runbook row for %s does not bind green PR run %s: %s", want.pr, want.run, rows[0])
		}
	}
}

func TestReleaseGuardValidatesTagReferencesAndImageSubjects(t *testing.T) {
	repository, commit := createGuardedReleaseTestRepository(t)
	trustedRepository := "registry.example.com/team/llm-temporal-worker"
	digest := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

	for _, releaseRef := range []string{"refs/tags/v1.2.3", "refs/tags/v2.3.4"} {
		t.Run(releaseRef, func(t *testing.T) {
			output := filepath.Join(t.TempDir(), "outputs")
			result := runReleaseGuard(t, repository, "validate-request",
				"--release-ref", releaseRef,
				"--image-reference", trustedRepository+"@"+digest,
				"--evidence-run-id", "29386817789",
				"--trusted-repository", trustedRepository,
				"--output", output,
			)
			if result.err != nil {
				t.Fatalf("guard rejected valid %s: %v\n%s", releaseRef, result.err, result.output)
			}
			outputs := readGuardOutputs(t, output)
			if outputs["tag_commit"] != commit {
				t.Fatalf("tag_commit = %q, want %q", outputs["tag_commit"], commit)
			}
			if outputs["image_digest"] != digest {
				t.Fatalf("image_digest = %q, want %q", outputs["image_digest"], digest)
			}
		})
	}

	for _, test := range []struct {
		name string
		args []string
	}{
		{
			name: "branch ref",
			args: []string{"--release-ref", "refs/heads/master", "--image-reference", trustedRepository + "@" + digest, "--evidence-run-id", "29386817789", "--trusted-repository", trustedRepository},
		},
		{
			name: "non-semver tag",
			args: []string{"--release-ref", "refs/tags/latest", "--image-reference", trustedRepository + "@" + digest, "--evidence-run-id", "29386817789", "--trusted-repository", trustedRepository},
		},
		{
			name: "tag outside protected master",
			args: []string{"--release-ref", "refs/tags/v3.4.5", "--image-reference", trustedRepository + "@" + digest, "--evidence-run-id", "29386817789", "--trusted-repository", trustedRepository},
		},
		{
			name: "missing digest",
			args: []string{"--release-ref", "refs/tags/v1.2.3", "--image-reference", trustedRepository + ":v1.2.3", "--evidence-run-id", "29386817789", "--trusted-repository", trustedRepository},
		},
		{
			name: "malformed digest",
			args: []string{"--release-ref", "refs/tags/v1.2.3", "--image-reference", trustedRepository + "@sha256:not-a-digest", "--evidence-run-id", "29386817789", "--trusted-repository", trustedRepository},
		},
		{
			name: "untrusted repository",
			args: []string{"--release-ref", "refs/tags/v1.2.3", "--image-reference", "registry.invalid/team/llm-temporal-worker@" + digest, "--evidence-run-id", "29386817789", "--trusted-repository", trustedRepository},
		},
		{
			name: "zero evidence run ID",
			args: []string{"--release-ref", "refs/tags/v1.2.3", "--image-reference", trustedRepository + "@" + digest, "--evidence-run-id", "0", "--trusted-repository", trustedRepository},
		},
		{
			name: "non-numeric evidence run ID",
			args: []string{"--release-ref", "refs/tags/v1.2.3", "--image-reference", trustedRepository + "@" + digest, "--evidence-run-id", "run-29386817789", "--trusted-repository", trustedRepository},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			output := filepath.Join(t.TempDir(), "outputs")
			result := runReleaseGuard(t, repository, append(test.args, "--output", output)...)
			if result.err == nil {
				t.Fatalf("guard accepted %s: %s", test.name, result.output)
			}
		})
	}
}

func TestReleaseGuardBindsDownloadedEvidenceToTheTagCommitAndDigest(t *testing.T) {
	repository, commit := createGuardedReleaseTestRepository(t)
	digest := "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

	for _, test := range []struct {
		name        string
		revision    string
		imageDigest string
		wantError   bool
	}{
		{name: "matching evidence", revision: commit, imageDigest: digest},
		{name: "different revision", revision: "cccccccccccccccccccccccccccccccccccccccc", imageDigest: digest, wantError: true},
		{name: "different digest", revision: commit, imageDigest: "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			evidence := filepath.Join(t.TempDir(), "evidence.json")
			content := fmt.Sprintf(`{"source":{"revision":%q},"image":{"digest":%q}}`, test.revision, test.imageDigest)
			if err := os.WriteFile(evidence, []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
			result := runReleaseGuard(t, repository, "verify-evidence",
				"--evidence", evidence,
				"--tag-commit", commit,
				"--image-reference", "registry.example.com/team/llm-temporal-worker@"+digest,
			)
			if (result.err != nil) != test.wantError {
				t.Fatalf("verify-evidence error = %v, want error %t\n%s", result.err, test.wantError, result.output)
			}
		})
	}
}

func TestReleaseGuardAcceptsVerifiedLocalTask23EvidenceFixture(t *testing.T) {
	// This is deliberately a complete, deterministic Task 23 evidence bundle:
	// no GitHub artifact or external registry is needed to exercise the success
	// path that binds a verified bundle to a local protected tag.
	repository, commit := createGuardedReleaseTestRepository(t)
	bundle := writeReleaseEvidenceBundle(t, false)
	evidence := readReleaseEvidence(t, bundle.directory)
	evidence["source"].(map[string]any)["revision"] = commit
	writeReleaseEvidence(t, bundle.directory, evidence)

	root := repositoryRoot(t)
	if output, err := runReleaseEvidenceVerifier(t, root, bundle.directory); err != nil {
		t.Fatalf("deterministic Task 23 fixture did not verify: %v\n%s", err, output)
	}

	result := runReleaseGuard(t, repository, "verify-evidence",
		"--evidence", filepath.Join(bundle.directory, "evidence.json"),
		"--tag-commit", commit,
		"--image-reference", "registry.example.com/team/llm-temporal-worker@"+bundle.imageDigest,
	)
	if result.err != nil {
		t.Fatalf("guard rejected verified local Task 23 evidence: %v\n%s", result.err, result.output)
	}
}

func TestPublicEvidenceRunVerifierValidatesTrustedMasterMetadata(t *testing.T) {
	const (
		repository = "mfow/llm-temporal-worker"
		runID      = "29386817789"
		commit     = "b06042682c202d379e75779149f27ac0e25328d4"
	)

	for _, test := range []struct {
		name      string
		mutate    func(map[string]any)
		wantError bool
	}{
		{name: "matching successful master push"},
		{
			name: "different run ID",
			mutate: func(metadata map[string]any) {
				metadata["id"] = int64(29386817790)
			},
			wantError: true,
		},
		{
			name: "different repository",
			mutate: func(metadata map[string]any) {
				metadata["repository"].(map[string]any)["full_name"] = "attacker/llm-temporal-worker"
			},
			wantError: true,
		},
		{
			name: "different workflow path",
			mutate: func(metadata map[string]any) {
				metadata["path"] = ".github/workflows/release.yml"
			},
			wantError: true,
		},
		{
			name: "non-push event",
			mutate: func(metadata map[string]any) {
				metadata["event"] = "workflow_dispatch"
			},
			wantError: true,
		},
		{
			name: "non-master branch",
			mutate: func(metadata map[string]any) {
				metadata["head_branch"] = "release-candidate"
			},
			wantError: true,
		},
		{
			name: "unfinished run",
			mutate: func(metadata map[string]any) {
				metadata["status"] = "in_progress"
			},
			wantError: true,
		},
		{
			name: "failed run",
			mutate: func(metadata map[string]any) {
				metadata["conclusion"] = "failure"
			},
			wantError: true,
		},
		{
			name: "different commit",
			mutate: func(metadata map[string]any) {
				metadata["head_sha"] = "cccccccccccccccccccccccccccccccccccccccc"
			},
			wantError: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			metadata := trustedPublicEvidenceRunMetadata(repository, commit)
			if test.mutate != nil {
				test.mutate(metadata)
			}
			data, err := json.Marshal(metadata)
			if err != nil {
				t.Fatal(err)
			}
			fixture := filepath.Join(t.TempDir(), "public-run.json")
			if err := os.WriteFile(fixture, data, 0o600); err != nil {
				t.Fatal(err)
			}

			result := runPublicRunVerifier(t,
				"validate",
				"--repository", repository,
				"--run-id", runID,
				"--tag-commit", commit,
				"--metadata", fixture,
			)
			if (result.err != nil) != test.wantError {
				t.Fatalf("public run verifier error = %v, want error %t\\n%s", result.err, test.wantError, result.output)
			}
		})
	}
}

func TestPublicEvidenceRunVerifierRejectsMalformedOrOversizedFixturesWithoutEchoingThem(t *testing.T) {
	const (
		repository = "mfow/llm-temporal-worker"
		runID      = "29386817789"
		commit     = "b06042682c202d379e75779149f27ac0e25328d4"
		marker     = "never-echo-public-workflow-metadata"
	)

	for _, test := range []struct {
		name     string
		contents []byte
	}{
		{name: "malformed JSON", contents: []byte(marker)},
		{name: "oversized JSON", contents: bytes.Repeat([]byte("x"), 1024*1024+1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := filepath.Join(t.TempDir(), "public-run.json")
			if err := os.WriteFile(fixture, test.contents, 0o600); err != nil {
				t.Fatal(err)
			}
			result := runPublicRunVerifier(t,
				"validate",
				"--repository", repository,
				"--run-id", runID,
				"--tag-commit", commit,
				"--metadata", fixture,
			)
			if result.err == nil {
				t.Fatalf("public run verifier accepted %s", test.name)
			}
			if strings.Contains(result.output, marker) {
				t.Fatalf("public run verifier echoed invalid metadata: %q", result.output)
			}
		})
	}
}

func trustedPublicEvidenceRunMetadata(repository, commit string) map[string]any {
	return map[string]any{
		"id":          int64(29386817789),
		"event":       "push",
		"head_branch": "master",
		"status":      "completed",
		"conclusion":  "success",
		"head_sha":    commit,
		"path":        ".github/workflows/master.yml",
		"repository": map[string]any{
			"full_name": repository,
		},
	}
}

func assertManualGuardedReleaseTrigger(t *testing.T, workflow workflowDocument) {
	t.Helper()
	triggers := workflowMapping(t, workflow, "on")
	if len(triggers) != 1 {
		t.Fatalf("%s triggers = %#v, want workflow_dispatch only", workflow.name, triggers)
	}
	dispatch := nestedMapping(t, workflow.name, triggers, "workflow_dispatch")
	inputs := nestedMapping(t, workflow.name, dispatch, "inputs")
	for _, name := range []string{"release_ref", "image_reference", "evidence_run_id"} {
		input := nestedMapping(t, workflow.name, inputs, name)
		if input["required"] != true || scalarString(t, workflow.name, input, "type") != "string" {
			t.Fatalf("%s manual input %q must be a required string, got %#v", workflow.name, name, input)
		}
	}
}

func assertJobPermissions(t *testing.T, workflowName, jobName string, job map[string]any, want map[string]string) {
	t.Helper()
	permissions := nestedMapping(t, workflowName, job, "permissions")
	if len(permissions) != len(want) {
		t.Fatalf("%s job %q permissions = %#v, want %#v", workflowName, jobName, permissions, want)
	}
	for name, value := range want {
		if scalarString(t, workflowName, permissions, name) != value {
			t.Fatalf("%s job %q permissions = %#v, want %#v", workflowName, jobName, permissions, want)
		}
	}
}

func assertReleaseWorkflowDoesNotPublish(t *testing.T, workflow workflowDocument) {
	t.Helper()
	for _, forbidden := range []string{
		"secrets.",
		"docker login",
		"docker push",
		"cosign sign",
		"gh release",
		"oras push",
		"skopeo copy",
		"packages: write",
		"attestations: write",
		"actions: write",
		"git push",
		"git tag",
		"gh api",
	} {
		if strings.Contains(strings.ToLower(workflow.raw), forbidden) {
			t.Fatalf("%s contains forbidden signing, publication, or external-write command %q", workflow.name, forbidden)
		}
	}
}

func assertTrustedMasterEvidenceArtifactSource(t *testing.T, master workflowDocument) {
	t.Helper()
	job := workflowJob(t, master, "release-evidence")
	if scalarString(t, master.name, job, "if") != "github.event_name == 'push' && github.ref == 'refs/heads/master'" {
		t.Fatalf("master release-evidence job must run only for a master push, got %#v", job["if"])
	}
	if scalarString(t, master.name, job, "needs") != "verify" {
		t.Fatalf("master release-evidence job must require verified master CI, got %#v", job["needs"])
	}
	assertJobUsesAction(t, master, "release-evidence", uploadArtifactActionPin)
	assertJobActionInput(t, master, "release-evidence", uploadArtifactActionPin, "name", "release-evidence")

	artifactStep := artifactUploadStep(t, master, "release-evidence", "release-evidence")
	if scalarString(t, master.name, artifactStep, "if") != "success()" {
		t.Fatalf("master release-evidence artifact must upload only after successful verification, got %#v", artifactStep["if"])
	}
	with := nestedMapping(t, master.name, artifactStep, "with")
	if scalarString(t, master.name, with, "path") != "release-artifacts/" {
		t.Fatalf("master release-evidence artifact path = %#v, want release-artifacts/", with["path"])
	}

	for jobName, rawJob := range workflowMapping(t, master, "jobs") {
		if jobName == "release-evidence" {
			continue
		}
		job, ok := rawJob.(map[string]any)
		if !ok {
			t.Fatalf("%s job %q = %#v, want mapping", master.name, jobName, rawJob)
		}
		for _, rawStep := range workflowSteps(t, master.name, jobName, job) {
			step, ok := rawStep.(map[string]any)
			if !ok || step["uses"] != uploadArtifactActionPin {
				continue
			}
			with, ok := step["with"].(map[string]any)
			if ok && with["name"] == "release-evidence" {
				t.Fatalf("%s job %q must not upload the trusted release-evidence artifact", master.name, jobName)
			}
		}
	}
}

func assertGitHubTokenIsExclusiveToArtifactDownload(t *testing.T, workflow workflowDocument) {
	t.Helper()
	const tokenExpression = "${{ github.token }}"
	if strings.Count(workflow.raw, tokenExpression) != 1 {
		t.Fatalf("%s must contain exactly one explicit GitHub token expression", workflow.name)
	}

	usesToken := 0
	for jobName, rawJob := range workflowMapping(t, workflow, "jobs") {
		job, ok := rawJob.(map[string]any)
		if !ok {
			t.Fatalf("%s job %q = %#v, want mapping", workflow.name, jobName, rawJob)
		}
		for _, rawStep := range workflowSteps(t, workflow.name, jobName, job) {
			step, ok := rawStep.(map[string]any)
			if !ok {
				continue
			}
			with, _ := step["with"].(map[string]any)
			for input, value := range with {
				if value != tokenExpression {
					continue
				}
				usesToken++
				if jobName != "preflight" || step["uses"] != downloadArtifactActionPin || input != "github-token" {
					t.Fatalf("%s job %q exposes the GitHub token outside pinned download-artifact", workflow.name, jobName)
				}
			}
		}
	}
	if usesToken != 1 {
		t.Fatalf("%s passes the GitHub token %d times, want once", workflow.name, usesToken)
	}
}

func assertAnonymousFixedPublicCheckout(t *testing.T, workflow workflowDocument) {
	t.Helper()
	if strings.Contains(strings.ToLower(workflow.raw), "actions/checkout@") {
		t.Fatalf("%s must not give actions/checkout a GitHub token when the public anonymous bootstrap is required", workflow.name)
	}

	preflight := workflowJob(t, workflow, "preflight")
	steps := workflowSteps(t, workflow.name, "preflight", preflight)
	shapeIndex := -1
	checkoutIndex := -1
	var shapeStep map[string]any
	var checkoutStep map[string]any
	for index, rawStep := range steps {
		step, ok := rawStep.(map[string]any)
		if !ok {
			continue
		}
		switch step["name"] {
		case "Validate release reference before anonymous fetch":
			shapeIndex = index
			shapeStep = step
		case "Check out fixed public master anonymously":
			checkoutIndex = index
			checkoutStep = step
		}
	}
	if shapeIndex < 0 || checkoutIndex < 0 || shapeIndex >= checkoutIndex {
		t.Fatalf("%s must validate the manual release ref before its anonymous public checkout", workflow.name)
	}

	shapeEnv := stepEnvironment(t, workflow.name, "preflight", shapeStep)
	if got := scalarString(t, workflow.name, shapeEnv, "RELEASE_REF"); got != "${{ inputs.release_ref }}" {
		t.Fatalf("%s release-ref shape step input = %q, want explicit environment binding", workflow.name, got)
	}
	shapeRun := scalarString(t, workflow.name, shapeStep, "run")
	for _, want := range []string{
		`[[ "$RELEASE_REF" =~ ^refs/tags/v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]`,
		`printf 'release_ref=%s\n' "$RELEASE_REF" >> "$GITHUB_OUTPUT"`,
	} {
		if !strings.Contains(shapeRun, want) {
			t.Fatalf("%s release-ref shape step does not retain %q", workflow.name, want)
		}
	}
	if strings.Contains(strings.ToLower(shapeRun), "git ") {
		t.Fatalf("%s release-ref shape step must not fetch or execute a manual ref", workflow.name)
	}

	checkoutEnv := stepEnvironment(t, workflow.name, "preflight", checkoutStep)
	if got := scalarString(t, workflow.name, checkoutEnv, "RELEASE_REF"); got != "${{ steps.release-ref.outputs.release_ref }}" {
		t.Fatalf("%s anonymous checkout must use only the validated release ref, got %q", workflow.name, got)
	}
	if got := scalarString(t, workflow.name, checkoutEnv, "TRUSTED_MASTER_SHA"); got != "${{ github.sha }}" {
		t.Fatalf("%s anonymous checkout must bind the protected workflow revision, got %q", workflow.name, got)
	}
	checkoutRun := scalarString(t, workflow.name, checkoutStep, "run")
	for _, want := range []string{
		`test -z "$(find "$GITHUB_WORKSPACE" -mindepth 1 -maxdepth 1 -print -quit)"`,
		`export GIT_CONFIG_NOSYSTEM=1`,
		`export GIT_TERMINAL_PROMPT=0`,
		`export GIT_ASKPASS=/bin/false`,
		`git init --quiet "$GITHUB_WORKSPACE"`,
		`git -C "$GITHUB_WORKSPACE" remote add origin https://github.com/mfow/llm-temporal-worker.git`,
		`git -C "$GITHUB_WORKSPACE" -c credential.helper= -c http.extraHeader= fetch --no-tags --force origin`,
		`+refs/heads/master:refs/remotes/origin/master`,
		`"$RELEASE_REF:$RELEASE_REF"`,
		`git -C "$GITHUB_WORKSPACE" checkout --detach --force "$TRUSTED_MASTER_SHA"`,
		`git -C "$GITHUB_WORKSPACE" rev-parse --verify refs/remotes/origin/master`,
	} {
		if !strings.Contains(checkoutRun, want) {
			t.Fatalf("%s anonymous checkout does not retain %q", workflow.name, want)
		}
	}
	for _, forbidden := range []string{"github.token", "github_token", "gh_token", "actions/checkout"} {
		if strings.Contains(strings.ToLower(checkoutRun), forbidden) {
			t.Fatalf("%s anonymous checkout exposes a credentialed checkout path %q", workflow.name, forbidden)
		}
	}
}

func assertPublicRunVerifierIsTokenless(t *testing.T) {
	t.Helper()
	verifier := readRepositoryFile(t, repositoryRoot(t), "scripts", "release", "verify-public-run.py")
	for _, required := range []string{
		`PUBLIC_GITHUB_API = "https://api.github.com"`,
		`f"{PUBLIC_GITHUB_API}/repos/{quote(owner, safe='')}/"`,
		`f"{quote(repository, safe='')}/actions/runs/{run_id}"`,
		`method="GET"`,
		"class RejectRedirects(HTTPRedirectHandler):",
		"build_opener(RejectRedirects())",
		"metadata request redirected",
		"timeout=15",
		"MAX_METADATA_BYTES",
		"response.read(MAX_METADATA_BYTES + 1)",
		`"event"`,
		`"head_branch"`,
		`"status"`,
		`"conclusion"`,
		`"head_sha"`,
		`".github/workflows/master.yml"`,
	} {
		if !strings.Contains(verifier, required) {
			t.Fatalf("public run verifier does not retain required fail-closed contract %q", required)
		}
	}
	for _, forbidden := range []string{
		"authorization",
		"github.token",
		"github_token",
		"os.environ",
		"getenv(",
		"subprocess",
		"curl ",
		"wget ",
		"error.read(",
		"print(data",
		"print(metadata",
	} {
		if strings.Contains(strings.ToLower(verifier), forbidden) {
			t.Fatalf("public run verifier accesses a credential or unsafe external command %q", forbidden)
		}
	}
}

func assertNoWorkflowShellTokenReference(t *testing.T, workflow workflowDocument) {
	t.Helper()
	for jobName, rawJob := range workflowMapping(t, workflow, "jobs") {
		job, ok := rawJob.(map[string]any)
		if !ok {
			t.Fatalf("%s job %q is not a mapping", workflow.name, jobName)
		}
		for _, rawStep := range workflowSteps(t, workflow.name, jobName, job) {
			step, ok := rawStep.(map[string]any)
			if !ok {
				continue
			}
			run, _ := step["run"].(string)
			for _, forbidden := range []string{"github.token", "github_token", "gh_token"} {
				if strings.Contains(strings.ToLower(run), forbidden) {
					t.Fatalf("%s job %q exposes a GitHub token to shell", workflow.name, jobName)
				}
				for name, value := range stepEnvironment(t, workflow.name, jobName, step) {
					if strings.Contains(strings.ToLower(fmt.Sprint(value)), forbidden) {
						t.Fatalf("%s job %q exposes a GitHub token through shell environment %q", workflow.name, jobName, name)
					}
				}
			}
		}
	}
}

func assertManualInputsAreNotInterpolatedInShell(t *testing.T, workflow workflowDocument) {
	t.Helper()
	for jobName, rawJob := range workflowMapping(t, workflow, "jobs") {
		job, ok := rawJob.(map[string]any)
		if !ok {
			t.Fatalf("%s job %q is not a mapping", workflow.name, jobName)
		}
		for _, rawStep := range job["steps"].([]any) {
			step, ok := rawStep.(map[string]any)
			if !ok {
				continue
			}
			run, _ := step["run"].(string)
			if strings.Contains(run, "${{ inputs.") {
				t.Fatalf("%s job %q interpolates a manual input directly into a shell step", workflow.name, jobName)
			}
		}
	}
}

func assertReleaseGuardDoesNotReachExternalSinks(t *testing.T) {
	t.Helper()
	guard := readRepositoryFile(t, repositoryRoot(t), "scripts", "release", "guard.sh")
	for _, forbidden := range []string{
		"curl ",
		"wget ",
		"docker ",
		"cosign ",
		"oras ",
		"skopeo ",
		"gh ",
		"git push",
		"git tag",
		"git remote",
	} {
		if strings.Contains(strings.ToLower(guard), forbidden) {
			t.Fatalf("release guard reaches forbidden external or Git-write sink %q", forbidden)
		}
	}
}

func artifactUploadStep(t *testing.T, workflow workflowDocument, jobName, artifactName string) map[string]any {
	t.Helper()
	job := workflowJob(t, workflow, jobName)
	for _, rawStep := range workflowSteps(t, workflow.name, jobName, job) {
		step, ok := rawStep.(map[string]any)
		if !ok || step["uses"] != uploadArtifactActionPin {
			continue
		}
		with, ok := step["with"].(map[string]any)
		if ok && with["name"] == artifactName {
			return step
		}
	}
	t.Fatalf("%s job %q does not upload artifact %q", workflow.name, jobName, artifactName)
	return nil
}

func workflowSteps(t *testing.T, workflowName, jobName string, job map[string]any) []any {
	t.Helper()
	steps, ok := job["steps"].([]any)
	if !ok {
		t.Fatalf("%s job %q steps = %#v, want sequence", workflowName, jobName, job["steps"])
	}
	return steps
}

func stepEnvironment(t *testing.T, workflowName, jobName string, step map[string]any) map[string]any {
	t.Helper()
	environment, found := step["env"]
	if !found {
		return nil
	}
	mapping, ok := environment.(map[string]any)
	if !ok {
		t.Fatalf("%s job %q step environment = %#v, want mapping", workflowName, jobName, environment)
	}
	return mapping
}

type releaseGuardResult struct {
	output string
	err    error
}

func runReleaseGuard(t *testing.T, directory string, arguments ...string) releaseGuardResult {
	t.Helper()
	command := exec.Command("bash", append([]string{filepath.Join(repositoryRoot(t), "scripts", "release", "guard.sh")}, arguments...)...)
	command.Dir = directory
	output, err := command.CombinedOutput()
	return releaseGuardResult{output: string(output), err: err}
}

func runPublicRunVerifier(t *testing.T, arguments ...string) releaseGuardResult {
	t.Helper()
	command := exec.Command("python3", append([]string{filepath.Join(repositoryRoot(t), "scripts", "release", "verify-public-run.py")}, arguments...)...)
	command.Dir = repositoryRoot(t)
	output, err := command.CombinedOutput()
	return releaseGuardResult{output: string(output), err: err}
}

func readGuardOutputs(t *testing.T, path string) map[string]string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	outputs := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		name, value, found := strings.Cut(line, "=")
		if !found {
			t.Fatalf("invalid guard output line %q", line)
		}
		outputs[name] = value
	}
	return outputs
}

func createGuardedReleaseTestRepository(t *testing.T) (string, string) {
	t.Helper()
	directory := t.TempDir()
	runGuardGit(t, directory, "init")
	runGuardGit(t, directory, "config", "user.email", "guard@example.test")
	runGuardGit(t, directory, "config", "user.name", "Release Guard")
	runGuardGit(t, directory, "config", "commit.gpgsign", "false")
	if err := os.WriteFile(filepath.Join(directory, "README.md"), []byte("guard test\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGuardGit(t, directory, "add", "README.md")
	runGuardGit(t, directory, "commit", "-m", "guard fixture")
	runGuardGit(t, directory, "branch", "-M", "master")
	commit := guardGitCommitID(t, runGuardGit(t, directory, "rev-parse", "HEAD"))
	runGuardGit(t, directory, "update-ref", "refs/remotes/origin/master", commit)
	runGuardGit(t, directory, "tag", "v1.2.3")
	runGuardGit(t, directory, "tag", "-a", "v2.3.4", "-m", "annotated guard fixture")
	runGuardGit(t, directory, "checkout", "-b", "untrusted-release")
	if err := os.WriteFile(filepath.Join(directory, "untrusted.txt"), []byte("not on protected master\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGuardGit(t, directory, "add", "untrusted.txt")
	runGuardGit(t, directory, "commit", "-m", "untrusted release fixture")
	runGuardGit(t, directory, "tag", "v3.4.5")
	runGuardGit(t, directory, "checkout", "master")
	return directory, commit
}

func runGuardGit(t *testing.T, directory string, arguments ...string) string {
	t.Helper()
	command := exec.Command("git", arguments...)
	command.Dir = directory
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(arguments, " "), err, output)
	}
	return string(output)
}

func guardGitCommitID(t *testing.T, output string) string {
	t.Helper()
	for _, line := range strings.Split(output, "\n") {
		candidate := strings.TrimSpace(line)
		if fullGitCommitID.MatchString(candidate) {
			return candidate
		}
	}
	t.Fatalf("git rev-parse output does not contain a commit ID: %q", output)
	return ""
}
