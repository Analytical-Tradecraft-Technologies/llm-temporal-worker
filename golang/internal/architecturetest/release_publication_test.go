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
	downloadArtifactActionPin        = "actions/download-artifact@d3f86a106a0bac45b974a628896c90dbdf5c8093"
	uploadArtifactActionPin          = "actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02"
	ecrConfigureCredentialsActionPin = "aws-actions/configure-aws-credentials@e6de054238d6b7531b4efff3b6587d9aade6a06c"
	awsECRLoginActionPin             = "aws-actions/amazon-ecr-login@d539f0932e70871a027e9d5a9d8fc38589180a64"
	cosignInstallerActionPin         = "sigstore/cosign-installer@d7543c93d881b35a8faa02e8e3605f69b7a1ce62"
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
		"actions":  "read",
		"contents": "read",
		"id-token": "write",
	})
	environment := nestedMapping(t, release.name, protected, "environment")
	if scalarString(t, release.name, environment, "name") != "release-publication" {
		t.Fatalf("protected publication environment = %#v, want release-publication", protected["environment"])
	}
	for _, action := range []string{
		setupGoActionPin,
		downloadArtifactActionPin,
		ecrConfigureCredentialsActionPin,
		awsECRLoginActionPin,
		cosignInstallerActionPin,
	} {
		assertJobUsesAction(t, release, "protected-signing-publication", action)
	}
	assertJobActionInput(t, release, "protected-signing-publication", setupGoActionPin, "token", "")
	assertJobActionInput(t, release, "protected-signing-publication", setupGoActionPin, "cache", "false")
	assertJobActionInput(t, release, "protected-signing-publication", downloadArtifactActionPin, "name", "release-evidence")
	assertJobActionInput(t, release, "protected-signing-publication", downloadArtifactActionPin, "github-token", "${{ github.token }}")
	assertJobActionInput(t, release, "protected-signing-publication", downloadArtifactActionPin, "repository", "${{ github.repository }}")
	assertJobActionInput(t, release, "protected-signing-publication", downloadArtifactActionPin, "run-id", "${{ inputs.evidence_run_id }}")
	assertJobActionInput(t, release, "protected-signing-publication", downloadArtifactActionPin, "path", "release-artifacts")
	assertJobActionInput(t, release, "protected-signing-publication", ecrConfigureCredentialsActionPin, "role-to-assume", "${{ vars.AWS_ECR_PUBLISH_ROLE_ARN }}")
	assertJobActionInput(t, release, "protected-signing-publication", ecrConfigureCredentialsActionPin, "aws-region", "${{ vars.AWS_REGION }}")
	assertJobActionInput(t, release, "protected-signing-publication", awsECRLoginActionPin, "registries", "${{ steps.aws.outputs.aws-account-id }}")
	assertJobActionInput(t, release, "protected-signing-publication", awsECRLoginActionPin, "mask-password", "true")
	assertJobActionInput(t, release, "protected-signing-publication", cosignInstallerActionPin, "cosign-release", "v3.1.3")

	for _, command := range []string{
		"bash scripts/release/guard.sh validate-request",
		"bash scripts/release/guard.sh verify-public-run",
		"make release-verify",
		"bash scripts/release/guard.sh verify-evidence",
		"bash scripts/release/setup-crane.sh",
		"bash scripts/release/stage-image.sh",
		"bash scripts/release/publish-ecr.sh",
		"bash scripts/release/finalize-ecr-tag.sh",
		"python3 scripts/release/write-provenance.py",
		"cosign sign",
		"cosign attest --type cyclonedx",
		"cosign attest --type slsaprovenance",
		"cosign verify",
		"cosign verify-attestation",
	} {
		assertJobRunContains(t, release, "protected-signing-publication", command)
	}

	for _, want := range []string{
		"bash scripts/release/guard.sh validate-request",
		"bash scripts/release/guard.sh verify-public-run",
		"bash scripts/release/guard.sh verify-evidence",
		"make release-verify",
		"bash scripts/release/setup-crane.sh",
		"bash scripts/release/stage-image.sh",
		"bash scripts/release/publish-ecr.sh",
		"bash scripts/release/finalize-ecr-tag.sh",
		"python3 scripts/release/write-provenance.py",
		"cosign sign \"$PUBLISHED_IMAGE\"",
		"cosign attest --type cyclonedx",
		"cosign attest --type slsaprovenance",
		"cosign verify \"${identity[@]}\" \"$PUBLISHED_IMAGE\"",
		"cosign verify-attestation \"${identity[@]}\" --type cyclonedx",
		"cosign verify-attestation \"${identity[@]}\" --type slsaprovenance",
		"EXPECTED_CERTIFICATE_IDENTITY: https://github.com/mfow/llm-temporal-worker/.github/workflows/release.yml@refs/heads/master",
		"EXPECTED_OIDC_ISSUER: https://token.actions.githubusercontent.com",
		"RELEASE_PUBLICATION_IMAGE_REPOSITORY: ${{ vars.RELEASE_PUBLICATION_IMAGE_REPOSITORY }}",
		"ECR_REPOSITORY: ${{ vars.ECR_REPOSITORY }}",
		"AWS_ACCESS_KEY_ID: \"\"",
		"AWS_SECRET_ACCESS_KEY: \"\"",
		"AWS_SESSION_TOKEN: \"\"",
		"STAGED_OCI_LAYOUT: ${{ runner.temp }}/source.oci",
		`run: rm -rf -- "$STAGED_OCI_LAYOUT"`,
	} {
		if !strings.Contains(release.raw, want) {
			t.Fatalf("protected publication does not retain required contract %q", want)
		}
	}
	for _, forbidden := range []string{
		"Stop before external signing or publication",
		"Task 24 stops before external signing",
		"secrets.",
		":latest",
		" latest",
		"docker build",
		"buildx build",
		"needs.preflight.outputs",
		"aws-access-key-id:",
		"aws-secret-access-key:",
	} {
		if strings.Contains(strings.ToLower(release.raw), strings.ToLower(forbidden)) {
			t.Fatalf("release workflow contains forbidden mutable, static-credential, or trust-bypass contract %q", forbidden)
		}
	}

	assertProtectedPublicationContract(t, release)
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
		"credential-free HTTPS",
		"fixed unauthenticated",
		"`https://github.com/mfow/llm-temporal-worker.git`",
		"`github.sha`",
		"`.github/workflows/master.yml`",
		"only as the input to GitHub's",
		"does not create or modify that environment",
		"`AWS_ECR_PUBLISH_ROLE_ARN`",
		"`AWS_REGION`",
		"`ECR_REPOSITORY=llm-temporal-worker`",
		"`crane pull --format=oci`",
		"never contacts the source registry",
		"pushed-reference check and final ECR root digest equality",
		"`--platform linux/amd64`",
		"not multi-architecture",
		"never pushes `latest`",
		"fails publication",
	} {
		if !strings.Contains(runbook, want) {
			t.Fatalf("release runbook does not document guarded-publication prerequisite %q", want)
		}
	}
}

func TestReleaseImageScriptsStageBeforeCredentialedLocalPush(t *testing.T) {
	temp := t.TempDir()
	fakeBin := filepath.Join(temp, "bin")
	runnerTemp := filepath.Join(temp, "runner")
	if err := os.MkdirAll(fakeBin, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(runnerTemp, 0o755); err != nil {
		t.Fatal(err)
	}
	callLog := filepath.Join(temp, "crane.log")
	writeFakeCommand(t, fakeBin, "crane", `
printf '%s\n' "$*" >> "$FAKE_CRANE_LOG"
case "$1" in
  pull)
    [[ "$2" == "--format=oci" && "$3" == "$EXPECTED_SOURCE" && "$4" == "$EXPECTED_LAYOUT" ]]
    mkdir -p "$4"
    printf '{"imageLayoutVersion":"1.0.0"}\n' > "$4/oci-layout"
    printf '{"schemaVersion":2,"manifests":[]}\n' > "$4/index.json"
    ;;
  push)
    [[ "$2" == "$EXPECTED_LAYOUT" && "$3" == "$EXPECTED_DESTINATION" ]]
    printf '%s\n' "$3"
    ;;
  tag)
    [[ "$2" == "$EXPECTED_DESTINATION" && "$3" == "v1.2.3" ]]
    ;;
  digest)
    [[ "$2" == "$EXPECTED_DESTINATION" || "$2" == "$EXPECTED_TAGGED_DESTINATION" ]]
    printf '%s\n' "$EXPECTED_IMAGE_DIGEST"
    ;;
  *)
    exit 97
    ;;
esac
`)
	writeFakeCommand(t, fakeBin, "go", `printf '%s\n' "$EXPECTED_IMAGE_DIGEST"`)

	digest := "sha256:" + strings.Repeat("a", 64)
	source := "registry.example/source/llm-temporal-worker@" + digest
	layout := filepath.Join(runnerTemp, "source.oci")
	destination := "123456789012.dkr.ecr.us-east-2.amazonaws.com/llm-temporal-worker@" + digest
	taggedDestination := "123456789012.dkr.ecr.us-east-2.amazonaws.com/llm-temporal-worker:v1.2.3"
	commonEnvironment := []string{
		"PATH=" + fakeBin + string(os.PathListSeparator) + os.Getenv("PATH"),
		"FAKE_CRANE_LOG=" + callLog,
		"EXPECTED_IMAGE_DIGEST=" + digest,
		"EXPECTED_SOURCE=" + source,
		"EXPECTED_LAYOUT=" + layout,
		"EXPECTED_DESTINATION=" + destination,
		"EXPECTED_TAGGED_DESTINATION=" + taggedDestination,
		"RUNNER_TEMP=" + runnerTemp,
	}

	stage := exec.Command("bash", filepath.Join(repositoryRoot(t), "scripts", "release", "stage-image.sh"))
	stage.Env = append(commonEnvironment, "SOURCE_IMAGE_REFERENCE="+source, "STAGED_OCI_LAYOUT="+layout)
	if output, err := stage.CombinedOutput(); err != nil {
		t.Fatalf("stage image: %v\n%s", err, output)
	}
	calls, err := os.ReadFile(callLog)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(calls), "pull --format=oci "+source+" "+layout+"\n"; got != want {
		t.Fatalf("pre-OIDC Crane calls = %q, want %q", got, want)
	}

	githubOutput := filepath.Join(temp, "github-output")
	githubSummary := filepath.Join(temp, "github-summary")
	publish := exec.Command("bash", filepath.Join(repositoryRoot(t), "scripts", "release", "publish-ecr.sh"))
	publish.Env = append(commonEnvironment,
		"STAGED_OCI_LAYOUT="+layout,
		"AWS_ECR_PUBLISH_ROLE_ARN=arn:aws:iam::123456789012:role/llmtw-publisher",
		"AWS_ACCOUNT_ID=123456789012",
		"AWS_REGION=us-east-2",
		"ECR_REGISTRY=123456789012.dkr.ecr.us-east-2.amazonaws.com",
		"ECR_REPOSITORY=llm-temporal-worker",
		"GITHUB_OUTPUT="+githubOutput,
		"GITHUB_STEP_SUMMARY="+githubSummary,
	)
	if output, err := publish.CombinedOutput(); err != nil {
		t.Fatalf("publish staged image: %v\n%s", err, output)
	}
	finalize := exec.Command("bash", filepath.Join(repositoryRoot(t), "scripts", "release", "finalize-ecr-tag.sh"))
	finalize.Env = append(commonEnvironment,
		"PUBLISHED_IMAGE="+destination,
		"RELEASE_REF=refs/tags/v1.2.3",
		"ECR_REGISTRY=123456789012.dkr.ecr.us-east-2.amazonaws.com",
		"ECR_REPOSITORY=llm-temporal-worker",
		"GITHUB_OUTPUT="+githubOutput,
		"GITHUB_STEP_SUMMARY="+githubSummary,
	)
	if output, err := finalize.CombinedOutput(); err != nil {
		t.Fatalf("finalize immutable tag: %v\n%s", err, output)
	}
	calls, err = os.ReadFile(callLog)
	if err != nil {
		t.Fatal(err)
	}
	wantCalls := "pull --format=oci " + source + " " + layout + "\n" +
		"push " + layout + " " + destination + "\n" +
		"digest " + destination + "\n" +
		"tag " + destination + " v1.2.3\n" +
		"digest " + taggedDestination + "\n"
	if string(calls) != wantCalls {
		t.Fatalf("Crane calls = %q, want local-only publication %q", calls, wantCalls)
	}
	output, err := os.ReadFile(githubOutput)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(output), "published_image="+destination+"\npublished_tag="+taggedDestination+"\n"; got != want {
		t.Fatalf("publication output = %q, want %q", got, want)
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
		{pr: "#567", commit: "559a098f5633a8258e37b735191bbe94f1b13d82", run: "30790691313"},
		{pr: "#568", commit: "1aae4ff2095a5adaf3fee13aa696d78d73616ddd", run: "30795754773"},
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

func assertProtectedPublicationContract(t *testing.T, workflow workflowDocument) {
	t.Helper()

	idTokenJobs := 0
	for jobName, rawJob := range workflowMapping(t, workflow, "jobs") {
		job, ok := rawJob.(map[string]any)
		if !ok {
			t.Fatalf("%s job %q = %#v, want mapping", workflow.name, jobName, rawJob)
		}
		permissions := nestedMapping(t, workflow.name, job, "permissions")
		if permissions["id-token"] != nil {
			idTokenJobs++
			if jobName != "protected-signing-publication" || permissions["id-token"] != "write" {
				t.Fatalf("%s job %q has unauthorized id-token permission %#v", workflow.name, jobName, permissions["id-token"])
			}
		}
	}
	if idTokenJobs != 1 {
		t.Fatalf("%s must grant id-token permission to exactly one protected job, got %d", workflow.name, idTokenJobs)
	}

	protected := workflowJob(t, workflow, "protected-signing-publication")
	evidenceIndex, stageIndex, awsIndex, ecrIndex, publishIndex := -1, -1, -1, -1, -1
	signIndex, verifyIndex, finalizeIndex, cleanupIndex := -1, -1, -1, -1
	credentialBlankedSteps := make(map[string]map[string]any)
	var publishStep map[string]any
	for index, rawStep := range workflowSteps(t, workflow.name, "protected-signing-publication", protected) {
		step, ok := rawStep.(map[string]any)
		if !ok {
			continue
		}
		switch {
		case step["name"] == "Reverify complete evidence and exact publication digest":
			evidenceIndex = index
		case step["run"] == "bash scripts/release/stage-image.sh":
			stageIndex = index
		case step["uses"] == ecrConfigureCredentialsActionPin:
			awsIndex = index
		case step["uses"] == awsECRLoginActionPin:
			ecrIndex = index
		case step["run"] == "bash scripts/release/publish-ecr.sh":
			publishIndex = index
			publishStep = step
			credentialBlankedSteps["publisher"] = step
		case step["name"] == "Keyless-sign image, SBOM, and SLSA provenance":
			signIndex = index
			credentialBlankedSteps["signer"] = step
		case step["name"] == "Verify exact GitHub workflow identity and every signed subject":
			verifyIndex = index
			credentialBlankedSteps["signature verifier"] = step
		case step["run"] == "bash scripts/release/finalize-ecr-tag.sh":
			finalizeIndex = index
			credentialBlankedSteps["tag finalizer"] = step
		case step["name"] == "Remove staged OCI layout":
			cleanupIndex = index
			credentialBlankedSteps["cleanup"] = step
		}
	}
	if evidenceIndex < 0 || stageIndex <= evidenceIndex || awsIndex != stageIndex+1 ||
		ecrIndex != awsIndex+1 || publishIndex != ecrIndex+1 ||
		signIndex != publishIndex+1 || verifyIndex != signIndex+1 ||
		finalizeIndex != verifyIndex+1 || cleanupIndex != finalizeIndex+1 {
		t.Fatalf("protected publication must stage verified source before consecutive OIDC, ECR login, digest publication, signing, identity verification, immutable tagging, and cleanup steps")
	}
	for stepName, step := range credentialBlankedSteps {
		stepEnv := stepEnvironment(t, workflow.name, "protected-signing-publication", step)
		for _, name := range []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"} {
			if value, found := stepEnv[name]; !found || value != "" {
				t.Fatalf("protected publication %s shell must blank %s, got %#v", stepName, name, value)
			}
		}
	}
	publishEnv := stepEnvironment(t, workflow.name, "protected-signing-publication", publishStep)
	if _, found := publishEnv["SOURCE_IMAGE_REFERENCE"]; found {
		t.Fatal("credentialed publisher must not receive the source registry reference")
	}

	setup := readRepositoryFile(t, repositoryRoot(t), "scripts", "release", "setup-crane.sh")
	for _, want := range []string{
		`readonly crane_version="v0.20.3"`,
		`readonly crane_archive_sha256="36c67a932f489b3f2724b64af90b599a8ef2aa7b004872597373c0ad694dc059"`,
		`sha256sum --check --status`,
	} {
		if !strings.Contains(setup, want) {
			t.Fatalf("pinned Crane setup is missing %q", want)
		}
	}

	stager := readRepositoryFile(t, repositoryRoot(t), "scripts", "release", "stage-image.sh")
	for _, want := range []string{
		`[[ "${SOURCE_IMAGE_REFERENCE}" == *@"${EXPECTED_IMAGE_DIGEST}" ]]`,
		`[[ "${STAGED_OCI_LAYOUT}" == "${runner_temp}/source.oci" ]]`,
		`crane pull --format=oci "${SOURCE_IMAGE_REFERENCE}" "${STAGED_OCI_LAYOUT}"`,
		`layout-digest -layout "${STAGED_OCI_LAYOUT}"`,
		`[[ "${staged_digest}" == "${EXPECTED_IMAGE_DIGEST}" ]]`,
		"every child",
	} {
		if !strings.Contains(stager, want) {
			t.Fatalf("pre-OIDC OCI stager is missing %q", want)
		}
	}
	for _, forbidden := range []string{"--platform", "--index", "ECR_REGISTRY", "AWS_ACCOUNT_ID", "crane push"} {
		if strings.Contains(stager, forbidden) {
			t.Fatalf("pre-OIDC OCI stager contains forbidden selection, credential, or destination operation %q", forbidden)
		}
	}

	publisher := readRepositoryFile(t, repositoryRoot(t), "scripts", "release", "publish-ecr.sh")
	for _, want := range []string{
		`[[ "${STAGED_OCI_LAYOUT}" == "${runner_temp}/source.oci" ]]`,
		`[[ "${ECR_REPOSITORY}" == "llm-temporal-worker" ]]`,
		`expected_registry="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"`,
		`staged_digest="$(go -C "${root}/golang" run ./tools/releaseverify layout-digest -layout "${STAGED_OCI_LAYOUT}")"`,
		`destination="${ECR_REGISTRY}/${ECR_REPOSITORY}@${EXPECTED_IMAGE_DIGEST}"`,
		`pushed_reference="$(crane push "${STAGED_OCI_LAYOUT}" "${destination}")"`,
		`[[ "${pushed_reference}" == "${destination}" ]]`,
		`[[ "${pushed_digest}" == "${EXPECTED_IMAGE_DIGEST}" ]]`,
		`destination_digest="$(crane digest "${destination}")"`,
		`[[ "${destination_digest}" == "${staged_digest}" ]]`,
		`[[ "${destination_digest}" == "${pushed_digest}" ]]`,
		`printf 'published_image=%s\n' "${destination}"`,
	} {
		if !strings.Contains(publisher, want) {
			t.Fatalf("local-layout ECR publisher is missing %q", want)
		}
	}
	for _, forbidden := range []string{
		":latest",
		"--platform",
		"--index",
		"docker push",
		"aws ecr",
		"git push",
		"SOURCE_IMAGE_REFERENCE",
		"crane pull",
		"crane copy",
		"--remote",
	} {
		if strings.Contains(publisher, forbidden) {
			t.Fatalf("local-layout ECR publisher contains forbidden source, mutable, or broad operation %q", forbidden)
		}
	}

	finalizer := readRepositoryFile(t, repositoryRoot(t), "scripts", "release", "finalize-ecr-tag.sh")
	for _, want := range []string{
		`[[ "${RELEASE_REF}" =~ ^refs/tags/v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]`,
		`[[ "${PUBLISHED_IMAGE}" == "${ECR_REGISTRY}/${ECR_REPOSITORY}@${EXPECTED_IMAGE_DIGEST}" ]]`,
		`crane tag "${PUBLISHED_IMAGE}" "${release_tag}"`,
		`tagged_digest="$(crane digest "${tagged_destination}")"`,
		`[[ "${tagged_digest}" == "${EXPECTED_IMAGE_DIGEST}" ]]`,
		`printf 'published_tag=%s\n' "${tagged_destination}"`,
	} {
		if !strings.Contains(finalizer, want) {
			t.Fatalf("immutable ECR tag finalizer is missing %q", want)
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
	if strings.Count(workflow.raw, tokenExpression) != 2 {
		t.Fatalf("%s must contain exactly two explicit GitHub token expressions, one per independent artifact download", workflow.name)
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
				if (jobName != "preflight" && jobName != "protected-signing-publication") ||
					step["uses"] != downloadArtifactActionPin || input != "github-token" {
					t.Fatalf("%s job %q exposes the GitHub token outside pinned download-artifact", workflow.name, jobName)
				}
			}
		}
	}
	if usesToken != 2 {
		t.Fatalf("%s passes the GitHub token %d times, want once per independent evidence boundary", workflow.name, usesToken)
	}
}

func assertAnonymousFixedPublicCheckout(t *testing.T, workflow workflowDocument) {
	t.Helper()
	if strings.Contains(strings.ToLower(workflow.raw), "actions/checkout@") {
		t.Fatalf("%s must not give actions/checkout a GitHub token when the public anonymous bootstrap is required", workflow.name)
	}

	for _, jobName := range []string{"preflight", "protected-signing-publication"} {
		job := workflowJob(t, workflow, jobName)
		steps := workflowSteps(t, workflow.name, jobName, job)
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
			t.Fatalf("%s job %q must validate the manual release ref before its anonymous public checkout", workflow.name, jobName)
		}

		shapeEnv := stepEnvironment(t, workflow.name, jobName, shapeStep)
		if got := scalarString(t, workflow.name, shapeEnv, "RELEASE_REF"); got != "${{ inputs.release_ref }}" {
			t.Fatalf("%s job %q release-ref shape step input = %q, want explicit environment binding", workflow.name, jobName, got)
		}
		shapeRun := scalarString(t, workflow.name, shapeStep, "run")
		for _, want := range []string{
			`[[ "$RELEASE_REF" =~ ^refs/tags/v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]`,
			`printf 'release_ref=%s\n' "$RELEASE_REF" >> "$GITHUB_OUTPUT"`,
		} {
			if !strings.Contains(shapeRun, want) {
				t.Fatalf("%s job %q release-ref shape step does not retain %q", workflow.name, jobName, want)
			}
		}
		if strings.Contains(strings.ToLower(shapeRun), "git ") {
			t.Fatalf("%s job %q release-ref shape step must not fetch or execute a manual ref", workflow.name, jobName)
		}

		checkoutEnv := stepEnvironment(t, workflow.name, jobName, checkoutStep)
		if got := scalarString(t, workflow.name, checkoutEnv, "RELEASE_REF"); got != "${{ steps.release-ref.outputs.release_ref }}" {
			t.Fatalf("%s job %q anonymous checkout must use only the validated release ref, got %q", workflow.name, jobName, got)
		}
		if got := scalarString(t, workflow.name, checkoutEnv, "TRUSTED_MASTER_SHA"); got != "${{ github.sha }}" {
			t.Fatalf("%s job %q anonymous checkout must bind the protected workflow revision, got %q", workflow.name, jobName, got)
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
				t.Fatalf("%s job %q anonymous checkout does not retain %q", workflow.name, jobName, want)
			}
		}
		for _, forbidden := range []string{"github.token", "github_token", "gh_token", "actions/checkout"} {
			if strings.Contains(strings.ToLower(checkoutRun), forbidden) {
				t.Fatalf("%s job %q anonymous checkout exposes a credentialed checkout path %q", workflow.name, jobName, forbidden)
			}
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
