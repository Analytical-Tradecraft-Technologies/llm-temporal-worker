package architecturetest

import (
	"regexp"
	"sort"
	"strings"
	"testing"
)

const (
	liveProviderWorkflow                  = "live-provider-contracts.yml"
	azureLoginActionPin                   = "azure/login@a457da9ea143d694b1b9c7c869ebb04ebe844ef5"
	awsConfigureCredentialsActionPin      = "aws-actions/configure-aws-credentials@e3dd6a429d7300a6a4c196c26e071d42e0343502"
	anonymousLiveProviderCheckoutStepName = "Check out fixed public master anonymously"
)

type liveProviderWorkflowProfile struct {
	id             string
	enableEnv      string
	secretName     string
	requiresOIDC   bool
	credentialKind string
}

var liveProviderWorkflowProfiles = []liveProviderWorkflowProfile{
	{id: "openai-responses", enableEnv: "LLMTW_LIVE_OPENAI_RESPONSES", secretName: "OPENAI_API_KEY", credentialKind: "api key"},
	{id: "azure-responses", enableEnv: "LLMTW_LIVE_AZURE_RESPONSES", requiresOIDC: true, credentialKind: "Azure workload identity"},
	{id: "openai-chat", enableEnv: "LLMTW_LIVE_OPENAI_CHAT", secretName: "OPENAI_API_KEY", credentialKind: "api key"},
	{id: "openrouter-chat", enableEnv: "LLMTW_LIVE_OPENROUTER_CHAT", secretName: "OPENROUTER_API_KEY", credentialKind: "api key"},
	{id: "exa-chat", enableEnv: "LLMTW_LIVE_EXA_CHAT", secretName: "EXA_API_KEY", credentialKind: "api key"},
	{id: "anthropic-direct", enableEnv: "LLMTW_LIVE_ANTHROPIC_DIRECT", secretName: "ANTHROPIC_API_KEY", credentialKind: "api key"},
	{id: "anthropic-aws", enableEnv: "LLMTW_LIVE_ANTHROPIC_AWS", requiresOIDC: true, credentialKind: "AWS workload identity"},
	{id: "bedrock-anthropic", enableEnv: "LLMTW_LIVE_BEDROCK_ANTHROPIC", requiresOIDC: true, credentialKind: "AWS workload identity"},
	{id: "bedrock-converse", enableEnv: "LLMTW_LIVE_BEDROCK_CONVERSE", requiresOIDC: true, credentialKind: "AWS workload identity"},
}

func TestLiveProviderContractsWorkflowIsManualProtectedAndSingleProfile(t *testing.T) {
	workflow := readWorkflow(t, liveProviderWorkflow)
	triggers := workflowMapping(t, workflow, "on")
	if len(triggers) != 1 {
		t.Fatalf("%s triggers = %#v, want only workflow_dispatch", workflow.name, triggers)
	}
	dispatch := nestedMapping(t, workflow.name, triggers, "workflow_dispatch")
	inputs := nestedMapping(t, workflow.name, dispatch, "inputs")
	profileInput := nestedMapping(t, workflow.name, inputs, "profile")
	if required, ok := profileInput["required"].(bool); !ok || !required {
		t.Fatalf("%s profile input must be required, got %#v", workflow.name, profileInput["required"])
	}
	if scalarString(t, workflow.name, profileInput, "type") != "choice" {
		t.Fatalf("%s profile input must be a closed choice", workflow.name)
	}
	if got, want := stringSequence(t, workflow.name, profileInput, "options"), liveProviderProfileIDs(); !sameStringSet(got, want) {
		t.Fatalf("%s profile options = %#v, want %#v", workflow.name, got, want)
	}

	assertReadOnlyPermissions(t, workflow)
	assertLiveProviderWorkflowSourceAndActionBoundary(t, workflow)
	jobs := workflowMapping(t, workflow, "jobs")
	if len(jobs) != len(liveProviderWorkflowProfiles)+1 {
		t.Fatalf("%s jobs = %#v, want validate-request plus exactly one job per profile", workflow.name, jobs)
	}
	validate := workflowJob(t, workflow, "validate-request")
	if scalarString(t, workflow.name, validate, "if") != "github.event_name == 'workflow_dispatch' && github.ref == 'refs/heads/master'" {
		t.Fatalf("%s validate-request must reject non-master dispatches", workflow.name)
	}
	assertJobReadOnlyPermissions(t, workflow.name, "validate-request", validate)
	if strings.Contains(strings.ToLower(workflow.raw), "secrets[") || strings.Contains(strings.ToLower(workflow.raw), "fromjson(secrets") {
		t.Fatalf("%s must not dynamically select a credential", workflow.name)
	}

	for _, profile := range liveProviderWorkflowProfiles {
		profile := profile
		t.Run(profile.id, func(t *testing.T) {
			job := workflowJob(t, workflow, profile.id)
			if scalarString(t, workflow.name, job, "needs") != "validate-request" {
				t.Fatalf("%s job %q must follow validate-request", workflow.name, profile.id)
			}
			wantIf := "github.event_name == 'workflow_dispatch' && github.ref == 'refs/heads/master' && inputs.profile == '" + profile.id + "'"
			if scalarString(t, workflow.name, job, "if") != wantIf {
				t.Fatalf("%s job %q if = %q, want %q", workflow.name, profile.id, scalarString(t, workflow.name, job, "if"), wantIf)
			}
			environment := nestedMapping(t, workflow.name, job, "environment")
			if scalarString(t, workflow.name, environment, "name") != "live-provider-contracts" {
				t.Fatalf("%s job %q environment = %#v, want protected live-provider-contracts", workflow.name, profile.id, environment)
			}

			wantPermissions := map[string]string{"contents": "read"}
			if profile.requiresOIDC {
				wantPermissions["id-token"] = "write"
			}
			assertLiveProviderJobPermissions(t, workflow.name, profile.id, job, wantPermissions)
			assertLiveProviderCredentialAction(t, workflow, profile, job)
			assertJobUsesAction(t, workflow, profile.id, setupGoActionPin)
			assertJobUsesAction(t, workflow, profile.id, uploadArtifactActionPin)

			testStep := namedWorkflowStep(t, workflow, job, "Run bounded live provider contract")
			env := nestedMapping(t, workflow.name, testStep, "env")
			assertExactLiveTestEnvironment(t, workflow.name, profile, env)
			run := scalarString(t, workflow.name, testStep, "run")
			if !strings.Contains(run, "go test -v -tags=live ./integration/live -run '^TestLiveProviderContracts$/^"+profile.id+"$'") {
				t.Fatalf("%s job %q does not run only its named profile", workflow.name, profile.id)
			}
			if !strings.Contains(run, "> \"$RUNNER_TEMP/live-contract.raw.log\" 2>&1") || strings.Contains(run, "tee ") {
				t.Fatalf("%s job %q must retain raw provider output only in runner temporary storage", workflow.name, profile.id)
			}

			record := namedWorkflowStep(t, workflow, job, "Record and verify redacted live evidence")
			if _, found := record["env"]; found {
				t.Fatalf("%s job %q recorder must not receive provider credentials", workflow.name, profile.id)
			}
			recordRun := scalarString(t, workflow.name, record, "run")
			if !strings.Contains(recordRun, "env -i PATH=\"$PATH\"") {
				t.Fatalf("%s job %q recorder must clear inherited credential environment", workflow.name, profile.id)
			}
			for _, command := range []string{"scripts/release/live-contract-evidence.py record", "scripts/release/live-contract-evidence.py verify"} {
				if !strings.Contains(recordRun, command) {
					t.Fatalf("%s job %q recorder step is missing %q", workflow.name, profile.id, command)
				}
			}
			if got := strings.Count(recordRun, "--source-revision \"${{ github.sha }}\""); got != 2 {
				t.Fatalf("%s job %q must bind both recorder and validator to github.sha, found %d source revision arguments", workflow.name, profile.id, got)
			}
			if !strings.Contains(workflow.raw, "name: live-provider-contract-"+profile.id+"-evidence") {
				t.Fatalf("%s does not retain a profile-specific redacted evidence artifact for %q", workflow.name, profile.id)
			}
		})
	}

	assertExactWorkflowSecretReferences(t, workflow)
}

func TestLiveProviderContractsDocumentationListsEveryWorkflowProfile(t *testing.T) {
	root := repositoryRoot(t)
	documentation := readRepositoryFile(t, root, "docs", "reference", "live-provider-contracts.md")
	rows := strings.Split(documentation, "\n")

	for _, profile := range liveProviderWorkflowProfiles {
		profile := profile
		t.Run(profile.id, func(t *testing.T) {
			marker := "| `" + profile.id + "` |"
			var matches []string
			for _, row := range rows {
				if strings.HasPrefix(row, marker) {
					matches = append(matches, row)
				}
			}
			if len(matches) != 1 {
				t.Fatalf("live-provider documentation rows for %q = %d, want exactly one", profile.id, len(matches))
			}
			if want := "`" + profile.enableEnv + "`"; !strings.Contains(matches[0], want) {
				t.Fatalf("live-provider documentation row for %q does not name gate %q: %s", profile.id, want, matches[0])
			}
		})
	}
}

func TestLiveContractOfflineTargetDisablesEveryProfileGate(t *testing.T) {
	makefile := readRepositoryFile(t, moduleRoot(t), "Makefile")
	target := makeTarget(t, makefile, "live-contract-verify:\n", "\n\nbuild:\n")

	for _, gate := range []string{"LLMTW_LIVE_TESTS", "LLMTW_LIVE_AUTHORIZED"} {
		assignment := "\t\t" + gate + "=0 \\\n"
		if strings.Count(target, assignment) != 1 {
			t.Fatalf("live-contract-verify must disable %s exactly once, found %d", gate, strings.Count(target, assignment))
		}
	}
	for _, profile := range liveProviderWorkflowProfiles {
		assignment := "\t\t" + profile.enableEnv + "=0 \\\n"
		if strings.Count(target, assignment) != 1 {
			t.Fatalf("live-contract-verify must disable profile %q exactly once, found %d", profile.enableEnv, strings.Count(target, assignment))
		}
	}
}

func assertLiveProviderWorkflowSourceAndActionBoundary(t *testing.T, workflow workflowDocument) {
	t.Helper()
	if strings.Contains(workflow.raw, "actions/checkout@") {
		t.Fatalf("%s must use credential-free fixed-source checkout, not actions/checkout", workflow.name)
	}
	for _, forbidden := range []string{"github.token", "secrets.GITHUB_TOKEN", "secrets.github_token"} {
		if strings.Contains(workflow.raw, forbidden) {
			t.Fatalf("%s must not expose an automatic GitHub token through %q", workflow.name, forbidden)
		}
	}
	for _, reference := range actionReferences(t, workflow) {
		parts := strings.SplitN(reference, "@", 2)
		if len(parts) != 2 || !immutableActionReference.MatchString(parts[1]) {
			t.Fatalf("%s action %q is not pinned to an immutable commit", workflow.name, reference)
		}
	}
	for _, want := range []string{
		"uses: " + setupGoActionPin + " # v6",
		"uses: " + uploadArtifactActionPin + " # v4",
		"uses: " + azureLoginActionPin + " # v2.3.0",
		"uses: " + awsConfigureCredentialsActionPin + " # v4.0.2",
	} {
		if !strings.Contains(workflow.raw, want) {
			t.Fatalf("%s does not retain readable immutable action pin %q", workflow.name, want)
		}
	}
	if got, want := strings.Count(workflow.raw, "name: "+anonymousLiveProviderCheckoutStepName), len(liveProviderWorkflowProfiles); got != want {
		t.Fatalf("%s anonymous checkout count = %d, want %d profile-local checkouts", workflow.name, got, want)
	}
	for _, profile := range liveProviderWorkflowProfiles {
		job := workflowJob(t, workflow, profile.id)
		checkout := namedWorkflowStep(t, workflow, job, anonymousLiveProviderCheckoutStepName)
		env := nestedMapping(t, workflow.name, checkout, "env")
		if scalarString(t, workflow.name, env, "TRUSTED_MASTER_SHA") != "${{ github.sha }}" {
			t.Fatalf("%s job %q must bind anonymous checkout to github.sha", workflow.name, profile.id)
		}
		run := scalarString(t, workflow.name, checkout, "run")
		for _, want := range []string{
			"GIT_CONFIG_NOSYSTEM=1",
			"GIT_TERMINAL_PROMPT=0",
			"GIT_ASKPASS=/bin/false",
			"git init --quiet \"$GITHUB_WORKSPACE\"",
			"https://github.com/mfow/llm-temporal-worker.git",
			"-c credential.helper= -c http.extraHeader= fetch --no-tags --force origin",
			"refs/remotes/origin/master",
		} {
			if !strings.Contains(run, want) {
				t.Fatalf("%s job %q anonymous checkout is missing %q", workflow.name, profile.id, want)
			}
		}
	}
}

func assertLiveProviderCredentialAction(t *testing.T, workflow workflowDocument, profile liveProviderWorkflowProfile, job map[string]any) {
	t.Helper()
	references := actionReferencesForJob(t, workflow, profile.id, job)
	switch profile.id {
	case "azure-responses":
		assertJobUsesAction(t, workflow, profile.id, azureLoginActionPin)
		for _, reference := range references {
			if reference == awsConfigureCredentialsActionPin {
				t.Fatalf("%s job %q must not configure an AWS credential", workflow.name, profile.id)
			}
		}
	case "anthropic-aws", "bedrock-anthropic", "bedrock-converse":
		assertJobUsesAction(t, workflow, profile.id, awsConfigureCredentialsActionPin)
		for _, reference := range references {
			if reference == azureLoginActionPin {
				t.Fatalf("%s job %q must not configure an Azure credential", workflow.name, profile.id)
			}
		}
	default:
		for _, reference := range references {
			if reference == azureLoginActionPin || reference == awsConfigureCredentialsActionPin {
				t.Fatalf("%s job %q must not configure an OIDC cloud credential", workflow.name, profile.id)
			}
		}
	}
}

func actionReferencesForJob(t *testing.T, workflow workflowDocument, jobName string, job map[string]any) []string {
	t.Helper()
	steps, ok := job["steps"].([]any)
	if !ok {
		t.Fatalf("%s job %q steps = %#v, want sequence", workflow.name, jobName, job["steps"])
	}
	var references []string
	for _, rawStep := range steps {
		step, ok := rawStep.(map[string]any)
		if !ok {
			continue
		}
		if reference, ok := step["uses"].(string); ok {
			references = append(references, reference)
		}
	}
	return references
}

func liveProviderProfileIDs() []string {
	ids := make([]string, 0, len(liveProviderWorkflowProfiles))
	for _, profile := range liveProviderWorkflowProfiles {
		ids = append(ids, profile.id)
	}
	return ids
}

func sameStringSet(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	got = append([]string(nil), got...)
	want = append([]string(nil), want...)
	sort.Strings(got)
	sort.Strings(want)
	return strings.Join(got, "\x00") == strings.Join(want, "\x00")
}

func assertLiveProviderJobPermissions(t *testing.T, workflowName, jobName string, job map[string]any, want map[string]string) {
	t.Helper()
	permissions := nestedMapping(t, workflowName, job, "permissions")
	if len(permissions) != len(want) {
		t.Fatalf("%s job %q permissions = %#v, want %#v", workflowName, jobName, permissions, want)
	}
	for name, expected := range want {
		if actual, ok := permissions[name].(string); !ok || actual != expected {
			t.Fatalf("%s job %q permission %q = %#v, want %q", workflowName, jobName, name, permissions[name], expected)
		}
	}
}

func namedWorkflowStep(t *testing.T, workflow workflowDocument, job map[string]any, name string) map[string]any {
	t.Helper()
	steps, ok := job["steps"].([]any)
	if !ok {
		t.Fatalf("%s job has no steps", workflow.name)
	}
	for _, rawStep := range steps {
		step, ok := rawStep.(map[string]any)
		if !ok {
			continue
		}
		if stepName, _ := step["name"].(string); stepName == name {
			return step
		}
	}
	t.Fatalf("%s is missing step %q", workflow.name, name)
	return nil
}

func assertExactLiveTestEnvironment(t *testing.T, workflowName string, profile liveProviderWorkflowProfile, env map[string]any) {
	t.Helper()
	want := map[string]string{
		"LLMTW_LIVE_TESTS":      "1",
		"LLMTW_LIVE_AUTHORIZED": "1",
		profile.enableEnv:       "1",
	}
	if profile.secretName != "" {
		want[profile.secretName] = "${{ secrets." + profile.secretName + " }}"
	}
	if profile.id == "azure-responses" {
		want["LLMTW_LIVE_AZURE_OPENAI_ENDPOINT"] = "${{ vars.LLMTW_LIVE_AZURE_OPENAI_ENDPOINT }}"
	}
	if profile.id == "anthropic-aws" {
		want["LLMTW_LIVE_ANTHROPIC_AWS_WORKSPACE_ID"] = "${{ vars.LLMTW_LIVE_ANTHROPIC_AWS_WORKSPACE_ID }}"
	}
	if len(env) != len(want) {
		t.Fatalf("%s profile %q live-test env = %#v, want exactly %#v", workflowName, profile.id, env, want)
	}
	for name, expected := range want {
		if actual, ok := env[name].(string); !ok || actual != expected {
			t.Fatalf("%s profile %q env %q = %#v, want %q", workflowName, profile.id, name, env[name], expected)
		}
	}
}

func assertExactWorkflowSecretReferences(t *testing.T, workflow workflowDocument) {
	t.Helper()
	references := regexp.MustCompile(`secrets\.([A-Z0-9_]+)`).FindAllStringSubmatch(workflow.raw, -1)
	got := make([]string, 0, len(references))
	for _, reference := range references {
		got = append(got, reference[1])
	}
	want := []string{"OPENAI_API_KEY", "OPENAI_API_KEY", "OPENROUTER_API_KEY", "EXA_API_KEY", "ANTHROPIC_API_KEY"}
	if !sameStringSet(got, want) {
		t.Fatalf("%s secret references = %#v, want only profile-local API credentials %#v", workflow.name, got, want)
	}
}
