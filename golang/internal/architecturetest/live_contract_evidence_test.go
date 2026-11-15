package architecturetest

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const liveContractSourceRevision = "0123456789abcdef0123456789abcdef01234567"

// liveContractEvidenceFields is the complete redacted evidence record; the
// recorder writes exactly these fields and the schema requires all of them.
var liveContractEvidenceFields = []string{
	"schema_version",
	"generated_at",
	"source_revision",
	"profile",
	"tenant",
	"ceiling_micro_usd",
	"actual_spend",
	"request_id",
	"response_id",
	"actual_service_class",
	"continuation_verified",
}

// redactedEvidenceForbiddenFields must never appear in redacted live contract
// or SLO evidence, nor in the live contract schema.
var redactedEvidenceForbiddenFields = []string{"prompt", "output", "endpoint", "credential", "api_key", "raw"}

func TestLiveContractEvidenceRecorderWritesOnlyRedactedAllowlistedFields(t *testing.T) {
	root := repositoryRoot(t)
	paths := newLiveContractPaths(t)
	evidencePath, logPath := paths.evidence, paths.log
	validLiveContractRawLog().write(t, paths.raw)

	output, err := recordLiveContractEvidence(root, "openai-responses", paths, liveContractSourceRevision)
	if err != nil {
		t.Fatalf("record safe live contract evidence: %v\n%s", err, output)
	}
	if got := strings.TrimSpace(string(output)); got != "live contract evidence recorded" {
		t.Fatalf("record output = %q, want fixed success message", got)
	}

	var evidence map[string]any
	readJSONFile(t, evidencePath, "redacted evidence", &evidence)
	assertExactKeys(t, "evidence", evidence, liveContractEvidenceFields...)
	for _, forbidden := range redactedEvidenceForbiddenFields {
		if _, found := evidence[forbidden]; found {
			t.Fatalf("evidence retains forbidden %q field: %#v", forbidden, evidence)
		}
	}
	if got := evidence["profile"]; got != "openai-responses" {
		t.Fatalf("evidence profile = %#v, want openai-responses", got)
	}
	if got := evidence["source_revision"]; got != liveContractSourceRevision {
		t.Fatalf("evidence source_revision = %#v, want %q", got, liveContractSourceRevision)
	}
	if got := evidence["ceiling_micro_usd"]; got != float64(25_000) {
		t.Fatalf("evidence ceiling_micro_usd = %#v, want 25000", got)
	}
	spend := mappingField(t, "evidence actual_spend", evidence, "actual_spend")
	if got := spend["micro_usd"]; got != float64(17) {
		t.Fatalf("evidence actual spend = %#v, want 17", got)
	}

	log := readText(t, logPath)
	if strings.Contains(log, "live-contract-ok") || strings.Contains(log, "api.openai.com") {
		t.Fatalf("redacted log contains non-allowlisted content: %q", log)
	}
	if !strings.Contains(log, "source_revision="+liveContractSourceRevision+"\n") {
		t.Fatalf("redacted log does not bind the source revision: %q", log)
	}

	output, err = runLiveContractEvidence(root,
		"verify",
		"--evidence", evidencePath,
		"--log", logPath,
		"--source-revision", liveContractSourceRevision,
	)
	if err != nil {
		t.Fatalf("verify recorded live contract evidence: %v\n%s", err, output)
	}
	if got := strings.TrimSpace(string(output)); got != "live contract evidence verified" {
		t.Fatalf("verify output = %q, want fixed success message", got)
	}

	output, err = runLiveContractEvidence(root,
		"verify",
		"--evidence", evidencePath,
		"--log", logPath,
		"--source-revision", "fedcba9876543210fedcba9876543210fedcba98",
	)
	if err == nil {
		t.Fatalf("verify accepted evidence for a different source revision: %s", output)
	}
}

func TestLiveContractEvidenceRecorderRejectsInvalidSourceRevision(t *testing.T) {
	root := repositoryRoot(t)
	for _, revision := range []string{
		"0123456789abcdef0123456789abcdef0123456",
		"0123456789abcdef0123456789abcdef0123456g",
		"0123456789ABCDEF0123456789abcdef01234567",
	} {
		t.Run(revision, func(t *testing.T) {
			paths := newLiveContractPaths(t)
			validLiveContractRawLog().write(t, paths.raw)

			output, err := recordLiveContractEvidence(root, "openai-responses", paths, revision)
			if err == nil {
				t.Fatalf("record accepted invalid source revision %q: %s", revision, output)
			}
			paths.assertNoOutputs(t, "invalid source revision")
		})
	}
}

func TestLiveContractEvidenceRecorderRejectsUnsafeOrInconsistentInputWithoutEchoingIt(t *testing.T) {
	root := repositoryRoot(t)
	type rejectedInput struct {
		name   string
		log    liveContractRawLog
		secret string
	}
	withLog := func(change func(*liveContractRawLog)) liveContractRawLog {
		log := validLiveContractRawLog()
		change(&log)
		return log
	}
	var tests []rejectedInput
	for _, unsafe := range []struct{ name, requestID string }{
		{name: "unsafe request identifier", requestID: "sk-unsafe-live-secret"},
		{name: "mixed-case OpenAI-style request identifier", requestID: "Sk-unsafe-live-secret"},
		{name: "JWT-like request identifier", requestID: "EYJunsafe-live-secret"},
		{name: "AWS access-key-like request identifier", requestID: "AkIaunsafe-live-secret"},
		{name: "AWS session-like request identifier", requestID: "ASIAunsafe-live-secret"},
		{name: "GitHub token-like request identifier", requestID: "GHP_unsafe-live-secret"},
		{name: "GitHub fine-grained token-like request identifier", requestID: "GitHub_Pat_unsafe-live-secret"},
		{name: "Slack bot token-like request identifier", requestID: "XoXb-unsafe-live-secret"},
		{name: "Slack user token-like request identifier", requestID: "xOxP-unsafe-live-secret"},
		{name: "Google API key-like request identifier", requestID: "AiZaunsafe-live-secret"},
	} {
		tests = append(tests, rejectedInput{
			name:   unsafe.name,
			log:    withLog(func(log *liveContractRawLog) { log.requestID = unsafe.requestID }),
			secret: unsafe.requestID,
		})
	}
	tests = append(tests,
		rejectedInput{name: "unrecognized reported cost method", log: withLog(func(log *liveContractRawLog) { log.method = "catalog_usage" })},
		rejectedInput{name: "nonstandard actual service class", log: withLog(func(log *liveContractRawLog) { log.serviceClass = "priority" })},
		rejectedInput{name: "spend over ceiling", log: withLog(func(log *liveContractRawLog) { log.microUSD = 25_001 })},
		rejectedInput{name: "profile mismatch", log: withLog(func(log *liveContractRawLog) {
			log.profile = "openai-chat"
			log.known, log.microUSD, log.method, log.continued = false, 0, "not_reported", false
		})},
		rejectedInput{name: "unreported cost with a reported method", log: withLog(func(log *liveContractRawLog) {
			log.known, log.microUSD = false, 0
		})},
	)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			paths := newLiveContractPaths(t)
			test.log.write(t, paths.raw)

			output, err := recordLiveContractEvidence(root, "openai-responses", paths, liveContractSourceRevision)
			if err == nil {
				t.Fatalf("record accepted %s: %s", test.name, output)
			}
			if test.secret != "" && strings.Contains(string(output), test.secret) {
				t.Fatalf("recorder echoed rejected secret-like input: %q", output)
			}
			paths.assertNoOutputs(t, "rejected input")
		})
	}
}

func TestLiveContractEvidenceRecorderAllowsEachSupportedReportedCostMethod(t *testing.T) {
	root := repositoryRoot(t)
	for _, test := range []struct {
		profile string
		method  string
	}{
		{profile: "openai-responses", method: "provider_reported"},
		{profile: "openai-responses", method: "usage"},
		{profile: "openrouter-chat", method: "openrouter_reported"},
		{profile: "exa-chat", method: "exa_reported"},
	} {
		t.Run(test.method, func(t *testing.T) {
			paths := newLiveContractPaths(t)
			log := validLiveContractRawLog()
			log.profile, log.method = test.profile, test.method
			log.write(t, paths.raw)

			output, err := recordLiveContractEvidence(root, test.profile, paths, liveContractSourceRevision)
			if err != nil {
				t.Fatalf("record %s evidence: %v\\n%s", test.method, err, output)
			}

			var evidence struct {
				ActualSpend struct {
					Method string `json:"method"`
				} `json:"actual_spend"`
			}
			readJSONFile(t, paths.evidence, test.method+" evidence", &evidence)
			if evidence.ActualSpend.Method != test.method {
				t.Fatalf("actual spend method = %q, want %q", evidence.ActualSpend.Method, test.method)
			}
		})
	}
}

func TestLiveContractEvidenceSchemaIsClosedAndMatchesTheRecorderContract(t *testing.T) {
	root := repositoryRoot(t)
	var schema map[string]any
	readJSONFile(t, filepath.Join(root, "docs", "release", "live-provider-contract.schema.json"), "live provider evidence schema", &schema)
	if schema["additionalProperties"] != false {
		t.Fatalf("live provider evidence schema must be closed: %#v", schema["additionalProperties"])
	}
	got := stringSequence(t, "live provider evidence schema", schema, "required")
	if !sameStringSet(got, liveContractEvidenceFields) {
		t.Fatalf("live provider evidence schema required fields = %#v, want %#v", got, liveContractEvidenceFields)
	}
	properties := mappingField(t, "live provider evidence schema properties", schema, "properties")
	for _, field := range liveContractEvidenceFields {
		if _, ok := properties[field]; !ok {
			t.Fatalf("live provider evidence schema is missing %q", field)
		}
	}
	sourceRevision, ok := properties["source_revision"].(map[string]any)
	if !ok || sourceRevision["pattern"] != "^[0-9a-f]{40}$" {
		t.Fatalf("live provider source revision schema must require a full lowercase Git SHA, got %#v", properties["source_revision"])
	}
	for _, forbidden := range redactedEvidenceForbiddenFields {
		if _, found := properties[forbidden]; found {
			t.Fatalf("live provider evidence schema permits forbidden %q", forbidden)
		}
	}
	actualSpend := mappingField(t, "live provider actual spend schema", properties, "actual_spend")
	spendProperties := mappingField(t, "live provider actual spend properties", actualSpend, "properties")
	method, ok := spendProperties["method"].(map[string]any)
	if !ok || !sameStringSet(stringSequence(t, "live provider cost method", method, "enum"), []string{"provider_reported", "usage", "openrouter_reported", "exa_reported", "not_reported"}) {
		t.Fatalf("live provider evidence cost method must remain closed to recorded methods, got %#v", spendProperties["method"])
	}
	allOfSpend, ok := actualSpend["allOf"].([]any)
	if !ok {
		t.Fatalf("live provider actual spend allOf = %#v, want sequence", actualSpend["allOf"])
	}
	var knownMethod map[string]any
	for _, rawRule := range allOfSpend {
		rule, ok := rawRule.(map[string]any)
		if !ok {
			continue
		}
		condition, ok := rule["if"].(map[string]any)
		if !ok {
			continue
		}
		conditionProperties, ok := condition["properties"].(map[string]any)
		if !ok {
			continue
		}
		known, ok := conditionProperties["known"].(map[string]any)
		if !ok || known["const"] != true {
			continue
		}
		then, ok := rule["then"].(map[string]any)
		if !ok {
			continue
		}
		thenProperties, ok := then["properties"].(map[string]any)
		if !ok {
			continue
		}
		knownMethod, _ = thenProperties["method"].(map[string]any)
	}
	if !sameStringSet(stringSequence(t, "known live provider cost method", knownMethod, "enum"), []string{"provider_reported", "usage", "openrouter_reported", "exa_reported"}) {
		t.Fatalf("known live provider evidence cost method must remain closed to provider-reported methods, got %#v", knownMethod)
	}
	actualServiceClass, ok := properties["actual_service_class"].(map[string]any)
	if !ok || actualServiceClass["const"] != "standard" {
		t.Fatalf("live provider actual service class must be const standard, got %#v", properties["actual_service_class"])
	}
	definitions := mappingField(t, "live provider evidence schema definitions", schema, "$defs")
	identifier := mappingField(t, "live provider safe identifier schema", definitions, "safe_identifier")
	allOf, ok := identifier["allOf"].([]any)
	if !ok {
		t.Fatalf("live provider safe identifier allOf = %#v, want sequence", identifier["allOf"])
	}
	gotSecretPatterns := make(map[string]bool)
	for _, rawRule := range allOf {
		rule, ok := rawRule.(map[string]any)
		if !ok {
			continue
		}
		not, ok := rule["not"].(map[string]any)
		if !ok {
			continue
		}
		if pattern, ok := not["pattern"].(string); ok {
			gotSecretPatterns[pattern] = true
		}
	}
	for _, pattern := range []string{
		"^[sS][kK][-_]",
		"^[eE][yY][jJ]",
		"^[aA][kK][iI][aA]",
		"^[aA][sS][iI][aA]",
		"^[gG][hH][pP]_",
		"^[gG][iI][tT][hH][uU][bB]_[pP][aA][tT]_",
		"^[xX][oO][xX][bB]-",
		"^[xX][oO][xX][pP]-",
		"^[aA][iI][zZ][aA]",
	} {
		if !gotSecretPatterns[pattern] {
			t.Fatalf("live provider safe identifier schema does not reject recorder secret pattern %q: %#v", pattern, gotSecretPatterns)
		}
	}
}

func runLiveContractEvidence(root string, arguments ...string) ([]byte, error) {
	return runReleaseScript(root, "live-contract-evidence.py", arguments...)
}

// liveContractPaths are the recorder's raw input and redacted outputs inside
// one temporary directory.
type liveContractPaths struct {
	raw, evidence, log string
}

func newLiveContractPaths(t *testing.T) liveContractPaths {
	t.Helper()
	directory := t.TempDir()
	return liveContractPaths{
		raw:      filepath.Join(directory, "live-contract.raw.log"),
		evidence: filepath.Join(directory, "evidence.json"),
		log:      filepath.Join(directory, "live-contract.log"),
	}
}

// assertNoOutputs fails if the recorder left evidence or a redacted log
// behind after rejecting its input for reason.
func (paths liveContractPaths) assertNoOutputs(t *testing.T, reason string) {
	t.Helper()
	for _, path := range []string{paths.evidence, paths.log} {
		if _, statErr := os.Lstat(path); !os.IsNotExist(statErr) {
			t.Fatalf("recorder left output %q after %s: %v", path, reason, statErr)
		}
	}
}

func recordLiveContractEvidence(root, profile string, paths liveContractPaths, sourceRevision string) ([]byte, error) {
	return runLiveContractEvidence(root,
		"record",
		"--profile", profile,
		"--input", paths.raw,
		"--evidence", paths.evidence,
		"--log", paths.log,
		"--source-revision", sourceRevision,
		"--generated-at", "2026-07-15T00:00:00Z",
	)
}

// liveContractRawLog is the one live provider test result line the recorder
// redacts.
type liveContractRawLog struct {
	profile, requestID, responseID, serviceClass string
	known                                        bool
	microUSD                                     int
	method                                       string
	continued                                    bool
}

func validLiveContractRawLog() liveContractRawLog {
	return liveContractRawLog{
		profile:      "openai-responses",
		requestID:    "req_123",
		responseID:   "resp_456",
		serviceClass: "standard",
		known:        true,
		microUSD:     17,
		method:       "provider_reported",
		continued:    true,
	}
}

func (log liveContractRawLog) write(t *testing.T, path string) {
	t.Helper()
	content := strings.Join([]string{
		"=== RUN   TestLiveProviderContracts/" + log.profile,
		"    live_provider_test.go:35: profile=" + log.profile + " tenant=llmtw-live-contract request_id=" + log.requestID + " response_id=" + log.responseID + " actual_service_class=" + log.serviceClass + " actual_spend_known=" + strconv.FormatBool(log.known) + " actual_micro_usd=" + strconv.Itoa(log.microUSD) + " cost_method=" + log.method + " continuation_verified=" + strconv.FormatBool(log.continued),
		"--- PASS: TestLiveProviderContracts/" + log.profile,
	}, "\n") + "\n"
	writeFile(t, path, []byte(content), 0o600)
}
