package joined_test

import (
	"os"
	"path/filepath"
	"testing"

	workerconfig "github.com/mfow/llm-temporal-worker/golang/config"
	"go.yaml.in/yaml/v4"
)

type composeContract struct {
	Services map[string]struct {
		Command     []string          `yaml:"command"`
		Entrypoint  []string          `yaml:"entrypoint"`
		Environment map[string]string `yaml:"environment"`
		Profiles    []string          `yaml:"profiles"`
		User        string            `yaml:"user"`
		Volumes     []struct {
			Target   string `yaml:"target"`
			ReadOnly bool   `yaml:"read_only"`
		} `yaml:"volumes"`
		Healthcheck struct {
			Test    []string `yaml:"test"`
			Timeout string   `yaml:"timeout"`
		} `yaml:"healthcheck"`
	} `yaml:"services"`
}

func TestJoinedTopologyUsesBothProductionTaskQueues(t *testing.T) {
	data, err := os.ReadFile("compose.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var topology composeContract
	if err = yaml.Unmarshal(data, &topology); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"joined-smoke", "schema-install", "budget-bootstrap", "worker", "victoria-worker", "victoria-engine", "victoria-btf25-engine"} {
		if _, ok := topology.Services[name]; !ok {
			t.Errorf("joined topology is missing %s", name)
		}
	}
	if got := topology.Services["worker"].Command; len(got) != 3 || got[0] != "worker" {
		t.Fatalf("LLM worker command = %#v", got)
	}
	if got := topology.Services["redis"].Environment["REDIS_KEY_PREFIX"]; got != "joined_smoke" {
		t.Fatalf("Redis ACL key prefix = %q, want joined_smoke", got)
	}
	if got := topology.Services["worker"].Environment["LLMTW_REDIS_KEY_PREFIX"]; got != "joined_smoke" {
		t.Fatalf("worker Redis key prefix = %q, want joined_smoke", got)
	}
	if got := topology.Services["budget-bootstrap"].Environment["DATABASE_URL"]; got == "" {
		t.Fatal("budget bootstrap PostgreSQL authority is empty")
	}
	if got, want := topology.Services["budget-bootstrap"].Environment["WORKER_POSTGRES_SCOPE_KEY"], topology.Services["worker"].Environment["WORKER_POSTGRES_SCOPE_KEY"]; got == "" || got != want {
		t.Fatalf("budget bootstrap scope key does not match worker: got %q want %q", got, want)
	}
	if got := topology.Services["budget-bootstrap"].Environment["PRICING_IDENTITY_FILE"]; got != "/run/smoke/pricing-identity.json" {
		t.Fatalf("budget bootstrap pricing identity handoff = %q", got)
	}
	if got := topology.Services["victoria-worker"].Environment["TEMPORAL_TASK_QUEUE"]; got != "ai-ach-competition" {
		t.Fatalf("Victoria task queue = %q", got)
	}
	if got := topology.Services["victoria-worker"].Environment["TEMPORAL_API_KEY_FILE"]; got != "/run/secrets/temporal-api-key" {
		t.Fatalf("Victoria Temporal API key file = %q", got)
	}
	for _, legacy := range []string{"TEMPORAL_JWT_FILE", "TEMPORAL_TLS_CERT_FILE", "TEMPORAL_TLS_KEY_FILE"} {
		if _, found := topology.Services["victoria-worker"].Environment[legacy]; found {
			t.Fatalf("Victoria worker retains legacy Temporal authentication variable %s", legacy)
		}
	}
	if got := topology.Services["victoria-worker"].User; got != "65532:65532" {
		t.Fatalf("Victoria worker user = %q, want 65532:65532", got)
	}
	if got := topology.Services["victoria-worker"].Environment["AI_ACH_SEARCH_API_URL"]; got != "https://joined-smoke" {
		t.Fatalf("Victoria search origin = %q, want path-free joined origin", got)
	}
	if got := topology.Services["victoria-worker"].Environment["SSL_CERT_FILE"]; got != "/run/smoke/ca.pem" {
		t.Fatalf("Victoria smoke trust bundle = %q", got)
	}
	if got := topology.Services["victoria-worker"].Healthcheck.Test; len(got) != 5 || got[1] != "/opt/venv/bin/python" || got[4] != "ready" {
		t.Fatalf("Victoria healthcheck = %#v", got)
	}
	if got := topology.Services["victoria-worker"].Healthcheck.Timeout; got != "20s" {
		t.Fatalf("Victoria healthcheck timeout = %q, want 20s", got)
	}
	if got := topology.Services["victoria-engine"].Entrypoint; len(got) != 1 || got[0] != "/opt/ai-ach/bin/prophet-temporal-engine" {
		t.Fatalf("Prophet engine entrypoint = %#v", got)
	}
	if got := topology.Services["victoria-btf25-engine"].Entrypoint; len(got) != 1 || got[0] != "/opt/ai-ach/bin/btf25-temporal-smoke" {
		t.Fatalf("BTF25 engine entrypoint = %#v", got)
	}
	if got := topology.Services["victoria-worker"].Environment["AI_ACH_PRICING_IDENTITY_FILE"]; got != "/run/smoke/pricing-identity.json" {
		t.Fatalf("Victoria pricing identity handoff = %q", got)
	}
	if got := topology.Services["worker"].Environment["BTF25_REAL_PROVIDER_API_KEY"]; got != "${BTF25_REAL_PROVIDER_API_KEY:-}" {
		t.Fatalf("real-provider credential gate = %q", got)
	}
	engine := topology.Services["victoria-btf25-engine"]
	if len(engine.Profiles) != 1 || engine.Profiles[0] != "btf25" {
		t.Fatalf("BTF25 engine profile = %#v", engine.Profiles)
	}
	targets := make(map[string]bool, len(engine.Volumes))
	targetCounts := make(map[string]int, len(engine.Volumes))
	for _, volume := range engine.Volumes {
		targets[volume.Target] = volume.ReadOnly
		targetCounts[volume.Target]++
	}
	if readOnly, ok := targets["/run/btf25-output"]; !ok || readOnly {
		t.Fatalf("BTF25 output mount = %#v, want writable", targets)
	}
	for _, target := range []string{"/run/btf25-input/profile.json", "/run/btf25-input/corpus.parquet", "/run/btf25-input/license-ack.json"} {
		if !targets[target] {
			t.Fatalf("BTF25 input mount %q is missing or writable: %#v", target, targets)
		}
	}
	for _, target := range []string{"/run/smoke", "/run/secrets", "/fixtures", "/run/release-attestation.json", "/run/release-trust-root.json", "/run/resource-capacity.json"} {
		if !targets[target] || targetCounts[target] != 1 {
			t.Fatalf("BTF25 inherited runtime mount %q must occur exactly once and be read-only: targets=%#v counts=%#v", target, targets, targetCounts)
		}
	}
	for _, name := range []string{"WORKER_POSTGRES_ENVELOPE_KEY", "WORKER_POSTGRES_SCOPE_KEY", "CONTINUATION_HMAC"} {
		if got := topology.Services["worker"].Environment[name]; len(got) != 32 {
			t.Fatalf("worker %s length = %d, want 32", name, len(got))
		}
	}

	configData, err := os.ReadFile("llm-config.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var config struct {
		Temporal struct {
			TaskQueue  string `yaml:"task_queue"`
			APIKeyFile string `yaml:"api_key_file"`
		} `yaml:"temporal"`
		Budgets struct {
			Policies []struct {
				Match struct {
					Tenant  string `yaml:"tenant"`
					Project string `yaml:"project"`
				} `yaml:"match"`
			} `yaml:"policies"`
		} `yaml:"budgets"`
	}
	if err = yaml.Unmarshal(configData, &config); err != nil {
		t.Fatal(err)
	}
	if config.Temporal.TaskQueue != "llm-inference" {
		t.Fatalf("LLM task queue = %q", config.Temporal.TaskQueue)
	}
	if config.Temporal.APIKeyFile != topology.Services["victoria-worker"].Environment["TEMPORAL_API_KEY_FILE"] {
		t.Fatalf("Temporal clients use inconsistent API key files: LLM=%q Victoria=%q", config.Temporal.APIKeyFile, topology.Services["victoria-worker"].Environment["TEMPORAL_API_KEY_FILE"])
	}
	if len(config.Budgets.Policies) != 1 || config.Budgets.Policies[0].Match.Tenant != "att" || config.Budgets.Policies[0].Match.Project != "competition" {
		t.Fatalf("joined budget scope = %#v, want att/competition", config.Budgets.Policies)
	}
}

func TestJoinedResourceCapacityFixtureAuthenticates(t *testing.T) {
	configData, err := os.ReadFile("llm-config.yaml")
	if err != nil {
		t.Fatal(err)
	}
	configuration, err := workerconfig.Load(configData)
	if err != nil {
		t.Fatal(err)
	}
	manifestPath, err := filepath.Abs("resource-capacity.json")
	if err != nil {
		t.Fatal(err)
	}
	trustRootPath, err := filepath.Abs("release-trust-root.json")
	if err != nil {
		t.Fatal(err)
	}
	configuration.ResourceCapacity.ManifestFile = manifestPath
	configuration.ResourceCapacity.TrustRootFile = trustRootPath
	verified, err := workerconfig.VerifyResourceCapacity(configuration.ResourceCapacity)
	if err != nil {
		t.Fatal(err)
	}
	if verified.ManifestSHA256 != configuration.ResourceCapacity.ManifestSHA256 || verified.GenerationID != configuration.ResourceCapacity.GenerationID {
		t.Fatalf("verified resource capacity identity = %#v, want digest %q generation %q", verified, configuration.ResourceCapacity.ManifestSHA256, configuration.ResourceCapacity.GenerationID)
	}
}
