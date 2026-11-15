package config_test

import (
	"fmt"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"os"
	"strings"
	"testing"
)

func withoutTemporalControllerEnvironment(t *testing.T) {
	t.Helper()
	for _, key := range []string{"TEMPORAL_ADDRESS", "TEMPORAL_NAMESPACE", "TEMPORAL_DEPLOYMENT_NAME", "TEMPORAL_WORKER_BUILD_ID"} {
		value, present := os.LookupEnv(key)
		testassert.NoError(t, os.Unsetenv(key))
		t.Cleanup(func() {
			if present {
				_ = os.Setenv(key, value)
			} else {
				_ = os.Unsetenv(key)
			}
		})
	}
}
func TestTemporalVersioningOptionalAndControllerOverrides(t *testing.T) {
	withoutTemporalControllerEnvironment(t)
	loaded := mustLoadConfig(t, exampleYAML(t))
	if loaded.Temporal.Worker.Versioning.Enabled {
		t.Fatal("versioning must default off")
	}
	data := replaceExample(t, "  worker:\n", "  worker:\n    versioning: {enabled: true, deployment_name: yaml-worker, build_id: yaml-build}\n")
	loaded = mustLoadConfig(t, data)
	if loaded.Temporal.Worker.Versioning.BuildID != "yaml-build" {
		t.Fatal("YAML identity lost")
	}
	t.Setenv("TEMPORAL_ADDRESS", "controller.example.internal:7233")
	t.Setenv("TEMPORAL_NAMESPACE", "controller-namespace")
	t.Setenv("TEMPORAL_DEPLOYMENT_NAME", "controller-worker")
	t.Setenv("TEMPORAL_WORKER_BUILD_ID", "controller-build")
	loaded = mustLoadConfig(t, data)
	if loaded.Temporal.Target != "controller.example.internal:7233" || loaded.Temporal.Namespace != "controller-namespace" || loaded.Temporal.Worker.Versioning != (config.WorkerVersioningConfig{Enabled: true, DeploymentName: "controller-worker", BuildID: "controller-build"}) {
		t.Fatalf("controller identity not effective: %+v", loaded.Temporal)
	}
}
func TestTemporalVersioningRejectsPartialOrEmptyControllerIdentity(t *testing.T) {
	for _, test := range []struct {
		name string
		env  map[string]string
	}{
		{"name only", map[string]string{"TEMPORAL_DEPLOYMENT_NAME": "worker"}},
		{"build only", map[string]string{"TEMPORAL_WORKER_BUILD_ID": "build"}},
		{"empty name", map[string]string{"TEMPORAL_DEPLOYMENT_NAME": "", "TEMPORAL_WORKER_BUILD_ID": "build"}},
		{"empty build", map[string]string{"TEMPORAL_DEPLOYMENT_NAME": "worker", "TEMPORAL_WORKER_BUILD_ID": ""}},
		{"empty address", map[string]string{"TEMPORAL_ADDRESS": ""}},
		{"empty namespace", map[string]string{"TEMPORAL_NAMESPACE": ""}},
	} {
		t.Run(test.name, func(t *testing.T) {
			withoutTemporalControllerEnvironment(t)
			for key, value := range test.env {
				t.Setenv(key, value)
			}
			requireLoadRejected(t, exampleYAML(t), "invalid controller configuration accepted")
		})
	}
}
func TestTemporalVersioningRejectsInvalidYAMLIdentity(t *testing.T) {
	withoutTemporalControllerEnvironment(t)
	for _, setting := range []string{"{enabled: true}", "{enabled: true, deployment_name: worker}", "{enabled: true, deployment_name: worker, build_id: 'bad build'}", "{enabled: false, deployment_name: worker, build_id: build}"} {
		requireLoadRejected(t, replaceExample(t, "  worker:\n", "  worker:\n    versioning: "+setting+"\n"), "invalid YAML identity accepted: %s", setting)
	}
}
func TestTemporalVersioningRejectsIdentitiesTemporalRejects(t *testing.T) {
	long := strings.Repeat("a", config.WorkerVersioningIdentityMaxBytes+1)
	for _, test := range []struct {
		name  string
		value config.WorkerVersioningConfig
		field string
		echo  string
	}{
		{"dotted deployment", config.WorkerVersioningConfig{Enabled: true, DeploymentName: "llm.worker", BuildID: "b1"}, "deployment_name", "llm.worker"},
		{"colon deployment", config.WorkerVersioningConfig{Enabled: true, DeploymentName: "llmtw:prod", BuildID: "b1"}, "deployment_name", "llmtw:prod"},
		{"reserved deployment", config.WorkerVersioningConfig{Enabled: true, DeploymentName: "__internal", BuildID: "b1"}, "deployment_name", "__internal"},
		{"reserved build", config.WorkerVersioningConfig{Enabled: true, DeploymentName: "worker", BuildID: "__b"}, "build_id", "__b"},
		{"long deployment", config.WorkerVersioningConfig{Enabled: true, DeploymentName: long, BuildID: "b1"}, "deployment_name", long},
		{"long build", config.WorkerVersioningConfig{Enabled: true, DeploymentName: "worker", BuildID: long}, "build_id", long},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := test.value.Validate()
			testassert.ErrorContains(t, err, "temporal.worker.versioning."+test.field, fmt.Sprintf("error does not name %s", test.field))
			if strings.Contains(err.Error(), test.echo) {
				t.Fatalf("error echoes the configured value: %v", err)
			}
		})
	}
	limit := strings.Repeat("a", config.WorkerVersioningIdentityMaxBytes)
	for _, valid := range []config.WorkerVersioningConfig{
		{Enabled: true, DeploymentName: "llm-temporal-worker_prod", BuildID: "v1.2.3+abc:def"},
		{Enabled: true, DeploymentName: limit, BuildID: limit},
		{Enabled: true, DeploymentName: "_worker", BuildID: "_b"},
	} {
		testassert.NoError(t, valid.Validate(), "valid identity rejected")
	}
}
func TestTemporalVersioningSchemaMatchesTemporalRules(t *testing.T) {
	withoutTemporalControllerEnvironment(t)
	compiled := configSchema(t)
	loaded := mustLoadConfig(t, exampleYAML(t))
	long := strings.Repeat("a", config.WorkerVersioningIdentityMaxBytes+1)
	for name, test := range map[string]struct {
		deployment, build string
		valid             bool
	}{
		"valid":               {"worker", "v1.2.3", true},
		"dotted deployment":   {"llm.worker", "b1", false},
		"colon deployment":    {"llmtw:prod", "b1", false},
		"reserved deployment": {"__internal", "b1", false},
		"reserved build":      {"worker", "__b", false},
		"long deployment":     {long, "b1", false},
		"long build":          {"worker", long, false},
		"long utf8 build":     {"worker", strings.Repeat("é", 128), false},
		"short utf8 build":    {"worker", strings.Repeat("é", 63), true},
	} {
		value := loaded.Clone()
		value.Temporal.Worker.Versioning = config.WorkerVersioningConfig{Enabled: true, DeploymentName: test.deployment, BuildID: test.build}
		encoded := testassert.MarshalJSON(t, value)
		if err := compiled.Validate(encoded); (err == nil) != test.valid {
			t.Errorf("%s: schema validation = %v, want valid=%t", name, err, test.valid)
		}
		if err := value.Temporal.Worker.Versioning.Validate(); (err == nil) != test.valid {
			t.Errorf("%s: Validate = %v, want valid=%t", name, err, test.valid)
		}
	}
}
