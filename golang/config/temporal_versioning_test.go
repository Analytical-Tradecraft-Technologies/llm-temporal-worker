package config_test

import (
	"github.com/mfow/llm-temporal-worker/golang/config"
	"os"
	"strings"
	"testing"
)

func withoutTemporalControllerEnvironment(t *testing.T) {
	t.Helper()
	for _, key := range []string{"TEMPORAL_ADDRESS", "TEMPORAL_NAMESPACE", "TEMPORAL_DEPLOYMENT_NAME", "TEMPORAL_WORKER_BUILD_ID"} {
		value, present := os.LookupEnv(key)
		if err := os.Unsetenv(key); err != nil {
			t.Fatal(err)
		}
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
	loaded, err := config.Load(exampleYAML(t))
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Temporal.Worker.Versioning.Enabled {
		t.Fatal("versioning must default off")
	}
	data := strings.Replace(string(exampleYAML(t)), "  worker:\n", "  worker:\n    versioning: {enabled: true, deployment_name: yaml-worker, build_id: yaml-build}\n", 1)
	loaded, err = config.Load([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Temporal.Worker.Versioning.BuildID != "yaml-build" {
		t.Fatal("YAML identity lost")
	}
	t.Setenv("TEMPORAL_ADDRESS", "controller.example.internal:7233")
	t.Setenv("TEMPORAL_NAMESPACE", "controller-namespace")
	t.Setenv("TEMPORAL_DEPLOYMENT_NAME", "controller-worker")
	t.Setenv("TEMPORAL_WORKER_BUILD_ID", "controller-build")
	loaded, err = config.Load([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
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
			if _, err := config.Load(exampleYAML(t)); err == nil {
				t.Fatal("invalid controller configuration accepted")
			}
		})
	}
}
func TestTemporalVersioningRejectsInvalidYAMLIdentity(t *testing.T) {
	withoutTemporalControllerEnvironment(t)
	for _, setting := range []string{"{enabled: true}", "{enabled: true, deployment_name: worker}", "{enabled: true, deployment_name: worker, build_id: 'bad build'}", "{enabled: false, deployment_name: worker, build_id: build}"} {
		data := strings.Replace(string(exampleYAML(t)), "  worker:\n", "  worker:\n    versioning: "+setting+"\n", 1)
		if _, err := config.Load([]byte(data)); err == nil {
			t.Fatalf("invalid YAML identity accepted: %s", setting)
		}
	}
}
