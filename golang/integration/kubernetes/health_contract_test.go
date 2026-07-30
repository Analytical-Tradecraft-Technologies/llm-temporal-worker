package kubernetes_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"go.yaml.in/yaml/v4"
)

const (
	liveHealthPath  = "/health/live"
	readyHealthPath = "/health/ready"
	healthPortName  = "health"
	healthPort      = 8080
)

type composeHealthDocument struct {
	Services map[string]struct {
		Healthcheck struct {
			Test []string `yaml:"test"`
		} `yaml:"healthcheck"`
	} `yaml:"services"`
}

type deploymentHealthManifest struct {
	Spec struct {
		Template struct {
			Spec struct {
				Containers []struct {
					Name  string `yaml:"name"`
					Ports []struct {
						Name          string `yaml:"name"`
						ContainerPort int    `yaml:"containerPort"`
					} `yaml:"ports"`
					LivenessProbe  healthProbe `yaml:"livenessProbe"`
					ReadinessProbe healthProbe `yaml:"readinessProbe"`
					StartupProbe   healthProbe `yaml:"startupProbe"`
				} `yaml:"containers"`
			} `yaml:"spec"`
		} `yaml:"template"`
	} `yaml:"spec"`
}

type healthProbe struct {
	HTTPGet struct {
		Path string `yaml:"path"`
		Port string `yaml:"port"`
	} `yaml:"httpGet"`
}

func TestComposeAndKubernetesShareWorkerHealthContract(t *testing.T) {
	compose := readHealthFixture(t, "compose.yaml")
	var composeDocument composeHealthDocument
	if err := yaml.NewDecoder(bytes.NewReader(compose)).Decode(&composeDocument); err != nil {
		t.Fatalf("decode Compose fixture: %v", err)
	}
	worker, ok := composeDocument.Services["worker"]
	if !ok {
		t.Fatal("Compose fixture is missing the worker service")
	}
	wantCommand := []string{
		"CMD",
		"/usr/local/bin/llm-temporal-worker",
		"healthcheck",
		"--url",
		"http://127.0.0.1:8080/health/live",
		"--url",
		"http://127.0.0.1:8080/health/ready",
	}
	if got := worker.Healthcheck.Test; len(got) != len(wantCommand) {
		t.Fatalf("Compose worker healthcheck arguments = %#v, want %#v", got, wantCommand)
	} else {
		for index := range wantCommand {
			if got[index] != wantCommand[index] {
				t.Fatalf("Compose worker healthcheck argument %d = %q, want %q (full command %#v)", index, got[index], wantCommand[index], got)
			}
		}
	}

	deploymentData := readHealthFixture(t, filepath.Join("deploy", "kubernetes", "base", "deployment.yaml"))
	var deployment deploymentHealthManifest
	if err := yaml.NewDecoder(bytes.NewReader(deploymentData)).Decode(&deployment); err != nil {
		t.Fatalf("decode Kubernetes deployment: %v", err)
	}
	if len(deployment.Spec.Template.Spec.Containers) != 1 {
		t.Fatalf("Kubernetes worker containers = %d, want exactly one", len(deployment.Spec.Template.Spec.Containers))
	}
	workerContainer := deployment.Spec.Template.Spec.Containers[0]
	if workerContainer.Name != "worker" {
		t.Fatalf("Kubernetes worker container name = %q, want worker", workerContainer.Name)
	}
	foundHealthPort := false
	for _, port := range workerContainer.Ports {
		if port.Name == healthPortName {
			foundHealthPort = true
			if port.ContainerPort != healthPort {
				t.Fatalf("Kubernetes health port = %d, want %d", port.ContainerPort, healthPort)
			}
		}
	}
	if !foundHealthPort {
		t.Fatalf("Kubernetes worker is missing named %q port", healthPortName)
	}
	assertHealthProbe(t, "liveness", workerContainer.LivenessProbe, liveHealthPath)
	assertHealthProbe(t, "readiness", workerContainer.ReadinessProbe, readyHealthPath)
	assertHealthProbe(t, "startup", workerContainer.StartupProbe, liveHealthPath)
}

func TestKubernetesOverlaysInheritTheBaseHealthContract(t *testing.T) {
	for _, overlay := range []string{
		"aws-workload-identity",
		"azure-workload-identity",
		"redis-tls",
	} {
		t.Run(overlay, func(t *testing.T) {
			data := readHealthFixture(t, filepath.Join("deploy", "kubernetes", "examples", overlay, "deployment-patch.yaml"))
			var document yaml.Node
			if err := yaml.NewDecoder(bytes.NewReader(data)).Decode(&document); err != nil {
				t.Fatalf("decode Kubernetes %s overlay: %v", overlay, err)
			}
			for _, key := range []string{"livenessProbe", "readinessProbe", "startupProbe"} {
				if yamlMappingContainsKey(&document, key) {
					t.Fatalf("overlay must inherit %s from the base deployment", key)
				}
			}
		})
	}
}

func yamlMappingContainsKey(node *yaml.Node, want string) bool {
	if node == nil {
		return false
	}
	if node.Kind == yaml.MappingNode {
		for index := 0; index+1 < len(node.Content); index += 2 {
			if node.Content[index].Value == want {
				return true
			}
		}
	}
	for _, child := range node.Content {
		if yamlMappingContainsKey(child, want) {
			return true
		}
	}
	return false
}

func assertHealthProbe(t *testing.T, name string, probe healthProbe, wantPath string) {
	t.Helper()
	if probe.HTTPGet.Path != wantPath {
		t.Fatalf("Kubernetes %s probe path = %q, want %q", name, probe.HTTPGet.Path, wantPath)
	}
	if probe.HTTPGet.Port != healthPortName {
		t.Fatalf("Kubernetes %s probe port = %q, want named port %q", name, probe.HTTPGet.Port, healthPortName)
	}
}

func readHealthFixture(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(moduleRoot(t), path))
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return data
}
