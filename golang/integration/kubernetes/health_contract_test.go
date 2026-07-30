package kubernetes_test

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"go.yaml.in/yaml/v4"
)

const (
	liveHealthPath     = "/health/live"
	readyHealthPath    = "/health/ready"
	healthPortName     = "health"
	expectedHealthPort = 8080
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

type serverConfigDocument struct {
	Server struct {
		HealthAddress string `yaml:"health_address"`
	} `yaml:"server"`
}

func TestComposeAndKubernetesShareWorkerHealthContract(t *testing.T) {
	healthPort := configuredHealthPort(t)
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
		fmt.Sprintf("http://127.0.0.1:%d%s", healthPort, liveHealthPath),
		"--url",
		fmt.Sprintf("http://127.0.0.1:%d%s", healthPort, readyHealthPath),
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
	examplesDirectory := filepath.Join(moduleRoot(t), "deploy", "kubernetes", "examples")
	entries, err := os.ReadDir(examplesDirectory)
	if err != nil {
		t.Fatalf("read Kubernetes examples directory: %v", err)
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		overlay := entry.Name()
		t.Run(overlay, func(t *testing.T) {
			overlayDirectory := filepath.Join(examplesDirectory, overlay)
			err := filepath.WalkDir(overlayDirectory, func(path string, directoryEntry os.DirEntry, walkErr error) error {
				if walkErr != nil {
					return walkErr
				}
				if directoryEntry.IsDir() || (filepath.Ext(path) != ".yaml" && filepath.Ext(path) != ".yml") {
					return nil
				}
				data, err := os.ReadFile(path)
				if err != nil {
					return err
				}
				var document yaml.Node
				if err := yaml.NewDecoder(bytes.NewReader(data)).Decode(&document); err != nil {
					return fmt.Errorf("decode %s: %w", filepath.Base(path), err)
				}
				for _, key := range []string{"livenessProbe", "readinessProbe", "startupProbe"} {
					if yamlMappingContainsKey(&document, key) {
						return fmt.Errorf("overlay must inherit %s from the base deployment (found in %s)", key, filepath.Base(path))
					}
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func configuredHealthPort(t *testing.T) int {
	t.Helper()
	ports := make([]int, 0, 2)
	for _, path := range []string{
		filepath.Join("deploy", "local", "config.yaml"),
		filepath.Join("deploy", "kubernetes", "base", "config.yaml"),
	} {
		data := readHealthFixture(t, path)
		var document serverConfigDocument
		if err := yaml.NewDecoder(bytes.NewReader(data)).Decode(&document); err != nil {
			t.Fatalf("decode %s: %v", path, err)
		}
		_, portText, err := net.SplitHostPort(document.Server.HealthAddress)
		if err != nil {
			t.Fatalf("parse %s server.health_address %q: %v", path, document.Server.HealthAddress, err)
		}
		port, err := strconv.Atoi(portText)
		if err != nil || port < 1 || port > 65535 {
			t.Fatalf("parse %s server.health_address port %q: %v", path, portText, err)
		}
		ports = append(ports, port)
	}
	if ports[0] != ports[1] {
		t.Fatalf("configured health listener ports differ: local=%d Kubernetes=%d", ports[0], ports[1])
	}
	if ports[0] != expectedHealthPort {
		t.Fatalf("configured health listener port = %d, want %d", ports[0], expectedHealthPort)
	}
	return ports[0]
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
