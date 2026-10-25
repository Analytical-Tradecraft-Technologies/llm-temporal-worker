package kubernetes_test

import (
	"bytes"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"go.yaml.in/yaml/v4"
)

type deploymentManifest struct {
	Kind string         `yaml:"kind"`
	Spec deploymentSpec `yaml:"spec"`
}

type deploymentSpec struct {
	Template podTemplate `yaml:"template"`
}

type podTemplate struct {
	Spec podSpec `yaml:"spec"`
}

type podSpec struct {
	SecurityContext podSecurityContext `yaml:"securityContext"`
	Containers      []podContainer     `yaml:"containers"`
	Volumes         []podVolume        `yaml:"volumes"`
}

type podSecurityContext struct {
	FSGroup      int  `yaml:"fsGroup"`
	RunAsGroup   int  `yaml:"runAsGroup"`
	RunAsUser    int  `yaml:"runAsUser"`
	RunAsNonRoot bool `yaml:"runAsNonRoot"`
}

type podContainer struct {
	Name         string        `yaml:"name"`
	VolumeMounts []volumeMount `yaml:"volumeMounts"`
}

type volumeMount struct {
	Name     string `yaml:"name"`
	ReadOnly bool   `yaml:"readOnly"`
}

type podVolume struct {
	Name      string           `yaml:"name"`
	Projected *projectedVolume `yaml:"projected"`
	Secret    *secretVolume    `yaml:"secret"`
}

type projectedVolume struct {
	DefaultMode int                     `yaml:"defaultMode"`
	Sources     []projectedVolumeSource `yaml:"sources"`
}

type projectedVolumeSource struct {
	Secret *projectedSecret `yaml:"secret"`
}

type projectedSecret struct {
	Name  string          `yaml:"name"`
	Items []projectedItem `yaml:"items"`
}

type projectedItem struct {
	Key  string `yaml:"key"`
	Path string `yaml:"path"`
}

type secretVolume struct {
	DefaultMode int `yaml:"defaultMode"`
}

func TestBaseProjectedSecretVolumeIsReadableByWorkerGroup(t *testing.T) {
	deployment := readDeploymentManifest(t, "deploy", "kubernetes", "base", "deployment.yaml")
	assertWorkerGroupAccess(t, deployment)

	runtimeSecrets := findVolume(t, deployment, "runtime-secrets")
	if runtimeSecrets.Secret != nil {
		t.Error("base runtime-secrets volume must not combine a Secret and projected source")
	}
	if runtimeSecrets.Projected == nil {
		t.Fatal("base runtime-secrets volume must use a projected source")
	}
	if got, want := runtimeSecrets.Projected.DefaultMode, 0o440; got != want {
		t.Errorf("base runtime-secrets defaultMode = %#o, want %#o", got, want)
	}
	assertProjectedSecretNames(t, runtimeSecrets.Projected, []string{"llmtw-worker-secrets"})
}

func TestRedisTLSOverlayExtendsProjectedRuntimeSecrets(t *testing.T) {
	patch := readDeploymentManifest(t, "deploy", "kubernetes", "examples", "redis-tls", "deployment-patch.yaml")
	runtimeSecrets := findVolume(t, patch, "runtime-secrets")
	if runtimeSecrets.Secret != nil {
		t.Error("redis TLS runtime-secrets patch must not add a second volume source type")
	}
	if runtimeSecrets.Projected == nil {
		t.Fatal("redis TLS runtime-secrets patch must use a projected source")
	}
	if got, want := runtimeSecrets.Projected.DefaultMode, 0o440; got != want {
		t.Errorf("redis TLS projected runtime-secrets defaultMode = %#o, want %#o", got, want)
	}
	assertProjectedSecretNames(t, runtimeSecrets.Projected, []string{"llmtw-worker-secrets", "llmtw-redis-ca"})
}

func TestRenderedSecretVolumesAreReadableByWorkerGroup(t *testing.T) {
	kubectl := os.Getenv("KUBECTL")
	if kubectl == "" {
		t.Skip("set KUBECTL to a reviewed kubectl executable to verify rendered Kubernetes policy")
	}

	for _, test := range []struct {
		name        string
		path        []string
		secretNames []string
	}{
		{name: "base", path: []string{"base"}, secretNames: []string{"llmtw-worker-secrets"}},
		{name: "redis TLS", path: []string{"examples", "redis-tls"}, secretNames: []string{"llmtw-worker-secrets", "llmtw-redis-ca"}},
		{name: "private state egress", path: []string{"examples", "private-state-egress"}, secretNames: []string{"llmtw-worker-secrets"}},
		{name: "AWS workload identity", path: []string{"examples", "aws-workload-identity"}, secretNames: []string{"llmtw-worker-secrets"}},
		{name: "Azure workload identity", path: []string{"examples", "azure-workload-identity"}, secretNames: []string{"llmtw-worker-secrets"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			deployment := renderDeployment(t, kubectl, test.path...)
			assertWorkerGroupAccess(t, deployment)

			runtimeSecrets := findVolume(t, deployment, "runtime-secrets")
			if runtimeSecrets.Secret != nil {
				t.Error("rendered runtime-secrets volume must not combine Secret and projected sources")
			}
			if runtimeSecrets.Projected == nil {
				t.Fatal("rendered runtime-secrets volume must use a projected source")
			}
			if got, want := runtimeSecrets.Projected.DefaultMode, 0o440; got != want {
				t.Errorf("rendered projected runtime-secrets defaultMode = %#o, want %#o", got, want)
			}
			assertProjectedSecretNames(t, runtimeSecrets.Projected, test.secretNames)
		})
	}
}

func readDeploymentManifest(t *testing.T, path ...string) deploymentManifest {
	t.Helper()

	var deployment deploymentManifest
	if err := yaml.Unmarshal([]byte(readRepositoryFile(t, path...)), &deployment); err != nil {
		t.Fatalf("decode %s: %v", filepath.Join(path...), err)
	}
	return deployment
}

func renderDeployment(t *testing.T, kubectl string, path ...string) deploymentManifest {
	t.Helper()

	overlay := filepath.Join(append([]string{moduleRoot(t), "deploy", "kubernetes"}, path...)...)
	output, err := exec.Command(kubectl, "kustomize", overlay).CombinedOutput()
	if err != nil {
		t.Fatalf("render %s: %v\n%s", filepath.Join(path...), err, output)
	}

	decoder := yaml.NewDecoder(bytes.NewReader(output))
	for {
		var document deploymentManifest
		err := decoder.Decode(&document)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("decode rendered %s: %v", filepath.Join(path...), err)
		}
		if document.Kind == "Deployment" {
			return document
		}
	}

	t.Fatalf("rendered %s does not contain a Deployment", filepath.Join(path...))
	return deploymentManifest{}
}

func assertWorkerGroupAccess(t *testing.T, deployment deploymentManifest) {
	t.Helper()

	context := deployment.Spec.Template.Spec.SecurityContext
	if !context.RunAsNonRoot {
		t.Error("worker pod must enforce non-root execution")
	}
	for name, got := range map[string]int{
		"fsGroup":    context.FSGroup,
		"runAsGroup": context.RunAsGroup,
		"runAsUser":  context.RunAsUser,
	} {
		if want := 65532; got != want {
			t.Errorf("worker pod %s = %d, want %d", name, got, want)
		}
	}

	worker := findWorkerContainer(t, deployment)
	for _, mount := range worker.VolumeMounts {
		if mount.Name == "runtime-secrets" && mount.ReadOnly {
			return
		}
	}
	t.Error("worker must mount runtime-secrets read-only")
}

func findWorkerContainer(t *testing.T, deployment deploymentManifest) podContainer {
	t.Helper()

	for _, container := range deployment.Spec.Template.Spec.Containers {
		if container.Name == "worker" {
			return container
		}
	}
	t.Fatal("deployment is missing the worker container")
	return podContainer{}
}

func findVolume(t *testing.T, deployment deploymentManifest, name string) podVolume {
	t.Helper()

	for _, volume := range deployment.Spec.Template.Spec.Volumes {
		if volume.Name == name {
			return volume
		}
	}
	t.Fatalf("deployment is missing volume %q", name)
	return podVolume{}
}

func assertProjectedSecretNames(t *testing.T, projected *projectedVolume, want []string) {
	t.Helper()
	if projected == nil {
		t.Fatal("projected volume is nil")
	}

	got := make([]string, 0, len(projected.Sources))
	for _, source := range projected.Sources {
		if source.Secret == nil {
			t.Error("runtime-secrets projected volume must contain only Secret sources")
			continue
		}
		got = append(got, source.Secret.Name)
	}
	if !slices.Equal(got, want) {
		t.Errorf("runtime-secrets projected Secret names = %q, want %q", got, want)
	}
}

// Every runtime secret file the base configuration reads is projected exactly
// once, in the base and in the Redis TLS overlay that moves the CA to its own
// Secret, so no Secret key goes undocumented and two sources never project
// onto one path, which the kubelet rejects (#1683).
func TestProjectedSecretsProvideEveryConfiguredFileOnce(t *testing.T) {
	config, err := os.ReadFile(filepath.Join(moduleRoot(t), "deploy", "kubernetes", "base", "config.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	const mount = "/var/run/secrets/llmtw/"
	wanted := map[string]bool{}
	for _, field := range strings.Fields(string(config)) {
		if strings.HasPrefix(field, mount) {
			wanted[strings.TrimPrefix(field, mount)] = true
		}
	}
	if len(wanted) < 9 {
		t.Fatalf("base config reads %d runtime secret files, want at least 9", len(wanted))
	}
	readme, err := os.ReadFile(filepath.Join(moduleRoot(t), "deploy", "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	for _, manifest := range [][]string{
		{"deploy", "kubernetes", "base", "deployment.yaml"},
		{"deploy", "kubernetes", "examples", "redis-tls", "deployment-patch.yaml"},
	} {
		projected := findVolume(t, readDeploymentManifest(t, manifest...), "runtime-secrets").Projected
		if projected == nil {
			t.Fatalf("%s runtime-secrets is not projected", filepath.Join(manifest...))
		}
		paths := map[string]string{}
		for _, source := range projected.Sources {
			if source.Secret == nil {
				continue
			}
			if len(source.Secret.Items) == 0 {
				t.Fatalf("%s projects every key of %s; list its items so paths cannot collide", filepath.Join(manifest...), source.Secret.Name)
			}
			for _, item := range source.Secret.Items {
				if owner, taken := paths[item.Path]; taken {
					t.Fatalf("%s projects %s from both %s and %s", filepath.Join(manifest...), item.Path, owner, source.Secret.Name)
				}
				paths[item.Path] = source.Secret.Name
				if !strings.Contains(string(readme), "`"+item.Key+"`") {
					t.Errorf("deploy/README.md does not document %s key %s", source.Secret.Name, item.Key)
				}
			}
		}
		for file := range wanted {
			if _, ok := paths[file]; !ok {
				t.Errorf("%s does not project %s, which the base config reads", filepath.Join(manifest...), file)
			}
		}
	}
}
