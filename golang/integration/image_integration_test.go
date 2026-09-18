//go:build imageintegration

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/internal/buildinfo"
)

const (
	imageVerifyUser       = "65532:65532"
	imageVerifyTmpfs      = "/tmp:rw,nosuid,nodev,noexec,size=64m"
	imageGoVersionLabel   = "io.github.mfow.llm-temporal-worker.go.version"
	imageHealthPort       = "8080/tcp"
	imageVerificationWait = 15 * time.Second
)

type imageInspection struct {
	Architecture string `json:"Architecture"`
	OS           string `json:"Os"`
	Config       struct {
		User       string            `json:"User"`
		Labels     map[string]string `json:"Labels"`
		Env        []string          `json:"Env"`
		Entrypoint []string          `json:"Entrypoint"`
		Cmd        []string          `json:"Cmd"`
	} `json:"Config"`
}

type containerInspection struct {
	Config struct {
		User string `json:"User"`
	} `json:"Config"`
	HostConfig struct {
		ReadonlyRootfs bool              `json:"ReadonlyRootfs"`
		Tmpfs          map[string]string `json:"Tmpfs"`
	} `json:"HostConfig"`
	NetworkSettings struct {
		Ports map[string][]struct {
			HostPort string `json:"HostPort"`
		} `json:"Ports"`
	} `json:"NetworkSettings"`
}

func TestHardenedImageRuntimeAndMetadata(t *testing.T) {
	image := os.Getenv("LLMTW_IMAGE")
	if image == "" {
		t.Skip("make image-verify supplies a locally built image")
	}
	expected := imageMetadataFromEnvironment(t)

	inspected := inspectImage(t, image)
	if inspected.Config.User != imageVerifyUser {
		t.Fatalf("image user = %q, want numeric non-root %q", inspected.Config.User, imageVerifyUser)
	}
	if inspected.Architecture != "amd64" || inspected.OS != "linux" {
		t.Fatalf("image platform = %s/%s, want linux/amd64", inspected.OS, inspected.Architecture)
	}
	if len(inspected.Config.Entrypoint) != 1 || inspected.Config.Entrypoint[0] != "/usr/local/bin/llm-temporal-worker" {
		t.Fatalf("image entrypoint = %#v, want native worker binary", inspected.Config.Entrypoint)
	}
	if len(inspected.Config.Cmd) != 0 {
		t.Fatalf("image command = %#v, want deployment-supplied production command", inspected.Config.Cmd)
	}
	assertNoSecretEnvironment(t, inspected.Config.Env)
	assertRuntimeToolsAbsent(t, image)
	assertImageLabels(t, inspected.Config.Labels, expected)
	assertImageEnvironment(t, inspected.Config.Env, expected)
	if got := imageVersion(t, image); got != expected {
		t.Fatalf("binary metadata = %#v, want %#v", got, expected)
	}

	container := startHardenedImage(t, image)
	inspectedContainer := inspectContainer(t, container)
	if inspectedContainer.Config.User != imageVerifyUser {
		t.Fatalf("container user = %q, want %q", inspectedContainer.Config.User, imageVerifyUser)
	}
	if !inspectedContainer.HostConfig.ReadonlyRootfs {
		t.Fatal("container root filesystem is writable")
	}
	if err := validateHardenedTmpfsOptions(inspectedContainer.HostConfig.Tmpfs); err != nil {
		t.Fatalf("container writable mounts = %#v: %v", inspectedContainer.HostConfig.Tmpfs, err)
	}
	bindings := inspectedContainer.NetworkSettings.Ports[imageHealthPort]
	if len(bindings) != 1 || bindings[0].HostPort == "" {
		t.Fatalf("health port bindings = %#v, want one published localhost port", bindings)
	}
	address := net.JoinHostPort("127.0.0.1", bindings[0].HostPort)
	waitForImageStatus(t, address, "/health/live", http.StatusOK)
	waitForImageStatus(t, address, "/health/ready", http.StatusServiceUnavailable)
}

func imageMetadataFromEnvironment(t *testing.T) buildinfo.Metadata {
	t.Helper()
	metadata := buildinfo.Metadata{
		Version:   requiredImageEnvironment(t, "LLMTW_IMAGE_VERSION"),
		Revision:  requiredImageEnvironment(t, "LLMTW_IMAGE_REVISION"),
		BuildTime: requiredImageEnvironment(t, "LLMTW_IMAGE_BUILD_TIME"),
		GoVersion: requiredImageEnvironment(t, "LLMTW_IMAGE_GO_VERSION"),
		Source:    requiredImageEnvironment(t, "LLMTW_IMAGE_SOURCE"),
	}
	output := runImageCommand(t, "git", "rev-parse", "HEAD")
	if got := strings.TrimSpace(output); metadata.Revision != got {
		t.Fatalf("requested image revision = %q, checked-out revision = %q", metadata.Revision, got)
	}
	return metadata
}

func requiredImageEnvironment(t *testing.T, name string) string {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		t.Fatalf("%s is required for image verification", name)
	}
	return value
}

func assertImageLabels(t *testing.T, labels map[string]string, metadata buildinfo.Metadata) {
	t.Helper()
	for name, want := range map[string]string{
		"org.opencontainers.image.version":  metadata.Version,
		"org.opencontainers.image.revision": metadata.Revision,
		"org.opencontainers.image.created":  metadata.BuildTime,
		"org.opencontainers.image.source":   metadata.Source,
		imageGoVersionLabel:                 metadata.GoVersion,
	} {
		if got := labels[name]; got != want {
			t.Errorf("image label %s = %q, want %q", name, got, want)
		}
	}
}

func assertImageEnvironment(t *testing.T, environment []string, metadata buildinfo.Metadata) {
	t.Helper()
	values := make(map[string]string, len(environment))
	for _, entry := range environment {
		name, value, ok := strings.Cut(entry, "=")
		if ok {
			values[name] = value
		}
	}
	for name, want := range map[string]string{
		"LLMTW_BUILD_VERSION":    metadata.Version,
		"LLMTW_BUILD_GIT_SHA":    metadata.Revision,
		"LLMTW_BUILD_TIMESTAMP":  metadata.BuildTime,
		"LLMTW_BUILD_SOURCE":     metadata.Source,
		"LLMTW_BUILD_GO_VERSION": metadata.GoVersion,
	} {
		if got := values[name]; got != want {
			t.Errorf("image environment %s = %q, want %q", name, got, want)
		}
	}
}

func assertNoSecretEnvironment(t *testing.T, environment []string) {
	t.Helper()
	for _, entry := range environment {
		name, _, _ := strings.Cut(entry, "=")
		upper := strings.ToUpper(name)
		for _, forbidden := range []string{"SECRET", "TOKEN", "PASSWORD", "CREDENTIAL", "API_KEY", "MODEL", "PROMPT"} {
			if strings.Contains(upper, forbidden) {
				t.Errorf("final image contains forbidden secret/provider environment name %q", name)
			}
		}
	}
}

func assertRuntimeToolsAbsent(t *testing.T, image string) {
	t.Helper()
	for _, tool := range []string{"/bin/sh", "/bin/bash", "/bin/busybox", "/usr/bin/apt-get", "/usr/bin/apk", "/usr/local/go/bin/go"} {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		output, err := exec.CommandContext(ctx, "docker", "run", "--rm", "--entrypoint", tool, image, "--version").CombinedOutput()
		cancel()
		if err == nil {
			t.Errorf("final image unexpectedly executed %s (%s)", tool, strings.TrimSpace(string(output)))
		}
	}
}

func imageVersion(t *testing.T, image string) buildinfo.Metadata {
	t.Helper()
	output := runImageDocker(t,
		"run", "--rm", "--read-only", "--tmpfs", imageVerifyTmpfs, "--user", imageVerifyUser,
		image, "version",
	)
	var metadata buildinfo.Metadata
	if err := json.Unmarshal([]byte(output), &metadata); err != nil {
		t.Fatalf("image version output is not metadata JSON: %v (%q)", err, output)
	}
	return metadata
}

func startHardenedImage(t *testing.T, image string) string {
	t.Helper()
	name := fmt.Sprintf("llmtw-image-verify-%d", time.Now().UnixNano())
	_ = runImageDocker(t,
		"run", "--detach", "--rm", "--name", name,
		"--read-only", "--tmpfs", imageVerifyTmpfs, "--user", imageVerifyUser,
		"--publish", "127.0.0.1::8080",
		image, "health-server", "--address", "0.0.0.0:8080",
	)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		output, err := exec.CommandContext(ctx, "docker", "rm", "--force", name).CombinedOutput()
		if err != nil && !strings.Contains(string(output), "No such container") {
			t.Logf("remove image verification container %s: %v (%s)", name, err, strings.TrimSpace(string(output)))
		}
	})
	return name
}

func inspectImage(t *testing.T, image string) imageInspection {
	t.Helper()
	var inspected []imageInspection
	if err := json.Unmarshal([]byte(runImageDocker(t, "image", "inspect", image)), &inspected); err != nil {
		t.Fatal(err)
	}
	if len(inspected) != 1 {
		t.Fatalf("image inspection count = %d, want 1", len(inspected))
	}
	return inspected[0]
}

func inspectContainer(t *testing.T, name string) containerInspection {
	t.Helper()
	var inspected []containerInspection
	if err := json.Unmarshal([]byte(runImageDocker(t, "inspect", name)), &inspected); err != nil {
		t.Fatal(err)
	}
	if len(inspected) != 1 {
		t.Fatalf("container inspection count = %d, want 1", len(inspected))
	}
	return inspected[0]
}

func waitForImageStatus(t *testing.T, address, path string, want int) {
	t.Helper()
	deadline := time.Now().Add(imageVerificationWait)
	var last error
	for time.Now().Before(deadline) {
		request, err := http.NewRequest(http.MethodGet, "http://"+address+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := (&http.Client{Timeout: time.Second}).Do(request)
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == want {
				return
			}
			last = fmt.Errorf("status %d", response.StatusCode)
		} else {
			last = err
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("%s did not reach HTTP %d: %v", path, want, last)
}

func runImageDocker(t *testing.T, arguments ...string) string {
	t.Helper()
	return runImageCommand(t, "docker", arguments...)
}

func runImageCommand(t *testing.T, name string, arguments ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, name, arguments...)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		t.Fatalf("%s %s: %v (stdout=%q stderr=%q)", name, strings.Join(arguments, " "), err, boundedImageCommandDiagnostic(stdout.String()), boundedImageCommandDiagnostic(stderr.String()))
	}
	return stdout.String()
}

func boundedImageCommandDiagnostic(value string) string {
	const limit = 4096
	value = strings.TrimSpace(value)
	if len(value) <= limit {
		return value
	}
	return value[:limit] + "...[truncated]"
}
