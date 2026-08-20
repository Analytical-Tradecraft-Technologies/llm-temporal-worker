package architecturetest

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const reviewedGoPatch = "1.26.7"

func TestDockerfileStampsEveryMetadataFieldIntoImageAndBinary(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(moduleRoot(t), "Dockerfile"))
	if err != nil {
		t.Fatal(err)
	}
	dockerfile := string(data)
	if strings.Contains(dockerfile, "@sha256:") {
		t.Fatal("Dockerfile base images must use mutable tags rather than pinned digests")
	}

	for _, want := range []string{
		"ARG VERSION=dev",
		"ARG REVISION=unknown",
		"ARG BUILD_TIME=unknown",
		"ARG SOURCE=https://github.com/mfow/llm-temporal-worker",
		"ARG GO_VERSION=unknown",
		"org.opencontainers.image.version=\"${VERSION}\"",
		"org.opencontainers.image.revision=\"${REVISION}\"",
		"org.opencontainers.image.created=\"${BUILD_TIME}\"",
		"org.opencontainers.image.source=\"${SOURCE}\"",
		"io.github.mfow.llm-temporal-worker.go.version=\"${GO_VERSION}\"",
		"ENV LLMTW_BUILD_VERSION=\"${VERSION}\"",
		"LLMTW_BUILD_GIT_SHA=\"${REVISION}\"",
		"LLMTW_BUILD_TIMESTAMP=\"${BUILD_TIME}\"",
		"LLMTW_BUILD_SOURCE=\"${SOURCE}\"",
		"LLMTW_BUILD_GO_VERSION=\"${GO_VERSION}\"",
		"-X github.com/mfow/llm-temporal-worker/golang/internal/buildinfo.Version=${VERSION}",
		"-X github.com/mfow/llm-temporal-worker/golang/internal/buildinfo.Revision=${REVISION}",
		"-X github.com/mfow/llm-temporal-worker/golang/internal/buildinfo.BuildTime=${BUILD_TIME}",
		"-X github.com/mfow/llm-temporal-worker/golang/internal/buildinfo.Source=${SOURCE}",
		"-X github.com/mfow/llm-temporal-worker/golang/internal/buildinfo.GoVersion=${go_version}",
	} {
		if !strings.Contains(dockerfile, want) {
			t.Errorf("Dockerfile does not stamp %q", want)
		}
	}
}

func TestImageBuildToolchainVersionPolicyUsesReviewedPatchTag(t *testing.T) {
	dockerfileData, err := os.ReadFile(filepath.Join(moduleRoot(t), "Dockerfile"))
	if err != nil {
		t.Fatal(err)
	}

	for _, want := range []string{
		"ARG GO_IMAGE=docker.io/library/golang:" + reviewedGoPatch,
		"FROM gcr.io/distroless/static-debian12:nonroot",
	} {
		if !strings.Contains(string(dockerfileData), want) {
			t.Errorf("Dockerfile toolchain policy is missing %q", want)
		}
	}

	makefileData, err := os.ReadFile(filepath.Join(moduleRoot(t), "Makefile"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(makefileData), "IMAGE_VERIFY_GO_VERSION ?= $(shell $(GO) env GOVERSION)") {
		t.Error("Makefile image verification must use the installed reviewed Go toolchain")
	}
}

func TestReviewedGoToolchainPinsStayAligned(t *testing.T) {
	repository := repositoryRoot(t)
	module := moduleRoot(t)

	versionData, err := os.ReadFile(filepath.Join(module, ".go-version"))
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(string(versionData)); got != reviewedGoPatch {
		t.Fatalf("golang/.go-version = %q, want reviewed patch %q", got, reviewedGoPatch)
	}

	for _, contract := range []struct {
		name string
		path string
		want string
	}{
		{
			name: "container builder",
			path: filepath.Join(module, "Dockerfile"),
			want: "ARG GO_IMAGE=docker.io/library/golang:" + reviewedGoPatch,
		},
		{
			name: "security verifier",
			path: filepath.Join(module, "Makefile"),
			want: "SECURITY_GO_TOOLCHAIN ?= go" + reviewedGoPatch,
		},
		{
			name: "dependency baseline current patch",
			path: filepath.Join(repository, "docs", "reference", "dependency-baseline.md"),
			want: "`go" + reviewedGoPatch + "`",
		},
		{
			name: "dependency baseline local hint",
			path: filepath.Join(repository, "docs", "reference", "dependency-baseline.md"),
			want: "`.go-version` = `" + reviewedGoPatch + "`",
		},
		{
			name: "security architecture",
			path: filepath.Join(repository, "docs", "architecture", "security-and-privacy.md"),
			want: reviewedGoPatch + " toolchain",
		},
	} {
		data, err := os.ReadFile(contract.path)
		if err != nil {
			t.Fatalf("read %s: %v", contract.name, err)
		}
		if !strings.Contains(string(data), contract.want) {
			t.Errorf("%s is not aligned with golang/.go-version %s", contract.name, reviewedGoPatch)
		}
	}
}

func TestImageVerifyTargetUsesHardenedRuntimeContract(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(moduleRoot(t), "Makefile"))
	if err != nil {
		t.Fatal(err)
	}
	makefile := string(data)

	for _, want := range []string{
		"image-verify:",
		"docker build --tag",
		"git rev-parse HEAD",
		"LLMTW_IMAGE=",
		"-tags=imageintegration ./integration",
	} {
		if !strings.Contains(makefile, want) {
			t.Errorf("Makefile image-verify contract is missing %q", want)
		}
	}

	runtimeData, err := os.ReadFile(filepath.Join(moduleRoot(t), "integration", "image_integration_test.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"--read-only",
		"/tmp:rw,nosuid,nodev,noexec,size=64m",
		"--user",
		"65532:65532",
	} {
		if !strings.Contains(string(runtimeData), want) {
			t.Errorf("image runtime verification is missing %q", want)
		}
	}
}

func TestImageVerifyOCIArchiveUsesOneSupportedBuildxSolve(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(moduleRoot(t), "Makefile"))
	if err != nil {
		t.Fatal(err)
	}
	makefile := string(data)

	start := strings.Index(makefile, `if [ -n "$(IMAGE_VERIFY_OCI_LAYOUT)" ]; then`)
	if start < 0 {
		t.Fatal("Makefile is missing the OCI layout image-verify branch")
	}
	end := strings.Index(makefile[start:], "\t\telse \\")
	if end < 0 {
		t.Fatal("Makefile OCI layout image-verify branch is missing its fallback boundary")
	}
	branch := makefile[start : start+end]

	for _, want := range []string{
		"docker buildx build --platform linux/amd64 --provenance=false --sbom=false",
		`--output "type=docker,oci-mediatypes=true,dest=$$archive,tar=true,name=$(IMAGE_VERIFY_TAG)"`,
		`archive_directory="$$(mktemp -d "$${TMPDIR:-/tmp}/llmtw-image-verify.XXXXXX")"`,
		`cleanup_archive() { rm -rf -- "$$archive_directory"; };`,
		`docker image load --input "$$archive"`,
		`tar -xf "$$archive" -C "$$layout"`,
		`docker image inspect "$(IMAGE_VERIFY_TAG)"`,
		"trap cleanup_archive EXIT HUP INT TERM",
	} {
		if !strings.Contains(branch, want) {
			t.Fatalf("OCI layout image-verify branch is missing %q", want)
		}
	}
	if strings.Count(branch, "docker buildx build") != 1 {
		t.Fatalf("OCI layout image-verify branch must use exactly one Buildx solve: %q", branch)
	}
	if strings.Count(branch, "--output") != 1 {
		t.Fatalf("OCI layout image-verify branch must use exactly one Buildx exporter: %q", branch)
	}
	for _, forbidden := range []string{
		"--load",
		`docker image load --input "$$layout"`,
		`--output "type=oci,`,
		`rm -rf -- "$$layout"`,
	} {
		if strings.Contains(branch, forbidden) {
			t.Fatalf("OCI layout image-verify branch must not retain competing or directory loading behavior %q", forbidden)
		}
	}
	build := strings.Index(branch, "docker buildx build")
	load := strings.Index(branch, `docker image load --input "$$archive"`)
	extract := strings.Index(branch, `tar -xf "$$archive" -C "$$layout"`)
	if build < 0 || load <= build || extract <= load {
		t.Fatalf("OCI archive must be built once, then loaded and extracted from that exact artifact: %q", branch)
	}
}
