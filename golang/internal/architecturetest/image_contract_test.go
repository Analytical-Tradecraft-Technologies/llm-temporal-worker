package architecturetest

import (
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const reviewedGoPatch = "1.26.11"

func TestDockerfileStampsEveryMetadataFieldIntoImageAndBinary(t *testing.T) {
	dockerfile := readRepositoryFile(t, moduleRoot(t), "Dockerfile")
	if strings.Contains(dockerfile, "@sha256:") {
		t.Fatal("Dockerfile base images must use mutable tags rather than pinned digests")
	}

	for _, want := range []string{
		"ARG VERSION=dev",
		"ARG REVISION=unknown",
		"ARG BUILD_TIME=unknown",
		"ARG SOURCE=https://github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker",
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
		"-X github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/buildinfo.Version=${VERSION}",
		"-X github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/buildinfo.Revision=${REVISION}",
		"-X github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/buildinfo.BuildTime=${BUILD_TIME}",
		"-X github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/buildinfo.Source=${SOURCE}",
		"-X github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/buildinfo.GoVersion=${go_version}",
	} {
		if !strings.Contains(dockerfile, want) {
			t.Errorf("Dockerfile does not stamp %q", want)
		}
	}
}

func TestDockerfileStatesTheOpenRouterUpstreamPolicy(t *testing.T) {
	dockerfile := readRepositoryFile(t, moduleRoot(t), "Dockerfile")
	for _, want := range []string{
		"ARG OPENROUTER_COUNTRIES=Core5",
		"ENV LLMTW_OPENROUTER_COUNTRIES=\"${OPENROUTER_COUNTRIES}\"",
		"LLMTW_OPENROUTER_REQUIRE_NATIVE_PRECISION=false",
		"LLMTW_OPENROUTER_REQUIRE_ZERO_RETENTION=false",
		"LLMTW_OPENROUTER_DENY_PROMPT_TRAINING=false",
		"ARG OPENROUTER_PROVIDER_LOCATIONS=google-vertex=US,google-ai-studio=US,xai=US",
		"LLMTW_OPENROUTER_PROVIDER_LOCATIONS=\"${OPENROUTER_PROVIDER_LOCATIONS}\"",
	} {
		if !strings.Contains(dockerfile, want) {
			t.Errorf("Dockerfile does not set %q", want)
		}
	}
}

func TestImageBuildToolchainVersionPolicyUsesReviewedPatchTag(t *testing.T) {
	dockerfileData := readRepositoryFile(t, moduleRoot(t), "Dockerfile")

	for _, want := range []string{
		"ARG GO_IMAGE=docker.io/library/golang:" + reviewedGoPatch,
		"FROM gcr.io/distroless/static-debian13:nonroot",
	} {
		if !strings.Contains(dockerfileData, want) {
			t.Errorf("Dockerfile toolchain policy is missing %q", want)
		}
	}
	// Upstream Distroless stopped updating Debian 12 images, so its mutable tag
	// no longer picks up base security fixes.
	if strings.Contains(dockerfileData, "static-debian12") {
		t.Error("Dockerfile must not use the deprecated Distroless static-debian12 runtime")
	}

	makefileData := readRepositoryFile(t, moduleRoot(t), "Makefile")
	if !strings.Contains(makefileData, "IMAGE_VERIFY_GO_VERSION ?= $(shell $(GO) env GOVERSION)") {
		t.Error("Makefile image verification must use the installed reviewed Go toolchain")
	}
}

func TestReviewedGoToolchainPinsStayAligned(t *testing.T) {
	repository := repositoryRoot(t)
	module := moduleRoot(t)

	versionData, err := os.ReadFile(filepath.Join(module, ".go-version"))
	testassert.NoError(t, err)
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
			name: "module toolchain",
			path: filepath.Join(module, "go.mod"),
			want: "\ntoolchain go" + reviewedGoPatch + "\n",
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
	makefile := readRepositoryFile(t, moduleRoot(t), "Makefile")

	expectContainsEach(t, makefile, "Makefile image-verify contract is missing %q",
		"image-verify:",
		"docker build --tag",
		"git rev-parse HEAD",
		"LLMTW_IMAGE=",
		"-tags=imageintegration ./integration",
	)

	runtimeData := readRepositoryFile(t, moduleRoot(t), "integration", "image_integration_test.go")
	expectContainsEach(t, runtimeData, "image runtime verification is missing %q",
		"--read-only",
		"/tmp:rw,nosuid,nodev,noexec,size=64m",
		"--user",
		"65532:65532",
	)
}

func TestImageVerifyOCIArchiveUsesOneSupportedBuildxSolve(t *testing.T) {
	makefile := readRepositoryFile(t, moduleRoot(t), "Makefile")

	branch := textSection(t, makefile, `if [ -n "$(IMAGE_VERIFY_OCI_LAYOUT)" ]; then`, "\t\telse \\", "Makefile OCI layout image-verify branch")

	assertContainsEach(t, branch, "OCI layout image-verify branch is missing %q",
		"docker buildx build --platform linux/amd64 --provenance=false --sbom=false",
		`--output "type=oci,oci-mediatypes=true,dest=$$archive,tar=true,name=$(IMAGE_VERIFY_TAG)"`,
		`archive_directory="$$(mktemp -d "$${TMPDIR:-/tmp}/llmtw-image-verify.XXXXXX")"`,
		`rm -rf -- "$$archive_directory";`,
		`docker image load --input "$$archive"`,
		`tar -xf "$$archive" -C "$$layout"`,
		`docker image inspect "$(IMAGE_VERIFY_TAG)"`,
		"trap cleanup_archive EXIT",
	)
	if strings.Count(branch, "docker buildx build") != 1 {
		t.Fatalf("OCI layout image-verify branch must use exactly one Buildx solve: %q", branch)
	}
	if strings.Count(branch, "--output") != 1 {
		t.Fatalf("OCI layout image-verify branch must use exactly one Buildx exporter: %q", branch)
	}
	for _, forbidden := range []string{
		"--load",
		`docker image load --input "$$layout"`,
		`--output "type=docker,`,
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

func TestImageVerifyOCIArchiveFailureReportsStageAndCleansArchive(t *testing.T) {
	for _, scenario := range []struct{ failAt, stage string }{
		{"export", "OCI export"},
		{"import", "OCI import (requires the containerd image store)"},
		{"runtime", "runtime checks"},
	} {
		t.Run(scenario.failAt, func(t *testing.T) {
			dir := t.TempDir()
			fake := func(name, body string) {
				t.Helper()
				testassert.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("#!/bin/sh\nset -eu\n"+body), 0700))
			}
			fake("docker", `case "$1 ${2:-}" in
  "info ") exit 0 ;;
  "buildx build")
    [ "$FAIL_AT" != export ] || exit 1
    while [ "$1" != --output ]; do shift; done
    case "$2" in type=oci,*) ;; *) exit 2 ;; esac
    archive=${2#*dest=}; archive=${archive%%,*}
    mkdir "$TMPDIR/payload"
    printf '{"imageLayoutVersion":"1.0.0"}' > "$TMPDIR/payload/oci-layout"
    tar -cf "$archive" -C "$TMPDIR/payload" oci-layout ;;
  "image load")
    [ "$FAIL_AT" != import ] || exit 1
    test -f "$4" ;;
  "image inspect") test -f "$IMAGE_VERIFY_OCI_LAYOUT/oci-layout" ;;
  *) exit 2 ;;
esac
`)
			fake("fake-go", `test "$1" = test
test -f "$IMAGE_VERIFY_OCI_LAYOUT/oci-layout"
test -n "$LLMTW_IMAGE"
exit 1
`)
			cmd := exec.Command("make", "image-verify", "GO="+filepath.Join(dir, "fake-go"), "IMAGE_VERIFY_GO_VERSION=go1.26.11")
			cmd.Dir = moduleRoot(t)
			cmd.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"), "TMPDIR="+dir, "FAIL_AT="+scenario.failAt, "IMAGE_VERIFY_OCI_LAYOUT="+filepath.Join(dir, "image.oci"))
			output, err := cmd.CombinedOutput()
			if err == nil || !strings.Contains(string(output), "image-verify: failed at "+scenario.stage+"\n") {
				t.Fatalf("wrong failure diagnostic: %v\n%s", err, output)
			}
			archives, err := filepath.Glob(filepath.Join(dir, "llmtw-image-verify.*"))
			if err != nil || len(archives) != 0 {
				t.Fatalf("failed verification retained temporary archives: %v, %v", archives, err)
			}
		})
	}
}
