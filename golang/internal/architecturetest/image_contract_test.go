package architecturetest

import (
	"os"
	"os/exec"
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
	if strings.Count(dockerfile, "@sha256:") != 2 {
		t.Fatal("Dockerfile must pin both reviewed base images by immutable digest")
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


func TestImageBuildContextAndFinalStageExcludeSecretsAndTools(t *testing.T) {
	dockerfileData, err := os.ReadFile(filepath.Join(moduleRoot(t), "Dockerfile"))
	if err != nil {
		t.Fatal(err)
	}
	dockerfile := string(dockerfileData)
	finalStage := dockerfile[strings.LastIndex(dockerfile, "\nFROM "):]
	for _, want := range []string{
		"COPY --from=build /out/llm-temporal-worker /usr/local/bin/llm-temporal-worker",
		"USER 65532:65532",
		`ENTRYPOINT ["/usr/local/bin/llm-temporal-worker"]`,
	} {
		if !strings.Contains(finalStage, want) {
			t.Errorf("final image contract is missing %q", want)
		}
	}
	for _, forbidden := range []string{"RUN ", "COPY .", " API_KEY", " TOKEN", " PASSWORD", " CREDENTIAL", " MODEL", " PROMPT", "CMD [", "/bin/sh"} {
		if strings.Contains(finalStage, forbidden) {
			t.Errorf("final image stage contains forbidden tool, secret, or wrapper surface %q", forbidden)
		}
	}

	ignoreData, err := os.ReadFile(filepath.Join(moduleRoot(t), ".dockerignore"))
	if err != nil {
		t.Fatal(err)
	}
	ignore := string(ignoreData)
	for _, want := range []string{".env.*", ".local/", "*.pem", "*.key", "secrets/", "**/.aws/", "**/.docker/config.json", "**/*credential*", "**/*token*", "release-artifacts/"} {
		if !strings.Contains(ignore, want) {
			t.Errorf(".dockerignore does not exclude %q", want)
		}
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
			want: "ARG GO_IMAGE=docker.io/library/golang:" + reviewedGoPatch + "@sha256:45a5f7a810238aabcbad211d70b9ae082022d96f7c7259e94041ad1b933575ac",
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
		"docker build --platform linux/amd64 --tag",
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
		`Entrypoint []string`,
		`"/bin/sh"`,
		`"amd64"`,
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
		`--output "type=oci,oci-mediatypes=true,dest=$$archive,tar=true,name=$(IMAGE_VERIFY_TAG)"`,
		`archive_directory="$$(mktemp -d "$${TMPDIR:-/tmp}/llmtw-image-verify.XXXXXX")"`,
		`rm -rf -- "$$archive_directory";`,
		`docker image load --input "$$archive"`,
		`tar -xf "$$archive" -C "$$layout"`,
		`docker image inspect "$(IMAGE_VERIFY_TAG)"`,
		"trap cleanup_archive EXIT",
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
				if err := os.WriteFile(filepath.Join(dir, name), []byte("#!/bin/sh\nset -eu\n"+body), 0700); err != nil {
					t.Fatal(err)
				}
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
			cmd := exec.Command("make", "image-verify", "GO="+filepath.Join(dir, "fake-go"), "IMAGE_VERIFY_GO_VERSION=go1.26.7")
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
