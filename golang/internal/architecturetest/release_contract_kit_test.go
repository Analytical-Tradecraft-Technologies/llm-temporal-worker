package architecturetest

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/contractkit"
)

// The guarded release packages the v1 consumer contract kit (#1575) after
// every evidence check and rescan, from the tag commit, and retains the
// versioned tarball and its digest as the contract-kit artifact. The master
// evidence job records the contract_kit manifest the packaging must match.
func TestWorkflowReleasePackagesTheContractKit(t *testing.T) {
	release := readWorkflow(t, "release.yml")
	master := readWorkflow(t, "master.yml")

	preflight := workflowJob(t, release, "preflight")
	if !jobHasRunCommand(preflight, "bash scripts/release/guard.sh package-contract-kit \\") {
		t.Fatal("release preflight does not package the contract kit with the release guard")
	}
	assertContainsEach(t, release.raw, "release contract kit packaging does not bind %q",
		`--artifact-dir release-artifacts`,
		`--release-ref "$RELEASE_REF"`,
		`--tag-commit "$TAG_COMMIT"`,
		`--output-dir "$RUNNER_TEMP/contract-kit"`,
	)
	imageScan := strings.Index(release.raw, "bash scripts/release/preflight-scan.sh image")
	packaging := strings.Index(release.raw, "bash scripts/release/guard.sh package-contract-kit")
	if imageScan < 0 || packaging < imageScan {
		t.Fatal("release preflight must package the contract kit only after the evidence checks and rescans")
	}
	upload := artifactUploadStep(t, release, "preflight", "contract-kit")
	with := nestedMapping(t, release.name, upload, "with")
	if with["path"] != "${{ runner.temp }}/contract-kit/" || with["if-no-files-found"] != "error" {
		t.Fatalf("contract-kit artifact upload = %#v, want the packaged directory, failing when empty", with)
	}
	if !strings.Contains(release.raw, "uses: actions/upload-artifact@cf430e030ddbb5b0abf93d22962f4752f3646cd9 # v7.0.2") {
		t.Fatal("release.yml must upload the contract kit with the pinned upload-artifact action")
	}

	if !strings.Contains(master.raw, "-artifact contract_kit=contract-kit.json \\") {
		t.Fatal("master release evidence does not record the contract_kit artifact")
	}
	collect := readRepositoryFile(t, repositoryRoot(t), "scripts", "release", "collect.sh")
	if !strings.Contains(collect, `go -C "$module_root" run ./contractkit/cmd/contractkit manifest \`) ||
		!strings.Contains(collect, `-output "$artifact_dir/contract-kit.json"`) {
		t.Fatal("release evidence collection does not write the embedded contract kit manifest")
	}

	guard := readRepositoryFile(t, repositoryRoot(t), "scripts", "release", "guard.sh")
	assertContainsEach(t, guard, "contract kit packaging does not retain %q",
		// Only data trees leave the tag commit; the packager is master's.
		`git archive --format=tar "$tag_commit" -- golang/api/schema/v1 golang/llm/testdata/v1`,
		`go -C "$root/golang" build -o "$work/contractkit" ./contractkit/cmd/contractkit`,
		`-expect "$artifact_dir/contract-kit.json"`,
		`raise SystemExit("guarded release: evidence does not record the v1 contract kit")`,
	)
	for _, forbidden := range []string{"go run", "go test", "make ", "git checkout", "git worktree"} {
		section := guard[strings.Index(guard, "package_contract_kit() {"):]
		section = section[:strings.Index(section, "\n}\n")]
		if strings.Contains(section, forbidden) {
			t.Fatalf("contract kit packaging executes tag code or changes the checkout: %q", forbidden)
		}
	}
}

// TestReleaseGuardPackagesTheContractKitFromTheTagCommit runs the release
// guard's packaging step against a repository whose tag commit holds the
// kit trees, as release.yml does after verifying the evidence.
func TestReleaseGuardPackagesTheContractKitFromTheTagCommit(t *testing.T) {
	repository, tagCommit, changedCommit := createContractKitTestRepository(t)
	evidence := t.TempDir()
	kit, err := contractkit.FromRepository(os.DirFS(repositoryRoot(t))) // the trees the tag commit holds
	if err != nil {
		t.Fatal(err)
	}
	writeReleaseArtifact(t, filepath.Join(evidence, "contract-kit.json"), kit.ManifestJSON())

	output := filepath.Join(t.TempDir(), "contract-kit")
	result := runReleaseGuard(t, repository, "package-contract-kit",
		"--artifact-dir", evidence, "--release-ref", "refs/tags/v1.2.3", "--tag-commit", tagCommit, "--output-dir", output)
	if result.err != nil {
		t.Fatalf("contract kit packaging failed: %v\n%s", result.err, result.output)
	}
	tarball, err := os.ReadFile(filepath.Join(output, "llmtw-contract-kit-v1.2.3.tar.gz"))
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(tarball)
	digest, err := os.ReadFile(filepath.Join(output, "llmtw-contract-kit-v1.2.3.tar.gz.sha256"))
	if err != nil || string(digest) != hex.EncodeToString(sum[:])+"  llmtw-contract-kit-v1.2.3.tar.gz\n" {
		t.Fatalf("tarball digest file = %q, %v", digest, err)
	}
	var want bytes.Buffer
	if err := kit.WriteTarball(&want, "v1.2.3"); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(tarball, want.Bytes()) {
		t.Fatal("packaged tarball is not the kit the evidence records")
	}
	if !strings.Contains(result.output, kit.SchemaDigest()) {
		t.Fatalf("packaging output does not name the schema digest:\n%s", result.output)
	}

	for name, test := range map[string]struct {
		arguments []string
		want      string
	}{
		"tag tree differs from the evidence": {
			arguments: []string{"--artifact-dir", evidence, "--release-ref", "refs/tags/v1.2.4", "--tag-commit", changedCommit},
			want:      "is not the kit",
		},
		"branch reference": {
			arguments: []string{"--artifact-dir", evidence, "--release-ref", "refs/heads/master", "--tag-commit", tagCommit},
			want:      "release_ref must be an exact",
		},
		"abbreviated commit": {
			arguments: []string{"--artifact-dir", evidence, "--release-ref", "refs/tags/v1.2.3", "--tag-commit", tagCommit[:12]},
			want:      "tag_commit must be a lowercase commit SHA",
		},
		"evidence without a kit": {
			arguments: []string{"--artifact-dir", t.TempDir(), "--release-ref", "refs/tags/v1.2.3", "--tag-commit", tagCommit},
			want:      "release evidence has no contract-kit.json",
		},
	} {
		t.Run(name, func(t *testing.T) {
			arguments := append([]string{"package-contract-kit"}, test.arguments...)
			arguments = append(arguments, "--output-dir", filepath.Join(t.TempDir(), "contract-kit"))
			result := runReleaseGuard(t, repository, arguments...)
			if result.err == nil || !strings.Contains(result.output, test.want) {
				t.Fatalf("packaging error = %v, output %q, want %q", result.err, result.output, test.want)
			}
		})
	}
	existing := runReleaseGuard(t, repository, "package-contract-kit",
		"--artifact-dir", evidence, "--release-ref", "refs/tags/v1.2.3", "--tag-commit", tagCommit, "--output-dir", output)
	if existing.err == nil || !strings.Contains(existing.output, "must not already exist") {
		t.Fatalf("packaging reused an existing output directory: %v\n%s", existing.err, existing.output)
	}
}

// createContractKitTestRepository commits a copy of this checkout's kit
// trees, tags it v1.2.3, and adds a second commit that changes one fixture.
func createContractKitTestRepository(t *testing.T) (directory, tagCommit, changedCommit string) {
	t.Helper()
	directory = t.TempDir()
	root := repositoryRoot(t)
	for _, tree := range []string{filepath.Join("golang", "api", "schema", "v1"), filepath.Join("golang", "llm", "testdata", "v1")} {
		entries, err := os.ReadDir(filepath.Join(root, tree))
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			data, err := os.ReadFile(filepath.Join(root, tree, entry.Name()))
			if err != nil {
				t.Fatal(err)
			}
			writeFixtureFile(t, filepath.Join(directory, tree, entry.Name()), string(data))
		}
	}
	runGuardGit(t, directory, "init")
	runGuardGit(t, directory, "config", "user.email", "guard@example.test")
	runGuardGit(t, directory, "config", "user.name", "Release Guard")
	runGuardGit(t, directory, "config", "commit.gpgsign", "false")
	runGuardGit(t, directory, "add", "golang")
	runGuardGit(t, directory, "commit", "-m", "contract kit fixture")
	tagCommit = guardGitCommitID(t, runGuardGit(t, directory, "rev-parse", "HEAD"))
	runGuardGit(t, directory, "tag", "v1.2.3")
	fixture := filepath.Join(directory, "golang", "llm", "testdata", "v1", "generate-root.json")
	data, err := os.ReadFile(fixture)
	if err != nil {
		t.Fatal(err)
	}
	writeReleaseArtifact(t, fixture, bytes.Replace(data, []byte(`"root-1"`), []byte(`"root-2"`), 1))
	runGuardGit(t, directory, "commit", "-am", "changed fixture")
	changedCommit = guardGitCommitID(t, runGuardGit(t, directory, "rev-parse", "HEAD"))
	// The working tree is irrelevant: only the named commit is read.
	writeReleaseArtifact(t, fixture, []byte("uncommitted\n"))
	return directory, tagCommit, changedCommit
}
