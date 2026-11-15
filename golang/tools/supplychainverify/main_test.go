package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/repotest"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
)

func TestVerifyAcceptsReviewedInventoryAndApprovedFinding(t *testing.T) {
	t.Parallel()

	report, err := verifyStream(
		testBaselineWith(testVulnerabilityException("GO-2099-0001", "module_only")),
		`{"finding":{"osv":"GO-2099-0001","trace":[{"module":"example.test/module","version":"v1.2.3"}]}}`,
	)
	testassert.NoError(t, err)
	if report.Status != "pass" {
		t.Fatalf("report status = %q, want pass", report.Status)
	}
	if report.DirectModuleCount != 2 {
		t.Fatalf("direct module count = %d, want 2", report.DirectModuleCount)
	}
	if !reflect.DeepEqual(report.ApprovedFindings, []string{"GO-2099-0001"}) {
		t.Fatalf("approved findings = %#v", report.ApprovedFindings)
	}
}

func TestVerifyRejectsUnreviewedAndExpiredVulnerabilityFindings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		baseline baseline
		want     string
	}{
		{
			name:     "unreviewed finding",
			baseline: testBaseline(),
			want:     "GO-2099-0001",
		},
		{
			name: "expired exception",
			baseline: testBaselineWith(editedException(func(exception *vulnerabilityException) {
				exception.Expires = "2026-07-13T00:00:00Z"
			})),
			want: "expired",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := verify(test.baseline, testRequirements(), strings.NewReader(`{"finding":{"osv":"GO-2099-0001"}}`), julyNow)
			testassert.ErrorContains(t, err, test.want, "verify")
		})
	}
}

func TestVerifyRejectsInventoryDriftAndIncompleteExceptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		baseline     baseline
		requirements []moduleRequirement
		want         string
	}{
		{
			name:         "unreviewed direct module",
			baseline:     testBaseline(),
			requirements: append(testRequirements(), moduleRequirement{Path: "example.test/new", Version: "v1.2.3"}),
			want:         "example.test/new",
		},
		{
			name:         "missing exception owner",
			baseline:     testBaselineWith(editedException(func(exception *vulnerabilityException) { exception.Owner = "" })),
			requirements: testRequirements(),
			want:         "owner",
		},
		{
			name:         "missing exception scope",
			baseline:     testBaselineWith(editedException(func(exception *vulnerabilityException) { exception.Scope = "" })),
			requirements: testRequirements(),
			want:         "scope",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := verify(test.baseline, test.requirements, strings.NewReader(""), julyNow)
			if err == nil {
				t.Fatal("verify accepted an invalid inventory or vulnerability exception")
			}
			if !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(test.want)) {
				t.Fatalf("verify error %q does not contain %q", err, test.want)
			}
		})
	}
}

func TestReportDoesNotRetainRawScannerTrace(t *testing.T) {
	t.Parallel()

	report, err := verifyStream(
		testBaselineWith(testVulnerabilityException("GO-2099-0001", "reachable")),
		`{"finding":{"osv":"GO-2099-0001","trace":[{"frame":{"position":{"filename":"raw-provider-response.txt"}}}]}}`,
	)
	testassert.NoError(t, err)

	var output bytes.Buffer
	testassert.NoError(t, writeReport(&output, report))
	for _, forbidden := range []string{"raw-provider-response.txt", "trace", "filename"} {
		if strings.Contains(output.String(), forbidden) {
			t.Fatalf("redacted report contains scanner detail %q: %s", forbidden, output.String())
		}
	}
}

func TestReadGoModExcludesIndirectRequirements(t *testing.T) {
	t.Parallel()

	requirements, err := readGoMod(strings.NewReader(`{"Require":[{"Path":"example.test/direct","Version":"v1.2.3"},{"Path":"example.test/indirect","Version":"v1.2.3","Indirect":true}]}`))
	testassert.NoError(t, err)
	if !reflect.DeepEqual(requirements, []moduleRequirement{{Path: "example.test/direct", Version: "v1.2.3"}}) {
		t.Fatalf("requirements = %#v", requirements)
	}
}

func TestReadGoModIncludesReviewedToolModules(t *testing.T) {
	t.Parallel()

	requirements, err := readGoMod(strings.NewReader(`{
		"Require":[{"Path":"golang.org/x/vuln","Version":"v1.7.0","Indirect":true}],
		"Tool":[{"Path":"golang.org/x/vuln/cmd/govulncheck"}]
	}`))
	testassert.NoError(t, err)
	want := []moduleRequirement{{Path: "golang.org/x/vuln", Version: "v1.7.0"}}
	if !reflect.DeepEqual(requirements, want) {
		t.Fatalf("requirements = %#v, want %#v", requirements, want)
	}
}

func TestReadGoModRejectsToolWithoutVersionedModule(t *testing.T) {
	t.Parallel()

	_, err := readGoMod(strings.NewReader(`{"Tool":[{"Path":"example.test/tool/cmd/tool"}]}`))
	testassert.ErrorContains(t, err, "no versioned module requirement")
}

func TestReadGoModRejectsReplacements(t *testing.T) {
	t.Parallel()

	_, err := readGoMod(strings.NewReader(`{
		"Require":[{"Path":"example.test/direct","Version":"v1.2.3"}],
		"Replace":[{
			"Old":{"Path":"example.test/direct","Version":"v1.2.3"},
			"New":{"Path":"./unreviewed-local-fork"}
		}]
	}`))
	testassert.ErrorContains(t, err, "replacement", "readGoMod")
}

func TestVerifyRejectsReachableTraceOutsideModuleOnlyException(t *testing.T) {
	t.Parallel()

	_, err := verifyStream(
		testBaselineWith(testVulnerabilityException("GO-2099-0001", "module_only")),
		`{"finding":{"osv":"GO-2099-0001","trace":[{"module":"example.test/module","version":"v1.2.3"}]}}
{"finding":{"osv":"GO-2099-0001","trace":[{"module":"example.test/module","version":"v1.2.3"},{"package":"example.test/module/vulnerable","function":"Parse"}]}}`,
	)
	testassert.ErrorContains(t, err, "reachable trace", "verify")
}

func TestPullRequestVerificationOnlyRejectsIntroducedVulnerabilities(t *testing.T) {
	t.Parallel()

	policy := testBaseline()
	base := `{"finding":{"osv":"GO-2099-0001","trace":[{"module":"example.test/one","version":"v1.2.3"}]}}`

	report, err := verifyPullRequest(
		policy,
		testRequirements(),
		strings.NewReader(base),
		strings.NewReader(base),
		julyNow,
	)
	testassert.NoError(t, err, "unchanged finding failed PR verification")
	if len(report.IntroducedFindings) != 0 {
		t.Fatalf("introduced findings = %#v, want none", report.IntroducedFindings)
	}

	_, err = verifyPullRequest(
		policy,
		testRequirements(),
		strings.NewReader(base),
		strings.NewReader(base+"\n"+`{"finding":{"osv":"GO-2099-0002"}}`),
		julyNow,
	)
	testassert.ErrorContains(t, err, "GO-2099-0002")
}

func TestPullRequestVerificationRejectsReachabilityEscalation(t *testing.T) {
	t.Parallel()

	policy := testBaselineWith(testVulnerabilityException("GO-2099-0001", "module_only"))
	base := `{"finding":{"osv":"GO-2099-0001","trace":[{"module":"example.test/one","version":"v1.2.3"}]}}`
	head := base + "\n" + `{"finding":{"osv":"GO-2099-0001","trace":[{"module":"example.test/one","version":"v1.2.3"},{"package":"example.test/one/vulnerable"}]}}`
	_, err := verifyPullRequest(
		policy,
		testRequirements(),
		strings.NewReader(base),
		strings.NewReader(head),
		julyNow,
	)
	testassert.ErrorContains(t, err, "reachable")
}

func TestTrustedModuleRootsAreVersionIndependentAndBoundaryAware(t *testing.T) {
	t.Parallel()

	roots := []moduleRecord{{Path: "example.test/publisher", License: "MIT", Source: "https://example.test/publisher"}}
	testassert.NoError(t, validateInventory(roots, []moduleRequirement{{Path: "example.test/publisher/module", Version: "v99.0.0"}}), "approved-root version bump failed")
	for _, path := range []string{"example.test/unreviewed", "example.test/publisher-typosquat/module"} {
		if err := validateInventory(roots, []moduleRequirement{{Path: path, Version: "v1.0.0"}}); err == nil {
			t.Fatalf("unapproved module %q passed trusted-root validation", path)
		}
	}
}

func TestRepositoryDirectModulesHaveReviewedTrustedRoots(t *testing.T) {
	t.Parallel()

	policyJSON, err := os.ReadFile(filepath.Join(moduleRoot(t), "tools", "supplychainverify", "baseline.json"))
	testassert.NoError(t, err)
	var policy baseline
	testassert.NoError(t, json.Unmarshal(policyJSON, &policy))
	command := exec.Command("go", "mod", "edit", "-json")
	command.Dir = moduleRoot(t)
	inventoryJSON, err := command.Output()
	testassert.NoError(t, err)
	requirements, err := readGoMod(bytes.NewReader(inventoryJSON))
	testassert.NoError(t, err)
	testassert.NoError(t, validateInventory(policy.TrustedModuleRoots, requirements))
}

func TestMakefileAndWorkflowsComposeBoundedSecurityVerification(t *testing.T) {
	t.Parallel()

	root := repositoryRoot(t)
	makefile := readTestFile(t, filepath.Join(moduleRoot(t), "Makefile"))
	for _, expected := range []string{
		"GOTOOLCHAIN=$(SECURITY_GO_TOOLCHAIN)",
		"$(GO) tool govulncheck",
		"security-pr-verify:",
		"base-vulnerability-output",
		"govulncheck.stderr",
		"$(GO) mod edit -json",
		"$(GO) run ./tools/supplychainverify",
		"SECURITY_REPORT",
	} {
		if !strings.Contains(makefile, expected) {
			t.Fatalf("Makefile does not contain %q", expected)
		}
	}

	if !strings.Contains(readTestFile(t, filepath.Join(root, ".github", "workflows", "pull-request.yml")), "make security-pr-verify") {
		t.Fatal("PR workflow does not run security-pr-verify")
	}

	if strings.Contains(readTestFile(t, filepath.Join(root, ".github", "workflows", "master.yml")), "security-verify") {
		t.Fatal("master workflow still runs security verification")
	}
	if !strings.Contains(readTestFile(t, filepath.Join(root, ".github", "workflows", "security-scheduled.yml")), "make security-verify") {
		t.Fatal("scheduled workflow does not run security-verify")
	}
}

func TestReadFindingsMergesDuplicatesAndRejectsMalformedStreams(t *testing.T) {
	t.Parallel()

	findings, err := readFindings(strings.NewReader(`{"event":"progress"}
{"finding":{"osv":" GO-2099-0002 ","trace":[]}}
{"finding":{"osv":"GO-2099-0001","trace":[{"module":"example.test/one","version":"v1.2.3"}]}}
{"finding":{"osv":"GO-2099-0001","trace":[{"module":"example.test/one","version":"v1.2.3"},{"package":"example.test/one/vulnerable"}]}}`))
	testassert.NoError(t, err)
	want := []vulnerabilityFinding{{ID: "GO-2099-0001", HasReachableTrace: true}, {ID: "GO-2099-0002"}}
	if !reflect.DeepEqual(findings, want) {
		t.Fatalf("findings = %#v, want %#v", findings, want)
	}

	for _, input := range []string{
		`{"finding":{"osv":"OSV-1"}}`,
		`{"finding":{"osv":"GO-2099-0001","trace":["not-an-object"]}}`,
		`{"finding":{"osv":"GO-2099-0001","trace":[{"module":123,"version":"v1.2.3"}]}}`,
		`{"finding":`,
	} {
		if _, err := readFindings(strings.NewReader(input)); err == nil {
			t.Fatalf("readFindings accepted malformed stream %s", input)
		}
	}
}

func TestHasReachableTraceClassifiesScannerShapes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		trace     []json.RawMessage
		reachable bool
		wantErr   bool
	}{
		{name: "no trace", trace: nil},
		{name: "module only", trace: traceFrames(`{"module":"example.test/one"}`), reachable: true},
		{name: "complete module frame", trace: traceFrames(`{"module":"example.test/one","version":"v1.2.3"}`)},
		{name: "blank module frame", trace: traceFrames(`{"module":"","version":"v1.2.3"}`), reachable: true},
		{name: "extra frame fields", trace: traceFrames(`{"module":"example.test/one","version":"v1.2.3","package":"vulnerable"}`), reachable: true},
		{name: "multiple frames", trace: traceFrames(`{"module":"example.test/one","version":"v1.2.3"}`, `{"package":"vulnerable"}`), reachable: true},
		{name: "malformed frame", trace: traceFrames(`"not-an-object"`), wantErr: true},
		{name: "malformed module", trace: traceFrames(`{"module":123,"version":"v1.2.3"}`), wantErr: true},
		{name: "malformed version", trace: traceFrames(`{"module":"example.test/one","version":123}`), wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			reachable, err := hasReachableTrace(test.trace)
			if test.wantErr {
				if err == nil {
					t.Fatal("hasReachableTrace accepted malformed frame")
				}
				return
			}
			if err != nil || reachable != test.reachable {
				t.Fatalf("hasReachableTrace = %v, %v; want %v", reachable, err, test.reachable)
			}
		})
	}
}

func TestValidateBaselineRejectsSecurityContractDrift(t *testing.T) {
	t.Parallel()

	// withException replaces the exceptions with one edited valid exception.
	withException := func(edit func(*vulnerabilityException)) func(*baseline) {
		return func(value *baseline) {
			value.VulnerabilityExceptions = []vulnerabilityException{editedException(edit)}
		}
	}
	tests := []struct {
		name   string
		mutate func(*baseline)
		want   string
	}{
		{name: "wrong version", mutate: func(value *baseline) { value.Version = 1 }, want: "version"},
		{name: "no allowed licenses", mutate: func(value *baseline) { value.AllowedLicenses = nil }, want: "no allowed licenses"},
		{name: "empty allowed license", mutate: func(value *baseline) { value.AllowedLicenses[0] = " " }, want: "empty allowed license"},
		{name: "duplicate allowed license", mutate: func(value *baseline) { value.AllowedLicenses = append(value.AllowedLicenses, "MIT") }, want: "repeats allowed license"},
		{name: "incomplete module root", mutate: func(value *baseline) { value.TrustedModuleRoots[0].Path = "" }, want: "incomplete trusted module root"},
		{name: "duplicate module root", mutate: func(value *baseline) {
			value.TrustedModuleRoots = append(value.TrustedModuleRoots, value.TrustedModuleRoots[0])
		}, want: "repeats trusted module root"},
		{name: "unapproved license", mutate: func(value *baseline) { value.TrustedModuleRoots[0].License = "GPL-3.0" }, want: "unapproved license"},
		{name: "invalid module source", mutate: func(value *baseline) { value.TrustedModuleRoots[0].Source = "http://example.test/one" }, want: "invalid source"},
		{name: "invalid exception id", mutate: withException(func(exception *vulnerabilityException) { exception.ID = "CVE-2099-0001" }), want: "requires an id"},
		{name: "duplicate exception", mutate: func(value *baseline) {
			exception := testVulnerabilityException("GO-2099-0001", "module_only")
			value.VulnerabilityExceptions = []vulnerabilityException{exception, exception}
		}, want: "repeats vulnerability exception"},
		{name: "expired exception", mutate: withException(func(exception *vulnerabilityException) { exception.Expires = "2026-07-13T00:00:00Z" }), want: "expired"},
		{name: "invalid remediation", mutate: withException(func(exception *vulnerabilityException) { exception.Remediation = "http://example.test/remediation" }), want: "invalid remediation"},
		{name: "invalid scope", mutate: withException(func(exception *vulnerabilityException) { exception.Scope = "package_only" }), want: "invalid scope"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			value := testBaseline()
			test.mutate(&value)
			testassert.ErrorContains(t, testassert.Err(validateBaseline(value, julyNow)), test.want)
		})
	}
}

func TestValidateInventoryRejectsDuplicatesAndUntrustedRoots(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		requirements []moduleRequirement
		want         string
	}{
		{name: "duplicate direct module", requirements: append(testRequirements(), testRequirements()[0]), want: "repeats direct module"},
		{name: "module outside trusted roots", requirements: append(testRequirements(), moduleRequirement{Path: "example.test/new", Version: "v1.2.3"}), want: "outside the reviewed trusted module roots"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			testassert.ErrorContains(t, validateInventory(testBaseline().TrustedModuleRoots, test.requirements), test.want)
		})
	}
}

func TestValidateComponentStatusRequiresPass(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		edit func(*componentStatus)
		want string
	}{
		{name: "invalid status", edit: func(status *componentStatus) { status.Test = "unknown" }, want: "invalid status"},
		{name: "failed component", edit: func(status *componentStatus) { status.Source = "fail" }, want: "did not pass"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			status := passingComponents()
			test.edit(&status)
			testassert.ErrorContains(t, validateComponentStatus(status), test.want)
		})
	}
}

func TestWriteReportFileCreatesPrivateNestedReport(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "nested", "security-verify.json")
	testassert.NoError(t, writeReportFile(path, verificationReport{Version: baselineVersion, Status: "fail", Findings: []string{}, ApprovedFindings: []string{}}))
	info, err := os.Stat(path)
	testassert.NoError(t, err)
	if permission := info.Mode().Perm(); permission != 0o600 {
		t.Fatalf("report permissions = %o, want 600", permission)
	}
	report, _ := readReport(t, path)
	if report.Status != "fail" || report.Version != baselineVersion {
		t.Fatalf("report = %#v", report)
	}
}

// minimalBaselineJSON is a baseline that trusts one MIT module root and has
// no vulnerability exceptions.
const minimalBaselineJSON = `{"version":2,"allowed_licenses":["MIT"],"trusted_module_roots":[{"path":"example.test/one","license":"MIT","source":"https://example.test/one"}],"vulnerability_exceptions":[]}`

// writeTestFile writes contents to root/name and returns the path.
func writeTestFile(t *testing.T, root, name string, contents []byte) string {
	t.Helper()
	path := filepath.Join(root, name)
	testassert.NoError(t, os.WriteFile(path, contents, 0o600))
	return path
}

func TestCommandSmokeWritesRedactedReport(t *testing.T) {
	root := t.TempDir()
	writeJSON := func(name string, value any) string {
		contents, err := json.Marshal(value)
		testassert.NoError(t, err)
		return writeTestFile(t, root, name, contents)
	}
	baselinePath := writeJSON("baseline.json", testBaselineWith(testVulnerabilityException("GO-2099-0001", "reachable")))
	goModPath := writeJSON("go-mod.json", goMod{Require: testRequirements()})
	vulnerabilityPath := writeTestFile(t, root, "vulnerabilities.json", []byte(`{"finding":{"osv":"GO-2099-0001","trace":[{"frame":{"position":{"filename":"/private/raw-scanner-trace.txt"}}}]}}`))
	reportPath := filepath.Join(root, "reports", "security-verify.json")
	binaryPath := buildSupplyChainVerify(t, root, "GOCACHE="+filepath.Join(root, "go-cache"))

	command := exec.Command(binaryPath, "-baseline", baselinePath, "-go-mod", goModPath, "-vulnerability-output", vulnerabilityPath, "-report", reportPath)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("supplychainverify failed: %v\n%s", err, output)
	}
	if !strings.Contains(string(output), "supply chain verification passed") {
		t.Fatalf("command output = %s", output)
	}
	value, report := readReport(t, reportPath)
	if strings.Contains(report, "raw-scanner-trace.txt") || strings.Contains(report, "trace") {
		t.Fatalf("report retained scanner detail: %s", report)
	}
	if value.Status != "pass" || !reflect.DeepEqual(value.ApprovedFindings, []string{"GO-2099-0001"}) {
		t.Fatalf("report = %#v", value)
	}
}

func TestExecuteValidatesInputsAndComponentStatuses(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	write := func(name, contents string) string { return writeTestFile(t, root, name, []byte(contents)) }
	baselinePath := write("baseline.json", minimalBaselineJSON)
	goModPath := write("go-mod.json", `{"Require":[{"Path":"example.test/one","Version":"v1.2.3"}]}`)
	vulnerabilityPath := write("vulnerabilities.json", "")
	components := passingComponents()

	report, err := execute(baselinePath, goModPath, vulnerabilityPath, components, julyNow)
	if err != nil || report.Status != "pass" || report.DirectModuleCount != 1 {
		t.Fatalf("execute success = %#v, %v", report, err)
	}

	tests := []struct {
		name       string
		baseline   string
		goMod      string
		vulnerable string
		components componentStatus
		want       string
	}{
		{name: "missing baseline", baseline: filepath.Join(root, "missing-baseline.json"), goMod: goModPath, vulnerable: vulnerabilityPath, components: components, want: "cannot read baseline"},
		{name: "invalid baseline", baseline: write("invalid-baseline.json", "{"), goMod: goModPath, vulnerable: vulnerabilityPath, components: components, want: "baseline is not valid JSON"},
		{name: "missing go.mod inventory", baseline: baselinePath, goMod: filepath.Join(root, "missing-go-mod.json"), vulnerable: vulnerabilityPath, components: components, want: "cannot read go.mod inventory"},
		{name: "invalid go.mod inventory", baseline: baselinePath, goMod: write("invalid-go-mod.json", "{"), vulnerable: vulnerabilityPath, components: components, want: "go.mod inventory is not valid JSON"},
		{name: "missing vulnerability output", baseline: baselinePath, goMod: goModPath, vulnerable: filepath.Join(root, "missing-vulnerabilities.json"), components: components, want: "cannot read vulnerability output"},
		{name: "invalid vulnerability output", baseline: baselinePath, goMod: goModPath, vulnerable: write("invalid-vulnerabilities.json", "{"), components: components, want: "vulnerability output is not valid JSON"},
		{name: "invalid component status", baseline: baselinePath, goMod: goModPath, vulnerable: vulnerabilityPath, components: componentStatus{Test: "unknown", Source: "pass", GoMod: "pass", Vulnerability: "pass"}, want: "component test has an invalid status"},
		{name: "failed component", baseline: baselinePath, goMod: goModPath, vulnerable: vulnerabilityPath, components: componentStatus{Test: "pass", Source: "fail", GoMod: "pass", Vulnerability: "pass"}, want: "component source did not pass"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			report, err := execute(test.baseline, test.goMod, test.vulnerable, test.components, julyNow)
			assertFailedReport(t, "execute error", report, err, test.want)
		})
	}
}

func TestReleaseTagVerificationAppliesExceptionScopesWithoutRequiringUse(t *testing.T) {
	t.Parallel()

	baseline := testBaselineWith(
		testVulnerabilityException("GO-2099-0001", "module_only"),
		testVulnerabilityException("GO-2099-0002", "reachable"),
		// The trusted master baseline may cover findings an older tag lacks.
		testVulnerabilityException("GO-2099-0003", "module_only"),
	)
	output := strings.Join([]string{
		`{"config":{"scanner_name":"govulncheck"}}`,
		`{"finding":{"osv":"GO-2099-0001","trace":[{"module":"example.test/one","version":"v1.2.3"}]}}`,
		`{"finding":{"osv":"GO-2099-0002","trace":[{"module":"stdlib","version":"v1.26.7","package":"net/http","function":"Serve"}]}}`,
	}, "\n")

	report, err := verifyReleaseTag(baseline, strings.NewReader(output), octoberNow)
	testassert.NoError(t, err)
	if report.Status != "pass" {
		t.Fatalf("report status = %q, want pass", report.Status)
	}
	if want := []string{"GO-2099-0001", "GO-2099-0002"}; !reflect.DeepEqual(report.Findings, want) || !reflect.DeepEqual(report.ApprovedFindings, want) {
		t.Fatalf("report findings = %#v, approved = %#v, want %#v", report.Findings, report.ApprovedFindings, want)
	}
	if report.DirectModuleCount != 0 {
		t.Fatalf("release tag verification must not claim a go.mod inventory, got %d direct modules", report.DirectModuleCount)
	}
}

func TestReleaseTagVerificationNamesEveryRejectedFinding(t *testing.T) {
	t.Parallel()

	baseline := testBaselineWith(testVulnerabilityException("GO-2099-0001", "module_only"))
	output := strings.Join([]string{
		// An imported package is reachable for the baseline, not only a called
		// function; govulncheck's text exit status counts only the latter.
		`{"finding":{"osv":"GO-2099-0001","trace":[{"module":"example.test/one","version":"v1.2.3","package":"example.test/one/vulnerable"}]}}`,
		`{"finding":{"osv":"GO-2099-0003","trace":[{"module":"example.test/two","version":"v2.3.4"}]}}`,
		`{"finding":{"osv":"GO-2099-0002","trace":[{"module":"stdlib","version":"v1.26.7","package":"net/http","function":"Serve"}]}}`,
	}, "\n")

	report, err := verifyReleaseTag(baseline, strings.NewReader(output), octoberNow)
	if err == nil {
		t.Fatal("release tag verification accepted unreviewed and out-of-scope findings")
	}
	for _, want := range []string{
		"rejected 3 release tag vulnerabilities",
		"GO-2099-0001 (reachable outside its module_only exception)",
		"GO-2099-0002 (unreviewed)",
		"GO-2099-0003 (unreviewed)",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("release tag verification error = %q, want %q", err, want)
		}
	}
	if report.Status != "fail" || len(report.ApprovedFindings) != 0 || len(report.Findings) != 3 {
		t.Fatalf("report = %#v", report)
	}
}

func TestReleaseTagVerificationRejectsInvalidPolicyAndOutput(t *testing.T) {
	t.Parallel()

	exception := testVulnerabilityException("GO-2099-0001", "reachable")
	exception.Expires = "2026-10-16T00:00:00Z"
	expired := testBaselineWith(exception)
	wrongVersion := testBaseline()
	wrongVersion.Version = 1

	for _, test := range []struct {
		name     string
		baseline baseline
		output   string
		want     string
	}{
		{name: "expired exception", baseline: expired, output: "", want: "expired"},
		{name: "baseline version drift", baseline: wrongVersion, output: "", want: "baseline version"},
		{name: "malformed output", baseline: testBaseline(), output: "{", want: "not valid JSON"},
		{name: "invalid identifier", baseline: testBaseline(), output: `{"finding":{"osv":"CVE-2099-0001"}}`, want: "invalid finding identifier"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			testassert.ErrorContains(t, testassert.Err(verifyReleaseTag(test.baseline, strings.NewReader(test.output), octoberNow)), test.want)
		})
	}
}

func TestExecuteReleaseTagRequiresTheScanToCompleteAndSkipsOtherComponents(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	write := func(name, contents string) string { return writeTestFile(t, root, name, []byte(contents)) }
	baselinePath := write("baseline.json", minimalBaselineJSON)
	vulnerabilityPath := write("vulnerabilities.json", "")

	report, err := executeReleaseTag(baselinePath, vulnerabilityPath, "pass", octoberNow)
	if err != nil || report.Status != "pass" {
		t.Fatalf("executeReleaseTag success = %#v, %v", report, err)
	}
	want := componentStatus{Test: "skipped", Source: "skipped", GoMod: "skipped", Vulnerability: "pass"}
	if report.Components != want {
		t.Fatalf("release tag components = %#v, want %#v", report.Components, want)
	}

	for _, test := range []struct {
		name          string
		baseline      string
		vulnerability string
		status        string
		want          string
	}{
		// A partial or empty stream from a failed scan must not pass.
		{name: "scan did not complete", baseline: baselinePath, vulnerability: vulnerabilityPath, status: "fail", want: "component vulnerability did not pass"},
		{name: "invalid scan status", baseline: baselinePath, vulnerability: vulnerabilityPath, status: "unknown", want: "component vulnerability has an invalid status"},
		{name: "missing baseline", baseline: filepath.Join(root, "missing.json"), vulnerability: vulnerabilityPath, status: "pass", want: "cannot read baseline"},
		{name: "invalid baseline", baseline: write("invalid-baseline.json", "{"), vulnerability: vulnerabilityPath, status: "pass", want: "baseline is not valid JSON"},
		{name: "missing vulnerability output", baseline: baselinePath, vulnerability: filepath.Join(root, "missing-vulnerabilities.json"), status: "pass", want: "cannot read vulnerability output"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			report, err := executeReleaseTag(test.baseline, test.vulnerability, test.status, octoberNow)
			assertFailedReport(t, "executeReleaseTag", report, err, test.want)
		})
	}
}

func TestCommandReleaseTagModeRejectsInventoryInputs(t *testing.T) {
	root := t.TempDir()
	binaryPath := buildSupplyChainVerify(t, root)
	baselinePath := writeTestFile(t, root, "baseline.json", []byte(minimalBaselineJSON))
	vulnerabilityPath := writeTestFile(t, root, "vulnerabilities.json", nil)
	reportPath := filepath.Join(root, "report.json")

	passed := exec.Command(binaryPath, "-release-tag", "-baseline", baselinePath, "-vulnerability-output", vulnerabilityPath, "-report", reportPath)
	if output, err := passed.CombinedOutput(); err != nil || !strings.Contains(string(output), "supply chain verification passed") {
		t.Fatalf("release tag command = %v\n%s", err, output)
	}

	for _, extra := range [][]string{
		{"-go-mod", baselinePath},
		{"-base-vulnerability-output", vulnerabilityPath},
	} {
		arguments := append([]string{"-release-tag", "-baseline", baselinePath, "-vulnerability-output", vulnerabilityPath, "-report", reportPath}, extra...)
		output, err := exec.Command(binaryPath, arguments...).CombinedOutput()
		var exitError *exec.ExitError
		if !errors.As(err, &exitError) || exitError.ExitCode() != 2 || !strings.Contains(string(output), "release tag verification requires") {
			t.Fatalf("release tag command with %v = %v\n%s", extra, err, output)
		}
	}
}

// julyNow and octoberNow are the fixed verification times of the inventory and
// release tag tests.
var (
	julyNow    = time.Date(2026, time.July, 14, 0, 0, 0, 0, time.UTC)
	octoberNow = time.Date(2026, time.October, 17, 0, 0, 0, 0, time.UTC)
)

// verifyStream verifies the scanner stream against policy, the test
// requirements and julyNow.
func verifyStream(policy baseline, stream string) (verificationReport, error) {
	return verify(policy, testRequirements(), strings.NewReader(stream), julyNow)
}

// testBaselineWith is testBaseline with the given vulnerability exceptions.
func testBaselineWith(exceptions ...vulnerabilityException) baseline {
	value := testBaseline()
	value.VulnerabilityExceptions = exceptions
	return value
}

// editedException is the valid module_only GO-2099-0001 exception after edit.
func editedException(edit func(*vulnerabilityException)) vulnerabilityException {
	exception := testVulnerabilityException("GO-2099-0001", "module_only")
	edit(&exception)
	return exception
}

// traceFrames converts JSON frame literals into a scanner trace.
func traceFrames(frames ...string) []json.RawMessage {
	trace := make([]json.RawMessage, 0, len(frames))
	for _, frame := range frames {
		trace = append(trace, json.RawMessage(frame))
	}
	return trace
}

// passingComponents is a component status in which every component passed.
func passingComponents() componentStatus {
	return componentStatus{Test: "pass", Source: "pass", GoMod: "pass", Vulnerability: "pass"}
}

// assertFailedReport requires err to name want and the report to record it as
// a failure.
func assertFailedReport(t *testing.T, label string, report verificationReport, err error, want string) {
	t.Helper()
	if err == nil || report.Status != "fail" || !strings.Contains(err.Error(), want) || report.Error != err.Error() {
		t.Fatalf("%s = %#v, %v; want %q", label, report, err, want)
	}
}

// readTestFile returns the contents of path.
func readTestFile(t *testing.T, path string) string {
	t.Helper()
	return string(testassert.ReadFile(t, path))
}

// readReport decodes the verification report at path and returns it with its
// raw JSON.
func readReport(t *testing.T, path string) (verificationReport, string) {
	t.Helper()
	contents := readTestFile(t, path)
	var report verificationReport
	testassert.NoError(t, json.Unmarshal([]byte(contents), &report))
	return report, contents
}

// buildSupplyChainVerify builds the command into root, adding env to the
// build environment when given, and returns the binary path.
func buildSupplyChainVerify(t *testing.T, root string, env ...string) string {
	t.Helper()
	moduleDirectory, err := os.Getwd()
	testassert.NoError(t, err)
	binaryPath := filepath.Join(root, "supplychainverify")
	build := exec.Command("go", "build", "-o", binaryPath, ".")
	build.Dir = moduleDirectory
	if len(env) > 0 {
		build.Env = append(os.Environ(), env...)
	}
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build failed: %v\n%s", err, output)
	}
	return binaryPath
}

func testBaseline() baseline {
	return baseline{
		Version:         2,
		AllowedLicenses: []string{"Apache-2.0", "MIT"},
		TrustedModuleRoots: []moduleRecord{
			{Path: "example.test/one", License: "MIT", Source: "https://example.test/one"},
			{Path: "example.test/two", License: "Apache-2.0", Source: "https://example.test/two"},
		},
	}
}

// testVulnerabilityException is a complete, unexpired exception for id.
func testVulnerabilityException(id, scope string) vulnerabilityException {
	return vulnerabilityException{
		ID:          id,
		Owner:       "security@example.test",
		Expires:     "2099-01-01T00:00:00Z",
		Remediation: "https://example.test/security/" + id,
		Scope:       scope,
	}
}

func testRequirements() []moduleRequirement {
	return []moduleRequirement{
		{Path: "example.test/one", Version: "v1.2.3"},
		{Path: "example.test/two", Version: "v2.3.4"},
	}
}

// repositoryRoot and moduleRoot locate the checkout these tests verify.
var (
	repositoryRoot = repotest.RepositoryRoot
	moduleRoot     = repotest.ModuleRoot
)
