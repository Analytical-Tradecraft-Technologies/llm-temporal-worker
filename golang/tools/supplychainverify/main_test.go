package main

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestVerifyAcceptsReviewedInventoryAndApprovedFinding(t *testing.T) {
	t.Parallel()

	baseline := testBaseline()
	baseline.VulnerabilityExceptions = []vulnerabilityException{{
		ID:          "GO-2099-0001",
		Owner:       "security@example.test",
		Expires:     "2099-01-01T00:00:00Z",
		Remediation: "https://example.test/security/GO-2099-0001",
		Scope:       "module_only",
	}}

	report, err := verify(
		baseline,
		testRequirements(),
		strings.NewReader(`{"finding":{"osv":"GO-2099-0001","trace":[{"module":"example.test/module","version":"v1.2.3"}]}}`),
		time.Date(2026, time.July, 14, 0, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
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

	now := time.Date(2026, time.July, 14, 0, 0, 0, 0, time.UTC)
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
			baseline: func() baseline {
				value := testBaseline()
				value.VulnerabilityExceptions = []vulnerabilityException{{
					ID:          "GO-2099-0001",
					Owner:       "security@example.test",
					Expires:     "2026-07-13T00:00:00Z",
					Remediation: "https://example.test/security/GO-2099-0001",
					Scope:       "module_only",
				}}
				return value
			}(),
			want: "expired",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := verify(test.baseline, testRequirements(), strings.NewReader(`{"finding":{"osv":"GO-2099-0001"}}`), now)
			if err == nil {
				t.Fatal("verify accepted an unreviewed or expired finding")
			}
			if !strings.Contains(err.Error(), test.want) {
				t.Fatalf("verify error %q does not contain %q", err, test.want)
			}
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
			name: "missing exception owner",
			baseline: func() baseline {
				value := testBaseline()
				value.VulnerabilityExceptions = []vulnerabilityException{{
					ID:          "GO-2099-0001",
					Expires:     "2099-01-01T00:00:00Z",
					Remediation: "https://example.test/security/GO-2099-0001",
					Scope:       "module_only",
				}}
				return value
			}(),
			requirements: testRequirements(),
			want:         "owner",
		},
		{
			name: "missing exception scope",
			baseline: func() baseline {
				value := testBaseline()
				value.VulnerabilityExceptions = []vulnerabilityException{{
					ID:          "GO-2099-0001",
					Owner:       "security@example.test",
					Expires:     "2099-01-01T00:00:00Z",
					Remediation: "https://example.test/security/GO-2099-0001",
				}}
				return value
			}(),
			requirements: testRequirements(),
			want:         "scope",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := verify(test.baseline, test.requirements, strings.NewReader(""), time.Date(2026, time.July, 14, 0, 0, 0, 0, time.UTC))
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

	baseline := testBaseline()
	baseline.VulnerabilityExceptions = []vulnerabilityException{{
		ID:          "GO-2099-0001",
		Owner:       "security@example.test",
		Expires:     "2099-01-01T00:00:00Z",
		Remediation: "https://example.test/security/GO-2099-0001",
		Scope:       "reachable",
	}}
	report, err := verify(
		baseline,
		testRequirements(),
		strings.NewReader(`{"finding":{"osv":"GO-2099-0001","trace":[{"frame":{"position":{"filename":"raw-provider-response.txt"}}}]}}`),
		time.Date(2026, time.July, 14, 0, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}

	var output bytes.Buffer
	if err := writeReport(&output, report); err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"raw-provider-response.txt", "trace", "filename"} {
		if strings.Contains(output.String(), forbidden) {
			t.Fatalf("redacted report contains scanner detail %q: %s", forbidden, output.String())
		}
	}
}

func TestReadGoModExcludesIndirectRequirements(t *testing.T) {
	t.Parallel()

	requirements, err := readGoMod(strings.NewReader(`{"Require":[{"Path":"example.test/direct","Version":"v1.2.3"},{"Path":"example.test/indirect","Version":"v1.2.3","Indirect":true}]}`))
	if err != nil {
		t.Fatal(err)
	}
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
	if err != nil {
		t.Fatal(err)
	}
	want := []moduleRequirement{{Path: "golang.org/x/vuln", Version: "v1.7.0"}}
	if !reflect.DeepEqual(requirements, want) {
		t.Fatalf("requirements = %#v, want %#v", requirements, want)
	}
}

func TestReadGoModRejectsToolWithoutVersionedModule(t *testing.T) {
	t.Parallel()

	_, err := readGoMod(strings.NewReader(`{"Tool":[{"Path":"example.test/tool/cmd/tool"}]}`))
	if err == nil || !strings.Contains(err.Error(), "no versioned module requirement") {
		t.Fatalf("readGoMod error = %v, want missing versioned module requirement", err)
	}
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
	if err == nil {
		t.Fatal("readGoMod accepted a replacement")
	}
	if !strings.Contains(err.Error(), "replacement") {
		t.Fatalf("readGoMod error %q does not identify the replacement", err)
	}
}

func TestVerifyRejectsReachableTraceOutsideModuleOnlyException(t *testing.T) {
	t.Parallel()

	baseline := testBaseline()
	baseline.VulnerabilityExceptions = []vulnerabilityException{{
		ID:          "GO-2099-0001",
		Owner:       "security@example.test",
		Expires:     "2099-01-01T00:00:00Z",
		Remediation: "https://example.test/security/GO-2099-0001",
		Scope:       "module_only",
	}}

	_, err := verify(
		baseline,
		testRequirements(),
		strings.NewReader(`{"finding":{"osv":"GO-2099-0001","trace":[{"module":"example.test/module","version":"v1.2.3"}]}}
{"finding":{"osv":"GO-2099-0001","trace":[{"module":"example.test/module","version":"v1.2.3"},{"package":"example.test/module/vulnerable","function":"Parse"}]}}`),
		time.Date(2026, time.July, 14, 0, 0, 0, 0, time.UTC),
	)
	if err == nil {
		t.Fatal("verify accepted a reachable trace outside the module-only exception scope")
	}
	if !strings.Contains(err.Error(), "reachable trace") {
		t.Fatalf("verify error %q does not identify the reachable trace", err)
	}
}

func TestPullRequestVerificationOnlyRejectsIntroducedVulnerabilities(t *testing.T) {
	t.Parallel()

	policy := testBaseline()
	now := time.Date(2026, time.July, 14, 0, 0, 0, 0, time.UTC)
	base := `{"finding":{"osv":"GO-2099-0001","trace":[{"module":"example.test/one","version":"v1.2.3"}]}}`

	report, err := verifyPullRequest(
		policy,
		testRequirements(),
		strings.NewReader(base),
		strings.NewReader(base),
		now,
	)
	if err != nil {
		t.Fatalf("unchanged finding failed PR verification: %v", err)
	}
	if len(report.IntroducedFindings) != 0 {
		t.Fatalf("introduced findings = %#v, want none", report.IntroducedFindings)
	}

	_, err = verifyPullRequest(
		policy,
		testRequirements(),
		strings.NewReader(base),
		strings.NewReader(base+"\n"+`{"finding":{"osv":"GO-2099-0002"}}`),
		now,
	)
	if err == nil || !strings.Contains(err.Error(), "GO-2099-0002") {
		t.Fatalf("new finding error = %v, want GO-2099-0002", err)
	}
}

func TestPullRequestVerificationRejectsReachabilityEscalation(t *testing.T) {
	t.Parallel()

	policy := testBaseline()
	policy.VulnerabilityExceptions = []vulnerabilityException{{
		ID:          "GO-2099-0001",
		Owner:       "security@example.test",
		Expires:     "2099-01-01T00:00:00Z",
		Remediation: "https://example.test/security/GO-2099-0001",
		Scope:       "module_only",
	}}
	base := `{"finding":{"osv":"GO-2099-0001","trace":[{"module":"example.test/one","version":"v1.2.3"}]}}`
	head := base + "\n" + `{"finding":{"osv":"GO-2099-0001","trace":[{"module":"example.test/one","version":"v1.2.3"},{"package":"example.test/one/vulnerable"}]}}`
	_, err := verifyPullRequest(
		policy,
		testRequirements(),
		strings.NewReader(base),
		strings.NewReader(head),
		time.Date(2026, time.July, 14, 0, 0, 0, 0, time.UTC),
	)
	if err == nil || !strings.Contains(err.Error(), "reachable") {
		t.Fatalf("reachability escalation error = %v, want reachable", err)
	}
}

func TestTrustedModuleRootsAreVersionIndependentAndBoundaryAware(t *testing.T) {
	t.Parallel()

	roots := []moduleRecord{{Path: "example.test/publisher", License: "MIT", Source: "https://example.test/publisher"}}
	if err := validateInventory(roots, []moduleRequirement{{Path: "example.test/publisher/module", Version: "v99.0.0"}}); err != nil {
		t.Fatalf("approved-root version bump failed: %v", err)
	}
	for _, path := range []string{"example.test/unreviewed", "example.test/publisher-typosquat/module"} {
		if err := validateInventory(roots, []moduleRequirement{{Path: path, Version: "v1.0.0"}}); err == nil {
			t.Fatalf("unapproved module %q passed trusted-root validation", path)
		}
	}
}

func TestMakefileAndWorkflowsComposeBoundedSecurityVerification(t *testing.T) {
	t.Parallel()

	root := repositoryRoot(t)
	makefile, err := os.ReadFile(filepath.Join(moduleRoot(t), "Makefile"))
	if err != nil {
		t.Fatal(err)
	}
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
		if !strings.Contains(string(makefile), expected) {
			t.Fatalf("Makefile does not contain %q", expected)
		}
	}

	pullRequest, err := os.ReadFile(filepath.Join(root, ".github", "workflows", "pull-request.yml"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(pullRequest), "make security-pr-verify") {
		t.Fatal("PR workflow does not run security-pr-verify")
	}

	master, err := os.ReadFile(filepath.Join(root, ".github", "workflows", "master.yml"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(master), "security-verify") {
		t.Fatal("master workflow still runs security verification")
	}
	scheduled, err := os.ReadFile(filepath.Join(root, ".github", "workflows", "security-scheduled.yml"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(scheduled), "make security-verify") {
		t.Fatal("scheduled workflow does not run security-verify")
	}
}

func TestReadFindingsMergesDuplicatesAndRejectsMalformedStreams(t *testing.T) {
	t.Parallel()

	findings, err := readFindings(strings.NewReader(`{"event":"progress"}
{"finding":{"osv":" GO-2099-0002 ","trace":[]}}
{"finding":{"osv":"GO-2099-0001","trace":[{"module":"example.test/one","version":"v1.2.3"}]}}
{"finding":{"osv":"GO-2099-0001","trace":[{"module":"example.test/one","version":"v1.2.3"},{"package":"example.test/one/vulnerable"}]}}`))
	if err != nil {
		t.Fatal(err)
	}
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
		{name: "module only", trace: []json.RawMessage{json.RawMessage(`{"module":"example.test/one"}`)}, reachable: true},
		{name: "complete module frame", trace: []json.RawMessage{json.RawMessage(`{"module":"example.test/one","version":"v1.2.3"}`)}},
		{name: "blank module frame", trace: []json.RawMessage{json.RawMessage(`{"module":"","version":"v1.2.3"}`)}, reachable: true},
		{name: "extra frame fields", trace: []json.RawMessage{json.RawMessage(`{"module":"example.test/one","version":"v1.2.3","package":"vulnerable"}`)}, reachable: true},
		{name: "multiple frames", trace: []json.RawMessage{json.RawMessage(`{"module":"example.test/one","version":"v1.2.3"}`), json.RawMessage(`{"package":"vulnerable"}`)}, reachable: true},
		{name: "malformed frame", trace: []json.RawMessage{json.RawMessage(`"not-an-object"`)}, wantErr: true},
		{name: "malformed module", trace: []json.RawMessage{json.RawMessage(`{"module":123,"version":"v1.2.3"}`)}, wantErr: true},
		{name: "malformed version", trace: []json.RawMessage{json.RawMessage(`{"module":"example.test/one","version":123}`)}, wantErr: true},
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

	validException := func() vulnerabilityException {
		return vulnerabilityException{
			ID: "GO-2099-0001", Owner: "security@example.test", Expires: "2099-01-01T00:00:00Z",
			Remediation: "https://example.test/security/GO-2099-0001", Scope: "module_only",
		}
	}
	now := time.Date(2026, time.July, 14, 0, 0, 0, 0, time.UTC)
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
		{name: "invalid exception id", mutate: func(value *baseline) {
			exception := validException()
			exception.ID = "CVE-2099-0001"
			value.VulnerabilityExceptions = []vulnerabilityException{exception}
		}, want: "requires an id"},
		{name: "duplicate exception", mutate: func(value *baseline) {
			exception := validException()
			value.VulnerabilityExceptions = []vulnerabilityException{exception, exception}
		}, want: "repeats vulnerability exception"},
		{name: "expired exception", mutate: func(value *baseline) {
			exception := validException()
			exception.Expires = "2026-07-13T00:00:00Z"
			value.VulnerabilityExceptions = []vulnerabilityException{exception}
		}, want: "expired"},
		{name: "invalid remediation", mutate: func(value *baseline) {
			exception := validException()
			exception.Remediation = "http://example.test/remediation"
			value.VulnerabilityExceptions = []vulnerabilityException{exception}
		}, want: "invalid remediation"},
		{name: "invalid scope", mutate: func(value *baseline) {
			exception := validException()
			exception.Scope = "package_only"
			value.VulnerabilityExceptions = []vulnerabilityException{exception}
		}, want: "invalid scope"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			value := testBaseline()
			test.mutate(&value)
			if _, err := validateBaseline(value, now); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validateBaseline error = %v, want %q", err, test.want)
			}
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
			if err := validateInventory(testBaseline().TrustedModuleRoots, test.requirements); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validateInventory error = %v, want %q", err, test.want)
			}
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
			status := componentStatus{Test: "pass", Source: "pass", GoMod: "pass", Vulnerability: "pass"}
			test.edit(&status)
			if err := validateComponentStatus(status); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validateComponentStatus error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestWriteReportFileCreatesPrivateNestedReport(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "nested", "security-verify.json")
	if err := writeReportFile(path, verificationReport{Version: baselineVersion, Status: "fail", Findings: []string{}, ApprovedFindings: []string{}}); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if permission := info.Mode().Perm(); permission != 0o600 {
		t.Fatalf("report permissions = %o, want 600", permission)
	}
	var report verificationReport
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(contents, &report); err != nil {
		t.Fatal(err)
	}
	if report.Status != "fail" || report.Version != baselineVersion {
		t.Fatalf("report = %#v", report)
	}
}

func TestCommandSmokeWritesRedactedReport(t *testing.T) {
	baseline := testBaseline()
	baseline.VulnerabilityExceptions = []vulnerabilityException{{
		ID: "GO-2099-0001", Owner: "security@example.test", Expires: "2099-01-01T00:00:00Z",
		Remediation: "https://example.test/security/GO-2099-0001", Scope: "reachable",
	}}
	root := t.TempDir()
	write := func(name string, contents []byte) string {
		t.Helper()
		path := filepath.Join(root, name)
		if err := os.WriteFile(path, contents, 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	baselineJSON, err := json.Marshal(baseline)
	if err != nil {
		t.Fatal(err)
	}
	goModJSON, err := json.Marshal(goMod{Require: []moduleRequirement{{Path: "example.test/one", Version: "v1.2.3"}, {Path: "example.test/two", Version: "v2.3.4"}}})
	if err != nil {
		t.Fatal(err)
	}
	baselinePath := write("baseline.json", baselineJSON)
	goModPath := write("go-mod.json", goModJSON)
	vulnerabilityPath := write("vulnerabilities.json", []byte(`{"finding":{"osv":"GO-2099-0001","trace":[{"frame":{"position":{"filename":"/private/raw-scanner-trace.txt"}}}]}}`))
	reportPath := filepath.Join(root, "reports", "security-verify.json")

	moduleDirectory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	binaryPath := filepath.Join(root, "supplychainverify")
	build := exec.Command("go", "build", "-o", binaryPath, ".")
	build.Dir = moduleDirectory
	build.Env = append(os.Environ(), "GOCACHE="+filepath.Join(root, "go-cache"))
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build failed: %v\n%s", err, output)
	}

	command := exec.Command(binaryPath, "-baseline", baselinePath, "-go-mod", goModPath, "-vulnerability-output", vulnerabilityPath, "-report", reportPath)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("supplychainverify failed: %v\n%s", err, output)
	}
	if !strings.Contains(string(output), "supply chain verification passed") {
		t.Fatalf("command output = %s", output)
	}
	report, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(report), "raw-scanner-trace.txt") || strings.Contains(string(report), "trace") {
		t.Fatalf("report retained scanner detail: %s", report)
	}
	var value verificationReport
	if err := json.Unmarshal(report, &value); err != nil {
		t.Fatal(err)
	}
	if value.Status != "pass" || !reflect.DeepEqual(value.ApprovedFindings, []string{"GO-2099-0001"}) {
		t.Fatalf("report = %#v", value)
	}
}

func TestExecuteValidatesInputsAndComponentStatuses(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	write := func(name, contents string) string {
		t.Helper()
		path := filepath.Join(root, name)
		if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	baselinePath := write("baseline.json", `{"version":2,"allowed_licenses":["MIT"],"trusted_module_roots":[{"path":"example.test/one","license":"MIT","source":"https://example.test/one"}],"vulnerability_exceptions":[]}`)
	goModPath := write("go-mod.json", `{"Require":[{"Path":"example.test/one","Version":"v1.2.3"}]}`)
	vulnerabilityPath := write("vulnerabilities.json", "")
	components := componentStatus{Test: "pass", Source: "pass", GoMod: "pass", Vulnerability: "pass"}
	now := time.Date(2026, time.July, 14, 0, 0, 0, 0, time.UTC)

	report, err := execute(baselinePath, goModPath, vulnerabilityPath, components, now)
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
			report, err := execute(test.baseline, test.goMod, test.vulnerable, test.components, now)
			if err == nil || report.Status != "fail" || !strings.Contains(err.Error(), test.want) || report.Error != err.Error() {
				t.Fatalf("execute error = %#v, %v; want %q", report, err, test.want)
			}
		})
	}
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

func testRequirements() []moduleRequirement {
	return []moduleRequirement{
		{Path: "example.test/one", Version: "v1.2.3"},
		{Path: "example.test/two", Version: "v2.3.4"},
	}
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	directory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(directory, ".git")); err == nil {
			return directory
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			t.Fatal("repository root with .git not found")
		}
		directory = parent
	}
}

func moduleRoot(t *testing.T) string {
	t.Helper()
	return filepath.Join(repositoryRoot(t), "golang")
}
