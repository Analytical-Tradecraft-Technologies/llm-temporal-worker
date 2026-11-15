package architecturetest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/repotest"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
)

const modulePath = "github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang"

type listedPackage struct {
	ImportPath string
	Imports    []string
}

func TestDomainImportBoundary(t *testing.T) {
	command := exec.Command("go", "list", "-deps", "-json", "./...")
	command.Dir = moduleRoot(t)
	output, err := command.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			t.Fatalf("go list failed: %v\n%s", err, exitErr.Stderr)
		}
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(output))
	var violations []string
	for {
		var packageInfo listedPackage
		err := decoder.Decode(&packageInfo)
		if err == io.EOF {
			break
		}
		testassert.NoError(t, err)
		if !protectedDomain(packageInfo.ImportPath) {
			continue
		}
		for _, imported := range packageInfo.Imports {
			if forbiddenDomainImport(imported) {
				violations = append(violations, fmt.Sprintf("%s imports %s", packageInfo.ImportPath, imported))
			}
		}
	}
	if len(violations) > 0 {
		t.Fatalf("forbidden domain dependency edges:\n%s", strings.Join(violations, "\n"))
	}
}

func protectedDomain(path string) bool {
	for _, prefix := range []string{
		modulePath + "/llm",
		modulePath + "/routing",
		modulePath + "/pricing",
		modulePath + "/budget",
		modulePath + "/admission",
		modulePath + "/state",
	} {
		if path == prefix || strings.HasPrefix(path, prefix+"/") {
			if strings.HasPrefix(path, modulePath+"/llm/provider") {
				return false
			}
			return true
		}
	}
	return false
}

func forbiddenDomainImport(imported string) bool {
	if strings.HasPrefix(imported, modulePath+"/config") || strings.HasPrefix(imported, modulePath+"/llm/provider") {
		return true
	}
	for _, prefix := range []string{
		"go.temporal.io/sdk",
		"github.com/openai/",
		"github.com/anthropics/",
		"github.com/aws/",
		"github.com/redis/",
		"github.com/mediocregopher/radix",
	} {
		if strings.HasPrefix(imported, prefix) {
			return true
		}
	}
	return false
}

// repositoryRoot and moduleRoot locate the checkout these tests verify, and
// readJSONObject decodes one of its JSON documents.
var (
	repositoryRoot = repotest.RepositoryRoot
	moduleRoot     = repotest.ModuleRoot
	readJSONObject = repotest.ReadJSONObject
	readText       = repotest.ReadText
	writeFile      = repotest.WriteFile

	assertContains     = repotest.AssertContains
	assertOmits        = repotest.AssertOmits
	assertContainsEach = repotest.AssertContainsEach
	assertOmitsEach    = repotest.AssertOmitsEach
)
