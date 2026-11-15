// Command mutationverify verifies that focused semantic invariants kill a
// reviewed set of critical source mutations. It compiles each mutant through
// Go's overlay support, so the checked-out source is never modified.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

type mutation struct {
	name        string
	path        string
	from        string
	to          string
	packagePath string
	testName    string
}

var mutations = []mutation{
	{
		name:        "money-round-up",
		path:        "pricing/decimal.go",
		from:        "if remainder.Sign() != 0 {\n\t\tquotient.Add(quotient, big.NewInt(1))\n\t}\n\treturn quotient",
		to:          "if remainder.Sign() != 0 {\n\t\t// mutant omits the required ceiling increment\n\t}\n\treturn quotient",
		packagePath: "./pricing",
		testName:    "TestDecimalCeilingAndOverflowInvariants",
	},
	{
		name:        "window-comparison-boundary",
		path:        "budget/window.go",
		from:        "if active > window.Limit || amount > window.Limit-active {\n\t\treturn false, active, nil\n\t}",
		to:          "if active > window.Limit || amount >= window.Limit-active {\n\t\treturn false, active, nil\n\t}",
		packagePath: "./budget",
		testName:    "TestSlidingWindowBoundaryInvariants",
	},
	{
		name:        "dispatch-certainty",
		path:        "admission/transition.go",
		from:        "outcome.Certainty != NotDispatched && outcome.Certainty != Rejected && outcome.Certainty != Accepted && outcome.Certainty != Ambiguous",
		to:          "outcome.Certainty != NotDispatched && outcome.Certainty != Rejected && outcome.Certainty != Accepted",
		packagePath: "./admission",
		testName:    "TestTransitionAndDispatchInvariants",
	},
	{
		name:        "service-class-default",
		path:        "llm/service_class.go",
		from:        "return ServiceClassStandard, nil",
		to:          "return ServiceClassEconomy, nil",
		packagePath: "./llm",
		testName:    "TestServiceClassContractInvariants",
	},
	{
		name:        "state-transition",
		path:        "admission/transition.go",
		from:        "if to == StateDispatching || to == StateDefiniteFailed || to == StateCanceled || to == StateAmbiguous {",
		to:          "if to == StateDispatching || to == StateDefiniteFailed || to == StateAmbiguous {",
		packagePath: "./admission",
		testName:    "TestTransitionAndDispatchInvariants",
	},
}

type overlay struct {
	Replace map[string]string `json:"Replace"`
}

func main() {
	root := flag.String("root", ".", "repository root")
	goBinary := flag.String("go", "go", "Go command")
	flag.Parse()

	if err := verify(*root, *goBinary); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("mutation verification passed")
}

func verify(root, goBinary string) error {
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return fmt.Errorf("mutation verification cannot resolve repository root")
	}
	if info, err := os.Stat(absRoot); err != nil || !info.IsDir() {
		return fmt.Errorf("mutation verification repository root is not a directory")
	}
	for _, mutant := range mutations {
		if err := runInvariant(absRoot, goBinary, mutant); err != nil {
			return fmt.Errorf("mutation %s baseline invariant: %w", mutant.name, err)
		}
		if err := killMutant(absRoot, goBinary, mutant); err != nil {
			return err
		}
		fmt.Printf("killed mutation %s\n", mutant.name)
	}
	return nil
}

func runInvariant(root, goBinary string, mutant mutation) error {
	command := exec.Command(goBinary, "test", mutant.packagePath, "-run", "^"+mutant.testName+"$", "-count=1")
	command.Dir = root
	output, err := command.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s failed: %s", mutant.testName, trimOutput(output))
	}
	return nil
}

func killMutant(root, goBinary string, mutant mutation) error {
	sourcePath := filepath.Join(root, mutant.path)
	source, err := os.ReadFile(sourcePath)
	if err != nil {
		return fmt.Errorf("mutation %s cannot read %s", mutant.name, mutant.path)
	}
	mutated, err := replaceExactlyOnce(source, []byte(mutant.from), []byte(mutant.to))
	if err != nil {
		return fmt.Errorf("mutation %s source selection: %w", mutant.name, err)
	}

	workspace, err := os.MkdirTemp("", "llmtw-mutation-")
	if err != nil {
		return fmt.Errorf("mutation %s cannot create temporary overlay", mutant.name)
	}
	defer os.RemoveAll(workspace)

	mutatedPath := filepath.Join(workspace, filepath.Base(sourcePath))
	if err := os.WriteFile(mutatedPath, mutated, 0o600); err != nil {
		return fmt.Errorf("mutation %s cannot write overlay source", mutant.name)
	}
	overlayPath := filepath.Join(workspace, "overlay.json")
	data, err := json.Marshal(overlay{Replace: map[string]string{sourcePath: mutatedPath}})
	if err != nil {
		return fmt.Errorf("mutation %s cannot encode overlay", mutant.name)
	}
	if err := os.WriteFile(overlayPath, data, 0o600); err != nil {
		return fmt.Errorf("mutation %s cannot write overlay", mutant.name)
	}

	command := exec.Command(goBinary, "test", "-overlay", overlayPath, mutant.packagePath, "-run", "^"+mutant.testName+"$", "-count=1")
	command.Dir = root
	output, err := command.CombinedOutput()
	if err == nil {
		return fmt.Errorf("mutation %s survived invariant %s", mutant.name, mutant.testName)
	}
	if !strings.Contains(string(output), "--- FAIL: "+mutant.testName) {
		return fmt.Errorf("mutation %s did not fail through invariant %s: %s", mutant.name, mutant.testName, trimOutput(output))
	}
	return nil
}

func replaceExactlyOnce(source, from, to []byte) ([]byte, error) {
	if len(from) == 0 {
		return nil, fmt.Errorf("mutation source cannot be empty")
	}
	if count := bytes.Count(source, from); count != 1 {
		return nil, fmt.Errorf("mutation source matched %d times, want 1", count)
	}
	return bytes.Replace(source, from, to, 1), nil
}

func trimOutput(output []byte) string {
	const max = 2 << 10
	value := strings.TrimSpace(string(output))
	if len(value) <= max {
		return value
	}
	return value[:max] + "..."
}
