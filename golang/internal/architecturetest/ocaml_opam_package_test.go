package architecturetest

import (
	"regexp"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

// The opam package must say which version it is and which Go contract its
// codecs were verified against, and bound every repository dependency's
// major version, so a dependency release cannot change what a consumer
// installs (#1518).
func TestOCamlPackageIsVersionedAndBoundsItsDependencies(t *testing.T) {
	root := repositoryRoot(t)
	opam := readRepositoryFile(t, root, "ocaml", "llm_temporal_worker", "llm-temporal-ocaml.opam")
	if !regexp.MustCompile(`(?m)^version: "[0-9]+\.[0-9]+\.[0-9]+"$`).MatchString(opam) {
		t.Fatalf("llm-temporal-ocaml.opam has no release version field:\n%s", opam)
	}
	apiVersion := regexp.MustCompile(`(?m)^x-llm-temporal-api-version: "([^"]+)"$`).FindStringSubmatch(opam)
	if apiVersion == nil || apiVersion[1] != llm.APIVersion {
		t.Fatalf("llm-temporal-ocaml.opam x-llm-temporal-api-version = %v, want %q", apiVersion, llm.APIVersion)
	}
	codec := readRepositoryFile(t, root, "ocaml", "llm_temporal_worker", "lib", "llm_temporal_codec.ml")
	if !strings.Contains(codec, `let api_version = "`+llm.APIVersion+`"`) {
		t.Fatalf("OCaml codec api_version does not match %q", llm.APIVersion)
	}

	depends := regexp.MustCompile(`(?s)\ndepends: \[\n(.*?)\n\]`).FindStringSubmatch(opam)
	if depends == nil {
		t.Fatal("llm-temporal-ocaml.opam has no depends block")
	}
	dependency := regexp.MustCompile(`^\s*"([^"]+)"(.*)$`)
	for _, line := range strings.Split(depends[1], "\n") {
		match := dependency.FindStringSubmatch(line)
		if match == nil {
			t.Fatalf("unrecognized depends line %q", line)
		}
		switch match[1] {
		case "ocaml":
			// The compiler is chosen by the switch.
			continue
		case "temporal-sdk":
			// Pinned to one commit by pin-depends.
			continue
		}
		if !strings.Contains(match[2], `< "`) {
			t.Errorf("dependency %s has no upper version bound: %s", match[1], strings.TrimSpace(line))
		}
	}
}
