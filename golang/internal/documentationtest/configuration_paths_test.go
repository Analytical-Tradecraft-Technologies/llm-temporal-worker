package documentationtest

import (
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
	"go.yaml.in/yaml/v4"
)

// TestConfigurationReferenceShowsEveryFieldInPlace requires every field of
// config.Config to appear as a key in a fenced YAML block of
// docs/reference/configuration.md under its own parent key, so a field named
// by a common word such as region or enabled is not "documented" by prose or
// by another block (#1673). A field of a type used in several places, such
// as a secret reference, needs one in-place example among them.
func TestConfigurationReferenceShowsEveryFieldInPlace(t *testing.T) {
	reference, err := os.ReadFile(filepath.Join("..", "..", "..", "docs", "reference", "configuration.md"))
	if err != nil {
		t.Fatal(err)
	}
	documented := documentedYAMLKeyChains(t, string(reference))
	type ownedField struct {
		owner reflect.Type
		name  string
	}
	shown := map[ownedField]bool{}
	first := map[ownedField]string{}
	for _, field := range configFieldPaths(reflect.TypeOf(config.Config{}), nil, map[reflect.Type]bool{}) {
		key := ownedField{owner: field.owner, name: field.path[len(field.path)-1]}
		if _, seen := first[key]; !seen {
			first[key] = strings.Join(field.path, ".")
		}
		shown[key] = shown[key] || keyChainDocumented(field.path, documented)
	}
	var missing []string
	for key, ok := range shown {
		if !ok && !undocumentedConfigFields[key.owner.Name()+"."+key.name] {
			missing = append(missing, first[key])
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Fatalf("docs/reference/configuration.md shows no YAML block with these fields under their parent: %s", strings.Join(missing, ", "))
	}
}

// undocumentedConfigFields names, as Type.field, fields with no valid value
// to show: validation rejects any non-empty AuthConfig path.
var undocumentedConfigFields = map[string]bool{
	"AuthConfig.path": true,
}

// A prose mention, or the key under another parent, does not document a
// field.
func TestConfigurationKeyChainsNeedTheParent(t *testing.T) {
	documented := documentedYAMLKeyChains(t, "The region is set by `region`.\n\n```yaml\nendpoints:\n  openai:\n    family: openai_responses\nbedrock:\n  region: us-east-1\n```\n")
	if keyChainDocumented([]string{"endpoints", "*", "region"}, documented) {
		t.Fatal("a region key under another parent documented endpoints.*.region")
	}
	if !keyChainDocumented([]string{"endpoints", "*", "family"}, documented) {
		t.Fatal("endpoints.*.family was not found under its parent")
	}
}

type configFieldPath struct {
	owner reflect.Type
	path  []string
}

// configFieldPaths lists the yaml key path of every field reachable from a
// type, with the struct type that declares it. A map's arbitrary keys are
// written as "*"; list elements share their list's path.
func configFieldPaths(kind reflect.Type, prefix []string, visiting map[reflect.Type]bool) []configFieldPath {
	for kind.Kind() == reflect.Pointer || kind.Kind() == reflect.Slice || kind.Kind() == reflect.Array {
		kind = kind.Elem()
	}
	if kind.Kind() == reflect.Map {
		return configFieldPaths(kind.Elem(), append(append([]string(nil), prefix...), "*"), visiting)
	}
	if kind.Kind() != reflect.Struct || visiting[kind] {
		return nil
	}
	visiting[kind] = true
	defer delete(visiting, kind)
	var paths []configFieldPath
	for index := 0; index < kind.NumField(); index++ {
		field := kind.Field(index)
		name, _, _ := strings.Cut(field.Tag.Get("yaml"), ",")
		if !field.IsExported() || name == "-" || name == "" {
			continue
		}
		path := append(append([]string(nil), prefix...), name)
		paths = append(paths, configFieldPath{owner: kind, path: path})
		paths = append(paths, configFieldPaths(field.Type, path, visiting)...)
	}
	return paths
}

// documentedYAMLKeyChains returns, for every mapping key in every fenced YAML
// block, the chain of keys from the block's root to it. A commented-out
// optional key such as "# mesh_transport: true" in the complete shape counts
// as shown in place, so such lines are uncommented before parsing; a block
// that then fails to parse is read as written.
func documentedYAMLKeyChains(t *testing.T, document string) [][]string {
	t.Helper()
	commented := regexp.MustCompile(`(?m)^(\s*)# ([A-Za-z0-9_]+:( .*)?)$`)
	var chains [][]string
	for _, block := range strings.Split(document, "```yaml\n")[1:] {
		end := strings.Index(block, "```")
		if end < 0 {
			t.Fatal("unterminated YAML block in the configuration reference")
		}
		var root yaml.Node
		if err := yaml.Unmarshal([]byte(commented.ReplaceAllString(block[:end], "$1$2")), &root); err != nil {
			root = yaml.Node{}
			if err := yaml.Unmarshal([]byte(block[:end]), &root); err != nil {
				continue
			}
		}
		var walk func(node *yaml.Node, prefix []string)
		walk = func(node *yaml.Node, prefix []string) {
			switch node.Kind {
			case yaml.DocumentNode, yaml.SequenceNode:
				for _, child := range node.Content {
					walk(child, prefix)
				}
			case yaml.MappingNode:
				for index := 0; index+1 < len(node.Content); index += 2 {
					chain := append(append([]string(nil), prefix...), node.Content[index].Value)
					chains = append(chains, chain)
					walk(node.Content[index+1], chain)
				}
			}
		}
		walk(&root, nil)
	}
	return chains
}

// keyChainDocumented reports whether a block shows path's key under its
// parent: some block's key chain must end with path's tail up to and
// including the nearest named ancestor, or with path itself for a top-level
// field. A block may start below the root, so only that tail must match; "*"
// (a map's own key) matches any key but never counts as the named ancestor.
func keyChainDocumented(path []string, chains [][]string) bool {
	need := len(path)
	for offset := 2; offset <= len(path); offset++ {
		if path[len(path)-offset] != "*" {
			need = offset
			break
		}
	}
	for _, chain := range chains {
		if len(chain) < need || (len(path) == 1 && len(chain) != 1) {
			continue
		}
		matched := true
		for offset := 1; offset <= need; offset++ {
			want, got := path[len(path)-offset], chain[len(chain)-offset]
			if want != "*" && want != got {
				matched = false
				break
			}
		}
		if matched {
			return true
		}
	}
	return false
}
