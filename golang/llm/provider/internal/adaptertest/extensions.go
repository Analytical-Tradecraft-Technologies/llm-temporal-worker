package adaptertest

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

// ExtensionFields is an extension allowlist: each namespace maps its fields
// to the wire field each one writes, an empty wire name meaning the field's
// own name.
type ExtensionFields map[string]map[string]string

// ExtensionCompiler compiles a request carrying extensions.
type ExtensionCompiler func(extensions map[string]json.RawMessage) (provider.Call, error)

// NewExtensionCompiler builds an adapter whose profile allows fields,
// validating the profile with NewProfile only when validate is set.
type NewExtensionCompiler func(t *testing.T, fields ExtensionFields, validate bool) ExtensionCompiler

// NewProfileRejectsCollidingExtensions checks that overlapping allowlist
// entries, which would let map iteration pick the provider value, are
// rejected with the same error every time across namespaces, within one
// namespace, and when an empty alias resolves to another field's wire name.
// validate validates the test profile allowing fields.
func NewProfileRejectsCollidingExtensions(t *testing.T, wireField string, validate func(ExtensionFields) error) {
	for name, fields := range map[string]ExtensionFields{
		"across namespaces":             {"alpha": {"value": wireField}, "beta": {"value": wireField}},
		"within namespace":              {"alpha": {"first": wireField, "second": wireField}},
		"empty alias":                   {"alpha": {wireField: "", "value": wireField}},
		"empty alias across namespaces": {"alpha": {wireField: ""}, "beta": {"value": wireField}},
	} {
		t.Run(name, func(t *testing.T) {
			var first string
			for range 50 {
				err := validate(fields)
				if err == nil || !strings.Contains(err.Error(), `both map to wire field "`+wireField+`"`) {
					t.Fatalf("NewProfile() = %v, want wire collision", err)
				}
				if first == "" {
					first = err.Error()
				} else if err.Error() != first {
					t.Fatalf("collision error changed between runs: %q then %q", first, err.Error())
				}
			}
		})
	}
}

// CompileRejectsCollidingExtensions checks that a profile that bypasses
// NewProfile still fails every compilation of a request that sets both
// colliding fields instead of choosing one at random.
func CompileRejectsCollidingExtensions(t *testing.T, wireField string, newCompiler NewExtensionCompiler) {
	compile := newCompiler(t, ExtensionFields{"alpha": {"value": wireField}, "beta": {"value": wireField}}, false)
	for range 200 {
		call, err := compile(map[string]json.RawMessage{"alpha": json.RawMessage(`{"value":"first"}`), "beta": json.RawMessage(`{"value":"second"}`)})
		if err == nil || !strings.Contains(err.Error(), `wire field "`+wireField+`" already set`) {
			t.Fatalf("Compile() = %v, want wire collision", err)
		}
		if call.SDKParams != nil {
			t.Fatal("rejected extension produced dispatch parameters")
		}
	}
}

// NonOverlappingExtensionsSerializeConsistently checks that extensions
// mapped to distinct wire fields both reach the wire and serialize
// identically on every compilation.
func NonOverlappingExtensionsSerializeConsistently(t *testing.T, firstField, secondField string, newCompiler NewExtensionCompiler) {
	compile := newCompiler(t, ExtensionFields{"alpha": {"value": firstField}, "beta": {"value": secondField}}, true)
	var first []byte
	for range 200 {
		call, err := compile(map[string]json.RawMessage{"alpha": json.RawMessage(`{"value":"first"}`), "beta": json.RawMessage(`{"value":"second"}`)})
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := json.Marshal(call.SDKParams)
		if err != nil {
			t.Fatal(err)
		}
		if first == nil {
			first = encoded
			var wire map[string]any
			if err := json.Unmarshal(encoded, &wire); err != nil {
				t.Fatal(err)
			}
			if wire[firstField] != "first" || wire[secondField] != "second" {
				t.Fatalf("wire = %#v", wire)
			}
		} else if string(encoded) != string(first) {
			t.Fatalf("serialization changed between compilations:\n%s\n%s", first, encoded)
		}
	}
}
