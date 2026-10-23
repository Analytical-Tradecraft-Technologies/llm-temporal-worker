package schema_test

import (
	"errors"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/schema"
)

// dagFanoutSchema chains levels of width-way allOf fan-out through $defs, so
// one value is checked width^levels times although the schema is small.
func dagFanoutSchema(width, levels int) string {
	return `{"$ref":"#/$defs/l0","$defs":{` + dagFanoutDefs(width, levels) + `}}`
}

func dagFanoutDefs(width, levels int) string {
	defs := make([]string, 0, levels)
	for level := 0; level < levels; level++ {
		refs := make([]string, width)
		for i := range refs {
			refs[i] = fmt.Sprintf(`{"$ref":"#/$defs/l%d"}`, level+1)
		}
		defs = append(defs, fmt.Sprintf(`"l%d":{"allOf":[%s]}`, level, strings.Join(refs, ",")))
	}
	defs = append(defs, fmt.Sprintf(`"l%d":{"type":"number"}`, levels))
	return strings.Join(defs, ",")
}

func TestParseRejectsExponentialFanout(t *testing.T) {
	for name, data := range map[string]string{
		"recursive allOf":         `{"properties":{"x":{"allOf":[{"$ref":"#"},{"$ref":"#"}]}}}`,
		"recursive via defs":      `{"$ref":"#/$defs/n","$defs":{"n":{"properties":{"x":{"anyOf":[{"$ref":"#/$defs/n"},{"$ref":"#/$defs/n"}]}}}}}`,
		"recursive items":         `{"items":{"oneOf":[{"$ref":"#"},{"$ref":"#"},{"$ref":"#"}]}}`,
		"DAG fan-out":             dagFanoutSchema(40, 3),
		"wide DAG":                dagFanoutSchema(1001, 1),
		"anchor recursion":        `{"$anchor":"top","additionalProperties":{"allOf":[{"$ref":"#top"},{"$ref":"#top"}]}}`,
		"in-place self ref":       `{"allOf":[{"$ref":"#"}]}`,
		"recursive tuple fan-out": `{"prefixItems":[{"allOf":[{"$ref":"#"},{"$ref":"#"}]}]}`,
		// An empty $id keeps the enclosing resource, so #/$defs/n is the
		// root's fanning definition, as the validator resolves it.
		"empty $id":           `{"properties":{"x":{"$id":"","$ref":"#/$defs/n","$defs":{"n":{}}}},"$defs":{"n":{"allOf":[{"$ref":"#"},{"$ref":"#"}]}}}`,
		"fan-out in a branch": `{"if":{"type":"object"},"then":{"properties":{"x":{"allOf":[{"$ref":"#"},{"$ref":"#"}]}}}}`,
	} {
		t.Run(name, func(t *testing.T) {
			started := time.Now()
			_, err := schema.Parse([]byte(data))
			var fanout *schema.FanoutError
			if !errors.As(err, &fanout) {
				t.Fatalf("Parse = %T %v, want FanoutError", err, err)
			}
			if elapsed := time.Since(started); elapsed > time.Second {
				t.Fatalf("Parse took %v", elapsed)
			}
		})
	}
}

func TestParseAcceptsOrdinarySchemas(t *testing.T) {
	properties := make([]string, 300)
	for i := range properties {
		properties[i] = fmt.Sprintf(`"p%d":{"anyOf":[{"type":"string"},{"type":"number"},{"type":"null"}]}`, i)
	}
	for name, data := range map[string]string{
		"wide object":        `{"type":"object","properties":{` + strings.Join(properties, ",") + `},"additionalProperties":false}`,
		"recursive tree":     `{"type":"object","properties":{"name":{"type":"string"},"children":{"type":"array","items":{"$ref":"#"}}}}`,
		"recursive via defs": `{"$ref":"#/$defs/node","$defs":{"node":{"type":"object","properties":{"next":{"anyOf":[{"$ref":"#/$defs/node"},{"type":"null"}]}}}}}`,
		"bounded DAG":        dagFanoutSchema(20, 2),
		"nested $id":         `{"properties":{"a":{"$id":"urn:example:a","$defs":{"x":{"type":"string"}},"$ref":"#/$defs/x"}}}`,
		"anchor":             `{"properties":{"a":{"$ref":"#leaf"}},"$defs":{"leaf":{"$anchor":"leaf","type":"string"}}}`,
		"conditional":        `{"if":{"properties":{"kind":{"const":"a"}}},"then":{"required":["a"]},"else":{"required":["b"]}}`,
		// An unused definition cannot reject the schema, and a reference may
		// name a boolean schema.
		"unused dangling definition": `{"type":"string","$defs":{"unused":{"$ref":"#/$defs/missing"},"also":{"$ref":"#nowhere"}}}`,
		"reference to boolean":       `{"$ref":"#/additionalProperties","additionalProperties":true}`,
		// The validator ignores then and else without if.
		"branches without if": `{"then":{"allOf":[{"$ref":"#"},{"$ref":"#"}]},"else":{"allOf":[{"$ref":"#"},{"$ref":"#"}]}}`,
		// Tuple positions and trailing items are disjoint array indices.
		"recursive tuple":       `{"type":"array","prefixItems":[{"$ref":"#"},{"$ref":"#"}]}`,
		"recursive tuple items": `{"type":"array","prefixItems":[{"$ref":"#"},{"type":"string"}],"items":{"$ref":"#"}}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := schema.Parse([]byte(data)); err != nil {
				t.Fatalf("Parse = %v", err)
			}
		})
	}
}

// A schema near the per-value bound still validates a bounded total amount of
// work: an instance with too many values is refused rather than run (#1521).
func TestValidateBoundsTotalWork(t *testing.T) {
	compiled, err := schema.Parse([]byte(`{"type":"array","items":{"$ref":"#/$defs/l0"},"$defs":{` + dagFanoutDefs(20, 2) + `}}`))
	if err != nil {
		t.Fatal(err)
	}
	small := "[" + strings.TrimSuffix(strings.Repeat(`1,`, 100), ",") + "]"
	if err := compiled.Validate([]byte(small)); err != nil {
		t.Fatalf("small instance: %v", err)
	}
	var validation *schema.ValidationError
	large := "[" + strings.TrimSuffix(strings.Repeat(`"s",`, schema.MaxValidationWork/800), ",") + "]"
	started := time.Now()
	err = compiled.Validate([]byte(large))
	if !errors.As(err, &validation) || !strings.Contains(validation.Message, "too many values") {
		t.Fatalf("Validate = %v, want the work bound", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("refusal took %v", elapsed)
	}
	simple, err := schema.Parse([]byte(`{"type":"array","items":{"type":"string"}}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := simple.Validate([]byte(large)); err != nil {
		t.Fatalf("a cheap schema refused the same instance: %v", err)
	}
}

// A long chain of in-place references is rejected in linear time and memory,
// not by copying every descendant's closure into each ancestor.
func TestParseRejectsALongReferenceChainCheaply(t *testing.T) {
	const links = 4000
	defs := make([]string, 0, links+1)
	for i := 0; i < links; i++ {
		defs = append(defs, fmt.Sprintf(`"d%d":{"$ref":"#/$defs/d%d"}`, i, i+1))
	}
	defs = append(defs, fmt.Sprintf(`"d%d":{"type":"string"}`, links))
	data := `{"$ref":"#/$defs/d0","$defs":{` + strings.Join(defs, ",") + `}}`
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	started := time.Now()
	_, err := schema.Parse([]byte(data))
	elapsed := time.Since(started)
	runtime.ReadMemStats(&after)
	var fanout *schema.FanoutError
	if !errors.As(err, &fanout) {
		t.Fatalf("Parse = %T %v, want FanoutError", err, err)
	}
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 64<<20 || elapsed > 2*time.Second {
		t.Fatalf("rejecting the chain allocated %d MiB in %v", allocated>>20, elapsed)
	}
}

// Fragments are percent-decoded before they are read as pointers or anchors,
// as the validator reads them.
func TestParseResolvesPercentEncodedReferences(t *testing.T) {
	for _, data := range []string{
		`{"$ref":"#fo%6F","$defs":{"x":{"$anchor":"foo","type":"string"}}}`,
		`{"$ref":"#%2F$defs%2Fx","$defs":{"x":{"type":"string"}}}`,
		`{"$ref":"#/%24defs/x","$defs":{"x":{"type":"string"}}}`,
	} {
		compiled, err := schema.Parse([]byte(data))
		if err != nil {
			t.Fatalf("Parse(%s) = %v", data, err)
		}
		if err := compiled.Validate([]byte(`"text"`)); err != nil {
			t.Fatalf("Validate against %s = %v", data, err)
		}
	}
}

// Many distinct child-location combinations fold into one merged location
// once maxFanoutBuckets is reached, instead of materializing them all.
func TestParseBoundsDistinctChildLocations(t *testing.T) {
	shared := make([]string, 500)
	for i := range shared {
		shared[i] = fmt.Sprintf(`"q%d":{}`, i)
	}
	root := make([]string, 500)
	for i := range root {
		root[i] = fmt.Sprintf(`"p%d":{"additionalProperties":{},"$ref":"#/$defs/s"}`, i)
	}
	data := `{"properties":{` + strings.Join(root, ",") + `},"$defs":{"s":{"properties":{` + strings.Join(shared, ",") + `}}}}`
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	started := time.Now()
	_, _ = schema.Parse([]byte(data))
	elapsed := time.Since(started)
	runtime.ReadMemStats(&after)
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 96<<20 || elapsed > 3*time.Second {
		t.Fatalf("analysing %d bytes allocated %d MiB in %v", len(data), allocated>>20, elapsed)
	}
}
