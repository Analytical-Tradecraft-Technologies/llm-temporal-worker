package schema_test

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/schema"
)

func joined(count int, element func(int) string) string {
	parts := make([]string, count)
	for i := range parts {
		parts[i] = element(i)
	}
	return strings.Join(parts, ",")
}

// enum, const, required and pattern cost more than one application each, so
// a schema and instance inside every size limit could pin a CPU for minutes
// after payment. Validation refuses them quickly instead (#1660).
func TestValidateBoundsKeywordWork(t *testing.T) {
	longString := `"` + strings.Repeat("a", 200_000) + `"`
	for name, test := range map[string]struct{ schema, instance string }{
		"integer enum under items": {
			schema:   `{"type":"array","items":{"enum":[` + joined(8000, func(i int) string { return fmt.Sprint(100_000 + i) }) + `]}}`,
			instance: `[` + joined(8000, func(int) string { return "1" }) + `]`,
		},
		"const array under items": {
			schema:   `{"type":"array","items":{"const":[` + joined(8000, func(int) string { return "1" }) + `]}}`,
			instance: `[` + joined(8000, func(int) string { return "[1]" }) + `]`,
		},
		"required under items": {
			schema:   `{"type":"array","items":{"required":[` + joined(20_000, func(i int) string { return fmt.Sprintf(`"k%d"`, i) }) + `]}}`,
			instance: `[` + joined(26_000, func(int) string { return "{}" }) + `]`,
		},
		"pattern over a long string": {
			schema:   `{"pattern":"[a-z]{1000}b"}`,
			instance: longString,
		},
		"propertyNames pattern": {
			schema:   `{"propertyNames":{"pattern":"[a-z]{1000}b"}}`,
			instance: `{` + longString + `:1}`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			compiled, err := schema.Parse([]byte(test.schema))
			if err != nil {
				t.Fatalf("Parse = %v", err)
			}
			started := time.Now()
			err = compiled.Validate([]byte(test.instance))
			var validation *schema.ValidationError
			if !errors.As(err, &validation) || !strings.Contains(validation.Message, "units of validation work") {
				t.Fatalf("Validate = %v, want the work bound", err)
			}
			if elapsed := time.Since(started); elapsed > time.Second {
				t.Fatalf("refusal took %v", elapsed)
			}
		})
	}
}

// Ordinary keyword sizes still validate, including a large string enum on
// every element of a long array and a pattern over many strings.
func TestValidateAcceptsOrdinaryKeywordWork(t *testing.T) {
	codes := joined(250, func(i int) string { return fmt.Sprintf(`"C%03d"`, i) })
	for name, test := range map[string]struct{ schema, instance string }{
		"string enum per element": {
			schema:   `{"type":"array","items":{"type":"object","required":["country","n"],"properties":{"country":{"enum":[` + codes + `]},"n":{"enum":[1,2,3,4,5]}}}}`,
			instance: `[` + joined(5000, func(i int) string { return fmt.Sprintf(`{"country":"C%03d","n":%d}`, i%250, 1+i%5) }) + `]`,
		},
		"pattern per element": {
			schema:   `{"type":"array","items":{"type":"string","pattern":"^[A-Z]{2}-[0-9]{4}$"}}`,
			instance: `[` + joined(10_000, func(int) string { return `"AB-1234"` }) + `]`,
		},
		"pattern over a long string": {
			schema:   `{"type":"string","pattern":"^[a-z ]*$"}`,
			instance: `"` + strings.Repeat("words and ", 20_000) + `"`,
		},
		"conditional required": {
			schema:   `{"type":"array","items":{"if":{"properties":{"kind":{"const":"a"}}},"then":{"required":["a"]},"else":{"required":["b"]}}}`,
			instance: `[` + joined(5000, func(int) string { return `{"kind":"a","a":1}` }) + `]`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			compiled, err := schema.Parse([]byte(test.schema))
			if err != nil {
				t.Fatalf("Parse = %v", err)
			}
			if err := compiled.Validate([]byte(test.instance)); err != nil {
				t.Fatalf("Validate = %v", err)
			}
		})
	}
}

// One parsed schema meters concurrent validations without shared mutation.
func TestValidateMetersConcurrently(t *testing.T) {
	compiled, err := schema.Parse([]byte(`{"type":"array","items":{"$ref":"#/$defs/item"},"$defs":{"item":{"anyOf":[{"enum":[1,2,3]},{"type":"string","pattern":"^x+$"}]}}}`))
	if err != nil {
		t.Fatal(err)
	}
	instance := []byte(`[1,"xx",3,"xxx"]`)
	var group sync.WaitGroup
	for range 8 {
		group.Add(1)
		go func() {
			defer group.Done()
			if err := compiled.Validate(instance); err != nil {
				t.Error(err)
			}
		}()
	}
	group.Wait()
}
