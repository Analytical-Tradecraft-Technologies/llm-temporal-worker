package schema_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/schema"
)

// Validation errors become provider safe messages, so they never carry the
// model's strings, property names or numbers, nor the schema's value lists
// (#1661).
func TestValidationErrorsAreContentFree(t *testing.T) {
	const secret = "SECRETXYZ"
	enum := make([]string, 2000)
	for i := range enum {
		enum[i] = fmt.Sprintf(`"member-%d"`, i)
	}
	for name, test := range map[string]struct {
		schema, instance, keyword, path string
		leaks                           []string
	}{
		"pattern": {
			schema: `{"properties":{"a":{"pattern":"^x"}}}`, instance: `{"a":"` + secret + `"}`,
			keyword: "pattern", path: "/a", leaks: []string{secret},
		},
		"additionalProperties false": {
			schema: `{"additionalProperties":false}`, instance: `{"` + secret + `":1}`,
			keyword: "additionalProperties", leaks: []string{secret},
		},
		"additionalProperties schema": {
			schema: `{"additionalProperties":{"type":"string"}}`, instance: `{"` + secret + `":{"x":1}}`,
			keyword: "type", path: "/*", leaks: []string{secret},
		},
		"propertyNames": {
			schema: `{"propertyNames":{"pattern":"^a"}}`, instance: `{"` + secret + `":1}`,
			keyword: "propertyNames", leaks: []string{secret},
		},
		"enum": {
			schema: `{"enum":[` + strings.Join(enum, ",") + `]}`, instance: `"` + secret + `"`,
			keyword: "enum", leaks: []string{secret, "member-1"},
		},
		"const": {
			schema: `{"const":"member-1"}`, instance: `"` + secret + `"`,
			keyword: "const", leaks: []string{secret, "member-1"},
		},
		"minimum": {
			schema: `{"items":{"minimum":5}}`, instance: `[1,987654321]`, keyword: "minimum", path: "/0",
			leaks: []string{"987654321"},
		},
		"multipleOf": {
			schema: `{"multipleOf":7}`, instance: `987654321`, keyword: "multipleOf",
			leaks: []string{"987654321"},
		},
		"required": {
			schema: `{"required":["member-1","member-2"]}`, instance: `{"` + secret + `":1}`,
			keyword: "required", leaks: []string{secret, "member-1"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			compiled, err := schema.Parse([]byte(test.schema))
			if err != nil {
				t.Fatalf("Parse = %v", err)
			}
			err = compiled.Validate([]byte(test.instance))
			var validation *schema.ValidationError
			if !errors.As(err, &validation) {
				t.Fatalf("Validate = %T %v, want ValidationError", err, err)
			}
			if validation.Keyword != test.keyword || validation.Path != test.path {
				t.Fatalf("validation error = %#v, want keyword %q at %q", validation, test.keyword, test.path)
			}
			if len(validation.Message) > 256 || validation.Message == "" {
				t.Fatalf("message %q is empty or unbounded", validation.Message)
			}
			for _, leak := range test.leaks {
				if strings.Contains(err.Error(), leak) {
					t.Fatalf("error %q contains %q", err, leak)
				}
			}
		})
	}
}

// Malformed instance JSON is reported without quoting it.
func TestInstanceJSONErrorsAreContentFree(t *testing.T) {
	compiled, err := schema.Parse([]byte(`{"type":"object"}`))
	if err != nil {
		t.Fatal(err)
	}
	for _, instance := range []string{
		`{"SECRETXYZ":1,"SECRETXYZ":2}`,
		`{"a":1} "SECRETXYZ"`,
		`{"SECRETXYZ":1e9999}`,
		`[{"SECRETXYZ":1e9999}]`,
	} {
		err := compiled.Validate([]byte(instance))
		if err == nil || strings.Contains(err.Error(), "SECRETXYZ") {
			t.Fatalf("Validate(%s) = %v, want a content-free error", instance, err)
		}
	}
}
