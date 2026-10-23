package schema_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/schema"
)

func TestValidateRejectsUnboundedNumbersWithoutPanicking(t *testing.T) {
	numeric := `{"type":"object","properties":{"amount":{"type":"number","minimum":0,"maximum":10,"exclusiveMinimum":-1,"exclusiveMaximum":11,"multipleOf":0.5}},"required":["amount"]}`
	unique := `{"type":"array","uniqueItems":true}`
	many := strings.Repeat("1,", 21)
	for _, test := range []struct {
		name, schema, instance string
	}{
		{name: "huge exponent", schema: numeric, instance: `{"amount":1e3000000}`},
		{name: "huge negative exponent", schema: numeric, instance: `{"amount":1e-3000000}`},
		{name: "exponent with sign and leading zeros", schema: numeric, instance: `{"amount":-2.5E+0000001001}`},
		{name: "exponent accepted by math/big", schema: numeric, instance: `{"amount":1e999999}`},
		{name: "too many digits", schema: numeric, instance: `{"amount":` + strings.Repeat("9", schema.MaxNumberDigits+1) + `}`},
		{name: "too many fraction digits", schema: numeric, instance: `{"amount":0.` + strings.Repeat("1", schema.MaxNumberDigits) + `}`},
		{name: "uniqueItems hash", schema: unique, instance: `[` + many + `1e3000000]`},
		{name: "const", schema: `{"const":1}`, instance: `1e3000000`},
	} {
		t.Run(test.name, func(t *testing.T) {
			compiled, err := schema.Parse([]byte(test.schema))
			if err != nil {
				t.Fatal(err)
			}
			var bound *schema.NumberBoundError
			if err := compiled.Validate([]byte(test.instance)); !errors.As(err, &bound) {
				t.Fatalf("Validate = %T %v, want NumberBoundError", err, err)
			}
		})
	}
}

func TestValidateAcceptsNumbersAtTheBounds(t *testing.T) {
	compiled, err := schema.Parse([]byte(`{"type":"number"}`))
	if err != nil {
		t.Fatal(err)
	}
	for _, instance := range []string{
		`1e1000`, `1E+1000`, `-1.5e-1000`, `0`, `-0.0`, `12.5`,
		strings.Repeat("9", schema.MaxNumberDigits),
		`0.` + strings.Repeat("1", schema.MaxNumberDigits-1),
	} {
		if err := compiled.Validate([]byte(instance)); err != nil {
			t.Fatalf("Validate(%.40s) = %v", instance, err)
		}
	}
	bounded, err := schema.Parse([]byte(`{"type":"number","minimum":-1e1000,"maximum":1e1000}`))
	if err != nil {
		t.Fatal(err)
	}
	var validationErr *schema.ValidationError
	if err := bounded.Validate([]byte(`2e1000`)); !errors.As(err, &validationErr) || validationErr.Keyword != "maximum" {
		t.Fatalf("Validate(2e1000) = %v, want maximum violation", err)
	}
}

func TestParseRejectsUnboundedNumericKeywords(t *testing.T) {
	for _, data := range []string{
		`{"type":"number","minimum":1e3000000}`,
		`{"type":"number","maximum":-1e3000000}`,
		`{"type":"number","exclusiveMinimum":1e1001}`,
		`{"type":"number","exclusiveMaximum":1e-1001}`,
		`{"type":"number","multipleOf":1e3000000}`,
		`{"type":"string","maxLength":1e3000000}`,
		`{"enum":[1,1e3000000]}`,
		`{"properties":{"x":{"const":1e3000000}}}`,
	} {
		var bound *schema.NumberBoundError
		if _, err := schema.Parse([]byte(data)); !errors.As(err, &bound) {
			t.Fatalf("Parse(%s) = %T %v, want NumberBoundError", data, err, err)
		}
	}
}
