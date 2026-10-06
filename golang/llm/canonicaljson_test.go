package llm_test

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

func TestCanonicalJSONSortsKeysAndPreservesExactNumbers(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "nested objects", in: `{"b":2,"a":{"y":false,"x":true}}`, want: `{"a":{"x":true,"y":false},"b":2}`},
		{name: "large integer", in: `{"value":9007199254740993}`, want: `{"value":9007199254740993}`},
		{name: "number spelling", in: `{"value":1.2300e+04}`, want: `{"value":1.2300e+04}`},
		{name: "array order", in: `[3,{"b":2,"a":1},1]`, want: `[3,{"a":1,"b":2},1]`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := llm.CanonicalJSON([]byte(test.in))
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != test.want {
				t.Fatalf("CanonicalJSON(%s) = %s, want %s", test.in, got, test.want)
			}
			again, err := llm.CanonicalJSON(got)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, again) {
				t.Fatalf("canonical output is not idempotent: %s != %s", got, again)
			}
		})
	}
}

func TestCanonicalJSONRejectsDuplicatesTrailingDepthAndSize(t *testing.T) {
	for _, input := range []string{
		`{"a":1,"a":2}`,
		`{"outer":{"a":1,"a":2}}`,
		`{"a":1} {"b":2}`,
	} {
		if _, err := llm.CanonicalJSON([]byte(input)); err == nil {
			t.Errorf("accepted invalid canonical JSON %s", input)
		}
	}

	deep := "0"
	for index := 0; index <= llm.DefaultCanonicalMaxDepth; index++ {
		deep = "[" + deep + "]"
	}
	if _, err := llm.CanonicalJSON([]byte(deep)); err == nil {
		t.Fatal("accepted JSON beyond the canonical depth limit")
	}

	oversized := []byte(fmt.Sprintf(`{"value":"%s"}`, strings.Repeat("x", llm.DefaultCanonicalMaxBytes)))
	if _, err := llm.CanonicalJSON(oversized); err == nil {
		t.Fatal("accepted JSON beyond the canonical size limit")
	}
}

func TestCanonicalJSONWithLimits(t *testing.T) {
	if _, err := llm.CanonicalJSONWithLimits([]byte(`[[]]`), 16, 1); err == nil {
		t.Fatal("accepted input beyond a caller-supplied depth limit")
	}
	if _, err := llm.CanonicalJSONWithLimits([]byte(`{"a":1}`), 3, llm.DefaultCanonicalMaxDepth); err == nil {
		t.Fatal("accepted input beyond a caller-supplied size limit")
	}
	if _, err := llm.CanonicalJSONWithLimits([]byte(`{"a":1}`), 0, llm.DefaultCanonicalMaxDepth); err == nil {
		t.Fatal("accepted zero byte limit")
	}
}
