package llm_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
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
			testassert.NoError(t, err)
			if string(got) != test.want {
				t.Fatalf("CanonicalJSON(%s) = %s, want %s", test.in, got, test.want)
			}
			again, err := llm.CanonicalJSON(got)
			testassert.NoError(t, err)
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

func TestCanonicalToolArgumentsHoldsTheToolArgumentDepth(t *testing.T) {
	nested := func(depth int) []byte {
		return []byte(strings.Repeat(`{"a":`, depth-1) + `{}` + strings.Repeat(`}`, depth-1))
	}
	testassert.NoError(t, testassert.Err(llm.CanonicalToolArguments(nested(llm.MaxToolArgumentsDepth))), "arguments at the depth limit rejected")
	testassert.ErrorIs(t, testassert.Err(llm.CanonicalToolArguments(nested(llm.MaxToolArgumentsDepth+1))), llm.ErrCanonicalJSONLimit, "arguments over the depth limit")
	// The general canonicalizer still accepts the deeper value; only tool
	// arguments leave room for the durable records that wrap them.
	testassert.NoError(t, testassert.Err(llm.CanonicalJSON(nested(llm.MaxToolArgumentsDepth+1))), "canonical JSON rejected a value within its own limit")
}

// TestCanonicalJSONStringsMatchJSONMarshal pins the unescaped-ASCII fast path
// (#1112) to json.Marshal: every byte value, in a string value and in an
// object key, canonicalizes to exactly the encoding json.Marshal gives the
// decoded string.
func TestCanonicalJSONStringsMatchJSONMarshal(t *testing.T) {
	values := []string{"", "plain ascii ~\x7f", "é", "日本", "  ", "a\xffb"}
	for b := 0; b < 256; b++ {
		values = append(values, "a"+string([]byte{byte(b)})+"z")
	}
	for _, value := range values {
		input := mustMarshal(t, map[string]string{value: value})
		var decoded map[string]string
		testassert.NoError(t, json.Unmarshal(input, &decoded))
		want := mustMarshal(t, decoded)
		got, err := llm.CanonicalJSON(input)
		testassert.NoError(t, err)
		if !bytes.Equal(got, want) {
			t.Fatalf("CanonicalJSON(%s) = %s, want %s", input, got, want)
		}
	}
}

func BenchmarkCanonicalJSONLargeObject(b *testing.B) {
	items := make([]map[string]any, 0, 1000)
	for i := 0; i < cap(items); i++ {
		items = append(items, map[string]any{"type": "message", "role": "user", "index": i, "text": strings.Repeat(fmt.Sprintf("item %d ", i), 128)})
	}
	input, err := json.Marshal(map[string]any{"items": items, "status": "running"})
	testassert.NoError(b, err)
	b.SetBytes(int64(len(input)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		testassert.NoError(b, testassert.Err(llm.CanonicalJSON(input)))
	}
}
