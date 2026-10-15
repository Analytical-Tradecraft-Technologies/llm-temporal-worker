package llm

import (
	"strings"
	"testing"
)

// TestCompactSizeHint pins the canonicalizer's buffer hint (#1112) to the
// compact length: insignificant whitespace is not reserved, while whitespace,
// escaped quotes and backslashes inside strings are.
func TestCompactSizeHint(t *testing.T) {
	tests := map[string]string{
		"  {\"a b\" : [1,\t2]}\r\n":           `{"a b":[1,2]}`,
		`{"q":"say \"hi \" there\\", "x": 1}`: `{"q":"say \"hi \" there\\","x":1}`,
		strings.Repeat(" ", 1<<20) + "null":   "null",
	}
	for input, compact := range tests {
		if got := compactSizeHint([]byte(input)); got != len(compact) {
			t.Fatalf("compactSizeHint(%.40q) = %d, want %d", input, got, len(compact))
		}
	}
}
