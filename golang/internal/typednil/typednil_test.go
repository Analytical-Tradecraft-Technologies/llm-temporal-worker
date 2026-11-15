package typednil

import (
	"strings"
	"testing"
)

func TestIs(t *testing.T) {
	var (
		nilPointer  *strings.Reader
		nilMap      map[string]int
		nilSlice    []int
		nilChannel  chan int
		nilFunction func()
	)
	for _, test := range []struct {
		name  string
		value any
		want  bool
	}{
		{name: "untyped nil", value: nil, want: true},
		{name: "nil pointer", value: nilPointer, want: true},
		{name: "nil map", value: nilMap, want: true},
		{name: "nil slice", value: nilSlice, want: true},
		{name: "nil channel", value: nilChannel, want: true},
		{name: "nil function", value: nilFunction, want: true},
		{name: "pointer", value: strings.NewReader(""), want: false},
		{name: "empty map", value: map[string]int{}, want: false},
		{name: "empty slice", value: []int{}, want: false},
		{name: "function", value: func() {}, want: false},
		{name: "struct", value: struct{}{}, want: false},
		{name: "zero int", value: 0, want: false},
		{name: "empty string", value: "", want: false},
	} {
		if got := Is(test.value); got != test.want {
			t.Errorf("%s: Is() = %v, want %v", test.name, got, test.want)
		}
	}
}
