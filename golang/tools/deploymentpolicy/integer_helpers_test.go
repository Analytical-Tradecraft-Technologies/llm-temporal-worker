package main

import (
	"math"
	"testing"
)

func TestPositiveIntegerAcceptsEveryWholeNumberKind(t *testing.T) {
	for _, value := range []any{int(1), int8(1), int16(1), int32(1), int64(1), uint(1), uint8(1), uint16(1), uint32(1), uint64(1), float32(1), float64(1), uint64(math.MaxUint64), uint(math.MaxUint64), float64(1e20)} {
		if !positiveInteger(value) {
			t.Fatalf("positiveInteger(%T %v) = false", value, value)
		}
	}
	for _, value := range []any{int(0), int64(-1), uint(0), uint64(0), float64(0.5), float32(-1), "1", nil} {
		if positiveInteger(value) {
			t.Fatalf("positiveInteger(%T %v) = true", value, value)
		}
	}
}

func TestIntegerEqualComparesWholeNumbers(t *testing.T) {
	for _, value := range []any{int(8080), int16(8080), int64(8080), uint(8080), uint32(8080), uint64(8080), float32(8080), float64(8080)} {
		if !integerEqual(value, 8080) {
			t.Fatalf("integerEqual(%T %v, 8080) = false", value, value)
		}
	}
	for _, value := range []any{int(8081), uint8(80), float64(8080.5), math.Inf(1), uint64(math.MaxUint64), "8080", nil} {
		if integerEqual(value, 8080) {
			t.Fatalf("integerEqual(%T %v, 8080) = true", value, value)
		}
	}
}
