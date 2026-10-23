package schema

import (
	"encoding/json"
	"fmt"
	"strings"
)

// The third-party validator converts every JSON number to a big.Rat. It
// ignores a failed conversion and then dereferences the nil result, and an
// accepted but huge exponent costs time and memory out of proportion to the
// number's few bytes (#1522). Numbers are therefore bounded before the
// validator sees a schema or an instance.
const (
	// MaxNumberDigits bounds the integer and fraction digits of one JSON
	// number, excluding sign and exponent.
	MaxNumberDigits = 1000
	// MaxNumberExponent bounds the magnitude of a JSON number's explicit
	// decimal exponent.
	MaxNumberExponent = 1000
)

// NumberBoundError reports a JSON number outside MaxNumberDigits or
// MaxNumberExponent. Path is a canonical JSON pointer; the number itself is
// not retained.
type NumberBoundError struct {
	Path   string
	Reason string
}

func (err *NumberBoundError) Error() string {
	return fmt.Sprintf("JSON number at %s %s", displayPath(err.Path), err.Reason)
}

// checkNumbers walks a value decoded with json.Decoder.UseNumber and rejects
// the first number outside the bounds.
func checkNumbers(value any, path string) error {
	switch value := value.(type) {
	case map[string]any:
		for key, child := range value {
			if err := checkNumbers(child, pointerJoin(path, key)); err != nil {
				return err
			}
		}
	case []any:
		for index, child := range value {
			if err := checkNumbers(child, fmt.Sprintf("%s/%d", path, index)); err != nil {
				return err
			}
		}
	case json.Number:
		if reason := numberBoundViolation(string(value)); reason != "" {
			return &NumberBoundError{Path: path, Reason: reason}
		}
	}
	return nil
}

// numberBoundViolation returns why valid JSON number text is out of bounds,
// or "" when it is within them.
func numberBoundViolation(text string) string {
	mantissa, exponent, hasExponent := strings.Cut(strings.ToLower(text), "e")
	mantissa = strings.TrimPrefix(mantissa, "-")
	if digits := len(mantissa) - strings.Count(mantissa, "."); digits > MaxNumberDigits {
		return fmt.Sprintf("has more than %d digits", MaxNumberDigits)
	}
	if !hasExponent {
		return ""
	}
	exponent = strings.TrimLeft(strings.TrimLeft(exponent, "+-"), "0")
	magnitude := 0
	for _, c := range exponent {
		if c < '0' || c > '9' {
			return "has an invalid exponent"
		}
		magnitude = magnitude*10 + int(c-'0')
		if magnitude > MaxNumberExponent {
			return fmt.Sprintf("has an exponent beyond ±%d", MaxNumberExponent)
		}
	}
	return ""
}
