package provider

import (
	"encoding/json"
	"fmt"
)

// The helpers in this file are the lowering steps every adapter that builds
// its request as a JSON wire map shares.

// LowerEach lowers values in order, prefixing an error with label and the
// failing value's index, as in "part 2: ...".
func LowerEach[T, R any](values []T, label string, lower func(T) (R, error)) ([]any, error) {
	lowered := make([]any, 0, len(values))
	for index, value := range values {
		result, err := lower(value)
		if err != nil {
			return nil, fmt.Errorf("%s %d: %w", label, index, err)
		}
		lowered = append(lowered, result)
	}
	return lowered, nil
}

// DecodeWireParams carries a lowered wire body into the SDK parameter type P
// and returns the encoded body alongside it. label names the SDK union in a
// decode failure, as in "openai chat".
func DecodeWireParams[P any](body map[string]any, label string) (P, []byte, error) {
	var params P
	encoded, err := json.Marshal(body)
	if err != nil {
		return params, nil, err
	}
	if err := json.Unmarshal(encoded, &params); err != nil {
		var zero P
		return zero, nil, fmt.Errorf("%s parameter union: %w", label, err)
	}
	return params, encoded, nil
}
