package llm

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
)

// This file holds the small codec helpers the closed-object decoders and
// encoders share, next to decodeOptionalField and friends in jsonutil.go.

// decodeClosedObject decodes one JSON object and rejects any field outside
// allowed.
func decodeClosedObject(data []byte, allowed ...string) (map[string]json.RawMessage, error) {
	fields, err := decodeObject(data)
	if err != nil {
		return nil, err
	}
	if err := checkUnknownFields(fields, allowed...); err != nil {
		return nil, err
	}
	return fields, nil
}

// decodePresentField decodes fields[name] into target when the field is
// present and returns decode's error unchanged.
func decodePresentField[T any](fields map[string]json.RawMessage, name string, target *T, decode func([]byte) (T, error)) error {
	raw, ok := fields[name]
	if !ok {
		return nil
	}
	value, err := decode(raw)
	if err != nil {
		return err
	}
	*target = value
	return nil
}

// decodeArray splits a JSON array with decodeJSON and decodes each element;
// see decodeEach.
func decodeArray[T any](data []byte, noun string, decode func([]byte) (T, error)) ([]T, error) {
	var values []json.RawMessage
	if err := decodeJSON(data, &values); err != nil {
		return nil, err
	}
	return decodeEach(values, noun, decode)
}

// decodeVerifiedList decodes a JSON array that is a sub-value of an already
// verified document (see decodeVerifiedArray). A null value is reported as
// "<noun>s must be an array".
func decodeVerifiedList[T any](data []byte, noun string, decode func([]byte) (T, error)) ([]T, error) {
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return nil, fmt.Errorf("%ss must be an array", noun)
	}
	values, err := decodeVerifiedArray(data)
	if err != nil {
		return nil, err
	}
	return decodeEach(values, noun, decode)
}

// decodeJSONValue is decodeJSON as a decode function for the field helpers.
func decodeJSONValue[T any](data []byte) (T, error) {
	var value T
	err := decodeJSON(data, &value)
	return value, err
}

// unmarshalValue is json.Unmarshal as a decode function for the field
// helpers.
func unmarshalValue[T any](data []byte) (T, error) {
	var value T
	err := json.Unmarshal(data, &value)
	return value, err
}

// pointerTo adapts decode to return a pointer to the decoded value.
func pointerTo[T any](decode func([]byte) (T, error)) func([]byte) (*T, error) {
	return func(data []byte) (*T, error) {
		value, err := decode(data)
		if err != nil {
			return nil, err
		}
		return &value, nil
	}
}

// requiredRawArray reads the required field name as a JSON array of raw
// values. A missing, null or non-array value is reported as
// "<name> must be an array".
func requiredRawArray(fields map[string]json.RawMessage, name string) ([]json.RawMessage, error) {
	raw, err := requireField(fields, name)
	if err != nil || string(raw) == "null" {
		return nil, fmt.Errorf("%s must be an array", name)
	}
	var values []json.RawMessage
	if err := decodeJSON(raw, &values); err != nil {
		return nil, fmt.Errorf("%s must be an array", name)
	}
	return values, nil
}

// requireStrings checks, in order, that every named field is present and a
// string.
func requireStrings(fields map[string]json.RawMessage, names ...string) error {
	for _, name := range names {
		if _, err := requiredString(fields, name); err != nil {
			return err
		}
	}
	return nil
}

// setIfNotEmpty stores value under key when it is not empty, as the encoders
// do for optional string members.
func setIfNotEmpty[S ~string](fields map[string]any, key string, value S) {
	if value != "" {
		fields[key] = value
	}
}

// setIfNotNil stores value under key when it is not nil, as the encoders do
// for optional object members.
func setIfNotNil[T any](fields map[string]any, key string, value *T) {
	if value != nil {
		fields[key] = value
	}
}

// emptyIfNil returns values, or an empty slice when values is nil, so the
// encoders write [] rather than null and the decoders never return nil.
func emptyIfNil[T any](values []T) []T {
	if values == nil {
		return []T{}
	}
	return values
}

// firstError returns the first non-nil error in errs.
func firstError(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// derefAs returns value as a T when it holds a T or a non-nil *T. Pointer
// forms satisfy the item, part and query result interfaces too.
func derefAs[T any](value any) (T, bool) {
	switch concrete := value.(type) {
	case T:
		return concrete, true
	case *T:
		if concrete != nil {
			return *concrete, true
		}
	}
	var zero T
	return zero, false
}

// fieldReader decodes the fields of one JSON object in order and keeps the
// first error, so a run of reads needs a single error check. Once a read
// fails every later read does nothing and returns the zero value, exactly as
// returning at the failing read would.
type fieldReader struct {
	fields map[string]json.RawMessage
	err    error
}

// readField runs read on the reader's fields unless an earlier read failed.
func readField[T any](reader *fieldReader, key string, read func(map[string]json.RawMessage, string) (T, error)) T {
	if reader.err != nil {
		var zero T
		return zero
	}
	value, err := read(reader.fields, key)
	reader.err = err
	return value
}

func (reader *fieldReader) requiredString(key string) string {
	return readField(reader, key, requiredString)
}

func (reader *fieldReader) optionalString(key string) string {
	value, _ := reader.optionalStringPresent(key)
	return value
}

// optionalStringPresent is optionalString that also reports whether the
// field was present.
func (reader *fieldReader) optionalStringPresent(key string) (string, bool) {
	if reader.err != nil {
		return "", false
	}
	value, present, err := optionalString(reader.fields, key)
	reader.err = err
	return value, present
}

func (reader *fieldReader) requiredInt64(key string) int64 {
	return readField(reader, key, requiredInt64)
}

func (reader *fieldReader) requiredBool(key string) bool {
	return readField(reader, key, requiredBool)
}

func (reader *fieldReader) optionalBool(key string) bool {
	if reader.err != nil {
		return false
	}
	value, _, err := optionalBool(reader.fields, key)
	reader.err = err
	return value
}

func (reader *fieldReader) requireField(key string) json.RawMessage {
	return readField(reader, key, requireField)
}

// readOptionalField is decodeOptionalField on a fieldReader.
func readOptionalField[T any](reader *fieldReader, name string, target *T, decode func([]byte) (T, error)) {
	if reader.err == nil {
		reader.err = decodeOptionalField(reader.fields, name, target, decode)
	}
}

// readRequiredField is readOptionalField for a field that must be present;
// a missing field is reported by requireField.
func readRequiredField[T any](reader *fieldReader, name string, target *T, decode func([]byte) (T, error)) {
	reader.requireField(name)
	readOptionalField(reader, name, target, decode)
}

// readNullableField is decodeNullableField on a fieldReader.
func readNullableField[T any](reader *fieldReader, name string, target *T, decode func([]byte) (T, error)) {
	if reader.err == nil {
		reader.err = decodeNullableField(reader.fields, name, target, decode)
	}
}

// readPresentField is decodePresentField on a fieldReader.
func readPresentField[T any](reader *fieldReader, name string, target *T, decode func([]byte) (T, error)) {
	if reader.err == nil {
		reader.err = decodePresentField(reader.fields, name, target, decode)
	}
}

// readEnum reads the optional string field key as a K; see defaultEnum.
func readEnum[K ~string](reader *fieldReader, key string, fallback K, label string, allowed ...K) K {
	value := K(reader.optionalString(key))
	if reader.err != nil {
		return ""
	}
	value, reader.err = defaultEnum(value, fallback, label, allowed...)
	return value
}

// defaultEnum replaces an empty value with fallback and reports a value
// outside allowed as "<label> %q is invalid".
func defaultEnum[K ~string](value, fallback K, label string, allowed ...K) (K, error) {
	if value == "" {
		value = fallback
	}
	if !slices.Contains(allowed, value) {
		return "", fmt.Errorf("%s %q is invalid", label, value)
	}
	return value, nil
}
