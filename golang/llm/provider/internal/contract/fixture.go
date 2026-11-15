package contract

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"

	yaml "go.yaml.in/yaml/v4"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/contracttest"
)

// ReadFixture returns the contents of the fixture file at path.
func ReadFixture(t testing.TB, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// DecodeJSON decodes the JSON in data into a T.
func DecodeJSON[T any](t testing.TB, data []byte) T {
	t.Helper()
	var value T
	if err := json.Unmarshal(data, &value); err != nil {
		t.Fatal(err)
	}
	return value
}

// DecodeMetadata decodes a fixture profile's metadata.yaml.
func DecodeMetadata(t testing.TB, data []byte) contracttest.Metadata {
	t.Helper()
	var metadata contracttest.Metadata
	if err := yaml.Unmarshal(data, &metadata); err != nil {
		t.Fatal(err)
	}
	return metadata
}

// MarshalJSON returns the JSON encoding of value.
func MarshalJSON(t testing.TB, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// JSONObject returns value encoded as JSON and decoded as an object, the
// shape a request body takes on the wire.
func JSONObject(t testing.TB, value any) map[string]any {
	t.Helper()
	return DecodeJSON[map[string]any](t, MarshalJSON(t, value))
}

// AssertCanonicalJSON checks that got and want are the same JSON once both
// are canonicalized, reporting a difference as "<name> mismatch".
func AssertCanonicalJSON(t testing.TB, got, want []byte, name string) {
	t.Helper()
	gotCanonical, err := llm.CanonicalJSON(got)
	if err != nil {
		t.Fatal(err)
	}
	wantCanonical, err := llm.CanonicalJSON(want)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotCanonical, wantCanonical) {
		t.Fatalf("%s mismatch\n got: %s\nwant: %s", name, gotCanonical, wantCanonical)
	}
}

// NormalizeRequest normalizes request and its service class, as Compile does
// before lowering.
func NormalizeRequest(t testing.TB, request llm.Request) (llm.Request, llm.ServiceClass) {
	t.Helper()
	normalized, err := llm.NormalizeRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	serviceClass, err := llm.NormalizeServiceClass(normalized.ServiceClass)
	if err != nil {
		t.Fatal(err)
	}
	return normalized, serviceClass
}
