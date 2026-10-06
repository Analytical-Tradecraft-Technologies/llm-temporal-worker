package llm_test

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

func TestNormalizeRequestIsIdempotent(t *testing.T) {
	request := readRequestFixture(t, "minimal.json")
	first, err := llm.NormalizeRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := llm.NormalizeRequest(first)
	if err != nil {
		t.Fatal(err)
	}
	firstJSON := mustMarshal(t, first)
	secondJSON := mustMarshal(t, second)
	if !bytes.Equal(firstJSON, secondJSON) {
		t.Fatalf("normalization is not idempotent:\nfirst:  %s\nsecond: %s", firstJSON, secondJSON)
	}
	if first.APIVersion != llm.APIVersion || first.ServiceClass != llm.ServiceClassStandard {
		t.Fatalf("normalization lost defaults: %#v", first)
	}
}

func TestNormalizeRequestRetainsFallbackOrder(t *testing.T) {
	request := readRequestFixture(t, "full.json")
	normalized, err := llm.NormalizeRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	want := []llm.ServiceClass{llm.ServiceClassStandard}
	if len(normalized.ServiceClassFallbacks) != len(want) || normalized.ServiceClassFallbacks[0] != want[0] {
		t.Fatalf("fallbacks = %#v, want %#v", normalized.ServiceClassFallbacks, want)
	}
}

func TestRequestDigestIgnoresOperationKey(t *testing.T) {
	first := readRequestFixture(t, "full.json")
	second := first
	second.OperationKey = "another-operation"
	one, err := llm.RequestDigest(first)
	if err != nil {
		t.Fatal(err)
	}
	two, err := llm.RequestDigest(second)
	if err != nil {
		t.Fatal(err)
	}
	if one != two {
		t.Fatalf("operation_key changed request digest: %x != %x", one, two)
	}
}

func TestRequestDigestCanonicalizesObjectKeyOrder(t *testing.T) {
	first := readRequestFixture(t, "minimal.json")
	first.Extensions = map[string]json.RawMessage{
		"payload": json.RawMessage(`{"b":2,"a":{"y":false,"x":true}}`),
	}
	second := first
	second.Extensions = map[string]json.RawMessage{
		"payload": json.RawMessage(`{"a":{"x":true,"y":false},"b":2}`),
	}
	one, err := llm.RequestDigest(first)
	if err != nil {
		t.Fatal(err)
	}
	two, err := llm.RequestDigest(second)
	if err != nil {
		t.Fatal(err)
	}
	if one != two {
		t.Fatalf("object key order changed request digest: %x != %x", one, two)
	}
}

func TestRequestDigestUsesDecodedBytes(t *testing.T) {
	firstData := readFixture(t, filepath.Join("request", "full.json"))
	secondData := bytes.Replace(firstData, []byte(`"AQI="`), []byte(`"A\u0051I="`), 1)
	first := unmarshalRequest(t, firstData)
	second := unmarshalRequest(t, secondData)
	one, err := llm.RequestDigest(first)
	if err != nil {
		t.Fatal(err)
	}
	two, err := llm.RequestDigest(second)
	if err != nil {
		t.Fatal(err)
	}
	if one != two {
		t.Fatalf("equivalent decoded bytes changed request digest: %x != %x", one, two)
	}
}

func TestRequestDigestChangesForSemanticContextAndRouteAuthorization(t *testing.T) {
	base := readRequestFixture(t, "full.json")
	baseDigest := requestDigest(t, base)

	tests := []struct {
		name   string
		mutate func(*llm.Request)
	}{
		{name: "model", mutate: func(request *llm.Request) { request.Model = "another-model" }},
		{name: "input", mutate: func(request *llm.Request) {
			message := request.Input[0].(llm.Message)
			message.Content[0] = llm.TextPart{Text: "different"}
			request.Input[0] = message
		}},
		{name: "service class", mutate: func(request *llm.Request) {
			request.ServiceClass = llm.ServiceClassStandard
			request.ServiceClassFallbacks = nil
		}},
		{name: "fallback authorization", mutate: func(request *llm.Request) {
			request.ServiceClassFallbacks = []llm.ServiceClass{llm.ServiceClassEconomy}
		}},
		{name: "tenant", mutate: func(request *llm.Request) { request.Context.Tenant = "other-tenant" }},
		{name: "extension", mutate: func(request *llm.Request) {
			request.Extensions["openrouter"] = json.RawMessage(`{"provider_order":["ProviderB"]}`)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := base
			test.mutate(&request)
			got, err := llm.RequestDigest(request)
			if err != nil {
				t.Fatal(err)
			}
			if got == baseDigest {
				t.Fatalf("%s mutation did not change digest %x", test.name, got)
			}
		})
	}
}

func readRequestFixture(t *testing.T, name string) llm.Request {
	t.Helper()
	return unmarshalRequest(t, readFixture(t, filepath.Join("request", name)))
}

func unmarshalRequest(t *testing.T, data []byte) llm.Request {
	t.Helper()
	var request llm.Request
	if err := json.Unmarshal(data, &request); err != nil {
		t.Fatal(err)
	}
	return request
}

func requestDigest(t *testing.T, request llm.Request) [32]byte {
	t.Helper()
	digest, err := llm.RequestDigest(request)
	if err != nil {
		t.Fatal(err)
	}
	return digest
}
