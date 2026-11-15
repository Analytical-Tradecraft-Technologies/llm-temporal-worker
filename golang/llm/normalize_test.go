package llm_test

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

func TestNormalizeRequestIsIdempotent(t *testing.T) {
	request := readRequestFixture(t, "minimal.json")
	first := mustNormalizeRequest(t, request)
	second := mustNormalizeRequest(t, first)
	firstJSON := testassert.MarshalJSON(t, first)
	secondJSON := testassert.MarshalJSON(t, second)
	if !bytes.Equal(firstJSON, secondJSON) {
		t.Fatalf("normalization is not idempotent:\nfirst:  %s\nsecond: %s", firstJSON, secondJSON)
	}
	if first.APIVersion != llm.APIVersion || first.ServiceClass != llm.ServiceClassStandard {
		t.Fatalf("normalization lost defaults: %#v", first)
	}
}

func TestNormalizeRequestRetainsFallbackOrder(t *testing.T) {
	request := readRequestFixture(t, "full.json")
	normalized := mustNormalizeRequest(t, request)
	want := []llm.ServiceClass{llm.ServiceClassStandard}
	if len(normalized.ServiceClassFallbacks) != len(want) || normalized.ServiceClassFallbacks[0] != want[0] {
		t.Fatalf("fallbacks = %#v, want %#v", normalized.ServiceClassFallbacks, want)
	}
}

func TestRequestDigestIgnoresOperationKey(t *testing.T) {
	first := readRequestFixture(t, "full.json")
	second := first
	second.OperationKey = "another-operation"
	assertSameRequestDigest(t, first, second, "operation_key")
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
	assertSameRequestDigest(t, first, second, "object key order")
}

func TestRequestDigestUsesDecodedBytes(t *testing.T) {
	firstData := readFixture(t, filepath.Join("request", "full.json"))
	secondData := bytes.Replace(firstData, []byte(`"AQI="`), []byte(`"A\u0051I="`), 1)
	assertSameRequestDigest(t, unmarshalRequest(t, firstData), unmarshalRequest(t, secondData), "equivalent decoded bytes")
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
			got := requestDigest(t, request)
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
	return testassert.UnmarshalJSON[llm.Request](t, data, "decode request")
}

// assertSameRequestDigest fails when first and second have different request
// digests; what names the difference between them.
func assertSameRequestDigest(t *testing.T, first, second llm.Request, what string) {
	t.Helper()
	if one, two := requestDigest(t, first), requestDigest(t, second); one != two {
		t.Fatalf("%s changed request digest: %x != %x", what, one, two)
	}
}

func requestDigest(t *testing.T, request llm.Request) [32]byte {
	t.Helper()
	digest, err := llm.RequestDigest(request)
	testassert.NoError(t, err)
	return digest
}

func mustNormalizeRequest(t *testing.T, request llm.Request) llm.Request {
	t.Helper()
	normalized, err := llm.NormalizeRequest(request)
	testassert.NoError(t, err)
	return normalized
}
