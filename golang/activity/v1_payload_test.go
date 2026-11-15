package activity

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

func TestV1PayloadCodecsRoundTripEveryWireKind(t *testing.T) {
	limits := PayloadLimits{MaxInlineBytes: 1 << 20}
	tests := []struct {
		name  string
		file  string
		codec func([]byte, PayloadLimits) (any, []byte, error)
	}{
		{
			name:  "generate root",
			file:  "generate-root.json",
			codec: payloadRoundTrip(MarshalGenerateV1, UnmarshalGenerateV1),
		},
		{
			name:  "generate delta",
			file:  "generate-delta.json",
			codec: payloadRoundTrip(MarshalGenerateV1, UnmarshalGenerateV1),
		},
		{
			name:  "generate response",
			file:  "generate-response.json",
			codec: payloadRoundTrip(MarshalGenerateResponseV1, UnmarshalGenerateResponseV1),
		},
		{
			name:  "compact request",
			file:  "compact-request.json",
			codec: payloadRoundTrip(MarshalCompactV1, UnmarshalCompactV1),
		},
		{
			name:  "compact response",
			file:  "compact-response.json",
			codec: payloadRoundTrip(MarshalCompactResponseV1, UnmarshalCompactResponseV1),
		},
		{
			name:  "query request",
			file:  "query-provider-status.json",
			codec: payloadRoundTrip(MarshalQueryV1, UnmarshalQueryV1),
		},
		{
			name:  "query response",
			file:  "query-provider-response.json",
			codec: payloadRoundTrip(MarshalQueryResponseV1, UnmarshalQueryResponseV1),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data := readV1PayloadFixture(t, test.file)
			decoded, encoded, err := test.codec(data, limits)
			testassert.NoError(t, err, "codec round trip")
			if decoded == nil || len(encoded) == 0 {
				t.Fatalf("codec returned empty result: decoded=%#v encoded=%q", decoded, encoded)
			}
			canonical, err := json.Marshal(decoded)
			testassert.NoError(t, err, "re-marshal decoded payload")
			if !bytes.Equal(encoded, canonical) {
				t.Fatalf("payload encoding is not deterministic: %s != %s", encoded, canonical)
			}
		})
	}
}

func TestV1PayloadCodecsEnforceInlineLimitOnEncodeAndDecode(t *testing.T) {
	data := readV1PayloadFixture(t, "query-provider-status.json")
	var request llm.QueryRequestV1
	testassert.NoError(t, json.Unmarshal(data, &request))
	assertCodecRejectsOneByteOverLimit(t, request, MarshalQueryV1, UnmarshalQueryV1)
}

func TestV1PayloadCodecsRoundTripContractMatrix(t *testing.T) {
	limits := PayloadLimits{MaxInlineBytes: 1 << 20}
	for _, name := range []string{
		"generate-fork-patch-set.json",
		"generate-fork-patch-clear.json",
		"generate-variant-positive-temperature.json",
		"generate-response-disabled-cache.json",
		"generate-response-cache-hit.json",
		"generate-response-miss-not-populated.json",
		"compact-request-no-cache.json",
		"query-model-inventory.json",
		"query-credit-status.json",
		"query-budget-status.json",
		"query-spend-summary.json",
		"query-model-inventory-response.json",
		"query-credit-status-response.json",
		"query-budget-status-response.json",
		"query-spend-summary-response.json",
	} {
		t.Run(name, func(t *testing.T) {
			codec := payloadRoundTrip(MarshalGenerateV1, UnmarshalGenerateV1)
			switch {
			case name == "compact-request-no-cache.json":
				codec = payloadRoundTrip(MarshalCompactV1, UnmarshalCompactV1)
			case strings.HasPrefix(name, "generate-response-"):
				codec = payloadRoundTrip(MarshalGenerateResponseV1, UnmarshalGenerateResponseV1)
			case strings.HasPrefix(name, "query-") && strings.HasSuffix(name, "-response.json"):
				codec = payloadRoundTrip(MarshalQueryResponseV1, UnmarshalQueryResponseV1)
			case strings.HasPrefix(name, "query-"):
				codec = payloadRoundTrip(MarshalQueryV1, UnmarshalQueryV1)
			}
			_, encoded, err := codec(readV1PayloadFixture(t, name), limits)
			testassert.NoError(t, err, "matrix codec round trip")
			if len(encoded) == 0 {
				t.Fatal("matrix codec returned empty payload")
			}
		})
	}
}

func TestV1PayloadCodecsRejectMalformedAndUnknownFields(t *testing.T) {
	for _, data := range [][]byte{
		[]byte(`{"api_version":"llm.temporal/query/v1","operation_key":"q"`),
		[]byte(`{"api_version":"llm.temporal/query/v1","operation_key":"q","context":{"tenant":"t","project":"p","actor":"a"},"kind":"provider_status","query":{},"unknown":true}`),
	} {
		if _, err := UnmarshalQueryV1(data, PayloadLimits{}); err == nil {
			t.Fatalf("invalid v1 payload was accepted: %s", data)
		}
	}
}

func readV1PayloadFixture(t *testing.T, name string) []byte {
	t.Helper()
	return testassert.ReadFile(t, filepath.Join("..", "llm", "testdata", "v1", name))
}

// payloadRoundTrip decodes a fixture into T, encodes it with marshal and
// decodes the encoding with unmarshal, returning the decoded value and the
// encoding.
func payloadRoundTrip[T any](marshal func(T, PayloadLimits) ([]byte, error), unmarshal func([]byte, PayloadLimits) (T, error)) func([]byte, PayloadLimits) (any, []byte, error) {
	return func(data []byte, limits PayloadLimits) (any, []byte, error) {
		var value T
		if err := json.Unmarshal(data, &value); err != nil {
			return nil, nil, err
		}
		encoded, err := marshal(value, limits)
		if err != nil {
			return nil, nil, err
		}
		decoded, err := unmarshal(encoded, limits)
		return decoded, encoded, err
	}
}
