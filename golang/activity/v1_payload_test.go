package activity

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

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
			name: "generate root",
			file: "generate-root.json",
			codec: func(data []byte, limits PayloadLimits) (any, []byte, error) {
				var value llm.GenerateRequestV1
				if err := json.Unmarshal(data, &value); err != nil {
					return nil, nil, err
				}
				encoded, err := MarshalGenerateV1(value, limits)
				if err != nil {
					return nil, nil, err
				}
				decoded, err := UnmarshalGenerateV1(encoded, limits)
				return decoded, encoded, err
			},
		},
		{
			name: "generate delta",
			file: "generate-delta.json",
			codec: func(data []byte, limits PayloadLimits) (any, []byte, error) {
				var value llm.GenerateRequestV1
				if err := json.Unmarshal(data, &value); err != nil {
					return nil, nil, err
				}
				encoded, err := MarshalGenerateV1(value, limits)
				if err != nil {
					return nil, nil, err
				}
				decoded, err := UnmarshalGenerateV1(encoded, limits)
				return decoded, encoded, err
			},
		},
		{
			name: "generate response",
			file: "generate-response.json",
			codec: func(data []byte, limits PayloadLimits) (any, []byte, error) {
				var value llm.GenerateResponseV1
				if err := json.Unmarshal(data, &value); err != nil {
					return nil, nil, err
				}
				encoded, err := MarshalGenerateResponseV1(value, limits)
				if err != nil {
					return nil, nil, err
				}
				decoded, err := UnmarshalGenerateResponseV1(encoded, limits)
				return decoded, encoded, err
			},
		},
		{
			name: "compact request",
			file: "compact-request.json",
			codec: func(data []byte, limits PayloadLimits) (any, []byte, error) {
				var value llm.CompactRequestV1
				if err := json.Unmarshal(data, &value); err != nil {
					return nil, nil, err
				}
				encoded, err := MarshalCompactV1(value, limits)
				if err != nil {
					return nil, nil, err
				}
				decoded, err := UnmarshalCompactV1(encoded, limits)
				return decoded, encoded, err
			},
		},
		{
			name: "compact response",
			file: "compact-response.json",
			codec: func(data []byte, limits PayloadLimits) (any, []byte, error) {
				var value llm.CompactResponseV1
				if err := json.Unmarshal(data, &value); err != nil {
					return nil, nil, err
				}
				encoded, err := MarshalCompactResponseV1(value, limits)
				if err != nil {
					return nil, nil, err
				}
				decoded, err := UnmarshalCompactResponseV1(encoded, limits)
				return decoded, encoded, err
			},
		},
		{
			name: "query request",
			file: "query-provider-status.json",
			codec: func(data []byte, limits PayloadLimits) (any, []byte, error) {
				var value llm.QueryRequestV1
				if err := json.Unmarshal(data, &value); err != nil {
					return nil, nil, err
				}
				encoded, err := MarshalQueryV1(value, limits)
				if err != nil {
					return nil, nil, err
				}
				decoded, err := UnmarshalQueryV1(encoded, limits)
				return decoded, encoded, err
			},
		},
		{
			name: "query response",
			file: "query-provider-response.json",
			codec: func(data []byte, limits PayloadLimits) (any, []byte, error) {
				var value llm.QueryResponseV1
				if err := json.Unmarshal(data, &value); err != nil {
					return nil, nil, err
				}
				encoded, err := MarshalQueryResponseV1(value, limits)
				if err != nil {
					return nil, nil, err
				}
				decoded, err := UnmarshalQueryResponseV1(encoded, limits)
				return decoded, encoded, err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data := readV1PayloadFixture(t, test.file)
			decoded, encoded, err := test.codec(data, limits)
			if err != nil {
				t.Fatalf("codec round trip: %v", err)
			}
			if decoded == nil || len(encoded) == 0 {
				t.Fatalf("codec returned empty result: decoded=%#v encoded=%q", decoded, encoded)
			}
			canonical, err := json.Marshal(decoded)
			if err != nil {
				t.Fatalf("re-marshal decoded payload: %v", err)
			}
			if !bytes.Equal(encoded, canonical) {
				t.Fatalf("payload encoding is not deterministic: %s != %s", encoded, canonical)
			}
		})
	}
}

func TestV1PayloadCodecsEnforceInlineLimitOnEncodeAndDecode(t *testing.T) {
	data := readV1PayloadFixture(t, "query-provider-status.json")
	var request llm.QueryRequestV1
	if err := json.Unmarshal(data, &request); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	limits := PayloadLimits{MaxInlineBytes: len(encoded) - 1}
	if _, err := MarshalQueryV1(request, limits); err == nil {
		t.Fatal("oversize v1 payload was accepted during encode")
	}
	if _, err := UnmarshalQueryV1(encoded, limits); err == nil {
		t.Fatal("oversize v1 payload was accepted during decode")
	}
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
			data := readV1PayloadFixture(t, name)
			var encoded []byte
			var err error
			switch {
			case name == "compact-request-no-cache.json":
				var value llm.CompactRequestV1
				if err = json.Unmarshal(data, &value); err == nil {
					encoded, err = MarshalCompactV1(value, limits)
					if err == nil {
						_, err = UnmarshalCompactV1(encoded, limits)
					}
				}
			case strings.HasPrefix(name, "generate-response-"):
				var value llm.GenerateResponseV1
				if err = json.Unmarshal(data, &value); err == nil {
					encoded, err = MarshalGenerateResponseV1(value, limits)
					if err == nil {
						_, err = UnmarshalGenerateResponseV1(encoded, limits)
					}
				}
			case strings.HasPrefix(name, "query-") && strings.HasSuffix(name, "-response.json"):
				var value llm.QueryResponseV1
				if err = json.Unmarshal(data, &value); err == nil {
					encoded, err = MarshalQueryResponseV1(value, limits)
					if err == nil {
						_, err = UnmarshalQueryResponseV1(encoded, limits)
					}
				}
			case strings.HasPrefix(name, "query-"):
				var value llm.QueryRequestV1
				if err = json.Unmarshal(data, &value); err == nil {
					encoded, err = MarshalQueryV1(value, limits)
					if err == nil {
						_, err = UnmarshalQueryV1(encoded, limits)
					}
				}
			default:
				var value llm.GenerateRequestV1
				if err = json.Unmarshal(data, &value); err == nil {
					encoded, err = MarshalGenerateV1(value, limits)
					if err == nil {
						_, err = UnmarshalGenerateV1(encoded, limits)
					}
				}
			}
			if err != nil {
				t.Fatalf("matrix codec round trip: %v", err)
			}
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
	data, err := os.ReadFile(filepath.Join("..", "llm", "testdata", "v1", name))
	if err != nil {
		t.Fatal(err)
	}
	return data
}
