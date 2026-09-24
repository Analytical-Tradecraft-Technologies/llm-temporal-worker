package bedrockmessages

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

func TestEmulatedJSONSchemaPreservesBoundsAndClaimOrder(t *testing.T) {
	schema := json.RawMessage(`{"type":"object","additionalProperties":false,"required":["claims"],"properties":{"claims":{"type":"array","minItems":2,"maxItems":2,"items":false,"prefixItems":[{"type":"object","additionalProperties":false,"required":["id","probability"],"properties":{"id":{"const":"first"},"probability":{"type":"number","minimum":0,"maximum":1}}},{"type":"object","additionalProperties":false,"required":["id","probability"],"properties":{"id":{"const":"second"},"probability":{"type":"number","minimum":0,"maximum":1}}}]}}}`)
	for _, test := range []struct {
		name  string
		text  string
		valid bool
	}{
		{"valid", `{"claims":[{"id":"first","probability":0.7},{"id":"second","probability":0.3}]}`, true},
		{"out_of_bounds", `{"claims":[{"id":"first","probability":1.2},{"id":"second","probability":0.3}]}`, false},
		{"reordered", `{"claims":[{"id":"second","probability":0.7},{"id":"first","probability":0.3}]}`, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			client, err := NewClient(context.Background(), ClientConfig{
				BaseURL: "http://127.0.0.1",
				HTTPClient: &http.Client{Transport: bedrockRoundTrip(func(request *http.Request) (*http.Response, error) {
					calls++
					var wire struct {
						OutputConfig struct {
							Format json.RawMessage `json:"format"`
						} `json:"output_config"`
					}
					if err := json.NewDecoder(request.Body).Decode(&wire); err != nil {
						t.Fatal(err)
					}
					// Bedrock's native grammar cannot express this bounded ordered vector.
					if len(wire.OutputConfig.Format) != 0 {
						return &http.Response{StatusCode: http.StatusBadRequest, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"message":"unsupported native JSON schema constraints"}`)), Request: request}, nil
					}
					body, err := json.Marshal(map[string]any{
						"id": "msg-contract", "type": "message", "role": "assistant", "model": "claude-contract", "stop_reason": "end_turn",
						"content": []map[string]string{{"type": "text", "text": test.text}},
						"usage":   map[string]any{"input_tokens": 10, "output_tokens": 20, "service_tier": "default"},
					})
					if err != nil {
						t.Fatal(err)
					}
					return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}, "X-Amzn-Requestid": {"schema-contract"}}, Body: io.NopCloser(strings.NewReader(string(body))), Request: request}, nil
				})},
				AWSConfig: aws.Config{Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("contract-access", "contract-secret", "")},
			})
			if err != nil {
				t.Fatal(err)
			}
			profile := mustBedrockProfile(t, "http://127.0.0.1")
			profile.Capabilities.Features[provider.FeatureStructuredOutput] = provider.Capability{State: provider.CapabilityEmulated, Transform: "json_schema_prompt_v1"}
			adapter, err := New(client, "bedrock-prod", profile)
			if err != nil {
				t.Fatal(err)
			}
			call, err := adapter.Compile(context.Background(), provider.CompileInput{Strict: true, Request: llm.Request{
				OperationKey: "schema-contract", Model: "claude-contract",
				Output:     &llm.OutputSpec{Format: llm.OutputFormat{Kind: llm.OutputKindJSONSchema, Name: "claims", Schema: schema, Strict: true}},
				ToolPolicy: llm.ToolPolicy{Mode: llm.ToolChoiceNone},
			}})
			if err != nil {
				t.Fatal(err)
			}
			result, err := adapter.Invoke(context.Background(), call, nil)
			if test.valid {
				if err != nil {
					t.Fatalf("valid forecast rejected: %v", err)
				}
				if result.Response.Status != llm.ResponseStatusCompleted {
					t.Fatalf("valid forecast status: %s", result.Response.Status)
				}
			} else {
				var providerErr *provider.Error
				if !errors.As(err, &providerErr) || providerErr.Code != provider.CodeProviderInvalidResponse || providerErr.Retry != provider.RetryNever {
					t.Fatalf("invalid forecast was not rejected without retry: %#v", err)
				}
			}
			if calls != 1 {
				t.Fatalf("schema validation dispatched %d calls; want exactly one", calls)
			}
		})
	}
}

func TestEmulatedStructuredOutputRejectsUnknownTransform(t *testing.T) {
	profile := mustBedrockProfile(t, "")
	profile.Capabilities.Features[provider.FeatureStructuredOutput] = provider.Capability{State: provider.CapabilityEmulated, Transform: "unknown"}
	adapter := &Adapter{endpointID: "bedrock-prod", profile: profile}
	_, err := adapter.Compile(context.Background(), provider.CompileInput{Strict: true, Request: llm.Request{
		OperationKey: "unknown-transform", Model: "claude-contract",
		Output: &llm.OutputSpec{Format: llm.OutputFormat{Kind: llm.OutputKindJSONSchema, Name: "bounded", Schema: json.RawMessage(`{"type":"number","minimum":0,"maximum":1}`), Strict: true}},
	}})
	var providerErr *provider.Error
	if !errors.As(err, &providerErr) || providerErr.Dispatch != provider.DispatchNotDispatched {
		t.Fatalf("unknown emulation transform was not rejected before dispatch: %#v", err)
	}
}
