package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider/anthropicmessages"
)

func TestProductionAnthropicProfileLiftsResponseTiers(t *testing.T) {
	for _, family := range []string{"anthropic_messages", "anthropic_aws_messages"} {
		for _, tc := range []struct {
			requested    llm.ServiceClass
			wire, actual string
			want         llm.ServiceClass
		}{
			{llm.ServiceClassStandard, "standard_only", "standard", llm.ServiceClassStandard},
			{llm.ServiceClassPriority, "auto", "priority", llm.ServiceClassPriority},
			{llm.ServiceClassPriority, "auto", "standard", llm.ServiceClassStandard},
			{llm.ServiceClassStandard, "standard_only", "unknown", ""},
		} {
			t.Run(family+"/"+tc.wire+"/"+tc.actual, func(t *testing.T) {
				ctx := context.Background()
				endpoint := config.EndpointConfig{Family: family, BaseURL: "https://anthropic.example.test", ServiceClasses: map[llm.ServiceClass]config.TierConfig{
					llm.ServiceClassStandard: {ProviderValue: "standard_only"},
					llm.ServiceClassPriority: {ProviderValue: "auto"},
				}}
				factory := &ProductionEngineFactory{}
				profile, err := factory.anthropicProfile("anthropic", endpoint, anthropicmessages.DefaultProfile("capabilities").Capabilities, EndpointProfile{})
				if err != nil {
					t.Fatal(err)
				}
				calls := 0
				httpClient := &http.Client{Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
					calls++
					var wire map[string]any
					if err := json.NewDecoder(req.Body).Decode(&wire); err != nil {
						t.Fatal(err)
					}
					if wire["service_tier"] != tc.wire {
						t.Fatalf("request tier = %v, want %s", wire["service_tier"], tc.wire)
					}
					body := fmt.Sprintf(`{"id":"msg_test","type":"message","role":"assistant","model":"claude-contract","content":[{"type":"text","text":"done"}],"stop_reason":"end_turn","usage":{"input_tokens":2,"output_tokens":1,"service_tier":%q}}`, tc.actual)
					return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: req}, nil
				})}
				var client *anthropicmessages.Client
				if family == "anthropic_aws_messages" {
					cfg := anthropicmessages.AWSClientConfig{BaseURL: endpoint.BaseURL, HTTPClient: httpClient}
					cfg.AWSConfig.AWSRegion = "us-east-1"
					cfg.AWSConfig.WorkspaceID = "ws-test"
					cfg.AWSConfig.SkipAuth = true
					client, err = anthropicmessages.NewAWSClient(ctx, cfg)
				} else {
					client, err = anthropicmessages.NewClient(anthropicmessages.ClientConfig{BaseURL: endpoint.BaseURL, APIKey: "test-key", HTTPClient: httpClient})
				}
				if err != nil {
					t.Fatal(err)
				}
				adapter, err := anthropicmessages.NewAdapter(client, "anthropic", *profile)
				if err != nil {
					t.Fatal(err)
				}
				call, err := adapter.Compile(ctx, provider.CompileInput{Request: llm.Request{OperationKey: "tier-test", Model: "claude-contract", ServiceClass: tc.requested}, Query: provider.CapabilityQuery{EndpointID: "anthropic", Family: provider.FamilyAnthropicMessages, Model: "claude-contract"}, Strict: true})
				if err != nil {
					t.Fatal(err)
				}
				result, err := adapter.Invoke(ctx, call, nil)
				if calls != 1 {
					t.Fatalf("calls = %d, want 1", calls)
				}
				if tc.want == "" {
					if err == nil {
						t.Fatal("unknown response tier accepted")
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				response := result.Response
				if response.Service.Actual == nil || *response.Service.Actual != tc.want || response.Service.Attempted != tc.requested || response.Service.ProviderValue != tc.actual {
					t.Fatalf("service facts = %+v", response.Service)
				}
				if response.Status != llm.ResponseStatusCompleted || response.Usage.InputTokens != 2 || response.Usage.OutputTokens != 1 {
					t.Fatalf("response = %+v", response)
				}
				output, err := json.Marshal(response.Output)
				if err != nil {
					t.Fatal(err)
				}
				if !strings.Contains(string(output), `"text":"done"`) {
					t.Fatalf("output = %s", output)
				}
			})
		}
	}
}

func TestProductionAnthropicProfilePreservesExplicitResponseMapping(t *testing.T) {
	supplied := anthropicmessages.DefaultProfile("custom")
	supplied.ActualServiceClasses = map[string]llm.ServiceClass{"custom-tier": llm.ServiceClassStandard}
	factory := &ProductionEngineFactory{}
	profile, err := factory.anthropicProfile("custom", config.EndpointConfig{}, provider.CapabilitySet{}, EndpointProfile{Anthropic: &supplied})
	if err != nil {
		t.Fatal(err)
	}
	if len(profile.ActualServiceClasses) != 1 || profile.ActualServiceClasses["custom-tier"] != llm.ServiceClassStandard {
		t.Fatalf("custom mapping overwritten: %+v", profile.ActualServiceClasses)
	}
}
