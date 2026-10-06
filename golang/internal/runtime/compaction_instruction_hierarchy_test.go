package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/budget"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/engine"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/anthropicmessages"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/bedrockconverse"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/bedrockmessages"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/openaichat"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/routing"
)

// hierarchyRoute is one endpoint that cannot keep policy and application
// instructions apart, compiled by its real adapter.
type hierarchyRoute struct {
	name    string
	family  provider.Family
	tier    string
	adapter func(*testing.T) provider.Adapter
}

func hierarchyHTTPClient() *http.Client {
	return &http.Client{Transport: planningTransportFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("unexpected HTTP request")
	})}
}

func hierarchyAWSConfig() aws.Config {
	return aws.Config{Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("test-access", "test-secret", "")}
}

func hierarchyRoutes() []hierarchyRoute {
	return []hierarchyRoute{
		{name: "anthropic_messages", family: provider.FamilyAnthropicMessages, tier: "standard_only", adapter: func(t *testing.T) provider.Adapter {
			client, err := anthropicmessages.NewClient(anthropicmessages.ClientConfig{BaseURL: "https://127.0.0.1", APIKey: "test-key", HTTPClient: hierarchyHTTPClient()})
			if err != nil {
				t.Fatal(err)
			}
			profile := anthropicmessages.DefaultProfile("endpoint")
			profile.CapabilityVersion, profile.Capabilities.Version = "profile/v1", "profile/v1"
			adapter, err := anthropicmessages.New(client, "endpoint", profile)
			if err != nil {
				t.Fatal(err)
			}
			return adapter
		}},
		{name: "bedrock_anthropic_messages", family: provider.FamilyBedrockMessages, tier: "default", adapter: func(t *testing.T) provider.Adapter {
			client, err := bedrockmessages.NewClient(context.Background(), bedrockmessages.ClientConfig{BaseURL: "https://127.0.0.1", HTTPClient: hierarchyHTTPClient(), AWSConfig: hierarchyAWSConfig()})
			if err != nil {
				t.Fatal(err)
			}
			profile := bedrockmessages.DefaultProfile("endpoint")
			profile.CapabilityVersion, profile.Capabilities.Version = "profile/v1", "profile/v1"
			adapter, err := bedrockmessages.New(client, "endpoint", profile)
			if err != nil {
				t.Fatal(err)
			}
			return adapter
		}},
		{name: "bedrock_converse", family: provider.FamilyBedrockConverse, tier: "default", adapter: func(t *testing.T) provider.Adapter {
			client, err := bedrockconverse.NewClient(context.Background(), bedrockconverse.ClientConfig{BaseURL: "https://127.0.0.1", HTTPClient: hierarchyHTTPClient(), AWSConfig: hierarchyAWSConfig()})
			if err != nil {
				t.Fatal(err)
			}
			profile := bedrockconverse.DefaultProfile("endpoint")
			profile.CapabilityVersion, profile.Capabilities.Version = "profile/v1", "profile/v1"
			adapter, err := bedrockconverse.New(client, "endpoint", profile)
			if err != nil {
				t.Fatal(err)
			}
			return adapter
		}},
		{name: "azure_chat", family: provider.FamilyOpenAIChat, tier: "default", adapter: func(t *testing.T) provider.Adapter {
			client, err := openaichat.NewAzureClient(openaichat.AzureClientConfig{Endpoint: "https://127.0.0.1", APIVersion: "2025-01-01", APIKey: "test-key", HTTPClient: hierarchyHTTPClient()})
			if err != nil {
				t.Fatal(err)
			}
			features := map[provider.Feature]provider.Capability{}
			for _, feature := range []provider.Feature{provider.FeatureText, provider.FeatureImage, provider.FeatureDocument, provider.FeatureToolCall,
				provider.FeatureStructuredOutput, provider.FeatureReasoning, provider.FeatureContinuation, provider.FeatureStreaming, provider.FeatureUsage} {
				features[feature] = provider.Capability{State: provider.CapabilityNative}
			}
			for _, feature := range []provider.Feature{provider.FeatureDocument, provider.FeatureContinuation, provider.FeatureStreaming} {
				features[feature] = provider.Capability{State: provider.CapabilityUnsupported, Reason: "profile fixture"}
			}
			adapter, err := openaichat.NewAzureAdapter(client, "endpoint", openaichat.AzureProfileConfig{ID: "endpoint", CapabilityVersion: "profile/v1",
				BaseURL: "https://127.0.0.1", Deployment: "provider-model", Capabilities: provider.CapabilitySet{Version: "profile/v1", Features: features},
				ServiceTiers:         map[llm.ServiceClass]string{llm.ServiceClassEconomy: "", llm.ServiceClassStandard: "default", llm.ServiceClassPriority: "priority"},
				ActualServiceClasses: map[string]llm.ServiceClass{"default": llm.ServiceClassStandard, "priority": llm.ServiceClassPriority}})
			if err != nil {
				t.Fatal(err)
			}
			return adapter
		}},
	}
}

// configure points the single fixture route, its price and its estimator at
// the family under test.
func (route hierarchyRoute) configure(f *budgetPlanningFixture) {
	model := f.source.value.Routes.Models["alias"]
	model.Routes[0].Family = string(route.family)
	model.Routes[0].ProviderTiers = map[llm.ServiceClass]string{llm.ServiceClassStandard: route.tier}
	f.source.value.Routes.Models["alias"] = model
	f.entry.Family, f.entry.ProviderTier = string(route.family), route.tier
	f.estimator = budget.Estimator{MaxOutput: 16, SafetyRatio: big.NewRat(3, 2), Tokenizer: func(llm.Request, routing.Candidate) (int64, error) { return 10, nil }}
}

var hierarchyApplicationInstruction = llm.Instruction{Kind: llm.InstructionKindText, Level: llm.InstructionLevelApplication, Text: "You are a helpful assistant"}

// TestCompactionWithApplicationInstructionsSurvivesBudgetPlanningAndRecovery
// follows the cloud order for a Compact prepare: budget planning quotes the
// compiled candidate and exact-route recovery then rebuilds it from the bound
// digest. All three must agree on the flattened summarizer request.
func TestCompactionWithApplicationInstructionsSurvivesBudgetPlanningAndRecovery(t *testing.T) {
	for _, route := range hierarchyRoutes() {
		t.Run(route.name, func(t *testing.T) {
			f := newBudgetPlanningFixture(t)
			route.configure(f)
			f.prices(t, []pricing.Entry{f.entry})
			f.cap.Adapters = engine.AdapterMap{"endpoint": route.adapter(t)}
			_, _, _, _, replay, compact := planningFixture()
			replay.State.Settings.Instructions = []llm.Instruction{hierarchyApplicationInstruction}
			summary, err := PrepareCompactInput(context.Background(), compact, replay)
			if err != nil {
				t.Fatal(err)
			}
			summary.Request.Output.MaxTokens = preparationPointer(16)
			planned, err := f.planning(t).Compact(context.Background(), summary, f.attempt)
			if err != nil {
				t.Fatalf("BudgetPlanning.Compact: %#v", err)
			}
			if summary.Request.Instructions[0].Level != llm.InstructionLevelPolicy {
				t.Fatal("planning changed the semantic summarizer request")
			}
			binding, err := planned.Provider.RecoveryBinding(planned.Route)
			if err != nil {
				t.Fatal(err)
			}
			recovery, err := f.cap.NewProviderRecovery(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			recovered, err := recovery.Compact(context.Background(), summary, binding)
			if err != nil {
				t.Fatalf("ProviderRecovery.Compact: %#v", err)
			}
			if recovered.Call.Metadata.SchemaDigest != planned.Provider.Call.Metadata.SchemaDigest || recovered.Candidate.ID != planned.Provider.Candidate.ID {
				t.Fatal("recovery rebuilt a different call")
			}
			// The unflattened form passes the early input check but is not
			// what this endpoint compiles, so it must not recover.
			semantic, err := f.estimator.PrepareRequest(*summary.Request)
			if err != nil {
				t.Fatal(err)
			}
			other := binding
			other.RequestDigest, err = llm.RequestDigest(candidateRequest(semantic, providerStatePins{}, planned.Provider.Candidate))
			if err != nil || other.RequestDigest == binding.RequestDigest {
				t.Fatal("summarizer request was not flattened", err)
			}
			_, err = recovery.Compact(context.Background(), summary, other)
			assertRecoveryError(t, err, provider.CodeConfiguration, provider.RetryNever)

			// An ordinary request that mixes the levels is not worker-built
			// and keeps the adapter's strict rejection.
			f.generate.Request.Instructions = []llm.Instruction{
				{Kind: llm.InstructionKindText, Level: llm.InstructionLevelPolicy, Text: "policy"}, hierarchyApplicationInstruction}
			_, err = f.planning(t).Generate(context.Background(), f.generate, f.attempt)
			assertPlanningError(t, err, provider.CodeUnsupportedCapability)
		})
	}
}

// TestCloudExecutionRuntimeCompactionWithApplicationInstructions runs a whole
// budgeted compaction, including the restart before the provider step, on
// routes that need the summarizer instructions flattened.
func TestCloudExecutionRuntimeCompactionWithApplicationInstructions(t *testing.T) {
	for _, route := range hierarchyRoutes() {
		t.Run(route.name, func(t *testing.T) {
			f := boundedCloud(t, false, func(b *budgetPlanningFixture) {
				route.configure(b)
				b.prices(t, []pricing.Entry{b.entry})
			})
			// Compile with the real adapter; only the paid call is canned.
			f.cap.Adapters = engine.AdapterMap{"endpoint": hierarchyInvokeAdapter{Adapter: route.adapter(t), invoke: f.adapter.invoke}}
			f.restart(t)
			policy := json.RawMessage(`{"recent_turns":0}`)
			f.request.SettingsPatch.CompactionPolicy.Set = &policy
			f.request.SettingsPatch.Instructions.Set = &[]llm.Instruction{hierarchyApplicationInstruction}
			parent := f.finish(t)
			f.now = f.now.Add(time.Minute)
			request := llm.CompactRequestV1{OperationKey: "compact", Context: f.request.Context, Parent: parent.Generate.Checkpoint.Handle}
			ctx := context.Background()
			v, err := f.runtime.PrepareExecutionV1(ctx, llm.PrepareExecutionV1{Compact: &request})
			boundedState(t, v, err, llm.ExecutionBudgetRequired)
			ref := llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: request.Context}
			v, err = f.runtime.AcquireBudgetV1(ctx, ref)
			boundedState(t, v, err, llm.ExecutionAcquired)
			f.restart(t)
			v, err = f.runtime.CompactStepV1(ctx, request)
			boundedState(t, v, err, llm.ExecutionProviderCompleted)
			f.restart(t)
			v, err = f.runtime.CompleteExecutionV1(ctx, ref)
			boundedState(t, v, err, llm.ExecutionCompleted)
			if v.Compact == nil || f.submits.Load() != 2 {
				t.Fatalf("compaction did not dispatch exactly once: submits=%d", f.submits.Load())
			}
		})
	}
}

// hierarchyInvokeAdapter keeps the real adapter's capabilities, compilation
// and instruction-hierarchy answer while replacing the provider call.
type hierarchyInvokeAdapter struct {
	provider.Adapter
	invoke func(context.Context, provider.Call, provider.Observer) (provider.Result, error)
}

func (adapter hierarchyInvokeAdapter) Invoke(ctx context.Context, call provider.Call, observer provider.Observer) (provider.Result, error) {
	return adapter.invoke(ctx, call, observer)
}

func (adapter hierarchyInvokeAdapter) PreservesInstructionHierarchy() bool {
	reporter, ok := adapter.Adapter.(provider.InstructionHierarchyReporter)
	return !ok || reporter.PreservesInstructionHierarchy()
}
