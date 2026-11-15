//go:build live

package live

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/runtime"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/anthropicmessages"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/bedrockconverse"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/bedrockmessages"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/openaichat"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/openairesponses"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	anthropicaws "github.com/anthropics/anthropic-sdk-go/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
)

const (
	liveOpenAIBaseURL                    = "https://api.openai.com/v1"
	liveOpenRouterBaseURL                = "https://openrouter.ai/api/v1/"
	liveExaBaseURL                       = "https://api.exa.ai/"
	liveAnthropicBaseURL                 = "https://api.anthropic.com/"
	liveAnthropicAWSBaseURL              = "https://aws-external-anthropic.us-east-1.api.aws/"
	liveAzureEndpointEnv                 = "LLMTW_LIVE_AZURE_OPENAI_ENDPOINT"
	liveAnthropicAWSWorkspaceEnvironment = "LLMTW_LIVE_ANTHROPIC_AWS_WORKSPACE_ID"
	liveAzureAPIVersion                  = "2024-10-21"
	liveAWSRegion                        = "us-east-1"
	liveHTTPTimeout                      = 45 * time.Second
)

var liveAzureOpenAIHostSuffixes = []string{
	".openai.azure.com",
	".openai.azure.us",
	".openai.azure.cn",
}

func familyFor(profile Profile) provider.Family {
	switch profile.ID {
	case "openai-responses", "azure-responses":
		return provider.FamilyOpenAIResponses
	case "openai-chat", "openrouter-chat", "exa-chat":
		return provider.FamilyOpenAIChat
	case "anthropic-direct", "anthropic-aws":
		return provider.FamilyAnthropicMessages
	case "bedrock-anthropic":
		return provider.FamilyBedrockMessages
	case "bedrock-converse":
		return provider.FamilyBedrockConverse
	default:
		return ""
	}
}

func chatProfileFor(liveProfile Profile) (openaichat.Profile, error) {
	capabilities := liveChatCapabilities("live-" + liveProfile.ID + "/v1")
	switch liveProfile.ID {
	case "openai-chat":
		return openaichat.NewOpenAIProfile(openaichat.Profile{
			ID:                liveProfile.ID,
			CapabilityVersion: capabilities.Version,
			Capabilities:      capabilities,
			ServiceTiers:      liveStandardServiceTiers("default"),
			ActualServiceClasses: map[string]llm.ServiceClass{
				"default": llm.ServiceClassStandard,
			},
			ExpectedBaseURL: liveOpenAIBaseURL,
			ExpectedModel:   liveProfile.Model,
		})
	case "openrouter-chat":
		return openaichat.NewOpenRouterProfile(openRouterProfileConfig(liveProfile))
	case "exa-chat":
		return openaichat.NewExaProfile(exaProfileConfig(liveProfile))
	default:
		return openaichat.Profile{}, fmt.Errorf("live profile has no chat contract")
	}
}

// liveStandardServiceTiers maps only the standard public class, to the given
// provider tier; economy and priority stay explicitly unmapped.
func liveStandardServiceTiers(standardTier string) map[llm.ServiceClass]string {
	return map[llm.ServiceClass]string{
		llm.ServiceClassEconomy:  "",
		llm.ServiceClassStandard: standardTier,
		llm.ServiceClassPriority: "",
	}
}

func liveChatCapabilities(version string) provider.CapabilitySet {
	features := map[provider.Feature]provider.Capability{
		provider.FeatureText:             {State: provider.CapabilityNative},
		provider.FeatureImage:            {State: provider.CapabilityUnsupported},
		provider.FeatureDocument:         {State: provider.CapabilityUnsupported},
		provider.FeatureToolCall:         {State: provider.CapabilityUnsupported},
		provider.FeatureStructuredOutput: {State: provider.CapabilityUnsupported},
		provider.FeatureReasoning:        {State: provider.CapabilityUnsupported},
		provider.FeatureContinuation:     {State: provider.CapabilityUnsupported},
		provider.FeatureStreaming:        {State: provider.CapabilityUnsupported},
		provider.FeatureUsage:            {State: provider.CapabilityNative},
	}
	return provider.CapabilitySet{Version: version, Features: features}
}

func compileProfile(ctx context.Context, adapter provider.Adapter, profile Profile, request llm.Request) (provider.Call, error) {
	if adapter == nil {
		return provider.Call{}, fmt.Errorf("live profile adapter is unavailable")
	}
	serviceClass, err := llm.NormalizeServiceClass(request.ServiceClass)
	if err != nil {
		return provider.Call{}, fmt.Errorf("live profile service class is invalid")
	}
	query := provider.CapabilityQuery{
		EndpointID:   profile.ID,
		Family:       familyFor(profile),
		Model:        profile.Model,
		ServiceClass: serviceClass,
	}
	capability, err := adapter.Capabilities(ctx, query)
	if err != nil {
		return provider.Call{}, fmt.Errorf("live profile capability resolution failed")
	}
	call, err := adapter.Compile(ctx, provider.CompileInput{
		Request:    request,
		Query:      query,
		Capability: capability,
		Strict:     true,
	})
	if err != nil {
		return provider.Call{}, fmt.Errorf("live profile compile failed")
	}
	return call, nil
}

// validateCompiledCall checks the provider-neutral call envelope before the
// adapter is allowed to make a real request. The individual adapter owns its
// SDK parameters, but it must not be able to change the pinned endpoint,
// protocol, model, operation key, or normalized public service class.
func validateCompiledCall(profile Profile, request llm.Request, call provider.Call) error {
	serviceClass, err := llm.NormalizeServiceClass(request.ServiceClass)
	if err != nil || serviceClass != profile.ServiceClass || familyFor(profile) == "" {
		return fmt.Errorf("live profile compiled wire does not match the pinned contract")
	}
	if call.EndpointID != profile.ID || call.Family != familyFor(profile) || call.Model != profile.Model || call.OperationKey != request.OperationKey || call.ServiceClass != serviceClass {
		return fmt.Errorf("live profile compiled wire does not match the pinned contract")
	}
	if strings.TrimSpace(call.Metadata.ProviderTier) == "" {
		return fmt.Errorf("live profile compiled wire does not declare a provider tier")
	}
	return nil
}

func preflightUnsupportedContinuation(ctx context.Context, adapter provider.Adapter, profile Profile) error {
	if profile.ContinuationExpectation != ContinuationUnsupported {
		return fmt.Errorf("live profile does not require an unsupported continuation probe")
	}
	request := requestFor(profile)
	request.Continuation = &llm.Continuation{
		Handle:     "live-contract-continuation-probe",
		EndpointID: profile.ID,
		Model:      profile.Model,
		Pinned:     true,
	}
	if _, err := compileProfile(ctx, adapter, profile, request); err == nil {
		return fmt.Errorf("live profile accepted an unsupported continuation")
	}
	return nil
}

func runWithAdapter(ctx context.Context, adapter provider.Adapter, profile Profile) (Evidence, error) {
	request := requestFor(profile)
	call, err := compileProfile(ctx, adapter, profile, request)
	if err != nil {
		return Evidence{}, err
	}
	if err := validateCompiledCall(profile, request, call); err != nil {
		return Evidence{}, err
	}
	if profile.ContinuationExpectation == ContinuationUnsupported {
		if err := preflightUnsupportedContinuation(ctx, adapter, profile); err != nil {
			return Evidence{}, err
		}
	}
	result, err := adapter.Invoke(ctx, call, provider.NopObserver{})
	if err != nil {
		return Evidence{}, fmt.Errorf("live provider call failed")
	}
	evidence, err := validateResponse(profile, result.Response)
	if err != nil {
		return Evidence{}, fmt.Errorf("live provider response failed contract")
	}
	return evidence, nil
}

type adapterFactory func(context.Context, Profile, func(string) (string, bool)) (provider.Adapter, error)

func runProfileWithFactory(ctx context.Context, profile Profile, lookup func(string) (string, bool), factory adapterFactory) (Evidence, error) {
	allowed, _ := authorize(profile, lookup)
	if !allowed {
		return Evidence{}, fmt.Errorf("live profile is not explicitly authorized")
	}
	if factory == nil {
		return Evidence{}, fmt.Errorf("live profile adapter factory is unavailable")
	}
	adapter, err := factory(ctx, profile, lookup)
	if err != nil || adapter == nil {
		return Evidence{}, fmt.Errorf("live profile adapter construction failed")
	}
	return runWithAdapter(ctx, adapter, profile)
}

func runProfile(ctx context.Context, profile Profile, lookup func(string) (string, bool)) (Evidence, error) {
	return runProfileWithFactory(ctx, profile, lookup, adapterFor)
}

func adapterFor(ctx context.Context, candidate Profile, lookup func(string) (string, bool)) (provider.Adapter, error) {
	return adapterForWithAzureDependencies(ctx, candidate, lookup, defaultAzureAdapterDependencies())
}

type azureAdapterDependencies struct {
	newAzureEgressHTTPClient func(config.EndpointConfig) (*http.Client, error)
	newAzureCredential       func() (azcore.TokenCredential, error)
	newAzureTokenClient      func(openairesponses.AzureTokenClientConfig) (*openairesponses.Client, error)
}

func defaultAzureAdapterDependencies() azureAdapterDependencies {
	return azureAdapterDependencies{
		newAzureEgressHTTPClient: func(endpoint config.EndpointConfig) (*http.Client, error) {
			return runtime.NewProviderEgressHTTPClient(&http.Client{Timeout: liveHTTPTimeout}, endpoint)
		},
		newAzureCredential: func() (azcore.TokenCredential, error) {
			return azidentity.NewDefaultAzureCredential(nil)
		},
		newAzureTokenClient: openairesponses.NewAzureTokenClient,
	}
}

func adapterForWithAzureDependencies(ctx context.Context, candidate Profile, lookup func(string) (string, bool), azureDependencies azureAdapterDependencies) (provider.Adapter, error) {
	profile, ok := pinnedProfile(candidate)
	if !ok {
		return nil, fmt.Errorf("live profile is not pinned")
	}
	switch profile.ID {
	case "openai-responses":
		key, err := credentialFor(profile, lookup)
		if err != nil {
			return nil, err
		}
		client, err := openairesponses.NewClient(openairesponses.ClientConfig{BaseURL: liveOpenAIBaseURL, APIKey: key, HTTPClient: newLiveHTTPClient()})
		return liveAdapter(client, err, func(client *openairesponses.Client) (provider.Adapter, error) {
			return openairesponses.NewOpenAIAdapter(client, profile.ID, "live-openai-responses/v1")
		})
	case "azure-responses":
		endpoint, err := requiredEnvironment(lookup, liveAzureEndpointEnv)
		if err != nil {
			return nil, err
		}
		endpointConfig, err := liveAzureOpenAIEndpointConfig(endpoint)
		if err != nil {
			return nil, err
		}
		if azureDependencies.newAzureEgressHTTPClient == nil || azureDependencies.newAzureCredential == nil || azureDependencies.newAzureTokenClient == nil {
			return nil, fmt.Errorf("live adapter construction failed")
		}
		httpClient, err := azureDependencies.newAzureEgressHTTPClient(endpointConfig)
		if err != nil {
			return nil, fmt.Errorf("live adapter construction failed")
		}
		credential, err := azureDependencies.newAzureCredential()
		if err != nil {
			return nil, fmt.Errorf("live adapter construction failed")
		}
		client, err := azureDependencies.newAzureTokenClient(openairesponses.AzureTokenClientConfig{Endpoint: endpointConfig.BaseURL, APIVersion: liveAzureAPIVersion, TokenCredential: credential, HTTPClient: httpClient})
		return liveAdapter(client, err, func(client *openairesponses.Client) (provider.Adapter, error) {
			return openairesponses.NewAzureAdapter(client, profile.ID, "live-azure-responses/v1")
		})
	case "openai-chat":
		key, err := credentialFor(profile, lookup)
		if err != nil {
			return nil, err
		}
		chatProfile, err := chatProfileFor(profile)
		if err != nil {
			return nil, fmt.Errorf("live adapter construction failed")
		}
		client, err := openaichat.NewClient(openaichat.ClientConfig{BaseURL: liveOpenAIBaseURL, APIKey: key, HTTPClient: newLiveHTTPClient()})
		return liveAdapter(client, err, func(client *openaichat.Client) (provider.Adapter, error) {
			return openaichat.NewOpenAIAdapter(client, profile.ID, chatProfile)
		})
	case "openrouter-chat":
		key, err := credentialFor(profile, lookup)
		if err != nil {
			return nil, err
		}
		client, err := openaichat.NewOpenRouterClient(openaichat.OpenRouterClientConfig{BaseURL: liveOpenRouterBaseURL, APIKey: key, HTTPClient: newLiveHTTPClient()})
		return liveAdapter(client, err, func(client *openaichat.Client) (provider.Adapter, error) {
			return openaichat.NewOpenRouterAdapter(client, profile.ID, openRouterProfileConfig(profile))
		})
	case "exa-chat":
		key, err := credentialFor(profile, lookup)
		if err != nil {
			return nil, err
		}
		client, err := openaichat.NewExaClient(openaichat.ExaClientConfig{BaseURL: liveExaBaseURL, APIKey: key, HTTPClient: newLiveHTTPClient()})
		return liveAdapter(client, err, func(client *openaichat.Client) (provider.Adapter, error) {
			return openaichat.NewExaAdapter(client, profile.ID, exaProfileConfig(profile))
		})
	case "anthropic-direct":
		key, err := credentialFor(profile, lookup)
		if err != nil {
			return nil, err
		}
		client, err := anthropicmessages.NewClient(anthropicmessages.ClientConfig{BaseURL: liveAnthropicBaseURL, APIKey: key, HTTPClient: newLiveHTTPClient()})
		return liveAdapter(client, err, func(client *anthropicmessages.Client) (provider.Adapter, error) {
			return anthropicmessages.NewAdapter(client, profile.ID, anthropicProfile(profile, liveAnthropicBaseURL))
		})
	case "anthropic-aws":
		workspace, err := requiredEnvironment(lookup, liveAnthropicAWSWorkspaceEnvironment)
		if err != nil {
			return nil, err
		}
		client, err := anthropicmessages.NewAWSGatewayClient(ctx, anthropicmessages.AWSClientConfig{
			BaseURL:    liveAnthropicAWSBaseURL,
			HTTPClient: newLiveHTTPClient(),
			AWSConfig:  anthropicaws.ClientConfig{AWSRegion: liveAWSRegion, WorkspaceID: workspace},
		})
		return liveAdapter(client, err, func(client *anthropicmessages.Client) (provider.Adapter, error) {
			return anthropicmessages.NewAdapter(client, profile.ID, anthropicProfile(profile, liveAnthropicAWSBaseURL))
		})
	case "bedrock-anthropic":
		config, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(liveAWSRegion))
		if err != nil {
			return nil, fmt.Errorf("live adapter construction failed")
		}
		client, err := bedrockmessages.NewClient(ctx, bedrockmessages.ClientConfig{HTTPClient: newLiveHTTPClient(), AWSConfig: config})
		return liveAdapter(client, err, func(client *bedrockmessages.Client) (provider.Adapter, error) {
			profileConfig := bedrockmessages.DefaultProfile(profile.ID)
			profileConfig.ExpectedModel = profile.Model
			return bedrockmessages.NewAdapter(client, profile.ID, profileConfig)
		})
	case "bedrock-converse":
		config, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(liveAWSRegion))
		if err != nil {
			return nil, fmt.Errorf("live adapter construction failed")
		}
		client, err := bedrockconverse.NewClient(ctx, bedrockconverse.ClientConfig{HTTPClient: newLiveHTTPClient(), AWSConfig: config})
		return liveAdapter(client, err, func(client *bedrockconverse.Client) (provider.Adapter, error) {
			profileConfig := bedrockconverse.DefaultProfile(profile.ID)
			profileConfig.ExpectedModel = profile.Model
			return bedrockconverse.NewAdapter(client, profile.ID, profileConfig)
		})
	default:
		return nil, fmt.Errorf("live profile is not supported")
	}
}

// liveAdapter finishes the client-then-adapter construction every live
// profile shares. Either failure collapses to the same content-free error, so
// SDK construction details never reach a credentialed test log.
func liveAdapter[C any](client C, clientErr error, newAdapter func(C) (provider.Adapter, error)) (provider.Adapter, error) {
	if clientErr != nil {
		return nil, fmt.Errorf("live adapter construction failed")
	}
	adapter, err := newAdapter(client)
	if err != nil {
		return nil, fmt.Errorf("live adapter construction failed")
	}
	return adapter, nil
}

// liveAzureOpenAIEndpointConfig accepts only HTTPS Azure OpenAI resource
// endpoints. The subsequent client construction uses the production egress
// guard, so this identity check cannot turn a caller-controlled hostname into
// an allowed destination or bypass address and redirect policy.
func liveAzureOpenAIEndpointConfig(value string) (config.EndpointConfig, error) {
	endpoint, err := url.ParseRequestURI(value)
	if err != nil || endpoint == nil || !strings.EqualFold(endpoint.Scheme, "https") || endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" {
		return config.EndpointConfig{}, fmt.Errorf("live Azure endpoint is not an approved Azure OpenAI endpoint")
	}
	if port := endpoint.Port(); port != "" && port != "443" {
		return config.EndpointConfig{}, fmt.Errorf("live Azure endpoint is not an approved Azure OpenAI endpoint")
	}
	host, err := config.NormalizeOutboundHost(endpoint.Hostname())
	if err != nil || !isLiveAzureOpenAIHost(host) {
		return config.EndpointConfig{}, fmt.Errorf("live Azure endpoint is not an approved Azure OpenAI endpoint")
	}
	return config.EndpointConfig{
		BaseURL:       endpoint.String(),
		OutboundHosts: []string{host},
		Timeout:       config.Duration(liveHTTPTimeout),
	}, nil
}

func isLiveAzureOpenAIHost(host string) bool {
	for _, suffix := range liveAzureOpenAIHostSuffixes {
		if len(host) > len(suffix) && strings.HasSuffix(host, suffix) {
			return true
		}
	}
	return false
}

func openRouterProfileConfig(profile Profile) openaichat.OpenRouterProfileConfig {
	capabilities := liveChatCapabilities("live-" + profile.ID + "/v1")
	return openaichat.OpenRouterProfileConfig{
		ID:                profile.ID,
		CapabilityVersion: capabilities.Version,
		BaseURL:           liveOpenRouterBaseURL,
		Model:             profile.Model,
		Capabilities:      capabilities,
		ServiceTiers:      liveStandardServiceTiers("standard"),
		ActualServiceClasses: map[string]llm.ServiceClass{
			"default":  llm.ServiceClassStandard,
			"standard": llm.ServiceClassStandard,
		},
		ProviderOrder:     []string{"openai"},
		AllowFallbacks:    false,
		RequireParameters: true,
		Upstreams:         liveOpenRouterUpstreams{},
	}
}

// liveOpenRouterUpstreams allows the first-party OpenAI endpoint for every
// live OpenRouter model, matching the pinned provider order.
type liveOpenRouterUpstreams struct{}

func (liveOpenRouterUpstreams) OpenRouterUpstream(string) (openaichat.OpenRouterUpstream, bool) {
	return openaichat.OpenRouterUpstream{Tags: []string{"openai"}}, true
}

func exaProfileConfig(profile Profile) openaichat.ExaProfileConfig {
	capabilities := liveChatCapabilities("live-" + profile.ID + "/v1")
	return openaichat.ExaProfileConfig{
		ID:                profile.ID,
		CapabilityVersion: capabilities.Version,
		BaseURL:           liveExaBaseURL,
		Model:             profile.Model,
		Capabilities:      capabilities,
		ServiceTiers:      liveStandardServiceTiers("standard"),
		ActualServiceClasses: map[string]llm.ServiceClass{
			"standard": llm.ServiceClassStandard,
		},
	}
}

func anthropicProfile(profile Profile, baseURL string) anthropicmessages.Profile {
	result := anthropicmessages.DefaultProfile(profile.ID)
	result.ExpectedBaseURL = baseURL
	result.ExpectedModel = profile.Model
	return result
}

func credentialFor(profile Profile, lookup func(string) (string, bool)) (string, error) {
	if profile.Credential.Environment == "" {
		return "", fmt.Errorf("live profile credential source is unavailable")
	}
	return requiredEnvironment(lookup, profile.Credential.Environment)
}

func requiredEnvironment(lookup func(string) (string, bool), name string) (string, error) {
	if lookup == nil || name == "" {
		return "", fmt.Errorf("live profile environment is unavailable")
	}
	value, ok := lookup(name)
	if !ok || strings.TrimSpace(value) == "" {
		return "", fmt.Errorf("live profile environment is unavailable")
	}
	return strings.TrimSpace(value), nil
}

func pinnedProfile(candidate Profile) (Profile, bool) {
	for _, profile := range Profiles() {
		if candidate.ID != profile.ID {
			continue
		}
		if candidate.EnableEnv != profile.EnableEnv || candidate.Model != profile.Model || candidate.Tenant != profile.Tenant || candidate.MaxMicroUSD != profile.MaxMicroUSD || candidate.Credential != profile.Credential || candidate.Prompt != profile.Prompt || candidate.ServiceClass != profile.ServiceClass || candidate.ContinuationExpectation != profile.ContinuationExpectation || !sameAllowedModels(candidate.AllowedModels, profile.AllowedModels) {
			return Profile{}, false
		}
		return profile, true
	}
	return Profile{}, false
}

func sameAllowedModels(left, right map[string]bool) bool {
	if len(left) != len(right) {
		return false
	}
	for model, allowed := range left {
		if right[model] != allowed {
			return false
		}
	}
	return true
}

func newLiveHTTPClient() *http.Client {
	return &http.Client{Timeout: liveHTTPTimeout}
}
