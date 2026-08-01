//go:build live

// Package live contains opt-in provider contract checks. It is intentionally
// excluded from ordinary Go test runs: a real request is possible only after a
// protected workflow explicitly enables the suite and one named profile.
package live

import (
	"fmt"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
)

const (
	// EnableSuiteEnv is the first fail-closed gate for every live request.
	// Its exact value must be "1"; truthy aliases are deliberately unsupported.
	EnableSuiteEnv = "LLMTW_LIVE_TESTS"
	// AuthorizeEnv is set only by the protected manual release workflow after
	// human approval. It is separate from EnableSuiteEnv so a local compile or
	// scheduled workflow cannot accidentally spend money.
	AuthorizeEnv = "LLMTW_LIVE_AUTHORIZED"

	liveTenant = "llmtw-live-contract"
	livePrompt = "Reply with exactly: live-contract-ok"
	// liveMaxOutputTokens keeps an authorized probe small even if a provider
	// ignores the requested wording. It is deliberately below normal product
	// defaults and never comes from a provider-specific environment default.
	liveMaxOutputTokens = 8
	// 25,000 microUSD is USD 0.025. Each profile also requests at most a tiny
	// response, so this is an evidence ceiling rather than a provider default.
	liveCeilingMicroUSD int64 = 25_000
)

// CredentialSource identifies the only credential mechanism a profile may
// use. Environment values are resolved only after both authorization gates
// pass and are never put in test names, errors, evidence, or logs.
type CredentialSource struct {
	Kind        string
	Environment string
}

// ContinuationExpectation distinguishes a profile whose live response must
// yield a pinned continuation from one that must reject continuation before a
// provider request is sent.
type ContinuationExpectation string

const (
	ContinuationPinned      ContinuationExpectation = "pinned"
	ContinuationUnsupported ContinuationExpectation = "unsupported"
)

// Profile is the checked-in, non-secret contract for one supported endpoint
// profile. Model selection is closed: a protected workflow can enable a
// profile, but it cannot substitute a different model through an environment
// variable.
type Profile struct {
	ID                      string
	EnableEnv               string
	Model                   string
	AllowedModels           map[string]bool
	Tenant                  string
	MaxMicroUSD             int64
	Credential              CredentialSource
	Prompt                  string
	ServiceClass            llm.ServiceClass
	ContinuationExpectation ContinuationExpectation
}

var profiles = []Profile{
	newProfile("openai-responses", "LLMTW_LIVE_OPENAI_RESPONSES", "gpt-4.1-mini", CredentialSource{Kind: "bearer_env", Environment: "OPENAI_API_KEY"}, ContinuationPinned),
	newProfile("azure-responses", "LLMTW_LIVE_AZURE_RESPONSES", "gpt-4.1-mini", CredentialSource{Kind: "azure_default_credential"}, ContinuationPinned),
	newProfile("openai-chat", "LLMTW_LIVE_OPENAI_CHAT", "gpt-4.1-mini", CredentialSource{Kind: "bearer_env", Environment: "OPENAI_API_KEY"}, ContinuationUnsupported),
	newProfile("openrouter-chat", "LLMTW_LIVE_OPENROUTER_CHAT", "openai/gpt-4.1-mini", CredentialSource{Kind: "bearer_env", Environment: "OPENROUTER_API_KEY"}, ContinuationUnsupported),
	newProfile("exa-chat", "LLMTW_LIVE_EXA_CHAT", "exa", CredentialSource{Kind: "header_env", Environment: "EXA_API_KEY"}, ContinuationUnsupported),
	newProfile("anthropic-direct", "LLMTW_LIVE_ANTHROPIC_DIRECT", "claude-3-5-haiku-latest", CredentialSource{Kind: "header_env", Environment: "ANTHROPIC_API_KEY"}, ContinuationPinned),
	newProfile("anthropic-aws", "LLMTW_LIVE_ANTHROPIC_AWS", "claude-3-5-haiku-latest", CredentialSource{Kind: "aws_default_chain"}, ContinuationPinned),
	newProfile("bedrock-anthropic", "LLMTW_LIVE_BEDROCK_ANTHROPIC", "anthropic.claude-3-5-haiku-20241022-v1:0", CredentialSource{Kind: "aws_default_chain"}, ContinuationPinned),
	newProfile("bedrock-converse", "LLMTW_LIVE_BEDROCK_CONVERSE", "amazon.nova-pro-v1:0", CredentialSource{Kind: "aws_default_chain"}, ContinuationUnsupported),
}

func newProfile(id, enableEnv, model string, credential CredentialSource, continuation ContinuationExpectation) Profile {
	return Profile{
		ID:                      id,
		EnableEnv:               enableEnv,
		Model:                   model,
		AllowedModels:           map[string]bool{model: true},
		Tenant:                  liveTenant,
		MaxMicroUSD:             liveCeilingMicroUSD,
		Credential:              credential,
		Prompt:                  livePrompt,
		ServiceClass:            llm.ServiceClassStandard,
		ContinuationExpectation: continuation,
	}
}

// Profiles returns defensive copies so a test cannot mutate the registered
// model allow-list used by a later subtest.
func Profiles() []Profile {
	result := make([]Profile, 0, len(profiles))
	for _, profile := range profiles {
		copy := profile
		copy.AllowedModels = make(map[string]bool, len(profile.AllowedModels))
		for model, allowed := range profile.AllowedModels {
			copy.AllowedModels[model] = allowed
		}
		result = append(result, copy)
	}
	return result
}

// authorize evaluates only non-secret explicit gates. In particular, it does
// not inspect an API key or ask a cloud SDK for credentials; callers must run
// it before constructing an adapter or resolving a CredentialSource.
func authorize(profile Profile, lookup func(string) (string, bool)) (bool, string) {
	if !enabled(lookup, EnableSuiteEnv) {
		return false, fmt.Sprintf("set %s=1 in the protected manual workflow", EnableSuiteEnv)
	}
	if !enabled(lookup, AuthorizeEnv) {
		return false, fmt.Sprintf("set %s=1 after protected-environment approval", AuthorizeEnv)
	}
	if !enabled(lookup, profile.EnableEnv) {
		return false, fmt.Sprintf("set %s=1 to enable profile %s", profile.EnableEnv, profile.ID)
	}
	return true, ""
}

func enabled(lookup func(string) (string, bool), name string) bool {
	if lookup == nil || name == "" {
		return false
	}
	value, ok := lookup(name)
	return ok && value == "1"
}

// requestFor uses an omitted ServiceClass on purpose. The public API's only
// omission behavior is standard; the adapter must never turn that omission
// into a provider-default class. The rest of the request is fixed so a
// protected workflow can reproduce one bounded probe per enabled profile.
func requestFor(profile Profile) llm.Request {
	maxTokens := liveMaxOutputTokens
	return llm.Request{
		OperationKey: "live-contract-" + profile.ID,
		Context: llm.RequestContext{
			Tenant:  profile.Tenant,
			Project: "provider-contract",
			Actor:   "protected-release",
		},
		Model: profile.Model,
		Input: []llm.Item{llm.Message{
			Actor:   llm.ActorHuman,
			Content: []llm.Part{llm.TextPart{Text: profile.Prompt}},
		}},
		Output: &llm.OutputSpec{
			MaxTokens: &maxTokens,
			Format:    llm.OutputFormat{Kind: llm.OutputKindText},
		},
	}
}

// Evidence is the intentionally small release-evidence record produced for a
// successful protected probe. It contains no prompt, output, raw provider
// payload, credential, endpoint, or pricing-catalog data. A zero
// ActualMicroUSD is meaningful only when ActualSpendKnown is true.
type Evidence struct {
	Profile              string
	Tenant               string
	CeilingMicroUSD      int64
	ActualSpendKnown     bool
	ActualMicroUSD       int64
	CostMethod           string
	RequestID            string
	ResponseID           string
	ActualServiceClass   llm.ServiceClass
	ContinuationVerified bool
}

// validateResponse keeps the live suite focused on facts a mock cannot prove.
// It deliberately does not infer a cost from a pricing catalog, refresh
// capabilities, or write any state when a provider fails to report a fact.
func validateResponse(profile Profile, response llm.Response) (Evidence, error) {
	if response.Status != llm.ResponseStatusCompleted {
		return Evidence{}, fmt.Errorf("profile %s response status %q is not completed", profile.ID, response.Status)
	}
	if response.Provider.RequestID == "" {
		return Evidence{}, fmt.Errorf("profile %s response is missing a request ID", profile.ID)
	}
	if response.Provider.ResponseID == "" {
		return Evidence{}, fmt.Errorf("profile %s response is missing a response ID", profile.ID)
	}
	if response.Service.Requested != profile.ServiceClass || response.Service.Attempted != profile.ServiceClass {
		return Evidence{}, fmt.Errorf("profile %s service class was not normalized to %q", profile.ID, profile.ServiceClass)
	}
	if response.Service.Actual == nil || !response.Service.Actual.Valid() {
		return Evidence{}, fmt.Errorf("profile %s response is missing a valid actual service class", profile.ID)
	}
	if *response.Service.Actual != profile.ServiceClass {
		return Evidence{}, fmt.Errorf("profile %s actual service class %q does not match requested %q", profile.ID, *response.Service.Actual, profile.ServiceClass)
	}
	if response.Usage.InputTokens <= 0 || response.Usage.OutputTokens <= 0 {
		return Evidence{}, fmt.Errorf("profile %s response has incomplete usage", profile.ID)
	}
	if err := validateContinuation(profile, response.Continuation); err != nil {
		return Evidence{}, err
	}

	evidence := Evidence{
		Profile:              profile.ID,
		Tenant:               profile.Tenant,
		CeilingMicroUSD:      profile.MaxMicroUSD,
		RequestID:            response.Provider.RequestID,
		ResponseID:           response.Provider.ResponseID,
		ActualServiceClass:   *response.Service.Actual,
		ContinuationVerified: profile.ContinuationExpectation == ContinuationPinned,
	}
	known, err := reportedCost(response.Cost)
	if err != nil {
		return Evidence{}, fmt.Errorf("profile %s cost: %w", profile.ID, err)
	}
	if !known {
		evidence.CostMethod = "not_reported"
		return evidence, nil
	}
	micro, err := costMicroUSD(response.Cost)
	if err != nil {
		return Evidence{}, fmt.Errorf("profile %s cost materialization: %w", profile.ID, err)
	}
	if micro > profile.MaxMicroUSD {
		return Evidence{}, fmt.Errorf("profile %s actual cost %d microUSD exceeds ceiling %d", profile.ID, micro, profile.MaxMicroUSD)
	}
	evidence.ActualSpendKnown = true
	evidence.ActualMicroUSD = micro
	evidence.CostMethod = response.Cost.Method
	return evidence, nil
}

func validateContinuation(profile Profile, continuation *llm.Continuation) error {
	switch profile.ContinuationExpectation {
	case ContinuationPinned:
		if continuation == nil || !continuation.Pinned || continuation.Handle == "" {
			return fmt.Errorf("profile %s did not return a pinned continuation", profile.ID)
		}
		if continuation.EndpointID != profile.ID || continuation.Model != profile.Model {
			return fmt.Errorf("profile %s continuation is not pinned to the enabled endpoint and model", profile.ID)
		}
	case ContinuationUnsupported:
		if continuation != nil {
			return fmt.Errorf("profile %s must not return a continuation", profile.ID)
		}
	default:
		return fmt.Errorf("profile %s has invalid continuation expectation %q", profile.ID, profile.ContinuationExpectation)
	}
	return nil
}

func reportedCost(cost llm.Cost) (bool, error) {
	if cost.Status != "" && !cost.Status.Valid() {
		return false, fmt.Errorf("status %q is invalid", cost.Status)
	}
	if cost.Status == llm.CostStatusUnknown {
		if cost.Method != "" || cost.ActualCostUSD != nil {
			return false, fmt.Errorf("unknown status contains reported cost fields")
		}
		return false, nil
	}
	known := cost.Status == llm.CostStatusKnown || cost.Method != "" || cost.ActualCostUSD != nil
	if !known {
		return false, nil
	}
	if cost.ActualCostUSD == nil {
		return false, fmt.Errorf("reported cost has no exact USD amount")
	}
	if cost.Method == "" {
		return false, fmt.Errorf("reported cost has no method")
	}
	return true, nil
}

func costMicroUSD(cost llm.Cost) (int64, error) {
	if cost.ActualCostUSD == nil {
		return 0, fmt.Errorf("actual cost is unknown")
	}
	micro, err := pricing.CeilMicroFromUSD(*cost.ActualCostUSD)
	if err != nil {
		return 0, err
	}
	return int64(micro), nil
}
