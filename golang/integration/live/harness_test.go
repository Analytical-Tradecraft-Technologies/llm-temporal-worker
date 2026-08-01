//go:build live

package live

import (
	"strings"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
)

func TestProfilesDeclareTheGuardedLiveContract(t *testing.T) {
	profiles := Profiles()
	wantIDs := map[string]struct{}{
		"openai-responses":  {},
		"azure-responses":   {},
		"openai-chat":       {},
		"openrouter-chat":   {},
		"exa-chat":          {},
		"anthropic-direct":  {},
		"anthropic-aws":     {},
		"bedrock-anthropic": {},
		"bedrock-converse":  {},
	}
	if len(profiles) != len(wantIDs) {
		t.Fatalf("live profile count = %d, want %d", len(profiles), len(wantIDs))
	}
	for _, profile := range profiles {
		if _, ok := wantIDs[profile.ID]; !ok {
			t.Fatalf("unexpected live profile %q", profile.ID)
		}
		delete(wantIDs, profile.ID)
		if profile.EnableEnv == "" {
			t.Errorf("%s enable flag is empty", profile.ID)
		}
		if profile.Model == "" || !profile.AllowedModels[profile.Model] {
			t.Errorf("%s model %q is not allow-listed", profile.ID, profile.Model)
		}
		if profile.Tenant == "" {
			t.Errorf("%s tenant is empty", profile.ID)
		}
		if profile.MaxMicroUSD <= 0 {
			t.Errorf("%s maximum microUSD = %d, want positive", profile.ID, profile.MaxMicroUSD)
		}
		if profile.Credential.Kind == "" {
			t.Errorf("%s credential source is empty", profile.ID)
		}
		if profile.Prompt == "" {
			t.Errorf("%s deterministic prompt is empty", profile.ID)
		}
		if profile.ServiceClass != llm.ServiceClassStandard {
			t.Errorf("%s service class = %q, want %q", profile.ID, profile.ServiceClass, llm.ServiceClassStandard)
		}
	}
	if len(wantIDs) != 0 {
		t.Errorf("missing live profiles: %v", wantIDs)
	}
}

func TestAuthorizationFailsClosedBeforeCredentialResolution(t *testing.T) {
	profile := Profiles()[0]
	tests := []struct {
		name string
		env  map[string]string
		want string
	}{
		{name: "suite disabled", want: EnableSuiteEnv},
		{name: "human authorization missing", env: map[string]string{EnableSuiteEnv: "1"}, want: AuthorizeEnv},
		{name: "profile disabled", env: map[string]string{EnableSuiteEnv: "1", AuthorizeEnv: "1"}, want: profile.EnableEnv},
		{name: "all explicit gates", env: map[string]string{EnableSuiteEnv: "1", AuthorizeEnv: "1", profile.EnableEnv: "1"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lookedUpCredential := false
			lookup := func(name string) (string, bool) {
				if name == profile.Credential.Environment {
					lookedUpCredential = true
				}
				value, ok := test.env[name]
				return value, ok
			}
			allowed, reason := authorize(profile, lookup)
			if lookedUpCredential {
				t.Fatalf("authorization looked up credential %q", profile.Credential.Environment)
			}
			if test.want == "" {
				if !allowed || reason != "" {
					t.Fatalf("authorization = (%t, %q), want allowed", allowed, reason)
				}
				return
			}
			if allowed || !strings.Contains(reason, test.want) {
				t.Fatalf("authorization = (%t, %q), want rejection mentioning %q", allowed, reason, test.want)
			}
		})
	}
}

func TestRequestOmitsServiceClassAndBoundsTheCall(t *testing.T) {
	for _, profile := range Profiles() {
		t.Run(profile.ID, func(t *testing.T) {
			request := requestFor(profile)
			if request.ServiceClass != "" {
				t.Fatalf("service class = %q, want omission so the public default is exercised", request.ServiceClass)
			}
			class, err := llm.NormalizeServiceClass(request.ServiceClass)
			if err != nil {
				t.Fatalf("normalize omitted service class: %v", err)
			}
			if class != llm.ServiceClassStandard {
				t.Fatalf("omitted service class = %q, want %q", class, llm.ServiceClassStandard)
			}
			if request.Model != profile.Model || !profile.AllowedModels[request.Model] {
				t.Fatalf("request model %q is not profile %s allow-listed model", request.Model, profile.ID)
			}
			if request.Context.Tenant != profile.Tenant {
				t.Fatalf("request tenant = %q, want %q", request.Context.Tenant, profile.Tenant)
			}
			if request.Output == nil || request.Output.MaxTokens == nil || *request.Output.MaxTokens != liveMaxOutputTokens {
				t.Fatalf("request output limit = %#v, want exactly %d", request.Output, liveMaxOutputTokens)
			}
			if len(request.Input) != 1 {
				t.Fatalf("request input count = %d, want one tiny deterministic prompt", len(request.Input))
			}
			message, ok := request.Input[0].(llm.Message)
			if !ok || len(message.Content) != 1 {
				t.Fatalf("request input = %#v, want one human text message", request.Input[0])
			}
			part, ok := message.Content[0].(llm.TextPart)
			if !ok || part.Text != livePrompt {
				t.Fatalf("request prompt = %#v, want the fixed contract prompt", message.Content[0])
			}
		})
	}
}

func TestValidateResponseCapturesReportedLiveFacts(t *testing.T) {
	profile := Profiles()[0]
	actual := llm.ServiceClassStandard
	response := llm.Response{
		Status: llm.ResponseStatusCompleted,
		Service: llm.ServiceFacts{
			Requested: llm.ServiceClassStandard,
			Attempted: llm.ServiceClassStandard,
			Actual:    &actual,
		},
		Usage: llm.Usage{InputTokens: 3, OutputTokens: 2},
		Cost: llm.Cost{
			Status:        llm.CostStatusKnown,
			ActualCostUSD: func() *pricing.USD { value := pricing.MustUSD("0.000123"); return &value }(),
			Method:        "provider_reported",
		},
		Provider: llm.ProviderFacts{RequestID: "request-123", ResponseID: "response-123"},
		Continuation: &llm.Continuation{
			Handle:     "continuation-123",
			EndpointID: profile.ID,
			Model:      profile.Model,
			Pinned:     true,
		},
	}

	evidence, err := validateResponse(profile, response)
	if err != nil {
		t.Fatalf("validate response: %v", err)
	}
	if evidence.Profile != profile.ID || evidence.Tenant != profile.Tenant {
		t.Fatalf("evidence profile = %#v, want %q/%q", evidence, profile.ID, profile.Tenant)
	}
	if evidence.RequestID != response.Provider.RequestID || evidence.ActualServiceClass != actual {
		t.Fatalf("evidence provider facts = %#v", evidence)
	}
	if !evidence.ActualSpendKnown || evidence.ActualMicroUSD != 123 || evidence.CostMethod != response.Cost.Method {
		t.Fatalf("evidence cost = %#v", evidence)
	}
	if evidence.CeilingMicroUSD != profile.MaxMicroUSD {
		t.Fatalf("evidence ceiling = %d, want %d", evidence.CeilingMicroUSD, profile.MaxMicroUSD)
	}
}

func TestValidateResponseRecordsUnreportedAndImplicitProviderCost(t *testing.T) {
	for _, profileID := range []string{"openrouter-chat", "exa-chat"} {
		profile := profileForTest(t, profileID)
		actual := llm.ServiceClassStandard
		response := llm.Response{
			Status: llm.ResponseStatusCompleted,
			Service: llm.ServiceFacts{
				Requested: llm.ServiceClassStandard,
				Attempted: llm.ServiceClassStandard,
				Actual:    &actual,
			},
			Usage:    llm.Usage{InputTokens: 3, OutputTokens: 2},
			Provider: llm.ProviderFacts{RequestID: "request-123", ResponseID: "response-123"},
		}

		t.Run(profileID+"/unreported", func(t *testing.T) {
			evidence, err := validateResponse(profile, response)
			if err != nil {
				t.Fatalf("validate unreported cost: %v", err)
			}
			if evidence.ActualSpendKnown || evidence.ActualMicroUSD != 0 || evidence.CostMethod != "not_reported" {
				t.Fatalf("unreported cost evidence = %#v", evidence)
			}
		})

		t.Run(profileID+"/implicit-known", func(t *testing.T) {
			response := response
			value := pricing.MustUSD("0.000017")
			response.Cost = llm.Cost{ActualCostUSD: &value, Method: "provider_reported"}
			evidence, err := validateResponse(profile, response)
			if err != nil {
				t.Fatalf("validate implicit known cost: %v", err)
			}
			if !evidence.ActualSpendKnown || evidence.ActualMicroUSD != 17 || evidence.CostMethod != "provider_reported" {
				t.Fatalf("implicit known cost evidence = %#v", evidence)
			}
		})
	}
}

func TestValidateResponseFailsClosedOnMissingOrOverCeilingFacts(t *testing.T) {
	pinned := Profiles()[0]
	unsupported := Profiles()[2]
	actual := llm.ServiceClassStandard
	valid := llm.Response{
		Status:   llm.ResponseStatusCompleted,
		Service:  llm.ServiceFacts{Requested: llm.ServiceClassStandard, Attempted: llm.ServiceClassStandard, Actual: &actual},
		Usage:    llm.Usage{InputTokens: 3, OutputTokens: 2},
		Provider: llm.ProviderFacts{RequestID: "request-123", ResponseID: "response-123"},
		Continuation: &llm.Continuation{
			Handle:     "continuation-123",
			EndpointID: pinned.ID,
			Model:      pinned.Model,
			Pinned:     true,
		},
	}
	tests := []struct {
		name    string
		profile Profile
		mutate  func(*llm.Response)
		want    string
	}{
		{name: "missing request ID", profile: pinned, mutate: func(response *llm.Response) { response.Provider.RequestID = "" }, want: "request ID"},
		{name: "missing actual class", profile: pinned, mutate: func(response *llm.Response) { response.Service.Actual = nil }, want: "actual service class"},
		{name: "missing usage", profile: pinned, mutate: func(response *llm.Response) { response.Usage.OutputTokens = 0 }, want: "usage"},
		{name: "unpinned continuation", profile: pinned, mutate: func(response *llm.Response) { response.Continuation.Pinned = false }, want: "pinned continuation"},
		{name: "reported cost over ceiling", profile: pinned, mutate: func(response *llm.Response) {
			value := pricing.MustUSD("0.025001")
			response.Cost = llm.Cost{Status: llm.CostStatusKnown, ActualCostUSD: &value, Method: "provider_reported"}
		}, want: "ceiling"},
		{name: "unsupported continuation", profile: unsupported, mutate: func(response *llm.Response) { response.Continuation = &llm.Continuation{Handle: "unexpected"} }, want: "must not return a continuation"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := valid
			response.Continuation = cloneContinuation(valid.Continuation)
			if test.profile.ContinuationExpectation == ContinuationUnsupported {
				response.Continuation = nil
			}
			test.mutate(&response)
			if _, err := validateResponse(test.profile, response); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validate response error = %v, want %q", err, test.want)
			}
		})
	}
}

func cloneContinuation(value *llm.Continuation) *llm.Continuation {
	if value == nil {
		return nil
	}
	result := *value
	return &result
}

func profileForTest(t *testing.T, id string) Profile {
	t.Helper()
	for _, profile := range Profiles() {
		if profile.ID == id {
			return profile
		}
	}
	t.Fatalf("live profile %q not found", id)
	return Profile{}
}
