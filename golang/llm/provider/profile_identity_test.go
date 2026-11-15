package provider

import (
	"context"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

func TestValidateProfileIdentity(t *testing.T) {
	versioned := CapabilitySet{Version: "v1"}
	for _, test := range []struct {
		name                     string
		id, baseURL, model, decl string
		capabilities             CapabilitySet
		want                     string
	}{
		{name: "valid", id: "p", decl: "v1"},
		{name: "set version only", id: "p", capabilities: versioned},
		{name: "missing ID", decl: "v1", want: "x profile ID is required"},
		{name: "bad base URL", id: "p", baseURL: "not a url", decl: "v1", want: `x profile "p" expected base URL: `},
		{name: "long model", id: "p", model: strings.Repeat("m", 257), decl: "v1", want: `x profile "p" expected model is too long`},
		{name: "no version", id: "p", want: `x profile "p" capability version is required`},
		{name: "conflicting versions", id: "p", decl: "v2", capabilities: versioned, want: `x profile "p" capability versions conflict`},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateProfileIdentity("x", test.id, test.baseURL, test.model, test.decl, test.capabilities)
			if test.want == "" {
				if err != nil {
					t.Fatalf("ValidateProfileIdentity() = %v", err)
				}
				return
			}
			if err == nil || !strings.HasPrefix(err.Error(), test.want) {
				t.Fatalf("ValidateProfileIdentity() = %v, want prefix %q", err, test.want)
			}
		})
	}
	if got := ProfileCapabilityVersion("", versioned); got != "v1" {
		t.Fatalf("ProfileCapabilityVersion(fallback) = %q", got)
	}
}

func TestValidateActualServiceClasses(t *testing.T) {
	valid := func(tier string) bool { return tier != "bad" }
	for _, test := range []struct {
		classes map[string]llm.ServiceClass
		missing llm.ServiceClass
		want    string
	}{
		{classes: map[string]llm.ServiceClass{"t": llm.ServiceClassStandard}, missing: llm.ServiceClassEconomy},
		{missing: "nope", want: `x profile "p" missing actual service class "nope" is invalid`},
		{classes: map[string]llm.ServiceClass{"bad": llm.ServiceClassStandard}, want: `x profile "p" actual provider tier "bad" is invalid`},
		{classes: map[string]llm.ServiceClass{"t": "nope"}, want: `x profile "p" actual provider tier "t" maps to invalid class "nope"`},
	} {
		err := ValidateActualServiceClasses("x", "p", test.classes, test.missing, valid)
		if (err == nil) != (test.want == "") || (err != nil && err.Error() != test.want) {
			t.Errorf("ValidateActualServiceClasses(%v, %q) = %v, want %q", test.classes, test.missing, err, test.want)
		}
	}
}

func TestActualServiceClass(t *testing.T) {
	classes := map[string]llm.ServiceClass{"flex": llm.ServiceClassEconomy}
	if got := ActualServiceClass(classes, "", "flex"); got == nil || *got != llm.ServiceClassEconomy {
		t.Fatalf("ActualServiceClass(flex) = %v", got)
	}
	if got := ActualServiceClass(classes, llm.ServiceClassStandard, ""); got == nil || *got != llm.ServiceClassStandard {
		t.Fatalf("ActualServiceClass(missing) = %v", got)
	}
	if ActualServiceClass(classes, "", "") != nil || ActualServiceClass(classes, llm.ServiceClassStandard, "reserved") != nil {
		t.Fatal("ActualServiceClass() guessed a class for no evidence")
	}
}

func TestOneShotProfileCapabilities(t *testing.T) {
	declared := CapabilitySet{Version: "set", Features: map[Feature]Capability{FeatureStreaming: {State: CapabilityNative}}}
	set, err := OneShotProfileCapabilities(context.Background(), CapabilityQuery{}, "x profile", FamilyOpenAIChat, "ep", declared, "v1")
	if err != nil || set.Version != "v1" || set.Features[FeatureStreaming].State != CapabilityUnsupported {
		t.Fatalf("OneShotProfileCapabilities() = %#v, %v", set, err)
	}
	if declared.Features[FeatureStreaming].State != CapabilityNative {
		t.Fatal("OneShotProfileCapabilities() changed the profile's own set")
	}
	if _, err := OneShotProfileCapabilities(context.Background(), CapabilityQuery{EndpointID: "other"}, "x profile", FamilyOpenAIChat, "ep", declared, "v1"); err == nil || !strings.HasPrefix(err.Error(), "x profile: capability endpoint") {
		t.Fatalf("OneShotProfileCapabilities(other endpoint) = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := OneShotProfileCapabilities(canceled, CapabilityQuery{}, "x profile", FamilyOpenAIChat, "ep", declared, "v1"); err != context.Canceled {
		t.Fatalf("OneShotProfileCapabilities(canceled) = %v", err)
	}
}
