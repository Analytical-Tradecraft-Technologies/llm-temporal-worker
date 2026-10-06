package routing

import (
	"bytes"
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

func FuzzPlannerCanonicalizesServiceClassFallbacks(f *testing.F) {
	for _, seed := range [][2]string{
		{"", ""},
		{"priority", "economy,standard"},
		{"standard", "priority"},
		{"provider_default", ""},
		{"priority", "priority"},
		{"priority", "economy,economy"},
		{"priority", "turbo"},
	} {
		f.Add(seed[0], seed[1])
	}
	catalog := canonicalizationCatalog(f)
	planner := DeterministicPlanner{}

	f.Fuzz(func(t *testing.T, rawRequested, rawFallbacks string) {
		if len(rawRequested)+len(rawFallbacks) > 128 {
			t.Skip()
		}
		fallbacks := fuzzServiceClassFallbacks(rawFallbacks)
		request := llm.Request{
			OperationKey:          "canonicalization-fuzz",
			Model:                 "logical",
			ServiceClass:          llm.ServiceClass(rawRequested),
			ServiceClassFallbacks: fallbacks,
		}
		requested, classErr := llm.NormalizeServiceClass(request.ServiceClass)
		fallbackErr := llm.ValidateServiceClassFallbacks(requested, fallbacks)
		plan, err := planner.Plan(context.Background(), Input{Request: request, Catalog: catalog})
		if classErr != nil || fallbackErr != nil {
			if err == nil {
				t.Fatalf("planner accepted invalid request class=%q fallbacks=%#v", rawRequested, fallbacks)
			}
			return
		}
		if err != nil {
			t.Fatalf("planner rejected valid request class=%q fallbacks=%#v: %v", rawRequested, fallbacks, err)
		}

		expected := append([]llm.ServiceClass{requested}, fallbacks...)
		for index, candidate := range plan.Candidates {
			if candidate.RequestedClass != requested {
				t.Fatalf("candidate[%d] requested class = %q, want %q", index, candidate.RequestedClass, requested)
			}
			if candidate.FallbackIndex < 0 || candidate.FallbackIndex >= len(expected) {
				t.Fatalf("candidate[%d] has invalid fallback index %d", index, candidate.FallbackIndex)
			}
			if candidate.AttemptedClass != expected[candidate.FallbackIndex] {
				t.Fatalf("candidate[%d] attempted class = %q, want explicit fallback[%d] = %q", index, candidate.AttemptedClass, candidate.FallbackIndex, expected[candidate.FallbackIndex])
			}
		}

		again, err := planner.Plan(context.Background(), Input{Request: request, Catalog: catalog})
		if err != nil {
			t.Fatalf("repeat plan: %v", err)
		}
		if !reflect.DeepEqual(plan, again) || plan.Digest != again.Digest || plan.DigestHex != again.DigestHex {
			t.Fatalf("planning was not deterministic:\nfirst:  %#v\nsecond: %#v", plan, again)
		}
		if firstBytes, secondBytes := canonicalPlanJSON(t, plan), canonicalPlanJSON(t, again); !bytes.Equal(firstBytes, secondBytes) {
			t.Fatalf("repeat plan changed canonical bytes:\nfirst:  %s\nsecond: %s", firstBytes, secondBytes)
		}
	})
}

func fuzzServiceClassFallbacks(raw string) []llm.ServiceClass {
	if raw == "" {
		return nil
	}
	values := strings.Split(raw, ",")
	fallbacks := make([]llm.ServiceClass, len(values))
	for index, value := range values {
		fallbacks[index] = llm.ServiceClass(value)
	}
	return fallbacks
}
