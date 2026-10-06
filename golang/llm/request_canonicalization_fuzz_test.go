package llm_test

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

func FuzzRequestCanonicalizesClosedServiceClasses(f *testing.F) {
	for _, seed := range [][2]string{
		{"", ""},
		{"economy", "priority,standard"},
		{"priority", "economy,standard"},
		{"provider_default", ""},
		{"priority", "priority"},
		{"priority", "economy,economy"},
		{"priority", "turbo"},
	} {
		f.Add(seed[0], seed[1])
	}

	f.Fuzz(func(t *testing.T, rawRequested, rawFallbacks string) {
		if len(rawRequested)+len(rawFallbacks) > 128 {
			t.Skip()
		}
		fallbacks := fuzzRequestServiceClassFallbacks(rawFallbacks)
		request := llm.Request{
			OperationKey:          "request-canonicalization-fuzz",
			Model:                 "logical",
			ServiceClass:          llm.ServiceClass(rawRequested),
			ServiceClassFallbacks: fallbacks,
		}
		requested, classErr := llm.NormalizeServiceClass(request.ServiceClass)
		fallbackErr := llm.ValidateServiceClassFallbacks(requested, fallbacks)
		normalized, err := llm.NormalizeRequest(request)
		if classErr != nil || fallbackErr != nil {
			if err == nil {
				t.Fatalf("NormalizeRequest accepted invalid class=%q fallbacks=%#v", rawRequested, fallbacks)
			}
			if _, digestErr := llm.RequestDigest(request); digestErr == nil {
				t.Fatalf("RequestDigest accepted invalid class=%q fallbacks=%#v", rawRequested, fallbacks)
			}
			return
		}
		if err != nil {
			t.Fatalf("NormalizeRequest rejected valid class=%q fallbacks=%#v: %v", rawRequested, fallbacks, err)
		}
		wantFallbacks := fallbacks
		if wantFallbacks == nil {
			wantFallbacks = []llm.ServiceClass{}
		}
		if normalized.ServiceClass != requested || !reflect.DeepEqual(normalized.ServiceClassFallbacks, wantFallbacks) {
			t.Fatalf("normalized classes = %q/%#v, want %q/%#v", normalized.ServiceClass, normalized.ServiceClassFallbacks, requested, wantFallbacks)
		}

		encoded, err := json.Marshal(normalized)
		if err != nil {
			t.Fatalf("marshal normalized request: %v", err)
		}
		canonical, err := llm.CanonicalJSON(encoded)
		if err != nil {
			t.Fatalf("canonicalize normalized request: %v", err)
		}
		again, err := llm.NormalizeRequest(normalized)
		if err != nil {
			t.Fatalf("repeat normalization: %v", err)
		}
		againEncoded, err := json.Marshal(again)
		if err != nil {
			t.Fatalf("marshal repeat normalized request: %v", err)
		}
		againCanonical, err := llm.CanonicalJSON(againEncoded)
		if err != nil {
			t.Fatalf("canonicalize repeat normalized request: %v", err)
		}
		if !bytes.Equal(canonical, againCanonical) {
			t.Fatalf("normalization changed canonical bytes:\nfirst:  %s\nsecond: %s", canonical, againCanonical)
		}

		firstDigest, err := llm.RequestDigest(request)
		if err != nil {
			t.Fatalf("first request digest: %v", err)
		}
		secondDigest, err := llm.RequestDigest(again)
		if err != nil {
			t.Fatalf("second request digest: %v", err)
		}
		if firstDigest != secondDigest {
			t.Fatalf("normalization changed request digest: %x != %x", firstDigest, secondDigest)
		}
	})
}

func fuzzRequestServiceClassFallbacks(raw string) []llm.ServiceClass {
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
