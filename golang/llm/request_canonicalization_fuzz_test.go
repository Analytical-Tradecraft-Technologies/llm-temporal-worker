package llm_test

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
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
		testassert.NoError(t, err, "marshal normalized request")
		canonical, err := llm.CanonicalJSON(encoded)
		testassert.NoError(t, err, "canonicalize normalized request")
		again, err := llm.NormalizeRequest(normalized)
		testassert.NoError(t, err, "repeat normalization")
		againEncoded, err := json.Marshal(again)
		testassert.NoError(t, err, "marshal repeat normalized request")
		againCanonical, err := llm.CanonicalJSON(againEncoded)
		testassert.NoError(t, err, "canonicalize repeat normalized request")
		if !bytes.Equal(canonical, againCanonical) {
			t.Fatalf("normalization changed canonical bytes:\nfirst:  %s\nsecond: %s", canonical, againCanonical)
		}

		firstDigest, err := llm.RequestDigest(request)
		testassert.NoError(t, err, "first request digest")
		secondDigest, err := llm.RequestDigest(again)
		testassert.NoError(t, err, "second request digest")
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
