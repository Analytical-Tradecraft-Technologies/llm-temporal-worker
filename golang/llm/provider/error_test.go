package provider_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

func TestCommonErrorKeepsCauseLocalAndSerializesSafeFacts(t *testing.T) {
	cause := errors.New("provider secret response body")
	err := &provider.Error{
		Code:        provider.CodeAuthentication,
		Phase:       provider.PhaseDispatch,
		Dispatch:    provider.DispatchRejected,
		Retry:       provider.RetryNever,
		OperationID: "op-1",
		Provider: llm.ProviderFacts{
			RequestID: "req-1",
			Raw:       map[string]json.RawMessage{"body": json.RawMessage(`"secret"`)},
		},
		SafeMessage: "provider authentication failed",
		SafeDetails: map[string]string{"status": "401"},
		Cause:       cause,
	}
	if err.Error() != err.SafeMessage {
		t.Fatalf("Error() = %q, want safe message", err.Error())
	}
	if !errors.Is(err, cause) {
		t.Fatal("Unwrap did not retain local cause")
	}
	encoded, marshalErr := json.Marshal(err)
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	serialized := string(encoded)
	if strings.Contains(serialized, "secret") || strings.Contains(serialized, "body") || strings.Contains(serialized, "Cause") {
		t.Fatalf("safe error leaked cause/provider raw data: %s", serialized)
	}
	if !strings.Contains(serialized, "provider authentication failed") || !strings.Contains(serialized, "req-1") {
		t.Fatalf("safe facts missing: %s", serialized)
	}
}

func TestDispatchCertaintiesAreClosed(t *testing.T) {
	for _, value := range []provider.DispatchCertainty{
		provider.DispatchNotDispatched,
		provider.DispatchRejected,
		provider.DispatchAccepted,
		provider.DispatchAmbiguous,
	} {
		if !value.Valid() {
			t.Errorf("dispatch certainty %q is invalid", value)
		}
	}
	if provider.DispatchCertainty("other").Valid() {
		t.Fatal("unknown dispatch certainty was accepted")
	}
}

func TestErrorEnumsAreClosed(t *testing.T) {
	if provider.Code("other").Valid() || provider.Phase("other").Valid() || provider.RetryDisposition("other").Valid() {
		t.Fatal("unknown common error enum was accepted")
	}
	if _, err := json.Marshal(&provider.Error{
		Code:        provider.Code("other"),
		Phase:       provider.PhaseDispatch,
		Dispatch:    provider.DispatchRejected,
		Retry:       provider.RetryNever,
		SafeMessage: "safe",
	}); err == nil {
		t.Fatal("marshaled an error with an invalid code")
	}
}

func TestWithEndpointIDCopiesSafeDetailsWithoutExposingCause(t *testing.T) {
	cause := errors.New("Authorization: Bearer secret; body=private-provider-content")
	original := &provider.Error{
		Code:        provider.CodeProviderUnavailable,
		Phase:       provider.PhaseDispatch,
		Dispatch:    provider.DispatchAmbiguous,
		Retry:       provider.RetrySameOperation,
		SafeMessage: "provider request failed before a response was classified",
		SafeDetails: map[string]string{"provider": "openai_responses"},
		Cause:       cause,
	}
	mapped := provider.WithEndpointID(original, "openai-production")
	if got, want := mapped.SafeDetails["endpoint"], "openai-production"; got != want {
		t.Fatalf("endpoint detail = %q, want %q", got, want)
	}
	if _, found := original.SafeDetails["endpoint"]; found {
		t.Fatal("WithEndpointID mutated the original error")
	}
	encoded, err := json.Marshal(mapped)
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{"secret", "private-provider-content"} {
		if strings.Contains(string(encoded), raw) || strings.Contains(mapped.Error(), raw) {
			t.Fatalf("safe endpoint error leaked %q: %s", raw, encoded)
		}
	}
	if !errors.Is(mapped, cause) {
		t.Fatal("WithEndpointID did not retain the local diagnostic cause")
	}
}

func TestNewEgressDeniedErrorKeepsDiagnosticCauseLocal(t *testing.T) {
	cause := fmt.Errorf("Authorization: Bearer provider-secret; prompt=private-content: %w", provider.ErrProviderEgressDenied)
	mapped := provider.NewEgressDeniedError(cause)
	if mapped.Code != provider.CodeProviderUnavailable || mapped.Phase != provider.PhaseDispatch || mapped.Dispatch != provider.DispatchNotDispatched || mapped.Retry != provider.RetryNextRoute {
		t.Fatalf("mapped egress error = %#v", mapped)
	}
	if !errors.Is(mapped, provider.ErrProviderEgressDenied) || !errors.Is(mapped, cause) {
		t.Fatal("egress marker or diagnostic cause was not preserved")
	}
	encoded, err := json.Marshal(mapped)
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{"provider-secret", "private-content"} {
		if strings.Contains(string(encoded), raw) || strings.Contains(mapped.Error(), raw) {
			t.Fatalf("safe egress error leaked %q: %s", raw, encoded)
		}
	}
}

func TestRedirectResponseErrorIsAmbiguousForEveryRedirectStatus(t *testing.T) {
	for _, status := range []int{
		http.StatusMultipleChoices,
		http.StatusMovedPermanently,
		http.StatusFound,
		http.StatusSeeOther,
		http.StatusTemporaryRedirect,
		http.StatusPermanentRedirect,
	} {
		if !provider.IsRedirectStatus(status) {
			t.Fatalf("status %d was not recognized as a redirect", status)
		}
		mapped := provider.NewRedirectResponseError(status)
		if mapped.Code != provider.CodeProviderUnavailable || mapped.Phase != provider.PhaseDispatch || mapped.Dispatch != provider.DispatchAmbiguous || mapped.Retry != provider.RetryNever {
			t.Fatalf("status %d mapped = %#v, want ambiguous non-retriable dispatch error", status, mapped)
		}
		if got, want := mapped.SafeDetails["status"], fmt.Sprintf("%d", status); got != want {
			t.Fatalf("status detail = %q, want %q", got, want)
		}
	}
	for _, status := range []int{http.StatusOK, http.StatusBadRequest} {
		if provider.IsRedirectStatus(status) {
			t.Fatalf("status %d was incorrectly recognized as a redirect", status)
		}
	}
}
