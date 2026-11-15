package adaptertest

import (
	"errors"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

// RequireAcceptedInvalidResponse fails t unless err is a provider error
// reporting an invalid response from a dispatch the provider accepted. what
// names the case in the failure message.
func RequireAcceptedInvalidResponse(t testing.TB, err error, what string) {
	t.Helper()
	var mapped *provider.Error
	if !errors.As(err, &mapped) || mapped.Code != provider.CodeProviderInvalidResponse || mapped.Dispatch != provider.DispatchAccepted {
		t.Fatalf("%s = %#v", what, err)
	}
}

// RequireLiftInvalidResponse is RequireAcceptedInvalidResponse for an error
// that must also come from lifting the response.
func RequireLiftInvalidResponse(t testing.TB, err error, what string) {
	t.Helper()
	var mapped *provider.Error
	if !errors.As(err, &mapped) || mapped.Code != provider.CodeProviderInvalidResponse || mapped.Phase != provider.PhaseLift || mapped.Dispatch != provider.DispatchAccepted {
		t.Fatalf("%s = %#v", what, err)
	}
}
