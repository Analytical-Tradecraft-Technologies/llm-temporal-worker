package adaptertest

import (
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testkit"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

// RequireAcceptedInvalidResponse fails t unless err is a provider error
// reporting an invalid response from a dispatch the provider accepted. what
// names the case in the failure message.
func RequireAcceptedInvalidResponse(t testing.TB, err error, what string) {
	t.Helper()
	testkit.ProviderError(t, err, testkit.ProviderClass{Code: provider.CodeProviderInvalidResponse, Dispatch: provider.DispatchAccepted}, what)
}

// RequireLiftInvalidResponse is RequireAcceptedInvalidResponse for an error
// that must also come from lifting the response.
func RequireLiftInvalidResponse(t testing.TB, err error, what string) {
	t.Helper()
	testkit.ProviderError(t, err, testkit.ProviderClass{Code: provider.CodeProviderInvalidResponse, Phase: provider.PhaseLift, Dispatch: provider.DispatchAccepted}, what)
}
