//go:build live

package live

import (
	"context"
	"os"
	"testing"
	"time"
)

const liveInvocationTimeout = 90 * time.Second

// TestLiveProviderContracts is deliberately safe by default: every profile is
// skipped unless the suite gate, human-authorization gate, and that profile's
// own enable gate all equal "1". It must be run only from the protected manual
// release workflow described in docs/reference/live-provider-contracts.md.
func TestLiveProviderContracts(t *testing.T) {
	for _, profile := range Profiles() {
		profile := profile
		t.Run(profile.ID, func(t *testing.T) {
			allowed, reason := authorize(profile, os.LookupEnv)
			if !allowed {
				t.Skip(reason)
			}

			ctx, cancel := context.WithTimeout(context.Background(), liveInvocationTimeout)
			defer cancel()
			evidence, err := runProfile(ctx, profile, os.LookupEnv)
			if err != nil {
				// Provider and SDK errors can contain raw request details, so never
				// print them from a credentialed test run.
				t.Fatal("live provider contract failed")
			}
			responseID := evidence.ResponseID
			if responseID == "" {
				responseID = "not_reported"
			}
			t.Logf("profile=%s tenant=%s request_id=%s response_id=%s actual_service_class=%s actual_spend_known=%t actual_micro_usd=%d cost_method=%s continuation_verified=%t", evidence.Profile, evidence.Tenant, evidence.RequestID, responseID, evidence.ActualServiceClass, evidence.ActualSpendKnown, evidence.ActualMicroUSD, evidence.CostMethod, evidence.ContinuationVerified)
		})
	}
}
