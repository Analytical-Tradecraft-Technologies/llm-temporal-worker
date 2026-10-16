package control

import (
	"testing"
	"time"
)

func TestClassifyCreditDoesNotTreatGenericRateLimitAsExhausted(t *testing.T) {
	incident := ClassifyCredit(SourceInference, "epoch", "", "", time.Unix(1, 0))
	if incident.Credit == CreditExhausted || incident.Billing == BillingIssue {
		t.Fatalf("generic rate limit became billing incident: %#v", incident)
	}
}

func TestClassifyCreditUsesDocumentedProviderCodes(t *testing.T) {
	incident := ClassifyCredit(SourceInference, "epoch", "insufficient_quota", "", time.Unix(1, 0))
	if incident.Credit != CreditExhausted || incident.Billing != BillingIssue {
		t.Fatalf("provider code was not classified: %#v", incident)
	}
	cleared := ClassifyCredit(SourceManagementAPI, "epoch", "quota_restored", "", time.Unix(2, 0))
	if cleared.Credit != CreditOK || cleared.Billing != BillingOK {
		t.Fatalf("restore code was not classified: %#v", cleared)
	}
}

// The billing 429 codes adapters now report as credit denials are reviewed
// evidence of exhausted credit, so they open a billing incident (#1442).
func TestClassifyCreditUsesDocumentedBillingRateLimitCodes(t *testing.T) {
	for _, code := range []string{"credit_balance_exhausted", "organization_spend_limit_exceeded", "project_spend_limit_exceeded", "organization_usage_limit_exceeded", "enforced_spend_limit_reached"} {
		incident := ClassifyCredit(SourceInference, "epoch", code, "", time.Unix(1, 0))
		if incident.Credit != CreditExhausted || incident.Billing != BillingIssue || !documentedCreditProviderCode(code) {
			t.Fatalf("provider code %q was not classified: %#v", code, incident)
		}
	}
}
