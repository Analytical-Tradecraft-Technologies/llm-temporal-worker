package control

import (
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"testing"
	"time"
)

func TestNewCreditStatusMapsSafeProviderEvidence(t *testing.T) {
	status, err := NewCreditStatus("openai", "primary", CreditExhausted, BillingIssue, time.Unix(10, 0), SourceInference, "", "insufficient_quota")
	testassert.NoError(t, err)
	if status.EvidenceSource != CreditEvidenceProviderAPI || status.SafeEvidenceCode != "insufficient_quota" {
		t.Fatalf("status = %#v", status)
	}
	if status.BillingWireState() != "blocked" {
		t.Fatalf("billing wire state = %q, want blocked", status.BillingWireState())
	}
}

func TestNewCreditStatusRejectsUndocumentedIncidentEvidence(t *testing.T) {
	if _, err := NewCreditStatus("openai", "primary", CreditExhausted, BillingIssue, time.Unix(10, 0), SourceInference, "", "provider_specific_free_text"); err == nil {
		t.Fatal("undocumented provider evidence was accepted")
	}
}

func TestNewCreditStatusMapsOperatorAndUnknownEvidence(t *testing.T) {
	operator, err := NewCreditStatus("provider", "operator-endpoint", CreditLow, BillingUnknown, time.Time{}, SourceOperator, "credit_low", "")
	testassert.NoError(t, err)
	if operator.EvidenceSource != CreditEvidenceOperator || operator.SafeEvidenceCode != "credit_low" || !operator.ConfirmedAt.IsZero() {
		t.Fatalf("operator status = %#v", operator)
	}

	unknown, err := NewCreditStatus("provider", "inference-endpoint", CreditOK, BillingOK, time.Time{}, SourceInference, "", "")
	testassert.NoError(t, err)
	if unknown.EvidenceSource != CreditEvidenceUnknown || unknown.SafeEvidenceCode != "" {
		t.Fatalf("unknown status = %#v", unknown)
	}
}

func TestCreditStatusValidationRejectsUnsafeAndUnorderedPages(t *testing.T) {
	if _, err := NewCreditStatus(" provider", "endpoint", CreditOK, BillingOK, time.Time{}, SourceInference, "", ""); err == nil {
		t.Fatal("unsafe provider accepted")
	}
	if _, err := NewCreditStatus("provider", "endpoint", CreditState("future"), BillingOK, time.Time{}, SourceInference, "", ""); err == nil {
		t.Fatal("unknown credit state accepted")
	}
	page := CreditStatusPage{Endpoints: []CreditStatus{
		{Provider: "provider", EndpointID: "endpoint-b", Credit: CreditOK, Billing: BillingOK, EvidenceSource: CreditEvidenceUnknown},
		{Provider: "provider", EndpointID: "endpoint-a", Credit: CreditOK, Billing: BillingOK, EvidenceSource: CreditEvidenceUnknown},
	}}
	testassert.ErrorContains(t, page.Validate(), "sorted")
	if page := (CreditStatusPage{Endpoints: []CreditStatus{
		{Provider: "provider-a", EndpointID: "endpoint", Credit: CreditOK, Billing: BillingOK, EvidenceSource: CreditEvidenceUnknown},
		{Provider: "provider-b", EndpointID: "endpoint", Credit: CreditOK, Billing: BillingOK, EvidenceSource: CreditEvidenceUnknown},
	}}); page.Validate() != nil {
		t.Fatal("same endpoint id from different providers should have distinct ordering keys")
	}
}

func TestCreditStatusValidationRequiresOrderedProvenance(t *testing.T) {
	observed := time.Date(2026, 7, 20, 0, 0, 0, 0, time.UTC)
	status := CreditStatus{
		Provider: "provider", EndpointID: "endpoint", Credit: CreditOK, Billing: BillingOK,
		ObservedAt: observed, StaleAfter: observed,
		EvidenceSource: CreditEvidenceUnknown,
	}
	testassert.ErrorContains(t, status.Validate(), "stale timestamp")
	status.StaleAfter = observed.Add(time.Minute)
	testassert.NoError(t, status.Validate(), "ordered provenance rejected")
}
