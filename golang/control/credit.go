package control

import "time"

// CreditIncident reports the only evidence that may establish or clear a
// provider credit/billing incident.  Inference 429s are intentionally not
// represented as exhausted credit by this package; provider adapters must
// classify them as capacity/rate failures unless a documented provider code
// or operator assertion says otherwise.
type CreditIncident struct {
	Credit       CreditState
	Billing      BillingState
	ProviderCode string
	SafeCode     string
	ObservedAt   time.Time
	Source       Source
	ConfigEpoch  string
}

// ClassifyCredit maps a provider observation to a safe credit incident. A
// generic rate-limit response remains unknown/ok and cannot open a billing
// incident without explicit provider evidence.
func ClassifyCredit(source Source, configEpoch, providerCode, safeCode string, observedAt time.Time) CreditIncident {
	incident := CreditIncident{Credit: CreditUnknown, Billing: BillingUnknown, ProviderCode: providerCode, SafeCode: safeCode, ObservedAt: observedAt, Source: source, ConfigEpoch: configEpoch}
	if source == SourceOperator {
		// Operators may provide an explicit safe code such as "credit_exhausted"
		// or "billing_blocked". Unknown operator text stays fail-closed.
		switch safeCode {
		case "credit_low":
			incident.Credit = CreditLow
		case "credit_exhausted":
			incident.Credit = CreditExhausted
		case "billing_issue":
			incident.Billing = BillingIssue
		case "credit_ok":
			incident.Credit, incident.Billing = CreditOK, BillingOK
		}
	}
	// Provider codes are deliberately an allow-list, not a substring match.
	// This prevents a generic HTTP 429 from being treated as exhausted credit.
	switch providerCode {
	case "insufficient_quota", "billing_hard_limit", "payment_required",
		// OpenAI's documented billing 429 codes and Anthropic's tier spend-cap
		// error.details.error_code; retrying none of them restores access.
		"credit_balance_exhausted", "organization_spend_limit_exceeded", "project_spend_limit_exceeded",
		"organization_usage_limit_exceeded", "enforced_spend_limit_reached":
		incident.Credit, incident.Billing = CreditExhausted, BillingIssue
	case "quota_low", "billing_warning":
		incident.Credit = CreditLow
	case "quota_restored", "billing_restored":
		incident.Credit, incident.Billing = CreditOK, BillingOK
	}
	return incident
}
