package control

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// CreditEvidenceSource identifies the safe, non-sensitive source retained for
// a credit/billing observation. It intentionally does not expose raw provider
// response text or credentials.
type CreditEvidenceSource string

const (
	CreditEvidenceProviderAPI CreditEvidenceSource = "provider_api"
	CreditEvidenceOperator    CreditEvidenceSource = "operator"
	CreditEvidenceUnknown     CreditEvidenceSource = "unknown"
)

func (source CreditEvidenceSource) valid() bool {
	return source == CreditEvidenceProviderAPI || source == CreditEvidenceOperator || source == CreditEvidenceUnknown
}

// CreditStatus is the normalized endpoint-level row returned by the durable
// control-plane read side. A zero ConfirmedAt means that no incident has a
// confirmed timestamp. SafeEvidenceCode is bounded provider/operator code,
// never a response body.
type CreditStatus struct {
	Provider         string
	EndpointID       string
	Credit           CreditState
	Billing          BillingState
	ObservedAt       time.Time
	StaleAfter       time.Time
	ConfirmedAt      time.Time
	EvidenceSource   CreditEvidenceSource
	SafeEvidenceCode string
}

// Key is the stable storage ordering key. Endpoint IDs are scoped by provider
// in configuration, so both components are required to avoid collapsing two
// providers that happen to use the same endpoint identifier.
func (status CreditStatus) Key() string { return status.Provider + "\x00" + status.EndpointID }

// BillingWireState translates the domain's explicit incident state to the
// query wire vocabulary without making callers depend on the storage value.
func (status CreditStatus) BillingWireState() string {
	return status.Billing.WireState()
}

// WireState maps the stored billing incident to the public query vocabulary.
func (billing BillingState) WireState() string {
	if billing == BillingIssue {
		return "blocked"
	}
	return string(billing)
}

// NewCreditStatus maps a persisted status event's source and safe fields to
// the closed query representation. A provider code is safe to expose only
// after NewStatusEvent has validated it; the mapper prefers an explicitly
// normalized safe error code and otherwise uses that provider code.
func NewCreditStatus(provider, endpoint string, credit CreditState, billing BillingState, confirmedAt time.Time, source Source, safeErrorCode, providerCode string) (CreditStatus, error) {
	if !source.valid() {
		return CreditStatus{}, fmt.Errorf("credit status evidence source %q is invalid", source)
	}
	if (credit == CreditExhausted || billing == BillingIssue) && source != SourceOperator && !documentedCreditProviderCode(providerCode) {
		return CreditStatus{}, errors.New("exhausted credit or billing issue requires a documented provider code or an operator event")
	}
	result := CreditStatus{
		Provider: provider, EndpointID: endpoint, Credit: credit, Billing: billing,
		ConfirmedAt: confirmedAt.UTC(), EvidenceSource: CreditEvidenceUnknown,
	}
	if source == SourceOperator {
		result.EvidenceSource = CreditEvidenceOperator
	} else if source == SourceManagementAPI || safeErrorCode != "" || providerCode != "" {
		result.EvidenceSource = CreditEvidenceProviderAPI
	}
	if safeErrorCode != "" {
		result.SafeEvidenceCode = safeErrorCode
	} else {
		result.SafeEvidenceCode = providerCode
	}
	if err := result.Validate(); err != nil {
		return CreditStatus{}, err
	}
	return result, nil
}

func (status CreditStatus) Validate() error {
	for name, value := range map[string]string{"provider": status.Provider, "endpoint_id": status.EndpointID} {
		if err := validateIdentifier(name, value); err != nil {
			return err
		}
	}
	if !status.Credit.valid() || !status.Billing.valid() {
		return errors.New("credit status contains an unknown state")
	}
	if !status.EvidenceSource.valid() {
		return fmt.Errorf("credit status evidence source %q is invalid", status.EvidenceSource)
	}
	if status.ConfirmedAt.IsZero() {
		status.ConfirmedAt = time.Time{}
	} else if status.ConfirmedAt.Location() == nil {
		return errors.New("credit status confirmed timestamp has no location")
	}
	if !status.ObservedAt.IsZero() && status.ObservedAt.Location() == nil {
		return errors.New("credit status observed timestamp has no location")
	}
	if !status.StaleAfter.IsZero() && status.StaleAfter.Location() == nil {
		return errors.New("credit status stale timestamp has no location")
	}
	if !status.ObservedAt.IsZero() && !status.StaleAfter.IsZero() && !status.StaleAfter.After(status.ObservedAt) {
		return errors.New("credit status stale timestamp must follow observed timestamp")
	}
	if status.SafeEvidenceCode != "" {
		if err := validateSafeCode("safe_evidence_code", status.SafeEvidenceCode); err != nil {
			return err
		}
	}
	return nil
}

// CreditStatusPage is a bounded, deterministic storage page. NextEndpointKey
// is an unsigned database keyset position containing provider and endpoint;
// the public query service must bind it to the signed scope/filter cursor
// before accepting it from a caller.
type CreditStatusPage struct {
	Endpoints       []CreditStatus
	NextEndpointKey string
}

func (page CreditStatusPage) Validate() error {
	previous := ""
	for index, endpoint := range page.Endpoints {
		if err := endpoint.Validate(); err != nil {
			return fmt.Errorf("credit status endpoint %d: %w", index, err)
		}
		if endpoint.Key() <= previous {
			return errors.New("credit status endpoints must be sorted and unique by provider and endpoint id")
		}
		previous = endpoint.Key()
	}
	if page.NextEndpointKey != "" {
		provider, endpoint, ok := strings.Cut(page.NextEndpointKey, "\x00")
		if !ok {
			return errors.New("next credit status key is invalid")
		}
		if err := validateIdentifier("next_provider", provider); err != nil {
			return err
		}
		if err := validateIdentifier("next_endpoint_id", endpoint); err != nil {
			return err
		}
	}
	return nil
}
