package pricing

import (
	"fmt"
	"time"
)

type UnitPrices struct {
	InputPerMillion      DecimalUSD
	OutputPerMillion     DecimalUSD
	CacheReadPerMillion  DecimalUSD
	CacheWritePerMillion DecimalUSD
	ReasoningPerMillion  DecimalUSD
	// PerRequest is the absolute USD charge applied once per invocation. It
	// intentionally does not use the per-million token denominator.
	PerRequest DecimalUSD
}

// PriceComponent identifies one independently quoted catalog component.
// Unknown components are preserved by the catalog loader instead of being
// represented by DecimalUSD's zero value, because zero is a known-free price.
type PriceComponent string

const (
	PriceComponentInput      PriceComponent = "input"
	PriceComponentOutput     PriceComponent = "output"
	PriceComponentCacheRead  PriceComponent = "cache_read"
	PriceComponentCacheWrite PriceComponent = "cache_write"
	PriceComponentReasoning  PriceComponent = "reasoning"
	PriceComponentPerRequest PriceComponent = "per_request"
)

func (component PriceComponent) Valid() bool {
	switch component {
	case PriceComponentInput, PriceComponentOutput, PriceComponentCacheRead,
		PriceComponentCacheWrite, PriceComponentReasoning, PriceComponentPerRequest:
		return true
	default:
		return false
	}
}

type Entry struct {
	Provider     string
	Family       string
	EndpointID   string
	Region       string
	Model        string
	ProviderTier string
	Prices       UnitPrices
	// LongPrompt, when set, replaces Prices for a call whose prompt reaches
	// its threshold, as a provider's long-context price tier does. It is
	// omitted from the canonical encoding when unset, so single-price
	// entries keep their digest.
	LongPrompt *LongPromptPrices `json:",omitempty"`
	// UnknownComponents records omitted source prices. It is deliberately
	// separate from UnitPrices so a missing value cannot be confused with a
	// known-free zero. Cost and estimate callers fail closed when they need one.
	UnknownComponents []PriceComponent
	EffectiveFrom     time.Time
	EffectiveUntil    time.Time
	Provenance        string
	Version           string
}

// LongPromptPrices is a price tier that applies from MinPromptTokens prompt
// tokens (input, cache read and cache write together).
type LongPromptPrices struct {
	MinPromptTokens int64
	Prices          UnitPrices
}

// cloneLongPrompt detaches a long-prompt tier, so a catalog never shares it
// with its source entries, its snapshots or its quotes.
func cloneLongPrompt(long *LongPromptPrices) *LongPromptPrices {
	if long == nil {
		return nil
	}
	copied := *long
	return &copied
}

// PricesFor returns the prices billed for a call of promptTokens prompt
// tokens.
func (entry Entry) PricesFor(promptTokens int64) UnitPrices {
	if entry.LongPrompt != nil && promptTokens >= entry.LongPrompt.MinPromptTokens {
		return entry.LongPrompt.Prices
	}
	return entry.Prices
}

func (entry Entry) ComponentUnknown(component PriceComponent) bool {
	for _, unknown := range entry.UnknownComponents {
		if unknown == component {
			return true
		}
	}
	return false
}

func (entry Entry) Active(now time.Time) bool {
	return (entry.EffectiveFrom.IsZero() || !now.Before(entry.EffectiveFrom)) && (entry.EffectiveUntil.IsZero() || now.Before(entry.EffectiveUntil))
}

type Usage struct {
	InputTokens      int64
	OutputTokens     int64
	CacheReadTokens  int64
	CacheWriteTokens int64
	ReasoningTokens  int64
	MediaUnits       int64
}

type CostMethod string

const (
	CostProviderReported    CostMethod = "provider_reported"
	CostCatalogUsage        CostMethod = "catalog_usage"
	CostReconstructedUsage  CostMethod = "reconstructed_usage"
	CostRetainedReservation CostMethod = "retained_reservation"
)

type Cost struct {
	// USD is the authoritative exact amount. MicroUSD is materialized only
	// when the engine crosses into the integer Redis admission boundary.
	USD            USD
	MicroUSD       MicroUSD
	Method         CostMethod
	CatalogVersion string
}

// ValidateUsagePricing rejects additive prices for overlapping usage counters.
// Chat and Responses report reasoning inside their inclusive output total.
func (entry Entry) ValidateUsagePricing() error {
	if entry.LongPrompt != nil && entry.LongPrompt.MinPromptTokens <= 0 {
		return fmt.Errorf("long-prompt price threshold must be positive")
	}
	switch entry.Family {
	case "openai_chat", "openai_responses":
		if entry.Prices.ReasoningPerMillion.numerator.Sign() != 0 || (entry.LongPrompt != nil && entry.LongPrompt.Prices.ReasoningPerMillion.numerator.Sign() != 0) {
			return fmt.Errorf("%s reasoning price must be zero: reasoning tokens are included in output usage", entry.Family)
		}
	}
	return nil
}
