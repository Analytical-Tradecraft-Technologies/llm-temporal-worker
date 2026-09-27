package pricing

import "time"

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
	// UnknownComponents records omitted source prices. It is deliberately
	// separate from UnitPrices so a missing value cannot be confused with a
	// known-free zero. Cost and estimate callers fail closed when they need one.
	UnknownComponents []PriceComponent
	// MaxPromptTokens, when positive, is the largest prompt this quote covers.
	// Providers such as OpenRouter bill a higher tier above a prompt-size
	// threshold (Gemini 3.1 Pro above 200,000, GPT-6 Astra above 272,000), so
	// a larger prompt must not be priced or reserved with this entry. Zero
	// means the quote has no prompt-size tier. It is omitted from the
	// canonical digest when zero so existing catalog identities are unchanged.
	MaxPromptTokens int64 `json:",omitempty"`
	EffectiveFrom   time.Time
	EffectiveUntil  time.Time
	Provenance      string
	Version         string
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
