package pricing

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"time"
)

var ErrNoActivePrice = errors.New("no active price")

type Catalog struct {
	Version string
	Entries []Entry
	Digest  [32]byte
}

// Validate verifies that a catalog is a complete, compiled USD snapshot.
// Catalog values are immutable once published; the compiled digest is part of
// the audit identity and must match the entries before a resolver accepts a
// replacement.
func (catalog Catalog) Validate() error {
	compiled, err := CompileUSD(catalog.Version, catalog.Entries)
	if err != nil {
		return err
	}
	if catalog.Digest == ([32]byte{}) {
		return errors.New("pricing catalog compiled digest is required")
	}
	if catalog.Digest != compiled.Digest {
		return fmt.Errorf("pricing catalog compiled digest does not match entries")
	}
	return nil
}

// CompileUSD compiles the public USD-only catalog contract. Currency is not
// part of the API or canonical digest: every DecimalUSD price is USD by
// contract, and a non-USD source must be rejected before this boundary.
func CompileUSD(version string, entries []Entry) (Catalog, error) {
	if version == "" {
		return Catalog{}, fmt.Errorf("pricing catalog version is required")
	}
	copyEntries := append([]Entry(nil), entries...)
	seenIdentities := make(map[string]struct{}, len(copyEntries))
	for index := range copyEntries {
		entry := &copyEntries[index]
		if entry.Provider == "" || entry.Family == "" || entry.EndpointID == "" || entry.Model == "" || entry.ProviderTier == "" {
			return Catalog{}, fmt.Errorf("pricing entry %d identity is incomplete", index)
		}
		for name, price := range map[string]DecimalUSD{
			"input": entry.Prices.InputPerMillion, "output": entry.Prices.OutputPerMillion, "cache_read": entry.Prices.CacheReadPerMillion, "cache_write": entry.Prices.CacheWritePerMillion, "reasoning": entry.Prices.ReasoningPerMillion, "per_request": entry.Prices.PerRequest,
		} {
			if err := price.valid(); err != nil {
				return Catalog{}, fmt.Errorf("pricing entry %d %s: %w", index, name, err)
			}
		}
		seenUnknown := make(map[PriceComponent]struct{}, len(entry.UnknownComponents))
		for _, component := range entry.UnknownComponents {
			if !component.Valid() {
				return Catalog{}, fmt.Errorf("pricing entry %d has unknown component %q", index, component)
			}
			if _, exists := seenUnknown[component]; exists {
				return Catalog{}, fmt.Errorf("pricing entry %d repeats unknown component %q", index, component)
			}
			// UnknownComponents is an omission marker, not an annotation on a
			// usable quote. Keeping a non-zero price alongside it would make the
			// catalog's cost and digest semantics contradictory, so fail closed at
			// the compilation boundary.
			price := decimalUSDForComponent(entry.Prices, component)
			if price.numerator.Sign() != 0 {
				return Catalog{}, fmt.Errorf("pricing entry %d marks component %q unknown but supplies a non-zero price", index, component)
			}
			seenUnknown[component] = struct{}{}
		}
		if len(entry.UnknownComponents) > 1 {
			entry.UnknownComponents = append([]PriceComponent(nil), entry.UnknownComponents...)
			sort.Slice(entry.UnknownComponents, func(i, j int) bool { return entry.UnknownComponents[i] < entry.UnknownComponents[j] })
		}
		if !entry.EffectiveFrom.IsZero() && !entry.EffectiveUntil.IsZero() && !entry.EffectiveUntil.After(entry.EffectiveFrom) {
			return Catalog{}, fmt.Errorf("pricing entry %d effective interval is empty", index)
		}
		identity := entryKey(*entry)
		if _, exists := seenIdentities[identity]; exists {
			return Catalog{}, fmt.Errorf("pricing entry %d duplicates pricing identity", index)
		}
		seenIdentities[identity] = struct{}{}
	}
	sort.SliceStable(copyEntries, func(i, j int) bool { return entryKey(copyEntries[i]) < entryKey(copyEntries[j]) })
	canonical, err := json.Marshal(struct {
		Version string  `json:"version"`
		Entries []Entry `json:"entries"`
	}{version, copyEntries})
	if err != nil {
		return Catalog{}, err
	}
	digest := sha256.Sum256(canonical)
	return Catalog{Version: version, Entries: copyEntries, Digest: digest}, nil
}

func decimalUSDForComponent(prices UnitPrices, component PriceComponent) DecimalUSD {
	switch component {
	case PriceComponentInput:
		return prices.InputPerMillion
	case PriceComponentOutput:
		return prices.OutputPerMillion
	case PriceComponentCacheRead:
		return prices.CacheReadPerMillion
	case PriceComponentCacheWrite:
		return prices.CacheWritePerMillion
	case PriceComponentReasoning:
		return prices.ReasoningPerMillion
	case PriceComponentPerRequest:
		return prices.PerRequest
	default:
		return DecimalUSD{}
	}
}

func (catalog Catalog) DigestHex() string { return hex.EncodeToString(catalog.Digest[:]) }

func entryKey(entry Entry) string {
	return strings.Join([]string{entry.Provider, entry.Family, entry.EndpointID, entry.Region, entry.Model, entry.ProviderTier, entry.EffectiveFrom.UTC().Format(time.RFC3339Nano)}, "\x00")
}

type Query struct {
	Provider     string
	Family       string
	EndpointID   string
	Region       string
	Model        string
	ProviderTier string
	At           time.Time
}

type Quote struct {
	Entry          Entry
	CatalogVersion string
	CatalogDigest  string
}

type Resolver interface {
	Resolve(Query) (Quote, error)
}

type PriceResolver struct {
	catalog atomic.Value // Catalog
}

func NewResolver(catalog Catalog) *PriceResolver {
	resolver := &PriceResolver{}
	// Initialize the atomic value even if defensive validation rejects the
	// initial catalog. Resolve then fails closed instead of panicking on an
	// uninitialized atomic.Value.
	resolver.catalog.Store(Catalog{})
	_ = resolver.ReloadValidated(catalog)
	return resolver
}

// Reload publishes a verified USD catalog. The historical no-error signature
// is retained for compatibility; invalid replacements are rejected and the
// previous snapshot remains active. Call ReloadValidated when the caller
// needs the validation error for diagnostics or metrics.
func (resolver *PriceResolver) Reload(catalog Catalog) {
	_ = resolver.ReloadValidated(catalog)
}

// ReloadValidated atomically publishes catalog only after validating its
// compiled USD digest. A failed validation never mutates the current
// resolver snapshot.
func (resolver *PriceResolver) ReloadValidated(catalog Catalog) error {
	if resolver == nil {
		return errors.New("price resolver is nil")
	}
	if err := catalog.Validate(); err != nil {
		return fmt.Errorf("validate pricing catalog reload: %w", err)
	}
	resolver.catalog.Store(cloneCatalog(catalog))
	return nil
}

func (resolver *PriceResolver) Resolve(query Query) (Quote, error) {
	if resolver == nil {
		return Quote{}, fmt.Errorf("price resolver is nil")
	}
	catalog := resolver.catalog.Load().(Catalog)
	return catalog.Resolve(query)
}

func (catalog Catalog) Resolve(query Query) (Quote, error) {
	when := query.At
	if when.IsZero() {
		when = time.Now()
	}
	for _, entry := range catalog.Entries {
		if entry.Provider == query.Provider && entry.Family == query.Family && entry.EndpointID == query.EndpointID && entry.Region == query.Region && entry.Model == query.Model && entry.ProviderTier == query.ProviderTier && entry.Active(when) {
			entry.UnknownComponents = append([]PriceComponent(nil), entry.UnknownComponents...)
			return Quote{Entry: entry, CatalogVersion: catalog.Version, CatalogDigest: catalog.DigestHex()}, nil
		}
	}
	return Quote{}, fmt.Errorf("%w for %s/%s/%s/%s/%s/%s", ErrNoActivePrice, query.Provider, query.Family, query.EndpointID, query.Region, query.Model, query.ProviderTier)
}

func cloneCatalog(catalog Catalog) Catalog {
	entries := make([]Entry, len(catalog.Entries))
	for index, entry := range catalog.Entries {
		entries[index] = entry
		entries[index].UnknownComponents = append([]PriceComponent(nil), entry.UnknownComponents...)
	}
	return Catalog{Version: catalog.Version, Entries: entries, Digest: catalog.Digest}
}
