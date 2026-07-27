package postgres

// Pricing catalog persistence is the maintenance/control-plane boundary for
// the exact-USD catalog. Runtime readers only need SELECT access to these
// tables; catalog publication is written as one transaction so a reload can
// never expose a catalog with only part of its entries.

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
)

const (
	priceCatalogStatusActive  = "active"
	priceCatalogStatusRetired = "retired"
	priceUnknownReason        = "catalog_component_unknown"
	usageFormulaVersion       = "pricing.usd.v1"
)

// PriceCatalogSnapshot is a durable catalog projection. SourceDigest is the
// digest of the operator-verified source document; Catalog.Digest is the
// compiled digest used by request manifests and budget reservations.
type PriceCatalogSnapshot struct {
	ID            uuid.UUID
	Catalog       pricing.Catalog
	SourceDigest  [32]byte
	LoadedAt      time.Time
	EffectiveFrom time.Time
	RetiredAt     *time.Time
	Status        string
}

// PricingCatalogRepository persists validated USD catalog snapshots. The
// repository deliberately accepts a pgxpool rather than a generic SQL
// interface so it shares the namespace, transaction, and redaction
// guarantees of the other PostgreSQL repositories.
type PricingCatalogRepository struct {
	Pool      *pgxpool.Pool
	Namespace Namespace
	Now       func() time.Time
	NewID     func() uuid.UUID
}

// pricingCatalogTestHooks coordinate the deterministic concurrent-publication
// integration test without exposing synchronization controls in the production
// repository API.
type pricingCatalogTestHooks struct {
	beforePublicationLock func(pricing.Catalog)
	afterPublicationLock  func(pricing.Catalog)
}

func (r PricingCatalogRepository) validate() error {
	if r.Pool == nil {
		return errors.New("pricing catalog repository pool is nil")
	}
	return r.Namespace.Validate()
}

func (r PricingCatalogRepository) clock() time.Time {
	if r.Now != nil {
		return r.Now().UTC()
	}
	return time.Now().UTC()
}

func (r PricingCatalogRepository) id() uuid.UUID {
	if r.NewID != nil {
		return r.NewID()
	}
	return uuid.New()
}

// pricingCatalogPublicationLockKey identifies the one catalog publication
// stream in a worker namespace. Catalog versions cannot use independent row
// locks because a version does not exist before its first publication; without
// this shared transaction lock, concurrent writers could both retire the same
// predecessor and publish overlapping active intervals.
func pricingCatalogPublicationLockKey(namespace Namespace) int64 {
	digest := sha256.Sum256([]byte(namespace.String() + "\x00price_catalog_publication"))
	return int64(binary.BigEndian.Uint64(digest[:8]))
}

// Store publishes catalog as one atomic snapshot. Repeating an identical
// catalog version is idempotent; reusing a version or compiled digest for
// different content is rejected. Existing snapshots become retired only in
// the same transaction as the new active snapshot.
func (r PricingCatalogRepository) Store(ctx context.Context, catalog pricing.Catalog, sourceDigest [32]byte, effectiveFrom time.Time) (PriceCatalogSnapshot, error) {
	return r.store(ctx, catalog, sourceDigest, effectiveFrom, pricingCatalogTestHooks{})
}

func (r PricingCatalogRepository) store(ctx context.Context, catalog pricing.Catalog, sourceDigest [32]byte, effectiveFrom time.Time, hooks pricingCatalogTestHooks) (PriceCatalogSnapshot, error) {
	var result PriceCatalogSnapshot
	if err := r.validate(); err != nil {
		return result, err
	}
	if ctx == nil {
		return result, errors.New("pricing catalog context is nil")
	}
	if catalog.Version == "" || len(catalog.Entries) == 0 {
		return result, errors.New("pricing catalog version and entries are required")
	}
	if catalog.Digest == [32]byte{} {
		return result, errors.New("pricing catalog compiled digest is required")
	}
	if sourceDigest == [32]byte{} {
		return result, errors.New("pricing catalog source digest is required")
	}
	if effectiveFrom.IsZero() {
		return result, errors.New("pricing catalog effective_from is required")
	}
	canonicalCatalog, err := validatePersistableCatalog(catalog)
	if err != nil {
		return result, err
	}
	catalog = canonicalCatalog
	catalogRelation, err := r.Namespace.Render("price_catalogs")
	if err != nil {
		return result, err
	}
	entryRelation, err := r.Namespace.Render("price_entries")
	if err != nil {
		return result, err
	}
	loadedAt := r.clock()
	result = PriceCatalogSnapshot{ID: r.id(), Catalog: catalog, SourceDigest: sourceDigest, LoadedAt: loadedAt, EffectiveFrom: effectiveFrom.UTC(), Status: priceCatalogStatusActive}
	err = WithTransaction(ctx, r.Pool, func(ctx context.Context, tx pgx.Tx) error {
		if hooks.beforePublicationLock != nil {
			hooks.beforePublicationLock(catalog)
		}
		if _, lockErr := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", pricingCatalogPublicationLockKey(r.Namespace)); lockErr != nil {
			return redactPostgresError(fmt.Errorf("lock pricing catalog publication: %w", lockErr))
		}
		if hooks.afterPublicationLock != nil {
			hooks.afterPublicationLock(catalog)
		}
		var existingID uuid.UUID
		var existingSource, existingCompiled []byte
		lookup := "SELECT price_catalog_id, source_digest, compiled_digest, status, loaded_at, effective_from, retired_at FROM " + catalogRelation + " WHERE catalog_version=$1 FOR UPDATE"
		var status string
		if scanErr := tx.QueryRow(ctx, lookup, catalog.Version).Scan(&existingID, &existingSource, &existingCompiled, &status, &result.LoadedAt, &result.EffectiveFrom, &result.RetiredAt); scanErr == nil {
			if !sameDigest(existingSource, sourceDigest[:]) || !sameDigest(existingCompiled, catalog.Digest[:]) {
				return fmt.Errorf("pricing catalog version %q already exists with different digest", catalog.Version)
			}
			result.ID, result.Status = existingID, status
			result.Catalog.Entries = nil
			return loadEntries(ctx, tx, entryRelation, existingID, sourceDigest, &result.Catalog)
		} else if !errors.Is(scanErr, pgx.ErrNoRows) {
			return redactPostgresError(fmt.Errorf("find pricing catalog %q: %w", catalog.Version, scanErr))
		}
		var existingVersion string
		if scanErr := tx.QueryRow(ctx, "SELECT catalog_version FROM "+catalogRelation+" WHERE compiled_digest=$1 FOR UPDATE", catalog.Digest[:]).Scan(&existingVersion); scanErr == nil {
			return fmt.Errorf("pricing compiled digest already belongs to catalog %q", existingVersion)
		} else if !errors.Is(scanErr, pgx.ErrNoRows) {
			return redactPostgresError(fmt.Errorf("find pricing compiled digest: %w", scanErr))
		}
		retireStatus := priceCatalogStatusRetired
		if effectiveFrom.After(r.clock()) {
			// A future snapshot schedules retirement but must not hide the
			// predecessor before the replacement becomes effective.
			retireStatus = priceCatalogStatusActive
		}
		if _, execErr := tx.Exec(ctx, "UPDATE "+catalogRelation+" SET status=$1, retired_at=$2 WHERE status=$3 AND effective_from <= $2 AND (retired_at IS NULL OR retired_at > $2)", retireStatus, effectiveFrom.UTC(), priceCatalogStatusActive); execErr != nil {
			return redactPostgresError(fmt.Errorf("retire previous pricing catalogs: %w", execErr))
		}
		if _, execErr := tx.Exec(ctx, "INSERT INTO "+catalogRelation+" (price_catalog_id, catalog_version, source_digest, compiled_digest, loaded_at, effective_from, status) VALUES ($1,$2,$3,$4,$5,$6,$7)", result.ID, catalog.Version, sourceDigest[:], catalog.Digest[:], loadedAt, effectiveFrom.UTC(), priceCatalogStatusActive); execErr != nil {
			return redactPostgresError(fmt.Errorf("insert pricing catalog: %w", execErr))
		}
		if err := insertEntries(ctx, tx, entryRelation, result.ID, catalog, sourceDigest, effectiveFrom.UTC()); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return PriceCatalogSnapshot{}, err
	}
	return result, nil
}

// LoadActive returns the snapshot effective at `at`. A future-dated snapshot
// is not selected until its effective time; a retired snapshot remains
// readable through Load but is never returned here.
func (r PricingCatalogRepository) LoadActive(ctx context.Context, at time.Time) (PriceCatalogSnapshot, error) {
	if err := r.validate(); err != nil {
		return PriceCatalogSnapshot{}, err
	}
	if ctx == nil {
		return PriceCatalogSnapshot{}, errors.New("pricing catalog context is nil")
	}
	if at.IsZero() {
		at = r.clock()
	}
	relation, err := r.Namespace.Render("price_catalogs")
	if err != nil {
		return PriceCatalogSnapshot{}, err
	}
	var version string
	if err := r.Pool.QueryRow(ctx, "SELECT catalog_version FROM "+relation+" WHERE status=$1 AND effective_from <= $2 AND (retired_at IS NULL OR $2 < retired_at) ORDER BY effective_from DESC, loaded_at DESC LIMIT 1", priceCatalogStatusActive, at.UTC()).Scan(&version); err != nil {
		return PriceCatalogSnapshot{}, redactPostgresError(fmt.Errorf("load active pricing catalog: %w", err))
	}
	return r.Load(ctx, version)
}

// Load reads a complete snapshot. Entry rows are selected only after the
// catalog row is found; a missing row is an error rather than a partial
// catalog. The stored compiled digest remains authoritative because the SQL
// projection intentionally does not retain source-document prose fields.
func (r PricingCatalogRepository) Load(ctx context.Context, version string) (PriceCatalogSnapshot, error) {
	var result PriceCatalogSnapshot
	if err := r.validate(); err != nil {
		return result, err
	}
	if ctx == nil {
		return result, errors.New("pricing catalog context is nil")
	}
	if version == "" {
		return result, errors.New("pricing catalog version is required")
	}
	catalogRelation, err := r.Namespace.Render("price_catalogs")
	if err != nil {
		return result, err
	}
	entryRelation, err := r.Namespace.Render("price_entries")
	if err != nil {
		return result, err
	}
	var sourceDigest, compiledDigest []byte
	if err := r.Pool.QueryRow(ctx, "SELECT price_catalog_id, source_digest, compiled_digest, loaded_at, effective_from, retired_at, status FROM "+catalogRelation+" WHERE catalog_version=$1", version).Scan(&result.ID, &sourceDigest, &compiledDigest, &result.LoadedAt, &result.EffectiveFrom, &result.RetiredAt, &result.Status); err != nil {
		return result, redactPostgresError(fmt.Errorf("load pricing catalog %q: %w", version, err))
	}
	result.LoadedAt = result.LoadedAt.UTC()
	result.EffectiveFrom = result.EffectiveFrom.UTC()
	if result.RetiredAt != nil {
		retiredAt := result.RetiredAt.UTC()
		result.RetiredAt = &retiredAt
	}
	if result.Status != priceCatalogStatusActive && result.Status != priceCatalogStatusRetired {
		return PriceCatalogSnapshot{}, fmt.Errorf("pricing catalog %q has unsupported status %q", version, result.Status)
	}
	if len(sourceDigest) != 32 || len(compiledDigest) != 32 {
		return PriceCatalogSnapshot{}, fmt.Errorf("pricing catalog %q has invalid digest length", version)
	}
	copy(result.SourceDigest[:], sourceDigest)
	var digest [32]byte
	copy(digest[:], compiledDigest)
	result.Catalog.Version = version
	if err := loadEntries(ctx, r.Pool, entryRelation, result.ID, result.SourceDigest, &result.Catalog); err != nil {
		return PriceCatalogSnapshot{}, err
	}
	compiled, err := pricing.CompileUSD(result.Catalog.Version, result.Catalog.Entries)
	if err != nil {
		return PriceCatalogSnapshot{}, fmt.Errorf("validate loaded pricing catalog: %w", err)
	}
	if !sameDigest(compiled.Digest[:], compiledDigest) {
		return PriceCatalogSnapshot{}, fmt.Errorf("pricing catalog %q compiled digest does not match persisted entries (calculated %x, persisted %x)", version, compiled.Digest, compiledDigest)
	}
	result.Catalog.Digest = digest
	return result, nil
}

func validatePersistableCatalog(catalog pricing.Catalog) (pricing.Catalog, error) {
	compiled, err := pricing.CompileUSD(catalog.Version, catalog.Entries)
	if err != nil {
		return pricing.Catalog{}, fmt.Errorf("validate pricing catalog: %w", err)
	}
	if catalog.Digest != compiled.Digest {
		return pricing.Catalog{}, errors.New("pricing catalog compiled digest does not match its entries")
	}
	entries := append([]pricing.Entry(nil), catalog.Entries...)
	for index, entry := range catalog.Entries {
		if entry.EffectiveFrom.IsZero() {
			return pricing.Catalog{}, fmt.Errorf("pricing entry %d requires effective_from for PostgreSQL persistence", index)
		}
		if entry.Provenance != "" {
			return pricing.Catalog{}, fmt.Errorf("pricing entry %d provenance is not representable by the existing PostgreSQL projection", index)
		}
		if entry.Version != catalog.Version {
			return pricing.Catalog{}, fmt.Errorf("pricing entry %d version %q must equal catalog version %q for PostgreSQL digest round-trip", index, entry.Version, catalog.Version)
		}
		entries[index].EffectiveFrom = entry.EffectiveFrom.UTC()
		if !entry.EffectiveUntil.IsZero() {
			entries[index].EffectiveUntil = entry.EffectiveUntil.UTC()
		}
		prices := &entries[index].Prices
		var normalizeErr error
		prices.InputPerMillion, normalizeErr = normalizeDecimal(prices.InputPerMillion)
		if normalizeErr != nil {
			return pricing.Catalog{}, fmt.Errorf("pricing entry %d input price: %w", index, normalizeErr)
		}
		prices.OutputPerMillion, normalizeErr = normalizeDecimal(prices.OutputPerMillion)
		if normalizeErr != nil {
			return pricing.Catalog{}, fmt.Errorf("pricing entry %d output price: %w", index, normalizeErr)
		}
		prices.CacheReadPerMillion, normalizeErr = normalizeDecimal(prices.CacheReadPerMillion)
		if normalizeErr != nil {
			return pricing.Catalog{}, fmt.Errorf("pricing entry %d cache-read price: %w", index, normalizeErr)
		}
		prices.CacheWritePerMillion, normalizeErr = normalizeDecimal(prices.CacheWritePerMillion)
		if normalizeErr != nil {
			return pricing.Catalog{}, fmt.Errorf("pricing entry %d cache-write price: %w", index, normalizeErr)
		}
		prices.ReasoningPerMillion, normalizeErr = normalizeDecimal(prices.ReasoningPerMillion)
		if normalizeErr != nil {
			return pricing.Catalog{}, fmt.Errorf("pricing entry %d reasoning price: %w", index, normalizeErr)
		}
		prices.PerRequest, normalizeErr = normalizeDecimal(prices.PerRequest)
		if normalizeErr != nil {
			return pricing.Catalog{}, fmt.Errorf("pricing entry %d per-request price: %w", index, normalizeErr)
		}
	}
	canonical, err := pricing.CompileUSD(catalog.Version, entries)
	if err != nil {
		return pricing.Catalog{}, fmt.Errorf("validate pricing catalog: %w", err)
	}
	return canonical, nil
}

func normalizeDecimal(value pricing.DecimalUSD) (pricing.DecimalUSD, error) {
	usd, err := pricing.ParseUSD(value.String())
	if err != nil {
		return pricing.DecimalUSD{}, err
	}
	return pricing.ParseDecimalUSD(usd.String())
}

func sameDigest(a, b []byte) bool {
	return len(a) == len(b) && subtle.ConstantTimeCompare(a, b) == 1
}

type priceRow struct {
	entry   pricing.Entry
	status  string
	unknown []string
	reason  *string
	prices  [6]*string
}

func entryRow(entry pricing.Entry, fallback time.Time) (priceRow, error) {
	row := priceRow{entry: entry, status: "exact", unknown: make([]string, 0, len(entry.UnknownComponents))}
	for _, component := range entry.UnknownComponents {
		code, ok := componentCode(component)
		if !ok {
			return priceRow{}, fmt.Errorf("pricing entry has unsupported unknown component %q", component)
		}
		row.unknown = append(row.unknown, code)
	}
	if len(row.unknown) > 0 {
		row.status = "partial"
		reason := priceUnknownReason
		row.reason = &reason
		if len(row.unknown) == 6 {
			row.status = "unknown"
		}
	}
	values := []pricing.DecimalUSD{entry.Prices.InputPerMillion, entry.Prices.OutputPerMillion, entry.Prices.CacheReadPerMillion, entry.Prices.CacheWritePerMillion, entry.Prices.ReasoningPerMillion, entry.Prices.PerRequest}
	for i, value := range values {
		if _, unknown := containsComponent(entry.UnknownComponents, i); unknown {
			continue
		}
		usd, err := pricing.ParseUSD(value.String())
		if err != nil {
			return priceRow{}, fmt.Errorf("parse pricing entry component %d as USD: %w", i, err)
		}
		text, err := EncodeUSD(usd)
		if err != nil {
			return priceRow{}, fmt.Errorf("encode pricing entry component %d: %w", i, err)
		}
		row.prices[i] = &text
	}
	row.entry.EffectiveFrom = entry.EffectiveFrom
	if row.entry.EffectiveFrom.IsZero() {
		row.entry.EffectiveFrom = fallback
	}
	if !row.entry.EffectiveUntil.IsZero() && !row.entry.EffectiveUntil.After(row.entry.EffectiveFrom) {
		return priceRow{}, errors.New("pricing entry effective interval is empty")
	}
	return row, nil
}

func insertEntries(ctx context.Context, tx pgx.Tx, relation string, catalogID uuid.UUID, catalog pricing.Catalog, sourceDigest [32]byte, fallback time.Time) error {
	query := "INSERT INTO " + relation + " (price_entry_id, price_catalog_id, provider, endpoint_family, endpoint_id, region, resolved_model, provider_tier, usage_formula_version, price_status, input_per_million_usd, output_per_million_usd, cache_read_per_million_usd, cache_write_per_million_usd, reasoning_per_million_usd, per_request_usd, unknown_component_codes, price_unknown_reason_code, source_price_digest, effective_from, effective_until) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21)"
	for _, entry := range catalog.Entries {
		row, err := entryRow(entry, fallback)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, query, uuid.New(), catalogID, row.entry.Provider, row.entry.Family, row.entry.EndpointID, row.entry.Region, row.entry.Model, row.entry.ProviderTier, usageFormulaVersion, row.status, row.prices[0], row.prices[1], row.prices[2], row.prices[3], row.prices[4], row.prices[5], row.unknown, row.reason, sourceDigest[:], row.entry.EffectiveFrom.UTC(), nullableTime(row.entry.EffectiveUntil)); err != nil {
			return redactPostgresError(fmt.Errorf("insert pricing entry: %w", err))
		}
	}
	return nil
}

func loadEntries(ctx context.Context, querier interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}, relation string, catalogID uuid.UUID, sourceDigest [32]byte, catalog *pricing.Catalog) error {
	rows, err := querier.Query(ctx, "SELECT provider, endpoint_family, endpoint_id, region, resolved_model, provider_tier, usage_formula_version, price_status, input_per_million_usd::text, output_per_million_usd::text, cache_read_per_million_usd::text, cache_write_per_million_usd::text, reasoning_per_million_usd::text, per_request_usd::text, unknown_component_codes, source_price_digest, effective_from, effective_until FROM "+relation+" WHERE price_catalog_id=$1 ORDER BY price_entry_id", catalogID)
	if err != nil {
		return redactPostgresError(fmt.Errorf("load pricing entries: %w", err))
	}
	defer rows.Close()
	for rows.Next() {
		var entry pricing.Entry
		var status, formula string
		var values [6]*string
		var unknownCodes []string
		var entrySourceDigest []byte
		var effectiveUntil *time.Time
		if err := rows.Scan(&entry.Provider, &entry.Family, &entry.EndpointID, &entry.Region, &entry.Model, &entry.ProviderTier, &formula, &status, &values[0], &values[1], &values[2], &values[3], &values[4], &values[5], &unknownCodes, &entrySourceDigest, &entry.EffectiveFrom, &effectiveUntil); err != nil {
			return redactPostgresError(fmt.Errorf("scan pricing entry: %w", err))
		}
		if !sameDigest(entrySourceDigest, sourceDigest[:]) {
			return fmt.Errorf("pricing entry source digest does not match catalog source digest")
		}
		if effectiveUntil != nil {
			entry.EffectiveUntil = effectiveUntil.UTC()
		}
		entry.EffectiveFrom = entry.EffectiveFrom.UTC()
		if formula != usageFormulaVersion {
			return fmt.Errorf("pricing entry uses unsupported formula version %q", formula)
		}
		entry.Version = catalog.Version
		unknownSet := make(map[pricing.PriceComponent]struct{}, len(unknownCodes))
		for _, code := range unknownCodes {
			component, ok := componentForCode(code)
			if !ok {
				return fmt.Errorf("pricing entry has unsupported unknown component code %q", code)
			}
			if _, exists := unknownSet[component]; exists {
				return fmt.Errorf("pricing entry repeats unknown component code %q", code)
			}
			unknownSet[component] = struct{}{}
		}
		for i, value := range values {
			if value == nil {
				component, ok := componentAt(i)
				if !ok {
					return errors.New("pricing entry has invalid component index")
				}
				if _, listed := unknownSet[component]; !listed {
					return fmt.Errorf("pricing entry NULL component %q is not listed as unknown", component)
				}
				entry.UnknownComponents = append(entry.UnknownComponents, component)
				setPriceComponent(&entry.Prices, i, pricing.MustDecimalUSD("0.000000000000000000"))
				continue
			}
			component, _ := componentAt(i)
			if _, listed := unknownSet[component]; listed {
				return fmt.Errorf("pricing entry known component %q is listed as unknown", component)
			}
			decoded, err := pricing.ParseDecimalUSD(*value)
			if err != nil {
				return fmt.Errorf("decode pricing entry component %d: %w", i, err)
			}
			switch i {
			case 0:
				entry.Prices.InputPerMillion = decoded
			case 1:
				entry.Prices.OutputPerMillion = decoded
			case 2:
				entry.Prices.CacheReadPerMillion = decoded
			case 3:
				entry.Prices.CacheWritePerMillion = decoded
			case 4:
				entry.Prices.ReasoningPerMillion = decoded
			case 5:
				entry.Prices.PerRequest = decoded
			}
		}
		if len(entry.UnknownComponents) != len(unknownSet) {
			return fmt.Errorf("pricing entry NULL and unknown component sets disagree")
		}
		sort.Slice(entry.UnknownComponents, func(i, j int) bool { return entry.UnknownComponents[i] < entry.UnknownComponents[j] })
		if err := validatePriceStatus(status); err != nil {
			return err
		}
		if status == "exact" && len(entry.UnknownComponents) != 0 || status == "unknown" && len(entry.UnknownComponents) != 6 || status == "partial" && (len(entry.UnknownComponents) == 0 || len(entry.UnknownComponents) == 6) {
			return fmt.Errorf("pricing entry has inconsistent status %q", status)
		}
		catalog.Entries = append(catalog.Entries, entry)
	}
	if err := rows.Err(); err != nil {
		return redactPostgresError(fmt.Errorf("iterate pricing entries: %w", err))
	}
	if len(catalog.Entries) == 0 {
		return errors.New("pricing catalog has no entries")
	}
	return nil
}

func validatePriceStatus(status string) error {
	if status != "exact" && status != "partial" && status != "unknown" {
		return fmt.Errorf("pricing entry has unsupported status %q", status)
	}
	return nil
}

func componentCode(component pricing.PriceComponent) (string, bool) {
	mapValue := map[pricing.PriceComponent]string{pricing.PriceComponentInput: "input_tokens", pricing.PriceComponentOutput: "output_tokens", pricing.PriceComponentCacheRead: "cache_read_tokens", pricing.PriceComponentCacheWrite: "cache_write_tokens", pricing.PriceComponentReasoning: "reasoning_tokens", pricing.PriceComponentPerRequest: "request"}
	value, ok := mapValue[component]
	return value, ok
}

func componentForCode(code string) (pricing.PriceComponent, bool) {
	for _, component := range []pricing.PriceComponent{pricing.PriceComponentInput, pricing.PriceComponentOutput, pricing.PriceComponentCacheRead, pricing.PriceComponentCacheWrite, pricing.PriceComponentReasoning, pricing.PriceComponentPerRequest} {
		if mapped, ok := componentCode(component); ok && mapped == code {
			return component, true
		}
	}
	return "", false
}

func setPriceComponent(prices *pricing.UnitPrices, index int, value pricing.DecimalUSD) {
	switch index {
	case 0:
		prices.InputPerMillion = value
	case 1:
		prices.OutputPerMillion = value
	case 2:
		prices.CacheReadPerMillion = value
	case 3:
		prices.CacheWritePerMillion = value
	case 4:
		prices.ReasoningPerMillion = value
	case 5:
		prices.PerRequest = value
	}
}

func componentAt(index int) (pricing.PriceComponent, bool) {
	values := []pricing.PriceComponent{pricing.PriceComponentInput, pricing.PriceComponentOutput, pricing.PriceComponentCacheRead, pricing.PriceComponentCacheWrite, pricing.PriceComponentReasoning, pricing.PriceComponentPerRequest}
	if index < 0 || index >= len(values) {
		return "", false
	}
	return values[index], true
}

func containsComponent(components []pricing.PriceComponent, index int) (pricing.PriceComponent, bool) {
	component, ok := componentAt(index)
	if !ok {
		return "", false
	}
	for _, value := range components {
		if value == component {
			return value, true
		}
	}
	return component, false
}

func nullableTime(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	utc := value.UTC()
	return &utc
}
