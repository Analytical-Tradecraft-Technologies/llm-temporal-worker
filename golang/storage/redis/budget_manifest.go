package redis

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
)

// Budget manifest values are deliberately storage-neutral.  The Redis
// Function and the PostgreSQL rebuild path can both consume the same bounded
// record without importing a client or making a provider/runtime decision.
const (
	BudgetManifestSchema   = "budget-manifest/v1"
	BudgetRoundingVersion  = "nano_usd_conservative/v1"
	MaxBudgetManifestBytes = 256 << 10
	// These bounds are independent of the configured per-window bucket limit.
	// They protect readiness and recovery from a malformed or unexpectedly
	// large Redis value before it can be decoded into a worker heap.
	MaxBudgetManifestMembers = 4096
	MaxBudgetManifestBuckets = 1 << 20
	MaxBudgetManifestIDBytes = 256
	MaxBudgetStreamIDBytes   = 128
)

var (
	// ErrBudgetManifestInvalid is returned for any state that cannot prove a
	// complete active budget generation.  Callers should fail closed rather
	// than attempting to repair an individual Redis member.
	ErrBudgetManifestInvalid = errors.New("invalid active budget manifest")
	sha256HexPattern         = regexp.MustCompile(`^[0-9a-f]{64}$`)
	streamIDPattern          = regexp.MustCompile(`^[0-9]+-[0-9]+$`)
)

// BudgetGenerationID and BudgetIncarnationID are opaque identifiers.  They
// intentionally remain strings (rather than UUIDs) because Redis restart
// incarnations are deployment-specific opaque values.
type BudgetGenerationID string
type BudgetIncarnationID string

// BudgetManifestMember is one expected policy/window member in the active
// horizon.  BucketCatalogDigest authenticates the bounded bucket-field
// catalog; individual bucket values remain in the generation window key.
type BudgetManifestMember struct {
	PolicyID            string        `json:"policy_id"`
	WindowID            string        `json:"window_id"`
	PolicyHash          string        `json:"policy_hash"`
	WindowHash          string        `json:"window_hash"`
	ConfigVersion       string        `json:"config_version"`
	PriceVersion        string        `json:"price_version"`
	CoverageStart       time.Time     `json:"coverage_start"`
	CoverageEnd         time.Time     `json:"coverage_end"`
	BucketCount         int           `json:"bucket_count"`
	BucketWidth         time.Duration `json:"bucket_width_ns"`
	BucketCatalogDigest string        `json:"bucket_catalog_digest"`
	// LimitNanoUSD is required by the versioned budget-status reader. It is
	// optional for the older generation-adoption contract so legacy manifests
	// continue to validate but can never be exposed as budget_status data.
	LimitNanoUSD string `json:"limit_nano_usd,omitempty"`
}

// Key is the stable, non-secret identity used in the expected member catalog.
// The identity is never used as a Redis key; keySpace is responsible for
// HMACing identifiers before they become key names.
func (member BudgetManifestMember) Key() string {
	return member.PolicyID + "\x00" + member.WindowID
}

// BudgetManifest is the immutable metadata value referenced by the active
// generation pointer.  MemberCatalogDigest is a SHA-256 over the canonical,
// sorted member list. ManifestDigest computes a second SHA-256 over this full
// record, including that catalog digest.
type BudgetManifest struct {
	Schema               string                 `json:"schema"`
	GenerationID         BudgetGenerationID     `json:"generation_id"`
	IncarnationID        BudgetIncarnationID    `json:"incarnation_id"`
	ConfigVersion        string                 `json:"config_version"`
	PriceVersion         string                 `json:"price_version"`
	PolicyHash           string                 `json:"policy_hash"`
	WindowHash           string                 `json:"window_hash"`
	RebuildComplete      bool                   `json:"rebuild_complete"`
	CoverageStart        time.Time              `json:"coverage_start"`
	CoverageEnd          time.Time              `json:"coverage_end"`
	PolicyCount          int                    `json:"policy_count"`
	WindowCount          int                    `json:"window_count"`
	BucketCount          int                    `json:"bucket_count"`
	StreamHighWaterMark  string                 `json:"stream_high_water_mark"`
	RoundingVersion      string                 `json:"rounding_version"`
	JournalHighWaterMark int64                  `json:"journal_high_water_mark"`
	MemberCatalogDigest  string                 `json:"member_catalog_digest"`
	Members              []BudgetManifestMember `json:"members"`
}

// ActiveBudgetGeneration is the small pointer value read before a worker
// adopts a generation. It binds the immutable manifest to the Redis dataset
// incarnation and prevents a stale pointer from authorizing work.
type ActiveBudgetGeneration struct {
	GenerationID   BudgetGenerationID  `json:"generation_id"`
	IncarnationID  BudgetIncarnationID `json:"incarnation_id"`
	ManifestDigest string              `json:"manifest_digest"`
}

// BudgetManifestExpectation is used by readiness/adoption code to compare a
// decoded manifest with the immutable configuration and expected member
// catalog. All non-zero fields are checked; Members, when supplied, must be
// the complete expected set (order is insignificant).
type BudgetManifestExpectation struct {
	GenerationID         BudgetGenerationID
	IncarnationID        BudgetIncarnationID
	ConfigVersion        string
	PriceVersion         string
	PolicyHash           string
	WindowHash           string
	CoverageStart        time.Time
	CoverageEnd          time.Time
	StreamHighWaterMark  string
	RoundingVersion      string
	JournalHighWaterMark int64
	Members              []BudgetManifestMember
}

// Validate checks the invariant required before an active generation can be
// adopted. It rejects incomplete coverage, duplicate/missing members, bad
// hashes, impossible bucket arithmetic, and unbounded input.
func (manifest BudgetManifest) Validate() error {
	return manifest.validate(BudgetManifestExpectation{})
}

// ValidateAgainst checks the manifest and then compares its provenance and
// complete member catalog with the supplied immutable expectation.
func (manifest BudgetManifest) ValidateAgainst(expected BudgetManifestExpectation) error {
	return manifest.validate(expected)
}

// ValidateBudgetManifest is a convenience for callers that do not need a
// method expression.
func ValidateBudgetManifest(manifest BudgetManifest) error { return manifest.Validate() }

// ValidateBudgetManifestAgainst is the corresponding expectation-aware
// convenience function.
func ValidateBudgetManifestAgainst(manifest BudgetManifest, expected BudgetManifestExpectation) error {
	return manifest.ValidateAgainst(expected)
}

func (manifest BudgetManifest) validate(expected BudgetManifestExpectation) error {
	fail := func(format string, args ...any) error {
		return fmt.Errorf("%w: %s", ErrBudgetManifestInvalid, fmt.Sprintf(format, args...))
	}
	if manifest.Schema != BudgetManifestSchema {
		return fail("schema %q is unsupported", manifest.Schema)
	}
	if err := validateOpaqueID("generation_id", string(manifest.GenerationID)); err != nil {
		return fail("%v", err)
	}
	if err := validateOpaqueID("incarnation_id", string(manifest.IncarnationID)); err != nil {
		return fail("%v", err)
	}
	for name, value := range map[string]string{
		"config_version": manifest.ConfigVersion,
		"price_version":  manifest.PriceVersion,
	} {
		if value == "" {
			return fail("%s is required", name)
		}
	}
	for name, value := range map[string]string{
		"policy_hash":           manifest.PolicyHash,
		"window_hash":           manifest.WindowHash,
		"member_catalog_digest": manifest.MemberCatalogDigest,
	} {
		if !sha256HexPattern.MatchString(value) {
			return fail("%s must be a lowercase SHA-256 hex digest", name)
		}
	}
	if !manifest.RebuildComplete {
		return fail("rebuild_complete must be true for an active generation")
	}
	if err := validateCoverage(manifest.CoverageStart, manifest.CoverageEnd); err != nil {
		return fail("manifest coverage: %v", err)
	}
	if !streamIDPattern.MatchString(manifest.StreamHighWaterMark) || len(manifest.StreamHighWaterMark) > MaxBudgetStreamIDBytes {
		return fail("stream_high_water_mark is not a concrete Redis stream ID")
	}
	if manifest.RoundingVersion != BudgetRoundingVersion {
		return fail("rounding_version %q is unsupported", manifest.RoundingVersion)
	}
	if manifest.JournalHighWaterMark < 0 {
		return fail("journal_high_water_mark must be non-negative")
	}
	if manifest.PolicyCount <= 0 || manifest.PolicyCount > MaxBudgetManifestMembers {
		return fail("policy_count is outside the bounded range")
	}
	if manifest.WindowCount <= 0 || manifest.WindowCount > MaxBudgetManifestMembers {
		return fail("window_count is outside the bounded range")
	}
	if manifest.BucketCount <= 0 || manifest.BucketCount > MaxBudgetManifestBuckets {
		return fail("bucket_count is outside the bounded range")
	}
	if len(manifest.Members) == 0 || len(manifest.Members) > MaxBudgetManifestMembers {
		return fail("members must be complete and bounded")
	}
	if len(manifest.Members) != manifest.WindowCount {
		return fail("window_count=%d does not match member count=%d", manifest.WindowCount, len(manifest.Members))
	}
	seen := make(map[string]struct{}, len(manifest.Members))
	policyIDs := make(map[string]struct{}, manifest.PolicyCount)
	totalBuckets := 0
	for index, member := range manifest.Members {
		if err := validateManifestMember(member, manifest); err != nil {
			return fail("member %d: %v", index, err)
		}
		if _, exists := seen[member.Key()]; exists {
			return fail("duplicate member %q", member.Key())
		}
		seen[member.Key()] = struct{}{}
		policyIDs[member.PolicyID] = struct{}{}
		totalBuckets += member.BucketCount
		if totalBuckets > MaxBudgetManifestBuckets {
			return fail("member bucket count exceeds bound")
		}
	}
	if len(policyIDs) != manifest.PolicyCount {
		return fail("policy_count=%d does not match distinct policies=%d", manifest.PolicyCount, len(policyIDs))
	}
	if totalBuckets != manifest.BucketCount {
		return fail("bucket_count=%d does not match member total=%d", manifest.BucketCount, totalBuckets)
	}
	digest, err := memberCatalogDigest(manifest.Members)
	if err != nil {
		return fail("member catalog: %v", err)
	}
	if digest != manifest.MemberCatalogDigest {
		return fail("member catalog digest mismatch")
	}
	if err := validateExpectation(manifest, expected); err != nil {
		return fail("expectation: %v", err)
	}
	if len(manifestBytes(manifest)) > MaxBudgetManifestBytes {
		return fail("manifest exceeds %d bytes", MaxBudgetManifestBytes)
	}
	return nil
}

func validateManifestMember(member BudgetManifestMember, manifest BudgetManifest) error {
	if err := validateOpaqueID("policy_id", member.PolicyID); err != nil {
		return err
	}
	if err := validateOpaqueID("window_id", member.WindowID); err != nil {
		return err
	}
	for name, value := range map[string]string{
		"policy_hash":           member.PolicyHash,
		"window_hash":           member.WindowHash,
		"bucket_catalog_digest": member.BucketCatalogDigest,
	} {
		if !sha256HexPattern.MatchString(value) {
			return fmt.Errorf("%s must be a lowercase SHA-256 hex digest", name)
		}
	}
	if member.ConfigVersion != manifest.ConfigVersion || member.PriceVersion != manifest.PriceVersion {
		return fmt.Errorf("configuration/price provenance differs from manifest")
	}
	if member.PolicyHash != manifest.PolicyHash || member.WindowHash != manifest.WindowHash {
		return fmt.Errorf("policy/window hash differs from manifest")
	}
	if err := validateCoverage(member.CoverageStart, member.CoverageEnd); err != nil {
		return err
	}
	if !sameInstant(member.CoverageStart, manifest.CoverageStart) || !sameInstant(member.CoverageEnd, manifest.CoverageEnd) {
		return fmt.Errorf("coverage does not cover the active horizon")
	}
	if member.BucketCount <= 0 || member.BucketCount > MaxBudgetManifestBuckets {
		return fmt.Errorf("bucket_count is outside the bounded range")
	}
	if member.BucketWidth <= 0 {
		return fmt.Errorf("bucket_width must be positive")
	}
	duration := member.CoverageEnd.UTC().Sub(member.CoverageStart.UTC())
	if duration <= 0 || duration%member.BucketWidth != 0 || int64(duration/member.BucketWidth) != int64(member.BucketCount) {
		return fmt.Errorf("bucket count/width does not provide complete contiguous coverage")
	}
	return nil
}

func validateExpectation(manifest BudgetManifest, expected BudgetManifestExpectation) error {
	if expected.GenerationID != "" && expected.GenerationID != manifest.GenerationID {
		return fmt.Errorf("generation_id mismatch")
	}
	if expected.IncarnationID != "" && expected.IncarnationID != manifest.IncarnationID {
		return fmt.Errorf("incarnation_id mismatch")
	}
	for _, values := range []struct{ name, got, want string }{
		{"config_version", manifest.ConfigVersion, expected.ConfigVersion},
		{"price_version", manifest.PriceVersion, expected.PriceVersion},
		{"policy_hash", manifest.PolicyHash, expected.PolicyHash},
		{"window_hash", manifest.WindowHash, expected.WindowHash},
		{"stream_high_water_mark", manifest.StreamHighWaterMark, expected.StreamHighWaterMark},
		{"rounding_version", manifest.RoundingVersion, expected.RoundingVersion},
	} {
		if values.want != "" && values.got != values.want {
			return fmt.Errorf("%s mismatch", values.name)
		}
	}
	if !expected.CoverageStart.IsZero() && !sameInstant(manifest.CoverageStart, expected.CoverageStart) {
		return fmt.Errorf("coverage_start mismatch")
	}
	if !expected.CoverageEnd.IsZero() && !sameInstant(manifest.CoverageEnd, expected.CoverageEnd) {
		return fmt.Errorf("coverage_end mismatch")
	}
	if expected.JournalHighWaterMark != 0 && manifest.JournalHighWaterMark != expected.JournalHighWaterMark {
		return fmt.Errorf("journal_high_water_mark mismatch")
	}
	if expected.Members != nil {
		if len(expected.Members) != len(manifest.Members) {
			return fmt.Errorf("complete member catalog is not covered")
		}
		want, err := memberCatalogDigest(expected.Members)
		if err != nil {
			return err
		}
		if want != manifest.MemberCatalogDigest {
			return fmt.Errorf("expected member catalog digest mismatch")
		}
	}
	return nil
}

func validateOpaqueID(name, value string) error {
	if value == "" || len(value) > MaxBudgetManifestIDBytes || strings.TrimSpace(value) != value || strings.ContainsAny(value, "\x00\r\n") {
		return fmt.Errorf("%s is empty, oversized, or contains unsafe whitespace", name)
	}
	return nil
}

func validateCoverage(start, end time.Time) error {
	if start.IsZero() || end.IsZero() || !end.After(start) {
		return fmt.Errorf("coverage must have a positive bounded interval")
	}
	return nil
}

func sameInstant(left, right time.Time) bool { return left.UTC().Equal(right.UTC()) }

// Canonical returns the bounded deterministic JSON representation used for
// digesting and for a Redis manifest value. Member order is normalized by
// policy/window identity and all timestamps are emitted in UTC.
func (manifest BudgetManifest) Canonical() ([]byte, error) {
	if err := manifest.Validate(); err != nil {
		return nil, err
	}
	return manifestBytes(manifest), nil
}

// ManifestDigest computes the SHA-256 identity of Canonical.
func (manifest BudgetManifest) ManifestDigest() ([32]byte, error) {
	canonical, err := manifest.Canonical()
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(canonical), nil
}

// ManifestDigestHex is the fixed-width lowercase digest used by the pointer
// and readiness evidence.
func (manifest BudgetManifest) ManifestDigestHex() (string, error) {
	digest, err := manifest.ManifestDigest()
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(digest[:]), nil
}

// Digest is a short alias for ManifestDigestHex for callers that treat the
// manifest as a content-addressed immutable record.
func (manifest BudgetManifest) Digest() (string, error) { return manifest.ManifestDigestHex() }

// MemberCatalogDigest computes the expected member identity/catalog hash. It
// is exported so a bootstrap implementation can construct a manifest without
// duplicating canonicalization rules.
func MemberCatalogDigest(members []BudgetManifestMember) (string, error) {
	return memberCatalogDigest(members)
}

func memberCatalogDigest(members []BudgetManifestMember) (string, error) {
	if len(members) == 0 || len(members) > MaxBudgetManifestMembers {
		return "", fmt.Errorf("member catalog is outside the bounded range")
	}
	canonical := canonicalMembers(members)
	for index := 1; index < len(canonical); index++ {
		if canonical[index-1].Key() == canonical[index].Key() {
			return "", fmt.Errorf("duplicate member %q", canonical[index].Key())
		}
	}
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return "", fmt.Errorf("marshal member catalog: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func manifestBytes(manifest BudgetManifest) []byte {
	copyManifest := manifest
	copyManifest.CoverageStart = canonicalTime(manifest.CoverageStart)
	copyManifest.CoverageEnd = canonicalTime(manifest.CoverageEnd)
	copyManifest.Members = canonicalMembers(manifest.Members)
	encoded, _ := json.Marshal(copyManifest)
	return encoded
}

func canonicalMembers(members []BudgetManifestMember) []BudgetManifestMember {
	copyMembers := append([]BudgetManifestMember(nil), members...)
	for index := range copyMembers {
		copyMembers[index].CoverageStart = canonicalTime(copyMembers[index].CoverageStart)
		copyMembers[index].CoverageEnd = canonicalTime(copyMembers[index].CoverageEnd)
	}
	sort.Slice(copyMembers, func(left, right int) bool {
		return copyMembers[left].Key() < copyMembers[right].Key()
	})
	return copyMembers
}

func canonicalTime(value time.Time) time.Time { return value.UTC().Round(0) }

// Pointer returns the value that may be atomically published as the active
// generation pointer after manifest verification.
func (manifest BudgetManifest) Pointer() (ActiveBudgetGeneration, error) {
	digest, err := manifest.ManifestDigestHex()
	if err != nil {
		return ActiveBudgetGeneration{}, err
	}
	return ActiveBudgetGeneration{GenerationID: manifest.GenerationID, IncarnationID: manifest.IncarnationID, ManifestDigest: digest}, nil
}

// ValidateAgainst checks pointer identity and the manifest digest without
// reading any other Redis key.
func (pointer ActiveBudgetGeneration) ValidateAgainst(manifest BudgetManifest) error {
	digest, err := manifest.ManifestDigestHex()
	if err != nil {
		return err
	}
	if pointer.GenerationID == "" || pointer.IncarnationID == "" || !sha256HexPattern.MatchString(pointer.ManifestDigest) {
		return fmt.Errorf("%w: active pointer is malformed", ErrBudgetManifestInvalid)
	}
	if pointer.GenerationID != manifest.GenerationID || pointer.IncarnationID != manifest.IncarnationID || pointer.ManifestDigest != digest {
		return fmt.Errorf("%w: active pointer does not identify manifest", ErrBudgetManifestInvalid)
	}
	return nil
}
