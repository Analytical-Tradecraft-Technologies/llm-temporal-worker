package routing

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
)

// ProviderCacheAffinity is re-exported from routing so route planners and
// checkpoint stores can share one persistence model without duplicating
// validation rules.
type ProviderCacheAffinity = state.ProviderCacheAffinity

// ProviderCacheAffinitySet is the ordered affinity collection attached to a
// checkpoint.
type ProviderCacheAffinitySet = state.ProviderCacheAffinitySet

// AffinityPreferences separates a required provider-state pin from optional
// prompt-cache preferences. A hard pin is still subject to ordinary route
// eligibility; this type never grants authorization to a route.
type AffinityPreferences struct {
	HardPin *ProviderCacheAffinity
	Soft    ProviderCacheAffinitySet
}

// Validate checks the immutable records before they can influence planning.
// Hard pins must be marked hard_pinned and soft records must not be marked
// hard_pinned, preventing a persistence/transport mismatch from silently
// changing fallback semantics.
func (preferences AffinityPreferences) Validate(now time.Time) error {
	seen := make(map[string]struct{}, 1+len(preferences.Soft))
	if preferences.HardPin != nil {
		if !preferences.HardPin.HardPinned {
			return fmt.Errorf("provider cache hard pin is not marked hard_pinned")
		}
		if err := preferences.HardPin.Validate(now); err != nil {
			return fmt.Errorf("provider cache hard pin: %w", err)
		}
		seen[affinityIdentity(*preferences.HardPin)] = struct{}{}
	}
	if err := preferences.Soft.Validate(now); err != nil {
		return fmt.Errorf("provider cache soft affinity: %w", err)
	}
	previousRank := -1
	for index, affinity := range preferences.Soft {
		if affinity.HardPinned {
			return fmt.Errorf("provider cache soft affinity %d is marked hard_pinned", index)
		}
		if index > 0 && affinity.Rank <= previousRank {
			return fmt.Errorf("provider cache soft affinity ranks must be strictly increasing")
		}
		previousRank = affinity.Rank
		identity := affinityIdentity(affinity)
		if _, ok := seen[identity]; ok {
			return fmt.Errorf("provider cache affinity route %q is duplicated", affinity.RouteID)
		}
		seen[identity] = struct{}{}
	}
	return nil
}

func affinityIdentity(affinity ProviderCacheAffinity) string {
	return affinity.Provider + "\x00" + affinity.RouteID + "\x00" + affinity.EndpointID + "\x00" + affinity.ModelLineage
}

// DeriveProviderCacheKey returns a stable, provider-specific HMAC digest for
// one parent checkpoint prefix. The fixed schema and length-delimited fields
// avoid concatenation ambiguity. The digest is safe to include in provider
// metadata; it never contains raw tenant IDs, prompt hashes, or credentials.
func DeriveProviderCacheKey(secret []byte, tenantScope string, parentDigest [32]byte, providerEpoch, modelLineage string) ([32]byte, error) {
	if len(secret) == 0 {
		return [32]byte{}, fmt.Errorf("provider cache key secret is required")
	}
	if strings.TrimSpace(tenantScope) == "" {
		return [32]byte{}, fmt.Errorf("provider cache key tenant scope is required")
	}
	if parentDigest == [32]byte{} {
		return [32]byte{}, fmt.Errorf("provider cache key parent digest is required")
	}
	if providerEpoch == "" {
		return [32]byte{}, fmt.Errorf("provider cache key provider epoch is required")
	}
	if modelLineage == "" {
		return [32]byte{}, fmt.Errorf("provider cache key model lineage is required")
	}
	payload := struct {
		Version       string `json:"version"`
		TenantScope   string `json:"tenant_scope"`
		ParentDigest  string `json:"parent_digest"`
		ProviderEpoch string `json:"provider_epoch"`
		ModelLineage  string `json:"model_lineage"`
	}{
		Version: "provider-cache/v1", TenantScope: tenantScope,
		ParentDigest: hex.EncodeToString(parentDigest[:]), ProviderEpoch: providerEpoch,
		ModelLineage: modelLineage,
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return [32]byte{}, fmt.Errorf("marshal provider cache key input: %w", err)
	}
	canonical, err := llm.CanonicalJSON(encoded)
	if err != nil {
		return [32]byte{}, fmt.Errorf("canonicalize provider cache key input: %w", err)
	}
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(canonical)
	var digest [32]byte
	copy(digest[:], mac.Sum(nil))
	return digest, nil
}

// ProviderCacheKeyHex is a diagnostic-safe encoding of DeriveProviderCacheKey.
func ProviderCacheKeyHex(secret []byte, tenantScope string, parentDigest [32]byte, providerEpoch, modelLineage string) (string, error) {
	digest, err := DeriveProviderCacheKey(secret, tenantScope, parentDigest, providerEpoch, modelLineage)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(digest[:]), nil
}

// DeriveEndpointAccountHMAC returns the stable opaque identity used when a
// runtime catalog route is matched against persisted affinity. Runtime catalog
// loading does not have access to credential material, so this is a
// domain-separated identity digest rather than a credential verifier. It
// intentionally includes the provider, endpoint, account region, route region,
// and configuration version while never retaining those raw values in
// checkpoint state. If no account/route identity is available, it returns the
// zero digest so callers fail closed rather than guessing.
func DeriveEndpointAccountHMAC(provider, endpointID, accountRegion, region, configVersion string) [32]byte {
	if strings.TrimSpace(provider) == "" || strings.TrimSpace(endpointID) == "" ||
		(strings.TrimSpace(accountRegion) == "" && strings.TrimSpace(region) == "") {
		return [32]byte{}
	}
	payload := strings.Join([]string{provider, endpointID, accountRegion, region, configVersion}, "\x00")
	return sha256.Sum256(append([]byte("llm-temporal-worker/provider-account/v1\x00"), []byte(payload)...))
}

// PreferProviderCacheAffinity reorders only eligible candidates. Candidates
// remain class-major and fallback-authorized; an exact affinity match is moved
// to the front of its existing requested/attempted-class group. Invalid
// persisted observations fail closed, while expired observations are ignored.
func PreferProviderCacheAffinity(plan Plan, preferences *AffinityPreferences, now time.Time) (Plan, error) {
	if preferences == nil {
		return plan, nil
	}
	if err := preferences.Validate(now); err != nil {
		return Plan{}, err
	}
	result := plan.Clone()
	if preferences.HardPin != nil {
		if !preferences.HardPin.Active(now) {
			return Plan{}, fmt.Errorf("provider cache hard pin %q is expired or invalid", preferences.HardPin.RouteID)
		}
		for _, candidate := range result.Candidates {
			if affinityMatchesCandidate(*preferences.HardPin, candidate) {
				// A hard pin is an eligibility constraint, not a preference. Do
				// not leave fallback candidates behind for a later dispatch.
				result.Candidates = []Candidate{candidate}
				result.Digest = digestPlan(result)
				result.DigestHex = fmt.Sprintf("%x", result.Digest[:])
				return result, nil
			}
		}
		return Plan{}, fmt.Errorf("provider cache hard pin %q is not eligible", preferences.HardPin.RouteID)
	}
	if len(result.Candidates) < 2 {
		return result, nil
	}
	ordered := make([]ProviderCacheAffinity, 0, 1+len(preferences.Soft))
	if preferences.HardPin != nil {
		ordered = append(ordered, *preferences.HardPin)
	}
	ordered = append(ordered, preferences.Soft...)
	for groupStart := 0; groupStart < len(result.Candidates); {
		groupEnd := groupStart + 1
		group := result.Candidates[groupStart]
		for groupEnd < len(result.Candidates) && sameClassGroup(group, result.Candidates[groupEnd]) {
			groupEnd++
		}
		for _, affinity := range ordered {
			if !affinity.Active(now) {
				continue
			}
			match := -1
			for index := groupStart; index < groupEnd; index++ {
				if affinityMatchesCandidate(affinity, result.Candidates[index]) {
					match = index
					break
				}
			}
			if match >= 0 && match != groupStart {
				candidate := result.Candidates[match]
				copy(result.Candidates[groupStart+1:match+1], result.Candidates[groupStart:match])
				result.Candidates[groupStart] = candidate
			}
			if match >= 0 {
				break
			}
		}
		groupStart = groupEnd
	}
	result.Digest = digestPlan(result)
	result.DigestHex = fmt.Sprintf("%x", result.Digest[:])
	return result, nil
}

// ApplyProviderCacheAffinity is a descriptive alias for callers that apply a
// previously planned route list outside DeterministicPlanner.
func ApplyProviderCacheAffinity(plan Plan, preferences *AffinityPreferences, now time.Time) (Plan, error) {
	return PreferProviderCacheAffinity(plan, preferences, now)
}

func sameClassGroup(left, right Candidate) bool {
	return left.FallbackIndex == right.FallbackIndex && left.AttemptedClass == right.AttemptedClass
}

func affinityMatchesCandidate(affinity ProviderCacheAffinity, candidate Candidate) bool {
	// PriceAvailable is a compile-time safety signal. A route whose snapshot
	// had no current quote stays in configured order until quotePlan resolves
	// it; affinity must never make an unpriced route the first dispatch.
	if !candidate.PriceAvailable {
		return false
	}
	if affinity.Provider != candidate.Provider || affinity.RouteID != candidate.RouteID || affinity.EndpointID != candidate.EndpointID {
		return false
	}
	if affinity.Region != candidate.Region || affinity.EndpointFamily != candidate.Family || affinity.ModelLineage != candidate.ModelLineage || affinity.RouteModelRevision != candidate.ModelRevision {
		return false
	}
	if affinity.EndpointAccountHMAC != [32]byte{} && affinity.EndpointAccountHMAC != candidate.EndpointAccountHMAC {
		return false
	}
	return true
}
