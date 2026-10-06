package state

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

func testTranscript() []llm.Item {
	return []llm.Item{
		llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "hello"}}},
		llm.ToolCall{ID: "call-1", Name: "lookup", Arguments: json.RawMessage(`{"q":"x"}`)},
	}
}

func validContinuation(t *testing.T, now time.Time) Continuation {
	t.Helper()
	transcript := testTranscript()
	_, digest, err := CanonicalTranscript(transcript)
	if err != nil {
		t.Fatal(err)
	}
	return Continuation{
		ID:                 "continuation-1",
		Tenant:             "tenant-a",
		Transcript:         transcript,
		TranscriptDigest:   digest,
		TranscriptComplete: true,
		Pinning:            Pinning{Provider: "anthropic", EndpointID: "prod", Family: "messages", ModelLineage: "claude"},
		ProviderState: []OpaqueStateRef{{
			Provider: "anthropic", EndpointID: "prod", Family: "messages", ModelLineage: "claude", Media: "application/json", Data: []byte("opaque"),
		}},
		ExpiresAt: now.Add(time.Hour),
	}
}

func TestBlobRefValidityAndDigestHex(t *testing.T) {
	digest := sha256.Sum256([]byte("blob"))
	if got := (BlobRef{Digest: digest, Size: 0, Media: "text/plain"}).Valid(); !got {
		t.Fatal("zero-length blob with metadata should be valid")
	}
	if got := (BlobRef{Digest: digest, Size: 1, Media: "text/plain"}).DigestHex(); got != hex.EncodeToString(digest[:]) {
		t.Fatalf("DigestHex() = %q", got)
	}

	for name, ref := range map[string]BlobRef{
		"zero digest":   {Size: 1, Media: "text/plain"},
		"negative size": {Digest: digest, Size: -1, Media: "text/plain"},
		"missing media": {Digest: digest, Size: 1},
	} {
		t.Run(name, func(t *testing.T) {
			if ref.Valid() {
				t.Fatalf("Valid() = true for %#v", ref)
			}
		})
	}
}

func TestPinningValidationAndEmpty(t *testing.T) {
	valid := Pinning{Provider: "anthropic", EndpointID: "prod", Family: "messages", ModelLineage: "claude"}
	if !valid.Empty() && ValidatePinning(valid) != nil {
		t.Fatal("valid pinning was rejected")
	}
	if !(Pinning{}).Empty() {
		t.Fatal("empty pinning was not recognized")
	}

	for name, pin := range map[string]Pinning{
		"provider": {EndpointID: valid.EndpointID, Family: valid.Family, ModelLineage: valid.ModelLineage},
		"endpoint": {Provider: valid.Provider, Family: valid.Family, ModelLineage: valid.ModelLineage},
		"family":   {Provider: valid.Provider, EndpointID: valid.EndpointID, ModelLineage: valid.ModelLineage},
		"lineage":  {Provider: valid.Provider, EndpointID: valid.EndpointID, Family: valid.Family},
	} {
		t.Run(name, func(t *testing.T) {
			if err := ValidatePinning(pin); err == nil || !strings.Contains(err.Error(), "provider pinning") {
				t.Fatalf("ValidatePinning() = %v", err)
			}
		})
	}
}

func TestCanonicalTranscriptIsStableAndRejectsInvalidItems(t *testing.T) {
	items := testTranscript()
	first, firstDigest, err := CanonicalTranscript(items)
	if err != nil {
		t.Fatal(err)
	}
	second, secondDigest, err := CanonicalTranscript(items)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) || firstDigest != secondDigest {
		t.Fatal("canonical transcript changed between identical calls")
	}
	empty, emptyDigest, err := CanonicalTranscript(nil)
	if err != nil {
		t.Fatal(err)
	}
	emptySlice, emptySliceDigest, err := CanonicalTranscript([]llm.Item{})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(empty, emptySlice) || emptyDigest != emptySliceDigest {
		t.Fatal("nil and empty transcripts should have the same canonical form")
	}

	for name, items := range map[string][]llm.Item{
		"nil item":      {nil},
		"invalid actor": {llm.Message{Actor: llm.Actor("robot")}},
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := CanonicalTranscript(items)
			if err == nil || !strings.Contains(err.Error(), "transcript item 0") {
				t.Fatalf("CanonicalTranscript() = %v", err)
			}
		})
	}
}

func TestContinuationCloneCopiesMutableFields(t *testing.T) {
	expires := time.Unix(300, 0)
	original := Continuation{Transcript: testTranscript(), ProviderState: []OpaqueStateRef{{Data: []byte{1, 2, 3}}}, Affinities: ProviderCacheAffinitySet{{Rank: 0, Provider: "openai", RouteID: "route", EndpointID: "endpoint", EndpointAccountHMAC: [32]byte{1}, Region: "us-east-1", EndpointFamily: "responses", ModelLineage: "lineage", RouteModelRevision: "revision", CacheEpoch: "epoch", LastSuccessAt: time.Unix(100, 0), ExpiresAt: &expires}}}
	clone := original.Clone()

	original.Transcript[0] = llm.Message{Actor: llm.ActorModel}
	original.ProviderState[0].Data[0] = 9
	if _, ok := clone.Transcript[0].(llm.Message); !ok {
		t.Fatalf("clone transcript item type = %T", clone.Transcript[0])
	}
	if clone.ProviderState[0].Data[0] != 1 {
		t.Fatalf("clone provider state was aliased: %v", clone.ProviderState[0].Data)
	}
	if clone.ProviderState == nil || len(clone.ProviderState) != 1 {
		t.Fatalf("clone provider state = %#v", clone.ProviderState)
	}
	original.Affinities[0].ExpiresAt = &expires
	*original.Affinities[0].ExpiresAt = time.Unix(400, 0)
	if clone.Affinities[0].ExpiresAt == nil || !clone.Affinities[0].ExpiresAt.Equal(time.Unix(300, 0)) {
		t.Fatalf("clone affinity expiry was aliased: %#v", clone.Affinities[0].ExpiresAt)
	}
}

func TestContinuationValidateEnforcesIntegrityAndLifetime(t *testing.T) {
	now := time.Unix(100, 0)
	base := validContinuation(t, now)
	if err := base.Validate(now); err != nil {
		t.Fatal(err)
	}

	tests := map[string]struct {
		mutate func(*Continuation)
		want   string
	}{
		"missing id":                {mutate: func(value *Continuation) { value.ID = "" }, want: "ID and tenant"},
		"missing tenant":            {mutate: func(value *Continuation) { value.Tenant = "" }, want: "ID and tenant"},
		"negative depth":            {mutate: func(value *Continuation) { value.Depth = -1 }, want: "depth"},
		"expired":                   {mutate: func(value *Continuation) { value.ExpiresAt = now }, want: "state record expired"},
		"digest mismatch":           {mutate: func(value *Continuation) { value.TranscriptDigest[0] ^= 1 }, want: "digest mismatch"},
		"nil transcript item":       {mutate: func(value *Continuation) { value.Transcript = []llm.Item{nil} }, want: "transcript item 0"},
		"incomplete provider state": {mutate: func(value *Continuation) { value.ProviderState[0].Family = "" }, want: "provider state 0 is incomplete"},
		"empty provider state":      {mutate: func(value *Continuation) { value.ProviderState[0].Data = nil }, want: "provider state 0 is empty"},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			value := base.Clone()
			test.mutate(&value)
			err := value.Validate(now)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Validate() = %v, want substring %q", err, test.want)
			}
			if test.want == "state record expired" && !errors.Is(err, ErrExpired) {
				t.Fatalf("Validate() = %v, want ErrExpired", err)
			}
		})
	}
}

func TestContinuationConstraintsExposeRoutingFacts(t *testing.T) {
	affinity := ProviderCacheAffinity{Rank: 0, Provider: "openai", RouteID: "route", EndpointID: "prod", EndpointAccountHMAC: [32]byte{1}, Region: "us-east-1", EndpointFamily: "responses", ModelLineage: "claude", RouteModelRevision: "revision", CacheEpoch: "epoch", LastSuccessAt: time.Unix(100, 0)}
	continuation := Continuation{
		ID: "continuation-1", Tenant: "tenant-a", Pinning: Pinning{Provider: "anthropic", EndpointID: "prod", AccountRegion: "us-east-1", Family: "messages", ModelLineage: "claude"},
		TranscriptComplete: true,
		ProviderState:      []OpaqueStateRef{{Required: false}},
		Affinities:         ProviderCacheAffinitySet{affinity},
	}
	constraints := continuation.Constraints(llm.PortabilityBestEffort)
	if !constraints.Present || constraints.Tenant != continuation.Tenant || constraints.RequiresOpaqueState || !constraints.TranscriptComplete || constraints.Portability != llm.PortabilityBestEffort {
		t.Fatalf("Constraints() = %#v", constraints)
	}
	if constraints.Provider != continuation.Pinning.Provider || constraints.EndpointID != continuation.Pinning.EndpointID || constraints.AccountRegion != continuation.Pinning.AccountRegion || constraints.Family != continuation.Pinning.Family || constraints.ModelLineage != continuation.Pinning.ModelLineage {
		t.Fatalf("Constraints() lost pinning: %#v", constraints)
	}
	if len(constraints.Affinities) != 1 || constraints.Affinities[0].RouteID != "route" {
		t.Fatalf("Constraints() lost provider cache affinity: %#v", constraints.Affinities)
	}
	constraints.Affinities[0].RouteID = "mutated"
	if continuation.Affinities[0].RouteID != "route" {
		t.Fatal("Constraints() returned aliased provider cache affinity")
	}

	if got := (Continuation{Tenant: "tenant-a"}).Constraints(llm.PortabilityStrict); got.Present || got.RequiresOpaqueState || got.TranscriptComplete || got.Portability != llm.PortabilityStrict {
		t.Fatalf("empty continuation constraints = %#v", got)
	}
}

func TestContinuationConstraintsMarkOnlyRequiredProviderStateAsPinned(t *testing.T) {
	continuation := Continuation{ID: "continuation-1", Tenant: "tenant-a", ProviderState: []OpaqueStateRef{
		{Provider: "provider", EndpointID: "endpoint", Family: "family", Media: "application/json", Data: []byte("optional"), Required: false},
		{Provider: "provider", EndpointID: "endpoint", Family: "family", Media: "application/json", Data: []byte("required"), Required: true},
	}}
	if constraints := continuation.Constraints(llm.PortabilityBestEffort); !constraints.RequiresOpaqueState {
		t.Fatal("a required provider-state entry did not pin the continuation")
	}
	continuation.ProviderState[1].Required = false
	if constraints := continuation.Constraints(llm.PortabilityBestEffort); constraints.RequiresOpaqueState {
		t.Fatal("optional provider-state entries unexpectedly pinned the continuation")
	}
}

func TestContinuationValidateRejectsRequiredStateWithoutMatchingPinning(t *testing.T) {
	now := time.Unix(100, 0)
	base := validContinuation(t, now)
	base.ProviderState[0].Required = true
	base.Pinning = Pinning{}
	if err := base.Validate(now); err == nil || !strings.Contains(err.Error(), "requires complete continuation pinning") {
		t.Fatalf("required state without pinning Validate() = %v", err)
	}
	base.Pinning = Pinning{Provider: "anthropic", EndpointID: "prod", Family: "messages", ModelLineage: "claude"}
	base.ProviderState[0].ModelLineage = "other-lineage"
	if err := base.Validate(now); err == nil || !strings.Contains(err.Error(), "does not match continuation pinning") {
		t.Fatalf("mismatched required state Validate() = %v", err)
	}
}

func TestContinuationValidateRejectsOptionalStateWithoutPinning(t *testing.T) {
	now := time.Unix(100, 0)
	base := validContinuation(t, now)
	base.Pinning = Pinning{}
	if err := base.Validate(now); err == nil || !strings.Contains(err.Error(), "requires complete continuation pinning") {
		t.Fatalf("optional state without pinning Validate() = %v", err)
	}
}

func TestContinuationValidateRejectsOptionalStateWithMismatchedProvenance(t *testing.T) {
	now := time.Unix(100, 0)
	base := validContinuation(t, now)
	base.Pinning = Pinning{Provider: "anthropic", EndpointID: "prod", Family: "messages", ModelLineage: "claude"}
	base.ProviderState[0].ModelLineage = "claude"
	if err := base.Validate(now); err != nil {
		t.Fatal(err)
	}
	base.ProviderState[0].Provider = "openai"
	if err := base.Validate(now); err == nil || !strings.Contains(err.Error(), "does not match continuation pinning") {
		t.Fatalf("mismatched optional state Validate() = %v", err)
	}
}
