package redis

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestBudgetManifestValidationAndPointerProvenance(t *testing.T) {
	manifest := testBudgetManifest(t)
	if err := manifest.Validate(); err != nil {
		t.Fatalf("valid manifest rejected: %v", err)
	}
	if got, err := manifest.ManifestDigestHex(); err != nil || len(got) != sha256.Size*2 {
		t.Fatalf("manifest digest = %q, %v", got, err)
	}
	pointer, err := manifest.Pointer()
	if err != nil {
		t.Fatal(err)
	}
	if err := pointer.ValidateAgainst(manifest); err != nil {
		t.Fatalf("valid pointer rejected: %v", err)
	}
	pointer.ManifestDigest = hex.EncodeToString(make([]byte, sha256.Size))
	if err := pointer.ValidateAgainst(manifest); !errors.Is(err, ErrBudgetManifestInvalid) {
		t.Fatalf("stale pointer error = %v, want ErrBudgetManifestInvalid", err)
	}
}

func TestBudgetManifestCanonicalDigestIsOrderIndependent(t *testing.T) {
	first := testBudgetManifest(t)
	second := first
	second.Members = []BudgetManifestMember{first.Members[1], first.Members[0]}
	firstDigest, err := first.ManifestDigestHex()
	if err != nil {
		t.Fatal(err)
	}
	secondDigest, err := second.ManifestDigestHex()
	if err != nil {
		t.Fatal(err)
	}
	if firstDigest != secondDigest {
		t.Fatalf("member order changed manifest digest: %s != %s", firstDigest, secondDigest)
	}
	firstCanonical, err := first.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	secondCanonical, err := second.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(firstCanonical, secondCanonical) {
		t.Fatalf("canonical bytes changed with member order")
	}
}

func TestBudgetManifestRejectsMissingDuplicateOrUnprovenMembers(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*BudgetManifest)
	}{
		{name: "missing member", mutate: func(value *BudgetManifest) {
			value.Members = value.Members[:1]
		}},
		{name: "duplicate member", mutate: func(value *BudgetManifest) {
			value.Members[1] = value.Members[0]
		}},
		{name: "catalog hash mismatch", mutate: func(value *BudgetManifest) {
			value.MemberCatalogDigest = hex.EncodeToString(make([]byte, sha256.Size))
		}},
		{name: "coverage width mismatch", mutate: func(value *BudgetManifest) {
			value.Members[0].BucketWidth += time.Second
		}},
		{name: "config provenance mismatch", mutate: func(value *BudgetManifest) {
			value.Members[1].ConfigVersion = "different-config"
		}},
		{name: "stream high water mark not concrete", mutate: func(value *BudgetManifest) {
			value.StreamHighWaterMark = "*"
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := testBudgetManifest(t)
			test.mutate(&value)
			if err := value.Validate(); !errors.Is(err, ErrBudgetManifestInvalid) {
				t.Fatalf("Validate() = %v, want ErrBudgetManifestInvalid", err)
			}
		})
	}
}

func TestBudgetManifestValidateAgainstExpectedCatalog(t *testing.T) {
	manifest := testBudgetManifest(t)
	expected := BudgetManifestExpectation{
		GenerationID:        manifest.GenerationID,
		IncarnationID:       manifest.IncarnationID,
		ConfigVersion:       manifest.ConfigVersion,
		PriceVersion:        manifest.PriceVersion,
		PolicyHash:          manifest.PolicyHash,
		WindowHash:          manifest.WindowHash,
		CoverageStart:       manifest.CoverageStart,
		CoverageEnd:         manifest.CoverageEnd,
		StreamHighWaterMark: manifest.StreamHighWaterMark,
		RoundingVersion:     manifest.RoundingVersion,
		Members:             append([]BudgetManifestMember(nil), manifest.Members...),
	}
	if err := manifest.ValidateAgainst(expected); err != nil {
		t.Fatalf("matching expectation rejected: %v", err)
	}
	expected.Members[0].WindowID = "missing-window"
	if err := manifest.ValidateAgainst(expected); !errors.Is(err, ErrBudgetManifestInvalid) {
		t.Fatalf("incomplete expected catalog error = %v", err)
	}
}

func TestMemberCatalogDigestIsOrderIndependentAndRejectsDuplicates(t *testing.T) {
	manifest := testBudgetManifest(t)
	first, err := MemberCatalogDigest(manifest.Members)
	if err != nil {
		t.Fatal(err)
	}
	second, err := MemberCatalogDigest([]BudgetManifestMember{manifest.Members[1], manifest.Members[0]})
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("member order changed catalog digest: %s != %s", first, second)
	}
	_, err = MemberCatalogDigest([]BudgetManifestMember{manifest.Members[0], manifest.Members[0]})
	if err == nil {
		t.Fatal("duplicate member catalog was accepted")
	}
}

func FuzzBudgetManifestMemberOrder(f *testing.F) {
	f.Add(uint8(0))
	f.Add(uint8(1))
	f.Add(uint8(255))
	f.Fuzz(func(t *testing.T, seed uint8) {
		manifest := testBudgetManifest(t)
		if seed%2 == 1 {
			manifest.Members[0], manifest.Members[1] = manifest.Members[1], manifest.Members[0]
		}
		if err := manifest.Validate(); err != nil {
			t.Fatalf("generated valid manifest rejected: %v", err)
		}
		digest, err := manifest.ManifestDigestHex()
		if err != nil {
			t.Fatal(err)
		}
		if len(digest) != sha256.Size*2 {
			t.Fatalf("digest length = %d", len(digest))
		}
	})
}

func testBudgetManifest(t *testing.T) BudgetManifest {
	t.Helper()
	start := time.Date(2026, time.July, 22, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	policyHash := testDigest("policy-catalog")
	windowHash := testDigest("window-catalog")
	members := []BudgetManifestMember{
		{
			PolicyID: "policy-a", WindowID: "window-a", PolicyHash: policyHash, WindowHash: windowHash,
			ConfigVersion: "config-v1", PriceVersion: "prices-v1", CoverageStart: start, CoverageEnd: end,
			BucketCount: 4, BucketWidth: 15 * time.Minute, BucketCatalogDigest: testDigest("buckets-a"),
		},
		{
			PolicyID: "policy-b", WindowID: "window-b", PolicyHash: policyHash, WindowHash: windowHash,
			ConfigVersion: "config-v1", PriceVersion: "prices-v1", CoverageStart: start, CoverageEnd: end,
			BucketCount: 4, BucketWidth: 15 * time.Minute, BucketCatalogDigest: testDigest("buckets-b"),
		},
	}
	catalog, err := MemberCatalogDigest(members)
	if err != nil {
		t.Fatal(err)
	}
	return BudgetManifest{
		Schema: BudgetManifestSchema, GenerationID: "generation-1", IncarnationID: "redis-incarnation-1",
		ConfigVersion: "config-v1", PriceVersion: "prices-v1", PolicyHash: policyHash, WindowHash: windowHash,
		RebuildComplete: true, CoverageStart: start, CoverageEnd: end, PolicyCount: 2, WindowCount: 2,
		BucketCount: 8, StreamHighWaterMark: "42-0", RoundingVersion: BudgetRoundingVersion,
		MemberCatalogDigest: catalog, Members: members,
	}
}

func testDigest(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}
