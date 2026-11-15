package blob

import (
	"errors"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"strings"
	"testing"
	"time"
)

func TestDigestAndTenantPrefixAreStableAndTenantBound(t *testing.T) {
	if got, want := Digest([]byte("hello")), "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"; got != want {
		t.Fatalf("digest = %q, want %q", got, want)
	}
	for _, tenant := range []string{"tenant-a", "tenant-b"} {
		prefix, err := TenantPrefix(tenant)
		if err != nil {
			t.Fatalf("TenantPrefix(%q): %v", tenant, err)
		}
		if prefix != Digest([]byte(tenant)) {
			t.Fatalf("TenantPrefix(%q) = %q, want digest-derived prefix", tenant, prefix)
		}
		if len(prefix) != 64 {
			t.Fatalf("TenantPrefix(%q) length = %d, want 64", tenant, len(prefix))
		}
	}
	first, err := TenantPrefix("tenant-a")
	testassert.NoError(t, err)
	second, err := TenantPrefix("tenant-b")
	testassert.NoError(t, err)
	if first == second {
		t.Fatal("different tenants share a prefix")
	}
	if _, err := TenantPrefix("  \t"); err == nil {
		t.Fatal("accepted a blank tenant")
	}
}

func TestRefValidateEnforcesIntegrityMetadataAndExpiry(t *testing.T) {
	now := time.Date(2026, 7, 14, 0, 0, 0, 0, time.UTC)
	valid := Ref{
		Store:      "files",
		Locator:    "objects/abc",
		Digest:     Digest([]byte("hello")),
		ByteLength: 5,
		MediaType:  "text/plain",
		ExpiresAt:  now.Add(time.Hour),
	}
	tests := []struct {
		name string
		ref  Ref
		want string
	}{
		{name: "valid", ref: valid},
		{name: "zero expiry is allowed", ref: func() Ref { ref := valid; ref.ExpiresAt = time.Time{}; return ref }()},
		{name: "missing store", ref: func() Ref { ref := valid; ref.Store = " \t"; return ref }(), want: "store and locator are required"},
		{name: "missing locator", ref: func() Ref { ref := valid; ref.Locator = ""; return ref }(), want: "store and locator are required"},
		{name: "wrong digest length", ref: func() Ref { ref := valid; ref.Digest = "abc"; return ref }(), want: "64-character"},
		{name: "non hexadecimal digest", ref: func() Ref { ref := valid; ref.Digest = strings.Repeat("g", 64); return ref }(), want: "not hexadecimal"},
		{name: "negative byte length", ref: func() Ref { ref := valid; ref.ByteLength = -1; return ref }(), want: "must not be negative"},
		{name: "missing media type", ref: func() Ref { ref := valid; ref.MediaType = "\n"; return ref }(), want: "media type is required"},
		{name: "expires at now", ref: func() Ref { ref := valid; ref.ExpiresAt = now; return ref }(), want: "expired"},
		{name: "already expired", ref: func() Ref { ref := valid; ref.ExpiresAt = now.Add(-time.Nanosecond); return ref }(), want: "expired"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.ref.Validate(now)
			if test.want == "" {
				testassert.NoError(t, err, "Validate()")
				return
			}
			testassert.ErrorContains(t, err, test.want)
			if strings.Contains(test.want, "expired") && !errors.Is(err, ErrExpired) {
				t.Fatalf("Validate() = %v, want ErrExpired", err)
			}
		})
	}
}
