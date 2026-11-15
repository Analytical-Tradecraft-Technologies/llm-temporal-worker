package state

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestHandleRoundTripAndTenantBinding(t *testing.T) {
	keyring := mustPrimaryKeyring(t, "k1", 7, bytes.NewReader(bytes.Repeat([]byte{9}, 16)))
	handle := mustIssueVerified(t, keyring, "tenant-a", "k1")
	if _, err := keyring.Verify("tenant-b", handle); err != ErrInvalidHandle {
		t.Fatalf("cross-tenant verification error = %v", err)
	}
}

func TestHandleRejectsTamperingAndRetiredKey(t *testing.T) {
	keyring := mustPrimaryKeyring(t, "k1", 1, nil)
	handle := mustIssue(t, keyring, "t")
	parts := strings.Split(handle, ".")
	replacement := byte('A')
	if parts[2][0] == replacement {
		replacement = 'B'
	}
	parts[2] = string(replacement) + parts[2][1:]
	if _, err := keyring.Verify("t", strings.Join(parts, ".")); err != ErrInvalidHandle {
		t.Fatalf("tampered handle error = %v", err)
	}
	unknown := strings.Replace(handle, ".k1.", ".retired.", 1)
	if _, err := keyring.Verify("t", unknown); err != ErrInvalidHandle {
		t.Fatalf("retired key error = %v", err)
	}
}

func TestKeyringUsesPrimaryKeyAfterRotation(t *testing.T) {
	keyring := mustKeyring(t, []Key{testKey("old", 1, false), testKey("new", 2, true)}, bytes.NewReader(bytes.Repeat([]byte{3}, 16)))
	mustIssueVerified(t, keyring, "tenant", "new")
}

func TestNewKeyringRejectsInvalidConfiguration(t *testing.T) {
	tests := []struct {
		name string
		keys []Key
		want string
	}{
		{name: "no keys", want: "exactly one primary"},
		{name: "invalid id", keys: []Key{testKey("bad.id", 1, true)}, want: "invalid continuation key"},
		{name: "short secret", keys: []Key{{ID: "k1", Secret: bytes.Repeat([]byte{1}, 31), Primary: true}}, want: "invalid continuation key"},
		{name: "duplicate id", keys: []Key{testKey("k1", 1, true), testKey("k1", 2, false)}, want: "duplicate continuation key"},
		{name: "multiple primary", keys: []Key{testKey("k1", 1, true), testKey("k2", 2, true)}, want: "multiple primary"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewKeyring(test.keys, nil)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestKeyringIssueRejectsEmptyTenantAndEntropyFailure(t *testing.T) {
	keyring := mustPrimaryKeyring(t, "k1", 1, errorReader{})
	if _, err := keyring.Issue(""); err != ErrInvalidHandle {
		t.Fatalf("empty tenant error = %v, want ErrInvalidHandle", err)
	}
	if _, err := keyring.Issue("tenant"); err == nil || !strings.Contains(err.Error(), "generate continuation handle") {
		t.Fatalf("entropy failure = %v, want generation error", err)
	}
}

func mustKeyring(t testing.TB, keys []Key, reader io.Reader) *Keyring {
	t.Helper()
	keyring, err := NewKeyring(keys, reader)
	if err != nil {
		t.Fatal(err)
	}
	return keyring
}

// testKey returns a continuation key whose 32-byte secret repeats fill.
func testKey(id string, fill byte, primary bool) Key {
	return Key{ID: id, Secret: bytes.Repeat([]byte{fill}, 32), Primary: primary}
}

// mustPrimaryKeyring builds a keyring holding one primary test key.
func mustPrimaryKeyring(t testing.TB, id string, fill byte, reader io.Reader) *Keyring {
	t.Helper()
	return mustKeyring(t, []Key{testKey(id, fill, true)}, reader)
}

// mustIssueVerified issues a handle for tenant, checks it names keyID, and
// verifies it round-trips.
func mustIssueVerified(t testing.TB, keyring *Keyring, tenant, keyID string) string {
	t.Helper()
	handle := mustIssue(t, keyring, tenant)
	if !strings.HasPrefix(handle, "ctn_v1."+keyID+".") {
		t.Fatalf("issued handle = %q, want primary key %s", handle, keyID)
	}
	if _, err := keyring.Verify(tenant, handle); err != nil {
		t.Fatal(err)
	}
	return handle
}

func mustIssue(t testing.TB, keyring *Keyring, tenant string) string {
	t.Helper()
	handle, err := keyring.Issue(tenant)
	if err != nil {
		t.Fatal(err)
	}
	return handle
}

type errorReader struct{}

func (errorReader) Read([]byte) (int, error) {
	return 0, errors.New("entropy unavailable")
}

func FuzzVerifyHandleNeverPanics(f *testing.F) {
	keyring := mustPrimaryKeyring(f, "k1", 1, bytes.NewReader(bytes.Repeat([]byte{2}, 16)))
	f.Add(mustIssue(f, keyring, "tenant"))
	f.Add("ctn_v1.k1.AAAAAAAAAAAAAAAAAAAAAA.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
	f.Fuzz(func(t *testing.T, value string) {
		keyring := mustPrimaryKeyring(t, "k1", 1, nil)
		identifier, err := keyring.Verify("tenant", value)
		if err == nil && len(identifier) != 16 {
			t.Fatalf("accepted handle has identifier length %d, want 16", len(identifier))
		}
		if err == nil {
			if _, crossTenantErr := keyring.Verify("other-tenant", value); crossTenantErr != ErrInvalidHandle {
				t.Fatalf("accepted handle crossed tenant boundary: %v", crossTenantErr)
			}
		}
	})
}

func TestReferenceCapabilityBindsScopeRequestAndKey(t *testing.T) {
	old := mustPrimaryKeyring(t, "k1", 7, nil)
	capability, err := old.IssueReferenceCapability("tenant", "project", "llmtw_req_a")
	if err != nil || !strings.HasPrefix(capability, "ref_v1.k1.") || len(capability) > 128 {
		t.Fatalf("capability=%q err=%v", capability, err)
	}
	if !old.VerifyReferenceCapability("tenant", "project", "llmtw_req_a", capability) {
		t.Fatal("issued capability was rejected")
	}
	for name, ok := range map[string]bool{
		"tenant":  old.VerifyReferenceCapability("other", "project", "llmtw_req_a", capability),
		"project": old.VerifyReferenceCapability("tenant", "other", "llmtw_req_a", capability),
		"request": old.VerifyReferenceCapability("tenant", "project", "llmtw_req_b", capability),
		"missing": old.VerifyReferenceCapability("tenant", "project", "llmtw_req_a", ""),
		"handle":  old.VerifyReferenceCapability("tenant", "project", "llmtw_req_a", "ctn_v1"+strings.TrimPrefix(capability, "ref_v1")),
	} {
		if ok {
			t.Fatalf("%s mismatch was accepted", name)
		}
	}
	// A rotation keeps verifying capabilities of a retained key and fails
	// closed once that key is retired.
	rotated := mustKeyring(t, []Key{testKey("k2", 8, true), testKey("k1", 7, false)}, nil)
	if !rotated.VerifyReferenceCapability("tenant", "project", "llmtw_req_a", capability) {
		t.Fatal("retained key's capability was rejected")
	}
	retired := mustPrimaryKeyring(t, "k2", 8, nil)
	if retired.VerifyReferenceCapability("tenant", "project", "llmtw_req_a", capability) {
		t.Fatal("retired key's capability was accepted")
	}
}
