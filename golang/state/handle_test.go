package state

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestHandleRoundTripAndTenantBinding(t *testing.T) {
	keyring, err := NewKeyring([]Key{{ID: "k1", Secret: bytes.Repeat([]byte{7}, 32), Primary: true}}, bytes.NewReader(bytes.Repeat([]byte{9}, 16)))
	if err != nil {
		t.Fatal(err)
	}
	handle, err := keyring.Issue("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(handle, "ctn_v1.k1.") {
		t.Fatalf("unexpected handle %q", handle)
	}
	if _, err := keyring.Verify("tenant-a", handle); err != nil {
		t.Fatal(err)
	}
	if _, err := keyring.Verify("tenant-b", handle); err != ErrInvalidHandle {
		t.Fatalf("cross-tenant verification error = %v", err)
	}
}

func TestHandleRejectsTamperingAndRetiredKey(t *testing.T) {
	keyring, err := NewKeyring([]Key{{ID: "k1", Secret: bytes.Repeat([]byte{1}, 32), Primary: true}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := keyring.Issue("t")
	if err != nil {
		t.Fatal(err)
	}
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
	keyring, err := NewKeyring([]Key{
		{ID: "old", Secret: bytes.Repeat([]byte{1}, 32)},
		{ID: "new", Secret: bytes.Repeat([]byte{2}, 32), Primary: true},
	}, bytes.NewReader(bytes.Repeat([]byte{3}, 16)))
	if err != nil {
		t.Fatal(err)
	}
	handle, err := keyring.Issue("tenant")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(handle, "ctn_v1.new.") {
		t.Fatalf("issued handle = %q, want primary key new", handle)
	}
	if _, err := keyring.Verify("tenant", handle); err != nil {
		t.Fatal(err)
	}
}

func TestNewKeyringRejectsInvalidConfiguration(t *testing.T) {
	tests := []struct {
		name string
		keys []Key
		want string
	}{
		{name: "no keys", want: "exactly one primary"},
		{name: "invalid id", keys: []Key{{ID: "bad.id", Secret: bytes.Repeat([]byte{1}, 32), Primary: true}}, want: "invalid continuation key"},
		{name: "short secret", keys: []Key{{ID: "k1", Secret: bytes.Repeat([]byte{1}, 31), Primary: true}}, want: "invalid continuation key"},
		{name: "duplicate id", keys: []Key{
			{ID: "k1", Secret: bytes.Repeat([]byte{1}, 32), Primary: true},
			{ID: "k1", Secret: bytes.Repeat([]byte{2}, 32)},
		}, want: "duplicate continuation key"},
		{name: "multiple primary", keys: []Key{
			{ID: "k1", Secret: bytes.Repeat([]byte{1}, 32), Primary: true},
			{ID: "k2", Secret: bytes.Repeat([]byte{2}, 32), Primary: true},
		}, want: "multiple primary"},
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
	keyring, err := NewKeyring([]Key{{ID: "k1", Secret: bytes.Repeat([]byte{1}, 32), Primary: true}}, errorReader{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := keyring.Issue(""); err != ErrInvalidHandle {
		t.Fatalf("empty tenant error = %v, want ErrInvalidHandle", err)
	}
	if _, err := keyring.Issue("tenant"); err == nil || !strings.Contains(err.Error(), "generate continuation handle") {
		t.Fatalf("entropy failure = %v, want generation error", err)
	}
}

type errorReader struct{}

func (errorReader) Read([]byte) (int, error) {
	return 0, errors.New("entropy unavailable")
}

func FuzzVerifyHandleNeverPanics(f *testing.F) {
	keyring, err := NewKeyring([]Key{{ID: "k1", Secret: bytes.Repeat([]byte{1}, 32), Primary: true}}, bytes.NewReader(bytes.Repeat([]byte{2}, 16)))
	if err != nil {
		f.Fatal(err)
	}
	valid, err := keyring.Issue("tenant")
	if err != nil {
		f.Fatal(err)
	}
	f.Add(valid)
	f.Add("ctn_v1.k1.AAAAAAAAAAAAAAAAAAAAAA.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
	f.Fuzz(func(t *testing.T, value string) {
		keyring, err := NewKeyring([]Key{{ID: "k1", Secret: bytes.Repeat([]byte{1}, 32), Primary: true}}, nil)
		if err != nil {
			t.Fatal(err)
		}
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
	old, err := NewKeyring([]Key{{ID: "k1", Secret: bytes.Repeat([]byte{7}, 32), Primary: true}}, nil)
	if err != nil {
		t.Fatal(err)
	}
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
	rotated, err := NewKeyring([]Key{{ID: "k2", Secret: bytes.Repeat([]byte{8}, 32), Primary: true}, {ID: "k1", Secret: bytes.Repeat([]byte{7}, 32)}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !rotated.VerifyReferenceCapability("tenant", "project", "llmtw_req_a", capability) {
		t.Fatal("retained key's capability was rejected")
	}
	retired, err := NewKeyring([]Key{{ID: "k2", Secret: bytes.Repeat([]byte{8}, 32), Primary: true}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if retired.VerifyReferenceCapability("tenant", "project", "llmtw_req_a", capability) {
		t.Fatal("retired key's capability was accepted")
	}
}
