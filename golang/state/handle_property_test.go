package state

import (
	"bytes"
	"testing"
)

func TestContinuationHandleTenantBindingInvariants(t *testing.T) {
	keyring := mustPrimaryKeyring(t, "primary", 7, bytes.NewReader(bytes.Repeat([]byte{9}, 64)))
	for _, tenant := range []string{"tenant-a", "tenant-b", "tenant-\u03b4"} {
		handle, err := keyring.Issue(tenant)
		if err != nil {
			t.Fatalf("Issue(%q): %v", tenant, err)
		}
		identifier, err := keyring.Verify(tenant, handle)
		if err != nil || len(identifier) != 16 {
			t.Fatalf("Verify(%q) = %x, %v", tenant, identifier, err)
		}
		if _, err := keyring.Verify(tenant+"-other", handle); err != ErrInvalidHandle {
			t.Fatalf("cross-tenant Verify(%q) error = %v, want ErrInvalidHandle", tenant, err)
		}
	}
}
