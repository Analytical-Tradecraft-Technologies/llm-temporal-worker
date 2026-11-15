package state

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"testing"
)

func TestLengthPrefixedMACFramesEveryField(t *testing.T) {
	secret := []byte("secret")
	want := hmac.New(sha256.New, secret)
	want.Write([]byte{0, 0, 0, 2, 'a', 'b', 0, 0, 0, 0, 0, 0, 0, 1, 'c'})
	if got := lengthPrefixedMAC(secret, []byte("ab"), nil, []byte("c")); !bytes.Equal(got, want.Sum(nil)) {
		t.Fatalf("lengthPrefixedMAC = %x, want %x", got, want.Sum(nil))
	}
	// Framing keeps field boundaries: moving a byte across them changes the MAC.
	if bytes.Equal(lengthPrefixedMAC(secret, []byte("ab"), []byte("c")), lengthPrefixedMAC(secret, []byte("a"), []byte("bc"))) {
		t.Fatal("field boundaries do not affect the MAC")
	}
}
