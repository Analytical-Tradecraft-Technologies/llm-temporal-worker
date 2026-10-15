package toolcallid

import (
	"regexp"
	"strings"
	"testing"
)

var portablePattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

func TestPortableKeepsValidIDs(t *testing.T) {
	for _, id := range []string{"call_1", "toolu_01ABC-xyz", strings.Repeat("a", MaxLength)} {
		if got := Portable(id); got != id {
			t.Fatalf("Portable(%q) = %q, want unchanged", id, got)
		}
	}
}

func TestPortableRemapsInvalidIDsDeterministically(t *testing.T) {
	for _, id := range []string{"functions.lookup:0", "call id", "é", strings.Repeat("b", MaxLength+1), strings.Repeat(".", 200)} {
		got := Portable(id)
		if !portablePattern.MatchString(got) {
			t.Fatalf("Portable(%q) = %q, not portable", id, got)
		}
		if again := Portable(id); again != got {
			t.Fatalf("Portable(%q) is not deterministic: %q then %q", id, got, again)
		}
	}
	if got := Portable("functions.lookup:0"); !strings.HasPrefix(got, "functions_lookup_0_") {
		t.Fatalf("Portable kept no readable prefix: %q", got)
	}
}

func TestPortableKeepsDistinctIDsDistinct(t *testing.T) {
	if Portable("functions.lookup:0") == Portable("functions:lookup.0") {
		t.Fatal("IDs that differ only in disallowed characters collided")
	}
	if Portable("functions.lookup:0") == Portable("functions.lookup:1") {
		t.Fatal("distinct IDs collided")
	}
}

// A portable ID shaped like a remapped one is remapped as well, so it cannot
// collide with the remapping of a different, non-portable ID.
func TestPortableKeepsRemappedIDsDisjointFromPassThroughIDs(t *testing.T) {
	remapped := Portable("functions.lookup:0")
	if got := Portable(remapped); got == remapped {
		t.Fatalf("portable ID %q shaped like a remapped ID passed through and collides with the remapping of functions.lookup:0", remapped)
	}
	if got := Portable("call_0123456789ABCDEF"); got != "call_0123456789ABCDEF" {
		t.Fatalf("ID without a lowercase hex suffix was remapped to %q", got)
	}
}
