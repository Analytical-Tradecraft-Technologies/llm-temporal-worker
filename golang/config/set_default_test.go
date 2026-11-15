package config

import "testing"

func TestSetDefaultFillsOnlyZeroValues(t *testing.T) {
	unset, set := "", "kept"
	setDefault(&unset, "default")
	setDefault(&set, "default")
	if unset != "default" || set != "kept" {
		t.Fatalf("strings = %q, %q", unset, set)
	}
	zero, positive := Duration(0), Duration(7)
	setDefault(&zero, Duration(3))
	setDefault(&positive, Duration(3))
	if zero != 3 || positive != 7 {
		t.Fatalf("durations = %d, %d", zero, positive)
	}
}
