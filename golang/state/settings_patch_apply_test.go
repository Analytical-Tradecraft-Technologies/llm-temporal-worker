package state

import "testing"

func TestPatchApplyHelpers(t *testing.T) {
	t.Run("apply", func(t *testing.T) {
		value := "base"
		Patch[string]{}.apply(&value, "cleared")
		if value != "base" {
			t.Fatalf("omitted patch changed value to %q", value)
		}
		SetPatch("set").apply(&value, "cleared")
		if value != "set" {
			t.Fatalf("set patch gave %q", value)
		}
		ClearPatch[string]().apply(&value, "cleared")
		if value != "cleared" {
			t.Fatalf("clear patch gave %q", value)
		}
	})
	t.Run("applyClone detaches and clears to zero", func(t *testing.T) {
		source := []string{"a"}
		var value []string
		SetPatch(source).applyClone(&value, cloneSlice)
		source[0] = "mutated"
		if len(value) != 1 || value[0] != "a" {
			t.Fatalf("cloned value = %v", value)
		}
		ClearPatch[[]string]().applyClone(&value, cloneSlice)
		if value != nil {
			t.Fatalf("cleared value = %v, want nil", value)
		}
	})
	t.Run("applyPointer detaches and clears to nil", func(t *testing.T) {
		patch := SetPatch(int64(7))
		var value *int64
		patch.applyPointer(&value)
		if value == nil || *value != 7 || value == patch.Set {
			t.Fatalf("pointer value = %v, want a detached 7", value)
		}
		ClearPatch[int64]().applyPointer(&value)
		if value != nil {
			t.Fatalf("cleared pointer = %v, want nil", value)
		}
	})
	t.Run("cloneSet", func(t *testing.T) {
		if got := (Patch[[]string]{}).cloneSet(cloneSlice); got != nil {
			t.Fatalf("omitted cloneSet = %v, want nil", got)
		}
		source := []string{"a"}
		got := SetPatch(source).cloneSet(cloneSlice)
		source[0] = "mutated"
		if got == nil || len(*got) != 1 || (*got)[0] != "a" {
			t.Fatalf("cloneSet = %v", got)
		}
	})
	t.Run("cloneSlice keeps empty as nil", func(t *testing.T) {
		if got := cloneSlice([]string{}); got != nil {
			t.Fatalf("cloneSlice(empty) = %#v, want nil", got)
		}
	})
}
