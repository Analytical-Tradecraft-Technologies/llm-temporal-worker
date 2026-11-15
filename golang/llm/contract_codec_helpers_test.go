package llm

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"
)

func TestPatchSetHelpers(t *testing.T) {
	failed := errors.New("failed")
	invalid := ServiceClass("bogus")
	seed := int64(1)
	if err := firstError(nil, checkSetEnum(Patch[ServiceClass]{Clear: true}, "service_class", ServiceClass.Valid), checkSet(Patch[int64]{}, func(int64) error { return failed })); err != nil {
		t.Fatalf("checks of unset patches = %v", err)
	}
	if err := checkSetEnum(Patch[ServiceClass]{Set: &invalid}, "service_class", ServiceClass.Valid); err == nil || err.Error() != `service_class: invalid value "bogus"` {
		t.Fatalf("checkSetEnum(invalid) = %v", err)
	}
	if err := firstError(nil, checkSet(Patch[int64]{Set: &seed}, func(int64) error { return failed }), errors.New("later")); err != failed {
		t.Fatalf("firstError(checkSet(set)) = %v", err)
	}
	if value, err := decodeSetEnum[ServiceClass](json.RawMessage(`"priority"`), "service_class", "service class", ServiceClass.Valid); err != nil || value != ServiceClassPriority {
		t.Fatalf("decodeSetEnum = %q, %v", value, err)
	}
	if _, err := decodeSetEnum[ServiceClass](json.RawMessage(`7`), "service_class", "service class", ServiceClass.Valid); err == nil || err.Error() != "service_class.set: service class is invalid" {
		t.Fatalf("decodeSetEnum(7) = %v", err)
	}
	if _, err := decodeSetWith[int](json.RawMessage(`1`), "seed", func([]byte) (int, error) { return 0, failed }); err == nil || err.Error() != "seed.set: failed" {
		t.Fatalf("decodeSetWith(failing) = %v", err)
	}
}

func TestDecodeContractObjectChecksVersionThenKey(t *testing.T) {
	fields, version, key, err := decodeContractObject([]byte(`{"api_version":"v","operation_key":"op","x":1}`), "v", "api_version", "operation_key", "x")
	if err != nil || version != "v" || key != "op" || string(fields["x"]) != "1" {
		t.Fatalf("decodeContractObject = %v, %q, %q, %v", fields, version, key, err)
	}
	for data, want := range map[string]string{
		`{"api_version":"w","operation_key":"op"}`: `api_version "w" is unsupported`,
		`{"api_version":"v"}`:                      `missing required JSON field "operation_key"`,
		`{"api_version":"v","other":1}`:            `unknown JSON field "other"`,
	} {
		if _, _, _, err := decodeContractObject([]byte(data), "v", "api_version", "operation_key"); err == nil || err.Error() != want {
			t.Errorf("decodeContractObject(%s) = %v, want %q", data, err, want)
		}
	}
}

func TestCopyEachAndCopyOptional(t *testing.T) {
	double := func(value int) (int, bool) { return value * 2, value >= 0 }
	if got, ok := copyEach([]int(nil), double); !ok || got == nil || len(got) != 0 {
		t.Fatalf("copyEach(nil) = %#v, %v", got, ok)
	}
	if got, ok := copyEach([]int{1, 2}, double); !ok || fmt.Sprint(got) != "[2 4]" {
		t.Fatalf("copyEach = %v, %v", got, ok)
	}
	if got, ok := copyEach([]int{1, -1}, double); ok || got != nil {
		t.Fatalf("copyEach(failing) = %v, %v", got, ok)
	}
	three, toPointer := 3, func(value int) (*int, bool) { return &value, value >= 0 }
	if got, ok := copyOptional(nil, toPointer); !ok || got != nil {
		t.Fatalf("copyOptional(nil) = %v, %v", got, ok)
	}
	if got, ok := copyOptional(&three, toPointer); !ok || *got != 3 {
		t.Fatalf("copyOptional = %v, %v", got, ok)
	}
}
