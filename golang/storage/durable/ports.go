package durable

import (
	"fmt"
	"reflect"
)

// requiredPort reports whether a callback port is callable. Callback fields
// are intentionally stored as concrete function types, but converting a
// typed nil function to any produces a non-nil interface. Keeping this check
// in one place prevents a partially composed runtime from passing validation
// and panicking only after an Activity has started.
func requiredPort(name string, callback any) error {
	if callback == nil {
		return fmt.Errorf("%w: %s port is required", ErrV1PortsInvalid, name)
	}
	value := reflect.ValueOf(callback)
	if value.Kind() == reflect.Func && value.IsNil() {
		return fmt.Errorf("%w: %s port is required", ErrV1PortsInvalid, name)
	}
	return nil
}
