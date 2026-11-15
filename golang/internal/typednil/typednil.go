// Package typednil reports whether an interface value is nil, including a
// non-nil interface that holds a nil pointer, map, slice, channel, function
// or interface. Constructors use it to reject a typed-nil dependency at
// composition time instead of panicking when a method is invoked later.
package typednil

import "reflect"

// Is reports whether value is nil or holds a nil channel, function,
// interface, map, pointer or slice.
func Is(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
