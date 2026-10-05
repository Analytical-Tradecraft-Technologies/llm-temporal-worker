// Package diagnostic marks errors whose message is safe to show an operator.
//
// The CLI hides unclassified errors behind a keyword filter because they may
// carry provider bodies or credential material. An error built here promises
// that its message names only configuration paths, reference kinds, reference
// names or local file paths, never a resolved secret value or caller content.
package diagnostic

import "errors"

// Error is an operator-safe message wrapping an unclassified cause. Only
// Message is shown; the cause stays available to errors.Is and errors.As.
type Error struct {
	Message string
	Err     error
}

func (e *Error) Error() string { return e.Message }

func (e *Error) Unwrap() error { return e.Err }

// Safe returns an error whose message is safe to show an operator.
func Safe(message string, cause error) error {
	return &Error{Message: message, Err: cause}
}

// Message returns the outermost operator-safe message in err's chain.
func Message(err error) (string, bool) {
	var safe *Error
	if !errors.As(err, &safe) || safe.Message == "" {
		return "", false
	}
	return safe.Message, true
}
