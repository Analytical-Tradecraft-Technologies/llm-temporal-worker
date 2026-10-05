// Package diagnostic marks errors whose message is safe to show an operator.
//
// The CLI hides unclassified errors behind a keyword filter because they may
// carry provider bodies or credential material. An error built here promises
// that its message names only configuration paths, reference kinds, reference
// names or local file paths, never a resolved secret value or caller content.
package diagnostic

import (
	"errors"
	"regexp"
	"strings"
)

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

// quotedInputValue matches the input values that configuration errors echo:
// backtick-quoted YAML scalars and Go %q-quoted validation values. Field paths
// are unquoted, so they survive redaction.
var quotedInputValue = regexp.MustCompile("`[^`]*`|\"(?:[^\"\\\\]|\\\\.)*\"")

// ConfigMessage returns the first line of a configuration compile error with
// every quoted input value replaced, so the failing field path can be shown
// without echoing what the operator typed. It reports false when nothing
// bounded remains. Both the CLI and the reload log use it, so they cannot
// disagree about what is safe to show.
func ConfigMessage(err error) (string, bool) {
	if err == nil {
		return "", false
	}
	message := strings.TrimSpace(strings.SplitN(err.Error(), "\n", 2)[0])
	message = quotedInputValue.ReplaceAllString(message, "<value>")
	if message == "" || len(message) > 512 {
		return "", false
	}
	return message, true
}
