package openairesponses

import (
	"errors"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

// isUnsupportedCapability reports whether a strict representability rejection
// uses the documented unsupported_capability code.
func isUnsupportedCapability(err error) bool {
	var mapped *provider.Error
	return errors.As(err, &mapped) && mapped.Code == provider.CodeUnsupportedCapability
}
