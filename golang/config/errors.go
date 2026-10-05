package config

import (
	"errors"

	"go.yaml.in/yaml/v4"
)

// IsYAMLError reports whether err is a decoding failure from the YAML loader:
// malformed syntax, an unknown or duplicate key, or a value of the wrong type.
// A document that decodes but fails Validate is not a YAML error.
func IsYAMLError(err error) bool {
	var load *yaml.LoadError
	var loads *yaml.LoadErrors
	return errors.As(err, &load) || errors.As(err, &loads)
}
