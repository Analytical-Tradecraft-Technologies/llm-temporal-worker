package schema

import (
	"fmt"
	"strings"

	jsonschema "github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/santhosh-tekuri/jsonschema/v6/kind"
)

// ValidationError is a bounded, safe schema-instance error. Instance holds no
// value by design; only the canonical pointer, keyword, and safe explanation
// are exposed. Path replaces every property name the schema does not declare
// with "*", and Message names only the failing keyword's kind and schema-side
// bounds, never instance content or the schema's value lists.
type ValidationError struct {
	Path     string
	Keyword  string
	Message  string
	Instance string
}

func (err *ValidationError) Error() string {
	path := err.Path
	if path == "" {
		path = "/"
	}
	if err.Keyword == "" {
		return fmt.Sprintf("schema validation failed at %s: %s", path, err.Message)
	}
	return fmt.Sprintf("schema validation failed at %s (%s): %s", path, err.Keyword, err.Message)
}

func normalizeValidationError(err error) error {
	validationErr, ok := err.(*jsonschema.ValidationError)
	if !ok {
		return fmt.Errorf("schema validation failed")
	}
	output := validationErr.BasicOutput()
	leaf := firstOutputError(output)
	if leaf == nil {
		return &ValidationError{Message: "instance does not satisfy schema"}
	}
	keyword := lastPointerToken(leaf.KeywordLocation)
	if _, ok := supportedKeywords[keyword]; !ok {
		// A false schema fails at its own location, which ends in a
		// property name or index rather than a keyword.
		keyword = ""
	}
	// The validator's own message quotes the instance (a pattern's string,
	// additional property names, numbers) or the schema's whole value list,
	// so the message is built from the error kind alone (#1661).
	return &ValidationError{
		Path:     safeInstancePath(leaf.InstanceLocation, leaf.KeywordLocation),
		Keyword:  keyword,
		Message:  safeMessage(leaf.Error),
		Instance: "",
	}
}

// maxMessageBytes and maxPathBytes bound a ValidationError's text whatever
// the schema's size.
const (
	maxMessageBytes = 256
	maxPathBytes    = 256
)

// safeMessage describes a validation failure without instance content or
// schema value lists. Only schema-side counts, bounds and type names appear.
func safeMessage(failure *jsonschema.OutputError) string {
	if failure == nil {
		return "instance does not satisfy schema"
	}
	var message string
	switch failure := failure.Kind.(type) {
	case *kind.Type:
		message = fmt.Sprintf("got %s, want %s", failure.Got, strings.Join(failure.Want, " or "))
	case *kind.Enum:
		message = "value is not one of the enum values"
	case *kind.Const:
		message = "value does not equal const"
	case *kind.Required:
		message = fmt.Sprintf("object is missing %d required properties", len(failure.Missing))
	case *kind.AdditionalProperties:
		message = fmt.Sprintf("object has %d properties the schema does not allow", len(failure.Properties))
	case *kind.PropertyNames:
		message = "a property name does not satisfy propertyNames"
	case *kind.Pattern:
		message = "string does not match pattern"
	case *kind.MinLength:
		message = fmt.Sprintf("string is shorter than %d characters", failure.Want)
	case *kind.MaxLength:
		message = fmt.Sprintf("string is longer than %d characters", failure.Want)
	case *kind.MinItems:
		message = fmt.Sprintf("array has fewer than %d items", failure.Want)
	case *kind.MaxItems:
		message = fmt.Sprintf("array has more than %d items", failure.Want)
	case *kind.MinProperties:
		message = fmt.Sprintf("object has fewer than %d properties", failure.Want)
	case *kind.MaxProperties:
		message = fmt.Sprintf("object has more than %d properties", failure.Want)
	case *kind.UniqueItems:
		message = "array items are not unique"
	case *kind.Minimum:
		message = "number is below minimum"
	case *kind.Maximum:
		message = "number is above maximum"
	case *kind.ExclusiveMinimum:
		message = "number is not above exclusiveMinimum"
	case *kind.ExclusiveMaximum:
		message = "number is not below exclusiveMaximum"
	case *kind.MultipleOf:
		message = "number is not a multiple of multipleOf"
	case *kind.FalseSchema:
		message = "the schema allows no value here"
	case *kind.Not:
		message = "value matches a not schema"
	case *kind.OneOf:
		message = "value does not match exactly one oneOf schema"
	case *kind.AnyOf:
		message = "value matches no anyOf schema"
	case *kind.Format:
		message = "value does not match format"
	case *kind.ContentEncoding:
		message = "string does not match contentEncoding"
	case *kind.ContentMediaType:
		message = "string does not match contentMediaType"
	default:
		message = "instance does not satisfy schema"
	}
	if len(message) > maxMessageBytes {
		message = message[:maxMessageBytes]
	}
	return message
}

// safeInstancePath returns the failing instance pointer with every token the
// schema did not supply replaced by "*". Array indexes are kept, and so is a
// property name that a "properties" keyword on the failing keyword's path
// declares; any other name, such as one matched by additionalProperties,
// came from the model. A pointer over maxPathBytes keeps its trailing
// tokens behind a leading "/…".
func safeInstancePath(instance, keyword string) string {
	if instance == "" {
		return ""
	}
	declared := map[string]bool{}
	keywordTokens := strings.Split(strings.TrimPrefix(keyword, "/"), "/")
	for index := 0; index+1 < len(keywordTokens); index++ {
		if keywordTokens[index] == "properties" {
			declared[keywordTokens[index+1]] = true
		}
	}
	tokens := strings.Split(strings.TrimPrefix(instance, "/"), "/")
	for index, token := range tokens {
		if !declared[token] && !isArrayIndex(token) {
			tokens[index] = "*"
		}
	}
	path := "/" + strings.Join(tokens, "/")
	for len(path) > maxPathBytes && len(tokens) > 1 {
		tokens = tokens[1:]
		path = "/…/" + strings.Join(tokens, "/")
	}
	if len(path) > maxPathBytes {
		return "/…"
	}
	return path
}

func isArrayIndex(token string) bool {
	if token == "" {
		return false
	}
	for _, c := range token {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

func firstOutputError(output *jsonschema.OutputUnit) *jsonschema.OutputUnit {
	if output == nil {
		return nil
	}
	if output.Error != nil {
		return output
	}
	for index := range output.Errors {
		if leaf := firstOutputError(&output.Errors[index]); leaf != nil {
			return leaf
		}
	}
	return nil
}

func lastPointerToken(pointer string) string {
	if pointer == "" || pointer == "#" {
		return ""
	}
	if index := strings.LastIndexByte(pointer, '/'); index >= 0 {
		pointer = pointer[index+1:]
	}
	return strings.ReplaceAll(strings.ReplaceAll(pointer, "~1", "/"), "~0", "~")
}
