package runtime

import (
	"context"
	"errors"
	"io/fs"
	"reflect"
	"strings"

	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/internal/app"
	"github.com/mfow/llm-temporal-worker/golang/internal/diagnostic"
	"github.com/mfow/llm-temporal-worker/golang/internal/secrets"
)

// The reload failure classes are a closed set, so they are safe as a log
// attribute. They are not a label on llmtw_config_reload_total, whose label
// set is outcome only.
const (
	reloadCauseRead            = "read"
	reloadCauseYAML            = "yaml"
	reloadCauseValidation      = "validation"
	reloadCauseSecret          = "secret"
	reloadCauseProcessLifetime = "process_lifetime"
	reloadCauseCatalog         = "catalog"
	reloadCauseDependency      = "dependency"
	reloadCauseCanceled        = "canceled"
	reloadCauseInternal        = "internal"
)

// catalogLoadError marks a capability or pricing catalog that could not be
// read, authenticated against its configured digest, or decoded. The message
// is unchanged; it quotes the catalog path and is never logged.
type catalogLoadError struct{ cause error }

func (err *catalogLoadError) Error() string { return err.cause.Error() }

func (err *catalogLoadError) Unwrap() error { return err.cause }

// classifyReloadFailure reduces a rejected reload to a bounded cause class
// and, for a validation or process-lifetime rejection, the configuration field
// path. It never returns a configured value, file path or error text.
func classifyReloadFailure(err error) (cause, field string) {
	stage, staged := app.ReloadStageOf(err)
	var change *processLifetimeChangeError
	var catalog *catalogLoadError
	var path *fs.PathError
	switch {
	case errors.As(err, &change):
		return reloadCauseProcessLifetime, change.field
	case stage == app.ReloadStageReplacement:
		return reloadCauseProcessLifetime, ""
	case stage == app.ReloadStageVerify:
		return reloadCauseDependency, ""
	// Cancellation is checked before the secret and catalog markers: a
	// resolver or loader interrupted by a canceled reload is not their fault.
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return reloadCauseCanceled, ""
	case errors.Is(err, secrets.ErrReference):
		return reloadCauseSecret, ""
	case errors.As(err, &catalog):
		return reloadCauseCatalog, ""
	case stage == app.ReloadStageBuild && config.IsYAMLError(err):
		return reloadCauseYAML, ""
	case stage == app.ReloadStageBuild:
		return reloadCauseValidation, validationFieldPath(staged)
	case stage == app.ReloadStageClients:
		return reloadCauseDependency, ""
	case errors.As(err, &path):
		return reloadCauseRead, ""
	default:
		return reloadCauseInternal, ""
	}
}

// validationFieldPath finds the field a validation error names. Callers wrap
// the validation error with their own context, so each error in the chain is
// tried, outermost first.
func validationFieldPath(err error) string {
	for ; err != nil; err = errors.Unwrap(err) {
		message, ok := diagnostic.ConfigMessage(err)
		if !ok {
			continue
		}
		if field := configFieldPath(message); field != "" {
			return field
		}
	}
	return ""
}

// configFieldPath returns the configuration schema path that message starts
// with, or "" when it does not start with one. The message is first reduced by
// diagnostic.ConfigMessage, the same redaction the CLI prints. Only names
// declared by config.Config are kept: an operator-chosen map key becomes "*"
// and anything unrecognized ends the path, so the result cannot carry a value.
func configFieldPath(message string) string {
	token, _, _ := strings.Cut(message, " ")
	token = strings.TrimRight(token, ":,")
	current := reflect.TypeFor[config.Config]()
	var path []string
	for _, segment := range strings.Split(token, ".") {
		for current.Kind() == reflect.Pointer || current.Kind() == reflect.Slice {
			current = current.Elem()
		}
		if current.Kind() == reflect.Map {
			path = append(path, "*")
			current = current.Elem()
			continue
		}
		if current.Kind() != reflect.Struct {
			break
		}
		name, index, indexed := strings.Cut(segment, "[")
		next, ok := yamlFieldType(current, name)
		if !ok {
			break
		}
		if indexed && (next.Kind() != reflect.Slice || !isListIndex(index)) {
			path = append(path, name)
			break
		}
		path = append(path, segment)
		current = next
	}
	return strings.Join(path, ".")
}

func yamlFieldType(structure reflect.Type, name string) (reflect.Type, bool) {
	if name == "" {
		return nil, false
	}
	for index := range structure.NumField() {
		field := structure.Field(index)
		tag, _, _ := strings.Cut(field.Tag.Get("yaml"), ",")
		if tag == name {
			return field.Type, true
		}
	}
	return nil, false
}

// isListIndex accepts the remainder of a "name[12]" segment after the bracket.
func isListIndex(value string) bool {
	digits, ok := strings.CutSuffix(value, "]")
	if !ok || digits == "" || len(digits) > 6 {
		return false
	}
	return strings.Trim(digits, "0123456789") == ""
}
