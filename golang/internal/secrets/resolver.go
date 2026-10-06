// Package secrets resolves configuration references without ever putting the
// resolved value back into a configuration snapshot or an error message.
package secrets

import (
	"context"
	"errors"
	"fmt"

	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/internal/diagnostic"
)

const DefaultMaxBytes int64 = 64 << 10

type Resolver interface {
	Resolve(context.Context, config.SecretRef) ([]byte, error)
}

type ResolverFunc func(context.Context, config.SecretRef) ([]byte, error)

func (function ResolverFunc) Resolve(ctx context.Context, ref config.SecretRef) ([]byte, error) {
	return function(ctx, ref)
}

type Options struct {
	MaxBytes  int64
	LookupEnv func(string) (string, bool)
	ReadFile  func(context.Context, string, int64) ([]byte, error)
	Workload  WorkloadIdentity
}

type DefaultResolver struct {
	maxBytes  int64
	lookupEnv func(string) (string, bool)
	readFile  func(context.Context, string, int64) ([]byte, error)
	workload  WorkloadIdentity
}

func New(options Options) *DefaultResolver {
	if options.MaxBytes <= 0 {
		options.MaxBytes = DefaultMaxBytes
	}
	if options.LookupEnv == nil {
		options.LookupEnv = lookupEnvironment
	}
	if options.ReadFile == nil {
		options.ReadFile = ReadSecretFile
	}
	return &DefaultResolver{maxBytes: options.MaxBytes, lookupEnv: options.LookupEnv, readFile: options.ReadFile, workload: options.Workload}
}

func (resolver *DefaultResolver) Resolve(ctx context.Context, ref config.SecretRef) ([]byte, error) {
	if resolver == nil {
		return nil, fmt.Errorf("secret resolver is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := ref.Validate("secret"); err != nil {
		return nil, err
	}
	var (
		value []byte
		err   error
	)
	switch ref.Kind {
	case config.SecretEnv:
		var ok bool
		var text string
		text, ok = resolver.lookupEnv(ref.Name)
		if !ok || text == "" {
			return nil, fmt.Errorf("environment secret %q %w", ref.Name, ErrUnset)
		}
		value = []byte(text)
	case config.SecretFile:
		value, err = resolver.readFile(ctx, ref.Path, resolver.maxBytes)
	case config.SecretWorkloadIdentity:
		if resolver.workload == nil {
			return nil, fmt.Errorf("workload identity resolver is unavailable")
		}
		value, err = resolver.workload.Resolve(ctx, ref.Audience)
	default:
		return nil, fmt.Errorf("unsupported secret kind %q", ref.Kind)
	}
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(value) == 0 {
		return nil, fmt.Errorf("secret reference resolved to an empty value")
	}
	if int64(len(value)) > resolver.maxBytes {
		return nil, fmt.Errorf("secret reference exceeds the configured size limit")
	}
	return append([]byte(nil), value...), nil
}

// ErrUnset marks an environment secret that is not set (or is empty), as
// opposed to one that failed to resolve. Its text completes the message
// "environment secret NAME is not set".
var ErrUnset = errors.New("is not set")

// ErrReference marks a configuration secret reference that could not be
// resolved, so callers can classify the failure without reading its message.
var ErrReference = errors.New("secret reference could not be resolved")

// referenceError carries ErrReference without changing the cause's message.
type referenceError struct{ cause error }

func (err *referenceError) Error() string { return err.cause.Error() }

func (err *referenceError) Unwrap() []error { return []error{ErrReference, err.cause} }

// MarkReference marks err as a secret reference that could not be resolved. A
// nil error stays nil.
func MarkReference(err error) error {
	if err == nil {
		return nil
	}
	return &referenceError{cause: err}
}

// ConfigResolver validates every SecretRef that is represented as a SecretRef
// in the external configuration. It intentionally discards returned bytes.
// Provider auth names are resolved by the provider client factory, where their
// authentication semantics are known.
type ConfigResolver struct{ Resolver Resolver }

func (resolver ConfigResolver) Resolve(ctx context.Context, value *config.Config) error {
	if resolver.Resolver == nil {
		return fmt.Errorf("secret resolver is required")
	}
	if value == nil {
		return fmt.Errorf("configuration is nil")
	}
	refs := make([]config.SecretRef, 0, 4+len(value.Continuation.HandleKeys))
	if value.State.Kind != config.StateKindMemory {
		refs = append(refs, value.State.Redis.Username, value.State.Redis.Password)
	}
	for _, key := range value.Continuation.HandleKeys {
		refs = append(refs, key.Secret)
	}
	if value.State.Requests != nil {
		refs = append(refs, value.State.Requests.Secret)
	}
	for index, ref := range refs {
		if _, err := resolver.Resolver.Resolve(ctx, ref); err != nil {
			return diagnostic.Safe(secretReferenceDiagnostic(index, ref, err), MarkReference(err))
		}
	}
	return nil
}

type WorkloadIdentity interface {
	Resolve(context.Context, string) ([]byte, error)
}

type WorkloadIdentityFunc func(context.Context, string) ([]byte, error)

func (function WorkloadIdentityFunc) Resolve(ctx context.Context, audience string) ([]byte, error) {
	return function(ctx, audience)
}

// secretReferenceDiagnostic names the failed reference for an operator. Local
// environment and file failures carry only the reference name or path, so
// their cause is included; workload identity causes come from cloud SDKs and
// are omitted.
func secretReferenceDiagnostic(index int, ref config.SecretRef, cause error) string {
	switch ref.Kind {
	case config.SecretEnv, config.SecretFile:
		return fmt.Sprintf("secret reference %d could not be resolved: %v", index, cause)
	default:
		return fmt.Sprintf("secret reference %d (%s) could not be resolved", index, ref.Kind)
	}
}
