package runtime

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/activity"
	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/internal/observability"
	"github.com/mfow/llm-temporal-worker/golang/internal/secrets"
	"github.com/mfow/llm-temporal-worker/golang/temporalcodec"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// Temporal's auto-setup image can report cluster health while the frontend
	// is still completing its first RPC accept. Keep the eager client boundary
	// fail-closed, but absorb that narrow transient with a short bounded retry.
	temporalDialMaxAttempts    = 5
	temporalDialInitialBackoff = 250 * time.Millisecond
	temporalDialMaxBackoff     = 2 * time.Second
)

// TemporalDialContext is the eager Temporal SDK dial seam used by the
// production factory. It is exported so applications embedding the runtime
// can provide a transport-aware dialer without replacing the whole runtime.
type TemporalDialContext func(context.Context, client.Options) (client.Client, error)

type temporalDialRetryPolicy struct {
	maxAttempts    int
	initialBackoff time.Duration
	maxBackoff     time.Duration
}

// TemporalClientFactory is the process boundary for creating a Temporal
// client. Runtime construction uses this interface so tests and local tools
// can avoid a live cluster while production uses the official SDK client.
type TemporalClientFactory interface {
	New(context.Context, config.Config) (client.Client, error)
}

// TemporalClientFactoryFunc adapts a function to TemporalClientFactory.
type TemporalClientFactoryFunc func(context.Context, config.Config) (client.Client, error)

func (function TemporalClientFactoryFunc) New(ctx context.Context, value config.Config) (client.Client, error) {
	return function(ctx, value)
}

// DefaultTemporalClientFactory creates an eagerly connected SDK client using
// only non-secret Temporal configuration. Credentials, if a deployment adds
// them through the SDK, stay inside the SDK's credential boundary.
type DefaultTemporalClientFactory struct {
	// Logger is the process logger. When omitted, use the configured log
	// format and level on stderr, never the SDK's legacy default logger.
	Logger *observability.Logger
	// Identity overrides the generated worker/client identity. It is useful for
	// tests and for deployments that already provide a stable identity.
	Identity string
	// ReadFile is injectable for tests. The default reads bounded local files.
	ReadFile func(string) ([]byte, error)
	// DialContext is injectable for tests and custom transports. Production
	// callers normally leave it nil, which uses the official eager SDK dial.
	DialContext TemporalDialContext
	// SecretResolver resolves temporal.payload_codec keys. When omitted, the
	// default env/file resolver is used.
	SecretResolver secrets.Resolver
}

func (factory DefaultTemporalClientFactory) New(ctx context.Context, value config.Config) (client.Client, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	logger := factory.Logger
	if logger == nil {
		var err error
		logger, err = newRuntimeLogger(value, Options{})
		if err != nil {
			return nil, err
		}
	}
	dataConverter, err := factory.dataConverter(ctx, value)
	if err != nil {
		return nil, err
	}
	options := client.Options{
		Logger:        logger.TemporalLogger(),
		HostPort:      value.Temporal.Target,
		Namespace:     value.Temporal.Namespace,
		Identity:      factory.identity(value.Temporal.IdentityPrefix),
		DataConverter: dataConverter,
	}
	if value.Temporal.TLS.Enabled {
		readFile := factory.ReadFile
		if readFile == nil {
			readFile = readBoundedFile
		}
		tlsConfig, err := loadTLSConfig(value.Temporal.TLS, readFile)
		if err != nil {
			return nil, err
		}
		options.ConnectionOptions.TLS = tlsConfig
		if value.Temporal.APIKeyFile != "" {
			encoded, err := readFile(value.Temporal.APIKeyFile)
			if err != nil {
				return nil, errors.New("read Temporal API key")
			}
			key := strings.TrimSpace(string(encoded))
			if key == "" || strings.ContainsAny(key, "\r\n") {
				return nil, errors.New("Temporal API key is invalid")
			}
			options.Credentials = client.NewAPIKeyStaticCredentials(key)
		}
	}
	dial := factory.DialContext
	if dial == nil {
		dial = client.DialContext
	}
	return dialTemporalWithRetry(ctx, dial, options, temporalDialRetryPolicy{
		maxAttempts:    temporalDialMaxAttempts,
		initialBackoff: temporalDialInitialBackoff,
		maxBackoff:     temporalDialMaxBackoff,
	})
}

// dataConverter returns the client converter. The SDK uses it before a
// registered Activity handler runs, so the bounded wrapper is installed here,
// at the normal decode path, rather than relying on standalone payload
// helpers. When temporal.payload_codec is configured, the codec wraps the
// bounded converter: inline limits apply to the plaintext payload, and only
// ciphertext reaches Temporal. Any key that cannot be resolved or used fails
// client construction rather than falling back to plaintext.
func (factory DefaultTemporalClientFactory) dataConverter(ctx context.Context, value config.Config) (converter.DataConverter, error) {
	bounded := activity.BoundedDataConverter(activity.PayloadLimits{MaxInlineBytes: value.Server.InlinePayloadBytes})
	codecConfig := value.Temporal.PayloadCodec
	if codecConfig == nil {
		return bounded, nil
	}
	if codecConfig.Kind != config.PayloadCodecAES256GCM {
		return nil, &payloadCodecError{safe: "Temporal payload codec kind is unsupported"}
	}
	resolver := factory.SecretResolver
	if resolver == nil {
		resolver = secrets.New(secrets.Options{})
	}
	keys := make([]temporalcodec.Key, 0, len(codecConfig.Keys))
	for _, key := range codecConfig.Keys {
		secret, err := resolver.Resolve(ctx, key.Secret)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, ctxErr
			}
			return nil, &payloadCodecError{safe: fmt.Sprintf("resolve Temporal payload codec key %q failed", key.ID), cause: secrets.MarkReference(err)}
		}
		keys = append(keys, temporalcodec.Key{ID: key.ID, Secret: secret, Primary: key.Primary})
	}
	codec, err := temporalcodec.NewAESGCM(keys)
	if err != nil {
		// NewAESGCM errors name only key IDs and lengths, never key bytes.
		return nil, &payloadCodecError{safe: "construct Temporal payload codec: " + err.Error(), cause: err}
	}
	return converter.NewCodecDataConverter(bounded, codec), nil
}

// payloadCodecError is a payload codec configuration failure. Its message
// names only key IDs, so startup can report it without the resolver's cause.
type payloadCodecError struct {
	safe  string
	cause error
}

func (err *payloadCodecError) Error() string { return err.safe }

func (err *payloadCodecError) Unwrap() error { return err.cause }

// dialTemporalWithRetry retries only gRPC Unavailable responses from the
// eager GetSystemInfo handshake. Authentication, TLS, malformed-target, and
// caller cancellation errors remain fail-closed and are returned immediately.
// The retry budget is intentionally short and bounded: the worker must not
// hide a persistent Temporal outage behind an unbounded startup loop.
func dialTemporalWithRetry(ctx context.Context, dial TemporalDialContext, options client.Options, policy temporalDialRetryPolicy) (client.Client, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if dial == nil {
		return nil, errors.New("Temporal client dialer is unavailable")
	}
	if policy.maxAttempts <= 0 {
		policy.maxAttempts = 1
	}
	if policy.initialBackoff <= 0 {
		policy.initialBackoff = temporalDialInitialBackoff
	}
	if policy.maxBackoff < policy.initialBackoff {
		policy.maxBackoff = policy.initialBackoff
	}
	backoff := policy.initialBackoff
	var lastErr error
	for attempt := 1; attempt <= policy.maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		value, err := dial(ctx, options)
		if err == nil {
			return value, nil
		}
		lastErr = err
		if !isTemporalDialRetryable(err) || attempt == policy.maxAttempts {
			return nil, err
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return nil, ctx.Err()
		case <-timer.C:
		}
		if backoff < policy.maxBackoff/2 {
			backoff *= 2
		} else {
			backoff = policy.maxBackoff
		}
	}
	return nil, lastErr
}

func isTemporalDialRetryable(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	return status.Code(err) == codes.Unavailable
}

func (factory DefaultTemporalClientFactory) identity(prefix string) string {
	if factory.Identity != "" {
		return factory.Identity
	}
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "worker"
	}
	hostname = sanitizeIdentity(hostname)
	if prefix == "" {
		prefix = "llmtw"
	}
	return fmt.Sprintf("%s-%s-%d", sanitizeIdentity(prefix), hostname, os.Getpid())
}

func sanitizeIdentity(value string) string {
	var builder strings.Builder
	for _, runeValue := range value {
		switch {
		case runeValue >= 'a' && runeValue <= 'z', runeValue >= 'A' && runeValue <= 'Z', runeValue >= '0' && runeValue <= '9', runeValue == '-', runeValue == '_', runeValue == '.':
			builder.WriteRune(runeValue)
		default:
			builder.WriteByte('-')
		}
	}
	if builder.Len() == 0 {
		return "worker"
	}
	return builder.String()
}

func loadTLSConfig(value config.TLSConfig, readFile func(string) ([]byte, error)) (*tls.Config, error) {
	if !value.Enabled {
		return nil, nil
	}
	if readFile == nil {
		return nil, errors.New("Temporal TLS CA reader is unavailable")
	}
	encoded, err := readFile(value.CAFile)
	if err != nil {
		return nil, errors.New("read Temporal TLS CA certificate")
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(encoded) {
		return nil, errors.New("Temporal TLS CA certificate is invalid")
	}
	result := &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: value.ServerName,
		RootCAs:    pool,
	}
	if value.CertFile != "" {
		certificate, err := readFile(value.CertFile)
		if err != nil {
			return nil, errors.New("read Temporal TLS client certificate")
		}
		key, err := readFile(value.KeyFile)
		if err != nil {
			return nil, errors.New("read Temporal TLS client key")
		}
		pair, err := tls.X509KeyPair(certificate, key)
		if err != nil {
			return nil, errors.New("Temporal TLS client certificate or key is invalid")
		}
		result.Certificates = []tls.Certificate{pair}
	}
	return result, nil
}

func readBoundedFile(path string) ([]byte, error) {
	// Kubernetes projected Secrets use symlinks. The shared reader follows
	// them and validates the opened target while keeping reads bounded.
	const maxBytes = 1 << 20
	return secrets.ReadSecretFile(context.Background(), path, maxBytes)
}
