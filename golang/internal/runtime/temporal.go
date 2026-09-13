package runtime

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/activity"
	"github.com/mfow/llm-temporal-worker/golang/config"
	"go.temporal.io/sdk/client"
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

// DefaultTemporalClientFactory creates an eagerly connected SDK client. A
// production client is authenticated with a compact JWT loaded from a bounded
// file and may send that credential only over its configured TLS connection.
type DefaultTemporalClientFactory struct {
	// Identity overrides the generated worker/client identity. It is useful for
	// tests and for deployments that already provide a stable identity.
	Identity string
	// ReadFile is injectable for tests. The default reads bounded local files.
	ReadFile func(string) ([]byte, error)
	// DialContext is injectable for tests and custom transports. Production
	// callers normally leave it nil, which uses the official eager SDK dial.
	DialContext TemporalDialContext
}

func (factory DefaultTemporalClientFactory) New(ctx context.Context, value config.Config) (client.Client, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	options := client.Options{
		HostPort:  value.Temporal.Target,
		Namespace: value.Temporal.Namespace,
		Identity:  factory.identity(value.Temporal.IdentityPrefix),
		// The client converter is used by the SDK before a registered Activity
		// handler runs. Install the bounded wrapper here, at the normal decode
		// path, rather than relying on standalone payload helpers.
		DataConverter: activity.BoundedDataConverter(activity.PayloadLimits{MaxInlineBytes: value.Server.InlinePayloadBytes}),
	}
	if value.Environment == "production" {
		if !value.Temporal.TLS.Enabled {
			return nil, errors.New("Temporal TLS is required in production")
		}
		if value.Temporal.APIKeyFile == "" {
			return nil, errors.New("Temporal API key file is required in production")
		}
	}
	if value.Temporal.APIKeyFile != "" && !value.Temporal.TLS.Enabled {
		return nil, errors.New("Temporal TLS is required when an API key is configured")
	}
	apiKeyReadFile := factory.ReadFile
	if apiKeyReadFile == nil {
		apiKeyReadFile = readBoundedTemporalAPIKeyFile
	}
	if value.Temporal.APIKeyFile != "" {
		token, err := loadTemporalJWT(value.Temporal.APIKeyFile, apiKeyReadFile)
		if err != nil {
			return nil, err
		}
		options.Credentials = client.NewAPIKeyStaticCredentials(token)
	}
	if value.Temporal.TLS.Enabled {
		tlsReadFile := factory.ReadFile
		if tlsReadFile == nil {
			tlsReadFile = readBoundedFile
		}
		tlsConfig, err := loadTLSConfig(value.Temporal.TLS, tlsReadFile)
		if err != nil {
			return nil, err
		}
		options.ConnectionOptions.TLS = tlsConfig
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
	return &tls.Config{
		MinVersion: tls.VersionTLS13,
		ServerName: value.ServerName,
		RootCAs:    pool,
	}, nil
}

func loadTemporalJWT(path string, readFile func(string) ([]byte, error)) (string, error) {
	if path == "" {
		return "", errors.New("Temporal JWT file is required")
	}
	if readFile == nil {
		return "", errors.New("Temporal JWT reader is unavailable")
	}
	encoded, err := readFile(path)
	if err != nil {
		return "", errors.New("read Temporal JWT file")
	}
	const maxJWTBytes = 16 << 10
	if len(encoded) == 0 || len(encoded) > maxJWTBytes {
		return "", errors.New("Temporal JWT is invalid")
	}
	token := string(encoded)
	if strings.TrimSpace(token) != token || strings.HasPrefix(strings.ToLower(token), "bearer ") {
		return "", errors.New("Temporal JWT is invalid")
	}
	segments := strings.Split(token, ".")
	if len(segments) != 3 {
		return "", errors.New("Temporal JWT is invalid")
	}
	for _, segment := range segments {
		if segment == "" {
			return "", errors.New("Temporal JWT is invalid")
		}
		if _, err := base64.RawURLEncoding.DecodeString(segment); err != nil {
			return "", errors.New("Temporal JWT is invalid")
		}
	}
	return token, nil
}

func readBoundedTemporalAPIKeyFile(path string) ([]byte, error) {
	return readBoundedFileLimit(path, 16<<10)
}

func readBoundedFile(path string) ([]byte, error) {
	return readBoundedFileLimit(path, 1<<20)
}

func readBoundedFileLimit(path string, maxBytes int64) ([]byte, error) {
	if path == "" {
		return nil, errors.New("file path is required")
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() > maxBytes {
		return nil, errors.New("file is unavailable")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.New("file is unavailable")
	}
	defer file.Close()
	value, err := io.ReadAll(io.LimitReader(file, maxBytes+1))
	if err != nil || int64(len(value)) > maxBytes {
		return nil, errors.New("file exceeds the safe size limit")
	}
	return value, nil
}
