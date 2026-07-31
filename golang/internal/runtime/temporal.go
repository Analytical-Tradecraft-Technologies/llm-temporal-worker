package runtime

import (
	"context"
	"crypto/tls"
	"crypto/x509"
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

// DefaultTemporalClientFactory creates an eagerly connected SDK client using
// only non-secret Temporal configuration. Credentials, if a deployment adds
// them through the SDK, stay inside the SDK's credential boundary.
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
		DataConverter: activity.BoundedDataConverter(activity.PayloadLimits{}),
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
		MinVersion: tls.VersionTLS12,
		ServerName: value.ServerName,
		RootCAs:    pool,
	}, nil
}

func readBoundedFile(path string) ([]byte, error) {
	if path == "" {
		return nil, errors.New("file path is required")
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() {
		return nil, errors.New("file is unavailable")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.New("file is unavailable")
	}
	defer file.Close()
	const maxBytes = 1 << 20
	value, err := io.ReadAll(io.LimitReader(file, maxBytes+1))
	if err != nil || len(value) > maxBytes {
		return nil, errors.New("file exceeds the safe size limit")
	}
	return value, nil
}
