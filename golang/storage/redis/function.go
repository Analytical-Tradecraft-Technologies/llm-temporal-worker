package redis

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	redisclient "github.com/redis/go-redis/v9"
)

const (
	AdmissionFunctionLibrary = "llmtw_admission_v1"
	// Redis Function identifiers permit letters, numbers, and underscores.
	// Keep the executable identity distinct from the admission record schema
	// (which remains admission/v1 inside the Lua payload).
	AdmissionFunctionVersion = "admission_v1"
)

// AdmissionMode selects the server-side transaction contract. Functions are
// the preferred Redis 7+ path. Lua is retained only for explicitly configured
// compatibility deployments whose preloaded script is verified by readiness.
type AdmissionMode string

const (
	AdmissionModeFunction AdmissionMode = "function"
	AdmissionModeLua      AdmissionMode = "lua"
)

type FunctionMetadata struct {
	Library string
	Version string
	Digest  string
}

// FunctionInvoker is the deliberately small seam used by the stores. The
// production implementation executes a preloaded Redis Function or a
// preloaded Lua script; tests can provide an offline command/function harness
// without importing a Redis server.
type FunctionInvoker interface {
	Run(context.Context, string, []string, ...string) ([]any, error)
}

// StringReader is the read-only subset needed to resolve operation and
// continuation records after a command/network error. The store never blindly
// retries a mutation; callers resolve the result by reading the record.
type StringReader interface {
	Get(context.Context, string) (string, error)
}

type redisInvoker struct {
	client  redisclient.Scripter
	mode    AdmissionMode
	version string
}

type functionCaller interface {
	FCall(context.Context, string, []string, ...interface{}) *redisclient.Cmd
}

func (invoker redisInvoker) Run(ctx context.Context, name string, keys []string, args ...string) ([]any, error) {
	if invoker.version == "" {
		invoker.version = AdmissionFunctionVersion
	}
	if name != invoker.version {
		return nil, fmt.Errorf("unsupported Redis function %q", name)
	}
	if invoker.client == nil {
		return nil, fmt.Errorf("Redis Function client is required")
	}
	values := make([]interface{}, 0, len(args))
	for _, value := range args {
		values = append(values, value)
	}
	var (
		result interface{}
		err    error
	)
	switch invoker.mode {
	case AdmissionModeFunction:
		client, ok := invoker.client.(functionCaller)
		if !ok {
			return nil, fmt.Errorf("Redis Function client does not support FCALL")
		}
		result, err = client.FCall(ctx, name, keys, values...).Result()
	case AdmissionModeLua:
		// Do not fall back to EVAL on NOSCRIPT. Readiness verifies that the
		// configured compatibility script is already present, so executing a
		// transaction must never mutate shared Redis code as a side effect.
		result, err = invoker.client.EvalSha(ctx, admissionScript.Hash(), keys, values...).Result()
	default:
		return nil, fmt.Errorf("unsupported Redis admission mode %q", invoker.mode)
	}
	if err != nil {
		return nil, err
	}
	array, ok := result.([]interface{})
	if !ok {
		return nil, fmt.Errorf("Redis function returned an invalid result")
	}
	return array, nil
}

type redisReader struct {
	client interface {
		Get(context.Context, string) *redisclient.StringCmd
	}
}

func (reader redisReader) Get(ctx context.Context, key string) (string, error) {
	return reader.client.Get(ctx, key).Result()
}

var admissionScript = redisclient.NewScript(admissionFunctionSource)

var admissionFunctionLibrarySource = "#!lua name=" + AdmissionFunctionLibrary + "\n" +
	"redis.register_function('" + AdmissionFunctionVersion + "', function(KEYS, ARGV)\n" +
	admissionFunctionSource + "\nend)\n"

// AdmissionFunctionDigest is checked by readiness/deployment tooling before a
// worker starts polling. It is a SHA-256 of the exact immutable Redis Function
// library bytes, including the library name and registered version.
func AdmissionFunctionDigest() string {
	digest := sha256.Sum256([]byte(admissionFunctionLibrarySource))
	return hex.EncodeToString(digest[:])
}

// AdmissionLuaDigest identifies the immutable compatibility script. It is a
// SHA-256 used in configuration, while Redis SCRIPT EXISTS uses the matching
// SHA-1 returned by AdmissionLuaSHA1.
func AdmissionLuaDigest() string {
	digest := sha256.Sum256([]byte(admissionFunctionSource))
	return hex.EncodeToString(digest[:])
}

// AdmissionLuaSHA1 is the Redis SCRIPT EXISTS identity for the compatibility
// source. It is computed locally and never causes SCRIPT LOAD.
func AdmissionLuaSHA1() string { return admissionScript.Hash() }

func AdmissionFunctionMetadata() FunctionMetadata {
	return FunctionMetadata{Library: AdmissionFunctionLibrary, Version: AdmissionFunctionVersion, Digest: AdmissionFunctionDigest()}
}

// AdmissionFunctionSource returns a copy of the versioned Redis Function
// library for startup verification and deployment tooling.
func AdmissionFunctionSource() string { return strings.Clone(admissionFunctionLibrarySource) }

// AdmissionLuaSource returns a copy of the explicitly configured Lua
// compatibility fallback source.
func AdmissionLuaSource() string { return strings.Clone(admissionFunctionSource) }
