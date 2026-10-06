package redis

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	redisclient "github.com/redis/go-redis/v9"
)

// The throttle library and function names carry a digest of the Lua source,
// like the admission Function's, so a release that changes the Lua loads a
// new library beside the old one and each worker calls its own.
var (
	ThrottleFunctionLibrary = "llmtw_throttle_" + throttleSourceTag()
	ThrottleFunctionVersion = "throttle_" + throttleSourceTag()
)

// throttleSourceTag is the first 64 bits of the Lua source's SHA-256.
func throttleSourceTag() string {
	digest := sha256.Sum256([]byte(throttleFunctionSource))
	return hex.EncodeToString(digest[:8])
}

type ThrottleFunctionMetadata struct {
	Library string
	Version string
	Digest  string
}

type throttleRedisInvoker struct {
	client  redisclient.Scripter
	mode    AdmissionMode
	version string
}

func (invoker throttleRedisInvoker) Run(ctx context.Context, name string, keys []string, args ...string) ([]any, error) {
	if name != invoker.version || name != ThrottleFunctionVersion {
		return nil, fmt.Errorf("unsupported Redis throttle function %q", name)
	}
	values := make([]interface{}, 0, len(args))
	for _, value := range args {
		values = append(values, value)
	}
	var result interface{}
	var err error
	switch invoker.mode {
	case AdmissionModeFunction:
		caller, ok := invoker.client.(functionCaller)
		if !ok {
			return nil, fmt.Errorf("Redis throttle client does not support FCALL")
		}
		result, err = caller.FCall(ctx, name, keys, values...).Result()
	case AdmissionModeLua:
		result, err = invoker.client.EvalSha(ctx, throttleScript.Hash(), keys, values...).Result()
	default:
		return nil, fmt.Errorf("unsupported Redis throttle mode %q", invoker.mode)
	}
	if err != nil {
		return nil, err
	}
	array, ok := result.([]interface{})
	if !ok {
		return nil, fmt.Errorf("Redis throttle function returned an invalid result")
	}
	return array, nil
}

var throttleScript = redisclient.NewScript(throttleFunctionSource)

var throttleFunctionLibrarySource = "#!lua name=" + ThrottleFunctionLibrary + "\n" +
	"redis.register_function('" + ThrottleFunctionVersion + "', function(KEYS, ARGV)\n" + throttleFunctionSource + "\nend)\n"

func ThrottleFunctionSource() string { return strings.Clone(throttleFunctionLibrarySource) }
func ThrottleLuaSource() string      { return strings.Clone(throttleFunctionSource) }

func ThrottleFunctionDigest() string {
	digest := sha256.Sum256([]byte(throttleFunctionLibrarySource))
	return hex.EncodeToString(digest[:])
}

func ThrottleLuaDigest() string {
	digest := sha256.Sum256([]byte(throttleFunctionSource))
	return hex.EncodeToString(digest[:])
}

func ThrottleLuaSHA1() string { return throttleScript.Hash() }

func ThrottleFunctionMetadataValue() ThrottleFunctionMetadata {
	return ThrottleFunctionMetadata{Library: ThrottleFunctionLibrary, Version: ThrottleFunctionVersion, Digest: ThrottleFunctionDigest()}
}
