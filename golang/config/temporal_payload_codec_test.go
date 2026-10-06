package config_test

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/schema"
)

const payloadCodecYAML = `  payload_codec:
    kind: aes256_gcm
    keys:
      - id: codec-2026-10
        primary: true
        secret:
          kind: file
          path: /var/run/secrets/temporal-codec
      - id: codec-2026-07
        primary: false
        secret:
          kind: env
          name: TEMPORAL_CODEC_PREVIOUS
`

func withPayloadCodec(t *testing.T, block string) []byte {
	t.Helper()
	base := string(exampleYAML(t))
	const anchor = "  identity_prefix: llmtw\n"
	if !strings.Contains(base, anchor) {
		t.Fatal("example no longer sets temporal.identity_prefix")
	}
	return []byte(strings.Replace(base, anchor, anchor+block, 1))
}

// The codec is opt-in: the example leaves it unset, and an unset codec does
// not appear in canonical JSON, so existing configuration digests are kept.
func TestPayloadCodecIsDisabledByDefault(t *testing.T) {
	loaded, err := config.Load(exampleYAML(t))
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Temporal.PayloadCodec != nil {
		t.Fatalf("example configures a payload codec: %+v", loaded.Temporal.PayloadCodec)
	}
	encoded, err := json.Marshal(loaded)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "payload_codec") {
		t.Fatal("an unset payload codec appears in canonical JSON")
	}

	enabled, err := config.Load(withPayloadCodec(t, payloadCodecYAML))
	if err != nil {
		t.Fatalf("Load() with payload codec = %v", err)
	}
	codec := enabled.Temporal.PayloadCodec
	if codec == nil || codec.Kind != config.PayloadCodecAES256GCM || len(codec.Keys) != 2 || !codec.Keys[0].Primary {
		t.Fatalf("payload codec = %+v", codec)
	}
	before, err := config.Compile(context.Background(), exampleYAML(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	after, err := config.Compile(context.Background(), withPayloadCodec(t, payloadCodecYAML), nil)
	if err != nil {
		t.Fatal(err)
	}
	if before.ConfigVersion() == after.ConfigVersion() {
		t.Fatal("enabling the payload codec did not change the configuration digest")
	}
	paths := enabled.WorkloadIdentityPaths()
	for _, path := range paths {
		if strings.HasPrefix(path, "temporal.payload_codec") {
			t.Fatalf("file/env codec keys reported as workload identity: %v", paths)
		}
	}
}

func TestPayloadCodecValidationFailsClosed(t *testing.T) {
	for name, test := range map[string]struct {
		block string
		want  string
	}{
		"unknown kind": {
			block: "  payload_codec:\n    kind: rot13\n    keys:\n      - id: k1\n        primary: true\n        secret: {kind: env, name: CODEC}\n",
			want:  "temporal.payload_codec.kind must be aes256_gcm",
		},
		"missing kind": {
			block: "  payload_codec:\n    keys:\n      - id: k1\n        primary: true\n        secret: {kind: env, name: CODEC}\n",
			want:  "temporal.payload_codec.kind must be aes256_gcm",
		},
		"no keys": {
			block: "  payload_codec:\n    kind: aes256_gcm\n    keys: []\n",
			want:  "temporal.payload_codec.keys must not be empty",
		},
		"no primary": {
			block: "  payload_codec:\n    kind: aes256_gcm\n    keys:\n      - id: k1\n        primary: false\n        secret: {kind: env, name: CODEC}\n",
			want:  "exactly one primary key",
		},
		"two primaries": {
			block: "  payload_codec:\n    kind: aes256_gcm\n    keys:\n      - id: k1\n        primary: true\n        secret: {kind: env, name: A}\n      - id: k2\n        primary: true\n        secret: {kind: env, name: B}\n",
			want:  "exactly one primary key",
		},
		"duplicate id": {
			block: "  payload_codec:\n    kind: aes256_gcm\n    keys:\n      - id: k1\n        primary: true\n        secret: {kind: env, name: A}\n      - id: k1\n        primary: false\n        secret: {kind: env, name: B}\n",
			want:  `duplicate key ID "k1"`,
		},
		"invalid id": {
			block: "  payload_codec:\n    kind: aes256_gcm\n    keys:\n      - id: \"key one\"\n        primary: true\n        secret: {kind: env, name: A}\n",
			want:  "temporal.payload_codec.keys[0].id must be",
		},
		"relative file secret": {
			block: "  payload_codec:\n    kind: aes256_gcm\n    keys:\n      - id: k1\n        primary: true\n        secret: {kind: file, path: codec.key}\n",
			want:  "temporal.payload_codec.keys[0].secret file secret requires an absolute path",
		},
		"unknown field": {
			block: "  payload_codec:\n    kind: aes256_gcm\n    endpoint: https://codec.example\n    keys:\n      - id: k1\n        primary: true\n        secret: {kind: env, name: A}\n",
			want:  "endpoint",
		},
	} {
		_, err := config.Load(withPayloadCodec(t, test.block))
		if err == nil || !strings.Contains(err.Error(), test.want) {
			t.Errorf("%s: Load() = %v, want %q", name, err, test.want)
		}
	}
}

func TestPayloadCodecWorkloadIdentityPath(t *testing.T) {
	block := "  payload_codec:\n    kind: aes256_gcm\n    keys:\n      - id: k1\n        primary: true\n        secret: {kind: workload_identity, audience: codec}\n"
	loaded, err := config.Load(withPayloadCodec(t, block))
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, path := range loaded.WorkloadIdentityPaths() {
		found = found || path == "temporal.payload_codec.keys[0].secret"
	}
	if !found {
		t.Fatalf("WorkloadIdentityPaths() = %v", loaded.WorkloadIdentityPaths())
	}
}

func TestConfigSchemaAcceptsPayloadCodec(t *testing.T) {
	schemaData, err := os.ReadFile("../api/schema/v1/config.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := schema.Parse(schemaData)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := config.Load(withPayloadCodec(t, payloadCodecYAML))
	if err != nil {
		t.Fatal(err)
	}
	for name, test := range map[string]struct {
		mutate func(*config.PayloadCodecConfig)
		valid  bool
	}{
		"valid":        {mutate: func(*config.PayloadCodecConfig) {}, valid: true},
		"unknown kind": {mutate: func(c *config.PayloadCodecConfig) { c.Kind = "rot13" }},
		"no keys":      {mutate: func(c *config.PayloadCodecConfig) { c.Keys = []config.PayloadCodecKey{} }},
		"invalid id":   {mutate: func(c *config.PayloadCodecConfig) { c.Keys[0].ID = "key one" }},
	} {
		value := loaded.Clone()
		test.mutate(value.Temporal.PayloadCodec)
		encoded, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if err := compiled.Validate(encoded); (err == nil) != test.valid {
			t.Errorf("%s: schema validation = %v, want valid=%t", name, err, test.valid)
		}
	}
}
