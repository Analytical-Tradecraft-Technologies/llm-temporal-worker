package runtime

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/internal/secrets"
	"github.com/mfow/llm-temporal-worker/golang/temporalcodec"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
)

func dialPayloadConverter(t *testing.T, factory DefaultTemporalClientFactory, value config.Config) (converter.DataConverter, error) {
	t.Helper()
	var dialed *client.Options
	factory.DialContext = func(_ context.Context, options client.Options) (client.Client, error) {
		dialed = &options
		return nil, nil
	}
	if _, err := factory.New(context.Background(), value); err != nil {
		if dialed != nil {
			t.Fatal("factory dialed Temporal despite a payload codec error")
		}
		return nil, err
	}
	if dialed == nil || dialed.DataConverter == nil {
		t.Fatal("factory did not dial with a data converter")
	}
	return dialed.DataConverter, nil
}

func codecSecrets(values map[string][]byte) secrets.Resolver {
	return secrets.ResolverFunc(func(_ context.Context, ref config.SecretRef) ([]byte, error) {
		if value, ok := values[ref.Name]; ok {
			return value, nil
		}
		return nil, errors.New("environment secret is not set")
	})
}

func codecConfig(keys ...config.PayloadCodecKey) config.Config {
	value := config.Config{}
	value.Server.InlinePayloadBytes = 1 << 10
	value.Temporal.PayloadCodec = &config.PayloadCodecConfig{Kind: config.PayloadCodecAES256GCM, Keys: keys}
	return value
}

func codecKey(id, env string, primary bool) config.PayloadCodecKey {
	return config.PayloadCodecKey{ID: id, Primary: primary, Secret: config.SecretRef{Kind: config.SecretEnv, Name: env}}
}

// Without temporal.payload_codec the client keeps the bounded converter and
// writes plaintext payloads, exactly as before the codec existed.
func TestTemporalFactoryLeavesPayloadsUnencryptedByDefault(t *testing.T) {
	value := config.Config{}
	value.Server.InlinePayloadBytes = 1 << 10
	dataConverter, err := dialPayloadConverter(t, DefaultTemporalClientFactory{}, value)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := dataConverter.ToPayload("prompt")
	if err != nil {
		t.Fatal(err)
	}
	if got := string(payload.Metadata[converter.MetadataEncoding]); got != converter.MetadataEncodingJSON {
		t.Fatalf("default encoding = %q, want %q", got, converter.MetadataEncodingJSON)
	}
	if _, err := dataConverter.ToPayload(strings.Repeat("x", 2<<10)); err == nil {
		t.Fatal("default converter lost the inline payload bound")
	}
}

func TestTemporalFactoryEncryptsPayloadsWithConfiguredCodec(t *testing.T) {
	resolver := codecSecrets(map[string][]byte{"NEW": bytes.Repeat([]byte{2}, 32), "OLD": bytes.Repeat([]byte{1}, 48)})
	factory := DefaultTemporalClientFactory{SecretResolver: resolver}

	before, err := dialPayloadConverter(t, factory, codecConfig(codecKey("old", "OLD", true)))
	if err != nil {
		t.Fatal(err)
	}
	written, err := before.ToPayload("summarize project bluebird")
	if err != nil {
		t.Fatal(err)
	}
	if got := string(written.Metadata[converter.MetadataEncoding]); got != temporalcodec.EncodingEncrypted {
		t.Fatalf("encoding = %q, want encrypted", got)
	}
	if bytes.Contains(written.Data, []byte("bluebird")) {
		t.Fatal("payload reached Temporal in plaintext")
	}

	rotated, err := dialPayloadConverter(t, factory, codecConfig(codecKey("new", "NEW", true), codecKey("old", "OLD", false)))
	if err != nil {
		t.Fatal(err)
	}
	var decoded string
	if err := rotated.FromPayload(written, &decoded); err != nil || decoded != "summarize project bluebird" {
		t.Fatalf("rotated decode = %q, %v", decoded, err)
	}
	fresh, err := rotated.ToPayload("next")
	if err != nil {
		t.Fatal(err)
	}
	if got := string(fresh.Metadata[temporalcodec.MetadataKeyID]); got != "new" {
		t.Fatalf("encrypted with %q, want the primary key", got)
	}
	// The inline bound still applies to the plaintext under the codec.
	if _, err := rotated.ToPayload(strings.Repeat("x", 2<<10)); err == nil {
		t.Fatal("codec converter lost the inline payload bound")
	}
}

func TestTemporalFactoryFailsClosedOnPayloadCodecErrors(t *testing.T) {
	resolver := codecSecrets(map[string][]byte{"SHORT": []byte("too-short-secret"), "GOOD": bytes.Repeat([]byte{3}, 32)})
	for name, test := range map[string]struct {
		value config.Config
		want  string
	}{
		"unresolved key": {value: codecConfig(codecKey("k1", "MISSING", true)), want: `resolve Temporal payload codec key "k1" failed`},
		"short key":      {value: codecConfig(codecKey("k1", "SHORT", true)), want: "at least 32 bytes"},
		"no primary":     {value: codecConfig(codecKey("k1", "GOOD", false)), want: "exactly one primary key"},
		"unknown kind": {value: func() config.Config {
			value := codecConfig(codecKey("k1", "GOOD", true))
			value.Temporal.PayloadCodec.Kind = "rot13"
			return value
		}(), want: "kind is unsupported"},
	} {
		_, err := dialPayloadConverter(t, DefaultTemporalClientFactory{SecretResolver: resolver}, test.value)
		if err == nil || !strings.Contains(err.Error(), test.want) {
			t.Errorf("%s: New() = %v, want %q", name, err, test.want)
			continue
		}
		safe := safeTemporalFactoryError(err)
		if !strings.Contains(safe.Error(), test.want) || strings.Contains(safe.Error(), "too-short-secret") {
			t.Errorf("%s: startup error = %q", name, safe)
		}
	}
}

// The client codec rejects ciphertext above the inline-derived ceiling before
// decrypting it, and the bounded converter still checks the plaintext.
func TestTemporalFactoryBoundsCiphertextBeforeDecrypting(t *testing.T) {
	secret := bytes.Repeat([]byte{4}, 32)
	value := codecConfig(codecKey("k1", "KEY", true))
	dataConverter, err := dialPayloadConverter(t, DefaultTemporalClientFactory{SecretResolver: codecSecrets(map[string][]byte{"KEY": secret})}, value)
	if err != nil {
		t.Fatal(err)
	}
	unbounded, err := temporalcodec.NewAESGCM([]temporalcodec.Key{{ID: "k1", Secret: secret, Primary: true}})
	if err != nil {
		t.Fatal(err)
	}
	writer := converter.NewCodecDataConverter(converter.GetDefaultDataConverter(), unbounded)
	var decoded string

	oversized, err := writer.ToPayload(strings.Repeat("x", temporalcodec.MaxCiphertextBytes(value.Server.InlinePayloadBytes)))
	if err != nil {
		t.Fatal(err)
	}
	if err := dataConverter.FromPayload(oversized, &decoded); err == nil || !strings.Contains(err.Error(), "encrypted payload is") {
		t.Fatalf("oversized ciphertext = %v, want a pre-decryption size error", err)
	}

	// Within the ciphertext ceiling but above the plaintext limit.
	overInline, err := writer.ToPayload(strings.Repeat("x", value.Server.InlinePayloadBytes+16))
	if err != nil {
		t.Fatal(err)
	}
	if err := dataConverter.FromPayload(overInline, &decoded); err == nil || !strings.Contains(err.Error(), "Temporal payload is") {
		t.Fatalf("over-limit plaintext = %v, want the bounded converter's error", err)
	}
}

func TestConfigResolverResolvesPayloadCodecKeys(t *testing.T) {
	value := codecConfig(codecKey("k1", "MISSING", true))
	value.State.Kind = config.StateKindMemory
	err := secrets.ConfigResolver{Resolver: codecSecrets(nil)}.Resolve(context.Background(), &value)
	if err == nil || !errors.Is(err, secrets.ErrReference) {
		t.Fatalf("Resolve() = %v, want a secret reference error", err)
	}
}
