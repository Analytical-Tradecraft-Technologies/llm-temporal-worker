package temporalcodec

import (
	"bytes"
	"crypto/rand"
	"strings"
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
)

const testPrompt = "summarize the quarterly results for project bluebird"

func testSecret(fill byte) []byte { return bytes.Repeat([]byte{fill}, MinSecretBytes) }

func testCodec(t *testing.T, keys ...Key) converter.PayloadCodec {
	t.Helper()
	codec, err := NewAESGCM(keys)
	if err != nil {
		t.Fatal(err)
	}
	return codec
}

func encodeValue(t *testing.T, codec converter.PayloadCodec, value any) *commonpb.Payload {
	t.Helper()
	payload, err := converter.NewCodecDataConverter(converter.GetDefaultDataConverter(), codec).ToPayload(value)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func decodeValue(codec converter.PayloadCodec, payload *commonpb.Payload) (string, error) {
	var value string
	err := converter.NewCodecDataConverter(converter.GetDefaultDataConverter(), codec).FromPayload(payload, &value)
	return value, err
}

func TestRoundTripEncryptsWithPrimaryKey(t *testing.T) {
	codec := testCodec(t, Key{ID: "k1", Secret: testSecret(1), Primary: true})
	payload := encodeValue(t, codec, testPrompt)

	if got := string(payload.Metadata[converter.MetadataEncoding]); got != EncodingEncrypted {
		t.Fatalf("encoding = %q, want %q", got, EncodingEncrypted)
	}
	if got := string(payload.Metadata[MetadataKeyID]); got != "k1" {
		t.Fatalf("key ID = %q, want k1", got)
	}
	if got := string(payload.Metadata[MetadataCipher]); got != CipherAES256GCMV1 {
		t.Fatalf("cipher = %q", got)
	}
	if bytes.Contains(payload.Data, []byte("bluebird")) || bytes.Contains(payload.Data, []byte("json/plain")) {
		t.Fatal("ciphertext exposes the plaintext payload")
	}
	got, err := decodeValue(codec, payload)
	if err != nil || got != testPrompt {
		t.Fatalf("decode = %q, %v", got, err)
	}

	again := encodeValue(t, codec, testPrompt)
	if bytes.Equal(again.Data, payload.Data) {
		t.Fatal("two encodings reused a nonce")
	}
}

func TestEncodePreservesNilPayloadsAndLength(t *testing.T) {
	codec := testCodec(t, Key{ID: "k1", Secret: testSecret(1), Primary: true})
	encoded, err := codec.Encode([]*commonpb.Payload{nil, {Data: []byte("x")}})
	if err != nil || len(encoded) != 2 || encoded[0] != nil || encoded[1] == nil {
		t.Fatalf("Encode() = %v, %v", encoded, err)
	}
	decoded, err := codec.Decode(encoded)
	if err != nil || decoded[0] != nil || string(decoded[1].Data) != "x" {
		t.Fatalf("Decode() = %v, %v", decoded, err)
	}
}

func TestKeyRotationDecryptsWithAnyConfiguredKey(t *testing.T) {
	before := testCodec(t, Key{ID: "old", Secret: testSecret(1), Primary: true})
	written := encodeValue(t, before, testPrompt)

	rotated := testCodec(t,
		Key{ID: "new", Secret: testSecret(2), Primary: true},
		Key{ID: "old", Secret: testSecret(1)},
	)
	if got, err := decodeValue(rotated, written); err != nil || got != testPrompt {
		t.Fatalf("rotated decode of old payload = %q, %v", got, err)
	}
	fresh := encodeValue(t, rotated, testPrompt)
	if got := string(fresh.Metadata[MetadataKeyID]); got != "new" {
		t.Fatalf("rotated codec encrypted with %q, want the primary key", got)
	}
	if _, err := decodeValue(before, fresh); err == nil || !strings.Contains(err.Error(), `"new" is not configured`) {
		t.Fatalf("decode without the new key = %v", err)
	}

	retired := testCodec(t, Key{ID: "new", Secret: testSecret(2), Primary: true})
	if _, err := decodeValue(retired, written); err == nil {
		t.Fatal("a retired key still decrypted its payload")
	}
}

func TestDecodeRejectsTamperedPayloads(t *testing.T) {
	codec := testCodec(t,
		Key{ID: "k1", Secret: testSecret(1), Primary: true},
		Key{ID: "k2", Secret: testSecret(2)},
	)
	clone := func(payload *commonpb.Payload) *commonpb.Payload {
		metadata := make(map[string][]byte, len(payload.Metadata))
		for key, value := range payload.Metadata {
			metadata[key] = append([]byte(nil), value...)
		}
		return &commonpb.Payload{Metadata: metadata, Data: append([]byte(nil), payload.Data...)}
	}
	original := encodeValue(t, codec, testPrompt)
	for name, test := range map[string]struct {
		mutate func(*commonpb.Payload)
		want   string
	}{
		"flipped ciphertext": {mutate: func(p *commonpb.Payload) { p.Data[len(p.Data)-1] ^= 1 }, want: "failed authentication"},
		"flipped nonce":      {mutate: func(p *commonpb.Payload) { p.Data[0] ^= 1 }, want: "failed authentication"},
		"relabelled key":     {mutate: func(p *commonpb.Payload) { p.Metadata[MetadataKeyID] = []byte("k2") }, want: "failed authentication"},
		"unknown key":        {mutate: func(p *commonpb.Payload) { p.Metadata[MetadataKeyID] = []byte("k9") }, want: `"k9" is not configured`},
		"malformed key id":   {mutate: func(p *commonpb.Payload) { p.Metadata[MetadataKeyID] = []byte("bad key\n") }, want: "key is not configured"},
		"unknown cipher":     {mutate: func(p *commonpb.Payload) { p.Metadata[MetadataCipher] = []byte("rot13") }, want: "unsupported cipher"},
		"truncated":          {mutate: func(p *commonpb.Payload) { p.Data = p.Data[:10] }, want: "truncated"},
	} {
		tampered := clone(original)
		test.mutate(tampered)
		if _, err := codec.Decode([]*commonpb.Payload{tampered}); err == nil || !strings.Contains(err.Error(), test.want) {
			t.Errorf("%s: Decode() = %v, want %q", name, err, test.want)
		} else if strings.Contains(err.Error(), "bluebird") {
			t.Errorf("%s: error exposes payload content", name)
		}
	}
	if got, err := decodeValue(codec, original); err != nil || got != testPrompt {
		t.Fatalf("untampered decode = %q, %v", got, err)
	}
}

// Histories written before the codec was enabled stay readable.
func TestDecodePassesThroughUnencryptedPayloads(t *testing.T) {
	codec := testCodec(t, Key{ID: "k1", Secret: testSecret(1), Primary: true})
	plain, err := converter.GetDefaultDataConverter().ToPayload(testPrompt)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := decodeValue(codec, plain); err != nil || got != testPrompt {
		t.Fatalf("decode of a plaintext payload = %q, %v", got, err)
	}
}

func TestNewAESGCMRejectsInvalidKeys(t *testing.T) {
	long := make([]byte, 64)
	if _, err := rand.Read(long); err != nil {
		t.Fatal(err)
	}
	for name, test := range map[string]struct {
		keys []Key
		want string
	}{
		"no keys":       {want: "at least one key"},
		"short secret":  {keys: []Key{{ID: "k1", Secret: make([]byte, MinSecretBytes-1), Primary: true}}, want: "at least 32 bytes"},
		"no primary":    {keys: []Key{{ID: "k1", Secret: long}}, want: "exactly one primary"},
		"two primaries": {keys: []Key{{ID: "k1", Secret: long, Primary: true}, {ID: "k2", Secret: long, Primary: true}}, want: "exactly one primary"},
		"duplicate id":  {keys: []Key{{ID: "k1", Secret: long, Primary: true}, {ID: "k1", Secret: long}}, want: "duplicate"},
		"invalid id":    {keys: []Key{{ID: "bad id", Secret: long, Primary: true}}, want: "key ID is invalid"},
		"empty id":      {keys: []Key{{Secret: long, Primary: true}}, want: "key ID is invalid"},
	} {
		if _, err := NewAESGCM(test.keys); err == nil || !strings.Contains(err.Error(), test.want) {
			t.Errorf("%s: NewAESGCM() = %v, want %q", name, err, test.want)
		}
	}
	if _, err := NewAESGCM([]Key{{ID: "k1", Secret: long, Primary: true}}); err != nil {
		t.Fatalf("64-byte secret rejected: %v", err)
	}
}

// Oversized ciphertext is rejected before decryption; payloads within the
// ceiling still decode so the caller can apply its own plaintext limit.
func TestDecodeRejectsCiphertextAboveCeilingBeforeDecrypting(t *testing.T) {
	keys := []Key{{ID: "k1", Secret: testSecret(1), Primary: true}}
	unbounded := testCodec(t, keys...)
	const limit = 64
	bounded, err := NewAESGCMWithOptions(keys, Options{MaxPayloadBytes: limit})
	if err != nil {
		t.Fatal(err)
	}

	small := encodeValue(t, unbounded, strings.Repeat("a", limit-8))
	if got, err := decodeValue(bounded, small); err != nil || got != strings.Repeat("a", limit-8) {
		t.Fatalf("decode within the ceiling = %q, %v", got, err)
	}

	large := encodeValue(t, unbounded, strings.Repeat("a", MaxCiphertextBytes(limit)))
	if len(large.Data) <= MaxCiphertextBytes(limit) {
		t.Fatal("fixture does not exceed the ceiling")
	}
	if _, err := bounded.Decode([]*commonpb.Payload{large}); err == nil || !strings.Contains(err.Error(), "limit is") {
		t.Fatalf("Decode() of oversized ciphertext = %v", err)
	}
	// With a corrupted tag the error is still the size error, so the
	// ceiling runs before authentication.
	large.Data[len(large.Data)-1] ^= 1
	if _, err := bounded.Decode([]*commonpb.Payload{large}); err == nil || !strings.Contains(err.Error(), "limit is") {
		t.Fatalf("ceiling did not run before decryption: %v", err)
	}
	if _, err := NewAESGCMWithOptions(keys, Options{MaxPayloadBytes: -1}); err == nil {
		t.Fatal("negative ceiling accepted")
	}
}
