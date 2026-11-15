package activity

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"go.temporal.io/sdk/converter"
)

func TestRequestPayloadRoundTrip(t *testing.T) {
	payload := GenerateRequest{APIVersion: APIVersion, Request: llm.Request{OperationKey: "operation-1", Context: llm.RequestContext{Tenant: "tenant-1"}, Model: "model-1", Input: []llm.Item{llm.HumanText("hello")}}}
	encoded, decoded := mustPayloadRoundTrip(t, payload, MarshalRequest, UnmarshalRequest)
	if decoded.Request.OperationKey != payload.Request.OperationKey || decoded.Request.ServiceClass != llm.ServiceClassStandard {
		t.Fatalf("decoded payload = %#v", decoded)
	}
	if !bytes.Equal(encoded, mustJSON(t, decoded)) {
		t.Fatalf("payload encoding is not deterministic: %s != %s", encoded, mustJSON(t, decoded))
	}
}

func TestEnvelopeMarshalAndUnmarshalUseTheSameLimit(t *testing.T) {
	response := GenerateResponse{
		APIVersion: APIVersion,
		Response: llm.Response{
			OperationKey: "operation-1",
			Status:       llm.ResponseStatusCompleted,
			Service:      llm.ServiceFacts{Requested: llm.ServiceClassStandard, Attempted: llm.ServiceClassStandard},
		},
		Metadata: ResultMetadata{OperationID: "operation-id"},
	}
	// The configured limit is deliberately one byte below the full envelope,
	// while the nested response remains small enough for its own validator.
	assertCodecRejectsOneByteOverLimit(t, response, MarshalResponse, UnmarshalResponse)
}

func TestRequestPayloadRejectsUnknownVersionFieldsAndDuplicates(t *testing.T) {
	base := `{"api_version":"llm.temporal/v1","request":{"api_version":"llm.temporal/v1","operation_key":"operation-1","model":"model-1","input":[]}}`
	for _, value := range []string{
		`{"api_version":"llm.temporal/v2","request":{"api_version":"llm.temporal/v1","operation_key":"operation-1","model":"model-1","input":[]}}`,
		`{"api_version":"llm.temporal/v1","request":{"api_version":"llm.temporal/v1","operation_key":"operation-1","model":"model-1","input":[]},"extra":true}`,
		`{"api_version":"llm.temporal/v1","api_version":"llm.temporal/v1","request":{"api_version":"llm.temporal/v1","operation_key":"operation-1","model":"model-1","input":[]}}`,
	} {
		if _, err := UnmarshalRequest([]byte(value), PayloadLimits{}); err == nil {
			t.Fatalf("payload unexpectedly accepted: %s", value)
		}
	}
	testassert.NoError(t, testassert.Err(UnmarshalRequest([]byte(base), PayloadLimits{})), "valid base payload rejected")
}

func TestRequestPayloadRejectsOversizeAndMalformedBlob(t *testing.T) {
	oversize := GenerateRequest{APIVersion: APIVersion, Request: llm.Request{OperationKey: "large", Model: "model-1", Input: []llm.Item{llm.HumanText(strings.Repeat("x", 128))}}}
	if _, err := MarshalRequest(oversize, PayloadLimits{MaxInlineBytes: 64}); err == nil {
		t.Fatal("oversize payload unexpectedly accepted")
	}
	malformed := GenerateRequest{APIVersion: APIVersion, Request: llm.Request{OperationKey: "blob", Model: "model-1", Input: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.ImagePart{Blob: &llm.BlobRef{Digest: "bad", ByteLength: 1, MediaType: "image/png", Locator: "s3://bucket/key"}, MediaType: "image/png"}}}}}}
	if _, err := malformed.Validate(16 * 1024); err == nil {
		t.Fatal("malformed embedded blob unexpectedly accepted")
	}
}

func TestRequestPayloadAllowsArbitraryDigestFields(t *testing.T) {
	payload := GenerateRequest{
		APIVersion: APIVersion,
		Request: llm.Request{
			OperationKey: "opaque-digests",
			Model:        "model-1",
			Input: []llm.Item{
				llm.ToolCall{ID: "call-1", Name: "lookup", Arguments: json.RawMessage(`{"digest":"business-value","nested":{"digest":"tool-nested-value"}}`)},
				llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.JSONPart{Value: json.RawMessage(`{"digest":"json-part-value","schema":{"digest":"schema-value"}}`)}}},
			},
			Extensions: map[string]json.RawMessage{
				"vendor": json.RawMessage(`{"digest":"extension-value"}`),
			},
		},
	}

	testassert.NoError(t, testassert.Err(MarshalRequest(payload, PayloadLimits{MaxInlineBytes: 16 * 1024})), "MarshalRequest rejected opaque digest fields")
}

func TestRequestPayloadValidatesDocumentBlobSource(t *testing.T) {
	document := llm.DocumentPart{
		Blob:      &llm.BlobRef{Digest: strings.Repeat("a", 64), ByteLength: 1, MediaType: "application/pdf", Locator: "s3://bucket/document"},
		MediaType: "application/pdf",
	}
	payload := GenerateRequest{APIVersion: APIVersion, Request: llm.Request{OperationKey: "document-blob", Model: "model-1", Input: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{document}}}}}
	testassert.NoError(t, testassert.Err(payload.Validate(16*1024)), "valid document blob rejected")

	document.Blob.Digest = "bad"
	payload.Request.Input = []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{document}}}
	if _, err := payload.Validate(16 * 1024); err == nil {
		t.Fatal("malformed document blob unexpectedly accepted")
	}
}

// The item codec no longer applies the remote-media URL policy (it also
// decodes stored transcripts), so the Activity payload boundary must.
func TestRequestPayloadAppliesMediaURLPolicy(t *testing.T) {
	payloadFor := func(raw string) GenerateRequest {
		return GenerateRequest{APIVersion: APIVersion, Request: llm.Request{OperationKey: "media-url", Model: "model-1",
			Instructions: []llm.Instruction{{Kind: llm.InstructionKindParts, Content: []llm.Part{llm.TextPart{Text: "read"}}}},
			Input:        []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.ImagePart{URL: raw, MediaType: "image/png"}}}}}}
	}
	encoded, err := MarshalRequest(payloadFor("https://cdn1.example.com/a.png"), PayloadLimits{})
	testassert.NoError(t, err, "public media URL rejected")
	testassert.NoError(t, testassert.Err(UnmarshalRequest(encoded, PayloadLimits{})), "public media URL rejected on decode")
	for _, raw := range []string{"http://localhost/a.png", "http://127.1/a.png", "http://2130706433/a.png", "http://[64:ff9b::a9fe:a9fe]/a.png", "http://100.64.0.1/a.png", "http://metadata.google.internal/a.png"} {
		if _, err := payloadFor(raw).Validate(16 * 1024); err == nil {
			t.Errorf("media URL %q accepted by Validate", raw)
		}
		wire := bytes.Replace(encoded, []byte("https://cdn1.example.com/a.png"), []byte(raw), 1)
		if _, err := UnmarshalRequest(wire, PayloadLimits{}); err == nil {
			t.Errorf("media URL %q accepted by UnmarshalRequest", raw)
		}
		document := payloadFor("https://cdn1.example.com/a.png")
		document.Request.Instructions[0].Content = []llm.Part{llm.DocumentPart{URL: raw, MediaType: "application/pdf"}}
		if _, err := document.Validate(16 * 1024); err == nil {
			t.Errorf("instruction media URL %q accepted by Validate", raw)
		}
	}
}

func TestPayloadUnmarshalRejectsOversizeBeforeJSONDecode(t *testing.T) {
	// This is intentionally malformed as JSON. The size gate must run first so
	// malformed or adversarial payloads cannot force an unbounded decode before
	// the Activity boundary rejects them.
	oversize := bytes.Repeat([]byte{'{'}, 128)
	for _, decode := range []struct {
		name string
		call func() error
	}{
		{name: "request", call: func() error { return errOnly(UnmarshalRequest(oversize, PayloadLimits{MaxInlineBytes: 64})) }},
		{name: "response", call: func() error { return errOnly(UnmarshalResponse(oversize, PayloadLimits{MaxInlineBytes: 64})) }},
	} {
		t.Run(decode.name, func(t *testing.T) {
			err := decode.call()
			testassert.ErrorContains(t, err, "payload is 128 bytes; limit is 64")
		})
	}
}

func TestBoundedDataConverterRejectsBeforeTemporalDecode(t *testing.T) {
	limits := PayloadLimits{MaxInlineBytes: 64}
	bounded := BoundedDataConverter(limits)
	oversize, err := converter.GetDefaultDataConverter().ToPayload(strings.Repeat("x", 128))
	testassert.NoError(t, err)
	var decoded string
	testassert.ErrorContains(t, bounded.FromPayload(oversize, &decoded), "Temporal payload is 130 bytes; limit is 64")
	if _, err := bounded.ToPayload(strings.Repeat("x", 128)); err == nil {
		t.Fatal("bounded converter accepted oversized encode")
	}
}

func TestBlobRefValidationAndResponseRoundTrip(t *testing.T) {
	expires := time.Now().Add(time.Hour).UTC().Truncate(time.Second)
	ref := BlobRef{Store: "s3", Locator: "tenant-1/abc", Digest: strings.Repeat("a", 64), ByteLength: 12, MediaType: "application/json", ExpiresAt: &expires}
	testassert.NoError(t, ref.Validate(time.Now()))
	response := GenerateResponse{APIVersion: APIVersion, Response: llm.Response{OperationKey: "operation-1", Status: llm.ResponseStatusCompleted, Service: llm.ServiceFacts{Requested: llm.ServiceClassStandard, Attempted: llm.ServiceClassStandard}}}
	_, decoded := mustPayloadRoundTrip(t, response, MarshalResponse, UnmarshalResponse)
	if decoded.Response.OperationKey != response.Response.OperationKey {
		t.Fatalf("decoded response = %#v", decoded)
	}
}

// errOnly discards a call's value and keeps its error.
func errOnly[T any](_ T, err error) error { return err }

// mustPayloadRoundTrip encodes value with marshal and decodes the encoding
// with unmarshal under a 16 KiB inline limit, failing on either error.
func mustPayloadRoundTrip[T any](t *testing.T, value T, marshal func(T, PayloadLimits) ([]byte, error), unmarshal func([]byte, PayloadLimits) (T, error)) ([]byte, T) {
	t.Helper()
	limits := PayloadLimits{MaxInlineBytes: 16 * 1024}
	encoded, err := marshal(value, limits)
	testassert.NoError(t, err)
	decoded, err := unmarshal(encoded, limits)
	testassert.NoError(t, err)
	return encoded, decoded
}

// assertCodecRejectsOneByteOverLimit checks marshal and unmarshal both reject
// value once the inline limit is one byte below its JSON encoding.
func assertCodecRejectsOneByteOverLimit[T any](t *testing.T, value T, marshal func(T, PayloadLimits) ([]byte, error), unmarshal func([]byte, PayloadLimits) (T, error)) {
	t.Helper()
	encoded := mustJSON(t, value)
	limits := PayloadLimits{MaxInlineBytes: len(encoded) - 1}
	if _, err := marshal(value, limits); err == nil {
		t.Fatalf("encode accepted a %T larger than its limit", value)
	}
	if _, err := unmarshal(encoded, limits); err == nil {
		t.Fatalf("decode accepted a %T larger than its limit", value)
	}
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	testassert.NoError(t, err)
	return data
}
