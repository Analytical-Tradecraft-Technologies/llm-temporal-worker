package activity

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"go.temporal.io/sdk/converter"
)

func TestRequestPayloadRoundTrip(t *testing.T) {
	payload := GenerateRequest{APIVersion: APIVersion, Request: llm.Request{OperationKey: "operation-1", Context: llm.RequestContext{Tenant: "tenant-1"}, Model: "model-1", Input: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "hello"}}}}}}
	encoded, err := MarshalRequest(payload, PayloadLimits{MaxInlineBytes: 16 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := UnmarshalRequest(encoded, PayloadLimits{MaxInlineBytes: 16 * 1024})
	if err != nil {
		t.Fatal(err)
	}
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
	encoded, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	// The configured limit is deliberately one byte below the full envelope,
	// while the nested response remains small enough for its own validator.
	limits := PayloadLimits{MaxInlineBytes: len(encoded) - 1}
	if _, err := MarshalResponse(response, limits); err == nil {
		t.Fatal("MarshalResponse accepted an envelope larger than its limit")
	}
	if _, err := UnmarshalResponse(encoded, limits); err == nil {
		t.Fatal("UnmarshalResponse accepted an envelope larger than its limit")
	}
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
	if _, err := UnmarshalRequest([]byte(base), PayloadLimits{}); err != nil {
		t.Fatalf("valid base payload rejected: %v", err)
	}
}

func TestRequestPayloadRejectsOversizeAndMalformedBlob(t *testing.T) {
	oversize := GenerateRequest{APIVersion: APIVersion, Request: llm.Request{OperationKey: "large", Model: "model-1", Input: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: strings.Repeat("x", 128)}}}}}}
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

	if _, err := MarshalRequest(payload, PayloadLimits{MaxInlineBytes: 16 * 1024}); err != nil {
		t.Fatalf("MarshalRequest rejected opaque digest fields: %v", err)
	}
}

func TestRequestPayloadValidatesDocumentBlobSource(t *testing.T) {
	document := llm.DocumentPart{
		Blob:      &llm.BlobRef{Digest: strings.Repeat("a", 64), ByteLength: 1, MediaType: "application/pdf", Locator: "s3://bucket/document"},
		MediaType: "application/pdf",
	}
	payload := GenerateRequest{APIVersion: APIVersion, Request: llm.Request{OperationKey: "document-blob", Model: "model-1", Input: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{document}}}}}
	if _, err := payload.Validate(16 * 1024); err != nil {
		t.Fatalf("valid document blob rejected: %v", err)
	}

	document.Blob.Digest = "bad"
	payload.Request.Input = []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{document}}}
	if _, err := payload.Validate(16 * 1024); err == nil {
		t.Fatal("malformed document blob unexpectedly accepted")
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
		{name: "request", call: func() error {
			_, err := UnmarshalRequest(oversize, PayloadLimits{MaxInlineBytes: 64})
			return err
		}},
		{name: "response", call: func() error {
			_, err := UnmarshalResponse(oversize, PayloadLimits{MaxInlineBytes: 64})
			return err
		}},
	} {
		t.Run(decode.name, func(t *testing.T) {
			err := decode.call()
			if err == nil || !strings.Contains(err.Error(), "payload is 128 bytes; limit is 64") {
				t.Fatalf("oversize malformed payload error = %v, want size rejection", err)
			}
		})
	}
}

func TestBoundedDataConverterRejectsBeforeTemporalDecode(t *testing.T) {
	limits := PayloadLimits{MaxInlineBytes: 64}
	bounded := BoundedDataConverter(limits)
	oversize, err := converter.GetDefaultDataConverter().ToPayload(strings.Repeat("x", 128))
	if err != nil {
		t.Fatal(err)
	}
	var decoded string
	if err := bounded.FromPayload(oversize, &decoded); err == nil || !strings.Contains(err.Error(), "Temporal payload is 130 bytes; limit is 64") {
		t.Fatalf("bounded converter error = %v, want pre-decode size rejection", err)
	}
	if _, err := bounded.ToPayload(strings.Repeat("x", 128)); err == nil {
		t.Fatal("bounded converter accepted oversized encode")
	}
}

func TestBlobRefValidationAndResponseRoundTrip(t *testing.T) {
	expires := time.Now().Add(time.Hour).UTC().Truncate(time.Second)
	ref := BlobRef{Store: "s3", Locator: "tenant-1/abc", Digest: strings.Repeat("a", 64), ByteLength: 12, MediaType: "application/json", ExpiresAt: &expires}
	if err := ref.Validate(time.Now()); err != nil {
		t.Fatal(err)
	}
	response := GenerateResponse{APIVersion: APIVersion, Response: llm.Response{OperationKey: "operation-1", Status: llm.ResponseStatusCompleted, Service: llm.ServiceFacts{Requested: llm.ServiceClassStandard, Attempted: llm.ServiceClassStandard}}}
	encoded, err := MarshalResponse(response, PayloadLimits{MaxInlineBytes: 16 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := UnmarshalResponse(encoded, PayloadLimits{MaxInlineBytes: 16 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Response.OperationKey != response.Response.OperationKey {
		t.Fatalf("decoded response = %#v", decoded)
	}
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
