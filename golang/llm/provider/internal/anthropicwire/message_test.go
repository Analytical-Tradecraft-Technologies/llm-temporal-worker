package anthropicwire

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

func TestResponseMetadataFallsBackToBodyRequestID(t *testing.T) {
	var message anthropic.Message
	testassert.NoError(t, json.Unmarshal([]byte(`{"id":"msg","request_id":"body-req"}`), &message))
	raw := &http.Response{StatusCode: 200, Header: http.Header{}}
	if got := ResponseMetadata(&message, raw, "tier", "request-id"); got != (provider.ResponseMetadata{ResponseID: "msg", ProviderTier: "tier", Status: 200, RequestID: "body-req"}) {
		t.Fatalf("ResponseMetadata(no header) = %+v", got)
	}
	raw.Header.Set("request-id", "header-req")
	if got := ResponseMetadata(&message, raw, "", "request-id"); got.RequestID != "header-req" {
		t.Fatalf("ResponseMetadata(header) = %+v", got)
	}
}

func TestContinuationNeedsIDAndStates(t *testing.T) {
	call, states := provider.Call{EndpointID: "ep"}, []llm.ProviderState{{Provider: "anthropic"}}
	if Continuation(call, &anthropic.Message{}, states, "p:") != nil || Continuation(call, &anthropic.Message{ID: "msg"}, nil, "p:") != nil {
		t.Fatal("Continuation without an ID or states must be nil")
	}
	got := Continuation(call, &anthropic.Message{ID: "msg", Model: "m"}, states, "p:")
	if got == nil || got.Handle != "p:msg" || got.EndpointID != "ep" || got.Model != "m" || !got.Pinned || len(got.ProviderStates) != 1 {
		t.Fatalf("Continuation = %+v", got)
	}
}
