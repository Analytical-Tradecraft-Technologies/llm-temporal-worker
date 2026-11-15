package messagestest

import (
	"net/http"
	"testing"
	"time"

	"github.com/anthropics/anthropic-sdk-go"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testkit"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/adaptertest"
)

// LargeNonStreamingRequestReachesTransport checks that a non-streaming request
// with a large output cap still reaches the transport, once, under an active
// deadline that a client timeout bounds. send builds the family's client on
// httpClient and sends params through its Messages service.
func LargeNonStreamingRequestReachesTransport(t *testing.T, send func(t *testing.T, httpClient *http.Client, params anthropic.MessageNewParams) error) {
	for _, timeout := range []time.Duration{0, time.Second} {
		calls := 0
		httpClient := &http.Client{Timeout: timeout, Transport: testkit.RoundTripFunc(func(request *http.Request) (*http.Response, error) {
			calls++
			deadline, ok := request.Context().Deadline()
			if !ok || time.Until(deadline) <= 0 || time.Until(deadline) > 10*time.Minute {
				t.Error("request has no active timeout")
			}
			if timeout > 0 && time.Until(deadline) > timeout {
				t.Error("request exceeds configured timeout")
			}
			return adaptertest.JSONResponse(request, 200, `{"id":"msg-local","type":"message","role":"assistant","content":[],"model":"claude-contract","stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}`), nil
		})}
		err := send(t, httpClient, anthropic.MessageNewParams{MaxTokens: 32768, Model: ContractModel, Messages: []anthropic.MessageParam{anthropic.NewUserMessage(anthropic.NewTextBlock("hello"))}})
		if err != nil || calls != 1 {
			t.Fatalf("timeout=%s calls=%d error=%v", timeout, calls, err)
		}
	}
}
