package openairesponses

import (
	"context"
	"encoding/json"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestBackgroundSubmitAndSinglePoll(t *testing.T) {
	for _, azure := range []bool{false, true} {
		t.Run(map[bool]string{false: "OpenAI", true: "Azure"}[azure], func(t *testing.T) {
			var requests int
			status := "queued"
			httpStatus := 200
			transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
				requests++
				prefix := "/v1/responses"
				if azure {
					prefix = "/openai/v1/responses"
				}
				if requests == 1 {
					if r.Method != "POST" || r.URL.Path != prefix {
						t.Fatalf("submit: %s %s", r.Method, r.URL.Path)
					}
					var payload map[string]any
					if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
						t.Fatal(err)
					}
					if payload["background"] != true {
						t.Fatalf("background: %v", payload)
					}
				} else if r.Method != "GET" || r.URL.Path != prefix+"/resp_1" {
					t.Fatalf("poll: %s %s", r.Method, r.URL.Path)
				}
				body := `{"id":"resp_1","status":"` + status + `","service_tier":"default","model":"gpt-contract","output":[]}`
				if httpStatus != 200 {
					body = `{"error":{"message":"test error","type":"server_error"}}`
				}
				return &http.Response{StatusCode: httpStatus, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
			})
			var adapter provider.ResumableAdapter
			if azure {
				c, err := NewAzureClient(AzureClientConfig{Endpoint: "https://127.0.0.1", APIVersion: "v1", APIKey: "test", HTTPClient: &http.Client{Transport: transport}})
				if err != nil {
					t.Fatal(err)
				}
				adapter, err = NewAzureAdapter(c, "endpoint", "test")
				if err != nil {
					t.Fatal(err)
				}
			} else {
				c, err := NewClient(ClientConfig{BaseURL: "https://127.0.0.1/v1", APIKey: "test", HTTPClient: &http.Client{Transport: transport}})
				if err != nil {
					t.Fatal(err)
				}
				adapter, err = NewOpenAIAdapter(c, "endpoint", "test")
				if err != nil {
					t.Fatal(err)
				}
				generic, err := New(c, "generic", "test")
				if err != nil {
					t.Fatal(err)
				}
				if _, ok := any(generic).(provider.ResumableAdapter); ok {
					t.Fatal("generic must remain synchronous")
				}
			}
			call, err := adapter.Compile(context.Background(), provider.CompileInput{Request: llm.Request{OperationKey: "op", Model: "gpt-contract"}})
			if err != nil {
				t.Fatal(err)
			}
			submitted, err := adapter.Submit(context.Background(), call, nil)
			if err != nil || submitted.State != provider.ResumablePending || requests != 1 {
				t.Fatalf("submit: %v %v", submitted, err)
			}
			for _, tc := range []struct {
				status string
				state  provider.ResumableState
			}{{"in_progress", provider.ResumablePending}, {"completed", provider.ResumableCompleted}, {"failed", provider.ResumableFailed}, {"cancelled", provider.ResumableFailed}} {
				status = tc.status
				before := requests
				result, err := adapter.Poll(context.Background(), call, "resp_1", nil)
				if err != nil || result.State != tc.state || requests != before+1 {
					t.Fatalf("poll %s: %v %v requests=%d", status, result, err, requests)
				}
			}
			httpStatus = 503
			before := requests
			if _, err = adapter.Poll(context.Background(), call, "resp_1", nil); err == nil || requests != before+1 {
				t.Fatal("GET retries or missing error")
			}
			httpStatus = 404
			result, err := adapter.Poll(context.Background(), call, "resp_1", nil)
			if err != nil || result.State != provider.ResumableNotFound {
				t.Fatalf("404: %v %v", result, err)
			}
			before = requests
			if _, err = adapter.Poll(context.Background(), call, "../other", nil); err == nil || requests != before {
				t.Fatal("unsafe ID dispatched")
			}
		})
	}
}
