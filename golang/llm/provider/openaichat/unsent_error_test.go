package openaichat

import (
	"context"
	"errors"
	"io"
	"net/http"
	"testing"

	"github.com/openai/openai-go/v3/option"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

func invokeWithClientMiddleware(t *testing.T, middleware option.Middleware, transport roundTripFunc) error {
	t.Helper()
	client, err := NewClient(ClientConfig{BaseURL: "https://127.0.0.1/contract", APIKey: "test-key", HTTPClient: &http.Client{Transport: transport}})
	if err != nil {
		t.Fatal(err)
	}
	if middleware != nil {
		client.requestOptions = append(client.requestOptions, option.WithMiddleware(middleware))
	}
	adapter, err := New(client, "chat-prod", testProfile())
	if err != nil {
		t.Fatal(err)
	}
	call, err := adapter.Compile(context.Background(), provider.CompileInput{
		Request: llm.Request{OperationKey: "op-unsent", Model: "chat-model"},
		Query:   provider.CapabilityQuery{EndpointID: "chat-prod", Family: provider.FamilyOpenAIChat, Model: "chat-model"},
		Strict:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = adapter.Invoke(context.Background(), call, nil)
	return err
}

// An SDK error raised before the dispatch probe means nothing reached the
// transport, so it may move to another route instead of being settled as an
// ambiguous, possibly paid outcome (#1495).
func TestChatErrorBeforeTheTransportIsNotDispatched(t *testing.T) {
	sent := 0
	transport := func(*http.Request) (*http.Response, error) {
		sent++
		return nil, errors.New("unexpected request")
	}
	for name, cause := range map[string]error{
		"middleware failure": errors.New("request signing failed"),
		"credential failure": provider.ErrProviderCredentials,
	} {
		t.Run(name, func(t *testing.T) {
			err := invokeWithClientMiddleware(t, func(*http.Request, option.MiddlewareNext) (*http.Response, error) {
				return nil, cause
			}, transport)
			var mapped *provider.Error
			if !errors.As(err, &mapped) || mapped.Dispatch != provider.DispatchNotDispatched || mapped.Retry != provider.RetryNextRoute || !errors.Is(mapped, provider.ErrProviderPreDispatch) {
				t.Fatalf("Invoke() error = %#v, want a pre-dispatch failure", err)
			}
			if wantCredential := errors.Is(cause, provider.ErrProviderCredentials); (mapped.Code == provider.CodeAuthentication) != wantCredential {
				t.Fatalf("code = %s, credential = %v", mapped.Code, wantCredential)
			}
		})
	}
	if sent != 0 {
		t.Fatalf("transport called %d times", sent)
	}
}

// A transport failure after the probe may have reached the provider.
func TestChatTransportFailureStaysAmbiguous(t *testing.T) {
	sent := 0
	err := invokeWithClientMiddleware(t, nil, func(*http.Request) (*http.Response, error) {
		sent++
		return nil, io.ErrUnexpectedEOF
	})
	var mapped *provider.Error
	if !errors.As(err, &mapped) || mapped.Dispatch != provider.DispatchAmbiguous || errors.Is(mapped, provider.ErrProviderPreDispatch) || sent != 1 {
		t.Fatalf("Invoke() error = %#v after %d sends, want an ambiguous dispatch", err, sent)
	}
}
