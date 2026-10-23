package redis

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	redisclient "github.com/redis/go-redis/v9"
)

func responseResultTestStore(t *testing.T, secret byte) *ResponseResultStore {
	t.Helper()
	// The client never connects; these tests exercise sealing only.
	client := redisclient.NewClient(&redisclient.Options{Addr: "127.0.0.1:1"})
	t.Cleanup(func() { _ = client.Close() })
	store, err := NewResponseResultStore(ResponseResultOptions{Client: client, Keys: KeyOptions{Prefix: "worker", HashTag: "admission", KeySecret: bytes.Repeat([]byte{secret}, 32)}})
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func responseResultTestResponse(key string) *llm.GenerateResponseV1 {
	return &llm.GenerateResponseV1{
		OperationKey: key, OperationID: "op", Status: llm.ResponseStatusCompleted,
		Checkpoint: llm.CheckpointMetadata{Handle: "checkpoint", Kind: "generation", Depth: 1},
		Cache:      llm.CacheDispositionV1{Disposition: "disabled"},
		Cost:       llm.CostV1{Status: "unknown", UnknownReason: "provider_did_not_report_cost"},
		Output:     []llm.Item{llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: "the private answer"}}}},
	}
}

func responseResultTestFailure() *llm.ExecutionResultV1 {
	return &llm.ExecutionResultV1{RequestID: "llmtw_req_5f0c6b2e-8a1d-4c3b-9e7f-2a4d6c8b0e1f", Kind: "generate", State: llm.ExecutionFailed, FailureCode: "provider_error", ErrorCode: "budget_denied"}
}

func TestResponseResultSealsAndBindsToItsKey(t *testing.T) {
	store := responseResultTestStore(t, 7)
	created := time.Unix(1700000000, 0).UTC()
	key := store.key("acme", "search", "resp_a")
	plain := []byte(`{"schema":"response-result/v1","created_at":"2023-11-14T22:13:20Z","response":` + mustJSON(t, responseResultTestResponse("resp_a")) + `}`)
	sealed, err := store.seal(key, plain)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(sealed, []byte("private answer")) || strings.Contains(key, "acme") || strings.Contains(key, "resp_a") {
		t.Fatalf("Redis exposes content or identifiers: key %q", key)
	}
	if !strings.HasPrefix(key, "worker:{admission}:response-result:") {
		t.Fatalf("key escaped the configured namespace: %q", key)
	}
	result, err := store.open(key, "resp_a", sealed)
	if err != nil || result.Response == nil || result.Response.Output[0].(llm.Message).Content[0].(llm.TextPart).Text != "the private answer" || !result.CreatedAt.Equal(created) {
		t.Fatalf("open = %+v, %v", result, err)
	}

	// A value moved to another scope's key, or read with another store's
	// secret, does not authenticate.
	if _, err := store.open(store.key("other", "search", "resp_a"), "resp_a", sealed); err == nil {
		t.Fatal("a value opened under another scope's key")
	}
	if _, err := responseResultTestStore(t, 8).open(key, "resp_a", sealed); err == nil {
		t.Fatal("a value opened with another secret")
	}
	tampered := append([]byte(nil), sealed...)
	tampered[len(tampered)-1] ^= 1
	if _, err := store.open(key, "resp_a", tampered); err == nil {
		t.Fatal("a tampered value opened")
	}
	if _, err := store.open(key, "resp_a", sealed[:4]); err == nil {
		t.Fatal("a truncated value opened")
	}
	// An authentic value naming another operation is not this operation's result.
	if _, err := store.open(key, "resp_b", sealed); err == nil {
		t.Fatal("a result for another operation key was accepted")
	}
}

func TestResponseResultAcceptsOnlyTerminalResults(t *testing.T) {
	failure := responseResultTestFailure()
	retryable := *failure
	retryable.Retryable = true
	pending := *failure
	pending.State, pending.FailureCode, pending.ErrorCode, pending.RetryAfterSeconds = llm.ExecutionPending, "", "", 1
	for name, test := range map[string]struct {
		result ResponseResult
		valid  bool
	}{
		"completed":          {ResponseResult{Response: responseResultTestResponse("resp_a")}, true},
		"permanent failure":  {ResponseResult{Failure: failure}, true},
		"other operation":    {ResponseResult{Response: responseResultTestResponse("resp_b")}, false},
		"retryable failure":  {ResponseResult{Failure: &retryable}, false},
		"nonterminal":        {ResponseResult{Failure: &pending}, false},
		"empty":              {ResponseResult{}, false},
		"response and error": {ResponseResult{Response: responseResultTestResponse("resp_a"), Failure: failure}, false},
	} {
		if got := validResponseResult("resp_a", test.result); got != test.valid {
			t.Errorf("%s: valid = %v, want %v", name, got, test.valid)
		}
	}
}

func TestNewResponseResultStoreValidatesOptions(t *testing.T) {
	keys := KeyOptions{Prefix: "worker", HashTag: "admission", KeySecret: bytes.Repeat([]byte{1}, 32)}
	if _, err := NewResponseResultStore(ResponseResultOptions{Keys: keys}); err == nil {
		t.Fatal("a store without a client was accepted")
	}
	client := redisclient.NewClient(&redisclient.Options{Addr: "127.0.0.1:1"})
	defer client.Close()
	if _, err := NewResponseResultStore(ResponseResultOptions{Client: client, Keys: keys, TTL: -time.Second}); err == nil {
		t.Fatal("a negative TTL was accepted")
	}
	store, err := NewResponseResultStore(ResponseResultOptions{Client: client, Keys: keys})
	if err != nil || store.ttl != DefaultResponseResultTTL {
		t.Fatalf("default TTL = %v, %v", store, err)
	}
}
