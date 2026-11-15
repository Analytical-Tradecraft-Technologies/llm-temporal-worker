package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/blob"
)

func TestBlobStoreLimitAdmitsResponsesLargerThanRequestLimit(t *testing.T) {
	value := config.Config{
		BlobStore: config.BlobStoreConfig{Kind: "file", File: config.FileBlobConfig{Root: t.TempDir()}},
		Limits:    config.LimitsConfig{RequestBytes: 1024, ProviderResponseBytes: 4096},
	}
	if got, want := blobMaxBytes(value.Limits), config.SavedResponseExpansion*4096+config.DurableRecordBookkeepingBytes; got != want {
		t.Fatalf("blobMaxBytes = %d, want %d: the escaped provider response limit and its facts", got, want)
	}
	if got := blobMaxBytes(config.LimitsConfig{RequestBytes: 64 << 20, ProviderResponseBytes: 1 << 20}); got != 64<<20 {
		t.Fatalf("blobMaxBytes = %d, want the larger request limit", got)
	}
	store, _, err := defaultBlobFactory(context.Background(), value)
	if err != nil {
		t.Fatal(err)
	}
	result := bytes.Repeat([]byte("a"), 4096)
	if _, err := store.Put(context.Background(), blob.PutRequest{Tenant: "tenant", MediaType: "application/json", Data: result, ExpiresAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatalf("result larger than request_bytes rejected: %v", err)
	}
}

// TestBlobStoreLimitAdmitsEscapedResultOfAcceptedResponse stores the result
// of the worst response the default provider limit admits: text that is all
// '<', which the result encoding escapes to six bytes each. A paid result the
// HTTP limit accepted must never be refused by the result store (#1319).
func TestBlobStoreLimitAdmitsEscapedResultOfAcceptedResponse(t *testing.T) {
	limits := config.LimitsConfig{RequestBytes: 1 << 20, ProviderResponseBytes: config.DefaultProviderResponseBytes}
	value := config.Config{BlobStore: config.BlobStoreConfig{Kind: "file", File: config.FileBlobConfig{Root: t.TempDir()}}, Limits: limits}
	store, _, err := defaultBlobFactory(context.Background(), value)
	if err != nil {
		t.Fatal(err)
	}
	response := llm.Response{OperationKey: "operation", Status: llm.ResponseStatusCompleted, Output: []llm.Item{llm.ModelText(strings.Repeat("<", int(limits.ProviderResponseBytes)))}}
	// Encode exactly as BlobResultStore.Put does.
	encoded, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := llm.CanonicalJSON(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if int64(len(canonical)) <= 2*limits.ProviderResponseBytes {
		t.Fatalf("result is %d bytes; the case needs more than twice the %d-byte response limit", len(canonical), limits.ProviderResponseBytes)
	}
	if _, err := store.Put(context.Background(), blob.PutRequest{Tenant: "tenant", MediaType: "application/json", Data: canonical, ExpiresAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatalf("escaped result of an accepted response rejected: %v", err)
	}
}
