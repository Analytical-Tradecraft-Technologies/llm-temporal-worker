package runtime

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/blob"
)

func TestBlobStoreLimitAdmitsResponsesLargerThanRequestLimit(t *testing.T) {
	value := config.Config{
		BlobStore: config.BlobStoreConfig{Kind: "file", File: config.FileBlobConfig{Root: t.TempDir()}},
		Limits:    config.LimitsConfig{RequestBytes: 1024, ProviderResponseBytes: 4096},
	}
	if got := blobMaxBytes(value.Limits); got != 8192 {
		t.Fatalf("blobMaxBytes = %d, want twice the provider response limit", got)
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
