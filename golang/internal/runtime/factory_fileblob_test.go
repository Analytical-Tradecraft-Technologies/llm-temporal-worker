package runtime

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/storage/fileblob"
)

func TestDefaultBlobFactoryBuildsDevelopmentFileStore(t *testing.T) {
	data, err := os.ReadFile("../../config.example.yaml")
	if err != nil {
		t.Fatal(err)
	}
	value := strings.Replace(string(data), "environment: production", "environment: development", 1)
	value = strings.Replace(value, `blob_store:
  kind: s3
  inline_bytes: 262144
  s3:
    bucket: acme-llmtw-production
    region: ap-southeast-2
    prefix: v1
    kms_key_id: arn:aws:kms:ap-southeast-2:123456789012:key/00000000-0000-0000-0000-000000000000
    auth:
      kind: aws_default_chain`, `blob_store:
  kind: file
  inline_bytes: 262144
  file:
    root: `+t.TempDir(), 1)
	loaded, err := config.Load([]byte(value))
	if err != nil {
		t.Fatal(err)
	}
	store, closer, err := defaultBlobFactory(context.Background(), loaded)
	if err != nil {
		t.Fatal(err)
	}
	if closer != nil {
		t.Fatal("development file store unexpectedly returned a closer")
	}
	if _, ok := store.(*fileblob.Store); !ok {
		t.Fatalf("blob store = %T, want *fileblob.Store", store)
	}
}
