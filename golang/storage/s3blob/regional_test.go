package s3blob

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/blob"
	"github.com/aws/smithy-go"
)

func TestRegionalBlobFailover(t *testing.T) {
	primary := &fakeS3{putErr: &smithy.GenericAPIError{Code: "ServiceUnavailable"}, getErr: &smithy.GenericAPIError{Code: "NoSuchKey"}, bucketHeadErr: &smithy.GenericAPIError{Code: "ServiceUnavailable"}}
	alternate := &fakeS3{data: []byte("hello")}
	makeStore := func(client *fakeS3, bucket string) *Store {
		s, err := New(Options{Client: client, Bucket: bucket, Prefix: "results", MaxBytes: 100})
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	store, err := NewRegional([]*Store{makeStore(primary, "primary-bucket"), makeStore(alternate, "fallback-bucket")}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ProbeBucket(context.Background()); err != nil {
		t.Fatal(err)
	}
	store.router.Active.Store(0)
	ref, err := store.Put(context.Background(), blob.PutRequest{Tenant: "tenant", MediaType: "text/plain", Data: []byte("hello"), ExpiresAt: time.Now().Add(time.Hour)})
	if err != nil || alternate.putInput == nil {
		t.Fatalf("put fallback: %v", err)
	}
	store.router.Active.Store(0)
	data, err := store.Get(context.Background(), "tenant", ref)
	if err != nil || string(data) != "hello" {
		t.Fatalf("get fallback: %q %v", data, err)
	}
	primary.getErr = &smithy.GenericAPIError{Code: "AccessDenied"}
	store.router.Active.Store(0)
	alternate.getInput = nil
	if _, err := store.Get(context.Background(), "tenant", ref); err == nil || alternate.getInput != nil {
		t.Fatal("authorization failure incorrectly bypassed")
	}
	primary.getErr = nil
	primary.data = []byte("wrong")
	store.router.Active.Store(0)
	if _, err := store.Get(context.Background(), "tenant", ref); !errors.Is(err, blob.ErrDigestMismatch) {
		t.Fatalf("corruption bypassed: %v", err)
	}
}
