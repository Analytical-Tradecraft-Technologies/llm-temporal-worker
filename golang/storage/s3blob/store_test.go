package s3blob

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/mfow/llm-temporal-worker/golang/storage/blob"
)

const testKMSKeyID = "arn:aws:kms:us-east-2:123456789012:key/00000000-0000-0000-0000-000000000000"

type fakeS3 struct {
	putInput        *s3.PutObjectInput
	getInput        *s3.GetObjectInput
	bucketHeadInput *s3.HeadBucketInput
	putErr          error
	getErr          error
	getOutput       *s3.GetObjectOutput
	getNilOutput    bool
	bucketHeadErr   error
	bucketHeadCalls int
	data            []byte
}

func (fake *fakeS3) PutObject(_ context.Context, input *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	fake.putInput = input
	return &s3.PutObjectOutput{}, fake.putErr
}

func (fake *fakeS3) GetObject(_ context.Context, input *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	fake.getInput = input
	if fake.getErr != nil {
		return nil, fake.getErr
	}
	if fake.getNilOutput {
		return nil, nil
	}
	if fake.getOutput != nil {
		return fake.getOutput, nil
	}
	return trustedGetOutput(io.NopCloser(bytes.NewReader(fake.data)), int64Ptr(int64(len(fake.data))), stringPtr("text/plain")), nil
}

func (fake *fakeS3) HeadBucket(_ context.Context, input *s3.HeadBucketInput, _ ...func(*s3.Options)) (*s3.HeadBucketOutput, error) {
	fake.bucketHeadCalls++
	fake.bucketHeadInput = input
	return &s3.HeadBucketOutput{}, fake.bucketHeadErr
}

func int64Ptr(value int64) *int64 { return &value }

func stringPtr(value string) *string { return &value }

func trustedGetOutput(body io.ReadCloser, contentLength *int64, contentType *string) *s3.GetObjectOutput {
	return &s3.GetObjectOutput{
		Body:                 body,
		ContentLength:        contentLength,
		ContentType:          contentType,
		ServerSideEncryption: types.ServerSideEncryptionAwsKms,
		SSEKMSKeyId:          stringPtr(testKMSKeyID),
	}
}

type apiOnlyS3 struct {
	fake *fakeS3
}

func (api *apiOnlyS3) PutObject(ctx context.Context, input *s3.PutObjectInput, options ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	return api.fake.PutObject(ctx, input, options...)
}

func (api *apiOnlyS3) GetObject(ctx context.Context, input *s3.GetObjectInput, options ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return api.fake.GetObject(ctx, input, options...)
}

type errorReader struct{}

func (errorReader) Read([]byte) (int, error) {
	return 0, errors.New("read failed")
}

type cancelOnReadReader struct {
	reader *bytes.Reader
	cancel context.CancelFunc
}

func (reader *cancelOnReadReader) Read(data []byte) (int, error) {
	count, err := reader.reader.Read(data)
	reader.cancel()
	return count, err
}

func testStore(t *testing.T, client API, now time.Time, maxBytes int64) *Store {
	t.Helper()
	store, err := New(Options{
		Client:   client,
		Bucket:   "bucket",
		Prefix:   "v1",
		KMSKeyID: testKMSKeyID,
		MaxBytes: maxBytes,
		Clock:    func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("New() = %v", err)
	}
	return store
}

func testRef(store *Store, now time.Time, tenant string, data []byte) blob.Ref {
	tenantPrefix, err := blob.TenantPrefix(tenant)
	if err != nil {
		panic(err)
	}
	return blob.Ref{
		Store:      "s3",
		Locator:    store.key(tenantPrefix, blob.Digest(data)),
		Digest:     blob.Digest(data),
		ByteLength: int64(len(data)),
		MediaType:  "text/plain",
		ExpiresAt:  now.Add(time.Hour),
	}
}

func TestNewRejectsUnsafeOptions(t *testing.T) {
	tests := []struct {
		name    string
		options Options
		want    string
	}{
		{name: "missing client", options: Options{Bucket: "bucket", Prefix: "v1", KMSKeyID: testKMSKeyID, MaxBytes: 1}, want: "S3 client is required"},
		{name: "missing bucket", options: Options{Client: &fakeS3{}, Prefix: "v1", KMSKeyID: testKMSKeyID, MaxBytes: 1}, want: "S3 bucket is required"},
		{name: "missing prefix", options: Options{Client: &fakeS3{}, Bucket: "bucket", KMSKeyID: testKMSKeyID, MaxBytes: 1}, want: "S3 prefix is unsafe"},
		{name: "backslash prefix", options: Options{Client: &fakeS3{}, Bucket: "bucket", Prefix: `v1\\objects`, KMSKeyID: testKMSKeyID, MaxBytes: 1}, want: "S3 prefix is unsafe"},
		{name: "newline prefix", options: Options{Client: &fakeS3{}, Bucket: "bucket", Prefix: "v1\nobjects", KMSKeyID: testKMSKeyID, MaxBytes: 1}, want: "S3 prefix is unsafe"},
		{name: "traversal prefix", options: Options{Client: &fakeS3{}, Bucket: "bucket", Prefix: "v1/../objects", KMSKeyID: testKMSKeyID, MaxBytes: 1}, want: "S3 prefix is unsafe"},
		{name: "missing KMS key ID", options: Options{Client: &fakeS3{}, Bucket: "bucket", Prefix: "v1", MaxBytes: 1}, want: "S3 KMS key ID is required"},
		{name: "KMS key ID with whitespace", options: Options{Client: &fakeS3{}, Bucket: "bucket", Prefix: "v1", KMSKeyID: " " + testKMSKeyID, MaxBytes: 1}, want: "S3 KMS key ID is required"},
		{name: "zero max bytes", options: Options{Client: &fakeS3{}, Bucket: "bucket", Prefix: "v1", KMSKeyID: testKMSKeyID, MaxBytes: 0}, want: "S3 max bytes must be positive"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := New(test.options); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("New() = %v, want error containing %q", err, test.want)
			}
		})
	}

	store, err := New(Options{Client: &fakeS3{}, Bucket: "bucket", Prefix: "/v1/", KMSKeyID: testKMSKeyID, MaxBytes: 1})
	if err != nil {
		t.Fatalf("New(normalized prefix) = %v", err)
	}
	if store.prefix != "v1" {
		t.Fatalf("normalized prefix = %q, want %q", store.prefix, "v1")
	}
}

func TestPutRejectsInvalidRequestsBeforeS3(t *testing.T) {
	now := time.Date(2026, 7, 14, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name     string
		request  blob.PutRequest
		maxBytes int64
		want     string
		is       error
	}{
		{name: "missing tenant", request: blob.PutRequest{MediaType: "text/plain", Data: []byte("payload"), ExpiresAt: now.Add(time.Hour)}, want: "blob tenant and media type are required"},
		{name: "missing media type", request: blob.PutRequest{Tenant: "tenant", Data: []byte("payload"), ExpiresAt: now.Add(time.Hour)}, want: "blob tenant and media type are required"},
		{name: "blank tenant", request: blob.PutRequest{Tenant: " \t", MediaType: "text/plain", Data: []byte("payload"), ExpiresAt: now.Add(time.Hour)}, want: "blob tenant is required"},
		{name: "too large", request: blob.PutRequest{Tenant: "tenant", MediaType: "text/plain", Data: []byte("payload"), ExpiresAt: now.Add(time.Hour)}, maxBytes: 4, want: "blob exceeds the configured size limit"},
		{name: "zero expiry", request: blob.PutRequest{Tenant: "tenant", MediaType: "text/plain", Data: []byte("payload")}, is: blob.ErrExpired},
		{name: "expired", request: blob.PutRequest{Tenant: "tenant", MediaType: "text/plain", Data: []byte("payload"), ExpiresAt: now}, is: blob.ErrExpired},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fake := &fakeS3{}
			maxBytes := test.maxBytes
			if maxBytes == 0 {
				maxBytes = 100
			}
			store := testStore(t, fake, now, maxBytes)
			_, err := store.Put(context.Background(), test.request)
			if test.is != nil {
				if !errors.Is(err, test.is) {
					t.Fatalf("Put() = %v, want %v", err, test.is)
				}
			} else if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Put() = %v, want error containing %q", err, test.want)
			}
			if fake.putInput != nil {
				t.Fatal("PutObject called for invalid request")
			}
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	fake := &fakeS3{}
	store := testStore(t, fake, now, 100)
	if _, err := store.Put(ctx, blob.PutRequest{Tenant: "tenant", MediaType: "text/plain", Data: []byte("data"), ExpiresAt: now.Add(time.Hour)}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Put(canceled) = %v, want context.Canceled", err)
	}
	if fake.putInput != nil {
		t.Fatal("PutObject called with canceled context")
	}

	var nilStore *Store
	if _, err := nilStore.Put(context.Background(), blob.PutRequest{}); err == nil || !strings.Contains(err.Error(), "S3 blob store is nil") {
		t.Fatalf("nil Store.Put() = %v, want nil-store diagnostic", err)
	}
}

func TestPutClassifiesS3WriteFailures(t *testing.T) {
	now := time.Date(2026, 7, 14, 0, 0, 0, 0, time.UTC)
	data := []byte("data")
	request := blob.PutRequest{Tenant: "tenant", MediaType: "text/plain", Data: data, ExpiresAt: now.Add(time.Hour)}

	t.Run("provider failure is wrapped", func(t *testing.T) {
		fake := &fakeS3{putErr: errors.New("provider unavailable")}
		store := testStore(t, fake, now, 100)
		if _, err := store.Put(context.Background(), request); err == nil || !strings.Contains(err.Error(), "put blob: provider unavailable") {
			t.Fatalf("Put() = %v, want wrapped provider error", err)
		}
	})

	matchingOutput := func(body []byte) *s3.GetObjectOutput {
		output := trustedGetOutput(io.NopCloser(bytes.NewReader(body)), int64Ptr(int64(len(data))), stringPtr(request.MediaType))
		output.Metadata = map[string]string{
			"llmtw-digest":      blob.Digest(data),
			"llmtw-byte-length": strconv.Itoa(len(data)),
		}
		return output
	}

	t.Run("precondition conflict with failed verification read is rejected", func(t *testing.T) {
		fake := &fakeS3{
			putErr: &smithy.GenericAPIError{Code: "PreconditionFailed", Message: "exists"},
			getErr: errors.New("get unavailable"),
		}
		store := testStore(t, fake, now, 100)
		if _, err := store.Put(context.Background(), request); !errors.Is(err, blob.ErrConflict) {
			t.Fatalf("Put() = %v, want ErrConflict", err)
		}
	})

	tests := []struct {
		name      string
		configure func(*s3.GetObjectOutput)
		body      []byte
	}{
		{
			name: "precondition conflict with missing identity metadata is rejected",
			configure: func(output *s3.GetObjectOutput) {
				output.Metadata = nil
			},
		},
		{
			name: "precondition conflict with forged digest metadata is rejected",
			configure: func(output *s3.GetObjectOutput) {
				output.Metadata["llmtw-digest"] = blob.Digest([]byte("else"))
			},
		},
		{
			name: "precondition conflict with different bytes is rejected",
			body: []byte("evil"),
		},
		{
			name: "precondition conflict with no encryption is rejected",
			configure: func(output *s3.GetObjectOutput) {
				output.ServerSideEncryption = ""
				output.SSEKMSKeyId = nil
			},
		},
		{
			name: "precondition conflict with SSE-S3 is rejected",
			configure: func(output *s3.GetObjectOutput) {
				output.ServerSideEncryption = types.ServerSideEncryptionAes256
				output.SSEKMSKeyId = nil
			},
		},
		{
			name: "precondition conflict with wrong KMS key is rejected",
			configure: func(output *s3.GetObjectOutput) {
				output.SSEKMSKeyId = stringPtr("arn:aws:kms:us-east-2:123456789012:key/11111111-1111-1111-1111-111111111111")
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			body := test.body
			if body == nil {
				body = data
			}
			output := matchingOutput(body)
			if test.configure != nil {
				test.configure(output)
			}
			fake := &fakeS3{
				putErr:    &smithy.GenericAPIError{Code: "PreconditionFailed", Message: "exists"},
				getOutput: output,
			}
			store := testStore(t, fake, now, 100)
			if _, err := store.Put(context.Background(), request); !errors.Is(err, blob.ErrConflict) {
				t.Fatalf("Put() = %v, want ErrConflict", err)
			}
		})
	}

	t.Run("precondition conflict with matching content and KMS policy is idempotent", func(t *testing.T) {
		fake := &fakeS3{
			putErr:    &smithy.GenericAPIError{Code: "PreconditionFailed", Message: "exists"},
			getOutput: matchingOutput(data),
		}
		store := testStore(t, fake, now, 100)
		if _, err := store.Put(context.Background(), request); err != nil {
			t.Fatalf("Put() = %v, want idempotent success", err)
		}
	})
}

func TestGetRejectsUntrustedS3Responses(t *testing.T) {
	now := time.Date(2026, 7, 14, 0, 0, 0, 0, time.UTC)
	payload := []byte("payload")
	tests := []struct {
		name      string
		configure func(*fakeS3, blob.Ref)
		want      string
		is        error
	}{
		{name: "provider failure", configure: func(fake *fakeS3, _ blob.Ref) { fake.getErr = errors.New("provider unavailable") }, want: "get blob: provider unavailable"},
		{name: "nil result", configure: func(fake *fakeS3, _ blob.Ref) { fake.getNilOutput = true }, is: blob.ErrNotFound},
		{name: "nil body", configure: func(fake *fakeS3, _ blob.Ref) { fake.getOutput = &s3.GetObjectOutput{} }, is: blob.ErrNotFound},
		{name: "missing SSE-KMS provenance", configure: func(fake *fakeS3, ref blob.Ref) {
			fake.getOutput = trustedGetOutput(io.NopCloser(bytes.NewReader(payload)), int64Ptr(ref.ByteLength), stringPtr(ref.MediaType))
			fake.getOutput.ServerSideEncryption = ""
			fake.getOutput.SSEKMSKeyId = nil
		}, is: blob.ErrDigestMismatch},
		{name: "wrong KMS key", configure: func(fake *fakeS3, ref blob.Ref) {
			fake.getOutput = trustedGetOutput(io.NopCloser(bytes.NewReader(payload)), int64Ptr(ref.ByteLength), stringPtr(ref.MediaType))
			fake.getOutput.SSEKMSKeyId = stringPtr("arn:aws:kms:us-east-2:123456789012:key/ffffffff-ffff-ffff-ffff-ffffffffffff")
		}, is: blob.ErrDigestMismatch},
		{name: "content length mismatch", configure: func(fake *fakeS3, ref blob.Ref) {
			fake.getOutput = trustedGetOutput(io.NopCloser(bytes.NewReader(payload)), int64Ptr(ref.ByteLength+1), nil)
		}, is: blob.ErrDigestMismatch},
		{name: "content type mismatch", configure: func(fake *fakeS3, ref blob.Ref) {
			fake.getOutput = trustedGetOutput(io.NopCloser(bytes.NewReader(payload)), int64Ptr(ref.ByteLength), stringPtr("application/octet-stream"))
		}, is: blob.ErrDigestMismatch},
		{name: "missing content type", configure: func(fake *fakeS3, ref blob.Ref) {
			fake.getOutput = trustedGetOutput(io.NopCloser(bytes.NewReader(payload)), int64Ptr(ref.ByteLength), nil)
		}, is: blob.ErrDigestMismatch},
		{name: "digest mismatch", configure: func(fake *fakeS3, ref blob.Ref) {
			fake.getOutput = trustedGetOutput(io.NopCloser(bytes.NewReader([]byte("baddata"))), int64Ptr(ref.ByteLength), stringPtr(ref.MediaType))
		}, is: blob.ErrDigestMismatch},
		{name: "read failure", configure: func(fake *fakeS3, ref blob.Ref) {
			fake.getOutput = trustedGetOutput(io.NopCloser(errorReader{}), int64Ptr(ref.ByteLength), stringPtr(ref.MediaType))
		}, want: "read blob: read failed"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fake := &fakeS3{}
			store := testStore(t, fake, now, 100)
			ref := testRef(store, now, "tenant", payload)
			test.configure(fake, ref)
			_, err := store.Get(context.Background(), "tenant", ref)
			if test.is != nil {
				if !errors.Is(err, test.is) {
					t.Fatalf("Get() = %v, want %v", err, test.is)
				}
			} else if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Get() = %v, want error containing %q", err, test.want)
			}
		})
	}

	t.Run("response larger than limit is rejected", func(t *testing.T) {
		fake := &fakeS3{getOutput: trustedGetOutput(io.NopCloser(bytes.NewReader([]byte("12345"))), int64Ptr(4), stringPtr("text/plain"))}
		store := testStore(t, fake, now, 4)
		ref := testRef(store, now, "tenant", []byte("1234"))
		if _, err := store.Get(context.Background(), "tenant", ref); !errors.Is(err, blob.ErrDigestMismatch) {
			t.Fatalf("Get() = %v, want ErrDigestMismatch", err)
		}
	})

	t.Run("cancellation after read is observed", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		fake := &fakeS3{}
		store := testStore(t, fake, now, 100)
		ref := testRef(store, now, "tenant", payload)
		fake.getOutput = trustedGetOutput(io.NopCloser(&cancelOnReadReader{reader: bytes.NewReader(payload), cancel: cancel}), int64Ptr(ref.ByteLength), stringPtr(ref.MediaType))
		if _, err := store.Get(ctx, "tenant", ref); !errors.Is(err, context.Canceled) {
			t.Fatalf("Get() = %v, want context.Canceled", err)
		}
	})

	var nilStore *Store
	if _, err := nilStore.Get(context.Background(), "tenant", blob.Ref{}); err == nil || !strings.Contains(err.Error(), "S3 blob store is nil") {
		t.Fatalf("nil Store.Get() = %v, want nil-store diagnostic", err)
	}

	store := testStore(t, &fakeS3{}, now, 100)
	if _, err := store.Get(context.Background(), "tenant", blob.Ref{}); err == nil || !strings.Contains(err.Error(), "store and locator are required") {
		t.Fatalf("invalid ref Get() = %v, want ref-validation diagnostic", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.Get(ctx, "tenant", testRef(store, now, "tenant", payload)); !errors.Is(err, context.Canceled) {
		t.Fatalf("Get(canceled) = %v, want context.Canceled", err)
	}
}

func TestProbeBucketFailsClosedForUnsupportedAndUnavailableClients(t *testing.T) {
	now := time.Date(2026, 7, 14, 0, 0, 0, 0, time.UTC)
	store := testStore(t, &apiOnlyS3{fake: &fakeS3{}}, now, 100)
	if err := store.ProbeBucket(context.Background()); err == nil || !strings.Contains(err.Error(), "does not support bucket probes") {
		t.Fatalf("ProbeBucket(unsupported) = %v, want capability diagnostic", err)
	}

	fake := &fakeS3{bucketHeadErr: errors.New("access denied")}
	store = testStore(t, fake, now, 100)
	if err := store.ProbeBucket(context.Background()); err == nil || !strings.Contains(err.Error(), "head S3 bucket: access denied") {
		t.Fatalf("ProbeBucket(error) = %v, want wrapped provider error", err)
	}
	if fake.bucketHeadCalls != 1 {
		t.Fatalf("HeadBucket calls = %d, want 1", fake.bucketHeadCalls)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := store.ProbeBucket(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("ProbeBucket(canceled) = %v, want context.Canceled", err)
	}

	var nilStore *Store
	if err := nilStore.ProbeBucket(context.Background()); err == nil || !strings.Contains(err.Error(), "S3 blob store is nil") {
		t.Fatalf("nil Store.ProbeBucket() = %v, want nil-store diagnostic", err)
	}
}

func TestStoreUsesContentAddressedConditionalS3Object(t *testing.T) {
	now := time.Date(2026, 7, 14, 0, 0, 0, 0, time.UTC)
	fake := &fakeS3{data: []byte("payload")}
	store, err := New(Options{Client: fake, Bucket: "bucket", Prefix: "v1", KMSKeyID: testKMSKeyID, MaxBytes: 100, Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := store.Put(context.Background(), blob.PutRequest{Tenant: "tenant", MediaType: "text/plain", Data: []byte("payload"), ExpiresAt: now.Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	if fake.putInput == nil || fake.putInput.IfNoneMatch == nil || *fake.putInput.IfNoneMatch != "*" {
		t.Fatal("PutObject did not request create-if-absent")
	}
	if fake.putInput.ServerSideEncryption != types.ServerSideEncryptionAwsKms {
		t.Fatalf("PutObject ServerSideEncryption = %q, want %q", fake.putInput.ServerSideEncryption, types.ServerSideEncryptionAwsKms)
	}
	if fake.putInput.SSEKMSKeyId == nil || *fake.putInput.SSEKMSKeyId != testKMSKeyID {
		t.Fatalf("PutObject SSEKMSKeyId = %v, want %q", fake.putInput.SSEKMSKeyId, testKMSKeyID)
	}
	got, err := store.Get(context.Background(), "tenant", ref)
	if err != nil || string(got) != "payload" {
		t.Fatalf("get = %q, %v", got, err)
	}
	if fake.getInput == nil || *fake.getInput.Key != ref.Locator {
		t.Fatal("GetObject used an unexpected key")
	}
	got[0] = 'X'
	again, err := store.Get(context.Background(), "tenant", ref)
	if err != nil {
		t.Fatal(err)
	}
	if string(again) != "payload" || string(fake.data) != "payload" {
		t.Fatalf("mutating Get result changed stored bytes: again=%q stored=%q", again, fake.data)
	}
}

func TestStoreHandlesExistingAndDigestMismatch(t *testing.T) {
	now := time.Now().UTC()
	data := []byte("payload")
	existing := trustedGetOutput(io.NopCloser(bytes.NewReader(data)), int64Ptr(int64(len(data))), stringPtr("text/plain"))
	existing.Metadata = map[string]string{
		"llmtw-digest":      blob.Digest(data),
		"llmtw-byte-length": strconv.Itoa(len(data)),
	}
	fake := &fakeS3{
		putErr:    &smithy.GenericAPIError{Code: "PreconditionFailed", Message: "exists"},
		getOutput: existing,
	}
	store, err := New(Options{Client: fake, Bucket: "bucket", Prefix: "v1", KMSKeyID: testKMSKeyID, MaxBytes: 100, Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.Put(context.Background(), blob.PutRequest{Tenant: "tenant", MediaType: "text/plain", Data: data, ExpiresAt: now.Add(time.Hour)})
	if err != nil || fake.getInput == nil {
		t.Fatalf("existing put = %v, verification get = %#v", err, fake.getInput)
	}
	fake.data = []byte("other")
	ref := blob.Ref{Store: "s3", Locator: "v1/" + "bad", Digest: blob.Digest([]byte("payload")), ByteLength: 7, MediaType: "text/plain", ExpiresAt: now.Add(time.Hour)}
	if _, err := store.Get(context.Background(), "tenant", ref); !errors.Is(err, blob.ErrTenantMismatch) {
		t.Fatalf("wrong locator error = %v", err)
	}
}

func TestProbeBucketUsesOnlyBucketMetadata(t *testing.T) {
	fake := &fakeS3{}
	store, err := New(Options{Client: fake, Bucket: "bucket", Prefix: "v1", KMSKeyID: testKMSKeyID, MaxBytes: 100})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ProbeBucket(context.Background()); err != nil {
		t.Fatal(err)
	}
	if fake.bucketHeadCalls != 1 || fake.bucketHeadInput == nil || fake.bucketHeadInput.Bucket == nil || *fake.bucketHeadInput.Bucket != "bucket" {
		t.Fatalf("HeadBucket calls/input = %d/%#v", fake.bucketHeadCalls, fake.bucketHeadInput)
	}
	if fake.putInput != nil || fake.getInput != nil {
		t.Fatal("bucket probe accessed tenant object content")
	}
}
