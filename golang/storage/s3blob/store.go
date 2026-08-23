// Package s3blob implements the production object-store port using the
// official AWS SDK v2 S3 client. Client construction and credential resolution
// remain outside this package so workload identity/default chains are used.
package s3blob

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/mfow/llm-temporal-worker/golang/storage/blob"
)

type API interface {
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

type HeadAPI interface {
	HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
}

// BucketHeadAPI is deliberately separate from tenant object access. Runtime
// readiness needs only to prove that the configured bucket is reachable; it
// must not read or write any tenant key to do so.
type BucketHeadAPI interface {
	HeadBucket(context.Context, *s3.HeadBucketInput, ...func(*s3.Options)) (*s3.HeadBucketOutput, error)
}

type Options struct {
	Client   API
	Bucket   string
	Prefix   string
	MaxBytes int64
	Clock    func() time.Time
}

type Store struct {
	client   API
	bucket   string
	prefix   string
	maxBytes int64
	clock    func() time.Time
}

func New(options Options) (*Store, error) {
	if options.Client == nil {
		return nil, fmt.Errorf("S3 client is required")
	}
	if strings.TrimSpace(options.Bucket) == "" {
		return nil, fmt.Errorf("S3 bucket is required")
	}
	if strings.TrimSpace(options.Prefix) == "" || strings.ContainsAny(options.Prefix, "\\\r\n") || strings.Contains(options.Prefix, "..") {
		return nil, fmt.Errorf("S3 prefix is unsafe")
	}
	if options.MaxBytes <= 0 {
		return nil, fmt.Errorf("S3 max bytes must be positive")
	}
	if options.Clock == nil {
		options.Clock = time.Now
	}
	return &Store{client: options.Client, bucket: options.Bucket, prefix: strings.Trim(options.Prefix, "/"), maxBytes: options.MaxBytes, clock: options.Clock}, nil
}

func (store *Store) Put(ctx context.Context, request blob.PutRequest) (blob.Ref, error) {
	if store == nil {
		return blob.Ref{}, fmt.Errorf("S3 blob store is nil")
	}
	if err := ctx.Err(); err != nil {
		return blob.Ref{}, err
	}
	if request.Tenant == "" || request.MediaType == "" {
		return blob.Ref{}, fmt.Errorf("blob tenant and media type are required")
	}
	if int64(len(request.Data)) > store.maxBytes {
		return blob.Ref{}, fmt.Errorf("blob exceeds the configured size limit")
	}
	if request.ExpiresAt.IsZero() || !store.clock().Before(request.ExpiresAt) {
		return blob.Ref{}, blob.ErrExpired
	}
	tenantPrefix, err := blob.TenantPrefix(request.Tenant)
	if err != nil {
		return blob.Ref{}, err
	}
	digest := blob.Digest(request.Data)
	key := store.key(tenantPrefix, digest)
	ref := blob.Ref{Store: "s3", Locator: key, Digest: digest, ByteLength: int64(len(request.Data)), MediaType: request.MediaType, ExpiresAt: request.ExpiresAt}
	if err := ref.Validate(store.clock()); err != nil {
		return blob.Ref{}, err
	}
	digestBytes := sha256.Sum256(request.Data)
	_, err = store.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:         aws.String(store.bucket),
		Key:            aws.String(key),
		Body:           bytes.NewReader(request.Data),
		ContentLength:  aws.Int64(int64(len(request.Data))),
		ContentType:    aws.String(request.MediaType),
		ChecksumSHA256: aws.String(base64.StdEncoding.EncodeToString(digestBytes[:])),
		IfNoneMatch:    aws.String("*"),
		Metadata: map[string]string{
			"llmtw-digest":      digest,
			"llmtw-byte-length": strconv.FormatInt(int64(len(request.Data)), 10),
		},
	})
	if err == nil {
		return ref, nil
	}
	if !isPreconditionFailure(err) {
		return blob.Ref{}, fmt.Errorf("put blob: %w", err)
	}
	if head, ok := store.client.(HeadAPI); ok {
		if existing, headErr := head.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(store.bucket), Key: aws.String(key)}); headErr == nil && store.existingObjectMatches(existing, ref) {
			return ref, nil
		}
	}
	return blob.Ref{}, blob.ErrConflict
}

// existingObjectMatches proves that a conditional-write conflict is the
// idempotent write of the same immutable object. A successful HEAD alone is
// not enough: a stale or manually replaced key must not be reported as a
// successful Put with a reference whose digest and metadata are unverified.
func (store *Store) existingObjectMatches(head *s3.HeadObjectOutput, ref blob.Ref) bool {
	if head == nil || head.ContentLength == nil || *head.ContentLength != ref.ByteLength {
		return false
	}
	if head.Metadata == nil || !strings.EqualFold(head.Metadata["llmtw-digest"], ref.Digest) {
		return false
	}
	if head.Metadata["llmtw-byte-length"] != strconv.FormatInt(ref.ByteLength, 10) {
		return false
	}
	return head.ContentType != nil && *head.ContentType == ref.MediaType
}

func (store *Store) Get(ctx context.Context, tenant string, ref blob.Ref) ([]byte, error) {
	if store == nil {
		return nil, fmt.Errorf("S3 blob store is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := ref.Validate(store.clock()); err != nil {
		return nil, err
	}
	prefix, err := blob.TenantPrefix(tenant)
	if err != nil {
		return nil, err
	}
	if ref.Store != "s3" || ref.Locator != store.key(prefix, ref.Digest) {
		return nil, blob.ErrTenantMismatch
	}
	result, err := store.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(store.bucket), Key: aws.String(ref.Locator)})
	if err != nil {
		return nil, fmt.Errorf("get blob: %w", err)
	}
	if result == nil || result.Body == nil {
		return nil, blob.ErrNotFound
	}
	defer result.Body.Close()
	if result.ContentLength != nil && *result.ContentLength != ref.ByteLength {
		return nil, blob.ErrDigestMismatch
	}
	// Content type is part of the immutable blob reference. A digest and
	// length match alone do not prove that an object has not been replaced
	// with the same bytes under a different media type.
	if result.ContentType == nil || *result.ContentType != ref.MediaType {
		return nil, blob.ErrDigestMismatch
	}
	data, err := io.ReadAll(io.LimitReader(result.Body, store.maxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read blob: %w", err)
	}
	if int64(len(data)) != ref.ByteLength || int64(len(data)) > store.maxBytes || blob.Digest(data) != ref.Digest {
		return nil, blob.ErrDigestMismatch
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return append([]byte(nil), data...), nil
}

// ProbeBucket checks access to the configured bucket without inspecting a
// tenant object. It is intended for the runtime's fail-closed dependency
// probe, not for request-path object validation.
func (store *Store) ProbeBucket(ctx context.Context) error {
	if store == nil || store.client == nil {
		return fmt.Errorf("S3 blob store is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	client, ok := store.client.(BucketHeadAPI)
	if !ok {
		return fmt.Errorf("S3 client does not support bucket probes")
	}
	if _, err := client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(store.bucket)}); err != nil {
		return fmt.Errorf("head S3 bucket: %w", err)
	}
	return nil
}

func (store *Store) key(tenantPrefix, digest string) string {
	return store.prefix + "/" + tenantPrefix + "/" + digest
}

func isPreconditionFailure(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		return apiErr.ErrorCode() == "PreconditionFailed" || apiErr.ErrorCode() == "ConditionalRequestConflict"
	}
	return false
}
