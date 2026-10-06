package cloudstate

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/blob"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/regional"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type regionalBucket struct {
	Region string `json:"region"`
	Bucket string `json:"bucket"`
}
type regionalOptions struct {
	DynamoDBRegions []string         `json:"dynamodb_regions"`
	PayloadReplicas []regionalBucket `json:"payload_replicas"`
	AttemptTimeout  string           `json:"attempt_timeout"`
	timeout         time.Duration
}

// Regional handles are opened lazily: a failed primary must not prevent startup.
// Every DynamoDB endpoint must prove MRSC membership before it can serve calls.
type regionalTable struct {
	router regional.Router
	open   func(context.Context, int) (kv.KeyValueStore, error)
	mu     sync.Mutex
	stores map[int]kv.KeyValueStore
}

func (s *regionalTable) store(ctx context.Context, i int) (kv.KeyValueStore, error) {
	s.mu.Lock()
	store := s.stores[i]
	s.mu.Unlock()
	if store != nil {
		return store, nil
	}
	store, err := s.open(ctx, i)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.stores[i] = store
	s.mu.Unlock()
	return store, nil
}
func (s *regionalTable) Get(ctx context.Context, key kv.KeyValueKey) (kv.KeyValueRecord, error) {
	return regional.Run(ctx, &s.router, regional.RetryRead, func(ctx context.Context, i int) (kv.KeyValueRecord, error) {
		store, err := s.store(ctx, i)
		if err != nil {
			return kv.KeyValueRecord{}, err
		}
		return store.Get(ctx, key)
	})
}
func (s *regionalTable) Create(ctx context.Context, item kv.KeyValueItem) (kv.KeyValueVersion, error) {
	return s.mutate(ctx, func(ctx context.Context, store kv.KeyValueStore) (kv.KeyValueVersion, error) {
		return store.Create(ctx, item)
	})
}
func (s *regionalTable) Replace(ctx context.Context, item kv.KeyValueItem, v kv.KeyValueVersion) (kv.KeyValueVersion, error) {
	return s.mutate(ctx, func(ctx context.Context, store kv.KeyValueStore) (kv.KeyValueVersion, error) {
		return store.Replace(ctx, item, v)
	})
}
func (s *regionalTable) Delete(ctx context.Context, key kv.KeyValueKey, v kv.KeyValueVersion) error {
	_, err := s.mutate(ctx, func(ctx context.Context, store kv.KeyValueStore) (kv.KeyValueVersion, error) {
		return "", store.Delete(ctx, key, v)
	})
	return err
}
func (s *regionalTable) mutate(ctx context.Context, call func(context.Context, kv.KeyValueStore) (kv.KeyValueVersion, error)) (kv.KeyValueVersion, error) {
	return regional.Run(ctx, &s.router, regional.RetryMutation, func(ctx context.Context, i int) (kv.KeyValueVersion, error) {
		store, err := s.store(ctx, i)
		if err != nil {
			return "", err
		}
		return call(ctx, store)
	})
}

type regionalPageToken struct {
	Region int    `json:"region"`
	Token  string `json:"token"`
}

func (s *regionalTable) QueryPartition(ctx context.Context, q kv.KeyValueQuery) (kv.KeyValueQueryPage, error) {
	wrap := func(page kv.KeyValueQueryPage, i int) (kv.KeyValueQueryPage, error) {
		if page.NextPageToken != "" {
			data, _ := json.Marshal(regionalPageToken{i, page.NextPageToken})
			page.NextPageToken = base64.RawURLEncoding.EncodeToString(data)
		}
		return page, nil
	}
	if q.PageToken != "" {
		// SDK continuation tokens bind the regional table ARN. Never forward a
		// regional cursor to another endpoint or silently skip records on failover.
		var token regionalPageToken
		if len(q.PageToken) > 256*1024 {
			return kv.KeyValueQueryPage{}, contracts.ErrInvalidArgument
		}
		data, err := base64.RawURLEncoding.DecodeString(q.PageToken)
		if err != nil {
			return kv.KeyValueQueryPage{}, contracts.ErrInvalidArgument
		}
		if json.Unmarshal(data, &token) != nil || token.Region < 0 || token.Region >= s.router.Count || token.Token == "" {
			return kv.KeyValueQueryPage{}, contracts.ErrInvalidArgument
		}
		q.PageToken = token.Token
		attempt, cancel := regional.AttemptContext(ctx, s.router.Timeout, 1)
		defer cancel()
		store, err := s.store(attempt, token.Region)
		if err != nil {
			return kv.KeyValueQueryPage{}, err
		}
		page, err := store.QueryPartition(attempt, q)
		if err != nil {
			return kv.KeyValueQueryPage{}, err
		}
		return wrap(page, token.Region)
	}
	return regional.Run(ctx, &s.router, regional.RetryRead, func(ctx context.Context, i int) (kv.KeyValueQueryPage, error) {
		store, err := s.store(ctx, i)
		if err != nil {
			return kv.KeyValueQueryPage{}, err
		}
		page, err := store.QueryPartition(ctx, q)
		if err != nil {
			return kv.KeyValueQueryPage{}, err
		}
		return wrap(page, i)
	})
}
func (s *regionalTable) probe(ctx context.Context) error {
	_, err := regional.Run(ctx, &s.router, regional.RetryRead, func(ctx context.Context, i int) (struct{}, error) { _, err := s.open(ctx, i); return struct{}{}, err })
	return err
}

type regionalBlobs struct {
	router regional.Router
	open   func(context.Context, int) (blob.BlobStore, error)
	mu     sync.Mutex
	stores map[int]blob.BlobStore
}

func (s *regionalBlobs) store(ctx context.Context, i int) (blob.BlobStore, error) {
	s.mu.Lock()
	store := s.stores[i]
	s.mu.Unlock()
	if !nilInterface(store) {
		return store, nil
	}
	store, err := s.open(ctx, i)
	if err != nil {
		return nil, err
	}
	if nilInterface(store) {
		return nil, ErrInvalid
	}
	s.mu.Lock()
	if s.stores == nil {
		s.stores = make(map[int]blob.BlobStore)
	}
	s.stores[i] = store
	s.mu.Unlock()
	return store, nil
}

func (s *regionalBlobs) Create(ctx context.Context, key blob.BlobKey, body io.Reader, size int64) error {
	// Bound staging and permit a fresh reader only after a definitive rejection.
	if body == nil || size < 0 || size > maxPayloadBytes+64 {
		return contracts.ErrInvalidArgument
	}
	data, err := io.ReadAll(io.LimitReader(body, size+1))
	if err != nil {
		return err
	}
	if int64(len(data)) != size {
		return contracts.ErrInvalidArgument
	}
	_, err = regional.Run(ctx, &s.router, regional.RetryMutation, func(ctx context.Context, i int) (struct{}, error) {
		store, err := s.store(ctx, i)
		if err != nil {
			return struct{}{}, err
		}
		return struct{}{}, store.Create(ctx, key, bytes.NewReader(data), size)
	})
	return err
}
func (s *regionalBlobs) Open(ctx context.Context, key blob.BlobKey) (blob.BlobReadResult, error) {
	// Consume each stream within its attempt deadline. No partially read body or
	// canceled stream escapes; repository decryption still verifies integrity.
	return regional.Run(ctx, &s.router, func(err error) bool { return regional.Unavailable(err) || regional.Missing(err) }, func(ctx context.Context, i int) (blob.BlobReadResult, error) {
		store, err := s.store(ctx, i)
		if err != nil {
			return blob.BlobReadResult{}, err
		}
		opened, err := store.Open(ctx, key)
		if err != nil {
			return blob.BlobReadResult{}, err
		}
		if opened.Body == nil {
			return blob.BlobReadResult{}, ErrCorrupt
		}
		defer opened.Body.Close()
		if opened.Size < 0 || opened.Size > maxPayloadBytes+64 {
			return blob.BlobReadResult{}, ErrCorrupt
		}
		data, err := io.ReadAll(io.LimitReader(opened.Body, opened.Size+1))
		if err != nil {
			return blob.BlobReadResult{}, err
		}
		if int64(len(data)) != opened.Size {
			return blob.BlobReadResult{}, ErrCorrupt
		}
		return blob.BlobReadResult{Body: io.NopCloser(bytes.NewReader(data)), Size: opened.Size}, nil
	})
}
func (s *regionalBlobs) Delete(context.Context, blob.BlobKey) error {
	// No worker path deletes payloads. A partial regional delete cannot uphold
	// the generic contract; retention is deployment-owned.
	return contracts.ErrUnsupported
}
func (s *regionalBlobs) probe(ctx context.Context) error {
	_, err := regional.Run(ctx, &s.router, regional.RetryRead, func(ctx context.Context, i int) (struct{}, error) { _, err := s.open(ctx, i); return struct{}{}, err })
	return err
}

func openRegional(ctx context.Context, c Config, secret []byte, initialize func(context.Context, map[string]any) (provider.StorageProvider, error), f regionalOptions) (*Repository, error) {
	var err error
	f.timeout, err = time.ParseDuration(f.AttemptTimeout)
	if err != nil {
		return nil, ErrInvalid
	}
	// Normalize generic provider maps before inspecting them. External callers may
	// supply typed maps; malformed documents must return errors rather than panic.
	data, err := json.Marshal(c.Provider)
	if err != nil {
		return nil, ErrInvalid
	}
	var values map[string]any
	if json.Unmarshal(data, &values) != nil {
		return nil, ErrInvalid
	}
	awsValues, ok := values["aws"].(map[string]any)
	if !ok {
		return nil, ErrInvalid
	}
	tables, ok := values["key_value_stores"].(map[string]any)
	if !ok {
		return nil, ErrInvalid
	}
	physical, ok := tables[c.RequestTable].(string)
	if !ok || !safeText(physical, 256) {
		return nil, ErrInvalid
	}
	payloads, ok := values["blob_stores"].(map[string]any)
	if !ok {
		return nil, ErrInvalid
	}
	primaryBucket, ok := payloads[c.PayloadStore].(string)
	if !ok || !safeText(primaryBucket, 256) {
		return nil, ErrInvalid
	}
	c.Provider = values
	primary, _ := awsValues["region"].(string)
	allow, _ := awsValues["allow_mrsc"].(bool)
	profile, _ := awsValues["profile"].(string)
	if !allow || primary == "" || len(f.DynamoDBRegions) < 1 || len(f.DynamoDBRegions) > 2 || len(f.PayloadReplicas) < 1 || len(f.PayloadReplicas) > 2 || f.timeout <= 0 || f.timeout > 30*time.Second {
		return nil, ErrInvalid
	}
	// Copy each provider document: never mutate caller-owned configuration.
	makeProvider := func(ctx context.Context, region, bucket string) (provider.StorageProvider, error) {
		data, err := json.Marshal(c.Provider)
		if err != nil {
			return nil, err
		}
		var values map[string]any
		if err = json.Unmarshal(data, &values); err != nil {
			return nil, err
		}
		aws := values["aws"].(map[string]any)
		delete(aws, "failover")
		aws["region"] = region
		if bucket != "" {
			values["blob_stores"].(map[string]any)[c.PayloadStore] = bucket
		}
		return initialize(ctx, values)
	}
	baseline, err := makeProvider(ctx, primary, "")
	if err != nil {
		return nil, err
	}
	if nilInterface(baseline) {
		return nil, ErrInvalid
	}
	regions := append([]string{primary}, f.DynamoDBRegions...)
	seen := map[string]bool{}
	for _, region := range regions {
		if region == "" || seen[region] {
			return nil, ErrInvalid
		}
		seen[region] = true
	}
	seen = map[string]bool{primary: true}
	for _, bucket := range f.PayloadReplicas {
		if bucket.Region == "" || bucket.Bucket == "" || seen[bucket.Region] {
			return nil, ErrInvalid
		}
		seen[bucket.Region] = true
	}
	table := &regionalTable{router: regional.Router{Count: len(regions), Timeout: f.timeout}, stores: map[int]kv.KeyValueStore{}}
	table.open = func(ctx context.Context, i int) (kv.KeyValueStore, error) {
		// allow_mrsc alone also accepts local tables; failover must require a global
		// table containing every configured endpoint, never unrelated local tables.
		opts := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(regions[i])}
		if profile != "" {
			opts = append(opts, awsconfig.WithSharedConfigProfile(profile))
		}
		cfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
		if err != nil {
			return nil, err
		}
		cfg.Retryer = func() aws.Retryer { return aws.NopRetryer{} }
		result, err := dynamodb.NewFromConfig(cfg).DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(physical)})
		if err != nil {
			return nil, err
		}
		if err := validateMRSCMembership(result.Table, regions, regions[i]); err != nil {
			return nil, err
		}
		backend, err := makeProvider(ctx, regions[i], "")
		if err != nil {
			return nil, err
		}
		if nilInterface(backend) {
			return nil, ErrInvalid
		}
		return backend.OpenKeyValueStore(ctx, c.RequestTable)
	}
	buckets := append([]regionalBucket{{Region: primary, Bucket: primaryBucket}}, f.PayloadReplicas...)
	blobs := &regionalBlobs{router: regional.Router{Count: len(buckets), Timeout: f.timeout}}
	blobs.open = func(ctx context.Context, i int) (blob.BlobStore, error) {
		backend, err := makeProvider(ctx, buckets[i].Region, buckets[i].Bucket)
		if err != nil {
			return nil, err
		}
		if nilInterface(backend) {
			return nil, ErrInvalid
		}
		return backend.OpenBlobStore(ctx, c.PayloadStore)
	}
	if err := table.probe(ctx); err != nil {
		return nil, err
	}
	if err := blobs.probe(ctx); err != nil {
		return nil, err
	}
	parentSnapshotBlob, err := c.parentSnapshotBlob()
	if err != nil {
		return nil, err
	}
	repository, err := NewRepository(Options{Table: table, Blobs: blobs, Namespace: c.Namespace, Secret: secret, ParentSnapshotBlob: parentSnapshotBlob})
	if err != nil {
		return nil, err
	}
	repository.probeStores = func(ctx context.Context) error {
		if err := table.probe(ctx); err != nil {
			return err
		}
		return blobs.probe(ctx)
	}
	return repository, nil
}

var _ kv.KeyValueStore = (*regionalTable)(nil)
var _ blob.BlobStore = (*regionalBlobs)(nil)

func validateMRSCMembership(table *ddbtypes.TableDescription, regions []string, current string) error {
	if table == nil || table.MultiRegionConsistency != ddbtypes.MultiRegionConsistencyStrong {
		return contracts.ErrUnsupported
	}
	members := map[string]bool{current: true}
	for _, replica := range table.Replicas {
		members[aws.ToString(replica.RegionName)] = true
	}
	for _, region := range regions {
		if !members[region] {
			return contracts.ErrUnsupported
		}
	}
	return nil
}

// ProbeRead needs one reachable bucket with read access. Unlike a real payload
// lookup, the deliberately absent readiness key need not exist in any replica.
func (s *regionalBlobs) ProbeRead(ctx context.Context, key blob.BlobKey) error {
	_, err := regional.Run(ctx, &s.router, regional.RetryRead, func(ctx context.Context, i int) (struct{}, error) {
		store, err := s.store(ctx, i)
		if err != nil {
			return struct{}{}, err
		}
		opened, err := store.Open(ctx, key)
		if errors.Is(err, contracts.ErrNotFound) {
			return struct{}{}, nil
		}
		if err != nil {
			return struct{}{}, err
		}
		if opened.Body == nil {
			return struct{}{}, ErrCorrupt
		}
		return struct{}{}, opened.Body.Close()
	})
	return err
}
