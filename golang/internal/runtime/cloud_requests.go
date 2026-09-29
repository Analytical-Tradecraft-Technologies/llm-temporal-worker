package runtime

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/cache"
	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/state"
	"github.com/mfow/llm-temporal-worker/golang/storage/cloudstate"
)

// CloudRequestRepository is snapshot-owned. It records operations; it does not
// authorize paid attempts or settle budgets. The production factory also
// requires CloudCheckpointSource, CloudResponseCacheSource and CloudResponseFillSource when enabled.
type CloudRequestRepository interface {
	BeginOperation(context.Context, cloudstate.Operation) (cloudstate.Record, error)
	CompleteOperation(context.Context, cloudstate.Scope, cloudstate.RequestID, json.RawMessage, time.Time) (cloudstate.Record, error)
	Probe(context.Context) error
}

// CloudRequestFactory opens existing stores using IAM. The default repository
// owns no background process or closable client. Overrides obey that contract.
type CloudRequestFactory func(context.Context, cloudstate.Config, []byte) (CloudRequestRepository, error)

// CloudCheckpointSource supplies checkpoint persistence from the same opened
// cloud stores. Enabling cloud requests replaces the entire checkpoint bundle;
// it must never mix cloud rows with a PostgreSQL blob reader or materializer.
type CloudCheckpointSource interface {
	Checkpoints() state.CheckpointStore
}

// CloudResponseCacheSource binds cache publication/consumption to the same
// snapshot stores as checkpoints. It does not activate automatic cache reuse.
type CloudResponseCacheSource interface {
	Responses() cache.ResponseRepository
}

// CloudResponseFillSource supplies durable fill fencing from the same stores.
// It does not activate caching or authorize paid calls without Redis budget.
type CloudResponseFillSource interface {
	ResponseFills() cache.FillRepository
}

func cloudResponseFills(repository CloudRequestRepository) (cache.FillRepository, error) {
	source, ok := repository.(CloudResponseFillSource)
	if !ok {
		return nil, fmt.Errorf("%w: cloud response fill source is missing", ErrProductionFactoryInvalid)
	}
	store := source.ResponseFills()
	if isNilCapability(store) {
		return nil, fmt.Errorf("%w: cloud response fill store is nil", ErrProductionFactoryInvalid)
	}
	return snapshotResponseFills{delegate: store}, nil
}

type snapshotResponseFills struct{ delegate cache.FillRepository }

func (s snapshotResponseFills) Acquire(ctx context.Context, lease cache.FillLease) (cache.FillDecision, error) {
	return s.delegate.Acquire(ctx, lease)
}
func (s snapshotResponseFills) Start(ctx context.Context, lease cache.FillLease, now time.Time) (bool, error) {
	return s.delegate.Start(ctx, lease, now)
}
func (s snapshotResponseFills) Release(ctx context.Context, lease cache.FillLease, now time.Time) error {
	return s.delegate.Release(ctx, lease, now)
}
func (s snapshotResponseFills) Complete(ctx context.Context, lease cache.FillLease, completion cache.FillCompletion) error {
	return s.delegate.Complete(ctx, lease, completion)
}

func cloudResponseCache(repository CloudRequestRepository) (cache.ResponseRepository, error) {
	source, ok := repository.(CloudResponseCacheSource)
	if !ok {
		return nil, fmt.Errorf("%w: cloud response cache source is missing", ErrProductionFactoryInvalid)
	}
	store := source.Responses()
	if isNilCapability(store) {
		return nil, fmt.Errorf("%w: cloud response cache store is nil", ErrProductionFactoryInvalid)
	}
	return snapshotResponseCache{delegate: store}, nil
}

type snapshotResponseCache struct{ delegate cache.ResponseRepository }

func (s snapshotResponseCache) Publish(ctx context.Context, entry cache.ResponseEntry) error {
	return s.delegate.Publish(ctx, entry)
}
func (s snapshotResponseCache) Lookup(ctx context.Context, lookup cache.ResponseLookup) (*cache.ResponseEntry, error) {
	return s.delegate.Lookup(ctx, lookup)
}
func (s snapshotResponseCache) RecordUse(ctx context.Context, use cache.ResponseUse) error {
	return s.delegate.RecordUse(ctx, use)
}
func (s snapshotResponseCache) ReadUse(ctx context.Context, scope string, operation state.OperationID) (cache.ResponseUse, error) {
	return s.delegate.ReadUse(ctx, scope, operation)
}

func cloudCheckpointCapabilities(repository CloudRequestRepository, verifier state.CheckpointHandleVerifier, clock func() time.Time) (CheckpointCapabilities, error) {
	source, ok := repository.(CloudCheckpointSource)
	if !ok || isNilCapability(verifier) {
		return CheckpointCapabilities{}, fmt.Errorf("%w: cloud checkpoints require a store and handle verifier", ErrProductionFactoryInvalid)
	}
	store := source.Checkpoints()
	if isNilCapability(store) {
		return CheckpointCapabilities{}, fmt.Errorf("%w: cloud checkpoint store is nil", ErrProductionFactoryInvalid)
	}
	capabilities := CheckpointCapabilities{
		Repository: snapshotCheckpointRepository{delegate: store},
		Blobs:      snapshotCheckpointBlobReader{delegate: store},
		BlobWriter: snapshotCheckpointBlobWriter{delegate: store},
	}
	capabilities.Materializer = snapshotCheckpointMaterializer{delegate: &state.DurableCheckpointMaterializer{
		Repository: capabilities.Repository, Blobs: capabilities.Blobs, HandleVerifier: verifier, Now: clock,
	}}
	return capabilities, capabilities.RequireMaterializer()
}

func (factory *ProductionEngineFactory) buildCloudRequests(ctx context.Context, c *config.CloudRequestConfig) (CloudRequestRepository, error) {
	if c == nil {
		return nil, nil
	}
	encoded, err := factory.options.Resolver.Resolve(ctx, c.Secret)
	if err != nil {
		return nil, errors.New("resolve cloud request storage secret")
	}
	defer clear(encoded)
	key, err := base64.StdEncoding.Strict().DecodeString(string(encoded))
	defer clear(key)
	if err != nil || len(key) != 32 {
		return nil, errors.New("cloud request storage secret must be base64 encoding of 32 bytes")
	}
	providerJSON, err := json.Marshal(c.Provider)
	if err != nil {
		return nil, errors.New("encode cloud request storage configuration")
	}
	var parsed map[string]any
	if err := json.Unmarshal(providerJSON, &parsed); err != nil {
		return nil, errors.New("decode cloud request storage configuration")
	}
	open := factory.options.CloudRequestFactory
	if open == nil {
		open = func(ctx context.Context, c cloudstate.Config, key []byte) (CloudRequestRepository, error) {
			return cloudstate.Open(ctx, c, key)
		}
	}
	repository, err := open(ctx, cloudstate.Config{Provider: parsed, RequestTable: c.RequestTable, PayloadStore: c.PayloadStore, Namespace: c.Namespace}, key)
	// Provider SDK errors can contain resource names or endpoint details. Startup
	// errors deliberately report only this bounded classification.
	if err != nil || isNilCapability(repository) {
		return nil, fmt.Errorf("%w: open cloud request storage", ErrDependencyUnavailable)
	}
	return repository, nil
}

func cloudRequestProbe(repository CloudRequestRepository) DependencyProbe {
	return identifyDependencyProbe(DependencyCloudRequests, DependencyProbeFunc(func(ctx context.Context) ProbeResult {
		result := ProbeResult{Dependency: DependencyCloudRequests, Status: ProbeStatusReady, Reason: ProbeReasonReady}
		err := repository.Probe(ctx)
		if ctx.Err() != nil {
			err = ctx.Err()
		}
		if err != nil {
			result.Status, result.Reason = ProbeStatusUnavailable, ProbeReasonUnavailable
			if errors.Is(err, context.DeadlineExceeded) {
				result.Status, result.Reason = ProbeStatusTimeout, ProbeReasonTimeout
			}
		}
		return result
	}))
}
