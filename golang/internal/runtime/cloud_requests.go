package runtime

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/state"
	"github.com/mfow/llm-temporal-worker/golang/storage/cloudstate"
)

// CloudRequestRepository is snapshot-owned. It records operations; it does not
// authorize paid attempts or settle budgets. The production factory also
// requires CloudCheckpointSource when this storage is enabled.
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
