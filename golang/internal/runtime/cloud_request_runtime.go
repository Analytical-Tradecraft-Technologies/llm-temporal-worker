package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/mfow/llm-temporal-worker/golang/activity"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/storage/cloudstate"
)

// cloudRequestRuntime records the outer operation boundary. The inner runtime
// MUST remain idempotent for OperationKey, including concurrent calls and a
// crash after provider submission. Running records are not dispatch leases.
// Completed results and committed finalization handoffs are replayed here.
// Handoffs resume only publication/settlement, never the inner provider phases.
// Errors stay pending for recovery; this layer never refunds or retries
// a provider call and never converts unknown paid work into a free attempt.
type cloudRequestRuntime struct {
	inner               activity.V1Runtime
	requests            CloudRequestRepository
	finalizer           *CloudFinalizer
	clock               func() time.Time
	finalizationTimeout time.Duration
}

type cloudRuntimeResult struct {
	Version  int             `json:"version"`
	Response json.RawMessage `json:"response"`
}

func (r *cloudRequestRuntime) GenerateV1(ctx context.Context, request llm.GenerateRequestV1) (llm.GenerateResponseV1, error) {
	var response llm.GenerateResponseV1
	input, err := json.Marshal(request)
	if err != nil {
		return response, cloudRuntimeError(cloudstate.ErrInvalid, false)
	}
	index := int64(0)
	if request.Cache != nil {
		index = int64(request.Cache.Variant)
	}
	data, err := r.execute(ctx, request.Context, "generate", request.OperationKey, index, input, func(ctx context.Context) ([]byte, error) {
		value, err := r.inner.GenerateV1(ctx, request)
		if err != nil {
			return nil, err
		}
		if value.OperationKey != request.OperationKey {
			return nil, cloudRuntimeError(cloudstate.ErrCorrupt, true)
		}
		data, err := json.Marshal(value)
		if err != nil {
			return nil, cloudRuntimeError(cloudstate.ErrCorrupt, true)
		}
		return data, nil
	})
	if err != nil {
		return response, err
	}
	if json.Unmarshal(data, &response) != nil || response.OperationKey != request.OperationKey {
		return llm.GenerateResponseV1{}, cloudRuntimeError(cloudstate.ErrCorrupt, true)
	}
	return response, nil
}

func (r *cloudRequestRuntime) CompactV1(ctx context.Context, request llm.CompactRequestV1) (llm.CompactResponseV1, error) {
	var response llm.CompactResponseV1
	input, err := json.Marshal(request)
	if err != nil {
		return response, cloudRuntimeError(cloudstate.ErrInvalid, false)
	}
	data, err := r.execute(ctx, request.Context, "compact", request.OperationKey, 0, input, func(ctx context.Context) ([]byte, error) {
		value, err := r.inner.CompactV1(ctx, request)
		if err != nil {
			return nil, err
		}
		if value.OperationKey != request.OperationKey {
			return nil, cloudRuntimeError(cloudstate.ErrCorrupt, true)
		}
		data, err := json.Marshal(value)
		if err != nil {
			return nil, cloudRuntimeError(cloudstate.ErrCorrupt, true)
		}
		return data, nil
	})
	if err != nil {
		return response, err
	}
	if json.Unmarshal(data, &response) != nil || response.OperationKey != request.OperationKey {
		return llm.CompactResponseV1{}, cloudRuntimeError(cloudstate.ErrCorrupt, true)
	}
	return response, nil
}

func (r *cloudRequestRuntime) QueryV1(ctx context.Context, request llm.QueryRequestV1) (llm.QueryResponseV1, error) {
	return r.inner.QueryV1(ctx, request)
}

func (r *cloudRequestRuntime) execute(ctx context.Context, caller llm.RequestContext, kind, key string, index int64, input json.RawMessage, run func(context.Context) ([]byte, error)) ([]byte, error) {
	if ctx == nil {
		return nil, cloudRuntimeError(cloudstate.ErrInvalid, false)
	}
	scope := cloudstate.Scope{Tenant: caller.Tenant, Project: caller.Project}
	record, err := r.requests.BeginOperation(ctx, cloudstate.Operation{Scope: scope, Kind: kind, Key: key, RequestIndex: index, Manifest: input, Now: r.clock()})
	if err != nil {
		return nil, cloudRuntimeError(err, false)
	}
	if record.Status == cloudstate.StatusCompleted {
		var stored cloudRuntimeResult
		if json.Unmarshal(record.Progress, &stored) != nil || stored.Version != 1 || len(stored.Response) == 0 {
			return nil, cloudRuntimeError(cloudstate.ErrCorrupt, true)
		}
		return stored.Response, nil
	}
	// Other states belong to future submit/poll workflow orchestration and must
	// not accidentally be resumed through this one-shot activity composition.
	if record.Status != cloudstate.StatusRunning {
		return nil, cloudRuntimeError(contracts.ErrConflict, false)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var response []byte
	if r.finalizer != nil {
		var found bool
		response, found, err = r.finalizer.replay(ctx, scope, record, kind, key, index)
		if err != nil {
			return nil, err
		}
		if !found {
			response, err = run(ctx)
		}
	} else {
		// A configured handoff must never fall back to earlier phases.
		var progress map[string]json.RawMessage
		_ = json.Unmarshal(record.Progress, &progress)
		if _, exists := progress["finalization_handoff"]; exists {
			return nil, cloudRuntimeError(cloudstate.ErrCorrupt, true)
		}
		response, err = run(ctx)
	}
	if err != nil {
		return nil, err
	}
	progress, err := json.Marshal(cloudRuntimeResult{Version: 1, Response: response})
	if err != nil {
		return nil, cloudRuntimeError(cloudstate.ErrCorrupt, true)
	}
	// Preserve a returned paid result even if the caller's context just ended.
	// This bounded finalization does not introduce a cancellation API.
	finalize, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.finalizationTimeout)
	defer cancel()
	if _, err := r.requests.CompleteOperation(finalize, scope, record.Request.ID, progress, r.clock()); err != nil {
		return nil, cloudRuntimeError(err, true)
	}
	return response, nil
}

func cloudRuntimeError(cause error, executed bool) error {
	phase, certainty := provider.PhaseStateLoad, provider.DispatchNotDispatched
	if executed {
		phase, certainty = provider.PhaseFinalize, provider.DispatchAccepted
	}
	code, retry := provider.CodeStateUnavailable, provider.RetrySameOperation
	if errors.Is(cause, contracts.ErrConflict) {
		code, retry = provider.CodeOperationConflict, provider.RetryNever
	}
	if errors.Is(cause, cloudstate.ErrInvalid) {
		code, retry = provider.CodeInvalidArgument, provider.RetryNever
	}
	if errors.Is(cause, cloudstate.ErrCorrupt) {
		code, retry = provider.CodeStateCorrupt, provider.RetryNever
	}
	// Keep raw SDK errors out of the Activity's serialized error contract.
	return provider.NewError(code, phase, certainty, retry, "durable request storage operation failed")
}
