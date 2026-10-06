package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	blob "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/blob"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/activity"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"go.temporal.io/sdk/temporal"
)

// failNextFillRead makes the next Open of a blob referenced by a cache-fill
// head fail once as a transient storage outage. The object itself is intact.
func (f *boundedCloudFixture) failNextFillRead(t *testing.T) *atomic.Int32 {
	t.Helper()
	var fired atomic.Int32
	f.blobs.open = func(key blob.BlobKey) error {
		if fired.Load() != 0 {
			return nil
		}
		f.table.mu.Lock()
		defer f.table.mu.Unlock()
		for row, record := range f.table.rows {
			if !strings.Contains(row.PartitionKey, "/cache/fill/") {
				continue
			}
			var pointer struct {
				Blob string `json:"blob"`
			}
			data, ok := record.Item.Fields["cache"].(kv.KeyValueBytes)
			if ok && json.Unmarshal(data, &pointer) == nil && blob.BlobKey(pointer.Blob) == key {
				fired.Add(1)
				return &contracts.StorageError{Kind: contracts.ErrUnavailable, Operation: "blob.open"}
			}
		}
		return nil
	}
	return &fired
}

func requireRetryableStateUnavailable(t *testing.T, fired *atomic.Int32, err error) {
	t.Helper()
	if fired.Load() != 1 {
		t.Fatalf("fill record reads failed = %d, want 1 (error %v)", fired.Load(), err)
	}
	var mapped *provider.Error
	if !errors.As(err, &mapped) || mapped.Code != provider.CodeStateUnavailable || mapped.Retry != provider.RetrySameOperation {
		t.Fatalf("transient fill record read = %#v, want retryable state_unavailable", err)
	}
	var application *temporal.ApplicationError
	if !errors.As(activity.ToTemporalError(err), &application) || application.NonRetryable() {
		t.Fatalf("transient fill record read became a non-retryable Activity error: %v", activity.ToTemporalError(err))
	}
}

func TestCloudCompleteTransientFillReadIsRetryable(t *testing.T) {
	f := boundedCloud(t, false)
	f.request.Cache = &llm.CachePolicyV1{}
	ctx := context.Background()
	v, err := f.runtime.GenerateStepV1(ctx, f.request)
	boundedState(t, v, err, llm.ExecutionProviderCompleted)
	ref := llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: f.request.Context}
	fired := f.failNextFillRead(t)
	_, err = f.runtime.CompleteExecutionV1(ctx, ref)
	requireRetryableStateUnavailable(t, fired, err)
	v, err = f.runtime.CompleteExecutionV1(ctx, ref)
	boundedState(t, v, err, llm.ExecutionCompleted)
	second := f.request
	second.OperationKey = "second"
	v, err = f.runtime.GenerateStepV1(ctx, second)
	boundedState(t, v, err, llm.ExecutionCompleted)
	if v.Generate.Cache.Disposition != "hit" || f.submits.Load() != 1 {
		t.Fatalf("second caller after retried completion: disposition=%s submits=%d", v.Generate.Cache.Disposition, f.submits.Load())
	}
}

func TestCloudTerminalFillTransientFillReadIsRetryable(t *testing.T) {
	f := boundedCloud(t, false)
	f.request.Cache = &llm.CachePolicyV1{}
	var fired *atomic.Int32
	f.adapter.invoke = func(ctx context.Context, call provider.Call, o provider.Observer) (provider.Result, error) {
		f.submits.Add(1)
		// The fill is started by now, so the next read is its terminal completion.
		fired = f.failNextFillRead(t)
		return provider.Result{}, provider.NewError(provider.CodeProviderRateLimited, provider.PhaseDispatch, provider.DispatchRejected, provider.RetryAfter, "rate limited")
	}
	ctx := context.Background()
	_, err := f.runtime.GenerateStepV1(ctx, f.request)
	requireRetryableStateUnavailable(t, fired, err)
	v, err := f.runtime.GenerateStepV1(ctx, f.request)
	boundedState(t, v, err, llm.ExecutionFailed)
	if f.submits.Load() != 1 {
		t.Fatalf("retry dispatched again: submits=%d", f.submits.Load())
	}
	// The fill is closed: another caller on the same cache key owns a new fill
	// instead of waiting on the failed one.
	second := f.request
	second.OperationKey = "second"
	f.now = f.now.Add(time.Minute)
	v, err = f.runtime.PrepareExecutionV1(ctx, llm.PrepareExecutionV1{Generate: &second})
	boundedState(t, v, err, llm.ExecutionBudgetRequired)
}

func TestCloudUnknownFillTransientFillReadIsRetryable(t *testing.T) {
	f := boundedCloud(t, false)
	f.request.Cache = &llm.CachePolicyV1{}
	original := f.adapter.invoke
	f.adapter.invoke = func(ctx context.Context, call provider.Call, o provider.Observer) (provider.Result, error) {
		f.submits.Add(1)
		if err := o.BeforePossibleWrite(ctx); err != nil {
			return provider.Result{}, err
		}
		return provider.Result{}, errors.New("lost paid response")
	}
	ctx := context.Background()
	v, err := f.runtime.GenerateStepV1(ctx, f.request)
	boundedState(t, v, err, llm.ExecutionPending)
	ref := llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: f.request.Context}
	f.now = f.now.Add(16 * time.Minute)
	f.restart(t)
	v, err = f.runtime.PollExecutionV1(ctx, ref)
	boundedState(t, v, err, llm.ExecutionOutcomeUnknown)
	fired := f.failNextFillRead(t)
	_, err = f.runtime.AcquireBudgetV1(ctx, ref)
	requireRetryableStateUnavailable(t, fired, err)
	v, err = f.runtime.AcquireBudgetV1(ctx, ref)
	boundedState(t, v, err, llm.ExecutionAcquired)
	f.adapter.invoke = original
	v, err = f.runtime.GenerateStepV1(ctx, f.request)
	boundedState(t, v, err, llm.ExecutionProviderCompleted)
	v, err = f.runtime.CompleteExecutionV1(ctx, ref)
	boundedState(t, v, err, llm.ExecutionCompleted)
	second := f.request
	second.OperationKey = "second"
	v, err = f.runtime.GenerateStepV1(ctx, second)
	boundedState(t, v, err, llm.ExecutionCompleted)
	if v.Generate.Cache.Disposition != "hit" || f.submits.Load() != 2 {
		t.Fatalf("second caller after recovered unknown fill: disposition=%s submits=%d", v.Generate.Cache.Disposition, f.submits.Load())
	}
}
