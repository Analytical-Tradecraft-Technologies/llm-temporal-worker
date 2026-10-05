package runtime

import (
	"context"
	"errors"
	"testing"

	blob "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/blob"
	"github.com/mfow/llm-temporal-worker/golang/activity"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"go.temporal.io/sdk/temporal"
)

// A completed, paid request whose stored payload is later lost (deletion,
// lifecycle rule, partial restore) must not read as a request that was never
// created. The same operation key fails as corrupt and never dispatches again.
func TestCloudLostRecordBlobIsCorruptNotANewOperation(t *testing.T) {
	f := boundedCloud(t, false)
	ctx := context.Background()
	v, err := f.runtime.GenerateStepV1(ctx, f.request)
	boundedState(t, v, err, llm.ExecutionProviderCompleted)
	ref := llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: f.request.Context}
	v, err = f.runtime.CompleteExecutionV1(ctx, ref)
	boundedState(t, v, err, llm.ExecutionCompleted)
	if f.submits.Load() != 1 {
		t.Fatalf("submits=%d", f.submits.Load())
	}

	f.blobs.mu.Lock()
	f.blobs.values = map[blob.BlobKey][]byte{}
	f.blobs.mu.Unlock()
	f.restart(t)

	requireCorrupt := func(name string, err error) {
		t.Helper()
		var mapped *provider.Error
		if !errors.As(err, &mapped) || mapped.Code != provider.CodeStateCorrupt || mapped.Retry != provider.RetryNever {
			t.Fatalf("%s after losing the record blob = %#v, want non-retryable state_corrupt", name, err)
		}
		var application *temporal.ApplicationError
		if !errors.As(activity.ToTemporalError(err), &application) || !application.NonRetryable() {
			t.Fatalf("%s: lost record blob is a retryable Activity error: %v", name, activity.ToTemporalError(err))
		}
	}
	_, err = f.runtime.PrepareExecutionV1(ctx, llm.PrepareExecutionV1{Generate: &f.request})
	requireCorrupt("prepare", err)
	_, err = f.runtime.GenerateStepV1(ctx, f.request)
	requireCorrupt("generate", err)
	_, err = f.runtime.PollExecutionV1(ctx, ref)
	requireCorrupt("poll", err)
	if f.submits.Load() != 1 {
		t.Fatalf("lost record blob dispatched the provider again: submits=%d", f.submits.Load())
	}
}
