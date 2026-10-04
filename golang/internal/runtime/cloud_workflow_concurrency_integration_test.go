//go:build cloudworkflowintegration

package runtime

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/storage/cloudstate"
	"github.com/mfow/llm-temporal-worker/golang/workflows"
	"go.temporal.io/sdk/client"
)

// Independent public workflow executions must converge on the same durable
// operation, even when two workers receive their activities concurrently.
func TestCloudWorkflowLiveConcurrentReplay(t *testing.T) {
	const callers = 100
	h := newLiveCloudWorkflow(t, true, false)
	request := h.fixture.request
	request.Cache = &llm.CachePolicyV1{}
	requests := make([]llm.GenerateRequestV1, callers)
	for i := range requests {
		requests[i] = request
	}
	runs := startConcurrentCloudGenerations(t, h, "replay", requests)
	h.startWorker(t)
	h.startWorker(t)
	select {
	case <-h.pending:
	case <-h.ctx.Done():
		t.Fatal("concurrent callers did not reach pending provider work")
	}
	h.complete.Store(true)
	var original llm.GenerateResponseV1
	for i, run := range runs {
		var result llm.GenerateResponseV1
		if err := run.Get(h.ctx, &result); err != nil {
			t.Fatalf("replay caller %d: %v", i, err)
		}
		if i == 0 {
			original = result
		}
		if result.OperationKey != request.OperationKey || result.Checkpoint.Handle == "" || !reflect.DeepEqual(result, original) {
			t.Fatalf("replay caller %d did not receive the original completed response", i)
		}
	}
	if got := h.fixture.submits.Load(); got != 1 {
		t.Fatalf("concurrent replay submitted %d paid requests, want one", got)
	}

	// One hundred different operations can consume the same successful cache
	// entry concurrently, but each must receive its own checkpoint and identity.
	for i := range requests {
		requests[i].OperationKey = fmt.Sprintf("cached-caller-%d", i)
	}
	runs = startConcurrentCloudGenerations(t, h, "cache", requests)
	operations, checkpoints := map[string]bool{}, map[llm.CheckpointHandle]bool{}
	for i, run := range runs {
		var result llm.GenerateResponseV1
		if err := run.Get(h.ctx, &result); err != nil {
			t.Fatalf("cache caller %d: %v", i, err)
		}
		if result.OperationKey != requests[i].OperationKey || result.Cache.Disposition != "hit" || result.OperationID == original.OperationID || result.OperationID == "" || result.Checkpoint.Handle == original.Checkpoint.Handle || result.Checkpoint.Handle == "" {
			t.Fatalf("cache caller %d lost its identity or cached result", i)
		}
		if operations[result.OperationID] || checkpoints[result.Checkpoint.Handle] {
			t.Fatalf("cache caller %d reused another caller's publication", i)
		}
		operations[result.OperationID], checkpoints[result.Checkpoint.Handle] = true, true
	}
	if got := h.fixture.submits.Load(); got != 1 {
		t.Fatalf("concurrent cache consumption submitted %d paid requests, want one", got)
	}
	h.assertSettled(t, 1)
	for shard := range cloudstate.PendingShards {
		page, err := h.fixture.repository.ListPending(h.ctx, shard, 100, "")
		if err != nil || len(page.Requests) != 0 || page.NextPageToken != "" {
			t.Fatalf("concurrent completion left pending work in shard %d: %v", shard, err)
		}
	}
}

func startConcurrentCloudGenerations(t *testing.T, h *liveCloudWorkflow, phase string, requests []llm.GenerateRequestV1) []client.WorkflowRun {
	t.Helper()
	runs := make([]client.WorkflowRun, len(requests))
	errors := make([]error, len(requests))
	start := make(chan struct{})
	var group sync.WaitGroup
	for i := range requests {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			runs[i], errors[i] = h.client.ExecuteWorkflow(h.ctx, client.StartWorkflowOptions{
				ID: fmt.Sprintf("%s-%s-%d", h.queue, phase, i), TaskQueue: h.queue,
				WorkflowExecutionTimeout: 2 * time.Minute,
			}, workflows.GenerateWorkflowName, requests[i])
		}()
	}
	close(start)
	group.Wait()
	for i, err := range errors {
		if err != nil {
			t.Fatalf("start %s caller %d: %v", phase, i, err)
		}
	}
	return runs
}
