package activity

import (
	"context"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"go.temporal.io/sdk/testsuite"
	"testing"
)

type pollRuntimeStub struct {
	v1RuntimeStub
	calls int
}

func (p *pollRuntimeStub) PollV1(_ context.Context, r llm.PollRequestV1) (llm.PollResponseV1, error) {
	p.calls++
	return llm.PollResponseV1{Status: "pending", Pending: r.Pending}, nil
}
func TestPollActivitySingleInvocation(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	runtime := &pollRuntimeStub{}
	activities := &Activities{V1Runtime: runtime}
	env.RegisterActivity(activities.PollV1)
	req := llm.PollRequestV1{Context: llm.RequestContext{Tenant: "t", Project: "p", Actor: "a"}, Pending: llm.PendingOperationV1{OperationID: "op", Kind: "generate", Provider: "openai", EndpointID: "endpoint", ProviderOperationID: "resp_1"}}
	value, err := env.ExecuteActivity(activities.PollV1, req)
	if err != nil {
		t.Fatal(err)
	}
	var response llm.PollResponseV1
	if err = value.Get(&response); err != nil || response.Status != "pending" || runtime.calls != 1 {
		t.Fatalf("poll: %v %v calls=%d", response, err, runtime.calls)
	}
}
