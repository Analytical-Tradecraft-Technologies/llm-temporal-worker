package runtime

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/workflows"
	"github.com/google/uuid"
)

// A request's public operation ID is only a locator (#1362). A reference step
// with an authorized scope and the request ID but without the capability
// preparation issued fails exactly like an unknown request, and never returns
// the stored response or its checkpoint handle.
func TestCloudReferenceStepsRequireCapability(t *testing.T) {
	f := boundedCloud(t, false)
	root := f.finish(t)
	if root.Generate == nil || root.Generate.OperationID != root.RequestID || root.Capability == "" {
		t.Fatalf("completed request = %+v", root)
	}
	if strings.Contains(string(root.Generate.Checkpoint.Handle), root.Capability) {
		t.Fatal("public response carries the reference capability")
	}
	ctx := context.Background()
	// The baseline is an unknown request that presents a genuine capability
	// for its own ID, so its lookup really reaches the store.
	unknownID := "llmtw_req_" + uuid.NewString()
	unknownCapability, err := f.options.Keyring.IssueReferenceCapability("tenant", "project", unknownID)
	if err != nil {
		t.Fatal(err)
	}
	unknown := llm.ExecutionReferenceV1{RequestID: unknownID, Context: f.request.Context, Capability: unknownCapability}
	otherCapability, err := f.options.Keyring.IssueReferenceCapability("tenant", "other-project", root.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	steps := map[string]func(llm.ExecutionReferenceV1) (llm.ExecutionResultV1, error){
		"acquire": func(ref llm.ExecutionReferenceV1) (llm.ExecutionResultV1, error) {
			return f.runtime.AcquireBudgetV1(ctx, ref)
		},
		"poll": func(ref llm.ExecutionReferenceV1) (llm.ExecutionResultV1, error) {
			return f.runtime.PollExecutionV1(ctx, ref)
		},
		"complete": func(ref llm.ExecutionReferenceV1) (llm.ExecutionResultV1, error) {
			return f.runtime.CompleteExecutionV1(ctx, ref)
		},
	}
	for name, step := range steps {
		t.Run(name, func(t *testing.T) {
			_, want := step(unknown)
			if want == nil {
				t.Fatal("unknown request was accepted")
			}
			for label, capability := range map[string]string{
				"missing":       "",
				"forged":        "ref_v1.test.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
				"other-request": unknownCapability,
				"other-scope":   otherCapability,
			} {
				got, err := step(llm.ExecutionReferenceV1{RequestID: root.RequestID, Context: f.request.Context, Capability: capability})
				if !reflect.DeepEqual(err, want) || !reflect.DeepEqual(got, llm.ExecutionResultV1{}) {
					t.Fatalf("%s capability: result=%+v error=%v, want unknown-request error %v", label, got, err, want)
				}
			}
			got, err := step(llm.ExecutionReferenceV1{RequestID: root.RequestID, Context: f.request.Context, Capability: root.Capability})
			if err != nil || got.State != llm.ExecutionCompleted || got.Generate == nil || got.Generate.Checkpoint.Handle != root.Generate.Checkpoint.Handle {
				t.Fatalf("issued capability: result=%+v error=%v", got, err)
			}
		})
	}
}

// Any namespace client can start llm.budget.wait.v1. Starting it with only a
// completed request's ID fails like an unknown request instead of returning
// the saved response.
func TestBudgetWorkflowWithoutCapabilityFailsLikeUnknownRequest(t *testing.T) {
	f := boundedCloud(t, false)
	root := f.finish(t)
	run := func(ref llm.ExecutionReferenceV1) (llm.ExecutionResultV1, error) {
		env := publicReplayEnvironment(f)
		env.ExecuteWorkflow(workflows.BudgetWorkflowName, workflows.BudgetRequest{Reference: ref, Kind: "generate"})
		var result llm.ExecutionResultV1
		if err := env.GetWorkflowError(); err != nil {
			return result, err
		}
		err := env.GetWorkflowResult(&result)
		return result, err
	}
	_, want := run(llm.ExecutionReferenceV1{RequestID: "llmtw_req_" + uuid.NewString(), Context: f.request.Context})
	if want == nil {
		t.Fatal("unknown request was accepted")
	}
	got, err := run(llm.ExecutionReferenceV1{RequestID: root.RequestID, Context: f.request.Context})
	if err == nil || err.Error() != want.Error() || got.Generate != nil {
		t.Fatalf("result=%+v error=%v, want unknown-request error %v", got, err, want)
	}
	got, err = run(llm.ExecutionReferenceV1{RequestID: root.RequestID, Context: f.request.Context, Capability: root.Capability})
	if err != nil || got.State != llm.ExecutionCompleted || got.Capability != "" {
		t.Fatalf("issued capability: result=%+v error=%v", got, err)
	}
}
