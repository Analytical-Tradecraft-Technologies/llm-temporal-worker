package workflows

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/activity"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	sdkactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/durationpb"
)

// The export runs in its own abandoned workflow, carrying the request's
// reference, so the request workflow does not wait for Langfuse (#1351).
func TestLangfuseExportRunsInAbandonedChild(t *testing.T) {
	f := workflowTest(t, "generate", workflowStep{name: activity.PrepareActivityName, state: llm.ExecutionCompleted})
	var children []string
	f.env.SetOnChildWorkflowStartedListener(func(info *workflow.Info, _ workflow.Context, args converter.EncodedValues) {
		var ref llm.ExecutionReferenceV1
		if err := args.Get(&ref); err != nil || ref.Capability != testCapability || ref.RequestID != testRequestID {
			t.Errorf("export child input = %+v, %v", ref, err)
		}
		children = append(children, info.WorkflowType.Name)
	})
	exported := 0
	f.env.RegisterActivityWithOptions(func(context.Context, llm.ExecutionReferenceV1) error { exported++; return nil }, sdkactivity.RegisterOptions{Name: activity.ExportLangfuseActivityName})
	f.run(t)
	if !reflect.DeepEqual(children, []string{LangfuseExportWorkflowName}) || exported != 1 {
		t.Fatalf("children = %v, exports = %d", children, exported)
	}
}

// A history that scheduled the export Activity before the export moved to a
// child keeps running it inline.
func TestLangfuseInlineExportHistoryKeepsActivity(t *testing.T) {
	f := workflowTest(t, "generate", workflowStep{name: activity.PrepareActivityName, state: llm.ExecutionCompleted})
	f.env.OnGetVersion(asyncExportChange, workflow.DefaultVersion, workflow.Version(1)).Return(workflow.DefaultVersion)
	f.env.SetOnChildWorkflowStartedListener(func(info *workflow.Info, _ workflow.Context, _ converter.EncodedValues) {
		t.Errorf("started child %s", info.WorkflowType.Name)
	})
	exported := 0
	f.env.RegisterActivityWithOptions(func(context.Context, llm.ExecutionReferenceV1) error { exported++; return nil }, sdkactivity.RegisterOptions{Name: activity.ExportLangfuseActivityName})
	f.run(t)
	if exported != 1 {
		t.Fatalf("exports = %d", exported)
	}
}

// The export workflow reports a failed export as an unsuccessful result
// rather than a failed workflow, after the bounded attempts.
func TestLangfuseExportWorkflowSwallowsFailure(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	RegisterInternal(env, activity.PayloadLimits{})
	calls := 0
	env.RegisterActivityWithOptions(func(context.Context, llm.ExecutionReferenceV1) error {
		calls++
		return errors.New("sink unavailable")
	}, sdkactivity.RegisterOptions{Name: activity.ExportLangfuseActivityName})
	f := workflowTest(t, "generate")
	env.ExecuteWorkflow(LangfuseExportWorkflowName, llm.ExecutionReferenceV1{RequestID: testRequestID, Context: f.caller(), Capability: testCapability})
	var exported bool
	if err := env.GetWorkflowResult(&exported); err != nil || exported || calls != 10 {
		t.Fatalf("exported = %t, err = %v, calls = %d", exported, err, calls)
	}
}

// A saturated in-flight limit is retried after the Activity's backoff until a
// slot frees, rather than dropping the trace (#1459).
func TestLangfuseExportWorkflowRetriesSaturation(t *testing.T) {
	for _, tc := range []struct {
		name    string
		version workflow.Version
		calls   int
		want    bool
	}{{"current", 1, 6, true}, {"before retry change", workflow.DefaultVersion, 3, false}} {
		t.Run(tc.name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			RegisterInternal(env, activity.PayloadLimits{})
			env.OnGetVersion(exportRetryChange, workflow.DefaultVersion, workflow.Version(1)).Return(tc.version)
			calls := 0
			env.RegisterActivityWithOptions(func(context.Context, llm.ExecutionReferenceV1) error {
				calls++
				if calls < 6 {
					return temporal.NewApplicationErrorWithOptions("Langfuse export saturated", "langfuse_export_saturated", temporal.ApplicationErrorOptions{NextRetryDelay: 4 * time.Second})
				}
				return nil
			}, sdkactivity.RegisterOptions{Name: activity.ExportLangfuseActivityName})
			f := workflowTest(t, "generate")
			env.ExecuteWorkflow(LangfuseExportWorkflowName, llm.ExecutionReferenceV1{RequestID: testRequestID, Context: f.caller(), Capability: testCapability})
			var exported bool
			if err := env.GetWorkflowResult(&exported); err != nil || exported != tc.want || calls != tc.calls {
				t.Fatalf("exported = %t, err = %v, calls = %d", exported, err, calls)
			}
		})
	}
}

// completedRequestHistory starts a request workflow whose Preparation finds
// the response already saved, then lets add record the export commands.
func completedRequestHistory(t *testing.T, add func(*legacyHistory, llm.ExecutionReferenceV1)) *historypb.History {
	f := workflowTest(t, "generate")
	h := &legacyHistory{t: t}
	h.add(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
			WorkflowType: &commonpb.WorkflowType{Name: RequestWorkflowName}, TaskQueue: &taskqueuepb.TaskQueue{Name: "llm"}, Input: h.payloads(f.input),
			OriginalExecutionRunId: "run", WorkflowRunTimeout: durationpb.New(time.Hour), WorkflowTaskTimeout: durationpb.New(10 * time.Second)}}
	})
	completed := f.completed
	completed.Capability = testCapability
	h.activity(activity.PrepareActivityName, f.input, completed)
	h.workflowTask()
	h.marker("langfuse-export-v1")
	add(h, llm.ExecutionReferenceV1{RequestID: testRequestID, Context: f.caller(), Capability: testCapability})
	h.workflowTask()
	h.add(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{Result: h.payloads(f.completed)}}
	})
	return &historypb.History{Events: h.events}
}

func replayRequest(t *testing.T, history *historypb.History) {
	t.Helper()
	replayer := worker.NewWorkflowReplayer()
	RegisterInternal(replayer, activity.PayloadLimits{})
	if err := replayer.ReplayWorkflowHistory(nil, history); err != nil {
		t.Fatalf("history does not replay: %v", err)
	}
}

// The request workflow completes once the export child has started; its
// history never records the child finishing.
func TestAsyncExportHistoryCompletesBeforeChild(t *testing.T) {
	replayRequest(t, completedRequestHistory(t, func(h *legacyHistory, ref llm.ExecutionReferenceV1) {
		h.marker(asyncExportChange)
		// The SDK names a child after the run and the event ID its command takes.
		id := "run_" + strconv.Itoa(len(h.events)+1)
		initiated := h.add(enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED, func(e *historypb.HistoryEvent) {
			e.Attributes = &historypb.HistoryEvent_StartChildWorkflowExecutionInitiatedEventAttributes{StartChildWorkflowExecutionInitiatedEventAttributes: &historypb.StartChildWorkflowExecutionInitiatedEventAttributes{
				WorkflowId: id, WorkflowType: &commonpb.WorkflowType{Name: LangfuseExportWorkflowName}, TaskQueue: &taskqueuepb.TaskQueue{Name: "llm"}, Input: h.payloads(ref),
				ParentClosePolicy: enumspb.PARENT_CLOSE_POLICY_ABANDON}}
		})
		h.add(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED, func(e *historypb.HistoryEvent) {
			e.Attributes = &historypb.HistoryEvent_ChildWorkflowExecutionStartedEventAttributes{ChildWorkflowExecutionStartedEventAttributes: &historypb.ChildWorkflowExecutionStartedEventAttributes{
				InitiatedEventId: initiated, WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: id, RunId: "export-run"}, WorkflowType: &commonpb.WorkflowType{Name: LangfuseExportWorkflowName}}}
		})
	}))
}

// A request that ran the export Activity inline before the change replays
// unchanged.
func TestInlineExportHistoryReplays(t *testing.T) {
	replayRequest(t, completedRequestHistory(t, func(h *legacyHistory, ref llm.ExecutionReferenceV1) {
		h.taskActivity(activity.ExportLangfuseActivityName, ref, nil)
	}))
}
