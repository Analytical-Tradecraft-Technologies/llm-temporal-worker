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
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/durationpb"
)

func legacyPrepare(state llm.ExecutionStateV1) workflowStep {
	return workflowStep{name: activity.PrepareActivityName, state: state, alter: func(v *llm.ExecutionResultV1) { v.Capability = "" }}
}

// A Preparation recorded before reference steps required a capability returned
// none (#1362). The request prepares again before its next reference step and
// presents the issued capability from then on (the fixture rejects reference
// steps without it); the public result still carries none.
func TestExecuteRequestObtainsCapabilityForLegacyPreparation(t *testing.T) {
	for name, steps := range map[string][]workflowStep{
		"poll": {legacyPrepare(llm.ExecutionPending), step(activity.PrepareActivityName, llm.ExecutionPending),
			step(activity.PollActivityName, llm.ExecutionProviderCompleted), step(activity.CompleteActivityName, llm.ExecutionCompleted)},
		"complete": {legacyPrepare(llm.ExecutionProviderCompleted), step(activity.PrepareActivityName, llm.ExecutionProviderCompleted),
			step(activity.CompleteActivityName, llm.ExecutionCompleted)},
		"budget": {legacyPrepare(llm.ExecutionBudgetRequired), step(activity.PrepareActivityName, llm.ExecutionBudgetRequired),
			step(activity.AcquireBudgetActivityName, llm.ExecutionAcquired), step(activity.GenerateActivityName, llm.ExecutionProviderCompleted),
			step(activity.CompleteActivityName, llm.ExecutionCompleted)},
	} {
		t.Run(name, func(t *testing.T) {
			f := workflowTest(t, "generate", steps...)
			f.run(t)
			if f.calls[0] != activity.PrepareActivityName || f.calls[1] != activity.PrepareActivityName {
				t.Fatalf("calls = %v, want a second Preparation", f.calls)
			}
		})
	}
}

// A run that already has a capability never prepares again or records a
// version marker for the legacy path.
func TestExecuteRequestWithCapabilitySkipsLegacyPath(t *testing.T) {
	f := workflowTest(t, "generate", step(activity.PrepareActivityName, llm.ExecutionPending), step(activity.PollActivityName, llm.ExecutionProviderCompleted), step(activity.CompleteActivityName, llm.ExecutionCompleted))
	f.env.OnGetVersion(requestCapabilityChange+"1", workflow.DefaultVersion, workflow.Version(1)).Panic("legacy path taken")
	f.run(t)
}

// A budget child started before the change (no capability, so its parent's
// first check replays as DefaultVersion) hands the request back instead of
// acquiring. The parent then prepares again and starts a child that acquires
// with the capability.
func TestLegacyBudgetChildReturnsToParent(t *testing.T) {
	f := workflowTest(t, "generate", legacyPrepare(llm.ExecutionBudgetRequired), step(activity.PrepareActivityName, llm.ExecutionBudgetRequired),
		step(activity.AcquireBudgetActivityName, llm.ExecutionAcquired), step(activity.GenerateActivityName, llm.ExecutionProviderCompleted),
		step(activity.CompleteActivityName, llm.ExecutionCompleted))
	f.env.OnGetVersion(requestCapabilityChange+"1", workflow.DefaultVersion, workflow.Version(1)).Return(workflow.DefaultVersion)
	var children []string
	f.env.SetOnChildWorkflowStartedListener(func(_ *workflow.Info, _ workflow.Context, args converter.EncodedValues) {
		var request BudgetRequest
		if err := args.Get(&request); err != nil {
			t.Error(err)
		}
		children = append(children, request.Reference.Capability)
	})
	f.run(t)
	if !reflect.DeepEqual(children, []string{"", testCapability}) {
		t.Fatalf("child capabilities = %q, want a legacy child then one with the capability", children)
	}
	want := []string{activity.PrepareActivityName, activity.PrepareActivityName, activity.AcquireBudgetActivityName, activity.GenerateActivityName, activity.CompleteActivityName}
	if !reflect.DeepEqual(f.calls, want) {
		t.Fatalf("calls = %v, want %v", f.calls, want)
	}
}

// A directly started budget workflow has no parent to obtain a capability, so
// it still presents the reference to acquisition, which fails like an unknown
// request.
func TestDirectBudgetWorkflowWithoutCapabilityStillAcquires(t *testing.T) {
	f := workflowTest(t, "generate", step(activity.AcquireBudgetActivityName, llm.ExecutionAcquired))
	f.env.ExecuteWorkflow(BudgetWorkflowName, BudgetRequest{Reference: llm.ExecutionReferenceV1{RequestID: testRequestID, Context: f.caller()}, Kind: "generate"})
	// The fixture rejects a reference without the capability, as the runtime does.
	var failed *temporal.ActivityError
	if !errors.As(f.env.GetWorkflowError(), &failed) || failed.ActivityType().GetName() != activity.AcquireBudgetActivityName {
		t.Fatalf("error = %v, want the acquisition's failure", f.env.GetWorkflowError())
	}
}

// legacyHistory builds request workflow history as the code before #1362's
// capability recorded it: Preparation returned no capability, and a poll ran
// with a reference that carried none. The workflow then waits on a timer, as
// an in-flight request does when the new code is deployed.
type legacyHistory struct {
	t      *testing.T
	events []*historypb.HistoryEvent
}

func (h *legacyHistory) add(eventType enumspb.EventType, set func(*historypb.HistoryEvent)) int64 {
	event := &historypb.HistoryEvent{EventId: int64(len(h.events) + 1), EventType: eventType}
	if set != nil {
		set(event)
	}
	h.events = append(h.events, event)
	return event.EventId
}
func (h *legacyHistory) payloads(value any) *commonpb.Payloads {
	payloads, err := converter.GetDefaultDataConverter().ToPayloads(value)
	if err != nil {
		h.t.Fatal(err)
	}
	return payloads
}
func (h *legacyHistory) workflowTask() {
	scheduled := h.add(enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{TaskQueue: &taskqueuepb.TaskQueue{Name: "llm"}}}
	})
	started := h.add(enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{ScheduledEventId: scheduled}}
	})
	h.add(enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{ScheduledEventId: scheduled, StartedEventId: started}}
	})
}
func (h *legacyHistory) activity(name string, input any, result llm.ExecutionResultV1) {
	h.workflowTask()
	h.taskActivity(name, input, result)
}

// taskActivity schedules an activity in the workflow task just completed.
func (h *legacyHistory) taskActivity(name string, input any, result llm.ExecutionResultV1) {
	scheduled := h.add(enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED, nil)
	h.events[scheduled-1].Attributes = &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
		ActivityId: strconv.FormatInt(scheduled, 10), ActivityType: &commonpb.ActivityType{Name: name}, TaskQueue: &taskqueuepb.TaskQueue{Name: "llm"}, Input: h.payloads(input)}}
	started := h.add(enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{ScheduledEventId: scheduled}}
	})
	h.add(enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{ScheduledEventId: scheduled, StartedEventId: started, Result: h.payloads(result)}}
	})
}
func (h *legacyHistory) timer(seconds int, fired bool) {
	h.workflowTask()
	started := h.add(enumspb.EVENT_TYPE_TIMER_STARTED, nil)
	h.events[started-1].Attributes = &historypb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &historypb.TimerStartedEventAttributes{
		TimerId: strconv.FormatInt(started, 10), StartToFireTimeout: durationpb.New(time.Duration(seconds) * time.Second)}}
	if fired {
		h.add(enumspb.EVENT_TYPE_TIMER_FIRED, func(e *historypb.HistoryEvent) {
			e.Attributes = &historypb.HistoryEvent_TimerFiredEventAttributes{TimerFiredEventAttributes: &historypb.TimerFiredEventAttributes{TimerId: strconv.FormatInt(started, 10), StartedEventId: started}}
		})
	}
}

// marker records a GetVersion call that returned version 1, without the
// change-version search attribute upsert.
func (h *legacyHistory) marker(changeID string) {
	h.add(enumspb.EVENT_TYPE_MARKER_RECORDED, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_MarkerRecordedEventAttributes{MarkerRecordedEventAttributes: &historypb.MarkerRecordedEventAttributes{
			MarkerName: "Version", Details: map[string]*commonpb.Payloads{"change-id": h.payloads(changeID), "version": h.payloads(1), "version-search-attribute-updated": h.payloads(false)}}}
	})
}

// A request in flight when the capability was introduced: its history has a
// Preparation without a capability and a poll that ran without one, then a
// timer the new code sees fire. The new code replays the old part unchanged,
// then prepares again at the first reference step past it (the second check)
// and presents the capability to the rest. The re-preparation result is a
// completed response, so code that consumed it as the poll result would
// complete early and diverge from the history.
func TestLegacyCapabilityHistoryReplays(t *testing.T) {
	f := workflowTest(t, "generate")
	h := &legacyHistory{t: t}
	h.add(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
			WorkflowType: &commonpb.WorkflowType{Name: RequestWorkflowName}, TaskQueue: &taskqueuepb.TaskQueue{Name: "llm"}, Input: h.payloads(f.input),
			WorkflowRunTimeout: durationpb.New(time.Hour), WorkflowTaskTimeout: durationpb.New(10 * time.Second)}}
	})
	pending := llm.ExecutionResultV1{RequestID: testRequestID, Kind: "generate", State: llm.ExecutionPending, RetryAfterSeconds: 3}
	legacyRef := llm.ExecutionReferenceV1{RequestID: testRequestID, Context: f.caller()}
	h.activity(activity.PrepareActivityName, f.input, pending)
	h.timer(3, true)
	h.activity(activity.PollActivityName, legacyRef, pending)
	h.timer(3, true)
	// The new code runs from here.
	h.workflowTask()
	h.marker(requestCapabilityChange + "2")
	completed := f.completed
	completed.Capability = testCapability
	h.taskActivity(activity.PrepareActivityName, f.input, completed)
	ref := legacyRef
	ref.Capability = testCapability
	providerCompleted := llm.ExecutionResultV1{RequestID: testRequestID, Kind: "generate", State: llm.ExecutionProviderCompleted, Capability: testCapability}
	h.activity(activity.PollActivityName, ref, providerCompleted)
	h.activity(activity.CompleteActivityName, ref, completed)
	h.workflowTask()
	h.add(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, func(e *historypb.HistoryEvent) {
		e.Attributes = &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{Result: h.payloads(f.completed)}}
	})

	replayer := worker.NewWorkflowReplayer()
	RegisterInternal(replayer, activity.PayloadLimits{})
	if err := replayer.ReplayWorkflowHistory(nil, &historypb.History{Events: h.events}); err != nil {
		t.Fatalf("legacy history does not replay: %v", err)
	}
}

// A legacy run that completes recovers the capability for the best-effort
// export under the export's bounded retries: a failing Preparation skips the
// export after three attempts instead of delaying the response.
func TestLegacyExportCapabilityRecoveryIsBounded(t *testing.T) {
	failure := temporal.NewApplicationError("storage temporarily unavailable", activity.ErrorTypeProviderTransient)
	f := workflowTest(t, "generate", legacyPrepare(llm.ExecutionCompleted),
		workflowStep{name: activity.PrepareActivityName, err: failure}, workflowStep{name: activity.PrepareActivityName, err: failure}, workflowStep{name: activity.PrepareActivityName, err: failure})
	exports := 0
	f.env.RegisterActivityWithOptions(func(context.Context, llm.ExecutionReferenceV1) error { exports++; return nil }, sdkactivity.RegisterOptions{Name: activity.ExportLangfuseActivityName})
	f.run(t)
	if exports != 0 || len(f.calls) != 4 {
		t.Fatalf("exports = %d, calls = %v", exports, f.calls)
	}
}
