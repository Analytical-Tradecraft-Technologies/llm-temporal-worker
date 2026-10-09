package runtime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/activity"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/workflows"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"
)

func generationRecoveryHistoryFixture(t *testing.T) ([]*historypb.HistoryEvent, converter.DataConverter, generationRecoveryProof) {
	t.Helper()
	f := boundedCloud(t, false)
	parent := f.finish(t).Generate.Checkpoint.Handle
	original := f.request
	original.OperationKey = "historical-child"
	original.Parent = &parent
	effective := original
	newParent := llm.CheckpointHandle("recovered-compaction")
	effective.Parent = &newParent
	value := codecConfig(codecKey("test", "KEY", true))
	value.Server.InlinePayloadBytes = 1 << 20
	dc, err := (DefaultTemporalClientFactory{SecretResolver: codecSecrets(map[string][]byte{"KEY": bytes.Repeat([]byte{4}, 32)})}).dataConverter(context.Background(), value)
	if err != nil {
		t.Fatal(err)
	}
	payload := func(v any) *commonpb.Payloads {
		p, err := dc.ToPayloads(v)
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	encoded, _ := original.MarshalJSON()
	digest := sha256.Sum256(encoded)
	compact := llm.CompactRequestV1{OperationKey: "llmtw_compact_" + hex.EncodeToString(digest[:]), Context: original.Context, Parent: parent, Cache: &llm.CachePolicyV1{}}
	zero := "0"
	result := llm.CompactResponseV1{APIVersion: llm.CompactAPIVersion, OperationKey: compact.OperationKey, OperationID: "compact-operation", Checkpoint: llm.CheckpointMetadata{Handle: newParent, Parent: &parent, Kind: "compaction"}, Cache: llm.CacheDispositionV1{Disposition: "disabled"}, Cost: llm.CostV1{Status: "exact", ActualCostUSD: &zero, Method: "provider_reported"}}
	compactExecution := &commonpb.WorkflowExecution{WorkflowId: "compact-child", RunId: "compact-run"}
	requestExecution := &commonpb.WorkflowExecution{WorkflowId: "request-child", RunId: "request-run"}
	events := []*historypb.HistoryEvent{
		{EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{WorkflowType: &commonpb.WorkflowType{Name: workflows.GenerateWorkflowName}, Input: payload(original)}}},
		{EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED, Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{ActivityType: &commonpb.ActivityType{Name: activity.PlanGenerationActivityName}, Input: payload(original)}}},
		{EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED, Attributes: &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{ScheduledEventId: 2}}},
		{EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED, Attributes: &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{ScheduledEventId: 2, StartedEventId: 3, Result: payload(llm.GenerationPlanV1{CompactBeforeGenerate: true})}}},
		{EventType: enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED, Attributes: &historypb.HistoryEvent_StartChildWorkflowExecutionInitiatedEventAttributes{StartChildWorkflowExecutionInitiatedEventAttributes: &historypb.StartChildWorkflowExecutionInitiatedEventAttributes{WorkflowId: compactExecution.WorkflowId, WorkflowType: &commonpb.WorkflowType{Name: workflows.CompactWorkflowName}, Input: payload(compact)}}},
		{EventType: enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED, Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionStartedEventAttributes{ChildWorkflowExecutionStartedEventAttributes: &historypb.ChildWorkflowExecutionStartedEventAttributes{InitiatedEventId: 5, WorkflowExecution: compactExecution, WorkflowType: &commonpb.WorkflowType{Name: workflows.CompactWorkflowName}}}},
		{EventType: enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED, Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionCompletedEventAttributes{ChildWorkflowExecutionCompletedEventAttributes: &historypb.ChildWorkflowExecutionCompletedEventAttributes{InitiatedEventId: 5, StartedEventId: 6, WorkflowExecution: compactExecution, WorkflowType: &commonpb.WorkflowType{Name: workflows.CompactWorkflowName}, Result: payload(result)}}},
		{EventType: enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED, Attributes: &historypb.HistoryEvent_StartChildWorkflowExecutionInitiatedEventAttributes{StartChildWorkflowExecutionInitiatedEventAttributes: &historypb.StartChildWorkflowExecutionInitiatedEventAttributes{WorkflowId: requestExecution.WorkflowId, WorkflowType: &commonpb.WorkflowType{Name: workflows.RequestWorkflowName}, Input: payload(llm.PrepareExecutionV1{Generate: &effective})}}},
		{EventType: enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED, Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionStartedEventAttributes{ChildWorkflowExecutionStartedEventAttributes: &historypb.ChildWorkflowExecutionStartedEventAttributes{InitiatedEventId: 8, WorkflowExecution: requestExecution, WorkflowType: &commonpb.WorkflowType{Name: workflows.RequestWorkflowName}}}},
	}
	for i, event := range events {
		event.EventId = int64(i + 1)
	}
	return events, dc, generationRecoveryProof{original: original, effective: effective}
}

func TestGenerationRecoveryHistoryRequiresCorrelatedEncryptedProvenance(t *testing.T) {
	events, dc, want := generationRecoveryHistoryFixture(t)
	got, err := parseGenerationRecoveryHistory(events, dc, "test")
	if err != nil || !recoveryJSONEqual(got.original, want.original) || !recoveryJSONEqual(got.effective, want.effective) {
		t.Fatalf("valid history: %v", err)
	}
	if _, err := parseGenerationRecoveryHistory(events, converter.GetDefaultDataConverter(), "test"); err == nil {
		t.Fatal("encrypted history accepted without configured codec")
	}
	mutations := map[string]func([]*historypb.HistoryEvent){
		"root type": func(e []*historypb.HistoryEvent) {
			e[0].GetWorkflowExecutionStartedEventAttributes().WorkflowType.Name = "other"
		},
		"event gap":        func(e []*historypb.HistoryEvent) { e[2].EventId++ },
		"plan association": func(e []*historypb.HistoryEvent) { e[3].GetActivityTaskCompletedEventAttributes().ScheduledEventId = 1 },
		"compact association": func(e []*historypb.HistoryEvent) {
			e[6].GetChildWorkflowExecutionCompletedEventAttributes().StartedEventId = 5
		},
		"compact run": func(e []*historypb.HistoryEvent) {
			e[6].GetChildWorkflowExecutionCompletedEventAttributes().WorkflowExecution.RunId = "other"
		},
		"namespace": func(e []*historypb.HistoryEvent) {
			e[4].GetStartChildWorkflowExecutionInitiatedEventAttributes().Namespace = "other"
		},
		"request association": func(e []*historypb.HistoryEvent) {
			e[8].GetChildWorkflowExecutionStartedEventAttributes().InitiatedEventId = 5
		},
		"codec corruption": func(e []*historypb.HistoryEvent) {
			e[0].GetWorkflowExecutionStartedEventAttributes().Input.Payloads[0].Data = []byte("private-invalid")
		},
		"effective input": func(e []*historypb.HistoryEvent) {
			v := want.effective
			v.OperationKey = "changed"
			p, _ := dc.ToPayloads(llm.PrepareExecutionV1{Generate: &v})
			e[7].GetStartChildWorkflowExecutionInitiatedEventAttributes().Input = p
		},
		"effective parent": func(e []*historypb.HistoryEvent) {
			v := want.effective
			h := llm.CheckpointHandle("different")
			v.Parent = &h
			p, _ := dc.ToPayloads(llm.PrepareExecutionV1{Generate: &v})
			e[7].GetStartChildWorkflowExecutionInitiatedEventAttributes().Input = p
		},
		"false plan": func(e []*historypb.HistoryEvent) {
			p, _ := dc.ToPayloads(llm.GenerationPlanV1{})
			e[3].GetActivityTaskCompletedEventAttributes().Result = p
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			copyEvents := make([]*historypb.HistoryEvent, len(events))
			for i, e := range events {
				copyEvents[i] = proto.Clone(e).(*historypb.HistoryEvent)
			}
			mutate(copyEvents)
			if _, err := parseGenerationRecoveryHistory(copyEvents, dc, "test"); !errors.Is(err, errGenerationRecoveryHistory) {
				t.Fatalf("invalid history accepted: %v", err)
			}
		})
	}
	for length := 0; length < len(events); length++ {
		if _, err := parseGenerationRecoveryHistory(events[:length], dc, "test"); err == nil {
			t.Fatalf("partial history accepted at %d", length)
		}
	}
}

type recoveryHistoryIterator struct {
	events []*historypb.HistoryEvent
	err    error
}

func (i *recoveryHistoryIterator) HasNext() bool { return len(i.events) > 0 || i.err != nil }
func (i *recoveryHistoryIterator) Next() (*historypb.HistoryEvent, error) {
	if i.err != nil {
		return nil, i.err
	}
	e := i.events[0]
	i.events = i.events[1:]
	return e, nil
}

func TestReadGenerationRecoveryHistoryFailsClosed(t *testing.T) {
	events, dc, _ := generationRecoveryHistoryFixture(t)
	if _, err := readGenerationRecoveryHistory(context.Background(), &recoveryHistoryIterator{events: events}, dc, "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := readGenerationRecoveryHistory(context.Background(), &recoveryHistoryIterator{err: errors.New("private transport detail")}, dc, "test"); err == nil || err.Error() != "generation recovery history could not be read" {
		t.Fatalf("unsafe read error: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := readGenerationRecoveryHistory(ctx, &recoveryHistoryIterator{events: events}, dc, "test"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel ignored: %v", err)
	}
	oversized := &historypb.HistoryEvent{Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{Input: &commonpb.Payloads{Payloads: []*commonpb.Payload{{Data: make([]byte, generationRecoveryMaxHistoryBytes+1)}}}}}}
	if _, err := readGenerationRecoveryHistory(context.Background(), &recoveryHistoryIterator{events: []*historypb.HistoryEvent{oversized}}, dc, "test"); !errors.Is(err, errGenerationRecoveryHistory) {
		t.Fatalf("history bound ignored: %v", err)
	}
}
