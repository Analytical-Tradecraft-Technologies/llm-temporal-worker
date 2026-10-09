package runtime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/activity"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/workflows"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"
)

const generationRecoveryMaxEvents = 20000
const generationRecoveryMaxHistoryBytes = 32 << 20

var errGenerationRecoveryHistory = errors.New("generation recovery history is invalid")

type generationRecoveryProof struct {
	original  llm.GenerateRequestV1
	effective llm.GenerateRequestV1
}

// Only the authenticated client's iterator supplies production history. The
// parser is deliberately private: an exported JSON history is not authority
// to restore an original public binding.
func readGenerationRecoveryHistory(ctx context.Context, history client.HistoryEventIterator, dc converter.DataConverter, namespace string) (generationRecoveryProof, error) {
	if ctx == nil || history == nil || dc == nil || namespace == "" {
		return generationRecoveryProof{}, errGenerationRecoveryHistory
	}
	var events []*historypb.HistoryEvent
	var total int
	for history.HasNext() {
		if err := ctx.Err(); err != nil {
			return generationRecoveryProof{}, err
		}
		event, err := history.Next()
		if err != nil {
			return generationRecoveryProof{}, errors.New("generation recovery history could not be read")
		}
		if event == nil || len(events) >= generationRecoveryMaxEvents {
			return generationRecoveryProof{}, errGenerationRecoveryHistory
		}
		size := proto.Size(event)
		if size > generationRecoveryMaxHistoryBytes-total {
			return generationRecoveryProof{}, errGenerationRecoveryHistory
		}
		total += size
		events = append(events, event)
	}
	if err := ctx.Err(); err != nil {
		return generationRecoveryProof{}, err
	}
	return parseGenerationRecoveryHistory(events, dc, namespace)
}

func recoveryDecode(dc converter.DataConverter, payloads *commonpb.Payloads, destination any) bool {
	return payloads != nil && len(payloads.Payloads) == 1 && dc.FromPayloads(payloads, destination) == nil
}

func recoveryJSONEqual(a, b any) bool {
	x, err := json.Marshal(a)
	if err != nil {
		return false
	}
	y, err := json.Marshal(b)
	return err == nil && bytes.Equal(x, y)
}

type recoveryChild struct {
	initiated     int64
	started       int64
	id, run, kind string
	completed     bool
}

func parseGenerationRecoveryHistory(events []*historypb.HistoryEvent, dc converter.DataConverter, namespace string) (generationRecoveryProof, error) {
	fail := func() (generationRecoveryProof, error) {
		return generationRecoveryProof{}, errGenerationRecoveryHistory
	}
	if len(events) == 0 || dc == nil || namespace == "" {
		return fail()
	}
	var proof generationRecoveryProof
	var scheduled, started, planned int64
	var compactRequest llm.CompactRequestV1
	var compactResult llm.CompactResponseV1
	children := map[int64]*recoveryChild{}
	var compact, request *recoveryChild
	sameNamespace := func(value string) bool { return value == "" || value == namespace }
	for i, event := range events {
		if event == nil || event.EventId != int64(i+1) {
			return fail()
		}
		switch event.EventType {
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED:
			a := event.GetWorkflowExecutionStartedEventAttributes()
			if i != 0 || a == nil || a.GetWorkflowType().GetName() != workflows.GenerateWorkflowName || !recoveryDecode(dc, a.Input, &proof.original) || proof.original.Parent == nil {
				return fail()
			}
			encoded, err := proof.original.MarshalJSON()
			if err != nil {
				return fail()
			}
			digest := sha256.Sum256(encoded)
			compactRequest = llm.CompactRequestV1{OperationKey: "llmtw_compact_" + hex.EncodeToString(digest[:]), Context: proof.original.Context, Parent: *proof.original.Parent, Cache: &llm.CachePolicyV1{}}
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
			a := event.GetActivityTaskScheduledEventAttributes()
			var input llm.GenerateRequestV1
			if i == 0 || a == nil || scheduled != 0 || a.GetActivityType().GetName() != activity.PlanGenerationActivityName || !recoveryDecode(dc, a.Input, &input) || !recoveryJSONEqual(input, proof.original) {
				return fail()
			}
			scheduled = event.EventId
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED:
			a := event.GetActivityTaskStartedEventAttributes()
			if a == nil || scheduled == 0 || started != 0 || a.ScheduledEventId != scheduled {
				return fail()
			}
			started = event.EventId
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
			a := event.GetActivityTaskCompletedEventAttributes()
			var plan llm.GenerationPlanV1
			if a == nil || started == 0 || planned != 0 || a.ScheduledEventId != scheduled || a.StartedEventId != started || !recoveryDecode(dc, a.Result, &plan) || !plan.CompactBeforeGenerate || plan.EffectiveParent != nil {
				return fail()
			}
			planned = event.EventId
		case enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED:
			a := event.GetStartChildWorkflowExecutionInitiatedEventAttributes()
			if a == nil || planned == 0 || a.WorkflowId == "" || !sameNamespace(a.Namespace) {
				return fail()
			}
			child := &recoveryChild{initiated: event.EventId, id: a.WorkflowId, kind: a.GetWorkflowType().GetName()}
			switch child.kind {
			case workflows.CompactWorkflowName:
				var input llm.CompactRequestV1
				if compact != nil || request != nil || !recoveryDecode(dc, a.Input, &input) || !recoveryJSONEqual(input, compactRequest) {
					return fail()
				}
				compact = child
			case workflows.RequestWorkflowName:
				var input llm.PrepareExecutionV1
				if compact == nil || !compact.completed || request != nil || !recoveryDecode(dc, a.Input, &input) || input.Generate == nil || input.Compact != nil || input.OriginalGenerate != nil {
					return fail()
				}
				proof.effective = *input.Generate
				if !samePublicGeneration(proof.original, proof.effective) || proof.effective.Parent == nil || *proof.effective.Parent != compactResult.Checkpoint.Handle {
					return fail()
				}
				request = child
			default:
				return fail()
			}
			children[event.EventId] = child
		case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED:
			a := event.GetChildWorkflowExecutionStartedEventAttributes()
			if a == nil {
				return fail()
			}
			child := children[a.InitiatedEventId]
			if child == nil || child.started != 0 || !sameNamespace(a.Namespace) || a.GetWorkflowType().GetName() != child.kind || a.GetWorkflowExecution().GetWorkflowId() != child.id || a.GetWorkflowExecution().GetRunId() == "" {
				return fail()
			}
			child.started, child.run = event.EventId, a.GetWorkflowExecution().GetRunId()
		case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED:
			a := event.GetChildWorkflowExecutionCompletedEventAttributes()
			if a == nil {
				return fail()
			}
			child := children[a.InitiatedEventId]
			if child == nil || child.started == 0 || child.completed || a.StartedEventId != child.started || !sameNamespace(a.Namespace) || a.GetWorkflowType().GetName() != child.kind || a.GetWorkflowExecution().GetWorkflowId() != child.id || a.GetWorkflowExecution().GetRunId() != child.run {
				return fail()
			}
			if child == compact {
				if !recoveryDecode(dc, a.Result, &compactResult) || compactResult.OperationKey != compactRequest.OperationKey || compactResult.Checkpoint.Handle == "" {
					return fail()
				}
			}
			child.completed = true
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED, enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT, enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED, enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED:
			return fail()
		default:
			if i == 0 {
				return fail()
			}
		}
	}
	if planned == 0 || compact == nil || !compact.completed || request == nil || request.started == 0 {
		return fail()
	}
	return proof, nil
}
