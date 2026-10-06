package activity

import (
	"fmt"
	"strings"

	sdkworker "go.temporal.io/sdk/worker"
)

// V1ActivityDescriptor is the immutable wire contract advertised by one v1
// Activity registration. Temporal binds a worker registry to one task queue;
// carrying that queue alongside the name prevents documentation and tooling
// from accidentally describing an Activity on a different queue.
//
// InputType and OutputType are documentation/inspection labels, not a second
// serializer. The registered handlers accept a raw payload and decode it
// strictly into these types inside the Activity (see decodeV1ActivityInput).
type V1ActivityDescriptor struct {
	TaskQueue  string
	Name       string
	InputType  string
	OutputType string
}

const (
	generateV1InputType   = "llm.GenerateRequestV1"
	generateV1OutputType  = "llm.ExecutionResultV1"
	compactV1InputType    = "llm.CompactRequestV1"
	compactV1OutputType   = "llm.ExecutionResultV1"
	prepareV1InputType    = "llm.PrepareExecutionV1"
	referenceV1InputType  = "llm.ExecutionReferenceV1"
	executionV1OutputType = "llm.ExecutionResultV1"
	queryV1InputType      = "llm.QueryRequestV1"
	queryV1OutputType     = "llm.QueryResponseV1"
	planV1OutputType      = "llm.GenerationPlanV1"
)

// V1ActivityDescriptors returns the nine bounded Activities exposed
// by a production v1 worker. The returned slice is newly allocated and can be
// safely retained by a registry/introspection endpoint.
func V1ActivityDescriptors(taskQueue string) ([]V1ActivityDescriptor, error) {
	if taskQueue == "" || taskQueue != strings.TrimSpace(taskQueue) {
		return nil, fmt.Errorf("v1 Activity task queue is required")
	}
	if len(taskQueue) > 255 || strings.ContainsAny(taskQueue, "\x00\r\n\t ") {
		return nil, fmt.Errorf("v1 Activity task queue is invalid")
	}
	descriptors := []V1ActivityDescriptor{
		{TaskQueue: taskQueue, Name: GenerateActivityName, InputType: generateV1InputType, OutputType: generateV1OutputType},
		{TaskQueue: taskQueue, Name: CompactActivityName, InputType: compactV1InputType, OutputType: compactV1OutputType},
		{TaskQueue: taskQueue, Name: QueryActivityName, InputType: queryV1InputType, OutputType: queryV1OutputType},
		{TaskQueue: taskQueue, Name: PrepareActivityName, InputType: prepareV1InputType, OutputType: executionV1OutputType},
		{TaskQueue: taskQueue, Name: AcquireBudgetActivityName, InputType: referenceV1InputType, OutputType: executionV1OutputType},
		{TaskQueue: taskQueue, Name: PollActivityName, InputType: referenceV1InputType, OutputType: executionV1OutputType},
		{TaskQueue: taskQueue, Name: CompleteActivityName, InputType: referenceV1InputType, OutputType: executionV1OutputType},
		{TaskQueue: taskQueue, Name: PlanGenerationActivityName, InputType: generateV1InputType, OutputType: planV1OutputType},
		{TaskQueue: taskQueue, Name: ExportLangfuseActivityName, InputType: referenceV1InputType, OutputType: "none"},
	}
	for _, descriptor := range descriptors {
		if err := descriptor.Validate(); err != nil {
			return nil, err
		}
	}
	return descriptors, nil
}

// Validate checks the closed descriptor vocabulary. It intentionally rejects
// token/stream names: v1 is a one-shot Temporal boundary.
func (descriptor V1ActivityDescriptor) Validate() error {
	if descriptor.TaskQueue == "" || len(descriptor.TaskQueue) > 255 || strings.ContainsAny(descriptor.TaskQueue, "\x00\r\n\t ") {
		return fmt.Errorf("v1 Activity descriptor has an invalid task queue")
	}
	switch descriptor.Name {
	case ExportLangfuseActivityName:
		if descriptor.InputType != referenceV1InputType || descriptor.OutputType != "none" {
			return fmt.Errorf("Langfuse export Activity descriptor types are invalid")
		}
	case GenerateActivityName:
		if descriptor.InputType != generateV1InputType || descriptor.OutputType != generateV1OutputType {
			return fmt.Errorf("Generate v1 Activity descriptor types are invalid")
		}
	case CompactActivityName:
		if descriptor.InputType != compactV1InputType || descriptor.OutputType != compactV1OutputType {
			return fmt.Errorf("Compact v1 Activity descriptor types are invalid")
		}
	case QueryActivityName:
		if descriptor.InputType != queryV1InputType || descriptor.OutputType != queryV1OutputType {
			return fmt.Errorf("Query v1 Activity descriptor types are invalid")
		}
	case PlanGenerationActivityName:
		if descriptor.InputType != generateV1InputType || descriptor.OutputType != planV1OutputType {
			return fmt.Errorf("generation plan Activity descriptor types are invalid")
		}
	case PrepareActivityName:
		if descriptor.InputType != prepareV1InputType || descriptor.OutputType != executionV1OutputType {
			return fmt.Errorf("Prepare v1 Activity descriptor types are invalid")
		}
	case AcquireBudgetActivityName, PollActivityName, CompleteActivityName:
		if descriptor.InputType != referenceV1InputType || descriptor.OutputType != executionV1OutputType {
			return fmt.Errorf("execution v1 Activity descriptor types are invalid")
		}
	default:
		return fmt.Errorf("unknown v1 Activity name %q", descriptor.Name)
	}
	return nil
}

// RegisterForTaskQueue validates the worker's task-queue contract before
// registering either the v1 set or the legacy development-only Generate
// helper. The Temporal SDK registry itself is queue-agnostic; NewWorker binds
// it to the validated queue when constructing the worker.
func (activities *Activities) RegisterForTaskQueue(registry sdkworker.ActivityRegistry, taskQueue string) error {
	if _, err := V1ActivityDescriptors(taskQueue); err != nil {
		return err
	}
	if registry == nil {
		return fmt.Errorf("Temporal Activity registry is required")
	}
	if activities == nil {
		return fmt.Errorf("Activity implementation is required")
	}
	activities.Register(registry)
	return nil
}
