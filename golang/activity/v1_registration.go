package activity

import (
	"errors"
	"fmt"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"strings"

	sdkworker "go.temporal.io/sdk/worker"
)

// V1ActivityDescriptor is the immutable wire contract advertised by one v1
// Activity registration. Temporal binds a worker registry to one task queue;
// carrying that queue alongside the name prevents documentation and tooling
// from accidentally describing an Activity on a different queue.
//
// InputType and OutputType are documentation/inspection labels, not a second
// serializer. The registered function signatures remain the source of truth
// for Temporal's payload codec.
type V1ActivityDescriptor struct {
	TaskQueue  string
	Name       string
	InputType  string
	OutputType string
}

const (
	generateV1InputType                 = "llm.GenerateRequestV1"
	generateV1OutputType                = "llm.GenerateResponseV1"
	compactV1InputType                  = "llm.CompactRequestV1"
	compactV1OutputType                 = "llm.CompactResponseV1"
	queryV1InputType                    = "llm.QueryRequestV1"
	queryV1OutputType                   = "llm.QueryResponseV1"
	reserveBatchV1InputType             = "llm.ReserveBatchRequestV1"
	reserveBatchV1OutputType            = "llm.ReserveBatchResponseV1"
	allocateBatchGrantsV1InputType      = "llm.AllocateBatchGrantsRequestV1"
	allocateBatchGrantsV1OutputType     = "llm.AllocateBatchGrantsResponseV1"
	closeBatchV1InputType               = "llm.CloseBatchRequestV1"
	closeBatchV1OutputType              = "llm.CloseBatchResponseV1"
	resourceCapacityAcquireV1InputType  = "llm.ResourceCapacityAcquireRequestV1"
	resourceCapacityLeaseV1OutputType   = "llm.ResourceCapacityLeaseV1"
	resourceCapacityRenewV1InputType    = "llm.ResourceCapacityRenewRequestV1"
	resourceCapacityReleaseV1InputType  = "llm.ResourceCapacityReleaseRequestV1"
	resourceCapacityReleaseV1OutputType = "llm.ResourceCapacityReleaseResponseV1"
)

// V1ActivityDescriptors returns the exact nine one-shot Activities exposed
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
		{TaskQueue: taskQueue, Name: ReserveBatchActivityName, InputType: reserveBatchV1InputType, OutputType: reserveBatchV1OutputType},
		{TaskQueue: taskQueue, Name: llm.AllocateBatchGrantsActivityName, InputType: allocateBatchGrantsV1InputType, OutputType: allocateBatchGrantsV1OutputType},
		{TaskQueue: taskQueue, Name: llm.CloseBatchActivityName, InputType: closeBatchV1InputType, OutputType: closeBatchV1OutputType},
		{TaskQueue: taskQueue, Name: llm.ResourceCapacityAcquireActivityName, InputType: resourceCapacityAcquireV1InputType, OutputType: resourceCapacityLeaseV1OutputType},
		{TaskQueue: taskQueue, Name: llm.ResourceCapacityRenewActivityName, InputType: resourceCapacityRenewV1InputType, OutputType: resourceCapacityLeaseV1OutputType},
		{TaskQueue: taskQueue, Name: llm.ResourceCapacityReleaseActivityName, InputType: resourceCapacityReleaseV1InputType, OutputType: resourceCapacityReleaseV1OutputType},
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
	case ReserveBatchActivityName:
		if descriptor.InputType != reserveBatchV1InputType || descriptor.OutputType != reserveBatchV1OutputType {
			return fmt.Errorf("ReserveBatch v1 Activity descriptor types are invalid")
		}
	case llm.AllocateBatchGrantsActivityName:
		if descriptor.InputType != allocateBatchGrantsV1InputType || descriptor.OutputType != allocateBatchGrantsV1OutputType {
			return fmt.Errorf("AllocateBatchGrants v1 Activity descriptor types are invalid")
		}
	case llm.CloseBatchActivityName:
		if descriptor.InputType != closeBatchV1InputType || descriptor.OutputType != closeBatchV1OutputType {
			return fmt.Errorf("CloseBatch v1 Activity descriptor types are invalid")
		}
	case llm.ResourceCapacityAcquireActivityName:
		if descriptor.InputType != resourceCapacityAcquireV1InputType || descriptor.OutputType != resourceCapacityLeaseV1OutputType {
			return errors.New("AcquireResourceCapacity v1 Activity descriptor types are invalid")
		}
	case llm.ResourceCapacityRenewActivityName:
		if descriptor.InputType != resourceCapacityRenewV1InputType || descriptor.OutputType != resourceCapacityLeaseV1OutputType {
			return errors.New("RenewResourceCapacity v1 Activity descriptor types are invalid")
		}
	case llm.ResourceCapacityReleaseActivityName:
		if descriptor.InputType != resourceCapacityReleaseV1InputType || descriptor.OutputType != resourceCapacityReleaseV1OutputType {
			return errors.New("ReleaseResourceCapacity v1 Activity descriptor types are invalid")
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
