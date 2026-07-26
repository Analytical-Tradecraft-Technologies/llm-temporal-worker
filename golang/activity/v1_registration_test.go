package activity

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

func TestV1ActivityDescriptorsAreClosedAndTaskQueueBound(t *testing.T) {
	descriptors, err := V1ActivityDescriptors("llm-inference")
	if err != nil {
		t.Fatal(err)
	}
	want := []V1ActivityDescriptor{
		{TaskQueue: "llm-inference", Name: GenerateActivityName, InputType: generateV1InputType, OutputType: generateV1OutputType},
		{TaskQueue: "llm-inference", Name: CompactActivityName, InputType: compactV1InputType, OutputType: compactV1OutputType},
		{TaskQueue: "llm-inference", Name: QueryActivityName, InputType: queryV1InputType, OutputType: queryV1OutputType},
	}
	if !reflect.DeepEqual(descriptors, want) {
		t.Fatalf("descriptors = %#v, want %#v", descriptors, want)
	}
	for _, descriptor := range descriptors {
		if err := descriptor.Validate(); err != nil {
			t.Fatalf("descriptor %#v is invalid: %v", descriptor, err)
		}
	}
}

func TestV1ActivityDescriptorsRejectUnsafeTaskQueues(t *testing.T) {
	for _, taskQueue := range []string{"", " queue", "queue ", "queue\nname", strings.Repeat("q", 256)} {
		t.Run(taskQueue, func(t *testing.T) {
			if _, err := V1ActivityDescriptors(taskQueue); err == nil {
				t.Fatalf("V1ActivityDescriptors(%q) unexpectedly succeeded", taskQueue)
			}
		})
	}
}

func TestRegisterForTaskQueueUsesTheV1SetWhenConfigured(t *testing.T) {
	registry := &v1Registry{}
	activities := &Activities{V1Runtime: &v1RuntimeStub{}}
	if err := activities.RegisterForTaskQueue(registry, "queue-a"); err != nil {
		t.Fatal(err)
	}
	want := []string{GenerateActivityName, CompactActivityName, QueryActivityName}
	if !reflect.DeepEqual(registry.names, want) {
		t.Fatalf("registered names = %v, want %v", registry.names, want)
	}
}

func TestDurableV1RuntimeQueryRejectsMismatchedResponse(t *testing.T) {
	runtime := &DurableV1Runtime{Query: func(context.Context, llm.QueryRequestV1) (llm.QueryResponseV1, error) {
		return llm.QueryResponseV1{APIVersion: llm.QueryAPIVersion, OperationKey: "other", Kind: llm.QueryProviderStatus}, nil
	}}
	_, err := runtime.QueryV1(context.Background(), validQueryV1Request())
	if err == nil || !strings.Contains(err.Error(), "identity") {
		t.Fatalf("QueryV1 error = %v, want identity mismatch", err)
	}
}

func TestDurableV1RuntimeQueryFailsClosedWithoutCallback(t *testing.T) {
	_, err := (&DurableV1Runtime{}).QueryV1(context.Background(), validQueryV1Request())
	if err == nil {
		t.Fatal("QueryV1 unexpectedly succeeded without a callback")
	}
	var providerErr *provider.Error
	if !errors.As(err, &providerErr) || providerErr.Code != provider.CodeConfiguration || !strings.Contains(err.Error(), "not configured") {
		t.Fatalf("QueryV1 error = %v, want configuration failure", err)
	}
}
