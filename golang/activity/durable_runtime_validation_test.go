package activity

import (
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/durable"
)

func TestNewDurableV1RuntimeRejectsIncompleteGeneratePorts(t *testing.T) {
	_, err := NewDurableV1Runtime(durable.GeneratePorts{}, durable.CompactPorts{}, nil)
	if err == nil {
		t.Fatal("NewDurableV1Runtime accepted incomplete phase ports")
	}
}

func TestDurableV1RuntimeValidateRejectsNilRuntime(t *testing.T) {
	var runtime *DurableV1Runtime
	if err := runtime.Validate(); err == nil {
		t.Fatal("nil DurableV1Runtime validated successfully")
	}
}

func TestNewDurableV1RuntimeAllowsIndependentQueryComposition(t *testing.T) {
	generate := recordingGeneratePorts(new([]string), validGenerateV1Request(), durable.RoutePlan{})
	compact := recordingCompactPorts(new([]string), validCompactV1Request(), durable.RoutePlan{})
	runtime, err := NewDurableV1Runtime(generate, compact, nil)
	if err != nil {
		t.Fatalf("NewDurableV1Runtime() error = %v", err)
	}
	if runtime.Query != nil {
		t.Fatal("runtime unexpectedly installed a Query callback")
	}
}
