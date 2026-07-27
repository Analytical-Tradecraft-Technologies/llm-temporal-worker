package durable

import (
	"context"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm"
)

func TestRequiredPortRejectsTypedNilFunction(t *testing.T) {
	var replay func(context.Context, llm.GenerateRequestV1) (GenerateReplay, error)
	if err := requiredPort("replay", replay); err == nil {
		t.Fatal("requiredPort accepted a typed nil function")
	}
}

func TestGeneratePortsValidateRejectsTypedNilFunction(t *testing.T) {
	var replay func(context.Context, llm.GenerateRequestV1) (GenerateReplay, error)
	ports := GeneratePorts{Replay: replay}
	if err := ports.Validate(); err == nil {
		t.Fatal("GeneratePorts.Validate accepted a typed nil callback")
	}
}

func TestCompactPortsValidateRejectsTypedNilFunction(t *testing.T) {
	var replay func(context.Context, llm.CompactRequestV1) (CompactReplay, error)
	ports := CompactPorts{Replay: replay}
	if err := ports.Validate(); err == nil {
		t.Fatal("CompactPorts.Validate accepted a typed nil callback")
	}
}
