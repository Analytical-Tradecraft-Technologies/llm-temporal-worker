package bedrockconverse

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"

	"github.com/mfow/llm-temporal-worker/golang/compaction"
	"github.com/mfow/llm-temporal-worker/golang/llm"
)

func TestCompactionRequestCompilesInStrictModeWithApplicationInstructions(t *testing.T) {
	input := []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "hello"}}}}
	source := llm.Request{OperationKey: "generate", Model: "claude-test", Input: input, Instructions: []llm.Instruction{
		{Kind: llm.InstructionKindText, Level: llm.InstructionLevelApplication, Text: "You are a helpful assistant"},
	}}
	request, err := compaction.PrepareRequest(source, "generate/compact", input, compaction.DefaultPolicy())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lowerRequest(request, DefaultProfile("nova"), string(types.ServiceTierTypeDefault), true); err != nil {
		t.Fatalf("strict compaction request lowering error = %v", err)
	}
}
