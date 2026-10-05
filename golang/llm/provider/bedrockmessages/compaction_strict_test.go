package bedrockmessages

import (
	"testing"

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
	if _, err := lowerRequestWithStrict(request, DefaultProfile("bedrock"), "", true); err != nil {
		t.Fatalf("strict compaction request lowering error = %v", err)
	}
}
