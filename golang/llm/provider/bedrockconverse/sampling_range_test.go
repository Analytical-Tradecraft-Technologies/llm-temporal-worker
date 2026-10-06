package bedrockconverse

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

func TestLowerRequestRejectsTemperatureAboveOne(t *testing.T) {
	temperature := 1.5
	request := llm.Request{OperationKey: "op", Model: "nova", Sampling: &llm.SamplingSpec{Temperature: &temperature}, Input: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "hi"}}}}}
	if _, err := lowerRequest(request, DefaultProfile("nova"), string(types.ServiceTierTypeDefault), true); err == nil {
		t.Fatal("temperature 1.5 was accepted")
	}
	temperature = 1
	if _, err := lowerRequest(request, DefaultProfile("nova"), string(types.ServiceTierTypeDefault), true); err != nil {
		t.Fatalf("temperature 1 rejected: %v", err)
	}
}
