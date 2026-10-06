package compaction

import (
	"embed"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/mfow/llm-temporal-worker/golang/llm"
)

//go:embed prompt/v1.txt
var promptFiles embed.FS

// Prompt returns the immutable repository-owned generic compaction prompt.
func Prompt(version string) (string, error) {
	if version != PromptVersion {
		return "", fmt.Errorf("unsupported generic compaction prompt version %q", version)
	}
	value, err := promptFiles.ReadFile("prompt/v1.txt")
	if err != nil {
		return "", fmt.Errorf("read generic compaction prompt: %w", err)
	}
	return string(value), nil
}

// PrepareRequest constructs the isolated summarizer call.  It copies the
// caller's routing and sampling settings, but never mutates the caller and
// always strips application tools, tool policy, continuation, reasoning, and
// structured output. It injects the versioned repository prompt and selected
// summary style as policy instructions. The prefix is not replayed as
// provider turns: it is rendered as text into one delimited human message
// that ends with the summarize instruction and the length budget. The
// returned request can therefore only ask for bounded plain text.
func PrepareRequest(source llm.Request, operationKey string, input []llm.Item, policy Policy) (llm.Request, error) {
	if operationKey == "" {
		return llm.Request{}, errors.New("compaction operation key is required")
	}
	if err := policy.Validate(); err != nil {
		return llm.Request{}, err
	}
	prompt, err := Prompt(policy.PromptVersion)
	if err != nil {
		return llm.Request{}, err
	}
	if len(input) == 0 {
		return llm.Request{}, errors.New("compaction input must not be empty")
	}
	maxTokens := policy.OutputReserveTokens
	result := source
	result.OperationKey = operationKey
	result.Input = summarizerInput(input, policy)
	result.Instructions = make([]llm.Instruction, 0, len(source.Instructions)+2)
	result.Instructions = append(result.Instructions,
		llm.Instruction{Kind: llm.InstructionKindText, Level: llm.InstructionLevelPolicy, Text: prompt},
		llm.Instruction{Kind: llm.InstructionKindText, Level: llm.InstructionLevelPolicy, Text: "Summary style: " + string(policy.SummaryStyle)},
	)
	result.Instructions = append(result.Instructions, source.Instructions...)
	result.WebSearch, result.WebFetch, result.CodeExecution = false, false, false
	result.Tools = nil
	result.ToolPolicy = llm.ToolPolicy{Mode: llm.ToolChoiceNone}
	result.Output = &llm.OutputSpec{MaxTokens: &maxTokens, Format: llm.OutputFormat{Kind: llm.OutputKindText}}
	result.Continuation = nil
	// Reasoning is an application setting, not part of the bounded plain-text
	// summarizer contract. Leaving it enabled could reserve unbounded provider
	// reasoning tokens even though OutputReserveTokens is capped.
	result.Reasoning = nil
	if source.ServiceClassFallbacks != nil {
		result.ServiceClassFallbacks = append([]llm.ServiceClass(nil), source.ServiceClassFallbacks...)
	}
	if source.Sampling != nil {
		value := *source.Sampling
		value.StopSequences = append([]string(nil), source.Sampling.StopSequences...)
		result.Sampling = &value
	}
	return result, nil
}

// PlainTextSummary extracts only a completed model message containing text
// parts.  Tool calls, tool results, provider state, multimodal parts, and
// non-completed statuses are rejected before a summary can become state.
func PlainTextSummary(response llm.Response, maxBytes int) (string, error) {
	if response.Status != llm.ResponseStatusCompleted {
		return "", fmt.Errorf("compaction response status %q is not completed", response.Status)
	}
	if maxBytes <= 0 {
		return "", errors.New("compaction summary byte limit must be positive")
	}
	var builder strings.Builder
	for _, item := range response.Output {
		message, ok := item.(llm.Message)
		if !ok || message.Actor != llm.ActorModel {
			return "", fmt.Errorf("compaction output contains non-model message")
		}
		for _, part := range message.Content {
			textPart, ok := part.(llm.TextPart)
			if !ok {
				return "", fmt.Errorf("compaction output contains non-text content")
			}
			if !utf8.ValidString(textPart.Text) {
				return "", errors.New("compaction output is not valid UTF-8")
			}
			builder.WriteString(textPart.Text)
		}
	}
	if builder.Len() == 0 {
		return "", errors.New("compaction output is empty")
	}
	if builder.Len() > maxBytes {
		return "", fmt.Errorf("compaction output is %d bytes; limit is %d", builder.Len(), maxBytes)
	}
	return builder.String(), nil
}

// FlattenSummarizerInstructions adapts a PrepareRequest result for a provider
// family that has a single system prompt and therefore cannot represent the
// policy/application instruction hierarchy. When every caller instruction is
// application level, the two injected summarizer instructions are moved to
// application level so strict portability can compile the request. Requests
// that were not built by PrepareRequest, or whose caller instructions include
// policy level, are returned unchanged. Families that preserve the hierarchy
// must not use this: there the summarizer prompt keeps policy priority over
// caller instructions.
func FlattenSummarizerInstructions(request llm.Request) llm.Request {
	if !isSummarizerRequest(request) || len(request.Instructions) == 2 {
		return request
	}
	for _, instruction := range request.Instructions[2:] {
		if instruction.Level == llm.InstructionLevelPolicy {
			return request
		}
	}
	instructions := append([]llm.Instruction(nil), request.Instructions...)
	instructions[0].Level = llm.InstructionLevelApplication
	instructions[1].Level = llm.InstructionLevelApplication
	request.Instructions = instructions
	return request
}

func isSummarizerRequest(request llm.Request) bool {
	if len(request.Instructions) < 2 {
		return false
	}
	prompt, err := Prompt(PromptVersion)
	if err != nil {
		return false
	}
	first, second := request.Instructions[0], request.Instructions[1]
	return first.Kind == llm.InstructionKindText && first.Level == llm.InstructionLevelPolicy && first.Text == prompt &&
		second.Kind == llm.InstructionKindText && second.Level == llm.InstructionLevelPolicy && strings.HasPrefix(second.Text, "Summary style: ")
}
