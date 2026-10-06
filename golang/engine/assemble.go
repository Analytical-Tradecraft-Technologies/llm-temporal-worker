package engine

import (
	"context"
	"fmt"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/routing"
)

const legacyReasoningMediaType = "application/vnd.llmtw.reasoning+octets"

// streamAssembler keeps the provider protocol validator and the public event
// publisher in one ordered path. A provider event is first accepted by the
// assembler, then exposed to the caller; invalid provider protocol therefore
// never leaks a partially accepted semantic event.
type streamAssembler struct {
	assembler      *provider.Assembler
	emitter        *streamEmitter
	candidate      routing.Candidate
	toolCalls      map[int]string
	completedItems int
}

func newStreamAssembler(operationKey string, candidate routing.Candidate, emitter *streamEmitter) *streamAssembler {
	return &streamAssembler{
		assembler: provider.NewAssembler(operationKey),
		emitter:   emitter,
		candidate: candidate,
		toolCalls: make(map[int]string),
	}
}

// Add accepts one provider event and returns a final response only after a
// valid provider terminal. The returned bool distinguishes a normal
// non-terminal event from StreamCompleted.
func (assembler *streamAssembler) Add(ctx context.Context, value provider.Event) (llm.Response, bool, error) {
	if assembler == nil || assembler.assembler == nil || assembler.emitter == nil {
		return llm.Response{}, false, fmt.Errorf("stream assembler is unavailable")
	}
	if toolArguments, ok := value.(provider.ToolArgumentsDelta); ok && (toolArguments.CallID == "" || toolArguments.Name == "") {
		// provider.Assembler can defer tool identity while assembling a generic
		// protocol sequence, but the public event union cannot: each emitted
		// ToolArgumentsDelta must carry its stable identity. Streaming adapters
		// therefore normalize or buffer early provider fragments before exposing
		// them through EventSource.
		return llm.Response{}, false, fmt.Errorf("streaming tool argument fragment at output %d is missing call ID or name", toolArguments.Index)
	}
	providerValue := value
	if reasoning, ok := value.(provider.ReasoningDelta); ok {
		// The legacy provider event did not carry provenance. Enrich it before
		// assembly so opaque bytes remain opaque in the final normalized result.
		providerValue = provider.ProviderStateDelta{Index: reasoning.Index, State: llm.ProviderState{
			Provider:       assembler.candidate.Provider,
			EndpointFamily: assembler.candidate.Family,
			MediaType:      legacyReasoningMediaType,
			Opaque:         append([]byte(nil), reasoning.Opaque...),
		}}
	}
	if err := assembler.assembler.Add(providerValue); err != nil {
		return llm.Response{}, false, err
	}

	switch event := providerValue.(type) {
	case provider.OutputStarted:
		if err := assembler.emitter.emit(ctx, &event.Index, nil, func(header llm.EventHeader) llm.Event {
			return llm.ContentStarted{EventHeader: header}
		}); err != nil {
			return llm.Response{}, false, err
		}
	case provider.TextDelta:
		if err := assembler.emitter.emit(ctx, &event.Index, nil, func(header llm.EventHeader) llm.Event {
			return llm.TextDelta{EventHeader: header, Text: event.Text}
		}); err != nil {
			return llm.Response{}, false, err
		}
	case provider.ToolArgumentsDelta:
		if existing, seen := assembler.toolCalls[event.Index]; !seen || existing != event.CallID {
			if err := assembler.emitter.emit(ctx, &event.Index, nil, func(header llm.EventHeader) llm.Event {
				return llm.ToolCallStarted{EventHeader: header, CallID: event.CallID, Name: event.Name}
			}); err != nil {
				return llm.Response{}, false, err
			}
			assembler.toolCalls[event.Index] = event.CallID
		}
		if err := assembler.emitter.emit(ctx, &event.Index, nil, func(header llm.EventHeader) llm.Event {
			return llm.ToolArgumentsDelta{EventHeader: header, CallID: event.CallID, Name: event.Name, Fragment: event.Fragment}
		}); err != nil {
			return llm.Response{}, false, err
		}
	case provider.ProviderStateDelta:
		state := event.State
		state.Opaque = append([]byte(nil), state.Opaque...)
		if err := assembler.emitter.emit(ctx, &event.Index, nil, func(header llm.EventHeader) llm.Event {
			return llm.ProviderStateDelta{EventHeader: header, State: state}
		}); err != nil {
			return llm.Response{}, false, err
		}
	case provider.OutputFinished:
		item, err := assembler.assembler.FinishedItem(event.Index)
		if err != nil {
			return llm.Response{}, false, err
		}
		if err := assembler.emitter.emit(ctx, &event.Index, nil, func(header llm.EventHeader) llm.Event {
			return llm.ContentCompleted{EventHeader: header, Item: item}
		}); err != nil {
			return llm.Response{}, false, err
		}
		assembler.completedItems++
	case provider.UsageUpdated:
		if err := assembler.emitter.emit(ctx, nil, nil, func(header llm.EventHeader) llm.Event {
			return llm.UsageUpdated{EventHeader: header, Usage: event.Usage}
		}); err != nil {
			return llm.Response{}, false, err
		}
	case provider.StreamCompleted:
		response, err := assembler.assembler.Result()
		if err != nil {
			return llm.Response{}, false, err
		}
		return response, true, nil
	case provider.StreamErrored:
		_, err := assembler.assembler.Result()
		if err == nil {
			err = fmt.Errorf("provider stream failed")
		}
		return llm.Response{}, false, err
	}
	return llm.Response{}, false, nil
}
