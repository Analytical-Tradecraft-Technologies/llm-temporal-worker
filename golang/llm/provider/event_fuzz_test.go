package provider_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

const (
	assemblerFuzzRecordSize = 4
	assemblerFuzzMaxEvents  = 64
)

type assemblerFuzzOutcome uint8

const (
	assemblerFuzzRejected assemblerFuzzOutcome = iota
	assemblerFuzzCompleted
	assemblerFuzzErrored
)

func TestAssemblerFuzzSequenceDecodesBoundedEventKinds(t *testing.T) {
	events := decodeAssemblerFuzzSequence([]byte{
		0, 1, 's', '0',
		1, 2, 't', '1',
		2, 3, 'u', '2',
		3, 4, 'v', '3',
		4, 5, 'w', '4',
		5, 1, 'x', '5',
		6, 2, 'y', '6',
		7, 3, 'z', '7',
		8, 4, 'q', '8',
	})
	want := []string{
		"provider.OutputStarted",
		"provider.TextDelta",
		"provider.ToolArgumentsDelta",
		"provider.ReasoningDelta",
		"provider.ProviderStateDelta",
		"provider.OutputFinished",
		"provider.UsageUpdated",
		"provider.StreamCompleted",
		"provider.StreamErrored",
	}
	if len(events) != len(want) {
		t.Fatalf("decoded %d events, want %d", len(events), len(want))
	}
	for index, event := range events {
		if got := fmt.Sprintf("%T", event); got != want[index] {
			t.Errorf("event %d type = %s, want %s", index, got, want[index])
		}
	}
}

func TestAssemblerFuzzSequenceClassifiesTerminalAndRejectedSequences(t *testing.T) {
	tests := []struct {
		name string
		raw  []byte
		want assemblerFuzzOutcome
	}{
		{
			name: "completed",
			raw: []byte{
				0, 1, 0, 0, // OutputStarted(0)
				1, 1, 'h', 'i', // TextDelta(0, "hi")
				5, 1, 0, 0, // OutputFinished(0)
				7, 1, 1, 0, // StreamCompleted(completed)
			},
			want: assemblerFuzzCompleted,
		},
		{
			name: "rejected before output start",
			raw:  []byte{1, 1, 'h', 'i'},
			want: assemblerFuzzRejected,
		},
		{
			name: "errored terminal",
			raw:  []byte{8, 1, 'x', 'y'},
			want: assemblerFuzzErrored,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			outcome, response, err := assembleFuzzSequence(decodeAssemblerFuzzSequence(test.raw))
			if outcome != test.want {
				t.Fatalf("outcome = %d, want %d (error %v)", outcome, test.want, err)
			}
			switch outcome {
			case assemblerFuzzCompleted:
				if err != nil || response.OperationKey == "" || !response.Status.Valid() {
					t.Fatalf("completed response = %#v, error = %v", response, err)
				}
			case assemblerFuzzRejected, assemblerFuzzErrored:
				if err == nil {
					t.Fatal("non-completed sequence did not return an error")
				}
			}
		})
	}
}

func FuzzAssemblerEventSequences(f *testing.F) {
	f.Add([]byte{
		0, 1, 0, 0, // OutputStarted(0)
		1, 1, 'o', 'k', // TextDelta(0, "ok")
		5, 1, 0, 0, // OutputFinished(0)
		7, 1, 1, 0, // StreamCompleted(completed)
	})
	f.Add([]byte{1, 1, 'n', 'o'})
	f.Add([]byte{8, 1, 'e', 'r'})
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > assemblerFuzzRecordSize*assemblerFuzzMaxEvents {
			return
		}
		outcome, response, err := assembleFuzzSequence(decodeAssemblerFuzzSequence(raw))
		switch outcome {
		case assemblerFuzzCompleted:
			if err != nil || response.OperationKey == "" || !response.Status.Valid() {
				t.Fatalf("completed sequence produced response %#v and error %v", response, err)
			}
		case assemblerFuzzRejected, assemblerFuzzErrored:
			if err == nil {
				t.Fatalf("outcome %d did not return a rejection error", outcome)
			}
		default:
			t.Fatalf("unknown assembler fuzz outcome %d", outcome)
		}
	})
}

// decodeAssemblerFuzzSequence maps fixed-size records into every public event
// kind. Indexes, strings, opaque bytes, usage, terminal status, and optional
// item/state fields are bounded by the input record so arbitrary fuzz bytes
// exercise the assembler state machine without unbounded allocations.
func decodeAssemblerFuzzSequence(raw []byte) []provider.Event {
	count := len(raw) / assemblerFuzzRecordSize
	if count > assemblerFuzzMaxEvents {
		count = assemblerFuzzMaxEvents
	}
	events := make([]provider.Event, 0, count)
	for offset := 0; offset+assemblerFuzzRecordSize <= len(raw) && len(events) < assemblerFuzzMaxEvents; offset += assemblerFuzzRecordSize {
		events = append(events, decodeAssemblerFuzzEvent(raw[offset:offset+assemblerFuzzRecordSize]))
	}
	return events
}

func decodeAssemblerFuzzEvent(record []byte) provider.Event {
	index := int(record[1]%5) - 1
	payload := append([]byte(nil), record[2:]...)
	rawText := string(payload)
	semanticText := strings.ToValidUTF8(rawText, "?")

	switch record[0] % 9 {
	case 0:
		return provider.OutputStarted{Index: index}
	case 1:
		return provider.TextDelta{Index: index, Text: rawText}
	case 2:
		callID, name := "", ""
		if record[2]&1 != 0 {
			callID = fmt.Sprintf("call-%02x", record[2])
		}
		if record[3]&1 != 0 {
			name = fmt.Sprintf("tool-%02x", record[3])
		}
		fragment := rawText
		if record[2]%3 == 0 {
			fragment = "{}"
		} else if record[2]%3 == 1 {
			fragment = "{"
		}
		return provider.ToolArgumentsDelta{Index: index, CallID: callID, Name: name, Fragment: fragment}
	case 3:
		return provider.ReasoningDelta{Index: index, Opaque: payload}
	case 4:
		state := llm.ProviderState{Opaque: payload}
		if record[2]&1 != 0 {
			state.Provider = "provider-fuzz"
		}
		if record[2]&2 != 0 {
			state.EndpointFamily = "family-fuzz"
		}
		if record[2]&4 != 0 {
			state.MediaType = "application/octet-stream"
		}
		return provider.ProviderStateDelta{Index: index, State: state}
	case 5:
		var item llm.Item
		if record[2]&1 != 0 {
			item = llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: semanticText}}}
		}
		return provider.OutputFinished{Index: index, Item: item}
	case 6:
		return provider.UsageUpdated{Usage: llm.Usage{InputTokens: int64(record[2]), OutputTokens: int64(record[3])}}
	case 7:
		status := llm.ResponseStatus("")
		switch record[2] % 4 {
		case 1:
			status = llm.ResponseStatusCompleted
		case 2:
			status = llm.ResponseStatusToolCalls
		case 3:
			status = llm.ResponseStatus("invalid-fuzz-status")
		}
		response := llm.Response{Status: status}
		if record[3]&1 != 0 {
			response.OperationKey = "explicit-fuzz-operation"
		}
		return provider.StreamCompleted{Response: response}
	default:
		return provider.StreamErrored{Err: fmt.Errorf("fuzz stream error %x", payload)}
	}
}

func assembleFuzzSequence(events []provider.Event) (assemblerFuzzOutcome, llm.Response, error) {
	assembler := provider.NewAssembler("assembler-fuzz")
	for _, event := range events {
		if err := assembler.Add(event); err != nil {
			return assemblerFuzzRejected, llm.Response{}, err
		}
	}
	response, err := assembler.Result()
	if err != nil {
		if len(events) > 0 {
			if _, errored := events[len(events)-1].(provider.StreamErrored); errored {
				return assemblerFuzzErrored, llm.Response{}, err
			}
		}
		return assemblerFuzzRejected, llm.Response{}, err
	}
	return assemblerFuzzCompleted, response, nil
}
