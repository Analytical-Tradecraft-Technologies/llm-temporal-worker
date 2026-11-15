package llm

import (
	"context"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"io"
	"strings"
	"testing"
)

func TestValidateStreamEventRequiresIdentityAndBoundsDelta(t *testing.T) {
	for _, event := range []Event{
		TextDelta{EventHeader: EventHeader{OperationID: "operation-1"}, Text: "hello"},
		TextDelta{EventHeader: EventHeader{Sequence: 1}, Text: "hello"},
		TextDelta{EventHeader: EventHeader{Sequence: 1, OperationID: "operation-1"}, Text: strings.Repeat("x", MaxStreamEventBytes+1)},
	} {
		if err := ValidateStreamEvent(event); err == nil {
			t.Fatalf("accepted invalid stream event %#v", event)
		}
	}

	valid := TextDelta{EventHeader: EventHeader{Sequence: 1, OperationID: "operation-1"}, Text: "hello"}
	testassert.NoError(t, ValidateStreamEvent(valid), "validate valid event")
}

func TestValidateStreamEventAllowsCompletedResponseAtResponsePayloadSize(t *testing.T) {
	completed := ResponseCompleted{
		EventHeader: EventHeader{Sequence: 1, OperationID: "operation-1"},
		Response: Response{
			OperationKey: "operation-1",
			OperationID:  "operation-1",
			Status:       ResponseStatusCompleted,
			Output:       []Item{ModelText(strings.Repeat("x", MaxStreamEventBytes+1))},
		},
	}
	if err := ValidateStreamEvent(completed); err != nil {
		t.Fatalf("completed response should use response payload limits, got %v", err)
	}
}

func TestEventStreamExposesExactlyOneTypedTerminalOutcome(t *testing.T) {
	stream := newSliceStream([]Event{
		ResponseStarted{EventHeader: EventHeader{Sequence: 1, OperationID: "operation-1"}},
		ResponseCompleted{EventHeader: EventHeader{Sequence: 2, OperationID: "operation-1"}, Response: Response{OperationKey: "operation-1", OperationID: "operation-1", Status: ResponseStatusCompleted}},
	})

	first, err := stream.Next(context.Background())
	testassert.NoError(t, err, "read first event")
	if _, ok := first.(ResponseStarted); !ok {
		t.Fatalf("first event = %T, want ResponseStarted", first)
	}
	terminal, err := stream.Next(context.Background())
	testassert.NoError(t, err, "read terminal event")
	if !IsTerminalEvent(terminal) {
		t.Fatalf("terminal event = %T, want typed terminal", terminal)
	}
	if _, err := stream.Next(context.Background()); err != io.EOF {
		t.Fatalf("read after terminal error = %v, want EOF", err)
	}
}

type sliceStream struct {
	events []Event
	next   int
}

func newSliceStream(events []Event) EventStream {
	return &sliceStream{events: events}
}

func (stream *sliceStream) Next(ctx context.Context) (Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if stream.next == len(stream.events) {
		return nil, io.EOF
	}
	event := stream.events[stream.next]
	stream.next++
	return event, nil
}

func (*sliceStream) Close() error { return nil }
