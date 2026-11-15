package redis

import (
	"context"
	"errors"
	"fmt"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"reflect"
	"testing"
	"time"
)

func TestBudgetStreamTailerAppliesBoundedBatchesAndPersistsCursorState(t *testing.T) {
	port := new(MemoryBudgetEventPort)
	appendTailerEvents(t, port, "generation-a", 2)
	var applied []string
	tailer := newTestBudgetStreamTailer(t, BudgetStreamTailerOptions{
		Port:      port,
		Initial:   BudgetStreamCursorState{GenerationID: "generation-a"},
		BatchSize: 1,
		Enabled:   true,
		Reload:    reloadTailerTo("generation-a", "2-0"),
		Apply: func(_ context.Context, record BudgetStreamRecord) error {
			applied = append(applied, record.ID)
			return nil
		},
	})
	first, err := tailer.Poll(context.Background())
	if err != nil || first.Records != 1 || first.State.Cursor != "1-0" {
		t.Fatalf("first poll = %#v, %v", first, err)
	}
	second, err := tailer.Poll(context.Background())
	if err != nil || second.Records != 1 || second.State.Cursor != "2-0" {
		t.Fatalf("second poll = %#v, %v", second, err)
	}
	if !reflect.DeepEqual(applied, []string{"1-0", "2-0"}) {
		t.Fatalf("applied IDs = %v", applied)
	}
}

func TestBudgetStreamTailerReloadsOnDisabledConsumptionGapAndGenerationSwitch(t *testing.T) {
	port := new(MemoryBudgetEventPort)
	appendTailerEvents(t, port, "generation-a", 1)
	reloads := 0
	reload := func(context.Context) (BudgetStreamCursorState, error) {
		reloads++
		return BudgetStreamCursorState{GenerationID: "generation-a", Cursor: "1-0"}, nil
	}
	tailer := newTestBudgetStreamTailer(t, BudgetStreamTailerOptions{
		Port:      port,
		Initial:   BudgetStreamCursorState{GenerationID: "generation-a", Cursor: "1-0"},
		BatchSize: 10,
		Enabled:   false,
		Reload:    reload,
	})
	result, err := tailer.Poll(context.Background())
	if err != nil || !result.Reloaded || result.Records != 0 || reloads != 1 {
		t.Fatalf("disabled poll = %#v, %v, reloads=%d", result, err, reloads)
	}

	// A retained-stream gap is recovered by the same Redis-only reload path.
	tailer.SetEnabled(true)
	gapPort := &scriptedBudgetEventPort{err: ErrBudgetStreamGap, currentCursor: "8-0"}
	gapTailer := newTestBudgetStreamTailer(t, BudgetStreamTailerOptions{
		Port:      gapPort,
		Initial:   BudgetStreamCursorState{GenerationID: "generation-a", Cursor: "1-0"},
		BatchSize: 1,
		Enabled:   true,
		Reload:    reloadTailerTo("generation-a", "9-0"),
	})
	result, err = gapTailer.Poll(context.Background())
	if err != nil || !result.Reloaded || result.State.Cursor != "8-0" || gapPort.reads != 1 {
		t.Fatalf("gap poll = %#v, %v, reads=%d", result, err, gapPort.reads)
	}
	withoutCursor := newTestBudgetStreamTailer(t, BudgetStreamTailerOptions{
		Port:      &scriptedBudgetEventPort{err: ErrBudgetStreamGap},
		Initial:   BudgetStreamCursorState{GenerationID: "generation-a", Cursor: "1-0"},
		BatchSize: 1,
		Enabled:   true,
		Reload:    reloadTailerTo("generation-a", "9-0"),
	})
	testassert.ErrorIs(t, testassert.Err(withoutCursor.Poll(context.Background())), ErrBudgetStreamCursorUnavailable, "gap without current cursor")

	// A generation-switch event is never applied to the old local plan.
	switchPort := new(MemoryBudgetEventPort)
	appendTailerEvents(t, switchPort, "generation-b", 1)
	applyCalled := false
	switchTailer := newTestBudgetStreamTailer(t, BudgetStreamTailerOptions{
		Port:      switchPort,
		Initial:   BudgetStreamCursorState{GenerationID: "generation-a"},
		BatchSize: 1,
		Enabled:   true,
		Reload:    reloadTailerTo("generation-b", "1-0"),
		Apply: func(context.Context, BudgetStreamRecord) error {
			applyCalled = true
			return nil
		},
	})
	result, err = switchTailer.Poll(context.Background())
	if err != nil || !result.Reloaded || result.Records != 0 || applyCalled {
		t.Fatalf("generation switch poll = %#v, %v, applied=%v", result, err, applyCalled)
	}
	if got := switchTailer.State(); got.GenerationID != "generation-b" || got.Cursor != "1-0" {
		t.Fatalf("generation switch state = %#v", got)
	}
}

func TestBudgetStreamTailerDoesNotAdvancePastApplyFailure(t *testing.T) {
	port := new(MemoryBudgetEventPort)
	appendTailerEvents(t, port, "generation-a", 2)
	fail := true
	var applied []string
	tailer := newTestBudgetStreamTailer(t, BudgetStreamTailerOptions{
		Port:      port,
		Initial:   BudgetStreamCursorState{GenerationID: "generation-a"},
		BatchSize: 2,
		Enabled:   true,
		Reload:    reloadTailerTo("generation-a", "2-0"),
		Apply: func(_ context.Context, record BudgetStreamRecord) error {
			applied = append(applied, record.ID)
			if fail && record.ID == "2-0" {
				return errors.New("transient hint failure")
			}
			return nil
		},
	})
	result, err := tailer.Poll(context.Background())
	if err == nil || result.Records != 1 || tailer.State().Cursor != "1-0" {
		t.Fatalf("failed poll = %#v, %v, state=%#v", result, err, tailer.State())
	}
	fail = false
	result, err = tailer.Poll(context.Background())
	if err != nil || result.Records != 1 || tailer.State().Cursor != "2-0" {
		t.Fatalf("retry poll = %#v, %v, state=%#v", result, err, tailer.State())
	}
	if !reflect.DeepEqual(applied, []string{"1-0", "2-0", "2-0"}) {
		t.Fatalf("applied IDs = %v", applied)
	}
}

func TestBudgetStreamTailerRejectsNonAdvancingAdapterRecords(t *testing.T) {
	event := testTailerEvent("generation-a", 1)
	port := &scriptedBudgetEventPort{records: []BudgetStreamRecord{
		{ID: "1-0", Event: event},
		{ID: "1-0", Event: event},
	}}
	tailer := newTestBudgetStreamTailer(t, BudgetStreamTailerOptions{
		Port:      port,
		Initial:   BudgetStreamCursorState{GenerationID: "generation-a"},
		BatchSize: 2,
		Enabled:   true,
		Reload:    reloadTailerTo("generation-a", "1-0"),
	})
	result, err := tailer.Poll(context.Background())
	if !errors.Is(err, ErrBudgetStreamInvalid) || result.Records != 1 || tailer.State().Cursor != "1-0" {
		t.Fatalf("non-advancing poll = %#v, %v, state=%#v", result, err, tailer.State())
	}
}

func TestBudgetStreamTailerRejectsUntrustedConfiguration(t *testing.T) {
	port := new(MemoryBudgetEventPort)
	base := BudgetStreamTailerOptions{
		Port:      port,
		Initial:   BudgetStreamCursorState{GenerationID: "generation-a"},
		BatchSize: 1,
		Enabled:   true,
		Reload:    reloadTailerTo("generation-a", ""),
	}
	tests := []struct {
		name string
		edit func(*BudgetStreamTailerOptions)
	}{
		{name: "missing port", edit: func(value *BudgetStreamTailerOptions) { value.Port = nil }},
		{name: "missing reload", edit: func(value *BudgetStreamTailerOptions) { value.Reload = nil }},
		{name: "zero batch", edit: func(value *BudgetStreamTailerOptions) { value.BatchSize = 0 }},
		{name: "empty generation", edit: func(value *BudgetStreamTailerOptions) { value.Initial.GenerationID = "" }},
		{name: "malformed cursor", edit: func(value *BudgetStreamTailerOptions) { value.Initial.Cursor = "latest" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := base
			test.edit(&value)
			if _, err := NewBudgetStreamTailer(value); err == nil {
				t.Fatal("constructor accepted untrusted configuration")
			}
		})
	}
}

func newTestBudgetStreamTailer(t *testing.T, options BudgetStreamTailerOptions) *BudgetStreamTailer {
	t.Helper()
	tailer := mustCall(t, NewBudgetStreamTailer, options)
	return tailer
}

// reloadTailerTo is a Reload callback that always returns the given state.
func reloadTailerTo(generation, cursor string) func(context.Context) (BudgetStreamCursorState, error) {
	return func(context.Context) (BudgetStreamCursorState, error) {
		return BudgetStreamCursorState{GenerationID: BudgetGenerationID(generation), Cursor: cursor}, nil
	}
}

// appendTailerEvents appends revisions 1 through count of generation to port.
func appendTailerEvents(t *testing.T, port *MemoryBudgetEventPort, generation string, count int64) {
	t.Helper()
	for index := int64(1); index <= count; index++ {
		testassert.NoError(t, testassert.Err(port.Append(context.Background(), testTailerEvent(generation, index))))
	}
}

func testTailerEvent(generation string, revision int64) BudgetStreamEvent {
	return BudgetStreamEvent{
		Schema: budgetStreamEventSchema, Kind: BudgetEventPolicyRefresh,
		GenerationID: BudgetGenerationID(generation), Revision: revision,
		OccurredAt: time.Unix(revision, 0).UTC(),
	}
}

type scriptedBudgetEventPort struct {
	reads         int
	records       []BudgetStreamRecord
	err           error
	currentCursor string
}

func (port *scriptedBudgetEventPort) Append(context.Context, BudgetStreamEvent) (string, error) {
	return "", fmt.Errorf("not implemented")
}

func (port *scriptedBudgetEventPort) Read(context.Context, string, int) ([]BudgetStreamRecord, error) {
	port.reads++
	return port.records, port.err
}

func (port *scriptedBudgetEventPort) CurrentCursor(context.Context) (string, error) {
	if port.currentCursor == "" {
		return "", ErrBudgetStreamCursorUnavailable
	}
	return port.currentCursor, nil
}
