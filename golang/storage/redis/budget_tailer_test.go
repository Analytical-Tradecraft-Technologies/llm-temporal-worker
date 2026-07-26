package redis

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"
)

func TestBudgetStreamTailerAppliesBoundedBatchesAndPersistsCursorState(t *testing.T) {
	port := new(MemoryBudgetEventPort)
	for index := int64(1); index <= 2; index++ {
		if _, err := port.Append(context.Background(), testTailerEvent("generation-a", index)); err != nil {
			t.Fatal(err)
		}
	}
	var applied []string
	tailer, err := NewBudgetStreamTailer(BudgetStreamTailerOptions{
		Port:      port,
		Initial:   BudgetStreamCursorState{GenerationID: "generation-a"},
		BatchSize: 1,
		Enabled:   true,
		Reload: func(context.Context) (BudgetStreamCursorState, error) {
			return BudgetStreamCursorState{GenerationID: "generation-a", Cursor: "2-0"}, nil
		},
		Apply: func(_ context.Context, record BudgetStreamRecord) error {
			applied = append(applied, record.ID)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
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
	if _, err := port.Append(context.Background(), testTailerEvent("generation-a", 1)); err != nil {
		t.Fatal(err)
	}
	reloads := 0
	reload := func(context.Context) (BudgetStreamCursorState, error) {
		reloads++
		return BudgetStreamCursorState{GenerationID: "generation-a", Cursor: "1-0"}, nil
	}
	tailer, err := NewBudgetStreamTailer(BudgetStreamTailerOptions{
		Port:      port,
		Initial:   BudgetStreamCursorState{GenerationID: "generation-a", Cursor: "1-0"},
		BatchSize: 10,
		Enabled:   false,
		Reload:    reload,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := tailer.Poll(context.Background())
	if err != nil || !result.Reloaded || result.Records != 0 || reloads != 1 {
		t.Fatalf("disabled poll = %#v, %v, reloads=%d", result, err, reloads)
	}

	// A retained-stream gap is recovered by the same Redis-only reload path.
	tailer.SetEnabled(true)
	gapPort := &scriptedBudgetEventPort{err: ErrBudgetStreamGap}
	gapTailer, err := NewBudgetStreamTailer(BudgetStreamTailerOptions{
		Port:      gapPort,
		Initial:   BudgetStreamCursorState{GenerationID: "generation-a", Cursor: "1-0"},
		BatchSize: 1,
		Enabled:   true,
		Reload: func(context.Context) (BudgetStreamCursorState, error) {
			return BudgetStreamCursorState{GenerationID: "generation-a", Cursor: "9-0"}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err = gapTailer.Poll(context.Background())
	if err != nil || !result.Reloaded || result.State.Cursor != "9-0" || gapPort.reads != 1 {
		t.Fatalf("gap poll = %#v, %v, reads=%d", result, err, gapPort.reads)
	}

	// A generation-switch event is never applied to the old local plan.
	switchPort := new(MemoryBudgetEventPort)
	if _, err := switchPort.Append(context.Background(), testTailerEvent("generation-b", 1)); err != nil {
		t.Fatal(err)
	}
	applyCalled := false
	switchTailer, err := NewBudgetStreamTailer(BudgetStreamTailerOptions{
		Port:      switchPort,
		Initial:   BudgetStreamCursorState{GenerationID: "generation-a"},
		BatchSize: 1,
		Enabled:   true,
		Reload: func(context.Context) (BudgetStreamCursorState, error) {
			return BudgetStreamCursorState{GenerationID: "generation-b", Cursor: "1-0"}, nil
		},
		Apply: func(context.Context, BudgetStreamRecord) error {
			applyCalled = true
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
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
	for index := int64(1); index <= 2; index++ {
		if _, err := port.Append(context.Background(), testTailerEvent("generation-a", index)); err != nil {
			t.Fatal(err)
		}
	}
	fail := true
	var applied []string
	tailer, err := NewBudgetStreamTailer(BudgetStreamTailerOptions{
		Port:      port,
		Initial:   BudgetStreamCursorState{GenerationID: "generation-a"},
		BatchSize: 2,
		Enabled:   true,
		Reload: func(context.Context) (BudgetStreamCursorState, error) {
			return BudgetStreamCursorState{GenerationID: "generation-a", Cursor: "2-0"}, nil
		},
		Apply: func(_ context.Context, record BudgetStreamRecord) error {
			applied = append(applied, record.ID)
			if fail && record.ID == "2-0" {
				return errors.New("transient hint failure")
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
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
	tailer, err := NewBudgetStreamTailer(BudgetStreamTailerOptions{
		Port:      port,
		Initial:   BudgetStreamCursorState{GenerationID: "generation-a"},
		BatchSize: 2,
		Enabled:   true,
		Reload: func(context.Context) (BudgetStreamCursorState, error) {
			return BudgetStreamCursorState{GenerationID: "generation-a", Cursor: "1-0"}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
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
		Reload: func(context.Context) (BudgetStreamCursorState, error) {
			return BudgetStreamCursorState{GenerationID: "generation-a"}, nil
		},
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

func testTailerEvent(generation string, revision int64) BudgetStreamEvent {
	return BudgetStreamEvent{
		Schema: budgetStreamEventSchema, Kind: BudgetEventPolicyRefresh,
		GenerationID: BudgetGenerationID(generation), Revision: revision,
		OccurredAt: time.Unix(revision, 0).UTC(),
	}
}

type scriptedBudgetEventPort struct {
	reads   int
	records []BudgetStreamRecord
	err     error
}

func (port *scriptedBudgetEventPort) Append(context.Context, BudgetStreamEvent) (string, error) {
	return "", fmt.Errorf("not implemented")
}

func (port *scriptedBudgetEventPort) Read(context.Context, string, int) ([]BudgetStreamRecord, error) {
	port.reads++
	return port.records, port.err
}
