package cloudstate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/blob"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/state"
)

func preparationFixture(t *testing.T) (*Repository, *memoryTable, *memoryBlobs, Record, RequestPreparation) {
	t.Helper()
	r, table, blobs, op := operationFixture(t)
	record, err := r.BeginOperation(context.Background(), op)
	if err != nil {
		t.Fatal(err)
	}
	decimal, _ := llm.NewDecimalV1("0.123456789012345678")
	settings := state.RootModelState("private-model")
	settings.TemperatureDecimal = &decimal
	settings.Extensions = map[string]json.RawMessage{"vendor": json.RawMessage(`{"integer":9007199254740993}`)}
	snapshot, err := (state.CheckpointBlobCodec{}).EncodeSnapshot(*state.NewCheckpointSnapshot(state.MaterializedState{
		Items:    []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "private parent transcript"}}}},
		Settings: settings, Lineage: []state.Handle{"00000000-0000-4000-8000-000000000001"},
	}))
	if err != nil {
		t.Fatal(err)
	}
	return r, table, blobs, record, RequestPreparation{Version: 1, ConfigDigest: [32]byte{1}, CheckpointScope: "private-checkpoint-scope", ParentSnapshot: snapshot, PreparedAt: op.Now.Add(time.Second)}
}

func TestRequestPreparationRestartAndEncryption(t *testing.T) {
	r, table, blobs, record, preparation := preparationFixture(t)
	ctx := context.Background()
	if _, err := r.LoadRequestPreparation(ctx, record.Request.Scope, record.Request.ID); !errors.Is(err, ErrRequestPreparationMissing) {
		t.Fatalf("initial read: %v", err)
	}
	if err := r.SaveRequestPreparation(ctx, record.Request.Scope, record.Request.ID, preparation); err != nil {
		t.Fatal(err)
	}
	reopened := reopen(t, table, blobs)
	got, err := reopened.LoadRequestPreparation(ctx, record.Request.Scope, record.Request.ID)
	if err != nil || !equalExecutionJSON(got, preparation) {
		t.Fatalf("restart: %v", err)
	}
	decoded, err := (state.CheckpointBlobCodec{}).DecodeSnapshot(got.ParentSnapshot)
	if err != nil || decoded.Settings.TemperatureDecimal.String() != "0.123456789012345678" || !bytes.Contains(decoded.Settings.Extensions["vendor"], []byte("9007199254740993")) {
		t.Fatalf("lost exact values: %v", err)
	}
	got.ParentSnapshot[0] = '!'
	if _, err := reopened.LoadRequestPreparation(ctx, record.Request.Scope, record.Request.ID); err != nil {
		t.Fatal("returned data aliased storage", err)
	}
	if _, err := reopened.LoadRequestPreparation(ctx, Scope{Tenant: record.Request.Scope.Tenant, Project: "other"}, record.Request.ID); !errors.Is(err, contracts.ErrNotFound) {
		t.Fatalf("cross-scope: %v", err)
	}
	var stored [][]byte
	for key, row := range table.rows {
		data, _ := row.Item.Fields.MarshalBinary()
		stored = append(stored, []byte(key.PartitionKey+key.SortKey), data)
	}
	for key, data := range blobs.values {
		stored = append(stored, []byte(key), data)
	}
	for _, data := range stored {
		for _, secret := range []string{"private parent transcript", "private-checkpoint-scope", "private-model", "9007199254740993"} {
			if bytes.Contains(data, []byte(secret)) {
				t.Fatalf("plaintext %s leaked", secret)
			}
		}
	}
	table.hook = func(string, kv.KeyValueItem) (error, error) { t.Error("healthy replay wrote a row"); return nil, nil }
	blobs.hook = func(blob.BlobKey) (error, error) { t.Error("healthy replay wrote a blob"); return nil, nil }
	if err := reopened.SaveRequestPreparation(ctx, record.Request.Scope, record.Request.ID, preparation); err != nil {
		t.Fatal(err)
	}
}

func TestRequestPreparationConcurrentProposalsHaveOneWinner(t *testing.T) {
	r, table, blobs, record, preparation := preparationFixture(t)
	ctx := context.Background()
	var group sync.WaitGroup
	winners := make(chan [32]byte, 20)
	for i := 0; i < 20; i++ {
		candidate := preparation
		candidate.ConfigDigest[0] = byte(i%2 + 1)
		client := reopen(t, table, blobs)
		group.Add(1)
		go func() {
			defer group.Done()
			err := client.SaveRequestPreparation(ctx, record.Request.Scope, record.Request.ID, candidate)
			if err == nil {
				winners <- candidate.ConfigDigest
			} else if !errors.Is(err, contracts.ErrConflict) {
				t.Error(err)
			}
		}()
	}
	group.Wait()
	close(winners)
	got, err := r.LoadRequestPreparation(ctx, record.Request.Scope, record.Request.ID)
	if err != nil || len(winners) == 0 {
		t.Fatalf("no winner: %v", err)
	}
	for winner := range winners {
		if winner != got.ConfigDigest {
			t.Fatal("different preparations both won")
		}
	}
	current, _ := r.Read(ctx, record.Request.Scope, record.Request.ID)
	if current.Revision != record.Revision+1 {
		t.Fatal("duplicate preparation events")
	}
}

func TestRequestPreparationCannotBeAddedAfterAdmission(t *testing.T) {
	for _, key := range []string{"budget_plan", "provider_execution", "checkpoint_finalization", "finalization_handoff"} {
		t.Run(key, func(t *testing.T) {
			r, _, _, record, preparation := preparationFixture(t)
			data, _ := json.Marshal(map[string]any{"version": 1, key: map[string]any{}})
			_, err := r.TryUpdate(context.Background(), record.Request.Scope, record.Request.ID, Update{ExpectedRevision: record.Revision, Token: "started", Status: StatusRunning, Progress: data, UpdatedAt: record.UpdatedAt})
			if err != nil {
				t.Fatal(err)
			}
			if err := r.SaveRequestPreparation(context.Background(), record.Request.Scope, record.Request.ID, preparation); !errors.Is(err, contracts.ErrConflict) {
				t.Fatalf("late save: %v", err)
			}
			if _, err := r.LoadRequestPreparation(context.Background(), record.Request.Scope, record.Request.ID); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("missing started preparation treated as cache miss: %v", err)
			}
		})
	}
}

func TestRequestPreparationPreservesOtherProgressAndRejectsReplacement(t *testing.T) {
	r, _, _, record, p := preparationFixture(t)
	ctx := context.Background()
	_, err := r.TryUpdate(ctx, record.Request.Scope, record.Request.ID, Update{ExpectedRevision: record.Revision, Token: "other", Status: StatusRunning, Progress: json.RawMessage(`{"version":1,"other":{"large":9007199254740993}}`), UpdatedAt: record.UpdatedAt})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.SaveRequestPreparation(ctx, record.Request.Scope, record.Request.ID, p); err != nil {
		t.Fatal(err)
	}
	current, _ := r.Read(ctx, record.Request.Scope, record.Request.ID)
	if !strings.Contains(string(current.Progress), `"large":9007199254740993`) {
		t.Fatal("lost unrelated exact progress")
	}
	for _, mutate := range []func(*RequestPreparation){func(p *RequestPreparation) { p.ConfigDigest[0]++ }, func(p *RequestPreparation) { p.CheckpointScope += "-other" }, func(p *RequestPreparation) { p.PreparedAt = p.PreparedAt.Add(time.Second) }, func(p *RequestPreparation) { p.ParentSnapshot = nil }} {
		changed := p
		mutate(&changed)
		if err := r.SaveRequestPreparation(ctx, record.Request.Scope, record.Request.ID, changed); !errors.Is(err, contracts.ErrConflict) {
			t.Fatalf("replaced saved preparation: %v", err)
		}
	}
	for _, mutate := range []func(*RequestPreparation){func(p *RequestPreparation) { p.Version = 2 }, func(p *RequestPreparation) { p.ConfigDigest = [32]byte{} }, func(p *RequestPreparation) { p.CheckpointScope = "" }, func(p *RequestPreparation) { p.ParentSnapshot = json.RawMessage(`{}`) }, func(p *RequestPreparation) { p.PreparedAt = time.Time{} }} {
		invalid := p
		mutate(&invalid)
		if err := r.SaveRequestPreparation(ctx, record.Request.Scope, record.Request.ID, invalid); !errors.Is(err, ErrInvalid) {
			t.Fatalf("accepted invalid preparation: %v", err)
		}
	}
}

func TestRequestPreparationUncertainWritesRepairWithoutChangingInput(t *testing.T) {
	for _, stage := range []string{"blob", "event", "index-before", "index-after"} {
		t.Run(stage, func(t *testing.T) {
			r, table, blobs, record, plan := preparationFixture(t)
			fault, fired := &contracts.StorageError{Kind: contracts.ErrOutcomeUnknown}, false
			blobs.hook = func(blob.BlobKey) (error, error) {
				if stage == "blob" && !fired {
					fired = true
					return nil, fault
				}
				return nil, nil
			}
			table.hook = func(action string, item kv.KeyValueItem) (error, error) {
				if !fired && (stage == "event" && action == "create" && strings.Contains(item.PartitionKey, "/request/") ||
					strings.HasPrefix(stage, "index") && action == "replace" && strings.Contains(item.PartitionKey, "/pending/")) {
					fired = true
					if stage == "index-before" {
						return fault, nil
					}
					return nil, fault
				}
				return nil, nil
			}
			ctx := context.Background()
			if err := r.SaveRequestPreparation(ctx, record.Request.Scope, record.Request.ID, plan); err == nil || !fired {
				t.Fatalf("fault not observed: %v", err)
			}
			table.hook, blobs.hook = nil, nil
			if err := reopen(t, table, blobs).SaveRequestPreparation(ctx, record.Request.Scope, record.Request.ID, plan); err != nil {
				t.Fatal(err)
			}
			stored, err := reopen(t, table, blobs).LoadRequestPreparation(ctx, record.Request.Scope, record.Request.ID)
			if err != nil || !reflect.DeepEqual(stored.ConfigDigest, plan.ConfigDigest) {
				t.Fatalf("recovery changed preparation: %v", err)
			}
			current, _ := r.Read(ctx, record.Request.Scope, record.Request.ID)
			if current.Revision != record.Revision+1 {
				t.Fatal("uncertain retry appended another event")
			}
		})
	}
}
