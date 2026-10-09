package cloudstate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/blob"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
)

func TestGenerationPlanRestartBindingAndNoPendingRequest(t *testing.T) {
	r, table, blobs, op := operationFixture(t)
	ctx := context.Background()
	want := GenerationPlan{CompactBeforeGenerate: true}
	if _, err := r.LoadGenerationPlan(ctx, op); !errors.Is(err, contracts.ErrNotFound) {
		t.Fatalf("missing: %v", err)
	}
	if got, err := r.SaveGenerationPlan(ctx, op, want); err != nil || got != want {
		t.Fatalf("save: %v %v", got, err)
	}
	if _, err := r.LookupOperation(ctx, op); !errors.Is(err, contracts.ErrNotFound) {
		t.Fatalf("plan created a pending request: %v", err)
	}
	for key, row := range table.rows {
		data, _ := row.Item.Fields.MarshalBinary()
		for _, private := range []string{op.Key, op.Scope.Tenant, op.Scope.Project, "private prompt"} {
			if strings.Contains(key.PartitionKey, private) || bytes.Contains(data, []byte(private)) {
				t.Fatal("plan metadata exposed caller input")
			}
		}
	}
	for _, data := range blobs.values {
		if bytes.Contains(data, []byte("private prompt")) {
			t.Fatal("plan blob is plaintext")
		}
	}
	table.hook = func(string, kv.KeyValueItem) (error, error) { t.Error("replay wrote table"); return nil, nil }
	blobs.hook = func(blob.BlobKey) (error, error) { t.Error("replay wrote blob"); return nil, nil }
	if got, err := reopen(t, table, blobs).SaveGenerationPlan(ctx, op, GenerationPlan{}); err != nil || got != want {
		t.Fatalf("changed decision replaced winner: %v %v", got, err)
	}
	for _, mutate := range []func(*Operation){
		func(o *Operation) { o.Manifest = json.RawMessage(`{"different":true}`) },
		func(o *Operation) { o.RequestIndex++ },
		func(o *Operation) {
			o.Manifest = json.RawMessage(`{"version":1,"prompt":"private prompt","account":9007199254740992}`)
		},
	} {
		changed := op
		mutate(&changed)
		if _, err := r.SaveGenerationPlan(ctx, changed, want); !errors.Is(err, contracts.ErrConflict) {
			t.Fatalf("input conflict lost: %v", err)
		}
	}
	other := op
	other.Scope.Project += "-other"
	if _, err := r.LoadGenerationPlan(ctx, other); !errors.Is(err, contracts.ErrNotFound) {
		t.Fatalf("cross-scope lookup: %v", err)
	}
}

func TestGenerationPlanConcurrentDifferentDecisionsChooseOneWinner(t *testing.T) {
	_, table, blobs, op := operationFixture(t)
	const workers = 16
	results := make(chan GenerationPlan, workers)
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		r := reopen(t, table, blobs)
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			got, err := r.SaveGenerationPlan(context.Background(), op, GenerationPlan{CompactBeforeGenerate: i%2 == 0})
			results <- got
			errs <- err
		}(i)
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	want, err := reopen(t, table, blobs).LoadGenerationPlan(context.Background(), op)
	if err != nil {
		t.Fatal(err)
	}
	for got := range results {
		if got != want {
			t.Fatal("planners observed different committed decisions")
		}
	}
}

func TestGenerationPlanLostAcknowledgementReconcilesWinner(t *testing.T) {
	r, table, blobs, op := operationFixture(t)
	table.hook = func(string, kv.KeyValueItem) (error, error) { return nil, contracts.ErrOutcomeUnknown }
	want := GenerationPlan{CompactBeforeGenerate: true}
	if _, err := r.SaveGenerationPlan(context.Background(), op, want); !errors.Is(err, contracts.ErrOutcomeUnknown) {
		t.Fatalf("uncertain write hidden: %v", err)
	}
	table.hook = nil
	if got, err := reopen(t, table, blobs).SaveGenerationPlan(context.Background(), op, GenerationPlan{}); err != nil || got != want {
		t.Fatalf("retry lost committed decision: %v %v", got, err)
	}
}

func TestGenerationPlanReferencedBlobMissingFailsClosed(t *testing.T) {
	r, _, blobs, op := operationFixture(t)
	if _, err := r.SaveGenerationPlan(context.Background(), op, GenerationPlan{}); err != nil {
		t.Fatal(err)
	}
	blobs.values = map[blob.BlobKey][]byte{}
	if _, err := r.LoadGenerationPlan(context.Background(), op); !errors.Is(err, ErrCorrupt) || errors.Is(err, contracts.ErrNotFound) {
		t.Fatalf("missing committed blob allowed fresh planning: %v", err)
	}
	if _, err := r.SaveGenerationPlan(context.Background(), op, GenerationPlan{CompactBeforeGenerate: true}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corrupt plan overwritten: %v", err)
	}
}

func TestGenerationBindingImmutableEncryptedAndLostAcknowledgement(t *testing.T) {
	r, table, blobs, op := operationFixture(t)
	ctx := context.Background()
	if _, err := r.SaveGenerationPlan(ctx, op, GenerationPlan{CompactBeforeGenerate: true}); err != nil {
		t.Fatal(err)
	}
	want := json.RawMessage(`{"parent":"ckp_v1.compacted-private","append":"private follow-up"}`)
	table.hook = func(string, kv.KeyValueItem) (error, error) { return nil, contracts.ErrOutcomeUnknown }
	if err := r.SaveGenerationBinding(ctx, op, want); !errors.Is(err, contracts.ErrOutcomeUnknown) {
		t.Fatalf("lost binding acknowledgement hidden: %v", err)
	}
	table.hook = nil
	if err := reopen(t, table, blobs).SaveGenerationBinding(ctx, op, want); err != nil {
		t.Fatal(err)
	}
	if err := r.SaveGenerationBinding(ctx, op, json.RawMessage(`{"parent":"ckp_v1.substitute"}`)); !errors.Is(err, contracts.ErrConflict) {
		t.Fatalf("effective input substitution: %v", err)
	}
	changed := op
	changed.Manifest = json.RawMessage(`{"other":true}`)
	if _, err := r.LoadGenerationBinding(ctx, changed); !errors.Is(err, contracts.ErrConflict) {
		t.Fatalf("original input substitution: %v", err)
	}
	for _, row := range table.rows {
		data, _ := row.Item.Fields.MarshalBinary()
		if bytes.Contains(data, []byte("compacted-private")) || bytes.Contains(data, []byte("private follow-up")) {
			t.Fatal("binding plaintext in metadata")
		}
	}
	for _, data := range blobs.values {
		if bytes.Contains(data, []byte("private follow-up")) {
			t.Fatal("binding plaintext in blob")
		}
	}
	key := r.generationPlanKey(op)
	key.SortKey = "effective-v1"
	row := table.rows[key]
	var pointer generationPlanPointer
	if err := json.Unmarshal(row.Item.Fields["generation_binding"].(kv.KeyValueBytes), &pointer); err != nil {
		t.Fatal(err)
	}
	delete(blobs.values, blob.BlobKey(pointer.Blob))
	if _, err := r.LoadGenerationBinding(ctx, op); !errors.Is(err, ErrCorrupt) || errors.Is(err, contracts.ErrNotFound) {
		t.Fatalf("lost effective binding treated as absent: %v", err)
	}
	if err := r.SaveGenerationBinding(ctx, op, want); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("lost effective binding regenerated: %v", err)
	}
}
