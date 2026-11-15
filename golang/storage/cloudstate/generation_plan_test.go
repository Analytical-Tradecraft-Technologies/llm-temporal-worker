package cloudstate

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/blob"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
)

func TestGenerationPlanRestartBindingAndNoPendingRequest(t *testing.T) {
	r, table, blobs, op := operationFixture(t)
	ctx := context.Background()
	want := GenerationPlan{CompactBeforeGenerate: true}
	testassert.ErrorIs(t, testassert.Err(r.LoadGenerationPlan(ctx, op)), contracts.ErrNotFound, "missing")
	if got, err := r.SaveGenerationPlan(ctx, op, want); err != nil || got != want {
		t.Fatalf("save: %v %v", got, err)
	}
	testassert.ErrorIs(t, testassert.Err(r.LookupOperation(ctx, op)), contracts.ErrNotFound, "plan created a pending request")
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
	forbidWrites(t, table, blobs, "replay")
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
		testassert.ErrorIs(t, testassert.Err(r.SaveGenerationPlan(ctx, changed, want)), contracts.ErrConflict, "input conflict lost")
	}
	other := op
	other.Scope.Project += "-other"
	testassert.ErrorIs(t, testassert.Err(r.LoadGenerationPlan(ctx, other)), contracts.ErrNotFound, "cross-scope lookup")
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
		testassert.NoError(t, err)
	}
	want, err := reopen(t, table, blobs).LoadGenerationPlan(context.Background(), op)
	testassert.NoError(t, err)
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
	testassert.ErrorIs(t, testassert.Err(r.SaveGenerationPlan(context.Background(), op, want)), contracts.ErrOutcomeUnknown, "uncertain write hidden")
	table.hook = nil
	if got, err := reopen(t, table, blobs).SaveGenerationPlan(context.Background(), op, GenerationPlan{}); err != nil || got != want {
		t.Fatalf("retry lost committed decision: %v %v", got, err)
	}
}

func TestGenerationPlanReferencedBlobMissingFailsClosed(t *testing.T) {
	r, _, blobs, op := operationFixture(t)
	testassert.NoError(t, testassert.Err(r.SaveGenerationPlan(context.Background(), op, GenerationPlan{})))
	blobs.values = map[blob.BlobKey][]byte{}
	requireErrorIsNot(t, testassert.Err(r.LoadGenerationPlan(context.Background(), op)), ErrCorrupt, contracts.ErrNotFound, "missing committed blob allowed fresh planning")
	testassert.ErrorIs(t, testassert.Err(r.SaveGenerationPlan(context.Background(), op, GenerationPlan{CompactBeforeGenerate: true})), ErrCorrupt, "corrupt plan overwritten")
}

func TestGenerationBindingImmutableEncryptedAndLostAcknowledgement(t *testing.T) {
	r, table, blobs, op := operationFixture(t)
	ctx := context.Background()
	testassert.NoError(t, testassert.Err(r.SaveGenerationPlan(ctx, op, GenerationPlan{CompactBeforeGenerate: true})))
	want := json.RawMessage(`{"parent":"ckp_v1.compacted-private","append":"private follow-up"}`)
	table.hook = func(string, kv.KeyValueItem) (error, error) { return nil, contracts.ErrOutcomeUnknown }
	testassert.ErrorIs(t, r.SaveGenerationBinding(ctx, op, want), contracts.ErrOutcomeUnknown, "lost binding acknowledgement hidden")
	table.hook = nil
	testassert.NoError(t, reopen(t, table, blobs).SaveGenerationBinding(ctx, op, want))
	testassert.ErrorIs(t, r.SaveGenerationBinding(ctx, op, json.RawMessage(`{"parent":"ckp_v1.substitute"}`)), contracts.ErrConflict, "effective input substitution")
	changed := op
	changed.Manifest = json.RawMessage(`{"other":true}`)
	testassert.ErrorIs(t, testassert.Err(r.LoadGenerationBinding(ctx, changed)), contracts.ErrConflict, "original input substitution")
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
	testassert.NoError(t, json.Unmarshal(row.Item.Fields["generation_binding"].(kv.KeyValueBytes), &pointer))
	delete(blobs.values, blob.BlobKey(pointer.Blob))
	requireErrorIsNot(t, testassert.Err(r.LoadGenerationBinding(ctx, op)), ErrCorrupt, contracts.ErrNotFound, "lost effective binding treated as absent")
	testassert.ErrorIs(t, r.SaveGenerationBinding(ctx, op, want), ErrCorrupt, "lost effective binding regenerated")
}
