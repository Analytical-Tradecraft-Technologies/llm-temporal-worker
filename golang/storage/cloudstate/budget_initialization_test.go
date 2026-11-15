package cloudstate

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts/kv"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/budget"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
)

func initializationIdentity() budget.InitializationIdentity {
	return budget.InitializationIdentity{Namespace: "worker:{admission}:", KeyFingerprint: strings.Repeat("a", 64)}
}

func TestBudgetInitializationHasOneCreatorAndNeverRegresses(t *testing.T) {
	r, table, blobs, request := fixture(t)
	identity := initializationIdentity()
	ctx := context.Background()
	testassert.ErrorIs(t, testassert.Err(r.ReadBudgetInitialization(ctx, identity.Namespace)), contracts.ErrNotFound, "absent receipt =")
	var wg sync.WaitGroup
	var creators atomic.Int32
	receipts := make(chan budget.Initialization, 24)
	for range 24 {
		wg.Go(func() {
			p, err := reopen(t, table, blobs).PrepareBudgetInitialization(ctx, identity, request.CreatedAt)
			if err != nil {
				t.Error(err)
				return
			}
			if p.Created {
				creators.Add(1)
			}
			receipts <- p.Receipt
			if err := r.CompleteBudgetInitialization(ctx, p.Receipt); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	close(receipts)
	value, err := r.ReadBudgetInitialization(ctx, identity.Namespace)
	if err != nil || !value.Ready || creators.Load() != 1 {
		t.Fatalf("completed receipt %#v, creators=%d, err=%v", value, creators.Load(), err)
	}
	for receipt := range receipts {
		if receipt.Marker() != value.Marker() || !receipt.CreatedAt.Equal(value.CreatedAt) {
			t.Fatal("concurrent initializer acquired another epoch")
		}
	}
	// Reopening with another payload encryption key must not select a new row.
	other, err := NewRepository(Options{Table: table, Blobs: blobs, Namespace: r.namespace, Secret: bytes.Repeat([]byte{9}, 32)})
	testassert.NoError(t, err)
	replayed, err := other.PrepareBudgetInitialization(ctx, identity, request.CreatedAt.Add(time.Hour))
	if err != nil || replayed.Created || !replayed.Receipt.Ready || replayed.Receipt.Marker() != value.Marker() {
		t.Fatalf("replay = %#v, %v", replayed, err)
	}
	changed := identity
	changed.KeyFingerprint = strings.Repeat("b", 64)
	testassert.ErrorIs(t, testassert.Err(r.PrepareBudgetInitialization(ctx, changed, request.CreatedAt)), contracts.ErrConflict, "key rotation created another budget")
	if len(table.rows) != 1 || len(blobs.values) != 0 {
		t.Fatal("initialization persisted balances or payloads")
	}
}

func TestBudgetInitializationLostRepliesDoNotReissueCreationPermission(t *testing.T) {
	for _, operation := range []string{"create", "replace"} {
		t.Run(operation, func(t *testing.T) {
			r, table, blobs, request := fixture(t)
			ctx := context.Background()
			identity := initializationIdentity()
			table.hook = func(op string, _ kv.KeyValueItem) (error, error) {
				if op == operation {
					return nil, contracts.ErrOutcomeUnknown
				}
				return nil, nil
			}
			p, err := r.PrepareBudgetInitialization(ctx, identity, request.CreatedAt)
			if operation == "create" {
				if !errors.Is(err, contracts.ErrOutcomeUnknown) || p.Created {
					t.Fatalf("unknown create = %#v, %v", p, err)
				}
			} else {
				testassert.NoError(t, err)
				testassert.ErrorIs(t, r.CompleteBudgetInitialization(ctx, p.Receipt), contracts.ErrOutcomeUnknown)
			}
			table.hook = nil
			p, err = reopen(t, table, blobs).PrepareBudgetInitialization(ctx, identity, request.CreatedAt)
			if err != nil || p.Created || p.Receipt.Ready != (operation == "replace") {
				t.Fatalf("lost-reply replay = %#v, %v", p, err)
			}
		})
	}
}

func TestBudgetInitializationRejectsConflictsAndCorruptReceipts(t *testing.T) {
	for _, corrupt := range []string{"schema", "epoch", "namespace", "fingerprint", "created", "version", "row-key", "field-type", "extra-field"} {
		t.Run(corrupt, func(t *testing.T) {
			r, table, _, request := fixture(t)
			ctx := context.Background()
			p, err := r.PrepareBudgetInitialization(ctx, initializationIdentity(), request.CreatedAt)
			testassert.NoError(t, err)
			other := p.Receipt
			other.Epoch = "00000000-0000-4000-8000-000000000001"
			testassert.ErrorIs(t, r.CompleteBudgetInitialization(ctx, other), contracts.ErrConflict, "wrong epoch =")
			key := r.budgetInitializationKey(p.Receipt.Identity.Namespace)
			row := table.rows[key]
			value := p.Receipt
			switch corrupt {
			case "schema":
				value.Schema = "future"
			case "epoch":
				value.Epoch = "invalid"
			case "namespace":
				value.Identity.Namespace = "another:{admission}:"
			case "fingerprint":
				value.Identity.KeyFingerprint = "invalid"
			case "created":
				value.CreatedAt = time.Time{}
			case "version":
				row.Version = ""
			case "row-key":
				row.Item.PartitionKey = "another-partition"
			case "field-type":
				row.Item.Fields["initialization"] = kv.String("not bytes")
			case "extra-field":
				row.Item.Fields["balance"] = kv.String("forbidden")
			}
			if corrupt == "schema" || corrupt == "epoch" || corrupt == "namespace" || corrupt == "fingerprint" || corrupt == "created" {
				row.Item = r.budgetInitializationItem(value)
			}
			table.rows[key] = row
			testassert.ErrorIs(t, testassert.Err(r.ReadBudgetInitialization(ctx, p.Receipt.Identity.Namespace)), ErrCorrupt, "read corrupt receipt =")
			testassert.ErrorIs(t, r.CompleteBudgetInitialization(ctx, p.Receipt), ErrCorrupt, "repaired corrupt receipt =")
		})
	}
}

func TestBudgetInitializationRejectsCanceledOrInvalidPreparation(t *testing.T) {
	r, table, _, request := fixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	testassert.ErrorIs(t, testassert.Err(r.PrepareBudgetInitialization(ctx, initializationIdentity(), request.CreatedAt)), context.Canceled)
	testassert.ErrorIs(t, testassert.Err(r.PrepareBudgetInitialization(context.Background(), budget.InitializationIdentity{}, request.CreatedAt)), ErrInvalid)
	testassert.ErrorIs(t, testassert.Err(r.PrepareBudgetInitialization(context.Background(), initializationIdentity(), time.Time{})), ErrInvalid)
	if len(table.rows) != 0 {
		t.Fatal("invalid initialization wrote a receipt")
	}
}
