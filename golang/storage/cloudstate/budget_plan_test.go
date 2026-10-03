package cloudstate

import (
	"context"
	"encoding/hex"
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
	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/cache"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/storage/durable"
)

func budgetPlanFixture(t *testing.T) (*Repository, *memoryTable, *memoryBlobs, Record, BudgetPlan) {
	t.Helper()
	r, table, blobs, op := operationFixture(t)
	record, err := r.BeginOperation(context.Background(), op)
	if err != nil {
		t.Fatal(err)
	}
	entry := pricing.Entry{Provider: "private-provider", Family: "private-family", EndpointID: "private-endpoint", Model: "private-model",
		Region: "private-region", ProviderTier: "private-tier", Version: "price/v1", Prices: pricing.UnitPrices{PerRequest: pricing.MustDecimalUSD("0.00000000015")},
		EffectiveUntil: op.Now.Add(time.Hour)}
	catalog, err := pricing.CompileUSD("catalog/v1", []pricing.Entry{entry})
	if err != nil {
		t.Fatal(err)
	}
	plan := BudgetPlan{Mode: BudgetReserved, Version: 1, Kind: op.Kind, ConfigDigest: [32]byte{1}, ConfigEpoch: "epoch/v1", RequestDigest: [32]byte{2},
		CapabilityVersion: "capability/v1", CompilerVersion: "compiler/v1", Family: entry.Family, ProviderTier: entry.ProviderTier,
		RequestedClass: llm.ServiceClassPriority, AttemptedClass: llm.ServiceClassStandard, QuotedAt: op.Now.Add(time.Second),
		Route: durable.RoutePlan{OperationID: "private-paid-attempt", GenerationID: "private-generation", RouteID: "private-route", EndpointID: entry.EndpointID,
			Provider: entry.Provider, Model: entry.Model, PriceVersion: entry.Version, CacheIdentity: cache.RouteIdentity{Provider: cache.Provider(entry.Provider), Endpoint: cache.Endpoint(entry.EndpointID),
				Model: cache.Model(entry.Model), Region: cache.Region(entry.Region), Revision: "private-revision", Compiler: cache.CompilerProfile(entry.Family + "/compiler/v1")}},
		Quote: pricing.Quote{CatalogVersion: catalog.Version, CatalogDigest: hex.EncodeToString(catalog.Digest[:]), Entry: entry},
		Estimate: budget.Estimate{CandidateID: "candidate-1", InputTokens: 9007199254740993, OutputTokens: 16, MicroUSD: 1,
			CostUSD: pricing.MustUSD("0.00000000015"), CatalogVersion: entry.Version}}
	plan.Reservation = durable.ReserveRequest{OperationID: plan.Route.OperationID, GenerationID: plan.Route.GenerationID, ExpiresAt: op.Now.Add(24 * time.Hour)}
	for _, window := range []budget.Window{{ID: "hour", Duration: time.Hour, Bucket: 5 * time.Minute}, {ID: "day", Duration: 24 * time.Hour, Bucket: time.Hour}} {
		plan.Reservation.Reservations = append(plan.Reservation.Reservations, admission.WindowReservation{PolicyID: "private-policy", WindowID: window.ID,
			Bucket: plan.QuotedAt.UnixNano() / int64(window.Bucket), BucketNanos: int64(window.Bucket), DurationNanos: int64(window.Duration),
			AmountUSD: plan.Estimate.CostUSD, Amount: plan.Estimate.MicroUSD, LimitUSD: pricing.MustUSD("0.01"), Limit: 10000})
	}
	if err := plan.Validate(); err != nil {
		t.Fatal(err)
	}
	return r, table, blobs, record, plan
}

func copyTestBudgetPlan(t *testing.T, plan BudgetPlan) BudgetPlan {
	t.Helper()
	data, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	var result BudgetPlan
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestBudgetPlanRestartReplaysExactReservationAndPreservesProgress(t *testing.T) {
	for _, kind := range []string{"generate", "compact"} {
		t.Run(kind, func(t *testing.T) {
			r, table, blobs, record, plan := budgetPlanFixture(t)
			if kind == "compact" {
				op := Operation{Scope: record.Request.Scope, Kind: kind, Key: "compact", Manifest: record.Request.Manifest, Now: record.Request.CreatedAt}
				var err error
				record, err = r.BeginOperation(context.Background(), op)
				if err != nil {
					t.Fatal(err)
				}
				plan.Kind = kind
			}
			ctx := context.Background()
			updated, err := r.TryUpdate(ctx, record.Request.Scope, record.Request.ID, Update{ExpectedRevision: record.Revision, Token: "other-progress",
				Status: StatusRunning, Progress: json.RawMessage(`{"version":1,"other":{"integer":9007199254740993}}`), UpdatedAt: plan.QuotedAt})
			if err != nil {
				t.Fatal(err)
			}
			if err := r.SaveBudgetPlan(ctx, record.Request.Scope, record.Request.ID, plan, plan.QuotedAt); err != nil {
				t.Fatal(err)
			}
			restarted := reopen(t, table, blobs)
			stored, err := restarted.LoadBudgetPlan(ctx, record.Request.Scope, record.Request.ID)
			if err != nil || !reflect.DeepEqual(stored.Reservation, plan.Reservation) || stored.Quote.CatalogDigest != plan.Quote.CatalogDigest ||
				stored.Estimate.InputTokens != 9007199254740993 || stored.Estimate.CostUSD.Cmp(plan.Estimate.CostUSD) != 0 {
				t.Fatalf("restart changed exact quote: %v", err)
			}
			current, _ := restarted.Read(ctx, record.Request.Scope, record.Request.ID)
			if current.Revision != updated.Revision+1 || !strings.Contains(string(current.Progress), `"integer":9007199254740993`) {
				t.Fatal("plan replaced unrelated progress or appended extra events")
			}
			// Reads and identical saves use the original quote even after its price
			// interval, bucket and admission expiry have passed.
			table.hook = func(string, kv.KeyValueItem) (error, error) { t.Error("healthy replay wrote a row"); return nil, nil }
			blobs.hook = func(blob.BlobKey) (error, error) { t.Error("healthy replay wrote a blob"); return nil, nil }
			if err := restarted.SaveBudgetPlan(ctx, record.Request.Scope, record.Request.ID, plan, plan.QuotedAt.Add(48*time.Hour)); err != nil {
				t.Fatal(err)
			}
			stored, err = restarted.LoadBudgetPlan(ctx, record.Request.Scope, record.Request.ID)
			if err != nil || !reflect.DeepEqual(stored.Reservation, plan.Reservation) {
				t.Fatalf("old quote was refreshed: %v", err)
			}
			stored.Reservation.Reservations[0].WindowID = "mutated"
			again, err := restarted.LoadBudgetPlan(ctx, record.Request.Scope, record.Request.ID)
			if err != nil || again.Reservation.Reservations[0].WindowID != "hour" {
				t.Fatal("returned plan mutated stored data")
			}
		})
	}
}

func TestBudgetPlanConcurrentProposalsHaveOneImmutableWinner(t *testing.T) {
	r, table, blobs, record, plan := budgetPlanFixture(t)
	ctx := context.Background()
	var group sync.WaitGroup
	successes := make(chan [32]byte, 20)
	for i := 0; i < 20; i++ {
		repository := reopen(t, table, blobs)
		candidate := copyTestBudgetPlan(t, plan)
		candidate.ConfigDigest[0] = byte(i%2 + 1)
		group.Add(1)
		go func() {
			defer group.Done()
			err := repository.SaveBudgetPlan(ctx, record.Request.Scope, record.Request.ID, candidate, plan.QuotedAt)
			if err == nil {
				successes <- candidate.ConfigDigest
			} else if !errors.Is(err, contracts.ErrConflict) {
				t.Error(err)
			}
		}()
	}
	group.Wait()
	close(successes)
	stored, err := r.LoadBudgetPlan(ctx, record.Request.Scope, record.Request.ID)
	if err != nil || len(successes) == 0 {
		t.Fatalf("no plan committed: %v", err)
	}
	for digest := range successes {
		if digest != stored.ConfigDigest {
			t.Fatal("different proposals both succeeded")
		}
	}
	after, _ := r.Read(ctx, record.Request.Scope, record.Request.ID)
	if after.Revision != record.Revision+1 {
		t.Fatal("race appended more than one plan")
	}
}

func TestBudgetPlanUncertainWritesRepairWithoutChangingQuote(t *testing.T) {
	for _, stage := range []string{"blob", "event", "index-before", "index-after"} {
		t.Run(stage, func(t *testing.T) {
			r, table, blobs, record, plan := budgetPlanFixture(t)
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
			if err := r.SaveBudgetPlan(ctx, record.Request.Scope, record.Request.ID, plan, plan.QuotedAt); err == nil || !fired {
				t.Fatalf("fault not observed: %v", err)
			}
			table.hook, blobs.hook = nil, nil
			if err := reopen(t, table, blobs).SaveBudgetPlan(ctx, record.Request.Scope, record.Request.ID, plan, plan.QuotedAt.Add(time.Hour)); err != nil {
				t.Fatal(err)
			}
			stored, err := reopen(t, table, blobs).LoadBudgetPlan(ctx, record.Request.Scope, record.Request.ID)
			if err != nil || !reflect.DeepEqual(stored.Reservation, plan.Reservation) {
				t.Fatalf("recovery changed quote: %v", err)
			}
			current, _ := r.Read(ctx, record.Request.Scope, record.Request.ID)
			if current.Revision != record.Revision+1 {
				t.Fatal("uncertain retry appended another event")
			}
		})
	}
}

func TestBudgetPlanLoadRepairsDiscoveryBeforeReturningAdmissionFacts(t *testing.T) {
	r, table, blobs, record, plan := budgetPlanFixture(t)
	fault := &contracts.StorageError{Kind: contracts.ErrUnavailable}
	table.hook = func(action string, item kv.KeyValueItem) (error, error) {
		if action == "replace" && strings.Contains(item.PartitionKey, "/pending/") {
			return fault, nil
		}
		return nil, nil
	}
	ctx := context.Background()
	if err := r.SaveBudgetPlan(ctx, record.Request.Scope, record.Request.ID, plan, plan.QuotedAt); !errors.Is(err, ErrIndexPending) {
		t.Fatalf("save index failure: %v", err)
	}
	if got, err := reopen(t, table, blobs).LoadBudgetPlan(ctx, record.Request.Scope, record.Request.ID); !errors.Is(err, ErrIndexPending) || got.Version != 0 {
		t.Fatalf("lagging discovery exposed usable plan: %v", err)
	}
	table.hook = nil
	if _, err := reopen(t, table, blobs).LoadBudgetPlan(ctx, record.Request.Scope, record.Request.ID); err != nil {
		t.Fatal(err)
	}
	index, err := table.Get(ctx, r.indexKey(record.Request.ID))
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := r.decodeIndex(index)
	if err != nil || decoded.Revision != record.Revision+1 {
		t.Fatal("discovery still points to earlier state")
	}
}

func TestBudgetPlanMissingIsDistinctFromWrongScopeCorruptionAndLaterPhases(t *testing.T) {
	r, _, _, record, plan := budgetPlanFixture(t)
	ctx := context.Background()
	if _, err := r.LoadBudgetPlan(ctx, record.Request.Scope, record.Request.ID); !errors.Is(err, ErrBudgetPlanMissing) {
		t.Fatalf("initial planning miss: %v", err)
	}
	wrong := record.Request.Scope
	wrong.Project = "other"
	if _, err := r.LoadBudgetPlan(ctx, wrong, record.Request.ID); !errors.Is(err, contracts.ErrNotFound) || errors.Is(err, ErrBudgetPlanMissing) {
		t.Fatalf("scope isolation: %v", err)
	}
	if err := r.SaveBudgetPlan(ctx, wrong, record.Request.ID, plan, plan.QuotedAt); !errors.Is(err, contracts.ErrNotFound) {
		t.Fatal(err)
	}
	for _, progress := range []string{`{"version":1,"budget_plan":null}`, `{"version":2}`, `{"version":1,"budget_plan":{"version":2}}`} {
		_, _, _, other, _ := budgetPlanFixture(t)
		// Use a distinct operation in the same repository for each corrupt case.
		other, err := r.BeginOperation(ctx, Operation{Scope: record.Request.Scope, Kind: "generate", Key: string(other.Request.ID), Manifest: record.Request.Manifest, Now: record.UpdatedAt})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := r.TryUpdate(ctx, other.Request.Scope, other.Request.ID, Update{ExpectedRevision: other.Revision, Token: "corrupt", Status: StatusRunning, Progress: json.RawMessage(progress), UpdatedAt: other.UpdatedAt}); err != nil {
			t.Fatal(err)
		}
		if _, err := r.LoadBudgetPlan(ctx, other.Request.Scope, other.Request.ID); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("corruption became planning miss: %v", err)
		}
	}
	for _, status := range []Status{StatusProviderPending, StatusCompleted, StatusFailed, StatusOutcomeUnknown} {
		t.Run(string(status), func(t *testing.T) {
			r, _, _, record, plan := budgetPlanFixture(t)
			if err := r.SaveBudgetPlan(ctx, record.Request.Scope, record.Request.ID, plan, plan.QuotedAt); err != nil {
				t.Fatal(err)
			}
			current, _ := r.Read(ctx, record.Request.Scope, record.Request.ID)
			if _, err := r.TryUpdate(ctx, current.Request.Scope, current.Request.ID, Update{ExpectedRevision: current.Revision, Token: "later", Status: status, Progress: current.Progress, UpdatedAt: current.UpdatedAt}); err != nil {
				t.Fatal(err)
			}
			if _, err := r.LoadBudgetPlan(ctx, record.Request.Scope, record.Request.ID); !errors.Is(err, contracts.ErrConflict) {
				t.Fatal("later phase resumed initial admission", err)
			}
			if err := r.SaveBudgetPlan(ctx, record.Request.Scope, record.Request.ID, plan, plan.QuotedAt); !errors.Is(err, contracts.ErrConflict) {
				t.Fatal("later phase accepted initial plan", err)
			}
		})
	}
}

func TestBudgetPlanValidationRejectsUnsafeBindingsBeforeWriting(t *testing.T) {
	r, table, blobs, record, original := budgetPlanFixture(t)
	mutations := map[string]func(*BudgetPlan){
		"version": func(p *BudgetPlan) { p.Version++ }, "kind": func(p *BudgetPlan) { p.Kind = "query" },
		"config": func(p *BudgetPlan) { p.ConfigDigest = [32]byte{} }, "input": func(p *BudgetPlan) { p.RequestDigest = [32]byte{} },
		"capability": func(p *BudgetPlan) { p.CapabilityVersion = "" }, "class": func(p *BudgetPlan) { p.AttemptedClass = "default" },
		"operation": func(p *BudgetPlan) { p.Reservation.OperationID = "other" }, "generation": func(p *BudgetPlan) { p.Reservation.GenerationID = "other" },
		"expiry": func(p *BudgetPlan) { p.Reservation.ExpiresAt = p.QuotedAt }, "overflow": func(p *BudgetPlan) { p.QuotedAt = time.Unix(1<<62, 0) },
		"provider": func(p *BudgetPlan) { p.Quote.Entry.Provider = "other" }, "tier": func(p *BudgetPlan) { p.ProviderTier = "other" },
		"region": func(p *BudgetPlan) { p.Route.CacheIdentity.Region = "other" }, "compiler": func(p *BudgetPlan) { p.CompilerVersion = "other" },
		"digest": func(p *BudgetPlan) { p.Quote.CatalogDigest = strings.Repeat("0", 64) }, "price-version": func(p *BudgetPlan) { p.Route.PriceVersion = "other" },
		"expired-price": func(p *BudgetPlan) { p.Quote.Entry.EffectiveUntil = p.QuotedAt }, "tokens": func(p *BudgetPlan) { p.Estimate.InputTokens = -1 },
		"unknown-input-price": func(p *BudgetPlan) {
			p.Quote.Entry.UnknownComponents = []pricing.PriceComponent{pricing.PriceComponentInput}
		},
		"unknown-request-price": func(p *BudgetPlan) {
			p.Quote.Entry.Prices.PerRequest = pricing.DecimalUSD{}
			p.Quote.Entry.UnknownComponents = []pricing.PriceComponent{pricing.PriceComponentPerRequest}
		},
		"free": func(p *BudgetPlan) { p.Estimate.CostUSD = pricing.USD{} }, "unmatched": func(p *BudgetPlan) { p.Reservation.Reservations = nil },
		"amount":        func(p *BudgetPlan) { p.Reservation.Reservations[0].AmountUSD = pricing.MustUSD("1") },
		"legacy-amount": func(p *BudgetPlan) { p.Reservation.Reservations[0].Amount++ },
		"bucket":        func(p *BudgetPlan) { p.Reservation.Reservations[0].Bucket++ }, "bucket-zero": func(p *BudgetPlan) { p.Reservation.Reservations[0].BucketNanos = 0 },
		"window-geometry": func(p *BudgetPlan) { p.Reservation.Reservations[0].DurationNanos++ },
		"window-overflow": func(p *BudgetPlan) { p.Reservation.Reservations[0].DurationNanos = 1 << 62 },
		"duplicate-window": func(p *BudgetPlan) {
			p.Reservation.Reservations = append(p.Reservation.Reservations, p.Reservation.Reservations[0])
		},
		"tiny-limit": func(p *BudgetPlan) { p.Reservation.Reservations[0].LimitUSD = pricing.MustUSD("0.0000000001") },
	}
	table.hook = func(string, kv.KeyValueItem) (error, error) { t.Error("invalid plan wrote a row"); return nil, nil }
	blobs.hook = func(blob.BlobKey) (error, error) { t.Error("invalid plan wrote a blob"); return nil, nil }
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			plan := copyTestBudgetPlan(t, original)
			mutate(&plan)
			if err := r.SaveBudgetPlan(context.Background(), record.Request.Scope, record.Request.ID, plan, original.QuotedAt); !errors.Is(err, ErrInvalid) {
				t.Fatalf("invalid %s: %v", name, err)
			}
		})
	}
	for _, ctx := range []context.Context{nil, canceledBudgetPlanContext()} {
		if err := r.SaveBudgetPlan(ctx, record.Request.Scope, record.Request.ID, original, original.QuotedAt); err == nil {
			t.Fatal("invalid context persisted a plan")
		}
	}
}

func canceledBudgetPlanContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func TestBudgetPlanCannotReplaceSavedValueOrStartDuringFinalization(t *testing.T) {
	for _, phase := range []string{"budget_plan", "checkpoint_finalization", "finalization_handoff"} {
		t.Run(phase, func(t *testing.T) {
			r, _, _, record, plan := budgetPlanFixture(t)
			ctx := context.Background()
			if phase == "budget_plan" {
				if err := r.SaveBudgetPlan(ctx, record.Request.Scope, record.Request.ID, plan, plan.QuotedAt); err != nil {
					t.Fatal(err)
				}
				plan.ConfigDigest[0]++
			} else {
				progress, _ := json.Marshal(map[string]any{"version": 1, phase: map[string]any{"version": 1}})
				if _, err := r.TryUpdate(ctx, record.Request.Scope, record.Request.ID, Update{ExpectedRevision: record.Revision, Token: "finalizing", Status: StatusRunning, Progress: progress, UpdatedAt: plan.QuotedAt}); err != nil {
					t.Fatal(err)
				}
				if _, err := r.LoadBudgetPlan(ctx, record.Request.Scope, record.Request.ID); !errors.Is(err, contracts.ErrConflict) {
					t.Fatal("finalizing became planning miss", err)
				}
			}
			if err := r.SaveBudgetPlan(ctx, record.Request.Scope, record.Request.ID, plan, plan.QuotedAt); !errors.Is(err, contracts.ErrConflict) {
				t.Fatalf("earlier phases resumed: %v", err)
			}
		})
	}
}

func TestBudgetPlanPrivateFactsRemainEncrypted(t *testing.T) {
	r, table, blobs, record, plan := budgetPlanFixture(t)
	if err := r.SaveBudgetPlan(context.Background(), record.Request.Scope, record.Request.ID, plan, plan.QuotedAt); err != nil {
		t.Fatal(err)
	}
	for _, row := range table.rows {
		for _, field := range row.Item.Fields {
			if data, ok := field.(kv.KeyValueBytes); ok && strings.Contains(string(data), "private-") {
				t.Fatal("private planning data leaked in KV")
			}
		}
	}
	for _, data := range blobs.values {
		if strings.Contains(string(data), "private-") {
			t.Fatal("private planning data leaked in blob storage")
		}
	}
}

func TestBudgetPlanExplicitUnreservedModes(t *testing.T) {
	for _, mode := range []BudgetMode{BudgetFree, BudgetUnmatched} {
		t.Run(string(mode), func(t *testing.T) {
			r, table, blobs, record, plan := budgetPlanFixture(t)
			plan.Mode = mode
			plan.Reservation.Reservations = nil
			if mode == BudgetFree {
				plan.Quote.Entry.Prices = pricing.UnitPrices{}
				plan.Estimate.CostUSD = pricing.MustUSD("0")
				plan.Estimate.MicroUSD = 0
			}
			if err := r.SaveBudgetPlan(context.Background(), record.Request.Scope, record.Request.ID, plan, plan.QuotedAt); err != nil {
				t.Fatal(err)
			}
			restored, err := reopen(t, table, blobs).LoadBudgetPlan(context.Background(), record.Request.Scope, record.Request.ID)
			if err != nil || !equalExecutionJSON(plan, restored) {
				t.Fatal("mode changed on restart", err)
			}
			for _, mutate := range []func(*BudgetPlan){
				func(p *BudgetPlan) { p.Mode = "" },
				func(p *BudgetPlan) { p.Mode = BudgetReserved },
				func(p *BudgetPlan) { p.Unpriced = true },
			} {
				invalid := copyTestBudgetPlan(t, plan)
				mutate(&invalid)
				if invalid.Validate() == nil {
					t.Fatal("malformed unreserved plan accepted")
				}
			}
			if mode == BudgetFree {
				plan.Quote.Entry.Prices.PerRequest = pricing.MustDecimalUSD("0.01")
				if plan.Validate() == nil {
					t.Fatal("zero estimate masked paid quote")
				}
			}
		})
	}
}
