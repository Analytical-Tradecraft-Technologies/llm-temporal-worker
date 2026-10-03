package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/routing"
	"github.com/mfow/llm-temporal-worker/golang/storage/cloudstate"
	"github.com/mfow/llm-temporal-worker/golang/storage/durable"
)

// Linearizable durable-plan boundary with before/after failure injection.
// Repository CAS/encryption/index repair have separate cloudstate coverage.
type admissionPlanStore struct {
	CloudRequestRepository
	mu                                      sync.Mutex
	record                                  cloudstate.Record
	data                                    []byte
	readErr, loadErr, saveBefore, saveAfter error
	winner                                  []byte
	saves, loads                            int
}

func (s *admissionPlanStore) Read(_ context.Context, scope cloudstate.Scope, id cloudstate.RequestID) (cloudstate.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.readErr != nil {
		return cloudstate.Record{}, s.readErr
	}
	if scope != s.record.Request.Scope || id != s.record.Request.ID {
		return cloudstate.Record{}, contracts.ErrNotFound
	}
	r := s.record
	r.Request.Manifest = append(json.RawMessage(nil), r.Request.Manifest...)
	return r, nil
}
func (s *admissionPlanStore) LoadBudgetPlan(_ context.Context, scope cloudstate.Scope, id cloudstate.RequestID) (cloudstate.BudgetPlan, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loads++
	if s.loadErr != nil {
		return cloudstate.BudgetPlan{}, s.loadErr
	}
	if s.record.Request.Scope != scope || s.record.Request.ID != id || s.record.Status != cloudstate.StatusRunning {
		return cloudstate.BudgetPlan{}, contracts.ErrConflict
	}
	if s.data == nil {
		return cloudstate.BudgetPlan{}, cloudstate.ErrBudgetPlanMissing
	}
	var plan cloudstate.BudgetPlan
	err := json.Unmarshal(s.data, &plan)
	return plan, err
}
func (s *admissionPlanStore) SaveBudgetPlan(_ context.Context, scope cloudstate.Scope, id cloudstate.RequestID, plan cloudstate.BudgetPlan, _ time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saves++
	if s.saveBefore != nil {
		return s.saveBefore
	}
	if scope != s.record.Request.Scope || id != s.record.Request.ID || s.record.Status != cloudstate.StatusRunning {
		return contracts.ErrConflict
	}
	if s.winner != nil {
		s.data, s.winner = s.winner, nil
	}
	if s.data != nil {
		return contracts.ErrConflict
	}
	s.data, _ = json.Marshal(plan)
	return s.saveAfter
}

type cloudAdmissionFixture struct {
	*budgetPlanningFixture
	store  *admissionPlanStore
	helper *CloudBudgetAdmission
	leaser *admissionLeaser
	replay durable.CompactReplay
}

func newCloudAdmissionFixture(t *testing.T, kind string, configure ...func(*budgetPlanningFixture)) *cloudAdmissionFixture {
	t.Helper()
	f := &cloudAdmissionFixture{budgetPlanningFixture: newBudgetPlanningFixture(t)}
	for _, change := range configure {
		change(f.budgetPlanningFixture)
	}
	_, _, _, _, f.replay, _ = planningFixture()
	// Keep preparation identical to the original stored request. The budget
	// fixture's synthetic small output bound is not used by this integration.
	var input any = f.gen
	if kind == "compact" {
		input = f.compact
	}
	data, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	id, _ := cloudstate.NewRequestID()
	f.store = &admissionPlanStore{record: cloudstate.Record{Request: cloudstate.CreateRequest{
		ID: id, Scope: cloudstate.Scope{Tenant: "tenant", Project: "project"}, Kind: kind, Manifest: data, CreatedAt: f.attempt.QuotedAt}, Status: cloudstate.StatusRunning}}
	reference, err := durable.NewReferenceBudgetMaterializer(f.attempt.GenerationID, "incarnation-1", func() time.Time { return f.attempt.QuotedAt })
	if err != nil {
		t.Fatal(err)
	}
	f.leaser = &admissionLeaser{BudgetLeaser: reference}
	composition := validCapabilityComposition()
	composition.Identity.ConfigDigest = f.cap.ConfigDigest
	composition.Identity.Postgres = durable.PostgresIdentity{}
	composition.Identity.Cloud = durable.CloudIdentity{Provider: "aws", Namespace: "requests-v1", RequestTable: "requests", PayloadStore: "payloads", ProviderDigest: [32]byte{2}}
	composition.Materializer = f.leaser
	f.cap.CloudIdentity, f.cap.composition = composition.Identity.Cloud, &composition
	f.cap.RedisIdentity, f.cap.Budgets = composition.Identity.Redis, f.leaser
	f.cap.Requests, f.cap.BudgetEstimator, f.cap.MaxBudgetBucketsPerWindow = f.store, f.estimator, 100
	f.helper, err = f.cap.NewCloudBudgetAdmission(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return f
}
func (f *cloudAdmissionFixture) prepare(ctx context.Context, attempt BudgetAttempt) (*CloudBudgetCall, error) {
	r := f.store.record.Request
	if r.Kind == "generate" {
		return f.helper.PrepareGenerate(ctx, r.Scope, r.ID, durable.GenerateReplay{}, attempt)
	}
	return f.helper.PrepareCompact(ctx, r.Scope, r.ID, f.replay, attempt)
}

func TestCloudBudgetAdmissionBothKindsPersistBeforeAcceptanceAndClaim(t *testing.T) {
	for _, kind := range []string{"generate", "compact"} {
		t.Run(kind, func(t *testing.T) {
			f := newCloudAdmissionFixture(t, kind)
			accepts := 0
			f.leaser.accept = func(ctx context.Context, req durable.ReserveRequest) (durable.ReserveResult, error) {
				accepts++
				if f.store.data == nil {
					t.Fatal("Redis accepted before plan persistence")
				}
				var saved cloudstate.BudgetPlan
				_ = json.Unmarshal(f.store.data, &saved)
				if !reflect.DeepEqual(saved.Reservation, req) {
					t.Fatal("Redis received a replacement quote")
				}
				return f.leaser.BudgetLeaser.Accept(ctx, req)
			}
			call, err := f.prepare(context.Background(), f.attempt)
			if err != nil {
				t.Fatal(err)
			}
			if accepts != 0 || f.store.saves != 1 || f.source.reads != 1 {
				t.Fatal("preparation acquired budget or recaptured snapshot")
			}
			plan := call.Plan()
			plan.Reservation.Reservations[0].WindowID = "mutated"
			accepted, err := f.helper.Reserve(context.Background(), call)
			if err != nil || !accepted.Accepted {
				t.Fatalf("reserve: %v", err)
			}
			receipt, err := f.helper.Claim(context.Background(), call, accepted)
			if err != nil || receipt.Validate(accepted) != nil {
				t.Fatalf("claim: %v", err)
			}
			_, err = f.helper.Claim(context.Background(), call, accepted)
			assertBudgetAdmissionError(t, err, provider.CodeAmbiguousDispatch, provider.DispatchAmbiguous, provider.RetryNever)
		})
	}
}

func TestCloudBudgetAdmissionRestartAfterLostRedisReplyRetainsExactPlan(t *testing.T) {
	f := newCloudAdmissionFixture(t, "generate")
	ctx := context.Background()
	call, err := f.prepare(ctx, f.attempt)
	if err != nil {
		t.Fatal(err)
	}
	original := call.Plan()
	f.leaser.accept = func(ctx context.Context, r durable.ReserveRequest) (durable.ReserveResult, error) {
		_, err := f.leaser.BudgetLeaser.Accept(ctx, r)
		if err != nil {
			return durable.ReserveResult{}, err
		}
		return durable.ReserveResult{}, errors.New("sensitive lost Redis reply")
	}
	_, err = f.helper.Reserve(ctx, call)
	assertBudgetAdmissionError(t, err, provider.CodeStateUnavailable, provider.DispatchNotDispatched, provider.RetrySameOperation)
	f.leaser.accept = nil
	// The new process sees changed live prices but the same immutable config
	// identity. No price lookup or selection may replace the stored quote.
	f.cap.Planner = planningPlannerFunc(func(context.Context, routing.Input) (routing.Plan, error) { panic("recovery selected a new route") })
	f.cap.BudgetEstimator.Tokenizer = func(llm.Request, routing.Candidate) (int64, error) { panic("recovery requoted") }
	changed := f.entry
	changed.Prices.InputPerMillion = pricing.MustDecimalUSD("99")
	f.prices(t, []pricing.Entry{changed})
	f.helper, err = f.cap.NewCloudBudgetAdmission(ctx)
	if err != nil {
		t.Fatal(err)
	}
	replacement := f.attempt
	replacement.OperationID = "must-not-use"
	replacement.QuotedAt = replacement.QuotedAt.Add(time.Minute)
	replacement.ExpiresAt = replacement.ExpiresAt.Add(time.Hour)
	resumed, err := f.prepare(ctx, replacement)
	if err != nil || !reflect.DeepEqual(resumed.Plan(), original) || resumed.Provider().Call.OperationKey != f.gen.OperationKey {
		t.Fatalf("restart changed saved authority: %v", err)
	}
	accepted, err := f.helper.Reserve(ctx, resumed)
	if err != nil || !accepted.Accepted {
		t.Fatalf("restart reserve: %v", err)
	}
	if _, err = f.helper.Claim(ctx, resumed, accepted); err != nil {
		t.Fatal(err)
	}
	if f.store.saves != 1 {
		t.Fatal("recovery overwrote plan")
	}
}

func TestCloudBudgetAdmissionSaveAcknowledgementAndCompetingWinner(t *testing.T) {
	for _, scenario := range []string{"lost save reply", "competing plan"} {
		t.Run(scenario, func(t *testing.T) {
			f := newCloudAdmissionFixture(t, "generate")
			ctx := context.Background()
			if scenario == "lost save reply" {
				f.store.saveAfter = errors.New("sensitive lost save reply")
				if call, err := f.prepare(ctx, f.attempt); err == nil || call != nil {
					t.Fatal("uncertain save supplied usable call")
				}
				f.store.saveAfter = nil
			} else {
				prepared, err := PrepareGenerateInput(ctx, f.gen, durable.GenerateReplay{})
				if err != nil {
					t.Fatal(err)
				}
				winner := f.attempt
				winner.OperationID = "winning-attempt"
				planned, err := f.helper.planning.Generate(ctx, prepared, winner)
				if err != nil {
					t.Fatal(err)
				}
				plan, err := planned.BudgetPlan("generate")
				if err != nil {
					t.Fatal(err)
				}
				f.store.winner, _ = json.Marshal(plan)
			}
			call, err := f.prepare(ctx, f.attempt)
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "competing plan" && call.Plan().Route.OperationID != "winning-attempt" {
				t.Fatal("loser's identity survived durable CAS")
			}
			accepted, err := f.helper.Reserve(ctx, call)
			if err != nil || !accepted.Accepted {
				t.Fatalf("recovered winner: %v", err)
			}
		})
	}
}

func TestCloudBudgetAdmissionStorageFailuresNeverReachRedis(t *testing.T) {
	for _, scenario := range []string{"read failure", "missing request", "missing blob", "load failure", "save failure", "corrupt plan", "pending provider", "unknown outcome", "completed", "wrong kind", "wrong scope", "bad manifest", "old quote"} {
		t.Run(scenario, func(t *testing.T) {
			f := newCloudAdmissionFixture(t, "generate")
			f.leaser.accept = func(context.Context, durable.ReserveRequest) (durable.ReserveResult, error) {
				t.Fatal("invalid preparation reached Redis")
				return durable.ReserveResult{}, nil
			}
			scope, id := f.store.record.Request.Scope, f.store.record.Request.ID
			switch scenario {
			case "read failure":
				f.store.readErr = errors.New("sensitive read details")
			case "missing request":
				f.store.readErr = contracts.ErrNotFound
			case "missing blob":
				f.store.loadErr = contracts.ErrNotFound
			case "load failure":
				f.store.loadErr = errors.New("sensitive load details")
			case "save failure":
				f.store.saveBefore = errors.New("sensitive save details")
			case "corrupt plan":
				f.store.data = []byte(`{"version":999}`)
			case "pending provider":
				f.store.record.Status = cloudstate.StatusProviderPending
			case "unknown outcome":
				f.store.record.Status = cloudstate.StatusOutcomeUnknown
			case "completed":
				f.store.record.Status = cloudstate.StatusCompleted
			case "wrong kind":
				f.store.record.Request.Kind = "compact"
			case "wrong scope":
				scope.Tenant = "other"
			case "bad manifest":
				f.store.record.Request.Manifest = []byte(`{"wrong":true}`)
			case "old quote":
				f.attempt.QuotedAt = f.attempt.QuotedAt.Add(-time.Hour)
			}
			call, err := f.helper.PrepareGenerate(context.Background(), scope, id, durable.GenerateReplay{}, f.attempt)
			if err == nil || call != nil {
				t.Fatal("invalid preparation returned a usable call")
			}
			var mapped *provider.Error
			if !errors.As(err, &mapped) || mapped.Cause != nil || mapped.Dispatch != provider.DispatchNotDispatched {
				t.Fatalf("unsafe error: %#v", err)
			}
		})
	}
}

func TestCloudBudgetAdmissionRechecksPlanBeforeReserveAndClaim(t *testing.T) {
	for _, phase := range []string{"reserve", "claim"} {
		for _, change := range []string{"status", "plan", "index", "canceled", "foreign call"} {
			t.Run(phase+"/"+change, func(t *testing.T) {
				f := newCloudAdmissionFixture(t, "generate")
				ctx := context.Background()
				call, err := f.prepare(ctx, f.attempt)
				if err != nil {
					t.Fatal(err)
				}
				var accepted durable.ReserveResult
				if phase == "claim" {
					accepted, err = f.helper.Reserve(ctx, call)
					if err != nil {
						t.Fatal(err)
					}
				}
				f.leaser.accept = func(context.Context, durable.ReserveRequest) (durable.ReserveResult, error) {
					t.Fatal("invalid reserve reached Redis")
					return durable.ReserveResult{}, nil
				}
				f.leaser.claim = func(context.Context, durable.ClaimRequest) (durable.ClaimReceipt, error) {
					t.Fatal("invalid claim reached Redis")
					return durable.ClaimReceipt{}, nil
				}
				switch change {
				case "status":
					f.store.record.Status = cloudstate.StatusProviderPending
				case "plan":
					plan := call.Plan()
					plan.Reservation.ExpiresAt = plan.Reservation.ExpiresAt.Add(time.Hour)
					f.store.data, _ = json.Marshal(plan)
				case "index":
					f.store.loadErr = cloudstate.ErrIndexPending
				case "canceled":
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					cancel()
				case "foreign call":
					call.owner = &CloudBudgetAdmission{}
				}
				if phase == "reserve" {
					_, err = f.helper.Reserve(ctx, call)
				} else {
					_, err = f.helper.Claim(ctx, call, accepted)
				}
				if err == nil {
					t.Fatal("invalid plan crossed admission boundary")
				}
			})
		}
	}
}

func TestCloudBudgetAdmissionConcurrentPlansConvergeAndClaimOnce(t *testing.T) {
	f := newCloudAdmissionFixture(t, "generate")
	ctx := context.Background()
	const count = 20
	var wg sync.WaitGroup
	calls := make(chan *CloudBudgetCall, count)
	for i := range count {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			attempt := f.attempt
			attempt.OperationID = durable.OperationID(fmt.Sprintf("attempt-%d", i))
			call, err := f.prepare(ctx, attempt)
			if err != nil {
				t.Error(err)
				return
			}
			calls <- call
		}(i)
	}
	wg.Wait()
	close(calls)
	var claims atomic.Int32
	for call := range calls {
		wg.Add(1)
		go func(call *CloudBudgetCall) {
			defer wg.Done()
			accepted, err := f.helper.Reserve(ctx, call)
			if err != nil {
				t.Error(err)
				return
			}
			if _, err = f.helper.Claim(ctx, call, accepted); err == nil {
				claims.Add(1)
			} else {
				var mapped *provider.Error
				if !errors.As(err, &mapped) || mapped.Code != provider.CodeAmbiguousDispatch {
					t.Error(err)
				}
			}
		}(call)
	}
	wg.Wait()
	if claims.Load() != 1 {
		t.Fatalf("paid dispatch grants: %d", claims.Load())
	}
}

func TestCloudBudgetAdmissionWaitAndLeaseExpiryDoNotBecomeDispatchGrants(t *testing.T) {
	f := newCloudAdmissionFixture(t, "generate")
	ctx := context.Background()
	call, err := f.prepare(ctx, f.attempt)
	if err != nil {
		t.Fatal(err)
	}
	// Saturate the same budget windows with another operation first.
	busy := call.Plan().Reservation
	busy.OperationID = "other-operation"
	for i := range busy.Reservations {
		busy.Reservations[i].AmountUSD = busy.Reservations[i].LimitUSD
	}
	if result, err := f.leaser.Accept(ctx, busy); err != nil || !result.Accepted {
		t.Fatalf("saturate: %v", err)
	}
	wait, err := f.helper.Reserve(ctx, call)
	if err != nil || wait.Accepted || wait.RetryAfter <= 0 {
		t.Fatalf("expected wait: %+v %v", wait, err)
	}
	if _, err := f.helper.Claim(ctx, call, wait); err == nil {
		t.Fatal("wait granted dispatch")
	}
	// A separate fixture proves the accepted claim's start deadline expires.
	f = newCloudAdmissionFixture(t, "generate")
	call, err = f.prepare(ctx, f.attempt)
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := f.helper.Reserve(ctx, call)
	if err != nil {
		t.Fatal(err)
	}
	f.attempt.QuotedAt = f.attempt.QuotedAt.Add(16 * time.Minute)
	_, err = f.helper.Claim(ctx, call, accepted)
	assertBudgetAdmissionError(t, err, provider.CodeBudgetDenied, provider.DispatchNotDispatched, provider.RetryAfter)
}

func TestCloudBudgetAdmissionIndependentAttemptsUseDifferentProviderKeys(t *testing.T) {
	for _, kind := range []string{"generate", "compact"} {
		t.Run(kind, func(t *testing.T) {
			f := newCloudAdmissionFixture(t, kind)
			root, _ := cloudstate.NewRequestID()
			var previous cloudstate.RequestID
			var previousKey string
			original := string(f.store.record.Request.Manifest)
			for number := uint64(1); number <= 2; number++ {
				id, _ := cloudstate.NewRequestID()
				f.store.record.Request.ID = id
				f.store.record.Progress, _ = json.Marshal(map[string]any{"version": 1, "attempt_parent": cloudstate.RequestAttempt{Version: 1, RootID: root, ID: id, PreviousID: previous, Number: number, CreatedAt: f.store.record.Request.CreatedAt}})
				f.store.data = nil
				attempt := f.attempt
				attempt.OperationID = durable.OperationID(id)
				wrong := attempt
				wrong.OperationID = durable.OperationID(root)
				if _, err := f.prepare(context.Background(), wrong); err == nil || f.store.data != nil {
					t.Fatal("wrong attempt identity persisted a plan", err)
				}
				call, err := f.prepare(context.Background(), attempt)
				if err != nil {
					t.Fatal(err)
				}
				key := call.Provider().Call.OperationKey
				if key == previousKey || key == f.gen.OperationKey || key == f.compact.OperationKey {
					t.Fatal("paid attempts shared provider idempotency key")
				}
				replay, err := f.prepare(context.Background(), attempt)
				if err != nil || replay.Provider().Call.OperationKey != key {
					t.Fatal("recovery changed attempt key", err)
				}
				if string(f.store.record.Request.Manifest) != original {
					t.Fatal("public operation key changed")
				}
				previous, previousKey = id, key
			}
		})
	}
}
