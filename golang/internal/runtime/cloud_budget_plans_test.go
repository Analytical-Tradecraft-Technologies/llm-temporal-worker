package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/storage/cloudstate"
	"github.com/mfow/llm-temporal-worker/golang/storage/durable"
)

type budgetPlanStoreStub struct {
	data  []byte
	err   error
	saves int
}

func (*budgetPlanStoreStub) BeginOperation(context.Context, cloudstate.Operation) (cloudstate.Record, error) {
	panic("plan helper must not create operations")
}
func (*budgetPlanStoreStub) CompleteOperation(context.Context, cloudstate.Scope, cloudstate.RequestID, json.RawMessage, time.Time) (cloudstate.Record, error) {
	panic("plan helper must not complete operations")
}
func (*budgetPlanStoreStub) Probe(context.Context) error { return nil }
func (s *budgetPlanStoreStub) SaveBudgetPlan(_ context.Context, _ cloudstate.Scope, _ cloudstate.RequestID, plan cloudstate.BudgetPlan, _ time.Time) error {
	s.saves++
	if s.err != nil {
		return s.err
	}
	var err error
	s.data, err = json.Marshal(plan)
	return err
}
func (s *budgetPlanStoreStub) LoadBudgetPlan(context.Context, cloudstate.Scope, cloudstate.RequestID) (cloudstate.BudgetPlan, error) {
	if s.err != nil {
		return cloudstate.BudgetPlan{}, s.err
	}
	if s.data == nil {
		return cloudstate.BudgetPlan{}, cloudstate.ErrBudgetPlanMissing
	}
	var result cloudstate.BudgetPlan
	err := json.Unmarshal(s.data, &result)
	return result, err
}

func TestCloudBudgetPlansProjectBothActivitiesWithoutSDKObjects(t *testing.T) {
	for _, kind := range []string{"generate", "compact"} {
		t.Run(kind, func(t *testing.T) {
			f := newBudgetPlanningFixture(t)
			planning := f.planning(t)
			var planned PlannedBudgetCall
			var err error
			if kind == "generate" {
				planned, err = planning.Generate(context.Background(), f.generate, f.attempt)
			} else {
				planned, err = planning.Compact(context.Background(), f.summary, f.attempt)
			}
			if err != nil {
				t.Fatal(err)
			}
			plan, err := planned.BudgetPlan(kind)
			if err != nil || plan.Kind != kind || plan.RequestDigest != planned.Provider.Call.Metadata.SchemaDigest || plan.ConfigDigest != f.cap.ConfigDigest ||
				plan.Route != planned.Route || plan.RequestedClass != planned.Provider.Candidate.RequestedClass || plan.AttemptedClass != planned.Provider.Candidate.AttemptedClass ||
				!reflect.DeepEqual(plan.Reservation, planned.Reservation) {
				t.Fatalf("durable projection changed bindings: %v", err)
			}
			data, _ := json.Marshal(plan)
			for _, secret := range []string{"sensitive prompt", "sensitive history", "SDKParams", "Adapter", "operation_key"} {
				if strings.Contains(string(data), secret) {
					t.Fatal("durable projection included invocation-local data")
				}
			}
			plan.Reservation.Reservations[0].WindowID = "changed"
			if planned.Reservation.Reservations[0].WindowID == "changed" {
				t.Fatal("projection shares reservation backing array")
			}
		})
	}
}

func TestCloudBudgetPlansStoredQuoteSurvivesReloadAndUncertainAdmission(t *testing.T) {
	f := newBudgetPlanningFixture(t)
	planned, err := f.planning(t).Generate(context.Background(), f.generate, f.attempt)
	if err != nil {
		t.Fatal(err)
	}
	store := &budgetPlanStoreStub{}
	f.cap.Requests = store
	plans, err := f.cap.NewCloudBudgetPlans()
	if err != nil {
		t.Fatal(err)
	}
	id, _ := cloudstate.NewRequestID()
	scope := cloudstate.Scope{Tenant: f.gen.Context.Tenant, Project: f.gen.Context.Project}
	ctx := context.Background()
	if _, err := plans.Save(ctx, scope, id, "generate", planned, f.attempt.QuotedAt); err != nil {
		t.Fatal(err)
	}
	admissionFixture := newBudgetAdmissionFixture(t)
	admissionFixture.gen, admissionFixture.route, admissionFixture.plan, admissionFixture.now = f.gen, planned.Route, planned.Reservation, planned.QuotedAt
	first := true
	admissionFixture.leaser.accept = func(ctx context.Context, request durable.ReserveRequest) (durable.ReserveResult, error) {
		if !reflect.DeepEqual(request, planned.Reservation) {
			t.Fatal("stored quote changed during acceptance retry")
		}
		accepted, err := admissionFixture.leaser.BudgetLeaser.Accept(ctx, request)
		if err == nil && first {
			first = false
			return durable.ReserveResult{}, errors.New("sensitive lost reply")
		}
		return accepted, err
	}
	_, err = admissionFixture.helper.ReserveGenerate(ctx, f.gen, planned.Route)
	assertBudgetAdmissionError(t, err, provider.CodeStateUnavailable, provider.DispatchNotDispatched, provider.RetrySameOperation)
	// Reconstruct the helper with a different config and no pricing/routing
	// capabilities. Replay must load the original facts without replanning.
	restarted, err := (V1RuntimeCapabilities{Requests: &budgetPlanStoreStub{data: store.data}, ConfigDigest: [32]byte{9}}).NewCloudBudgetPlans()
	if err != nil {
		t.Fatal(err)
	}
	stored, err := restarted.Load(ctx, scope, id)
	if err != nil || stored.ConfigDigest != planned.Provider.ConfigDigest || !reflect.DeepEqual(stored.Reservation, planned.Reservation) {
		t.Fatalf("reload changed recovered plan: %v", err)
	}
	admissionFixture.now = admissionFixture.now.Add(2 * time.Minute)
	admissionFixture.route, admissionFixture.plan = stored.Route, stored.Reservation
	accepted, err := admissionFixture.helper.ReserveGenerate(ctx, f.gen, stored.Route)
	if err != nil || !accepted.Accepted {
		t.Fatalf("recovered admission failed: %v", err)
	}
	if _, err := admissionFixture.helper.ClaimGenerate(ctx, f.gen, stored.Route, accepted); err != nil {
		t.Fatal(err)
	}
	_, err = admissionFixture.helper.ClaimGenerate(ctx, f.gen, stored.Route, accepted)
	assertBudgetAdmissionError(t, err, provider.CodeAmbiguousDispatch, provider.DispatchAmbiguous, provider.RetryNever)
}

func TestCloudBudgetPlansFailClosedAndSanitizeStoreFailures(t *testing.T) {
	f := newBudgetPlanningFixture(t)
	planned, err := f.planning(t).Generate(context.Background(), f.generate, f.attempt)
	if err != nil {
		t.Fatal(err)
	}
	var missing *budgetPlanStoreStub
	for _, capabilities := range []V1RuntimeCapabilities{{}, {Requests: missing, ConfigDigest: f.cap.ConfigDigest}, {Requests: &budgetPlanStoreStub{}}} {
		if _, err := capabilities.NewCloudBudgetPlans(); err == nil {
			t.Fatal("incomplete cloud capabilities constructed a plan store")
		}
	}
	store := &budgetPlanStoreStub{}
	f.cap.Requests = store
	plans, _ := f.cap.NewCloudBudgetPlans()
	id, _ := cloudstate.NewRequestID()
	scope := cloudstate.Scope{Tenant: "tenant", Project: "project"}
	ctx := context.Background()
	if _, err := plans.Load(ctx, scope, id); !errors.Is(err, cloudstate.ErrBudgetPlanMissing) {
		t.Fatal("initial plan miss was lost", err)
	}
	for _, failure := range []error{contracts.ErrNotFound, errors.New("sensitive provider SDK failure"), cloudstate.ErrCorrupt, contracts.ErrConflict} {
		store.err = failure
		for _, action := range []func() error{
			func() error { _, err := plans.Load(ctx, scope, id); return err },
			func() error { _, err := plans.Save(ctx, scope, id, "generate", planned, planned.QuotedAt); return err },
		} {
			err := action()
			var mapped *provider.Error
			if !errors.As(err, &mapped) || errors.Is(err, cloudstate.ErrBudgetPlanMissing) || strings.Contains(err.Error(), "sensitive") ||
				mapped.Cause != nil || len(mapped.SafeDetails) != 0 || mapped.Dispatch != provider.DispatchNotDispatched {
				t.Fatalf("unsafe storage failure: %#v", err)
			}
		}
	}
	store.err = nil
	saves := store.saves
	changed := planned
	changed.Provider.ConfigDigest[0]++
	if _, err := plans.Save(ctx, scope, id, "generate", changed, planned.QuotedAt); err == nil || store.saves != saves {
		t.Fatal("mismatched config reached storage")
	}
	for _, mutate := range []func(*PlannedBudgetCall){
		func(p *PlannedBudgetCall) { p.Route.EndpointID = "other" },
		func(p *PlannedBudgetCall) { p.Quote = nil },
		func(p *PlannedBudgetCall) { p.Reservation.Reservations = nil },
		func(p *PlannedBudgetCall) { p.Provider.Adapter = nil },
	} {
		invalid := planned
		mutate(&invalid)
		if _, err := invalid.BudgetPlan("generate"); err == nil {
			t.Fatal("invalid projection succeeded")
		}
	}
	store.data = []byte(`{"version":2}`)
	if _, err := plans.Load(ctx, scope, id); err == nil {
		t.Fatal("invalid stored plan was usable")
	}
	if _, err := plans.Load(nil, scope, id); err == nil {
		t.Fatal("nil context was accepted")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := plans.Save(canceled, scope, id, "generate", planned, planned.QuotedAt); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled save reached storage", err)
	}
}
