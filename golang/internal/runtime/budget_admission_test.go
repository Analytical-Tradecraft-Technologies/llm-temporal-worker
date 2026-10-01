package runtime

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/admission"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/storage/durable"
)

type admissionLeaser struct {
	durable.BudgetLeaser
	accept func(context.Context, durable.ReserveRequest) (durable.ReserveResult, error)
	claim  func(context.Context, durable.ClaimRequest) (durable.ClaimReceipt, error)
}

func (m *admissionLeaser) Accept(ctx context.Context, r durable.ReserveRequest) (durable.ReserveResult, error) {
	if m.accept != nil {
		return m.accept(ctx, r)
	}
	return m.BudgetLeaser.Accept(ctx, r)
}
func (m *admissionLeaser) Claim(ctx context.Context, r durable.ClaimRequest) (durable.ClaimReceipt, error) {
	if m.claim != nil {
		return m.claim(ctx, r)
	}
	return m.BudgetLeaser.Claim(ctx, r)
}

type budgetAdmissionFixture struct {
	cap     V1RuntimeCapabilities
	helper  *BudgetAdmission
	leaser  *admissionLeaser
	plan    durable.ReserveRequest
	route   durable.RoutePlan
	gen     llm.GenerateRequestV1
	compact llm.CompactRequestV1
	now     time.Time
}

func newBudgetAdmissionFixture(t *testing.T) *budgetAdmissionFixture {
	t.Helper()
	f := &budgetAdmissionFixture{now: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)}
	_, _, f.gen, f.compact = checkpointReplayFixture(t)
	f.gen.Parent = nil
	f.route = durable.RoutePlan{OperationID: "operation-1", GenerationID: "generation-1", RouteID: "route-1", EndpointID: "endpoint-1", Provider: "provider", Model: "model"}
	f.plan = durable.ReserveRequest{OperationID: f.route.OperationID, GenerationID: f.route.GenerationID, Reservations: []admission.WindowReservation{{PolicyID: "policy", WindowID: "window", Bucket: f.now.UnixNano() / int64(time.Hour), BucketNanos: int64(time.Hour), DurationNanos: int64(2 * time.Hour), AmountUSD: pricing.MustUSD("0.25"), LimitUSD: pricing.MustUSD("1")}}}
	reference, err := durable.NewReferenceBudgetMaterializer(f.route.GenerationID, "incarnation-1", func() time.Time { return f.now })
	if err != nil {
		t.Fatal(err)
	}
	f.leaser = &admissionLeaser{BudgetLeaser: reference}
	composition := validCapabilityComposition()
	composition.Identity.Postgres = durable.PostgresIdentity{}
	composition.Identity.Cloud = durable.CloudIdentity{Provider: "aws", Namespace: "requests-v1", RequestTable: "requests", PayloadStore: "payloads", ProviderDigest: [32]byte{2}}
	composition.Materializer = f.leaser
	f.cap = V1RuntimeCapabilities{ConfigDigest: composition.Identity.ConfigDigest, CloudIdentity: composition.Identity.Cloud, composition: &composition}
	f.helper, err = f.cap.NewBudgetAdmission(
		func(_ context.Context, got llm.GenerateRequestV1, route durable.RoutePlan) (durable.ReserveRequest, error) {
			if !reflect.DeepEqual(got, f.gen) || route != f.route {
				t.Fatal("Generate planner lost request or route")
			}
			return f.plan, nil
		},
		func(_ context.Context, got llm.CompactRequestV1, route durable.RoutePlan) (durable.ReserveRequest, error) {
			if !reflect.DeepEqual(got, f.compact) || route != f.route {
				t.Fatal("Compact planner lost request or route")
			}
			return f.plan, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func assertBudgetAdmissionError(t *testing.T, err error, code provider.Code, dispatch provider.DispatchCertainty, retry provider.RetryDisposition) {
	t.Helper()
	var mapped *provider.Error
	if !errors.As(err, &mapped) || mapped.Code != code || mapped.Phase != provider.PhaseAdmission || mapped.Dispatch != dispatch || mapped.Retry != retry {
		t.Fatalf("error = %#v, want %s/%s/%s", err, code, dispatch, retry)
	}
	if strings.Contains(err.Error(), "sensitive") || mapped.Cause != nil || len(mapped.SafeDetails) != 0 {
		t.Fatal("exposed planner or Redis error")
	}
}

func TestBudgetAdmissionRequiresBoundSnapshotAndBothPlanners(t *testing.T) {
	f := newBudgetAdmissionFixture(t)
	for _, name := range []string{"unbound", "digest", "cloud", "leaser", "typed nil", "Generate planner", "Compact planner"} {
		t.Run(name, func(t *testing.T) {
			cap := f.cap
			composition := *cap.composition
			cap.composition = &composition
			g, c := f.helper.generate, f.helper.compact
			cap.CompositionFactory = func(context.Context, V1RuntimeCapabilities) (durable.Composition, error) {
				t.Fatal("adapter invoked composition factory")
				return durable.Composition{}, nil
			}
			switch name {
			case "unbound":
				cap.composition = nil
			case "digest":
				cap.ConfigDigest = [32]byte{3}
			case "cloud":
				cap.CloudIdentity.Namespace = "other"
			case "leaser":
				composition.Materializer = nil
			case "typed nil":
				var m *admissionLeaser
				composition.Materializer = m
			case "Generate planner":
				g = nil
			case "Compact planner":
				c = nil
			}
			if _, err := cap.NewBudgetAdmission(g, c); err == nil {
				t.Fatal("accepted incomplete or mixed snapshot")
			}
		})
	}
}

func TestBudgetAdmissionBothPhasesShareIdempotentReservationAndSingleUseClaim(t *testing.T) {
	f := newBudgetAdmissionFixture(t)
	ctx := context.Background()
	first, err := f.helper.ReserveGenerate(ctx, f.gen, f.route)
	if err != nil || !first.Accepted {
		t.Fatalf("Generate reserve = %+v, %v", first, err)
	}
	second, err := f.helper.ReserveCompact(ctx, f.compact, f.route)
	if err != nil || !reflect.DeepEqual(first, second) {
		t.Fatalf("Compact changed admission = %+v, %v", second, err)
	}
	// Claim must not re-run a quote or introduce a new acceptance fingerprint.
	f.helper.generate = func(context.Context, llm.GenerateRequestV1, durable.RoutePlan) (durable.ReserveRequest, error) {
		t.Fatal("claim re-ran Generate planner")
		return durable.ReserveRequest{}, nil
	}
	receipt, err := f.helper.ClaimGenerate(ctx, f.gen, f.route, first)
	if err != nil || receipt.Validate(first) != nil {
		t.Fatalf("claim = %+v, %v", receipt, err)
	}
	_, err = f.helper.ClaimCompact(ctx, f.compact, f.route, second)
	assertBudgetAdmissionError(t, err, provider.CodeAmbiguousDispatch, provider.DispatchAmbiguous, provider.RetryNever)
	// Start never refunds budget. Another operation still sees the charge.
	f.route.OperationID = "followup"
	f.plan.OperationID = f.route.OperationID
	f.plan.Reservations[0].AmountUSD = pricing.MustUSD("0.80")
	denied, err := f.helper.ReserveCompact(ctx, f.compact, f.route)
	if err != nil || denied.Accepted || denied.RetryAfter <= 0 || len(denied.Events) != 0 || denied.Denial == nil {
		t.Fatalf("charged reservation disappeared = %+v, %v", denied, err)
	}
}

func TestBudgetAdmissionRejectsInvalidInputsBeforeRedis(t *testing.T) {
	for _, name := range []string{"Generate request", "Compact request", "route", "planner error", "operation", "generation", "no windows", "canceled", "nil context", "nil helper"} {
		t.Run(name, func(t *testing.T) {
			f := newBudgetAdmissionFixture(t)
			ctx := context.Background()
			code := provider.CodeInvalidArgument
			f.leaser.accept = func(context.Context, durable.ReserveRequest) (durable.ReserveResult, error) {
				t.Fatal("invalid input reached Redis")
				return durable.ReserveResult{}, nil
			}
			switch name {
			case "Generate request":
				f.gen.OperationKey = ""
			case "Compact request":
				f.compact.Parent = ""
			case "route":
				f.route.EndpointID = ""
			case "planner error":
				f.helper.generate = func(context.Context, llm.GenerateRequestV1, durable.RoutePlan) (durable.ReserveRequest, error) {
					return durable.ReserveRequest{}, errors.New("sensitive planner error")
				}
				code = provider.CodeConfiguration
			case "operation":
				f.plan.OperationID = "other"
				code = provider.CodeConfiguration
			case "generation":
				f.plan.GenerationID = "other"
				code = provider.CodeConfiguration
			case "no windows":
				f.plan.Reservations = nil
				code = provider.CodeConfiguration
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "nil context":
				ctx, code = nil, provider.CodeConfiguration
			case "nil helper":
				f.helper, code = nil, provider.CodeConfiguration
			}
			var err error
			if name == "Compact request" {
				_, err = f.helper.ReserveCompact(ctx, f.compact, f.route)
			} else {
				_, err = f.helper.ReserveGenerate(ctx, f.gen, f.route)
			}
			if name == "canceled" {
				if !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			} else {
				assertBudgetAdmissionError(t, err, code, provider.DispatchNotDispatched, provider.RetryNever)
			}
		})
	}
}

func TestBudgetAdmissionValidatesReservationBeforeClaiming(t *testing.T) {
	for _, name := range []string{"wait", "operation", "generation", "incarnation", "no events", "event identity", "duplicate event"} {
		t.Run(name, func(t *testing.T) {
			f := newBudgetAdmissionFixture(t)
			result, err := f.helper.ReserveGenerate(context.Background(), f.gen, f.route)
			if err != nil {
				t.Fatal(err)
			}
			f.leaser.claim = func(context.Context, durable.ClaimRequest) (durable.ClaimReceipt, error) {
				t.Fatal("invalid reservation reached Redis claim")
				return durable.ClaimReceipt{}, nil
			}
			switch name {
			case "wait":
				result.Accepted, result.Events = false, nil
			case "operation":
				result.OperationID = "other"
			case "generation":
				result.GenerationID = "other"
			case "incarnation":
				result.IncarnationID = ""
			case "no events":
				result.Events = nil
			case "event identity":
				result.Events[0].OperationID = "other"
			case "duplicate event":
				result.Events = append(result.Events, result.Events[0])
			}
			_, err = f.helper.ClaimGenerate(context.Background(), f.gen, f.route, result)
			assertBudgetAdmissionError(t, err, provider.CodeInvalidArgument, provider.DispatchNotDispatched, provider.RetryNever)
			_, err = f.helper.ClaimCompact(context.Background(), f.compact, f.route, result)
			assertBudgetAdmissionError(t, err, provider.CodeInvalidArgument, provider.DispatchNotDispatched, provider.RetryNever)
		})
	}
}

func TestBudgetAdmissionLostAcceptanceReplyCanReplayIdenticalInputs(t *testing.T) {
	f := newBudgetAdmissionFixture(t)
	first := true
	f.leaser.accept = func(ctx context.Context, request durable.ReserveRequest) (durable.ReserveResult, error) {
		if !reflect.DeepEqual(request, f.plan) {
			t.Fatal("changed reservation inputs")
		}
		result, err := f.leaser.BudgetLeaser.Accept(ctx, request)
		if first && err == nil {
			first = false
			return durable.ReserveResult{}, errors.New("sensitive lost acceptance reply")
		}
		return result, err
	}
	_, err := f.helper.ReserveGenerate(context.Background(), f.gen, f.route)
	assertBudgetAdmissionError(t, err, provider.CodeStateUnavailable, provider.DispatchNotDispatched, provider.RetrySameOperation)
	f.now = f.now.Add(time.Minute)
	result, err := f.helper.ReserveGenerate(context.Background(), f.gen, f.route)
	if err != nil || !result.Accepted {
		t.Fatalf("replay = %+v, %v", result, err)
	}
	// Acceptance replay does not renew the original 15-minute start deadline.
	f.now = f.now.Add(durable.BudgetStartLease - time.Minute)
	_, err = f.helper.ClaimGenerate(context.Background(), f.gen, f.route, result)
	assertBudgetAdmissionError(t, err, provider.CodeBudgetDenied, provider.DispatchNotDispatched, provider.RetryAfter)
}

func TestBudgetAdmissionClaimLossAndCanceledReplyNeverGrantPermission(t *testing.T) {
	for _, cancelReply := range []bool{false, true} {
		t.Run(map[bool]string{false: "lost reply", true: "canceled reply"}[cancelReply], func(t *testing.T) {
			f := newBudgetAdmissionFixture(t)
			result, err := f.helper.ReserveGenerate(context.Background(), f.gen, f.route)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			f.leaser.claim = func(ctx context.Context, request durable.ClaimRequest) (durable.ClaimReceipt, error) {
				receipt, err := f.leaser.BudgetLeaser.Claim(ctx, request)
				if err != nil {
					return receipt, err
				}
				if cancelReply {
					cancel()
					return receipt, nil
				}
				return durable.ClaimReceipt{}, errors.New("sensitive lost claim reply")
			}
			receipt, err := f.helper.ClaimGenerate(ctx, f.gen, f.route, result)
			assertBudgetAdmissionError(t, err, provider.CodeAmbiguousDispatch, provider.DispatchAmbiguous, provider.RetryNever)
			if receipt != (durable.ClaimReceipt{}) {
				t.Fatal("uncertain claim granted permission")
			}
			f.leaser.claim = nil
			_, err = f.helper.ClaimGenerate(context.Background(), f.gen, f.route, result)
			assertBudgetAdmissionError(t, err, provider.CodeAmbiguousDispatch, provider.DispatchAmbiguous, provider.RetryNever)
			f.now = f.now.Add(time.Hour)
			f.route.OperationID, f.plan.OperationID = "fresh", "fresh"
			f.plan.Reservations[0].AmountUSD = pricing.MustUSD("0.80")
			wait, err := f.helper.ReserveGenerate(context.Background(), f.gen, f.route)
			if err != nil || wait.Accepted {
				t.Fatalf("uncertain claim refunded budget: %+v, %v", wait, err)
			}
		})
	}
}

func TestBudgetAdmissionConcurrentClaimsGrantOnePermission(t *testing.T) {
	f := newBudgetAdmissionFixture(t)
	reservation, err := f.helper.ReserveGenerate(context.Background(), f.gen, f.route)
	if err != nil {
		t.Fatal(err)
	}
	var successes atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := f.helper.ClaimGenerate(context.Background(), f.gen, f.route, reservation); err == nil {
				successes.Add(1)
			} else {
				assertBudgetAdmissionError(t, err, provider.CodeAmbiguousDispatch, provider.DispatchAmbiguous, provider.RetryNever)
			}
		}()
	}
	wg.Wait()
	if successes.Load() != 1 {
		t.Fatalf("claim permissions = %d, want 1", successes.Load())
	}
}

func TestBudgetAdmissionCapturesCompositionLeaserAcrossReload(t *testing.T) {
	f := newBudgetAdmissionFixture(t)
	other := &admissionLeaser{accept: func(context.Context, durable.ReserveRequest) (durable.ReserveResult, error) {
		t.Fatal("old adapter used new snapshot or separate capability leaser")
		return durable.ReserveResult{}, nil
	}}
	f.cap.Budgets = other
	f.cap.composition.Materializer = other
	f.cap.ConfigDigest = [32]byte{4}
	result, err := f.helper.ReserveGenerate(context.Background(), f.gen, f.route)
	if err != nil || !result.Accepted {
		t.Fatalf("old adapter lost snapshot = %+v, %v", result, err)
	}
	if _, err := f.helper.ClaimCompact(context.Background(), f.compact, f.route, result); err != nil {
		t.Fatal(err)
	}
}

func TestBudgetAdmissionBothRunnersRequireAdmissionAndClaimBeforeDispatch(t *testing.T) {
	for _, compact := range []bool{false, true} {
		for _, name := range []string{"wait", "accept unavailable", "invalid acceptance", "lost claim", "expired claim", "invalid receipt", "acquired"} {
			t.Run(map[bool]string{false: "Generate/", true: "Compact/"}[compact]+name, func(t *testing.T) {
				f := newBudgetAdmissionFixture(t)
				code, certainty, retry := provider.CodeAmbiguousDispatch, provider.DispatchAmbiguous, provider.RetryNever
				claims, dispatches := 0, 0
				f.leaser.claim = func(ctx context.Context, request durable.ClaimRequest) (durable.ClaimReceipt, error) {
					claims++
					if name == "expired claim" {
						f.now = f.now.Add(durable.BudgetStartLease)
					}
					receipt, err := f.leaser.BudgetLeaser.Claim(ctx, request)
					if err == nil && name == "lost claim" {
						return durable.ClaimReceipt{}, errors.New("sensitive Redis lost reply")
					}
					if err == nil && name == "invalid receipt" {
						receipt.OperationID = "other"
					}
					return receipt, err
				}
				wantClaims := 1
				if name == "wait" {
					f.plan.Reservations[0].AmountUSD = pricing.MustUSD("2")
					code, certainty, retry, wantClaims = provider.CodeBudgetDenied, provider.DispatchNotDispatched, provider.RetryAfter, 0
				} else if name == "accept unavailable" {
					f.leaser.accept = func(context.Context, durable.ReserveRequest) (durable.ReserveResult, error) {
						return durable.ReserveResult{}, errors.New("sensitive Redis unavailable")
					}
					code, certainty, retry, wantClaims = provider.CodeStateUnavailable, provider.DispatchNotDispatched, provider.RetrySameOperation, 0
				} else if name == "invalid acceptance" {
					f.leaser.accept = func(ctx context.Context, request durable.ReserveRequest) (durable.ReserveResult, error) {
						result, err := f.leaser.BudgetLeaser.Accept(ctx, request)
						result.Events = append(result.Events, result.Events...)
						return result, err
					}
					code, certainty, retry, wantClaims = provider.CodeStateCorrupt, provider.DispatchNotDispatched, provider.RetryNever, 0
				} else if name == "expired claim" {
					code, certainty, retry = provider.CodeBudgetDenied, provider.DispatchNotDispatched, provider.RetryAfter
				}
				reachedDispatch := errors.New("dispatch reached with validated claim")
				var err error
				if compact {
					ports := validCompactPorts()
					_, materializer, _, _ := checkpointReplayFixture(t)
					ports.Replay = func(context.Context, llm.CompactRequestV1) (durable.CompactReplay, error) {
						return durable.CompactReplay{State: materializer.result}, nil
					}
					ports.Route = func(context.Context, llm.CompactRequestV1, durable.CompactReplay) (durable.RoutePlan, error) {
						return f.route, nil
					}
					ports.Reserve, ports.Claim = f.helper.ReserveCompact, f.helper.ClaimCompact
					ports.Dispatch = func(_ context.Context, _ llm.CompactRequestV1, _ durable.CompactReplay, _ durable.RoutePlan, receipt durable.ClaimReceipt) (durable.CompactDispatchResult, error) {
						dispatches++
						if receipt.OperationID != f.route.OperationID || receipt.GenerationID != f.route.GenerationID || receipt.IncarnationID != "incarnation-1" {
							t.Fatal("dispatch received mismatched budget receipt")
						}
						return durable.CompactDispatchResult{}, reachedDispatch
					}
					_, err = durable.CompactV1(context.Background(), f.compact, ports)
				} else {
					ports := validBuilderGeneratePorts(nil)
					ports.Reserve, ports.Claim = f.helper.ReserveGenerate, f.helper.ClaimGenerate
					ports.Dispatch = func(_ context.Context, _ llm.GenerateRequestV1, _ durable.GenerateReplay, _ durable.RoutePlan, receipt durable.ClaimReceipt) (durable.DispatchResult, error) {
						dispatches++
						if receipt.OperationID != f.route.OperationID || receipt.GenerationID != f.route.GenerationID || receipt.IncarnationID != "incarnation-1" {
							t.Fatal("dispatch received mismatched budget receipt")
						}
						return durable.DispatchResult{}, reachedDispatch
					}
					_, err = durable.GenerateV1(context.Background(), f.gen, ports)
				}
				wantDispatches := 0
				if name == "acquired" {
					wantDispatches = 1
					if !errors.Is(err, reachedDispatch) {
						t.Fatal(err)
					}
				} else {
					assertBudgetAdmissionError(t, err, code, certainty, retry)
				}
				if claims != wantClaims || dispatches != wantDispatches {
					t.Fatalf("claims/dispatches = %d/%d, want %d/%d", claims, dispatches, wantClaims, wantDispatches)
				}
			})
		}
	}
}

func TestBudgetAdmissionStartDeadlineAndShorterPlannerExpiry(t *testing.T) {
	for _, name := range []string{"just before deadline", "at deadline", "shorter expiry"} {
		t.Run(name, func(t *testing.T) {
			f := newBudgetAdmissionFixture(t)
			if name == "shorter expiry" {
				f.plan.ExpiresAt = f.now.Add(time.Minute)
			}
			result, err := f.helper.ReserveCompact(context.Background(), f.compact, f.route)
			if err != nil {
				t.Fatal(err)
			}
			if name == "shorter expiry" {
				f.now = f.plan.ExpiresAt
			} else {
				f.now = f.now.Add(durable.BudgetStartLease)
				if name == "just before deadline" {
					f.now = f.now.Add(-time.Nanosecond)
				}
			}
			_, err = f.helper.ClaimCompact(context.Background(), f.compact, f.route, result)
			if name == "just before deadline" {
				if err != nil {
					t.Fatal(err)
				}
			} else {
				assertBudgetAdmissionError(t, err, provider.CodeBudgetDenied, provider.DispatchNotDispatched, provider.RetryAfter)
			}
		})
	}
}
