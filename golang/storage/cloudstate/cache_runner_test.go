package cloudstate

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/mfow/llm-temporal-worker/golang/activity"
	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/cache"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/state"
	"github.com/mfow/llm-temporal-worker/golang/storage/durable"
	"go.temporal.io/sdk/temporal"
)

var errCacheRunnerSubmitted = errors.New("test provider was submitted")

// Execute real Generate/Compact runners with real cloud cache repositories.
// Redis/provider calls are counted ports; no network or paid provider is used.
// Dispatch stops the test before finalization, which is outside this gate PR.
func runCloudCacheGate(ctx context.Context, kind cache.OperationKind, c *durable.ResponseCache, lease cache.FillLease, claims, submissions *atomic.Int32) error {
	route := durable.RoutePlan{OperationID: durable.OperationID(lease.OperationID), GenerationID: durable.GenerationID(lease.Attempt),
		RouteID: "route", Provider: string(lease.Key.Route.Provider), EndpointID: string(lease.Key.Route.Endpoint), Model: string(lease.Key.Route.Model), CacheIdentity: lease.Key.Route}
	reservation := durable.ReserveResult{OperationID: route.OperationID, GenerationID: route.GenerationID, IncarnationID: "incarnation", Accepted: true,
		Events: []budget.ReservationEvent{{EventID: "reservation", OperationID: string(route.OperationID), GenerationID: string(route.GenerationID), WindowID: "window", BucketStart: lease.AcquiredAt, ReservationRevision: 1, AmountUSD: pricing.MustUSD("0.1"), OccurredAt: lease.AcquiredAt}}}
	claim := durable.ClaimReceipt{OperationID: route.OperationID, GenerationID: route.GenerationID, IncarnationID: reservation.IncarnationID}
	if kind == cache.OperationGenerate {
		request := llm.GenerateRequestV1{OperationKey: string(lease.OperationID), Context: llm.RequestContext{Tenant: "tenant", Project: "project", Actor: "actor"}}
		_, err := durable.GenerateV1(ctx, request, durable.GeneratePorts{
			Replay: func(context.Context, llm.GenerateRequestV1) (durable.GenerateReplay, error) {
				return durable.GenerateReplay{}, nil
			},
			CacheLookup: func(ctx context.Context, _ llm.GenerateRequestV1, _ durable.GenerateReplay) (durable.CacheDecision, error) {
				return c.PrepareGenerate(ctx, lease, nil)
			},
			CompactionDecision: func(context.Context, llm.GenerateRequestV1, durable.GenerateReplay, durable.CacheDecision) (durable.CompactionDecision, error) {
				return durable.CompactionDecision{}, nil
			},
			Route: func(context.Context, llm.GenerateRequestV1, durable.GenerateReplay, durable.CompactionDecision) (durable.RoutePlan, error) {
				return route, nil
			},
			Reserve: func(context.Context, llm.GenerateRequestV1, durable.RoutePlan) (durable.ReserveResult, error) {
				return reservation, nil
			},
			Claim: func(context.Context, llm.GenerateRequestV1, durable.RoutePlan, durable.ReserveResult) (durable.ClaimReceipt, error) {
				claims.Add(1)
				return claim, nil
			},
			Dispatch: func(context.Context, llm.GenerateRequestV1, durable.GenerateReplay, durable.RoutePlan, durable.ClaimReceipt) (durable.DispatchResult, error) {
				submissions.Add(1)
				return durable.DispatchResult{}, errCacheRunnerSubmitted
			},
			Finalize: func(context.Context, llm.GenerateRequestV1, durable.GenerateReplay, durable.RoutePlan, durable.ReserveResult, durable.DispatchResult) (durable.GenerateFinalization, error) {
				panic("unexpected finalization")
			},
			FinalizeCache: func(context.Context, llm.GenerateRequestV1, durable.GenerateReplay, durable.CacheDecision) (durable.GenerateFinalization, error) {
				panic("unexpected cache hit")
			},
			Reconcile: func(context.Context, llm.GenerateRequestV1, durable.RoutePlan, durable.ReserveResult, durable.GenerateFinalization) error {
				panic("unexpected reconciliation")
			},
		})
		return err
	}
	request := llm.CompactRequestV1{OperationKey: string(lease.OperationID), Context: llm.RequestContext{Tenant: "tenant", Project: "project", Actor: "actor"}, Parent: "parent"}
	_, err := durable.CompactV1(ctx, request, durable.CompactPorts{
		Replay: func(context.Context, llm.CompactRequestV1) (durable.CompactReplay, error) {
			return durable.CompactReplay{State: state.MaterializedState{Handle: "parent", Tenant: "tenant", Project: "project"}}, nil
		},
		CacheLookup: func(ctx context.Context, _ llm.CompactRequestV1, _ durable.CompactReplay) (durable.CompactCacheDecision, error) {
			return c.PrepareCompact(ctx, lease, nil)
		},
		Route: func(context.Context, llm.CompactRequestV1, durable.CompactReplay) (durable.RoutePlan, error) {
			return route, nil
		},
		Reserve: func(context.Context, llm.CompactRequestV1, durable.RoutePlan) (durable.ReserveResult, error) {
			return reservation, nil
		},
		Claim: func(context.Context, llm.CompactRequestV1, durable.RoutePlan, durable.ReserveResult) (durable.ClaimReceipt, error) {
			claims.Add(1)
			return claim, nil
		},
		Dispatch: func(context.Context, llm.CompactRequestV1, durable.CompactReplay, durable.RoutePlan, durable.ClaimReceipt) (durable.CompactDispatchResult, error) {
			submissions.Add(1)
			return durable.CompactDispatchResult{}, errCacheRunnerSubmitted
		},
		Finalize: func(context.Context, llm.CompactRequestV1, durable.CompactReplay, durable.RoutePlan, durable.ReserveResult, durable.CompactDispatchResult) (durable.CompactFinalization, error) {
			panic("unexpected finalization")
		},
		FinalizeCache: func(context.Context, llm.CompactRequestV1, durable.CompactReplay, durable.CompactCacheDecision) (durable.CompactFinalization, error) {
			panic("unexpected cache hit")
		},
		Reconcile: func(context.Context, llm.CompactRequestV1, durable.RoutePlan, durable.ReserveResult, durable.CompactFinalization) error {
			panic("unexpected reconciliation")
		},
	})
	return err
}

func TestCloudCacheRunnersHundredIndependentMissesAndRetries(t *testing.T) {
	for _, kind := range []cache.OperationKind{cache.OperationGenerate, cache.OperationCompact} {
		for _, sameAttempt := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/same_attempt=%t", kind, sameAttempt), func(t *testing.T) {
				_, table, blobs, entry, _ := responseCacheFixture(t)
				lease := fillLease(entry)
				lease.Key.Operation = kind
				var claims, submissions atomic.Int32
				var wg sync.WaitGroup
				errs := make(chan error, 100)
				for i := range 100 {
					repository := reopen(t, table, blobs)
					c, err := durable.NewResponseCache(repository.Responses(), repository.ResponseFills(), func() time.Time { return lease.AcquiredAt })
					if err != nil {
						t.Fatal(err)
					}
					attempt := lease
					if !sameAttempt {
						attempt.Attempt = fmt.Sprintf("attempt-%d", i)
						attempt.OperationID = state.OperationID(fmt.Sprintf("operation-%d", i))
					}
					wg.Go(func() { errs <- runCloudCacheGate(context.Background(), kind, c, attempt, &claims, &submissions) })
				}
				wg.Wait()
				close(errs)
				for err := range errs {
					if !errors.Is(err, errCacheRunnerSubmitted) && !errors.Is(err, durable.ErrCacheWait) && !errors.Is(err, durable.ErrCacheRecoveryRequired) {
						t.Errorf("unexpected runner error: %v", err)
					}
					if errors.Is(err, durable.ErrCacheWait) {
						var application *temporal.ApplicationError
						if !errors.As(activity.ToTemporalError(err), &application) || application.NonRetryable() || application.NextRetryDelay() <= 0 {
							t.Fatalf("cache wait lost retry hint at Temporal boundary: %v", application)
						}
					}
				}
				if claims.Load() != 1 || submissions.Load() != 1 {
					t.Fatalf("claims=%d submissions=%d", claims.Load(), submissions.Load())
				}
				// A restarted worker after expiry still cannot submit started work.
				repository := reopen(t, table, blobs)
				lease.AcquiredAt, lease.ExpiresAt = lease.AcquiredAt.Add(time.Hour), lease.ExpiresAt.Add(time.Hour)
				lease.Attempt = "next-attempt"
				c, err := durable.NewResponseCache(repository.Responses(), repository.ResponseFills(), func() time.Time { return lease.AcquiredAt })
				if err != nil {
					t.Fatal(err)
				}
				err = runCloudCacheGate(context.Background(), kind, c, lease, &claims, &submissions)
				if !errors.Is(err, durable.ErrCacheRecoveryRequired) || submissions.Load() != 1 || claims.Load() != 1 {
					t.Fatalf("restart: %v, submissions=%d", err, submissions.Load())
				}
			})
		}
	}
}

type lostStartAcknowledgement struct{ cache.FillRepository }

func (s lostStartAcknowledgement) Start(ctx context.Context, lease cache.FillLease, now time.Time) (bool, error) {
	started, err := s.FillRepository.Start(ctx, lease, now)
	if err != nil || !started {
		return started, err
	}
	return false, contracts.ErrOutcomeUnknown
}

func TestCloudCacheRunnerRecoversLostStartAcknowledgement(t *testing.T) {
	for _, kind := range []cache.OperationKind{cache.OperationGenerate, cache.OperationCompact} {
		r, table, blobs, entry, _ := responseCacheFixture(t)
		lease := fillLease(entry)
		lease.Key.Operation = kind
		c, err := durable.NewResponseCache(r.Responses(), lostStartAcknowledgement{r.ResponseFills()}, func() time.Time { return lease.AcquiredAt })
		if err != nil {
			t.Fatal(err)
		}
		var claims, submissions atomic.Int32
		err = runCloudCacheGate(context.Background(), kind, c, lease, &claims, &submissions)
		if !errors.Is(err, contracts.ErrOutcomeUnknown) || !errors.Is(err, durable.ErrCacheRecoveryRequired) {
			t.Fatal(err)
		}
		r = reopen(t, table, blobs)
		c, err = durable.NewResponseCache(r.Responses(), r.ResponseFills(), func() time.Time { return lease.AcquiredAt })
		if err != nil {
			t.Fatal(err)
		}
		err = runCloudCacheGate(context.Background(), kind, c, lease, &claims, &submissions)
		if !errors.Is(err, durable.ErrCacheRecoveryRequired) || claims.Load() != 0 || submissions.Load() != 0 {
			t.Fatalf("%v claims=%d submissions=%d", err, claims.Load(), submissions.Load())
		}
	}
}

func TestCloudCacheWaitRemainsRetryableThroughTemporal(t *testing.T) {
	for _, kind := range []cache.OperationKind{cache.OperationGenerate, cache.OperationCompact} {
		r, _, _, entry, _ := responseCacheFixture(t)
		lease := fillLease(entry)
		lease.Key.Operation = kind
		mustAcquireFill(t, r.ResponseFills(), lease, cache.FillOwned)
		waiter := lease
		waiter.OperationID, waiter.Attempt = "waiting-operation", "waiting-generation"
		c, err := durable.NewResponseCache(r.Responses(), r.ResponseFills(), func() time.Time { return lease.AcquiredAt })
		if err != nil {
			t.Fatal(err)
		}
		var claims, submissions atomic.Int32
		err = runCloudCacheGate(context.Background(), kind, c, waiter, &claims, &submissions)
		if !errors.Is(err, durable.ErrCacheWait) {
			t.Fatal(err)
		}
		var application *temporal.ApplicationError
		if !errors.As(activity.ToTemporalError(err), &application) || application.NonRetryable() || application.NextRetryDelay() != cache.MaxFillLease {
			t.Fatalf("cache wait lost retry hint: %v", application)
		}
		var details activity.SafeErrorDetails
		if application.Details(&details) != nil || details.OperationID != string(waiter.OperationID) || details.Dispatch != "not_dispatched" || details.RetryAfterMillis != cache.MaxFillLease.Milliseconds() {
			t.Fatalf("invalid wait details: %+v", details)
		}
		if claims.Load() != 0 || submissions.Load() != 0 {
			t.Fatal("waiter spent budget")
		}
	}
}

func TestCloudCacheRunnerNewAttemptAfterResolvedUnknownClaimsAgain(t *testing.T) {
	for _, kind := range []cache.OperationKind{cache.OperationGenerate, cache.OperationCompact} {
		r, _, _, entry, _ := responseCacheFixture(t)
		lease := fillLease(entry)
		lease.Key.Operation = kind
		var claims, submissions atomic.Int32
		c, err := durable.NewResponseCache(r.Responses(), r.ResponseFills(), func() time.Time { return lease.AcquiredAt })
		if err != nil {
			t.Fatal(err)
		}
		if err := runCloudCacheGate(context.Background(), kind, c, lease, &claims, &submissions); !errors.Is(err, errCacheRunnerSubmitted) {
			t.Fatal(err)
		}
		// Simulate the recovery finalizer having durably charged the unknown
		// attempt. The cache layer itself cannot infer or perform that settlement.
		if err := r.ResponseFills().Complete(context.Background(), lease, cache.FillCompletion{Outcome: cache.FillUnknown, CompletedAt: lease.AcquiredAt.Add(time.Second)}); err != nil {
			t.Fatal(err)
		}
		if err := runCloudCacheGate(context.Background(), kind, c, lease, &claims, &submissions); !errors.Is(err, durable.ErrCacheRecoveryRequired) {
			t.Fatal(err)
		}
		lease.Attempt = "fresh-budget-generation"
		lease.AcquiredAt, lease.ExpiresAt = lease.AcquiredAt.Add(2*time.Second), lease.ExpiresAt.Add(2*time.Second)
		if err := runCloudCacheGate(context.Background(), kind, c, lease, &claims, &submissions); !errors.Is(err, errCacheRunnerSubmitted) {
			t.Fatal(err)
		}
		if claims.Load() != 2 || submissions.Load() != 2 {
			t.Fatalf("claims=%d submissions=%d", claims.Load(), submissions.Load())
		}
	}
}
