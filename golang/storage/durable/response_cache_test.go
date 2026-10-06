package durable

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/cache"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

type cacheResponseStub struct {
	cache.ResponseRepository
	lookup func(context.Context, cache.ResponseLookup) (*cache.ResponseEntry, error)
}

func (s *cacheResponseStub) Lookup(ctx context.Context, lookup cache.ResponseLookup) (*cache.ResponseEntry, error) {
	return s.lookup(ctx, lookup)
}

type cacheFillStub struct {
	cache.FillRepository
	acquire func(context.Context, cache.FillLease) (cache.FillDecision, error)
	start   func(context.Context, cache.FillLease, time.Time) (bool, error)
	release func(context.Context, cache.FillLease, time.Time) error
}

func (s *cacheFillStub) Acquire(ctx context.Context, lease cache.FillLease, _ time.Time) (cache.FillDecision, error) {
	return s.acquire(ctx, lease)
}
func (s *cacheFillStub) Start(ctx context.Context, lease cache.FillLease, now time.Time) (bool, error) {
	return s.start(ctx, lease, now)
}
func (s *cacheFillStub) Release(ctx context.Context, lease cache.FillLease, now time.Time) error {
	return s.release(ctx, lease, now)
}

func cacheRunnerFixture(t *testing.T, kind cache.OperationKind, events *[]string) (*ResponseCache, cache.FillLease, *cacheResponseStub, *cacheFillStub) {
	t.Helper()
	now := time.Date(2026, 9, 29, 0, 0, 0, 0, time.UTC)
	lease := cache.FillLease{Key: cache.ResponseKey{ScopeID: "scope", Operation: kind,
		Route:       cache.RouteIdentity{Provider: "provider", Endpoint: "endpoint-1", Model: "model", Account: "account", Region: "region", Revision: "revision", Compiler: "compiler"},
		Fingerprint: cache.Fingerprint{1}}, OperationID: "operation-id", Attempt: "generation-id", AcquiredAt: now, ExpiresAt: now.Add(time.Minute)}
	responses := &cacheResponseStub{lookup: func(context.Context, cache.ResponseLookup) (*cache.ResponseEntry, error) { return nil, nil }}
	fills := &cacheFillStub{
		acquire: func(_ context.Context, lease cache.FillLease) (cache.FillDecision, error) {
			return cache.FillDecision{Disposition: cache.FillOwned, Record: cache.FillRecord{Lease: lease, State: cache.FillHeld}}, nil
		},
		start: func(context.Context, cache.FillLease, time.Time) (bool, error) {
			*events = append(*events, "start")
			return true, nil
		},
	}
	c, err := NewResponseCache(responses, fills, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	return c, lease, responses, fills
}

// Both real runners use the same repositories/gate. The surrounding ports are
// instrumented so any accidental admission or submission is visible.
func runCacheFixture(ctx context.Context, kind cache.OperationKind, c *ResponseCache, lease cache.FillLease, events *[]string, editRoute func(*RoutePlan), deny bool) error {
	route := testRoutePlan()
	route.CacheIdentity = lease.Key.Route
	if editRoute != nil {
		editRoute(&route)
	}
	reservation := testReservation(route)
	reservation.Accepted = !deny
	if deny {
		reservation.Events = nil
	}
	if kind == cache.OperationGenerate {
		ports := testGeneratePorts(events, "")
		ports.CacheLookup = func(ctx context.Context, _ llm.GenerateRequestV1, _ GenerateReplay) (CacheDecision, error) {
			*events = append(*events, "cache")
			return c.PrepareGenerate(ctx, lease, nil)
		}
		ports.Route = func(context.Context, llm.GenerateRequestV1, GenerateReplay, CompactionDecision) (RoutePlan, error) {
			*events = append(*events, "route")
			return route, nil
		}
		ports.Reserve = func(context.Context, llm.GenerateRequestV1, RoutePlan) (ReserveResult, error) {
			*events = append(*events, "reserve")
			return reservation, nil
		}
		_, err := GenerateV1(ctx, testGenerateRequest(), ports)
		return err
	}
	ports := testCompactPorts(events, "")
	ports.CacheLookup = func(ctx context.Context, _ llm.CompactRequestV1, _ CompactReplay) (CompactCacheDecision, error) {
		*events = append(*events, "cache")
		return c.PrepareCompact(ctx, lease, nil)
	}
	ports.Route = func(context.Context, llm.CompactRequestV1, CompactReplay) (RoutePlan, error) {
		*events = append(*events, "route")
		return route, nil
	}
	ports.Reserve = func(context.Context, llm.CompactRequestV1, RoutePlan) (ReserveResult, error) {
		*events = append(*events, "reserve")
		return reservation, nil
	}
	ports.Claim = func(context.Context, llm.CompactRequestV1, RoutePlan, ReserveResult) (ClaimReceipt, error) {
		*events = append(*events, "claim")
		return ClaimReceipt{OperationID: route.OperationID, GenerationID: route.GenerationID, IncarnationID: reservation.IncarnationID}, nil
	}
	_, err := CompactV1(ctx, testCompactRequest(), ports)
	return err
}

func TestResponseCacheRunnerAdmissionOrder(t *testing.T) {
	for _, kind := range []cache.OperationKind{cache.OperationGenerate, cache.OperationCompact} {
		t.Run(string(kind), func(t *testing.T) {
			events := []string{}
			c, lease, _, _ := cacheRunnerFixture(t, kind, &events)
			if err := runCacheFixture(context.Background(), kind, c, lease, &events, nil, false); err != nil {
				t.Fatal(err)
			}
			want := []string{"replay", "cache", "route", "reserve", "start", "claim", "dispatch", "finalize", "reconcile"}
			if kind == cache.OperationGenerate {
				want = append(want[:2], append([]string{"compaction"}, want[2:]...)...)
			}
			if !reflect.DeepEqual(events, want) {
				t.Fatalf("events = %v, want %v", events, want)
			}
		})
	}
}

func TestResponseCacheWaitAndRecoveryStopBeforeAdmission(t *testing.T) {
	for _, kind := range []cache.OperationKind{cache.OperationGenerate, cache.OperationCompact} {
		for _, disposition := range []cache.FillDisposition{cache.FillWait, cache.FillRecoveryNeeded, cache.FillAttemptFinished} {
			t.Run(string(kind)+"/"+string(disposition), func(t *testing.T) {
				events := []string{}
				c, lease, _, fills := cacheRunnerFixture(t, kind, &events)
				fills.acquire = func(context.Context, cache.FillLease) (cache.FillDecision, error) {
					return cache.FillDecision{Disposition: disposition, Record: cache.FillRecord{Lease: lease, State: cache.FillHeld}}, nil
				}
				err := runCacheFixture(context.Background(), kind, c, lease, &events, nil, false)
				var mapped *provider.Error
				if !errors.As(err, &mapped) {
					t.Fatalf("unmapped error: %v", err)
				}
				if disposition == cache.FillWait {
					if !errors.Is(err, ErrCacheWait) || mapped.Retry != provider.RetryAfter || mapped.RetryAfter != time.Minute || mapped.Dispatch != provider.DispatchNotDispatched {
						t.Fatal(err)
					}
				} else if !errors.Is(err, ErrCacheRecoveryRequired) || mapped.Retry != provider.RetrySameOperation || mapped.Dispatch != provider.DispatchAmbiguous {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(events, []string{"replay", "cache"}) {
					t.Fatal(events)
				}
			})
		}
	}
}

func TestResponseCacheUncertainOrRepeatedStartCannotClaimOrDispatch(t *testing.T) {
	uncertain := errors.New("lost acknowledgement")
	for _, kind := range []cache.OperationKind{cache.OperationGenerate, cache.OperationCompact} {
		for _, startErr := range []error{nil, uncertain} {
			events := []string{}
			c, lease, _, fills := cacheRunnerFixture(t, kind, &events)
			fills.start = func(context.Context, cache.FillLease, time.Time) (bool, error) {
				events = append(events, "start")
				return startErr != nil, startErr
			}
			err := runCacheFixture(context.Background(), kind, c, lease, &events, nil, false)
			if !errors.Is(err, ErrCacheRecoveryRequired) || (startErr != nil && !errors.Is(err, startErr)) {
				t.Fatal(err)
			}
			if events[len(events)-1] != "start" {
				t.Fatal(events)
			}
		}
	}
}

func TestResponseCacheBudgetDenialDoesNotStartFill(t *testing.T) {
	for _, kind := range []cache.OperationKind{cache.OperationGenerate, cache.OperationCompact} {
		events := []string{}
		c, lease, _, _ := cacheRunnerFixture(t, kind, &events)
		err := runCacheFixture(context.Background(), kind, c, lease, &events, nil, true)
		if !errors.Is(err, ErrReservationDenied) || events[len(events)-1] != "reserve" {
			t.Fatalf("%v: %v", events, err)
		}
	}
}

func TestResponseCacheFencesEveryRouteAndBudgetIdentity(t *testing.T) {
	for _, kind := range []cache.OperationKind{cache.OperationGenerate, cache.OperationCompact} {
		for _, edit := range []func(*RoutePlan){
			func(r *RoutePlan) { r.OperationID += "-other" }, func(r *RoutePlan) { r.GenerationID += "-other" },
			func(r *RoutePlan) { r.Provider += "-other" }, func(r *RoutePlan) { r.EndpointID += "-other" }, func(r *RoutePlan) { r.Model += "-other" },
			func(r *RoutePlan) { r.CacheIdentity.Provider += "-other" }, func(r *RoutePlan) { r.CacheIdentity.Endpoint += "-other" },
			func(r *RoutePlan) { r.CacheIdentity.Account += "-other" }, func(r *RoutePlan) { r.CacheIdentity.Region += "-other" },
			func(r *RoutePlan) { r.CacheIdentity.Model += "-other" }, func(r *RoutePlan) { r.CacheIdentity.Revision += "-other" }, func(r *RoutePlan) { r.CacheIdentity.Compiler += "-other" },
		} {
			events := []string{}
			c, lease, _, _ := cacheRunnerFixture(t, kind, &events)
			err := runCacheFixture(context.Background(), kind, c, lease, &events, edit, false)
			if err == nil || events[len(events)-1] != "route" {
				t.Fatalf("%v: %v", events, err)
			}
		}
	}
}

func TestResponseCacheRejectsClockRegressionAndExpiredStart(t *testing.T) {
	for _, kind := range []cache.OperationKind{cache.OperationGenerate, cache.OperationCompact} {
		for _, elapsed := range []time.Duration{-time.Nanosecond, time.Minute} {
			events := []string{}
			c, lease, _, _ := cacheRunnerFixture(t, kind, &events)
			calls := 0
			c.now = func() time.Time {
				calls++
				if calls == 1 {
					return lease.AcquiredAt
				}
				return lease.AcquiredAt.Add(elapsed)
			}
			err := runCacheFixture(context.Background(), kind, c, lease, &events, nil, false)
			if !errors.Is(err, ErrCacheRecoveryRequired) || events[len(events)-1] != "reserve" {
				t.Fatalf("%v: %v", events, err)
			}
		}
	}
}

func cacheTemplate(t *testing.T, kind cache.OperationKind, lease cache.FillLease) cache.ResponseEntry {
	t.Helper()
	var value any
	if kind == cache.OperationGenerate {
		value = testFinalization(testGenerateRequest()).Response
	} else {
		value = testCompactFinalization(testCompactRequest()).Response
	}
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return cache.ResponseEntry{ID: "entry-1", Key: lease.Key, OriginOperationID: "operation-id", OriginCheckpointID: "origin", CompletedAt: lease.AcquiredAt, Response: raw}
}

func TestResponseCacheDecodesTemplatesAndClonesOrigin(t *testing.T) {
	for _, kind := range []cache.OperationKind{cache.OperationGenerate, cache.OperationCompact} {
		events := []string{}
		c, lease, responses, _ := cacheRunnerFixture(t, kind, &events)
		entry := cacheTemplate(t, kind, lease)
		responses.lookup = func(context.Context, cache.ResponseLookup) (*cache.ResponseEntry, error) { return &entry, nil }
		var origin func() *cache.ResponseEntry
		if kind == cache.OperationGenerate {
			d, err := c.PrepareGenerate(context.Background(), lease, nil)
			if err != nil || d.Disposition != CacheHit || d.Response == nil {
				t.Fatalf("%v %v", d, err)
			}
			origin = d.Entry
		} else {
			d, err := c.PrepareCompact(context.Background(), lease, nil)
			if err != nil || d.Disposition != CacheHit || d.Response == nil {
				t.Fatalf("%v %v", d, err)
			}
			origin = d.Entry
		}
		copy := origin()
		copy.Response[0] = '!'
		entry.Response[0] = '!'
		if origin().Response[0] != '{' {
			t.Fatal("origin aliases mutable storage")
		}
		if len(events) != 0 {
			t.Fatal(events)
		}
	}
}

func TestResponseCacheRejectsInvalidCachedTemplates(t *testing.T) {
	for _, kind := range []cache.OperationKind{cache.OperationGenerate, cache.OperationCompact} {
		for _, edit := range []func(*cache.ResponseEntry){
			func(e *cache.ResponseEntry) { e.Key.ScopeID = "other" }, func(e *cache.ResponseEntry) { e.Key.RequestIndex++ },
			func(e *cache.ResponseEntry) { e.ID = "" }, func(e *cache.ResponseEntry) { e.CompletedAt = e.CompletedAt.Add(time.Hour) },
			func(e *cache.ResponseEntry) { e.OriginOperationID = "different" }, func(e *cache.ResponseEntry) { e.Response = []byte(`{}`) },
		} {
			events := []string{}
			c, lease, responses, _ := cacheRunnerFixture(t, kind, &events)
			entry := cacheTemplate(t, kind, lease)
			edit(&entry)
			responses.lookup = func(context.Context, cache.ResponseLookup) (*cache.ResponseEntry, error) { return &entry, nil }
			var err error
			if kind == cache.OperationGenerate {
				_, err = c.PrepareGenerate(context.Background(), lease, nil)
			} else {
				_, err = c.PrepareCompact(context.Background(), lease, nil)
			}
			if err == nil {
				t.Fatal("invalid template accepted")
			}
		}
	}
}

func TestResponseCacheRejectsMissingDependenciesAndChangedDecisions(t *testing.T) {
	events := []string{}
	c, lease, responses, fills := cacheRunnerFixture(t, cache.OperationGenerate, &events)
	var nilResponses *cacheResponseStub
	var nilFills *cacheFillStub
	for _, deps := range []struct {
		r   cache.ResponseRepository
		f   cache.FillRepository
		now func() time.Time
	}{
		{nil, fills, c.now}, {nilResponses, fills, c.now}, {responses, nil, c.now}, {responses, nilFills, c.now}, {responses, fills, nil},
	} {
		if _, err := NewResponseCache(deps.r, deps.f, deps.now); err == nil {
			t.Fatal("invalid constructor")
		}
	}
	d, err := c.PrepareGenerate(context.Background(), lease, nil)
	if err != nil {
		t.Fatal(err)
	}
	d.Disposition = CacheDisabled
	if d.Validate() == nil {
		t.Fatal("ownership bypassed")
	}
	if (CacheDecision{Disposition: CacheWait}).Validate() == nil || (CompactCacheDecision{Disposition: CacheRecoveryRequired}).Validate() == nil {
		t.Fatal("unprepared wait accepted")
	}
	if (CacheDecision{}).Entry() != nil || (CompactCacheDecision{}).Entry() != nil {
		t.Fatal("empty origin")
	}
}
