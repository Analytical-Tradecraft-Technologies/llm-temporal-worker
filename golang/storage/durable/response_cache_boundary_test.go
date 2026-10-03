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
)

func TestResponseCacheHitsReachOnlyZeroCostFinalization(t *testing.T) {
	for _, kind := range []cache.OperationKind{cache.OperationGenerate, cache.OperationCompact} {
		t.Run(string(kind), func(t *testing.T) {
			events := []string{}
			c, lease, responses, _ := cacheRunnerFixture(t, kind, &events)
			entry := cacheTemplate(t, kind, lease)
			entry.OriginOperationID = "origin-operation"
			responses.lookup = func(context.Context, cache.ResponseLookup) (*cache.ResponseEntry, error) { return &entry, nil }
			var err error
			if kind == cache.OperationGenerate {
				origin := testFinalization(testGenerateRequest()).Response
				origin.OperationID, origin.OperationKey, origin.Checkpoint.Handle = "origin-operation", "origin-key", "origin-checkpoint"
				entry.Response, err = json.Marshal(origin)
				if err != nil {
					t.Fatal(err)
				}
				ports := testGeneratePorts(&events, "")
				ports.CacheLookup = func(ctx context.Context, _ llm.GenerateRequestV1, _ GenerateReplay) (CacheDecision, error) {
					events = append(events, "cache")
					return c.PrepareGenerate(ctx, lease, nil)
				}
				ports.FinalizeCache = func(_ context.Context, request llm.GenerateRequestV1, _ GenerateReplay, d CacheDecision) (GenerateFinalization, error) {
					events = append(events, "cache-finalize")
					if d.Entry().ID != entry.ID || d.Response.OperationID != string(entry.OriginOperationID) {
						t.Fatal("lost origin")
					}
					f := testFinalization(request)
					f.Response.Checkpoint.Kind, f.Response.Cache.Disposition = "cache_replay", "hit"
					return f, nil
				}
				_, err = GenerateV1(context.Background(), testGenerateRequest(), ports)
			} else {
				origin := testCompactFinalization(testCompactRequest()).Response
				origin.OperationID, origin.OperationKey, origin.Checkpoint.Handle = "origin-operation", "origin-key", "origin-checkpoint"
				entry.Response, err = json.Marshal(origin)
				if err != nil {
					t.Fatal(err)
				}
				ports := testCompactPorts(&events, "")
				ports.CacheLookup = func(ctx context.Context, _ llm.CompactRequestV1, _ CompactReplay) (CompactCacheDecision, error) {
					events = append(events, "cache")
					return c.PrepareCompact(ctx, lease, nil)
				}
				ports.FinalizeCache = func(_ context.Context, request llm.CompactRequestV1, _ CompactReplay, d CompactCacheDecision) (CompactFinalization, error) {
					events = append(events, "cache-finalize")
					if d.Entry().ID != entry.ID || d.Response.OperationID != string(entry.OriginOperationID) {
						t.Fatal("lost origin")
					}
					f := testCompactFinalization(request)
					f.Response.Cache.Disposition = "hit"
					f.Response.Cost.ActualCostUSD = stringPtr("0")
					f.Response.Provenance = []byte(`{"source":"worker_cache"}`)
					return f, nil
				}
				_, err = CompactV1(context.Background(), testCompactRequest(), ports)
			}
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(events, []string{"replay", "cache", "cache-finalize"}) {
				t.Fatal(events)
			}
		})
	}
}

func TestResponseCacheStorageFailuresStopRunner(t *testing.T) {
	failure := errors.New("storage unavailable")
	for _, kind := range []cache.OperationKind{cache.OperationGenerate, cache.OperationCompact} {
		for _, stage := range []string{"first lookup", "acquire", "second lookup", "release"} {
			t.Run(string(kind)+"/"+stage, func(t *testing.T) {
				events := []string{}
				c, lease, responses, fills := cacheRunnerFixture(t, kind, &events)
				entry := cacheTemplate(t, kind, lease)
				calls := 0
				responses.lookup = func(context.Context, cache.ResponseLookup) (*cache.ResponseEntry, error) {
					calls++
					if stage == "first lookup" || (stage == "second lookup" && calls == 2) {
						return nil, failure
					}
					if stage == "release" && calls == 2 {
						return &entry, nil
					}
					return nil, nil
				}
				if stage == "acquire" {
					fills.acquire = func(context.Context, cache.FillLease) (cache.FillDecision, error) {
						return cache.FillDecision{}, failure
					}
				}
				fills.release = func(context.Context, cache.FillLease, time.Time) error { return failure }
				err := runCacheFixture(context.Background(), kind, c, lease, &events, nil, false)
				if !errors.Is(err, failure) || !reflect.DeepEqual(events, []string{"replay", "cache"}) {
					t.Fatalf("%v: %v", events, err)
				}
			})
		}
	}
}

func TestResponseCacheSecondLookupHitReleasesBeforeFinalization(t *testing.T) {
	events := []string{}
	c, lease, responses, fills := cacheRunnerFixture(t, cache.OperationGenerate, &events)
	entry := cacheTemplate(t, cache.OperationGenerate, lease)
	calls := 0
	responses.lookup = func(context.Context, cache.ResponseLookup) (*cache.ResponseEntry, error) {
		calls++
		if calls == 1 {
			return nil, nil
		}
		return &entry, nil
	}
	fills.release = func(context.Context, cache.FillLease, time.Time) error {
		events = append(events, "release")
		return nil
	}
	d, err := c.PrepareGenerate(context.Background(), lease, nil)
	if err != nil || d.Disposition != CacheHit || !reflect.DeepEqual(events, []string{"release"}) {
		t.Fatalf("%v %v %v", d, err, events)
	}
}

func TestResponseCacheRejectsIncompleteAndAcceptsToolCalls(t *testing.T) {
	for _, status := range []llm.ResponseStatus{llm.ResponseStatusCompleted, llm.ResponseStatusToolCalls, llm.ResponseStatusLength, llm.ResponseStatusRefused, llm.ResponseStatusContentFiltered} {
		events := []string{}
		c, lease, responses, _ := cacheRunnerFixture(t, cache.OperationGenerate, &events)
		entry := cacheTemplate(t, cache.OperationGenerate, lease)
		response := testFinalization(testGenerateRequest()).Response
		response.Status = status
		if status == llm.ResponseStatusToolCalls {
			response.Output = []llm.Item{llm.ToolCall{ID: "call-1", Name: "lookup", Arguments: []byte(`{}`)}}
		}
		var err error
		entry.Response, err = json.Marshal(response)
		if err != nil {
			t.Fatal(err)
		}
		responses.lookup = func(context.Context, cache.ResponseLookup) (*cache.ResponseEntry, error) { return &entry, nil }
		d, err := c.PrepareGenerate(context.Background(), lease, nil)
		if status != llm.ResponseStatusCompleted && status != llm.ResponseStatusToolCalls {
			if err == nil {
				t.Fatal("incomplete response accepted")
			}
		} else if err != nil || d.Disposition != CacheHit {
			t.Fatalf("%v %v", d, err)
		}
	}
}

func TestResponseCacheSampleIndexCannotBeDroppedByGenerate(t *testing.T) {
	events := []string{}
	c, lease, _, _ := cacheRunnerFixture(t, cache.OperationGenerate, &events)
	lease.Key.RequestIndex = 1
	err := runCacheFixture(context.Background(), cache.OperationGenerate, c, lease, &events, nil, false)
	if err == nil || !reflect.DeepEqual(events, []string{"replay", "cache"}) {
		t.Fatalf("%v: %v", events, err)
	}
}

func TestResponseCacheRejectsInvalidInputBeforeStorage(t *testing.T) {
	events := []string{}
	c, lease, responses, _ := cacheRunnerFixture(t, cache.OperationGenerate, &events)
	responses.lookup = func(context.Context, cache.ResponseLookup) (*cache.ResponseEntry, error) {
		t.Fatal("read on invalid input")
		return nil, nil
	}
	for _, edit := range []func(*cache.FillLease){
		func(l *cache.FillLease) { l.Key.Operation = cache.OperationCompact }, func(l *cache.FillLease) { l.Key.ScopeID = "" },
		func(l *cache.FillLease) { l.Key.RequestIndex = -1 }, func(l *cache.FillLease) { l.Key.Fingerprint = cache.Fingerprint{} },
		func(l *cache.FillLease) { l.OperationID = "" }, func(l *cache.FillLease) { l.Attempt = "" },
		func(l *cache.FillLease) { l.AcquiredAt = time.Time{} }, func(l *cache.FillLease) { l.ExpiresAt = l.AcquiredAt.Add(16 * time.Minute) },
		func(l *cache.FillLease) { l.Key.Route.Revision = "" }, func(l *cache.FillLease) { l.Key.Route.Compiler = "" },
	} {
		invalid := lease
		edit(&invalid)
		if _, err := c.PrepareGenerate(context.Background(), invalid, nil); err == nil {
			t.Fatal("invalid input accepted")
		}
	}
	zero := time.Duration(0)
	if _, err := c.PrepareGenerate(context.Background(), lease, &zero); err == nil {
		t.Fatal("invalid max age accepted")
	}
	if _, err := c.PrepareGenerate(nil, lease, nil); err == nil {
		t.Fatal("nil context accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.PrepareGenerate(ctx, lease, nil); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	lease.Key.Operation, lease.Key.RequestIndex = cache.OperationCompact, -1
	if _, err := c.PrepareCompact(context.Background(), lease, nil); err == nil {
		t.Fatal("negative compact sample accepted")
	}
}
