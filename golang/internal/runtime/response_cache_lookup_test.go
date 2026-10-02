package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/cache"
	"github.com/mfow/llm-temporal-worker/golang/compaction"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/state"
	"github.com/mfow/llm-temporal-worker/golang/storage/durable"
)

type lookupResponses struct {
	cache.ResponseRepository
	lookup func(context.Context, cache.ResponseLookup) (*cache.ResponseEntry, error)
}

func (s *lookupResponses) Lookup(ctx context.Context, input cache.ResponseLookup) (*cache.ResponseEntry, error) {
	return s.lookup(ctx, input)
}

type lookupFills struct {
	cache.FillRepository
	acquire func(context.Context, cache.FillLease) (cache.FillDecision, error)
	start   func(context.Context, cache.FillLease, time.Time) (bool, error)
	release func(context.Context, cache.FillLease, time.Time) error
}

func (s *lookupFills) Acquire(ctx context.Context, input cache.FillLease) (cache.FillDecision, error) {
	return s.acquire(ctx, input)
}
func (s *lookupFills) Start(ctx context.Context, input cache.FillLease, now time.Time) (bool, error) {
	return s.start(ctx, input, now)
}
func (s *lookupFills) Release(ctx context.Context, input cache.FillLease, now time.Time) error {
	return s.release(ctx, input, now)
}

type cacheLookupFixture struct {
	cap                 V1RuntimeCapabilities
	helper              *ResponseCacheLookup
	responses           *lookupResponses
	fills               *lookupFills
	gen                 llm.GenerateRequestV1
	compact             llm.CompactRequestV1
	genReplay           durable.GenerateReplay
	compactReplay       durable.CompactReplay
	genLease, compLease cache.FillLease
	events              []string
	lookups             []cache.ResponseLookup
	plans               int
	now                 time.Time
}

func newCacheLookupFixture(t *testing.T) *cacheLookupFixture {
	t.Helper()
	f := &cacheLookupFixture{now: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)}
	_, materializer, generate, compact := checkpointReplayFixture(t)
	f.gen, f.compact = generate, compact
	f.gen.Parent = nil
	f.gen.SettingsPatch.Model = llm.Patch[string]{Set: preparationPointer("model")}
	f.gen.Cache = &llm.CachePolicyV1{MaxAgeSeconds: 60}
	f.compact.Cache = &llm.CachePolicyV1{MaxAgeSeconds: 120}
	f.compactReplay.State = materializer.result
	f.compactReplay.State.Settings = state.RootModelState("model")
	policy := compaction.DefaultPolicy()
	policy.RecentTurns = 0
	f.compactReplay.State.Settings.CompactionPolicy, _ = json.Marshal(policy)
	f.genLease = cache.FillLease{Key: cache.ResponseKey{ScopeID: "opaque-scope", Operation: cache.OperationGenerate,
		Route: cache.RouteIdentity{Provider: "provider", Endpoint: "endpoint-1", Model: "model", Revision: "revision", Compiler: "compiler", Account: "account", Region: "region"}, Fingerprint: cache.Fingerprint{1}},
		OperationID: "operation-1", Attempt: "generation-1", AcquiredAt: f.now, ExpiresAt: f.now.Add(time.Minute)}
	f.compLease = f.genLease
	f.compLease.Key.Operation = cache.OperationCompact
	f.responses = &lookupResponses{lookup: func(_ context.Context, input cache.ResponseLookup) (*cache.ResponseEntry, error) {
		f.events = append(f.events, "lookup")
		f.lookups = append(f.lookups, input)
		return nil, nil
	}}
	f.fills = &lookupFills{
		acquire: func(_ context.Context, input cache.FillLease) (cache.FillDecision, error) {
			f.events = append(f.events, "acquire")
			return cache.FillDecision{Disposition: cache.FillOwned, Record: cache.FillRecord{Lease: input, State: cache.FillHeld}}, nil
		},
		start: func(context.Context, cache.FillLease, time.Time) (bool, error) {
			f.events = append(f.events, "start")
			return true, nil
		},
		release: func(context.Context, cache.FillLease, time.Time) error {
			f.events = append(f.events, "release")
			return nil
		},
	}
	composition := validCapabilityComposition()
	composition.Identity.Postgres = durable.PostgresIdentity{}
	composition.Identity.Cloud = durable.CloudIdentity{Provider: "aws", Namespace: "requests-v1", RequestTable: "requests", PayloadStore: "payloads", ProviderDigest: [32]byte{2}}
	f.cap = V1RuntimeCapabilities{ConfigDigest: composition.Identity.ConfigDigest, CloudIdentity: composition.Identity.Cloud, composition: &composition, Responses: f.responses, ResponseFills: f.fills, Clock: func() time.Time { return f.now }}
	var err error
	f.helper, err = f.cap.NewResponseCacheLookup(
		func(_ context.Context, request llm.GenerateRequestV1, prepared PreparedGenerateInput) (cache.FillLease, error) {
			f.plans++
			if !reflect.DeepEqual(request, f.gen) || prepared.Request.Model != "model" || prepared.SampleIndex != int64(request.Cache.Variant) {
				t.Fatal("Generate planner lost request or prepared input")
			}
			return f.genLease, nil
		},
		func(_ context.Context, request llm.CompactRequestV1, prepared PreparedCompactInput) (cache.FillLease, error) {
			f.plans++
			if !reflect.DeepEqual(request, f.compact) || prepared.Request == nil || prepared.Request.Model != "model" {
				t.Fatal("Compact planner lost request or prepared input")
			}
			return f.compLease, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func assertCacheLookupError(t *testing.T, err error, code provider.Code, retry provider.RetryDisposition) {
	t.Helper()
	var mapped *provider.Error
	if !errors.As(err, &mapped) || mapped.Code != code || mapped.Phase != provider.PhaseStateLoad || mapped.Dispatch != provider.DispatchNotDispatched || mapped.Retry != retry {
		t.Fatalf("error = %#v, want %s/%s before dispatch", err, code, retry)
	}
	if strings.Contains(err.Error(), "sensitive") || mapped.Cause != nil || len(mapped.SafeDetails) != 0 {
		t.Fatal("exposed planner or cache-store error")
	}
}

func TestResponseCacheLookupRequiresBoundSnapshotAndCompleteCapabilities(t *testing.T) {
	for _, name := range []string{"composition", "digest", "cloud", "responses", "nil responses", "fills", "nil fills", "clock", "Generate planner", "Compact planner"} {
		t.Run(name, func(t *testing.T) {
			f := newCacheLookupFixture(t)
			g, c := f.helper.generate, f.helper.compact
			f.cap.CompositionFactory = func(context.Context, V1RuntimeCapabilities) (durable.Composition, error) {
				t.Fatal("lookup constructed another composition")
				return durable.Composition{}, nil
			}
			switch name {
			case "composition":
				f.cap.composition = nil
			case "digest":
				f.cap.ConfigDigest = [32]byte{3}
			case "cloud":
				f.cap.CloudIdentity.Namespace = "other"
			case "responses":
				f.cap.Responses = nil
			case "nil responses":
				var s *lookupResponses
				f.cap.Responses = s
			case "fills":
				f.cap.ResponseFills = nil
			case "nil fills":
				var s *lookupFills
				f.cap.ResponseFills = s
			case "clock":
				f.cap.Clock = nil
			case "Generate planner":
				g = nil
			case "Compact planner":
				c = nil
			}
			if _, err := f.cap.NewResponseCacheLookup(g, c); err == nil {
				t.Fatal("accepted incomplete or mixed capabilities")
			}
		})
	}
}

func TestResponseCacheLookupOmissionSkipsPlannerAndStores(t *testing.T) {
	f := newCacheLookupFixture(t)
	f.gen.Cache, f.compact.Cache = nil, nil
	g, err := f.helper.Generate(context.Background(), f.gen, f.genReplay)
	if err != nil || g.Disposition != durable.CacheDisabled || g.Entry() != nil {
		t.Fatalf("Generate = %+v, %v", g, err)
	}
	c, err := f.helper.Compact(context.Background(), f.compact, f.compactReplay)
	if err != nil || c.Disposition != durable.CacheDisabled || c.Entry() != nil {
		t.Fatalf("Compact = %+v, %v", c, err)
	}
	if f.plans != 0 || len(f.events) != 0 {
		t.Fatalf("disabled cache reached planner/stores: %d, %v", f.plans, f.events)
	}
}

func TestResponseCacheLookupBothPathsPreserveLeaseSampleAndFreshness(t *testing.T) {
	f := newCacheLookupFixture(t)
	f.gen.Cache.Variant, f.genLease.Key.RequestIndex = 7, 7
	g, err := f.helper.Generate(context.Background(), f.gen, f.genReplay)
	if err != nil || g.Disposition != durable.CacheMiss {
		t.Fatalf("Generate = %+v, %v", g, err)
	}
	c, err := f.helper.Compact(context.Background(), f.compact, f.compactReplay)
	if err != nil || c.Disposition != durable.CacheMiss {
		t.Fatalf("Compact = %+v, %v", c, err)
	}
	if f.plans != 2 || !reflect.DeepEqual(f.events, []string{"lookup", "acquire", "lookup", "lookup", "acquire", "lookup"}) {
		t.Fatal(f.events)
	}
	for i, query := range f.lookups {
		lease, age := f.genLease, time.Minute
		if i >= 2 {
			lease, age = f.compLease, 2*time.Minute
		}
		if query.Key != lease.Key || !query.Now.Equal(f.now) || query.MaxAge == nil || *query.MaxAge != age {
			t.Fatalf("lookup lost policy or domain: %+v", query)
		}
	}
}

func TestResponseCacheLookupRejectsInvalidRequestsAndPlansBeforeStorage(t *testing.T) {
	for _, name := range []string{"Generate request", "Compact request", "planner", "domain", "sample", "Compact sample", "operation", "attempt", "scope", "fingerprint", "route", "lease time", "expired", "age", "completed", "pending reconciliation", "canceled", "planning cancellation", "nil context", "nil helper"} {
		t.Run(name, func(t *testing.T) {
			f := newCacheLookupFixture(t)
			ctx := context.Background()
			code := provider.CodeConfiguration
			switch name {
			case "Generate request":
				f.gen.OperationKey = ""
				code = provider.CodeInvalidArgument
			case "Compact request":
				f.compact.Parent = ""
				code = provider.CodeInvalidArgument
			case "planner":
				f.helper.generate = func(context.Context, llm.GenerateRequestV1, PreparedGenerateInput) (cache.FillLease, error) {
					return cache.FillLease{}, errors.New("sensitive planner error")
				}
			case "domain":
				f.genLease.Key.Operation = cache.OperationCompact
			case "sample":
				f.genLease.Key.RequestIndex++
			case "Compact sample":
				f.compLease.Key.RequestIndex++
			case "operation":
				f.genLease.OperationID = ""
			case "attempt":
				f.genLease.Attempt = ""
			case "scope":
				f.genLease.Key.ScopeID = ""
			case "fingerprint":
				f.genLease.Key.Fingerprint = cache.Fingerprint{}
			case "route":
				f.genLease.Key.Route.Revision = ""
			case "lease time":
				f.genLease.ExpiresAt = f.genLease.AcquiredAt
			case "expired":
				f.now = f.genLease.ExpiresAt
			case "age":
				f.gen.Cache.MaxAgeSeconds = 0
				code = provider.CodeInvalidArgument
			case "completed":
				f.genReplay.Completed = &llm.GenerateResponseV1{}
			case "pending reconciliation":
				f.genReplay.ReconciliationPending = &durable.GenerateReconciliation{}
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "planning cancellation":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				defer cancel()
				f.helper.generate = func(context.Context, llm.GenerateRequestV1, PreparedGenerateInput) (cache.FillLease, error) {
					cancel()
					return f.genLease, nil
				}
			case "nil context":
				ctx = nil
			case "nil helper":
				f.helper = nil
			}
			var err error
			if name == "Compact request" || name == "Compact sample" {
				_, err = f.helper.Compact(ctx, f.compact, f.compactReplay)
			} else {
				_, err = f.helper.Generate(ctx, f.gen, f.genReplay)
			}
			if name == "canceled" || name == "planning cancellation" {
				if !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			} else {
				assertCacheLookupError(t, err, code, provider.RetryNever)
			}
			if len(f.events) != 0 {
				t.Fatalf("invalid input reached stores: %v", f.events)
			}
		})
	}
}

func cacheLookupEntry(t *testing.T, f *cacheLookupFixture, compact bool) cache.ResponseEntry {
	t.Helper()
	lease := f.genLease
	cost := "0.10"
	var data []byte
	var err error
	if compact {
		lease = f.compLease
		data, err = json.Marshal(llm.CompactResponseV1{OperationKey: "origin-key", OperationID: "origin-operation", Checkpoint: llm.CheckpointMetadata{Handle: "origin-checkpoint", Parent: &f.compact.Parent, Kind: "compaction"}, Cache: llm.CacheDispositionV1{Disposition: "miss_populated"}, Cost: llm.CostV1{Status: "exact", ActualCostUSD: &cost, Method: "provider_reported"}})
	} else {
		response := builderFinalization(f.gen, "origin-operation").Response
		response.OperationKey, response.Checkpoint.Handle, response.Cache.Disposition = "origin-key", "origin-checkpoint", "miss_populated"
		response.Cache.Variant = f.gen.Cache.Variant
		data, err = json.Marshal(response)
	}
	if err != nil {
		t.Fatal(err)
	}
	return cache.ResponseEntry{ID: "entry-origin", Key: lease.Key, OriginOperationID: "origin-operation", OriginCheckpointID: "checkpoint-origin", CompletedAt: f.now.Add(-30 * time.Second), Response: data}
}

func TestResponseCacheLookupHitsPreserveOriginMetadata(t *testing.T) {
	for _, compact := range []bool{false, true} {
		t.Run(map[bool]string{false: "Generate", true: "Compact"}[compact], func(t *testing.T) {
			f := newCacheLookupFixture(t)
			entry := cacheLookupEntry(t, f, compact)
			f.responses.lookup = func(context.Context, cache.ResponseLookup) (*cache.ResponseEntry, error) { return &entry, nil }
			var got *cache.ResponseEntry
			if compact {
				d, err := f.helper.Compact(context.Background(), f.compact, f.compactReplay)
				if err != nil || d.Disposition != durable.CacheHit || d.Response.OperationID != string(entry.OriginOperationID) {
					t.Fatalf("Compact = %+v, %v", d, err)
				}
				got = d.Entry()
			} else {
				d, err := f.helper.Generate(context.Background(), f.gen, f.genReplay)
				if err != nil || d.Disposition != durable.CacheHit || d.Response.OperationID != string(entry.OriginOperationID) {
					t.Fatalf("Generate = %+v, %v", d, err)
				}
				got = d.Entry()
			}
			if !reflect.DeepEqual(*got, entry) || len(f.events) != 0 {
				t.Fatal("origin receipt changed or hit acquired a fill")
			}
			got.Response[0] = '!'
			if entry.Response[0] != '{' {
				t.Fatal("shared origin response bytes")
			}
		})
	}
}

func TestResponseCacheLookupStoreFailuresNeverBecomeMisses(t *testing.T) {
	for _, compact := range []bool{false, true} {
		for _, stage := range []string{"lookup", "acquire", "second lookup", "release"} {
			t.Run(map[bool]string{false: "Generate/", true: "Compact/"}[compact]+stage, func(t *testing.T) {
				f := newCacheLookupFixture(t)
				calls := 0
				entry := cacheLookupEntry(t, f, compact)
				failure := errors.New("sensitive cloud SDK error")
				f.responses.lookup = func(context.Context, cache.ResponseLookup) (*cache.ResponseEntry, error) {
					calls++
					if stage == "lookup" || (stage == "second lookup" && calls == 2) {
						return nil, failure
					}
					if stage == "release" && calls == 2 {
						return &entry, nil
					}
					return nil, nil
				}
				if stage == "acquire" {
					f.fills.acquire = func(context.Context, cache.FillLease) (cache.FillDecision, error) {
						return cache.FillDecision{}, failure
					}
				}
				f.fills.release = func(context.Context, cache.FillLease, time.Time) error { return failure }
				var err error
				if compact {
					_, err = f.helper.Compact(context.Background(), f.compact, f.compactReplay)
				} else {
					_, err = f.helper.Generate(context.Background(), f.gen, f.genReplay)
				}
				assertCacheLookupError(t, err, provider.CodeStateUnavailable, provider.RetrySameOperation)
			})
		}
	}
}

func TestResponseCacheLookupRejectsCorruptOrigins(t *testing.T) {
	for _, compact := range []bool{false, true} {
		for _, name := range []string{"scope", "route", "sample", "response sample", "expired", "future", "malformed", "origin"} {
			t.Run(map[bool]string{false: "Generate/", true: "Compact/"}[compact]+name, func(t *testing.T) {
				f := newCacheLookupFixture(t)
				entry := cacheLookupEntry(t, f, compact)
				switch name {
				case "scope":
					entry.Key.ScopeID = "other"
				case "route":
					entry.Key.Route.Account = "other"
				case "sample":
					entry.Key.RequestIndex++
				case "response sample":
					if compact {
						var response llm.CompactResponseV1
						if err := json.Unmarshal(entry.Response, &response); err != nil {
							t.Fatal(err)
						}
						response.Cache.Variant = 1
						var err error
						entry.Response, err = json.Marshal(response)
						if err != nil {
							t.Fatal(err)
						}
					} else {
						var response llm.GenerateResponseV1
						if err := json.Unmarshal(entry.Response, &response); err != nil {
							t.Fatal(err)
						}
						response.Cache.Variant++
						var err error
						entry.Response, err = json.Marshal(response)
						if err != nil {
							t.Fatal(err)
						}
					}
				case "expired":
					entry.CompletedAt = f.now.Add(-10 * time.Minute)
				case "future":
					entry.CompletedAt = f.now.Add(time.Second)
				case "malformed":
					entry.Response = []byte(`{"sensitive":"invalid"}`)
				case "origin":
					entry.OriginOperationID = "other"
				}
				f.responses.lookup = func(context.Context, cache.ResponseLookup) (*cache.ResponseEntry, error) { return &entry, nil }
				var err error
				if compact {
					_, err = f.helper.Compact(context.Background(), f.compact, f.compactReplay)
				} else {
					_, err = f.helper.Generate(context.Background(), f.gen, f.genReplay)
				}
				assertCacheLookupError(t, err, provider.CodeStateCorrupt, provider.RetryNever)
			})
		}
	}
}

func TestResponseCacheLookupRunnerGatesBothActivities(t *testing.T) {
	for _, compact := range []bool{false, true} {
		for _, outcome := range []string{"miss", "hit", "wait", "recovery", "finished", "changed route", "uncertain start", "lookup failure"} {
			t.Run(map[bool]string{false: "Generate/", true: "Compact/"}[compact]+outcome, func(t *testing.T) {
				f := newCacheLookupFixture(t)
				entry := cacheLookupEntry(t, f, compact)
				lease := f.genLease
				if compact {
					lease = f.compLease
				}
				switch outcome {
				case "hit":
					f.responses.lookup = func(context.Context, cache.ResponseLookup) (*cache.ResponseEntry, error) {
						f.events = append(f.events, "lookup")
						return &entry, nil
					}
				case "wait", "recovery", "finished":
					f.fills.acquire = func(context.Context, cache.FillLease) (cache.FillDecision, error) {
						f.events = append(f.events, "acquire")
						owner := lease
						owner.OperationID, owner.Attempt = "other-operation", "other-generation"
						disposition, state := cache.FillWait, cache.FillHeld
						if outcome == "recovery" {
							disposition, state = cache.FillRecoveryNeeded, cache.FillStarted
						} else if outcome == "finished" {
							disposition, state = cache.FillAttemptFinished, cache.FillFinished
						}
						return cache.FillDecision{Disposition: disposition, Record: cache.FillRecord{Lease: owner, State: state}}, nil
					}
				case "uncertain start":
					f.fills.start = func(context.Context, cache.FillLease, time.Time) (bool, error) {
						f.events = append(f.events, "start")
						return false, errors.New("lost acknowledgement")
					}
				case "lookup failure":
					f.responses.lookup = func(context.Context, cache.ResponseLookup) (*cache.ResponseEntry, error) {
						f.events = append(f.events, "lookup")
						return nil, errors.New("sensitive SDK error")
					}
				}
				ports := validBuilderGeneratePorts(&f.events)
				ports.Replay = func(context.Context, llm.GenerateRequestV1) (durable.GenerateReplay, error) {
					f.events = append(f.events, "replay")
					return f.genReplay, nil
				}
				ports.CacheLookup = f.helper.Generate
				route := durable.RoutePlan{OperationID: durable.OperationID(lease.OperationID), GenerationID: durable.GenerationID(lease.Attempt), RouteID: "route-1", EndpointID: "endpoint-1", Provider: "provider", Model: "model", CacheIdentity: lease.Key.Route}
				if outcome == "changed route" {
					route.CacheIdentity.Account = "other-account"
				}
				ports.Route = func(context.Context, llm.GenerateRequestV1, durable.GenerateReplay, durable.CompactionDecision) (durable.RoutePlan, error) {
					f.events = append(f.events, "route")
					return route, nil
				}
				stop := errors.New("test stops at provider dispatch")
				ports.Dispatch = func(context.Context, llm.GenerateRequestV1, durable.GenerateReplay, durable.RoutePlan, durable.ClaimReceipt) (durable.DispatchResult, error) {
					f.events = append(f.events, "dispatch")
					return durable.DispatchResult{}, stop
				}
				ports.FinalizeCache = func(_ context.Context, request llm.GenerateRequestV1, _ durable.GenerateReplay, decision durable.CacheDecision) (durable.GenerateFinalization, error) {
					f.events = append(f.events, "cache-finalize")
					if !reflect.DeepEqual(*decision.Entry(), entry) {
						t.Fatal("lost origin receipt")
					}
					result := builderFinalization(request, "consumer-operation")
					result.Response.Checkpoint.Kind, result.Response.Cache.Disposition = "cache_replay", "hit"
					return result, nil
				}
				var err error
				if compact {
					p := validCompactPorts()
					p.Replay = func(context.Context, llm.CompactRequestV1) (durable.CompactReplay, error) {
						f.events = append(f.events, "replay")
						return f.compactReplay, nil
					}
					p.CacheLookup = f.helper.Compact
					p.Route = func(context.Context, llm.CompactRequestV1, durable.CompactReplay) (durable.RoutePlan, error) {
						return ports.Route(context.Background(), f.gen, f.genReplay, durable.CompactionDecision{})
					}
					p.Reserve = func(ctx context.Context, _ llm.CompactRequestV1, route durable.RoutePlan) (durable.ReserveResult, error) {
						return ports.Reserve(ctx, f.gen, route)
					}
					p.Claim = func(ctx context.Context, _ llm.CompactRequestV1, route durable.RoutePlan, reservation durable.ReserveResult) (durable.ClaimReceipt, error) {
						return ports.Claim(ctx, f.gen, route, reservation)
					}
					p.Dispatch = func(context.Context, llm.CompactRequestV1, durable.CompactReplay, durable.RoutePlan, durable.ClaimReceipt) (durable.CompactDispatchResult, error) {
						f.events = append(f.events, "dispatch")
						return durable.CompactDispatchResult{}, stop
					}
					p.FinalizeCache = func(_ context.Context, request llm.CompactRequestV1, _ durable.CompactReplay, decision durable.CompactCacheDecision) (durable.CompactFinalization, error) {
						f.events = append(f.events, "cache-finalize")
						if !reflect.DeepEqual(*decision.Entry(), entry) {
							t.Fatal("lost origin receipt")
						}
						zero := "0"
						return durable.CompactFinalization{Response: llm.CompactResponseV1{OperationKey: request.OperationKey, OperationID: "consumer-operation", Checkpoint: llm.CheckpointMetadata{Handle: "consumer-checkpoint", Parent: &request.Parent, Kind: "compaction"}, Cache: llm.CacheDispositionV1{Disposition: "hit"}, Cost: llm.CostV1{Status: "exact", ActualCostUSD: &zero, Method: "provider_reported"}, Provenance: []byte(`{"source":"worker_cache"}`)}}, nil
					}
					_, err = durable.CompactV1(context.Background(), f.compact, p)
				} else {
					_, err = durable.GenerateV1(context.Background(), f.gen, ports)
				}
				want := []string{"replay", "lookup", "acquire", "lookup"}
				switch outcome {
				case "hit":
					want = []string{"replay", "lookup", "cache-finalize"}
					if err != nil {
						t.Fatal(err)
					}
				case "wait":
					if !errors.Is(err, durable.ErrCacheWait) {
						t.Fatal(err)
					}
				case "recovery", "finished":
					want = want[:3]
					if !errors.Is(err, durable.ErrCacheRecoveryRequired) {
						t.Fatal(err)
					}
				case "lookup failure":
					want = want[:2]
					assertCacheLookupError(t, err, provider.CodeStateUnavailable, provider.RetrySameOperation)
				default:
					if !compact {
						want = append(want, "compaction")
					}
					want = append(want, "route")
					if outcome != "changed route" {
						want = append(want, "reserve", "start")
					}
					if outcome == "miss" {
						want = append(want, "claim", "dispatch")
						if !errors.Is(err, stop) {
							t.Fatal(err)
						}
					} else if outcome == "uncertain start" {
						if !errors.Is(err, durable.ErrCacheRecoveryRequired) {
							t.Fatal(err)
						}
					} else if err == nil {
						t.Fatal("changed route was accepted")
					}
				}
				if !reflect.DeepEqual(f.events, want) {
					t.Fatalf("phases = %v, want %v", f.events, want)
				}
			})
		}
	}
}

func TestResponseCacheLookupUncertainAcquireRetainsOriginalLease(t *testing.T) {
	for _, compact := range []bool{false, true} {
		t.Run(map[bool]string{false: "Generate", true: "Compact"}[compact], func(t *testing.T) {
			f := newCacheLookupFixture(t)
			var leases []cache.FillLease
			f.fills.acquire = func(_ context.Context, lease cache.FillLease) (cache.FillDecision, error) {
				leases = append(leases, lease)
				if len(leases) == 1 {
					return cache.FillDecision{}, errors.New("sensitive lost write acknowledgement")
				}
				return cache.FillDecision{Disposition: cache.FillOwned, Record: cache.FillRecord{Lease: lease, State: cache.FillHeld}}, nil
			}
			lookup := func() (durable.CacheDisposition, error) {
				if compact {
					d, err := f.helper.Compact(context.Background(), f.compact, f.compactReplay)
					return d.Disposition, err
				}
				d, err := f.helper.Generate(context.Background(), f.gen, f.genReplay)
				return d.Disposition, err
			}
			_, err := lookup()
			assertCacheLookupError(t, err, provider.CodeStateUnavailable, provider.RetrySameOperation)
			f.now = f.now.Add(10 * time.Second)
			disposition, err := lookup()
			if err != nil || disposition != durable.CacheMiss || len(leases) != 2 || !reflect.DeepEqual(leases[0], leases[1]) {
				t.Fatalf("uncertain acquire changed persisted lease: %v, %v, %v", leases, disposition, err)
			}
		})
	}
}

func TestResponseCacheLookupRetainsOriginalSnapshot(t *testing.T) {
	f := newCacheLookupFixture(t)
	newNow := f.now.Add(time.Hour)
	newLease := f.genLease
	newLease.AcquiredAt, newLease.ExpiresAt = newNow, newNow.Add(time.Minute)
	entry := cacheLookupEntry(t, f, false)
	entry.CompletedAt = newNow.Add(-30 * time.Second)
	newReads := 0
	f.cap.Responses = &lookupResponses{lookup: func(context.Context, cache.ResponseLookup) (*cache.ResponseEntry, error) {
		newReads++
		return &entry, nil
	}}
	f.cap.ResponseFills = &lookupFills{} // A hit must never invoke this store.
	f.cap.Clock = func() time.Time { return newNow }
	next, err := f.cap.NewResponseCacheLookup(func(context.Context, llm.GenerateRequestV1, PreparedGenerateInput) (cache.FillLease, error) {
		return newLease, nil
	}, f.helper.compact)
	if err != nil {
		t.Fatal(err)
	}
	old, err := f.helper.Generate(context.Background(), f.gen, f.genReplay)
	if err != nil || old.Disposition != durable.CacheMiss || newReads != 0 || len(f.lookups) != 2 || !f.lookups[0].Now.Equal(f.now) {
		t.Fatalf("old snapshot mixed repositories/clock: %+v, %v", old, err)
	}
	current, err := next.Generate(context.Background(), f.gen, f.genReplay)
	if err != nil || current.Disposition != durable.CacheHit || newReads != 1 {
		t.Fatalf("new snapshot was not captured: %+v, %v", current, err)
	}
}
