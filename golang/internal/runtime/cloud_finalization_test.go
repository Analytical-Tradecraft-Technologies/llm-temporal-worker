package runtime

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/mfow/llm-temporal-worker/golang/budget"
	"github.com/mfow/llm-temporal-worker/golang/cache"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/mfow/llm-temporal-worker/golang/state"
	"github.com/mfow/llm-temporal-worker/golang/storage/cloudstate"
	"github.com/mfow/llm-temporal-worker/golang/storage/durable"
)

type replayEffectsStore struct {
	cache.ResponseRepository
	cache.FillRepository
	durable.BudgetLeaser
	events      []string
	fail        string
	settlements []durable.ReconcileRequest
	completions []cache.FillCompletion
	uses        []cache.ResponseUse
}

func (s *replayEffectsStore) step(name string) error {
	s.events = append(s.events, name)
	if s.fail == name {
		return contracts.ErrOutcomeUnknown
	}
	return nil
}
func (s *replayEffectsStore) Publish(context.Context, cache.ResponseEntry) error {
	return s.step("publish")
}
func (s *replayEffectsStore) Reconcile(_ context.Context, request durable.ReconcileRequest) error {
	s.settlements = append(s.settlements, request)
	return s.step("budget")
}
func (s *replayEffectsStore) Complete(_ context.Context, _ cache.FillLease, completion cache.FillCompletion) error {
	s.completions = append(s.completions, completion)
	return s.step("fill")
}
func (s *replayEffectsStore) RecordUse(_ context.Context, use cache.ResponseUse) error {
	s.uses = append(s.uses, use)
	return s.step("use")
}

func finalizationFixture(t *testing.T, kind, mode string) (*cloudRequestRuntime, *recordingCloudRequests, *replayEffectsStore, func() ([]byte, error), func(FinalizationEffects) error, FinalizationEffects) {
	t.Helper()
	r, repository, inner, generate := cloudRuntimeFixture()
	now := time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC)
	r.clock = func() time.Time { return now }
	effectsStore := &replayEffectsStore{}
	finalizer, err := newCloudFinalizer(repository, effectsStore, effectsStore, effectsStore, r.clock)
	if err != nil {
		t.Fatal(err)
	}
	r.finalizer = finalizer
	inner.generate = func(context.Context, llm.GenerateRequestV1) (llm.GenerateResponseV1, error) {
		t.Fatal("inner Generate invoked during finalization replay")
		return llm.GenerateResponseV1{}, nil
	}
	inner.compact = func(context.Context, llm.CompactRequestV1) (llm.CompactResponseV1, error) {
		t.Fatal("inner Compact invoked during finalization replay")
		return llm.CompactResponseV1{}, nil
	}
	operation := "internal-operation-id"
	checkpoint := "consumer-checkpoint"
	response := builderFinalization(generate, durable.OperationID(operation)).Response
	response.Cache.Disposition = "miss_populated"
	cost := "0.07"
	response.Cost.ActualCostUSD = &cost
	originData, _ := json.Marshal(response)
	key := cache.ResponseKey{ScopeID: "trusted-scope", Operation: cache.OperationKind(kind)}
	entry := cache.ResponseEntry{ID: "entry", Key: key, OriginOperationID: "internal-operation-id", OriginCheckpointID: "consumer-checkpoint", CompletedAt: now.Add(time.Second), Response: originData}
	lease := cache.FillLease{Key: key, OperationID: entry.OriginOperationID, Attempt: "generation", AcquiredAt: now, ExpiresAt: now.Add(time.Minute)}
	actual := pricing.MustUSD("0.07")
	settlement := durable.ReconcileRequest{OperationID: durable.OperationID(operation), GenerationID: "generation", IncarnationID: "original-redis-incarnation", Events: []budget.CompletionEvent{{EventID: "exact-event", GenerationID: "generation", OperationID: operation, WindowID: "window", BucketStart: now, ReservationRevision: 2, Kind: budget.JournalFinalizeExact, ReservedDecreaseUSD: pricing.MustUSD("0.10"), AccountedIncreaseUSD: actual, ActualCostUSD: &actual, CostStatus: budget.CostExact, OccurredAt: now.Add(time.Second)}}}
	effects := FinalizationEffects{Provider: &ProviderFinalizationEffects{Lease: lease, Entry: &entry, Completion: cache.FillCompletion{Outcome: cache.FillPublished, EntryID: entry.ID, CompletedAt: entry.CompletedAt}, Budget: settlement}}
	if mode == "cache" {
		zero := "0"
		response.Cache.Disposition = "hit"
		response.Cost.ActualCostUSD = &zero
		response.Checkpoint.Kind = "cache_replay"
		entry.OriginOperationID = "origin-operation"
		entry.OriginCheckpointID = "origin-checkpoint"
		effects = FinalizationEffects{Cache: &CacheFinalizationEffects{Origin: entry, Use: cache.ResponseUse{ScopeID: key.ScopeID, OperationID: "internal-operation-id", EntryID: entry.ID, CheckpointID: "consumer-checkpoint", CompletedAt: now.Add(time.Minute)}}}
	}
	if kind == "generate" {
		run := func() ([]byte, error) {
			v, e := r.GenerateV1(context.Background(), generate)
			if e != nil {
				return nil, e
			}
			return json.Marshal(v)
		}
		save := func(effects FinalizationEffects) error {
			return finalizer.SaveGenerate(context.Background(), generate, key.ScopeID, "consumer-checkpoint", response, effects)
		}
		return r, repository, effectsStore, run, save, effects
	}
	compact := llm.CompactRequestV1{OperationKey: generate.OperationKey, Context: generate.Context, Parent: "parent-checkpoint"}
	compactResponse := llm.CompactResponseV1{APIVersion: llm.CompactAPIVersion, OperationKey: compact.OperationKey, OperationID: operation, Checkpoint: llm.CheckpointMetadata{Handle: "checkpoint-1", Kind: "compaction", Parent: &compact.Parent}, Cache: response.Cache, Cost: response.Cost}
	if effects.Provider != nil {
		data, _ := json.Marshal(compactResponse)
		effects.Provider.Entry.Response = data
	}
	run := func() ([]byte, error) {
		v, e := r.CompactV1(context.Background(), compact)
		if e != nil {
			return nil, e
		}
		return json.Marshal(v)
	}
	save := func(effects FinalizationEffects) error {
		return finalizer.SaveCompact(context.Background(), compact, key.ScopeID, state.CheckpointID(checkpoint), compactResponse, effects)
	}
	return r, repository, effectsStore, run, save, effects
}

func TestCloudFinalizationReplayAfterEveryUncertainWrite(t *testing.T) {
	for _, kind := range []string{"generate", "compact"} {
		for _, failure := range []string{"save", "publish", "budget", "fill", "terminal"} {
			t.Run(kind+"/"+failure, func(t *testing.T) {
				r, repository, store, run, save, effects := finalizationFixture(t, kind, "provider")
				if failure == "save" {
					repository.saveHandoffErr = contracts.ErrOutcomeUnknown
				}
				saveErr := save(effects)
				if failure == "save" {
					if saveErr == nil {
						t.Fatal("lost save acknowledgement hidden")
					}
				} else if saveErr != nil {
					t.Fatal(saveErr)
				}
				if len(store.events) != 0 {
					t.Fatal("effects before handoff save")
				}
				repository.saveHandoffErr = nil
				if failure == "terminal" {
					repository.completeErr = contracts.ErrOutcomeUnknown
				} else if failure != "save" {
					store.fail = failure
				}
				if failure != "save" {
					if _, err := run(); err == nil {
						t.Fatal("lost acknowledgement hidden")
					}
					if repository.record.Status != cloudstate.StatusRunning {
						t.Fatal("premature terminal success")
					}
				}
				if failure == "publish" && !reflect.DeepEqual(store.events, []string{"publish"}) {
					t.Fatal("budget settled before publication", store.events)
				}
				if failure == "budget" && !reflect.DeepEqual(store.events, []string{"publish", "budget"}) {
					t.Fatal("fill completed before budget", store.events)
				}
				store.fail = ""
				store.events = nil
				repository.completeErr = nil
				// Rebuild the adapter to lose all process-local state and advance the clock.
				f, err := newCloudFinalizer(repository, store, store, store, func() time.Time { return r.clock().Add(48 * time.Hour) })
				if err != nil {
					t.Fatal(err)
				}
				r.finalizer = f
				if _, err := run(); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(store.events, []string{"publish", "budget", "fill"}) {
					t.Fatal("incorrect replay order", store.events)
				}
				if repository.record.Status != cloudstate.StatusCompleted {
					t.Fatal("not completed")
				}
				// JSON persistence strips monotonic/location representation; compare the
				// exact serialized settlement rather than reconstructing current inputs.
				want, _ := json.Marshal(effects.Provider.Budget)
				for _, got := range store.settlements {
					data, _ := json.Marshal(got)
					if string(data) != string(want) {
						t.Fatal("changed original settlement")
					}
				}
				for _, got := range store.completions {
					if !got.CompletedAt.Equal(effects.Provider.Completion.CompletedAt) {
						t.Fatal("changed completion timestamp")
					}
				}
				before := len(store.events)
				if _, err := run(); err != nil {
					t.Fatal(err)
				}
				if len(store.events) != before {
					t.Fatal("completed replay performed effects")
				}
			})
		}
	}
}

func TestCloudFinalizationCacheUseRetriesWithoutBudget(t *testing.T) {
	for _, kind := range []string{"generate", "compact"} {
		t.Run(kind, func(t *testing.T) {
			_, repository, store, run, save, effects := finalizationFixture(t, kind, "cache")
			if err := save(effects); err != nil {
				t.Fatal(err)
			}
			store.fail = "use"
			if _, err := run(); err == nil {
				t.Fatal("lost use acknowledgement hidden")
			}
			store.fail = ""
			if _, err := run(); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(store.events, []string{"use", "use"}) || len(store.settlements) != 0 {
				t.Fatal("cache hit touched provider/budget", store.events)
			}
			if !store.uses[0].CompletedAt.Equal(store.uses[1].CompletedAt) || repository.record.Status != cloudstate.StatusCompleted {
				t.Fatal("changed use receipt")
			}
		})
	}
}

func TestCloudFinalizationRejectsInvalidSavedPayloadBeforeEffects(t *testing.T) {
	for _, mutation := range []string{"version", "mode", "response", "generation", "scope", "checkpoint", "both", "unknown_field", "missing_checkpoint", "cost", "index", "operation", "incomplete", "event", "stale_missing_checkpoint"} {
		t.Run(mutation, func(t *testing.T) {
			_, repository, store, run, save, effects := finalizationFixture(t, "generate", "provider")
			if err := save(effects); err != nil {
				t.Fatal(err)
			}
			var payload cloudFinalizationPayload
			if err := json.Unmarshal(repository.handoff.Payload, &payload); err != nil {
				t.Fatal(err)
			}
			switch mutation {
			case "version":
				payload.Version = 2
			case "mode":
				repository.handoff.Mode = "cache"
			case "response":
				payload.Response = json.RawMessage(`{}`)
			case "generation":
				payload.Effects.Provider.Budget.GenerationID = "new-generation"
			case "scope":
				payload.Effects.Provider.Lease.Key.ScopeID = "other-scope"
			case "checkpoint":
				repository.handoff.CheckpointID = "other-checkpoint"
			case "cost":
				wrong := pricing.MustUSD("0.08")
				payload.Effects.Provider.Budget.Events[0].ActualCostUSD = &wrong
				payload.Effects.Provider.Budget.Events[0].AccountedIncreaseUSD = wrong
			case "index":
				payload.Effects.Provider.Lease.Key.RequestIndex = 1
			case "operation":
				repository.handoff.OperationID = "other-operation"
			case "incomplete":
				var response llm.GenerateResponseV1
				_ = json.Unmarshal(payload.Response, &response)
				response.Status = llm.ResponseStatusLength
				payload.Response, _ = json.Marshal(response)
				payload.Effects.Provider.Entry.Response = payload.Response
			case "event":
				payload.Effects.Provider.Budget.Events[0].Kind = budget.JournalRelease
			case "both":
				payload.Effects.Cache = &CacheFinalizationEffects{}
			case "stale_missing_checkpoint":
				repository.record.Progress = json.RawMessage(`{"version":1}`)
				repository.loadHandoffErr = contracts.ErrNotFound
			case "missing_checkpoint":
				repository.loadHandoffErr = contracts.ErrNotFound
			}
			repository.handoff.Payload, _ = json.Marshal(payload)
			if mutation == "unknown_field" {
				var fields map[string]any
				_ = json.Unmarshal(repository.handoff.Payload, &fields)
				fields["unexpected"] = true
				repository.handoff.Payload, _ = json.Marshal(fields)
			}
			if _, err := run(); err == nil {
				t.Fatal("invalid handoff accepted")
			}
			if len(store.events) != 0 || repository.record.Status != cloudstate.StatusRunning {
				t.Fatal("invalid handoff caused effects")
			}
		})
	}
}

func TestCloudFinalizationUncacheableResponseSettlesWithoutPublication(t *testing.T) {
	_, _, store, run, save, effects := finalizationFixture(t, "generate", "provider")
	effects.Provider.Entry = nil
	effects.Provider.Completion.Outcome = cache.FillNotCacheable
	effects.Provider.Completion.EntryID = ""
	if err := save(effects); err != nil {
		t.Fatal(err)
	}
	if _, err := run(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(store.events, []string{"budget", "fill"}) {
		t.Fatal(store.events)
	}
}

func TestCloudFinalizationDependenciesFailClosed(t *testing.T) {
	_, repository, store, _, save, effects := finalizationFixture(t, "generate", "provider")
	if err := save(FinalizationEffects{}); err == nil || repository.handoff != nil {
		t.Fatal("empty effects saved")
	}
	var typedNil *recordingCloudRequests
	if _, err := newCloudFinalizer(typedNil, store, store, store, time.Now); err == nil {
		t.Fatal("typed nil store accepted")
	}
	f, err := newCloudFinalizer(repository, store, store, nil, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	request := llm.GenerateRequestV1{OperationKey: "operation-1", Context: llm.RequestContext{Tenant: "tenant", Project: "project", Actor: "actor"}}
	if err := f.SaveGenerate(context.Background(), request, "trusted-scope", "consumer-checkpoint", builderFinalization(request, "internal-operation-id").Response, effects); err == nil || repository.handoff != nil {
		t.Fatal("provider saved without budget authority")
	}
}

func TestCloudFinalizationFirstAttemptAndReplayUseSameBoundary(t *testing.T) {
	r, repository, store, run, _, effects := finalizationFixture(t, "generate", "provider")
	inner := r.inner.(*cloudInnerRuntime)
	calls := 0
	inner.generate = func(ctx context.Context, request llm.GenerateRequestV1) (llm.GenerateResponseV1, error) {
		calls++
		var response llm.GenerateResponseV1
		if err := json.Unmarshal(effects.Provider.Entry.Response, &response); err != nil {
			t.Fatal(err)
		}
		err := r.finalizer.CompleteGenerate(ctx, request, "trusted-scope", "consumer-checkpoint", response, effects)
		return response, err
	}
	store.fail = "budget"
	if _, err := run(); err == nil {
		t.Fatal("lost settlement acknowledgement hidden")
	}
	if repository.handoff == nil || calls != 1 || !reflect.DeepEqual(store.events, []string{"publish", "budget"}) {
		t.Fatal("handoff not saved before effects")
	}
	store.fail = ""
	if _, err := run(); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatal("replay repeated provider phases")
	}
}

func TestCloudFinalizationToolCallsArePublishable(t *testing.T) {
	r, repository, store, run, _, effects := finalizationFixture(t, "generate", "provider")
	request := llm.GenerateRequestV1{OperationKey: "operation-1", Context: llm.RequestContext{Tenant: "tenant", Project: "project", Actor: "actor"}}
	var response llm.GenerateResponseV1
	if err := json.Unmarshal(effects.Provider.Entry.Response, &response); err != nil {
		t.Fatal(err)
	}
	response.Status = llm.ResponseStatusToolCalls
	effects.Provider.Entry.Response, _ = json.Marshal(response)
	if err := r.finalizer.CompleteGenerate(context.Background(), request, "trusted-scope", "consumer-checkpoint", response, effects); err != nil {
		t.Fatal(err)
	}
	if repository.handoff == nil || !reflect.DeepEqual(store.events, []string{"publish", "budget", "fill"}) {
		t.Fatal("tool calls were not published")
	}
	if _, err := run(); err != nil {
		t.Fatal(err)
	}
}

func TestCloudFinalizationIncompleteResponseIsReturnedWithoutCaching(t *testing.T) {
	r, _, store, run, _, effects := finalizationFixture(t, "generate", "provider")
	request := llm.GenerateRequestV1{OperationKey: "operation-1", Context: llm.RequestContext{Tenant: "tenant", Project: "project", Actor: "actor"}}
	var response llm.GenerateResponseV1
	if err := json.Unmarshal(effects.Provider.Entry.Response, &response); err != nil {
		t.Fatal(err)
	}
	response.Status = llm.ResponseStatusLength
	response.Cache.Disposition = "miss_not_populated"
	effects.Provider.Entry = nil
	effects.Provider.Completion.Outcome = cache.FillNotCacheable
	effects.Provider.Completion.EntryID = ""
	if err := r.finalizer.SaveGenerate(context.Background(), request, "trusted-scope", "consumer-checkpoint", response, effects); err != nil {
		t.Fatal(err)
	}
	data, err := run()
	if err != nil {
		t.Fatal(err)
	}
	var got llm.GenerateResponseV1
	if json.Unmarshal(data, &got) != nil || got.Status != llm.ResponseStatusLength || !reflect.DeepEqual(store.events, []string{"budget", "fill"}) {
		t.Fatal("incomplete response treated as normal cache success")
	}
}

func TestCloudFinalizationCompletesCompactCacheUseWithoutRedis(t *testing.T) {
	r, _, store, _, _, effects := finalizationFixture(t, "compact", "cache")
	r.finalizer.budgets = nil
	request := llm.CompactRequestV1{OperationKey: "operation-1", Context: llm.RequestContext{Tenant: "tenant", Project: "project", Actor: "actor"}, Parent: "parent-checkpoint"}
	zero := "0"
	response := llm.CompactResponseV1{APIVersion: llm.CompactAPIVersion, OperationKey: request.OperationKey, OperationID: "internal-operation-id", Checkpoint: llm.CheckpointMetadata{Handle: "checkpoint-1", Kind: "compaction", Parent: &request.Parent}, Cache: llm.CacheDispositionV1{Disposition: "hit"}, Cost: llm.CostV1{Status: "exact", ActualCostUSD: &zero, Method: "provider_reported"}}
	if err := r.finalizer.CompleteCompact(context.Background(), request, "trusted-scope", "consumer-checkpoint", response, effects); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(store.events, []string{"use"}) {
		t.Fatal("cache use touched budget", store.events)
	}
}
