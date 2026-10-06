package runtime

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/cache"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/cloudstate"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/durable"
)

type attemptFinalizationStore struct {
	*recordingCloudRequests
	child       cloudstate.Record
	attempt     cloudstate.RequestAttempt
	saved       cloudstate.SavedProviderExecution
	preparation cloudstate.RequestPreparation
}

func (s *attemptFinalizationStore) Read(_ context.Context, scope cloudstate.Scope, id cloudstate.RequestID) (cloudstate.Record, error) {
	if scope != s.record.Request.Scope {
		return cloudstate.Record{}, contracts.ErrNotFound
	}
	if id == s.record.Request.ID {
		return s.record, nil
	}
	if id == s.child.Request.ID {
		return s.child, nil
	}
	return cloudstate.Record{}, contracts.ErrNotFound
}
func (s *attemptFinalizationStore) LoadRequestAttempt(_ context.Context, scope cloudstate.Scope, id cloudstate.RequestID) (cloudstate.RequestAttempt, error) {
	if scope != s.record.Request.Scope || id != s.record.Request.ID {
		return cloudstate.RequestAttempt{}, contracts.ErrNotFound
	}
	return s.attempt, nil
}
func (s *attemptFinalizationStore) LoadRequestPreparation(_ context.Context, scope cloudstate.Scope, id cloudstate.RequestID) (cloudstate.RequestPreparation, error) {
	if scope != s.record.Request.Scope || id != s.record.Request.ID {
		return cloudstate.RequestPreparation{}, contracts.ErrNotFound
	}
	return s.preparation, nil
}
func (s *attemptFinalizationStore) LoadProviderExecution(_ context.Context, scope cloudstate.Scope, id cloudstate.RequestID) (cloudstate.SavedProviderExecution, error) {
	if scope != s.record.Request.Scope || id != s.child.Request.ID {
		return cloudstate.SavedProviderExecution{}, contracts.ErrNotFound
	}
	return s.saved, nil
}

type attemptFinalizationFixture struct {
	finalizer  *CloudFinalizer
	repository *attemptFinalizationStore
	store      *replayEffectsStore
	provider   *providerExecutionFixture
	effects    FinalizationEffects
	generate   llm.GenerateResponseV1
	compact    llm.CompactResponseV1
}

func newAttemptFinalizationFixture(t *testing.T, kind string, unreserved, uncached bool) *attemptFinalizationFixture {
	t.Helper()
	configure := []func(*budgetPlanningFixture){func(f *budgetPlanningFixture) {
		for i := range f.source.value.BudgetPolicies[0].Windows {
			f.source.value.BudgetPolicies[0].Windows[i].LimitUSD = pricing.MustUSD("1")
		}
	}}
	if unreserved {
		configure = append(configure, unreservedPlanning(t, "free"))
	}
	p := newProviderExecutionFixture(t, kind, false, configure...)
	rootID, _ := cloudstate.NewRequestID()
	childID := p.store.record.Request.ID
	attempt := cloudstate.RequestAttempt{Version: 1, RootID: rootID, ID: childID, Number: 1, CreatedAt: p.store.record.Request.CreatedAt}
	p.store.record.Progress, _ = json.Marshal(map[string]any{"version": 1, "attempt_parent": attempt})
	p.store.data = nil
	p.attempt.OperationID = durable.OperationID(childID)
	var err error
	p.call, err = p.prepare(context.Background(), p.attempt)
	if err != nil {
		t.Fatalf("fixture %s free=%t uncached=%t: %v", kind, unreserved, uncached, err)
	}
	p.reservation, err = p.helper.Reserve(context.Background(), p.call)
	if err != nil {
		t.Fatalf("fixture %s free=%t uncached=%t: %v", kind, unreserved, uncached, err)
	}
	completed, err := p.submit(context.Background())
	if err != nil {
		t.Fatalf("fixture %s free=%t uncached=%t: %v", kind, unreserved, uncached, err)
	}
	root := p.store.record
	root.Request.ID = rootID
	repository := &attemptFinalizationStore{recordingCloudRequests: &recordingCloudRequests{record: root}, child: p.store.record, attempt: attempt, saved: completed.Saved,
		preparation: cloudstate.RequestPreparation{Version: 1, ConfigDigest: completed.Saved.Plan.ConfigDigest, CheckpointScope: "trusted-scope", PreparedAt: attempt.CreatedAt}}
	store := &replayEffectsStore{}
	finalizer, err := newCloudFinalizer(repository, store, store, store, func() time.Time { return p.now })
	if err != nil {
		t.Fatalf("fixture %s free=%t uncached=%t: %v", kind, unreserved, uncached, err)
	}
	projected, err := finalizer.LoadAttemptResult(context.Background(), root.Request.Scope, rootID)
	if err != nil {
		t.Fatalf("project %s child result: %v", kind, err)
	}
	response := projected.Response
	f := &attemptFinalizationFixture{finalizer: finalizer, repository: repository, store: store, provider: p}
	f.generate = llm.GenerateResponseV1{APIVersion: llm.APIVersion, OperationKey: response.OperationKey, OperationID: response.OperationID, Status: response.Status,
		Output: response.Output, Checkpoint: llm.CheckpointMetadata{Handle: "checkpoint-1", Kind: "generation"}, Cache: llm.CacheDispositionV1{Disposition: "miss_populated"},
		Route: &response.Route, Usage: &response.Usage, Cost: publicationCost(response.Cost)}
	f.compact = llm.CompactResponseV1{APIVersion: llm.CompactAPIVersion, OperationKey: response.OperationKey, OperationID: response.OperationID,
		Checkpoint: llm.CheckpointMetadata{Handle: "checkpoint-1", Kind: "compaction", Parent: &p.compact.Parent}, Cache: f.generate.Cache, Usage: &response.Usage, Cost: f.generate.Cost}
	key := cache.ResponseKey{ScopeID: "trusted-scope", Operation: cache.OperationKind(kind), Route: completed.Saved.Plan.Route.CacheIdentity}
	lease := cache.FillLease{Key: key, OperationID: state.OperationID(rootID), Attempt: string(childID), AcquiredAt: attempt.CreatedAt, ExpiresAt: attempt.CreatedAt.Add(cache.MaxFillLease)}
	entry := &cache.ResponseEntry{ID: "entry", Key: key, OriginOperationID: state.OperationID(rootID), OriginCheckpointID: "consumer-checkpoint", CompletedAt: completed.Saved.Execution.CompletedAt}
	if kind == "generate" {
		entry.Response, _ = json.Marshal(f.generate)
	} else {
		entry.Response, _ = json.Marshal(f.compact)
	}
	effects := &ProviderFinalizationEffects{AttemptID: childID, Lease: lease, Entry: entry, Unreserved: unreserved,
		Completion: cache.FillCompletion{Outcome: cache.FillPublished, EntryID: entry.ID, CompletedAt: entry.CompletedAt}}
	if !unreserved {
		effects.Budget = *completed.Saved.Execution.Settlement
	}
	if uncached {
		effects.Uncached, effects.Lease, effects.Entry = true, cache.FillLease{}, nil
		effects.Completion.Outcome, effects.Completion.EntryID = cache.FillNotCacheable, ""
		f.generate.Cache.Disposition, f.compact.Cache.Disposition = "disabled", "disabled"
	}
	f.effects.Provider = effects
	return f
}
func (f *attemptFinalizationFixture) save() error {
	if f.repository.record.Request.Kind == "generate" {
		return f.finalizer.SaveGenerate(context.Background(), f.provider.gen, "trusted-scope", "consumer-checkpoint", f.generate, f.effects)
	}
	return f.finalizer.SaveCompact(context.Background(), f.provider.cloudAdmissionFixture.compact, "trusted-scope", "consumer-checkpoint", f.compact, f.effects)
}
func (f *attemptFinalizationFixture) replay() error {
	r := f.repository.record
	_, found, err := f.finalizer.replay(context.Background(), r.Request.Scope, r, r.Request.Kind, f.generate.OperationKey, 0, nil)
	if err == nil && !found {
		return cloudstate.ErrFinalizationHandoffMissing
	}
	return err
}
func TestCloudAttemptFinalizationKeepsPaidIdentityAcrossReplay(t *testing.T) {
	for _, kind := range []string{"generate", "compact"} {
		for _, unreserved := range []bool{false, true} {
			for _, uncached := range []bool{false, true} {
				f := newAttemptFinalizationFixture(t, kind, unreserved, uncached)
				f.repository.saveHandoffErr = contracts.ErrOutcomeUnknown
				if err := f.save(); err == nil {
					t.Fatal("lost handoff acknowledgement hidden")
				} else if f.repository.handoff == nil {
					t.Fatalf("lost acknowledgement did not save handoff for %s: %v", kind, err)
				}
				if len(f.store.events) != 0 {
					t.Fatal("effects before confirmed handoff")
				}
				f.repository.saveHandoffErr = nil
				for _, fault := range []string{"publish", "budget", "fill", ""} {
					if (uncached && fault != "budget" && fault != "") || (unreserved && fault == "budget") {
						continue
					}
					f.store.fail = fault
					var err error
					f.finalizer, err = newCloudFinalizer(f.repository, f.store, f.store, f.store, func() time.Time { return f.provider.now.Add(365 * 24 * time.Hour) })
					if err != nil {
						t.Fatal(err)
					}
					err = f.replay()
					if (err != nil) != (fault != "") {
						t.Fatal("replay", kind, unreserved, uncached, fault, err)
					}
				}
				for _, settlement := range f.store.settlements {
					if settlement.OperationID != durable.OperationID(f.repository.attempt.ID) || !equalFinalizationValue(settlement, f.effects.Provider.Budget) {
						t.Fatal("settlement changed paid identity")
					}
				}
				if unreserved && len(f.store.settlements) != 0 {
					t.Fatal("unreserved work settled budget")
				}
				for _, event := range f.store.events {
					if uncached && event != "budget" {
						t.Fatal("uncached work touched cache")
					}
				}
				if f.provider.submits.Load() != 1 {
					t.Fatal("finalization redispatched")
				}
			}
		}
	}
}
func TestCloudAttemptFinalizationRejectsMismatchedProof(t *testing.T) {
	for _, kind := range []string{"generate", "compact"} {
		for _, fault := range []string{"active", "parent", "manifest", "config", "scope", "unsettled", "result_identity", "provider_key", "budget", "fill", "cost", "usage", "completion", "unreserved", "uncached"} {
			t.Run(kind+"/"+fault, func(t *testing.T) {
				f := newAttemptFinalizationFixture(t, kind, false, false)
				switch fault {
				case "active":
					f.repository.attempt.ID, _ = cloudstate.NewRequestID()
				case "parent":
					f.repository.attempt.RootID, _ = cloudstate.NewRequestID()
				case "manifest":
					f.repository.child.Request.Manifest = json.RawMessage(`{"operation_key":"another"}`)
				case "config":
					f.repository.preparation.ConfigDigest[0]++
				case "scope":
					f.repository.preparation.CheckpointScope = "other-scope"
				case "unsettled":
					f.repository.saved.Execution.Settled = false
				case "result_identity":
					f.repository.saved.Execution.Response.OperationID = string(f.repository.record.Request.ID)
				case "provider_key":
					f.repository.saved.Execution.Response.OperationKey = f.generate.OperationKey
				case "budget":
					f.effects.Provider.Budget.GenerationID = "other-generation"
				case "fill":
					f.effects.Provider.Lease.Attempt = string(f.repository.saved.Plan.Route.GenerationID)
				case "cost":
					f.repository.saved.Execution.Response.Cost.CatalogVersion = "another-version"
				case "usage":
					f.repository.saved.Execution.Response.Usage.InputTokens++
				case "completion":
					f.effects.Provider.Completion.CompletedAt = f.effects.Provider.Completion.CompletedAt.Add(time.Second)
				case "unreserved":
					f.effects.Provider.Unreserved = true
					f.effects.Provider.Budget = durable.ReconcileRequest{}
				case "uncached":
					f.effects.Provider.Uncached = true
				}
				if err := f.save(); err == nil {
					t.Fatal("mismatched proof accepted")
				}
				if f.repository.handoff != nil || len(f.store.events) != 0 {
					t.Fatal("invalid proof produced effects")
				}
			})
		}
	}
}
func TestCloudAttemptResultProjectionDoesNotChangePaidRecord(t *testing.T) {
	f := newAttemptFinalizationFixture(t, "generate", false, false)
	scope, id := f.repository.record.Request.Scope, f.repository.record.Request.ID
	before, _ := json.Marshal(f.repository.saved)
	got, err := f.finalizer.LoadAttemptResult(context.Background(), scope, id)
	if err != nil {
		t.Fatal(err)
	}
	if got.Response.OperationID != string(id) || got.Response.OperationKey != f.provider.gen.OperationKey || got.Saved.Execution.Response.OperationID != string(got.Attempt.ID) {
		t.Fatal("incorrect identity projection")
	}
	got.Response.Cost.CatalogVersion = "caller-change"
	got.Response.Usage.InputTokens++
	after, _ := json.Marshal(f.repository.saved)
	if string(before) != string(after) {
		t.Fatal("projection mutated the paid record")
	}
	scope.Project = "other"
	if _, err := f.finalizer.LoadAttemptResult(context.Background(), scope, id); err == nil {
		t.Fatal("cross-project read succeeded")
	}
}

func TestCloudAttemptFinalizationRevalidatesSavedProofBeforeEffects(t *testing.T) {
	for _, kind := range []string{"generate", "compact"} {
		t.Run(kind, func(t *testing.T) {
			f := newAttemptFinalizationFixture(t, kind, false, false)
			if err := f.save(); err != nil {
				t.Fatal(err)
			}
			f.repository.saved.Execution.Settled = false
			if err := f.replay(); err == nil {
				t.Fatal("replay accepted an unsettled child")
			}
			if len(f.store.events) != 0 {
				t.Fatal("invalid saved proof produced effects")
			}
		})
	}
}

func TestCloudAttemptFinalizationIncompleteResponseNeverPublishes(t *testing.T) {
	f := newAttemptFinalizationFixture(t, "generate", false, false)
	f.repository.saved.Execution.Response.Status = llm.ResponseStatusLength
	f.generate.Status = llm.ResponseStatusLength
	f.effects.Provider.Entry.Response, _ = json.Marshal(f.generate)
	if err := f.save(); err == nil {
		t.Fatal("incomplete result accepted as cache success")
	}
	if f.repository.handoff != nil || len(f.store.events) != 0 {
		t.Fatal("incomplete result published")
	}
	f.effects.Provider.Entry = nil
	f.effects.Provider.Completion.Outcome = cache.FillNotCacheable
	f.effects.Provider.Completion.EntryID = ""
	f.generate.Cache.Disposition = "miss_not_populated"
	if err := f.save(); err != nil {
		t.Fatal(err)
	}
	if err := f.replay(); err != nil {
		t.Fatal(err)
	}
	for _, event := range f.store.events {
		if event == "publish" {
			t.Fatal("incomplete result published")
		}
	}
	if len(f.store.settlements) != 1 {
		t.Fatal("incomplete result did not settle its paid budget")
	}
}
