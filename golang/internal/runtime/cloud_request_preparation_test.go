package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/cloudstate"
)

type preparationTestStore struct {
	CloudRequestRepository
	record           cloudstate.Record
	data             []byte
	accesses, saves  int
	lookups, begins  int
	loadErr, saveErr error
	beforeLoad       func()
	beforeBegin      func(cloudstate.Operation)
}

func (s *preparationTestStore) BeginOperation(ctx context.Context, op cloudstate.Operation) (cloudstate.Record, error) {
	s.accesses++
	s.begins++
	if s.beforeBegin != nil {
		hook := s.beforeBegin
		s.beforeBegin = nil
		hook(op)
	}
	if s.record.Request.ID == "" {
		s.record = cloudstate.Record{Status: cloudstate.StatusRunning, Request: cloudstate.CreateRequest{ID: "llmtw_req_00000000-0000-4000-8000-000000000001", Scope: op.Scope, Kind: op.Kind, Manifest: op.Manifest, CreatedAt: op.Now, RequestIndex: op.RequestIndex}}
	} else if s.record.Request.Scope != op.Scope || s.record.Request.Kind != op.Kind || string(s.record.Request.Manifest) != string(op.Manifest) {
		return cloudstate.Record{}, contracts.ErrConflict
	}
	return s.record, nil
}
func (s *preparationTestStore) LookupOperation(ctx context.Context, op cloudstate.Operation) (cloudstate.Record, error) {
	s.accesses++
	s.lookups++
	if s.record.Request.ID == "" || s.record.Request.Scope != op.Scope || s.record.Request.Kind != op.Kind {
		return cloudstate.Record{}, contracts.ErrNotFound
	}
	return s.record, nil
}
func (s *preparationTestStore) Read(ctx context.Context, scope cloudstate.Scope, id cloudstate.RequestID) (cloudstate.Record, error) {
	s.accesses++
	if scope != s.record.Request.Scope || id != s.record.Request.ID {
		return cloudstate.Record{}, contracts.ErrNotFound
	}
	return s.record, nil
}
func (s *preparationTestStore) LoadRequestPreparation(ctx context.Context, scope cloudstate.Scope, id cloudstate.RequestID) (cloudstate.RequestPreparation, error) {
	s.accesses++
	if s.beforeLoad != nil {
		hook := s.beforeLoad
		s.beforeLoad = nil
		hook()
	}
	if s.loadErr != nil {
		return cloudstate.RequestPreparation{}, s.loadErr
	}
	if s.data == nil {
		return cloudstate.RequestPreparation{}, cloudstate.ErrRequestPreparationMissing
	}
	var result cloudstate.RequestPreparation
	err := json.Unmarshal(s.data, &result)
	return result, err
}

func TestCloudRequestPreparationConcurrentCompletion(t *testing.T) {
	for _, kind := range []string{"root", "generate", "compact"} {
		for _, method := range []string{"prepare", "load"} {
			for _, outcome := range []string{"completed", "still-running", "changed-input"} {
				t.Run(kind+"/"+method+"/"+outcome, func(t *testing.T) {
					p, store, materializer, input := cloudPreparationFixture(t, kind)
					first, err := p.Prepare(context.Background(), input)
					if err != nil {
						t.Fatal(err)
					}
					calls := materializer.calls
					materializer.err = state.ErrExpired
					store.loadErr = contracts.ErrUnavailable
					store.beforeLoad = func() {
						if outcome != "still-running" {
							store.record.Status = cloudstate.StatusCompleted
							store.data = nil
						}
						if outcome == "changed-input" {
							store.record.Request.CreatedAt = store.record.Request.CreatedAt.Add(time.Second)
						}
					}
					var result PreparedCloudRequest
					if method == "prepare" {
						result, err = p.Prepare(context.Background(), input)
					} else {
						result, err = p.Load(context.Background(), referenceFor(first))
					}
					if outcome == "completed" {
						if err != nil || result.Record.Status != cloudstate.StatusCompleted {
							t.Fatalf("lost competing completion: %v", err)
						}
					} else {
						code := provider.CodeStateUnavailable
						if outcome == "changed-input" {
							code = provider.CodeStateCorrupt
						}
						assertCheckpointReplayError(t, err, code)
					}
					if store.beforeLoad != nil || materializer.calls != calls || store.saves != 1 {
						t.Fatal("did not exercise the race, or reopened/rewrote the parent")
					}
				})
			}
		}
	}
}
func (s *preparationTestStore) SaveRequestPreparation(ctx context.Context, scope cloudstate.Scope, id cloudstate.RequestID, p cloudstate.RequestPreparation) error {
	s.accesses++
	s.saves++
	if s.data != nil {
		return contracts.ErrConflict
	}
	s.data, _ = json.Marshal(p)
	return s.saveErr
}
func cloudPreparationFixture(t *testing.T, kind string) (*CloudRequestPreparation, *preparationTestStore, *checkpointReplayMaterializer, llm.PrepareExecutionV1) {
	t.Helper()
	_, materializer, g, c := checkpointReplayFixture(t)
	materializer.result.Settings = state.RootModelState("model")
	decimal, _ := llm.NewDecimalV1("0.123456789012345678")
	materializer.result.Settings.TemperatureDecimal = &decimal
	materializer.result.Lineage = []state.Handle{"00000000-0000-4000-8000-000000000010"}
	store := &preparationTestStore{}
	caps := replayCapabilities(materializer)
	caps.Requests, caps.ConfigDigest, caps.Clock = store, [32]byte{1}, func() time.Time { return time.Date(2026, 10, 4, 1, 0, 0, 0, time.UTC) }
	helper, err := caps.NewCloudRequestPreparation(func(context.Context, llm.RequestContext) (string, error) { return "opaque-scope", nil }, state.MaterializeLimits{})
	if err != nil {
		t.Fatal(err)
	}
	var input llm.PrepareExecutionV1
	if kind == "compact" {
		input.Compact = &c
	} else {
		input.Generate = &g
		if kind == "root" {
			g.Parent = nil
			g.SettingsPatch.Model = llm.Patch[string]{Set: preparationPointer("model")}
		}
	}
	return helper, store, materializer, input
}
func referenceFor(prepared PreparedCloudRequest) llm.ExecutionReferenceV1 {
	caller := prepared.Record.Request.Scope
	return llm.ExecutionReferenceV1{RequestID: string(prepared.Record.Request.ID), Context: llm.RequestContext{Tenant: caller.Tenant, Project: caller.Project, Actor: "actor"}}
}

func TestCloudRequestPreparationSurvivesParentExpiryAndProcessRestart(t *testing.T) {
	for _, kind := range []string{"root", "generate", "compact"} {
		t.Run(kind, func(t *testing.T) {
			p, store, m, input := cloudPreparationFixture(t, kind)
			first, err := p.Prepare(context.Background(), input)
			if err != nil {
				t.Fatal(err)
			}
			wantCalls := 1
			if kind == "root" {
				wantCalls = 0
			}
			if m.calls != wantCalls || store.saves != 1 {
				t.Fatal("incorrect initial materialization")
			}
			originalData := string(store.data)
			m.err = state.ErrExpired
			restarted := *p
			restarted.clock = func() time.Time { return p.clock().Add(365 * 24 * time.Hour) }
			replay, err := restarted.Load(context.Background(), referenceFor(first))
			if err != nil || !reflect.DeepEqual(first.Preparation, replay.Preparation) {
				t.Fatalf("restart: %v", err)
			}
			if kind != "root" && replay.GenerateReplay.State.Settings.TemperatureDecimal.String() != "0.123456789012345678" {
				t.Fatal("lost exact inherited temperature")
			}
			again, err := restarted.Prepare(context.Background(), input)
			if err != nil || !reflect.DeepEqual(again.GenerateReplay, first.GenerateReplay) || !reflect.DeepEqual(again.CompactReplay, first.CompactReplay) {
				t.Fatalf("retry changed input: %v", err)
			}
			if m.calls != wantCalls || store.saves != 1 || string(store.data) != originalData {
				t.Fatal("reopened parent or rewrote preparation")
			}
		})
	}
}

func TestCloudRequestPreparationAuthorizesBeforeAnyStorageAccess(t *testing.T) {
	for _, completed := range []bool{false, true} {
		t.Run(map[bool]string{false: "running", true: "completed"}[completed], func(t *testing.T) {
			p, store, _, input := cloudPreparationFixture(t, "generate")
			first, err := p.Prepare(context.Background(), input)
			if err != nil {
				t.Fatal(err)
			}
			if completed {
				store.record.Status = cloudstate.StatusCompleted
			}
			store.accesses = 0
			p.replay.resolve = func(context.Context, llm.RequestContext) (string, error) { return "", errors.New("sensitive denial") }
			if _, err := p.Prepare(context.Background(), input); err == nil {
				t.Fatal("unauthorized prepare")
			} else {
				assertCheckpointReplayError(t, err, provider.CodePermissionDenied)
			}
			if _, err := p.Load(context.Background(), referenceFor(first)); err == nil {
				t.Fatal("unauthorized load")
			}
			if store.accesses != 0 {
				t.Fatal("authorization followed storage read")
			}
		})
	}
}

func TestCloudRequestPreparationLostSaveAcknowledgementNeverReopensParent(t *testing.T) {
	p, store, m, input := cloudPreparationFixture(t, "generate")
	store.saveErr = contracts.ErrOutcomeUnknown
	if _, err := p.Prepare(context.Background(), input); err == nil {
		t.Fatal("lost acknowledgement hidden")
	}
	original := string(store.data)
	m.err = state.ErrExpired
	store.saveErr = nil
	result, err := p.Prepare(context.Background(), input)
	if err != nil || result.Generate == nil || store.saves != 1 || m.calls != 1 || string(store.data) != original {
		t.Fatalf("lost acknowledgement recovery: %v", err)
	}
}

func TestCloudRequestPreparationFailsClosedOnStorageAndBindingErrors(t *testing.T) {
	for _, fault := range []error{contracts.ErrNotFound, cloudstate.ErrCorrupt, contracts.ErrUnavailable} {
		p, store, m, input := cloudPreparationFixture(t, "generate")
		// An already-running operation without preparation: a storage fault
		// must not be treated as a miss that rematerializes the parent.
		if _, err := p.Prepare(context.Background(), input); err != nil {
			t.Fatal(err)
		}
		store.data, store.saves, m.calls = nil, 0, 0
		store.loadErr = fault
		if _, err := p.Prepare(context.Background(), input); err == nil || m.calls != 0 || store.saves != 0 {
			t.Fatalf("storage error treated as preparation miss: %v", err)
		}
	}
	for _, mutation := range []func(*cloudstate.RequestPreparation){func(p *cloudstate.RequestPreparation) { p.CheckpointScope = "other" }, func(p *cloudstate.RequestPreparation) { p.ParentSnapshot = nil }, func(p *cloudstate.RequestPreparation) { p.PreparedAt = p.PreparedAt.Add(-time.Hour) }} {
		p, store, m, input := cloudPreparationFixture(t, "generate")
		first, err := p.Prepare(context.Background(), input)
		if err != nil {
			t.Fatal(err)
		}
		var changed cloudstate.RequestPreparation
		_ = json.Unmarshal(store.data, &changed)
		mutation(&changed)
		store.data, _ = json.Marshal(changed)
		if _, err := p.Load(context.Background(), referenceFor(first)); err == nil || m.calls != 1 {
			t.Fatalf("bad binding accepted: %v", err)
		}
	}
}

func TestCloudRequestPreparationInvalidSemanticInputIsNotSaved(t *testing.T) {
	p, store, _, input := cloudPreparationFixture(t, "root")
	input.Generate.SettingsPatch.Model = llm.Patch[string]{}
	if _, err := p.Prepare(context.Background(), input); err == nil || store.saves != 0 {
		t.Fatalf("invalid root saved: %v", err)
	}
}

func TestCloudRequestPreparationCompletedReplayNeedsNoParent(t *testing.T) {
	p, store, m, input := cloudPreparationFixture(t, "generate")
	first, err := p.Prepare(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	store.record.Status = cloudstate.StatusCompleted
	store.data = nil
	store.loadErr = errors.New("must not read preparation")
	m.err = state.ErrExpired
	result, err := p.Load(context.Background(), referenceFor(first))
	if err != nil || result.Record.Status != cloudstate.StatusCompleted || m.calls != 1 {
		t.Fatalf("completed replay depended on parent: %v", err)
	}
}

func TestCloudRequestPreparationRejectsWrongSampleButRestoresAcrossConfiguration(t *testing.T) {
	p, store, _, input := cloudPreparationFixture(t, "generate")
	prepared, err := p.Prepare(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	store.record.Request.RequestIndex++
	if _, err := p.Load(context.Background(), referenceFor(prepared)); err == nil {
		t.Fatal("accepted inconsistent sample")
	} else {
		assertCheckpointReplayError(t, err, provider.CodeStateCorrupt)
	}
	store.record.Request.RequestIndex--
	p.digest[0]++
	if _, err := p.Load(context.Background(), referenceFor(prepared)); err != nil {
		t.Fatalf("saved input could not be restored after reload: %v", err)
	}
}

func TestCloudRequestPreparationRecoversInputWhileProviderIsPendingOrUnknown(t *testing.T) {
	for _, status := range []cloudstate.Status{cloudstate.StatusProviderPending, cloudstate.StatusOutcomeUnknown} {
		p, store, m, input := cloudPreparationFixture(t, "generate")
		prepared, err := p.Prepare(context.Background(), input)
		if err != nil {
			t.Fatal(err)
		}
		store.record.Status = status
		m.err = state.ErrExpired
		if _, err := p.Load(context.Background(), referenceFor(prepared)); err != nil || m.calls != 1 {
			t.Fatalf("%s recovery: %v", status, err)
		}
	}
}

// A rejected Prepare must not create a running record: nothing could ever
// terminalize it, so it would stay in the pending index forever (#1007).
func TestCloudRequestPreparationRejectedInputIsNeverBegun(t *testing.T) {
	cases := map[string]func(*checkpointReplayMaterializer, *llm.PrepareExecutionV1){
		"invalid-root": func(_ *checkpointReplayMaterializer, input *llm.PrepareExecutionV1) {
			input.Generate.SettingsPatch.Model = llm.Patch[string]{}
		},
		"expired-parent": func(m *checkpointReplayMaterializer, _ *llm.PrepareExecutionV1) { m.err = state.ErrExpired },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			kind := "generate"
			if name == "invalid-root" {
				kind = "root"
			}
			p, store, m, input := cloudPreparationFixture(t, kind)
			mutate(m, &input)
			if _, err := p.Prepare(context.Background(), input); err == nil {
				t.Fatal("accepted rejected input")
			}
			if store.begins != 0 || store.saves != 0 || store.record.Request.ID != "" {
				t.Fatalf("rejected input began an operation: begins=%d saves=%d", store.begins, store.saves)
			}
		})
	}
}

// Only new operations are validated before BeginOperation. An existing one
// replays without reopening its parent, so expiry cannot break the replay.
func TestCloudRequestPreparationExistingOperationSkipsPrevalidation(t *testing.T) {
	for _, completed := range []bool{false, true} {
		p, store, m, input := cloudPreparationFixture(t, "generate")
		if _, err := p.Prepare(context.Background(), input); err != nil || m.calls != 1 || store.lookups != 1 || store.begins != 1 {
			t.Fatalf("first prepare: calls=%d lookups=%d begins=%d err=%v", m.calls, store.lookups, store.begins, err)
		}
		if completed {
			store.record.Status = cloudstate.StatusCompleted
		}
		m.err = state.ErrExpired
		if _, err := p.Prepare(context.Background(), input); err != nil || m.calls != 1 || store.saves != 1 {
			t.Fatalf("completed=%v replay reopened parent: %v", completed, err)
		}
	}
}

// Two workers miss the lookup; the faster one creates the record (with a later
// CreatedAt) and crashes before saving its preparation, then the parent expires.
// The slower worker must keep its validated snapshot rather than reopen the
// parent, or the competitor's running record would stay pending forever.
func TestCloudRequestPreparationConcurrentBeginReusesValidatedSnapshot(t *testing.T) {
	for _, kind := range []string{"generate", "compact"} {
		t.Run(kind, func(t *testing.T) {
			p, store, m, input := cloudPreparationFixture(t, kind)
			competitorAt := p.clock().Add(time.Second)
			store.beforeBegin = func(op cloudstate.Operation) {
				store.record = cloudstate.Record{Status: cloudstate.StatusRunning, Request: cloudstate.CreateRequest{ID: "llmtw_req_00000000-0000-4000-8000-000000000002", Scope: op.Scope, Kind: op.Kind, Manifest: op.Manifest, CreatedAt: competitorAt, RequestIndex: op.RequestIndex}}
				m.err = state.ErrExpired
			}
			prepared, err := p.Prepare(context.Background(), input)
			if err != nil {
				t.Fatalf("prepare after concurrent begin: %v", err)
			}
			if m.calls != 1 || store.saves != 1 || !prepared.Preparation.PreparedAt.Equal(competitorAt) || prepared.Record.Request.ID != store.record.Request.ID {
				t.Fatalf("calls=%d saves=%d prepared_at=%v", m.calls, store.saves, prepared.Preparation.PreparedAt)
			}
			if _, err := p.Load(context.Background(), referenceFor(prepared)); err != nil {
				t.Fatalf("saved preparation does not restore: %v", err)
			}
		})
	}
}

func TestCloudExecutionRuntimeRejectedPrepareLeavesNothingPending(t *testing.T) {
	cases := map[string]func(*boundedCloudFixture) llm.PrepareExecutionV1{
		"unknown-parent": func(f *boundedCloudFixture) llm.PrepareExecutionV1 {
			parent := llm.CheckpointHandle("unknown-checkpoint")
			f.request.Parent = &parent
			return llm.PrepareExecutionV1{Generate: &f.request}
		},
		"unknown-compact-parent": func(f *boundedCloudFixture) llm.PrepareExecutionV1 {
			return llm.PrepareExecutionV1{Compact: &llm.CompactRequestV1{OperationKey: f.request.OperationKey, Context: f.request.Context, Parent: "unknown-checkpoint", Cache: &llm.CachePolicyV1{}}}
		},
		"invalid-settings": func(f *boundedCloudFixture) llm.PrepareExecutionV1 {
			f.request.SettingsPatch.Model = llm.Patch[string]{}
			return llm.PrepareExecutionV1{Generate: &f.request}
		},
	}
	for name, build := range cases {
		t.Run(name, func(t *testing.T) {
			f := boundedCloud(t, false)
			valid := f.request
			ctx := context.Background()
			if _, err := f.runtime.PrepareExecutionV1(ctx, build(f)); err == nil {
				t.Fatal("accepted rejected input")
			}
			for shard := range cloudstate.PendingShards {
				page, err := f.repository.ListPending(ctx, shard, 100, "")
				if err != nil || len(page.Requests) != 0 {
					t.Fatalf("rejected prepare left pending records: %+v, err=%v", page.Requests, err)
				}
			}
			// Nothing was bound to the key, so corrected input can still use it.
			v, err := f.runtime.PrepareExecutionV1(ctx, llm.PrepareExecutionV1{Generate: &valid})
			boundedState(t, v, err, llm.ExecutionBudgetRequired)
		})
	}
}
