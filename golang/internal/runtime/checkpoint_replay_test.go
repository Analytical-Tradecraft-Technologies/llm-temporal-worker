package runtime

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	contracts "github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage/providercontracts"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/durable"
)

type checkpointReplayMaterializer struct {
	state.CheckpointHandleMaterializer
	calls         int
	scope, handle string
	limits        state.MaterializeLimits
	result        state.MaterializedState
	err           error
}

func (m *checkpointReplayMaterializer) MaterializeHandle(_ context.Context, scope, handle string, limits state.MaterializeLimits) (state.MaterializedState, error) {
	m.calls++
	m.scope, m.handle, m.limits = scope, handle, limits
	return m.result, m.err
}
func replayCapabilities(m state.CheckpointHandleMaterializer) V1RuntimeCapabilities {
	return V1RuntimeCapabilities{Checkpoints: CheckpointCapabilities{Repository: builderCheckpointRepository{}, Blobs: builderCheckpointBlobReader{}, Materializer: m}}
}
func checkpointReplayFixture(t *testing.T) (*CheckpointReplay, *checkpointReplayMaterializer, llm.GenerateRequestV1, llm.CompactRequestV1) {
	t.Helper()
	parent := llm.CheckpointHandle("cp1.opaque.parent")
	caller := llm.RequestContext{Tenant: "tenant", Project: "project", Actor: "actor"}
	m := &checkpointReplayMaterializer{result: state.MaterializedState{Handle: state.Handle(parent), Tenant: "opaque-scope", Items: []llm.Item{llm.HumanText("parent message")}}}
	r, err := replayCapabilities(m).NewCheckpointReplay(func(_ context.Context, got llm.RequestContext) (string, error) {
		if !reflect.DeepEqual(got, caller) {
			t.Fatal("lost caller scope")
		}
		return "opaque-scope", nil
	}, state.MaterializeLimits{MaxDepth: 8, MaxRows: 9, MaxItems: 20, MaxBytes: 4096})
	testassert.NoError(t, err)
	return r, m, llm.GenerateRequestV1{OperationKey: "generate", Context: caller, Parent: &parent}, llm.CompactRequestV1{OperationKey: "compact", Context: caller, Parent: parent}
}
func assertCheckpointReplayError(t *testing.T, err error, code provider.Code) {
	t.Helper()
	var mapped *provider.Error
	if !errors.As(err, &mapped) || mapped.Code != code || mapped.Phase != provider.PhaseStateLoad || mapped.Dispatch != provider.DispatchNotDispatched {
		t.Fatalf("error = %#v, want %s before dispatch", err, code)
	}
	if strings.Contains(err.Error(), "sensitive") {
		t.Fatal("leaked cause")
	}
}
func TestCheckpointReplayMaterializesBothPhasesAndPreservesDelta(t *testing.T) {
	r, m, g, c := checkpointReplayFixture(t)
	g.Append = []llm.Item{llm.HumanText("new delta")}
	base, err := r.Generate(context.Background(), g)
	if err != nil || !reflect.DeepEqual(base.State, replayCallerState(m.result, g.Context)) || base.Completed != nil || base.ReconciliationPending != nil {
		t.Fatalf("Generate = %#v, %v", base, err)
	}
	compact, err := r.Compact(context.Background(), c)
	if err != nil || !reflect.DeepEqual(compact.State, replayCallerState(m.result, c.Context)) {
		t.Fatalf("Compact = %#v, %v", compact, err)
	}
	if m.calls != 2 || m.scope != "opaque-scope" || m.handle != string(c.Parent) || m.limits != r.limits {
		t.Fatal("lost materialization binding")
	}
	if len(base.State.Items) != 1 || len(g.Append) != 1 {
		t.Fatal("appended delta to stored parent")
	}
}
func TestCheckpointReplayRootAuthorizesWithoutLoadingParent(t *testing.T) {
	r, m, g, _ := checkpointReplayFixture(t)
	g.Parent = nil
	calls := 0
	r.resolve = func(context.Context, llm.RequestContext) (string, error) { calls++; return "scope", nil }
	got, err := r.Generate(context.Background(), g)
	if err != nil || !reflect.DeepEqual(got, durable.GenerateReplay{}) || m.calls != 0 || calls != 1 {
		t.Fatalf("root = %#v, %v, calls=%d/%d", got, err, calls, m.calls)
	}
	r.resolve = func(context.Context, llm.RequestContext) (string, error) { return "", errors.New("sensitive denial") }
	_, err = r.Generate(context.Background(), g)
	assertCheckpointReplayError(t, err, provider.CodePermissionDenied)
}
func TestCheckpointReplayRejectsMissingBindingsAndInvalidLimits(t *testing.T) {
	resolver := func(context.Context, llm.RequestContext) (string, error) { return "opaque-scope", nil }
	var typedNil *checkpointReplayMaterializer
	for _, test := range []struct {
		name    string
		cap     V1RuntimeCapabilities
		resolve CheckpointScopeResolver
		limits  state.MaterializeLimits
	}{
		{"missing capabilities", V1RuntimeCapabilities{}, resolver, state.MaterializeLimits{}},
		{"typed nil", replayCapabilities(typedNil), resolver, state.MaterializeLimits{}},
		{"resolver", replayCapabilities(&checkpointReplayMaterializer{}), nil, state.MaterializeLimits{}},
		{"depth", replayCapabilities(&checkpointReplayMaterializer{}), resolver, state.MaterializeLimits{MaxDepth: -1}},
		{"rows", replayCapabilities(&checkpointReplayMaterializer{}), resolver, state.MaterializeLimits{MaxRows: -1}},
		{"items", replayCapabilities(&checkpointReplayMaterializer{}), resolver, state.MaterializeLimits{MaxItems: -1}},
		{"bytes", replayCapabilities(&checkpointReplayMaterializer{}), resolver, state.MaterializeLimits{MaxBytes: -1}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := test.cap.NewCheckpointReplay(test.resolve, test.limits); err == nil {
				t.Fatal("accepted incomplete binding")
			}
		})
	}
}
func TestCheckpointReplayFailuresStopBeforeRoutingAndBudgets(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*CheckpointReplay, *checkpointReplayMaterializer)
		code   provider.Code
	}{
		{"denied", func(r *CheckpointReplay, _ *checkpointReplayMaterializer) {
			r.resolve = func(context.Context, llm.RequestContext) (string, error) { return "", errors.New("sensitive denial") }
		}, provider.CodePermissionDenied},
		{"empty scope", func(r *CheckpointReplay, _ *checkpointReplayMaterializer) {
			r.resolve = func(context.Context, llm.RequestContext) (string, error) { return "", nil }
		}, provider.CodeInvalidArgument},
		{"scope whitespace", func(r *CheckpointReplay, _ *checkpointReplayMaterializer) {
			r.resolve = func(context.Context, llm.RequestContext) (string, error) { return " scope", nil }
		}, provider.CodeInvalidArgument},
		{"storage", func(_ *CheckpointReplay, m *checkpointReplayMaterializer) { m.err = errors.New("sensitive SDK error") }, provider.CodeStateUnavailable},
		{"forged handle", func(_ *CheckpointReplay, m *checkpointReplayMaterializer) { m.err = state.ErrInvalidHandle }, provider.CodeInvalidArgument},
		{"expired", func(_ *CheckpointReplay, m *checkpointReplayMaterializer) { m.err = state.ErrExpired }, provider.CodeInvalidArgument},
		{"missing", func(_ *CheckpointReplay, m *checkpointReplayMaterializer) { m.err = state.ErrNotFound }, provider.CodeInvalidArgument},
		{"missing cloud row", func(_ *CheckpointReplay, m *checkpointReplayMaterializer) {
			m.err = fmt.Errorf("load checkpoint: %w", contracts.ErrNotFound)
		}, provider.CodeInvalidArgument},
		{"lineage limit", func(_ *CheckpointReplay, m *checkpointReplayMaterializer) {
			m.err = fmt.Errorf("checkpoint materialization exceeds item limit: %w", state.ErrLimitExceeded)
		}, provider.CodeInvalidArgument},
		{"tenant", func(_ *CheckpointReplay, m *checkpointReplayMaterializer) { m.result.Tenant = "other" }, provider.CodeStateCorrupt},
		{"project", func(_ *CheckpointReplay, m *checkpointReplayMaterializer) { m.result.Project = "other" }, provider.CodeStateCorrupt},
		{"handle", func(_ *CheckpointReplay, m *checkpointReplayMaterializer) { m.result.Handle = "other" }, provider.CodeStateCorrupt},
		{"frontier", func(_ *CheckpointReplay, m *checkpointReplayMaterializer) {
			m.result.PendingToolCalls = []string{"missing-tool"}
		}, provider.CodeStateCorrupt},
	} {
		t.Run(test.name, func(t *testing.T) {
			r, m, g, c := checkpointReplayFixture(t)
			test.mutate(r, m)
			var events []string
			ports := validBuilderGeneratePorts(&events)
			ports.Replay = r.Generate
			_, err := durable.GenerateV1(context.Background(), g, ports)
			assertCheckpointReplayError(t, err, test.code)
			if len(events) != 0 {
				t.Fatalf("failure reached later phases: %v", events)
			}
			compact := validCompactPorts()
			compact.Replay = r.Compact
			compact.CacheLookup = func(context.Context, llm.CompactRequestV1, durable.CompactReplay) (durable.CompactCacheDecision, error) {
				t.Fatal("failure reached Compact cache")
				return durable.CompactCacheDecision{}, nil
			}
			_, err = durable.CompactV1(context.Background(), c, compact)
			assertCheckpointReplayError(t, err, test.code)
		})
	}
}
func TestCheckpointReplayCancellationAndMalformedRequestsDoNotRead(t *testing.T) {
	r, m, g, c := checkpointReplayFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	testassert.ErrorIs(t, testassert.Err(r.Generate(ctx, g)), context.Canceled)
	testassert.ErrorIs(t, testassert.Err(r.Compact(ctx, c)), context.Canceled)
	_, err := r.Generate(nil, g)
	assertCheckpointReplayError(t, err, provider.CodeConfiguration)
	var missing *CheckpointReplay
	_, err = missing.Generate(context.Background(), g)
	assertCheckpointReplayError(t, err, provider.CodeConfiguration)
	g.OperationKey = ""
	_, err = r.Generate(context.Background(), g)
	assertCheckpointReplayError(t, err, provider.CodeInvalidArgument)
	c.Parent = ""
	_, err = r.Compact(context.Background(), c)
	assertCheckpointReplayError(t, err, provider.CodeInvalidArgument)
	if m.calls != 0 {
		t.Fatal("read on invalid input")
	}
}
func TestCheckpointReplayRealHandleVerifierRejectsOtherScopeBeforeRead(t *testing.T) {
	keyring := testKeyring(t, state.Key{ID: "key", Secret: bytes.Repeat([]byte{4}, 32)})
	handle, err := keyring.IssueCheckpointHandle("signed-scope", "00000000-0000-4000-8000-000000000001")
	testassert.NoError(t, err)
	store := &checkpointReplayPoisonStore{}
	cap, err := cloudCheckpointCapabilities(&recordingCloudRequests{checkpointStore: store}, keyring, nil)
	testassert.NoError(t, err)
	replay, err := (V1RuntimeCapabilities{Checkpoints: cap}).NewCheckpointReplay(func(context.Context, llm.RequestContext) (string, error) { return "other-scope", nil }, state.MaterializeLimits{})
	testassert.NoError(t, err)
	parent := llm.CheckpointHandle(handle)
	_, err = replay.Generate(context.Background(), llm.GenerateRequestV1{OperationKey: "key", Context: llm.RequestContext{Tenant: "tenant", Project: "project", Actor: "actor"}, Parent: &parent})
	assertCheckpointReplayError(t, err, provider.CodeInvalidArgument)
	// The embedded repository is nil: any row read would panic. The MAC must
	// reject the handle before the cloud repository can be consulted.
}

// Every promoted method panics if handle verification allows a cloud read.
type checkpointReplayPoisonStore struct{ state.CheckpointStore }

func TestCheckpointReplayRetainsOriginalSnapshotBinding(t *testing.T) {
	_, first, g, _ := checkpointReplayFixture(t)
	second := &checkpointReplayMaterializer{err: errors.New("new snapshot should not be consulted")}
	capabilities := replayCapabilities(first)
	resolver := func(context.Context, llm.RequestContext) (string, error) { return "opaque-scope", nil }
	limits := state.MaterializeLimits{MaxRows: 7}
	old, err := capabilities.NewCheckpointReplay(resolver, limits)
	testassert.NoError(t, err)
	capabilities.Checkpoints.Materializer = second
	limits.MaxRows = 99
	_, err = old.Generate(context.Background(), g)
	if err != nil || first.calls != 1 || second.calls != 0 || first.limits.MaxRows != 7 {
		t.Fatalf("mixed snapshot: %v", err)
	}
	next, err := capabilities.NewCheckpointReplay(resolver, limits)
	testassert.NoError(t, err)
	_, err = next.Generate(context.Background(), g)
	assertCheckpointReplayError(t, err, provider.CodeStateUnavailable)
	if second.calls != 1 || second.limits.MaxRows != 99 {
		t.Fatal("new snapshot used stale binding")
	}
}

func TestCheckpointReplayCancellationDuringAuthorizationStopsBeforeRead(t *testing.T) {
	r, m, g, _ := checkpointReplayFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	r.resolve = func(context.Context, llm.RequestContext) (string, error) { cancel(); return "", context.Canceled }
	_, err := r.Generate(ctx, g)
	if !errors.Is(err, context.Canceled) || m.calls != 0 {
		t.Fatalf("canceled authorization: %v", err)
	}
}

func replayCallerState(value state.MaterializedState, caller llm.RequestContext) state.MaterializedState {
	value.Tenant, value.Project = caller.Tenant, caller.Project
	return value
}
