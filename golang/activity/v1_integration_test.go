package activity

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
)

type materializingRuntimeProbe struct {
	events       *[]string
	generateSeen *state.MaterializedState
	compactSeen  *state.MaterializedState
}

func (probe *materializingRuntimeProbe) GenerateV1(_ context.Context, _ llm.GenerateRequestV1) (llm.GenerateResponseV1, error) {
	*probe.events = append(*probe.events, "generate:normal")
	return validMaterializedGenerateResponse("root"), nil
}

func (probe *materializingRuntimeProbe) CompactV1(_ context.Context, _ llm.CompactRequestV1) (llm.CompactResponseV1, error) {
	*probe.events = append(*probe.events, "compact:normal")
	return validMaterializedCompactResponse("compact"), nil
}

func (probe *materializingRuntimeProbe) QueryV1(context.Context, llm.QueryRequestV1) (llm.QueryResponseV1, error) {
	return llm.QueryResponseV1{}, nil
}

func (probe *materializingRuntimeProbe) GenerateV1Materialized(_ context.Context, request llm.GenerateRequestV1, materialized state.MaterializedState) (llm.GenerateResponseV1, error) {
	*probe.events = append(*probe.events, "generate:"+string(materialized.Handle))
	probe.generateSeen = &materialized
	return validMaterializedGenerateResponse(request.OperationKey), nil
}

func (probe *materializingRuntimeProbe) CompactV1Materialized(_ context.Context, request llm.CompactRequestV1, materialized state.MaterializedState) (llm.CompactResponseV1, error) {
	*probe.events = append(*probe.events, "compact:"+string(materialized.Handle))
	probe.compactSeen = &materialized
	return validMaterializedCompactResponse(request.OperationKey), nil
}

type materializingMaterializerProbe struct {
	events    *[]string
	result    state.MaterializedState
	err       error
	gotScope  string
	gotHandle string
}

type deadlineMaterializerProbe struct{}

func (*deadlineMaterializerProbe) MaterializeHandle(ctx context.Context, _ string, _ string, _ state.MaterializeLimits) (state.MaterializedState, error) {
	<-ctx.Done()
	return state.MaterializedState{}, fmt.Errorf("load checkpoint: %w", ctx.Err())
}

type canceledV1RuntimeProbe struct {
	entered chan struct{}
}

func (probe *canceledV1RuntimeProbe) GenerateV1(ctx context.Context, _ llm.GenerateRequestV1) (llm.GenerateResponseV1, error) {
	close(probe.entered)
	<-ctx.Done()
	return llm.GenerateResponseV1{}, provider.NewError(provider.CodeProviderUnavailable, provider.PhaseDispatch, provider.DispatchNotDispatched, provider.RetrySameOperation, "provider canceled")
}

func (*canceledV1RuntimeProbe) CompactV1(context.Context, llm.CompactRequestV1) (llm.CompactResponseV1, error) {
	return llm.CompactResponseV1{}, nil
}

func (*canceledV1RuntimeProbe) QueryV1(context.Context, llm.QueryRequestV1) (llm.QueryResponseV1, error) {
	return llm.QueryResponseV1{}, nil
}

func (probe *materializingMaterializerProbe) MaterializeHandle(_ context.Context, scopeID, handle string, _ state.MaterializeLimits) (state.MaterializedState, error) {
	probe.gotScope = scopeID
	probe.gotHandle = handle
	*probe.events = append(*probe.events, "materialize:"+scopeID+":"+handle)
	if probe.err != nil {
		return state.MaterializedState{}, probe.err
	}
	return probe.result, nil
}

func validMaterializedState(handle state.Handle) state.MaterializedState {
	return state.MaterializedState{
		Handle:   handle,
		Tenant:   "scope-id",
		Depth:    2,
		Items:    []llm.Item{llm.HumanText("replayed")},
		Settings: state.RootModelState("logical-model"),
		Lineage:  []state.Handle{"root", handle},
	}
}

// childGenerateV1Request is validGenerateV1Request continuing from parent.
func childGenerateV1Request(parent llm.CheckpointHandle) llm.GenerateRequestV1 {
	request := validGenerateV1Request()
	request.Parent = &parent
	return request
}

// fixedScopeID resolves every request context to the "scope-id" scope.
func fixedScopeID(llm.RequestContext) (string, error) { return "scope-id", nil }

// materializingV1Fixture is a MaterializingV1Runtime scoped by fixedScopeID
// whose runtime and materializer probes share one event log.
type materializingV1Fixture struct {
	events       []string
	runtime      *materializingRuntimeProbe
	materializer *materializingMaterializerProbe
	wrapped      *MaterializingV1Runtime
}

// newMaterializingV1Fixture materializes validMaterializedState("parent").
func newMaterializingV1Fixture() *materializingV1Fixture {
	fixture := &materializingV1Fixture{events: []string{}}
	fixture.runtime = &materializingRuntimeProbe{events: &fixture.events}
	fixture.materializer = &materializingMaterializerProbe{events: &fixture.events, result: validMaterializedState("parent")}
	fixture.wrapped = &MaterializingV1Runtime{Runtime: fixture.runtime, Materializer: fixture.materializer, Scope: fixedScopeID}
	return fixture
}

// assertEvents fails unless got equals want, naming what the events describe.
func assertEvents(t *testing.T, what string, got []string, want ...string) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s = %v, want %v", what, got, want)
	}
}

func validMaterializedGenerateResponse(operationKey string) llm.GenerateResponseV1 {
	return completedGenerateResponseV1(operationKey, "operation-generate", llm.CheckpointMetadata{Handle: "checkpoint-generate", Kind: "generation", Depth: 2}, "disabled")
}

func validMaterializedCompactResponse(operationKey string) llm.CompactResponseV1 {
	parent := llm.CheckpointHandle("parent")
	return compactResponseV1(operationKey, "operation-compact", llm.CheckpointMetadata{Handle: "checkpoint-compact", Parent: &parent, Kind: "compaction", Depth: 3}, "disabled")
}

func TestMaterializingV1RuntimeMaterializesBeforeGenerateDispatch(t *testing.T) {
	fixture := newMaterializingV1Fixture()
	fixture.materializer.result.Tenant = "tenant/project"
	fixture.wrapped.Scope = func(requestContext llm.RequestContext) (string, error) {
		return requestContext.Tenant + "/" + requestContext.Project, nil
	}
	request := childGenerateV1Request("parent")
	response, err := (&Activities{V1Runtime: fixture.wrapped}).GenerateV1(context.Background(), request)
	testassert.NoError(t, err, "GenerateV1 error")
	if response == nil || response.OperationKey != request.OperationKey {
		t.Fatalf("response = %#v", response)
	}
	assertEvents(t, "dispatch order", fixture.events, "materialize:tenant/project:parent", "generate:parent")
	if seen := fixture.runtime.generateSeen; seen == nil || seen.Settings.Model != "logical-model" {
		t.Fatalf("materialized state = %#v, want replayed model", seen)
	}
}

func TestMaterializingV1RuntimeMaterializesBeforeCompactDispatch(t *testing.T) {
	fixture := newMaterializingV1Fixture()
	request := validCompactV1Request()
	request.Parent = "parent"
	response, err := (&Activities{V1Runtime: fixture.wrapped}).CompactV1(context.Background(), request)
	testassert.NoError(t, err, "CompactV1 error")
	if response == nil || response.OperationKey != request.OperationKey {
		t.Fatalf("response = %#v", response)
	}
	assertEvents(t, "dispatch order", fixture.events, "materialize:scope-id:parent", "compact:parent")
	if seen := fixture.runtime.compactSeen; seen == nil || seen.Handle != state.Handle("parent") {
		t.Fatalf("materialized state = %#v, want parent", seen)
	}
}

func TestMaterializingV1RuntimeRootGenerateDoesNotRequireCheckpointMaterializer(t *testing.T) {
	events := []string{}
	runtime := &materializingRuntimeProbe{events: &events}
	wrapped := &MaterializingV1Runtime{Runtime: runtime}
	testassert.NoError(t, testassert.Err((&Activities{V1Runtime: wrapped}).GenerateV1(context.Background(), validGenerateV1Request())), "root GenerateV1 error")
	assertEvents(t, "root dispatch", events, "generate:normal")
}

func TestMaterializingV1RuntimeFailsClosedBeforeDispatchOnMaterializationError(t *testing.T) {
	fixture := newMaterializingV1Fixture()
	fixture.materializer.err = state.ErrNotFound
	request := childGenerateV1Request("missing")
	if _, err := (&Activities{V1Runtime: fixture.wrapped}).GenerateV1(context.Background(), request); err == nil {
		t.Fatal("GenerateV1 unexpectedly dispatched after materialization failure")
	}
	assertEvents(t, "events", fixture.events, "materialize:scope-id:missing")
}

func TestMaterializingV1RuntimeFailsClosedWithoutStateAwareRuntime(t *testing.T) {
	request := childGenerateV1Request("parent")
	base := &v1RuntimeStub{}
	fixture := newMaterializingV1Fixture()
	fixture.wrapped.Runtime = base
	if _, err := fixture.wrapped.GenerateV1(context.Background(), request); err == nil {
		t.Fatal("GenerateV1 unexpectedly accepted a runtime without the state-aware extension")
	}
	if base.generateCalls != 0 {
		t.Fatalf("normal runtime calls = %d, want zero", base.generateCalls)
	}
}

func TestMaterializingV1RuntimeMapsScopeResolverFailure(t *testing.T) {
	request := childGenerateV1Request("parent")
	wantErr := errors.New("scope lookup failed")
	wrapped := newMaterializingV1Fixture().wrapped
	wrapped.Scope = func(llm.RequestContext) (string, error) { return "", wantErr }
	if _, err := wrapped.GenerateV1(context.Background(), request); err == nil {
		t.Fatal("GenerateV1 unexpectedly accepted scope resolver failure")
	}
}

func TestMaterializingV1RuntimePreservesWrappedCancellation(t *testing.T) {
	request := childGenerateV1Request("parent")
	fixture := newMaterializingV1Fixture()
	fixture.materializer.err = fmt.Errorf("load checkpoint: %w", context.Canceled)
	testassert.ErrorIs(t, testassert.Err((&Activities{V1Runtime: fixture.wrapped}).GenerateV1(context.Background(), request)), context.Canceled, "GenerateV1")
	assertEvents(t, "events (materialization without runtime dispatch)", fixture.events, "materialize:scope-id:parent")
}

func TestMapMaterializationDeadlineRetainsRetryableStateLoad(t *testing.T) {
	err := mapMaterializationError(fmt.Errorf("read checkpoint: %w", context.DeadlineExceeded))
	assertProviderError(t, err, provider.CodeDeadlineExceeded, provider.PhaseStateLoad, provider.DispatchNotDispatched, provider.RetrySameOperation)
}

// assertProviderError checks err is a *provider.Error with the given code,
// phase, dispatch certainty and retry disposition.
func assertProviderError(t *testing.T, err error, code provider.Code, phase provider.Phase, dispatch provider.DispatchCertainty, retry provider.RetryDisposition) {
	t.Helper()
	var providerErr *provider.Error
	if !errors.As(err, &providerErr) {
		t.Fatalf("error = %T %v, want provider.Error", err, err)
	}
	if providerErr.Code != code || providerErr.Phase != phase || providerErr.Dispatch != dispatch || providerErr.Retry != retry {
		t.Fatalf("provider error = %#v, want %s/%s/%s/%s", providerErr, code, phase, dispatch, retry)
	}
}

func TestMaterializingV1RuntimeDeadlineUsesRetryableTemporalError(t *testing.T) {
	request := childGenerateV1Request("parent")
	wrapper := newMaterializingV1Fixture().wrapped
	wrapper.Materializer = &deadlineMaterializerProbe{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err := (&Activities{V1Runtime: wrapper}).GenerateV1(ctx, request)
	application, details := safeApplicationError(t, err)
	assertApplicationType(t, application, ErrorTypeProviderTransient, false)
	if details.Code != string(provider.CodeDeadlineExceeded) || details.Phase != string(provider.PhaseStateLoad) {
		t.Fatalf("safe details = %#v, want state-load deadline", details)
	}
}

func TestV1RuntimeCancellationTakesPrecedenceOverProviderError(t *testing.T) {
	runtime := &canceledV1RuntimeProbe{entered: make(chan struct{})}
	activities := &Activities{V1Runtime: runtime}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := activities.GenerateV1(ctx, validGenerateV1Request())
		result <- err
	}()
	<-runtime.entered
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("GenerateV1 error = %v, want context.Canceled precedence", err)
	}
}

func TestV1ActivityPayloadDoesNotScaleWithAncestorLineage(t *testing.T) {
	limits := PayloadLimits{MaxInlineBytes: 1 << 20}
	var requestBytes, responseBytes []int
	for _, turns := range []int{1, 100, 10_000} {
		fixture := newMaterializingV1Fixture()
		materializer := fixture.materializer
		materializer.result.Items = make([]llm.Item, turns)
		for index := range materializer.result.Items {
			materializer.result.Items[index] = llm.HumanText(fmt.Sprintf("ancestor-%05d", index))
		}
		request := childGenerateV1Request("parent")
		encodedRequest, err := MarshalGenerateV1(request, limits)
		if err != nil {
			t.Fatalf("turns=%d marshal request: %v", turns, err)
		}
		response, err := (&Activities{V1Runtime: fixture.wrapped, PayloadLimits: limits}).GenerateV1(context.Background(), request)
		if err != nil {
			t.Fatalf("turns=%d GenerateV1: %v", turns, err)
		}
		encodedResponse, err := MarshalGenerateResponseV1(*response, limits)
		if err != nil {
			t.Fatalf("turns=%d marshal response: %v", turns, err)
		}
		if strings.Contains(string(encodedRequest), "ancestor-") || strings.Contains(string(encodedResponse), "ancestor-") {
			t.Fatalf("turns=%d leaked ancestor transcript into Activity payload", turns)
		}
		if got := len(fixture.runtime.generateSeen.Items); got != turns {
			t.Fatalf("turns=%d materialized item count=%d, want %d", turns, got, turns)
		}
		requestBytes = append(requestBytes, len(encodedRequest))
		responseBytes = append(responseBytes, len(encodedResponse))
	}
	if !reflect.DeepEqual(requestBytes, []int{requestBytes[0], requestBytes[0], requestBytes[0]}) {
		t.Fatalf("request payload sizes scaled with ancestor lineage: %v", requestBytes)
	}
	if !reflect.DeepEqual(responseBytes, []int{responseBytes[0], responseBytes[0], responseBytes[0]}) {
		t.Fatalf("response payload sizes scaled with ancestor lineage: %v", responseBytes)
	}
}
