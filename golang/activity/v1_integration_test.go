package activity

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/state"
	"go.temporal.io/sdk/temporal"
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
		Items:    []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "replayed"}}}},
		Settings: state.RootModelState("logical-model"),
		Lineage:  []state.Handle{"root", handle},
	}
}

func validMaterializedGenerateResponse(operationKey string) llm.GenerateResponseV1 {
	return llm.GenerateResponseV1{
		APIVersion: llm.APIVersion, OperationKey: operationKey, OperationID: "operation-generate",
		Status:     llm.ResponseStatusCompleted,
		Output:     []llm.Item{llm.Message{Actor: llm.ActorModel, Content: []llm.Part{llm.TextPart{Text: "ok"}}}},
		Checkpoint: llm.CheckpointMetadata{Handle: "checkpoint-generate", Kind: "generation", Depth: 2},
		Cache:      llm.CacheDispositionV1{Disposition: "disabled"},
		Cost:       llm.CostV1{Status: "exact", ActualCostUSD: stringPointer("0"), Method: "provider_reported"},
	}
}

func validMaterializedCompactResponse(operationKey string) llm.CompactResponseV1 {
	parent := llm.CheckpointHandle("parent")
	return llm.CompactResponseV1{
		APIVersion: llm.CompactAPIVersion, OperationKey: operationKey, OperationID: "operation-compact",
		Checkpoint: llm.CheckpointMetadata{Handle: "checkpoint-compact", Parent: &parent, Kind: "compaction", Depth: 3},
		Cache:      llm.CacheDispositionV1{Disposition: "disabled"},
		Cost:       llm.CostV1{Status: "exact", ActualCostUSD: stringPointer("0"), Method: "provider_reported"},
	}
}

func TestMaterializingV1RuntimeMaterializesBeforeGenerateDispatch(t *testing.T) {
	events := []string{}
	runtime := &materializingRuntimeProbe{events: &events}
	materialized := validMaterializedState("parent")
	materialized.Tenant = "tenant/project"
	materializer := &materializingMaterializerProbe{events: &events, result: materialized}
	wrapped := &MaterializingV1Runtime{
		Runtime: runtime, Materializer: materializer,
		Scope: func(requestContext llm.RequestContext) (string, error) {
			return requestContext.Tenant + "/" + requestContext.Project, nil
		},
	}
	parent := llm.CheckpointHandle("parent")
	request := validGenerateV1Request()
	request.Parent = &parent
	response, err := (&Activities{V1Runtime: wrapped}).GenerateV1(context.Background(), request)
	if err != nil {
		t.Fatalf("GenerateV1 error = %v", err)
	}
	if response == nil || response.OperationKey != request.OperationKey {
		t.Fatalf("response = %#v", response)
	}
	if got, want := events, []string{"materialize:tenant/project:parent", "generate:parent"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("dispatch order = %v, want %v", got, want)
	}
	if runtime.generateSeen == nil || runtime.generateSeen.Settings.Model != "logical-model" {
		t.Fatalf("materialized state = %#v, want replayed model", runtime.generateSeen)
	}
}

func TestMaterializingV1RuntimeMaterializesBeforeCompactDispatch(t *testing.T) {
	events := []string{}
	runtime := &materializingRuntimeProbe{events: &events}
	materializer := &materializingMaterializerProbe{events: &events, result: validMaterializedState("parent")}
	wrapped := &MaterializingV1Runtime{
		Runtime: runtime, Materializer: materializer,
		Scope: func(llm.RequestContext) (string, error) { return "scope-id", nil },
	}
	request := validCompactV1Request()
	request.Parent = "parent"
	response, err := (&Activities{V1Runtime: wrapped}).CompactV1(context.Background(), request)
	if err != nil {
		t.Fatalf("CompactV1 error = %v", err)
	}
	if response == nil || response.OperationKey != request.OperationKey {
		t.Fatalf("response = %#v", response)
	}
	if got, want := events, []string{"materialize:scope-id:parent", "compact:parent"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("dispatch order = %v, want %v", got, want)
	}
	if runtime.compactSeen == nil || runtime.compactSeen.Handle != state.Handle("parent") {
		t.Fatalf("materialized state = %#v, want parent", runtime.compactSeen)
	}
}

func TestMaterializingV1RuntimeRootGenerateDoesNotRequireCheckpointMaterializer(t *testing.T) {
	events := []string{}
	runtime := &materializingRuntimeProbe{events: &events}
	wrapped := &MaterializingV1Runtime{Runtime: runtime}
	if _, err := (&Activities{V1Runtime: wrapped}).GenerateV1(context.Background(), validGenerateV1Request()); err != nil {
		t.Fatalf("root GenerateV1 error = %v", err)
	}
	if got, want := events, []string{"generate:normal"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("root dispatch = %v, want %v", got, want)
	}
}

func TestMaterializingV1RuntimeFailsClosedBeforeDispatchOnMaterializationError(t *testing.T) {
	events := []string{}
	runtime := &materializingRuntimeProbe{events: &events}
	materializer := &materializingMaterializerProbe{events: &events, err: state.ErrNotFound}
	wrapped := &MaterializingV1Runtime{
		Runtime: runtime, Materializer: materializer,
		Scope: func(llm.RequestContext) (string, error) { return "scope-id", nil },
	}
	parent := llm.CheckpointHandle("missing")
	request := validGenerateV1Request()
	request.Parent = &parent
	if _, err := (&Activities{V1Runtime: wrapped}).GenerateV1(context.Background(), request); err == nil {
		t.Fatal("GenerateV1 unexpectedly dispatched after materialization failure")
	}
	if got, want := events, []string{"materialize:scope-id:missing"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
}

func TestMaterializingV1RuntimeFailsClosedWithoutStateAwareRuntime(t *testing.T) {
	parent := llm.CheckpointHandle("parent")
	request := validGenerateV1Request()
	request.Parent = &parent
	base := &v1RuntimeStub{}
	materializer := &materializingMaterializerProbe{events: new([]string), result: validMaterializedState("parent")}
	wrapped := &MaterializingV1Runtime{
		Runtime: base, Materializer: materializer,
		Scope: func(llm.RequestContext) (string, error) { return "scope-id", nil },
	}
	if _, err := wrapped.GenerateV1(context.Background(), request); err == nil {
		t.Fatal("GenerateV1 unexpectedly accepted a runtime without the state-aware extension")
	}
	if base.generateCalls != 0 {
		t.Fatalf("normal runtime calls = %d, want zero", base.generateCalls)
	}
}

func TestMaterializingV1RuntimeMapsScopeResolverFailure(t *testing.T) {
	parent := llm.CheckpointHandle("parent")
	request := validGenerateV1Request()
	request.Parent = &parent
	wantErr := errors.New("scope lookup failed")
	wrapped := &MaterializingV1Runtime{
		Runtime:      &materializingRuntimeProbe{events: new([]string)},
		Materializer: &materializingMaterializerProbe{events: new([]string), result: validMaterializedState("parent")},
		Scope:        func(llm.RequestContext) (string, error) { return "", wantErr },
	}
	if _, err := wrapped.GenerateV1(context.Background(), request); err == nil {
		t.Fatal("GenerateV1 unexpectedly accepted scope resolver failure")
	}
}

func TestMaterializingV1RuntimePreservesWrappedCancellation(t *testing.T) {
	events := []string{}
	parent := llm.CheckpointHandle("parent")
	request := validGenerateV1Request()
	request.Parent = &parent
	wrapper := &MaterializingV1Runtime{
		Runtime:      &materializingRuntimeProbe{events: &events},
		Materializer: &materializingMaterializerProbe{events: &events, err: fmt.Errorf("load checkpoint: %w", context.Canceled)},
		Scope:        func(llm.RequestContext) (string, error) { return "scope-id", nil },
	}
	if _, err := (&Activities{V1Runtime: wrapper}).GenerateV1(context.Background(), request); !errors.Is(err, context.Canceled) {
		t.Fatalf("GenerateV1 error = %v, want context.Canceled", err)
	}
	if got, want := events, []string{"materialize:scope-id:parent"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("events = %v, want materialization without runtime dispatch", got)
	}
}

func TestMapMaterializationDeadlineRetainsRetryableStateLoad(t *testing.T) {
	err := mapMaterializationError(fmt.Errorf("read checkpoint: %w", context.DeadlineExceeded))
	var providerErr *provider.Error
	if !errors.As(err, &providerErr) {
		t.Fatalf("mapped error = %T %v, want provider.Error", err, err)
	}
	if providerErr.Code != provider.CodeDeadlineExceeded || providerErr.Phase != provider.PhaseStateLoad || providerErr.Dispatch != provider.DispatchNotDispatched || providerErr.Retry != provider.RetrySameOperation {
		t.Fatalf("mapped error = %#v, want retryable state-load deadline", providerErr)
	}
}

func TestMaterializingV1RuntimeDeadlineUsesRetryableTemporalError(t *testing.T) {
	parent := llm.CheckpointHandle("parent")
	request := validGenerateV1Request()
	request.Parent = &parent
	wrapper := &MaterializingV1Runtime{
		Runtime:      &materializingRuntimeProbe{events: new([]string)},
		Materializer: &deadlineMaterializerProbe{},
		Scope:        func(llm.RequestContext) (string, error) { return "scope-id", nil },
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err := (&Activities{V1Runtime: wrapper}).GenerateV1(ctx, request)
	var application *temporal.ApplicationError
	if !errors.As(err, &application) {
		t.Fatalf("GenerateV1 error = %T %v, want Temporal application error", err, err)
	}
	if application.Type() != ErrorTypeProviderTransient || application.NonRetryable() {
		t.Fatalf("application error type=%q non_retryable=%v, want retryable provider transient", application.Type(), application.NonRetryable())
	}
	var details SafeErrorDetails
	if err := application.Details(&details); err != nil {
		t.Fatal(err)
	}
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
		events := []string{}
		runtime := &materializingRuntimeProbe{events: &events}
		materializer := &materializingMaterializerProbe{events: &events, result: validMaterializedState("parent")}
		materializer.result.Items = make([]llm.Item, turns)
		for index := range materializer.result.Items {
			materializer.result.Items[index] = llm.Message{
				Actor:   llm.ActorHuman,
				Content: []llm.Part{llm.TextPart{Text: fmt.Sprintf("ancestor-%05d", index)}},
			}
		}
		wrapped := &MaterializingV1Runtime{
			Runtime: runtime, Materializer: materializer,
			Scope: func(llm.RequestContext) (string, error) { return "scope-id", nil },
		}
		parent := llm.CheckpointHandle("parent")
		request := validGenerateV1Request()
		request.Parent = &parent
		encodedRequest, err := MarshalGenerateV1(request, limits)
		if err != nil {
			t.Fatalf("turns=%d marshal request: %v", turns, err)
		}
		response, err := (&Activities{V1Runtime: wrapped, PayloadLimits: limits}).GenerateV1(context.Background(), request)
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
		if got := len(runtime.generateSeen.Items); got != turns {
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
