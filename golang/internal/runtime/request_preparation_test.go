package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/cache"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/compaction"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/state"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/durable"
)

func preparationPointer[T any](value T) *T { return &value }

func preparationMessage(text string) llm.Item {
	return llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: text}}}
}

func preparationFixture() (llm.GenerateRequestV1, durable.GenerateReplay, llm.CompactRequestV1) {
	parent := llm.CheckpointHandle("cp1.opaque.parent")
	caller := llm.RequestContext{Tenant: "tenant", Project: "project", Actor: "current-actor"}
	settings := state.RootModelState("inherited-model")
	settings.ServiceClass = llm.ServiceClassPriority
	settings.ServiceClassFallbacks = []llm.ServiceClass{llm.ServiceClassStandard}
	settings.Portability = llm.PortabilityBestEffort
	settings.Instructions = []llm.Instruction{{Text: "application instructions"}}
	settings.Tools = []llm.Tool{{Name: "lookup", InputSchema: json.RawMessage(`{"type":"object"}`)}}
	settings.ToolPolicy = llm.ToolPolicy{Mode: llm.ToolChoiceNamed, Name: "lookup"}
	settings.Output = &llm.OutputSpec{MaxTokens: preparationPointer(42), Format: llm.OutputFormat{Kind: llm.OutputKindJSONSchema, Name: "result", Schema: json.RawMessage(`{"type":"object"}`)}}
	settings.Temperature = preparationPointer(0.2)
	settings.TemperatureDecimal = preparationPointer(llm.DecimalV1("0.200000000000000001"))
	settings.ReasoningEffort = llm.ReasoningEffortHigh
	settings.ReasoningSummary = llm.ReasoningSummaryConcise
	settings.Extensions = map[string]json.RawMessage{"example": json.RawMessage(`{"value":1}`)}
	policy := compaction.DefaultPolicy()
	policy.RecentTurns = 1
	settings.CompactionPolicy, _ = json.Marshal(policy)
	replay := durable.GenerateReplay{State: state.MaterializedState{
		Handle: state.Handle(parent), Tenant: caller.Tenant, Project: caller.Project,
		Depth: 2, Items: []llm.Item{preparationMessage("older"), preparationMessage("recent")}, Settings: settings,
	}}
	generate := llm.GenerateRequestV1{OperationKey: "new-operation", Context: caller, Parent: &parent,
		Append: []llm.Item{preparationMessage("delta")}, Cache: &llm.CachePolicyV1{MaxAgeSeconds: 60, Variant: 3}}
	compact := llm.CompactRequestV1{OperationKey: "compact-operation", Context: caller, Parent: parent}
	return generate, replay, compact
}

func assertPreparationError(t *testing.T, err error, code provider.Code) {
	t.Helper()
	var mapped *provider.Error
	if !errors.As(err, &mapped) || mapped.Code != code || mapped.Phase != provider.PhaseStateLoad || mapped.Dispatch != provider.DispatchNotDispatched || mapped.Retry != provider.RetryNever {
		t.Fatalf("error = %#v; want %s before dispatch", err, code)
	}
	if mapped.Cause != nil || len(mapped.SafeDetails) != 0 || strings.Contains(err.Error(), "sensitive") {
		t.Fatal("preparation exposed request or stored content")
	}
}

func TestPrepareGenerateInputInheritsAndAppliesSparseSettings(t *testing.T) {
	request, replay, _ := preparationFixture()
	request.SettingsPatch.Model.Set = preparationPointer("new-model")
	request.SettingsPatch.Output.Clear = true
	request.SettingsPatch.Instructions.Set = preparationPointer([]llm.Instruction{{Text: "replacement"}})
	request.SettingsPatch.Temperature.Set = preparationPointer(llm.DecimalV1("0.100000000000000001"))
	prepared, err := PrepareGenerateInput(context.Background(), request, replay)
	if err != nil {
		t.Fatal(err)
	}
	got := prepared.Request
	if got.APIVersion != llm.APIVersion || got.OperationKey != request.OperationKey || !reflect.DeepEqual(got.Context, request.Context) || got.Model != "new-model" || got.Continuation != nil {
		t.Fatalf("lost request identity or invented provider continuation: %+v", got)
	}
	if got.ServiceClass != llm.ServiceClassPriority || got.Portability != llm.PortabilityBestEffort || !reflect.DeepEqual(got.ServiceClassFallbacks, replay.State.Settings.ServiceClassFallbacks) {
		t.Fatal("lost inherited routing controls")
	}
	if len(got.Instructions) != 1 || got.Instructions[0].Text != "replacement" || got.Output != nil || len(got.Tools) != 1 || got.ToolPolicy.Name != "lookup" {
		t.Fatal("sparse patch did not replace, clear and inherit independently")
	}
	if got.Sampling == nil || *got.Sampling.Temperature != 0.1 || prepared.Settings.TemperatureDecimal.String() != "0.100000000000000001" || got.Reasoning.Effort != llm.ReasoningEffortHigh || got.Reasoning.Summary != llm.ReasoningSummaryConcise {
		t.Fatal("lost exact decimal or provider-facing sampling/reasoning")
	}
	if len(got.Input) != 3 || got.Input[0].(llm.Message).Content[0].(llm.TextPart).Text != "older" || got.Input[2].(llm.Message).Content[0].(llm.TextPart).Text != "delta" || prepared.SampleIndex != 3 {
		t.Fatal("lost history, append ordering or sample index")
	}
	if replay.State.Settings.Model != "inherited-model" || replay.State.Settings.Output == nil || len(replay.State.Items) != 2 {
		t.Fatal("preparation changed the parent checkpoint")
	}
}

func TestPrepareGenerateInputRootDefaultsAndRequiredModel(t *testing.T) {
	request, _, _ := preparationFixture()
	request.Parent, request.Cache = nil, nil
	_, err := PrepareGenerateInput(context.Background(), request, durable.GenerateReplay{})
	assertPreparationError(t, err, provider.CodeInvalidArgument)
	request.SettingsPatch.Model.Set = preparationPointer("root-model")
	prepared, err := PrepareGenerateInput(context.Background(), request, durable.GenerateReplay{})
	if err != nil || prepared.Request.ServiceClass != llm.ServiceClassStandard || prepared.Request.Portability != llm.PortabilityStrict || prepared.SampleIndex != 0 || len(prepared.Request.Input) != 1 || prepared.Request.Sampling != nil || prepared.Request.Reasoning != nil {
		t.Fatalf("root defaults = %+v, %v", prepared, err)
	}
}

func TestPrepareGenerateInputResolvesEntireInheritedToolFrontier(t *testing.T) {
	request, replay, _ := preparationFixture()
	replay.State.Items = []llm.Item{
		llm.ToolCall{ID: "one", Name: "lookup", Arguments: json.RawMessage(`{}`)},
		llm.ToolCall{ID: "two", Name: "lookup", Arguments: json.RawMessage(`{}`)},
	}
	replay.State.PendingToolCalls = []string{"one", "two"}
	request.Append = []llm.Item{llm.ToolResult{CallID: "two", Content: []llm.Part{llm.TextPart{Text: "second result"}}}}
	_, err := PrepareGenerateInput(context.Background(), request, replay)
	assertPreparationError(t, err, provider.CodeInvalidArgument)
	request.Append = append(request.Append, llm.ToolResult{CallID: "one", Content: []llm.Part{llm.TextPart{Text: "first result"}}})
	prepared, err := PrepareGenerateInput(context.Background(), request, replay)
	if err != nil || len(prepared.Request.Input) != 4 {
		t.Fatalf("complete frontier = %+v, %v", prepared, err)
	}
	request.Append = append(request.Append, llm.ToolCall{ID: "one", Name: "lookup", Arguments: json.RawMessage(`{}`)})
	_, err = PrepareGenerateInput(context.Background(), request, replay)
	assertPreparationError(t, err, provider.CodeInvalidArgument)
}

func TestPrepareGenerateInputRejectsCorruptReplayAndInvalidDelta(t *testing.T) {
	for _, name := range []string{"handle", "tenant", "project", "depth", "frontier", "parent transcript", "parent model", "orphan result", "pending call", "conflicting patch", "root state", "completed", "reconciliation", "nil context", "canceled"} {
		t.Run(name, func(t *testing.T) {
			request, replay, _ := preparationFixture()
			ctx := context.Background()
			code := provider.CodeStateCorrupt
			switch name {
			case "handle":
				replay.State.Handle = "cp1.other.parent"
			case "tenant":
				replay.State.Tenant = "sensitive-other-tenant"
			case "project":
				replay.State.Project = "sensitive-other-project"
			case "depth":
				replay.State.Depth = -1
			case "frontier":
				replay.State.PendingToolCalls = []string{"sensitive-missing-call"}
			case "parent transcript":
				replay.State.Items = []llm.Item{llm.ToolResult{CallID: "sensitive-orphan"}}
			case "parent model":
				replay.State.Settings.Model = ""
			case "orphan result":
				request.Append = []llm.Item{llm.ToolResult{CallID: "sensitive-orphan"}}
				code = provider.CodeInvalidArgument
			case "pending call":
				request.Append = []llm.Item{llm.ToolCall{ID: "sensitive-call", Name: "lookup", Arguments: json.RawMessage(`{}`)}}
				code = provider.CodeInvalidArgument
			case "conflicting patch":
				request.SettingsPatch.Model = llm.Patch[string]{Set: preparationPointer("sensitive-model"), Clear: true}
				code = provider.CodeInvalidArgument
			case "root state":
				request.Parent = nil
			case "completed":
				replay.Completed = &llm.GenerateResponseV1{}
				code = provider.CodeConfiguration
			case "reconciliation":
				replay.ReconciliationPending = &durable.GenerateReconciliation{}
				code = provider.CodeConfiguration
			case "nil context":
				ctx = nil
				code = provider.CodeConfiguration
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			_, err := PrepareGenerateInput(ctx, request, replay)
			if name == "canceled" {
				if !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			} else {
				assertPreparationError(t, err, code)
			}
		})
	}
}

func TestPreparedInputCopiesMutableApplicationAndTranscriptValues(t *testing.T) {
	request, replay, _ := preparationFixture()
	request.Append = []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.JSONPart{Value: json.RawMessage(`{"value":1}`)}}}}
	request.SettingsPatch.Tools.Set = preparationPointer([]llm.Tool{{Name: "patched", InputSchema: json.RawMessage(`{"type":"object"}`)}})
	prepared, err := PrepareGenerateInput(context.Background(), request, replay)
	if err != nil {
		t.Fatal(err)
	}
	prepared.Request.Tools[0].InputSchema[0] = '['
	prepared.Request.Extensions["example"][0] = '['
	prepared.Request.Input[2].(llm.Message).Content[0].(llm.JSONPart).Value[0] = '['
	prepared.Settings.Instructions[0].Text = "changed"
	*prepared.Settings.Temperature = 0.8
	prepared.Settings.Tools[0].Name = "changed"
	if (*request.SettingsPatch.Tools.Set)[0].InputSchema[0] != '{' || replay.State.Settings.Extensions["example"][0] != '{' || request.Append[0].(llm.Message).Content[0].(llm.JSONPart).Value[0] != '{' || replay.State.Settings.Instructions[0].Text != "application instructions" || *replay.State.Settings.Temperature != 0.2 || prepared.Request.Tools[0].Name != "patched" {
		t.Fatal("prepared values alias caller data, checkpoint state, or each other")
	}
}

func TestPrepareCompactInputUsesVersionedBoundedSummarizerAndRetainsToolFrontier(t *testing.T) {
	_, generation, request := preparationFixture()
	parent := generation.State
	parent.Items = []llm.Item{preparationMessage("old"),
		llm.ToolCall{ID: "closed", Name: "lookup", Arguments: json.RawMessage(`{}`)},
		llm.ToolResult{CallID: "closed", Content: []llm.Part{llm.TextPart{Text: "tool result"}}},
		llm.ToolCall{ID: "pending", Name: "lookup", Arguments: json.RawMessage(`{}`)}}
	parent.PendingToolCalls = []string{"pending"}
	request.Policy = json.RawMessage(`{"target_tokens":1000,"summary_style":"concise"}`)
	prepared, err := PrepareCompactInput(context.Background(), request, durable.CompactReplay{State: parent})
	if err != nil {
		t.Fatal(err)
	}
	if prepared.Request == nil || len(prepared.Selection.Prefix) != 3 || len(prepared.Selection.Retained) != 1 || prepared.Selection.Retained[0].(llm.ToolCall).ID != "pending" {
		t.Fatalf("unsafe selection: %+v", prepared)
	}
	got := prepared.Request
	if got.OperationKey != request.OperationKey || got.Model != parent.Settings.Model || !reflect.DeepEqual(got.Context, request.Context) || len(got.Tools) != 0 || got.ToolPolicy.Mode != llm.ToolChoiceNone || got.Reasoning != nil || got.Continuation != nil || got.Output.Format.Kind != llm.OutputKindText || *got.Output.MaxTokens != prepared.Policy.OutputReserveTokens {
		t.Fatalf("summarizer retained application controls or lost routing: %+v", got)
	}
	if prepared.Policy.Version != compaction.PolicyVersion || prepared.Policy.PromptVersion != compaction.PromptVersion || prepared.Policy.TargetTokens != 1000 || prepared.Policy.SummaryStyle != compaction.SummaryConcise || !strings.Contains(got.Instructions[1].Text, "concise") || prepared.Settings.Output.Format.Kind != llm.OutputKindJSONSchema || len(prepared.Settings.Tools) != 1 {
		t.Fatal("lost effective policy or application's checkpoint settings")
	}
	// The prefix reaches the summarizer as one quoted human message, never as
	// replayed tool turns.
	if len(got.Input) != 1 || got.Input[0].(llm.Message).Actor != llm.ActorHuman || !strings.Contains(got.Input[0].(llm.Message).Content[0].(llm.TextPart).Text, `model tool call id="closed" name="lookup"`) {
		t.Fatalf("summarizer input = %+v", got.Input)
	}
	got.Instructions[2].Text = "changed"
	prepared.Selection.Retained[0] = preparationMessage("changed")
	if prepared.Selection.Prefix[1].(llm.ToolCall).Arguments[0] != '{' || parent.Items[1].(llm.ToolCall).Arguments[0] != '{' || parent.Items[3].(llm.ToolCall).ID != "pending" || parent.Settings.Instructions[0].Text != "application instructions" {
		t.Fatal("compaction values alias the source checkpoint or each other")
	}
}

func TestPrepareCompactInputReportsNothingToCompactWithoutModelRequest(t *testing.T) {
	for _, empty := range []bool{false, true} {
		_, replay, request := preparationFixture()
		replay.State.Settings.CompactionPolicy = nil // default recent window retains both messages
		if empty {
			replay.State.Items = nil
		}
		prepared, err := PrepareCompactInput(context.Background(), request, durable.CompactReplay{State: replay.State})
		if err != nil || prepared.Request != nil || len(prepared.Selection.Prefix) != 0 || len(prepared.Selection.Retained) != len(replay.State.Items) {
			t.Fatalf("nothing-to-compact = %+v, %v", prepared, err)
		}
	}
}

func TestPrepareCompactInputRejectsInvalidStoredAndOverridePolicies(t *testing.T) {
	for _, raw := range []string{`null`, `[]`, `{"typo":1}`, `{"version":"future"}`, `{"target_tokens":12000}`, `{} {}`} {
		_, replay, request := preparationFixture()
		replay.State.Settings.CompactionPolicy = json.RawMessage(raw)
		_, err := PrepareCompactInput(context.Background(), request, durable.CompactReplay{State: replay.State})
		assertPreparationError(t, err, provider.CodeStateCorrupt)
	}
	_, replay, request := preparationFixture()
	request.Policy = json.RawMessage(`{"target_tokens":12000}`)
	_, err := PrepareCompactInput(context.Background(), request, durable.CompactReplay{State: replay.State})
	assertPreparationError(t, err, provider.CodeInvalidArgument)
	request.Policy = json.RawMessage(`{"summary_style":"sensitive-invalid-style"}`)
	_, err = PrepareCompactInput(context.Background(), request, durable.CompactReplay{State: replay.State})
	assertPreparationError(t, err, provider.CodeInvalidArgument)
}

func TestPrepareCompactInputRejectsMismatchedAndResolvedReplay(t *testing.T) {
	for _, name := range []string{"handle", "tenant", "frontier", "completed", "reconciliation", "nil context", "canceled"} {
		t.Run(name, func(t *testing.T) {
			_, generation, request := preparationFixture()
			replay := durable.CompactReplay{State: generation.State}
			ctx := context.Background()
			code := provider.CodeStateCorrupt
			switch name {
			case "handle":
				replay.State.Handle = "cp1.other.parent"
			case "tenant":
				replay.State.Tenant = "sensitive-other-tenant"
			case "frontier":
				replay.State.PendingToolCalls = []string{"sensitive-missing"}
			case "completed":
				replay.Completed = &llm.CompactResponseV1{}
				code = provider.CodeConfiguration
			case "reconciliation":
				replay.ReconciliationPending = &durable.CompactReconciliation{}
				code = provider.CodeConfiguration
			case "nil context":
				ctx = nil
				code = provider.CodeConfiguration
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			_, err := PrepareCompactInput(ctx, request, replay)
			if name == "canceled" {
				if !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			} else {
				assertPreparationError(t, err, code)
			}
		})
	}
}

func TestResponseCacheLookupPreparationStopsBeforePlannerAndStorage(t *testing.T) {
	for _, name := range []string{"incomplete tool results", "missing root model", "Compact policy", "Compact no prefix"} {
		t.Run(name, func(t *testing.T) {
			f := newCacheLookupFixture(t)
			code := provider.CodeInvalidArgument
			var err error
			switch name {
			case "incomplete tool results":
				f.gen.Append = []llm.Item{llm.ToolCall{ID: "pending", Name: "lookup", Arguments: json.RawMessage(`{}`)}}
			case "missing root model":
				f.gen.SettingsPatch.Model.Set = nil
			case "Compact policy":
				f.compactReplay.State.Settings.CompactionPolicy = json.RawMessage(`{"version":"future"}`)
				code = provider.CodeStateCorrupt
			case "Compact no prefix":
				f.compactReplay.State.Settings.CompactionPolicy = nil
			}
			if strings.HasPrefix(name, "Compact") {
				_, err = f.helper.Compact(context.Background(), f.compact, f.compactReplay)
			} else {
				_, err = f.helper.Generate(context.Background(), f.gen, f.genReplay)
			}
			assertPreparationError(t, err, code)
			if f.plans != 0 || len(f.events) != 0 {
				t.Fatalf("preparation failure reached planner/storage: %d/%v", f.plans, f.events)
			}
		})
	}
}

func TestResponseCachePlannerReceivesEffectiveHistoryAndSettings(t *testing.T) {
	f := newCacheLookupFixture(t)
	request, replay, _ := preparationFixture()
	request.SettingsPatch.Model.Set = preparationPointer("patched-model")
	f.gen, f.genReplay = request, replay
	f.genLease.Key.RequestIndex = 3
	f.helper.generate = func(_ context.Context, _ llm.GenerateRequestV1, prepared PreparedGenerateInput) (cache.FillLease, error) {
		if prepared.Request.Model != "patched-model" || len(prepared.Request.Input) != 3 || prepared.SampleIndex != 3 || prepared.Settings.TemperatureDecimal.String() != "0.200000000000000001" {
			t.Fatal("cache planner received raw delta rather than effective input")
		}
		return f.genLease, nil
	}
	if _, err := f.helper.Generate(context.Background(), f.gen, f.genReplay); err != nil {
		t.Fatal(err)
	}
	_, generation, compact := preparationFixture()
	compact.Cache = &llm.CachePolicyV1{MaxAgeSeconds: 60}
	compact.Policy = json.RawMessage(`{"summary_style":"concise","target_tokens":1000}`)
	f.helper.compact = func(_ context.Context, _ llm.CompactRequestV1, prepared PreparedCompactInput) (cache.FillLease, error) {
		if prepared.Request == nil || len(prepared.Request.Input) != 1 || len(prepared.Selection.Retained) != 1 || prepared.Policy.TargetTokens != 1000 || prepared.Policy.SummaryStyle != compaction.SummaryConcise || len(prepared.Request.Tools) != 0 || len(prepared.Settings.Tools) != 1 {
			t.Fatal("compaction planner lost effective prefix, policy or application settings")
		}
		return f.compLease, nil
	}
	if _, err := f.helper.Compact(context.Background(), compact, durable.CompactReplay{State: generation.State}); err != nil {
		t.Fatal(err)
	}
}

func TestPrepareInputsConcurrentInvocationsDoNotShareMutableState(t *testing.T) {
	request, replay, compact := preparationFixture()
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			generated, err := PrepareGenerateInput(context.Background(), request, replay)
			if err != nil {
				t.Error(err)
				return
			}
			generated.Request.Tools[0].InputSchema[0] = '['
			generated.Settings.Extensions["example"][0] = '['
			compacted, err := PrepareCompactInput(context.Background(), compact, durable.CompactReplay{State: replay.State})
			if err != nil || compacted.Request == nil {
				t.Errorf("compact = %+v, %v", compacted, err)
				return
			}
			compacted.Request.Instructions[2].Text = "changed"
			compacted.Settings.Tools[0].InputSchema[0] = '['
		}()
	}
	wg.Wait()
	if replay.State.Settings.Tools[0].InputSchema[0] != '{' || replay.State.Settings.Extensions["example"][0] != '{' || replay.State.Settings.Instructions[0].Text != "application instructions" {
		t.Fatal("mutated shared checkpoint state")
	}
}

func TestPrepareGenerateInputRejectsUndecodableCompactionPolicy(t *testing.T) {
	for _, test := range []struct {
		policy string
		valid  bool
	}{
		{policy: `{"bogus":1}`},
		{policy: `{"target_tokens":0}`},
		{policy: `{"version":"x"}`},
		{policy: `{"trigger_tokens":50000}`, valid: true},
		{policy: `{}`, valid: true},
	} {
		t.Run(test.policy, func(t *testing.T) {
			request, replay, compact := preparationFixture()
			raw := json.RawMessage(test.policy)
			request.SettingsPatch.CompactionPolicy.Set = &raw
			prepared, err := PrepareGenerateInput(context.Background(), request, replay)
			if !test.valid {
				assertPreparationError(t, err, provider.CodeInvalidArgument)
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			// An accepted policy must remain usable by a descendant's plan.
			child := durable.CompactReplay{State: replay.State}
			child.State.Settings = prepared.Settings
			if _, err := PrepareCompactInput(context.Background(), compact, child); err != nil {
				t.Fatalf("descendant compaction rejected an accepted policy: %v", err)
			}
		})
	}
}

func TestPrepareGenerateInputRejectsBlobReferencedMediaAsUnsupported(t *testing.T) {
	blob := &llm.BlobRef{Digest: strings.Repeat("a", 64), ByteLength: 1024, MediaType: "image/png", Locator: "blobs/tenant/image"}
	for name, part := range map[string]llm.Part{
		"image":    llm.ImagePart{Blob: blob, MediaType: "image/png"},
		"document": llm.DocumentPart{Blob: &llm.BlobRef{Digest: strings.Repeat("b", 64), ByteLength: 2048, MediaType: "application/pdf", Locator: "blobs/tenant/doc"}, MediaType: "application/pdf"},
	} {
		t.Run(name, func(t *testing.T) {
			request, replay, _ := preparationFixture()
			request.Append = []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "describe"}, part}}}
			if _, err := request.MarshalJSON(); err != nil {
				t.Fatalf("blob-referenced %s must still decode as a valid v1 request: %v", name, err)
			}
			_, err := PrepareGenerateInput(context.Background(), request, replay)
			assertPreparationError(t, err, provider.CodeUnsupportedCapability)
		})
	}
	request, replay, _ := preparationFixture()
	request.Append = []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.ImagePart{Bytes: []byte{0x89, 'P', 'N', 'G'}, MediaType: "image/png"}}}}
	if _, err := PrepareGenerateInput(context.Background(), request, replay); err != nil {
		t.Fatalf("inline image rejected: %v", err)
	}
}
