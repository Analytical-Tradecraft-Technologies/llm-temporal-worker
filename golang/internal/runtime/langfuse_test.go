package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/engine"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/observability"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/langfuse"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/cloudstate"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestCloudLangfuseExportsFinalizedRequestOnce(t *testing.T) {
	f := boundedCloud(t, false)
	calls := 0
	var payload string
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		var v any
		json.NewDecoder(r.Body).Decode(&v)
		b, _ := json.Marshal(v)
		payload = string(b)
		w.Write([]byte(`{}`))
	}))
	defer server.Close()
	exporter, err := langfuse.NewClient(server.URL, "public", "secret", server.Client(), 0)
	if err != nil {
		t.Fatal(err)
	}
	f.cap.Langfuse = exporter
	f.cap.LangfuseEndpoints = map[string]config.EndpointConfig{"endpoint": {Family: "openai_chat"}}
	f.restart(t)
	result := f.finish(t)
	ref := llm.ExecutionReferenceV1{RequestID: result.RequestID, Context: f.request.Context, Capability: result.Capability}
	if err := f.runtime.ExportLangfuseV1(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
	if err := f.runtime.ExportLangfuseV1(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || !strings.Contains(payload, "answer") || !strings.Contains(payload, "actor") || !strings.Contains(payload, "endpoint_id") {
		t.Fatalf("unexpected export: calls=%d payload=%s", calls, payload)
	}
	if f.submits.Load() != 1 {
		t.Fatal("export dispatched model again")
	}
}

func TestLangfuseContentPreservesToolArgumentsButExcludesOpaqueState(t *testing.T) {
	data := langfuseContent(map[string]any{"continuation": map[string]any{"handle": "secret-handle"}, "input": []any{map[string]any{"kind": "provider_state", "opaque": "private-state"}, map[string]any{"kind": "tool_call", "arguments": map[string]any{"operation_key": "user-key", "authorization": "user-content", "kind": "provider_state"}}}})
	if strings.Contains(string(data), "secret-handle") || strings.Contains(string(data), "private-state") || !strings.Contains(string(data), "user-key") || !strings.Contains(string(data), "user-content") {
		t.Fatalf("incorrect content scrub: %s", data)
	}
}

// Inline media is represented by a digest so captures and the OTLP body never
// carry base64 media, once per attempt (#1536).
func TestLangfuseContentReplacesInlineMedia(t *testing.T) {
	media := []byte(strings.Repeat("inline-media-payload", 64))
	encoded := base64.StdEncoding.EncodeToString(media)
	sum := sha256.Sum256(media)
	digest := "sha256:" + hex.EncodeToString(sum[:])
	data := langfuseContent(map[string]any{"input": []any{
		llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "describe"}, llm.ImagePart{Bytes: media, MediaType: "image/png", Detail: "low"}, llm.DocumentPart{Bytes: media, MediaType: "application/pdf", Title: "report"}}},
		llm.ToolResult{CallID: "call-1", Name: "render", Content: []llm.Part{llm.ImagePart{Bytes: media, MediaType: "image/jpeg"}}},
		map[string]any{"kind": "message", "actor": "human", "content": []any{map[string]any{"kind": "audio", "media_type": "audio/wav", "bytes": encoded}}},
		llm.ToolCall{ID: "call-2", Name: "store", Arguments: json.RawMessage(`{"kind":"image","bytes":"` + encoded + `"}`)},
	}})
	var tree struct {
		Input []struct {
			Content []map[string]any `json:"content"`
		} `json:"input"`
	}
	if err := json.Unmarshal(data, &tree); err != nil || len(tree.Input) != 4 {
		t.Fatalf("unexpected content: %v %s", err, data)
	}
	for _, part := range append(append(tree.Input[0].Content[1:], tree.Input[1].Content...), tree.Input[2].Content...) {
		inline, _ := part["inline"].(map[string]any)
		if _, found := part["bytes"]; found || inline["digest"] != digest || inline["byte_length"] != float64(len(media)) || inline["media_type"] != part["media_type"] {
			t.Fatalf("inline media not replaced: %v", part)
		}
	}
	if !strings.Contains(string(data), `"detail":"low"`) || !strings.Contains(string(data), `"title":"report"`) {
		t.Fatalf("media metadata lost: %s", data)
	}
	// Caller-owned tool arguments are not media parts and stay verbatim.
	if strings.Count(string(data), encoded) != 1 {
		t.Fatalf("base64 media survived outside tool arguments: %s", data)
	}
}

func TestCloudLangfuseExportOmitsInlineMediaBytes(t *testing.T) {
	f := boundedCloud(t, false)
	media := []byte(strings.Repeat("inline-image-bytes", 256))
	f.request.Append = []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "what is this"}, llm.ImagePart{Bytes: media, MediaType: "image/png"}}}}
	var payload string
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		payload = string(b)
		w.Write([]byte(`{}`))
	}))
	defer server.Close()
	f.cap.Langfuse, _ = langfuse.NewClient(server.URL, "public", "secret", server.Client(), 0)
	f.cap.LangfuseEndpoints = map[string]config.EndpointConfig{"endpoint": {Family: "openai_chat"}}
	f.restart(t)
	result := f.finish(t)
	if err := f.runtime.ExportLangfuseV1(context.Background(), llm.ExecutionReferenceV1{RequestID: result.RequestID, Context: f.request.Context, Capability: result.Capability}); err != nil {
		t.Fatal(err)
	}
	// The OTLP body nests content as JSON strings, so check the raw base64 run
	// that survives any level of string escaping.
	encoded := base64.StdEncoding.EncodeToString(media)
	if payload == "" || strings.Contains(payload, encoded[:64]) || !strings.Contains(payload, "what is this") || !strings.Contains(payload, "sha256:") {
		t.Fatalf("inline media reached the OTLP body: %s", payload)
	}
}

type oversizedLangfuseCapture struct{ *cloudstate.Repository }

// Behaves like the 8 MB blob cap for any capture still carrying content.
func (s oversizedLangfuseCapture) SaveLangfuseCapture(ctx context.Context, scope cloudstate.Scope, id cloudstate.RequestID, attempt string, data json.RawMessage) error {
	if !strings.Contains(string(data), `"oversized":true`) {
		return cloudstate.ErrPayloadTooLarge
	}
	return s.Repository.SaveLangfuseCapture(ctx, scope, id, attempt, data)
}

func TestCloudLangfuseOversizedCaptureFailsExportAsOversized(t *testing.T) {
	f := boundedCloud(t, false)
	calls := 0
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; w.Write([]byte(`{}`)) }))
	defer server.Close()
	f.cap.Langfuse, _ = langfuse.NewClient(server.URL, "public", "secret", server.Client(), 0)
	f.cap.LangfuseEndpoints = map[string]config.EndpointConfig{"endpoint": {}}
	f.cap.Requests = oversizedLangfuseCapture{f.repository}
	f.restart(t)
	result := f.finish(t)
	err := f.runtime.ExportLangfuseV1(context.Background(), llm.ExecutionReferenceV1{RequestID: result.RequestID, Context: f.request.Context, Capability: result.Capability})
	if !errors.Is(err, langfuse.ErrOversized) || errors.Is(err, langfuse.ErrExport) {
		t.Fatalf("oversized capture not reported as oversized: %v", err)
	}
	if calls != 0 || f.submits.Load() != 1 {
		t.Fatal("oversized capture changed model execution or sent a trace")
	}
}

func TestCloudLangfuseDefaultOpenRouterExcludesContent(t *testing.T) {
	f := boundedCloud(t, false)
	calls := 0
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; w.Write([]byte(`{}`)) }))
	defer server.Close()
	f.cap.Langfuse, _ = langfuse.NewClient(server.URL, "public", "secret", server.Client(), 0)
	f.cap.LangfuseEndpoints = map[string]config.EndpointConfig{"endpoint": {Family: "openai_chat", Extensions: map[string]map[string]any{"openrouter": {}}}}
	f.restart(t)
	result := f.finish(t)
	if err := f.runtime.ExportLangfuseV1(context.Background(), llm.ExecutionReferenceV1{RequestID: result.RequestID, Context: f.request.Context, Capability: result.Capability}); err != nil {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatal("disabled OpenRouter exported content")
	}
}

func TestCloudLangfuseCacheHitHasTraceWithoutPaidGeneration(t *testing.T) {
	f := boundedCloud(t, false)
	f.request.Cache = &llm.CachePolicyV1{}
	calls := 0
	payloads := []string{}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		var v any
		json.NewDecoder(r.Body).Decode(&v)
		b, _ := json.Marshal(v)
		payloads = append(payloads, string(b))
		w.Write([]byte(`{}`))
	}))
	defer server.Close()
	f.cap.Langfuse, _ = langfuse.NewClient(server.URL, "public", "secret", server.Client(), 0)
	f.cap.LangfuseEndpoints = map[string]config.EndpointConfig{"endpoint": {Family: "openai_chat"}}
	f.restart(t)
	first := f.finish(t)
	f.runtime.ExportLangfuseV1(context.Background(), llm.ExecutionReferenceV1{RequestID: first.RequestID, Context: f.request.Context, Capability: first.Capability})
	f.request.OperationKey = "cache-client"
	second := f.finish(t)
	if second.Generate.Cache.Disposition != "hit" {
		t.Fatal("fixture did not use cache", second.Generate.Cache)
	}
	if err := f.runtime.ExportLangfuseV1(context.Background(), llm.ExecutionReferenceV1{RequestID: second.RequestID, Context: f.request.Context, Capability: second.Capability}); err != nil {
		t.Fatal(err)
	}
	if calls != 2 || !strings.Contains(payloads[1], "hit") || strings.Contains(payloads[1], `"stringValue":"generation"`) {
		t.Fatalf("cache trace fabricated paid generation: %v", payloads)
	}
}

func TestCloudLangfuseForksAndCompactionKeepSessionAndCaller(t *testing.T) {
	f := boundedCloud(t, false)
	ops := []map[string]any{}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var v map[string]any
		json.NewDecoder(r.Body).Decode(&v)
		ops = append(ops, v)
		w.Write([]byte(`{}`))
	}))
	defer server.Close()
	f.cap.Langfuse, _ = langfuse.NewClient(server.URL, "public", "secret", server.Client(), 0)
	f.cap.LangfuseEndpoints = map[string]config.EndpointConfig{"endpoint": {Family: "openai_chat"}}
	f.restart(t)
	policy := json.RawMessage(`{"recent_turns":0}`)
	f.request.SettingsPatch.CompactionPolicy.Set = &policy
	export := func(v llm.ExecutionResultV1) {
		t.Helper()
		if err := f.runtime.ExportLangfuseV1(context.Background(), llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: f.request.Context, Capability: v.Capability}); err != nil {
			t.Fatal(err)
		}
	}
	parent := f.finish(t)
	export(parent)
	original := f.request
	for _, key := range []string{"branch-a", "branch-b"} {
		f.request = original
		f.request.OperationKey = key
		h := parent.Generate.Checkpoint.Handle
		f.request.Parent = &h
		export(f.finish(t))
	}
	compact := llm.CompactRequestV1{OperationKey: "compact", Context: f.request.Context, Parent: parent.Generate.Checkpoint.Handle, Cache: &llm.CachePolicyV1{}}
	ctx := context.Background()
	v, err := f.runtime.PrepareExecutionV1(ctx, llm.PrepareExecutionV1{Compact: &compact})
	boundedState(t, v, err, llm.ExecutionBudgetRequired)
	ref := llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: compact.Context, Capability: v.Capability}
	v, err = f.runtime.AcquireBudgetV1(ctx, ref)
	boundedState(t, v, err, llm.ExecutionAcquired)
	v, err = f.runtime.CompactStepV1(ctx, compact)
	boundedState(t, v, err, llm.ExecutionProviderCompleted)
	v, err = f.runtime.CompleteExecutionV1(ctx, ref)
	boundedState(t, v, err, llm.ExecutionCompleted)
	export(v)
	compact.OperationKey = "cached-compact"
	v, err = f.runtime.PrepareExecutionV1(ctx, llm.PrepareExecutionV1{Compact: &compact})
	boundedState(t, v, err, llm.ExecutionCompleted)
	if v.Compact.Cache.Disposition != "hit" {
		t.Fatal("compaction fixture missed cache")
	}
	export(v)
	cachedPayload, _ := json.Marshal(ops[len(ops)-1])
	if !strings.Contains(string(cachedPayload), "answer") || strings.Contains(string(cachedPayload), `"stringValue":"generation"`) {
		t.Fatalf("cached compaction lost summary or fabricated payment: %s", cachedPayload)
	}
	if len(ops) != 5 {
		t.Fatalf("operations exported=%d", len(ops))
	}
	var session string
	ids := map[string]bool{}
	for i, op := range ops {
		spans := op["resourceSpans"].([]any)[0].(map[string]any)["scopeSpans"].([]any)[0].(map[string]any)["spans"].([]any)
		root := spans[0].(map[string]any)
		id := root["traceId"].(string)
		if ids[id] {
			t.Fatal("fork shares operation trace")
		}
		ids[id] = true
		attrs := map[string]string{}
		for _, a := range root["attributes"].([]any) {
			v := a.(map[string]any)
			attrs[v["key"].(string)] = v["value"].(map[string]any)["stringValue"].(string)
		}
		if i == 0 {
			session = attrs["langfuse.session.id"]
		} else if attrs["langfuse.session.id"] != session {
			t.Fatal("lost session across fork or compaction")
		}
		if attrs["langfuse.user.id"] != f.request.Context.Actor {
			t.Fatal("lost client attribution")
		}
		if i > 0 && attrs["langfuse.observation.metadata.parent_trace_id"] == "" {
			t.Fatal("lost parent edge")
		}
	}
}

func TestCloudLangfuseDisabledFallbackDoesNotExportResponse(t *testing.T) {
	f := boundedCloud(t, false, func(b *budgetPlanningFixture) {
		model := b.source.value.Routes.Models["alias"]
		second := model.Routes[0]
		second.ID, second.EndpointID = "fallback-route", "fallback"
		model.Routes = append(model.Routes, second)
		b.source.value.Routes.Models["alias"] = model
		b.source.value.BudgetPolicies[0].Match.EndpointID = ""
		entry := b.entry
		entry.EndpointID = "fallback"
		b.prices(t, []pricing.Entry{b.entry, entry})
	})
	success := f.adapter.invoke
	f.adapter.invoke = func(ctx context.Context, call provider.Call, observer provider.Observer) (provider.Result, error) {
		if call.EndpointID == "endpoint" {
			return provider.Result{}, provider.NewError(provider.CodeProviderUnavailable, provider.PhaseDispatch, provider.DispatchRejected, provider.RetryNextRoute, "unavailable")
		}
		return success(ctx, call, observer)
	}
	f.cap.Adapters = engine.AdapterMap{"endpoint": f.adapter.executionSyncAdapter, "fallback": f.adapter.executionSyncAdapter}
	payload := ""
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		payload = string(b)
		w.Write([]byte(`{}`))
	}))
	defer server.Close()
	f.cap.Langfuse, _ = langfuse.NewClient(server.URL, "public", "secret", server.Client(), 0)
	disabled := false
	f.cap.LangfuseEndpoints = map[string]config.EndpointConfig{"endpoint": {}, "fallback": {Langfuse: &config.LangfuseEndpointConfig{Enabled: &disabled}}}
	f.restart(t)
	ctx := context.Background()
	v, err := f.runtime.PrepareExecutionV1(ctx, llm.PrepareExecutionV1{Generate: &f.request})
	ref := llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: f.request.Context, Capability: v.Capability}
	for n := 0; n < 10 && err == nil && v.State != llm.ExecutionCompleted; n++ {
		switch v.State {
		case llm.ExecutionBudgetRequired:
			v, err = f.runtime.AcquireBudgetV1(ctx, ref)
		case llm.ExecutionFailed:
			if !v.Retryable {
				t.Fatal("fixture failure not retryable", v)
			}
			f.now = f.now.Add(time.Minute)
			v, err = f.runtime.AcquireBudgetV1(ctx, ref)
		case llm.ExecutionAcquired:
			v, err = f.runtime.GenerateStepV1(ctx, f.request)
		case llm.ExecutionProviderCompleted:
			v, err = f.runtime.CompleteExecutionV1(ctx, ref)
		default:
			t.Fatalf("unexpected state %s", v.State)
		}
	}
	boundedState(t, v, err, llm.ExecutionCompleted)
	paid, last, loadErr := f.repository.LangfusePaidAttempts(ctx, cloudstate.Scope{Tenant: f.request.Context.Tenant, Project: f.request.Context.Project}, cloudstate.RequestID(v.RequestID))
	if loadErr != nil || paid[last].Plan.Route.EndpointID != "fallback" {
		t.Fatal("fixture did not fall back", loadErr)
	}

	if err := f.runtime.ExportLangfuseV1(ctx, ref); err != nil {
		t.Fatal(err)
	}
	if payload == "" || strings.Contains(payload, "answer") || strings.Contains(payload, "actual_cost_usd") {
		t.Fatalf("disabled response exported: %s", payload)
	}
}

// A plan on an enabled endpoint that is renewed before it runs must not make
// the trace eligible when only a Langfuse-disabled endpoint served it (#1358).
func TestCloudLangfuseSupersededPlanDoesNotExportDisabledRequest(t *testing.T) {
	var planning *budgetPlanningFixture
	f := boundedCloud(t, false, func(b *budgetPlanningFixture) {
		planning = b
		model := b.source.value.Routes.Models["alias"]
		second := model.Routes[0]
		second.ID, second.EndpointID = "fallback-route", "fallback"
		model.Routes = append(model.Routes, second)
		b.source.value.Routes.Models["alias"] = model
		b.source.value.BudgetPolicies[0].Match.EndpointID = ""
		entry := b.entry
		entry.EndpointID = "fallback"
		b.prices(t, []pricing.Entry{b.entry, entry})
	})
	f.cap.Adapters = engine.AdapterMap{"endpoint": f.adapter.executionSyncAdapter, "fallback": f.adapter.executionSyncAdapter}
	calls := 0
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; w.Write([]byte(`{}`)) }))
	defer server.Close()
	f.cap.Langfuse, _ = langfuse.NewClient(server.URL, "public", "secret", server.Client(), 0)
	disabled := false
	f.cap.LangfuseEndpoints = map[string]config.EndpointConfig{"endpoint": {}, "fallback": {Langfuse: &config.LangfuseEndpointConfig{Enabled: &disabled}}}
	f.restart(t)
	ctx := context.Background()
	v, err := f.runtime.PrepareExecutionV1(ctx, llm.PrepareExecutionV1{Generate: &f.request})
	boundedState(t, v, err, llm.ExecutionBudgetRequired)
	ref := llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: f.request.Context, Capability: v.Capability}
	scope := cloudstate.Scope{Tenant: f.request.Context.Tenant, Project: f.request.Context.Project}
	captures, err := f.repository.LoadLangfuseCaptures(ctx, scope, cloudstate.RequestID(v.RequestID))
	planned := false
	for key, data := range captures {
		var capture generationCapture
		if key != "operation" && json.Unmarshal(data, &capture) == nil && capture.Plan.Route.EndpointID == "endpoint" {
			planned = true
		}
	}
	if err != nil || !planned {
		t.Fatal("fixture did not plan the enabled endpoint first", err)
	}
	// The enabled endpoint is no longer priced when the expired plan renews.
	entry := planning.entry
	entry.EndpointID = "fallback"
	planning.prices(t, []pricing.Entry{entry})
	f.restart(t)
	f.now = f.now.Add(16 * time.Minute)
	v, err = f.runtime.AcquireBudgetV1(ctx, ref)
	boundedState(t, v, err, llm.ExecutionAcquired)
	v, err = f.runtime.GenerateStepV1(ctx, f.request)
	boundedState(t, v, err, llm.ExecutionProviderCompleted)
	v, err = f.runtime.CompleteExecutionV1(ctx, ref)
	boundedState(t, v, err, llm.ExecutionCompleted)
	paid, last, err := f.repository.LangfusePaidAttempts(ctx, scope, cloudstate.RequestID(v.RequestID))
	if err != nil || len(paid) != 1 || paid[last].Plan.Route.EndpointID != "fallback" {
		t.Fatal("fixture was not served only by the disabled endpoint", err)
	}
	if err := f.runtime.ExportLangfuseV1(ctx, ref); err != nil {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatal("superseded plan exported a request served by a disabled endpoint")
	}
}

type failedLangfuseCapture struct{ *cloudstate.Repository }

func (s failedLangfuseCapture) SaveLangfuseCapture(context.Context, cloudstate.Scope, cloudstate.RequestID, string, json.RawMessage) error {
	return errors.New("capture unavailable")
}
func TestCloudLangfuseCaptureFailurePreservesModelAndFailsExport(t *testing.T) {
	f := boundedCloud(t, false)
	calls := 0
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; w.Write([]byte(`{}`)) }))
	defer server.Close()
	f.cap.Langfuse, _ = langfuse.NewClient(server.URL, "public", "secret", server.Client(), 0)
	f.cap.LangfuseEndpoints = map[string]config.EndpointConfig{"endpoint": {}}
	f.cap.Requests = failedLangfuseCapture{f.repository}
	f.restart(t)
	result := f.finish(t)
	if err := f.runtime.ExportLangfuseV1(context.Background(), llm.ExecutionReferenceV1{RequestID: result.RequestID, Context: f.request.Context, Capability: result.Capability}); !errors.Is(err, langfuse.ErrExport) {
		t.Fatalf("capture loss silently acknowledged: %v", err)
	}
	if calls != 0 || f.submits.Load() != 1 {
		t.Fatal("capture failure changed model execution")
	}
}

// Capture failures go through the configured logger, as structured records
// carrying only allow-listed keys (#1359).
func TestCloudLangfuseCaptureFailureUsesConfiguredLogger(t *testing.T) {
	f := boundedCloud(t, false)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(`{}`)) }))
	defer server.Close()
	f.cap.Langfuse, _ = langfuse.NewClient(server.URL, "public", "secret", server.Client(), 0)
	f.cap.LangfuseEndpoints = map[string]config.EndpointConfig{"endpoint": {}}
	f.cap.Requests = failedLangfuseCapture{f.repository}
	f.restart(t)
	var output strings.Builder
	logger, err := observability.NewLogger(observability.LogOptions{Output: &output, Format: "json"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := observability.WithLogger(context.Background(), logger)
	v, err := f.runtime.PrepareExecutionV1(ctx, llm.PrepareExecutionV1{Generate: &f.request})
	boundedState(t, v, err, llm.ExecutionBudgetRequired)
	v, err = f.runtime.AcquireBudgetV1(ctx, llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: f.request.Context, Capability: v.Capability})
	boundedState(t, v, err, llm.ExecutionAcquired)
	seen := map[string]bool{}
	for _, line := range strings.Split(strings.TrimSpace(output.String()), "\n") {
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("log line is not JSON: %q", line)
		}
		if record["level"] == "WARN" && record["request_id"] != nil {
			seen[record["msg"].(string)] = true
		}
	}
	if !seen["Langfuse operation capture failed"] || !seen["Langfuse generation capture failed"] {
		t.Fatalf("capture failures not logged through the configured logger: %s", output.String())
	}
}

func TestCloudLangfuseNoWorkCompactionExportsOnlyRoot(t *testing.T) {
	f := boundedCloud(t, false)
	serverCalls := 0
	payload := ""
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serverCalls++
		b, _ := io.ReadAll(r.Body)
		payload = string(b)
		w.Write([]byte(`{}`))
	}))
	defer server.Close()
	f.cap.Langfuse, _ = langfuse.NewClient(server.URL, "public", "secret", server.Client(), 0)
	f.cap.LangfuseEndpoints = map[string]config.EndpointConfig{"endpoint": {}}
	f.restart(t)
	parent := f.finish(t)
	compact := llm.CompactRequestV1{OperationKey: "no-work", Context: f.request.Context, Parent: parent.Generate.Checkpoint.Handle}
	v, err := f.runtime.PrepareExecutionV1(context.Background(), llm.PrepareExecutionV1{Compact: &compact})
	boundedState(t, v, err, llm.ExecutionCompleted)
	if err := f.runtime.ExportLangfuseV1(context.Background(), llm.ExecutionReferenceV1{RequestID: v.RequestID, Context: compact.Context, Capability: v.Capability}); err != nil {
		t.Fatal(err)
	}
	if serverCalls != 1 || !strings.Contains(payload, "no_work") || strings.Contains(payload, `\"stringValue\":\"generation\"`) {
		t.Fatalf("no-work trace incorrect: %s", payload)
	}
}
