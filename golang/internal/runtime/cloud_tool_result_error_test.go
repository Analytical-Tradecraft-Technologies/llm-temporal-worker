package runtime

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/engine"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/openairesponses"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/routing"
)

// A failed tool reported with is_error=true must continue a checkpoint on an
// OpenAI Responses route: the real adapter compiles it and the request reaches
// the provider carrying the documented error prefix.
func TestCloudExecutionToolResultErrorReachesResponsesProvider(t *testing.T) {
	f := boundedCloud(t, false)
	source := f.cap.Snapshot.(*planningSource)
	model := source.value.Routes.Models["alias"]
	model.Routes[0].Capabilities.Features[routing.FeatureToolCall] = routing.Capability{State: routing.CapabilityNative}
	source.value.Routes.Models["alias"] = model
	f.adapter.invoke = func(ctx context.Context, call provider.Call, o provider.Observer) (provider.Result, error) {
		f.submits.Add(1)
		if err := o.BeforePossibleWrite(ctx); err != nil {
			return provider.Result{}, err
		}
		result := executionResponse(call).Result
		result.Response.Output = []llm.Item{llm.ToolCall{ID: "call-1", Name: "lookup", Arguments: json.RawMessage(`{"q":"x"}`)}}
		return result, nil
	}
	f.restart(t)
	parent := f.finish(t)

	var requests atomic.Int32
	var body atomic.Value
	client, err := openairesponses.NewClient(openairesponses.ClientConfig{BaseURL: "https://api.openai.com/v1/", APIKey: "test-key", HTTPClient: &http.Client{Transport: planningTransportFunc(func(r *http.Request) (*http.Response, error) {
		requests.Add(1)
		data, err := io.ReadAll(r.Body)
		if err != nil {
			return nil, err
		}
		body.Store(string(data))
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Request: r, Body: io.NopCloser(strings.NewReader(
			`{"id":"resp-1","object":"response","created_at":1710000000,"model":"provider-model","status":"completed","service_tier":"default",` +
				`"output":[{"id":"msg-1","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"recovered","annotations":[]}]}],` +
				`"usage":{"input_tokens":10,"input_tokens_details":{"cached_tokens":0},"output_tokens":5,"output_tokens_details":{"reasoning_tokens":0},"total_tokens":15}}`))}, nil
	})}})
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := openairesponses.New(client, "endpoint", "profile/v1")
	if err != nil {
		t.Fatal(err)
	}
	f.cap.Adapters = engine.AdapterMap{"endpoint": adapter}
	handle := parent.Generate.Checkpoint.Handle
	f.request.Parent = &handle
	f.request.OperationKey = "tool-error-turn"
	f.request.SettingsPatch = llm.SettingsPatchV1{}
	f.request.Append = []llm.Item{llm.ToolResult{CallID: "call-1", Name: "lookup", Content: []llm.Part{llm.TextPart{Text: "upstream timed out"}}, IsError: true}}
	f.now = f.now.Add(time.Second)
	f.restart(t)
	f.finish(t)

	sent, _ := body.Load().(string)
	if requests.Load() != 1 || !strings.Contains(sent, `"output":"[is_error=true] The tool call failed; its output follows.\nupstream timed out"`) {
		t.Fatalf("provider requests=%d body=%s", requests.Load(), sent)
	}
}

// Compaction planning must count the prefix an OpenAI-family route adds to a
// failed tool result: a transcript whose serialized size fits the route's
// byte limit but not with the prefix has to be compacted first.
func TestCloudGenerationPlanCountsToolResultErrorPrefix(t *testing.T) {
	f := boundedCloud(t, false)
	source := f.cap.Snapshot.(*planningSource)
	model := source.value.Routes.Models["alias"]
	model.Routes[0].Capabilities.Features[routing.FeatureToolCall] = routing.Capability{State: routing.CapabilityNative}
	source.value.Routes.Models["alias"] = model
	f.adapter.invoke = func(ctx context.Context, call provider.Call, o provider.Observer) (provider.Result, error) {
		f.submits.Add(1)
		if err := o.BeforePossibleWrite(ctx); err != nil {
			return provider.Result{}, err
		}
		result := executionResponse(call).Result
		result.Response.Output = []llm.Item{llm.ToolCall{ID: "call-1", Name: "lookup", Arguments: json.RawMessage(`{"q":"x"}`)}}
		return result, nil
	}
	policy := json.RawMessage(`{"recent_turns":0}`)
	f.request.SettingsPatch.CompactionPolicy.Set = &policy
	f.restart(t)
	parent := f.finish(t)
	handle := parent.Generate.Checkpoint.Handle
	f.request.Parent = &handle
	f.request.OperationKey = "tool-error-plan"
	f.request.SettingsPatch = llm.SettingsPatchV1{}
	f.now = f.now.Add(time.Second)
	f.restart(t)
	planningAttempt := 0
	compacts := func(limit int, isError bool) bool {
		t.Helper()
		providers := f.runtime.execution.admission.planning.providers
		model := providers.catalog.Models["alias"]
		model.Routes[0].ContextBytes = limit
		providers.catalog.Models["alias"] = model
		request := f.request
		planningAttempt++
		request.OperationKey += "-" + strconv.Itoa(planningAttempt)
		request.Append = []llm.Item{llm.ToolResult{CallID: "call-1", Name: "lookup", Content: []llm.Part{llm.TextPart{Text: "upstream timed out"}}, IsError: isError}}
		decision, err := f.runtime.PlanGenerationV1(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}
		return decision.CompactBeforeGenerate
	}
	// Find the smallest byte limit the successful transcript fits in.
	low, high := 1, 1<<20
	if !compacts(low, false) || compacts(high, false) {
		t.Fatal("byte limit does not drive the planning decision")
	}
	for high-low > 1 {
		if middle := (low + high) / 2; compacts(middle, false) {
			low = middle
		} else {
			high = middle
		}
	}
	// 30 spare bytes hold the transcript but not the 60-byte error prefix.
	if compacts(high+30, false) {
		t.Fatal("successful tool result was charged the error prefix")
	}
	if !compacts(high+30, true) {
		t.Fatal("failed tool result prefix was not counted against the route byte limit")
	}
	if compacts(high+30+len(llm.ToolResultErrorTextPrefix), true) {
		t.Fatal("failed tool result still compacts with room for the prefix")
	}
}
