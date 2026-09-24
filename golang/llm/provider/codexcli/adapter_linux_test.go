//go:build linux

package codexcli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

// The test binary is a pinned native child solely for process-boundary tests.
// It never invokes Codex, uses credentials or performs network I/O.
func TestMain(main *testing.M) {
	if len(os.Args) > 1 && os.Args[1] == "--version" {
		fmt.Println("codex-cli " + Version)
		os.Exit(0)
	}
	if len(os.Args) > 1 && os.Args[1] == "exec" {
		testChild()
		os.Exit(0)
	}
	os.Exit(main.Run())
}

func testChild() {
	cwd, _ := os.Getwd()
	info, err := os.Stat(cwd)
	if err != nil || info.Mode().Perm() != 0700 || os.Getenv("HOME") != cwd || os.Getenv("CODEX_EXEC_SERVER_URL") != "none" || os.Getenv("OPENAI_API_KEY") != "" || os.Getenv("CODEX_API_KEY") != "" || os.Getenv("HTTPS_PROXY") != "" {
		fmt.Fprintln(os.Stderr, "unsafe process environment")
		os.Exit(20)
	}
	model := ""
	for index, value := range os.Args {
		if value == "--model" && index+1 < len(os.Args) {
			model = os.Args[index+1]
		}
	}
	_, _ = io.Copy(io.Discard, os.Stdin)
	if model == "interrupt" {
		time.Sleep(30 * time.Second)
		return
	}
	if model == "auth-failure" {
		fmt.Fprintln(os.Stderr, "revoked refresh token SECRET-AUTH-DIAGNOSTIC")
		os.Exit(21)
	}
	fmt.Print(validStreamPrefix)
	if model == "native-tool" {
		fmt.Println(`{"type":"item.started","item":{"id":"tool","type":"command_execution","command":"forbidden"}}`)
		time.Sleep(30 * time.Second)
		return
	}
	fmt.Print(validStreamFinal)
}

const validStreamPrefix = "{\"type\":\"thread.started\",\"thread_id\":\"test-thread\"}\n{\"type\":\"turn.started\"}\n"
const validStreamFinal = "{\"type\":\"item.completed\",\"item\":{\"id\":\"answer\",\"type\":\"agent_message\",\"text\":\"{\\\"ok\\\":true}\"}}\n{\"type\":\"turn.completed\",\"usage\":{\"input_tokens\":10,\"cached_input_tokens\":2,\"cache_write_input_tokens\":0,\"output_tokens\":5,\"reasoning_output_tokens\":1}}\n"

func privateTempDir(t *testing.T) string {
	t.Helper()
	path := t.TempDir()
	if err := os.Chmod(path, 0700); err != nil {
		t.Fatal(err)
	}
	return path
}

func testAdapter(t *testing.T, model string) *Adapter {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(executable)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		t.Fatal(err)
	}
	_ = file.Close()
	config := Config{PrivateOperatorMode: true, BudgetIsEstimate: true, Executable: executable, ExecutableSHA256: hex.EncodeToString(hash.Sum(nil)), Model: model,
		AuthHome: privateTempDir(t), TempRoot: privateTempDir(t), ApprovedTenant: "tenant", ApprovedRootRunID: "root"}
	adapter, err := New("private", "test-v1", config, 3*time.Second, 65536)
	if err != nil {
		t.Fatal(err)
	}
	return adapter
}

func testRequest(model string) llm.Request {
	return llm.Request{APIVersion: llm.APIVersion, OperationKey: "operation", Model: model, ServiceClass: llm.ServiceClassStandard,
		Context: llm.RequestContext{Tenant: "tenant", Tags: map[string]string{llm.RootRunIDContextTag: "root", llm.CostAdmissionContextTag: llm.CostAdmissionForecastV1}},
		Input:   []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "SECRET-PROMPT"}}}},
		Output:  &llm.OutputSpec{Format: llm.OutputFormat{Kind: llm.OutputKindJSONSchema, Name: "result", Strict: true, Schema: json.RawMessage(`{"type":"object","additionalProperties":false,"required":["ok"],"properties":{"ok":{"type":"boolean"}}}`)}}}
}

func TestCompileRejectsUnauthorizedScopeAndHardOutputCap(t *testing.T) {
	adapter := testAdapter(t, "test-model")
	for _, mutate := range []func(*llm.Request){
		func(request *llm.Request) { request.Context.Tenant = "other" },
		func(request *llm.Request) { request.Context.Tags[llm.RootRunIDContextTag] = "other" },
		func(request *llm.Request) { request.Model = "other" },
		func(request *llm.Request) { limit := 20; request.Output.MaxTokens = &limit },
		func(request *llm.Request) { request.Sampling = &llm.SamplingSpec{} },
	} {
		request := testRequest("test-model")
		mutate(&request)
		_, err := adapter.Compile(context.Background(), provider.CompileInput{Request: request})
		var mapped *provider.Error
		if !errors.As(err, &mapped) || mapped.Dispatch != provider.DispatchNotDispatched || mapped.Retry != provider.RetryNever {
			t.Fatalf("unauthorized/unsupported request reached dispatch: %v", err)
		}
	}
	adapter.config.ApprovedOperationKeys = []string{"another-operation"}
	if _, err := adapter.Compile(context.Background(), provider.CompileInput{Request: testRequest("test-model")}); err == nil {
		t.Fatal("exact operation approval was bypassed")
	}
}

func TestStreamRejectsMalformedOrUncertainReceipt(t *testing.T) {
	valid := validStreamPrefix + validStreamFinal
	cases := map[string]string{
		"duplicate-terminal":    valid + `{"type":"turn.completed","usage":{}}` + "\n",
		"missing-terminal":      validStreamPrefix,
		"missing-usage":         validStreamPrefix + strings.Replace(validStreamFinal, `"input_tokens":10,`, "", 1),
		"fabricated-zero-usage": strings.Replace(valid, `"input_tokens":10`, `"input_tokens":0`, 1),
		"duplicate-key":         strings.Replace(valid, `"input_tokens":10`, `"input_tokens":10,"input_tokens":11`, 1),
		"model-reroute":         validStreamPrefix + `{"type":"item.completed","item":{"id":"warning","type":"error","message":"model rerouted"}}` + "\n",
		"native-tool":           validStreamPrefix + `{"type":"item.started","item":{"id":"tool","type":"command_execution"}}` + "\n",
		"truncated-json":        valid[:len(valid)-2],
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			output := &stream{maxBytes: 65536}
			_, _ = output.Write([]byte(body))
			if err := output.finish(); err == nil {
				t.Fatal("malformed receipt accepted")
			}
		})
	}
	output := &stream{maxBytes: 65536}
	if _, err := output.Write([]byte(valid)); err != nil {
		t.Fatal(err)
	}
	if err := output.finish(); err != nil {
		t.Fatal(err)
	}
	if output.usage.InputTokens != 10 || output.usage.OutputTokens != 5 || output.usage.ReasoningTokens != 1 || output.usage.CacheReadTokens != 2 {
		t.Fatal("provider usage did not preserve actual turn counts")
	}
}

type testObserver struct {
	writes int
	deny   bool
}

func (observer *testObserver) BeforePossibleWrite(context.Context) error {
	observer.writes++
	if observer.deny {
		return errors.New("durable state unavailable")
	}
	return nil
}
func (*testObserver) AfterResponseHeaders(context.Context, provider.ResponseMetadata) error {
	return nil
}
func (*testObserver) OnProgress(context.Context, provider.Progress) {}

func TestInvokePreservesIsolationAndActualReceipt(t *testing.T) {
	t.Setenv("OPENAI_API_KEY", "ambient-must-not-propagate")
	t.Setenv("CODEX_API_KEY", "ambient-must-not-propagate")
	t.Setenv("HTTPS_PROXY", "ambient-must-not-propagate")
	adapter := testAdapter(t, "test-model")
	call, err := adapter.Compile(context.Background(), provider.CompileInput{Request: testRequest("test-model")})
	if err != nil {
		t.Fatal(err)
	}
	observer := &testObserver{}
	result, err := adapter.Invoke(context.Background(), call, observer)
	if err != nil {
		t.Fatal(err)
	}
	if observer.writes != 1 || result.Response.Usage.OutputTokens != 5 || string(result.Response.Provider.Raw["billing_mode"]) != `"subscription_included"` {
		t.Fatal("missing durable dispatch or honest subscription receipt")
	}
	if result.Response.Route.ModelIdentityBasis != llm.ModelIdentityBasisUnknown || result.Response.Route.ObservedModelRevision != "" {
		t.Fatalf("CLI usage-only receipt fabricated an observed model: %#v", result.Response.Route)
	}
	entries, err := os.ReadDir(adapter.config.TempRoot)
	if err != nil || len(entries) != 0 {
		t.Fatal("private prompt workspace was not removed")
	}
}

func TestInterruptedOrRejectedChildNeverReissues(t *testing.T) {
	for _, model := range []string{"interrupt", "auth-failure", "native-tool"} {
		t.Run(model, func(t *testing.T) {
			adapter := testAdapter(t, model)
			adapter.timeout = 250 * time.Millisecond
			call, err := adapter.Compile(context.Background(), provider.CompileInput{Request: testRequest(model)})
			if err != nil {
				t.Fatal(err)
			}
			observer := &testObserver{}
			_, err = adapter.Invoke(context.Background(), call, observer)
			var mapped *provider.Error
			if !errors.As(err, &mapped) || mapped.Dispatch != provider.DispatchAmbiguous || mapped.Retry != provider.RetryNever || observer.writes != 1 {
				t.Fatalf("uncertain child dispatch was not retained: %v", err)
			}
			if strings.Contains(err.Error(), "SECRET") {
				t.Fatal("diagnostic leaked private content")
			}
		})
	}
}

func TestAuthHomeLockSerializesIndependentOpenHandles(t *testing.T) {
	home := privateTempDir(t)
	first, err := lockAuthHome(context.Background(), home)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if second, err := lockAuthHome(ctx, home); err == nil {
		_ = second.Close()
		t.Fatal("concurrent auth-home ownership admitted")
	}
	_ = first.Close()
	third, err := lockAuthHome(context.Background(), home)
	if err != nil {
		t.Fatal(err)
	}
	_ = third.Close()
	if err := os.Remove(filepath.Join(home, ".llmtw-codex.lock")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(home, "unrelated"), filepath.Join(home, ".llmtw-codex.lock")); err != nil {
		t.Fatal(err)
	}
	if lock, err := lockAuthHome(context.Background(), home); err == nil {
		_ = lock.Close()
		t.Fatal("symlink auth lock admitted")
	}
}

func TestVirtualToolsEnforceCallerSchemasAndPolicy(t *testing.T) {
	adapter := testAdapter(t, "test-model")
	request := testRequest("test-model")
	request.Tools = []llm.Tool{{Kind: llm.ToolKindFunction, Name: "lookup", InputSchema: json.RawMessage(`{"type":"object","additionalProperties":false,"required":["query"],"properties":{"query":{"type":"string"}}}`)}}
	request.ToolPolicy = llm.ToolPolicy{Mode: llm.ToolChoiceRequired}
	call, err := adapter.Compile(context.Background(), provider.CompileInput{Request: request})
	if err != nil {
		t.Fatal(err)
	}
	compiled := call.SDKParams.(*compiledCall)
	valid := []byte(`{"kind":"tool_calls","text":"","calls":[{"id":"one","name":"lookup","arguments_json":"{\"query\":\"evidence\"}"}]}`)
	items, status, err := compiled.lift(valid)
	if err != nil || status != llm.ResponseStatusToolCalls || items[0].(llm.ToolCall).Name != "lookup" {
		t.Fatalf("valid virtual call rejected: %v", err)
	}
	for _, raw := range []string{
		`{"kind":"final","text":"{\"ok\":true}","calls":[]}`,
		`{"kind":"tool_calls","text":"","calls":[{"id":"one","name":"exec","arguments_json":"{}"}]}`,
		`{"kind":"tool_calls","text":"","calls":[{"id":"one","name":"lookup","arguments_json":"{\"query\":42}"}]}`,
	} {
		if _, _, err := compiled.lift([]byte(raw)); err == nil {
			t.Fatal("tool schema or policy bypass accepted")
		}
	}
}

func TestAdmissionAndExecutablePinPreventDispatch(t *testing.T) {
	adapter := testAdapter(t, "test-model")
	call, err := adapter.Compile(context.Background(), provider.CompileInput{Request: testRequest("test-model")})
	if err != nil {
		t.Fatal(err)
	}
	observer := &testObserver{deny: true}
	_, err = adapter.Invoke(context.Background(), call, observer)
	var mapped *provider.Error
	if !errors.As(err, &mapped) || mapped.Dispatch != provider.DispatchNotDispatched || observer.writes != 1 {
		t.Fatalf("durable admission refusal did not prevent dispatch: %v", err)
	}
	adapter.config.ExecutableSHA256 = strings.Repeat("0", 64)
	observer = &testObserver{}
	_, err = adapter.Invoke(context.Background(), call, observer)
	if !errors.As(err, &mapped) || mapped.Dispatch != provider.DispatchNotDispatched || observer.writes != 0 {
		t.Fatalf("changed executable reached the dispatch boundary: %v", err)
	}
}
