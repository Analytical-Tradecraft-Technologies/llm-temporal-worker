package codexcli

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/llm/schema"
)

type Adapter struct {
	config     Config
	endpointID string
	version    string
	timeout    time.Duration
	maxBytes   int
}

type compiledCall struct {
	request      llm.Request
	prompt       []byte
	outputSchema []byte
	finalSchema  *schema.Schema
	toolSchemas  map[string]*schema.Schema
	envelope     bool
}

func New(endpointID, capabilityVersion string, config Config, timeout time.Duration, maxBytes int) (*Adapter, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if endpointID == "" || capabilityVersion == "" || timeout <= 0 || timeout > 30*time.Minute || maxBytes <= 0 || maxBytes > 64<<20 {
		return nil, fmt.Errorf("codex_cli requires endpoint, capability version and bounded duration/output")
	}
	if err := privateDirectory(config.AuthHome); err != nil {
		return nil, fmt.Errorf("codex_cli auth home must be an existing private owner-only directory")
	}
	if err := privateDirectory(config.TempRoot); err != nil {
		return nil, fmt.Errorf("codex_cli temp root must be an existing private owner-only directory")
	}
	file, err := pinnedExecutable(config)
	if err != nil {
		return nil, err
	}
	_ = file.Close()
	config.ApprovedOperationKeys = append([]string(nil), config.ApprovedOperationKeys...)
	return &Adapter{config: config, endpointID: endpointID, version: capabilityVersion, timeout: timeout, maxBytes: maxBytes}, nil
}

func (adapter *Adapter) Name() string { return Transport }

func Capabilities(version string) provider.CapabilitySet {
	features := make(map[provider.Feature]provider.Capability)
	for _, feature := range []provider.Feature{provider.FeatureImage, provider.FeatureDocument, provider.FeatureReasoning, provider.FeatureContinuation, provider.FeatureStreaming} {
		features[feature] = provider.Capability{State: provider.CapabilityUnsupported, Reason: "private stateless text-only CLI transport"}
	}
	features[provider.FeatureText] = provider.Capability{State: provider.CapabilityNative}
	features[provider.FeatureUsage] = provider.Capability{State: provider.CapabilityNative}
	features[provider.FeatureStructuredOutput] = provider.Capability{State: provider.CapabilityNative}
	features[provider.FeatureToolCall] = provider.Capability{State: provider.CapabilityEmulated, Transform: "codex_cli_json_tool_envelope_v1", Reason: "validated JSON declarations returned to caller; no CLI tools execute"}
	return provider.CapabilitySet{Version: version, Features: features}
}

func (adapter *Adapter) Capabilities(_ context.Context, query provider.CapabilityQuery) (provider.CapabilitySet, error) {
	if (query.Family != "" && query.Family != provider.FamilyCodexCLI) ||
		(query.EndpointID != "" && query.EndpointID != adapter.endpointID) ||
		(query.Model != "" && query.Model != adapter.config.Model) ||
		(query.ServiceClass != "" && query.ServiceClass != llm.ServiceClassStandard) {
		return provider.CapabilitySet{}, rejected(provider.CodeUnsupportedCapability, "codex_cli route is not approved")
	}
	return Capabilities(adapter.version), nil
}

func (adapter *Adapter) authorized(request llm.Request) bool {
	if request.Context.Tenant != adapter.config.ApprovedTenant || request.Context.Tags[llm.RootRunIDContextTag] != adapter.config.ApprovedRootRunID ||
		request.Context.Tags[llm.CostAdmissionContextTag] != llm.CostAdmissionForecastV1 || request.OperationKey == "" {
		return false
	}
	if len(adapter.config.ApprovedOperationKeys) == 0 {
		return true
	}
	for _, key := range adapter.config.ApprovedOperationKeys {
		if key == request.OperationKey {
			return true
		}
	}
	return false
}

func rejected(code provider.Code, message string) *provider.Error {
	return provider.NewError(code, provider.PhaseCompile, provider.DispatchNotDispatched, provider.RetryNever, message)
}

func uncertain(message string) *provider.Error {
	return provider.NewError(provider.CodeAmbiguousDispatch, provider.PhaseDispatch, provider.DispatchAmbiguous, provider.RetryNever, message)
}

func (adapter *Adapter) Compile(ctx context.Context, input provider.CompileInput) (provider.Call, error) {
	if err := ctx.Err(); err != nil {
		return provider.Call{}, provider.NewPreDispatchContextError(err)
	}
	if !adapter.authorized(input.Request) {
		return provider.Call{}, rejected(provider.CodePermissionDenied, "codex_cli requires the approved private tenant and root operation")
	}
	if _, err := adapter.Capabilities(ctx, input.Query); err != nil {
		return provider.Call{}, err
	}
	request, err := llm.NormalizeRequest(input.Request)
	if err != nil {
		return provider.Call{}, rejected(provider.CodeInvalidArgument, "codex_cli request is invalid")
	}
	if request.Model != adapter.config.Model || request.ServiceClass != llm.ServiceClassStandard || len(request.ServiceClassFallbacks) != 0 ||
		request.Continuation != nil || request.Sampling != nil || request.Reasoning != nil || len(request.Extensions) != 0 {
		return provider.Call{}, rejected(provider.CodeUnsupportedCapability, "codex_cli rejects unapproved model, continuation, sampling, reasoning controls, extensions and tier fallback")
	}
	if request.Output != nil && request.Output.MaxTokens != nil {
		return provider.Call{}, rejected(provider.CodeUnsupportedCapability, "codex_cli cannot enforce output max_tokens; an explicit hard limit is unsupported")
	}
	for _, instruction := range request.Instructions {
		if !textParts(instruction.Content) {
			return provider.Call{}, rejected(provider.CodeUnsupportedCapability, "codex_cli accepts text and JSON content only")
		}
	}
	for _, item := range request.Input {
		var parts []llm.Part
		switch value := item.(type) {
		case llm.Message:
			parts = value.Content
		case llm.ToolResult:
			parts = value.Content
		case llm.ToolCall:
		default:
			return provider.Call{}, rejected(provider.CodeUnsupportedCapability, "codex_cli rejects provider state, references and non-text inputs")
		}
		if !textParts(parts) {
			return provider.Call{}, rejected(provider.CodeUnsupportedCapability, "codex_cli accepts text and JSON content only")
		}
	}
	compiled := &compiledCall{request: request, toolSchemas: make(map[string]*schema.Schema)}
	for _, tool := range request.Tools {
		if tool.Kind != llm.ToolKindFunction {
			return provider.Call{}, rejected(provider.CodeUnsupportedCapability, "codex_cli supports caller-executed virtual functions only")
		}
		parsed, err := schema.Parse(tool.InputSchema)
		if err != nil {
			return provider.Call{}, rejected(provider.CodeInvalidArgument, "codex_cli tool schema is unsupported")
		}
		if compiled.toolSchemas[tool.Name] != nil {
			return provider.Call{}, rejected(provider.CodeInvalidArgument, "codex_cli tool names must be unique")
		}
		compiled.toolSchemas[tool.Name] = parsed
	}
	if request.Output != nil && request.Output.Format.Kind == llm.OutputKindJSONSchema {
		compiled.finalSchema, err = schema.Parse(request.Output.Format.Schema)
		if err != nil {
			return provider.Call{}, rejected(provider.CodeInvalidArgument, "codex_cli final schema is unsupported")
		}
	}
	compiled.envelope = len(request.Tools) != 0 || compiled.finalSchema == nil
	compiled.outputSchema = []byte(envelopeSchema)
	if !compiled.envelope {
		compiled.outputSchema = compiled.finalSchema.Canonical()
	}
	// Only the semantic transcript is sent; tenant, run IDs and operation keys
	// remain local authorization/receipt data, not prompt metadata.
	payload := struct {
		Instructions []llm.Instruction `json:"instructions"`
		Input        []llm.Item        `json:"input"`
		Tools        []llm.Tool        `json:"virtual_tools"`
		ToolPolicy   llm.ToolPolicy    `json:"tool_policy"`
		Output       *llm.OutputSpec   `json:"output"`
	}{request.Instructions, request.Input, request.Tools, request.ToolPolicy, request.Output}
	body, err := json.Marshal(payload)
	if err != nil || len(body) > adapter.maxBytes {
		return provider.Call{}, rejected(provider.CodeInvalidArgument, "codex_cli prompt exceeds its admitted byte limit")
	}
	prefix := "Process the following private caller transcript. Honor its ordered instructions and data. No native tools or environmental actions are permitted. Return only the requested JSON schema.\n"
	if compiled.envelope {
		prefix += "The output is a JSON emulation envelope, not a native tool invocation. Use kind=tool_calls with text empty and calls containing caller-executed functions (arguments_json is an exact JSON string satisfying that function's input_schema), or kind=final with calls empty and text containing the final answer. Honor tool_policy, including required/named/none and parallel=false. For JSON output, text must itself be exact JSON satisfying the requested output schema. Never execute functions yourself.\n"
	}
	compiled.prompt = append([]byte(prefix), body...)
	return provider.Call{EndpointID: adapter.endpointID, Family: provider.FamilyCodexCLI, Model: request.Model, OperationKey: request.OperationKey, ServiceClass: request.ServiceClass, SDKParams: compiled, Metadata: input.Metadata}, nil
}

func textParts(parts []llm.Part) bool {
	for _, part := range parts {
		switch part.(type) {
		case llm.TextPart, llm.JSONPart:
		default:
			return false
		}
	}
	return true
}

const envelopeSchema = `{"type":"object","additionalProperties":false,"required":["kind","text","calls"],"properties":{"kind":{"type":"string","enum":["final","tool_calls"]},"text":{"type":"string"},"calls":{"type":"array","items":{"type":"object","additionalProperties":false,"required":["id","name","arguments_json"],"properties":{"id":{"type":"string"},"name":{"type":"string"},"arguments_json":{"type":"string"}}}}}}`

func (compiled *compiledCall) lift(raw []byte) ([]llm.Item, llm.ResponseStatus, error) {
	if _, err := llm.CanonicalJSON(raw); err != nil {
		return nil, "", fmt.Errorf("invalid exact JSON")
	}
	text := string(raw)
	if compiled.envelope {
		if err := schema.Validate([]byte(envelopeSchema), raw); err != nil {
			return nil, "", fmt.Errorf("invalid virtual tool envelope")
		}
		var envelope struct {
			Kind  string `json:"kind"`
			Text  string `json:"text"`
			Calls []struct {
				ID        string `json:"id"`
				Name      string `json:"name"`
				Arguments string `json:"arguments_json"`
			} `json:"calls"`
		}
		if err := json.Unmarshal(raw, &envelope); err != nil {
			return nil, "", fmt.Errorf("invalid virtual tool envelope")
		}
		if envelope.Kind == "tool_calls" {
			if envelope.Text != "" || len(envelope.Calls) == 0 || compiled.request.ToolPolicy.Mode == llm.ToolChoiceNone || (!compiled.request.ToolPolicy.Parallel && len(envelope.Calls) != 1) {
				return nil, "", fmt.Errorf("virtual tool policy violation")
			}
			seen := make(map[string]bool)
			items := make([]llm.Item, 0, len(envelope.Calls))
			for _, call := range envelope.Calls {
				toolSchema := compiled.toolSchemas[call.Name]
				if call.ID == "" || seen[call.ID] || toolSchema == nil || (compiled.request.ToolPolicy.Mode == llm.ToolChoiceNamed && compiled.request.ToolPolicy.Name != call.Name) || toolSchema.Validate([]byte(call.Arguments)) != nil {
					return nil, "", fmt.Errorf("invalid virtual tool call")
				}
				seen[call.ID] = true
				items = append(items, llm.ToolCall{ID: call.ID, Name: call.Name, Arguments: json.RawMessage(call.Arguments)})
			}
			return items, llm.ResponseStatusToolCalls, nil
		}
		if len(envelope.Calls) != 0 || compiled.request.ToolPolicy.Mode == llm.ToolChoiceRequired || compiled.request.ToolPolicy.Mode == llm.ToolChoiceNamed {
			return nil, "", fmt.Errorf("virtual tool policy violation")
		}
		text = envelope.Text
	}
	var part llm.Part = llm.TextPart{Text: text}
	if compiled.finalSchema != nil {
		if err := compiled.finalSchema.Validate([]byte(text)); err != nil {
			return nil, "", fmt.Errorf("final JSON violates the requested schema")
		}
		part = llm.JSONPart{Value: json.RawMessage(text)}
	} else if compiled.request.Output != nil && compiled.request.Output.Format.Kind == llm.OutputKindJSON {
		if _, err := llm.CanonicalJSON([]byte(text)); err != nil {
			return nil, "", fmt.Errorf("final output is not exact JSON")
		}
		part = llm.JSONPart{Value: json.RawMessage(text)}
	}
	return []llm.Item{llm.Message{Actor: llm.ActorModel, Content: []llm.Part{part}}}, llm.ResponseStatusCompleted, nil
}
