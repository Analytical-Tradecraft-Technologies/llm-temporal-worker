package responsesapi

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"net/url"
	"path"
	"strings"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

// createRequest is the accepted subset of the OpenAI Responses create body.
// Unknown fields are rejected rather than ignored, so a caller never believes
// a setting took effect when the worker cannot honour it.
type createRequest struct {
	Model              *string           `json:"model"`
	Input              json.RawMessage   `json:"input"`
	Instructions       *string           `json:"instructions"`
	PreviousResponseID *string           `json:"previous_response_id"`
	Background         *bool             `json:"background"`
	Stream             *bool             `json:"stream"`
	Store              *bool             `json:"store"`
	Tools              []json.RawMessage `json:"tools"`
	ToolChoice         json.RawMessage   `json:"tool_choice"`
	ParallelToolCalls  *bool             `json:"parallel_tool_calls"`
	Temperature        *json.Number      `json:"temperature"`
	TopP               *json.Number      `json:"top_p"`
	MaxOutputTokens    *int              `json:"max_output_tokens"`
	Text               *textConfig       `json:"text"`
	Reasoning          *reasoningConfig  `json:"reasoning"`
	ServiceTier        *string           `json:"service_tier"`
	Metadata           map[string]string `json:"metadata"`
	Truncation         *string           `json:"truncation"`
	Include            []string          `json:"include"`
	// Accepted for SDK compatibility and ignored: the actor comes from the API
	// key and the worker manages provider prompt caching itself.
	User             *string `json:"user"`
	SafetyIdentifier *string `json:"safety_identifier"`
	PromptCacheKey   *string `json:"prompt_cache_key"`
}

type textConfig struct {
	Format    *textFormat `json:"format"`
	Verbosity *string     `json:"verbosity"`
}

type textFormat struct {
	Type        string          `json:"type"`
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Schema      json.RawMessage `json:"schema"`
	Strict      *bool           `json:"strict"`
}

type reasoningConfig struct {
	Effort  *string `json:"effort"`
	Summary *string `json:"summary"`
	// GenerateSummary is the deprecated spelling of Summary.
	GenerateSummary *string `json:"generate_summary"`
}

// mappedRequest is a decoded create request.
type mappedRequest struct {
	Generate           llm.GenerateRequestV1
	Background         bool
	PreviousResponseID string
}

func decodeCreateRequest(body []byte) (createRequest, *apiError) {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	var request createRequest
	if err := decoder.Decode(&request); err != nil {
		return createRequest{}, invalidRequest("the request body is not a valid Responses API request: " + jsonErrorText(err))
	}
	if decoder.More() {
		return createRequest{}, invalidRequest("the request body must be a single JSON object")
	}
	return request, nil
}

// jsonErrorText keeps decode errors useful without echoing request content.
func jsonErrorText(err error) string {
	var field *json.UnmarshalTypeError
	if errors.As(err, &field) && field.Field != "" {
		return "field " + field.Field + " has the wrong type"
	}
	message := err.Error()
	if strings.HasPrefix(message, "json: unknown field ") {
		return "unsupported " + strings.TrimPrefix(message, "json: ")
	}
	return "malformed JSON"
}

// mapCreateRequest converts the OpenAI request to the v1 Generate request.
// Settings are a patch over the parent checkpoint, so an omitted setting on a
// continuation inherits the previous response's value. The exceptions follow
// OpenAI, which applies them per request rather than carrying them across
// previous_response_id: an omitted instructions clears them, and the
// composite tool policy (tool_choice, parallel_tool_calls) and output
// (max_output_tokens, text.format) are always set in full, with OpenAI's
// defaults for omitted parts. A partial patch of either composite would
// otherwise silently replace the inherited other half.
func mapCreateRequest(request createRequest, identity llm.RequestContext, responseID string) (mappedRequest, *apiError) {
	if request.Stream != nil && *request.Stream {
		return mappedRequest{}, invalidParam("stream", "streaming is not supported; use background: true and poll GET /v1/responses/{id}")
	}
	if request.Truncation != nil && *request.Truncation != "disabled" {
		return mappedRequest{}, invalidParam("truncation", "only truncation: disabled is supported; the worker compacts long conversations itself")
	}
	if len(request.Include) > 0 {
		return mappedRequest{}, invalidParam("include", "include is not supported")
	}
	if len(request.Metadata) > 0 {
		return mappedRequest{}, invalidParam("metadata", "metadata is not supported")
	}
	if request.Text != nil && request.Text.Verbosity != nil {
		return mappedRequest{}, invalidParam("text.verbosity", "text.verbosity is not supported")
	}
	mapped := mappedRequest{Generate: llm.GenerateRequestV1{
		APIVersion: llm.APIVersion, OperationKey: responseID, Context: identity,
	}}
	if request.Background != nil {
		mapped.Background = *request.Background
	}
	if request.PreviousResponseID != nil {
		if !validResponseID(*request.PreviousResponseID) {
			return mappedRequest{}, invalidParam("previous_response_id", "previous_response_id is not a response ID issued by this API")
		}
		mapped.PreviousResponseID = *request.PreviousResponseID
	}
	settings := &mapped.Generate.SettingsPatch
	if request.Model != nil {
		if *request.Model == "" {
			return mappedRequest{}, invalidParam("model", "model must not be empty")
		}
		model := *request.Model
		settings.Model.Set = &model
	} else if mapped.PreviousResponseID == "" {
		return mappedRequest{}, invalidParam("model", "model is required")
	}

	items, leading, apiErr := mapInput(request.Input)
	if apiErr != nil {
		return mappedRequest{}, apiErr
	}
	mapped.Generate.Append = items
	var instructions []llm.Instruction
	if request.Instructions != nil && *request.Instructions != "" {
		instructions = append(instructions, llm.Instruction{Kind: llm.InstructionKindText, Text: *request.Instructions})
	}
	instructions = append(instructions, leading...)
	if len(instructions) > 0 {
		settings.Instructions.Set = &instructions
	} else if mapped.PreviousResponseID != "" {
		settings.Instructions.Clear = true
	}

	continuation := mapped.PreviousResponseID != ""
	if apiErr := mapTools(request, settings, continuation); apiErr != nil {
		return mappedRequest{}, apiErr
	}
	if request.Temperature != nil {
		value, err := llm.NewDecimalV1(request.Temperature.String())
		if err != nil {
			return mappedRequest{}, invalidParam("temperature", "temperature must be a non-negative decimal")
		}
		settings.Temperature.Set = &value
	}
	if request.TopP != nil {
		value, err := llm.NewDecimalV1(request.TopP.String())
		if err != nil || llm.ValidateTopPV1(value) != nil {
			return mappedRequest{}, invalidParam("top_p", "top_p must be greater than 0 and at most 1")
		}
		settings.TopP.Set = &value
	}
	if apiErr := mapOutput(request, settings, continuation); apiErr != nil {
		return mappedRequest{}, apiErr
	}
	if apiErr := mapReasoning(request.Reasoning, settings); apiErr != nil {
		return mappedRequest{}, apiErr
	}
	if request.ServiceTier != nil {
		class, ok := serviceClassFromTier(*request.ServiceTier)
		if !ok {
			return mappedRequest{}, invalidParam("service_tier", "service_tier must be auto, default, flex or priority")
		}
		if class != "" {
			settings.ServiceClass.Set = &class
		}
	}
	return mapped, nil
}

// serviceClassFromTier maps OpenAI's service tiers to the closed service
// class enum. "auto" leaves the class unset, which inherits the parent's
// class or defaults to standard.
func serviceClassFromTier(tier string) (llm.ServiceClass, bool) {
	switch tier {
	case "auto":
		return "", true
	case "flex":
		return llm.ServiceClassEconomy, true
	case "default":
		return llm.ServiceClassStandard, true
	case "priority":
		return llm.ServiceClassPriority, true
	default:
		return "", false
	}
}

func serviceTierFromClass(class llm.ServiceClass) string {
	switch class {
	case llm.ServiceClassEconomy:
		return "flex"
	case llm.ServiceClassPriority:
		return "priority"
	default:
		return "default"
	}
}

type inputItem struct {
	Type      string          `json:"type"`
	ID        string          `json:"id"`
	Role      string          `json:"role"`
	Content   json.RawMessage `json:"content"`
	Status    string          `json:"status"`
	CallID    string          `json:"call_id"`
	Name      string          `json:"name"`
	Arguments *string         `json:"arguments"`
	Output    json.RawMessage `json:"output"`
}

// mapInput converts input to appended items. Leading system and developer
// messages become instructions; later ones cannot be represented, because
// instructions are not positioned inside the transcript.
func mapInput(raw json.RawMessage) ([]llm.Item, []llm.Instruction, *apiError) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil, invalidParam("input", "input is required")
	}
	var text string
	if json.Unmarshal(raw, &text) == nil {
		if text == "" {
			return nil, nil, invalidParam("input", "input must not be empty")
		}
		return []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: text}}}}, nil, nil
	}
	var values []json.RawMessage
	if err := json.Unmarshal(raw, &values); err != nil {
		return nil, nil, invalidParam("input", "input must be a string or an array of items")
	}
	if len(values) == 0 {
		return nil, nil, invalidParam("input", "input must not be empty")
	}
	var items []llm.Item
	var instructions []llm.Instruction
	for index, value := range values {
		param := fmt.Sprintf("input[%d]", index)
		decoder := json.NewDecoder(bytes.NewReader(value))
		decoder.DisallowUnknownFields()
		var item inputItem
		if err := decoder.Decode(&item); err != nil {
			return nil, nil, invalidParam(param, param+" is not a supported input item: "+jsonErrorText(err))
		}
		switch item.Type {
		case "", "message":
			if item.CallID != "" || item.Name != "" || item.Arguments != nil || len(item.Output) > 0 {
				return nil, nil, invalidParam(param, param+" mixes message and tool call fields")
			}
			switch item.Role {
			case "system", "developer":
				if len(items) > 0 {
					return nil, nil, invalidParam(param, "system and developer messages must come before other input items")
				}
				parts, apiErr := mapContent(item.Content, param, false)
				if apiErr != nil {
					return nil, nil, apiErr
				}
				instructions = append(instructions, llm.Instruction{Kind: llm.InstructionKindParts, Content: parts})
			case "user", "assistant":
				parts, apiErr := mapContent(item.Content, param, item.Role == "assistant")
				if apiErr != nil {
					return nil, nil, apiErr
				}
				actor := llm.ActorHuman
				if item.Role == "assistant" {
					actor = llm.ActorModel
				}
				items = append(items, llm.Message{Actor: actor, Content: parts})
			default:
				return nil, nil, invalidParam(param, param+" role must be user, assistant, system or developer")
			}
		case "function_call":
			if item.CallID == "" || item.Name == "" || item.Arguments == nil {
				return nil, nil, invalidParam(param, param+" function_call requires call_id, name and arguments")
			}
			arguments := json.RawMessage(*item.Arguments)
			if !json.Valid(arguments) {
				return nil, nil, invalidParam(param, param+" function_call arguments must be a JSON-encoded string")
			}
			items = append(items, llm.ToolCall{ID: item.CallID, Name: item.Name, Arguments: arguments})
		case "function_call_output":
			if item.CallID == "" || len(item.Output) == 0 {
				return nil, nil, invalidParam(param, param+" function_call_output requires call_id and output")
			}
			parts, apiErr := mapContent(item.Output, param, false)
			if apiErr != nil {
				return nil, nil, apiErr
			}
			items = append(items, llm.ToolResult{CallID: item.CallID, Content: parts})
		case "reasoning", "item_reference":
			return nil, nil, invalidParam(param, item.Type+" input items are not supported; use previous_response_id to continue a conversation")
		default:
			return nil, nil, invalidParam(param, "input item type "+item.Type+" is not supported")
		}
	}
	return items, instructions, nil
}

type contentPart struct {
	Type        string          `json:"type"`
	Text        *string         `json:"text"`
	Refusal     *string         `json:"refusal"`
	ImageURL    *string         `json:"image_url"`
	Detail      string          `json:"detail"`
	FileID      *string         `json:"file_id"`
	FileData    *string         `json:"file_data"`
	FileURL     *string         `json:"file_url"`
	Filename    string          `json:"filename"`
	Annotations json.RawMessage `json:"annotations"`
	Logprobs    json.RawMessage `json:"logprobs"`
}

// mapContent converts message content, which is either a string or an array
// of typed parts. Assistant messages replayed by a client carry output_text
// and refusal parts.
func mapContent(raw json.RawMessage, param string, model bool) ([]llm.Part, *apiError) {
	var text string
	if json.Unmarshal(raw, &text) == nil {
		if text == "" {
			return nil, invalidParam(param, param+" content must not be empty")
		}
		return []llm.Part{llm.TextPart{Text: text}}, nil
	}
	var values []json.RawMessage
	if err := json.Unmarshal(raw, &values); err != nil || len(values) == 0 {
		return nil, invalidParam(param, param+" content must be a non-empty string or array")
	}
	parts := make([]llm.Part, 0, len(values))
	for index, value := range values {
		partParam := fmt.Sprintf("%s.content[%d]", param, index)
		decoder := json.NewDecoder(bytes.NewReader(value))
		decoder.DisallowUnknownFields()
		var part contentPart
		if err := decoder.Decode(&part); err != nil {
			return nil, invalidParam(partParam, partParam+" is not a supported content part: "+jsonErrorText(err))
		}
		switch part.Type {
		case "input_text", "output_text":
			if part.Text == nil || *part.Text == "" {
				return nil, invalidParam(partParam, partParam+" text must not be empty")
			}
			parts = append(parts, llm.TextPart{Text: *part.Text})
		case "refusal":
			if !model || part.Refusal == nil {
				return nil, invalidParam(partParam, partParam+" refusal parts are only valid in assistant messages")
			}
			parts = append(parts, llm.RefusalPart{Text: *part.Refusal})
		case "input_image":
			if part.FileID != nil || part.ImageURL == nil {
				return nil, invalidParam(partParam, partParam+" input_image requires image_url; file_id is not supported")
			}
			image, apiErr := mapImage(*part.ImageURL, part.Detail, partParam)
			if apiErr != nil {
				return nil, apiErr
			}
			parts = append(parts, image)
		case "input_file":
			document, apiErr := mapFile(part, partParam)
			if apiErr != nil {
				return nil, apiErr
			}
			parts = append(parts, document)
		default:
			return nil, invalidParam(partParam, "content part type "+part.Type+" is not supported")
		}
	}
	return parts, nil
}

func mapImage(source, detail, param string) (llm.Part, *apiError) {
	switch detail {
	case "", "auto", "low", "high":
	default:
		return nil, invalidParam(param, param+" detail must be auto, low or high")
	}
	if detail == "auto" {
		detail = ""
	}
	if strings.HasPrefix(source, "data:") {
		mediaType, data, ok := decodeDataURL(source)
		if !ok || !strings.HasPrefix(mediaType, "image/") {
			return nil, invalidParam(param, param+" image_url must be an https URL or a base64 image data URL")
		}
		return llm.ImagePart{Bytes: data, MediaType: mediaType, Detail: detail}, nil
	}
	mediaType := mediaTypeFromURL(source)
	if !strings.HasPrefix(mediaType, "image/") {
		return nil, invalidParam(param, param+" image_url must end in a known image extension (.png, .jpg, .gif, .webp) or be a data URL")
	}
	return llm.ImagePart{URL: source, MediaType: mediaType, Detail: detail}, nil
}

func mapFile(part contentPart, param string) (llm.Part, *apiError) {
	if part.FileID != nil {
		return nil, invalidParam(param, param+" file_id is not supported; send file_data or file_url")
	}
	switch {
	case part.FileData != nil && part.FileURL == nil:
		mediaType, data, ok := decodeDataURL(*part.FileData)
		if !ok {
			return nil, invalidParam(param, param+" file_data must be a base64 data URL")
		}
		return llm.DocumentPart{Bytes: data, MediaType: mediaType, Title: part.Filename}, nil
	case part.FileURL != nil && part.FileData == nil:
		mediaType := mediaTypeFromURL(*part.FileURL)
		if mediaType == "" {
			return nil, invalidParam(param, param+" file_url must end in a known file extension")
		}
		return llm.DocumentPart{URL: *part.FileURL, MediaType: mediaType, Title: part.Filename}, nil
	default:
		return nil, invalidParam(param, param+" input_file requires exactly one of file_data or file_url")
	}
}

func decodeDataURL(value string) (string, []byte, bool) {
	header, payload, found := strings.Cut(strings.TrimPrefix(value, "data:"), ",")
	mediaType, encoding, _ := strings.Cut(header, ";")
	if !found || encoding != "base64" || mediaType == "" {
		return "", nil, false
	}
	data, err := base64.StdEncoding.DecodeString(payload)
	if err != nil || len(data) == 0 {
		return "", nil, false
	}
	return mediaType, data, true
}

func mediaTypeFromURL(value string) string {
	parsed, err := url.Parse(value)
	if err != nil {
		return ""
	}
	mediaType := mime.TypeByExtension(strings.ToLower(path.Ext(parsed.Path)))
	mediaType, _, _ = strings.Cut(mediaType, ";")
	return mediaType
}

type functionTool struct {
	Type        string          `json:"type"`
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Parameters  json.RawMessage `json:"parameters"`
	Strict      *bool           `json:"strict"`
}

func mapTools(request createRequest, settings *llm.SettingsPatchV1, continuation bool) *apiError {
	if request.Tools != nil {
		tools := make([]llm.Tool, 0, len(request.Tools))
		for index, raw := range request.Tools {
			param := fmt.Sprintf("tools[%d]", index)
			decoder := json.NewDecoder(bytes.NewReader(raw))
			decoder.DisallowUnknownFields()
			var tool functionTool
			if err := decoder.Decode(&tool); err != nil || tool.Type != "function" {
				return invalidParam(param, "only function tools are supported; hosted tools are not available through this API")
			}
			schema := tool.Parameters
			if len(schema) == 0 || string(schema) == "null" {
				schema = json.RawMessage(`{"type":"object","properties":{}}`)
			}
			tools = append(tools, llm.Tool{Kind: llm.ToolKindFunction, Name: tool.Name, Description: tool.Description, InputSchema: schema})
		}
		settings.Tools.Set = &tools
	}
	if !continuation && len(request.ToolChoice) == 0 && request.ParallelToolCalls == nil && request.Tools == nil {
		return nil
	}
	policy := llm.ToolPolicy{Mode: llm.ToolChoiceAuto, Parallel: true}
	if request.ParallelToolCalls != nil {
		policy.Parallel = *request.ParallelToolCalls
	}
	if len(request.ToolChoice) > 0 {
		var mode string
		if json.Unmarshal(request.ToolChoice, &mode) == nil {
			switch mode {
			case "none":
				policy.Mode = llm.ToolChoiceNone
			case "auto":
				policy.Mode = llm.ToolChoiceAuto
			case "required":
				policy.Mode = llm.ToolChoiceRequired
			default:
				return invalidParam("tool_choice", "tool_choice must be none, auto, required or a function")
			}
		} else {
			var named struct {
				Type string `json:"type"`
				Name string `json:"name"`
			}
			if json.Unmarshal(request.ToolChoice, &named) != nil || named.Type != "function" || named.Name == "" {
				return invalidParam("tool_choice", "tool_choice must be none, auto, required or a function")
			}
			policy.Mode, policy.Name = llm.ToolChoiceNamed, named.Name
		}
	}
	settings.ToolPolicy.Set = &policy
	return nil
}

func mapOutput(request createRequest, settings *llm.SettingsPatchV1, continuation bool) *apiError {
	var format *textFormat
	if request.Text != nil {
		format = request.Text.Format
	}
	if !continuation && format == nil && request.MaxOutputTokens == nil {
		return nil
	}
	output := llm.OutputSpec{Format: llm.OutputFormat{Kind: llm.OutputKindText}}
	if request.MaxOutputTokens != nil {
		if *request.MaxOutputTokens <= 0 {
			return invalidParam("max_output_tokens", "max_output_tokens must be positive")
		}
		tokens := *request.MaxOutputTokens
		output.MaxTokens = &tokens
	}
	if format != nil {
		switch format.Type {
		case "text":
		case "json_object":
			output.Format.Kind = llm.OutputKindJSON
		case "json_schema":
			if format.Name == "" || len(format.Schema) == 0 {
				return invalidParam("text.format", "json_schema format requires name and schema")
			}
			strict := false
			if format.Strict != nil {
				strict = *format.Strict
			}
			output.Format = llm.OutputFormat{Kind: llm.OutputKindJSONSchema, Name: format.Name, Description: format.Description, Schema: format.Schema, Strict: strict}
		default:
			return invalidParam("text.format", "text.format type must be text, json_object or json_schema")
		}
	}
	settings.Output.Set = &output
	return nil
}

func mapReasoning(reasoning *reasoningConfig, settings *llm.SettingsPatchV1) *apiError {
	if reasoning == nil {
		return nil
	}
	if reasoning.Effort != nil {
		switch effort := *reasoning.Effort; effort {
		case "none":
			mode := llm.ReasoningModeDisabled
			settings.ReasoningMode.Set = &mode
		case "minimal", "low", "medium", "high", "xhigh":
			value := llm.ReasoningEffort(effort)
			settings.ReasoningEffort.Set = &value
		default:
			return invalidParam("reasoning.effort", "reasoning.effort must be none, minimal, low, medium, high or xhigh")
		}
	}
	summary := reasoning.Summary
	if summary == nil {
		summary = reasoning.GenerateSummary
	}
	if summary != nil {
		switch *summary {
		case "auto", "concise", "detailed":
			value := llm.ReasoningSummary(*summary)
			settings.ReasoningSummary.Set = &value
		default:
			return invalidParam("reasoning.summary", "reasoning.summary must be auto, concise or detailed")
		}
	}
	return nil
}
