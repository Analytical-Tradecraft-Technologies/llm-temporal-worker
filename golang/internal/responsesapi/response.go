package responsesapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

// responseObject is the OpenAI Response. The request-echo fields (tools,
// tool_choice, instructions, metadata) carry their OpenAI defaults: a poll is
// answered from the durable result, which does not keep the request settings.
type responseObject struct {
	ID                 string             `json:"id"`
	Object             string             `json:"object"`
	CreatedAt          int64              `json:"created_at"`
	Status             string             `json:"status"`
	Background         bool               `json:"background"`
	Model              string             `json:"model"`
	Output             []any              `json:"output"`
	Error              *apiError          `json:"error"`
	IncompleteDetails  *incompleteDetails `json:"incomplete_details"`
	Instructions       any                `json:"instructions"`
	Metadata           map[string]string  `json:"metadata"`
	ParallelToolCalls  bool               `json:"parallel_tool_calls"`
	ToolChoice         string             `json:"tool_choice"`
	Tools              []any              `json:"tools"`
	PreviousResponseID *string            `json:"previous_response_id"`
	ServiceTier        *string            `json:"service_tier,omitempty"`
	Usage              *usageObject       `json:"usage"`
}

type incompleteDetails struct {
	Reason string `json:"reason"`
}

type usageObject struct {
	InputTokens         int64              `json:"input_tokens"`
	InputTokensDetails  inputTokenDetails  `json:"input_tokens_details"`
	OutputTokens        int64              `json:"output_tokens"`
	OutputTokensDetails outputTokenDetails `json:"output_tokens_details"`
	TotalTokens         int64              `json:"total_tokens"`
}

type inputTokenDetails struct {
	CachedTokens int64 `json:"cached_tokens"`
}

type outputTokenDetails struct {
	ReasoningTokens int64 `json:"reasoning_tokens"`
}

type outputMessage struct {
	Type    string `json:"type"`
	ID      string `json:"id"`
	Status  string `json:"status"`
	Role    string `json:"role"`
	Content []any  `json:"content"`
}

type outputText struct {
	Type        string `json:"type"`
	Text        string `json:"text"`
	Annotations []any  `json:"annotations"`
}

type outputRefusal struct {
	Type    string `json:"type"`
	Refusal string `json:"refusal"`
}

type outputFunctionCall struct {
	Type      string `json:"type"`
	ID        string `json:"id"`
	CallID    string `json:"call_id"`
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
	Status    string `json:"status"`
}

// responseMeta is what the API knows about a response besides its result.
type responseMeta struct {
	ID                 string
	CreatedAt          time.Time
	Background         bool
	Model              string
	PreviousResponseID string
}

func newResponseObject(meta responseMeta, status string) responseObject {
	object := responseObject{
		ID: meta.ID, Object: "response", CreatedAt: meta.CreatedAt.Unix(), Status: status,
		Background: meta.Background, Model: meta.Model, Output: []any{},
		Metadata: map[string]string{}, ParallelToolCalls: true, ToolChoice: "auto", Tools: []any{},
	}
	if meta.PreviousResponseID != "" {
		previous := meta.PreviousResponseID
		object.PreviousResponseID = &previous
	}
	return object
}

func failedResponse(meta responseMeta, err *apiError) responseObject {
	object := newResponseObject(meta, "failed")
	object.Error = err
	return object
}

func cancelledResponse(meta responseMeta) responseObject {
	return newResponseObject(meta, "cancelled")
}

// completedResponse converts a Generate result. Provider state and reference
// items stay in the checkpoint that previous_response_id resumes from.
func completedResponse(meta responseMeta, response llm.GenerateResponseV1) responseObject {
	if response.Route != nil {
		if response.Route.ResolvedModel != "" {
			meta.Model = response.Route.ResolvedModel
		} else if response.Route.RequestedModel != "" {
			meta.Model = response.Route.RequestedModel
		}
	}
	object := newResponseObject(meta, "completed")
	switch response.Status {
	case llm.ResponseStatusLength:
		object.Status, object.IncompleteDetails = "incomplete", &incompleteDetails{Reason: "max_output_tokens"}
	case llm.ResponseStatusContentFiltered:
		object.Status, object.IncompleteDetails = "incomplete", &incompleteDetails{Reason: "content_filter"}
	case llm.ResponseStatusPaused:
		object.Status, object.IncompleteDetails = "incomplete", &incompleteDetails{Reason: "paused"}
	}
	for index, item := range response.Output {
		itemID := fmt.Sprintf("%s_%d", strings.TrimPrefix(meta.ID, responseIDPrefix), index)
		switch item := item.(type) {
		case llm.Message:
			if item.Actor != llm.ActorModel {
				continue
			}
			content := outputContent(item.Content)
			if len(content) == 0 {
				continue
			}
			object.Output = append(object.Output, outputMessage{Type: "message", ID: "msg_" + itemID, Status: "completed", Role: "assistant", Content: content})
		case llm.ToolCall:
			object.Output = append(object.Output, outputFunctionCall{
				Type: "function_call", ID: "fc_" + itemID, CallID: item.ID, Name: item.Name,
				Arguments: string(item.Arguments), Status: "completed",
			})
		}
	}
	if response.Service != nil {
		class := response.Service.Requested
		if response.Service.Actual != nil {
			class = *response.Service.Actual
		}
		tier := serviceTierFromClass(class)
		object.ServiceTier = &tier
	}
	if response.Usage != nil {
		usage := response.Usage
		input := usage.InputTokens + usage.CacheReadTokens + usage.CacheWriteTokens
		object.Usage = &usageObject{
			InputTokens:         input,
			InputTokensDetails:  inputTokenDetails{CachedTokens: usage.CacheReadTokens},
			OutputTokens:        usage.OutputTokens,
			OutputTokensDetails: outputTokenDetails{ReasoningTokens: usage.ReasoningTokens},
			TotalTokens:         input + usage.OutputTokens,
		}
	}
	return object
}

func outputContent(parts []llm.Part) []any {
	content := make([]any, 0, len(parts))
	for _, part := range parts {
		switch part := part.(type) {
		case llm.TextPart:
			content = append(content, outputText{Type: "output_text", Text: part.Text, Annotations: []any{}})
		case llm.JSONPart:
			content = append(content, outputText{Type: "output_text", Text: string(compactJSON(part.Value)), Annotations: []any{}})
		case llm.RefusalPart:
			content = append(content, outputRefusal{Type: "refusal", Refusal: part.Text})
		}
	}
	return content
}

func compactJSON(value json.RawMessage) []byte {
	var buffer bytes.Buffer
	if err := json.Compact(&buffer, value); err != nil {
		return value
	}
	return buffer.Bytes()
}
