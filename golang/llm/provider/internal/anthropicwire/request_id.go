package anthropicwire

import (
	"encoding/json"

	"github.com/anthropics/anthropic-sdk-go"
)

// ResponseRequestID returns the request_id field of the raw response body, or
// "" when the response is nil, has none, or is not a JSON object.
func ResponseRequestID(response *anthropic.Message) string {
	if response == nil {
		return ""
	}
	var envelope struct {
		RequestID string `json:"request_id"`
	}
	if err := json.Unmarshal([]byte(response.RawJSON()), &envelope); err == nil {
		return envelope.RequestID
	}
	return ""
}
