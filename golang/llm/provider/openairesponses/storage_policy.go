package openairesponses

import (
	"encoding/json"
	"fmt"

	"github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/packages/param"
	"github.com/openai/openai-go/v3/responses"
)

// AdapterOption configures immutable endpoint policy at construction time.
type AdapterOption func(*Adapter)

// WithProviderStoragePermitted controls Responses persistence and stored-response
// continuations. Production always supplies the endpoint's configured policy.
// Omitting this option preserves standalone clients' historical API defaults.
func WithProviderStoragePermitted(permitted bool) AdapterOption {
	return func(adapter *Adapter) { adapter.storageDenied = !permitted }
}

func (adapter *Adapter) enforceStoragePolicy(params *responses.ResponseNewParams) error {
	if !adapter.storageDenied {
		return nil
	}
	if params.Store.Value || params.Background.Value || params.PreviousResponseID.Value != "" {
		return fmt.Errorf("endpoint forbids provider storage, background execution and stored-response continuation")
	}
	// SDK raw JSON overrides take precedence over subsequent typed-field
	// assignments. Inspect and update the actual body while retaining every
	// schema's numeric literals, including calls compiled under an older policy.
	encoded, err := json.Marshal(params)
	if err != nil {
		return fmt.Errorf("provider storage policy could not inspect request")
	}
	var policy struct {
		Store              bool   `json:"store"`
		Background         bool   `json:"background"`
		PreviousResponseID string `json:"previous_response_id"`
	}
	if json.Unmarshal(encoded, &policy) != nil || policy.Store || policy.Background || policy.PreviousResponseID != "" {
		return fmt.Errorf("endpoint forbids provider storage, background execution and stored-response continuation")
	}
	var body map[string]json.RawMessage
	if json.Unmarshal(encoded, &body) != nil || body == nil {
		return fmt.Errorf("provider storage policy could not inspect request")
	}
	body["store"] = json.RawMessage("false")
	encoded, err = json.Marshal(body)
	if err != nil {
		return fmt.Errorf("provider storage policy could not encode request")
	}
	params.Store = openai.Bool(false)
	param.SetJSON(encoded, params)
	return nil
}
