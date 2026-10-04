package openairesponses

import (
	"fmt"

	"github.com/openai/openai-go/v3"
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
	params.Store = openai.Bool(false)
	return nil
}
