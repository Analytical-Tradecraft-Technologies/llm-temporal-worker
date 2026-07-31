package openaichat

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

// ListModels implements the optional provider management capability for the
// direct OpenAI endpoint. Compatible endpoints use different model-list
// contracts, so their constructors leave this capability disabled rather than
// treating an inference-compatible /models response as authoritative.
func (adapter *Adapter) ListModels(ctx context.Context, query provider.ModelListQuery) (provider.ModelListPage, error) {
	if adapter == nil || adapter.client == nil || !adapter.client.modelListingSupported {
		return provider.ModelListPage{}, provider.ErrModelInventoryUnsupported
	}
	if err := query.Validate(); err != nil {
		return provider.ModelListPage{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return provider.ModelListPage{}, err
	}
	page, err := adapter.client.sdk.Models.List(ctx, adapter.client.options()...)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return provider.ModelListPage{}, err
		}
		return provider.ModelListPage{}, errors.New("provider model inventory request failed")
	}
	if page == nil {
		return provider.ModelListPage{}, errors.New("provider model inventory response was empty")
	}
	models := make([]provider.Model, 0, len(page.Data))
	for _, model := range page.Data {
		if model.ID == "" {
			return provider.ModelListPage{}, errors.New("provider model inventory contains an empty model id")
		}
		created := time.Time{}
		if model.Created > 0 {
			created = time.Unix(model.Created, 0).UTC()
		}
		models = append(models, provider.Model{
			ProviderModelID: model.ID,
			DisplayName:     model.ID,
			OwnedBy:         model.OwnedBy,
			CreatedAt:       created,
			Lifecycle:       provider.ModelAvailable,
		})
	}
	sort.Slice(models, func(i, j int) bool { return models[i].ProviderModelID < models[j].ProviderModelID })
	return pageModels(models, query.Cursor, query.Limit)
}

func pageModels(models []provider.Model, cursor string, limit int) (provider.ModelListPage, error) {
	offset := 0
	if cursor != "" {
		parsed, err := strconv.Atoi(cursor)
		if err != nil || parsed < 0 {
			return provider.ModelListPage{}, fmt.Errorf("provider model inventory cursor is invalid")
		}
		offset = parsed
	}
	if offset > len(models) {
		return provider.ModelListPage{}, fmt.Errorf("provider model inventory cursor is outside the result")
	}
	end := offset + limit
	if end > len(models) {
		end = len(models)
	}
	result := provider.ModelListPage{Models: append([]provider.Model(nil), models[offset:end]...)}
	if end == len(models) {
		result.Complete = true
	} else {
		result.NextCursor = strconv.Itoa(end)
	}
	return result, result.Validate()
}

var _ provider.ModelLister = (*Adapter)(nil)
