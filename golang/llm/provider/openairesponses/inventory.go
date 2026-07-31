package openairesponses

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

// ListModels implements the optional provider management capability for the
// direct OpenAI Responses endpoint. Azure Responses uses a different
// management surface and remains explicitly unsupported until it has a
// capability-specific contract.
func (adapter *ModelListerAdapter) ListModels(ctx context.Context, query provider.ModelListQuery) (provider.ModelListPage, error) {
	if adapter == nil || adapter.Adapter == nil || adapter.client == nil {
		return provider.ModelListPage{}, provider.ErrModelInventoryUnsupported
	}
	if err := query.Validate(); err != nil {
		return provider.ModelListPage{}, err
	}
	if query.EndpointID != adapter.endpointID {
		return provider.ModelListPage{}, fmt.Errorf("provider model inventory endpoint does not match adapter")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return provider.ModelListPage{}, err
	}
	page, err := adapter.client.sdk.Models.List(ctx)
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
	digest := modelDigest(models)
	offset := 0
	if cursor != "" {
		parts := strings.Split(cursor, ":")
		if len(parts) != 2 || parts[0] != digest {
			if len(parts) == 2 && parts[0] != digest {
				return provider.ModelListPage{}, errors.New("provider model inventory snapshot changed during pagination")
			}
			return provider.ModelListPage{}, fmt.Errorf("provider model inventory cursor is invalid")
		}
		parsed, err := strconv.Atoi(parts[1])
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
		result.NextCursor = digest + ":" + strconv.Itoa(end)
	}
	return result, result.Validate()
}

func modelDigest(models []provider.Model) string {
	hash := sha256.New()
	for _, model := range models {
		fmt.Fprintf(hash, "%s\x00%s\x00%s\x00%d\x00%s\x00", model.ProviderModelID, model.DisplayName, model.OwnedBy, model.CreatedAt.UnixNano(), model.Lifecycle)
		keys := make([]string, 0, len(model.SafeMetadata))
		for key := range model.SafeMetadata {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			fmt.Fprintf(hash, "%s\x00%s\x00", key, model.SafeMetadata[key])
		}
	}
	return fmt.Sprintf("%x", hash.Sum(nil))
}

var _ provider.ModelLister = (*ModelListerAdapter)(nil)
