package provider

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// ModelLifecycle is the provider-neutral lifecycle reported by a model-list
// management API. Unknown is a valid value: an adapter must not infer
// availability from an undocumented provider field.
type ModelLifecycle string

const (
	ModelAvailable   ModelLifecycle = "available"
	ModelDeprecated  ModelLifecycle = "deprecated"
	ModelUnavailable ModelLifecycle = "unavailable"
	ModelUnknown     ModelLifecycle = "unknown"
)

func (lifecycle ModelLifecycle) Valid() bool {
	switch lifecycle {
	case ModelAvailable, ModelDeprecated, ModelUnavailable, ModelUnknown:
		return true
	default:
		return false
	}
}

const (
	// ModelListMaxPageSize matches the bounded persisted inventory query page.
	ModelListMaxPageSize = 1000
	// ModelListMaxModels bounds one refresh before persistence. A provider
	// management API must not be able to make a refresh retain an unbounded
	// in-memory inventory while it follows cursors.
	ModelListMaxModels   = 10000
	modelListMaxCursor   = 2048
	modelListMaxMetadata = 32
)

// ErrModelInventoryUnsupported is returned when an adapter does not expose a
// documented model-list management API. Configured routes remain authoritative
// in this case; callers must not treat an empty page as an unsupported result.
var ErrModelInventoryUnsupported = errors.New("provider model inventory is unsupported")

// ModelListQuery is the provider-neutral input to an optional management API.
// Cursor is opaque to the worker and must be returned by the same endpoint
// profile that received it. The registry never calls this method.
type ModelListQuery struct {
	EndpointID string
	Cursor     string
	Limit      int
}

func (query ModelListQuery) Validate() error {
	if err := validateModelListText("endpoint ID", query.EndpointID, 256, true); err != nil {
		return err
	}
	if len(query.Cursor) > modelListMaxCursor || strings.ContainsAny(query.Cursor, "\x00\r\n") {
		return errors.New("model-list cursor is too long or unsafe")
	}
	if query.Limit < 1 || query.Limit > ModelListMaxPageSize {
		return fmt.Errorf("model-list limit must be between 1 and %d", ModelListMaxPageSize)
	}
	return nil
}

// Model is one bounded, normalized provider model record. SafeMetadata is
// deliberately string-only; raw provider response bodies and credentials do
// not cross this boundary.
type Model struct {
	ProviderModelID  string
	DisplayName      string
	OwnedBy          string
	CreatedAt        time.Time
	Lifecycle        ModelLifecycle
	CapabilityDigest [32]byte
	SafeMetadata     map[string]string
}

// ModelListPage is one immutable page from a provider management API.
// Complete=false requires NextCursor so a caller cannot mistake a truncated
// listing for a complete inventory. Unsupported listing is represented by the
// absence of ModelLister, not by an empty successful page.
type ModelListPage struct {
	Models     []Model
	NextCursor string
	Complete   bool
}

func (page ModelListPage) Validate() error {
	if len(page.NextCursor) > modelListMaxCursor || strings.ContainsAny(page.NextCursor, "\x00\r\n") {
		return errors.New("model-list next cursor is too long or unsafe")
	}
	if page.Complete && page.NextCursor != "" {
		return errors.New("complete model-list page cannot have a continuation cursor")
	}
	if !page.Complete && page.NextCursor == "" {
		return errors.New("incomplete model-list page requires a continuation cursor")
	}
	if len(page.Models) > ModelListMaxPageSize {
		return fmt.Errorf("model-list page exceeds maximum page size of %d", ModelListMaxPageSize)
	}
	previous := ""
	for _, model := range page.Models {
		if err := model.validate(); err != nil {
			return err
		}
		if model.ProviderModelID <= previous {
			return errors.New("model-list records must be sorted by provider model ID")
		}
		previous = model.ProviderModelID
	}
	return nil
}

func (model Model) validate() error {
	if err := validateModelListText("provider model ID", model.ProviderModelID, 256, true); err != nil {
		return err
	}
	for name, value := range map[string]string{"display name": model.DisplayName, "owned by": model.OwnedBy} {
		if err := validateModelListText(name, value, 512, false); err != nil {
			return err
		}
	}
	if !model.Lifecycle.Valid() {
		return fmt.Errorf("model %q has unsupported lifecycle %q", model.ProviderModelID, model.Lifecycle)
	}
	if len(model.SafeMetadata) > modelListMaxMetadata {
		return fmt.Errorf("model %q has too much metadata", model.ProviderModelID)
	}
	for key, value := range model.SafeMetadata {
		if err := validateModelListText("metadata key", key, 128, true); err != nil {
			return err
		}
		if err := validateModelListText("metadata value", value, 128, false); err != nil {
			return err
		}
	}
	return nil
}

func validateModelListText(name, value string, max int, required bool) error {
	if (required && value == "") || len(value) > max || strings.TrimSpace(value) != value || strings.ContainsAny(value, "\x00\r\n") {
		return fmt.Errorf("%s is empty, too long, or unsafe", name)
	}
	return nil
}

// ModelLister is an optional management capability of an Adapter. It is
// intentionally separate from Invoke: model discovery must never be
// implemented by sending an inference request or by treating configured
// routes as a provider inventory. Adapters without this extension are
// explicitly unsupported for refresh and remain valid one-shot adapters.
type ModelLister interface {
	Adapter
	ListModels(context.Context, ModelListQuery) (ModelListPage, error)
}

// CollectModelInventory follows one provider listing from its initial page to
// completion. The helper centralizes the safety rules that every management
// caller needs: page validation, strictly increasing model IDs across pages,
// cursor-loop detection, a total model bound, and cancellation checks. It
// performs no inference calls and never turns configured routes into an
// inventory. A missing lister is an explicit unsupported result.
func CollectModelInventory(ctx context.Context, lister ModelLister, query ModelListQuery) ([]Model, error) {
	if lister == nil {
		return nil, ErrModelInventoryUnsupported
	}
	if err := query.Validate(); err != nil {
		return nil, fmt.Errorf("model inventory query: %w", err)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	models := make([]Model, 0)
	seenCursors := make(map[string]struct{})
	previousID := ""
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		page, err := lister.ListModels(ctx, query)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil, err
			}
			// Provider errors can contain response bodies or request metadata. The
			// caller receives only a stable boundary error; adapters may record a
			// redacted provider.Error through their normal observer path.
			return nil, errors.New("provider model inventory request failed")
		}
		if err := page.Validate(); err != nil {
			return nil, fmt.Errorf("validate provider model page: %w", err)
		}
		for _, model := range page.Models {
			if model.ProviderModelID <= previousID {
				return nil, errors.New("provider model inventory is not strictly ordered across pages")
			}
			previousID = model.ProviderModelID
		}
		if len(models)+len(page.Models) > ModelListMaxModels {
			return nil, fmt.Errorf("provider model inventory exceeds maximum of %d models", ModelListMaxModels)
		}
		models = append(models, page.Models...)
		if page.Complete {
			return models, nil
		}
		if _, exists := seenCursors[page.NextCursor]; exists {
			return nil, errors.New("provider model inventory cursor repeated")
		}
		seenCursors[page.NextCursor] = struct{}{}
		query.Cursor = page.NextCursor
	}
}
