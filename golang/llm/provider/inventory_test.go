package provider_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
)

type inventoryLister struct {
	pages   map[string]provider.ModelListPage
	queries []provider.ModelListQuery
	err     error
}

func (lister *inventoryLister) Name() string { return "inventory-test" }
func (lister *inventoryLister) Capabilities(context.Context, provider.CapabilityQuery) (provider.CapabilitySet, error) {
	return provider.CapabilitySet{}, nil
}
func (lister *inventoryLister) Compile(context.Context, provider.CompileInput) (provider.Call, error) {
	return provider.Call{}, nil
}
func (lister *inventoryLister) Invoke(context.Context, provider.Call, provider.Observer) (provider.Result, error) {
	return provider.Result{}, nil
}
func (lister *inventoryLister) ListModels(_ context.Context, query provider.ModelListQuery) (provider.ModelListPage, error) {
	lister.queries = append(lister.queries, query)
	if lister.err != nil {
		return provider.ModelListPage{}, lister.err
	}
	return lister.pages[query.Cursor], nil
}

func TestModelListPageRequiresBoundedCompleteOrCursor(t *testing.T) {
	valid := provider.ModelListPage{Complete: true, Models: []provider.Model{{ProviderModelID: "model-a", Lifecycle: provider.ModelUnknown}}}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	for name, page := range map[string]provider.ModelListPage{
		"missing cursor":    {Models: valid.Models},
		"complete cursor":   {Complete: true, NextCursor: "next", Models: valid.Models},
		"unsorted":          {Complete: true, Models: []provider.Model{{ProviderModelID: "model-b", Lifecycle: provider.ModelUnknown}, {ProviderModelID: "model-a", Lifecycle: provider.ModelUnknown}}},
		"invalid lifecycle": {Complete: true, Models: []provider.Model{{ProviderModelID: "model-a", Lifecycle: provider.ModelLifecycle("future")}}},
	} {
		t.Run(name, func(t *testing.T) {
			if err := page.Validate(); err == nil {
				t.Fatal("invalid model-list page accepted")
			}
		})
	}
	tooMany := make([]provider.Model, provider.ModelListMaxPageSize+1)
	for i := range tooMany {
		tooMany[i] = provider.Model{ProviderModelID: fmt.Sprintf("model-%04d", i), Lifecycle: provider.ModelUnknown}
	}
	if err := (provider.ModelListPage{Complete: true, Models: tooMany}).Validate(); err == nil {
		t.Fatalf("model-list page with %d rows accepted", len(tooMany))
	}
}

func TestModelListQueryBounds(t *testing.T) {
	for name, query := range map[string]provider.ModelListQuery{
		"missing endpoint": {Limit: 1},
		"zero limit":       {EndpointID: "endpoint", Limit: 0},
		"oversized limit":  {EndpointID: "endpoint", Limit: provider.ModelListMaxPageSize + 1},
		"unsafe cursor":    {EndpointID: "endpoint", Cursor: "bad\n", Limit: 1},
	} {
		t.Run(name, func(t *testing.T) {
			if err := query.Validate(); err == nil {
				t.Fatal("invalid model-list query accepted")
			}
		})
	}
}

func TestCollectModelInventoryFollowsCursorsAndPreservesOrder(t *testing.T) {
	lister := &inventoryLister{pages: map[string]provider.ModelListPage{
		"":     {NextCursor: "next", Models: []provider.Model{{ProviderModelID: "model-a", Lifecycle: provider.ModelUnknown}}},
		"next": {Complete: true, Models: []provider.Model{{ProviderModelID: "model-b", Lifecycle: provider.ModelAvailable}}},
	}}
	models, err := provider.CollectModelInventory(context.Background(), lister, provider.ModelListQuery{EndpointID: "endpoint", Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(models) != 2 || models[0].ProviderModelID != "model-a" || models[1].ProviderModelID != "model-b" {
		t.Fatalf("collected models = %#v", models)
	}
	if len(lister.queries) != 2 || lister.queries[1].Cursor != "next" {
		t.Fatalf("queries = %#v, want initial and next cursor", lister.queries)
	}
}

func TestCollectModelInventoryRejectsUnsupportedAndCursorLoops(t *testing.T) {
	if _, err := provider.CollectModelInventory(context.Background(), nil, provider.ModelListQuery{EndpointID: "endpoint", Limit: 1}); !errors.Is(err, provider.ErrModelInventoryUnsupported) {
		t.Fatalf("nil lister error = %v", err)
	}
	lister := &inventoryLister{pages: map[string]provider.ModelListPage{
		"":     {NextCursor: "same", Models: []provider.Model{{ProviderModelID: "model-a", Lifecycle: provider.ModelUnknown}}},
		"same": {NextCursor: "same", Models: []provider.Model{{ProviderModelID: "model-b", Lifecycle: provider.ModelUnknown}}},
	}}
	if _, err := provider.CollectModelInventory(context.Background(), lister, provider.ModelListQuery{EndpointID: "endpoint", Limit: 1}); err == nil {
		t.Fatal("repeated model-list cursor accepted")
	}
}

func TestCollectModelInventoryRejectsCrossPageRegression(t *testing.T) {
	lister := &inventoryLister{pages: map[string]provider.ModelListPage{
		"":     {NextCursor: "next", Models: []provider.Model{{ProviderModelID: "model-b", Lifecycle: provider.ModelUnknown}}},
		"next": {Complete: true, Models: []provider.Model{{ProviderModelID: "model-a", Lifecycle: provider.ModelUnknown}}},
	}}
	if _, err := provider.CollectModelInventory(context.Background(), lister, provider.ModelListQuery{EndpointID: "endpoint", Limit: 1}); err == nil {
		t.Fatal("cross-page model ordering regression accepted")
	}
}

func TestCollectModelInventoryHonorsCancellationBeforeFetch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	lister := &inventoryLister{}
	if _, err := provider.CollectModelInventory(ctx, lister, provider.ModelListQuery{EndpointID: "endpoint", Limit: 1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled inventory error = %v", err)
	}
	if len(lister.queries) != 0 {
		t.Fatal("canceled inventory called provider")
	}
}
