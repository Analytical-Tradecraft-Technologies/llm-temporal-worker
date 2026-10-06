package runtime

import (
	"context"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

// The published v1 response reports the requested, attempted and provider-
// observed service classes, with a diagnostic when the provider served a
// lower class than the worker attempted (#1101).
func TestCloudGenerateResponseReportsServiceClassesAndProviderDowngrade(t *testing.T) {
	f := boundedCloud(t, false)
	f.adapter.invoke = func(ctx context.Context, call provider.Call, o provider.Observer) (provider.Result, error) {
		f.submits.Add(1)
		if err := o.BeforePossibleWrite(ctx); err != nil {
			return provider.Result{}, err
		}
		result := executionResponse(call).Result
		economy := llm.ServiceClassEconomy
		result.Response.Service = llm.ServiceFacts{Actual: &economy, ProviderValue: "flex"}
		return result, nil
	}
	result := f.finish(t)
	if result.Generate == nil || result.Generate.Service == nil {
		t.Fatalf("generate response = %#v, want a service object", result.Generate)
	}
	service := *result.Generate.Service
	if service.Requested == "" || service.Attempted == "" || service.Actual == nil || *service.Actual != llm.ServiceClassEconomy || service.ProviderValue != "flex" {
		t.Fatalf("service = %#v, want requested/attempted classes and the provider's economy", service)
	}
	if service.Attempted == llm.ServiceClassEconomy {
		t.Skip("fixture attempts economy; no downgrade to report")
	}
	found := false
	for _, diagnostic := range result.Generate.Diagnostics {
		if diagnostic.Code == llm.DiagnosticServiceClassProviderDowngrade {
			found = true
		}
	}
	if !found {
		t.Fatalf("diagnostics = %#v, want %s", result.Generate.Diagnostics, llm.DiagnosticServiceClassProviderDowngrade)
	}
}
