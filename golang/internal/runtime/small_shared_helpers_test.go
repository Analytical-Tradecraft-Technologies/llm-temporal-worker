package runtime

import (
	"errors"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/config"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/control"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/storage/cloudstate"
)

func TestConfiguredQueryFilterChecksSourceThenType(t *testing.T) {
	policy := control.PolicyKey("policy")
	request := control.QueryRequest{Kind: llm.QueryBudgetStatus, Filter: control.BudgetStatusQuery{PolicyKey: &policy}}
	_, err := configuredQueryFilter[control.BudgetStatusQuery](request, false, "reader is not configured", "budget status")
	var classified *provider.Error
	if !errors.As(err, &classified) || classified.Code != provider.CodeUnsupportedCapability || !strings.Contains(err.Error(), "reader is not configured") {
		t.Fatalf("unconfigured = %v", err)
	}
	query, err := configuredQueryFilter[control.BudgetStatusQuery](request, true, "reader is not configured", "budget status")
	if err != nil || query.PolicyKey == nil || *query.PolicyKey != policy {
		t.Fatalf("configured = %#v, %v", query, err)
	}
	if _, err := configuredQueryFilter[control.SpendSummaryQuery](request, true, "unused", "spend summary"); err == nil || err.Error() != "spend summary filter has unexpected type" {
		t.Fatalf("wrong filter = %v", err)
	}
}

func TestAmbiguousExecutionKeepsOtherFields(t *testing.T) {
	execution := cloudstate.ProviderExecution{Stage: cloudstate.ExecutionPending, ProviderOperationID: "job"}
	got := ambiguousExecution(execution)
	if got.Stage != cloudstate.ExecutionUnknown || got.Failure == nil || got.Failure.Code != provider.CodeAmbiguousDispatch || got.Failure.Dispatch != provider.DispatchAmbiguous || got.ProviderOperationID != "job" {
		t.Fatalf("ambiguous execution = %#v", got)
	}
	if execution.Stage != cloudstate.ExecutionPending || execution.Failure != nil {
		t.Fatalf("input changed: %#v", execution)
	}
}

func TestIdentityExtensionSpecsMapsFieldsToThemselves(t *testing.T) {
	endpoint := config.EndpointConfig{Extensions: map[string]map[string]any{"vendor": {"a": 1, "b": true}}}
	specs := identityExtensionSpecs(endpoint, func(fields map[string]string) map[string]string { return fields })
	if len(specs) != 1 || len(specs["vendor"]) != 2 || specs["vendor"]["a"] != "a" || specs["vendor"]["b"] != "b" {
		t.Fatalf("specs = %#v", specs)
	}
}
