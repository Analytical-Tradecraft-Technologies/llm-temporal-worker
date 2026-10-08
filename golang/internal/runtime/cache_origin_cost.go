package runtime

import (
	"encoding/json"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/cache"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

// Original cost is provenance, not another charge. Keep it in the existing v1
// diagnostic extension so clients with strict response codecs remain compatible.
// Read the immutable origin receipt; never infer costs from current token prices.
func cacheOriginCost(origin cache.ResponseEntry) (llm.Diagnostic, error) {
	var receipt struct {
		APIVersion  string     `json:"api_version"`
		OperationID string     `json:"operation_id"`
		Cost        llm.CostV1 `json:"cost"`
	}
	if json.Unmarshal(origin.Response, &receipt) != nil || receipt.OperationID != string(origin.OriginOperationID) ||
		(receipt.APIVersion != llm.APIVersion && receipt.APIVersion != llm.CompactAPIVersion) {
		return llm.Diagnostic{}, checkpointPublicationError(provider.CodeStateCorrupt)
	}
	if _, err := receipt.Cost.MarshalJSON(); err != nil {
		return llm.Diagnostic{}, checkpointPublicationError(provider.CodeStateCorrupt)
	}
	details := map[string]string{"origin_operation_id": receipt.OperationID, "cost_status": receipt.Cost.Status}
	if receipt.Cost.Status == "exact" {
		details["actual_cost_usd"] = *receipt.Cost.ActualCostUSD
		details["method"] = receipt.Cost.Method
		if receipt.Cost.CatalogVersion != "" {
			details["catalog_version"] = receipt.Cost.CatalogVersion
		}
	} else {
		details["unknown_reason"] = receipt.Cost.UnknownReason
	}
	return llm.Diagnostic{Code: "cache_origin_cost", Severity: llm.DiagnosticInfo, Path: "cache",
		Message: "Original response cost; this cache replay incurs no new provider charge.", Details: details}, nil
}

func withCacheOriginCost(diagnostics []llm.Diagnostic, origin cache.ResponseEntry) ([]llm.Diagnostic, error) {
	provenance, err := cacheOriginCost(origin)
	if err != nil {
		return nil, err
	}
	// Only the authenticated cache origin supplies this reserved provenance.
	var result []llm.Diagnostic
	for _, diagnostic := range diagnostics {
		if diagnostic.Code != "cache_origin_cost" {
			result = append(result, diagnostic)
		}
	}
	return append(result, provenance), nil
}
