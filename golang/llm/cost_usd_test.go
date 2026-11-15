package llm_test

import (
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
)

func TestCostExactUSDJSONPreservesUnknownAndKnownFree(t *testing.T) {
	free := pricing.MustUSD("0")
	known := llm.Cost{Status: llm.CostStatusKnown, ReservedCostUSD: &free, ActualCostUSD: &free, Method: "catalog_usage"}
	encoded := testassert.MarshalJSON(t, known)
	if string(encoded) != `{"actual_cost_usd":"0.000000000000000000","catalog_version":"","cost_status":"known","method":"catalog_usage","reserved_cost_usd":"0.000000000000000000"}` {
		t.Fatalf("exact cost JSON = %s", encoded)
	}
	decoded := testassert.UnmarshalJSON[llm.Cost](t, encoded, "decode known cost")
	if decoded.ActualCostUSD == nil || !decoded.ActualCostUSD.IsZero() || decoded.ReservedCostUSD == nil || !decoded.ReservedCostUSD.IsZero() {
		t.Fatalf("known zero cost did not round trip: %#v", decoded)
	}
	unknown := llm.Cost{Status: llm.CostStatusUnknown}
	unknownJSON := testassert.MarshalJSON(t, unknown)
	if string(unknownJSON) != `{"actual_cost_usd":null,"catalog_version":"","cost_status":"unknown","method":"","reserved_cost_usd":null}` {
		t.Fatalf("unknown cost JSON = %s", unknownJSON)
	}
	unknownDecoded := testassert.UnmarshalJSON[llm.Cost](t, unknownJSON, "decode unknown cost")
	if unknownDecoded.ReservedCostUSD != nil || unknownDecoded.ActualCostUSD != nil {
		t.Fatalf("unknown cost was coalesced to zero: %s", unknownJSON)
	}
}
