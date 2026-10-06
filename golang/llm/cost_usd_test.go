package llm_test

import (
	"encoding/json"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
)

func TestCostExactUSDJSONPreservesUnknownAndKnownFree(t *testing.T) {
	free := pricing.MustUSD("0")
	known := llm.Cost{Status: llm.CostStatusKnown, ReservedCostUSD: &free, ActualCostUSD: &free, Method: "catalog_usage"}
	encoded, err := json.Marshal(known)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != `{"actual_cost_usd":"0.000000000000000000","catalog_version":"","cost_status":"known","method":"catalog_usage","reserved_cost_usd":"0.000000000000000000"}` {
		t.Fatalf("exact cost JSON = %s", encoded)
	}
	var decoded llm.Cost
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.ActualCostUSD == nil || !decoded.ActualCostUSD.IsZero() || decoded.ReservedCostUSD == nil || !decoded.ReservedCostUSD.IsZero() {
		t.Fatalf("known zero cost did not round trip: %#v", decoded)
	}
	unknown := llm.Cost{Status: llm.CostStatusUnknown}
	unknownJSON, err := json.Marshal(unknown)
	if err != nil {
		t.Fatal(err)
	}
	if string(unknownJSON) != `{"actual_cost_usd":null,"catalog_version":"","cost_status":"unknown","method":"","reserved_cost_usd":null}` {
		t.Fatalf("unknown cost JSON = %s", unknownJSON)
	}
	var unknownDecoded llm.Cost
	if err := json.Unmarshal(unknownJSON, &unknownDecoded); err != nil {
		t.Fatal(err)
	}
	if unknownDecoded.ReservedCostUSD != nil || unknownDecoded.ActualCostUSD != nil {
		t.Fatalf("unknown cost was coalesced to zero: %s", unknownJSON)
	}
}
