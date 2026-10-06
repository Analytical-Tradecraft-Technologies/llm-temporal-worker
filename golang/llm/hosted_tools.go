package llm

import (
	"encoding/json"
	"fmt"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"strings"
)

// HostedToolReservation adds a conservative allowance for reviewed tool
// versions: $10/1000 native searches, 1GB OpenAI container sessions, and one
// hour of Anthropic execution. Rates verified 2026-10-06. Session/free-tier
// billing cannot be inferred from call counts and stays unknown at settlement.
func HostedToolReservation(request Request, family string) pricing.USD {
	amount := pricing.MustUSD("0")
	if request.WebSearch {
		amount = pricing.MustUSD("0.03")
	}
	if request.CodeExecution {
		fee := pricing.MustUSD("0.09")
		if family == "anthropic_messages" {
			fee = pricing.MustUSD("0.05")
		}
		amount, _ = amount.Add(fee)
	}
	return amount
}

// HostedToolCharge prices only separately observed tool usage. A container-use
// count does not establish execution time or whether a session was billed.
func HostedToolCharge(usage Usage, model string) (pricing.USD, error) {
	if string(usage.ProviderRaw["hosted_execution_used"]) == "true" {
		return pricing.USD{}, fmt.Errorf("container execution charge is not reported")
	}
	raw, present := usage.ProviderRaw["web_search_calls"]
	if !present {
		return pricing.MustUSD("0"), nil
	}
	var calls int64
	if string(raw) == "null" || json.Unmarshal(raw, &calls) != nil || calls < 0 {
		return pricing.USD{}, fmt.Errorf("hosted search usage is unknown")
	}
	if calls > 0 && (strings.Contains(model, "gpt-4o-mini") || strings.Contains(model, "gpt-4.1-mini")) {
		return pricing.USD{}, fmt.Errorf("fixed search-content billing is not separately reported")
	}
	return pricing.CeilUSD(pricing.MustDecimalUSD("0.01"), calls, 1)
}
