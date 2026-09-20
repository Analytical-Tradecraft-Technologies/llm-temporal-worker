package openairesponses

import (
	"errors"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/pricing"
	"github.com/openai/openai-go/v3/responses"
	"math"
	"strconv"
	"testing"
)

func TestCachedInputCostUsesDisjointComponents(t *testing.T) {
	for _, tc := range []struct {
		name                         string
		total, read, write, ordinary int64
		cost                         string
		invalid                      bool
	}{
		{name: "no cache", total: 1_000_000, ordinary: 1_000_000, cost: "1.000000000000000000"},
		{name: "partial hit", total: 1_000_000, read: 800_000, ordinary: 200_000, cost: "0.280000000000000000"},
		{name: "full hit", total: 1_000_000, read: 1_000_000, cost: "0.100000000000000000"},
		{name: "cache write", total: 1_000_000, write: 1_000_000, cost: "1.250000000000000000"},
		{name: "mixed", total: 1_000_000, read: 600_000, write: 200_000, ordinary: 200_000, cost: "0.510000000000000000"},
		{name: "zero", cost: "0.000000000000000000"},
		{name: "negative total", total: -1, invalid: true},
		{name: "negative read", total: 1, read: -1, invalid: true},
		{name: "negative write", total: 1, write: -1, invalid: true},
		{name: "read exceeds total", total: 1, read: 2, invalid: true},
		{name: "combined exceeds total", total: 3, read: 2, write: 2, invalid: true},
		{name: "overflowing cache sum", total: math.MaxInt64, read: math.MaxInt64, write: math.MaxInt64, invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			response := minimalResponse(responses.ResponseServiceTierDefault, responses.ResponseStatusCompleted)
			response.Usage.InputTokens = tc.total
			response.Usage.InputTokensDetails.CachedTokens = tc.read
			response.Usage.InputTokensDetails.CacheWriteTokens = tc.write
			response.Usage.TotalTokens = tc.total
			lifted, err := liftResponse(provider.Call{ServiceClass: llm.ServiceClassStandard}, &response, "req")
			if tc.invalid {
				var mapped *provider.Error
				if !errors.As(err, &mapped) || mapped.Code != provider.CodeProviderInvalidResponse || mapped.Dispatch != provider.DispatchAccepted || mapped.Provider.ResponseID != "resp" {
					t.Fatalf("error = %#v, want accepted invalid response", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			usage := lifted.Usage
			if tc.total > 0 && string(usage.ProviderRaw["input_tokens"]) != strconv.FormatInt(tc.total, 10) {
				t.Fatalf("inclusive input total lost: %+v", usage.ProviderRaw)
			}
			if usage.InputTokens != tc.ordinary || usage.CacheReadTokens != tc.read || usage.CacheWriteTokens != tc.write {
				t.Fatalf("usage = %+v", usage)
			}
			cost, err := pricing.CostFromUsage(pricing.Entry{Prices: pricing.UnitPrices{InputPerMillion: pricing.MustDecimalUSD("1"), CacheReadPerMillion: pricing.MustDecimalUSD("0.10"), CacheWritePerMillion: pricing.MustDecimalUSD("1.25")}}, pricing.Usage{InputTokens: usage.InputTokens, CacheReadTokens: usage.CacheReadTokens, CacheWriteTokens: usage.CacheWriteTokens})
			if err != nil {
				t.Fatal(err)
			}
			if cost.USD.String() != tc.cost {
				t.Fatalf("cost = %s, want %s", cost.USD.String(), tc.cost)
			}
			if tc.total > 0 && string(usage.ProviderRaw["total_tokens"]) != strconv.FormatInt(tc.total, 10) {
				t.Fatalf("provider total lost: %+v", usage.ProviderRaw)
			}
		})
	}
}
