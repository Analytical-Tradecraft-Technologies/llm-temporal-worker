package openaichat

import (
	"encoding/json"
	"fmt"
	"strings"

	openai "github.com/openai/openai-go/v3"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/pricing"
)

func rawResponseObject(response *openai.ChatCompletion) (map[string]json.RawMessage, error) {
	if response == nil || strings.TrimSpace(response.RawJSON()) == "" {
		return map[string]json.RawMessage{}, nil
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(response.RawJSON()), &fields); err != nil {
		return nil, fmt.Errorf("provider response metadata: %w", err)
	}
	if fields == nil {
		return nil, fmt.Errorf("provider response metadata is not an object")
	}
	return fields, nil
}

func addRawFact(target map[string]json.RawMessage, key string, value json.RawMessage) {
	if target == nil || key == "" || !json.Valid(value) {
		return
	}
	target[key] = append(json.RawMessage(nil), value...)
}

func parseDecimalUSD(raw json.RawMessage, field string) (pricing.USD, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return pricing.USD{}, fmt.Errorf("%s is missing", field)
	}
	value := strings.TrimSpace(string(raw))
	if strings.HasPrefix(value, "\"") {
		var decoded string
		if err := json.Unmarshal(raw, &decoded); err != nil {
			return pricing.USD{}, fmt.Errorf("%s is not a decimal", field)
		}
		value = strings.TrimSpace(decoded)
	}
	amount, err := pricing.ParseUSD(value)
	if err != nil {
		return pricing.USD{}, fmt.Errorf("%s: %w", field, err)
	}
	return amount, nil
}

func responseAugmentCost(response *llm.Response, raw json.RawMessage, field string, method string) error {
	actual, err := parseDecimalUSD(raw, field)
	if err != nil {
		return err
	}
	response.Cost = llm.Cost{ActualCostUSD: &actual, Method: method}
	addRawFact(response.Provider.Raw, field, raw)
	addRawFact(response.Usage.ProviderRaw, field, raw)
	return nil
}
