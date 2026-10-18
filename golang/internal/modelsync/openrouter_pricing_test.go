package modelsync

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHighPrecisionPerTokenPrices(t *testing.T) {
	for input, want := range map[string]string{
		"0.0000000416666666666667":      "0.0416666666666667",
		"0.0000000833333333333333":      "0.0833333333333333",
		"0.000000000000000000000001":    "0.000000000000000001",
		"000.0000000416666666666667000": "0.0416666666666667",
	} {
		t.Run(input, func(t *testing.T) {
			got, err := perMillion(input)
			if err != nil || got.CanonicalString() != want {
				t.Fatalf("perMillion(%q) = %s, %v; want %s", input, got.CanonicalString(), err, want)
			}
		})
	}
	for _, input := range []string{"", "-1", "+1", "1e-8", " 0.1", ".1", "1.", "1.2.3", "NaN", "0.0000000000000000000000001", "100000000000000"} {
		if _, err := perMillion(input); err == nil {
			t.Errorf("perMillion accepted invalid or unrepresentable price %q", input)
		}
	}
	if got := maxPrice("0.0000000416666666666667", "0.000000075"); got != "0.000000075" {
		t.Fatalf("maxPrice = %q; must not understate the higher endpoint price", got)
	}
	if got := maxPrice("0.000000075", "0.0000000833333333333333"); got != "0.0000000833333333333333" {
		t.Fatalf("maxPrice = %q; must compare high-precision prices exactly", got)
	}
}

func TestScheduledPricesReserveAllWindowsConservatively(t *testing.T) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal([]byte(`{
		"prompt":"0.000001", "completion":"0.000004", "input_cache_read":"0.0000001", "request":"0.001",
		"overrides":[
			{"utc_days":["saturday","sunday"], "prompt":"0.0000005", "completion":"0.000002"},
			{"utc_days":["monday"], "utc_start":2300, "utc_end":0, "prompt":"0.000003", "input_cache_read":"0.0000003", "request":"0.002"},
			{"utc_start":100, "utc_end":400, "completion":"0.000008"},
			{"min_prompt_tokens":100000, "prompt":"0.000002", "completion":"0.000006"}
		]
	}`), &raw); err != nil {
		t.Fatal(err)
	}
	prices, err := decodePricing(raw)
	if err != nil {
		t.Fatal(err)
	}
	if prices.Prompt != "0.000003" || prices.Completion != "0.000008" || prices.InputCacheRead != "0.0000003" || prices.Request != "0.002" {
		t.Fatalf("schedule envelope = %+v; want componentwise maximum of base and all windows", prices)
	}
	if len(prices.Overrides) != 1 || prices.Overrides[0].MinPromptTokens != 100000 || prices.Overrides[0].Prompt != "0.000003" || prices.Overrides[0].Completion != "0.000008" {
		t.Fatalf("long-prompt override = %+v; must also cover scheduled prices", prices.Overrides)
	}
	quoted, err := maxPrices([]Endpoint{{Tag: "upstream", Pricing: prices}})
	if err != nil || quoted.InputPerMillion.CanonicalString() != "3" || quoted.OutputPerMillion.CanonicalString() != "8" || quoted.PerRequest.CanonicalString() != "0.002" {
		t.Fatalf("quoted prices = %+v, %v", quoted, err)
	}
}

func TestFetchedCatalogRetainsPrecisionAndScheduledPrices(t *testing.T) {
	prices := map[string]map[string]any{
		"vendor/precision": {"prompt": "0.000001", "completion": "0.000002", "input_cache_write": "0.0000000416666666666667"},
		"vendor/calendar":  {"prompt": "0.000001", "completion": "0.000002", "overrides": []any{map[string]any{"utc_days": []string{"monday", "tuesday"}, "prompt": "0.000003"}}},
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/models" {
			var models []map[string]any
			for id, price := range prices {
				models = append(models, map[string]any{"id": id, "architecture": map[string]any{"output_modalities": []string{"text"}}, "pricing": price})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"data": models})
			return
		}
		for id, price := range prices {
			if r.URL.Path == "/models/"+id+"/endpoints" {
				_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"endpoints": []any{map[string]any{"tag": "upstream", "pricing": price}}}})
				return
			}
		}
		http.NotFound(w, r)
	}))
	defer server.Close()
	document, err := (Fetcher{Client: server.Client(), BaseURL: server.URL}).Fetch(context.Background(), Document{})
	if err != nil || len(document.Models) != 2 {
		t.Fatalf("Fetch = %d models, %v; want both priced text models", len(document.Models), err)
	}
	encoded, err := document.Encode()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeDocument(encoded)
	if err != nil {
		t.Fatal(err)
	}
	for _, model := range decoded.Models {
		if _, err := maxPrices(model.Endpoints); err != nil {
			t.Fatalf("price fetched model %q: %v", model.ID, err)
		}
	}
}

func TestPricingRejectsMalformedOrUnrepresentableOverrides(t *testing.T) {
	for _, override := range []string{
		`{}`, `{"utc_days":[]}`, `{"utc_days":["funday"]}`, `{"utc_days":["monday","monday"]}`,
		`{"utc_start":100}`, `{"utc_start":2360,"utc_end":0}`, `{"utc_start":-1,"utc_end":100}`,
		`{"utc_start":0,"utc_end":2400}`, `{"utc_days":null}`, `{"min_prompt_tokens":0}`,
		`{"min_prompt_tokens":100,"request":"0.1"}`,
	} {
		t.Run(override, func(t *testing.T) {
			var raw map[string]json.RawMessage
			if err := json.Unmarshal([]byte(`{"prompt":"0.000001","completion":"0.000002","overrides":[`+override+`]}`), &raw); err != nil {
				t.Fatal(err)
			}
			if _, err := decodePricing(raw); err == nil {
				t.Fatal("accepted malformed or unsupported override")
			}
		})
	}
	var request map[string]json.RawMessage
	if err := json.Unmarshal([]byte(`{"prompt":"0.000001","completion":"0.000002","request":"0.0000000416666666666667"}`), &request); err != nil {
		t.Fatal(err)
	}
	if _, err := decodePricing(request); err == nil {
		t.Fatal("accepted an unrepresentable absolute request price")
	}
	if err := json.Unmarshal([]byte(`{"completion":"0.000002","overrides":[{"utc_days":["monday"],"prompt":"0.000001"}]}`), &request); err != nil {
		t.Fatal(err)
	}
	delete(request, "prompt")
	delete(request, "request")
	if _, err := decodePricing(request); err == nil {
		t.Fatal("a schedule cannot fill an unlisted base price")
	}
}

// A fetch reports the models and endpoints it leaves out for an unusable
// price, so a model dropped from the catalog is visible before a request
// fails planning with no_route.
func TestFetchReportsModelsLeftOutForUnusablePrices(t *testing.T) {
	usable := map[string]any{"prompt": "0.000001", "completion": "0.000002"}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/models":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []any{
				map[string]any{"id": "vendor/priced", "architecture": map[string]any{"output_modalities": []string{"text"}}, "pricing": usable},
				map[string]any{"id": "vendor/router", "architecture": map[string]any{"output_modalities": []string{"text"}}, "pricing": map[string]any{"prompt": "-1", "completion": "-1"}},
			}})
		case "/models/vendor/priced/endpoints":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"endpoints": []any{
				map[string]any{"tag": "good", "pricing": usable},
				map[string]any{"tag": "bad", "pricing": map[string]any{"prompt": "1e-6", "completion": "0.000002"}},
			}}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	var skipped []SkippedModel
	document, err := (Fetcher{Client: server.Client(), BaseURL: server.URL, OnSkipped: func(models []SkippedModel) { skipped = models }}).Fetch(context.Background(), Document{})
	if err != nil || len(document.Models) != 1 || len(document.Models[0].Endpoints) != 1 {
		t.Fatalf("Fetch = %+v, %v; want the priced model with its usable endpoint", document.Models, err)
	}
	want := []SkippedModel{{ID: "vendor/router", Cause: SkipModelPrice}, {ID: "vendor/priced", Cause: SkipEndpointPrice}}
	if len(skipped) != len(want) || skipped[0] != want[0] || skipped[1] != want[1] {
		t.Fatalf("skipped = %+v, want %+v", skipped, want)
	}
}
