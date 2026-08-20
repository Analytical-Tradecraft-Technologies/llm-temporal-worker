package llm_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm"
)

func TestV1GenerateAndCompactFixturesRoundTrip(t *testing.T) {
	for _, name := range []string{"generate-root.json", "generate-delta.json"} {
		data := readV1Fixture(t, name)
		var request llm.GenerateRequestV1
		if err := json.Unmarshal(data, &request); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		encoded, err := json.Marshal(request)
		if err != nil {
			t.Fatalf("%s marshal: %v", name, err)
		}
		var again llm.GenerateRequestV1
		if err := json.Unmarshal(encoded, &again); err != nil {
			t.Fatalf("%s second decode: %v", name, err)
		}
		if string(encoded) != string(mustCanonicalJSON(t, encoded)) {
			t.Fatalf("%s was not deterministic", name)
		}
	}
	for _, name := range []string{"generate-response.json", "compact-response.json"} {
		data := readV1Fixture(t, name)
		if name == "generate-response.json" {
			var value llm.GenerateResponseV1
			if err := json.Unmarshal(data, &value); err != nil {
				t.Fatal(err)
			}
			if _, err := json.Marshal(value); err != nil {
				t.Fatal(err)
			}
		} else {
			var value llm.CompactResponseV1
			if err := json.Unmarshal(data, &value); err != nil {
				t.Fatal(err)
			}
			if _, err := json.Marshal(value); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestV1RequestContextRejectsTags(t *testing.T) {
	contextWithTags := llm.RequestContext{
		Tenant: "tenant", Project: "project", Actor: "actor",
		Tags: map[string]string{"region": "au"},
	}
	tests := []struct {
		name   string
		value  any
		wire   string
		target func() any
	}{
		{
			name:   "generate",
			value:  llm.GenerateRequestV1{OperationKey: "generate", Context: contextWithTags},
			wire:   `{"api_version":"llm.temporal/v1","operation_key":"generate","context":{"tenant":"tenant","project":"project","actor":"actor","tags":{"region":"au"}},"append":[]}`,
			target: func() any { return new(llm.GenerateRequestV1) },
		},
		{
			name:   "compact",
			value:  llm.CompactRequestV1{OperationKey: "compact", Context: contextWithTags, Parent: "ckp_v1.parent"},
			wire:   `{"api_version":"llm.temporal/compact/v1","operation_key":"compact","context":{"tenant":"tenant","project":"project","actor":"actor","tags":{"region":"au"}},"parent":"ckp_v1.parent"}`,
			target: func() any { return new(llm.CompactRequestV1) },
		},
		{
			name:   "query",
			value:  llm.QueryRequestV1{OperationKey: "query", Context: contextWithTags, Kind: llm.QueryProviderStatus, Query: json.RawMessage(`{}`)},
			wire:   `{"api_version":"llm.temporal/query/v1","operation_key":"query","context":{"tenant":"tenant","project":"project","actor":"actor","tags":{"region":"au"}},"kind":"provider_status","query":{}}`,
			target: func() any { return new(llm.QueryRequestV1) },
		},
	}
	for _, test := range tests {
		t.Run(test.name+" marshal", func(t *testing.T) {
			_, err := json.Marshal(test.value)
			if err == nil {
				t.Fatal("nonempty context tags were accepted")
			}
			if message := err.Error(); !strings.Contains(message, "context") || !strings.Contains(message, "tags") {
				t.Fatalf("error = %q, want clear context tags rejection", message)
			}
		})
		t.Run(test.name+" unmarshal", func(t *testing.T) {
			err := json.Unmarshal([]byte(test.wire), test.target())
			if err == nil {
				t.Fatal("wire context tags were accepted")
			}
			if message := err.Error(); !strings.Contains(message, "context") || !strings.Contains(message, "tags") {
				t.Fatalf("error = %q, want clear context tags rejection", message)
			}
		})
	}
}

func TestV1RequestContextRoundTripsClosedFields(t *testing.T) {
	context := llm.RequestContext{Tenant: "tenant", Project: "project", Actor: "actor"}
	tests := []struct {
		name       string
		value      any
		target     func() any
		getContext func(any) llm.RequestContext
	}{
		{
			name:       "generate",
			value:      llm.GenerateRequestV1{OperationKey: "generate", Context: context},
			target:     func() any { return new(llm.GenerateRequestV1) },
			getContext: func(value any) llm.RequestContext { return value.(*llm.GenerateRequestV1).Context },
		},
		{
			name:       "compact",
			value:      llm.CompactRequestV1{OperationKey: "compact", Context: context, Parent: "ckp_v1.parent"},
			target:     func() any { return new(llm.CompactRequestV1) },
			getContext: func(value any) llm.RequestContext { return value.(*llm.CompactRequestV1).Context },
		},
		{
			name:       "query",
			value:      llm.QueryRequestV1{OperationKey: "query", Context: context, Kind: llm.QueryProviderStatus, Query: json.RawMessage(`{}`)},
			target:     func() any { return new(llm.QueryRequestV1) },
			getContext: func(value any) llm.RequestContext { return value.(*llm.QueryRequestV1).Context },
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			encoded, err := json.Marshal(test.value)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			target := test.target()
			if err := json.Unmarshal(encoded, target); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			got := test.getContext(target)
			if got.Tenant != context.Tenant || got.Project != context.Project || got.Actor != context.Actor || len(got.Tags) != 0 {
				t.Fatalf("context = %#v, want %#v with no tags", got, context)
			}
		})
	}
}

func TestV1RejectsUnknownTranscriptAndMismatchedQueryResult(t *testing.T) {
	var request llm.GenerateRequestV1
	if err := json.Unmarshal(readV1Fixture(t, "negative-unknown-field.json"), &request); err == nil {
		t.Fatal("unknown transcript field was accepted")
	}
	query := llm.QueryResponseV1{OperationKey: "q", QueryExecutionID: "id", Kind: llm.QueryProviderStatus, Result: llm.SpendSummary{}, Cost: llm.CostV1{Status: "exact", ActualCostUSD: stringPointer("0"), Method: "control_query_zero"}}
	if _, err := json.Marshal(query); err == nil {
		t.Fatal("mismatched query result was accepted")
	}
	var queryRequest llm.QueryRequestV1
	unknown := []byte(`{"api_version":"llm.temporal/query/v1","operation_key":"q","context":{"tenant":"t","project":"p","actor":"a"},"kind":"provider_status","query":{"page_size":1001}}`)
	if err := json.Unmarshal(unknown, &queryRequest); err == nil {
		t.Fatal("oversized query page accepted")
	}
	unknown = []byte(`{"api_version":"llm.temporal/query/v1","operation_key":"q","context":{"tenant":"t","project":"p","actor":"a"},"kind":"provider_status","query":{"unknown":true}}`)
	if err := json.Unmarshal(unknown, &queryRequest); err == nil {
		t.Fatal("unknown query field accepted")
	}
	unknown = []byte(`{"api_version":"llm.temporal/query/v1","operation_key":"q","context":{"tenant":"t","project":"p"},"kind":"provider_status","query":{}}`)
	if err := json.Unmarshal(unknown, &queryRequest); err == nil {
		t.Fatal("incomplete query context accepted")
	}
}

func TestV1CostCanonicalizationAndVariantFields(t *testing.T) {
	value, err := json.Marshal(llm.CostV1{Status: "exact", ActualCostUSD: stringPointer("10.500000000000"), Method: "provider_reported"})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(value, []byte(`"actual_cost_usd":"10.5"`)) {
		t.Fatalf("cost was not canonicalized: %s", value)
	}
	var cost llm.CostV1
	if err := json.Unmarshal([]byte(`{"status":"exact","actual_cost_usd":"0.1000","method":"provider_reported"}`), &cost); err != nil {
		t.Fatal(err)
	}
	value, err = json.Marshal(cost)
	if err != nil || !bytes.Contains(value, []byte(`"actual_cost_usd":"0.1"`)) {
		t.Fatalf("round-trip cost was not canonicalized: %s (%v)", value, err)
	}
	if _, err := json.Marshal(llm.CostV1{Status: "exact", ActualCostUSD: stringPointer("0"), Method: "estimate"}); err == nil {
		t.Fatal("unknown exact cost method accepted")
	}
	if _, err := json.Marshal(llm.CostV1{Status: "unknown", UnknownReason: "state_unavailable", CatalogVersion: "catalog-1"}); err == nil {
		t.Fatal("unknown cost with catalog version accepted")
	}
	request := llm.GenerateRequestV1{OperationKey: "nil-arrays", Context: llm.RequestContext{Tenant: "t", Project: "p", Actor: "a"}}
	value, err = json.Marshal(request)
	if err != nil || !bytes.Contains(value, []byte(`"append":[]`)) {
		t.Fatalf("nil append was not normalized: %s (%v)", value, err)
	}
	response := llm.GenerateResponseV1{OperationKey: "nil-output", OperationID: "id", Status: llm.ResponseStatusCompleted, Checkpoint: llm.CheckpointMetadata{Handle: "ckp_v1.nil", Kind: "generation", Depth: 0}, Cache: llm.CacheDispositionV1{Disposition: "disabled"}, Cost: llm.CostV1{Status: "exact", ActualCostUSD: stringPointer("0"), Method: "provider_reported"}}
	value, err = json.Marshal(response)
	if err != nil || !bytes.Contains(value, []byte(`"output":[]`)) {
		t.Fatalf("nil output was not normalized: %s (%v)", value, err)
	}
}

func TestQueryContractRejectsUnknownEnumsAndMalformedTimes(t *testing.T) {
	base := `{"api_version":"llm.temporal/query/v1","operation_key":"q","context":{"tenant":"t","project":"p","actor":"a"},"kind":"provider_status","query":%s}`
	for _, test := range []struct {
		name  string
		query string
	}{
		{name: "availability", query: `{"availability":"unknown"}`},
		{name: "lifecycle", query: `{"lifecycle":"future"}`},
		{name: "missing spend interval", query: `{}`},
		{name: "malformed spend interval", query: `{"start_time":"not-a-time","end_time":"2026-07-19T00:00:00Z"}`},
		{name: "empty spend interval", query: `{"start_time":"2026-07-19T00:00:00Z","end_time":"2026-07-19T00:00:00Z"}`},
		{name: "inverted spend interval", query: `{"start_time":"2026-07-20T00:00:00Z","end_time":"2026-07-19T00:00:00Z"}`},
		{name: "unknown group", query: `{"start_time":"2026-07-18T00:00:00Z","end_time":"2026-07-19T00:00:00Z","group_by":["region"]}`},
		{name: "duplicate operation kind", query: `{"start_time":"2026-07-18T00:00:00Z","end_time":"2026-07-19T00:00:00Z","operation_kinds":["query","query"]}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			payload := fmt.Sprintf(base, test.query)
			if test.name == "availability" || test.name == "lifecycle" {
				kind := "provider_status"
				if test.name == "lifecycle" {
					kind = "model_inventory"
				}
				payload = fmt.Sprintf(`{"api_version":"llm.temporal/query/v1","operation_key":"q","context":{"tenant":"t","project":"p","actor":"a"},"kind":%q,"query":%s}`, kind, test.query)
			} else {
				payload = fmt.Sprintf(`{"api_version":"llm.temporal/query/v1","operation_key":"q","context":{"tenant":"t","project":"p","actor":"a"},"kind":"spend_summary","query":%s}`, test.query)
			}
			var request llm.QueryRequestV1
			if err := json.Unmarshal([]byte(payload), &request); err == nil {
				t.Fatalf("invalid query %s was accepted", test.name)
			}
		})
	}

	valid := llm.QueryResponseV1{
		OperationKey: "q", QueryExecutionID: "query-id", Kind: llm.QuerySpendSummary,
		ObservedAt: "2026-07-19T00:00:00Z", Source: "persisted", Freshness: "current", Complete: true,
		Result: llm.SpendSummary{StartTime: "2026-07-18T00:00:00Z", EndTime: "2026-07-19T00:00:00Z"},
		Cost:   llm.CostV1{Status: "exact", ActualCostUSD: stringPointer("0"), Method: "control_query_zero"},
	}
	for _, test := range []struct {
		name string
		edit func(*llm.QueryResponseV1)
	}{
		{name: "observed timestamp", edit: func(response *llm.QueryResponseV1) { response.ObservedAt = "tomorrow" }},
		{name: "source", edit: func(response *llm.QueryResponseV1) { response.Source = "provider" }},
		{name: "freshness", edit: func(response *llm.QueryResponseV1) { response.Freshness = "fresh" }},
		{name: "spend start timestamp", edit: func(response *llm.QueryResponseV1) {
			response.Result = llm.SpendSummary{StartTime: "bad", EndTime: "2026-07-19T00:00:00Z"}
		}},
		{name: "cost method", edit: func(response *llm.QueryResponseV1) { response.Cost.Method = "estimate" }},
		{name: "nonzero control cost", edit: func(response *llm.QueryResponseV1) { response.Cost.ActualCostUSD = stringPointer("0.1") }},
		{name: "spend pagination cursor", edit: func(response *llm.QueryResponseV1) { response.NextCursor = stringPointer("spend-page-2") }},
	} {
		t.Run("response_"+test.name, func(t *testing.T) {
			candidate := valid
			test.edit(&candidate)
			if _, err := json.Marshal(candidate); err == nil {
				t.Fatalf("invalid response %s was accepted", test.name)
			}
		})
	}
	budget := valid
	budget.Kind = llm.QueryBudgetStatus
	budget.Result = llm.BudgetStatus{ActiveAt: "2026-07-19T00:00:00Z", GenerationID: "generation-1", ManifestDigest: strings.Repeat("0", 64), StreamHighWaterMark: "1-0"}
	budget.NextCursor = stringPointer("budget-page-2")
	if _, err := json.Marshal(budget); err == nil {
		t.Fatal("budget response with a pagination cursor was accepted")
	}
}

func TestQueryContractRejectsNullOptionalStrings(t *testing.T) {
	tests := []struct {
		name  string
		kind  string
		query string
	}{
		{name: "provider status provider", kind: "provider_status", query: `{"provider":null}`},
		{name: "provider status endpoint", kind: "provider_status", query: `{"endpoint":null}`},
		{name: "model inventory prefix", kind: "model_inventory", query: `{"model_prefix":null}`},
		{name: "credit status provider", kind: "credit_status", query: `{"provider":null}`},
		{name: "budget policy", kind: "budget_status", query: `{"policy_key":null}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			payload := fmt.Sprintf(`{"api_version":"llm.temporal/query/v1","operation_key":"q","context":{"tenant":"t","project":"p","actor":"a"},"kind":%q,"query":%s}`, test.kind, test.query)
			var request llm.QueryRequestV1
			if err := json.Unmarshal([]byte(payload), &request); err == nil {
				t.Fatalf("query with null optional string was accepted: %s", test.query)
			}
		})
	}
}

func TestQueryResponseRequiresCursorForIncompletePages(t *testing.T) {
	for _, kind := range []llm.QueryKind{llm.QueryProviderStatus, llm.QueryModelInventory, llm.QueryCreditStatus} {
		t.Run(string(kind), func(t *testing.T) {
			completeWithCursor := validQueryResponse(kind)
			completeWithCursor.NextCursor = stringPointer("page-2")
			if _, err := json.Marshal(completeWithCursor); err == nil {
				t.Fatal("complete response with a cursor was accepted")
			}

			incompleteWithoutCursor := validQueryResponse(kind)
			incompleteWithoutCursor.Complete = false
			if _, err := json.Marshal(incompleteWithoutCursor); err == nil {
				t.Fatal("incomplete response without a cursor was accepted")
			}

			incompleteWithCursor := validQueryResponse(kind)
			incompleteWithCursor.Complete = false
			incompleteWithCursor.NextCursor = stringPointer("page-2")
			if _, err := json.Marshal(incompleteWithCursor); err != nil {
				t.Fatalf("incomplete response with a cursor was rejected: %v", err)
			}
		})
	}

	for _, kind := range []llm.QueryKind{llm.QueryBudgetStatus, llm.QuerySpendSummary} {
		t.Run(string(kind)+"_snapshot", func(t *testing.T) {
			response := validQueryResponse(kind)
			response.Complete = false
			if _, err := json.Marshal(response); err == nil {
				t.Fatal("incomplete snapshot response was accepted")
			}
		})
	}
}

func validQueryResponse(kind llm.QueryKind) llm.QueryResponseV1 {
	response := llm.QueryResponseV1{
		OperationKey: "query-operation", QueryExecutionID: "query-execution", Kind: kind,
		ObservedAt: "2026-07-19T00:00:00Z", Source: "persisted", Freshness: "current", Complete: true,
		Cost: llm.CostV1{Status: "exact", ActualCostUSD: stringPointer("0"), Method: "control_query_zero"},
	}
	switch kind {
	case llm.QueryProviderStatus:
		response.Result = llm.ProviderStatusPage{Routes: []json.RawMessage{}}
	case llm.QueryModelInventory:
		response.Result = llm.ModelInventoryPage{Models: []json.RawMessage{}}
	case llm.QueryCreditStatus:
		response.Result = llm.CreditStatusPage{Endpoints: []json.RawMessage{}}
	case llm.QueryBudgetStatus:
		response.Result = llm.BudgetStatus{ActiveAt: "2026-07-19T00:00:00Z", GenerationID: "generation-1", ManifestDigest: strings.Repeat("a", 64), StreamHighWaterMark: "stream-1", Windows: []json.RawMessage{}}
	case llm.QuerySpendSummary:
		response.Result = llm.SpendSummary{StartTime: "2026-07-18T00:00:00Z", EndTime: "2026-07-19T00:00:00Z", Buckets: []json.RawMessage{}}
	}
	return response
}

func TestQueryResultBoundaryRejectsOpenNestedRows(t *testing.T) {
	base := `{"api_version":"llm.temporal/query/v1","operation_key":"q","query_execution_id":"query-id","kind":"provider_status","observed_at":"2026-07-19T00:00:00Z","source":"persisted","freshness":"current","complete":true,"result":%s,"cost_status":"exact","actual_cost_usd":"0","cost_method":"control_query_zero"}`
	for _, test := range []struct {
		name   string
		result string
	}{
		{name: "unknown page field", result: `{"routes":[],"unexpected":true}`},
		{name: "null required page", result: `{"routes":null}`},
		{name: "unknown route field", result: `{"routes":[{"route_id":"r","provider":"p","endpoint":"e","availability":"available","observed_at":"2026-07-19T00:00:00Z","stale_after":"2026-07-20T00:00:00Z","unexpected":true}]}`},
		{name: "null route row", result: `{"routes":[null]}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			var response llm.QueryResponseV1
			if err := json.Unmarshal([]byte(fmt.Sprintf(base, test.result)), &response); err == nil {
				t.Fatalf("invalid query result %s was accepted", test.name)
			}
		})
	}
	response := llm.QueryResponseV1{
		OperationKey: "q", QueryExecutionID: "query-id", Kind: llm.QueryProviderStatus,
		ObservedAt: "2026-07-19T00:00:00Z", Source: "persisted", Freshness: "current", Complete: true,
		Result: llm.ProviderStatusPage{Routes: []json.RawMessage{json.RawMessage(`{"route_id":"r","provider":"p","endpoint":"e","availability":"available","observed_at":"2026-07-19T00:00:00Z","stale_after":"2026-07-20T00:00:00Z","unexpected":true}`)}},
		Cost:   llm.CostV1{Status: "exact", ActualCostUSD: stringPointer("0"), Method: "control_query_zero"},
	}
	if _, err := json.Marshal(response); err == nil {
		t.Fatal("marshal accepted an unknown nested result field")
	}
	for _, test := range []struct {
		name   string
		kind   string
		result string
	}{
		{name: "budget unknown page field", kind: "budget_status", result: `{"active_at":"2026-07-19T00:00:00Z","generation_id":"g","manifest_digest":"0000000000000000000000000000000000000000000000000000000000000000","stream_high_water_mark":"h","windows":[],"unexpected":true}`},
		{name: "spend unknown page field", kind: "spend_summary", result: `{"start_time":"2026-07-18T00:00:00Z","end_time":"2026-07-19T00:00:00Z","buckets":[],"unexpected":true}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			payload := fmt.Sprintf(`{"api_version":"llm.temporal/query/v1","operation_key":"q","query_execution_id":"query-id","kind":%q,"observed_at":"2026-07-19T00:00:00Z","source":"persisted","freshness":"current","complete":true,"result":%s,"cost_status":"exact","actual_cost_usd":"0","cost_method":"control_query_zero"}`, test.kind, test.result)
			var response llm.QueryResponseV1
			if err := json.Unmarshal([]byte(payload), &response); err == nil {
				t.Fatalf("invalid query result %s was accepted", test.name)
			}
		})
	}
}

func TestV1SettingsPatchAndResponseMetadataUseWireDecoders(t *testing.T) {
	requestData := []byte(`{"api_version":"llm.temporal/v1","operation_key":"op","context":{"tenant":"t","project":"p","actor":"a"},"append":[],"settings_patch":{"instructions":{"set":[{"kind":"parts","content":[{"kind":"text","text":"hello"}]}]},"tools":{"set":[{"name":"lookup","description":"lookup data","input_schema":{"type":"object"}}]},"output":{"set":{"max_tokens":32,"format":{"kind":"text"}}}}}`)
	var request llm.GenerateRequestV1
	if err := json.Unmarshal(requestData, &request); err != nil {
		t.Fatal(err)
	}
	if request.SettingsPatch.Instructions.Set == nil || len(*request.SettingsPatch.Instructions.Set) != 1 || len((*request.SettingsPatch.Instructions.Set)[0].Content) != 1 {
		t.Fatalf("parts instruction was not decoded: %#v", request.SettingsPatch.Instructions)
	}
	if request.SettingsPatch.Tools.Set == nil || len(*request.SettingsPatch.Tools.Set) != 1 || string((*request.SettingsPatch.Tools.Set)[0].InputSchema) != `{"type":"object"}` {
		t.Fatalf("tool input schema was not decoded: %#v", request.SettingsPatch.Tools)
	}
	if request.SettingsPatch.Output.Set == nil || request.SettingsPatch.Output.Set.MaxTokens == nil || *request.SettingsPatch.Output.Set.MaxTokens != 32 {
		t.Fatalf("output max_tokens was not decoded: %#v", request.SettingsPatch.Output)
	}
	canonicalTemperature := []byte(`{"api_version":"llm.temporal/v1","operation_key":"op","context":{"tenant":"t","project":"p","actor":"a"},"append":[],"settings_patch":{"temperature":{"set":"0.7000"}}}`)
	var decimalRequest llm.GenerateRequestV1
	if err := json.Unmarshal(canonicalTemperature, &decimalRequest); err != nil {
		t.Fatalf("decimal temperature was not decoded: %v", err)
	}
	encodedDecimal, err := json.Marshal(decimalRequest)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(encodedDecimal, []byte(`"temperature":{"set":"0.7"}`)) {
		t.Fatalf("temperature was not canonically re-encoded as a string: %s", encodedDecimal)
	}
	legacyNumeric := bytes.Replace(canonicalTemperature, []byte(`"0.7000"`), []byte(`0.7`), 1)
	var legacyRequest llm.GenerateRequestV1
	if err := json.Unmarshal(legacyNumeric, &legacyRequest); err != nil {
		t.Fatalf("legacy numeric temperature was not accepted during compatibility window: %v", err)
	}
	invalidDecimal := bytes.Replace(canonicalTemperature, []byte(`"0.7000"`), []byte(`"1.0000000000000000001"`), 1)
	if err := json.Unmarshal(invalidDecimal, &legacyRequest); err == nil {
		t.Fatal("temperature with excessive precision was accepted")
	}

	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(readV1Fixture(t, "generate-response.json"), &envelope); err != nil {
		t.Fatal(err)
	}
	envelope["route"] = json.RawMessage(`{"route_id":"route-1","endpoint_id":"endpoint-1","api_family":"responses","requested_model":"gpt-test","resolved_model":"gpt-test-2026"}`)
	envelope["usage"] = json.RawMessage(`{"input_tokens":10,"output_tokens":20,"reasoning_tokens":3,"cache_read_tokens":4,"cache_write_tokens":5}`)
	data, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	var response llm.GenerateResponseV1
	if err := json.Unmarshal(data, &response); err != nil {
		t.Fatal(err)
	}
	if response.Route == nil || response.Route.RouteID != "route-1" || response.Route.ResolvedModel != "gpt-test-2026" {
		t.Fatalf("route metadata was not decoded: %#v", response.Route)
	}
	if response.Usage == nil || response.Usage.InputTokens != 10 || response.Usage.CacheWriteTokens != 5 {
		t.Fatalf("usage metadata was not decoded: %#v", response.Usage)
	}
}

func TestV1TemperaturePreservesAllEighteenFractionalDigits(t *testing.T) {
	const input = `{"api_version":"llm.temporal/v1","operation_key":"op","context":{"tenant":"t","project":"p","actor":"a"},"append":[],"settings_patch":{"temperature":{"set":"0.123456789012345678"}}}`
	var request llm.GenerateRequestV1
	if err := json.Unmarshal([]byte(input), &request); err != nil {
		t.Fatal(err)
	}
	if request.SettingsPatch.Temperature.Set == nil || request.SettingsPatch.Temperature.Set.String() != "0.123456789012345678" {
		t.Fatalf("exact decimal was not retained: %#v", request.SettingsPatch.Temperature.Set)
	}
	encoded, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(encoded, []byte(`"temperature":{"set":"0.123456789012345678"}`)) {
		t.Fatalf("exact decimal was rounded during re-encoding: %s", encoded)
	}

	trailingZeros := bytes.Replace([]byte(input), []byte("0.123456789012345678"), []byte("12.340000000000000000"), 1)
	if err := json.Unmarshal(trailingZeros, &request); err != nil {
		t.Fatal(err)
	}
	encoded, err = json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(encoded, []byte(`"temperature":{"set":"12.34"}`)) {
		t.Fatalf("decimal was not normalized on output: %s", encoded)
	}
}

func TestV1TemperatureRejectsBeyondContractPrecision(t *testing.T) {
	var request llm.GenerateRequestV1
	for _, value := range []string{
		"0.1234567890123456789",
		"1e-3",
		"01.2",
		"-0.1",
	} {
		payload := fmt.Sprintf(`{"api_version":"llm.temporal/v1","operation_key":"op","context":{"tenant":"t","project":"p","actor":"a"},"append":[],"settings_patch":{"temperature":{"set":%q}}}`, value)
		if err := json.Unmarshal([]byte(payload), &request); err == nil {
			t.Fatalf("invalid temperature %q was accepted", value)
		}
	}
}

func TestV1VariantBoundariesAndTemperature(t *testing.T) {
	zero := 0.0
	for _, variant := range []int32{0} {
		if err := llm.ValidateVariantTemperature(variant, nil); err != nil {
			t.Fatalf("variant %d: %v", variant, err)
		}
	}
	for _, variant := range []int32{1, 2, 2147483647} {
		if err := llm.ValidateVariantTemperature(variant, &zero); err == nil {
			t.Fatalf("variant %d with zero temperature accepted", variant)
		}
		if err := llm.ValidateVariantTemperature(variant, nil); err != nil {
			t.Fatalf("variant %d with inherited temperature rejected: %v", variant, err)
		}
	}
	if err := llm.ValidateVariantTemperature(-1, nil); err == nil {
		t.Fatal("negative variant accepted")
	}
	if err := llm.ValidateVariantTemperature(1, &zero); err == nil {
		t.Fatal("positive variant with zero temperature accepted")
	}
	positive := 0.2
	if err := llm.ValidateVariantTemperature(2147483647, &positive); err != nil {
		t.Fatal(err)
	}
}

func TestV1FixtureMatrixCoversForkPatchesCacheVariantsAndQueries(t *testing.T) {
	for _, name := range []string{
		"compact-request.json",
		"generate-fork-patch-set.json",
		"generate-fork-patch-clear.json",
		"generate-variant-unknown-temperature.json",
		"generate-variant-positive-temperature.json",
		"query-provider-status.json",
		"query-provider-response.json",
		"compact-request-no-cache.json",
		"query-model-inventory.json",
		"query-credit-status.json",
		"query-budget-status.json",
		"query-spend-summary.json",
		"query-model-inventory-response.json",
		"query-credit-status-response.json",
		"query-budget-status-response.json",
		"query-spend-summary-response.json",
		"generate-response-disabled-cache.json",
		"generate-response-cache-hit.json",
		"generate-response-miss-not-populated.json",
	} {
		t.Run(name, func(t *testing.T) {
			data := readV1Fixture(t, name)
			var value any
			switch {
			case strings.HasPrefix(name, "generate-"):
				if strings.HasPrefix(name, "generate-response-") {
					value = new(llm.GenerateResponseV1)
				} else {
					value = new(llm.GenerateRequestV1)
				}
			case strings.HasPrefix(name, "compact-"):
				value = new(llm.CompactRequestV1)
			case strings.HasPrefix(name, "query-") && strings.HasSuffix(name, "-response.json"):
				value = new(llm.QueryResponseV1)
			default:
				value = new(llm.QueryRequestV1)
			}
			if err := json.Unmarshal(data, value); err != nil {
				t.Fatalf("fixture decode: %v", err)
			}
			encoded, err := json.Marshal(value)
			if err != nil {
				t.Fatalf("fixture encode: %v", err)
			}
			repeated, err := json.Marshal(value)
			if err != nil {
				t.Fatalf("fixture re-encode: %v", err)
			}
			if !bytes.Equal(encoded, repeated) {
				t.Fatalf("fixture encoding was not deterministic: %s != %s", encoded, repeated)
			}
		})
	}
}

func TestV1VariantFixturesApplyMaterializedTemperatureRules(t *testing.T) {
	var unknown llm.GenerateRequestV1
	if err := json.Unmarshal(readV1Fixture(t, "generate-variant-unknown-temperature.json"), &unknown); err != nil {
		t.Fatal(err)
	}
	if err := llm.ValidateVariantTemperature(unknown.Cache.Variant, nil); err != nil {
		t.Fatalf("positive variant with inherited temperature rejected: %v", err)
	}

	var positive llm.GenerateRequestV1
	if err := json.Unmarshal(readV1Fixture(t, "generate-variant-positive-temperature.json"), &positive); err != nil {
		t.Fatal(err)
	}
	if positive.SettingsPatch.Temperature.Set == nil {
		t.Fatal("positive-temperature fixture omitted the temperature patch")
	}
	if err := llm.ValidateVariantDecimalTemperature(positive.Cache.Variant, positive.SettingsPatch.Temperature.Set); err != nil {
		t.Fatalf("positive temperature variant rejected: %v", err)
	}

	var zero llm.GenerateRequestV1
	if err := json.Unmarshal(readV1Fixture(t, "generate-variant-zero-temperature.json"), &zero); err != nil {
		t.Fatal(err)
	}
	if zero.SettingsPatch.Temperature.Set == nil {
		t.Fatal("zero-temperature fixture omitted the temperature patch")
	}
	if err := llm.ValidateVariantDecimalTemperature(zero.Cache.Variant, zero.SettingsPatch.Temperature.Set); err == nil {
		t.Fatal("positive variant with zero temperature accepted")
	}
}

func TestV1RejectsNegativeContractFixtures(t *testing.T) {
	for _, name := range []string{
		"negative-generate-cache-field.json",
		"negative-generate-compaction-checkpoint.json",
		"negative-generate-cost-enum.json",
		"negative-generate-depth-overflow.json",
		"negative-generate-diagnostics.json",
		"negative-generate-transcript.json",
		"negative-currency-field.json",
		"negative-numeric-usd.json",
		"negative-generate-cost-cross-variant.json",
		"negative-generate-enum-patch.json",
		"negative-generate-empty-reasoning-patch.json",
		"negative-generate-compaction-scalar.json",
		"negative-generate-extensions-null.json",
		"negative-generate-null-output.json",
		"negative-compact-tools.json",
		"negative-compact-structured-output.json",
		"negative-compact-positive-variant.json",
		"negative-query-mismatched-result.json",
		"negative-query-page-size.json",
		"negative-query-cursor.json",
	} {
		t.Run(name, func(t *testing.T) {
			data := readV1Fixture(t, name)
			var value any
			switch {
			case name == "negative-generate-cache-field.json", name == "negative-generate-null-append.json", name == "negative-generate-enum-patch.json", name == "negative-generate-empty-reasoning-patch.json", name == "negative-generate-compaction-scalar.json", name == "negative-generate-extensions-null.json":
				value = new(llm.GenerateRequestV1)
			case strings.HasPrefix(name, "negative-generate"),
				strings.HasPrefix(name, "negative-currency"),
				strings.HasPrefix(name, "negative-numeric"):
				value = new(llm.GenerateResponseV1)
			case strings.HasPrefix(name, "negative-compact"):
				value = new(llm.CompactRequestV1)
			default:
				if strings.Contains(name, "mismatched") {
					value = new(llm.QueryResponseV1)
				} else {
					value = new(llm.QueryRequestV1)
				}
			}
			if err := json.Unmarshal(data, value); err == nil {
				t.Fatal("negative fixture was accepted")
			}
		})
	}
}

func readV1Fixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "v1", name))
	if err != nil {
		t.Fatal(err)
	}
	return data
}
func stringPointer(value string) *string { return &value }
func mustCanonicalJSON(t *testing.T, data []byte) []byte {
	t.Helper()
	var value any
	if err := json.Unmarshal(data, &value); err != nil {
		t.Fatal(err)
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return canonical
}
