package llm_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/internal/testassert"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
)

func TestV1GenerateAndCompactFixturesRoundTrip(t *testing.T) {
	for _, name := range []string{"generate-root.json", "generate-delta.json", "generate-cache-any-age.json"} {
		request := testassert.UnmarshalJSON[llm.GenerateRequestV1](t, readV1Fixture(t, name), name)
		encoded, err := json.Marshal(request)
		if err != nil {
			t.Fatalf("%s marshal: %v", name, err)
		}
		testassert.UnmarshalJSON[llm.GenerateRequestV1](t, encoded, name+" second decode")
		if string(encoded) != string(mustCanonicalJSON(t, encoded)) {
			t.Fatalf("%s was not deterministic", name)
		}
	}
	testassert.MarshalJSON(t, testassert.UnmarshalJSON[llm.GenerateResponseV1](t, readV1Fixture(t, "generate-response.json"), "generate-response.json"))
	testassert.MarshalJSON(t, testassert.UnmarshalJSON[llm.CompactResponseV1](t, readV1Fixture(t, "compact-response.json"), "compact-response.json"))
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
			testassert.NoError(t, err, "marshal")
			target := test.target()
			testassert.NoError(t, json.Unmarshal(encoded, target), "unmarshal")
			got := test.getContext(target)
			if got.Tenant != context.Tenant || got.Project != context.Project || got.Actor != context.Actor || len(got.Tags) != 0 {
				t.Fatalf("context = %#v, want %#v with no tags", got, context)
			}
		})
	}
}

func TestCompactV1RequestContextRejectsMissingOrEmptyMembers(t *testing.T) {
	tests := []struct {
		name    string
		context string
	}{
		{name: "missing tenant", context: `{"project":"project","actor":"actor"}`},
		{name: "empty tenant", context: `{"tenant":"","project":"project","actor":"actor"}`},
		{name: "missing project", context: `{"tenant":"tenant","actor":"actor"}`},
		{name: "empty project", context: `{"tenant":"tenant","project":"","actor":"actor"}`},
		{name: "missing actor", context: `{"tenant":"tenant","project":"project"}`},
		{name: "empty actor", context: `{"tenant":"tenant","project":"project","actor":""}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			payload := fmt.Sprintf(`{"api_version":"llm.temporal/compact/v1","operation_key":"compact","context":%s,"parent":"ckp_v1.parent"}`, test.context)
			var request llm.CompactRequestV1
			err := json.Unmarshal([]byte(payload), &request)
			if err == nil {
				t.Fatal("incomplete compact context was accepted")
			}
			if message := err.Error(); !strings.Contains(message, "context") || !strings.Contains(message, "tenant, project, and actor") {
				t.Fatalf("error = %q, want clear complete context rejection", message)
			}
		})
	}
}

// The summarizer model/service class overrides round-trip, reject empty or
// unknown values, and are set by a Generate's automatic compaction only from
// its settings_patch (#1348).
func TestCompactV1SummarizerOverrides(t *testing.T) {
	const prefix = `{"api_version":"llm.temporal/compact/v1","operation_key":"compact","context":{"tenant":"tenant","project":"project","actor":"actor"},"parent":"ckp_v1.parent"`
	request := testassert.UnmarshalJSON[llm.CompactRequestV1](t, []byte(prefix+`,"model":"alias2","service_class":"priority"}`), "decode overrides")
	if request.Model != "alias2" || request.ServiceClass != llm.ServiceClassPriority {
		t.Fatalf("decoded overrides = %q/%q", request.Model, request.ServiceClass)
	}
	encoded, err := request.MarshalJSON()
	if err != nil || !strings.Contains(string(encoded), `"model":"alias2"`) || !strings.Contains(string(encoded), `"service_class":"priority"`) {
		t.Fatalf("encoded = %s, %v", encoded, err)
	}
	long := strings.Repeat("é", llm.MaxCompactModelLengthV1)
	testassert.UnmarshalJSON[llm.CompactRequestV1](t, []byte(prefix+`,"model":"`+long+`"}`), fmt.Sprintf("a %d code point model was rejected", llm.MaxCompactModelLengthV1))
	for _, field := range []string{`"model":""`, `"model":1`, `"model":"` + long + `x"`, `"service_class":"turbo"`} {
		requireUnmarshalRejected[llm.CompactRequestV1](t, []byte(prefix+","+field+"}"), "%s was accepted", field)
	}
	request.ServiceClass = "turbo"
	if _, err := request.MarshalJSON(); err == nil {
		t.Fatal("invalid service class override was encoded")
	}
	request.ServiceClass, request.Model = "", long+"x"
	if _, err := request.MarshalJSON(); err == nil {
		t.Fatal("overlong model override was encoded")
	}

	parent := llm.CheckpointHandle("ckp_v1.parent")
	generate := llm.GenerateRequestV1{OperationKey: "generate", Context: request.Context, Parent: &parent}
	plain := llm.GenerateCompaction(generate, "compact")
	if plain.Model != "" || plain.ServiceClass != "" || plain.Parent != parent || plain.Cache == nil {
		t.Fatalf("unpatched compaction = %+v", plain)
	}
	model, class := "alias2", llm.ServiceClassEconomy
	generate.SettingsPatch.Model.Set, generate.SettingsPatch.ServiceClass.Set = &model, &class
	patched := llm.GenerateCompaction(generate, "compact")
	if patched.Model != model || patched.ServiceClass != class {
		t.Fatalf("patched compaction = %+v", patched)
	}
	legacy, _ := patched.ParentRoutedCompaction().MarshalJSON()
	want, _ := plain.MarshalJSON()
	if !bytes.Equal(legacy, want) {
		t.Fatalf("parent-routed form = %s, want %s", legacy, want)
	}
}

func TestV1RejectsUnknownTranscriptAndMismatchedQueryResult(t *testing.T) {
	requireUnmarshalRejected[llm.GenerateRequestV1](t, readV1Fixture(t, "negative-unknown-field.json"), "unknown transcript field was accepted")
	query := llm.QueryResponseV1{OperationKey: "q", QueryExecutionID: "id", Kind: llm.QueryProviderStatus, Result: llm.SpendSummary{}, Cost: llm.CostV1{Status: "exact", ActualCostUSD: stringPointer("0"), Method: "control_query_zero"}}
	if _, err := json.Marshal(query); err == nil {
		t.Fatal("mismatched query result was accepted")
	}
	requireUnmarshalRejected[llm.QueryRequestV1](t, queryRequestPayload("provider_status", `{"page_size":1001}`), "oversized query page accepted")
	requireUnmarshalRejected[llm.QueryRequestV1](t, queryRequestPayload("provider_status", `{"unknown":true}`), "unknown query field accepted")
	requireUnmarshalRejected[llm.QueryRequestV1](t, []byte(`{"api_version":"llm.temporal/query/v1","operation_key":"q","context":{"tenant":"t","project":"p"},"kind":"provider_status","query":{}}`), "incomplete query context accepted")
}

func TestV1CostCanonicalizationAndVariantFields(t *testing.T) {
	value := testassert.MarshalJSON(t, llm.CostV1{Status: "exact", ActualCostUSD: stringPointer("10.500000000000"), Method: "provider_reported"})
	requireEncodedContains(t, value, `"actual_cost_usd":"10.5"`, "cost was not canonicalized")
	cost := testassert.UnmarshalJSON[llm.CostV1](t, []byte(`{"status":"exact","actual_cost_usd":"0.1000","method":"provider_reported"}`), "decode cost")
	requireEncodedContains(t, testassert.MarshalJSON(t, cost), `"actual_cost_usd":"0.1"`, "round-trip cost was not canonicalized")
	if _, err := json.Marshal(llm.CostV1{Status: "exact", ActualCostUSD: stringPointer("0"), Method: "estimate"}); err == nil {
		t.Fatal("unknown exact cost method accepted")
	}
	if _, err := json.Marshal(llm.CostV1{Status: "unknown", UnknownReason: "state_unavailable", CatalogVersion: "catalog-1"}); err == nil {
		t.Fatal("unknown cost with catalog version accepted")
	}
	request := llm.GenerateRequestV1{OperationKey: "nil-arrays", Context: llm.RequestContext{Tenant: "t", Project: "p", Actor: "a"}}
	requireEncodedContains(t, testassert.MarshalJSON(t, request), `"append":[]`, "nil append was not normalized")
	response := llm.GenerateResponseV1{OperationKey: "nil-output", OperationID: "id", Status: llm.ResponseStatusCompleted, Checkpoint: llm.CheckpointMetadata{Handle: "ckp_v1.nil", Kind: "generation", Depth: 0}, Cache: llm.CacheDispositionV1{Disposition: "disabled"}, Cost: llm.CostV1{Status: "exact", ActualCostUSD: stringPointer("0"), Method: "provider_reported"}}
	requireEncodedContains(t, testassert.MarshalJSON(t, response), `"output":[]`, "nil output was not normalized")
}

func TestQueryContractRejectsUnknownEnumsAndMalformedTimes(t *testing.T) {
	for _, test := range []struct {
		name  string
		query string
	}{
		{name: "availability", query: `{"availability":"invalid"}`},
		{name: "lifecycle", query: `{"lifecycle":"future"}`},
		{name: "missing spend interval", query: `{}`},
		{name: "malformed spend interval", query: `{"start_time":"not-a-time","end_time":"2026-07-19T00:00:00Z"}`},
		{name: "empty spend interval", query: `{"start_time":"2026-07-19T00:00:00Z","end_time":"2026-07-19T00:00:00Z"}`},
		{name: "inverted spend interval", query: `{"start_time":"2026-07-20T00:00:00Z","end_time":"2026-07-19T00:00:00Z"}`},
		{name: "unknown group", query: `{"start_time":"2026-07-18T00:00:00Z","end_time":"2026-07-19T00:00:00Z","group_by":["region"]}`},
		{name: "duplicate operation kind", query: `{"start_time":"2026-07-18T00:00:00Z","end_time":"2026-07-19T00:00:00Z","operation_kinds":["query","query"]}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			kind := "spend_summary"
			switch test.name {
			case "availability":
				kind = "provider_status"
			case "lifecycle":
				kind = "model_inventory"
			}
			requireUnmarshalRejected[llm.QueryRequestV1](t, queryRequestPayload(kind, test.query), "invalid query %s was accepted", test.name)
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
			requireUnmarshalRejected[llm.QueryRequestV1](t, queryRequestPayload(test.kind, test.query), "query with null optional string was accepted: %s", test.query)
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
			testassert.NoError(t, testassert.Err(json.Marshal(incompleteWithCursor)), "incomplete response with a cursor was rejected")
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

func TestQueryResponseV1RequiresExplicitActualCost(t *testing.T) {
	unknownPayload := func(actualCost string) []byte {
		return []byte(fmt.Sprintf(`{"api_version":"llm.temporal/query/v1","operation_key":"q","query_execution_id":"query-id","kind":"provider_status","observed_at":"2026-07-19T00:00:00Z","source":"persisted","freshness":"current","complete":true,"result":{"routes":[]},"cost_status":"unknown"%s,"cost_unknown_reason_code":"state_unavailable"}`, actualCost))
	}

	t.Run("missing unknown actual cost", func(t *testing.T) {
		var response llm.QueryResponseV1
		err := json.Unmarshal(unknownPayload(""), &response)
		testassert.ErrorContains(t, err, `missing required JSON field "actual_cost_usd"`)
	})

	t.Run("explicit null unknown actual cost", func(t *testing.T) {
		testassert.UnmarshalJSON[llm.QueryResponseV1](t, unknownPayload(`,"actual_cost_usd":null`), "explicit null actual_cost_usd was rejected")
	})

	t.Run("unknown marshal emits null actual cost", func(t *testing.T) {
		response := validQueryResponse(llm.QueryProviderStatus)
		response.Cost = llm.CostV1{Status: "unknown", UnknownReason: "state_unavailable"}
		requireEncodedContains(t, testassert.MarshalJSON(t, response), `"actual_cost_usd":null`, "unknown query response omitted explicit null actual cost")
	})

	t.Run("exact actual cost remains canonical", func(t *testing.T) {
		payload := []byte(`{"api_version":"llm.temporal/query/v1","operation_key":"q","query_execution_id":"query-id","kind":"provider_status","observed_at":"2026-07-19T00:00:00Z","source":"persisted","freshness":"current","complete":true,"result":{"routes":[]},"cost_status":"exact","actual_cost_usd":"0.1000","cost_method":"provider_reported"}`)
		response := testassert.UnmarshalJSON[llm.QueryResponseV1](t, payload, "exact actual cost was rejected")
		requireEncodedContains(t, testassert.MarshalJSON(t, response), `"actual_cost_usd":"0.1"`, "exact actual cost was not canonicalized")
	})
}

// queryRequestPayload is a v1 query request of kind with the given query JSON.
func queryRequestPayload(kind, query string) []byte {
	return []byte(fmt.Sprintf(`{"api_version":"llm.temporal/query/v1","operation_key":"q","context":{"tenant":"t","project":"p","actor":"a"},"kind":%q,"query":%s}`, kind, query))
}

// queryResponsePayload is a complete, persisted, zero-cost v1 query response
// of kind with the given result JSON.
func queryResponsePayload(kind, result string) []byte {
	return []byte(fmt.Sprintf(`{"api_version":"llm.temporal/query/v1","operation_key":"q","query_execution_id":"query-id","kind":%q,"observed_at":"2026-07-19T00:00:00Z","source":"persisted","freshness":"current","complete":true,"result":%s,"cost_status":"exact","actual_cost_usd":"0","cost_method":"control_query_zero"}`, kind, result))
}

func TestQueryResultBoundaryRejectsOpenNestedRows(t *testing.T) {
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
			requireUnmarshalRejected[llm.QueryResponseV1](t, queryResponsePayload("provider_status", test.result), "invalid query result %s was accepted", test.name)
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
			requireUnmarshalRejected[llm.QueryResponseV1](t, queryResponsePayload(test.kind, test.result), "invalid query result %s was accepted", test.name)
		})
	}
}

func TestV1SettingsPatchAndResponseMetadataUseWireDecoders(t *testing.T) {
	requestData := []byte(`{"api_version":"llm.temporal/v1","operation_key":"op","context":{"tenant":"t","project":"p","actor":"a"},"append":[],"settings_patch":{"instructions":{"set":[{"kind":"parts","content":[{"kind":"text","text":"hello"}]}]},"tools":{"set":[{"name":"lookup","description":"lookup data","input_schema":{"type":"object"}}]},"output":{"set":{"max_tokens":32,"format":{"kind":"text"}}}}}`)
	request := testassert.UnmarshalJSON[llm.GenerateRequestV1](t, requestData, "decode settings patch")
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
	decimalRequest := testassert.UnmarshalJSON[llm.GenerateRequestV1](t, canonicalTemperature, "decimal temperature was not decoded")
	requireEncodedContains(t, testassert.MarshalJSON(t, decimalRequest), `"temperature":{"set":"0.7"}`, "temperature was not canonically re-encoded as a string")
	legacyNumeric := bytes.Replace(canonicalTemperature, []byte(`"0.7000"`), []byte(`0.7`), 1)
	testassert.UnmarshalJSON[llm.GenerateRequestV1](t, legacyNumeric, "legacy numeric temperature was not accepted during compatibility window")
	invalidDecimal := bytes.Replace(canonicalTemperature, []byte(`"0.7000"`), []byte(`"1.0000000000000000001"`), 1)
	requireUnmarshalRejected[llm.GenerateRequestV1](t, invalidDecimal, "temperature with excessive precision was accepted")

	envelope := testassert.UnmarshalJSON[map[string]json.RawMessage](t, readV1Fixture(t, "generate-response.json"), "decode generate-response.json")
	envelope["route"] = json.RawMessage(`{"route_id":"route-1","endpoint_id":"endpoint-1","api_family":"responses","requested_model":"gpt-test","resolved_model":"gpt-test-2026"}`)
	envelope["usage"] = json.RawMessage(`{"input_tokens":10,"output_tokens":20,"reasoning_tokens":3,"cache_read_tokens":4,"cache_write_tokens":5}`)
	response := testassert.UnmarshalJSON[llm.GenerateResponseV1](t, testassert.MarshalJSON(t, envelope), "decode response metadata")
	if response.Route == nil || response.Route.RouteID != "route-1" || response.Route.ResolvedModel != "gpt-test-2026" {
		t.Fatalf("route metadata was not decoded: %#v", response.Route)
	}
	if response.Usage == nil || response.Usage.InputTokens != 10 || response.Usage.CacheWriteTokens != 5 {
		t.Fatalf("usage metadata was not decoded: %#v", response.Usage)
	}
}

func TestV1TemperaturePreservesAllEighteenFractionalDigits(t *testing.T) {
	const input = `{"api_version":"llm.temporal/v1","operation_key":"op","context":{"tenant":"t","project":"p","actor":"a"},"append":[],"settings_patch":{"temperature":{"set":"0.123456789012345678"}}}`
	request := testassert.UnmarshalJSON[llm.GenerateRequestV1](t, []byte(input), "decode exact decimal")
	if request.SettingsPatch.Temperature.Set == nil || request.SettingsPatch.Temperature.Set.String() != "0.123456789012345678" {
		t.Fatalf("exact decimal was not retained: %#v", request.SettingsPatch.Temperature.Set)
	}
	requireEncodedContains(t, testassert.MarshalJSON(t, request), `"temperature":{"set":"0.123456789012345678"}`, "exact decimal was rounded during re-encoding")

	trailingZeros := bytes.Replace([]byte(input), []byte("0.123456789012345678"), []byte("12.340000000000000000"), 1)
	request = testassert.UnmarshalJSON[llm.GenerateRequestV1](t, trailingZeros, "decode trailing zeros")
	requireEncodedContains(t, testassert.MarshalJSON(t, request), `"temperature":{"set":"12.34"}`, "decimal was not normalized on output")
}

func TestV1TemperatureRejectsBeyondContractPrecision(t *testing.T) {
	for _, value := range []string{
		"0.1234567890123456789",
		"1e-3",
		"01.2",
		"-0.1",
	} {
		payload := fmt.Sprintf(`{"api_version":"llm.temporal/v1","operation_key":"op","context":{"tenant":"t","project":"p","actor":"a"},"append":[],"settings_patch":{"temperature":{"set":%q}}}`, value)
		requireUnmarshalRejected[llm.GenerateRequestV1](t, []byte(payload), "invalid temperature %q was accepted", value)
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
		if err := llm.ValidateVariantTemperature(variant, &zero); err != nil {
			t.Fatalf("variant %d with zero temperature rejected", variant)
		}
		if err := llm.ValidateVariantTemperature(variant, nil); err != nil {
			t.Fatalf("variant %d with inherited temperature rejected: %v", variant, err)
		}
	}
	if err := llm.ValidateVariantTemperature(-1, nil); err == nil {
		t.Fatal("negative variant accepted")
	}
	if err := llm.ValidateVariantTemperature(1, &zero); err != nil {
		t.Fatal("positive variant with zero temperature rejected")
	}
	positive := 0.2
	testassert.NoError(t, llm.ValidateVariantTemperature(2147483647, &positive))
}

func TestV1FixtureMatrixCoversForkPatchesCacheVariantsAndQueries(t *testing.T) {
	for _, name := range []string{
		"compact-request.json",
		"compact-positive-variant.json",
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
			testassert.NoError(t, json.Unmarshal(data, value), "fixture decode")
			encoded, err := json.Marshal(value)
			testassert.NoError(t, err, "fixture encode")
			repeated, err := json.Marshal(value)
			testassert.NoError(t, err, "fixture re-encode")
			if !bytes.Equal(encoded, repeated) {
				t.Fatalf("fixture encoding was not deterministic: %s != %s", encoded, repeated)
			}
		})
	}
}

func TestV1VariantFixturesApplyMaterializedTemperatureRules(t *testing.T) {
	unknown := readV1GenerateFixture(t, "generate-variant-unknown-temperature.json")
	testassert.NoError(t, llm.ValidateVariantTemperature(unknown.Cache.Variant, nil), "positive variant with inherited temperature rejected")

	positive := readV1GenerateFixture(t, "generate-variant-positive-temperature.json")
	if positive.SettingsPatch.Temperature.Set == nil {
		t.Fatal("positive-temperature fixture omitted the temperature patch")
	}
	testassert.NoError(t, llm.ValidateVariantDecimalTemperature(positive.Cache.Variant, positive.SettingsPatch.Temperature.Set), "positive temperature variant rejected")

	zero := readV1GenerateFixture(t, "generate-variant-zero-temperature.json")
	if zero.SettingsPatch.Temperature.Set == nil {
		t.Fatal("zero-temperature fixture omitted the temperature patch")
	}
	if err := llm.ValidateVariantDecimalTemperature(zero.Cache.Variant, zero.SettingsPatch.Temperature.Set); err != nil {
		t.Fatal("positive variant with zero temperature rejected")
	}
}

// TestV1RejectsNegativeContractFixtures rejects every negative fixture with
// the decoder of the contract testdata/v1/negative-fixtures.tsv names for it,
// and requires the error to name the corrupted field. The OCaml corpus test
// reads the same manifest, so neither side can send a request fixture to a
// response decoder that rejects it for an unrelated reason (#1678).
func TestV1RejectsNegativeContractFixtures(t *testing.T) {
	manifest := readNegativeFixtureManifest(t)
	paths, err := filepath.Glob(filepath.Join("testdata", "v1", "negative-*.json"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("no negative fixtures: %v", err)
	}
	for _, path := range paths {
		if _, ok := manifest[filepath.Base(path)]; !ok {
			t.Errorf("%s is not listed in testdata/v1/negative-fixtures.tsv", filepath.Base(path))
		}
	}
	decoders := map[string]func() any{
		"generate-request":  func() any { return new(llm.GenerateRequestV1) },
		"generate-response": func() any { return new(llm.GenerateResponseV1) },
		"compact-request":   func() any { return new(llm.CompactRequestV1) },
		"query-request":     func() any { return new(llm.QueryRequestV1) },
		"query-response":    func() any { return new(llm.QueryResponseV1) },
	}
	for name, entry := range manifest {
		t.Run(name, func(t *testing.T) {
			decoder, ok := decoders[entry.contract]
			if !ok {
				t.Fatalf("unknown contract %q", entry.contract)
			}
			err := json.Unmarshal(readV1Fixture(t, name), decoder())
			testassert.ErrorContains(t, err, entry.field, "rejection")
		})
	}
}

type negativeFixture struct{ contract, field string }

// readNegativeFixtureManifest parses negative-fixtures.tsv: one
// fixture<TAB>contract<TAB>field line per negative fixture, # comments.
func readNegativeFixtureManifest(t *testing.T) map[string]negativeFixture {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "v1", "negative-fixtures.tsv"))
	testassert.NoError(t, err)
	manifest := map[string]negativeFixture{}
	for number, line := range strings.Split(string(data), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		columns := strings.Split(line, "\t")
		if len(columns) != 3 || columns[2] == "" {
			t.Fatalf("negative-fixtures.tsv line %d is not fixture<TAB>contract<TAB>field", number+1)
		}
		if _, duplicate := manifest[columns[0]]; duplicate {
			t.Fatalf("negative-fixtures.tsv lists %s twice", columns[0])
		}
		if _, err := os.Stat(filepath.Join("testdata", "v1", columns[0])); err != nil {
			t.Fatalf("negative-fixtures.tsv lists missing fixture %s", columns[0])
		}
		manifest[columns[0]] = negativeFixture{contract: columns[1], field: columns[2]}
	}
	return manifest
}

func readV1Fixture(t *testing.T, name string) []byte {
	t.Helper()
	return testassert.ReadFile(t, filepath.Join("testdata", "v1", name))
}

// readV1GenerateFixture decodes the named testdata/v1 Generate request.
func readV1GenerateFixture(t *testing.T, name string) llm.GenerateRequestV1 {
	t.Helper()
	return testassert.UnmarshalJSON[llm.GenerateRequestV1](t, readV1Fixture(t, name), name)
}

func stringPointer(value string) *string { return &value }
func mustCanonicalJSON(t *testing.T, data []byte) []byte {
	t.Helper()
	return testassert.MarshalJSON(t, testassert.UnmarshalJSON[any](t, data, "decode canonical JSON"))
}

func TestCacheAgeOmissionIsUnrestrictedButZeroAndNullAreInvalid(t *testing.T) {
	var policy llm.CachePolicyV1
	if err := json.Unmarshal([]byte(`{}`), &policy); err != nil || policy.MaxAgeSeconds != 0 {
		t.Fatalf("omitted age: %v", err)
	}
	encoded, err := json.Marshal(policy)
	if err != nil || string(encoded) != `{}` {
		t.Fatalf("unrestricted encoding: %s %v", encoded, err)
	}
	for _, input := range []string{`{"max_age_seconds":0}`, `{"max_age_seconds":null}`, `{"max_age_seconds":-1}`, `{"max_age_seconds":31536001}`} {
		requireUnmarshalRejected[llm.CachePolicyV1](t, []byte(input), "accepted %s", input)
	}
}

func TestV1CompactResponseUsagePreservedAndValidated(t *testing.T) {
	fields := testassert.UnmarshalJSON[map[string]json.RawMessage](t, readV1Fixture(t, "compact-response.json"), "decode compact-response.json")
	valid := `{"input_tokens":9007199254740993,"output_tokens":12,"reasoning_tokens":3,"cache_read_tokens":4,"cache_write_tokens":5,"provider_raw":{"tokens":9007199254740993}}`
	fields["usage"] = json.RawMessage(valid)
	response := testassert.UnmarshalJSON[llm.CompactResponseV1](t, testassert.MarshalJSON(t, fields), "decode valid usage")
	if response.Usage == nil || response.Usage.InputTokens != 9007199254740993 || response.Usage.OutputTokens != 12 || response.Usage.ReasoningTokens != 3 || response.Usage.CacheReadTokens != 4 || response.Usage.CacheWriteTokens != 5 || string(response.Usage.ProviderRaw["tokens"]) != "9007199254740993" {
		t.Fatalf("usage lost on decode: %+v", response.Usage)
	}
	again := testassert.UnmarshalJSON[llm.CompactResponseV1](t, testassert.MarshalJSON(t, response), "decode round-tripped usage")
	first, _ := json.Marshal(response.Usage)
	second, _ := json.Marshal(again.Usage)
	if !bytes.Equal(first, second) {
		t.Fatal("usage changed on round trip")
	}
	for _, invalid := range []string{`{"input_tokens":-1}`, `{"output_tokens":1.5}`, `{"reasoning_tokens":9223372036854775808}`, `{"unknown":1}`, `null`} {
		fields["usage"] = json.RawMessage(invalid)
		data, _ := json.Marshal(fields)
		requireUnmarshalRejected[llm.CompactResponseV1](t, data, "accepted invalid usage %s", invalid)
	}
}
