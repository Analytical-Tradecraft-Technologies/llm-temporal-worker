package schema_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/mfow/llm-temporal-worker/golang/llm"
	jsonschema "github.com/santhosh-tekuri/jsonschema/v6"
)

const parityRequestID = "llmtw_req_49c0cb63-15ed-4da7-aa74-17a7d711c9a5"

// parityParts lists every part kind and source the Go codec marshals.
func parityParts() map[string]llm.Part {
	blob := func(mediaType string) *llm.BlobRef {
		return &llm.BlobRef{Digest: "sha256:2cf24dba", ByteLength: 5, MediaType: mediaType, Locator: "blob://media/1"}
	}
	return map[string]llm.Part{
		"text":                 llm.TextPart{Text: "hello"},
		"text-empty":           llm.TextPart{},
		"image-url":            llm.ImagePart{URL: "https://example.com/a.png", MediaType: "image/png", Detail: "high"},
		"image-bytes":          llm.ImagePart{Bytes: []byte("hello"), MediaType: "image/png"},
		"image-blob":           llm.ImagePart{Blob: blob("image/png"), MediaType: "image/png"},
		"document-url":         llm.DocumentPart{URL: "https://example.com/a.pdf", MediaType: "application/pdf", Title: "Report"},
		"document-bytes":       llm.DocumentPart{Bytes: []byte("hello"), MediaType: "text/plain"},
		"document-blob":        llm.DocumentPart{Blob: blob("application/pdf"), MediaType: "application/pdf"},
		"json-object":          llm.JSONPart{Value: json.RawMessage(`{"a":[1,null,true]}`)},
		"json-null":            llm.JSONPart{Value: json.RawMessage(`null`)},
		"refusal":              llm.RefusalPart{Text: "no"},
		"refusal-code":         llm.RefusalPart{Text: "no", ProviderCode: "openai.refusal"},
		"provider-state":       llm.ProviderStatePart{Provider: "anthropic", EndpointFamily: "anthropic_messages", MediaType: "application/json", Opaque: []byte(`{"type":"thinking"}`)},
		"provider-state-empty": llm.ProviderStatePart{Provider: "anthropic", EndpointFamily: "anthropic_messages", MediaType: "application/json"},
	}
}

// parityItems lists every item kind, with each part kind carried by a message.
func parityItems() map[string]llm.Item {
	items := map[string]llm.Item{
		"message-empty":        llm.Message{Actor: llm.ActorModel},
		"tool-call":            llm.ToolCall{ID: "call-1", Name: "lookup", Arguments: json.RawMessage(`{"q":"x"}`)},
		"tool-call-scalar":     llm.ToolCall{ID: "call-1", Name: "lookup", Arguments: json.RawMessage(`"x"`)},
		"tool-result":          llm.ToolResult{CallID: "call-1", Name: "lookup", Content: []llm.Part{llm.JSONPart{Value: json.RawMessage(`{"ok":true}`)}}, IsError: true},
		"tool-result-minimal":  llm.ToolResult{CallID: "call-1"},
		"provider-state":       llm.ProviderState{Provider: "openai", EndpointFamily: "openai_responses", MediaType: "application/json", Opaque: []byte(`{"type":"reasoning"}`)},
		"provider-state-empty": llm.ProviderState{Provider: "openai", EndpointFamily: "openai_responses", MediaType: "application/json"},
		"reference":            llm.Reference{URI: "https://example.com/source"},
		"reference-metadata":   llm.Reference{URI: "urn:claims:1", Metadata: map[string]json.RawMessage{"title": json.RawMessage(`"Source"`), "rank": json.RawMessage(`1`)}},
	}
	for name, part := range parityParts() {
		items["message-"+name] = llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{part}}
		items["tool-result-"+name] = llm.ToolResult{CallID: "call-1", Content: []llm.Part{part}}
	}
	return items
}

func parityCosts() map[string]llm.CostV1 {
	amount, zero := "0.000014725", "0"
	costs := map[string]llm.CostV1{
		"provider-reported":         {Status: "exact", ActualCostUSD: &amount, Method: "provider_reported"},
		"provider-reported-catalog": {Status: "exact", ActualCostUSD: &amount, Method: "provider_reported", CatalogVersion: "prices-2026-07-13"},
		"catalog-usage":             {Status: "exact", ActualCostUSD: &amount, Method: "catalog_usage", CatalogVersion: "prices-2026-07-13"},
		"control-query-zero":        {Status: "exact", ActualCostUSD: &zero, Method: "control_query_zero"},
	}
	for _, reason := range []string{"provider_did_not_report_cost", "catalog_incomplete", "state_unavailable", "ambiguous_dispatch"} {
		costs["unknown-"+reason] = llm.CostV1{Status: "unknown", UnknownReason: reason}
	}
	return costs
}

func parityUsages() map[string]*llm.Usage {
	return map[string]*llm.Usage{
		"absent": nil,
		"zero":   {},
		"plain":  {InputTokens: 6, OutputTokens: 7, ReasoningTokens: 2, CacheReadTokens: 3, CacheWriteTokens: 1},
		"provider-raw": {InputTokens: 6, OutputTokens: 7, ProviderRaw: map[string]json.RawMessage{
			"total_tokens": json.RawMessage(`17`), "service_tier": json.RawMessage(`"default"`), "details": json.RawMessage(`{"audio_tokens":0,"nested":[1.5,null]}`),
		}},
	}
}

func parityDiagnostics() map[string][]llm.Diagnostic {
	return map[string][]llm.Diagnostic{
		"absent":             nil,
		"empty":              {},
		"plain":              {{Code: "transform_applied", Severity: llm.DiagnosticInfo, Message: "flattened"}},
		"defaulted-severity": {{Code: "transform_applied", Message: "flattened"}},
		"path":               {{Code: "unsupported", Severity: llm.DiagnosticError, Path: "/settings_patch/model", Message: "unsupported"}},
		"details":            {{Code: "route_unavailable", Severity: llm.DiagnosticWarning, Message: "Route unavailable", Details: map[string]string{"route_id": "route-primary"}}},
		"path-and-details": {
			{Code: "route_unavailable", Severity: llm.DiagnosticWarning, Path: "/append/0", Message: "Route unavailable", Details: map[string]string{"route_id": "route-primary", "attempt": "2"}},
			{Code: "transform_applied", Severity: llm.DiagnosticInfo, Message: "flattened"},
		},
	}
}

func parityCaches() map[string]llm.CacheDispositionV1 {
	age := int64(42)
	return map[string]llm.CacheDispositionV1{
		"disabled":           {Disposition: "disabled"},
		"miss-populated":     {Disposition: "miss_populated", Variant: 1},
		"miss-not-populated": {Disposition: "miss_not_populated", Variant: 2147483647},
		"hit":                {Disposition: "hit", EntryAgeSeconds: &age},
	}
}

func parityGenerateRequests() map[string]llm.GenerateRequestV1 {
	context := llm.RequestContext{Tenant: "tenant", Project: "project", Actor: "actor"}
	parent := llm.CheckpointHandle("ckp_v1.parent")
	age := int64(60)
	requests := map[string]llm.GenerateRequestV1{
		"empty-append": {OperationKey: "op", Context: context},
		"fork-with-cache": {OperationKey: "op", Context: context, Parent: &parent, Append: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "hello"}}}},
			Cache: &llm.CachePolicyV1{MaxAgeSeconds: age, Variant: 3}},
	}
	var all []llm.Item
	for name, item := range parityItems() {
		requests["append-"+name] = llm.GenerateRequestV1{OperationKey: "op", Context: context, Append: []llm.Item{item}}
		all = append(all, item)
	}
	requests["append-every-item"] = llm.GenerateRequestV1{OperationKey: "op", Context: context, Parent: &parent, Append: all}
	for name, part := range parityParts() {
		instructions := []llm.Instruction{{Kind: llm.InstructionKindParts, Level: llm.InstructionLevelApplication, Content: []llm.Part{part}}}
		requests["instruction-"+name] = llm.GenerateRequestV1{OperationKey: "op", Context: context, SettingsPatch: llm.SettingsPatchV1{Instructions: llm.Patch[[]llm.Instruction]{Set: &instructions}}}
	}
	return requests
}

func parityGenerateResponses() map[string]llm.GenerateResponseV1 {
	parent := llm.CheckpointHandle("ckp_v1.parent")
	amount := "0.5"
	base := llm.GenerateResponseV1{OperationKey: "op", OperationID: "op-id", Status: llm.ResponseStatusCompleted,
		Checkpoint: llm.CheckpointMetadata{Handle: "ckp_v1.child", Kind: "generation"}, Cache: llm.CacheDispositionV1{Disposition: "disabled"},
		Cost: llm.CostV1{Status: "exact", ActualCostUSD: &amount, Method: "provider_reported"}}
	responses := map[string]llm.GenerateResponseV1{"minimal": base}
	var all []llm.Item
	for name, item := range parityItems() {
		response := base
		response.Output = []llm.Item{item}
		responses["output-"+name] = response
		all = append(all, item)
	}
	everything := base
	everything.Output = all
	responses["output-every-item"] = everything
	for _, status := range []llm.ResponseStatus{llm.ResponseStatusCompleted, llm.ResponseStatusToolCalls, llm.ResponseStatusRefused, llm.ResponseStatusLength, llm.ResponseStatusContentFiltered} {
		response := base
		response.Status = status
		responses["status-"+string(status)] = response
	}
	for name, checkpoint := range map[string]llm.CheckpointMetadata{
		"generation-child":  {Handle: "ckp_v1.child", Parent: &parent, Kind: "generation", Depth: 7},
		"cache-replay-root": {Handle: "ckp_v1.child", Kind: "cache_replay"},
		"cache-replay":      {Handle: "ckp_v1.child", Parent: &parent, Kind: "cache_replay", Depth: 2147483647},
	} {
		response := base
		response.Checkpoint = checkpoint
		responses["checkpoint-"+name] = response
	}
	for name, cache := range parityCaches() {
		response := base
		response.Cache = cache
		responses["cache-"+name] = response
	}
	for name, route := range map[string]*llm.RouteFacts{
		"empty":   {},
		"partial": {RouteID: "route", ResolvedModel: "model-1"},
		"full":    {RouteID: "route", EndpointID: "endpoint", APIFamily: "openai_responses", RequestedModel: "model", ResolvedModel: "model-1"},
	} {
		response := base
		response.Route = route
		responses["route-"+name] = response
	}
	for name, usage := range parityUsages() {
		response := base
		response.Usage = usage
		responses["usage-"+name] = response
	}
	for name, cost := range parityCosts() {
		response := base
		response.Cost = cost
		responses["cost-"+name] = response
	}
	for name, diagnostics := range parityDiagnostics() {
		response := base
		response.Diagnostics = diagnostics
		responses["diagnostics-"+name] = response
	}
	return responses
}

func parityCompactResponses() map[string]llm.CompactResponseV1 {
	parent := llm.CheckpointHandle("ckp_v1.parent")
	amount := "0.5"
	base := llm.CompactResponseV1{OperationKey: "op", OperationID: "op-id",
		Checkpoint: llm.CheckpointMetadata{Handle: "ckp_v1.compact", Parent: &parent, Kind: "compaction", Depth: 1}, Cache: llm.CacheDispositionV1{Disposition: "disabled"},
		Cost: llm.CostV1{Status: "exact", ActualCostUSD: &amount, Method: "provider_reported"}}
	responses := map[string]llm.CompactResponseV1{"minimal": base}
	// The first three are the shapes checkpoint publication emits.
	for name, provenance := range map[string]string{
		"provider":     `{"source":"provider","policy_version":"policy-v1","prompt_version":"prompt-v1"}`,
		"worker-cache": `{"source":"worker_cache","policy_version":"policy-v1","prompt_version":"prompt-v1"}`,
		"no-work":      `{"source":"no_work","policy_version":"policy-v1","prompt_version":"prompt-v1"}`,
		"source-only":  `{"source":"provider"}`,
		"full":         `{"source":"worker_cache","origin_operation_id":"op-origin","policy":"default","policy_version":"policy-v1","prompt_version":"prompt-v1"}`,
	} {
		response := base
		response.Provenance = json.RawMessage(provenance)
		responses["provenance-"+name] = response
	}
	for name, cache := range parityCaches() {
		response := base
		response.Cache = cache
		responses["cache-"+name] = response
	}
	for name, usage := range parityUsages() {
		response := base
		response.Usage = usage
		responses["usage-"+name] = response
	}
	for name, cost := range parityCosts() {
		response := base
		response.Cost = cost
		responses["cost-"+name] = response
	}
	for name, diagnostics := range parityDiagnostics() {
		response := base
		response.Diagnostics = diagnostics
		responses["diagnostics-"+name] = response
	}
	return responses
}

// assertCodecRecordMatchesSchema marshals value with the Go codec, validates
// the bytes against the published schema and checks that the Go decoder
// reproduces the same bytes.
func assertCodecRecordMatchesSchema[T any](t *testing.T, validate func([]byte) error, value T) {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := validate(encoded); err != nil {
		t.Fatalf("published schema rejects the Go-marshalled record: %v\n%s", err, encoded)
	}
	var decoded T
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("Go decoder rejects its own record: %v\n%s", err, encoded)
	}
	again, err := json.Marshal(decoded)
	if err != nil {
		t.Fatalf("re-marshal: %v", err)
	}
	if !bytes.Equal(encoded, again) {
		t.Fatalf("record changed on round trip\n got %s\nwant %s", again, encoded)
	}
}

// TestV1CodecMatrixMatchesPublishedSchemas keeps golang/api/schema/v1 in step
// with what the Go codec marshals: every item kind, part kind, cost variant,
// usage shape, diagnostic shape and checkpoint, cache and provenance variant.
func TestV1CodecMatrixMatchesPublishedSchemas(t *testing.T) {
	generateRequest := readV1Schema(t, "generate-request.schema.json").Validate
	generateResponse := readV1Schema(t, "generate-response.schema.json").Validate
	compactRequest := readV1Schema(t, "compact-request.schema.json").Validate
	compactResponse := readV1Schema(t, "compact-response.schema.json").Validate
	prepare := compileV1ExecutionSchema(t, "prepare-execution.schema.json")
	result := compileV1ExecutionSchema(t, "execution-result.schema.json")

	for name, request := range parityGenerateRequests() {
		t.Run("generate-request/"+name, func(t *testing.T) {
			assertCodecRecordMatchesSchema(t, generateRequest, request)
			assertCodecRecordMatchesSchema(t, prepare, llm.PrepareExecutionV1{Generate: &request})
		})
	}
	for name, response := range parityGenerateResponses() {
		t.Run("generate-response/"+name, func(t *testing.T) {
			assertCodecRecordMatchesSchema(t, generateResponse, response)
			assertCodecRecordMatchesSchema(t, result, llm.ExecutionResultV1{RequestID: parityRequestID, Kind: "generate", State: llm.ExecutionCompleted, Generate: &response})
		})
	}
	context := llm.RequestContext{Tenant: "tenant", Project: "project", Actor: "actor"}
	age := int64(60)
	for name, request := range map[string]llm.CompactRequestV1{
		"minimal": {OperationKey: "op", Context: context, Parent: "ckp_v1.parent"},
		"policy":  {OperationKey: "op", Context: context, Parent: "ckp_v1.parent", Policy: json.RawMessage(`{"target_tokens":100,"summary_style":"concise"}`)},
		"cache":   {OperationKey: "op", Context: context, Parent: "ckp_v1.parent", Cache: &llm.CachePolicyV1{MaxAgeSeconds: age, Variant: 3}},
	} {
		t.Run("compact-request/"+name, func(t *testing.T) {
			assertCodecRecordMatchesSchema(t, compactRequest, request)
			assertCodecRecordMatchesSchema(t, prepare, llm.PrepareExecutionV1{Compact: &request})
		})
	}
	for name, response := range parityCompactResponses() {
		t.Run("compact-response/"+name, func(t *testing.T) {
			assertCodecRecordMatchesSchema(t, compactResponse, response)
			assertCodecRecordMatchesSchema(t, result, llm.ExecutionResultV1{RequestID: parityRequestID, Kind: "compact", State: llm.ExecutionCompleted, Compact: &response})
		})
	}
}

// v1FixtureContract maps a positive fixture name to its schema and Go type.
func v1FixtureContract(name string) (string, func() any) {
	switch {
	case strings.HasPrefix(name, "generate-response"):
		return "generate-response.schema.json", func() any { return new(llm.GenerateResponseV1) }
	case strings.HasPrefix(name, "compact-response"):
		return "compact-response.schema.json", func() any { return new(llm.CompactResponseV1) }
	case strings.HasPrefix(name, "generate-"):
		return "generate-request.schema.json", func() any { return new(llm.GenerateRequestV1) }
	case strings.HasPrefix(name, "compact-"):
		return "compact-request.schema.json", func() any { return new(llm.CompactRequestV1) }
	case strings.HasPrefix(name, "query-") && strings.HasSuffix(name, "-response.json"):
		return "query-response.schema.json", func() any { return new(llm.QueryResponseV1) }
	case strings.HasPrefix(name, "query-"):
		return "query-request.schema.json", func() any { return new(llm.QueryRequestV1) }
	}
	return "", nil
}

// TestV1PositiveFixturesMatchSchemaAndCodec is the reverse direction: every
// positive fixture is schema-valid, decodes with the Go codec, and the record
// the Go codec writes back is schema-valid as well.
func TestV1PositiveFixturesMatchSchemaAndCodec(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join("..", "testdata", "v1", "*.json"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("no fixtures: %v", err)
	}
	for _, path := range paths {
		name := filepath.Base(path)
		if strings.HasPrefix(name, "negative-") {
			continue
		}
		t.Run(name, func(t *testing.T) {
			schemaName, target := v1FixtureContract(name)
			if schemaName == "" {
				t.Fatal("fixture name does not identify a v1 contract")
			}
			compiled := readV1Schema(t, schemaName)
			fixture := readV1Fixture(t, name)
			if err := compiled.Validate(fixture); err != nil {
				t.Fatalf("schema rejects fixture: %v", err)
			}
			value := target()
			if err := json.Unmarshal(fixture, value); err != nil {
				t.Fatalf("Go codec rejects schema-valid fixture: %v", err)
			}
			encoded, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			if err := compiled.Validate(encoded); err != nil {
				t.Fatalf("schema rejects the re-marshalled fixture: %v\n%s", err, encoded)
			}
		})
	}
}

// TestV1OCamlFixturesMirrorGoFixtures pins the copies the OCaml codec tests
// decode to the Go fixtures validated above, so both codecs read one corpus.
func TestV1OCamlFixturesMirrorGoFixtures(t *testing.T) {
	directory := filepath.Join("..", "..", "..", "ocaml", "llm_temporal_worker", "test", "fixtures", "v1")
	paths, err := filepath.Glob(filepath.Join(directory, "*.json"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("no OCaml v1 fixtures: %v", err)
	}
	for _, path := range paths {
		mirror, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(mirror, readV1Fixture(t, filepath.Base(path))) {
			t.Fatalf("%s differs from golang/llm/testdata/v1/%s", path, filepath.Base(path))
		}
	}
}

// TestV1SchemasShareDefinitions fails when a definition that the request and
// response schemas both publish is edited in only one of them.
func TestV1SchemasShareDefinitions(t *testing.T) {
	definitions := func(name string) map[string]any {
		t.Helper()
		data, err := os.ReadFile(filepath.Join("..", "..", "api", "schema", "v1", name))
		if err != nil {
			t.Fatal(err)
		}
		var document struct {
			Defs map[string]any `json:"$defs"`
		}
		if err := json.Unmarshal(data, &document); err != nil {
			t.Fatal(err)
		}
		return document.Defs
	}
	for _, test := range []struct {
		left, right string
		names       []string
	}{
		{"generate-request.schema.json", "generate-response.schema.json", []string{"item", "part", "media_type", "media_url", "blob", "opaque"}},
		{"generate-response.schema.json", "compact-response.schema.json", []string{"cache_disposition", "usage", "cost", "diagnostic"}},
		{"generate-request.schema.json", "compact-request.schema.json", []string{"context"}},
	} {
		left, right := definitions(test.left), definitions(test.right)
		for _, name := range test.names {
			if left[name] == nil || !reflect.DeepEqual(left[name], right[name]) {
				t.Errorf("$defs/%s differs between %s and %s", name, test.left, test.right)
			}
		}
	}
}

// compileV1ExecutionSchema compiles a schema that references the request and
// response schemas by $id, which schema.Parse deliberately does not resolve.
func compileV1ExecutionSchema(t *testing.T, name string) func([]byte) error {
	t.Helper()
	compiler := jsonschema.NewCompiler()
	var target string
	for _, resource := range []string{"generate-request", "compact-request", "generate-response", "compact-response", "prepare-execution", "execution-result"} {
		data, err := os.ReadFile(filepath.Join("..", "..", "api", "schema", "v1", resource+".schema.json"))
		if err != nil {
			t.Fatal(err)
		}
		document, err := jsonschema.UnmarshalJSON(bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		id := document.(map[string]any)["$id"].(string)
		if err := compiler.AddResource(id, document); err != nil {
			t.Fatal(err)
		}
		if resource+".schema.json" == name {
			target = id
		}
	}
	compiled, err := compiler.Compile(target)
	if err != nil {
		t.Fatal(err)
	}
	return func(instance []byte) error {
		value, err := jsonschema.UnmarshalJSON(bytes.NewReader(instance))
		if err != nil {
			return err
		}
		return compiled.Validate(value)
	}
}
