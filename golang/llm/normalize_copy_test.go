package llm

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// assertNormalizeMatchesRoundTrip proves NormalizeRequest equal to the JSON
// round trip that defines it: the same value (reflect.DeepEqual, so nil and
// empty collections must match too) or the same error. wantCopy states
// whether the typed copy, rather than the round-trip fallback, must handle
// the request, so a regression to the slow path is caught as well.
func assertNormalizeMatchesRoundTrip(t *testing.T, name string, request Request, wantCopy bool) {
	t.Helper()
	want, wantErr := normalizeRequestRoundTrip(request)
	got, gotErr := NormalizeRequest(request)
	if (wantErr == nil) != (gotErr == nil) || (wantErr != nil && wantErr.Error() != gotErr.Error()) {
		t.Fatalf("%s: error = %v, round trip error = %v", name, gotErr, wantErr)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s: NormalizeRequest differs from round trip:\n got: %#v\nwant: %#v", name, got, want)
	}
	copied, ok := normalizeRequestCopy(request)
	if ok != wantCopy {
		t.Fatalf("%s: typed copy handled = %v, want %v (round trip error %v)", name, ok, wantCopy, wantErr)
	}
	if ok {
		if wantErr != nil {
			t.Fatalf("%s: typed copy accepted a request the round trip rejects: %v", name, wantErr)
		}
		if !reflect.DeepEqual(copied, want) {
			t.Fatalf("%s: typed copy differs from round trip:\n got: %#v\nwant: %#v", name, copied, want)
		}
	}
}

func normalizeTestPointer[T any](value T) *T { return &value }

// normalizeFullRequest sets every request field and uses every item and part
// type, in forms the typed copy handles.
func normalizeFullRequest() Request {
	sydney := time.FixedZone("AEST", 10*3600)
	return Request{
		WebSearch:             true,
		WebFetch:              true,
		CodeExecution:         true,
		OperationKey:          "operation",
		Context:               RequestContext{Tenant: "tenant", Project: "project", Actor: "actor", Tags: map[string]string{"a": "b", "": "empty key", "ünïcode": "välue"}},
		Model:                 "model",
		ServiceClass:          ServiceClassPriority,
		ServiceClassFallbacks: []ServiceClass{ServiceClassStandard, ServiceClassEconomy},
		Portability:           PortabilityBestEffort,
		Instructions: []Instruction{
			{Text: "plain"},
			{Kind: InstructionKindText, Level: InstructionLevelPolicy, Text: "policy"},
			{Kind: InstructionKindText, Content: []Part{TextPart{Text: "from content"}}},
			{Kind: InstructionKindText, Text: "same", Content: []Part{TextPart{Text: "same"}}},
			{Content: []Part{TextPart{Text: "implicit parts"}, JSONPart{Value: json.RawMessage(`{"k":[1,2.5,"s",null,true]}`)}}},
			{Kind: InstructionKindParts, Text: "dropped by the round trip", Content: []Part{TextPart{Text: "x"}}},
			{Kind: InstructionKindParts},
		},
		Input: []Item{
			Message{Actor: ActorHuman, Content: []Part{
				TextPart{Text: "hello \"quoted\" \\ \t\n\u0000 snowman \u2603"},
				TextPart{},
				ImagePart{URL: "https://example.com/a.png", MediaType: "image/png", Detail: "high"},
				ImagePart{Bytes: []byte{1, 2, 3}, MediaType: "image/jpeg"},
				ImagePart{Blob: &BlobRef{Digest: "sha256:abc", ByteLength: 10, MediaType: "image/webp", Locator: "blob://x"}, MediaType: "image/webp"},
				DocumentPart{URL: "https://example.com/a.pdf", MediaType: "application/pdf", Title: "title"},
				DocumentPart{Bytes: []byte("%PDF"), MediaType: "application/pdf"},
				DocumentPart{Blob: &BlobRef{Digest: "sha256:def", MediaType: "text/plain", Locator: "blob://y"}, MediaType: "text/plain"},
				JSONPart{Value: json.RawMessage(`"escaped \u003c \" \\ \u2028"`)},
				JSONPart{Value: json.RawMessage(`-1.5e10`)},
				RefusalPart{Text: "no", ProviderCode: "policy"},
				RefusalPart{},
				ProviderStatePart{Provider: "openai", EndpointFamily: "responses", MediaType: "application/octet-stream", Opaque: []byte{0, 255}},
				ProviderStatePart{Provider: "openai", EndpointFamily: "responses", MediaType: "application/octet-stream", Opaque: []byte{}},
				ProviderStatePart{Provider: "openai", EndpointFamily: "responses", MediaType: "application/octet-stream"},
			}},
			Message{Actor: ActorModel},
			Message{Actor: ActorModel, Content: []Part{}},
			ToolCall{ID: "call-1", Name: "lookup", Arguments: json.RawMessage(`{"q":"x","n":{"deep":[{}]}}`)},
			ToolCall{ID: "call-2", Name: "lookup", Arguments: json.RawMessage(`[]`)},
			ToolResult{CallID: "call-1", Name: "lookup", Content: []Part{TextPart{Text: "result"}}, IsError: true},
			ToolResult{CallID: "call-2"},
			ProviderState{Provider: "anthropic", EndpointFamily: "messages", MediaType: "application/json", Opaque: []byte(`{"x":1}`)},
			ProviderState{Provider: "anthropic", EndpointFamily: "messages", MediaType: "application/json"},
			Reference{URI: "https://example.com/doc", Metadata: map[string]json.RawMessage{"k": json.RawMessage(`{"v":1}`), "n": json.RawMessage(`null`)}},
			Reference{URI: "s3://bucket/key", Metadata: map[string]json.RawMessage{}},
			Reference{URI: "urn:x"},
		},
		Tools: []Tool{
			{Name: "lookup", Description: "find", InputSchema: json.RawMessage(`{"type":"object"}`)},
			{Kind: ToolKindFunction, Name: "explicit", InputSchema: json.RawMessage(`{}`), OutputSchema: json.RawMessage(`{"type":"string"}`)},
			{Kind: ToolKindProvider, Name: "web_search", InputSchema: json.RawMessage(`{}`), OutputSchema: json.RawMessage{}},
			{Kind: ToolKindRemoteMCP, Name: "mcp", InputSchema: json.RawMessage(`{"a":{"b":{"c":[]}}}`)},
		},
		ToolPolicy: ToolPolicy{Mode: ToolChoiceNamed, Name: "lookup", Parallel: true},
		Output:     &OutputSpec{MaxTokens: normalizeTestPointer(0), Format: OutputFormat{Kind: OutputKindJSONSchema, Name: "result-1", Description: "desc", Strict: true, Schema: json.RawMessage(`{"type":"object"}`)}},
		Sampling: &SamplingSpec{Temperature: normalizeTestPointer(0.7), TopP: normalizeTestPointer(math.SmallestNonzeroFloat64), TopK: normalizeTestPointer(math.MaxInt64),
			Seed: normalizeTestPointer(int64(math.MinInt64)), PresencePenalty: normalizeTestPointer(math.Copysign(0, -1)), FrequencyPenalty: normalizeTestPointer(1e308), StopSequences: []string{"stop", ""}},
		Reasoning:    &ReasoningSpec{Mode: ReasoningModeEnabled, Effort: ReasoningEffortHigh, TokenBudget: normalizeTestPointer(1024), Summary: ReasoningSummaryAuto},
		Continuation: &Continuation{Handle: "handle", EndpointID: "endpoint", Model: "model", ExpiresAt: normalizeTestPointer(time.Date(2026, 1, 2, 3, 4, 5, 6, sydney)), Pinned: true, ProviderStates: []ProviderState{{Provider: "p", EndpointFamily: "f", MediaType: "m", Opaque: []byte{9}}, {Provider: "p", EndpointFamily: "f", MediaType: "m"}}},
		Extensions:   map[string]json.RawMessage{"openrouter": json.RawMessage(`{"provider_order":["B","A"]}`), "": json.RawMessage(`0`), "s": json.RawMessage(`"x"`)},
	}
}

func TestNormalizeRequestCopyMatchesRoundTrip(t *testing.T) {
	full := normalizeFullRequest()
	assertNormalizeMatchesRoundTrip(t, "full", full, true)
	assertNormalizeMatchesRoundTrip(t, "minimal", Request{OperationKey: "op", Model: "m"}, true)

	// Each case changes one field of the full request. copy reports whether
	// the typed copy must handle the result; every other case must fall back
	// to the round trip, which then defines the value or the error.
	cases := []struct {
		name   string
		copy   bool
		mutate func(*Request)
	}{
		// nil versus empty collections and defaults.
		{"api version explicit", true, func(r *Request) { r.APIVersion = APIVersion }},
		{"api version unsupported", false, func(r *Request) { r.APIVersion = "v0" }},
		{"empty collections", true, func(r *Request) {
			r.ServiceClassFallbacks, r.Instructions, r.Input, r.Tools, r.Extensions = []ServiceClass{}, []Instruction{}, []Item{}, []Tool{}, map[string]json.RawMessage{}
		}},
		{"nil collections", true, func(r *Request) {
			r.ServiceClassFallbacks, r.Instructions, r.Input, r.Tools, r.Extensions = nil, nil, nil, nil, nil
		}},
		{"default service class and portability", true, func(r *Request) {
			r.ServiceClass, r.Portability, r.ServiceClassFallbacks = "", "", []ServiceClass{ServiceClassPriority}
		}},
		{"context empty with empty tags", true, func(r *Request) { r.Context = RequestContext{Tags: map[string]string{}} }},
		{"context with empty tags", true, func(r *Request) { r.Context.Tags = map[string]string{} }},
		{"context tags only", true, func(r *Request) { r.Context = RequestContext{Tags: map[string]string{"k": "v"}} }},
		{"tool policy default", true, func(r *Request) { r.ToolPolicy = ToolPolicy{} }},
		{"tool policy none parallel", true, func(r *Request) { r.ToolPolicy = ToolPolicy{Mode: ToolChoiceNone, Parallel: true} }},
		{"no optional specs", true, func(r *Request) { r.Output, r.Sampling, r.Reasoning, r.Continuation = nil, nil, nil, nil }},
		{"empty optional specs", true, func(r *Request) {
			r.Output, r.Sampling, r.Reasoning = &OutputSpec{}, &SamplingSpec{}, &ReasoningSpec{}
		}},
		{"output json", true, func(r *Request) { r.Output = &OutputSpec{Format: OutputFormat{Kind: OutputKindJSON}} }},
		{"output empty stop sequences", true, func(r *Request) { r.Sampling.StopSequences = []string{} }},
		{"continuation minimal", true, func(r *Request) { r.Continuation = &Continuation{Handle: "h"} }},
		{"continuation empty provider states", true, func(r *Request) { r.Continuation.ProviderStates = []ProviderState{} }},
		{"continuation monotonic local expiry", true, func(r *Request) { r.Continuation.ExpiresAt = normalizeTestPointer(time.Now()) }},
		{"continuation utc expiry", true, func(r *Request) {
			r.Continuation.ExpiresAt = normalizeTestPointer(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC))
		}},
		{"continuation expiry outside RFC 3339", false, func(r *Request) {
			r.Continuation.ExpiresAt = normalizeTestPointer(time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC))
		}},
		{"continuation without handle", false, func(r *Request) { r.Continuation.Handle = "" }},

		// Raw JSON the encoder rewrites: the round trip compacts it and
		// escapes HTML characters, which the copy reproduces. Raw JSON the
		// round trip rejects or that is invalid UTF-8 takes the fallback.
		{"extension whitespace", true, func(r *Request) { r.Extensions["openrouter"] = json.RawMessage(" { \"a\" : 1 }\n") }},
		{"extension html", true, func(r *Request) { r.Extensions["openrouter"] = json.RawMessage(`"<b>&</b>"`) }},
		{"extension line separator", true, func(r *Request) { r.Extensions["openrouter"] = json.RawMessage("\"\u2028\"") }},
		{"extension mixed rewrites", true, func(r *Request) {
			r.Extensions["openrouter"] = json.RawMessage("\t{ \"<k>\" : [ \"a b\\\" & \\\\\" , \"\u2029\u2028\" ,{ } ] ,\r\n\"x\":\"\\u003c\" }  ")
		}},
		{"extension nil", false, func(r *Request) { r.Extensions["openrouter"] = nil }},
		{"extension invalid", false, func(r *Request) { r.Extensions["openrouter"] = json.RawMessage(`{`) }},
		{"extension duplicate keys", false, func(r *Request) { r.Extensions["openrouter"] = json.RawMessage(`{"a":1,"a":2}`) }},
		{"extension invalid utf-8 key", false, func(r *Request) { r.Extensions["\xff"] = json.RawMessage(`1`) }},
		{"extension invalid utf-8 value", false, func(r *Request) { r.Extensions["openrouter"] = json.RawMessage("\"\xff\"") }},
		{"extension deep nesting", false, func(r *Request) {
			r.Extensions["openrouter"] = json.RawMessage(strings.Repeat("[", maxStableRawDepth+1) + strings.Repeat("]", maxStableRawDepth+1))
		}},
		{"tool call arguments whitespace", true, func(r *Request) {
			r.Input[3] = ToolCall{ID: "call-1", Name: "lookup", Arguments: json.RawMessage(`{"q": 1}`)}
		}},
		{"tool call arguments empty", false, func(r *Request) { r.Input[3] = ToolCall{ID: "call-1", Name: "lookup"} }},
		{"json part html", true, func(r *Request) {
			r.Input[0].(Message).Content[8] = JSONPart{Value: json.RawMessage(`"a>b"`)}
		}},
		{"tool schema whitespace", true, func(r *Request) { r.Tools[0].InputSchema = json.RawMessage(`{ }`) }},
		{"tool schema not object", false, func(r *Request) { r.Tools[0].InputSchema = json.RawMessage(`[]`) }},
		{"tool output schema not object", false, func(r *Request) { r.Tools[1].OutputSchema = json.RawMessage(`1`) }},
		{"reference metadata whitespace", true, func(r *Request) {
			r.Input[9] = Reference{URI: "https://example.com", Metadata: map[string]json.RawMessage{"k": json.RawMessage(`[1, 2]`)}}
		}},
		{"reference metadata empty key", false, func(r *Request) {
			r.Input[9] = Reference{URI: "https://example.com", Metadata: map[string]json.RawMessage{"": json.RawMessage(`1`)}}
		}},
		{"output schema whitespace", true, func(r *Request) { r.Output.Format.Schema = json.RawMessage("{\n}") }},

		// Strings encoding/json rewrites.
		{"invalid utf-8 text", false, func(r *Request) { r.Input[0].(Message).Content[0] = TextPart{Text: "bad \xff"} }},
		{"invalid utf-8 tag", false, func(r *Request) { r.Context.Tags["k"] = "\xfe" }},
		{"invalid utf-8 instruction", false, func(r *Request) { r.Instructions[0].Text = "\xc3" }},
		{"invalid utf-8 model", false, func(r *Request) { r.Model = "\xff" }},

		// Values that are not the decoders' concrete value types.
		{"pointer item", false, func(r *Request) { r.Input[1] = &Message{Actor: ActorHuman} }},
		{"pointer part", false, func(r *Request) { r.Input[0].(Message).Content[0] = &TextPart{Text: "x"} }},
		{"nil item", false, func(r *Request) { r.Input[1] = nil }},
		{"nil part", false, func(r *Request) { r.Input[0].(Message).Content[0] = nil }},
		{"pointer instruction text part", false, func(r *Request) {
			r.Instructions[2].Content = []Part{&TextPart{Text: "from content"}}
		}},

		// Values the round trip rejects.
		{"missing operation key", false, func(r *Request) { r.OperationKey = "" }},
		{"missing model", false, func(r *Request) { r.Model = "" }},
		{"invalid service class", false, func(r *Request) { r.ServiceClass = "gold" }},
		{"fallback repeats class", false, func(r *Request) { r.ServiceClassFallbacks = []ServiceClass{ServiceClassPriority} }},
		{"invalid portability", false, func(r *Request) { r.Portability = "loose" }},
		{"invalid instruction kind", false, func(r *Request) { r.Instructions[0].Kind = "image" }},
		{"invalid instruction level", false, func(r *Request) { r.Instructions[0].Level = "system" }},
		{"text instruction conflict", false, func(r *Request) { r.Instructions[3].Text = "other" }},
		{"text instruction with two parts", false, func(r *Request) {
			r.Instructions[2].Content = []Part{TextPart{Text: "a"}, TextPart{Text: "b"}}
		}},
		{"text instruction with json part", false, func(r *Request) {
			r.Instructions[2].Content = []Part{JSONPart{Value: json.RawMessage(`1`)}}
		}},
		{"invalid actor", false, func(r *Request) { r.Input[1] = Message{Actor: "system"} }},
		{"tool call without id", false, func(r *Request) { r.Input[3] = ToolCall{Name: "lookup", Arguments: json.RawMessage(`{}`)} }},
		{"tool call bad name", false, func(r *Request) { r.Input[3] = ToolCall{ID: "c", Name: "bad name", Arguments: json.RawMessage(`{}`)} }},
		{"tool result without call id", false, func(r *Request) { r.Input[5] = ToolResult{} }},
		{"provider state incomplete", false, func(r *Request) { r.Input[7] = ProviderState{Provider: "p"} }},
		{"reference without scheme", false, func(r *Request) { r.Input[9] = Reference{URI: "no-scheme"} }},
		{"reference javascript", false, func(r *Request) { r.Input[9] = Reference{URI: "javascript:alert(1)"} }},
		{"image two sources", false, func(r *Request) {
			r.Input[0].(Message).Content[2] = ImagePart{URL: "https://x", Bytes: []byte{1}, MediaType: "image/png"}
		}},
		{"image empty bytes", false, func(r *Request) {
			r.Input[0].(Message).Content[2] = ImagePart{Bytes: []byte{}, MediaType: "image/png"}
		}},
		{"image without media type", false, func(r *Request) { r.Input[0].(Message).Content[2] = ImagePart{URL: "https://x"} }},
		{"document blob media mismatch", false, func(r *Request) {
			r.Input[0].(Message).Content[7] = DocumentPart{Blob: &BlobRef{Digest: "d", MediaType: "a/b", Locator: "l"}, MediaType: "c/d"}
		}},
		{"blob negative length", false, func(r *Request) {
			r.Input[0].(Message).Content[4] = ImagePart{Blob: &BlobRef{Digest: "d", ByteLength: -1, MediaType: "a/b", Locator: "l"}, MediaType: "a/b"}
		}},
		{"blob without locator", false, func(r *Request) {
			r.Input[0].(Message).Content[4] = ImagePart{Blob: &BlobRef{Digest: "d", MediaType: "a/b"}, MediaType: "a/b"}
		}},
		{"provider state part incomplete", false, func(r *Request) { r.Input[0].(Message).Content[12] = ProviderStatePart{} }},
		{"invalid tool kind", false, func(r *Request) { r.Tools[0].Kind = "builtin" }},
		{"invalid tool name", false, func(r *Request) { r.Tools[0].Name = "" }},
		{"tool policy name without named mode", false, func(r *Request) { r.ToolPolicy = ToolPolicy{Mode: ToolChoiceAuto, Name: "lookup"} }},
		{"tool policy named without name", false, func(r *Request) { r.ToolPolicy = ToolPolicy{Mode: ToolChoiceNamed} }},
		{"invalid tool policy mode", false, func(r *Request) { r.ToolPolicy = ToolPolicy{Mode: "any"} }},
		{"negative max tokens", false, func(r *Request) { r.Output.MaxTokens = normalizeTestPointer(-1) }},
		{"invalid output kind", false, func(r *Request) { r.Output.Format.Kind = "xml" }},
		{"invalid output name", false, func(r *Request) { r.Output.Format.Name = "bad name" }},
		{"json schema without schema", false, func(r *Request) { r.Output.Format.Schema = nil }},
		{"text output with schema", false, func(r *Request) { r.Output.Format.Kind = OutputKindText }},
		{"text output strict", false, func(r *Request) { r.Output.Format = OutputFormat{Strict: true} }},
		{"nan temperature", false, func(r *Request) { r.Sampling.Temperature = normalizeTestPointer(math.NaN()) }},
		{"infinite penalty", false, func(r *Request) { r.Sampling.FrequencyPenalty = normalizeTestPointer(math.Inf(-1)) }},
		{"invalid reasoning mode", false, func(r *Request) { r.Reasoning.Mode = "always" }},
		{"negative reasoning budget", false, func(r *Request) { r.Reasoning.TokenBudget = normalizeTestPointer(-5) }},
		{"continuation provider state incomplete", false, func(r *Request) { r.Continuation.ProviderStates = []ProviderState{{}} }},
	}
	for _, test := range cases {
		request := normalizeFullRequest()
		test.mutate(&request)
		assertNormalizeMatchesRoundTrip(t, test.name, request, test.copy)
	}
}

// TestNormalizeRequestCopyMatchesRoundTripForFixtures covers every decoded
// request fixture, which are exactly the values the round trip produces.
func TestNormalizeRequestCopyMatchesRoundTripForFixtures(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join("testdata", "request", "*.json"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("fixtures: %v %v", paths, err)
	}
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var request Request
		if err := json.Unmarshal(data, &request); err != nil {
			t.Fatal(err)
		}
		assertNormalizeMatchesRoundTrip(t, path, request, true)
		normalized, err := NormalizeRequest(request)
		if err != nil {
			t.Fatal(err)
		}
		assertNormalizeMatchesRoundTrip(t, path+" normalized", normalized, true)
	}
}

// TestNormalizeRequestCopyMatchesRoundTripRandomized combines the values of
// every field, valid and invalid, at random.
func TestNormalizeRequestCopyMatchesRoundTripRandomized(t *testing.T) {
	random := rand.New(rand.NewSource(1112))
	pick := func(values ...string) string { return values[random.Intn(len(values))] }
	raws := []json.RawMessage{nil, {}, json.RawMessage(`{}`), json.RawMessage(`[]`), json.RawMessage(`1`), json.RawMessage(`"s"`), json.RawMessage(`{"a":[1,{"b":null}]}`),
		json.RawMessage(` {}`), json.RawMessage(`{"a":1,"a":1}`), json.RawMessage(`"<"`), json.RawMessage(`{"x":">"}`), json.RawMessage("\"\xff\""), json.RawMessage(`{`)}
	raw := func() json.RawMessage { return raws[random.Intn(len(raws))] }
	text := func() string { return pick("", "text", "ünï", "\xff", "<&>", "a\u2028b") }
	var part func() Part
	part = func() Part {
		var bytes []byte
		switch random.Intn(3) {
		case 1:
			bytes = []byte{}
		case 2:
			bytes = []byte{1}
		}
		var blob *BlobRef
		if random.Intn(3) == 0 {
			blob = &BlobRef{Digest: pick("", "d"), ByteLength: int64(random.Intn(3) - 1), MediaType: pick("", "a/b"), Locator: pick("", "l")}
		}
		switch random.Intn(9) {
		case 0:
			return TextPart{Text: text()}
		case 1:
			return ImagePart{URL: pick("", "https://x", "bad"), Bytes: bytes, Blob: blob, MediaType: pick("", "a/b", "c/d"), Detail: text()}
		case 2:
			return DocumentPart{URL: pick("", "https://x"), Bytes: bytes, Blob: blob, MediaType: pick("a/b", "c/d"), Title: text()}
		case 3:
			return JSONPart{Value: raw()}
		case 4:
			return RefusalPart{Text: text(), ProviderCode: text()}
		case 5:
			return ProviderStatePart{Provider: pick("", "p"), EndpointFamily: "f", MediaType: "m", Opaque: bytes}
		case 6:
			return &TextPart{Text: "pointer"}
		case 7:
			return nil
		default:
			return TextPart{Text: "x"}
		}
	}
	parts := func() []Part {
		if random.Intn(4) == 0 {
			return nil
		}
		result := []Part{}
		for n := random.Intn(4); n > 0; n-- {
			result = append(result, part())
		}
		return result
	}
	item := func() Item {
		switch random.Intn(8) {
		case 0:
			return Message{Actor: Actor(pick("human", "model", "")), Content: parts()}
		case 1:
			return ToolCall{ID: pick("", "c"), Name: pick("tool", "bad name"), Arguments: raw()}
		case 2:
			return ToolResult{CallID: pick("", "c"), Name: text(), Content: parts(), IsError: random.Intn(2) == 0}
		case 3:
			return ProviderState{Provider: "p", EndpointFamily: pick("", "f"), MediaType: "m", Opaque: []byte(pick("", "x"))}
		case 4:
			reference := Reference{URI: pick("https://x", "x", "data:x")}
			if random.Intn(2) == 0 {
				reference.Metadata = map[string]json.RawMessage{pick("", "k"): raw()}
			}
			return reference
		case 5:
			return &ToolCall{ID: "c", Name: "tool", Arguments: json.RawMessage(`{}`)}
		default:
			return Message{Actor: ActorHuman, Content: []Part{TextPart{Text: "hi"}}}
		}
	}
	for iteration := 0; iteration < 20000; iteration++ {
		request := normalizeFullRequest()
		switch random.Intn(12) {
		case 0:
			request.Input = append(request.Input, item())
		case 1:
			request.Input = []Item{item(), item()}
		case 2:
			request.Instructions = append(request.Instructions, Instruction{Kind: InstructionKind(pick("", "text", "parts", "x")), Level: InstructionLevel(pick("", "policy", "x")), Text: text(), Content: parts()})
		case 3:
			request.Tools = append(request.Tools, Tool{Kind: ToolKind(pick("", "function", "provider", "x")), Name: pick("t", ""), Description: text(), InputSchema: raw(), OutputSchema: raw()})
		case 4:
			request.Extensions[text()] = raw()
		case 5:
			request.Output = &OutputSpec{Format: OutputFormat{Kind: OutputKind(pick("", "text", "json", "json_schema")), Name: pick("", "n", "bad name"), Description: text(), Strict: random.Intn(2) == 0, Schema: raw()}}
		case 6:
			request.Context = RequestContext{Tenant: text(), Tags: map[string]string{text(): text()}}
		case 7:
			request.ToolPolicy = ToolPolicy{Mode: ToolChoiceMode(pick("", "none", "auto", "required", "named", "x")), Name: pick("", "t", "bad name")}
		case 8:
			request.ServiceClass = ServiceClass(pick("", "economy", "standard", "priority"))
			request.ServiceClassFallbacks = []ServiceClass{ServiceClass(pick("economy", "standard", "priority"))}
		case 9:
			request.Continuation = &Continuation{Handle: pick("", "h"), ProviderStates: []ProviderState{{Provider: "p", EndpointFamily: "f", MediaType: pick("", "m")}}}
		case 10:
			request.Sampling = &SamplingSpec{Temperature: normalizeTestPointer([]float64{0, 1, math.NaN(), math.Inf(1), -0.0, 1e-300}[random.Intn(6)])}
		default:
			request.Input[0] = Message{Actor: ActorHuman, Content: parts()}
		}
		want, wantErr := normalizeRequestRoundTrip(request)
		got, gotErr := NormalizeRequest(request)
		if (wantErr == nil) != (gotErr == nil) || (wantErr != nil && wantErr.Error() != gotErr.Error()) || !reflect.DeepEqual(got, want) {
			t.Fatalf("iteration %d: NormalizeRequest = %#v, %v; round trip = %#v, %v", iteration, got, gotErr, want, wantErr)
		}
		if copied, ok := normalizeRequestCopy(request); ok && (wantErr != nil || !reflect.DeepEqual(copied, want)) {
			t.Fatalf("iteration %d: typed copy = %#v; round trip = %#v, %v", iteration, copied, want, wantErr)
		}
	}
}

// TestNormalizeRequestCopyIsIndependent mutates every reference-typed value
// of the input after normalizing it.
func TestNormalizeRequestCopyIsIndependent(t *testing.T) {
	request := normalizeFullRequest()
	normalized, err := NormalizeRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	before := fmt.Sprintf("%#v", normalized)
	want, _ := normalizeRequestRoundTrip(normalizeFullRequest())
	request.ServiceClassFallbacks[0] = ServiceClassEconomy
	request.Context.Tags["a"] = "changed"
	request.Instructions[4].Content[0] = TextPart{Text: "changed"}
	message := request.Input[0].(Message)
	message.Content[0] = TextPart{Text: "changed"}
	message.Content[3].(ImagePart).Bytes[0] = 99
	message.Content[4].(ImagePart).Blob.Digest = "changed"
	message.Content[8].(JSONPart).Value[1] = 'X'
	message.Content[12].(ProviderStatePart).Opaque[0] = 99
	request.Input[3].(ToolCall).Arguments[2] = 'X'
	request.Input[7].(ProviderState).Opaque[0] = 'X'
	request.Input[9].(Reference).Metadata["k"][1] = 'X'
	request.Tools[0].InputSchema[1] = 'X'
	request.Tools[1].OutputSchema[1] = 'X'
	*request.Output.MaxTokens = 7
	request.Output.Format.Schema[1] = 'X'
	*request.Sampling.Temperature = 9
	request.Sampling.StopSequences[0] = "changed"
	*request.Reasoning.TokenBudget = 9
	*request.Continuation.ExpiresAt = time.Time{}
	request.Continuation.ProviderStates[0].Opaque[0] = 0
	request.Extensions["openrouter"][1] = 'X'
	request.Extensions["new"] = json.RawMessage(`1`)
	if after := fmt.Sprintf("%#v", normalized); after != before || !reflect.DeepEqual(normalized, want) {
		t.Fatalf("normalized request aliases its input:\nbefore: %s\nafter:  %s", before, after)
	}
}

// FuzzMarshaledRawJSON checks the raw JSON rewrite against encoding/json
// itself: whenever marshaledRawJSON accepts a value, json.Marshal must emit
// exactly those bytes for it.
func FuzzMarshaledRawJSON(f *testing.F) {
	escapedLess := `"\` + `u003c"`
	for _, seed := range []string{`{}`, ` [ 1 , 2 ] `, `"<&>"`, "\"\xe2\x80\xa8\xe2\x80\xa9\"", `{"a":"b\"<"}`, "{\"k\" :\n\"\\\\\"}", escapedLess, `1e5`, `{"a":1,"a":2}`, `{`, "\"\xff\""} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		got, ok := marshaledRawJSON(data)
		if !ok {
			return
		}
		want, err := json.Marshal(json.RawMessage(data))
		if err != nil || string(got) != string(want) {
			t.Fatalf("marshaledRawJSON(%q) = %q; json.Marshal = %q, %v", data, got, want, err)
		}
	})
}

func BenchmarkNormalizeRequestLargeTranscript(b *testing.B) {
	request := Request{OperationKey: "op", Model: "model"}
	for i := 0; i < 10; i++ {
		request.Input = append(request.Input, Message{Actor: ActorHuman, Content: []Part{TextPart{Text: strings.Repeat(fmt.Sprintf("turn %d ", i), 100<<10/7)}}})
	}
	for name, normalize := range map[string]func(Request) (Request, error){"copy": NormalizeRequest, "roundtrip": normalizeRequestRoundTrip} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := normalize(request); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
