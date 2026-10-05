package llm

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// The fast decode paths must accept, reject and report exactly what the
// encoding/json Decoder paths do. referenceJSONDecoding switches every one of
// them off, which is the decoding this package performed before the paths
// existed, so each test here decodes the same bytes both ways and compares.

type decodeOutcome struct {
	value   any
	err     string
	encoded string
}

// differentialDecoders are the exported entry points that accept untrusted
// bytes. Each returns the decoded value so the comparison covers what was
// decoded and not only whether it was accepted.
var differentialDecoders = map[string]func([]byte) (any, error){
	"Request":              unmarshalInto[Request],
	"Response":             unmarshalInto[Response],
	"GenerateRequestV1":    unmarshalInto[GenerateRequestV1],
	"GenerateResponseV1":   unmarshalInto[GenerateResponseV1],
	"CompactRequestV1":     unmarshalInto[CompactRequestV1],
	"CompactResponseV1":    unmarshalInto[CompactResponseV1],
	"QueryRequestV1":       unmarshalInto[QueryRequestV1],
	"QueryResponseV1":      unmarshalInto[QueryResponseV1],
	"PrepareExecutionV1":   unmarshalInto[PrepareExecutionV1],
	"ExecutionReferenceV1": unmarshalInto[ExecutionReferenceV1],
	"ExecutionResultV1":    unmarshalInto[ExecutionResultV1],
	"SettingsPatchV1":      unmarshalInto[SettingsPatchV1],
	"CachePolicyV1":        unmarshalInto[CachePolicyV1],
	"GenerationPlanV1":     unmarshalInto[GenerationPlanV1],
	"Cost":                 unmarshalInto[Cost],
	"DecodeItems": func(data []byte) (any, error) {
		return DecodeItems(data)
	},
	"CanonicalJSON": func(data []byte) (any, error) {
		return CanonicalJSON(data)
	},
	"decodeObject": func(data []byte) (any, error) {
		return decodeObject(data)
	},
	"rejectDuplicateJSONKeys": func(data []byte) (any, error) {
		return nil, rejectDuplicateJSONKeys(data)
	},
}

func unmarshalInto[T any](data []byte) (any, error) {
	var value T
	if err := json.Unmarshal(data, &value); err != nil {
		return nil, err
	}
	return value, nil
}

func decodeBothWays(t testing.TB, decode func([]byte) (any, error), data []byte) (fast, reference decodeOutcome) {
	t.Helper()
	run := func() decodeOutcome {
		var outcome decodeOutcome
		// A decoder that retained part of its input would be changed by
		// overwriting the input after the call.
		input := bytes.Clone(data)
		value, err := decode(input)
		for index := range input {
			input[index] = '#'
		}
		if err != nil {
			outcome.err = err.Error()
			return outcome
		}
		outcome.value = value
		encoded, err := json.Marshal(value)
		if err != nil {
			outcome.encoded = "marshal error: " + err.Error()
		} else {
			outcome.encoded = string(encoded)
		}
		return outcome
	}
	if referenceJSONDecoding {
		t.Fatal("reference decoding is already enabled")
	}
	fast = run()
	referenceJSONDecoding = true
	defer func() { referenceJSONDecoding = false }()
	reference = run()
	return fast, reference
}

// sameOutcome compares two outcomes. A document with several unknown fields
// reports whichever one map iteration reaches first, so those messages are
// compared up to the field name.
func sameOutcome(fast, reference decodeOutcome) bool {
	if fast.err != reference.err {
		const unknown = "unknown JSON field "
		fastIndex, referenceIndex := strings.Index(fast.err, unknown), strings.Index(reference.err, unknown)
		return fastIndex >= 0 && referenceIndex >= 0 && fast.err[:fastIndex] == reference.err[:referenceIndex]
	}
	return fast.encoded == reference.encoded && reflect.DeepEqual(fast.value, reference.value)
}

func requireSameOutcome(t testing.TB, name string, decode func([]byte) (any, error), data []byte) decodeOutcome {
	t.Helper()
	fast, reference := decodeBothWays(t, decode, data)
	if !sameOutcome(fast, reference) {
		t.Fatalf("%s decoded %s differently\nfast:      err=%q value=%s\nreference: err=%q value=%s", name, truncateForLog(data), fast.err, fast.encoded, reference.err, reference.encoded)
	}
	return fast
}

func truncateForLog(data []byte) string {
	if len(data) > 400 {
		return string(data[:400]) + "..."
	}
	return string(data)
}

func differentialFixtures(t testing.TB) map[string][]byte {
	t.Helper()
	fixtures := map[string][]byte{}
	for _, pattern := range []string{"testdata/v1/*.json", "testdata/request/*.json", "testdata/response/*.json"} {
		paths, err := filepath.Glob(pattern)
		if err != nil {
			t.Fatal(err)
		}
		for _, path := range paths {
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			fixtures[path] = data
		}
	}
	if len(fixtures) < 40 {
		t.Fatalf("found only %d fixtures", len(fixtures))
	}
	return fixtures
}

// richTranscriptRequest carries every open JSON leaf an item or request can
// hold, so generated mutations reach each of them.
const richTranscriptRequest = `{
  "api_version": "llm.temporal/v1",
  "operation_key": "differential",
  "model": "logical",
  "context": {"tenant": "t", "project": "p", "actor": "a", "tags": {"a": "b"}},
  "instructions": [{"kind": "text", "text": "Follow\npolicy é."}],
  "input": [
    {"kind": "message", "actor": "human", "content": [
      {"kind": "text", "text": "{\"a\":1,\"a\":2}"},
      {"kind": "json", "value": {"nested": {"deep": [1, {"leaf": true}]}, "n": 9007199254740993}},
      {"kind": "image", "media_type": "image/png", "bytes": "aGVsbG8=", "detail": "low"},
      {"kind": "document", "media_type": "application/pdf", "title": "T", "blob": {"digest": "sha256:00", "byte_length": 5, "media_type": "application/pdf", "locator": "s3://bucket/key"}},
      {"kind": "refusal", "text": "no", "provider_code": "policy"},
      {"kind": "provider_state", "provider": "p", "endpoint_family": "f", "media_type": "application/octet-stream", "opaque": "AAEC"}
    ]},
    {"kind": "tool_call", "id": "call-1", "name": "lookup", "arguments": {"query": {"term": "a", "filters": [{"field": "x"}]}}},
    {"kind": "tool_result", "call_id": "call-1", "name": "lookup", "is_error": false, "content": [{"kind": "json", "value": {"hits": [{"id": 1}]}}]},
    {"kind": "provider_state", "provider": "p", "endpoint_family": "f", "media_type": "application/json", "opaque": ""},
    {"kind": "reference", "uri": "https://example.com/doc", "metadata": {"source": {"page": 3}}}
  ],
  "tools": [{"name": "lookup", "description": "Lookup", "input_schema": {"type": "object", "properties": {"query": {"type": "object"}}}}],
  "tool_policy": {"mode": "auto"},
  "output": {"max_tokens": 64},
  "sampling": {"temperature": 0.5, "seed": 7, "stop_sequences": ["END"]},
  "reasoning": {"mode": "enabled", "token_budget": 128},
  "extensions": {"vendor": {"flag": {"on": true}}}
}`

func differentialCorpus(t testing.TB) map[string][]byte {
	t.Helper()
	corpus := differentialFixtures(t)
	corpus["rich-request"] = []byte(richTranscriptRequest)
	var request map[string]json.RawMessage
	if err := json.Unmarshal([]byte(richTranscriptRequest), &request); err != nil {
		t.Fatal(err)
	}
	corpus["rich-items"] = request["input"]
	corpus["rich-generate-request"] = []byte(richGenerateRequest(t))
	for name, data := range map[string]string{
		"empty":             ``,
		"whitespace":        " \n\t",
		"null":              `null`,
		"padded-null":       " null\n",
		"empty-object":      `{}`,
		"empty-array":       ` [ ] `,
		"scalar":            `12`,
		"string":            `"text"`,
		"trailing-value":    `{} {}`,
		"trailing-garbage":  `[]x`,
		"unterminated":      `{"a":[1,2`,
		"bare-key":          `{a:1}`,
		"trailing-comma":    `[1,]`,
		"escaped-duplicate": `{"a":1,"a":2}`,
		"invalid-utf8-keys": "{\"\xff\":1,\"\xfe\":2}",
		"surrogate-keys":    `{"\ud800":1,"\udfff":2}`,
		"case-keys":         `{"a":1,"A":2}`,
		"wide-object":       wideObject(40, false),
		"wide-duplicate":    wideObject(40, true),
		"items-null":        `[null]`,
		"items-nested":      `[[{"kind":"message","actor":"human","content":[]}]]`,
		"item-number-kind":  `[{"kind":1}]`,
		"item-escaped-kind": `[{"kind":"message","actor":"human","content":[{"kind":"text","text":"a\tb"}]}]`,
		"item-null-content": `[{"kind":"message","actor":"human","content":null}]`,
		"item-big-int":      `[{"kind":"message","actor":"human","content":[{"kind":"document","media_type":"a/b","blob":{"digest":"d","byte_length":9223372036854775808,"media_type":"a/b","locator":"l"}}]}]`,
		"item-float-int":    `[{"kind":"message","actor":"human","content":[{"kind":"document","media_type":"a/b","blob":{"digest":"d","byte_length":1.0,"media_type":"a/b","locator":"l"}}]}]`,
		"item-bad-base64":   `[{"kind":"provider_state","provider":"p","endpoint_family":"f","media_type":"m","opaque":"***"}]`,
		"item-bool-string":  `[{"kind":"tool_result","call_id":"c","content":[],"is_error":"true"}]`,
		"item-deep-args":    `[{"kind":"tool_call","id":"c","name":"n","arguments":` + strings.Repeat("[", 64) + strings.Repeat("]", 64) + `}]`,
	} {
		corpus["literal/"+name] = []byte(data)
	}
	return corpus
}

func wideObject(keys int, duplicate bool) string {
	var object strings.Builder
	object.WriteByte('{')
	for index := range keys {
		if index > 0 {
			object.WriteByte(',')
		}
		fmt.Fprintf(&object, `"key-%d":%d`, index, index)
	}
	if duplicate {
		object.WriteString(`,"key-39":0`)
	}
	object.WriteByte('}')
	return object.String()
}

// jsonNode is a parsed document that keeps member order, so one object or
// value can be rewritten while every other byte is reproduced.
type jsonNode struct {
	kind     byte // '{', '[' or 0 for a scalar
	scalar   string
	keys     []string
	children []*jsonNode
}

func parseJSONNode(t testing.TB, data []byte) *jsonNode {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var parse func() *jsonNode
	parse = func() *jsonNode {
		token, err := decoder.Token()
		if err != nil {
			t.Fatal(err)
		}
		delimiter, ok := token.(json.Delim)
		if !ok {
			encoded, err := json.Marshal(token)
			if err != nil {
				t.Fatal(err)
			}
			return &jsonNode{scalar: string(encoded)}
		}
		node := &jsonNode{kind: byte(delimiter)}
		for decoder.More() {
			if delimiter == '{' {
				key, err := decoder.Token()
				if err != nil {
					t.Fatal(err)
				}
				node.keys = append(node.keys, key.(string))
			}
			node.children = append(node.children, parse())
		}
		if _, err := decoder.Token(); err != nil {
			t.Fatal(err)
		}
		return node
	}
	return parse()
}

func (node *jsonNode) walk(visit func(*jsonNode)) {
	visit(node)
	for _, child := range node.children {
		child.walk(visit)
	}
}

// render writes the document, letting rewrite replace the rendering of any
// one node.
func (node *jsonNode) render(output *strings.Builder, rewrite func(*jsonNode, *strings.Builder) bool) {
	if rewrite != nil && rewrite(node, output) {
		return
	}
	switch node.kind {
	case 0:
		output.WriteString(node.scalar)
	case '[':
		output.WriteByte('[')
		for index, child := range node.children {
			if index > 0 {
				output.WriteString(", ")
			}
			child.render(output, rewrite)
		}
		output.WriteByte(']')
	case '{':
		output.WriteByte('{')
		for index, child := range node.children {
			if index > 0 {
				output.WriteString(", ")
			}
			key, _ := json.Marshal(node.keys[index])
			output.Write(key)
			output.WriteString(": ")
			child.render(output, rewrite)
		}
		output.WriteByte('}')
	}
}

func (node *jsonNode) text(rewrite func(*jsonNode, *strings.Builder) bool) []byte {
	var output strings.Builder
	node.render(&output, rewrite)
	return []byte(output.String())
}

// duplicateKeyMutants returns one document per non-empty object of the
// source, each repeating that object's first key.
func duplicateKeyMutants(t testing.TB, data []byte) [][]byte {
	t.Helper()
	root := parseJSONNode(t, data)
	var mutants [][]byte
	root.walk(func(target *jsonNode) {
		if target.kind != '{' || len(target.children) == 0 {
			return
		}
		mutants = append(mutants, root.text(func(node *jsonNode, output *strings.Builder) bool {
			if node != target {
				return false
			}
			var object strings.Builder
			node.render(&object, nil)
			key, _ := json.Marshal(node.keys[0])
			output.WriteString(strings.TrimSuffix(object.String(), "}"))
			output.WriteString(", ")
			output.Write(key)
			output.WriteString(": ")
			node.children[0].render(output, nil)
			output.WriteByte('}')
			return true
		}))
	})
	return mutants
}

// shapeMutants returns documents that each change one node of the source in a
// way the strict decoders care about: a null, an unknown field, a changed
// scalar type, or an escaped spelling of a string.
func shapeMutants(t testing.TB, data []byte) [][]byte {
	t.Helper()
	root := parseJSONNode(t, data)
	var mutants [][]byte
	replace := func(target *jsonNode, with func(*jsonNode, *strings.Builder)) {
		mutants = append(mutants, root.text(func(node *jsonNode, output *strings.Builder) bool {
			if node != target {
				return false
			}
			with(node, output)
			return true
		}))
	}
	root.walk(func(target *jsonNode) {
		replace(target, func(_ *jsonNode, output *strings.Builder) { output.WriteString("null") })
		switch {
		case target.kind == '{':
			replace(target, func(node *jsonNode, output *strings.Builder) {
				var object strings.Builder
				node.render(&object, nil)
				output.WriteString(strings.TrimSuffix(object.String(), "}"))
				if len(node.children) > 0 {
					output.WriteString(", ")
				}
				output.WriteString(`"zz_unknown": {"a": 1}}`)
			})
			replace(target, func(_ *jsonNode, output *strings.Builder) { output.WriteString("[]") })
		case target.kind == '[':
			replace(target, func(_ *jsonNode, output *strings.Builder) { output.WriteString("{}") })
		case strings.HasPrefix(target.scalar, `"`):
			replace(target, func(_ *jsonNode, output *strings.Builder) { output.WriteString("7") })
			replace(target, func(node *jsonNode, output *strings.Builder) {
				// Spell the first character as an escape: the same string
				// through the decoder's slow path.
				var value string
				if err := json.Unmarshal([]byte(node.scalar), &value); err != nil || value == "" || value[0] >= 0x80 {
					output.WriteString(`"A"`)
					return
				}
				rest, _ := json.Marshal(value[1:])
				fmt.Fprintf(output, `"\u%04x%s`, value[0], rest[1:])
			})
		default:
			replace(target, func(node *jsonNode, output *strings.Builder) { output.WriteString(`"` + node.scalar + `"`) })
			replace(target, func(_ *jsonNode, output *strings.Builder) { output.WriteString("1.5e400") })
		}
	})
	return mutants
}

func TestFastJSONDecodingMatchesReferenceOnCorpus(t *testing.T) {
	accepted := map[string]bool{}
	for name, data := range differentialCorpus(t) {
		for decoderName, decode := range differentialDecoders {
			if outcome := requireSameOutcome(t, decoderName+" "+name, decode, data); outcome.err == "" {
				accepted[name] = true
			}
		}
	}
	// The comparison is only meaningful if the corpus exercises acceptance as
	// well as rejection.
	for name := range differentialFixtures(t) {
		if negative := strings.Contains(name, "negative-"); !negative && !accepted[name] {
			t.Errorf("positive fixture %s was accepted by no decoder", name)
		}
	}
}

func TestFastJSONDecodingMatchesReferenceOnMutatedCorpus(t *testing.T) {
	for name, data := range differentialCorpus(t) {
		if !json.Valid(data) {
			continue
		}
		// Mutants are compared through the decoders that accept the source
		// document, which are the ones that decode it to any depth.
		decoders := map[string]func([]byte) (any, error){}
		for decoderName, decode := range differentialDecoders {
			if _, err := decode(data); err == nil {
				decoders[decoderName] = decode
			}
		}
		for index, mutant := range shapeMutants(t, data) {
			for decoderName, decode := range decoders {
				requireSameOutcome(t, fmt.Sprintf("%s %s shape mutant %d", decoderName, name, index), decode, mutant)
			}
		}
	}
}

// Every object of every document, at whatever depth and whether it is a typed
// record or JSON kept verbatim, must still be rejected when it repeats a key,
// by every entry point, with the error the reference scan reports.
func TestDuplicateKeysAreRejectedAtEveryDepthOfEveryDocument(t *testing.T) {
	mutants := 0
	for name, data := range differentialCorpus(t) {
		if !json.Valid(data) || rejectDuplicateJSONKeys(data) != nil {
			continue
		}
		for index, mutant := range duplicateKeyMutants(t, data) {
			mutants++
			for decoderName, decode := range differentialDecoders {
				label := fmt.Sprintf("%s %s duplicate mutant %d", decoderName, name, index)
				outcome := requireSameOutcome(t, label, decode, mutant)
				if outcome.err == "" {
					t.Fatalf("%s: accepted %s", label, truncateForLog(mutant))
				}
				// Some decoders replace the cause with their own message; the
				// ones that report it must name the duplicate.
				if reportsCause := decoderName == "rejectDuplicateJSONKeys" || decoderName == "decodeObject" || decoderName == "DecodeItems" || decoderName == "Request"; reportsCause && !strings.Contains(outcome.err, "duplicate JSON object key") {
					t.Fatalf("%s: %s was not rejected as a duplicate key: err=%q", label, truncateForLog(mutant), outcome.err)
				}
			}
		}
	}
	if mutants < 300 {
		t.Fatalf("generated only %d duplicate-key mutants", mutants)
	}
}

// The named nesting levels are pinned individually so a corpus change cannot
// silently stop covering one of them.
func TestNestedDuplicateKeysAreRejectedAtNamedLevels(t *testing.T) {
	generateRequest := richGenerateRequest(t)
	type level struct {
		decoder string
		base    string
		old     string
		new     string
	}
	for name, test := range map[string]level{
		"request field":          {"Request", richTranscriptRequest, `"model": "logical"`, `"model": "logical", "model": "logical"`},
		"request context":        {"Request", richTranscriptRequest, `{"a": "b"}`, `{"a": "b", "a": "b"}`},
		"item field":             {"Request", richTranscriptRequest, `"actor": "human"`, `"actor": "human", "actor": "human"`},
		"part field":             {"Request", richTranscriptRequest, `"text": "no"`, `"text": "no", "text": "no"`},
		"blob field":             {"Request", richTranscriptRequest, `"byte_length": 5`, `"byte_length": 5, "byte_length": 5`},
		"tool arguments":         {"Request", richTranscriptRequest, `{"term": "a", `, `{"term": "a", "term": "a", `},
		"tool arguments array":   {"Request", richTranscriptRequest, `{"field": "x"}`, `{"field": "x", "field": "x"}`},
		"JSON part value":        {"Request", richTranscriptRequest, `{"leaf": true}`, `{"leaf": true, "leaf": true}`},
		"tool result JSON value": {"Request", richTranscriptRequest, `{"id": 1}`, `{"id": 1, "id": 1}`},
		"reference metadata":     {"Request", richTranscriptRequest, `{"page": 3}`, `{"page": 3, "page": 3}`},
		"tool schema":            {"Request", richTranscriptRequest, `{"query": {"type": "object"}}`, `{"query": {"type": "object", "type": "object"}}`},
		"extensions":             {"Request", richTranscriptRequest, `{"on": true}`, `{"on": true, "on": true}`},
		"escaped spelling":       {"Request", richTranscriptRequest, `{"on": true}`, `{"on": true, "on": true}`},
		"items list item":        {"DecodeItems", richTranscriptItems(t), `"actor": "human"`, `"actor": "human", "actor": "human"`},
		"items list arguments":   {"DecodeItems", richTranscriptItems(t), `{"term": "a", `, `{"term": "a", "term": "a", `},
		"items list JSON value":  {"DecodeItems", richTranscriptItems(t), `{"leaf": true}`, `{"leaf": true, "leaf": true}`},
		"v1 request field":       {"GenerateRequestV1", generateRequest, `"parent": "ckp_v1.parent"`, `"parent": "ckp_v1.parent", "parent": "ckp_v1.parent"`},
		"v1 append item":         {"GenerateRequestV1", generateRequest, `"actor": "human"`, `"actor": "human", "actor": "human"`},
		"v1 append part":         {"GenerateRequestV1", generateRequest, `{"kind": "text", "text": "Continue."}`, `{"kind": "text", "text": "Continue.", "text": "Continue."}`},
		"v1 append JSON value":   {"GenerateRequestV1", generateRequest, `{"x": 1}`, `{"x": 1, "x": 1}`},
		"settings patch":         {"GenerateRequestV1", generateRequest, `{"set": "gpt-example"}`, `{"set": "gpt-example", "set": "gpt-example"}`},
		"settings patch leaf":    {"GenerateRequestV1", generateRequest, `{"max_tokens": 64, `, `{"max_tokens": 64, "max_tokens": 64, `},
		"settings patch schema":  {"GenerateRequestV1", generateRequest, `"input_schema": {"type": "object"}`, `"input_schema": {"type": "object", "type": "object"}`},
		"settings patch part":    {"GenerateRequestV1", generateRequest, `{"kind": "text", "text": "Follow policy."}`, `{"kind": "text", "text": "Follow policy.", "kind": "text"}`},
		"settings patch policy":  {"GenerateRequestV1", generateRequest, `"parallel": false}`, `"parallel": false, "parallel": false}`},
		"settings extensions":    {"GenerateRequestV1", generateRequest, `{"trace_mode": "safe"}`, `{"trace_mode": "safe", "trace_mode": "safe"}`},
		"cache policy":           {"GenerateRequestV1", generateRequest, `"max_age_seconds": 300`, `"max_age_seconds": 300, "max_age_seconds": 300`},
	} {
		t.Run(name, func(t *testing.T) {
			decode := differentialDecoders[test.decoder]
			if outcome := requireSameOutcome(t, name, decode, []byte(test.base)); outcome.err != "" {
				t.Fatalf("control document rejected: %s", outcome.err)
			}
			mutated := strings.Replace(test.base, test.old, test.new, 1)
			if mutated == test.base {
				t.Fatalf("document does not contain %s", test.old)
			}
			outcome := requireSameOutcome(t, name, decode, []byte(mutated))
			if !strings.Contains(outcome.err, "duplicate JSON object key") {
				t.Fatalf("nested duplicate was not rejected: err=%q", outcome.err)
			}
		})
	}
}

// richGenerateRequest is the fork fixture, which sets every settings patch
// leaf, with a non-empty append.
func richGenerateRequest(t testing.TB) string {
	t.Helper()
	fixture, err := os.ReadFile("testdata/v1/generate-fork-patch-set.json")
	if err != nil {
		t.Fatal(err)
	}
	const appended = `"append": [{"kind": "message", "actor": "human", "content": [{"kind": "text", "text": "Continue."}, {"kind": "json", "value": {"leaf": {"x": 1}}}]}]`
	request := strings.Replace(string(fixture), `"append": []`, appended, 1)
	if request == string(fixture) {
		t.Fatal("fork fixture no longer has an empty append")
	}
	return request
}

func richTranscriptItems(t testing.TB) string {
	t.Helper()
	var request map[string]json.RawMessage
	if err := json.Unmarshal([]byte(richTranscriptRequest), &request); err != nil {
		t.Fatal(err)
	}
	return string(request["input"])
}

// JSON carried inside a string is text, not structure: it was never scanned
// for duplicate keys and still is not.
func TestDuplicateKeysInsideStringsAreNotStructure(t *testing.T) {
	items := `[{"kind":"message","actor":"human","content":[{"kind":"text","text":"{\"a\":1,\"a\":2}"}]}]`
	outcome := requireSameOutcome(t, "DecodeItems", differentialDecoders["DecodeItems"], []byte(items))
	if outcome.err != "" {
		t.Fatalf("text holding duplicate-key JSON was rejected: %s", outcome.err)
	}
}

// The internal item decoders skip the per-subtree scan, so they must not be
// the first to see bytes that no entry point scanned. DecodeItems is the only
// exported way into them that does not start from an UnmarshalJSON method.
func TestDecodeItemsScansBeforeDecodingVerifiedSubtrees(t *testing.T) {
	// The outer array is malformed only after a complete, decodable item.
	for _, data := range []string{
		`[{"kind":"tool_call","id":"c","name":"n","arguments":{"a":1,"a":2}}]`,
		`[{"kind":"message","actor":"human","content":[]}] trailing`,
		`[{"kind":"message","actor":"human","content":[]},]`,
		`[{"kind":"message","actor":"human","content":[],}]`,
		`[{"kind":"message","actor":"human","content":[tru]}]`,
	} {
		outcome := requireSameOutcome(t, "DecodeItems", differentialDecoders["DecodeItems"], []byte(data))
		if outcome.err == "" {
			t.Fatalf("accepted %s", data)
		}
	}
}

func FuzzFastJSONDecodingMatchesReference(f *testing.F) {
	for _, data := range differentialCorpus(f) {
		f.Add(data)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1<<16 {
			t.Skip()
		}
		for _, name := range []string{"rejectDuplicateJSONKeys", "decodeObject", "DecodeItems", "Request", "GenerateRequestV1"} {
			requireSameOutcome(t, name, differentialDecoders[name], data)
		}
	})
}
