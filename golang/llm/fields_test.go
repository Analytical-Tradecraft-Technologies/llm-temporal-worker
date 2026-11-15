package llm

import (
	"encoding/json"
	"reflect"
	"testing"
)

func rawFields(t *testing.T, data string) map[string]json.RawMessage {
	t.Helper()
	fields, err := decodeObject([]byte(data))
	if err != nil {
		t.Fatalf("decodeObject(%s) = %v", data, err)
	}
	return fields
}

func TestDecodeClosedObjectRejectsUnknownFields(t *testing.T) {
	if fields, err := decodeClosedObject([]byte(`{"a":1}`), "a", "b"); err != nil || string(fields["a"]) != "1" {
		t.Fatalf("decodeClosedObject = %v, %v", fields, err)
	}
	if _, err := decodeClosedObject([]byte(`{"c":1}`), "a"); err == nil || err.Error() != `unknown JSON field "c"` {
		t.Fatalf("unknown field error = %v", err)
	}
	if _, err := decodeClosedObject([]byte(`[]`), "a"); err == nil {
		t.Fatal("decodeClosedObject accepted an array")
	}
}

func TestFieldReaderKeepsTheFirstError(t *testing.T) {
	reader := fieldReader{fields: rawFields(t, `{"name":"n","count":1,"flag":true,"blob":{}}`)}
	if got := reader.requiredString("name"); got != "n" || reader.err != nil {
		t.Fatalf("requiredString = %q, %v", got, reader.err)
	}
	if got, present := reader.optionalStringPresent("absent"); got != "" || present || reader.err != nil {
		t.Fatalf("optionalStringPresent(absent) = %q, %v, %v", got, present, reader.err)
	}
	if got := reader.requiredInt64("count"); got != 1 || reader.err != nil {
		t.Fatalf("requiredInt64 = %d, %v", got, reader.err)
	}
	if got := reader.optionalBool("flag"); !got || reader.err != nil {
		t.Fatalf("optionalBool = %v, %v", got, reader.err)
	}
	if got := reader.requiredString("count"); got != "" || reader.err == nil {
		t.Fatalf("requiredString(count) = %q, %v", got, reader.err)
	}
	first := reader.err
	if got := reader.requiredString("name"); got != "" {
		t.Fatalf("read after a failure = %q", got)
	}
	if got := reader.requireField("blob"); got != nil {
		t.Fatalf("requireField after a failure = %s", got)
	}
	if reader.requiredBool("flag") || reader.optionalBool("flag") || reader.requiredInt64("count") != 0 {
		t.Fatal("typed read after a failure returned a value")
	}
	if reader.err != first {
		t.Fatalf("later reads replaced the first error %v with %v", first, reader.err)
	}
	fresh := fieldReader{fields: reader.fields}
	if fresh.requiredString("missing"); fresh.err == nil || fresh.err.Error() != `missing required JSON field "missing"` {
		t.Fatalf("missing field error = %v", fresh.err)
	}
}

func TestReadFieldHelpersPrefixOnlyWhereTheWrappedHelperDoes(t *testing.T) {
	fields := rawFields(t, `{"count":7,"bad":"x","none":null}`)
	var value int
	if err := decodePresentField(fields, "bad", &value, decodeInt); err == nil || err.Error()[:4] == "bad:" {
		t.Fatalf("decodePresentField error = %v, want it unprefixed", err)
	}
	reader := fieldReader{fields: fields}
	readOptionalField(&reader, "bad", &value, decodeInt)
	if reader.err == nil || reader.err.Error()[:4] != "bad:" {
		t.Fatalf("readOptionalField error = %v, want a bad: prefix", reader.err)
	}
	reader = fieldReader{fields: fields}
	var pointer *int
	readNullableField(&reader, "none", &pointer, pointerTo(decodeInt))
	readPresentField(&reader, "count", &pointer, pointerTo(decodeInt))
	if reader.err != nil || pointer == nil || *pointer != 7 {
		t.Fatalf("pointer read = %v, %v", pointer, reader.err)
	}
	readRequiredField(&reader, "absent", &value, decodeInt)
	if reader.err == nil || reader.err.Error() != `missing required JSON field "absent"` {
		t.Fatalf("readRequiredField(absent) error = %v", reader.err)
	}
}

func TestReadEnumDefaultsAndRejects(t *testing.T) {
	reader := fieldReader{fields: rawFields(t, `{"empty":"","good":"b","bad":"z"}`)}
	for key, want := range map[string]string{"absent": "a", "empty": "a", "good": "b"} {
		if got := readEnum(&reader, key, "a", "test kind", "a", "b"); got != want || reader.err != nil {
			t.Fatalf("readEnum(%s) = %q, %v; want %q", key, got, reader.err, want)
		}
	}
	if got := readEnum(&reader, "bad", "a", "test kind", "a", "b"); got != "" || reader.err == nil || reader.err.Error() != `test kind "z" is invalid` {
		t.Fatalf("readEnum(bad) = %q, %v", got, reader.err)
	}
	if _, err := defaultEnum(ToolKind(""), ToolKindFunction, "tool kind", toolKinds...); err != nil {
		t.Fatalf("defaultEnum(empty) = %v", err)
	}
}

func TestArrayHelpersNameTheirFailures(t *testing.T) {
	if _, err := decodeVerifiedList([]byte(` null `), "part", decodePart); err == nil || err.Error() != "parts must be an array" {
		t.Fatalf("decodeVerifiedList(null) = %v", err)
	}
	if got, err := decodeArray([]byte(`[1,2]`), "number", decodeInt); err != nil || !reflect.DeepEqual(got, []int{1, 2}) {
		t.Fatalf("decodeArray = %v, %v", got, err)
	}
	if _, err := decodeArray([]byte(`[1,"x"]`), "number", decodeInt); err == nil || err.Error()[:9] != "number 1:" {
		t.Fatalf("decodeArray error = %v", err)
	}
	fields := rawFields(t, `{"rows":[{}],"none":null,"text":"x"}`)
	if values, err := requiredRawArray(fields, "rows"); err != nil || len(values) != 1 {
		t.Fatalf("requiredRawArray = %v, %v", values, err)
	}
	for _, name := range []string{"none", "text", "absent"} {
		if _, err := requiredRawArray(fields, name); err == nil || err.Error() != name+" must be an array" {
			t.Fatalf("requiredRawArray(%s) = %v", name, err)
		}
	}
	if err := requireStrings(fields, "text", "rows"); err == nil {
		t.Fatal("requireStrings accepted an array")
	}
}

func TestEncoderHelpersSkipEmptyValues(t *testing.T) {
	fields := map[string]any{}
	setIfNotEmpty(fields, "empty", "")
	setIfNotEmpty(fields, "kind", ToolKindProvider)
	setIfNotNil(fields, "none", (*Usage)(nil))
	usage := &Usage{}
	setIfNotNil(fields, "usage", usage)
	if len(fields) != 2 || fields["kind"] != ToolKindProvider || fields["usage"] != usage {
		t.Fatalf("fields = %#v", fields)
	}
	if got := emptyIfNil[Item](nil); got == nil || len(got) != 0 {
		t.Fatalf("emptyIfNil(nil) = %#v", got)
	}
	values := []int{1}
	if got := emptyIfNil(values); &got[0] != &values[0] {
		t.Fatal("emptyIfNil copied a non-nil slice")
	}
}

func TestDerefAsAcceptsValuesAndNonNilPointers(t *testing.T) {
	var result QueryResult = BudgetStatus{ActiveAt: "a"}
	if got, ok := derefAs[BudgetStatus](result); !ok || got.ActiveAt != "a" {
		t.Fatalf("derefAs(value) = %v, %v", got, ok)
	}
	result = &BudgetStatus{ActiveAt: "b"}
	if got, ok := derefAs[BudgetStatus](result); !ok || got.ActiveAt != "b" {
		t.Fatalf("derefAs(pointer) = %v, %v", got, ok)
	}
	if _, ok := derefAs[BudgetStatus]((*BudgetStatus)(nil)); ok {
		t.Fatal("derefAs accepted a nil pointer")
	}
	if _, ok := derefAs[BudgetStatus](SpendSummary{}); ok {
		t.Fatal("derefAs accepted another type")
	}
	if err := validateQueryResult(QueryBudgetStatus, (*BudgetStatus)(nil)); err == nil || err.Error() != "budget status result has unexpected type" {
		t.Fatalf("validateQueryResult(nil pointer) = %v", err)
	}
}

func TestSanitizeCopyNeverMutatesThePointee(t *testing.T) {
	sanitize := func(call *ToolCall) { call.Name = "sanitized" }
	original := &ToolCall{Name: "original"}
	got, ok := sanitizeCopy[Item](original, sanitize)
	if !ok || got.(*ToolCall).Name != "sanitized" || original.Name != "original" {
		t.Fatalf("sanitizeCopy(pointer) = %#v, %v; original %#v", got, ok, original)
	}
	if got, ok := sanitizeCopy[Item](ToolCall{}, sanitize); !ok || got.(ToolCall).Name != "sanitized" {
		t.Fatalf("sanitizeCopy(value) = %#v, %v", got, ok)
	}
	var nilCall *ToolCall
	if got, ok := sanitizeCopy[Item](nilCall, sanitize); !ok || got.(*ToolCall) != nil {
		t.Fatalf("sanitizeCopy(nil) = %#v, %v", got, ok)
	}
	if got, ok := sanitizeCopy[Item](Message{}, sanitize); ok || !reflect.DeepEqual(got, Message{}) {
		t.Fatalf("sanitizeCopy(other) = %#v, %v", got, ok)
	}
}

func TestPatchWriterAndEnvelope(t *testing.T) {
	value := "m"
	writer := patchWriter{fields: map[string]any{}}
	writePatch(&writer, "set", Patch[string]{Set: &value})
	writePatch(&writer, "clear", Patch[string]{Clear: true})
	writePatch(&writer, "omitted", Patch[string]{})
	writePatch(&writer, "both", Patch[string]{Set: &value, Clear: true})
	writePatch(&writer, "after", Patch[string]{Clear: true})
	if writer.err == nil || writer.err.Error() != "both: patch cannot contain both set and clear" {
		t.Fatalf("writer error = %v", writer.err)
	}
	want := map[string]any{"set": map[string]any{"set": "m"}, "clear": map[string]any{"clear": true}}
	if !reflect.DeepEqual(writer.fields, want) {
		t.Fatalf("writer fields = %#v", writer.fields)
	}
	for input, message := range map[string]string{
		`{}`:                      "leaf must contain exactly one of set or clear",
		`{"clear":false}`:         "leaf.clear must be true",
		`{"set":1,"clear":true}`:  "leaf must contain exactly one of set or clear",
		`{"other":1}`:             `leaf: unknown JSON field "other"`,
		`[]`:                      "leaf: expected JSON object",
		`{"set":1,"set":2}`:       `leaf: invalid JSON: duplicate JSON object key "set"`,
		`{"clear":true,"x":true}`: `leaf: unknown JSON field "x"`,
	} {
		if _, _, err := decodePatchEnvelope(json.RawMessage(input), "leaf"); err == nil || err.Error() != message {
			t.Fatalf("decodePatchEnvelope(%s) = %v, want %q", input, err, message)
		}
	}
	if set, clear, err := decodePatchEnvelope(json.RawMessage(`{"set":"v"}`), "leaf"); err != nil || clear || string(set) != `"v"` {
		t.Fatalf("decodePatchEnvelope(set) = %s, %v, %v", set, clear, err)
	}
}
