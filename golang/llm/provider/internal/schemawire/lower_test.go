package schemawire

import (
	"reflect"
	"testing"
)

func TestLowerMapAndLowerListJoinPaths(t *testing.T) {
	lower := func(_ any, path string) (map[string]any, error) { return map[string]any{"path": path}, nil }
	mapped, err := LowerMap(map[string]any{"a/b": 1}, "/properties", lower)
	if err != nil || !reflect.DeepEqual(mapped, map[string]any{"a/b": map[string]any{"path": "/properties/a~1b"}}) {
		t.Fatalf("LowerMap = %v, %v", mapped, err)
	}
	listed, err := LowerList([]any{1, 2}, "/anyOf", lower)
	if err != nil || !reflect.DeepEqual(listed, []any{map[string]any{"path": "/anyOf/0"}, map[string]any{"path": "/anyOf/1"}}) {
		t.Fatalf("LowerList = %v, %v", listed, err)
	}
}

func TestMergeOneOfKeepsAnExistingAnyOf(t *testing.T) {
	result, extras := map[string]any{"oneOf": "lowered", "anyOf": "kept"}, map[string]any{}
	MergeOneOf(result, map[string]any{"oneOf": "source"}, extras)
	if !reflect.DeepEqual(result, map[string]any{"anyOf": "kept"}) || !reflect.DeepEqual(extras, map[string]any{"oneOf": "source"}) {
		t.Fatalf("MergeOneOf = %v, extras %v", result, extras)
	}
}

func TestNoteExtrasAppendsToDescription(t *testing.T) {
	result := map[string]any{"description": "Answer"}
	if err := NoteExtras(result, map[string]any{"pattern": "^a"}, ""); err != nil || result["description"] != "Answer\n\n{pattern: ^a}" {
		t.Fatalf("NoteExtras = %v, %v", result, err)
	}
	if err := NoteExtras(map[string]any{}, map[string]any{"bad": func() {}}, ""); err == nil || err.Error() != "output schema at /: json: unsupported type: func()" {
		t.Fatalf("NoteExtras(unencodable) = %v", err)
	}
}
