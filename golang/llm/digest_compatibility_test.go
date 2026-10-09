package llm

import (
	"crypto/sha256"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

// Preserve the pre-optimization algorithm as an independent compatibility
// oracle. Digests identify persisted requests and cache entries; a faster
// encoder must not invalidate those identities.
func legacyRequestDigest(request Request) ([32]byte, error) {
	var zero [32]byte
	normalized, err := NormalizeRequest(request)
	if err != nil {
		return zero, err
	}
	encoded, err := json.Marshal(normalized)
	if err != nil {
		return zero, err
	}
	fields, err := decodeObject(encoded)
	if err != nil {
		return zero, err
	}
	delete(fields, "operation_key")
	encoded, err = json.Marshal(fields)
	if err != nil {
		return zero, err
	}
	canonical, err := CanonicalJSON(encoded)
	if err != nil {
		return zero, err
	}
	return sha256.Sum256(append([]byte(requestDigestDomain), canonical...)), nil
}

func assertDigestCompatibility(t *testing.T, request Request) {
	t.Helper()
	want, wantErr := legacyRequestDigest(request)
	got, gotErr := RequestDigest(request)
	if (wantErr == nil) != (gotErr == nil) || (wantErr != nil && wantErr.Error() != gotErr.Error()) {
		t.Fatalf("digest errors differ: got %v, legacy %v", gotErr, wantErr)
	}
	if got != want {
		t.Fatalf("persisted identity changed: got %x, legacy %x", got, want)
	}
}

func TestRequestDigestLegacyCompatibility(t *testing.T) {
	for _, path := range []string{"testdata/request/minimal.json", "testdata/request/full.json"} {
		t.Run(path, func(t *testing.T) {
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var request Request
			if err := json.Unmarshal(data, &request); err != nil {
				t.Fatal(err)
			}
			assertDigestCompatibility(t, request)
		})
	}
	// Exercise every typed item/part and optional request field, including byte
	// payloads, timestamps, defaults, and HTML/Unicode escaping.
	assertDigestCompatibility(t, normalizeFullRequest())
	assertDigestCompatibility(t, Request{OperationKey: "op", Model: "m"})
	for name, mutate := range map[string]func(*Request){
		"large numbers": func(r *Request) {
			r.Extensions = map[string]json.RawMessage{"opaque": json.RawMessage(`{"large":9007199254740993,"decimal":0.1234567890123456789,"exponent":1e100}`)}
		},
		"escaped text": func(r *Request) {
			r.Input = []Item{Message{Actor: ActorHuman, Content: []Part{TextPart{Text: "<>&\u2028\u2029\x00\xff"}}}}
		},
		"nil collections": func(r *Request) {
			r.Input, r.Instructions, r.Tools, r.Extensions, r.ServiceClassFallbacks = nil, nil, nil, nil, nil
		},
		"unsupported version": func(r *Request) { r.APIVersion = "unsupported" },
		"empty operation key": func(r *Request) { r.OperationKey = "" },
		"invalid fallback":    func(r *Request) { r.ServiceClassFallbacks = []ServiceClass{r.ServiceClass} },
		"duplicate extension": func(r *Request) {
			r.Extensions = map[string]json.RawMessage{"opaque": json.RawMessage(`{"a":1,"a":2}`)}
		},
		"invalid extension": func(r *Request) { r.Extensions = map[string]json.RawMessage{"opaque": json.RawMessage(`{"a":`)} },
	} {
		t.Run(name, func(t *testing.T) {
			request := normalizeFullRequest()
			mutate(&request)
			assertDigestCompatibility(t, request)
		})
	}
}

func FuzzRequestDigestLegacyCompatibility(f *testing.F) {
	f.Add("<>&\u2028\u2029", `{"b":9007199254740993,"a":1.0000}`, true)
	f.Add("\x00\xff", `{"a":1,"a":2}`, false)
	f.Add("text", `{"a":`, true)
	f.Fuzz(func(t *testing.T, text, raw string, features bool) {
		if len(text)+len(raw) > 32<<10 {
			t.Skip()
		}
		request := normalizeFullRequest()
		request.WebSearch, request.WebFetch, request.CodeExecution = features, features, features
		request.Input = []Item{Message{Actor: ActorHuman, Content: []Part{TextPart{Text: text}, JSONPart{Value: json.RawMessage(raw)}}}}
		request.Extensions = map[string]json.RawMessage{"opaque": json.RawMessage(raw)}
		assertDigestCompatibility(t, request)
	})
}

func BenchmarkRequestDigestLargeTranscript(b *testing.B) {
	request := Request{OperationKey: "op", Model: "m", Input: []Item{Message{Actor: ActorHuman, Content: []Part{TextPart{Text: strings.Repeat("a", 1<<20)}}}}}
	for _, method := range []struct {
		name string
		run  func(Request) ([32]byte, error)
	}{{"legacy", legacyRequestDigest}, {"direct_fields", RequestDigest}} {
		b.Run(method.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := method.run(request); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
