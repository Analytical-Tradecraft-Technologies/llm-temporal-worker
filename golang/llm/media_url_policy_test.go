package llm

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const mediaURLPlaceholder = "https://placeholder.example/media"

// mediaURLRequestWires returns one valid Generate request wire document for
// every position in which a request can introduce a media URL. Each contains
// mediaURLPlaceholder exactly once.
func mediaURLRequestWires(t *testing.T) map[string]string {
	t.Helper()
	image := ImagePart{URL: mediaURLPlaceholder, MediaType: "image/png"}
	document := DocumentPart{URL: mediaURLPlaceholder, MediaType: "application/pdf"}
	base := func() GenerateRequestV1 {
		return GenerateRequestV1{OperationKey: "op", Context: RequestContext{Tenant: "t", Project: "p", Actor: "a"}}
	}
	requests := map[string]GenerateRequestV1{}
	request := base()
	request.Append = []Item{Message{Actor: ActorHuman, Content: []Part{TextPart{Text: "look"}, image}}}
	requests["append message image"] = request
	request = base()
	request.Append = []Item{Message{Actor: ActorHuman, Content: []Part{document}}}
	requests["append message document"] = request
	request = base()
	request.Append = []Item{ToolResult{CallID: "call-1", Name: "lookup", Content: []Part{image}}}
	requests["append tool result image"] = request
	request = base()
	instructions := []Instruction{{Kind: InstructionKindParts, Level: InstructionLevelApplication, Content: []Part{document}}}
	request.SettingsPatch.Instructions.Set = &instructions
	requests["instructions document"] = request

	wires := map[string]string{}
	for name, request := range requests {
		data, err := json.Marshal(request)
		if err != nil {
			t.Fatalf("%s: marshal placeholder request: %v", name, err)
		}
		if strings.Count(string(data), mediaURLPlaceholder) != 1 {
			t.Fatalf("%s: wire does not contain the placeholder once: %s", name, data)
		}
		wires[name] = string(data)
	}
	return wires
}

func decodeMediaURLRequest(t *testing.T, wire, raw string) (GenerateRequestV1, error) {
	t.Helper()
	quoted, err := json.Marshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	var request GenerateRequestV1
	err = json.Unmarshal([]byte(strings.Replace(wire, `"`+mediaURLPlaceholder+`"`, string(quoted), 1)), &request)
	return request, err
}

// The policy cases live in testdata/v1/media-url-policy.tsv, which the OCaml
// codec tests read too, so both sides judge the same URLs (#1676).
var blockedMediaURLs, allowedMediaURLs = mediaURLPolicyCases()

func mediaURLPolicyCases() (blocked, allowed []string) {
	data, err := os.ReadFile(filepath.Join("testdata", "v1", "media-url-policy.tsv"))
	if err != nil {
		panic(err)
	}
	for _, line := range strings.Split(string(data), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		verdict, url, ok := strings.Cut(line, "\t")
		switch {
		case ok && verdict == "blocked":
			blocked = append(blocked, url)
		case ok && verdict == "allowed":
			allowed = append(allowed, url)
		default:
			panic("media-url-policy.tsv line is not blocked|allowed<TAB>url: " + line)
		}
	}
	return blocked, allowed
}

func TestGenerateRequestV1DecodeRejectsBlockedMediaURLs(t *testing.T) {
	for name, wire := range mediaURLRequestWires(t) {
		for _, raw := range blockedMediaURLs {
			if _, err := decodeMediaURLRequest(t, wire, raw); err == nil {
				t.Errorf("%s: media URL %q accepted at request decode", name, raw)
			}
		}
	}
}

func TestGenerateRequestV1DecodeAcceptsPublicMediaURLs(t *testing.T) {
	for name, wire := range mediaURLRequestWires(t) {
		for _, raw := range allowedMediaURLs {
			request, err := decodeMediaURLRequest(t, wire, raw)
			if err != nil {
				t.Errorf("%s: media URL %q rejected: %v", name, raw, err)
				continue
			}
			if _, err := json.Marshal(request); err != nil {
				t.Errorf("%s: media URL %q rejected at request encode: %v", name, raw, err)
			}
		}
	}
}

// The runtime validates an in-process request by marshalling it, so a request
// built as a Go value must be held to the same policy as a decoded one.
func TestGenerateRequestV1EncodeRejectsBlockedMediaURLs(t *testing.T) {
	for _, raw := range blockedMediaURLs {
		request := GenerateRequestV1{OperationKey: "op", Context: RequestContext{Tenant: "t", Project: "p", Actor: "a"},
			Append: []Item{Message{Actor: ActorHuman, Content: []Part{&ImagePart{URL: raw, MediaType: "image/png"}}}}}
		if _, err := json.Marshal(request); err == nil {
			t.Errorf("media URL %q accepted at request encode", raw)
		}
	}
}

// Stored transcripts are decoded through the item codec. A URL that was valid
// when its checkpoint was written must stay decodable and re-encodable after
// the request policy is tightened.
func TestItemCodecKeepsStoredMediaURLsOutsideTheRequestPolicy(t *testing.T) {
	for _, raw := range []string{"http://localhost:8080/a.png", "http://10.0.0.1/a.png", "http://127.1/a.png", "http://metadata.google.internal/a.png"} {
		stored := `[{"kind":"message","actor":"human","content":[{"kind":"image","url":"` + raw + `","media_type":"image/png"},{"kind":"document","url":"` + raw + `","media_type":"application/pdf"}]}]`
		items, err := DecodeItems([]byte(stored))
		if err != nil {
			t.Errorf("stored media URL %q no longer decodes: %v", raw, err)
			continue
		}
		if _, err := json.Marshal(items); err != nil {
			t.Errorf("stored media URL %q no longer encodes: %v", raw, err)
		}
		if err := ValidateMediaURLs(nil, items); err == nil {
			t.Errorf("stored media URL %q passes the request policy", raw)
		}
	}
	for _, raw := range []string{"javascript:alert(1)", "data:image/png;base64,AA==", "no-scheme"} {
		stored := `[{"kind":"message","actor":"human","content":[{"kind":"image","url":"` + raw + `","media_type":"image/png"}]}]`
		if _, err := DecodeItems([]byte(stored)); err == nil {
			t.Errorf("structurally invalid media URL %q decoded", raw)
		}
	}
}
