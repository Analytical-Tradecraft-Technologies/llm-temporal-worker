package llm

import (
	"encoding/json"
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

var blockedMediaURLs = []string{
	// Policy of #997.
	"http://169.254.169.254/latest/meta-data/",
	"file:///etc/passwd",
	"http://localhost:6379/",
	"http://api.localhost/x",
	"gopher://10.0.0.1/",
	"https://user:pw@internal.example/",
	"http://10.0.0.1/image.png",
	"http://[::1]/image.png",
	"http://[::ffff:127.0.0.1]/image.png",
	"https://168.63.129.16/doc.pdf",
	"https://0.0.0.0/doc.pdf",
	"ftp://example.com/doc.pdf",
	// Non-canonical IPv4 forms that resolvers normalise (#1091).
	"http://127.1/x",
	"http://2130706433/x",
	"http://0x7f000001/x",
	"http://0X7F000001/x",
	"http://0177.0.0.1/x",
	"http://0x7f.0.0.1/x",
	"http://127.0.0.1./x",
	"http://example.com.0x7f000001/x",
	"http://１２７.０.０.１/x",
	"http://127.0.0.1\\.example.com/x",
	// Ranges of the provider egress policy.
	"http://[64:ff9b::a9fe:a9fe]/x",
	"http://[64:ff9b:1::a00:1]/x",
	"http://[2002:a9fe:a9fe::1]/x",
	"http://[2001:0:a9fe:a9fe::1]/x",
	"http://[::169.254.169.254]/x",
	"http://[fec0::1]/x",
	"http://[fe80::1%25eth0]/x",
	"http://[2001:4860:4860::8888%25eth0]/x",
	"http://100.64.0.1/x",
	"http://192.0.0.8/x",
	"http://198.18.0.1/x",
	"http://240.0.0.1/x",
	// Metadata and private-zone names.
	"http://metadata.google.internal/x",
	"http://METADATA.GOOGLE.INTERNAL./x",
	"http://metadata.goog/x",
	"http://metadata/x",
	"http://instance-data/latest/meta-data/",
	"http://instance-data.ec2.internal/x",
}

var allowedMediaURLs = []string{
	"https://example.com/image.png",
	"http://cdn.example.com/a.png",
	"https://cdn1.example.com/a.png",
	"https://1password.com/logo.png",
	"https://3.example.co.uk/a.png",
	"https://0x7f.example.com/a.png",
	"https://xn--bcher-kva.example/a.png",
	"https://my_bucket.s3.amazonaws.com/a.png",
	"https://internal.example/a.png",
	"https://example.com:8443/a.png?x=1#frag",
	"https://8.8.8.8/image.png",
	"https://[2001:4860:4860::8888]/image.png",
	"https://[64:ff9b::808:808]/image.png",
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
