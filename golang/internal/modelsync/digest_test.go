package modelsync

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestDocumentDigestCoversContentOnly(t *testing.T) {
	document := fetchFixture(t)
	encoded, err := document.Encode()
	if err != nil {
		t.Fatal(err)
	}
	later := document
	later.FetchedAt = later.FetchedAt.Add(time.Hour)
	again, err := later.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded, again) || Digest(encoded) != Digest(again) {
		t.Fatal("a later fetch of the same catalog changed the encoded Document")
	}
	if bytes.Contains(encoded, []byte("fetched_at")) {
		t.Fatalf("encoded Document stores the fetch time: %s", encoded)
	}
	decoded, err := DecodeDocument(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !decoded.FetchedAt.IsZero() {
		t.Fatalf("decoded FetchedAt = %v, want zero until set from the Store", decoded.FetchedAt)
	}
	// A price change is content, so it changes the digest.
	changed := document
	changed.Models = append([]Model(nil), document.Models...)
	changed.Models[0].Endpoints = append([]Endpoint(nil), changed.Models[0].Endpoints...)
	changed.Models[0].Endpoints[0].Pricing.Prompt = "0.5"
	if other, err := changed.Encode(); err != nil || Digest(other) == Digest(encoded) {
		t.Fatalf("a price change kept the digest: %v", err)
	}
	// A schema v2 body, which embedded fetched_at, is rejected rather than
	// read under the new schema.
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatal(err)
	}
	fields["schema"], fields["fetched_at"] = json.RawMessage(`"openrouter-catalog/v2"`), json.RawMessage(`"2026-10-06T12:00:00Z"`)
	legacy, _ := json.Marshal(fields)
	if _, err := DecodeDocument(legacy); err == nil {
		t.Fatal("DecodeDocument accepted a schema v2 body")
	}
}

func TestSyncInstallsTheStoredFetchTime(t *testing.T) {
	store := NewMemoryStore(nil)
	document := fetchFixture(t)
	encoded, _ := document.Encode()
	publishedAt := fixtureTime.Add(3 * time.Hour)
	if _, err := store.Publish(context.Background(), Digest(encoded), publishedAt, encoded); err != nil {
		t.Fatal(err)
	}
	var installed Document
	refresher := &Refresher{Store: store, Install: func(document Document) error { installed = document; return nil }}
	if ok, err := refresher.Sync(context.Background()); !ok || err != nil {
		t.Fatalf("Sync() = %v, %v", ok, err)
	}
	if !installed.FetchedAt.Equal(publishedAt) {
		t.Fatalf("installed FetchedAt = %v, want the Store's %v", installed.FetchedAt, publishedAt)
	}
}

func TestRefreshObservesAnUnchangedCatalog(t *testing.T) {
	server := fixtureServer(t, nil)
	store := NewMemoryStore(nil)
	now := fixtureTime
	var events []Event
	installs := 0
	refresher := &Refresher{Store: store, MinInterval: time.Hour, MaxInterval: time.Hour, Clock: func() time.Time { return now },
		Fetch: func(ctx context.Context, previous Document) (Document, error) {
			return Fetcher{Client: server.Client(), BaseURL: server.URL, Clock: func() time.Time { return now }}.Fetch(ctx, previous)
		},
		Install: func(Document) error { installs++; return nil },
		Observe: func(event Event) { events = append(events, event) }}
	for range 3 {
		if err := refresher.Refresh(context.Background()); err != nil {
			t.Fatal(err)
		}
		now = now.Add(2 * time.Hour)
	}
	var published []string
	for _, event := range events {
		if event.Outcome == OutcomePublished {
			published = append(published, event.Cause)
		}
	}
	if installs != 1 || strings.Join(published, ",") != ",unchanged,unchanged" {
		t.Fatalf("installs = %d published causes = %q, want one install then two unchanged publications", installs, published)
	}
}

// etagServer serves bodies by path with a content ETag and answers a
// matching If-None-Match with 304. It records each response status.
type etagServer struct {
	mu          sync.Mutex
	bodies      map[string]string
	statuses    []int
	conditional int
}

func (server *etagServer) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	server.mu.Lock()
	defer server.mu.Unlock()
	body, ok := server.bodies[request.URL.Path]
	if !ok {
		http.NotFound(writer, request)
		return
	}
	sum := sha256.Sum256([]byte(body))
	etag := `"` + hex.EncodeToString(sum[:8]) + `"`
	if match := request.Header.Get("If-None-Match"); match != "" {
		server.conditional++
		if match == etag {
			server.statuses = append(server.statuses, http.StatusNotModified)
			writer.WriteHeader(http.StatusNotModified)
			return
		}
	}
	server.statuses = append(server.statuses, http.StatusOK)
	writer.Header().Set("ETag", etag)
	_, _ = writer.Write([]byte(body))
}

func (server *etagServer) take() (statuses []int, conditional int) {
	server.mu.Lock()
	defer server.mu.Unlock()
	statuses, conditional = server.statuses, server.conditional
	server.statuses, server.conditional = nil, 0
	return statuses, conditional
}

func endpointBody(prompt string) string {
	return fmt.Sprintf(`{"data":{"endpoints":[{"tag":"upstream","provider_name":%q,"context_length":1000,"pricing":{"prompt":%q,"completion":"0.000002"}}]}}`, testUpstreamProvider, prompt)
}

func TestFetchRevalidatesUnchangedResponsesWithETags(t *testing.T) {
	lists := &etagServer{bodies: map[string]string{
		"/models": `{"data":[` +
			`{"id":"openai/a","context_length":1000,"architecture":{"input_modalities":["text"],"output_modalities":["text"]},"pricing":{"prompt":"0.000001","completion":"0.000002"}},` +
			`{"id":"openai/b","context_length":1000,"architecture":{"input_modalities":["text"],"output_modalities":["text"]},"pricing":{"prompt":"0.000001","completion":"0.000002"}}]}`,
		"/providers":                 fmt.Sprintf(`{"data":[{"name":%q,"slug":"upstream","headquarters":"US","datacenters":["US"]}]}`, testUpstreamProvider),
		"/endpoints/zdr":             `{"data":[]}`,
		"/models/openai/a/endpoints": endpointBody("0.000001"),
		"/models/openai/b/endpoints": endpointBody("0.000001"),
	}}
	server := httptest.NewServer(lists)
	defer server.Close()
	cache := &ResponseCache{}
	fetch := func(previous Document, cache *ResponseCache) Document {
		t.Helper()
		document, err := Fetcher{Client: server.Client(), BaseURL: server.URL, Cache: cache}.Fetch(context.Background(), previous)
		if err != nil {
			t.Fatal(err)
		}
		return document
	}
	first := fetch(Document{}, cache)
	if statuses, conditional := lists.take(); len(statuses) != 5 || conditional != 0 {
		t.Fatalf("first fetch statuses %v with %d conditional requests, want 5 plain requests", statuses, conditional)
	}
	// Nothing changed: every request revalidates and answers 304, and the
	// Document is identical.
	second := fetch(first, cache)
	statuses, conditional := lists.take()
	if conditional != 5 || len(statuses) != 5 {
		t.Fatalf("second fetch statuses %v with %d conditional requests, want 5 conditional", statuses, conditional)
	}
	for _, status := range statuses {
		if status != http.StatusNotModified {
			t.Fatalf("second fetch statuses %v, want every response 304", statuses)
		}
	}
	firstBytes, _ := first.Encode()
	secondBytes, _ := second.Encode()
	if !bytes.Equal(firstBytes, secondBytes) {
		t.Fatal("a fully revalidated fetch changed the Document")
	}
	// A changed endpoint list answers in full and the price follows it.
	lists.mu.Lock()
	lists.bodies["/models/openai/b/endpoints"] = endpointBody("0.000003")
	lists.mu.Unlock()
	third := fetch(second, cache)
	if got := third.Models[1].Endpoints[0].Pricing.Prompt; got != "0.000003" {
		t.Fatalf("openai/b prompt price = %s after its endpoints changed, want 0.000003", got)
	}
	ok := 0
	statuses, _ = lists.take()
	for _, status := range statuses {
		if status == http.StatusOK {
			ok++
		}
	}
	if ok != 1 {
		t.Fatalf("third fetch statuses %v, want only the changed response in full", statuses)
	}
	// Without a cache no request is conditional.
	fetch(third, nil)
	if _, conditional := lists.take(); conditional != 0 {
		t.Fatalf("a fetch without a cache sent %d conditional requests", conditional)
	}
}

func TestFetchCapsEndpointLookupsOfKnownModels(t *testing.T) {
	var mu sync.Mutex
	ids := make([]string, 20)
	for index := range ids {
		ids[index] = fmt.Sprintf("openai/m%02d", index)
	}
	contextLength := map[string]int{}
	prompt := "0.000001"
	zeroRetention := ""
	lookups := map[string]int{}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch request.URL.Path {
		case "/providers":
			_ = json.NewEncoder(writer).Encode(map[string]any{"data": []map[string]any{{"name": testUpstreamProvider, "slug": "upstream", "headquarters": "US", "datacenters": []string{"US"}}}})
			return
		case "/endpoints/zdr":
			data := []map[string]any{}
			if zeroRetention != "" {
				data = append(data, map[string]any{"model_id": zeroRetention, "tag": "upstream"})
			}
			_ = json.NewEncoder(writer).Encode(map[string]any{"data": data})
			return
		case "/models":
			var data []map[string]any
			for _, id := range ids {
				length := 1000
				if value, ok := contextLength[id]; ok {
					length = value
				}
				data = append(data, map[string]any{"id": id, "context_length": length, "architecture": map[string]any{"input_modalities": []string{"text"}, "output_modalities": []string{"text"}},
					"pricing": map[string]string{"prompt": "0.000001", "completion": "0.000002"}})
			}
			_ = json.NewEncoder(writer).Encode(map[string]any{"data": data})
			return
		}
		id := strings.TrimSuffix(strings.TrimPrefix(request.URL.Path, "/models/"), "/endpoints")
		lookups[id]++
		_, _ = writer.Write([]byte(endpointBody(prompt)))
	}))
	defer server.Close()
	fetcher := Fetcher{Client: server.Client(), BaseURL: server.URL, MaxEndpointLookups: 5}
	// With no previous Document every model is new, so the cap does not
	// apply and the first catalog is complete.
	first, err := fetcher.Fetch(context.Background(), Document{})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Models) != 20 || len(lookups) != 20 {
		t.Fatalf("first fetch kept %d models after %d lookups, want 20 and 20", len(first.Models), len(lookups))
	}
	// Every price rises; m20 is new, m03's /models entry changed and m10
	// becomes zero retention.
	mu.Lock()
	lookups = map[string]int{}
	prompt = "0.000009"
	ids = append(ids, "openai/m20")
	contextLength["openai/m03"] = 2000
	zeroRetention = "openai/m10"
	mu.Unlock()
	second, err := fetcher.Fetch(context.Background(), first)
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(lookups) != 6 || lookups["openai/m20"] != 1 || lookups["openai/m03"] != 1 {
		t.Fatalf("second fetch looked up %v, want m20, m03 and four others", lookups)
	}
	if len(second.Models) != 21 {
		t.Fatalf("second fetch kept %d models, want 21", len(second.Models))
	}
	for _, model := range second.Models {
		endpoint := model.Endpoints[0]
		wantPrompt := "0.000001"
		if lookups[model.ID] > 0 {
			wantPrompt = "0.000009"
		}
		if endpoint.Pricing.Prompt != wantPrompt {
			t.Fatalf("%s prompt price = %s, want %s (looked up: %v)", model.ID, endpoint.Pricing.Prompt, wantPrompt, lookups[model.ID] > 0)
		}
		if endpoint.ZeroRetention != (model.ID == "openai/m10") {
			t.Fatalf("%s zero retention = %v, want it re-read from this fetch", model.ID, endpoint.ZeroRetention)
		}
		if model.ID == "openai/m03" && model.ContextLength != 2000 {
			t.Fatalf("m03 context length = %d, want the changed listing", model.ContextLength)
		}
	}
}
