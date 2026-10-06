// Package langfuse provides an isolated content-bearing OTLP HTTP exporter.
package langfuse

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

var ErrExport = errors.New("Langfuse export failed")
var ErrRejected = errors.New("Langfuse export rejected")

const maxBody = 16 << 20

type Client struct {
	endpoint, public, secret string
	http                     *http.Client
}

func NewClient(base, public, secret string, transport *http.Client) (*Client, error) {
	u, err := url.Parse(base)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || public == "" || secret == "" {
		return nil, ErrExport
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/api/public/otel/v1/traces"
	if transport == nil {
		t := http.DefaultTransport.(*http.Transport).Clone()
		t.Proxy = nil
		transport = &http.Client{Timeout: 8 * time.Second, Transport: t}
	}
	copy := *transport
	copy.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &Client{endpoint: u.String(), public: public, secret: secret, http: &copy}, nil
}

type Operation struct {
	TraceID, SpanID, SessionID, Kind string
	StartedAt, EndedAt               time.Time
	Context                          llm.RequestContext
	Input, Output                    any
	Metadata                         map[string]any
	Generations                      []Generation
}
type Generation struct {
	ID, Provider, Endpoint, Family, Model string
	StartedAt, EndedAt                    time.Time
	Input, Output                         any
	Metadata                              map[string]any
	Usage, Cost                           map[string]any
}
type attribute struct {
	Key   string         `json:"key"`
	Value map[string]any `json:"value"`
}
type span struct {
	TraceID      string      `json:"traceId"`
	SpanID       string      `json:"spanId"`
	ParentSpanID string      `json:"parentSpanId,omitempty"`
	Name         string      `json:"name"`
	Kind         int         `json:"kind"`
	Start        string      `json:"startTimeUnixNano"`
	End          string      `json:"endTimeUnixNano"`
	Attributes   []attribute `json:"attributes"`
}

func attr(k string, v any) attribute {
	s, ok := v.(string)
	if !ok {
		b, _ := json.Marshal(v)
		s = string(b)
		if len(b) > 0 && b[0] == '"' {
			_ = json.Unmarshal(b, &s)
		}
	}
	return attribute{Key: k, Value: map[string]any{"stringValue": s}}
}
func (o Operation) attributes() []attribute {
	a := []attribute{attr("langfuse.trace.name", o.Kind), attr("langfuse.observation.metadata.tenant", o.Context.Tenant), attr("langfuse.observation.metadata.project", o.Context.Project), attr("langfuse.observation.metadata.actor", o.Context.Actor)}
	if o.Context.Actor != "" {
		a = append(a, attr("langfuse.user.id", o.Context.Actor))
	}
	if o.SessionID != "" {
		a = append(a, attr("langfuse.session.id", o.SessionID))
	}
	if len(o.Context.Tags) > 0 {
		a = append(a, attr("langfuse.observation.metadata.caller_tags", o.Context.Tags))
	}
	return a
}
func makeSpan(o Operation, id, parent, name, typ string, start, end time.Time, input, output any, metadata map[string]any) span {
	if end.Before(start) {
		end = start
	}
	s := span{TraceID: o.TraceID, SpanID: id, ParentSpanID: parent, Name: name, Kind: 1, Start: strconv.FormatInt(start.UnixNano(), 10), End: strconv.FormatInt(end.UnixNano(), 10), Attributes: append(o.attributes(), attr("langfuse.observation.type", typ))}
	if input != nil {
		s.Attributes = append(s.Attributes, attr("langfuse.observation.input", input))
	}
	if output != nil {
		s.Attributes = append(s.Attributes, attr("langfuse.observation.output", output))
	}
	for k, v := range metadata {
		s.Attributes = append(s.Attributes, attr("langfuse.observation.metadata."+k, v))
	}
	return s
}
func (c *Client) Export(ctx context.Context, o Operation) error {
	spans := []span{makeSpan(o, o.SpanID, "", o.Kind, "span", o.StartedAt, o.EndedAt, o.Input, o.Output, o.Metadata)}
	for _, g := range o.Generations {
		s := makeSpan(o, g.ID, o.SpanID, g.Provider+"/"+g.Model, "generation", g.StartedAt, g.EndedAt, g.Input, g.Output, g.Metadata)
		s.Attributes = append(s.Attributes, attr("langfuse.observation.model.name", g.Provider+"/"+g.Model), attr("langfuse.observation.metadata.resolved_model", g.Model), attr("langfuse.observation.metadata.provider", g.Provider), attr("langfuse.observation.metadata.endpoint_id", g.Endpoint), attr("langfuse.observation.metadata.api_family", g.Family))
		if g.Usage != nil {
			s.Attributes = append(s.Attributes, attr("langfuse.observation.usage_details", g.Usage))
		}
		if g.Cost != nil {
			s.Attributes = append(s.Attributes, attr("langfuse.observation.cost_details", g.Cost))
		}
		spans = append(spans, s)
	}
	payload := map[string]any{"resourceSpans": []any{map[string]any{"resource": map[string]any{"attributes": []attribute{attr("service.name", "llm-temporal-worker")}}, "scopeSpans": []any{map[string]any{"scope": map[string]string{"name": "llmtw.langfuse"}, "spans": spans}}}}}
	body, err := json.Marshal(payload)
	if err != nil || len(body) > maxBody {
		return ErrRejected
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return ErrExport
	}
	req.SetBasicAuth(c.public, c.secret)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-langfuse-ingestion-version", "4")
	response, err := c.http.Do(req)
	if err != nil {
		return ErrExport
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 && response.StatusCode < 500 && response.StatusCode != 408 && response.StatusCode != 429 {
		return ErrRejected
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return ErrExport
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, 65537))
	if err != nil || len(data) > 65536 {
		return ErrExport
	}
	var result struct {
		Partial *struct {
			Rejected json.RawMessage `json:"rejectedSpans"`
			Message  string          `json:"errorMessage"`
		} `json:"partialSuccess"`
	}
	if len(data) > 0 && json.Unmarshal(data, &result) != nil {
		return ErrExport
	}
	if result.Partial != nil && (result.Partial.Message != "" || (len(result.Partial.Rejected) > 0 && string(result.Partial.Rejected) != "0" && string(result.Partial.Rejected) != `"0"`)) {
		return ErrExport
	}
	return nil
}

// Close releases connections when an immutable runtime snapshot drains.
func (c *Client) Close() {
	if c != nil {
		c.http.CloseIdleConnections()
	}
}
