package runtime

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/mfow/llm-temporal-worker/golang/config"
	"github.com/mfow/llm-temporal-worker/golang/engine"
	"github.com/mfow/llm-temporal-worker/golang/internal/secrets"
	"github.com/mfow/llm-temporal-worker/golang/llm"
	"github.com/mfow/llm-temporal-worker/golang/llm/provider"
	"github.com/mfow/llm-temporal-worker/golang/routing"
)

const (
	tierTestChatBody      = `{"id":"chatcmpl-tier","object":"chat.completion","created":1,"model":"served-model","choices":[{"index":0,"message":{"role":"assistant","content":"done"},"finish_reason":"stop"}],"usage":{"prompt_tokens":2,"completion_tokens":1,"total_tokens":3}%s}`
	tierTestResponsesBody = `{"id":"resp_tier","object":"response","created_at":1,"status":"completed","model":"served-model","output":[{"type":"message","id":"msg_tier","status":"completed","role":"assistant","content":[{"type":"output_text","text":"done","annotations":[]}]}],"usage":{"input_tokens":2,"output_tokens":1,"total_tokens":3,"input_tokens_details":{"cached_tokens":0},"output_tokens_details":{"reasoning_tokens":0}}%s}`
	tierTestMessagesBody  = `{"id":"msg_tier","type":"message","role":"assistant","model":"served-model","content":[{"type":"text","text":"done"}],"stop_reason":"end_turn","usage":{"input_tokens":2,"output_tokens":1%s}}`
	tierTestConverseBody  = `{"output":{"message":{"role":"assistant","content":[{"text":"done"}]}},"stopReason":"end_turn","usage":{"inputTokens":2,"outputTokens":1,"totalTokens":3},"metrics":{"latencyMs":1}%s}`
)

// tierTestFamily describes one production endpoint shape. tierField renders
// a provider tier into the place that family's response body carries it.
type tierTestFamily struct {
	name      string
	endpoint  config.EndpointConfig
	family    provider.Family
	model     string
	class     llm.ServiceClass
	body      string
	tierField func(string) string
	// sendsTier is the service_tier the request body must carry; empty means
	// the provider API does not define the field and it must be absent.
	sendsTier string
	// skipRequestTier leaves the request tier unchecked for families whose
	// request placement this test does not pin.
	skipRequestTier bool
	unmapped        string
}

func tierTestFamilies() []tierTestFamily {
	rootField := func(tier string) string { return `,"service_tier":"` + tier + `"` }
	classes := func(standard, priority string) map[llm.ServiceClass]config.TierConfig {
		result := map[llm.ServiceClass]config.TierConfig{llm.ServiceClassStandard: {ProviderValue: standard}}
		if priority != "" {
			result[llm.ServiceClassPriority] = config.TierConfig{ProviderValue: priority}
		}
		return result
	}
	key := config.AuthConfig{Kind: "bearer_env", Name: "PROVIDER_API_KEY"}
	header := config.AuthConfig{Kind: "header_env", Name: "PROVIDER_API_KEY"}
	chain := config.AuthConfig{Kind: "aws_default_chain"}
	return []tierTestFamily{
		{
			name: "azure chat", family: provider.FamilyOpenAIChat, model: "chat-deployment", class: llm.ServiceClassPriority,
			endpoint: config.EndpointConfig{Family: "azure_openai_chat", BaseURL: "https://example.openai.azure.com", OutboundHosts: []string{"example.openai.azure.com"}, Auth: header,
				ServiceClasses: classes("default", "priority"), Extensions: map[string]map[string]any{"azure": {"api_version": "2024-10-21", "deployment": "chat-deployment"}}},
			body: tierTestChatBody, tierField: rootField, unmapped: "scale",
		},
		{
			name: "azure responses", family: provider.FamilyOpenAIResponses, model: "responses-deployment", class: llm.ServiceClassPriority,
			endpoint: config.EndpointConfig{Family: "azure_openai_responses", BaseURL: "https://example.openai.azure.com/openai/v1", OutboundHosts: []string{"example.openai.azure.com"}, Auth: header,
				ServiceClasses: classes("default", "priority"), Extensions: map[string]map[string]any{"azure": {"api_version": "2024-10-21"}}},
			body: tierTestResponsesBody, tierField: rootField, unmapped: "scale",
		},
		{
			name: "openai responses", family: provider.FamilyOpenAIResponses, model: "gpt-tier", class: llm.ServiceClassPriority,
			endpoint: config.EndpointConfig{Family: "openai_responses", BaseURL: "https://api.openai.com/v1", OutboundHosts: []string{"api.openai.com"}, Auth: key,
				ServiceClasses: classes("default", "priority")},
			body: tierTestResponsesBody, tierField: rootField, sendsTier: "priority", unmapped: "scale",
		},
		{
			name: "openrouter", family: provider.FamilyOpenAIChat, model: "vendor/model", class: llm.ServiceClassPriority,
			endpoint: config.EndpointConfig{Family: "openai_chat", BaseURL: "https://openrouter.ai/api/v1", OutboundHosts: []string{"openrouter.ai"}, Auth: key,
				ServiceClasses: classes("default", "priority"), Extensions: map[string]map[string]any{"openrouter": {"provider_order": []any{"ProviderA"}}}},
			// OpenRouter relays the upstream provider's own label.
			body: tierTestChatBody, tierField: rootField, sendsTier: "priority", unmapped: "standard",
		},
		{
			name: "exa", family: provider.FamilyOpenAIChat, model: "exa", class: llm.ServiceClassStandard,
			endpoint: config.EndpointConfig{Family: "openai_chat", BaseURL: "https://api.exa.ai", OutboundHosts: []string{"api.exa.ai"}, Auth: key,
				ServiceClasses: classes("standard", ""), Extensions: map[string]map[string]any{"exa": {}}},
			body: tierTestChatBody, tierField: rootField, unmapped: "scale",
		},
		{
			name: "generic chat", family: provider.FamilyOpenAIChat, model: "compatible-model", class: llm.ServiceClassPriority,
			endpoint: config.EndpointConfig{Family: "openai_chat", BaseURL: "https://chat.example.com/v1", OutboundHosts: []string{"chat.example.com"}, Auth: key,
				ServiceClasses: classes("default", "priority")},
			body: tierTestChatBody, tierField: rootField, sendsTier: "priority", unmapped: "scale",
		},
		{
			name: "bedrock messages", family: provider.FamilyBedrockMessages, model: "anthropic.claude-tier-v1:0", class: llm.ServiceClassPriority,
			endpoint: config.EndpointConfig{Family: "bedrock_anthropic_messages", BaseURL: "https://bedrock-runtime.us-east-1.amazonaws.com", OutboundHosts: []string{"bedrock-runtime.us-east-1.amazonaws.com"}, Region: "us-east-1", Auth: chain,
				ServiceClasses: classes("default", "priority")},
			// InvokeModel takes the requested tier as a request header, so the
			// Anthropic body must not carry one.
			body: tierTestMessagesBody, tierField: rootField, unmapped: "reserved",
		},
		{
			name: "bedrock converse", family: provider.FamilyBedrockConverse, model: "anthropic.claude-tier-v1:0", class: llm.ServiceClassPriority,
			endpoint: config.EndpointConfig{Family: "bedrock_converse", BaseURL: "https://bedrock-runtime.us-east-1.amazonaws.com", OutboundHosts: []string{"bedrock-runtime.us-east-1.amazonaws.com"}, Region: "us-east-1", Auth: chain,
				ServiceClasses: classes("default", "priority")},
			body: tierTestConverseBody, tierField: func(tier string) string { return `,"serviceTier":{"type":"` + tier + `"}` }, skipRequestTier: true, unmapped: "reserved",
		},
	}
}

// Every family is built by ProductionEngineFactory.buildAdapter from endpoint
// configuration alone, so the profile under test is the one production gets.
// A successful response is paid for: neither a missing service tier (the
// documented shape for Azure, Exa, Bedrock InvokeModel and an optional field
// everywhere else) nor a tier label the profile cannot map may discard it.
func TestProductionAdaptersAcceptResponsesWithoutMappableServiceTier(t *testing.T) {
	for _, family := range tierTestFamilies() {
		for _, test := range []struct {
			name, tier string
		}{
			{name: "no tier"},
			{name: "unmapped tier", tier: family.unmapped},
		} {
			t.Run(family.name+"/"+test.name, func(t *testing.T) {
				tierField := ""
				if test.tier != "" {
					tierField = family.tierField(test.tier)
				}
				body := jsonTemplate(family.body, tierField)
				result, request, _ := invokeProductionAdapter(t, family, body, nil)
				service := result.Response.Service
				if service.Actual != nil {
					t.Fatalf("actual class = %q, want none for provider tier %q", *service.Actual, test.tier)
				}
				if service.Attempted != family.class || service.Requested != family.class || service.ProviderValue != test.tier {
					t.Fatalf("service facts = %+v, want attempted %q and provider value %q", service, family.class, test.tier)
				}
				if result.Response.Status != llm.ResponseStatusCompleted || result.Response.Usage.InputTokens != 2 || result.Response.Usage.OutputTokens != 1 {
					t.Fatalf("response = %+v", result.Response)
				}
				if family.skipRequestTier {
					return
				}
				sent, present := request["service_tier"]
				if family.sendsTier == "" && present {
					t.Fatalf("request sent service_tier %v to an API that does not define it", sent)
				}
				if family.sendsTier != "" && sent != family.sendsTier {
					t.Fatalf("request service_tier = %v, want %q", sent, family.sendsTier)
				}
			})
		}
	}
}

func TestProductionExaAdapterSendsTextAsRootField(t *testing.T) {
	for _, family := range tierTestFamilies() {
		if family.name != "exa" {
			continue
		}
		_, request, _ := invokeProductionAdapter(t, family, jsonTemplate(family.body, ""), nil)
		if request["text"] != true {
			t.Fatalf("request text = %v, want root-level true", request["text"])
		}
		if _, present := request["extra_body"]; present {
			t.Fatalf("request sent a literal extra_body: %v", request)
		}
	}
}

// InvokeModel reports the tier that served the request in a response header
// rather than in the Anthropic body.
func TestProductionBedrockMessagesAdapterReadsServiceTierHeader(t *testing.T) {
	for _, family := range tierTestFamilies() {
		if family.family != provider.FamilyBedrockMessages {
			continue
		}
		result, _, _ := invokeProductionAdapter(t, family, jsonTemplate(family.body, ""), http.Header{"X-Amzn-Bedrock-Service-Tier": {"default"}})
		service := result.Response.Service
		if service.Actual == nil || *service.Actual != llm.ServiceClassStandard || service.ProviderValue != "default" || service.Attempted != llm.ServiceClassPriority {
			t.Fatalf("service facts = %+v, want the header tier lifted as standard", service)
		}
	}
}

// InvokeModel defines the requested tier as the X-Amzn-Bedrock-Service-Tier
// request header, the same header that reports the served tier. The Anthropic
// body has no field for a Bedrock tier, so one there is rejected or ignored.
func TestProductionBedrockMessagesAdapterSendsServiceTierHeader(t *testing.T) {
	for _, family := range tierTestFamilies() {
		if family.family != provider.FamilyBedrockMessages {
			continue
		}
		_, request, headers := invokeProductionAdapter(t, family, jsonTemplate(family.body, ""), nil)
		if got := headers.Get("X-Amzn-Bedrock-Service-Tier"); got != "priority" {
			t.Fatalf("request service tier header = %q, want %q", got, "priority")
		}
		if sent, present := request["service_tier"]; present {
			t.Fatalf("request body carried service_tier %v, which InvokeModel does not define", sent)
		}
	}
}

func jsonTemplate(template, tierField string) string {
	for index := 0; index < len(template)-1; index++ {
		if template[index] == '%' && template[index+1] == 's' {
			return template[:index] + tierField + template[index+2:]
		}
	}
	return template
}

// invokeProductionAdapter builds the family's adapter through the production
// factory, serves one canned response over the guarded egress transport and
// returns the lifted result with the decoded request body and headers.
func invokeProductionAdapter(t *testing.T, family tierTestFamily, body string, headers http.Header) (provider.Result, map[string]any, http.Header) {
	t.Helper()
	const endpointID = "tier-endpoint"
	var mu sync.Mutex
	var captured []byte
	var capturedHeaders http.Header
	server, base := newTierTestProvider(t, family.endpoint.OutboundHosts, http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		received, _ := io.ReadAll(request.Body)
		mu.Lock()
		captured = received
		capturedHeaders = request.Header.Clone()
		mu.Unlock()
		for name, values := range headers {
			writer.Header()[name] = values
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(writer, body)
	}))
	factory, err := NewProductionEngineFactory(ProductionFactoryOptions{
		Resolver:       secrets.ResolverFunc(func(context.Context, config.SecretRef) ([]byte, error) { return []byte("test-key"), nil }),
		SnapshotLoader: SnapshotLoaderFunc(func(context.Context, *config.Snapshot) (engine.Snapshot, error) { return engine.Snapshot{}, nil }),
		HTTPClient:     base,
		EgressResolver: &egressTestResolver{addresses: []net.IPAddr{{IP: net.ParseIP("8.8.8.8")}}},
		EgressDial: func(ctx context.Context, network, _ string) (net.Conn, error) {
			connection, dialErr := (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
			if dialErr != nil {
				return nil, dialErr
			}
			return egressTestConn{Conn: connection, remote: &net.TCPAddr{IP: net.ParseIP("8.8.8.8"), Port: 443}}, nil
		},
		AWSConfigFactory: func(_ context.Context, region string) (aws.Config, error) {
			return aws.Config{Region: region, Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
				return aws.Credentials{AccessKeyID: "AKIDTIERTEST", SecretAccessKey: "tier-test-secret"}, nil
			})}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	features := map[string]routing.Capability{}
	for _, feature := range allProviderFeatures {
		features[string(feature)] = routing.Capability{State: routing.CapabilityNative}
	}
	for _, feature := range []provider.Feature{provider.FeatureStreaming, provider.FeatureContinuation} {
		features[string(feature)] = routing.Capability{State: routing.CapabilityUnsupported, Reason: "not exercised by the tier test"}
	}
	snapshot := engine.Snapshot{Routes: routing.Catalog{Models: map[string]routing.Model{
		"model": {Routes: []routing.Route{{EndpointID: endpointID, Capabilities: routing.CapabilitySet{Version: "tier-test/v1"}, ProviderFeatures: features}}},
	}}}
	value := config.Config{Endpoints: map[string]config.EndpointConfig{endpointID: family.endpoint}}
	adapter, err := factory.buildAdapter(context.Background(), value, snapshot, endpointID, nil)
	if err != nil {
		t.Fatalf("buildAdapter() error = %v", err)
	}
	call, err := adapter.Compile(context.Background(), provider.CompileInput{
		Request: llm.Request{OperationKey: "tier-test", Model: family.model, ServiceClass: family.class, Input: []llm.Item{
			llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "hello"}}},
		}},
		Query:  provider.CapabilityQuery{EndpointID: endpointID, Family: family.family, Model: family.model},
		Strict: true,
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	result, err := adapter.Invoke(context.Background(), call, nil)
	if err != nil {
		t.Fatalf("Invoke() error = %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	var request map[string]any
	if err := json.Unmarshal(captured, &request); err != nil {
		t.Fatalf("request body %q: %v", captured, err)
	}
	return result, request, capturedHeaders
}

// newTierTestProvider starts a loopback TLS server whose certificate names the
// endpoint's real hosts, so the production egress guard (which rejects
// InsecureSkipVerify and ServerName overrides) verifies it normally.
func newTierTestProvider(t *testing.T, hosts []string, handler http.Handler) (*httptest.Server, *http.Client) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "tier test provider"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true, IsCA: true, DNSNames: hosts,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Skipf("test environment does not allow a loopback listener: %v", err)
	}
	server := &httptest.Server{Listener: listener, Config: &http.Server{Handler: handler}}
	server.TLS = &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}, MinVersion: tls.VersionTLS12}
	server.StartTLS()
	t.Cleanup(server.Close)
	pool := x509.NewCertPool()
	pool.AddCert(certificate)
	return server, &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}}}
}
