package bedrockconverse

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider/internal/adaptertest"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

func TestInvokePreservesExactSchemaNumbers(t *testing.T) {
	const schema = `{"type":"object","properties":{"id":{"type":"integer","enum":[9007199254740993,-9007199254740993,18446744073709551615],"minimum":9007199254740993},"decimal":{"type":"number","const":0.100000000000000000001,"maximum":1.234567890123456789e+20}},"required":["id","decimal"],"additionalProperties":false}`
	var wire []byte
	client, err := NewClient(context.Background(), ClientConfig{
		BaseURL:   "http://127.0.0.1",
		AWSConfig: aws.Config{Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("contract-access", "contract-secret", "")},
		HTTPClient: &http.Client{Transport: adaptertest.RoundTripFunc(func(request *http.Request) (*http.Response, error) {
			wire, _ = io.ReadAll(request.Body)
			return adaptertest.JSONResponse(request, http.StatusOK, `{}`), nil
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	params, err := lowerRequest(llm.Request{Model: bedrockFixtureModel, Input: []llm.Item{llm.HumanText("lookup")}, Tools: []llm.Tool{{Name: "lookup", InputSchema: json.RawMessage(schema)}}}, mustBedrockConverseProfile(t), "default", false)
	if err != nil {
		t.Fatal(err)
	}
	// Serialize through the real AWS SDK with a synthetic HTTP transport.
	_, invokeErr := client.converse.Converse(context.Background(), &params)
	if len(wire) == 0 {
		t.Fatalf("request was not dispatched: %v", invokeErr)
	}
	var body struct {
		ToolConfig struct {
			Tools []struct {
				ToolSpec struct {
					InputSchema struct {
						JSON json.RawMessage `json:"json"`
					} `json:"inputSchema"`
				} `json:"toolSpec"`
			} `json:"tools"`
		} `json:"toolConfig"`
	}
	if err := json.Unmarshal(wire, &body); err != nil {
		t.Fatal(err)
	}
	if len(body.ToolConfig.Tools) != 1 {
		t.Fatalf("tool schema missing: %s", wire)
	}
	actual := body.ToolConfig.Tools[0].ToolSpec.InputSchema.JSON
	if !adaptertest.SameJSON(t, actual, []byte(schema)) {
		t.Fatalf("schema changed on AWS transport: %s", actual)
	}
}
