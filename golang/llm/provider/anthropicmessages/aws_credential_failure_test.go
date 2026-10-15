package anthropicmessages

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	anthropicaws "github.com/anthropics/anthropic-sdk-go/aws"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm"
	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

// The AWS gateway client resolves credentials once at construction and again
// on every request while signing. A credential_process source that succeeds
// once with already-expired credentials and then fails reproduces a refresh
// failure in the SDK's own SigV4 middleware, before the transport (#1316).
func TestAWSGatewayCredentialFailureIsPreDispatch(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("credential_process fixture is a POSIX shell script")
	}
	dir := t.TempDir()
	script := filepath.Join(dir, "credentials.sh")
	if err := os.WriteFile(script, []byte(`#!/bin/sh
if [ -e "$0.used" ]; then exit 1; fi
: > "$0.used"
echo '{"Version":1,"AccessKeyId":"AKIDFIXTURE","SecretAccessKey":"fixture-secret","SessionToken":"fixture-token","Expiration":"2000-01-01T00:00:00Z"}'
`), 0o700); err != nil {
		t.Fatal(err)
	}
	configFile := filepath.Join(dir, "config")
	if err := os.WriteFile(configFile, []byte("[profile gateway-fixture]\ncredential_process = "+script+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_PROFILE", "AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_ROLE_ARN", "AWS_CONTAINER_CREDENTIALS_FULL_URI", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "ANTHROPIC_AWS_API_KEY"} {
		t.Setenv(name, "")
	}
	t.Setenv("AWS_CONFIG_FILE", configFile)
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(dir, "credentials"))
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")

	calls := 0
	client, err := NewAWSClient(context.Background(), AWSClientConfig{
		BaseURL: "http://127.0.0.1/aws-contract",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			calls++
			return nil, errors.New("transport must not be reached")
		})},
		AWSConfig: anthropicaws.ClientConfig{AWSRegion: "us-east-1", WorkspaceID: "ws-contract", AWSProfile: "gateway-fixture"},
	})
	if err != nil {
		t.Fatal(err)
	}
	profile := testProfile()
	profile.ExpectedBaseURL = "http://127.0.0.1/aws-contract"
	adapter, err := New(client, "anthropic-aws", profile)
	if err != nil {
		t.Fatal(err)
	}
	call, err := adapter.Compile(context.Background(), provider.CompileInput{
		Request: llm.Request{OperationKey: "op-credentials", Model: "claude-contract", Input: []llm.Item{llm.Message{Actor: llm.ActorHuman, Content: []llm.Part{llm.TextPart{Text: "hello"}}}}},
		Query:   provider.CapabilityQuery{EndpointID: "anthropic-aws", Family: provider.FamilyAnthropicMessages, Model: "claude-contract"},
		Strict:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = adapter.Invoke(context.Background(), call, nil)
	var mapped *provider.Error
	if !errors.As(err, &mapped) || mapped.Dispatch != provider.DispatchNotDispatched || mapped.Retry != provider.RetryNextRoute || !errors.Is(err, provider.ErrProviderPreDispatch) {
		t.Fatalf("Invoke() error = %#v, want a not-dispatched next-route failure with the pre-dispatch marker", err)
	}
	if mapped.SafeDetails["endpoint"] != "anthropic-aws" || calls != 0 {
		t.Fatalf("details = %v, transport calls = %d, want the endpoint and no transport call", mapped.SafeDetails, calls)
	}
}
