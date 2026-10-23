package azurecredentials

import (
	"context"
	"errors"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

type credentialFunc func(context.Context, policy.TokenRequestOptions) (azcore.AccessToken, error)

func (f credentialFunc) GetToken(ctx context.Context, options policy.TokenRequestOptions) (azcore.AccessToken, error) {
	return f(ctx, options)
}

func TestWrapMarksTokenFailuresAndPassesTokensThrough(t *testing.T) {
	cause := errors.New("workload identity token expired")
	failing := Wrap(credentialFunc(func(context.Context, policy.TokenRequestOptions) (azcore.AccessToken, error) {
		return azcore.AccessToken{}, cause
	}))
	if _, err := failing.GetToken(context.Background(), policy.TokenRequestOptions{}); !errors.Is(err, provider.ErrProviderCredentials) || !errors.Is(err, cause) {
		t.Fatalf("GetToken error = %v, want the credential marker and its cause", err)
	}
	canceled := Wrap(credentialFunc(func(ctx context.Context, _ policy.TokenRequestOptions) (azcore.AccessToken, error) {
		return azcore.AccessToken{}, ctx.Err()
	}))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := canceled.GetToken(ctx, policy.TokenRequestOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("GetToken error = %v, want the context error kept", err)
	}
	working := Wrap(credentialFunc(func(_ context.Context, options policy.TokenRequestOptions) (azcore.AccessToken, error) {
		return azcore.AccessToken{Token: options.Scopes[0]}, nil
	}))
	token, err := working.GetToken(context.Background(), policy.TokenRequestOptions{Scopes: []string{"scope"}})
	if err != nil || token.Token != "scope" {
		t.Fatalf("GetToken = %#v, %v", token, err)
	}
	if Wrap(nil) != nil {
		t.Fatal("Wrap(nil) should stay nil")
	}
}
