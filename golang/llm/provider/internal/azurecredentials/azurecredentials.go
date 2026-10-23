// Package azurecredentials marks Azure token-credential failures so the
// OpenAI-SDK adapters can classify them as pre-dispatch authentication
// failures instead of ambiguous provider outcomes.
package azurecredentials

import (
	"context"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

// Wrap returns a token credential whose GetToken errors also match
// provider.ErrProviderCredentials. The underlying error, including any
// context error, stays in the chain. A nil credential is returned unchanged
// so client construction still rejects it.
func Wrap(credential azcore.TokenCredential) azcore.TokenCredential {
	if credential == nil {
		return nil
	}
	return markedCredential{next: credential}
}

type markedCredential struct{ next azcore.TokenCredential }

func (marked markedCredential) GetToken(ctx context.Context, options policy.TokenRequestOptions) (azcore.AccessToken, error) {
	token, err := marked.next.GetToken(ctx, options)
	if err != nil {
		return azcore.AccessToken{}, fmt.Errorf("%w: %w", provider.ErrProviderCredentials, err)
	}
	return token, nil
}
