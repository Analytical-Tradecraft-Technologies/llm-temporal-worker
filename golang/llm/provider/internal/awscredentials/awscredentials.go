// Package awscredentials marks AWS credential-resolution failures so Bedrock
// adapters can classify them as pre-dispatch authentication failures instead
// of ambiguous provider outcomes.
package awscredentials

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

// Wrap returns a credentials provider whose Retrieve errors also match
// provider.ErrProviderCredentials. The underlying error, including any
// context error, stays in the chain. Nil and anonymous providers are returned
// unchanged so the SDK still recognises them.
func Wrap(credentials aws.CredentialsProvider) aws.CredentialsProvider {
	if credentials == nil || aws.IsCredentialsProvider(credentials, (*aws.AnonymousCredentials)(nil)) {
		return credentials
	}
	return markedProvider{next: credentials}
}

type markedProvider struct{ next aws.CredentialsProvider }

func (marked markedProvider) Retrieve(ctx context.Context) (aws.Credentials, error) {
	credentials, err := marked.next.Retrieve(ctx)
	if err != nil {
		return aws.Credentials{}, fmt.Errorf("%w: %w", provider.ErrProviderCredentials, err)
	}
	return credentials, nil
}

// IsCredentialsProvider lets aws.IsCredentialsProvider see through the wrapper.
func (marked markedProvider) IsCredentialsProvider(target aws.CredentialsProvider) bool {
	return aws.IsCredentialsProvider(marked.next, target)
}
